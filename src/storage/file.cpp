#include "kvdb/storage/file.hpp"
#include "file_test_hooks.hpp"
#include <cstdio>
#include <cstring>
#include <algorithm>
#include <filesystem>
#include <mutex>

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#endif

namespace kvdb::storage {

namespace {

std::mutex failure_mutex;
testing::FileFailurePoint failure_point = testing::FileFailurePoint::None;
std::size_t failure_bytes_before = 0;

bool consume_failure(testing::FileFailurePoint point, std::size_t& bytes_before_failure) {
    std::lock_guard<std::mutex> lock(failure_mutex);
    if (failure_point != point) return false;
    bytes_before_failure = failure_bytes_before;
    failure_point = testing::FileFailurePoint::None;
    failure_bytes_before = 0;
    return true;
}

} // namespace

void testing::fail_next_file_operation(FileFailurePoint point, std::size_t bytes_before_failure) {
    std::lock_guard<std::mutex> lock(failure_mutex);
    failure_point = point;
    failure_bytes_before = bytes_before_failure;
}

void testing::clear_file_failure() {
    std::lock_guard<std::mutex> lock(failure_mutex);
    failure_point = FileFailurePoint::None;
    failure_bytes_before = 0;
}

struct File::Impl {
    std::string path;
    std::uint64_t file_size{0};
#if defined(_WIN32)
    HANDLE handle{INVALID_HANDLE_VALUE};
#else
    int fd{-1};
#endif
};

File::File() : impl_(std::make_unique<Impl>()) {}

File::~File() {
    close();
}

File::File(File&& other) noexcept = default;
File& File::operator=(File&& other) noexcept = default;

Status File::open(const std::string& path, bool create_if_missing) {
    close();
    impl_->path = path;

#if defined(_WIN32)
    // Convert UTF-8 path to UTF-16 wide string for full Unicode support
    int wlen = MultiByteToWideChar(CP_UTF8, 0, path.c_str(), -1, nullptr, 0);
    std::wstring wpath(wlen, L'\0');
    MultiByteToWideChar(CP_UTF8, 0, path.c_str(), -1, &wpath[0], wlen);

    DWORD creation = create_if_missing ? OPEN_ALWAYS : OPEN_EXISTING;
    HANDLE h = CreateFileW(
        wpath.c_str(),
        GENERIC_READ | GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        nullptr,
        creation,
        FILE_ATTRIBUTE_NORMAL,
        nullptr
    );

    if (h == INVALID_HANDLE_VALUE) {
        DWORD err = GetLastError();
        if (err == ERROR_FILE_NOT_FOUND) return Status::NotFound;
        if (err == ERROR_DISK_FULL || err == ERROR_HANDLE_DISK_FULL) return Status::NoSpace;
        return Status::IoError;
    }

    LARGE_INTEGER size_li;
    if (!GetFileSizeEx(h, &size_li)) {
        CloseHandle(h);
        return Status::IoError;
    }

    impl_->handle = h;
    impl_->file_size = static_cast<std::uint64_t>(size_li.QuadPart);
    return Status::Ok;
#else
    int flags = O_RDWR;
    if (create_if_missing) {
        flags |= O_CREAT;
    }
    int fd = ::open(path.c_str(), flags, 0644);
    if (fd < 0) {
        if (errno == ENOENT) return Status::NotFound;
        if (errno == ENOSPC) return Status::NoSpace;
        return Status::IoError;
    }

    struct stat st;
    if (::fstat(fd, &st) != 0) {
        ::close(fd);
        return Status::IoError;
    }

    impl_->fd = fd;
    impl_->file_size = static_cast<std::uint64_t>(st.st_size);
    return Status::Ok;
#endif
}

Status File::close() {
#if defined(_WIN32)
    if (impl_->handle != INVALID_HANDLE_VALUE) {
        CloseHandle(impl_->handle);
        impl_->handle = INVALID_HANDLE_VALUE;
        impl_->file_size = 0;
    }
#else
    if (impl_->fd >= 0) {
        ::close(impl_->fd);
        impl_->fd = -1;
        impl_->file_size = 0;
    }
#endif
    return Status::Ok;
}

Status File::append(const std::uint8_t* data, std::size_t size) {
    if (!is_open()) return Status::NotOpen;
    if (size == 0) return Status::Ok;

    std::size_t failure_after = 0;
    const bool inject_failure = consume_failure(testing::FileFailurePoint::Append, failure_after);
    const std::size_t write_size = inject_failure ? std::min(size, failure_after) : size;
    if (write_size == 0 && inject_failure) return Status::IoError;

#if defined(_WIN32)
    std::size_t total_written = 0;
    while (total_written < write_size) {
        DWORD to_write = static_cast<DWORD>(std::min<std::size_t>(write_size - total_written, 64 * 1024 * 1024));
        DWORD written = 0;

        OVERLAPPED ov{};
        std::uint64_t offset = impl_->file_size + total_written;
        ov.Offset = static_cast<DWORD>(offset & 0xFFFFFFFF);
        ov.OffsetHigh = static_cast<DWORD>((offset >> 32) & 0xFFFFFFFF);

        if (!WriteFile(impl_->handle, data + total_written, to_write, &written, &ov)) {
            DWORD err = GetLastError();
            impl_->file_size += total_written;
            if (err == ERROR_DISK_FULL || err == ERROR_HANDLE_DISK_FULL) {
                return Status::NoSpace;
            }
            return Status::IoError;
        }

        if (written == 0) {
            impl_->file_size += total_written;
            return Status::IoError;
        }
        total_written += written;
    }
    impl_->file_size += total_written;
    return inject_failure ? Status::IoError : Status::Ok;
#else
    std::size_t total_written = 0;
    while (total_written < write_size) {
        ssize_t written = ::pwrite(
            impl_->fd,
            data + total_written,
            write_size - total_written,
            static_cast<off_t>(impl_->file_size + total_written)
        );
        if (written < 0) {
            if (errno == EINTR) continue;
            impl_->file_size += total_written;
            if (errno == ENOSPC) return Status::NoSpace;
            return Status::IoError;
        }
        if (written == 0) {
            impl_->file_size += total_written;
            return Status::IoError;
        }
        total_written += static_cast<std::size_t>(written);
    }
    impl_->file_size += total_written;
    return inject_failure ? Status::IoError : Status::Ok;
#endif
}

Status File::read_at(std::uint64_t offset, std::uint8_t* buffer, std::size_t size) const {
    if (!is_open()) return Status::NotOpen;
    if (size == 0) return Status::Ok;
    if (offset + size > impl_->file_size) {
        return Status::IoError; // Attempt to read past EOF
    }

#if defined(_WIN32)
    std::size_t total_read = 0;
    while (total_read < size) {
        DWORD to_read = static_cast<DWORD>(std::min<std::size_t>(size - total_read, 64 * 1024 * 1024));
        DWORD bytes_read = 0;

        OVERLAPPED ov{};
        std::uint64_t cur_offset = offset + total_read;
        ov.Offset = static_cast<DWORD>(cur_offset & 0xFFFFFFFF);
        ov.OffsetHigh = static_cast<DWORD>((cur_offset >> 32) & 0xFFFFFFFF);

        if (!ReadFile(impl_->handle, buffer + total_read, to_read, &bytes_read, &ov)) {
            return Status::IoError;
        }
        if (bytes_read == 0) {
            return Status::IoError;
        }
        total_read += bytes_read;
    }
    return Status::Ok;
#else
    std::size_t total_read = 0;
    while (total_read < size) {
        ssize_t bytes_read = ::pread(
            impl_->fd,
            buffer + total_read,
            size - total_read,
            static_cast<off_t>(offset + total_read)
        );
        if (bytes_read < 0) {
            if (errno == EINTR) continue;
            return Status::IoError;
        }
        if (bytes_read == 0) return Status::IoError;
        total_read += static_cast<std::size_t>(bytes_read);
    }
    return Status::Ok;
#endif
}

Status File::sync() {
    if (!is_open()) return Status::NotOpen;

    std::size_t unused_failure_bytes = 0;
    if (consume_failure(testing::FileFailurePoint::Sync, unused_failure_bytes)) {
        return Status::IoError;
    }

#if defined(_WIN32)
    if (!FlushFileBuffers(impl_->handle)) {
        return Status::IoError;
    }
    return Status::Ok;
#else
    #if defined(__APPLE__)
        if (::fcntl(impl_->fd, F_FULLFSYNC) != 0) return Status::IoError;
    #else
        if (::fsync(impl_->fd) != 0) return Status::IoError;
    #endif
    return Status::Ok;
#endif
}

Status File::truncate(std::uint64_t new_size) {
    if (!is_open()) return Status::NotOpen;

#if defined(_WIN32)
    LARGE_INTEGER li;
    li.QuadPart = static_cast<LONGLONG>(new_size);
    if (!SetFilePointerEx(impl_->handle, li, nullptr, FILE_BEGIN)) {
        return Status::IoError;
    }
    if (!SetEndOfFile(impl_->handle)) {
        return Status::IoError;
    }
    impl_->file_size = new_size;
    return sync();
#else
    if (::ftruncate(impl_->fd, static_cast<off_t>(new_size)) != 0) {
        return Status::IoError;
    }
    impl_->file_size = new_size;
    return sync();
#endif
}

std::uint64_t File::size() const {
    return impl_->file_size;
}

bool File::is_open() const {
#if defined(_WIN32)
    return impl_->handle != INVALID_HANDLE_VALUE;
#else
    return impl_->fd >= 0;
#endif
}

const std::string& File::path() const {
    return impl_->path;
}

Status atomic_rename_file(const std::string& old_path, const std::string& new_path) {
#if defined(_WIN32)
    int old_wlen = MultiByteToWideChar(CP_UTF8, 0, old_path.c_str(), -1, nullptr, 0);
    std::wstring old_w(old_wlen, L'\0');
    MultiByteToWideChar(CP_UTF8, 0, old_path.c_str(), -1, &old_w[0], old_wlen);

    int new_wlen = MultiByteToWideChar(CP_UTF8, 0, new_path.c_str(), -1, nullptr, 0);
    std::wstring new_w(new_wlen, L'\0');
    MultiByteToWideChar(CP_UTF8, 0, new_path.c_str(), -1, &new_w[0], new_wlen);

    if (!MoveFileExW(old_w.c_str(), new_w.c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
        return Status::IoError;
    }
    return Status::Ok;
#else
    if (::rename(old_path.c_str(), new_path.c_str()) != 0) {
        return Status::IoError;
    }
    return Status::Ok;
#endif
}

} // namespace kvdb::storage
