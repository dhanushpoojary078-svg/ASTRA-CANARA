#include "kvdb/database.hpp"
#include "kvdb/storage/file.hpp"
#include "kvdb/storage/index.hpp"
#include "kvdb/storage/format.hpp"
#include "kvdb/recovery/recovery.hpp"
#include "kvdb/concurrency/lock_manager.hpp"
#include "kvdb/concurrency/compactor.hpp"
#include <iostream>
#include <atomic>
#include <algorithm>
#include <cstring>

namespace kvdb {

class DatabaseImpl {
public:
    std::string path;
    Config config;
    storage::File file;
    storage::Index index;
    concurrency::LockManager lock_mgr;
    std::atomic<TxId> next_tx_id{1};
    bool is_open{false};
    bool write_failed{false};

    Status append_transaction(const std::vector<Operation>& ops) {
        if (!is_open) return Status::NotOpen;
        if (write_failed) return Status::IoError;

        TxId tx_id = next_tx_id.fetch_add(1);

        // Pre-encode frames to calculate size
        storage::Frame begin_frame{RecordType::Begin, tx_id, "", ""};
        auto begin_bytes = storage::serialize_frame(begin_frame);

        const std::uint64_t transaction_start = file.size();
        std::vector<std::uint8_t> transaction_bytes;
        transaction_bytes.insert(transaction_bytes.end(), begin_bytes.begin(), begin_bytes.end());
        std::uint64_t total_payload_bytes = begin_bytes.size();

        struct StagedOp {
            Operation::Type type;
            std::string key;
            std::uint64_t val_offset;
            std::uint64_t val_len;
        };
        std::vector<StagedOp> staged;
        std::uint64_t current_file_offset = transaction_start + begin_bytes.size();

        for (const auto& op : ops) {
            RecordType rtype = (op.type == Operation::Type::Put) ? RecordType::Put : RecordType::Delete;
            storage::Frame f{rtype, tx_id, op.key, op.value};
            auto b = storage::serialize_frame(f);
            total_payload_bytes += b.size();
            transaction_bytes.insert(transaction_bytes.end(), b.begin(), b.end());

            std::uint64_t val_offset = current_file_offset + storage::FRAME_HEADER_SIZE + op.key.size();
            staged.push_back({op.type, op.key, val_offset, static_cast<std::uint64_t>(op.value.size())});
            current_file_offset += b.size();

        }

        storage::Frame commit_frame{RecordType::Commit, tx_id, "", ""};
        auto commit_bytes = storage::serialize_frame(commit_frame);
        total_payload_bytes += commit_bytes.size();
        transaction_bytes.insert(transaction_bytes.end(), commit_bytes.begin(), commit_bytes.end());

        // Check space capacity
        if (!concurrency::SpaceManager::has_space_for(file.size(), total_payload_bytes, config.max_bytes)) {
            return Status::NoSpace;
        }

        auto rollback_transaction = [&](Status failure) {
            if (!is_ok(file.truncate(transaction_start))) {
                write_failed = true;
            }
            return failure;
        };

        // Write the complete framed transaction before synchronizing it.
        Status s = file.append(transaction_bytes.data(), transaction_bytes.size());
        if (!is_ok(s)) return rollback_transaction(s);

        // Durable sync before updating in-memory index or reporting success
        s = file.sync();
        if (!is_ok(s)) return rollback_transaction(s);

        // Publish to in-memory index
        for (const auto& item : staged) {
            if (item.type == Operation::Type::Put) {
                index.put(item.key, storage::IndexEntry{item.val_offset, item.val_len, tx_id});
            } else {
                index.remove(item.key);
            }
        }

        return Status::Ok;
    }
};

Database::Database() : impl_(std::make_unique<DatabaseImpl>()) {}
Database::~Database() { close(); }

Database::Database(Database&&) noexcept = default;
Database& Database::operator=(Database&&) noexcept = default;

Status Database::open(const std::string& path, std::uint64_t max_bytes) {
    if (impl_->is_open) {
        return Status::AlreadyOpen;
    }

    impl_->path = path;
    impl_->config.max_bytes = max_bytes;

    Status s = impl_->file.open(path, true);
    if (!is_ok(s)) {
        return s;
    }

    // Run crash recovery
    auto rec_result = recovery::RecoveryEngine::recover(impl_->file, impl_->index);
    if (!is_ok(rec_result.status)) {
        impl_->file.close();
        return rec_result.status;
    }

    impl_->next_tx_id = rec_result.committed_transactions + 1;
    impl_->write_failed = false;
    impl_->is_open = true;
    return Status::Ok;
}

Status Database::put(const std::string& key, const std::string& value) {
    if (!impl_->is_open) return Status::NotOpen;
    if (key.empty() || key.size() > MAX_KEY_SIZE || value.size() > MAX_VALUE_SIZE) {
        return Status::InvalidArgument;
    }

    impl_->lock_mgr.lock_exclusive();
    Status s = impl_->append_transaction({{Operation::Type::Put, key, value}});
    impl_->lock_mgr.unlock_exclusive();
    return s;
}

Result Database::get(const std::string& key) {
    Result res;
    if (!impl_->is_open) {
        res.status = Status::NotOpen;
        return res;
    }
    if (key.empty() || key.size() > MAX_KEY_SIZE) {
        res.status = Status::InvalidArgument;
        return res;
    }

    impl_->lock_mgr.lock_shared();
    auto entry_opt = impl_->index.get(key);
    if (entry_opt.has_value()) {
        const auto& entry = *entry_opt;
        if (entry.val_len == 0) {
            res.status = Status::Ok;
            res.value.clear();
        } else {
            res.value.resize(static_cast<std::size_t>(entry.val_len));
            Status s = impl_->file.read_at(
                entry.val_file_offset,
                reinterpret_cast<std::uint8_t*>(&res.value[0]),
                static_cast<std::size_t>(entry.val_len)
            );
            if (is_ok(s)) {
                res.status = Status::Ok;
            } else {
                res.status = s;
                res.value.clear();
            }
        }
    } else {
        res.status = Status::NotFound;
    }
    impl_->lock_mgr.unlock_shared();
    return res;
}

Status Database::remove(const std::string& key) {
    if (!impl_->is_open) return Status::NotOpen;
    if (key.empty() || key.size() > MAX_KEY_SIZE) {
        return Status::InvalidArgument;
    }

    impl_->lock_mgr.lock_exclusive();
    Status s = impl_->append_transaction({{Operation::Type::Delete, key, ""}});
    impl_->lock_mgr.unlock_exclusive();
    return s;
}

Status Database::batch(const std::vector<Operation>& operations) {
    if (!impl_->is_open) return Status::NotOpen;
    if (operations.empty() || operations.size() > MAX_BATCH_OPERATIONS) {
        return Status::InvalidArgument;
    }

    for (const auto& op : operations) {
        if (op.key.empty() || op.key.size() > MAX_KEY_SIZE) {
            return Status::InvalidArgument;
        }
        if (op.type == Operation::Type::Put && op.value.size() > MAX_VALUE_SIZE) {
            return Status::InvalidArgument;
        }
    }

    impl_->lock_mgr.lock_exclusive();
    Status s = impl_->append_transaction(operations);
    impl_->lock_mgr.unlock_exclusive();
    return s;
}

Status Database::compact() {
    if (!impl_->is_open) return Status::NotOpen;
    return concurrency::Compactor::compact(
        impl_->path,
        impl_->file,
        impl_->index,
        impl_->lock_mgr,
        impl_->config.max_bytes
    );
}

Status Database::close() {
    if (!impl_->is_open) return Status::Ok;

    impl_->lock_mgr.lock_exclusive();
    Status sync_status = impl_->file.sync();
    impl_->file.close();
    impl_->index.clear();
    impl_->is_open = false;
    impl_->lock_mgr.unlock_exclusive();

    return sync_status;
}

Status Database::dump(std::ostream& out) {
    if (!impl_->is_open) return Status::NotOpen;

    impl_->lock_mgr.lock_shared();
    auto entries = impl_->index.get_all_entries();
    // Deterministic lexicographical sort by key for deterministic dump output
    std::sort(entries.begin(), entries.end(), [](const auto& a, const auto& b) {
        return a.first < b.first;
    });

    for (const auto& [k, entry] : entries) {
        std::string val;
        if (entry.val_len > 0) {
            val.resize(static_cast<std::size_t>(entry.val_len));
            impl_->file.read_at(
                entry.val_file_offset,
                reinterpret_cast<std::uint8_t*>(&val[0]),
                static_cast<std::size_t>(entry.val_len)
            );
        }
        out << k << "=" << val << "\n";
    }
    impl_->lock_mgr.unlock_shared();
    return Status::Ok;
}

Status Database::check() {
    if (!impl_->is_open) return Status::NotOpen;
    return check_file(impl_->path);
}

Status Database::check_file(const std::string& path) {
    storage::File file;
    Status s = file.open(path, false);
    if (!is_ok(s)) {
        return s;
    }

    std::uint64_t file_len = file.size();
    if (file_len < sizeof(storage::RawFileHeader)) {
        file.close();
        return Status::Corruption;
    }

    // 1. Verify 32-byte header
    std::vector<std::uint8_t> hdr_buf(sizeof(storage::RawFileHeader));
    s = file.read_at(0, hdr_buf.data(), hdr_buf.size());
    if (!is_ok(s)) {
        file.close();
        return s;
    }

    s = storage::deserialize_file_header(hdr_buf.data(), hdr_buf.size());
    if (!is_ok(s)) {
        file.close();
        return Status::Corruption;
    }

    // 2. Sequential frame walk without mutation
    std::uint64_t cursor = sizeof(storage::RawFileHeader);
    std::vector<std::uint8_t> frame_hdr_buf(sizeof(storage::RawFrameHeader));

    while (cursor < file_len) {
        if (file_len - cursor < sizeof(storage::RawFrameHeader) + sizeof(std::uint32_t)) {
            // Torn or incomplete trailing bytes
            file.close();
            return Status::Corruption;
        }

        s = file.read_at(cursor, frame_hdr_buf.data(), sizeof(storage::RawFrameHeader));
        if (!is_ok(s)) {
            file.close();
            return s;
        }

        const auto* hdr = reinterpret_cast<const storage::RawFrameHeader*>(frame_hdr_buf.data());
        if (hdr->frame_magic != FRAME_MAGIC || hdr->format_version != FRAME_FORMAT_VERSION) {
            file.close();
            return Status::Corruption;
        }

        if (hdr->key_len > MAX_KEY_SIZE || hdr->val_len > MAX_VALUE_SIZE) {
            file.close();
            return Status::Corruption;
        }

        std::uint64_t total_frame_size = sizeof(storage::RawFrameHeader) + hdr->key_len + hdr->val_len + sizeof(std::uint32_t);
        if (cursor + total_frame_size > file_len) {
            file.close();
            return Status::Corruption;
        }

        // Validate frame CRC32
        std::vector<std::uint8_t> full_frame(total_frame_size);
        s = file.read_at(cursor, full_frame.data(), total_frame_size);
        if (!is_ok(s)) {
            file.close();
            return s;
        }

        std::uint32_t stored_crc;
        std::memcpy(&stored_crc, full_frame.data() + total_frame_size - sizeof(std::uint32_t), sizeof(std::uint32_t));
        std::uint32_t computed_crc = storage::calculate_crc32(full_frame.data(), total_frame_size - sizeof(std::uint32_t));
        if (stored_crc != computed_crc) {
            file.close();
            return Status::Corruption;
        }

        cursor += total_frame_size;
    }

    file.close();
    return (cursor == file_len) ? Status::Ok : Status::Corruption;
}

bool Database::is_open() const {
    return impl_->is_open;
}

} // namespace kvdb

