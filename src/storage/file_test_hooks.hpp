#pragma once

#include <cstddef>

namespace kvdb::storage::testing {

enum class FileFailurePoint {
    None,
    Append,
    Sync
};

void fail_next_file_operation(FileFailurePoint point, std::size_t bytes_before_failure = 0);
void clear_file_failure();

} // namespace kvdb::storage::testing