#pragma once

#include "nvmefs_config.hpp"

namespace duckdb {
namespace gtestutils {
static bool DeallocDevice();

static const struct NvmeConfig TEST_CONFIG {
    .device_path = "/dev/ng1n1",
    .plhdls = 8,
    .max_temp_size = 1 << 30, // 1 GiB in bytes
    .max_wal_size = 1 << 25   // 32 MiB
};
} // namespace gtestutils
} // namespace duckdb
