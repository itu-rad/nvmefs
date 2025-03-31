#pragma once

#include "duckdb.hpp"

namespace duckdb {

struct DeviceGeometry {
	idx_t lba_size;
	idx_t lba_count;
};

class Device {
	public:
		virtual ~Device() = default;

		virtual idx_t Write(void *buffer, idx_t nr_bytes, idx_t nr_lba, idx_t start_lba, idx_t offset);
		virtual idx_t Read(void *buffer, idx_t nr_bytes, idx_t nr_lba, idx_t start_lba, idx_t offset);

		virtual DeviceGeometry GetDeviceGeometry();

		virtual string GetName() const = 0;
};

}
