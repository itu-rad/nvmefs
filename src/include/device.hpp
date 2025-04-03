#pragma once

#include "duckdb.hpp"

namespace duckdb {

struct DeviceGeometry {
	idx_t lba_size;
	idx_t lba_count;
};

struct CmdContext {
	idx_t nr_bytes;
	idx_t nr_lbas;
	idx_t start_lba;
	idx_t offset;
};

class Device {
	public:
		virtual ~Device() = default;

		virtual idx_t Write(void *buffer, CmdContext &context);
		virtual idx_t Read(void *buffer, CmdContext &context);

		virtual DeviceGeometry GetDeviceGeometry();

		virtual string GetName() const = 0;
};

}
