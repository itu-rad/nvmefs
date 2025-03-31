#include "device.hpp"

namespace duckdb {
	idx_t Device::Write(void *buffer, idx_t nr_bytes, idx_t nr_lba, idx_t start_lba, idx_t offset) {
		throw NotImplementedException("%s: Write is not implemented", GetName());
	}

	idx_t Device::Read(void *buffer, idx_t nr_bytes, idx_t nr_lba, idx_t start_lba, idx_t offset) {
		throw NotImplementedException("%s: Read is not implemented", GetName());
	}

	DeviceGeometry Device::GetDeviceGeometry() {
		throw NotImplementedException("%s: GetDeviceGeometry is not implemented", GetName());
	}
}
