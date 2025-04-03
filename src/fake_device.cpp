#include "fake_device.hpp"

namespace duckdb {
FakeDevice::FakeDevice(idx_t lba_count, idx_t lba_size) : Device(), geometry(DeviceGeometry {lba_count, lba_size}) {
	vector<uint8_t> data(lba_count * lba_size);
}

FakeDevice::~FakeDevice() {
}

idx_t FakeDevice::Write(void *buffer, idx_t nr_bytes, idx_t nr_lba, idx_t start_lba, idx_t offset) {
	return nr_lba;
}

idx_t FakeDevice::Read(void *buffer, idx_t nr_bytes, idx_t nr_lba, idx_t start_lba, idx_t offset) {
	return nr_lba;
}

DeviceGeometry FakeDevice::GetDeviceGeometry() {
	return geometry;
}
} // namespace duckdb
