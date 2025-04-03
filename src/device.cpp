#include "device.hpp"

namespace duckdb {
idx_t Device::Write(void *buffer, const CmdContext &context) {
	throw NotImplementedException("%s: Write is not implemented", GetName());
}

idx_t Device::Read(void *buffer, const CmdContext &context) {
	throw NotImplementedException("%s: Read is not implemented", GetName());
}

DeviceGeometry Device::GetDeviceGeometry() {
	throw NotImplementedException("%s: GetDeviceGeometry is not implemented", GetName());
}
} // namespace duckdb
