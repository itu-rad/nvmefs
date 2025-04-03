#include "device.hpp"

namespace duckdb {
constexpr idx_t DEFAULT_BLOCK_SIZE = 1ULL << 12;

class FakeDevice : public Device {
public:
	FakeDevice(idx_t lba_count, idx_t lba_size = DEFAULT_BLOCK_SIZE);
	~FakeDevice();

	idx_t Write(void *buffer, const CmdContext &context) override;
	idx_t Read(void *buffer, const CmdContext &context) override;

	DeviceGeometry GetDeviceGeometry() override;

	string GetName() const override {
		return "FakeDevice";
	}

private:
	const DeviceGeometry geometry;
	vector<uint8_t> memory;
};
} // namespace duckdb
