#pragma once

#include "duckdb/common/map.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/string_util.hpp"
#include "device.hpp"
#include <libxnvme.h>
#include <mutex>
#include <future>
#include <chrono>

namespace duckdb {

typedef void *nvme_buf_ptr;
static constexpr idx_t XNVME_QUEUE_DEPTH = 1 << 4;
static constexpr std::chrono::milliseconds POKE_MAX_BACKOFF_TIME = std::chrono::milliseconds(200);
static constexpr idx_t DATA_PLACEMENT_MODE = 2;

struct NvmeDeviceGeometry : public DeviceGeometry {};
struct NvmeCmdContext : public CmdContext {
	string filepath;
};

class NvmeDevice : public Device {
public:
	NvmeDevice(const string &device_path, const idx_t placement_handles, const string &backend, const bool async,
	           const idx_t max_threads);
	~NvmeDevice();

	/// @brief Writes data from the input buffer to the device at the specified LBA position
	/// @param buffer The input buffer that contains data to be written
	/// @param nr_bytes The amount of bytes to write
	/// @param nr_lbas The amount of LBAs to write
	/// @param start_lab The LBA to start writing from
	/// @param offset An offset into the LBA
	/// @return The amount of LBAs written to the device
	idx_t Write(void *buffer, const CmdContext &context) override;

	/// @brief Reads data from the device at the specified LBA position into the output buffer
	/// @param buffer The output buffer that will contain data read from the device
	/// @param nr_bytes The amount of bytes to read
	/// @param nr_lbas The amount of LBAs to read
	/// @param start_lab The LBA to start reading from
	/// @param offset An offset into the LBA
	/// @return The amount of LBAs read from the device
	idx_t Read(void *buffer, const CmdContext &context) override;

	/// @brief Fetches the geometry of the device
	/// @return The device geometry
	DeviceGeometry GetDeviceGeometry() override;

	/// @brief Get the name of the device
	/// @return Name of device
	string GetName() const {
		return "NvmeDevice";
	}

private:
	/// @brief Determines which placment handler should be used for the given path
	/// @param path The path of the file that will be opened
	/// @return A placement identifier
	uint8_t GetPlacementIdentifierOrDefault(const string &path);

	/// @brief Allocates a device specific buffer. Should be freed with FreeDeviceBuffer.
	/// @param nr_bytes The number of bytes to allocate (The allocated buffer mighr be larger)
	/// @return Pointer to allocated device buffer
	nvme_buf_ptr AllocateDeviceBuffer(idx_t nr_bytes);

	/// @brief Frees the given device buffer
	/// @param buffer The device buffer to free
	void FreeDeviceBuffer(nvme_buf_ptr buffer);

	/// @brief Loads the geometry of the decvice
	/// @return The device geometry
	DeviceGeometry LoadDeviceGeometry();

	/// @brief Specifies the backend and sync/async used for the device
	/// @param opts xNVMe options
	void PrepareOpts(xnvme_opts &opts);

	static void CommandCallback(struct xnvme_cmd_ctx *ctx, void *cb_args);

	idx_t ReadAsync(void *buffer, const CmdContext &context);
	idx_t WriteAsync(void *buffer, const CmdContext &context);

	void PrepareIOCmdContext(xnvme_cmd_ctx *ctx, const CmdContext &cmd_ctx, idx_t plid_idx, idx_t dtype, bool write);
	bool CheckFDP();
	void InitializePlacementHandles();
	idx_t GetThreadIndex();

private:
	map<string, uint8_t> allocated_placement_identifiers;
	vector<uint16_t> placement_handlers;
	xnvme_dev *device;
	const string dev_path;
	const idx_t plhdls;
	DeviceGeometry geometry;
	const string backend;
	const bool async;
	bool fdp;
	vector<xnvme_queue *> queues;
	const idx_t max_threads;
	atomic<idx_t> thread_id_counter;
	static thread_local optional_idx index;
	vector<std::once_flag> init_queue_flags;
};

} // namespace duckdb
