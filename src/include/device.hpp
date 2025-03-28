#pragma once

#include "duckdb/common/map.hpp"
#include "duckdb/common/string_util.hpp"
#include <libxnvme.h>

namespace duckdb {

struct NvmeDeviceGeometry {
	idx_t lba_size;
	idx_t lba_count;
};

class NvmeDevice {
	public:
		NvmeDevice(const string& device_path,  const idx_t placement_handles);
		~NvmeDevice() = default;

		/// @brief Writes data from the input buffer to the device at the specified LBA position
		/// @param buffer The input buffer that contains data to be written
		/// @param nr_bytes The amount of bytes to write
		/// @param start_lab The LBA to start writing from
		/// @param offset An offset into the LBA
		/// @return The amount of LBAs written to the device
		idx_t Write(void *buffer, idx_t nr_bytes, idx_t start_lba, idx_t offset);

		/// @brief Reads data from the device at the specified LBA position into the output buffer
		/// @param buffer The output buffer that will contain data read from the device
		/// @param nr_bytes The amount of bytes to read
		/// @param start_lab The LBA to start reading from
		/// @param offset An offset into the LBA
		/// @return The amount of LBAs read from the device
		idx_t Read(void *buffer, idx_t nr_bytes, idx_t start_lba, idx_t offset);

		/// @brief Fetches the geometry of the device
		/// @return Device geometry
		NvmeDeviceGeometry GetDeviceGeometry();



	private:
		map<string, uint8_t> allocated_placement_identifiers;
		const string dev_path;
		const idx_t plhdls;
		const NvmeDeviceGeometry geometry;

};

}
