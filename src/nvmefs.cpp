#include "include/nvmefs.hpp"
#include "duckdb/common/string_util.hpp"
#include <libxnvme.h>

namespace duckdb
{

	/***************************
	 * NvmeFileHandle
	 ****************************/

	NvmeFileHandle::NvmeFileHandle(FileSystem &file_system, string path) : FileHandle(file_system, path)
	{
	}

	NvmeFileHandle::~NvmeFileHandle() = default;

	void NvmeFileHandle::Read(void *buffer, idx_t nr_bytes, idx_t location)
	{
	}

	void NvmeFileHandle::Write(void *buffer, idx_t nr_bytes, idx_t location)
	{
	}

	/***************************
	 * NvmeFileSystem
	 ****************************/

	NvmeFileSystem::NvmeFileSystem()
	{
		allocated_paths.push_back("nvme:///tmp");
		allocated_placement_identifiers["nvme:///tmp"] = 1;
	}

	unique_ptr<FileHandle> NvmeFileSystem::OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener = nullptr)
	{
		const string device_path = "/dev/ng0n1"; // TODO: Temporary device path. Should come from settings

		// TODO: Read settings from FileOpener if pressent. Else use defaults...

		// Create NvmeFileHandler
		auto xnvme_opts = xnvme_opts_default();
		xnvme_dev *device = xnvme_dev_open(device_path.c_str(), &xnvme_opts);

		// If device is not opened then we should fail... for now return null
		if (!device)
		{
			return nullptr;
		}

		// Get and add placement identifier for path
		uint8_t placement_identifier_index = GetPlacementIdentifierIndexOrDefault(path);

		unique_ptr<NvmeFileHandle> file_handler = make_uniq<NvmeFileHandle>();

		return std::move(file_handler);
	}

	/*
	 * Get placement identifier for path. If path is not allocated
	 * to a specific placement identifier index then return default placement identifier
	 */
	uint8_t NvmeFileSystem::GetPlacementIdentifierIndexOrDefault(const string &path)
	{
		int8_t placement_identifier_index = 0; // Set to default index for now
		for (auto &path_prefix : allocated_paths)
		{
			// Check if path starts with path_prefix
			if (StringUtil::StartsWith(path, path_prefix))
			{
				// Get placement identifier index
				placement_identifier_index = allocated_placement_identifiers[path_prefix];
				break;
			}
		}

		return placement_identifier_index;
	}

	void NvmeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location)
	{
	}

	void NvmeFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location)
	{
	}

	int64_t NvmeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes)
	{
	}

	int64_t NvmeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes)
	{
	}

	unique_ptr<NvmeFileHandle> NvmeFileSystem::CreateHandle(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener = nullptr)
	{
	}

	bool NvmeFileSystem::CanHandleFile(const string &path)
	{
		return StringUtil::StartsWith(path, NVMEFS_PATH_PREFIX);
	}

}
