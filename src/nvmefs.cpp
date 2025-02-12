#include "include/nvmefs.hpp"
#include "duckdb/common/string_util.hpp"
#include <libxnvme.h>

namespace duckdb
{

	/***************************
	 * NvmeFileHandle
	 ****************************/

	NvmeFileHandle::NvmeFileHandle(FileSystem &file_system, string path, uint8_t plid_idx, xnvme_dev *device)
		: FileHandle(file_system, path)
	{
		// Get placemenet handle indentifier and create placement idenetifier
		// Inspiration: https://github.com/xnvme/xnvme/blob/be52a634c139647b14940ba8a3ff254d6b1ca8c4/tools/xnvme.c#L833
		this->device = device;

		struct xnvme_cmd_ctx ctx = xnvme_cmd_ctx_from_dev(device);
		uint32_t nsid = xnvme_dev_get_nsid(device);

		struct xnvme_spec_ruhs *ruhs = nullptr;
		uint32_t ruhs_nbytes = sizeof(*ruhs) + FDP_PLID_COUNT + sizeof(struct xnvme_spec_ruhs_desc);

		ruhs = (struct xnvme_spec_ruhs *)xnvme_buf_alloc(device, ruhs_nbytes);
		memset(ruhs, 0, ruhs_nbytes);

		xnvme_nvm_mgmt_recv(&ctx, nsid, XNVME_SPEC_IO_MGMT_RECV_RUHS, 0, ruhs, ruhs_nbytes);

		uint16_t phid = ruhs->desc[plid_idx].pi;

		this->placement_identifier = phid << 16;

		xnvme_buf_free(device, ruhs);
	}

	NvmeFileHandle::~NvmeFileHandle() = default;

	void NvmeFileHandle::Read(void *buffer, idx_t nr_bytes, idx_t location)
	{
	}

	void NvmeFileHandle::Write(void *buffer, idx_t nr_bytes, idx_t location)
	{
	}

	unique_ptr<xnvme_cmd_ctx> NvmeFileHandle::PrepareWriteCommand()
	{
		auto ctx = duckdb::make_uniq<xnvme_cmd_ctx>(xnvme_cmd_ctx_from_dev(device));

		ctx->cmd.common.cdw13 = placement_identifier;

		return std::move(ctx);
	}

	unique_ptr<xnvme_cmd_ctx> NvmeFileHandle::PrepareReadCommand()
	{
		auto ctx = duckdb::make_uniq<xnvme_cmd_ctx>(xnvme_cmd_ctx_from_dev(device));

		ctx->cmd.common.cdw13 = placement_identifier;

		return std::move(ctx);
	}

	/***************************
	 * NvmeFileSystem
	 ****************************/

	NvmeFileSystem::NvmeFileSystem()
	{
		allocated_paths.push_back("xnvme:///tmp");
		allocated_placement_identifiers["xnvme:///tmp"] = 1;
	}

	unique_ptr<FileHandle> NvmeFileSystem::OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener)
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

		unique_ptr<NvmeFileHandle> file_handler = make_uniq<NvmeFileHandle>(*this, path, placement_identifier_index, device);

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
		unique_ptr<xnvme_cmd_ctx> ctx = handle.Cast<NvmeFileHandle>().PrepareWriteCommand();

		int namespace_id = 0;	  // TODO: Get namespace id from device
		uint64_t lba_size = 2048; // TODO: Get lba size from device

		uint64_t number_of_lbas = nr_bytes / lba_size;

		int err = xnvme_nvm_write(ctx.get(), namespace_id, location, number_of_lbas, buffer, nullptr);
		if (err)
		{
			// TODO: Handle error
		}
	}

	void NvmeFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location)
	{
		unique_ptr<xnvme_cmd_ctx> ctx = handle.Cast<NvmeFileHandle>().PrepareWriteCommand();

		int namespace_id = 0;	  // TODO: Get namespace id from device
		uint64_t lba_size = 2048; // TODO: Get lba size from device

		uint64_t number_of_lbas = nr_bytes / lba_size;

		int err = xnvme_nvm_write(ctx.get(), namespace_id, location, number_of_lbas, buffer, nullptr);
		if (err)
		{
			// TODO: Handle error
		}
	}

	int64_t NvmeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes)
	{
		throw IOException("Not implemented");
		return 0;
	}

	int64_t NvmeFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes)
	{
		throw IOException("Not implemented");
		return 0;
	}

	bool NvmeFileSystem::CanHandleFile(const string &path)
	{
		return StringUtil::StartsWith(path, NVMEFS_PATH_PREFIX);
	}

}
