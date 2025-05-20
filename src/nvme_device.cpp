#include "nvme_device.hpp"

namespace duckdb {
thread_local optional_idx NvmeDevice::index = optional_idx();
NvmeDevice::NvmeDevice(const string &device_path, const idx_t placement_handles, const string &backend,
                       const bool async, const idx_t max_threads)
    : dev_path(device_path), plhdls(placement_handles), backend(backend), async(async), max_threads(max_threads) {
	xnvme_opts opts = xnvme_opts_default();
	PrepareOpts(opts);
	device = xnvme_dev_open(device_path.c_str(), &opts);
	if (!device) {
		xnvme_cli_perr("xnvme_dev_open()", errno);
		throw InternalException("Unable to open device");
	}

	// Initialize the xnvme queue for asynchronous IO
	if (async) {
		queues = vector<xnvme_queue *>(max_threads, nullptr);
		// Set the callback function for completed commands. No callback arguments, hence last argument equal to NULL
	}

	fdp = CheckFDP();

	if (fdp) {
		InitializePlacementHandles();
	}

	GetThreadIndex();
	allocated_placement_identifiers["nvmefs:///tmp"] = 1;
	geometry = LoadDeviceGeometry();
}

NvmeDevice::~NvmeDevice() {
	if (async) {
		for (const auto &queue : queues) {
			xnvme_queue_term(queue);
		}
	}
	xnvme_dev_close(device);
}

idx_t NvmeDevice::Write(void *buffer, const CmdContext &context) {
	if (async) {
		return WriteAsync(buffer, context);
	}

	const NvmeCmdContext &ctx = static_cast<const NvmeCmdContext &>(context);
	D_ASSERT(ctx.nr_lbas > 0);
	// We only support offset writes within a single block
	D_ASSERT((ctx.offset == 0 && ctx.nr_lbas > 1) || (ctx.offset >= 0 && ctx.nr_lbas == 1));

	nvme_buf_ptr dev_buffer = AllocateDeviceBuffer(ctx.nr_bytes);
	if (ctx.offset > 0) {
		// Check if write is fully contained within single block
		D_ASSERT(ctx.offset + ctx.nr_bytes < geometry.lba_size);
		// Read the whole LBA block
		Read(dev_buffer, ctx);
	}
	memcpy(dev_buffer, (char *)buffer + ctx.offset, ctx.nr_bytes);

	uint32_t nsid = xnvme_dev_get_nsid(device);
	uint8_t plid_idx = GetPlacementIdentifierOrDefault(ctx.filepath);
	xnvme_cmd_ctx xnvme_ctx = xnvme_cmd_ctx_from_dev(device);

	PrepareIOCmdContext(&xnvme_ctx, context, plid_idx, DATA_PLACEMENT_MODE, true);

	int err = xnvme_nvm_write(&xnvme_ctx, nsid, ctx.start_lba, ctx.nr_lbas - 1, dev_buffer, nullptr);
	if (err) {
		xnvme_cli_perr("Could not write to device with xnvme_nvme_write(): ", err);
		throw IOException("Encountered error when writing to NVMe device");
	}

	FreeDeviceBuffer(dev_buffer);

	return ctx.nr_lbas;
}

idx_t NvmeDevice::Read(void *buffer, const CmdContext &context) {
	if (async) {
		return ReadAsync(buffer, context);
	}

	const NvmeCmdContext &ctx = static_cast<const NvmeCmdContext &>(context);
	D_ASSERT(ctx.nr_lbas > 0);
	// We only support offset reads within a single block
	D_ASSERT((ctx.offset == 0 && ctx.nr_lbas > 1) || (ctx.offset >= 0 && ctx.nr_lbas == 1));

	nvme_buf_ptr dev_buffer = AllocateDeviceBuffer(ctx.nr_bytes);

	uint32_t nsid = xnvme_dev_get_nsid(device);
	uint8_t plid_idx = GetPlacementIdentifierOrDefault(ctx.filepath);
	xnvme_cmd_ctx xnvme_ctx = xnvme_cmd_ctx_from_dev(device);

	PrepareIOCmdContext(&xnvme_ctx, context, plid_idx, 0, false);

	int err = xnvme_nvm_read(&xnvme_ctx, nsid, ctx.start_lba, ctx.nr_lbas - 1, dev_buffer, nullptr);
	if (err) {
		xnvme_cli_perr("Could not write to device with xnvme_nvme_write(): ", err);
		throw IOException("Encountered error when writing to NVMe device");
	}

	memcpy(buffer, (char *)dev_buffer + ctx.offset, ctx.nr_bytes);

	FreeDeviceBuffer(dev_buffer);

	return ctx.nr_lbas;
}

DeviceGeometry NvmeDevice::GetDeviceGeometry() {
	return geometry;
}

uint8_t NvmeDevice::GetPlacementIdentifierOrDefault(const string &path) {
	uint8_t placement_identifier = 0;
	for (const auto &kv : allocated_placement_identifiers) {
		if (StringUtil::StartsWith(path, kv.first)) {
			placement_identifier = kv.second;
		}
	}

	return placement_identifier;
}

nvme_buf_ptr NvmeDevice::AllocateDeviceBuffer(idx_t nr_bytes) {
	return xnvme_buf_alloc(device, nr_bytes);
}

void NvmeDevice::FreeDeviceBuffer(nvme_buf_ptr buffer) {
	xnvme_buf_free(device, buffer);
}

DeviceGeometry NvmeDevice::LoadDeviceGeometry() {
	NvmeDeviceGeometry geometry {};

	const xnvme_geo *geo = xnvme_dev_get_geo(device);
	const xnvme_spec_idfy_ns *nsgeo = xnvme_dev_get_ns(device);

	geometry.lba_size = geo->lba_nbytes;
	geometry.lba_count = nsgeo->nsze;

	return geometry;
}

void NvmeDevice::PrepareOpts(xnvme_opts &opts) {
	if (StringUtil::Equals(this->backend.data(), "spdk")) {
        opts.be = "spdk";
    }

	if (this->async) {
		opts.async = this->backend.data();
		if (StringUtil::Equals(this->backend.data(), "io_uring_cmd")) {
			opts.sync = "nvme";
		}
	} else {
		opts.sync = this->backend.data();
	}
}

void NvmeDevice::CommandCallback(struct xnvme_cmd_ctx *ctx, void *cb_args) {
	// Cast callback args into defined callback struct
	std::promise<void> *notifier = (std::promise<void> *)cb_args;

	// Check status
	if (xnvme_cmd_ctx_cpl_status(ctx)) {
		xnvme_cli_pinf("Command did not complete successfully");
		xnvme_cmd_ctx_pr(ctx, XNVME_PR_DEF);
	}

	// Put command context back to queue, and notify the future
	xnvme_queue_put_cmd_ctx(ctx->async.queue, ctx);
	notifier->set_value();
}

idx_t NvmeDevice::ReadAsync(void *buffer, const CmdContext &context) {

	const NvmeCmdContext &ctx = static_cast<const NvmeCmdContext &>(context);
	D_ASSERT(ctx.nr_lbas > 0);
	// We only support offset reads within a single block
	D_ASSERT((ctx.offset == 0 && ctx.nr_lbas > 1) || (ctx.offset >= 0 && ctx.nr_lbas == 1));

	nvme_buf_ptr dev_buffer = AllocateDeviceBuffer(ctx.nr_bytes);

	uint32_t nsid = xnvme_dev_get_nsid(device);
	uint8_t plid_idx = GetPlacementIdentifierOrDefault(ctx.filepath);

	idx_t thread_index = GetThreadIndex();

	xnvme_queue *queue = queues[thread_index];

	if (!queue) {
		int err = xnvme_queue_init(device, XNVME_QUEUE_DEPTH, 0, &queues[thread_index]);
		if (err) {
			xnvme_cli_perr("Unable to create an queue for asynchronous IO", err);
		}

		queue = queues[thread_index];
	}

	xnvme_cmd_ctx *xnvme_ctx = xnvme_queue_get_cmd_ctx(queue);
	PrepareIOCmdContext(xnvme_ctx, context, plid_idx, 0, false);

	std::promise<void> cb_notify;
	std::future<void> fut = cb_notify.get_future();

	xnvme_cmd_ctx_set_cb(xnvme_ctx, CommandCallback, &cb_notify);

	std::future_status status;
	std::chrono::milliseconds interval = std::chrono::milliseconds(0);

	int err = xnvme_nvm_read(xnvme_ctx, nsid, ctx.start_lba, ctx.nr_lbas - 1, dev_buffer, nullptr);
	if (err) {
		xnvme_cli_perr("Could not submit command to queue with xnvme_nvme_read(): ", err);
		throw IOException("Encountered error when writing to NVMe device");
	}

	do {
		xnvme_queue_poke(queue, 0);
		status = fut.wait_for(interval);
	} while (status != std::future_status::ready);

	memcpy(buffer, dev_buffer + ctx.offset, ctx.nr_bytes);

	FreeDeviceBuffer(dev_buffer);

	// auto end_time = std::chrono::high_resolution_clock::now();
	// auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	// // Print the duration
	// printf("ReadAsync took %d milliseconds.\n", duration.count());

	return ctx.nr_lbas;
}

idx_t NvmeDevice::WriteAsync(void *buffer, const CmdContext &context) {
	// auto start_time = std::chrono::high_resolution_clock::now();
	const NvmeCmdContext &ctx = static_cast<const NvmeCmdContext &>(context);
	D_ASSERT(ctx.nr_lbas > 0);
	// We only support offset reads within a single block
	D_ASSERT((ctx.offset == 0 && ctx.nr_lbas > 1) || (ctx.offset >= 0 && ctx.nr_lbas == 1));

	// Prepare buffer
	nvme_buf_ptr dev_buffer = AllocateDeviceBuffer(ctx.nr_bytes);
	memcpy(dev_buffer, buffer + ctx.offset, ctx.nr_bytes);

	uint32_t nsid = xnvme_dev_get_nsid(device);
	uint8_t plid_idx = GetPlacementIdentifierOrDefault(ctx.filepath);

	idx_t thread_index = GetThreadIndex();

	xnvme_queue *queue = queues[thread_index];
	if (!queue) {
		int err = xnvme_queue_init(device, XNVME_QUEUE_DEPTH, 0, &queues[thread_index]);
		if (err) {
			xnvme_cli_perr("Unable to create an queue for asynchronous IO", err);
		}

		queue = queues[thread_index];
	}

	xnvme_cmd_ctx *xnvme_ctx = xnvme_queue_get_cmd_ctx(queue);
	PrepareIOCmdContext(xnvme_ctx, context, plid_idx, DATA_PLACEMENT_MODE, true);

	std::promise<void> cb_notify;
	std::future<void> fut = cb_notify.get_future();

	xnvme_cmd_ctx_set_cb(xnvme_ctx, CommandCallback, &cb_notify);

	std::future_status status;
	std::chrono::milliseconds interval = std::chrono::milliseconds(0);

	int err = xnvme_nvm_write(xnvme_ctx, nsid, ctx.start_lba, ctx.nr_lbas - 1, dev_buffer, nullptr);
	if (err) {
		xnvme_cli_perr("Could not submit command to queue with xnvme_nvme_write(): ", err);
		throw IOException("Encountered error when writing to NVMe device");
	}

	do {
		xnvme_queue_poke(queue, 0);
		status = fut.wait_for(interval);
	} while (status != std::future_status::ready);

	FreeDeviceBuffer(dev_buffer);

	// auto end_time = std::chrono::high_resolution_clock::now();
	// auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	// // Print the duration
	// printf("WriteAsync took %d milliseconds.\n", duration.count());

	return ctx.nr_lbas;
}

void NvmeDevice::PrepareIOCmdContext(xnvme_cmd_ctx *ctx, const CmdContext &cmd_ctx, idx_t plid_idx, idx_t dtype,
                                     bool write) {
	const NvmeCmdContext &nvme_cmd_ctx = static_cast<const NvmeCmdContext &>(cmd_ctx);

	// Specified by the command set specification:
	// https://nvmexpress.org/wp-content/uploads/NVM-Express-NVM-Command-Set-Specification-Revision-1.1-2024.08.05-Ratified.pdf
	// cdw12 specifies data placement (dtype) and number of lbas to write/read (0 indexed)
	// cdw13 hold placement handle id in bit range 16-31
	uint16_t nr_lbas = nvme_cmd_ctx.nr_lbas - 1;

	ctx->cmd.common.cdw12 = nr_lbas;
	if (write && fdp) {
		ctx->cmd.common.cdw12 |= dtype << 20;

		uint16_t phid = placement_handlers[plid_idx];
		ctx->cmd.common.cdw13 = phid << 16;
	}
}

bool NvmeDevice::CheckFDP() {
	// Create admin cmd to get feature
	xnvme_cmd_ctx ctx = xnvme_cmd_ctx_from_dev(device);
	uint32_t nsid = xnvme_dev_get_nsid(device);
	uint8_t feat_id = 0x1D; // identifier of fdp
	uint8_t sel = 0x0;      // look up current value

	xnvme_prep_adm_gfeat(&ctx, nsid, feat_id, sel);
	// ctx.cmd.gfeat.cdw11 = 0x1;

	int err = xnvme_cmd_pass_admin(&ctx, NULL, 0x0, NULL, 0x0);
	if (err) {
		xnvme_cli_perr("xnvme_cmd_pass_admin()", err);
		xnvme_cmd_ctx_pr(&ctx, XNVME_PR_DEF);
	}
	// The first bit of cdw0 in the completion entry specifies if fdp is enabled
	return ctx.cpl.cdw0 & 0x1;
}

void NvmeDevice::InitializePlacementHandles() {
	uint32_t nsid = xnvme_dev_get_nsid(device);
	xnvme_cmd_ctx xnvme_ctx = xnvme_cmd_ctx_from_dev(device);

	// Retrieve number of RUHs on the device
	struct xnvme_spec_ruhs header;
	uint32_t header_bytes = sizeof(header);
	xnvme_nvm_mgmt_recv(&xnvme_ctx, nsid, XNVME_SPEC_IO_MGMT_RECV_RUHS, 0, &header, header_bytes);
	uint16_t max_placement_handles = header.nruhsd - 1;

	// Retrieve information about recliam unit handles
	struct xnvme_spec_ruhs *ruhs = nullptr;
	uint32_t ruhs_nbytes = sizeof(*ruhs) + max_placement_handles * sizeof(struct xnvme_spec_ruhs_desc);
	ruhs = (struct xnvme_spec_ruhs *)xnvme_buf_alloc(device, ruhs_nbytes);
	memset(ruhs, 0, ruhs_nbytes);
	xnvme_nvm_mgmt_recv(&xnvme_ctx, nsid, XNVME_SPEC_IO_MGMT_RECV_RUHS, 0, ruhs, ruhs_nbytes);

	for (int i = 0; i < max_placement_handles; ++i) {
		placement_handlers.emplace_back(ruhs->desc[i].pi);
	}

	xnvme_buf_free(device, ruhs);
}

idx_t NvmeDevice::GetThreadIndex() {
	if (!index.IsValid()) {
		index = thread_id_counter++ % max_threads;
	}

	return index.GetIndex();
}
} // namespace duckdb
