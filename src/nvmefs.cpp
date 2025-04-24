#include "nvmefs.hpp"

namespace duckdb {
NvmeFileHandle::NvmeFileHandle(FileSystem &file_system, string path, FileOpenFlags flags)
    : FileHandle(file_system, path, flags), cursor_offset(0) {
}

void NvmeFileHandle::Read(void *buffer, idx_t nr_bytes, idx_t location) {
	file_system.Read(*this, buffer, nr_bytes, location);
}

void NvmeFileHandle::Write(void *buffer, idx_t nr_bytes, idx_t location) {
	file_system.Write(*this, buffer, nr_bytes, location);
}

idx_t NvmeFileHandle::GetFileSize() {
	return file_system.GetFileSize(*this);
}

void NvmeFileHandle::Sync() {
	file_system.FileSync(*this);
}

void NvmeFileHandle::Close() {
}

unique_ptr<CmdContext> NvmeFileHandle::PrepareWriteCommand(idx_t nr_bytes, idx_t start_lba, idx_t offset) {
	unique_ptr<NvmeCmdContext> nvme_cmd_ctx = make_uniq<NvmeCmdContext>();
	nvme_cmd_ctx->nr_bytes = nr_bytes;
	nvme_cmd_ctx->filepath = path;
	nvme_cmd_ctx->offset = offset;
	nvme_cmd_ctx->start_lba = start_lba;
	nvme_cmd_ctx->nr_lbas = CalculateRequiredLBACount(nr_bytes);

	return std::move(nvme_cmd_ctx);
}

unique_ptr<CmdContext> NvmeFileHandle::PrepareReadCommand(idx_t nr_bytes, idx_t start_lba, idx_t offset) {
	unique_ptr<NvmeCmdContext> nvme_cmd_ctx = make_uniq<NvmeCmdContext>();
	nvme_cmd_ctx->nr_bytes = nr_bytes;
	nvme_cmd_ctx->filepath = path;
	nvme_cmd_ctx->offset = offset;
	nvme_cmd_ctx->start_lba = start_lba;
	nvme_cmd_ctx->nr_lbas = CalculateRequiredLBACount(nr_bytes);

	return std::move(nvme_cmd_ctx);
}

idx_t NvmeFileHandle::CalculateRequiredLBACount(idx_t nr_bytes) {
	NvmeFileSystem &nvmefs = file_system.Cast<NvmeFileSystem>();
	DeviceGeometry geo = nvmefs.GetDevice().GetDeviceGeometry();
	idx_t lba_size = geo.lba_size;
	return (nr_bytes + lba_size - 1) / lba_size;
}

void NvmeFileHandle::SetFilePointer(idx_t location) {
	cursor_offset = location;
}

idx_t NvmeFileHandle::GetFilePointer() {
	return cursor_offset;
}

////////////////////////////////////////

std::recursive_mutex NvmeFileSystem::api_lock;

NvmeFileSystem::NvmeFileSystem(NvmeConfig config)
    : allocator(Allocator::DefaultAllocator()),
      device(make_uniq<NvmeDevice>(config.device_path, config.plhdls, config.backend, config.async)),
      max_temp_size(config.max_temp_size), max_wal_size(config.max_wal_size) {
}

NvmeFileSystem::NvmeFileSystem(NvmeConfig config, unique_ptr<Device> device)
    : allocator(Allocator::DefaultAllocator()), device(std::move(device)), max_temp_size(config.max_temp_size),
      max_wal_size(config.max_wal_size) {
}

unique_ptr<FileHandle> NvmeFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                optional_ptr<FileOpener> opener) {
	api_lock.lock();
	// std::cout << "Locking Openfile\n";
	bool internal = StringUtil::Equals(NVMEFS_GLOBAL_METADATA_PATH.data(), path.data());
	if (!internal && !TryLoadMetadata()) {
		if (GetMetadataType(path) != MetadataType::DATABASE) {
			throw IOException("No database is attached");
		} else {
			InitializeMetadata(path);
		}
	}
	unique_ptr<FileHandle> handle = make_uniq<NvmeFileHandle>(*this, path, flags);
	// std::cout << "Unlocking Openfile\n";
	api_lock.unlock();
	return std::move(handle);
}

void NvmeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	api_lock.lock();
	// std::cout << "Locking Read\n";
	NvmeFileHandle &fh = handle.Cast<NvmeFileHandle>();
	DeviceGeometry geo = device->GetDeviceGeometry();

	idx_t cursor_offset = SeekPosition(handle);
	location += cursor_offset;
	idx_t start_lba = GetLBA(handle.path, location);
	idx_t in_block_offset = location % geo.lba_size;
	unique_ptr<CmdContext> cmd_ctx = fh.PrepareReadCommand(nr_bytes, start_lba, in_block_offset);

	if (!IsLBAInRange(handle.path, start_lba, cmd_ctx->nr_lbas)) {
		throw IOException("Read out of range");
	}

	device->Read(buffer, *cmd_ctx);
	// std::cout << "Unocking Read\n";
	api_lock.unlock();
}

void NvmeFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	api_lock.lock();
	// std::cout << "Locking Write\n";
	NvmeFileHandle &fh = handle.Cast<NvmeFileHandle>();
	DeviceGeometry geo = device->GetDeviceGeometry();

	idx_t cursor_offset = SeekPosition(handle);
	location += cursor_offset;
	idx_t start_lba = GetLBA(fh.path, nr_bytes, location);
	idx_t in_block_offset = location % geo.lba_size;
	unique_ptr<CmdContext> cmd_ctx = fh.PrepareWriteCommand(nr_bytes, start_lba, in_block_offset);

	if (!IsLBAInRange(handle.path, start_lba, cmd_ctx->nr_lbas)) {
		throw IOException("Read out of range");
	}

	idx_t written_lbas = device->Write(buffer, *cmd_ctx);
	UpdateMetadata(*cmd_ctx);
	// std::cout << "Unlocking Write\n";
	api_lock.unlock();
}

int64_t NvmeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	api_lock.lock();
	Read(handle, buffer, nr_bytes, 0);
	api_lock.unlock();
	return nr_bytes;
}

int64_t NvmeFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	api_lock.lock();
	Write(handle, buffer, nr_bytes, 0);
	api_lock.unlock();
	return nr_bytes;
}

bool NvmeFileSystem::CanHandleFile(const string &fpath) {
	return StringUtil::StartsWith(fpath, NVMEFS_PATH_PREFIX);
}

bool NvmeFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	api_lock.lock();
	// std::cout << "Locking FileExists\n";
	if (!TryLoadMetadata()) {
		return false;
	}

	MetadataType type = GetMetadataType(filename);
	string path_no_ext = StringUtil::GetFileStem(filename);
	string db_path_no_ext = StringUtil::GetFileStem(metadata->db_path);

	bool exists = false;

	switch (type) {

	case WAL:
		/*
		    Need to remove the '.wal' and db ext before evaluating if the file exists.
		    Example:
		        string filename = "test.db.wal"
		        // After two calls to GetFileStem would be: "test"
		*/
		path_no_ext = StringUtil::GetFileStem(path_no_ext);
		if (StringUtil::Equals(path_no_ext.data(), db_path_no_ext.data())) {
			exists = true;
		}
		break;
	case DATABASE:
		if (StringUtil::Equals(path_no_ext.data(), db_path_no_ext.data())) {
			uint64_t start_lba = GetStartLBA(filename);
			uint64_t location_lba = GetLocationLBA(filename);

			if ((location_lba - start_lba) > 0) {
				exists = true;
			}
		} else {
			throw IOException("Not possible to have multiple databases");
		}
		break;
	case TEMPORARY:
		if (file_to_temp_meta.count(filename)) {
			exists = true;
		}
		break;
	default:
		throw IOException("No such metadata type");
		break;
	}
	// std::cout << "Unlocking FileExists\n";
	api_lock.unlock();
	return exists;
}

int64_t NvmeFileSystem::GetFileSize(FileHandle &handle) {
	api_lock.lock();
	// std::cout << "Locking GetFileSize\n";
	DeviceGeometry geo = device->GetDeviceGeometry();
	NvmeFileHandle &fh = handle.Cast<NvmeFileHandle>();
	MetadataType type = GetMetadataType(fh.path);

	idx_t nr_lbas{};
	switch (type) {
	case MetadataType::DATABASE:
		nr_lbas = metadata->database.location - metadata->database.start;
		break;
	case MetadataType::TEMPORARY: {
		TemporaryFileMetadata tfmeta = file_to_temp_meta[fh.path];
		nr_lbas = tfmeta.block_size * tfmeta.block_map.size();
		break;
	}
	case MetadataType::WAL:
		nr_lbas = metadata->write_ahead_log.location - metadata->write_ahead_log.start;
		break;
	default:
		throw InvalidInputException("Unknown metadata type!");
		break;
	}
	api_lock.unlock();
	return nr_lbas * geo.lba_size;
}

void NvmeFileSystem::FileSync(FileHandle &handle) {
	// No need for sync. All writes are directly to disk.
}

bool NvmeFileSystem::OnDiskFile(FileHandle &handle) {
	// No remote accesses to disks. We only interact with physical device, i.e. always disk "files".
	return true;
}

void NvmeFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	api_lock.lock();
	// std::cout << "Locking Truncate\n";
	NvmeFileHandle &nvme_handle = handle.Cast<NvmeFileHandle>();
	int64_t current_size = GetFileSize(nvme_handle);

	if (new_size <= current_size) {
		MetadataType type = GetMetadataType(nvme_handle.path);
		idx_t new_lba_location = nvme_handle.CalculateRequiredLBACount(new_size);

		switch (type) {
		case MetadataType::WAL:
			metadata->write_ahead_log.location = metadata->write_ahead_log.start + new_lba_location;
			break;
		case MetadataType::DATABASE:
			metadata->database.location = metadata->database.start + new_lba_location;
			break;
		case MetadataType::TEMPORARY:
			// TODO: Handle fragmentation? Truncating a file that not have been allocated last
			file_to_temp_meta[nvme_handle.path].end = file_to_temp_meta[nvme_handle.path].start + new_lba_location;
			break;
		default:
			throw InvalidInputException("Unknown metadata type");
			break;
		}
	} else {
		throw InvalidInputException("new_size is bigger than the current file size.");
	}
	// std::cout << "Unlocking Truncate\n";
	api_lock.unlock();
}

bool NvmeFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	api_lock.lock();
	// std::cout << "Locking DirectoryExists\n";
	// The directory exists if metadata exists
	if (TryLoadMetadata()) {
		api_lock.unlock();
		// std::cout << "Unlocking DirectoryExists\n";
		return true;
	}
	api_lock.unlock();
	// std::cout << "Unlocking DirectoryExists\n";
	return false;
}

void NvmeFileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	api_lock.lock();
	// std::cout << "Locking RemoveDirectory\n";
	// We only support removal of temporary directory
	MetadataType type = GetMetadataType(directory);
	if (type == MetadataType::TEMPORARY) {
		file_to_temp_meta.clear();
	} else {
		throw IOException("Cannot delete unknown directory");
	}
	// std::cout << "Unlocking RemoveDirectory\n";
	api_lock.unlock();
}

void NvmeFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	// All necessary directories (i.e. tmp and main folder) is already created
	// if metadata is present
	api_lock.lock();
	// std::cout << "Locking CreateDirectory\n";
	if (!TryLoadMetadata()) {
		throw IOException("No directories can exist when there is no metadata");
	}
	// std::cout << "Unlocking CreateDirectory\n";
	api_lock.unlock();
}

void NvmeFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	api_lock.lock();
	MetadataType type = GetMetadataType(filename);

	switch (type) {
	case WAL:
		// Reset the location poitner (next lba to write to) to the start effectively removing the wal
		metadata->write_ahead_log.location = metadata->write_ahead_log.start;
		break;

	case TEMPORARY: {
		TemporaryFileMetadata tfmeta = file_to_temp_meta[filename];
		for (const auto& kv : tfmeta.block_map) {
			temp_block_manager.FreeBlock(kv.second);
		}
		file_to_temp_meta.erase(filename);
		} break;
	default:
		// No other files to delete - we only have the database file, temporary files and the write_ahead_log
		break;
	}
	api_lock.unlock();
}

void NvmeFileSystem::Seek(FileHandle &handle, idx_t location) {
	api_lock.lock();
	// std::cout << "Locking Seek\n";
	NvmeFileHandle &nvme_handle = handle.Cast<NvmeFileHandle>();
	DeviceGeometry geo = device->GetDeviceGeometry();
	// We only support seek to start of an LBA block
	D_ASSERT(location % geo.lba_size == 0);

	idx_t start_bound = GetStartLBA(nvme_handle.path);
	idx_t end_bound = GetEndLBA(nvme_handle.path);
	idx_t max_seek_bound = (end_bound - start_bound) * geo.lba_size;

	if (location >= max_seek_bound) {
		throw IOException("Seek location is out of bounds");
	}

	nvme_handle.SetFilePointer(location);
	// std::cout << "Unlocking Seek\n";
	api_lock.unlock();
}

idx_t NvmeFileSystem::SeekPosition(FileHandle &handle) {
	return handle.Cast<NvmeFileHandle>().GetFilePointer();
}

Device &NvmeFileSystem::GetDevice() {
	return *device;
}

bool NvmeFileSystem::TryLoadMetadata() {
	if (metadata) {
		return true;
	}

	unique_ptr<GlobalMetadata> global = ReadMetadata();
	if (global) {
		metadata = std::move(global);
		return true;
	}

	return false;
}

void NvmeFileSystem::InitializeMetadata(const string &filename) {
	// We only support database paths/names up to 100 characters (this includes NVMEFS_PATH_PREFIX)
	if (filename.length() > 100) {
		throw IOException("Database name is too long.");
	}

	DeviceGeometry geo = device->GetDeviceGeometry();

	idx_t temp_start = (geo.lba_count - 1) - (max_temp_size / geo.lba_size);
	idx_t wal_lba_count = max_wal_size / geo.lba_size;
	idx_t wal_start = (temp_start - 1) - wal_lba_count;

	Metadata meta_temp = {.start = temp_start, .end = geo.lba_count - 1, .location = temp_start};
	Metadata meta_wal = {.start = wal_start, .end = temp_start - 1, .location = wal_start};
	// 1 is the first LBA because 0 is used for device metadata (global metadata)
	Metadata meta_db = {.start = 1, .end = wal_start - 1, .location = 1};

	unique_ptr<GlobalMetadata> global = make_uniq<GlobalMetadata>(GlobalMetadata {});

	global->database = meta_db;
	global->write_ahead_log = meta_wal;
	global->temporary = meta_temp;
	global->db_path_size = filename.length();

	strncpy(global->db_path, filename.data(), filename.length());
	global->db_path[100] = '\0';

	temp_block_manager = NvmeTemporaryBlockManager(metadata->temporary.start, metadata->temporary.end);

	WriteMetadata(*global);

	metadata = std::move(global);
}

unique_ptr<GlobalMetadata> NvmeFileSystem::ReadMetadata() {
	idx_t nr_bytes_magic = sizeof(NVMEFS_MAGIC_BYTES);
	idx_t nr_bytes_global = sizeof(GlobalMetadata);
	idx_t bytes_to_read = nr_bytes_magic + nr_bytes_global;

	data_ptr_t buffer = allocator.AllocateData(bytes_to_read);
	unique_ptr<GlobalMetadata> global = nullptr;

	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ;
	unique_ptr<FileHandle> fh = OpenFile(NVMEFS_GLOBAL_METADATA_PATH, flags);
	unique_ptr<CmdContext> cmd_ctx =
	    fh->Cast<NvmeFileHandle>().PrepareReadCommand(bytes_to_read, NVMEFS_GLOBAL_METADATA_LOCATION, 0);

	device->Read(buffer, *cmd_ctx);

	if (memcmp(buffer, NVMEFS_MAGIC_BYTES, nr_bytes_magic) == 0) {
		global = make_uniq<GlobalMetadata>(GlobalMetadata {});
		memcpy(global.get(), buffer + nr_bytes_magic, nr_bytes_global);
		temp_block_manager = NvmeTemporaryBlockManager(global->temporary.start, global->temporary.end);
	}

	allocator.FreeData(buffer, bytes_to_read);

	return std::move(global);
}

void NvmeFileSystem::WriteMetadata(GlobalMetadata &global) {
	idx_t nr_bytes_magic = sizeof(NVMEFS_MAGIC_BYTES);
	idx_t nr_bytes_global = sizeof(GlobalMetadata);
	idx_t bytes_to_write = nr_bytes_magic + nr_bytes_global;

	data_ptr_t buffer = allocator.AllocateData(bytes_to_write);
	memcpy(buffer, NVMEFS_MAGIC_BYTES, nr_bytes_magic);
	memcpy(buffer + nr_bytes_magic, &global, nr_bytes_global);

	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = OpenFile(NVMEFS_GLOBAL_METADATA_PATH, flags);
	unique_ptr<CmdContext> cmd_ctx =
	    fh->Cast<NvmeFileHandle>().PrepareWriteCommand(bytes_to_write, NVMEFS_GLOBAL_METADATA_LOCATION, 0);

	device->Write(buffer, *cmd_ctx);

	allocator.FreeData(buffer, bytes_to_write);
}

void NvmeFileSystem::UpdateMetadata(CmdContext &context) {
	NvmeCmdContext &ctx = static_cast<NvmeCmdContext &>(context);
	MetadataType type = GetMetadataType(ctx.filepath);
	bool write = false;

	switch (type) {
	case MetadataType::WAL:
		if (ctx.start_lba >= metadata->write_ahead_log.location) {
			metadata->write_ahead_log.location = ctx.start_lba + ctx.nr_lbas;
			write = true;
		}
		break;
	case MetadataType::TEMPORARY:
		// The temporary metadata remain static given that location is unused.
		// The file_to_temp_meta map will be updated during GetLBA, hence
		// no action is required here.
		break;
	case MetadataType::DATABASE:
		if (ctx.start_lba >= metadata->database.location) {
			metadata->database.location = ctx.start_lba + ctx.nr_lbas;
			write = true;
		}
		break;
	default:
		throw InvalidInputException("no such metadatatype");
	}

	if (write) {
		WriteMetadata(*metadata);
	}
}

MetadataType NvmeFileSystem::GetMetadataType(const string &filename) {
	if (StringUtil::Contains(filename, ".wal")) {
		return MetadataType::WAL;
	} else if (StringUtil::Contains(filename, "/tmp")) {
		return MetadataType::TEMPORARY;
	} else if (StringUtil::Contains(filename, ".db")) {
		return MetadataType::DATABASE;
	} else {
		throw InvalidInputException("Unknown file format");
	}
}

idx_t NvmeFileSystem::GetLBA(const string &filename, idx_t nr_bytes, idx_t location) {
	idx_t lba {};
	MetadataType type = GetMetadataType(filename);
	DeviceGeometry geo = device->GetDeviceGeometry();

	idx_t lba_location = location / geo.lba_size;

	switch (type) {
	case MetadataType::WAL:
		if (lba_location < metadata->write_ahead_log.location) {
			lba = metadata->write_ahead_log.start + lba_location;
		} else {
			lba = metadata->write_ahead_log.location;
		}
		break;
	case MetadataType::TEMPORARY: {
		TemporaryFileMetadata tfmeta;
		idx_t block_index = location / nr_bytes;
		idx_t nr_lbas = nr_bytes / geo.lba_size;

		if (file_to_temp_meta.count(filename)) {
			tfmeta = file_to_temp_meta[filename];
			if (!tfmeta.block_map.count(block_index)) {
				TemporaryBlock *block = temp_block_manager.AllocateBlock(nr_lbas);
				tfmeta.block_map[block_index] = block;
			}
			lba = tfmeta.block_map[block_index]->GetStartLBA();

		} else {
			tfmeta = {.block_size = nr_bytes};
			file_to_temp_meta[filename] = tfmeta;

			TemporaryBlock *block = temp_block_manager.AllocateBlock(nr_lbas);
			file_to_temp_meta[filename].block_map[block_index] = block;
			lba = block->GetStartLBA();
		}
	} break;
	case MetadataType::DATABASE:
		lba = metadata->database.start + lba_location;
		break;
	default:
		throw InvalidInputException("No such metadata type");
		break;
	}

	return lba;
}

idx_t NvmeFileSystem::GetStartLBA(const string &filename) {
	idx_t lba {};
	MetadataType type = GetMetadataType(filename);

	switch (type) {
	case MetadataType::WAL:
		lba = metadata->write_ahead_log.start;
		break;
	case MetadataType::TEMPORARY: {
		TemporaryFileMetadata tfmeta;
		if (file_to_temp_meta.count(filename)) {
			tfmeta = file_to_temp_meta[filename];
			lba = tfmeta.start;
		} else {
			lba = metadata->temporary.location;
			tfmeta = {.start = lba, .end = lba};
			file_to_temp_meta[filename] = tfmeta;
		}
	} break;
	case MetadataType::DATABASE:
		lba = metadata->database.start;
		break;
	default:
		throw InvalidInputException("No such metadata type");
		break;
	}

	return lba;
}

idx_t NvmeFileSystem::GetLocationLBA(const string &filename) {
	idx_t lba {};
	MetadataType type = GetMetadataType(filename);

	switch (type) {
	case MetadataType::WAL:
		lba = metadata->write_ahead_log.location;
		break;
	case MetadataType::TEMPORARY: {
		TemporaryFileMetadata tfmeta = file_to_temp_meta[filename];
		lba = tfmeta.end;
	} break;
	case MetadataType::DATABASE:
		lba = metadata->database.location;
		break;
	default:
		throw InvalidInputException("No such metadata type");
		break;
	}

	return lba;
}

idx_t NvmeFileSystem::GetEndLBA(const string &filename) {
	idx_t lba {};
	MetadataType type = GetMetadataType(filename);

	switch (type) {
	case MetadataType::WAL:
		lba = metadata->write_ahead_log.end;
		break;
	case MetadataType::TEMPORARY: {
		TemporaryFileMetadata tfmeta = file_to_temp_meta[filename];
		lba = tfmeta.end;
	} break;
	case MetadataType::DATABASE:
		lba = metadata->database.end;
		break;
	default:
		throw InvalidInputException("No such metadata type");
		break;
	}

	return lba;
}

bool NvmeFileSystem::IsLBAInRange(const string &filename, idx_t start_lba, idx_t lba_count) {

	MetadataType type = GetMetadataType(filename);
	Metadata current_metadata;

	switch (type) {
	case MetadataType::WAL:
		current_metadata = metadata->write_ahead_log;
		break;
	case MetadataType::TEMPORARY:
		current_metadata = metadata->temporary;
		break;
	case MetadataType::DATABASE:
		current_metadata = metadata->database;
		break;
	default:
		throw InvalidInputException("No such metadata type");
		break;
	}

	// Check if the LBA start location is within the range of the metadata range
	if ((start_lba < current_metadata.start || start_lba > current_metadata.end)) {
		return false;
	}

	// Check that if the lba is in range that we are not going to read or write out of range
	if ((start_lba + lba_count) > current_metadata.end) {
		return false;
	}

	return true;
}
} // namespace duckdb
