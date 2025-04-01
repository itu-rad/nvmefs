#include "nvmefs.hpp"

namespace duckdb {
	NvmeFileHandle::NvmeFileHandle(FileSystem &file_system, string path, FileOpenFlags flags) :
		FileHandle(file_system, path, flags) {

	}

	void NvmeFileHandle::Read(void *buffer, idx_t nr_bytes, idx_t location) {
		file_system.Read(*this, buffer, nr_bytes, location);
	}

	void NvmeFileHandle::Write(void *buffer, idx_t nr_bytes, idx_t location) {
		file_system.Write(*this, buffer, nr_bytes, location);
	}

	idx_t NvmeFileHandle::GetFileSize() {
		file_system.GetFileSize(*this);
	}

	void NvmeFileHandle::Sync() {
		file_system.FileSync(*this);
	}

	////////////////////////////////////////

	NvmeFileSystem::NvmeFileSystem(NvmeConfig config) : allocator(Allocator::DefaultAllocator()), device(make_uniq<NvmeDevice>(config.device_path, config.plhdls)),
		max_temp_size(config.max_temp_size), max_wal_size(config.max_wal_size)  {
		// Load metadata
	}

	NvmeFileSystem::NvmeFileSystem(NvmeConfig config, Device device) : allocator(Allocator::DefaultAllocator()), device(make_uniq<Device>(device)),
		max_temp_size(config.max_temp_size), max_wal_size(config.max_wal_size) {
		// Load metadata
	}

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener) {

	}

	bool NvmeFileSystem::TryLoadMetadata() {
		if(Metadata){
			return true;
		}

		unique_ptr<GlobalMetadata> global = ReadMetadata()
	}

	MetadataType GetMetadataType(const string &path) {
		if (StringUtil::Contains(path, ".wal")) {
			return MetadataType::WAL;
		} else if (StringUtil::Contains(path, "/tmp") {
			return MetadataType::TEMPORARY;
		} else if (StringUtil::Contains(path, ".db")) {
			return MetadataType::DATABASE;
		} else {
			throw InvalidInputException("Unknown file format");
		}
	}

	idx_t NvmeFileSystem::GetLBA(const string &filename, idx_t location) {
		idx_t lba{};
		MetadataType type = GetMetadataType(filename);
		DeviceGeometry geo = device->GetDeviceGeometry();

		idx_t lba_location = location / geo.lba_size;
$
		switch (type) {
		case MetadataType::WAL:
			if (lba_location < metadata->write_ahead_log.location) {
				lba = metadata->write_ahead_log.start + lba_location;
			} else {
				lba = metadata->write_ahead_log.location;
			}
			break;
		case MetadataType::TEMPORARY:
			{
				TemporaryFileMetadata tfmeta;
				if(file_to_temp_meta.count(filename)){
					tfmeta = file_to_temp_meta[filename]
					lba = tfmeta.start + lba_location;
				} else {
					lba = metadata->temporary.location;
					tfmeta = {.start=lba, .end=lba};
					file_to_temp_meta[filename] = tfmeta;
				}
			}
			break;
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
		idx_t lba{};
		MetadataType type = GetMetadataType(filename);

		switch (type) {
			case MetadataType::WAL:
				lba = metadata->write_ahead_log.start;
				break;
			case MetadataType::TEMPORARY:
				{
					TemporaryFileMetadata tfmeta;
					if(file_to_temp_meta.count(filename)){
						tfmeta = file_to_temp_meta[filename]
						lba = tfmeta.start;
					} else {
						lba = metadata->temporary.location;
						tfmeta = {.start=lba, .end=lba};
						file_to_temp_meta[filename] = tfmeta;
					}
				}
				break;
			case MetadataType::DATABASE:
				lba = metadata->database.start;
				break;
			default:
				throw InvalidInputException("No such metadata type");
				break;
			}
	}

	idx_t NvmeFileSystem::GetLocationLBA(const string &filename) {
		idx_t lba{};
		MetadataType type = GetMetadataType(filename);

		switch (type) {
		case MetadataType::WAL:
			lba = metadata->write_ahead_log.location;
			break;
		case MetadataType::TEMPORARY:
			{
				TemporaryFileMetadata tfmeta = file_to_temp_meta[filename];
				// Consider temp file lba 0 to 4. end = 4. proper size of tempfile is 5 lbas, so end+1
				lba = tfmeta.end + 1;
			}
			break;
		case MetadataType::DATABASE:
			lba = metadata->database.location;
			break;
		default:
			throw InvalidInputException("No such metadata type");
			break;
		}
	}

	idx_t NvmeFileSystem::GetEndLBA(const string &filename) {
		idx_t lba{};
		MetadataType type = GetMetadataType(filename);

		switch (type) {
		case MetadataType::WAL:
			lba = metadata->write_ahead_log.end;
			break;
		case MetadataType::TEMPORARY:
			{
				TemporaryFileMetadata tfmeta = file_to_temp_meta[filename];
				lba = tfmeta.end;
			}
			break;
		case MetadataType::DATABASE:
			lba = metadata->database.end;
			break;
		default:
			throw InvalidInputException("No such metadata type");
			break;
		}

		return lba;
	}
}
