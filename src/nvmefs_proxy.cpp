#include "nvmefs_proxy.hpp"

#include "duckdb/main/extension_util.hpp"

#include <iostream>

namespace duckdb {

const char MAGIC_BYTES[] = "NVMEFS";

#ifdef DEBUG
void PrintMetadata(Metadata &meta, string name) {
	printf("Metadata for %s\n", name.c_str());
	std::cout << "start: " << meta.start << " end: " << meta.end << " loc: " << meta.location << std::endl;
}
#else
void PrintMetadata(Metadata &meta, string name) {
}
#endif

NvmeFileSystemProxy::NvmeFileSystemProxy()
    : fs(make_uniq<NvmeFileSystem>(*this)), allocator(Allocator::DefaultAllocator()) {

}

void NvmeFileSystemProxy::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	fs->Read(handle, buffer, nr_bytes, location);
}

void NvmeFileSystemProxy::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	fs->Write(handle, buffer, nr_bytes, location);
}

unique_ptr<FileHandle> NvmeFileSystemProxy::OpenFile(const string &path, FileOpenFlags flags,
                                                     optional_ptr<FileOpener> opener) {
	if (!metadata) {
		metadata = LoadMetadata(opener);

		PrintMetadata(metadata->database, "database");
		PrintMetadata(metadata->write_ahead_log, "write_ahead_log");
		PrintMetadata(metadata->temporary, "temporary");
	}
	return fs->OpenFile(path, flags, opener);
}

bool NvmeFileSystemProxy::CanHandleFile(const string &fpath) {
	return fs->CanHandleFile(fpath);
}

unique_ptr<GlobalMetadata> NvmeFileSystemProxy::LoadMetadata(optional_ptr<FileOpener> opener) {

	idx_t bytes_to_read = sizeof(MAGIC_BYTES) + sizeof(GlobalMetadata);
	data_ptr_t buffer = allocator.AllocateData(bytes_to_read);
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ;

	unique_ptr<FileHandle> fh = fs->OpenFile(NVME_GLOBAL_METADATA_PATH, flags, opener);

	fs->Read(*fh, buffer, bytes_to_read, NVMEFS_METADATA_LOCATION);

	// Check magic bytes
	if (memcmp(buffer, MAGIC_BYTES, sizeof(MAGIC_BYTES)) != 0) {
		allocator.FreeData(buffer, bytes_to_read);
		return InitializeMetadata(opener);
	}

	unique_ptr<GlobalMetadata> global = make_uniq<GlobalMetadata>(new GlobalMetadata);
	memcpy(&global, (buffer + sizeof(MAGIC_BYTES)), sizeof(GlobalMetadata));

	allocator.FreeData(buffer, bytes_to_read);

	return std::move(global);
}

void NvmeFileSystemProxy::WriteMetadata() {
}

uint64_t NvmeFileSystemProxy::GetLBA(MetadataType type, std::string filename) {
}

unique_ptr<GlobalMetadata> NvmeFileSystemProxy::InitializeMetadata(optional_ptr<FileOpener> opener) {
	// Create buffer
	// insert magic bytes
	// insert metadata
	// 	- We know size of db and WAL metadata and partly temp
	// 	- We do not know the size of the file mapping field in temp
	// 		- Should this be constant based on the size of directory?
	//		- Dynamic?
	// Example:
	//  1 GB temp data -> x files -> map that supports x files total (this is the size)

	idx_t bytes_to_write = sizeof(MAGIC_BYTES) + sizeof(GlobalMetadata);
	idx_t medata_location = 0;

	data_ptr_t buffer = allocator.AllocateData(bytes_to_write);

	Metadata meta_db {.start = 1, .end = 5001, .location = 1};
	Metadata meta_wal {.start = 5002, .end = 10002, .location = 5002};
	Metadata meta_temp {.start = 10003, .end = 15003, .location = 10003};

	unique_ptr<GlobalMetadata> global = make_uniq<GlobalMetadata>(new GlobalMetadata);

	global->database = meta_db;
	global->temporary = meta_temp;
	global->write_ahead_log = meta_wal;

	memcpy(buffer, MAGIC_BYTES, sizeof(MAGIC_BYTES));
	memcpy(buffer + sizeof(MAGIC_BYTES), &global, sizeof(GlobalMetadata));

	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE;

	unique_ptr<FileHandle> fh = fs->OpenFile(NVME_GLOBAL_METADATA_PATH, flags, opener);
	fs->Write(*fh, buffer, bytes_to_write, medata_location);

	allocator.FreeData(buffer, bytes_to_write);

	return std::move(global);
}

} // namespace duckdb
