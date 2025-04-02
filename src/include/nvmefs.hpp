#pragma once

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/map.hpp"

#include "device.hpp"
#include "nvmefs_config.hpp"

namespace duckdb {

constexpr idx_t NVMEFS_GLOBAL_METADATA_LOCATION = 0;

enum MetadataType { DATABASE, WAL, TEMPORARY };

struct Metadata {
	uint64_t start;
	uint64_t end;
	uint64_t location;
};

struct GlobalMetadata {
	uint64_t db_path_size;
	char db_path[101];

	Metadata database;
	Metadata write_ahead_log;
	Metadata temporary;
};

struct TemporaryFileMetadata {
	uint64_t start;
	uint64_t end;
};

class NvmeFileHandle : public FileHandle {
public:
	NvmeFileHandle(FileSystem &file_system, string path, FileOpenFlags flags);
	~NvmeFileHandle() override;

	void Read(void *buffer, idx_t nr_bytes, idx_t location);
	void Write(void *buffer, idx_t nr_bytes, idx_t location);

	idx_t GetFileSize();
	void Sync();

private:
	unique_ptr<CmdContext> PrepareWriteCommand(int64_t nr_bytes);
	unique_ptr<CmdContext> PrepareReadCommand(int64_t nr_bytes);

	void SetFilePointer(idx_t location);
	idx_t GetFilePointer();

private:
	idx_t cursor_offset;
};

class NvmeFileSystem : public FileSystem {
public:
	NvmeFileSystem(NvmeConfig config);
	NvmeFileSystem(NvmeConfig config, Device device);
	~NvmeFileSystem() = default;

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
		optional_ptr<FileOpener> opener = nullptr) override;
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes);
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes);
	bool CanHandleFile(const string &fpath) override;
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	int64_t GetFileSize(FileHandle &handle) override;
	void FileSync(FileHandle &handle) override;
	bool OnDiskFile(FileHandle &handle) override;
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	void Seek(FileHandle &handle, idx_t location) override;
	idx_t SeekPosition(FileHandle &handle) override;

	string GetName() const {
		return "NvmeFileSystem";
	}

private:
	bool TryLoadMetadata();
	void InitializeMetadata(FileHandle &handle, string path);
	unique_ptr<GlobalMetadata> ReadMetadata(FileHandle &handle);
	void WriteMetadata(FileHandle &handle, GlobalMetadata *global);
	void UpdateMetadata(FileHandle &handle, uint64_t location, uint64_t nr_lbas, MetadataType type);
	MetadataType GetMetadataType(const string &path);
	idx_t GetLBA(const string &filename, idx_t location);
	idx_t GetStartLBA(const string &filename);
	idx_t GetLocationLBA(const string &filename);
	idx_t GetEndLBA(const string &filename);

private:
	Allocator &allocator;
	unique_ptr<GlobalMetadata> metadata;
	unique_ptr<Device> device;
	map<string, TemporaryFileMetadata> file_to_temp_meta;
	idx_t max_temp_size;
	idx_t max_wal_size;
};
}
