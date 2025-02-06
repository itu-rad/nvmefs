#pragma once

#include "duckdb/common/file_system.hpp"

namespace duckdb {

	class NvmeFileHandle : public FileHandle {
	public:
		NvmeFileHandle(FileSystem &file_system, string &path);
		~NvmeFileHandle() override;

		void Read(void *buffer, idx_t nr_bytes, idx_t location);
		void Write(void *buffer, idx_t nr_bytes, idx_t location);
		int64_t Read(void *buffer, idx_t nr_bytes);
		int64_t Write(void *buffer, idx_t nr_bytes);

		void Close(){}

	protected:
		uint64_t placement_identifier;
	};

	class NvmeFileSystem : public FileSystem {
	public:
		unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener = nullptr) override;
		void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
		void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
		int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
		int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	protected:
		unique_ptr<NvmeFileHandle> CreateHandle(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener = nullptr);
	};

}
