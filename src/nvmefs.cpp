#include "include/nvmefs.hpp"

namespace duckdb {

	NvmeFileHandle::NvmeFileHandle(FileSystem &file_system, string path) : FileHandle(file_system, path) {

	}

	NvmeFileHandle::~NvmeFileHandle() = default;

	void NvmeFileHandle::Read(void *buffer, idx_t nr_bytes, idx_t location) {

	}

	void NvmeFileHandle::Write(void *buffer, idx_t nr_bytes, idx_t location) {

	}

	unique_ptr<FileHandle> NvmeFileSystem::OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener = nullptr) {

	}

	void NvmeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {

	}

	void NvmeFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {

	}

	int64_t NvmeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {

	}

	int64_t NvmeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {

	}

	unique_ptr<NvmeFileHandle> NvmeFileSystem::CreateHandle(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener = nullptr) {

	}

}
