#include "temporary_file_metadata_manager.hpp"

namespace duckdb {

inline idx_t GetBufferSize(const string buffer_size_string) {

	idx_t buffer_size = 262144;
	if (buffer_size_string == "S32K")
		buffer_size = 32768;
	else if (buffer_size_string == "S64K")
		buffer_size = 65536;
	else if (buffer_size_string == "S96K")
		buffer_size = 98304;
	else if (buffer_size_string == "S128K")
		buffer_size = 131072;
	else if (buffer_size_string == "S160K")
		buffer_size = 163840;
	else if (buffer_size_string == "S192K")
		buffer_size = 196608;
	else if (buffer_size_string == "S224K")
		buffer_size = 229376;

	return buffer_size;
}

inline unique_ptr<TempFileMetadata> CreateTempFileMetadata(const string &filename) {

	unique_ptr<TempFileMetadata> tfmeta = make_uniq<TempFileMetadata>();
	tfmeta->is_active.store(true);

	// Find the position of the first number
	size_t first_number_start = filename.find_last_of('_') + 1;       // Start after the last '_'
	size_t first_number_end = filename.find('-', first_number_start); // Find the '-' after the first number

	// Extract the first number
	std::string block_size_str = filename.substr(first_number_start, first_number_end - first_number_start);
	idx_t block_size = GetBufferSize(block_size_str);

	// Find the position of the second number
	size_t file_index_start = first_number_end + 1;               // Start after the '-'
	size_t file_index_end = filename.find('.', file_index_start); // Find the '.' after the second number

	// Extract the second number
	std::string file_index_str = filename.substr(file_index_start, file_index_end - file_index_start);
	int file_index = std::stoi(file_index_str);

	tfmeta->block_size = block_size;
	tfmeta->file_index = file_index;
	tfmeta->nr_blocks = (1 << file_index) * 4000;
	tfmeta->lba_location.store(0);
	tfmeta->block_range = nullptr;

	return std::move(tfmeta);
}

TempFileMetadata *TemporaryFileMetadataManager::GetOrCreateFile(const string &filename) {
	{
		// Lock the shared mutex for reading
		boost::shared_lock<boost::shared_mutex> lock(temp_mutex);

		// Check if the file already exists
		if (file_to_temp_meta.count(filename)) {
			return file_to_temp_meta[filename].get();
		}
	}

	// Lock the shared mutex for writing
	boost::unique_lock<boost::shared_mutex> alloc_lock(temp_mutex);

	// Create a new TempFileMetadata object
	unique_ptr<TempFileMetadata> tfmeta = CreateTempFileMetadata(filename);
	tfmeta->is_active.store(true);
	auto [entry, is_new] = file_to_temp_meta.emplace(filename, std::move(tfmeta));

	printf("Temporary file %s created with block size %d and file index %d\n", filename.c_str());

	// Lock the shared range block allocation
	if (is_new) {
		printf("Creating block range for %s\n", filename.c_str());
		TemporaryBlock *block =
		    block_manager->AllocateBlock((entry->second->nr_blocks * entry->second->block_size) / lba_size);
		printf("1\n");
		entry->second->block_range = block;
		printf("2\n");
		entry->second->lba_location.store(block->GetStartLBA());
	}

	return file_to_temp_meta[filename].get();
}

void TemporaryFileMetadataManager::CreateFile(const string &filename) {

	GetOrCreateFile(filename);
}

idx_t TemporaryFileMetadataManager::GetLBA(const string &filename, idx_t lba_location) {
	boost::shared_lock<boost::shared_mutex> lock(temp_mutex);

	if (!file_to_temp_meta.count(filename)) {
		throw InternalException("Temporary file %s not found", filename);
	}

	TempFileMetadata *tfmeta = file_to_temp_meta[filename].get();

	boost::shared_lock<boost::shared_mutex> file_lock(tfmeta->file_mutex);

	return tfmeta->block_range->GetStartLBA() + tfmeta->lba_location.load();
}

void TemporaryFileMetadataManager::MoveLBALocation(const string &filename, idx_t lba_location) {
	boost::shared_lock<boost::shared_mutex> lock(temp_mutex);

	if (!file_to_temp_meta.count(filename)) {
		return;
	}

	TempFileMetadata *tfmeta = file_to_temp_meta[filename].get();
	boost::shared_lock<boost::shared_mutex> file_lock(tfmeta->file_mutex);

	// Use atomic compare-and-swap to update lba_location if the new location is larger
	idx_t current_lba = tfmeta->lba_location.load();
	do {
		// Location does not need to be updated from this thread anymore
		// Another thread have surpassed it
		if (lba_location < current_lba) {
			break;
		}
	} while (!tfmeta->lba_location.compare_exchange_weak(current_lba, lba_location));
}

void TemporaryFileMetadataManager::TruncateFile(const string &filename, idx_t new_size) {
	boost::shared_lock<boost::shared_mutex> lock(temp_mutex);

	if (!file_to_temp_meta.count(filename)) {
		return;
	}

	TempFileMetadata *tfmeta = file_to_temp_meta[filename].get();

	boost::unique_lock<boost::shared_mutex> file_lock(tfmeta->file_mutex);

	idx_t new_lba_location = tfmeta->block_range->GetStartLBA() + (new_size / lba_size);
	tfmeta->lba_location.store(new_lba_location);
}

void TemporaryFileMetadataManager::DeleteFile(const string &filename) {
	boost::shared_lock<boost::shared_mutex> lock(temp_mutex);

	if (!file_to_temp_meta.count(filename)) {
		return;
	}

	TempFileMetadata *tfmeta = file_to_temp_meta[filename].get();
	boost::unique_lock<boost::shared_mutex> file_lock(tfmeta->file_mutex);

	// If file exists, soft delete it
	if (tfmeta) {
		tfmeta->is_active.store(false);
		tfmeta->lba_location.store(tfmeta->block_range->GetStartLBA());
	}
}

bool TemporaryFileMetadataManager::FileExists(const string &filename) {
	boost::shared_lock<boost::shared_mutex> lock(temp_mutex);

	if (file_to_temp_meta.count(filename)) {
		return file_to_temp_meta[filename]->is_active.load();
	}

	return false;
}

idx_t TemporaryFileMetadataManager::GetFileSizeLBA(const string &filename) {
	boost::shared_lock<boost::shared_mutex> lock(temp_mutex);
	if (!file_to_temp_meta.count(filename)) {
		return 0;
	}

	TempFileMetadata *tfmeta = file_to_temp_meta[filename].get();

	boost::shared_lock<boost::shared_mutex> file_lock(tfmeta->file_mutex);

	return tfmeta->lba_location.load() - tfmeta->block_range->GetStartLBA();
}

void TemporaryFileMetadataManager::Clear() {
	boost::unique_lock<boost::shared_mutex> alloc_lock(temp_mutex);
	for (auto &kv : file_to_temp_meta) {
		block_manager->FreeBlock(kv.second->block_range);
	}

	file_to_temp_meta.clear();
} // namespace duckdb

idx_t TemporaryFileMetadataManager::GetAvailableSpace() {
	boost::shared_lock<boost::shared_mutex> lock(temp_mutex);

	idx_t available_space = lba_amount * lba_size;
	for (auto &kv : file_to_temp_meta) {
		boost::shared_lock<boost::shared_mutex> file_lock(kv.second->file_mutex);

		if (kv.second->is_active.load()) {
			idx_t size = (kv.second->lba_location.load() - kv.second->block_range->GetStartLBA()) * lba_size;
			available_space -= size;
		}
	}

	return available_space;
}

void TemporaryFileMetadataManager::ListFiles(const string &directory,
                                             const std::function<void(const string &, bool)> &callback) {
	boost::shared_lock<boost::shared_mutex> lock(temp_mutex);

	for (const auto &kv : file_to_temp_meta) {
		callback(StringUtil::GetFileName(kv.first), false);
	}
}
} // namespace duckdb
