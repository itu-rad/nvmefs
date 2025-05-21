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
	// tfmeta->block_range = nullptr;

	return std::move(tfmeta);
}

TempFileMetadata *TemporaryFileMetadataManager::GetOrCreateFile(const string &filename) {

	// Lock the shared mutex for writing
	boost::unique_lock<boost::shared_mutex> alloc_lock(temp_mutex);

	// Check if the file already exists
	if (file_to_temp_meta.count(filename)) {
		return file_to_temp_meta[filename].get();
	}

	// Create a new TempFileMetadata object
	unique_ptr<TempFileMetadata> tfmeta = CreateTempFileMetadata(filename);
	tfmeta->is_active.store(true);
	printf("Temporary file %s created with block size %d and file index %d\n", filename.c_str(), tfmeta->block_size,
	       tfmeta->file_index);
	auto [entry, is_new] = file_to_temp_meta.emplace(filename, std::move(tfmeta));

	// Lock the shared range block allocation
	if (is_new) {
		// printf("Creating block range for %s\n", filename.c_str());
		// TemporaryBlock *block =
		//     block_manager->AllocateBlock((entry->second->nr_blocks * entry->second->block_size) / lba_size);
		// printf("1\n");
		// entry->second->block_range = block;
		// printf("2\n");
		// entry->second->lba_location.store(block->GetStartLBA());
	}

	return file_to_temp_meta[filename].get();
}

void TemporaryFileMetadataManager::CreateFile(const string &filename) {

	GetOrCreateFile(filename);
}

idx_t TemporaryFileMetadataManager::GetLBA(const string &filename, idx_t location, idx_t nr_lbas) {
	boost::unique_lock<boost::shared_mutex> lock(temp_mutex);

	TempFileMetadata *tfmeta = file_to_temp_meta[filename].get();
	idx_t block_index = location / tfmeta->block_size;

	if (!tfmeta->block_map.count(block_index)) {
		TemporaryBlock *block = block_manager->AllocateBlock(nr_lbas);
		tfmeta->block_map[block_index] = block;
	}
	idx_t lba = tfmeta->block_map[block_index]->GetStartLBA();
	// file_to_temp_meta[filename] = tfmeta; // From old solution

	return lba;
}

void TemporaryFileMetadataManager::MoveLBALocation(const string &filename, idx_t lba_location) {
	// boost::shared_lock<boost::shared_mutex> lock(temp_mutex);

	// if (!file_to_temp_meta.count(filename)) {
	// 	return;
	// }

	// TempFileMetadata *tfmeta = file_to_temp_meta[filename].get();
	// boost::shared_lock<boost::shared_mutex> file_lock(tfmeta->file_mutex);

	// // Use atomic compare-and-swap to update lba_location if the new location is larger
	// idx_t current_lba = tfmeta->lba_location.load();
	// do {
	// 	// Location does not need to be updated from this thread anymore
	// 	// Another thread have surpassed it
	// 	if (lba_location < current_lba) {
	// 		printf("MoveLBALocation %s, location %d\n", filename.c_str(), current_lba);
	// 		break;
	// 	}
	// } while (!tfmeta->lba_location.compare_exchange_weak(current_lba, lba_location));

	// printf("MoveLBALocation %s, location %d\n", filename.c_str(), lba_location);
}

void TemporaryFileMetadataManager::TruncateFile(const string &filename, idx_t new_size) {
	boost::unique_lock<boost::shared_mutex> lock(temp_mutex);

	TempFileMetadata *tfmeta = file_to_temp_meta[filename].get();

	idx_t to_block_index = new_size / tfmeta->block_size;
	idx_t from_block_index = tfmeta->block_map.size();

	for (idx_t i = from_block_index; i > to_block_index; i--) {
		idx_t block_index = i - 1;
		TemporaryBlock *block = tfmeta->block_map[block_index];
		block_manager->FreeBlock(block);
		tfmeta->block_map.erase(block_index);
	}

	// file_to_temp_meta[nvme_handle.path] = tfmeta;
}

void TemporaryFileMetadataManager::DeleteFile(const string &filename) {
	boost::unique_lock<boost::shared_mutex> lock(temp_mutex);

	TempFileMetadata *tfmeta = file_to_temp_meta[filename].get();
	for (const auto &kv : tfmeta->block_map) {
		block_manager->FreeBlock(kv.second);
	}
	file_to_temp_meta.erase(filename);
}

bool TemporaryFileMetadataManager::FileExists(const string &filename) {
	boost::unique_lock<boost::shared_mutex> lock(temp_mutex);

	if (file_to_temp_meta.count(filename)) {
		return true;
	}

	return false;
}

idx_t TemporaryFileMetadataManager::GetFileSizeLBA(const string &filename) {
	boost::shared_lock<boost::shared_mutex> lock(temp_mutex);
	TempFileMetadata *tfmeta = file_to_temp_meta[filename].get();
	idx_t nr_lbas = (tfmeta->block_size * tfmeta->block_map.size()) / lba_size;

	return nr_lbas;
}

void TemporaryFileMetadataManager::Clear() {
	boost::unique_lock<boost::shared_mutex> alloc_lock(temp_mutex);

	file_to_temp_meta.clear();
}

idx_t TemporaryFileMetadataManager::GetSeekBound(const string &filename) {
	boost::shared_lock<boost::shared_mutex> lock(temp_mutex);

	TempFileMetadata *tfmeta = file_to_temp_meta[filename].get();
	return tfmeta->block_size * tfmeta->block_map.size();
}

idx_t TemporaryFileMetadataManager::GetAvailableSpace(idx_t lba_count, idx_t lba_start) {
	boost::unique_lock<boost::shared_mutex> temp_lock(temp_mutex);
	idx_t temp_max_bytes = ((lba_count - 1) - lba_start) * lba_size;
	idx_t temp_used_bytes {};

	for (const auto &kv : file_to_temp_meta) {
		temp_used_bytes += kv.second->block_size * kv.second->block_map.size();
	}

	return (temp_max_bytes - temp_used_bytes);
}

void TemporaryFileMetadataManager::ListFiles(const string &directory,
                                             const std::function<void(const string &, bool)> &callback) {
	boost::unique_lock<boost::shared_mutex> lock(temp_mutex);

	for (const auto &kv : file_to_temp_meta) {
		callback(StringUtil::GetFileName(kv.first), false);
	}
}
} // namespace duckdb
