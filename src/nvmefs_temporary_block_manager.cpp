#include "nvmefs_temporary_block_manager.hpp"
#include <optional>

namespace duckdb {
TemporaryBlock::TemporaryBlock(idx_t start_lba, idx_t lba_amount)
    : start_lba(start_lba), lba_amount(lba_amount), is_free(true) {
}

idx_t TemporaryBlock::GetSizeInBytes() {
	return lba_amount * 4096; // TODO: Get the LBA size from the device
}

idx_t TemporaryBlock::GetStartLBA() {
	return start_lba;
}

idx_t TemporaryBlock::GetEndLBA() {
	return start_lba + lba_amount;
}

bool TemporaryBlock::IsFree() {
	return is_free;
}

NvmeTemporaryBlockManager::NvmeTemporaryBlockManager(idx_t allocated_lba_start, idx_t allocated_lba_end) {
	// Initialize the linked list of free blocks
	blocks =
	    vector<unique_ptr<TemporaryBlock>>(8); // There are 8 different allocation sizes for the TemporaryBufferSize
	blocks[7] = make_unique<TemporaryBlock>(allocated_lba_start, allocated_lba_end - allocated_lba_start);
}

uint8_t NvmeTemporaryBlockManager::GetFreeListIndex(idx_t lba_amount) {
	// Get the index of the free list for the given size
	if (lba_amount <= 8) {
		return 0;
	} else if (lba_amount <= 16) {
		return 1;
	} else if (lba_amount <= 24) {
		return 2;
	} else if (lba_amount <= 32) {
		return 3;
	} else if (lba_amount <= 40) {
		return 4;
	} else if (lba_amount <= 48) {
		return 5;
	} else if (lba_amount <= 56) {
		return 6;
	} else if (lba_amount <= 64) {
		return 7;
	}

	return 7;
}

TemporaryBlock &NvmeTemporaryBlockManager::AllocateBlock(idx_t lba_amount) {
	// Get the free list index for the given size
	uint8_t free_list_index = GetFreeListIndex(lba_amount);

	// Get the free block from the free list
	for (uint8_t i = free_list_index; i < 8; i++) {
		if (blocks[i] != nullptr) {
			// Get the block from the free list
			TemporaryBlock &block = *blocks[i];

			// Check if the block is large enough
			// Split the block if it is larger than the requested size
			if (block.lba_amount > lba_amount) {
				block = SplitBlock(block, lba_amount);
			}

			// Mark the block as used
			block.is_free = false;

			return block;
		}
	}

	// Mark the block as used
	block.is_free = false;

	// Return the block
	return block;
}

} // namespace duckdb
