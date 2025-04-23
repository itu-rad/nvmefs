#include "nvmefs_temporary_block_manager.hpp"
#include <optional>

namespace duckdb {
TemporaryBlock::TemporaryBlock(idx_t start_lba, idx_t lba_amount)
    : start_lba(start_lba), lba_amount(lba_amount), is_free(false) {

	next_block = nullptr;
	previous_block = nullptr;
	next_free_block = nullptr;
	previous_free_block = nullptr;
}

idx_t TemporaryBlock::GetSizeInBytes() {
	return (lba_amount + 1) * 4096; // TODO: Get the LBA size from the device
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
	blocks = make_uniq<TemporaryBlock>(allocated_lba_start, allocated_lba_end - allocated_lba_start);
	blocks_free =
	    vector<TemporaryBlock *>(8, nullptr); // There are 8 different allocation sizes for the TemporaryBufferSize

	blocks_free[7] = blocks.get(); // The largest block is the first one
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

	TemporaryBlock *block = nullptr;

	// Get the free block from the free list
	for (uint8_t i = free_list_index; i < 8; i++) {
		if (blocks_free[i] != nullptr) {
			// Get the block from the free list
			block = PopFreeBlock(i);

			// Check if the block is large enough
			// Split the block if it is larger than the requested size
			if (block->lba_amount > lba_amount) {
				block = SplitBlock(block, lba_amount);
			}

			break; // If we are in here we have found a block
		}
	}

	if (block == nullptr) {
		throw std::runtime_error("No free block available");
	}

	// Return the block
	return *block;
}

TemporaryBlock *NvmeTemporaryBlockManager::SplitBlock(TemporaryBlock *block, idx_t lba_amount) {
	// Create a new block with the remaining size
	unique_ptr<TemporaryBlock> new_block = make_uniq<TemporaryBlock>(block->start_lba, lba_amount - 1);
	TemporaryBlock *new_block_ptr = new_block.get();

	// Update the original block size
	idx_t endLba = block->GetEndLBA();
	block->start_lba += lba_amount;
	block->lba_amount = endLba - block->start_lba;

	// Add the new block to the free list
	if (block->previous_block != nullptr) {
		new_block->previous_block = block->previous_block; // Set the previous block to the new split block
		new_block->next_block = move(block->previous_block->next_block);
		block->previous_block->next_block = move(new_block); // Move the new block to the previous block
	} else {
		new_block->next_block = move(blocks); // Move the new block to the head of the list
		blocks = move(new_block);             // Move the new block to the head of the list
	}

	block->previous_block = new_block.get(); // Set the previous block to the new split block

	PushFreeBlock(block); // Add the block to the free list

	return new_block_ptr;
}

void NvmeTemporaryBlockManager::FreeBlock(TemporaryBlock &block) {
	TemporaryBlock *block_ptr = block.next_block->previous_block;

	// Mark the block as free
	block.is_free = true;

	// Coalesce the free blocks
	CoalesceFreeBlocks(block);

	// Add the block to the free list
	PushFreeBlock(block_ptr);
}

void NvmeTemporaryBlockManager::PushFreeBlock(TemporaryBlock *block) {
	D_ASSERT(block->IsFree());
	D_ASSERT(block->next_free_block == nullptr);
	D_ASSERT(block->previous_free_block == nullptr);

	// Add the block to the free list
	uint8_t free_list_index = GetFreeListIndex(block->lba_amount);

	TemporaryBlock *previous_top_block = blocks_free[free_list_index];
	if (previous_top_block != nullptr) {
		previous_top_block->previous_free_block = block; // Set the previous block to the new free block
	}

	// Add the block to be the new top of the free list
	blocks_free[free_list_index] = block;
	block->next_free_block = previous_top_block; // Attach the provious top block to the new top block
}

TemporaryBlock *NvmeTemporaryBlockManager::PopFreeBlock(uint8_t free_list_index) {
	// Get the block from the free list
	TemporaryBlock *popped_block = blocks_free[free_list_index];

	D_ASSERT(popped_block != nullptr);
	D_ASSERT(popped_block->IsFree());

	TemporaryBlock *new_head_block = popped_block->next_free_block;

	// Remove the block from the free list
	blocks_free[free_list_index] = new_head_block;
	popped_block->is_free = false; // Mark the block as free

	// Clean the free block pointers
	popped_block->next_free_block = nullptr;
	popped_block->previous_free_block = nullptr;

	if (new_head_block != nullptr) {
		new_head_block->previous_free_block = nullptr; // Set the previous block to null
	}

	return popped_block;
}

void NvmeTemporaryBlockManager::RemoveFreeBlock(TemporaryBlock *block) {
	if (block->next_free_block != nullptr) {
		block->next_free_block->previous_free_block = block->previous_free_block;
	}

	if (block->previous_free_block == nullptr) {
		uint8_t free_list_index = GetFreeListIndex(block->lba_amount);

		blocks_free[free_list_index] = block->next_free_block;
		blocks_free[free_list_index]->previous_free_block = nullptr; // Set the previous block to null
	} else {
		// Set the previous free block to the next free block
		block->previous_free_block->next_free_block = block->next_free_block;
	}

	block->next_free_block = nullptr;
	block->previous_free_block = nullptr;
}

void NvmeTemporaryBlockManager::CoalesceFreeBlocks(TemporaryBlock &block) {
	// Check if the previous block is free
	if ((block.previous_block != nullptr && block.previous_block->IsFree()) &&
	    (block.next_block != nullptr && block.next_block->IsFree())) {

		block.lba_amount += block.previous_block->lba_amount + block.next_block->lba_amount;

		unique_ptr<TemporaryBlock> prev_block =
		    move(block.previous_block->previous_block
		             ->next_block); // Save the previous blocks smart pointer so it doesn't get cleaned up
		unique_ptr<TemporaryBlock> next_block = move(block.next_block);

		// Merge the previous block
		if (prev_block->previous_block != nullptr) {
			prev_block->previous_block->next_block =
			    move(prev_block->next_block); // Move the previous block to the new merged block
		} else {
			blocks = move(prev_block->next_block);
		}
		block.previous_block = prev_block->previous_block; // Set the previous block to the new merged block

		RemoveFreeBlock(prev_block.get()); // Remove the previous block from the free list

		// Merge the next block
		next_block->next_block->previous_block =
		    next_block->previous_block;                        // Set the previous block to the new merged block
		block.next_block = move(block.next_block->next_block); // Move the next block to the new merged block

		RemoveFreeBlock(next_block.get()); // Remove the next block from the free list

	} else if (block.previous_block != nullptr && block.previous_block->IsFree()) {
		block.lba_amount += block.previous_block->lba_amount;

		unique_ptr<TemporaryBlock> prev_block =
		    move(block.previous_block->previous_block
		             ->next_block); // Save the previous blocks smart pointer so it doesn't get cleaned up

		// Merge the previous block
		if (prev_block->previous_block != nullptr) {
			prev_block->previous_block->next_block =
			    move(prev_block->next_block); // Move the previous block to the new merged block
		} else {
			blocks = move(prev_block->next_block);
		}
		block.previous_block = prev_block->previous_block; // Set the previous block to the new merged block

		RemoveFreeBlock(prev_block.get()); // Remove the previous block from the free list
	} else if (block.next_block != nullptr && block.next_block->IsFree()) {
		block.lba_amount += block.next_block->lba_amount;

		unique_ptr<TemporaryBlock> next_block = move(block.next_block);

		// Merge the next block
		next_block->next_block->previous_block =
		    next_block->previous_block;                        // Set the previous block to the new merged block
		block.next_block = move(block.next_block->next_block); // Move the next block to the new merged block

		RemoveFreeBlock(next_block.get()); // Remove the next block from the free list
	}
}

} // namespace duckdb
