#pragma once

#include "duckdb.hpp"

namespace duckdb {

/// @brief TemporaryBlock is a class that represents a block of LBAs that are use to store temporary data.
class TemporaryBlock {
	friend class NvmeTemporaryBlockManager;

public:
	/// @brief Constructor for TemporaryBlock
	/// @param start_lba Start LBA of the block (inclusive)
	/// @param lba_amount Number of LBAs in the block
	TemporaryBlock(idx_t start_lba, idx_t lba_amount);
	idx_t GetSizeInBytes();
	idx_t GetStartLBA();
	idx_t GetEndLBA();

	bool IsFree();

private:
	idx_t start_lba;
	idx_t lba_amount;
	bool is_free;

	// The next and previous blocks in the linked list
	unique_ptr<TemporaryBlock> next_block;
	TemporaryBlock *previous_block;

	// The next and previous blocks of the same size
	TemporaryBlock *next_free_block;
	TemporaryBlock *previous_free_block;
};

class NvmeTemporaryBlockManager {
public:
	NvmeTemporaryBlockManager(idx_t allocated_lba_start, idx_t allocated_lba_end);

public:
	TemporaryBlock *AllocateBlock(idx_t lba_amount);
	void FreeBlock(TemporaryBlock *block);

private:
	uint8_t GetFreeListIndex(idx_t lba_amount);

	/// @brief Splits a block into two blocks. The first block will be the requested size and the second block will be
	/// the remaining size.
	/// @param block The block to split
	/// @param lba_amount Amount that the new splitted block should have
	/// @return The splitted block
	TemporaryBlock *SplitBlock(TemporaryBlock *block, idx_t lba_amount);
	void PrintBlocks(TemporaryBlock *block);
	void PrintBlocksBackwards(TemporaryBlock *block);

	/// @brief Looks if the previous and next blocks are free. If they are, it merges them into one block.
	/// @param block The block to merge
	void CoalesceFreeBlocks(TemporaryBlock *block);

	void PushFreeBlock(TemporaryBlock *block);
	TemporaryBlock *PopFreeBlock(uint8_t free_list_index);
	void RemoveFreeBlock(TemporaryBlock *block);

private:
	/// @brief Free blocks are stored in a vector of unique_ptrs to TemporaryBlock objects. This is to sort a block into
	/// a linked list of equally sized blocks so that it is quicker to fetch the block that is needed.
	unique_ptr<TemporaryBlock> blocks;
	vector<TemporaryBlock *> blocks_free;

	idx_t allocated_start_lba;
	idx_t allocated_end_lba;
};

} // namespace duckdb
