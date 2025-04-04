#include <gtest/gtest.h>
#include "nvmefs.hpp"
#include "nvmefs_config.hpp"
#include "utils/gtest_utils.hpp"
#include "utils/fake_device.hpp"

namespace duckdb {

class NoDiskInteractionTest : public testing::Test {
protected:
	NoDiskInteractionTest() {
		// Set up the test environment
		file_system = make_uniq<NvmeFileSystem>(gtestutils::TEST_CONFIG, make_uniq<FakeDevice>(0));
	}

	unique_ptr<NvmeFileSystem> file_system;
};

class DiskInteractionTest : public testing::Test {

protected:
	DiskInteractionTest() {
		// Set up the test environment
		idx_t block_size = 4096;
		idx_t page_size = 4096 * 64;

		NvmeConfig testConfig {
		    .device_path = "/dev/ng1n1",
		    .plhdls = 8,
		    .max_temp_size = page_size * 10, // 10 pages
		    .max_wal_size = 1ULL << 25       // 32 MiB
		};

		file_system = make_uniq<NvmeFileSystem>(testConfig, make_uniq<FakeDevice>(1ULL << 30)); // 1 GiB
	}

	unique_ptr<NvmeFileSystem> file_system;
};

TEST_F(NoDiskInteractionTest, GetNameReturnsName) {
	string result = file_system->GetName();

	EXPECT_EQ(result, "NvmeFileSystem");
}

TEST_F(NoDiskInteractionTest, CanHandleFileValidPathReturnsTrue) {
	bool result = file_system->CanHandleFile("nvmefs://test.db");
	EXPECT_TRUE(result);
}

TEST_F(NoDiskInteractionTest, CanHandleFileInvalidPathReturnsFalse) {
	bool result = file_system->CanHandleFile("test.db");
	EXPECT_FALSE(result);
}

TEST_F(DiskInteractionTest, WriteAndReadData) {

	// Create a file
	string file_path = "nvmefs://test.db";
	FileHandle file = fs->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	// Write some data to the file
	string data = "Hello, World!";
	file->Write(data.c_str(), data.size());

	// Read the data back
	vector<char> buffer(data.size());
	file->Read(buffer.data(), data.size());

	// Check that the data is correct
	EXPECT_EQ(string(buffer.data(), buffer.size()), data);
}

} // namespace duckdb
