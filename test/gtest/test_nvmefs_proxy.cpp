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

TEST_F(DiskInteractionTest, OpenFileCompleteInvalidPathThrowInvalidInputException) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_WRITE;
	ASSERT_THROW(file_system->OpenFile("nvmefs://test", flags), InvalidInputException);
}

TEST_F(DiskInteractionTest, OpenFileInvalidDBPathThrowIOException) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_WRITE;
	ASSERT_THROW(file_system->OpenFile("nvmefs://test.wal", flags), IOException);
}

TEST_F(DiskInteractionTest, OpenFileValidDBPathThrowNoExpeception) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_WRITE;
	ASSERT_NO_THROW(file_system->OpenFile("nvmefs://test.db", flags));
}

TEST_F(DiskInteractionTest, OpenFileProducesCorrectFileHandle){
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);
	NvmeFileHandle& nvme_fh = fh->Cast<NvmeFileHandle>();
	NvmeFileSystem& nvme_fs = nvme_fh.file_system.Cast<NvmeFileSystem>();


	EXPECT_EQ(nvme_fh.path, "nvmefs://test.db");
	EXPECT_EQ(nvme_fh.flags.OpenForWriting(), true);

	// Check that underlying filesytem is the same
	EXPECT_EQ(&nvme_fs, &*file_system);
}

} // namespace duckdb
