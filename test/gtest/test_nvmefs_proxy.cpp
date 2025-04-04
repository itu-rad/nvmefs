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
	static void SetUpTestSuite() {
		idx_t block_size = 4096;
		idx_t page_size = 4096 * 64;

		NvmeConfig testConfig {
		    .device_path = "/dev/ng1n1",
		    .plhdls = 8,
		    .max_temp_size = page_size * 10, // 10 pages
		    .max_wal_size = 1ULL << 25       // 32 MiB
		};

		fs = make_uniq<NvmeFileSystem>(testConfig, make_uniq<FakeDevice>(1ULL << 30)); // 1 GiB
	}

	static unique_ptr<NvmeFileSystem> fs;
};

// provide variable to access for tests
// instead of writing DiskInteractionTest::fs in every test, just write fs
unique_ptr<NvmeFileSystem> DiskInteractionTest::fs = nullptr;

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

// TEST_F(DiskInteractionTest, FileSync) {
// 	file_system
// }

} // namespace duckdb
