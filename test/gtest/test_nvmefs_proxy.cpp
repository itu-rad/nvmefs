#include <gtest/gtest.h>
#include "nvmefs.hpp"
#include "nvmefs_config.hpp"
#include "utils/gtest_utils.hpp"
#include "utils/fake_device.hpp"

namespace duckdb {

class NoDiskInteractionTest : public testing::Test {
protected:
	static void SetUpTestSuite() {
		file_system = make_uniq<NvmeFileSystem>(gtestutils::TEST_CONFIG, make_uniq<FakeDevice>(0));
	}

	static unique_ptr<NvmeFileSystem> file_system;
};

class DiskInteractionTest : public testing::Test {
protected:
	static void SetUpTestSuite() {
		idx_t block_size = 4096;
		idx_t page_size = 4096 * 64;

		NvmeConfig testConfig {
		    .device_path = "/dev/ng1n1",
		    .plhdls = 8,
		    .max_temp_size = page_size * 10, // 1 GiB in bytes
		    .max_wal_size = 1ULL << 25       // 32 MiB
		};

		file_system = make_uniq<NvmeFileSystem>(testConfig, make_uniq<FakeDevice>(1ULL << 30)); // 1 GiB
	}

	static unique_ptr<NvmeFileSystem> file_system;
};

// provide variable to access for tests
// instead of writing NoDiskInteractionTest::file_system in every test, just file_system
unique_ptr<NvmeFileSystem> NoDiskInteractionTest::file_system = nullptr;

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

} // namespace duckdb
