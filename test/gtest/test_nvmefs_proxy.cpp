#include <gtest/gtest.h>
#include "nvmefs_proxy.hpp"
#include "nvmefs_config.hpp"
#include "utils/gtest_utils.hpp"

namespace duckdb
{

class NoDiskInteractionTest : public testing::Test {
	protected:

		static void SetUpTestSuite() {
			file_system = make_uniq<NvmeFileSystemProxy>(gtestutils::TEST_CONFIG);
		}

		static unique_ptr<NvmeFileSystemProxy> file_system;
};

// provide variable to access for tests
// instead of writing NoDiskInteractionTest::file_system in every test, just file_system
unique_ptr<NvmeFileSystemProxy> NoDiskInteractionTest::file_system = nullptr;

TEST_F(NoDiskInteractionTest, GetNameReturnsName) {
	string result = file_system->GetName();
	EXPECT_EQ(result, "NvmeFileSystemProxy");
}

TEST_F(NoDiskInteractionTest, CanHandleFileValidPathReturnsTrue) {
	bool result = file_system->CanHandleFile("nvmefs://test.db");
	EXPECT_TRUE(result);
}

TEST_F(NoDiskInteractionTest, CanHandleFileInvalidPathReturnsFalse) {
	bool result = file_system->CanHandleFile("test.db");
	EXPECT_FALSE(result);
}

}
