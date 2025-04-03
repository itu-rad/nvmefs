#include <gtest/gtest.h>
#include "nvmefs.hpp"
#include "nvmefs_config.hpp"
#include "utils/gtest_utils.hpp"
#include "utils/fake_device.hpp"

namespace duckdb
{

class NoDiskInteractionTest : public testing::Test {
	protected:

		static void SetUpTestSuite() {
			file_system = make_uniq<NvmeFileSystem>(gtestutils::TEST_CONFIG, make_uniq<FakeDevice>(0));
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

}
