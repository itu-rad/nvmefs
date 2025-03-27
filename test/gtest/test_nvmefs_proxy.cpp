#include <gtest/gtest.h>
#include "nvmefs_proxy.hpp"

namespace duckdb
{

TEST(GetMetadataType, BasicAssertions) {
	auto fs = make_uniq<NvmeFileSystemProxy>();

	auto result = fs->GetName();
	EXPECT_EQ(result, "NvmeFileSystemProxy");
}

}
