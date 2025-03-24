#include <gtest/gtest.h>

TEST(helloworldtest, BasicAssertions) {
	EXPECT_STRNE("hello", "world");
	EXPECT_EQ(7*7, 49);
}
