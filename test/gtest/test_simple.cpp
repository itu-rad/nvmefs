#include <gtest/gtest.h>
#include "nvmefs_proxy.hpp"
#include "test_helpers.hpp"

namespace duckdb
{

TEST(TestSimpleSum, Assertion) {
	int err = system("sh ../../scripts/nvme/device_dealloc.sh");
	EXPECT_EQ(err, 0);

	auto config = GetTestConfig();
	DuckDB db("nvmefs://test.db", config.get());
	Connection con(db);

	EXPECT_NO_THROW(con.Query("CREATE SCHEMA public;"));
	EXPECT_NO_THROW(con.Query("CREATE TABLE public.test (num INTEGER);"));
	EXPECT_NO_THROW(con.Query("INSERT INTO public.test VALUES (1), (2), (3), (4), (5);"));

	for(idx_t i = 0; i < 5; ++i) {
		DuckDB db("nvmefs://test.db", config.get());
		Connection con(db);

		auto result = con.Query("SELECT SUM(num) from public.test;");
		EXPECT_EQ(result->ToString(), "15")
	}
}

}
