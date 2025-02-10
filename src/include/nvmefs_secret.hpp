#pragma once

#include "nvmefs_extension.hpp"
#include "duckdb.hpp"
#include <duckdb/main/secret/secret.hpp>

namespace duckdb {

struct CreateSecretInput;
class CreateSecretFunction;

struct CreateNvmefsSecretFunctions {
	public:
		static void Register(DatabaseInstance &instance);
};

} // namespace duckdb
