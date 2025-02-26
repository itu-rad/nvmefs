#pragma once

#include "duckdb.hpp"
#include "nvmefs_extension.hpp"

#include <duckdb/main/secret/secret.hpp>

namespace duckdb {

struct CreateSecretInput;
class CreateSecretFunction;

struct CreateNvmefsSecretFunctions {
public:
	static void Register(DatabaseInstance &instance);
};

} // namespace duckdb
