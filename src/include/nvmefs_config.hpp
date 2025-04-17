#pragma once

#include "duckdb.hpp"

#include <duckdb/main/secret/secret.hpp>

namespace duckdb {

struct CreateSecretInput;
class CreateSecretFunction;

struct CreateNvmefsSecretFunctions {
public:
	static void Register(DatabaseInstance &instance);
};

struct NvmeConfig {
	string device_path;
	string backend;
	bool async;
	uint64_t plhdls;
	uint64_t max_temp_size;
	uint64_t max_wal_size;
};

class NvmeConfigManager {
public:
	static void RegisterConfigFunctions(DatabaseInstance &instance) {
		CreateNvmefsSecretFunctions::Register(instance);
	};
	static NvmeConfig LoadConfig(DatabaseInstance &instance);

private:
	static bool IsAsynchronousBackend(const string &backend);
	static string SanatizeBackend(const string &backend);
};

} // namespace duckdb
