#pragma once

#include "duckdb.hpp"
#include "nvmefs.hpp"
#include "nvmefs_config.hpp"

namespace duckdb {

class NvmefsExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb
