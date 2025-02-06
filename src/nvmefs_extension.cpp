#define DUCKDB_EXTENSION_MAIN

#include "nvmefs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb
{
	struct NvmeFsHelloFunctionData : public TableFunctionData
	{
		NvmeFsHelloFunctionData()
		{
		}

		bool finished = false;
	};

	static void NvmefsHelloWorld(ClientContext &context, TableFunctionInput &data_p, DataChunk &output)
	{
		auto &data = data_p.bind_data->CastNoConst<NvmeFsHelloFunctionData>();

		if (data.finished)
		{
			return;
		}

		std::string name = context.db->GetFileSystem().GetName();
		idx_t cardinality = 1;

		output.SetValue(0, 0, Value(name));
		output.SetCardinality(cardinality);

		data.finished = true;
	}

	static unique_ptr<FunctionData> NvmefsHelloWorldBind(ClientContext &ctx, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names)
	{
		names.emplace_back("file_system");
		return_types.emplace_back(LogicalType::VARCHAR);

		auto result = make_uniq<NvmeFsHelloFunctionData>();
		result->finished = false;

		return std::move(result);
	}

	inline void NvmefsScalarFun(DataChunk &args, ExpressionState &state, Vector &result)
	{
		auto &name_vector = args.data[0];
		UnaryExecutor::Execute<string_t, string_t>(
			name_vector, result, args.size(),
			[&](string_t name)
			{
				return StringVector::AddString(result, "Nvmefs " + name.GetString() + " 🐥");
				;
			});
	}

	inline void NvmefsOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result)
	{
		auto &name_vector = args.data[0];
		UnaryExecutor::Execute<string_t, string_t>(
			name_vector, result, args.size(),
			[&](string_t name)
			{
				return StringVector::AddString(result, "Nvmefs " + name.GetString() +
														   ", my linked OpenSSL version is " +
														   OPENSSL_VERSION_TEXT);
				;
			});
	}

	static void LoadInternal(DatabaseInstance &instance)
	{
		// Register a scalar function
		auto nvmefs_scalar_function = ScalarFunction("nvmefs", {LogicalType::VARCHAR}, LogicalType::VARCHAR, NvmefsScalarFun);
		ExtensionUtil::RegisterFunction(instance, nvmefs_scalar_function);

		// Register another scalar function
		auto nvmefs_openssl_version_scalar_function = ScalarFunction("nvmefs_openssl_version", {LogicalType::VARCHAR},
																	 LogicalType::VARCHAR, NvmefsOpenSSLVersionScalarFun);
		ExtensionUtil::RegisterFunction(instance, nvmefs_openssl_version_scalar_function);

		TableFunction nvmefs_hello_world_function("nvmefs_hello", {}, NvmefsHelloWorld, NvmefsHelloWorldBind);
		ExtensionUtil::RegisterFunction(instance, nvmefs_hello_world_function);
	}

	void NvmefsExtension::Load(DuckDB &db)
	{
		LoadInternal(*db.instance);
	}
	std::string NvmefsExtension::Name()
	{
		return "nvmefs";
	}

	std::string NvmefsExtension::Version() const
	{
#ifdef EXT_VERSION_NVMEFS
		return EXT_VERSION_NVMEFS;
#else
		return "";
#endif
	}

} // namespace duckdb

extern "C"
{

	DUCKDB_EXTENSION_API void nvmefs_init(duckdb::DatabaseInstance &db)
	{
		duckdb::DuckDB db_wrapper(db);
		db_wrapper.LoadExtension<duckdb::NvmefsExtension>();
	}

	DUCKDB_EXTENSION_API const char *nvmefs_version()
	{
		return duckdb::DuckDB::LibraryVersion();
	}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
