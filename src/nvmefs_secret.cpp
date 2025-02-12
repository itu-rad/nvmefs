#include "nvmefs_secret.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

	static unique_ptr<BaseSecret> CreateNvmefsSecretFromConfig(ClientContext &context, CreateSecretInput &input) {
		auto scope = input.scope;

		if (scope.empty()) {
			scope.push_back("nvmefs://");
		}

		auto config = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);

		for (const auto &pair : input.options) {
			auto lower = StringUtil::Lower(pair.first);
			config->secret_map[lower] = pair.second;
		}

		return std::move(config);
	}

	void SetNvmefsSecretParameters(CreateSecretFunction &function) {
		function.named_parameters["nvme_device_path"] = LogicalType::VARCHAR;
		function.named_parameters["fdp_plhdls"] = LogicalType::BIGINT;
	}

	void RegisterCreateNvmefsSecretFunciton(DatabaseInstance &instance) {
		string type = "nvmefs";

		SecretType secret_type;
		secret_type.name = type;
		secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
		secret_type.default_provider = "config";

		ExtensionUtil::RegisterSecretType(instance, secret_type);

		CreateSecretFunction config_function = {type, "config", CreateNvmefsSecretFromConfig};
		SetNvmefsSecretParameters(config_function);
		ExtensionUtil::RegisterFunction(instance, config_function);
	}

	void CreateNvmefsSecretFunctions::Register(DatabaseInstance &instance) {
		RegisterCreateNvmefsSecretFunciton(instance);
	}
} // namespace duckdb
