#pragma once

#include "duckdb.h"

namespace duckdb {

	struct Metadata {
		uint64_t location;
	};

	struct GlobalMetadata : Metadata {
		uint64_t catalog;
		uint64_t temporary;
	};

	struct CatalogMetadata : Metadata {

	};

	struct TemporaryMetadata : Metadata {

	};



	class NvmeMetadataManager {
		public:
			NvmeMetadataManager();
			~NvmeMetadataManager() = default;

			Metadata ReadMetadata();
			int64_t WriteMetadata();

	};

} // namespace duckdb
