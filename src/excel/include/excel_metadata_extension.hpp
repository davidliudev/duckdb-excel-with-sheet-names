//===----------------------------------------------------------------------===//
//                         DuckDB
//
// excel_metadata_extension.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

class ExcelMetadataExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
};

} // namespace duckdb

