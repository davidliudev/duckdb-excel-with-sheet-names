#include "excel_metadata_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "xlsx/read_xlsx_metadata.hpp"

namespace duckdb {

//--------------------------------------------------------------------------------------------------
// Load
//--------------------------------------------------------------------------------------------------

static void LoadInternal(ExtensionLoader &loader) {
	ReadXLSXMetadata::Register(loader);
}

void ExcelMetadataExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string ExcelMetadataExtension::Name() {
	return "excel_metadata";
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(excel_metadata, loader) {
	duckdb::LoadInternal(loader);
}

}
