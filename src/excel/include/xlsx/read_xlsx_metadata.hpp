#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

class ExtensionLoader;

struct ReadXLSXMetadata {
	static void Register(ExtensionLoader &loader);
	static TableFunction GetFunction();
};

} // namespace duckdb

