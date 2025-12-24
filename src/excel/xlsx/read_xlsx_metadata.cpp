#include "xlsx/read_xlsx_metadata.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "xlsx/parsers/relationship_parser.hpp"
#include "xlsx/parsers/workbook_parser.hpp"
#include "xlsx/zip_file.hpp"

namespace duckdb {

static string XLSXUnescapeXMLEntities(const string &input) {
	string result;
	result.reserve(input.length());

	for (idx_t i = 0; i < input.length(); i++) {
		if (input[i] == '&') {
			auto semi = input.find(';', i);
			if (semi != string::npos) {
				string entity = input.substr(i, semi - i + 1);
				if (entity == "&lt;") {
					result += '<';
					i = semi;
				} else if (entity == "&gt;") {
					result += '>';
					i = semi;
				} else if (entity == "&amp;") {
					result += '&';
					i = semi;
				} else if (entity == "&quot;") {
					result += '"';
					i = semi;
				} else if (entity == "&apos;") {
					result += '\'';
					i = semi;
				} else {
					result += input[i];
				}
			} else {
				result += input[i];
			}
		} else {
			result += input[i];
		}
	}
	return result;
}

static vector<string> ListXLSXWorksheetNames(ZipFileReader &reader) {
	if (!reader.TryOpenEntry("xl/workbook.xml")) {
		throw BinderException("No xl/workbook.xml found in xlsx file");
	}
	const auto workbook_sheets = WorkBookParser::GetSheets(reader);
	reader.CloseEntry();

	if (!reader.TryOpenEntry("xl/_rels/workbook.xml.rels")) {
		throw BinderException("No xl/_rels/workbook.xml.rels found in xlsx file");
	}
	const auto wbrels = RelParser::ParseRelations(reader);
	reader.CloseEntry();

	unordered_set<string> worksheet_rids;
	for (const auto &rel : wbrels) {
		if (StringUtil::EndsWith(rel.type, "/worksheet")) {
			worksheet_rids.insert(rel.id);
		}
	}

	vector<string> sheet_names;
	sheet_names.reserve(workbook_sheets.size());
	for (const auto &sheet : workbook_sheets) {
		if (!worksheet_rids.empty() && worksheet_rids.find(sheet.second) == worksheet_rids.end()) {
			continue;
		}
		sheet_names.emplace_back(XLSXUnescapeXMLEntities(sheet.first));
	}

	if (sheet_names.empty()) {
		throw BinderException("No sheets found in xlsx file (is the file corrupt?)");
	}
	return sheet_names;
}

class XLSXMetadataData final : public TableFunctionData {
public:
	vector<string> sheet_names;
};

class XLSXMetadataGlobalState final : public GlobalTableFunctionState {
public:
	idx_t offset = 0;
};

static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<XLSXMetadataData>();
	const auto file_path = StringValue::Get(input.inputs[0]);

	// Glob here so that we auto load any required extension filesystems (e.g., httpfs)
	auto &fs = FileSystem::GetFileSystem(context);
	auto files = fs.GlobFiles(file_path, context, FileGlobOptions::ALLOW_EMPTY);
	if (files.empty()) {
		throw IOException("Cannot open file \"%s\": No such file or directory", file_path);
	}

	ZipFileReader archive(context, files.front().path);
	result->sheet_names = ListXLSXWorksheetNames(archive);

	names = {"sheet_name", "sheet_index"};
	return_types = {LogicalType::VARCHAR, LogicalType::BIGINT};
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<XLSXMetadataGlobalState>();
}

static void Execute(ClientContext &, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<XLSXMetadataData>();
	auto &state = data.global_state->Cast<XLSXMetadataGlobalState>();

	if (state.offset >= bind_data.sheet_names.size()) {
		return;
	}

	const auto remaining = bind_data.sheet_names.size() - state.offset;
	const auto out_count = MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE);

	auto &name_vec = output.data[0];
	auto &index_vec = output.data[1];
	auto name_ptr = FlatVector::GetData<string_t>(name_vec);
	auto index_ptr = FlatVector::GetData<int64_t>(index_vec);

	for (idx_t row_idx = 0; row_idx < out_count; row_idx++) {
		const auto sheet_idx = state.offset + row_idx;
		name_ptr[row_idx] = StringVector::AddString(name_vec, bind_data.sheet_names[sheet_idx]);
		index_ptr[row_idx] = NumericCast<int64_t>(sheet_idx);
	}

	state.offset += out_count;
	output.SetCardinality(out_count);
}

TableFunction ReadXLSXMetadata::GetFunction() {
	TableFunction fun("read_xlsx_metadata", {LogicalType::VARCHAR}, Execute, Bind);
	fun.init_global = InitGlobal;
	return fun;
}

void ReadXLSXMetadata::Register(ExtensionLoader &loader) {
	loader.RegisterFunction(GetFunction());
}

} // namespace duckdb
