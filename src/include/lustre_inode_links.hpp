//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_inode_links.hpp
//
// Internal fused table function for inode/link joins
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {
namespace lustre {

class LustreInodeLinksFunction {
public:
	static TableFunctionSet GetFunctionSet();
	static TableFunction GetFunction(bool multi_device);
	static const vector<string> &GetColumnNames();
	static const vector<LogicalType> &GetColumnTypes();
};

} // namespace lustre
} // namespace duckdb
