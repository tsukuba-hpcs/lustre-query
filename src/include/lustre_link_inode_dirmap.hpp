//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_link_inode_dirmap.hpp
//
// Internal fused table function for 3-way link/dirmap/inode joins.
// Scans links and resolves parent_fid → dir_fid → inode inline via cached lookups.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {
namespace lustre {

class LustreLinkInodeDirMapFunction {
public:
	static TableFunctionSet GetFunctionSet();
	static TableFunction GetFunction(bool multi_device);
	static const vector<string> &GetColumnNames();
	static const vector<LogicalType> &GetColumnTypes();
};

} // namespace lustre
} // namespace duckdb
