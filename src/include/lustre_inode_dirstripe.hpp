//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_inode_dirstripe.hpp
//
// Internal fused table function for inode/dirstripe joins.
// Co-scans directory inodes and generates dirstripe rows inline.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {
namespace lustre {

class LustreInodeDirStripeFunction {
public:
	static TableFunctionSet GetFunctionSet();
	static TableFunction GetFunction(bool multi_device);
	static const vector<string> &GetColumnNames();
	static const vector<LogicalType> &GetColumnTypes();
};

} // namespace lustre
} // namespace duckdb
