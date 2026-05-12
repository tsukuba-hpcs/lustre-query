//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_link_dirstripe.hpp
//
// Internal fused table function for link/dirstripe joins.
// Scans links and resolves parent_fid → dir_fid inline via cached LMV lookup.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {
namespace lustre {

class LustreLinkDirStripeFunction {
public:
	static TableFunctionSet GetFunctionSet();
	static TableFunction GetFunction(bool multi_device);
	static const vector<string> &GetColumnNames();
	static const vector<LogicalType> &GetColumnTypes();
};

} // namespace lustre
} // namespace duckdb
