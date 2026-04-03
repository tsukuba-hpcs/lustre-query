//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_table_filter_eval.hpp
//
// Generic row-wise evaluation helpers for DuckDB TableFilter trees
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {
namespace lustre {

bool IsGenericTableFilterRewritable(const TableFilter &filter);
bool TableFilterHasDynamicFilter(const TableFilter &filter);
bool EvaluateTableFilterValue(const TableFilter &filter, const Value &value);

} // namespace lustre
} // namespace duckdb
