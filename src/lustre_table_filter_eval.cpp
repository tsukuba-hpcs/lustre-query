//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_table_filter_eval.cpp
//
// Generic row-wise evaluation helpers for DuckDB TableFilter trees
//===----------------------------------------------------------------------===//

#include "lustre_table_filter_eval.hpp"

#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/planner/filter/bloom_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"

namespace duckdb {
namespace lustre {

bool IsGenericTableFilterRewritable(const TableFilter &filter) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON:
	case TableFilterType::IN_FILTER:
	case TableFilterType::IS_NULL:
	case TableFilterType::IS_NOT_NULL:
	case TableFilterType::BLOOM_FILTER:
	case TableFilterType::DYNAMIC_FILTER:
		return true;
	case TableFilterType::CONJUNCTION_AND: {
		auto &and_filter = filter.Cast<ConjunctionAndFilter>();
		for (const auto &child : and_filter.child_filters) {
			if (!IsGenericTableFilterRewritable(*child)) {
				return false;
			}
		}
		return true;
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &or_filter = filter.Cast<ConjunctionOrFilter>();
		for (const auto &child : or_filter.child_filters) {
			if (!IsGenericTableFilterRewritable(*child)) {
				return false;
			}
		}
		return true;
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		return !optional_filter.child_filter || IsGenericTableFilterRewritable(*optional_filter.child_filter);
	}
	default:
		return false;
	}
}

bool TableFilterHasDynamicFilter(const TableFilter &filter) {
	switch (filter.filter_type) {
	case TableFilterType::DYNAMIC_FILTER:
		return true;
	case TableFilterType::CONJUNCTION_AND: {
		auto &and_filter = filter.Cast<ConjunctionAndFilter>();
		for (const auto &child : and_filter.child_filters) {
			if (TableFilterHasDynamicFilter(*child)) {
				return true;
			}
		}
		return false;
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &or_filter = filter.Cast<ConjunctionOrFilter>();
		for (const auto &child : or_filter.child_filters) {
			if (TableFilterHasDynamicFilter(*child)) {
				return true;
			}
		}
		return false;
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		return optional_filter.child_filter && TableFilterHasDynamicFilter(*optional_filter.child_filter);
	}
	default:
		return false;
	}
}

bool EvaluateTableFilterValue(const TableFilter &filter, const Value &value) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON:
		return !value.IsNull() && filter.Cast<ConstantFilter>().Compare(value);
	case TableFilterType::IN_FILTER: {
		if (value.IsNull()) {
			return false;
		}
		auto &in_filter = filter.Cast<InFilter>();
		for (const auto &entry : in_filter.values) {
			if (ValueOperations::Equals(value, entry)) {
				return true;
			}
		}
		return false;
	}
	case TableFilterType::IS_NULL:
		return value.IsNull();
	case TableFilterType::IS_NOT_NULL:
		return !value.IsNull();
	case TableFilterType::CONJUNCTION_AND: {
		auto &and_filter = filter.Cast<ConjunctionAndFilter>();
		for (const auto &child : and_filter.child_filters) {
			if (!EvaluateTableFilterValue(*child, value)) {
				return false;
			}
		}
		return true;
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &or_filter = filter.Cast<ConjunctionOrFilter>();
		for (const auto &child : or_filter.child_filters) {
			if (EvaluateTableFilterValue(*child, value)) {
				return true;
			}
		}
		return false;
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		return !optional_filter.child_filter || EvaluateTableFilterValue(*optional_filter.child_filter, value);
	}
	case TableFilterType::BLOOM_FILTER: {
		auto &bf_filter = filter.Cast<BFTableFilter>();
		if (value.IsNull()) {
			return !bf_filter.FiltersNullValues();
		}
		return bf_filter.FilterValue(value);
	}
	case TableFilterType::DYNAMIC_FILTER: {
		auto &dynamic_filter = filter.Cast<DynamicFilter>();
		if (!dynamic_filter.filter_data) {
			return true;
		}
		lock_guard<mutex> lock(dynamic_filter.filter_data->lock);
		if (!dynamic_filter.filter_data->initialized || !dynamic_filter.filter_data->filter) {
			return true;
		}
		return !value.IsNull() && dynamic_filter.filter_data->filter->Compare(value);
	}
	default:
		return true;
	}
}

} // namespace lustre
} // namespace duckdb
