//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_fid_filter.cpp
//
// Shared FID-only filter implementation
//===----------------------------------------------------------------------===//

#include "lustre_fid_filter.hpp"

#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"

namespace duckdb {
namespace lustre {

static void SortAndUniqueFIDs(vector<LustreFID> &fid_values) {
	std::sort(fid_values.begin(), fid_values.end());
	fid_values.erase(std::unique(fid_values.begin(), fid_values.end()), fid_values.end());
}

static void ParseFIDFilterColumn(const TableFilter &filter,
                                 vector<LustreFID> &fid_values,
                                 vector<shared_ptr<DynamicFilterData>> *dynamic_fid_filters) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &const_filter = filter.Cast<ConstantFilter>();
		if (const_filter.comparison_type != ExpressionType::COMPARE_EQUAL) {
			return;
		}
		auto str = StringValue::Get(const_filter.constant);
		LustreFID fid;
		if (LustreFID::FromString(str, fid)) {
			fid_values.push_back(fid);
		}
		break;
	}

	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		for (const auto &val : in_filter.values) {
			auto str = StringValue::Get(val);
			LustreFID fid;
			if (LustreFID::FromString(str, fid)) {
				fid_values.push_back(fid);
			}
		}
		break;
	}

	case TableFilterType::CONJUNCTION_AND: {
		auto &and_filter = filter.Cast<ConjunctionAndFilter>();
		for (const auto &child : and_filter.child_filters) {
			ParseFIDFilterColumn(*child, fid_values, dynamic_fid_filters);
		}
		break;
	}

	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		if (optional_filter.child_filter) {
			ParseFIDFilterColumn(*optional_filter.child_filter, fid_values, dynamic_fid_filters);
		}
		break;
	}

	case TableFilterType::DYNAMIC_FILTER: {
		auto &dynamic_filter = filter.Cast<DynamicFilter>();
		if (dynamic_filter.filter_data) {
			lock_guard<mutex> lock(dynamic_filter.filter_data->lock);
			if (dynamic_filter.filter_data->initialized && dynamic_filter.filter_data->filter) {
				ParseFIDFilterColumn(*dynamic_filter.filter_data->filter,
				                     fid_values, dynamic_fid_filters);
			} else if (dynamic_fid_filters) {
				dynamic_fid_filters->push_back(dynamic_filter.filter_data);
			}
		}
		break;
	}

	default:
		break;
	}
}

unique_ptr<FIDOnlyFilter> FIDOnlyFilter::Create(const TableFilterSet *filters,
                                                const vector<idx_t> &column_ids,
                                                idx_t fid_column_idx) {
	auto result = make_uniq<FIDOnlyFilter>();

	if (!filters) {
		return result;
	}

	for (const auto &entry : filters->filters) {
		idx_t filter_col_idx = entry.first;
		if (filter_col_idx >= column_ids.size()) {
			continue;
		}
		idx_t actual_column_idx = column_ids[filter_col_idx];
		if (actual_column_idx != fid_column_idx) {
			continue;
		}
		if (!result->fid_predicate) {
			result->fid_predicate = LustreStringFilter::Create(*entry.second);
		}
		ParseFIDFilterColumn(*entry.second, result->fid_values,
		                     &result->dynamic_fid_filters);
	}

	SortAndUniqueFIDs(result->fid_values);
	result->static_fid_count = result->fid_values.size();
	return result;
}

bool FIDOnlyFilter::ResolveDynamicFilters() {
	vector<LustreFID> old_dynamic_fids(fid_values.begin() + static_fid_count, fid_values.end());

	fid_values.resize(static_fid_count);

	for (auto &fd : dynamic_fid_filters) {
		if (!fd) {
			continue;
		}
		lock_guard<mutex> lock(fd->lock);
		if (fd->initialized && fd->filter) {
			ParseFIDFilterColumn(*fd->filter, fid_values, nullptr);
		}
	}

	SortAndUniqueFIDs(fid_values);
	vector<LustreFID> new_dynamic_fids(fid_values.begin() + static_fid_count, fid_values.end());
	return old_dynamic_fids != new_dynamic_fids;
}

} // namespace lustre
} // namespace duckdb
