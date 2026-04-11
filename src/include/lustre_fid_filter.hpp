//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_fid_filter.hpp
//
// Shared FID-only filter for lustre_layouts and lustre_objects
//===----------------------------------------------------------------------===//

#pragma once

#include "lustre_types.hpp"
#include "lustre_string_filter.hpp"

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"

#include <algorithm>

namespace duckdb {
namespace lustre {

//===----------------------------------------------------------------------===//
// FIDOnlyFilter - extracts FID values from WHERE clause (fid column only)
//===----------------------------------------------------------------------===//
struct FIDOnlyFilter {
	//! Static filter values (from WHERE clause constants)
	vector<LustreFID> fid_values;

	//! Number of static values recorded at Create time
	idx_t static_fid_count = 0;

	//! Dynamic filter data pointers (persistent, never cleared)
	vector<shared_ptr<DynamicFilterData>> dynamic_fid_filters;

	//! Full predicate on fid, used when exact lookup is not suitable.
	unique_ptr<LustreStringFilter> fid_predicate;

	bool HasFIDFilter() const {
		return !fid_values.empty();
	}

	bool HasFIDPredicate() const {
		return fid_predicate && fid_predicate->HasPredicate();
	}

	bool HasDynamicFilter() const {
		return !dynamic_fid_filters.empty();
	}

	bool HasAnyFilter() const {
		return HasFIDFilter() || HasFIDPredicate() || HasDynamicFilter();
	}

	bool RequiresGenericEvaluation() const {
		return fid_predicate && fid_predicate->RequiresGenericEvaluation();
	}

	bool ContainsFID(const LustreFID &fid) const {
		if (fid_values.empty()) {
			return true;
		}
		return std::binary_search(fid_values.begin(), fid_values.end(), fid);
	}

	bool EvaluateFID(const LustreFID &fid) const {
		if (!fid_predicate || !fid_predicate->HasPredicate()) {
			return true;
		}
		if (!fid.IsValid()) {
			return fid_predicate->Evaluate(nullptr);
		}
		auto fid_string = fid.ToString();
		return fid_predicate->Evaluate(&fid_string);
	}

	//! Re-resolve dynamic filters. Returns true if values changed.
	bool ResolveDynamicFilters();

	//! Create from DuckDB's TableFilterSet. fid_column_idx is the actual column index for FID.
	static unique_ptr<FIDOnlyFilter> Create(const TableFilterSet *filters, const vector<idx_t> &column_ids,
	                                        idx_t fid_column_idx);
};

} // namespace lustre
} // namespace duckdb
