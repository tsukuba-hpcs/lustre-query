//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_links.hpp
//
// Table function for querying Lustre MDT hard link information
//===----------------------------------------------------------------------===//

#pragma once

#include "lustre_types.hpp"
#include "lustre_scan_state.hpp"
#include "lustre_string_filter.hpp"
#include "mdt_scanner.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"

#include <algorithm>

namespace duckdb {
namespace lustre {

//===----------------------------------------------------------------------===//
// Link Column Index Constants
//===----------------------------------------------------------------------===//
enum class LinkColumnIdx : idx_t { FID = 0, PARENT_FID = 1, NAME = 2, DEVICE = 3 };

//===----------------------------------------------------------------------===//
// Link Filter - extracts FID/parent_fid values from WHERE clause
//===----------------------------------------------------------------------===//
struct LinkFilter {
	//! Static filter values (from WHERE clause constants, fixed at Create time)
	vector<LustreFID> fid_values;
	vector<LustreFID> parent_fid_values;

	//! Number of static values recorded at Create time (for ResolveDynamicFilters rollback)
	idx_t static_fid_count = 0;
	idx_t static_parent_fid_count = 0;

	//! Dynamic filter data pointers (persistent, never cleared).
	//! JOIN ON conditions arrive as DynamicFilter which is not yet initialized
	//! at InitGlobal time — the join operator populates the value later.
	vector<shared_ptr<DynamicFilterData>> dynamic_fid_filters;
	vector<shared_ptr<DynamicFilterData>> dynamic_parent_fid_filters;

	//! Full predicates used by sequential fallback.
	unique_ptr<LustreStringFilter> fid_predicate;
	unique_ptr<LustreStringFilter> parent_fid_predicate;

	bool HasFIDFilter() const {
		return !fid_values.empty();
	}

	bool HasFIDPredicate() const {
		return fid_predicate && fid_predicate->HasPredicate();
	}

	bool HasParentFIDFilter() const {
		return !parent_fid_values.empty();
	}

	bool HasParentFIDPredicate() const {
		return parent_fid_predicate && parent_fid_predicate->HasPredicate();
	}

	//! Whether unresolved dynamic filters exist on fid or parent_fid
	bool HasDynamicFilter() const {
		return !dynamic_fid_filters.empty() || !dynamic_parent_fid_filters.empty();
	}

	bool HasAnyFilter() const {
		return HasFIDFilter() || HasFIDPredicate() || HasParentFIDFilter() || HasParentFIDPredicate() ||
		       HasDynamicFilter();
	}

	bool RequiresGenericEvaluation() const {
		return (fid_predicate && fid_predicate->RequiresGenericEvaluation()) ||
		       (parent_fid_predicate && parent_fid_predicate->RequiresGenericEvaluation());
	}

	bool ContainsFID(const LustreFID &fid) const {
		if (fid_values.empty()) {
			return true;
		}
		return std::binary_search(fid_values.begin(), fid_values.end(), fid);
	}

	bool ContainsParentFID(const LustreFID &fid) const {
		if (parent_fid_values.empty()) {
			return true;
		}
		return std::binary_search(parent_fid_values.begin(), parent_fid_values.end(), fid);
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

	bool EvaluateParentFID(const LustreFID &fid) const {
		if (!parent_fid_predicate || !parent_fid_predicate->HasPredicate()) {
			return true;
		}
		if (!fid.IsValid()) {
			return parent_fid_predicate->Evaluate(nullptr);
		}
		auto fid_string = fid.ToString();
		return parent_fid_predicate->Evaluate(&fid_string);
	}

	//! Re-resolve dynamic filters into fid_values / parent_fid_values.
	//! Resets to static values and re-parses dynamic filter data each call.
	//! Returns true if the resolved values changed from the previous call.
	bool ResolveDynamicFilters();

	//! Create a LinkFilter from DuckDB's TableFilterSet
	static unique_ptr<LinkFilter> Create(const TableFilterSet *filters, const vector<idx_t> &column_ids);
};

//===----------------------------------------------------------------------===//
// Global State for lustre_links (filter-pushdown based)
//===----------------------------------------------------------------------===//
struct LustreLinksGlobalState : public GlobalTableFunctionState {
	//! Device paths to scan
	vector<string> device_paths;

	//! Column IDs to output (for projection pushdown)
	vector<idx_t> column_ids;

	//! Scan configuration
	MDTScanConfig scan_config;

	//! Extracted filter from WHERE clause
	unique_ptr<LinkFilter> link_filter;

	//! Current device index
	atomic<idx_t> current_device_idx;

	//! Scan finished flag
	atomic<bool> finished;

	//! Current device initialized
	atomic<bool> device_initialized;

	//! Mutex for device transitions
	mutex device_transition_lock;

	//! Current device path
	string current_device_path;

	//! Next FID index (atomic for parallel access)
	atomic<idx_t> next_fid_idx;

	//! Sequential fallback state
	atomic<int> next_block_group;
	atomic<int> active_block_groups;
	int total_block_groups = 0;
	uint32_t inodes_per_group = 0;
	bool use_sequential_scan = false;

	//! Number of threads to use
	idx_t thread_count = 1;

	idx_t MaxThreads() const override {
		// Dynamic filters require single-threaded execution
		if (link_filter && link_filter->HasDynamicFilter()) {
			return 1;
		}
		if (!link_filter) {
			return 1;
		}
		if (use_sequential_scan) {
			return MinValue<idx_t>(thread_count, static_cast<idx_t>(MaxValue(total_block_groups, 1)));
		}
		idx_t fid_count =
		    link_filter->HasFIDFilter() ? link_filter->fid_values.size() : link_filter->parent_fid_values.size();
		return MinValue<idx_t>(fid_count, thread_count);
	}
};

//===----------------------------------------------------------------------===//
// Local State for lustre_links (per-thread)
//===----------------------------------------------------------------------===//
struct LustreLinksLocalState : public LocalTableFunctionState {
	//! Per-thread MDT scanner
	unique_ptr<MDTScanner> scanner;
	bool scanner_initialized = false;
	idx_t initialized_device_idx = DConstants::INVALID_INDEX;
	bool sequential_scan_mode = false;
	bool block_group_active = false;
	ext2_ino_t block_group_max_ino = 0;
	int next_block_group_in_batch = 0;
	int block_group_batch_end = 0;

	//! Per-thread buffered results from link expansion
	vector<LustreLink> pending_results;
	idx_t pending_results_idx = 0;

	LustreLinksLocalState() {
		scanner = make_uniq<MDTScanner>();
	}
};

//===----------------------------------------------------------------------===//
// lustre_links Table Function
//===----------------------------------------------------------------------===//
class LustreLinksFunction {
public:
	//! Get the table function set (VARCHAR and LIST(VARCHAR) overloads)
	static TableFunctionSet GetFunctionSet();
};

} // namespace lustre
} // namespace duckdb
