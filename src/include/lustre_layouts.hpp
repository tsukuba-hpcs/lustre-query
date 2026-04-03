//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_layouts.hpp
//
// Table function for querying Lustre MDT layout components
//===----------------------------------------------------------------------===//

#pragma once

#include "lustre_types.hpp"
#include "lustre_scan_state.hpp"
#include "lustre_fid_filter.hpp"
#include "mdt_scanner.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {
namespace lustre {

//===----------------------------------------------------------------------===//
// Layout Column Index Constants
//===----------------------------------------------------------------------===//
enum class LayoutColumnIdx : idx_t {
	FID = 0,
	COMP_INDEX = 1,
	COMP_ID = 2,
	MIRROR_ID = 3,
	COMP_FLAGS = 4,
	EXTENT_START = 5,
	EXTENT_END = 6,
	PATTERN = 7,
	STRIPE_SIZE = 8,
	STRIPE_COUNT = 9,
	STRIPE_OFFSET = 10,
	POOL = 11,
	DSTRIPE_COUNT = 12,
	CSTRIPE_COUNT = 13,
	COMPR_TYPE = 14,
	COMPR_LVL = 15,
	DEVICE = 16
};

//===----------------------------------------------------------------------===//
// Global State for lustre_layouts
//===----------------------------------------------------------------------===//
struct LustreLayoutsGlobalState : public GlobalTableFunctionState {
	vector<string> device_paths;
	vector<idx_t> column_ids;
	MDTScanConfig scan_config;

	unique_ptr<FIDOnlyFilter> fid_filter;

	atomic<idx_t> current_device_idx;
	atomic<bool> finished;
	atomic<bool> device_initialized;
	mutex device_transition_lock;
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

	//! Buffered results (multiple components per inode)
	struct PendingRow {
		LustreFID fid;
		LustreLayoutComponent component;
	};

	idx_t MaxThreads() const override {
		if (fid_filter && fid_filter->HasDynamicFilter()) {
			return 1;
		}
		if (!fid_filter) {
			return 1;
		}
		if (use_sequential_scan) {
			return MinValue<idx_t>(thread_count, static_cast<idx_t>(MaxValue(total_block_groups, 1)));
		}
		if (!fid_filter->HasFIDFilter()) {
			return 1;
		}
		return MinValue<idx_t>(fid_filter->fid_values.size(), thread_count);
	}
};

//===----------------------------------------------------------------------===//
// Local State for lustre_layouts (per-thread)
//===----------------------------------------------------------------------===//
struct LustreLayoutsLocalState : public LocalTableFunctionState {
	unique_ptr<MDTScanner> scanner;
	bool scanner_initialized = false;
	idx_t initialized_device_idx = DConstants::INVALID_INDEX;
	string initialized_device_path;
	bool sequential_scan_mode = false;
	bool block_group_active = false;
	ext2_ino_t block_group_max_ino = 0;
	int next_block_group_in_batch = 0;
	int block_group_batch_end = 0;

	vector<LustreLayoutsGlobalState::PendingRow> pending_results;
	idx_t pending_results_idx = 0;

	LustreLayoutsLocalState() {
		scanner = make_uniq<MDTScanner>();
	}
};

//===----------------------------------------------------------------------===//
// lustre_layouts Table Function
//===----------------------------------------------------------------------===//
class LustreLayoutsFunction {
public:
	static TableFunctionSet GetFunctionSet();
};

} // namespace lustre
} // namespace duckdb
