//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_objects.hpp
//
// Table function for querying Lustre MDT per-stripe OST object placement
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
// Object Column Index Constants
//===----------------------------------------------------------------------===//
enum class ObjectColumnIdx : idx_t {
	FID = 0,
	COMP_INDEX = 1,
	STRIPE_INDEX = 2,
	OST_IDX = 3,
	OST_OI_ID = 4,
	OST_OI_SEQ = 5,
	DEVICE = 6
};

//===----------------------------------------------------------------------===//
// Global State for lustre_objects
//===----------------------------------------------------------------------===//
struct LustreObjectsGlobalState : public GlobalTableFunctionState {
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

	//! Buffered results (multiple objects per inode)
	struct PendingRow {
		LustreFID fid;
		LustreOSTObject object;
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
// Local State for lustre_objects (per-thread)
//===----------------------------------------------------------------------===//
struct LustreObjectsLocalState : public LocalTableFunctionState {
	unique_ptr<MDTScanner> scanner;
	bool scanner_initialized = false;
	idx_t initialized_device_idx = DConstants::INVALID_INDEX;
	string initialized_device_path;
	bool sequential_scan_mode = false;
	bool block_group_active = false;
	ext2_ino_t block_group_max_ino = 0;
	int next_block_group_in_batch = 0;
	int block_group_batch_end = 0;

	vector<LustreObjectsGlobalState::PendingRow> pending_results;
	idx_t pending_results_idx = 0;

	LustreObjectsLocalState() {
		scanner = make_uniq<MDTScanner>();
	}
};

//===----------------------------------------------------------------------===//
// lustre_objects Table Function
//===----------------------------------------------------------------------===//
class LustreObjectsFunction {
public:
	static TableFunctionSet GetFunctionSet();
};

} // namespace lustre
} // namespace duckdb
