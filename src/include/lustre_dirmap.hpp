//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_dirmap.hpp
//
// Table function for querying Lustre DNE2-aware directory mapping.
// Maps physical parent FIDs (shard/namespace bearers) to logical directory FIDs.
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
// DirMap Column Index Constants
//===----------------------------------------------------------------------===//
enum class DirMapColumnIdx : idx_t {
	DIR_FID = 0,
	PARENT_FID = 1,
	DIR_DEVICE = 2,
	PARENT_DEVICE = 3,
	MASTER_MDT_INDEX = 4,
	STRIPE_INDEX = 5,
	STRIPE_COUNT = 6,
	HASH_TYPE = 7,
	LAYOUT_VERSION = 8,
	SOURCE = 9,
	LMA_INCOMPAT = 10
};

//===----------------------------------------------------------------------===//
// Global State for lustre_dirmap
//===----------------------------------------------------------------------===//
struct LustreDirMapGlobalState : public GlobalTableFunctionState {
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

	//! Buffered results (multiple rows per directory for striped dirs)
	struct PendingRow {
		LustreFID dir_fid;
		LustreFID parent_fid;
		std::string dir_device;
		std::string parent_device;
		uint32_t master_mdt_index;
		uint32_t stripe_index;
		uint32_t stripe_count;
		uint32_t hash_type;
		uint32_t layout_version;
		std::string source;
		uint32_t lma_incompat;
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
// Local State for lustre_dirmap (per-thread)
//===----------------------------------------------------------------------===//
struct LustreDirMapLocalState : public LocalTableFunctionState {
	unique_ptr<MDTScanner> scanner;
	bool scanner_initialized = false;
	idx_t initialized_device_idx = DConstants::INVALID_INDEX;
	string initialized_device_path;
	bool sequential_scan_mode = false;
	bool block_group_active = false;
	ext2_ino_t block_group_max_ino = 0;
	int next_block_group_in_batch = 0;
	int block_group_batch_end = 0;

	vector<LustreDirMapGlobalState::PendingRow> pending_results;
	idx_t pending_results_idx = 0;

	//! Cross-MDT OI resolution scanners (following lustre_fid2path.cpp pattern)
	vector<unique_ptr<MDTScanner>> resolve_scanners;
	vector<string> resolve_device_paths;
	bool resolve_initialized = false;

	LustreDirMapLocalState() {
		scanner = make_uniq<MDTScanner>();
	}

	void EnsureResolveInitialized(const vector<string> &device_paths) {
		if (resolve_initialized) {
			return;
		}
		resolve_scanners.resize(device_paths.size());
		resolve_device_paths = device_paths;
		for (idx_t i = 0; i < device_paths.size(); i++) {
			resolve_scanners[i] = make_uniq<MDTScanner>();
			resolve_scanners[i]->Open(device_paths[i]);
			resolve_scanners[i]->InitOI();
		}
		resolve_initialized = true;
	}

	bool ResolveFIDToDevice(const LustreFID &fid, string &device_out) {
		for (idx_t i = 0; i < resolve_scanners.size(); i++) {
			ext2_ino_t ino;
			if (resolve_scanners[i]->LookupFID(fid, ino)) {
				device_out = resolve_device_paths[i];
				return true;
			}
		}
		device_out.clear();
		return false;
	}

	bool IsFIDReachable(const LustreFID &fid) {
		for (auto &s : resolve_scanners) {
			ext2_ino_t ino;
			if (s->LookupFID(fid, ino)) {
				return true;
			}
		}
		return false;
	}
};

//===----------------------------------------------------------------------===//
// lustre_dirmap Table Function
//===----------------------------------------------------------------------===//
class LustreDirMapFunction {
public:
	static TableFunctionSet GetFunctionSet();
};

} // namespace lustre
} // namespace duckdb
