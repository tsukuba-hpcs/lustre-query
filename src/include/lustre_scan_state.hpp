//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_scan_state.hpp
//
// Shared scan state and bind data for lustre_inodes / lustre_links
//===----------------------------------------------------------------------===//

#pragma once

#include "lustre_types.hpp"
#include "lustre_filter.hpp"
#include "mdt_scanner.hpp"

#include "duckdb/function/table_function.hpp"

namespace duckdb {
namespace lustre {

//===----------------------------------------------------------------------===//
// Bind Data - immutable configuration for the scan (shared by both functions)
//===----------------------------------------------------------------------===//
struct LustreQueryBindData : public TableFunctionData {
	//! Device path(s) to scan
	vector<string> device_paths;

	//! Scan configuration options
	MDTScanConfig scan_config;

	//! Estimated cardinality (sum of used inodes across all devices)
	idx_t estimated_cardinality = 0;

	//! Internal fused inode/link join mode
	bool inode_link_join_on_parent_fid = false;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;
};

//===----------------------------------------------------------------------===//
// Global State - shared across all threads (base for both scan functions)
//===----------------------------------------------------------------------===//
struct LustreQueryGlobalState : public GlobalTableFunctionState {
	//! Constructor
	LustreQueryGlobalState(const vector<string> &device_paths, const vector<idx_t> &column_ids,
	                       const MDTScanConfig &scan_config);

	//! Destructor
	~LustreQueryGlobalState() override;

	//! Maximum number of threads
	idx_t MaxThreads() const override;

	//! Initialize device metadata (block group count, inodes per group)
	bool InitializeDevice(idx_t device_idx);

	//! Current device index (for multi-device scanning)
	atomic<idx_t> current_device_idx;

	//! Total inodes scanned (across all devices)
	atomic<uint64_t> total_scanned;

	//! Inodes scanned on the current device (reset per device)
	atomic<uint64_t> current_device_scanned;

	//! Total inodes returned
	atomic<uint64_t> total_returned;

	//! Scan finished flag
	atomic<bool> finished;

	//! Device paths
	vector<string> device_paths;

	//! Current device initialized
	atomic<bool> device_initialized;

	//! Mutex for device transitions only
	mutex device_transition_lock;

	//! Current device path (set during device initialization)
	string current_device_path;

	//! Column IDs to output (for projection pushdown)
	vector<idx_t> column_ids;

	//! Compiled filters for filter pushdown
	unique_ptr<LustreFilterSet> filters;

	//! Scan configuration
	MDTScanConfig scan_config;

	//! FID values extracted from WHERE clause for OI lookup
	vector<LustreFID> fid_filter_values;

	//! Whether to use OI lookup path instead of sequential scan
	bool use_oi_lookup = false;

	//! Next FID index to process (OI lookup mode, atomic for parallel access)
	atomic<idx_t> next_fid_idx;

	//! Next block group to scan (sequential scan mode, atomic for parallel access)
	atomic<int> next_block_group;

	//! Number of claimed block groups that have not been fully drained yet
	atomic<int> active_block_groups;

	//! Total block groups on the current device
	int total_block_groups = 0;

	//! Inodes per block group (from superblock)
	uint32_t inodes_per_group = 0;

	//! Number of threads to use
	idx_t thread_count = 1;
};

//===----------------------------------------------------------------------===//
// Shared Helper Functions
//===----------------------------------------------------------------------===//

//! Parse named parameters (skip_no_fid, skip_no_linkea) into scan_config
void ParseNamedParameters(const named_parameter_map_t &named_parameters, MDTScanConfig &scan_config);

//! Whether the scan must materialize a specific column, considering filters too.
bool ScanNeedsColumn(const TableFunctionInitInput &input, idx_t actual_column_idx);

//! Estimate cardinality by probing superblocks
void EstimateCardinality(LustreQueryBindData &bind_data);

//! Progress function shared by both lustre_inodes and lustre_links
double LustreScanProgress(ClientContext &context, const FunctionData *bind_data_p,
                          const GlobalTableFunctionState *global_state_p);

} // namespace lustre
} // namespace duckdb
