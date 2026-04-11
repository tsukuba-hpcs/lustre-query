//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_scan_state.cpp
//
// Shared scan state and bind data implementation
//===----------------------------------------------------------------------===//

#include "lustre_scan_state.hpp"
#include "mdt_scanner.hpp"

#include "duckdb/main/client_context.hpp"

namespace duckdb {
namespace lustre {

//===----------------------------------------------------------------------===//
// LustreQueryBindData
//===----------------------------------------------------------------------===//

unique_ptr<FunctionData> LustreQueryBindData::Copy() const {
	auto result = make_uniq<LustreQueryBindData>();
	result->device_paths = device_paths;
	result->scan_config = scan_config;
	result->estimated_cardinality = estimated_cardinality;
	result->inode_link_join_on_parent_fid = inode_link_join_on_parent_fid;
	return std::move(result);
}

bool LustreQueryBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<LustreQueryBindData>();
	return device_paths == other.device_paths && scan_config.skip_no_fid == other.scan_config.skip_no_fid &&
	       scan_config.skip_no_linkea == other.scan_config.skip_no_linkea &&
	       scan_config.read_link_names == other.scan_config.read_link_names &&
	       inode_link_join_on_parent_fid == other.inode_link_join_on_parent_fid;
}

//===----------------------------------------------------------------------===//
// LustreQueryGlobalState
//===----------------------------------------------------------------------===//

LustreQueryGlobalState::LustreQueryGlobalState(const vector<string> &paths, const vector<idx_t> &cols,
                                               const MDTScanConfig &config)
    : current_device_idx(0), total_scanned(0), current_device_scanned(0), total_returned(0), finished(false),
      device_paths(paths), device_initialized(false), column_ids(cols), scan_config(config), next_fid_idx(0),
      next_block_group(0), active_block_groups(0) {
}

LustreQueryGlobalState::~LustreQueryGlobalState() {
}

idx_t LustreQueryGlobalState::MaxThreads() const {
	if (use_oi_lookup) {
		return MinValue<idx_t>(fid_filter_values.size(), thread_count);
	}
	return MinValue<idx_t>(static_cast<idx_t>(total_block_groups), thread_count);
}

bool LustreQueryGlobalState::InitializeDevice(idx_t device_idx) {
	if (device_idx >= device_paths.size()) {
		finished = true;
		return false;
	}

	// Open a temporary scanner to read superblock metadata
	MDTScanner probe;
	probe.Open(device_paths[device_idx]);
	total_block_groups = static_cast<int>(probe.GetBlockGroupCount());
	inodes_per_group = probe.GetInodesPerGroup();
	current_device_path = device_paths[device_idx];
	next_block_group.store(0);
	active_block_groups.store(0);
	current_device_scanned.store(0);
	probe.Close();
	device_initialized = true;
	return true;
}

//===----------------------------------------------------------------------===//
// Shared Helper Functions
//===----------------------------------------------------------------------===//

void ParseNamedParameters(const named_parameter_map_t &named_parameters, MDTScanConfig &scan_config) {
	for (auto &entry : named_parameters) {
		if (entry.first == "skip_no_fid") {
			scan_config.skip_no_fid = BooleanValue::Get(entry.second);
		} else if (entry.first == "skip_no_linkea") {
			scan_config.skip_no_linkea = BooleanValue::Get(entry.second);
		}
	}
}

bool ScanNeedsColumn(const TableFunctionInitInput &input, idx_t actual_column_idx) {
	for (auto projected_column_idx : input.column_ids) {
		if (projected_column_idx == actual_column_idx) {
			return true;
		}
	}

	if (!input.filters) {
		return false;
	}

	for (const auto &entry : input.filters->filters) {
		idx_t filter_col_idx = entry.first;
		if (filter_col_idx >= input.column_ids.size()) {
			return true;
		}
		if (input.column_ids[filter_col_idx] == actual_column_idx) {
			return true;
		}
	}

	return false;
}

void EstimateCardinality(LustreQueryBindData &bind_data) {
	idx_t total_used = 0;
	MDTScanner probe;
	for (auto &device_path : bind_data.device_paths) {
		try {
			probe.Open(device_path);
			total_used += probe.GetUsedInodes();
			probe.Close();
		} catch (...) {
			// If we can't probe a device, skip it for estimation
		}
	}
	bind_data.estimated_cardinality = total_used;
}

double LustreScanProgress(ClientContext &context, const FunctionData *bind_data_p,
                          const GlobalTableFunctionState *global_state_p) {
	auto &global_state = global_state_p->Cast<LustreQueryGlobalState>();

	if (global_state.finished) {
		return 1.0;
	}

	if (global_state.total_block_groups == 0) {
		return 0.0;
	}

	// Account for multiple devices
	double device_progress =
	    static_cast<double>(global_state.current_device_idx.load()) / global_state.device_paths.size();
	double current_device_progress =
	    static_cast<double>(global_state.next_block_group.load()) / global_state.total_block_groups;
	double per_device = 1.0 / global_state.device_paths.size();

	return device_progress + (current_device_progress * per_device);
}

} // namespace lustre
} // namespace duckdb
