//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_inodes.cpp
//
// Table function implementation for querying Lustre MDT inodes
//===----------------------------------------------------------------------===//

#include "lustre_inodes.hpp"
#include "mdt_scanner.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {
namespace lustre {

//===----------------------------------------------------------------------===//
// Column Definitions
//===----------------------------------------------------------------------===//

static const vector<string> COLUMN_NAMES = {
    "fid",           // Lustre FID
    "ino",           // Inode number
    "type",          // File type (file/dir/link/...)
    "mode",          // File mode (permissions)
    "nlink",         // Hard link count
    "uid",           // User ID
    "gid",           // Group ID
    "size",          // File size
    "blocks",        // Block count
    "atime",         // Access time
    "mtime",         // Modification time
    "ctime",         // Change time
    "projid",        // Project ID
    "flags",         // Inode flags
    "device"         // Source device path
};

static const vector<LogicalType> COLUMN_TYPES = {
    LogicalType::VARCHAR,     // fid
    LogicalType::UBIGINT,     // ino
    LogicalType::VARCHAR,     // type
    LogicalType::UINTEGER,    // mode
    LogicalType::UINTEGER,    // nlink
    LogicalType::UINTEGER,    // uid
    LogicalType::UINTEGER,    // gid
    LogicalType::UBIGINT,     // size
    LogicalType::UBIGINT,     // blocks
    LogicalType::TIMESTAMP,   // atime
    LogicalType::TIMESTAMP,   // mtime
    LogicalType::TIMESTAMP,   // ctime
    LogicalType::UINTEGER,    // projid
    LogicalType::UINTEGER,    // flags
    LogicalType::VARCHAR      // device
};

const vector<string> &LustreInodesFunction::GetColumnNames() {
	return COLUMN_NAMES;
}

const vector<LogicalType> &LustreInodesFunction::GetColumnTypes() {
	return COLUMN_TYPES;
}

//===----------------------------------------------------------------------===//
// FID Filter Extraction (for OI lookup pushdown)
//===----------------------------------------------------------------------===//

static void ExtractFIDFilterValues(idx_t actual_col, const TableFilter &filter,
                                   vector<LustreFID> &fid_values) {
	// Only process fid column (column 0)
	if (actual_col != static_cast<idx_t>(LustreColumnIdx::FID)) {
		return;
	}

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
			ExtractFIDFilterValues(actual_col, *child, fid_values);
		}
		break;
	}

	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		if (optional_filter.child_filter) {
			ExtractFIDFilterValues(actual_col, *optional_filter.child_filter, fid_values);
		}
		break;
	}

	case TableFilterType::DYNAMIC_FILTER: {
		auto &dynamic_filter = filter.Cast<DynamicFilter>();
		if (dynamic_filter.filter_data) {
			lock_guard<mutex> lock(dynamic_filter.filter_data->lock);
			if (dynamic_filter.filter_data->initialized && dynamic_filter.filter_data->filter) {
				ExtractFIDFilterValues(actual_col, *dynamic_filter.filter_data->filter, fid_values);
			}
		}
		break;
	}

	default:
		break;
	}
}

static bool InodeColumnRequiresXattrs(idx_t actual_column_idx) {
	switch (static_cast<LustreColumnIdx>(actual_column_idx)) {
	case LustreColumnIdx::FID:
	case LustreColumnIdx::SIZE:
	case LustreColumnIdx::BLOCKS:
		return true;
	default:
		return false;
	}
}

static bool InodeScanRequiresXattrs(const TableFunctionInitInput &input, const MDTScanConfig &scan_config) {
	for (auto actual_column_idx : input.column_ids) {
		if (InodeColumnRequiresXattrs(actual_column_idx)) {
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
		if (InodeColumnRequiresXattrs(input.column_ids[filter_col_idx])) {
			return true;
		}
	}

	return false;
}

//===----------------------------------------------------------------------===//
// Bind Function
//===----------------------------------------------------------------------===//

static unique_ptr<FunctionData> LustreInodesBindSingle(ClientContext &context,
                                                      TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types,
                                                      vector<string> &names) {
	auto result = make_uniq<LustreQueryBindData>();

	// Single device path (VARCHAR overload)
	result->device_paths.push_back(StringValue::Get(input.inputs[0]));

	// Read named parameters
	ParseNamedParameters(input.named_parameters, result->scan_config);

	// Estimate cardinality from superblock
	EstimateCardinality(*result);

	// Set return schema
	names = LustreInodesFunction::GetColumnNames();
	return_types = LustreInodesFunction::GetColumnTypes();

	return std::move(result);
}

static unique_ptr<FunctionData> LustreInodesBindMulti(ClientContext &context,
                                                     TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types,
                                                     vector<string> &names) {
	auto result = make_uniq<LustreQueryBindData>();

	// Multiple device paths (LIST(VARCHAR) overload)
	auto &list_values = ListValue::GetChildren(input.inputs[0]);
	for (auto &val : list_values) {
		result->device_paths.push_back(StringValue::Get(val));
	}

	if (result->device_paths.empty()) {
		throw BinderException("lustre_inodes requires at least one device path");
	}

	// Read named parameters
	ParseNamedParameters(input.named_parameters, result->scan_config);

	// Estimate cardinality from superblock
	EstimateCardinality(*result);

	// Set return schema
	names = LustreInodesFunction::GetColumnNames();
	return_types = LustreInodesFunction::GetColumnTypes();

	return std::move(result);
}

//===----------------------------------------------------------------------===//
// Init Functions
//===----------------------------------------------------------------------===//

static unique_ptr<GlobalTableFunctionState> LustreInodesInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<LustreQueryBindData>();
	auto result = make_uniq<LustreQueryGlobalState>(bind_data.device_paths,
	                                                input.column_ids, bind_data.scan_config);
	result->scan_config.read_xattrs = InodeScanRequiresXattrs(input, result->scan_config);

	// Compile filters for pushdown
	if (input.filters) {
		result->filters = LustreFilterSet::Create(input.filters.get(), input.column_ids);

		// Extract FID EQUAL/IN values for OI lookup pushdown
		for (const auto &entry : input.filters->filters) {
			idx_t filter_col_idx = entry.first;
			if (filter_col_idx >= input.column_ids.size()) {
				continue;
			}
			idx_t actual_column_idx = input.column_ids[filter_col_idx];
			ExtractFIDFilterValues(actual_column_idx, *entry.second, result->fid_filter_values);
		}

		if (!result->fid_filter_values.empty()) {
			result->use_oi_lookup = true;
		}
	}

	// Set thread count from scheduler
	result->thread_count = NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());

	// Pre-initialize first device metadata
	if (!result->device_paths.empty()) {
		result->InitializeDevice(0);
	}

	return std::move(result);
}

static unique_ptr<LocalTableFunctionState> LustreInodesInitLocal(ExecutionContext &context,
                                                                TableFunctionInitInput &input,
                                                                GlobalTableFunctionState *global_state) {
	return make_uniq<LustreQueryLocalState>();
}

//===----------------------------------------------------------------------===//
// Helper: Check if inode passes filters
//===----------------------------------------------------------------------===//

static bool PassesFilters(const LustreInode &inode, const LustreQueryGlobalState &gstate) {
	if (!gstate.filters || !gstate.filters->HasFilters()) {
		return true;
	}
	return gstate.filters->Evaluate(inode);
}

//===----------------------------------------------------------------------===//
// Helper: Write a timestamp to a vector (NULL if zero)
//===----------------------------------------------------------------------===//

static void WriteTimestamp(Vector &vec, idx_t row_idx, int64_t unix_timestamp) {
	if (unix_timestamp == 0) {
		FlatVector::SetNull(vec, row_idx, true);
	} else {
		FlatVector::GetData<timestamp_t>(vec)[row_idx] = Timestamp::FromEpochSeconds(unix_timestamp);
	}
}

//===----------------------------------------------------------------------===//
// Helper: Write a single column value for one inode directly into the vector
//===----------------------------------------------------------------------===//

static void WriteOutputColumn(Vector &vec, idx_t col_idx, idx_t row_idx,
                              const LustreInode &inode, const string &device_path) {
	switch (col_idx) {
	case 0: { // fid (VARCHAR)
		if (inode.fid.IsValid()) {
			auto fid_str = inode.fid.ToString();
			FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, fid_str);
		} else {
			FlatVector::SetNull(vec, row_idx, true);
		}
		break;
	}
	case 1: // ino (UBIGINT)
		FlatVector::GetData<uint64_t>(vec)[row_idx] = inode.ino;
		break;
	case 2: { // type (VARCHAR)
		auto type_str = FileTypeToString(inode.type);
		FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, type_str);
		break;
	}
	case 3: // mode (UINTEGER)
		FlatVector::GetData<uint32_t>(vec)[row_idx] = inode.mode;
		break;
	case 4: // nlink (UINTEGER)
		FlatVector::GetData<uint32_t>(vec)[row_idx] = inode.nlink;
		break;
	case 5: // uid (UINTEGER)
		FlatVector::GetData<uint32_t>(vec)[row_idx] = inode.uid;
		break;
	case 6: // gid (UINTEGER)
		FlatVector::GetData<uint32_t>(vec)[row_idx] = inode.gid;
		break;
	case 7: // size (UBIGINT)
		FlatVector::GetData<uint64_t>(vec)[row_idx] = inode.size;
		break;
	case 8: // blocks (UBIGINT)
		FlatVector::GetData<uint64_t>(vec)[row_idx] = inode.blocks;
		break;
	case 9: // atime (TIMESTAMP)
		WriteTimestamp(vec, row_idx, inode.atime);
		break;
	case 10: // mtime (TIMESTAMP)
		WriteTimestamp(vec, row_idx, inode.mtime);
		break;
	case 11: // ctime (TIMESTAMP)
		WriteTimestamp(vec, row_idx, inode.ctime);
		break;
	case 12: // projid (UINTEGER)
		FlatVector::GetData<uint32_t>(vec)[row_idx] = inode.projid;
		break;
	case 13: // flags (UINTEGER)
		FlatVector::GetData<uint32_t>(vec)[row_idx] = inode.flags;
		break;
	case 14: { // device (VARCHAR)
		FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, device_path);
		break;
	}
	default:
		break;
	}
}

static void ResetBlockGroupState(LustreQueryLocalState &lstate) {
	lstate.block_group_active = false;
	lstate.block_group_max_ino = 0;
	lstate.next_block_group_in_batch = 0;
	lstate.block_group_batch_end = 0;
	lstate.pending_scanned = 0;
	lstate.pending_returned = 0;
}

static void FlushPendingStats(LustreQueryGlobalState &gstate, LustreQueryLocalState &lstate) {
	if (lstate.pending_scanned != 0) {
		gstate.total_scanned.fetch_add(lstate.pending_scanned);
		gstate.current_device_scanned.fetch_add(lstate.pending_scanned);
		lstate.pending_scanned = 0;
	}
	if (lstate.pending_returned != 0) {
		gstate.total_returned.fetch_add(lstate.pending_returned);
		lstate.pending_returned = 0;
	}
}

static bool ClaimNextBlockGroup(LustreQueryGlobalState &gstate, LustreQueryLocalState &lstate) {
	static constexpr int BLOCK_GROUP_BATCH_SIZE = 8;

	if (lstate.next_block_group_in_batch >= lstate.block_group_batch_end) {
		int bg_start = gstate.next_block_group.fetch_add(BLOCK_GROUP_BATCH_SIZE);
		if (bg_start >= gstate.total_block_groups) {
			return false;
		}

		int bg_end = bg_start + BLOCK_GROUP_BATCH_SIZE;
		if (bg_end > gstate.total_block_groups) {
			bg_end = gstate.total_block_groups;
		}

		gstate.active_block_groups.fetch_add(bg_end - bg_start);
		lstate.next_block_group_in_batch = bg_start;
		lstate.block_group_batch_end = bg_end;
	}

	int bg = lstate.next_block_group_in_batch++;
	lstate.scanner->GotoBlockGroup(bg);
	lstate.block_group_max_ino = static_cast<ext2_ino_t>(bg + 1) * gstate.inodes_per_group;
	lstate.block_group_active = true;
	return true;
}

//===----------------------------------------------------------------------===//
// Helper: Ensure local scanner is initialized for the current device
//===----------------------------------------------------------------------===//

static bool EnsureLocalScanner(LustreQueryGlobalState &gstate, LustreQueryLocalState &lstate,
                               bool init_oi = false) {
	idx_t dev_idx = gstate.current_device_idx.load();

	if (lstate.scanner_initialized && lstate.initialized_device_idx == dev_idx) {
		return true;  // Already set up for the right device
	}

	// Close old scanner if from a different device
	if (lstate.scanner_initialized) {
		lstate.scanner->CloseScan();
		lstate.scanner->Close();
		lstate.scanner_initialized = false;
		ResetBlockGroupState(lstate);
		lstate.initialized_device_path.clear();
	}

	if (dev_idx >= gstate.device_paths.size() || gstate.finished.load()) {
		return false;
	}

	// Ensure device metadata is initialized (one thread does this)
	if (!gstate.device_initialized.load()) {
		lock_guard<mutex> lock(gstate.device_transition_lock);
		if (!gstate.device_initialized.load()) {
			if (!gstate.InitializeDevice(dev_idx)) {
				return false;
			}
		}
	}

	lstate.scanner->Open(gstate.current_device_path);
	if (init_oi) {
		lstate.scanner->InitOI();
	} else {
		lstate.scanner->StartScan();
	}
	lstate.scanner_initialized = true;
	lstate.initialized_device_idx = dev_idx;
	lstate.initialized_device_path = gstate.current_device_path;
	ResetBlockGroupState(lstate);
	return true;
}

//===----------------------------------------------------------------------===//
// OI Lookup Execution Path (FID filter pushdown) - parallel by FID
//===----------------------------------------------------------------------===//

static void LustreInodesExecuteOILookup(LustreQueryGlobalState &gstate,
                                         LustreQueryLocalState &lstate,
                                         DataChunk &output) {
	idx_t output_count = 0;

	while (output_count < STANDARD_VECTOR_SIZE) {
		if (!EnsureLocalScanner(gstate, lstate, true)) {
			break;
		}

		const auto &current_device = lstate.initialized_device_path;

		// Atomically claim next FID
		idx_t fid_idx = gstate.next_fid_idx.fetch_add(1);
		if (fid_idx >= gstate.fid_filter_values.size()) {
			// No more FIDs — advance to next device
			lock_guard<mutex> lock(gstate.device_transition_lock);
			idx_t current_dev = gstate.current_device_idx.load();
			if (lstate.initialized_device_idx == current_dev) {
				gstate.device_initialized.store(false);
				gstate.current_device_idx++;
				gstate.next_fid_idx.store(0);

				if (gstate.current_device_idx >= gstate.device_paths.size()) {
					gstate.finished = true;
				}
			}
			lstate.scanner_initialized = false;
			lstate.initialized_device_path.clear();
			continue;
		}

		auto &fid = gstate.fid_filter_values[fid_idx];

		// Lookup FID -> inode number via OI B-tree
		ext2_ino_t ino;
		if (!lstate.scanner->LookupFID(fid, ino)) {
			continue;
		}

		// Read full inode metadata
		LustreInode inode;
		if (!lstate.scanner->ReadInode(ino, inode, gstate.scan_config)) {
			continue;
		}

		// Write directly into output vectors
		for (idx_t i = 0; i < gstate.column_ids.size(); i++) {
			WriteOutputColumn(output.data[i], gstate.column_ids[i], output_count,
			                  inode, current_device);
		}
		output_count++;
	}

	if (output_count != 0) {
		gstate.total_returned.fetch_add(output_count);
	}
	output.SetCardinality(output_count);
}

//===----------------------------------------------------------------------===//
// Sequential Scan Execution Path - parallel by block group
//===----------------------------------------------------------------------===//

static void LustreInodesExecuteSeqScan(LustreQueryGlobalState &gstate,
                                        LustreQueryLocalState &lstate,
                                        DataChunk &output) {
	idx_t output_count = 0;
	LustreInode inode;

	while (output_count < STANDARD_VECTOR_SIZE) {
		if (!EnsureLocalScanner(gstate, lstate)) {
			break;
		}

		const auto &current_device = lstate.initialized_device_path;

		if (!lstate.block_group_active) {
			if (!ClaimNextBlockGroup(gstate, lstate)) {
				// Wait until every claimed block group on this device has been fully drained.
				if (gstate.active_block_groups.load() != 0) {
					break;
				}

				// No more block groups on this device — advance to next device
				lock_guard<mutex> lock(gstate.device_transition_lock);
				idx_t current_dev = gstate.current_device_idx.load();
				if (lstate.initialized_device_idx == current_dev &&
				    gstate.next_block_group.load() >= gstate.total_block_groups &&
				    gstate.active_block_groups.load() == 0) {
					gstate.device_initialized.store(false);
					gstate.current_device_idx++;

					if (gstate.current_device_idx >= gstate.device_paths.size()) {
						gstate.finished = true;
					}
				}
				lstate.scanner_initialized = false;
				ResetBlockGroupState(lstate);
				lstate.initialized_device_path.clear();
				continue;
			}
		}

		// Scan all inodes within this block group
		while (output_count < STANDARD_VECTOR_SIZE) {
			if (!lstate.scanner->GetNextInode(inode, gstate.scan_config, lstate.block_group_max_ino)) {
				lstate.block_group_active = false;
				gstate.active_block_groups.fetch_sub(1);
				FlushPendingStats(gstate, lstate);
				break;  // End of this block group
			}

			lstate.pending_scanned++;

			// Apply filters
			if (!PassesFilters(inode, gstate)) {
				continue;
			}

			// Write directly into output vectors
			for (idx_t i = 0; i < gstate.column_ids.size(); i++) {
				WriteOutputColumn(output.data[i], gstate.column_ids[i], output_count,
				                  inode, current_device);
			}
			output_count++;
			lstate.pending_returned++;
		}
	}

	output.SetCardinality(output_count);
}

static void LustreInodesExecute(ClientContext &context, TableFunctionInput &data_p,
                              DataChunk &output) {
	auto &gstate = data_p.global_state->Cast<LustreQueryGlobalState>();
	auto &lstate = data_p.local_state->Cast<LustreQueryLocalState>();

	if (gstate.finished) {
		output.SetCardinality(0);
		return;
	}

	if (gstate.use_oi_lookup) {
		LustreInodesExecuteOILookup(gstate, lstate, output);
	} else {
		LustreInodesExecuteSeqScan(gstate, lstate, output);
	}
}

//===----------------------------------------------------------------------===//
// Cardinality Function
//===----------------------------------------------------------------------===//

static unique_ptr<NodeStatistics> LustreInodesCardinality(ClientContext &context,
                                                         const FunctionData *bind_data_p) {
	if (!bind_data_p) {
		return nullptr;
	}
	auto &bind_data = bind_data_p->Cast<LustreQueryBindData>();
	if (bind_data.estimated_cardinality == 0) {
		return nullptr;
	}
	return make_uniq<NodeStatistics>(bind_data.estimated_cardinality);
}

//===----------------------------------------------------------------------===//
// GetFunctionSet
//===----------------------------------------------------------------------===//

static void SetCommonProperties(TableFunction &func) {
	func.named_parameters["skip_no_fid"] = LogicalType::BOOLEAN;
	func.named_parameters["skip_no_linkea"] = LogicalType::BOOLEAN;
	func.projection_pushdown = true;
	func.filter_pushdown = true;
	func.table_scan_progress = LustreScanProgress;
	func.cardinality = LustreInodesCardinality;
}

TableFunctionSet LustreInodesFunction::GetFunctionSet() {
	TableFunctionSet set("lustre_inodes");

	// Overload 1: single device path (VARCHAR)
	TableFunction single_func("lustre_inodes", {LogicalType::VARCHAR}, LustreInodesExecute,
	                          LustreInodesBindSingle, LustreInodesInitGlobal, LustreInodesInitLocal);
	SetCommonProperties(single_func);
	set.AddFunction(std::move(single_func));

	// Overload 2: multiple device paths (LIST(VARCHAR))
	TableFunction multi_func("lustre_inodes", {LogicalType::LIST(LogicalType::VARCHAR)}, LustreInodesExecute,
	                         LustreInodesBindMulti, LustreInodesInitGlobal, LustreInodesInitLocal);
	SetCommonProperties(multi_func);
	set.AddFunction(std::move(multi_func));

	return set;
}

} // namespace lustre
} // namespace duckdb
