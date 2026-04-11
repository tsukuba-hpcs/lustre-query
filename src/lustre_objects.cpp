//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_objects.cpp
//
// Table function implementation for querying Lustre MDT per-stripe OST object
// placement. Requires a WHERE clause with fid filter; uses OI lookup.
//===----------------------------------------------------------------------===//

#include "lustre_objects.hpp"
#include "mdt_scanner.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {
namespace lustre {

static constexpr idx_t OBJECT_EXACT_LOOKUP_BATCH_SIZE = 512;
static constexpr idx_t OBJECT_SEQ_SCAN_THRESHOLD = 8192;

//===----------------------------------------------------------------------===//
// Column Definitions
//===----------------------------------------------------------------------===//

static const vector<string> OBJECT_COLUMN_NAMES = {"fid",       "comp_index", "stripe_index", "ost_idx",
                                                   "ost_oi_id", "ost_oi_seq", "device"};

static const vector<LogicalType> OBJECT_COLUMN_TYPES = {
    LogicalType::VARCHAR,  // fid
    LogicalType::UINTEGER, // comp_index
    LogicalType::UINTEGER, // stripe_index
    LogicalType::UINTEGER, // ost_idx
    LogicalType::UBIGINT,  // ost_oi_id
    LogicalType::UBIGINT,  // ost_oi_seq
    LogicalType::VARCHAR   // device
};

//===----------------------------------------------------------------------===//
// Bind Functions
//===----------------------------------------------------------------------===//

static unique_ptr<FunctionData> LustreObjectsBindSingle(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<LustreQueryBindData>();
	result->device_paths.push_back(StringValue::Get(input.inputs[0]));
	ParseNamedParameters(input.named_parameters, result->scan_config);
	names = OBJECT_COLUMN_NAMES;
	return_types = OBJECT_COLUMN_TYPES;
	return std::move(result);
}

static unique_ptr<FunctionData> LustreObjectsBindMulti(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<LustreQueryBindData>();
	auto &list_values = ListValue::GetChildren(input.inputs[0]);
	for (auto &val : list_values) {
		result->device_paths.push_back(StringValue::Get(val));
	}
	if (result->device_paths.empty()) {
		throw BinderException("lustre_objects requires at least one device path");
	}
	ParseNamedParameters(input.named_parameters, result->scan_config);
	names = OBJECT_COLUMN_NAMES;
	return_types = OBJECT_COLUMN_TYPES;
	return std::move(result);
}

//===----------------------------------------------------------------------===//
// Init Functions
//===----------------------------------------------------------------------===//

static unique_ptr<GlobalTableFunctionState> LustreObjectsInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<LustreQueryBindData>();

	auto gstate = make_uniq<LustreObjectsGlobalState>();
	gstate->device_paths = bind_data.device_paths;
	gstate->column_ids = input.column_ids;
	gstate->scan_config = bind_data.scan_config;
	gstate->current_device_idx.store(0);
	gstate->finished.store(false);
	gstate->device_initialized.store(false);
	gstate->next_fid_idx.store(0);
	gstate->next_block_group.store(0);
	gstate->active_block_groups.store(0);

	// Set thread count from scheduler
	gstate->thread_count = NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());

	gstate->fid_filter =
	    FIDOnlyFilter::Create(input.filters.get(), input.column_ids, static_cast<idx_t>(ObjectColumnIdx::FID));
	gstate->use_sequential_scan =
	    gstate->fid_filter && (gstate->fid_filter->RequiresGenericEvaluation() || !gstate->fid_filter->HasFIDFilter() ||
	                           gstate->fid_filter->fid_values.size() > OBJECT_SEQ_SCAN_THRESHOLD);
	if (gstate->use_sequential_scan && !gstate->device_paths.empty()) {
		MDTScanner probe;
		probe.Open(gstate->device_paths[0]);
		gstate->current_device_path = gstate->device_paths[0];
		gstate->total_block_groups = static_cast<int>(probe.GetBlockGroupCount());
		gstate->inodes_per_group = probe.GetInodesPerGroup();
		probe.Close();
		gstate->device_initialized = true;
	}

	return std::move(gstate);
}

static unique_ptr<LocalTableFunctionState> LustreObjectsInitLocal(ExecutionContext &context,
                                                                  TableFunctionInitInput &input,
                                                                  GlobalTableFunctionState *global_state) {
	return make_uniq<LustreObjectsLocalState>();
}

//===----------------------------------------------------------------------===//
// Helper: Write an object output row
//===----------------------------------------------------------------------===//

static void WriteObjectRow(DataChunk &output, idx_t row_idx, const vector<idx_t> &column_ids, const LustreFID &fid,
                           const LustreOSTObject &obj, const string &device_path) {
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto &vec = output.data[i];
		switch (column_ids[i]) {
		case static_cast<idx_t>(ObjectColumnIdx::FID): {
			auto fid_str = fid.ToString();
			FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, fid_str);
			break;
		}
		case static_cast<idx_t>(ObjectColumnIdx::COMP_INDEX):
			FlatVector::GetData<uint32_t>(vec)[row_idx] = obj.comp_index;
			break;
		case static_cast<idx_t>(ObjectColumnIdx::STRIPE_INDEX):
			FlatVector::GetData<uint32_t>(vec)[row_idx] = obj.stripe_index;
			break;
		case static_cast<idx_t>(ObjectColumnIdx::OST_IDX):
			FlatVector::GetData<uint32_t>(vec)[row_idx] = obj.ost_idx;
			break;
		case static_cast<idx_t>(ObjectColumnIdx::OST_OI_ID):
			FlatVector::GetData<uint64_t>(vec)[row_idx] = obj.ost_oi_id;
			break;
		case static_cast<idx_t>(ObjectColumnIdx::OST_OI_SEQ):
			FlatVector::GetData<uint64_t>(vec)[row_idx] = obj.ost_oi_seq;
			break;
		case static_cast<idx_t>(ObjectColumnIdx::DEVICE):
			FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, device_path);
			break;
		default:
			break;
		}
	}
}

static void ResetObjectsBlockGroupState(LustreObjectsLocalState &lstate) {
	lstate.block_group_active = false;
	lstate.block_group_max_ino = 0;
	lstate.next_block_group_in_batch = 0;
	lstate.block_group_batch_end = 0;
}

//===----------------------------------------------------------------------===//
// Helper: Ensure local scanner is initialized for the current device
//===----------------------------------------------------------------------===//

static bool InitializeObjectsDevice(LustreObjectsGlobalState &gstate, idx_t dev_idx) {
	if (dev_idx >= gstate.device_paths.size()) {
		gstate.finished = true;
		return false;
	}

	gstate.current_device_path = gstate.device_paths[dev_idx];
	gstate.next_fid_idx.store(0);
	gstate.next_block_group.store(0);
	gstate.active_block_groups.store(0);

	if (!gstate.use_sequential_scan) {
		gstate.device_initialized = true;
		return true;
	}

	MDTScanner probe;
	probe.Open(gstate.current_device_path);
	gstate.total_block_groups = static_cast<int>(probe.GetBlockGroupCount());
	gstate.inodes_per_group = probe.GetInodesPerGroup();
	probe.Close();
	gstate.device_initialized = true;
	return true;
}

static bool EnsureObjectsLocalScanner(LustreObjectsGlobalState &gstate, LustreObjectsLocalState &lstate,
                                      bool sequential_scan) {
	idx_t dev_idx = gstate.current_device_idx.load();

	if (lstate.scanner_initialized && lstate.initialized_device_idx == dev_idx &&
	    lstate.sequential_scan_mode == sequential_scan) {
		return true;
	}

	if (lstate.scanner_initialized) {
		lstate.scanner->Close();
		lstate.scanner_initialized = false;
		ResetObjectsBlockGroupState(lstate);
		lstate.initialized_device_path.clear();
	}

	if (dev_idx >= gstate.device_paths.size() || gstate.finished.load()) {
		return false;
	}

	if (!gstate.device_initialized.load()) {
		lock_guard<mutex> lock(gstate.device_transition_lock);
		if (!gstate.device_initialized.load()) {
			if (!InitializeObjectsDevice(gstate, dev_idx)) {
				return false;
			}
		}
	}

	lstate.scanner->Open(gstate.current_device_path);
	if (sequential_scan) {
		lstate.scanner->StartScan();
	} else {
		lstate.scanner->InitOI();
	}
	lstate.scanner_initialized = true;
	lstate.initialized_device_idx = dev_idx;
	lstate.initialized_device_path = gstate.current_device_path;
	lstate.sequential_scan_mode = sequential_scan;
	ResetObjectsBlockGroupState(lstate);
	return true;
}

static bool ClaimNextObjectsBlockGroup(LustreObjectsGlobalState &gstate, LustreObjectsLocalState &lstate) {
	static constexpr int BLOCK_GROUP_BATCH_SIZE = 8;

	if (lstate.block_group_active) {
		return true;
	}
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
// Predicate Helpers
//===----------------------------------------------------------------------===//

static bool MatchesObjectFIDPredicate(const FIDOnlyFilter &filter, const LustreFID &fid) {
	if (!filter.ContainsFID(fid)) {
		return false;
	}
	if (filter.RequiresGenericEvaluation()) {
		return filter.EvaluateFID(fid);
	}
	return true;
}

//===----------------------------------------------------------------------===//
// FID Path: batched lookup by fid → OI → inode → LOV → object rows
//===----------------------------------------------------------------------===//

static bool ExecuteExactFIDPath(LustreObjectsGlobalState &gstate, LustreObjectsLocalState &lstate, DataChunk &output,
                                idx_t &output_count) {
	auto &filter = *gstate.fid_filter;
	const auto &current_device = lstate.initialized_device_path;
	struct LookupEntry {
		ext2_ino_t ino;
		LustreFID fid;
	};

	while (output_count < STANDARD_VECTOR_SIZE) {
		// Drain pending results first
		while (lstate.pending_results_idx < lstate.pending_results.size() && output_count < STANDARD_VECTOR_SIZE) {
			auto &row = lstate.pending_results[lstate.pending_results_idx];
			WriteObjectRow(output, output_count, gstate.column_ids, row.fid, row.object, current_device);
			lstate.pending_results_idx++;
			output_count++;
		}

		if (output_count >= STANDARD_VECTOR_SIZE) {
			return true;
		}

		vector<LookupEntry> lookup_batch;
		lookup_batch.reserve(OBJECT_EXACT_LOOKUP_BATCH_SIZE);

		while (lookup_batch.size() < OBJECT_EXACT_LOOKUP_BATCH_SIZE) {
			idx_t fid_idx = gstate.next_fid_idx.fetch_add(1);
			if (fid_idx >= filter.fid_values.size()) {
				break;
			}

			auto &fid = filter.fid_values[fid_idx];
			ext2_ino_t ino;
			if (!lstate.scanner->LookupFID(fid, ino)) {
				continue;
			}
			lookup_batch.push_back({ino, fid});
		}

		if (lookup_batch.empty()) {
			return false;
		}

		std::sort(lookup_batch.begin(), lookup_batch.end(), [](const LookupEntry &left, const LookupEntry &right) {
			if (left.ino != right.ino) {
				return left.ino < right.ino;
			}
			return left.fid < right.fid;
		});

		lstate.pending_results.clear();
		lstate.pending_results_idx = 0;
		ext2_ino_t last_ino = 0;
		bool have_last_ino = false;

		for (auto &entry : lookup_batch) {
			if (have_last_ino && entry.ino == last_ino) {
				continue;
			}
			have_last_ino = true;
			last_ino = entry.ino;

			LustreFID actual_fid;
			std::vector<LustreOSTObject> objects;
			if (!lstate.scanner->ReadInodeObjects(entry.ino, actual_fid, objects)) {
				continue;
			}

			for (auto &obj : objects) {
				LustreObjectsGlobalState::PendingRow row;
				row.fid = actual_fid;
				row.object = std::move(obj);
				lstate.pending_results.push_back(std::move(row));
			}
		}
	}

	return true;
}

//===----------------------------------------------------------------------===//
// Sequential Scan Path - linear scan by block group
//===----------------------------------------------------------------------===//

static bool ExecuteSequentialPath(LustreObjectsGlobalState &gstate, LustreObjectsLocalState &lstate, DataChunk &output,
                                  idx_t &output_count) {
	auto &filter = *gstate.fid_filter;
	const auto &current_device = lstate.initialized_device_path;

	while (output_count < STANDARD_VECTOR_SIZE) {
		while (lstate.pending_results_idx < lstate.pending_results.size() && output_count < STANDARD_VECTOR_SIZE) {
			auto &row = lstate.pending_results[lstate.pending_results_idx];
			WriteObjectRow(output, output_count, gstate.column_ids, row.fid, row.object, current_device);
			lstate.pending_results_idx++;
			output_count++;
		}

		if (output_count >= STANDARD_VECTOR_SIZE) {
			return true;
		}

		if (!ClaimNextObjectsBlockGroup(gstate, lstate)) {
			return false;
		}

		while (output_count < STANDARD_VECTOR_SIZE) {
			LustreFID fid;
			std::vector<LustreOSTObject> objects;
			if (!lstate.scanner->GetNextInodeObjects(fid, objects, gstate.scan_config, lstate.block_group_max_ino)) {
				lstate.block_group_active = false;
				gstate.active_block_groups.fetch_sub(1);
				break;
			}
			if (!MatchesObjectFIDPredicate(filter, fid)) {
				continue;
			}

			lstate.pending_results.clear();
			lstate.pending_results_idx = 0;
			for (auto &obj : objects) {
				LustreObjectsGlobalState::PendingRow row;
				row.fid = fid;
				row.object = std::move(obj);
				lstate.pending_results.push_back(std::move(row));
			}
			break;
		}
	}

	return true;
}

//===----------------------------------------------------------------------===//
// Execute Function
//===----------------------------------------------------------------------===//

static void LustreObjectsExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &gstate = data_p.global_state->Cast<LustreObjectsGlobalState>();
	auto &lstate = data_p.local_state->Cast<LustreObjectsLocalState>();

	if (gstate.fid_filter->HasDynamicFilter()) {
		bool changed = gstate.fid_filter->ResolveDynamicFilters();
		if (changed) {
			gstate.use_sequential_scan = gstate.fid_filter->RequiresGenericEvaluation() ||
			                             !gstate.fid_filter->HasFIDFilter() ||
			                             gstate.fid_filter->fid_values.size() > OBJECT_SEQ_SCAN_THRESHOLD;
			gstate.finished = false;
			gstate.next_fid_idx.store(0);
			gstate.next_block_group.store(0);
			gstate.active_block_groups.store(0);
			lstate.pending_results.clear();
			lstate.pending_results_idx = 0;
			gstate.current_device_idx = 0;
			gstate.device_initialized = false;
			lstate.scanner_initialized = false;
			ResetObjectsBlockGroupState(lstate);
			lstate.initialized_device_path.clear();
		}
	}

	if (gstate.finished) {
		output.SetCardinality(0);
		return;
	}

	idx_t output_count = 0;

	while (output_count < STANDARD_VECTOR_SIZE) {
		if (!EnsureObjectsLocalScanner(gstate, lstate, gstate.use_sequential_scan)) {
			break;
		}

		bool has_more = gstate.use_sequential_scan ? ExecuteSequentialPath(gstate, lstate, output, output_count)
		                                           : ExecuteExactFIDPath(gstate, lstate, output, output_count);

		if (!has_more) {
			if (gstate.use_sequential_scan && gstate.active_block_groups.load() != 0) {
				break;
			}
			lock_guard<mutex> lock(gstate.device_transition_lock);
			idx_t current_dev = gstate.current_device_idx.load();
			if (lstate.initialized_device_idx == current_dev &&
			    (!gstate.use_sequential_scan || (gstate.next_block_group.load() >= gstate.total_block_groups &&
			                                     gstate.active_block_groups.load() == 0))) {
				gstate.device_initialized.store(false);
				gstate.current_device_idx++;
				gstate.next_fid_idx.store(0);
				gstate.next_block_group.store(0);
				gstate.active_block_groups.store(0);

				if (gstate.current_device_idx >= gstate.device_paths.size()) {
					gstate.finished = true;
				}
			}
			lstate.scanner_initialized = false;
			ResetObjectsBlockGroupState(lstate);
			lstate.initialized_device_path.clear();
		}
	}

	output.SetCardinality(output_count);
}

//===----------------------------------------------------------------------===//
// Cardinality Function
//===----------------------------------------------------------------------===//

static unique_ptr<NodeStatistics> LustreObjectsCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	return make_uniq<NodeStatistics>(1000000000000);
}

//===----------------------------------------------------------------------===//
// GetFunctionSet
//===----------------------------------------------------------------------===//

static void SetObjectCommonProperties(TableFunction &func) {
	func.named_parameters["skip_no_fid"] = LogicalType::BOOLEAN;
	func.named_parameters["skip_no_linkea"] = LogicalType::BOOLEAN;
	func.projection_pushdown = true;
	func.filter_pushdown = true;
	func.cardinality = LustreObjectsCardinality;
}

TableFunctionSet LustreObjectsFunction::GetFunctionSet() {
	TableFunctionSet set("lustre_objects");

	TableFunction single_func("lustre_objects", {LogicalType::VARCHAR}, LustreObjectsExecute, LustreObjectsBindSingle,
	                          LustreObjectsInitGlobal, LustreObjectsInitLocal);
	SetObjectCommonProperties(single_func);
	set.AddFunction(std::move(single_func));

	TableFunction multi_func("lustre_objects", {LogicalType::LIST(LogicalType::VARCHAR)}, LustreObjectsExecute,
	                         LustreObjectsBindMulti, LustreObjectsInitGlobal, LustreObjectsInitLocal);
	SetObjectCommonProperties(multi_func);
	set.AddFunction(std::move(multi_func));

	return set;
}

} // namespace lustre
} // namespace duckdb
