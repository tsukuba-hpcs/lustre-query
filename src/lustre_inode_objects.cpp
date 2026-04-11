//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_inode_objects.cpp
//
// Internal fused table function for inode/object joins
//===----------------------------------------------------------------------===//

#include "lustre_inode_objects.hpp"

#include "lustre_fid_filter.hpp"
#include "lustre_filter.hpp"
#include "lustre_objects.hpp"
#include "lustre_scan_state.hpp"
#include "lustre_string_filter.hpp"
#include "lustre_table_filter_eval.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"

#include <algorithm>
#include <iterator>

namespace duckdb {
namespace lustre {

static constexpr idx_t INODE_OBJECT_EXACT_LOOKUP_BATCH_SIZE = 512;
static constexpr idx_t INODE_OBJECT_SEQ_SCAN_THRESHOLD = 8192;

static const vector<string> INODE_OBJECT_COLUMN_NAMES = {
    "inode_fid",  "ino",          "type",    "mode",      "nlink",      "uid",          "gid",    "size",
    "blocks",     "atime",        "mtime",   "ctime",     "projid",     "flags",        "device", "object_fid",
    "comp_index", "stripe_index", "ost_idx", "ost_oi_id", "ost_oi_seq", "object_device"};

static const vector<LogicalType> INODE_OBJECT_COLUMN_TYPES = {
    LogicalType::VARCHAR,  LogicalType::UBIGINT,   LogicalType::VARCHAR,   LogicalType::UINTEGER,
    LogicalType::UINTEGER, LogicalType::UINTEGER,  LogicalType::UINTEGER,  LogicalType::UBIGINT,
    LogicalType::UBIGINT,  LogicalType::TIMESTAMP, LogicalType::TIMESTAMP, LogicalType::TIMESTAMP,
    LogicalType::UINTEGER, LogicalType::UINTEGER,  LogicalType::VARCHAR,   LogicalType::VARCHAR,
    LogicalType::UINTEGER, LogicalType::UINTEGER,  LogicalType::UINTEGER,  LogicalType::UBIGINT,
    LogicalType::UBIGINT,  LogicalType::VARCHAR};

struct LustreInodeObjectsFilter {
	unique_ptr<LustreFilterSet> inode_filters;
	unique_ptr<FIDOnlyFilter> inode_fid_filter;
	unique_ptr<FIDOnlyFilter> object_fid_filter;
	unique_ptr<LustreStringFilter> inode_device_predicate;
	map<idx_t, unique_ptr<TableFilter>> object_filters;
	vector<LustreFID> lookup_fids;

	bool HasDynamicFilter() const {
		if ((inode_fid_filter && inode_fid_filter->HasDynamicFilter()) ||
		    (object_fid_filter && object_fid_filter->HasDynamicFilter())) {
			return true;
		}
		for (const auto &entry : object_filters) {
			if (TableFilterHasDynamicFilter(*entry.second)) {
				return true;
			}
		}
		return false;
	}

	bool HasAnyFilter() const {
		return (inode_filters && inode_filters->HasFilters()) ||
		       (inode_fid_filter && inode_fid_filter->HasAnyFilter()) ||
		       (object_fid_filter && object_fid_filter->HasAnyFilter()) ||
		       (inode_device_predicate && inode_device_predicate->HasPredicate()) || !object_filters.empty();
	}

	bool UseSequentialScan() const {
		return lookup_fids.empty() || lookup_fids.size() > INODE_OBJECT_SEQ_SCAN_THRESHOLD;
	}

	bool ResolveDynamicFilters() {
		bool changed = false;
		if (inode_fid_filter && inode_fid_filter->HasDynamicFilter()) {
			changed |= inode_fid_filter->ResolveDynamicFilters();
		}
		if (object_fid_filter && object_fid_filter->HasDynamicFilter()) {
			changed |= object_fid_filter->ResolveDynamicFilters();
		}
		if (changed) {
			RebuildLookupFIDs();
		}
		return changed;
	}

	void RebuildLookupFIDs() {
		lookup_fids.clear();
		bool has_inode_fids = inode_fid_filter && !inode_fid_filter->fid_values.empty();
		bool has_object_fids = object_fid_filter && !object_fid_filter->fid_values.empty();

		if (has_inode_fids && has_object_fids) {
			lookup_fids.reserve(MinValue(inode_fid_filter->fid_values.size(), object_fid_filter->fid_values.size()));
			std::set_intersection(inode_fid_filter->fid_values.begin(), inode_fid_filter->fid_values.end(),
			                      object_fid_filter->fid_values.begin(), object_fid_filter->fid_values.end(),
			                      std::back_inserter(lookup_fids));
		} else if (has_inode_fids) {
			lookup_fids.insert(lookup_fids.end(), inode_fid_filter->fid_values.begin(),
			                   inode_fid_filter->fid_values.end());
		} else if (has_object_fids) {
			lookup_fids.insert(lookup_fids.end(), object_fid_filter->fid_values.begin(),
			                   object_fid_filter->fid_values.end());
		}
		std::sort(lookup_fids.begin(), lookup_fids.end());
		lookup_fids.erase(std::unique(lookup_fids.begin(), lookup_fids.end()), lookup_fids.end());
	}

	Value GetObjectValue(idx_t actual_column_idx, const LustreInodeObjectRow &row, const string &device_path) const {
		switch (actual_column_idx) {
		case 15:
			return Value(row.object_fid.ToString());
		case 16:
			return Value::UINTEGER(row.object.comp_index);
		case 17:
			return Value::UINTEGER(row.object.stripe_index);
		case 18:
			return Value::UINTEGER(row.object.ost_idx);
		case 19:
			return Value::UBIGINT(row.object.ost_oi_id);
		case 20:
			return Value::UBIGINT(row.object.ost_oi_seq);
		case 21:
			return Value(device_path);
		default:
			return Value();
		}
	}

	bool Matches(const LustreInodeObjectRow &row, const string &device_path) const {
		if (inode_filters && inode_filters->HasFilters() && !inode_filters->Evaluate(row.inode)) {
			return false;
		}
		if (inode_device_predicate && inode_device_predicate->HasPredicate() &&
		    !inode_device_predicate->Evaluate(&device_path)) {
			return false;
		}
		if (inode_fid_filter) {
			if (!inode_fid_filter->ContainsFID(row.inode.fid)) {
				return false;
			}
			if (inode_fid_filter->RequiresGenericEvaluation() && !inode_fid_filter->EvaluateFID(row.inode.fid)) {
				return false;
			}
		}
		if (object_fid_filter) {
			if (!object_fid_filter->ContainsFID(row.object_fid)) {
				return false;
			}
			if (object_fid_filter->RequiresGenericEvaluation() && !object_fid_filter->EvaluateFID(row.object_fid)) {
				return false;
			}
		}
		for (const auto &entry : object_filters) {
			if (!EvaluateTableFilterValue(*entry.second, GetObjectValue(entry.first, row, device_path))) {
				return false;
			}
		}
		return true;
	}

	static unique_ptr<LustreInodeObjectsFilter> Create(const TableFilterSet *filters, const vector<idx_t> &column_ids) {
		auto result = make_uniq<LustreInodeObjectsFilter>();
		result->inode_filters = LustreFilterSet::Create(filters, column_ids);
		result->inode_fid_filter = FIDOnlyFilter::Create(filters, column_ids, 0);
		result->object_fid_filter = FIDOnlyFilter::Create(filters, column_ids, 15);

		if (filters) {
			for (const auto &entry : filters->filters) {
				if (entry.first >= column_ids.size()) {
					continue;
				}
				auto actual_column_idx = column_ids[entry.first];
				if (actual_column_idx == 14) {
					if (!result->inode_device_predicate) {
						result->inode_device_predicate = LustreStringFilter::Create(*entry.second);
					}
					continue;
				}
				if (actual_column_idx >= 16 && actual_column_idx <= 21) {
					result->object_filters[actual_column_idx] = entry.second->Copy();
				}
			}
		}

		result->RebuildLookupFIDs();
		return result;
	}
};

struct LustreInodeObjectsGlobalState : public GlobalTableFunctionState {
	vector<string> device_paths;
	vector<idx_t> column_ids;
	MDTScanConfig scan_config;
	unique_ptr<LustreInodeObjectsFilter> filters;
	atomic<idx_t> current_device_idx;
	atomic<bool> finished;
	atomic<bool> device_initialized;
	mutex device_transition_lock;
	string current_device_path;
	atomic<idx_t> next_fid_idx;
	atomic<int> next_block_group;
	atomic<int> active_block_groups;
	int total_block_groups = 0;
	uint32_t inodes_per_group = 0;
	bool use_sequential_scan = true;
	idx_t thread_count = 1;

	LustreInodeObjectsGlobalState()
	    : current_device_idx(0), finished(false), device_initialized(false), next_fid_idx(0), next_block_group(0),
	      active_block_groups(0) {
	}

	idx_t MaxThreads() const override {
		if (filters && filters->HasDynamicFilter()) {
			return 1;
		}
		if (!filters || use_sequential_scan) {
			return MinValue<idx_t>(thread_count, static_cast<idx_t>(MaxValue(total_block_groups, 1)));
		}
		return MinValue<idx_t>(thread_count, filters->lookup_fids.size());
	}
};

struct LustreInodeObjectsLocalState : public LocalTableFunctionState {
	unique_ptr<MDTScanner> scanner;
	bool scanner_initialized = false;
	idx_t initialized_device_idx = DConstants::INVALID_INDEX;
	string initialized_device_path;
	bool sequential_scan_mode = true;
	bool block_group_active = false;
	ext2_ino_t block_group_max_ino = 0;
	int next_block_group_in_batch = 0;
	int block_group_batch_end = 0;
	vector<LustreInodeObjectRow> pending_results;
	idx_t pending_results_idx = 0;

	LustreInodeObjectsLocalState() {
		scanner = make_uniq<MDTScanner>();
	}
};

const vector<string> &LustreInodeObjectsFunction::GetColumnNames() {
	return INODE_OBJECT_COLUMN_NAMES;
}

const vector<LogicalType> &LustreInodeObjectsFunction::GetColumnTypes() {
	return INODE_OBJECT_COLUMN_TYPES;
}

static unique_ptr<FunctionData> LustreInodeObjectsBindSingle(ClientContext &context, TableFunctionBindInput &input,
                                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<LustreQueryBindData>();
	result->device_paths.push_back(StringValue::Get(input.inputs[0]));
	ParseNamedParameters(input.named_parameters, result->scan_config);
	EstimateCardinality(*result);
	names = LustreInodeObjectsFunction::GetColumnNames();
	return_types = LustreInodeObjectsFunction::GetColumnTypes();
	return std::move(result);
}

static unique_ptr<FunctionData> LustreInodeObjectsBindMulti(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<LustreQueryBindData>();
	auto &list_values = ListValue::GetChildren(input.inputs[0]);
	for (auto &val : list_values) {
		result->device_paths.push_back(StringValue::Get(val));
	}
	if (result->device_paths.empty()) {
		throw BinderException("lustre_inode_objects_internal requires at least one device path");
	}
	ParseNamedParameters(input.named_parameters, result->scan_config);
	EstimateCardinality(*result);
	names = LustreInodeObjectsFunction::GetColumnNames();
	return_types = LustreInodeObjectsFunction::GetColumnTypes();
	return std::move(result);
}

static bool InitializeInodeObjectsDevice(LustreInodeObjectsGlobalState &gstate, idx_t device_idx) {
	if (device_idx >= gstate.device_paths.size()) {
		gstate.finished = true;
		return false;
	}

	MDTScanner probe;
	probe.Open(gstate.device_paths[device_idx]);
	gstate.current_device_path = gstate.device_paths[device_idx];
	gstate.total_block_groups = static_cast<int>(probe.GetBlockGroupCount());
	gstate.inodes_per_group = probe.GetInodesPerGroup();
	gstate.next_block_group.store(0);
	gstate.active_block_groups.store(0);
	gstate.next_fid_idx.store(0);
	probe.Close();
	gstate.device_initialized = true;
	return true;
}

static unique_ptr<GlobalTableFunctionState> LustreInodeObjectsInitGlobal(ClientContext &context,
                                                                         TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<LustreQueryBindData>();
	auto result = make_uniq<LustreInodeObjectsGlobalState>();
	result->device_paths = bind_data.device_paths;
	result->column_ids = input.column_ids;
	result->scan_config = bind_data.scan_config;
	result->filters = LustreInodeObjectsFilter::Create(input.filters.get(), input.column_ids);
	result->use_sequential_scan = !result->filters || result->filters->UseSequentialScan();
	result->thread_count = NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());
	if (!result->device_paths.empty()) {
		InitializeInodeObjectsDevice(*result, 0);
	}
	return std::move(result);
}

static unique_ptr<LocalTableFunctionState> LustreInodeObjectsInitLocal(ExecutionContext &context,
                                                                       TableFunctionInitInput &input,
                                                                       GlobalTableFunctionState *global_state) {
	return make_uniq<LustreInodeObjectsLocalState>();
}

static void WriteTimestamp(Vector &vec, idx_t row_idx, int64_t unix_timestamp) {
	if (unix_timestamp == 0) {
		FlatVector::SetNull(vec, row_idx, true);
	} else {
		FlatVector::GetData<timestamp_t>(vec)[row_idx] = Timestamp::FromEpochSeconds(unix_timestamp);
	}
}

static void WriteInodeObjectColumn(Vector &vec, idx_t col_idx, idx_t row_idx, const LustreInodeObjectRow &row,
                                   const string &device_path) {
	switch (col_idx) {
	case 0: {
		auto fid_str = row.inode.fid.ToString();
		FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, fid_str);
		break;
	}
	case 1:
		FlatVector::GetData<uint64_t>(vec)[row_idx] = row.inode.ino;
		break;
	case 2: {
		auto type_str = FileTypeToString(row.inode.type);
		FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, type_str);
		break;
	}
	case 3:
		FlatVector::GetData<uint32_t>(vec)[row_idx] = row.inode.mode;
		break;
	case 4:
		FlatVector::GetData<uint32_t>(vec)[row_idx] = row.inode.nlink;
		break;
	case 5:
		FlatVector::GetData<uint32_t>(vec)[row_idx] = row.inode.uid;
		break;
	case 6:
		FlatVector::GetData<uint32_t>(vec)[row_idx] = row.inode.gid;
		break;
	case 7:
		FlatVector::GetData<uint64_t>(vec)[row_idx] = row.inode.size;
		break;
	case 8:
		FlatVector::GetData<uint64_t>(vec)[row_idx] = row.inode.blocks;
		break;
	case 9:
		WriteTimestamp(vec, row_idx, row.inode.atime);
		break;
	case 10:
		WriteTimestamp(vec, row_idx, row.inode.mtime);
		break;
	case 11:
		WriteTimestamp(vec, row_idx, row.inode.ctime);
		break;
	case 12:
		FlatVector::GetData<uint32_t>(vec)[row_idx] = row.inode.projid;
		break;
	case 13:
		FlatVector::GetData<uint32_t>(vec)[row_idx] = row.inode.flags;
		break;
	case 14:
		FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, device_path);
		break;
	case 15: {
		auto fid_str = row.object_fid.ToString();
		FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, fid_str);
		break;
	}
	case 16:
		FlatVector::GetData<uint32_t>(vec)[row_idx] = row.object.comp_index;
		break;
	case 17:
		FlatVector::GetData<uint32_t>(vec)[row_idx] = row.object.stripe_index;
		break;
	case 18:
		FlatVector::GetData<uint32_t>(vec)[row_idx] = row.object.ost_idx;
		break;
	case 19:
		FlatVector::GetData<uint64_t>(vec)[row_idx] = row.object.ost_oi_id;
		break;
	case 20:
		FlatVector::GetData<uint64_t>(vec)[row_idx] = row.object.ost_oi_seq;
		break;
	case 21:
		FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, device_path);
		break;
	default:
		break;
	}
}

static void ResetInodeObjectsBlockGroupState(LustreInodeObjectsLocalState &lstate) {
	lstate.block_group_active = false;
	lstate.block_group_max_ino = 0;
	lstate.next_block_group_in_batch = 0;
	lstate.block_group_batch_end = 0;
}

static bool ClaimNextInodeObjectsBlockGroup(LustreInodeObjectsGlobalState &gstate,
                                            LustreInodeObjectsLocalState &lstate) {
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

static bool EnsureInodeObjectsScanner(LustreInodeObjectsGlobalState &gstate, LustreInodeObjectsLocalState &lstate,
                                      bool sequential_scan) {
	idx_t dev_idx = gstate.current_device_idx.load();

	if (lstate.scanner_initialized && lstate.initialized_device_idx == dev_idx &&
	    lstate.sequential_scan_mode == sequential_scan) {
		return true;
	}

	if (lstate.scanner_initialized) {
		lstate.scanner->CloseScan();
		lstate.scanner->CloseOI();
		lstate.scanner->Close();
		lstate.scanner_initialized = false;
		ResetInodeObjectsBlockGroupState(lstate);
		lstate.initialized_device_path.clear();
	}

	if (dev_idx >= gstate.device_paths.size() || gstate.finished.load()) {
		return false;
	}

	if (!gstate.device_initialized.load()) {
		lock_guard<mutex> lock(gstate.device_transition_lock);
		if (!gstate.device_initialized.load()) {
			if (!InitializeInodeObjectsDevice(gstate, dev_idx)) {
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
	ResetInodeObjectsBlockGroupState(lstate);
	return true;
}

static bool ExecuteExactFIDPath(LustreInodeObjectsGlobalState &gstate, LustreInodeObjectsLocalState &lstate,
                                DataChunk &output, idx_t &output_count) {
	const auto &current_device = lstate.initialized_device_path;
	struct LookupEntry {
		ext2_ino_t ino;
		LustreFID fid;
	};

	while (output_count < STANDARD_VECTOR_SIZE) {
		while (lstate.pending_results_idx < lstate.pending_results.size() && output_count < STANDARD_VECTOR_SIZE) {
			auto &row = lstate.pending_results[lstate.pending_results_idx];
			for (idx_t i = 0; i < gstate.column_ids.size(); i++) {
				WriteInodeObjectColumn(output.data[i], gstate.column_ids[i], output_count, row, current_device);
			}
			lstate.pending_results_idx++;
			output_count++;
		}

		if (output_count >= STANDARD_VECTOR_SIZE) {
			return true;
		}

		vector<LookupEntry> lookup_batch;
		lookup_batch.reserve(INODE_OBJECT_EXACT_LOOKUP_BATCH_SIZE);

		while (lookup_batch.size() < INODE_OBJECT_EXACT_LOOKUP_BATCH_SIZE) {
			idx_t fid_idx = gstate.next_fid_idx.fetch_add(1);
			if (fid_idx >= gstate.filters->lookup_fids.size()) {
				break;
			}

			auto &fid = gstate.filters->lookup_fids[fid_idx];
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

			LustreInode inode;
			vector<LustreOSTObject> objects;
			if (!lstate.scanner->ReadInodeObjects(entry.ino, inode, objects, gstate.scan_config)) {
				continue;
			}

			for (auto &object : objects) {
				LustreInodeObjectRow row;
				row.inode = inode;
				row.object_fid = inode.fid;
				row.object = std::move(object);
				if (gstate.filters && !gstate.filters->Matches(row, current_device)) {
					continue;
				}
				lstate.pending_results.push_back(std::move(row));
			}
		}
	}

	return true;
}

static bool ExecuteSequentialPath(LustreInodeObjectsGlobalState &gstate, LustreInodeObjectsLocalState &lstate,
                                  DataChunk &output, idx_t &output_count) {
	const auto &current_device = lstate.initialized_device_path;
	while (output_count < STANDARD_VECTOR_SIZE) {
		if (!ClaimNextInodeObjectsBlockGroup(gstate, lstate)) {
			return false;
		}

		while (output_count < STANDARD_VECTOR_SIZE) {
			LustreInodeObjectRow row;
			if (!lstate.scanner->GetNextInodeObject(row, gstate.scan_config, lstate.block_group_max_ino)) {
				lstate.block_group_active = false;
				gstate.active_block_groups.fetch_sub(1);
				break;
			}
			if (gstate.filters && !gstate.filters->Matches(row, current_device)) {
				continue;
			}

			for (idx_t i = 0; i < gstate.column_ids.size(); i++) {
				WriteInodeObjectColumn(output.data[i], gstate.column_ids[i], output_count, row, current_device);
			}
			output_count++;
		}
	}

	return true;
}

static void LustreInodeObjectsExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &gstate = data_p.global_state->Cast<LustreInodeObjectsGlobalState>();
	auto &lstate = data_p.local_state->Cast<LustreInodeObjectsLocalState>();

	if (gstate.filters && gstate.filters->HasDynamicFilter()) {
		bool changed = gstate.filters->ResolveDynamicFilters();
		if (changed) {
			gstate.use_sequential_scan = gstate.filters->UseSequentialScan();
			gstate.finished = false;
			gstate.current_device_idx = 0;
			gstate.device_initialized = false;
			gstate.next_fid_idx.store(0);
			gstate.next_block_group.store(0);
			gstate.active_block_groups.store(0);
			lstate.pending_results.clear();
			lstate.pending_results_idx = 0;
			if (lstate.scanner_initialized) {
				lstate.scanner->CloseScan();
				lstate.scanner->CloseOI();
				lstate.scanner->Close();
			}
			lstate.scanner_initialized = false;
			ResetInodeObjectsBlockGroupState(lstate);
			lstate.initialized_device_path.clear();
		}
	}

	if (gstate.finished) {
		output.SetCardinality(0);
		return;
	}

	idx_t output_count = 0;
	while (output_count < STANDARD_VECTOR_SIZE) {
		if (!EnsureInodeObjectsScanner(gstate, lstate, gstate.use_sequential_scan)) {
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
			ResetInodeObjectsBlockGroupState(lstate);
			lstate.initialized_device_path.clear();
			lstate.pending_results.clear();
			lstate.pending_results_idx = 0;
		}
	}

	output.SetCardinality(output_count);
}

static unique_ptr<NodeStatistics> LustreInodeObjectsCardinality(ClientContext &context,
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

TableFunction LustreInodeObjectsFunction::GetFunction(bool multi_device) {
	TableFunction func("lustre_inode_objects_internal",
	                   {multi_device ? LogicalType::LIST(LogicalType::VARCHAR) : LogicalType::VARCHAR},
	                   LustreInodeObjectsExecute,
	                   multi_device ? LustreInodeObjectsBindMulti : LustreInodeObjectsBindSingle,
	                   LustreInodeObjectsInitGlobal, LustreInodeObjectsInitLocal);
	func.named_parameters["skip_no_fid"] = LogicalType::BOOLEAN;
	func.named_parameters["skip_no_linkea"] = LogicalType::BOOLEAN;
	func.projection_pushdown = true;
	func.filter_pushdown = true;
	func.table_scan_progress = LustreScanProgress;
	func.cardinality = LustreInodeObjectsCardinality;
	return func;
}

TableFunctionSet LustreInodeObjectsFunction::GetFunctionSet() {
	TableFunctionSet set("lustre_inode_objects_internal");
	set.AddFunction(GetFunction(false));
	set.AddFunction(GetFunction(true));
	return set;
}

} // namespace lustre
} // namespace duckdb
