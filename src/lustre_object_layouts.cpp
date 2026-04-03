//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_object_layouts.cpp
//
// Internal fused table function for object/layout joins
//===----------------------------------------------------------------------===//

#include "lustre_object_layouts.hpp"

#include "lustre_fid_filter.hpp"
#include "lustre_layouts.hpp"
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

static constexpr idx_t OBJECT_LAYOUT_EXACT_LOOKUP_BATCH_SIZE = 512;
static constexpr idx_t OBJECT_LAYOUT_SEQ_SCAN_THRESHOLD = 8192;

static const vector<string> OBJECT_LAYOUT_COLUMN_NAMES = {
    "object_fid",       "object_comp_index", "stripe_index",  "ost_idx",       "ost_oi_id",   "ost_oi_seq",
    "object_device",    "layout_fid",        "layout_comp_index", "comp_id",    "mirror_id",   "comp_flags",
    "extent_start",     "extent_end",        "pattern",       "stripe_size",   "stripe_count", "stripe_offset",
    "pool",             "dstripe_count",     "cstripe_count", "compr_type",    "compr_lvl",    "layout_device"};

static const vector<LogicalType> OBJECT_LAYOUT_COLUMN_TYPES = {
    LogicalType::VARCHAR,   LogicalType::UINTEGER,  LogicalType::UINTEGER,  LogicalType::UINTEGER,
    LogicalType::UBIGINT,   LogicalType::UBIGINT,   LogicalType::VARCHAR,   LogicalType::VARCHAR,
    LogicalType::UINTEGER,  LogicalType::UINTEGER,  LogicalType::USMALLINT, LogicalType::UINTEGER,
    LogicalType::UBIGINT,   LogicalType::UBIGINT,   LogicalType::UINTEGER,  LogicalType::UINTEGER,
    LogicalType::USMALLINT, LogicalType::USMALLINT, LogicalType::VARCHAR,   LogicalType::UTINYINT,
    LogicalType::UTINYINT,  LogicalType::UTINYINT,  LogicalType::UTINYINT,  LogicalType::VARCHAR};

struct LustreObjectLayoutRow {
	LustreFID fid;
	LustreOSTObject object;
	LustreLayoutComponent layout;
};

struct LustreObjectLayoutsFilter {
	unique_ptr<FIDOnlyFilter> object_fid_filter;
	unique_ptr<FIDOnlyFilter> layout_fid_filter;
	unique_ptr<LustreStringFilter> object_device_predicate;
	unique_ptr<LustreStringFilter> layout_device_predicate;
	map<idx_t, unique_ptr<TableFilter>> object_filters;
	map<idx_t, unique_ptr<TableFilter>> layout_filters;
	vector<LustreFID> lookup_fids;

	bool HasDynamicFilter() const {
		if ((object_fid_filter && object_fid_filter->HasDynamicFilter()) ||
		    (layout_fid_filter && layout_fid_filter->HasDynamicFilter())) {
			return true;
		}
		for (const auto &entry : object_filters) {
			if (TableFilterHasDynamicFilter(*entry.second)) {
				return true;
			}
		}
		for (const auto &entry : layout_filters) {
			if (TableFilterHasDynamicFilter(*entry.second)) {
				return true;
			}
		}
		return false;
	}

	bool HasAnyFilter() const {
		return (object_fid_filter && object_fid_filter->HasAnyFilter()) ||
		       (layout_fid_filter && layout_fid_filter->HasAnyFilter()) ||
		       (object_device_predicate && object_device_predicate->HasPredicate()) ||
		       (layout_device_predicate && layout_device_predicate->HasPredicate()) || !object_filters.empty() ||
		       !layout_filters.empty();
	}

	bool UseSequentialScan() const {
		return lookup_fids.empty() || lookup_fids.size() > OBJECT_LAYOUT_SEQ_SCAN_THRESHOLD;
	}

	bool ResolveDynamicFilters() {
		bool changed = false;
		if (object_fid_filter && object_fid_filter->HasDynamicFilter()) {
			changed |= object_fid_filter->ResolveDynamicFilters();
		}
		if (layout_fid_filter && layout_fid_filter->HasDynamicFilter()) {
			changed |= layout_fid_filter->ResolveDynamicFilters();
		}
		if (changed) {
			RebuildLookupFIDs();
		}
		return changed;
	}

	void RebuildLookupFIDs() {
		lookup_fids.clear();
		bool has_object_fids = object_fid_filter && !object_fid_filter->fid_values.empty();
		bool has_layout_fids = layout_fid_filter && !layout_fid_filter->fid_values.empty();

		if (has_object_fids && has_layout_fids) {
			lookup_fids.reserve(MinValue(object_fid_filter->fid_values.size(), layout_fid_filter->fid_values.size()));
			std::set_intersection(object_fid_filter->fid_values.begin(), object_fid_filter->fid_values.end(),
			                      layout_fid_filter->fid_values.begin(), layout_fid_filter->fid_values.end(),
			                      std::back_inserter(lookup_fids));
		} else if (has_object_fids) {
			lookup_fids.insert(lookup_fids.end(), object_fid_filter->fid_values.begin(), object_fid_filter->fid_values.end());
		} else if (has_layout_fids) {
			lookup_fids.insert(lookup_fids.end(), layout_fid_filter->fid_values.begin(), layout_fid_filter->fid_values.end());
		}
		std::sort(lookup_fids.begin(), lookup_fids.end());
		lookup_fids.erase(std::unique(lookup_fids.begin(), lookup_fids.end()), lookup_fids.end());
	}

	Value GetColumnValue(idx_t actual_column_idx, const LustreObjectLayoutRow &row, const string &device_path) const {
		switch (actual_column_idx) {
		case 0:
		case 7:
			return Value(row.fid.ToString());
		case 1:
			return Value::UINTEGER(row.object.comp_index);
		case 2:
			return Value::UINTEGER(row.object.stripe_index);
		case 3:
			return Value::UINTEGER(row.object.ost_idx);
		case 4:
			return Value::UBIGINT(row.object.ost_oi_id);
		case 5:
			return Value::UBIGINT(row.object.ost_oi_seq);
		case 6:
		case 23:
			return Value(device_path);
		case 8:
			return Value::UINTEGER(row.layout.comp_index);
		case 9:
			return Value::UINTEGER(row.layout.comp_id);
		case 10:
			return Value::USMALLINT(row.layout.mirror_id);
		case 11:
			return Value::UINTEGER(row.layout.comp_flags);
		case 12:
			return Value::UBIGINT(row.layout.extent_start);
		case 13:
			return Value::UBIGINT(row.layout.extent_end);
		case 14:
			return Value::UINTEGER(row.layout.pattern);
		case 15:
			return Value::UINTEGER(row.layout.stripe_size);
		case 16:
			return Value::USMALLINT(row.layout.stripe_count);
		case 17:
			return Value::USMALLINT(row.layout.stripe_offset);
		case 18:
			return row.layout.pool_name.empty() ? Value() : Value(row.layout.pool_name);
		case 19:
			return Value::UTINYINT(row.layout.dstripe_count);
		case 20:
			return Value::UTINYINT(row.layout.cstripe_count);
		case 21:
			return Value::UTINYINT(row.layout.compr_type);
		case 22:
			return Value::UTINYINT(row.layout.compr_lvl);
		default:
			return Value();
		}
	}

	bool Matches(const LustreObjectLayoutRow &row, const string &device_path) const {
		if (object_device_predicate && object_device_predicate->HasPredicate() &&
		    !object_device_predicate->Evaluate(&device_path)) {
			return false;
		}
		if (layout_device_predicate && layout_device_predicate->HasPredicate() &&
		    !layout_device_predicate->Evaluate(&device_path)) {
			return false;
		}
		if (object_fid_filter) {
			if (!object_fid_filter->ContainsFID(row.fid)) {
				return false;
			}
			if (object_fid_filter->RequiresGenericEvaluation() && !object_fid_filter->EvaluateFID(row.fid)) {
				return false;
			}
		}
		if (layout_fid_filter) {
			if (!layout_fid_filter->ContainsFID(row.fid)) {
				return false;
			}
			if (layout_fid_filter->RequiresGenericEvaluation() && !layout_fid_filter->EvaluateFID(row.fid)) {
				return false;
			}
		}
		for (const auto &entry : object_filters) {
			if (!EvaluateTableFilterValue(*entry.second, GetColumnValue(entry.first, row, device_path))) {
				return false;
			}
		}
		for (const auto &entry : layout_filters) {
			if (!EvaluateTableFilterValue(*entry.second, GetColumnValue(entry.first, row, device_path))) {
				return false;
			}
		}
		return true;
	}

	static unique_ptr<LustreObjectLayoutsFilter> Create(const TableFilterSet *filters, const vector<idx_t> &column_ids) {
		auto result = make_uniq<LustreObjectLayoutsFilter>();
		result->object_fid_filter = FIDOnlyFilter::Create(filters, column_ids, 0);
		result->layout_fid_filter = FIDOnlyFilter::Create(filters, column_ids, 7);

		if (filters) {
			for (const auto &entry : filters->filters) {
				if (entry.first >= column_ids.size()) {
					continue;
				}
				auto actual_column_idx = column_ids[entry.first];
				if (actual_column_idx == 6) {
					if (!result->object_device_predicate) {
						result->object_device_predicate = LustreStringFilter::Create(*entry.second);
					}
					continue;
				}
				if (actual_column_idx == 23) {
					if (!result->layout_device_predicate) {
						result->layout_device_predicate = LustreStringFilter::Create(*entry.second);
					}
					continue;
				}
				if (actual_column_idx >= 1 && actual_column_idx <= 5) {
					result->object_filters[actual_column_idx] = entry.second->Copy();
					continue;
				}
				if (actual_column_idx >= 8 && actual_column_idx <= 22) {
					result->layout_filters[actual_column_idx] = entry.second->Copy();
				}
			}
		}

		result->RebuildLookupFIDs();
		return result;
	}
};

struct LustreObjectLayoutsGlobalState : public GlobalTableFunctionState {
	vector<string> device_paths;
	vector<idx_t> column_ids;
	MDTScanConfig scan_config;
	unique_ptr<LustreObjectLayoutsFilter> filters;
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

	LustreObjectLayoutsGlobalState()
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

struct LustreObjectLayoutsLocalState : public LocalTableFunctionState {
	unique_ptr<MDTScanner> scanner;
	bool scanner_initialized = false;
	idx_t initialized_device_idx = DConstants::INVALID_INDEX;
	string initialized_device_path;
	bool sequential_scan_mode = true;
	bool block_group_active = false;
	ext2_ino_t block_group_max_ino = 0;
	int next_block_group_in_batch = 0;
	int block_group_batch_end = 0;
	vector<LustreObjectLayoutRow> pending_results;
	idx_t pending_results_idx = 0;

	LustreObjectLayoutsLocalState() {
		scanner = make_uniq<MDTScanner>();
	}
};

const vector<string> &LustreObjectLayoutsFunction::GetColumnNames() {
	return OBJECT_LAYOUT_COLUMN_NAMES;
}

const vector<LogicalType> &LustreObjectLayoutsFunction::GetColumnTypes() {
	return OBJECT_LAYOUT_COLUMN_TYPES;
}

static unique_ptr<FunctionData> LustreObjectLayoutsBindSingle(ClientContext &context, TableFunctionBindInput &input,
                                                              vector<LogicalType> &return_types,
                                                              vector<string> &names) {
	auto result = make_uniq<LustreQueryBindData>();
	result->device_paths.push_back(StringValue::Get(input.inputs[0]));
	ParseNamedParameters(input.named_parameters, result->scan_config);
	EstimateCardinality(*result);
	names = LustreObjectLayoutsFunction::GetColumnNames();
	return_types = LustreObjectLayoutsFunction::GetColumnTypes();
	return std::move(result);
}

static unique_ptr<FunctionData> LustreObjectLayoutsBindMulti(ClientContext &context, TableFunctionBindInput &input,
                                                             vector<LogicalType> &return_types,
                                                             vector<string> &names) {
	auto result = make_uniq<LustreQueryBindData>();
	auto &list_values = ListValue::GetChildren(input.inputs[0]);
	for (auto &val : list_values) {
		result->device_paths.push_back(StringValue::Get(val));
	}
	if (result->device_paths.empty()) {
		throw BinderException("lustre_object_layouts_internal requires at least one device path");
	}
	ParseNamedParameters(input.named_parameters, result->scan_config);
	EstimateCardinality(*result);
	names = LustreObjectLayoutsFunction::GetColumnNames();
	return_types = LustreObjectLayoutsFunction::GetColumnTypes();
	return std::move(result);
}

static bool InitializeObjectLayoutsDevice(LustreObjectLayoutsGlobalState &gstate, idx_t device_idx) {
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

static unique_ptr<GlobalTableFunctionState> LustreObjectLayoutsInitGlobal(ClientContext &context,
                                                                          TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<LustreQueryBindData>();
	auto result = make_uniq<LustreObjectLayoutsGlobalState>();
	result->device_paths = bind_data.device_paths;
	result->column_ids = input.column_ids;
	result->scan_config = bind_data.scan_config;
	result->filters = LustreObjectLayoutsFilter::Create(input.filters.get(), input.column_ids);
	result->use_sequential_scan = !result->filters || result->filters->UseSequentialScan();
	result->thread_count = NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());
	if (!result->device_paths.empty()) {
		InitializeObjectLayoutsDevice(*result, 0);
	}
	return std::move(result);
}

static unique_ptr<LocalTableFunctionState> LustreObjectLayoutsInitLocal(ExecutionContext &context,
                                                                        TableFunctionInitInput &input,
                                                                        GlobalTableFunctionState *global_state) {
	return make_uniq<LustreObjectLayoutsLocalState>();
}

static void WriteObjectLayoutColumn(Vector &vec, idx_t col_idx, idx_t row_idx, const LustreObjectLayoutRow &row,
                                    const string &device_path) {
	switch (col_idx) {
	case 0:
	case 7: {
		auto fid_str = row.fid.ToString();
		FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, fid_str);
		break;
	}
	case 1:
		FlatVector::GetData<uint32_t>(vec)[row_idx] = row.object.comp_index;
		break;
	case 2:
		FlatVector::GetData<uint32_t>(vec)[row_idx] = row.object.stripe_index;
		break;
	case 3:
		FlatVector::GetData<uint32_t>(vec)[row_idx] = row.object.ost_idx;
		break;
	case 4:
		FlatVector::GetData<uint64_t>(vec)[row_idx] = row.object.ost_oi_id;
		break;
	case 5:
		FlatVector::GetData<uint64_t>(vec)[row_idx] = row.object.ost_oi_seq;
		break;
	case 6:
	case 23:
		FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, device_path);
		break;
	case 8:
		FlatVector::GetData<uint32_t>(vec)[row_idx] = row.layout.comp_index;
		break;
	case 9:
		FlatVector::GetData<uint32_t>(vec)[row_idx] = row.layout.comp_id;
		break;
	case 10:
		FlatVector::GetData<uint16_t>(vec)[row_idx] = row.layout.mirror_id;
		break;
	case 11:
		FlatVector::GetData<uint32_t>(vec)[row_idx] = row.layout.comp_flags;
		break;
	case 12:
		FlatVector::GetData<uint64_t>(vec)[row_idx] = row.layout.extent_start;
		break;
	case 13:
		FlatVector::GetData<uint64_t>(vec)[row_idx] = row.layout.extent_end;
		break;
	case 14:
		FlatVector::GetData<uint32_t>(vec)[row_idx] = row.layout.pattern;
		break;
	case 15:
		FlatVector::GetData<uint32_t>(vec)[row_idx] = row.layout.stripe_size;
		break;
	case 16:
		FlatVector::GetData<uint16_t>(vec)[row_idx] = row.layout.stripe_count;
		break;
	case 17:
		FlatVector::GetData<uint16_t>(vec)[row_idx] = row.layout.stripe_offset;
		break;
	case 18:
		if (!row.layout.pool_name.empty()) {
			FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, row.layout.pool_name);
		} else {
			FlatVector::SetNull(vec, row_idx, true);
		}
		break;
	case 19:
		FlatVector::GetData<uint8_t>(vec)[row_idx] = row.layout.dstripe_count;
		break;
	case 20:
		FlatVector::GetData<uint8_t>(vec)[row_idx] = row.layout.cstripe_count;
		break;
	case 21:
		FlatVector::GetData<uint8_t>(vec)[row_idx] = row.layout.compr_type;
		break;
	case 22:
		FlatVector::GetData<uint8_t>(vec)[row_idx] = row.layout.compr_lvl;
		break;
	default:
		break;
	}
}

static void ExpandObjectLayoutRows(vector<LustreObjectLayoutRow> &out, const LustreFID &fid,
                                   const vector<LustreOSTObject> &objects,
                                   const vector<LustreLayoutComponent> &layouts,
                                   const LustreObjectLayoutsFilter *filters, const string &device_path) {
	if (objects.empty() || layouts.empty()) {
		return;
	}

	for (const auto &object : objects) {
		for (const auto &layout : layouts) {
			if (object.comp_index != layout.comp_index) {
				continue;
			}
			LustreObjectLayoutRow row;
			row.fid = fid;
			row.object = object;
			row.layout = layout;
			if (filters && !filters->Matches(row, device_path)) {
				continue;
			}
			out.push_back(std::move(row));
		}
	}
}

static void ResetObjectLayoutsBlockGroupState(LustreObjectLayoutsLocalState &lstate) {
	lstate.block_group_active = false;
	lstate.block_group_max_ino = 0;
	lstate.next_block_group_in_batch = 0;
	lstate.block_group_batch_end = 0;
}

static bool ClaimNextObjectLayoutsBlockGroup(LustreObjectLayoutsGlobalState &gstate,
                                             LustreObjectLayoutsLocalState &lstate) {
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

static bool EnsureObjectLayoutsScanner(LustreObjectLayoutsGlobalState &gstate, LustreObjectLayoutsLocalState &lstate,
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
		ResetObjectLayoutsBlockGroupState(lstate);
		lstate.initialized_device_path.clear();
	}

	if (dev_idx >= gstate.device_paths.size() || gstate.finished.load()) {
		return false;
	}

	if (!gstate.device_initialized.load()) {
		lock_guard<mutex> lock(gstate.device_transition_lock);
		if (!gstate.device_initialized.load()) {
			if (!InitializeObjectLayoutsDevice(gstate, dev_idx)) {
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
	ResetObjectLayoutsBlockGroupState(lstate);
	return true;
}

static bool ExecuteExactFIDPath(LustreObjectLayoutsGlobalState &gstate, LustreObjectLayoutsLocalState &lstate,
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
				WriteObjectLayoutColumn(output.data[i], gstate.column_ids[i], output_count, row, current_device);
			}
			lstate.pending_results_idx++;
			output_count++;
		}

		if (output_count >= STANDARD_VECTOR_SIZE) {
			return true;
		}

		vector<LookupEntry> lookup_batch;
		lookup_batch.reserve(OBJECT_LAYOUT_EXACT_LOOKUP_BATCH_SIZE);

		while (lookup_batch.size() < OBJECT_LAYOUT_EXACT_LOOKUP_BATCH_SIZE) {
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

			LustreFID actual_fid;
			vector<LustreLayoutComponent> layouts;
			vector<LustreOSTObject> objects;
			if (!lstate.scanner->ReadInodeLayoutObjects(entry.ino, actual_fid, layouts, objects, gstate.scan_config)) {
				continue;
			}

			ExpandObjectLayoutRows(lstate.pending_results, actual_fid, objects, layouts, gstate.filters.get(),
			                       current_device);
		}
	}

	return true;
}

static bool ExecuteSequentialPath(LustreObjectLayoutsGlobalState &gstate, LustreObjectLayoutsLocalState &lstate,
                                  DataChunk &output, idx_t &output_count) {
	const auto &current_device = lstate.initialized_device_path;
	while (output_count < STANDARD_VECTOR_SIZE) {
		while (lstate.pending_results_idx < lstate.pending_results.size() && output_count < STANDARD_VECTOR_SIZE) {
			auto &row = lstate.pending_results[lstate.pending_results_idx];
			for (idx_t i = 0; i < gstate.column_ids.size(); i++) {
				WriteObjectLayoutColumn(output.data[i], gstate.column_ids[i], output_count, row, current_device);
			}
			lstate.pending_results_idx++;
			output_count++;
		}

		if (output_count >= STANDARD_VECTOR_SIZE) {
			return true;
		}

		if (!ClaimNextObjectLayoutsBlockGroup(gstate, lstate)) {
			return false;
		}

		while (output_count < STANDARD_VECTOR_SIZE) {
			while (lstate.pending_results_idx < lstate.pending_results.size() && output_count < STANDARD_VECTOR_SIZE) {
				auto &row = lstate.pending_results[lstate.pending_results_idx];
				for (idx_t i = 0; i < gstate.column_ids.size(); i++) {
					WriteObjectLayoutColumn(output.data[i], gstate.column_ids[i], output_count, row, current_device);
				}
				lstate.pending_results_idx++;
				output_count++;
			}

			if (output_count >= STANDARD_VECTOR_SIZE) {
				return true;
			}

			LustreFID fid;
			vector<LustreLayoutComponent> layouts;
			vector<LustreOSTObject> objects;
			if (!lstate.scanner->GetNextInodeLayoutObjects(fid, layouts, objects, gstate.scan_config,
			                                               lstate.block_group_max_ino)) {
				lstate.block_group_active = false;
				gstate.active_block_groups.fetch_sub(1);
				break;
			}

			lstate.pending_results.clear();
			lstate.pending_results_idx = 0;
			ExpandObjectLayoutRows(lstate.pending_results, fid, objects, layouts, gstate.filters.get(), current_device);
		}
	}

	return true;
}

static void LustreObjectLayoutsExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &gstate = data_p.global_state->Cast<LustreObjectLayoutsGlobalState>();
	auto &lstate = data_p.local_state->Cast<LustreObjectLayoutsLocalState>();

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
			ResetObjectLayoutsBlockGroupState(lstate);
			lstate.initialized_device_path.clear();
		}
	}

	if (gstate.finished) {
		output.SetCardinality(0);
		return;
	}

	idx_t output_count = 0;
	while (output_count < STANDARD_VECTOR_SIZE) {
		if (!EnsureObjectLayoutsScanner(gstate, lstate, gstate.use_sequential_scan)) {
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
			    (!gstate.use_sequential_scan ||
			     (gstate.next_block_group.load() >= gstate.total_block_groups &&
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
			ResetObjectLayoutsBlockGroupState(lstate);
			lstate.initialized_device_path.clear();
			lstate.pending_results.clear();
			lstate.pending_results_idx = 0;
		}
	}

	output.SetCardinality(output_count);
}

static unique_ptr<NodeStatistics> LustreObjectLayoutsCardinality(ClientContext &context,
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

TableFunction LustreObjectLayoutsFunction::GetFunction(bool multi_device) {
	TableFunction func("lustre_object_layouts_internal",
	                   {multi_device ? LogicalType::LIST(LogicalType::VARCHAR) : LogicalType::VARCHAR},
	                   LustreObjectLayoutsExecute,
	                   multi_device ? LustreObjectLayoutsBindMulti : LustreObjectLayoutsBindSingle,
	                   LustreObjectLayoutsInitGlobal, LustreObjectLayoutsInitLocal);
	func.named_parameters["skip_no_fid"] = LogicalType::BOOLEAN;
	func.named_parameters["skip_no_linkea"] = LogicalType::BOOLEAN;
	func.projection_pushdown = true;
	func.filter_pushdown = true;
	func.table_scan_progress = LustreScanProgress;
	func.cardinality = LustreObjectLayoutsCardinality;
	return func;
}

TableFunctionSet LustreObjectLayoutsFunction::GetFunctionSet() {
	TableFunctionSet set("lustre_object_layouts_internal");
	set.AddFunction(GetFunction(false));
	set.AddFunction(GetFunction(true));
	return set;
}

} // namespace lustre
} // namespace duckdb
