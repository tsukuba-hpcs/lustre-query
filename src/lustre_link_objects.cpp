//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_link_objects.cpp
//
// Internal fused table function for link/object joins
//===----------------------------------------------------------------------===//

#include "lustre_link_objects.hpp"

#include "lustre_fid_filter.hpp"
#include "lustre_links.hpp"
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

static constexpr idx_t LINK_OBJECT_EXACT_LOOKUP_BATCH_SIZE = 512;
static constexpr idx_t LINK_OBJECT_SEQ_SCAN_THRESHOLD = 8192;

static const vector<string> LINK_OBJECT_COLUMN_NAMES = {
    "link_fid", "parent_fid", "name", "object_fid", "comp_index",
    "stripe_index", "ost_idx", "ost_oi_id", "ost_oi_seq", "object_device"};

static const vector<LogicalType> LINK_OBJECT_COLUMN_TYPES = {
    LogicalType::VARCHAR,  LogicalType::VARCHAR, LogicalType::VARCHAR,  LogicalType::VARCHAR, LogicalType::UINTEGER,
    LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::VARCHAR};

struct LustreLinkObjectRow {
	LustreFID fid;
	LustreFID parent_fid;
	string name;
	LustreOSTObject object;
};

struct LustreLinkObjectsFilter {
	unique_ptr<LinkFilter> link_filter;
	unique_ptr<LustreStringFilter> name_predicate;
	unique_ptr<FIDOnlyFilter> object_fid_filter;
	unique_ptr<LustreStringFilter> object_device_predicate;
	map<idx_t, unique_ptr<TableFilter>> object_filters;
	vector<LustreFID> lookup_fids;

	bool HasDynamicFilter() const {
		return (link_filter && link_filter->HasDynamicFilter()) ||
		       (object_fid_filter && object_fid_filter->HasDynamicFilter()) ||
		       std::any_of(object_filters.begin(), object_filters.end(), [](const pair<const idx_t, unique_ptr<TableFilter>> &entry) {
			       return TableFilterHasDynamicFilter(*entry.second);
		       });
	}

	bool HasAnyFilter() const {
		return (link_filter && link_filter->HasAnyFilter()) || (name_predicate && name_predicate->HasPredicate()) ||
		       (object_fid_filter && object_fid_filter->HasAnyFilter()) ||
		       (object_device_predicate && object_device_predicate->HasPredicate()) || !object_filters.empty();
	}

	bool UseSequentialScan() const {
		return lookup_fids.empty() || lookup_fids.size() > LINK_OBJECT_SEQ_SCAN_THRESHOLD;
	}

	bool ResolveDynamicFilters() {
		bool changed = false;
		if (link_filter && link_filter->HasDynamicFilter()) {
			changed |= link_filter->ResolveDynamicFilters();
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
		bool has_link_fids = link_filter && !link_filter->fid_values.empty();
		bool has_object_fids = object_fid_filter && !object_fid_filter->fid_values.empty();

		if (has_link_fids && has_object_fids) {
			lookup_fids.reserve(MinValue(link_filter->fid_values.size(), object_fid_filter->fid_values.size()));
			std::set_intersection(link_filter->fid_values.begin(), link_filter->fid_values.end(),
			                      object_fid_filter->fid_values.begin(), object_fid_filter->fid_values.end(),
			                      std::back_inserter(lookup_fids));
		} else if (has_link_fids) {
			lookup_fids.insert(lookup_fids.end(), link_filter->fid_values.begin(), link_filter->fid_values.end());
		} else if (has_object_fids) {
			lookup_fids.insert(lookup_fids.end(), object_fid_filter->fid_values.begin(), object_fid_filter->fid_values.end());
		}
		std::sort(lookup_fids.begin(), lookup_fids.end());
		lookup_fids.erase(std::unique(lookup_fids.begin(), lookup_fids.end()), lookup_fids.end());
	}

	Value GetObjectValue(idx_t actual_column_idx, const LustreOSTObject &object, const string &device_path) const {
		switch (actual_column_idx) {
		case 3:
			return Value();
		case 4:
			return Value::UINTEGER(object.comp_index);
		case 5:
			return Value::UINTEGER(object.stripe_index);
		case 6:
			return Value::UINTEGER(object.ost_idx);
		case 7:
			return Value::UBIGINT(object.ost_oi_id);
		case 8:
			return Value::UBIGINT(object.ost_oi_seq);
		case 9:
			return Value(device_path);
		default:
			return Value();
		}
	}

	bool MatchesFID(const LustreFID &fid) const {
		if (link_filter) {
			if (!link_filter->ContainsFID(fid)) {
				return false;
			}
			if (link_filter->fid_predicate && link_filter->fid_predicate->RequiresGenericEvaluation() &&
			    !link_filter->EvaluateFID(fid)) {
				return false;
			}
		}
		if (object_fid_filter) {
			if (!object_fid_filter->ContainsFID(fid)) {
				return false;
			}
			if (object_fid_filter->RequiresGenericEvaluation() && !object_fid_filter->EvaluateFID(fid)) {
				return false;
			}
		}
		return true;
	}

	bool MatchesLink(const LustreFID &fid, const LinkEntry &link) const {
		if (link_filter) {
			if (!link_filter->ContainsFID(fid) || !link_filter->ContainsParentFID(link.parent_fid)) {
				return false;
			}
			if (link_filter->parent_fid_predicate && link_filter->parent_fid_predicate->RequiresGenericEvaluation() &&
			    !link_filter->EvaluateParentFID(link.parent_fid)) {
				return false;
			}
		}
		if (name_predicate && name_predicate->HasPredicate() && !name_predicate->Evaluate(&link.name)) {
			return false;
		}
		return true;
	}

	bool MatchesObject(const LustreFID &fid, const LustreOSTObject &object, const string &device_path) const {
		if (object_device_predicate && object_device_predicate->HasPredicate() &&
		    !object_device_predicate->Evaluate(&device_path)) {
			return false;
		}
		if (object_fid_filter) {
			if (!object_fid_filter->ContainsFID(fid)) {
				return false;
			}
			if (object_fid_filter->RequiresGenericEvaluation() && !object_fid_filter->EvaluateFID(fid)) {
				return false;
			}
		}
		for (const auto &entry : object_filters) {
			if (entry.first == 3) {
				if (!EvaluateTableFilterValue(*entry.second, Value(fid.ToString()))) {
					return false;
				}
				continue;
			}
			if (!EvaluateTableFilterValue(*entry.second, GetObjectValue(entry.first, object, device_path))) {
				return false;
			}
		}
		return true;
	}

	static unique_ptr<LustreLinkObjectsFilter> Create(const TableFilterSet *filters, const vector<idx_t> &column_ids) {
		auto result = make_uniq<LustreLinkObjectsFilter>();

		auto normalized_link_filters = make_uniq<TableFilterSet>();
		vector<idx_t> normalized_link_column_ids = {0, 1, 2};

		if (filters) {
			for (const auto &entry : filters->filters) {
				if (entry.first >= column_ids.size()) {
					continue;
				}
				auto actual_column_idx = column_ids[entry.first];
				switch (actual_column_idx) {
				case 0:
					normalized_link_filters->filters[0] = entry.second->Copy();
					break;
				case 1:
					normalized_link_filters->filters[1] = entry.second->Copy();
					break;
				case 2:
					normalized_link_filters->filters[2] = entry.second->Copy();
					break;
				case 9:
					if (!result->object_device_predicate) {
						result->object_device_predicate = LustreStringFilter::Create(*entry.second);
					}
					break;
				default:
					if (actual_column_idx >= 3 && actual_column_idx <= 8) {
						result->object_filters[actual_column_idx] = entry.second->Copy();
					}
					break;
				}
			}
		}

		result->link_filter = LinkFilter::Create(normalized_link_filters.get(), normalized_link_column_ids);
		auto name_entry = normalized_link_filters->filters.find(2);
		if (name_entry != normalized_link_filters->filters.end()) {
			result->name_predicate = LustreStringFilter::Create(*name_entry->second);
		}
		result->object_fid_filter = FIDOnlyFilter::Create(filters, column_ids, 3);
		result->RebuildLookupFIDs();
		return result;
	}
};

struct LustreLinkObjectsGlobalState : public GlobalTableFunctionState {
	vector<string> device_paths;
	vector<idx_t> column_ids;
	MDTScanConfig scan_config;
	unique_ptr<LustreLinkObjectsFilter> filters;
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

	LustreLinkObjectsGlobalState()
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

struct LustreLinkObjectsLocalState : public LocalTableFunctionState {
	unique_ptr<MDTScanner> scanner;
	bool scanner_initialized = false;
	idx_t initialized_device_idx = DConstants::INVALID_INDEX;
	string initialized_device_path;
	bool sequential_scan_mode = true;
	bool block_group_active = false;
	ext2_ino_t block_group_max_ino = 0;
	int next_block_group_in_batch = 0;
	int block_group_batch_end = 0;
	vector<LustreLinkObjectRow> pending_results;
	idx_t pending_results_idx = 0;

	LustreLinkObjectsLocalState() {
		scanner = make_uniq<MDTScanner>();
	}
};

const vector<string> &LustreLinkObjectsFunction::GetColumnNames() {
	return LINK_OBJECT_COLUMN_NAMES;
}

const vector<LogicalType> &LustreLinkObjectsFunction::GetColumnTypes() {
	return LINK_OBJECT_COLUMN_TYPES;
}

static unique_ptr<FunctionData> LustreLinkObjectsBindSingle(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types,
                                                            vector<string> &names) {
	auto result = make_uniq<LustreQueryBindData>();
	result->device_paths.push_back(StringValue::Get(input.inputs[0]));
	ParseNamedParameters(input.named_parameters, result->scan_config);
	EstimateCardinality(*result);
	names = LustreLinkObjectsFunction::GetColumnNames();
	return_types = LustreLinkObjectsFunction::GetColumnTypes();
	return std::move(result);
}

static unique_ptr<FunctionData> LustreLinkObjectsBindMulti(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types,
                                                           vector<string> &names) {
	auto result = make_uniq<LustreQueryBindData>();
	auto &list_values = ListValue::GetChildren(input.inputs[0]);
	for (auto &val : list_values) {
		result->device_paths.push_back(StringValue::Get(val));
	}
	if (result->device_paths.empty()) {
		throw BinderException("lustre_link_objects_internal requires at least one device path");
	}
	ParseNamedParameters(input.named_parameters, result->scan_config);
	EstimateCardinality(*result);
	names = LustreLinkObjectsFunction::GetColumnNames();
	return_types = LustreLinkObjectsFunction::GetColumnTypes();
	return std::move(result);
}

static bool InitializeLinkObjectsDevice(LustreLinkObjectsGlobalState &gstate, idx_t device_idx) {
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

static unique_ptr<GlobalTableFunctionState> LustreLinkObjectsInitGlobal(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<LustreQueryBindData>();
	auto result = make_uniq<LustreLinkObjectsGlobalState>();
	result->device_paths = bind_data.device_paths;
	result->column_ids = input.column_ids;
	result->scan_config = bind_data.scan_config;
	result->filters = LustreLinkObjectsFilter::Create(input.filters.get(), input.column_ids);
	result->use_sequential_scan = !result->filters || result->filters->UseSequentialScan();
	result->thread_count = NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());
	if (!result->device_paths.empty()) {
		InitializeLinkObjectsDevice(*result, 0);
	}
	return std::move(result);
}

static unique_ptr<LocalTableFunctionState> LustreLinkObjectsInitLocal(ExecutionContext &context,
                                                                      TableFunctionInitInput &input,
                                                                      GlobalTableFunctionState *global_state) {
	return make_uniq<LustreLinkObjectsLocalState>();
}

static void WriteLinkObjectColumn(Vector &vec, idx_t col_idx, idx_t row_idx, const LustreLinkObjectRow &row,
                                  const string &device_path) {
	switch (col_idx) {
	case 0: {
		auto fid_str = row.fid.ToString();
		FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, fid_str);
		break;
	}
	case 1: {
		auto pfid_str = row.parent_fid.ToString();
		FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, pfid_str);
		break;
	}
	case 2:
		FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, row.name);
		break;
	case 3: {
		auto fid_str = row.fid.ToString();
		FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, fid_str);
		break;
	}
	case 4:
		FlatVector::GetData<uint32_t>(vec)[row_idx] = row.object.comp_index;
		break;
	case 5:
		FlatVector::GetData<uint32_t>(vec)[row_idx] = row.object.stripe_index;
		break;
	case 6:
		FlatVector::GetData<uint32_t>(vec)[row_idx] = row.object.ost_idx;
		break;
	case 7:
		FlatVector::GetData<uint64_t>(vec)[row_idx] = row.object.ost_oi_id;
		break;
	case 8:
		FlatVector::GetData<uint64_t>(vec)[row_idx] = row.object.ost_oi_seq;
		break;
	case 9:
		FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, device_path);
		break;
	default:
		break;
	}
}

static void ExpandLinkObjectRows(vector<LustreLinkObjectRow> &out, const LustreFID &fid, const vector<LinkEntry> &links,
                                 const vector<LustreOSTObject> &objects, const LustreLinkObjectsFilter *filters,
                                 const string &device_path) {
	if (!links.size() || !objects.size()) {
		return;
	}
	if (filters && !filters->MatchesFID(fid)) {
		return;
	}

	vector<const LinkEntry *> filtered_links;
	filtered_links.reserve(links.size());
	for (const auto &link : links) {
		if (!filters || filters->MatchesLink(fid, link)) {
			filtered_links.push_back(&link);
		}
	}
	if (filtered_links.empty()) {
		return;
	}

	vector<const LustreOSTObject *> filtered_objects;
	filtered_objects.reserve(objects.size());
	for (const auto &object : objects) {
		if (!filters || filters->MatchesObject(fid, object, device_path)) {
			filtered_objects.push_back(&object);
		}
	}
	if (filtered_objects.empty()) {
		return;
	}

	for (auto link : filtered_links) {
		for (auto object : filtered_objects) {
			LustreLinkObjectRow row;
			row.fid = fid;
			row.parent_fid = link->parent_fid;
			row.name = link->name;
			row.object = *object;
			out.push_back(std::move(row));
		}
	}
}

static void ResetLinkObjectsBlockGroupState(LustreLinkObjectsLocalState &lstate) {
	lstate.block_group_active = false;
	lstate.block_group_max_ino = 0;
	lstate.next_block_group_in_batch = 0;
	lstate.block_group_batch_end = 0;
}

static bool ClaimNextLinkObjectsBlockGroup(LustreLinkObjectsGlobalState &gstate,
                                           LustreLinkObjectsLocalState &lstate) {
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

static bool EnsureLinkObjectsScanner(LustreLinkObjectsGlobalState &gstate, LustreLinkObjectsLocalState &lstate,
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
		ResetLinkObjectsBlockGroupState(lstate);
		lstate.initialized_device_path.clear();
	}

	if (dev_idx >= gstate.device_paths.size() || gstate.finished.load()) {
		return false;
	}

	if (!gstate.device_initialized.load()) {
		lock_guard<mutex> lock(gstate.device_transition_lock);
		if (!gstate.device_initialized.load()) {
			if (!InitializeLinkObjectsDevice(gstate, dev_idx)) {
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
	ResetLinkObjectsBlockGroupState(lstate);
	return true;
}

static bool ExecuteExactFIDPath(LustreLinkObjectsGlobalState &gstate, LustreLinkObjectsLocalState &lstate,
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
				WriteLinkObjectColumn(output.data[i], gstate.column_ids[i], output_count, row, current_device);
			}
			lstate.pending_results_idx++;
			output_count++;
		}

		if (output_count >= STANDARD_VECTOR_SIZE) {
			return true;
		}

		vector<LookupEntry> lookup_batch;
		lookup_batch.reserve(LINK_OBJECT_EXACT_LOOKUP_BATCH_SIZE);
		while (lookup_batch.size() < LINK_OBJECT_EXACT_LOOKUP_BATCH_SIZE) {
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

			LustreFID fid;
			vector<LinkEntry> links;
			vector<LustreOSTObject> objects;
			if (!lstate.scanner->ReadInodeLinkObjects(entry.ino, fid, links, objects, gstate.scan_config)) {
				continue;
			}
			ExpandLinkObjectRows(lstate.pending_results, fid, links, objects, gstate.filters.get(), current_device);
		}
	}

	return true;
}

static bool ExecuteSequentialPath(LustreLinkObjectsGlobalState &gstate, LustreLinkObjectsLocalState &lstate,
                                  DataChunk &output, idx_t &output_count) {
	const auto &current_device = lstate.initialized_device_path;
	while (output_count < STANDARD_VECTOR_SIZE) {
		while (lstate.pending_results_idx < lstate.pending_results.size() && output_count < STANDARD_VECTOR_SIZE) {
			auto &row = lstate.pending_results[lstate.pending_results_idx];
			for (idx_t i = 0; i < gstate.column_ids.size(); i++) {
				WriteLinkObjectColumn(output.data[i], gstate.column_ids[i], output_count, row, current_device);
			}
			lstate.pending_results_idx++;
			output_count++;
		}

		if (output_count >= STANDARD_VECTOR_SIZE) {
			return true;
		}

		if (!ClaimNextLinkObjectsBlockGroup(gstate, lstate)) {
			return false;
		}

		while (output_count < STANDARD_VECTOR_SIZE) {
			while (lstate.pending_results_idx < lstate.pending_results.size() && output_count < STANDARD_VECTOR_SIZE) {
				auto &row = lstate.pending_results[lstate.pending_results_idx];
				for (idx_t i = 0; i < gstate.column_ids.size(); i++) {
					WriteLinkObjectColumn(output.data[i], gstate.column_ids[i], output_count, row, current_device);
				}
				lstate.pending_results_idx++;
				output_count++;
			}

			if (output_count >= STANDARD_VECTOR_SIZE) {
				return true;
			}

			LustreFID fid;
			vector<LinkEntry> links;
			vector<LustreOSTObject> objects;
			if (!lstate.scanner->GetNextInodeLinkObjects(fid, links, objects, gstate.scan_config,
			                                             lstate.block_group_max_ino)) {
				lstate.block_group_active = false;
				gstate.active_block_groups.fetch_sub(1);
				break;
			}
			lstate.pending_results.clear();
			lstate.pending_results_idx = 0;
			ExpandLinkObjectRows(lstate.pending_results, fid, links, objects, gstate.filters.get(), current_device);
		}
	}

	return true;
}

static void LustreLinkObjectsExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &gstate = data_p.global_state->Cast<LustreLinkObjectsGlobalState>();
	auto &lstate = data_p.local_state->Cast<LustreLinkObjectsLocalState>();

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
			ResetLinkObjectsBlockGroupState(lstate);
			lstate.initialized_device_path.clear();
		}
	}

	if (gstate.finished) {
		output.SetCardinality(0);
		return;
	}

	idx_t output_count = 0;
	while (output_count < STANDARD_VECTOR_SIZE) {
		if (!EnsureLinkObjectsScanner(gstate, lstate, gstate.use_sequential_scan)) {
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
			ResetLinkObjectsBlockGroupState(lstate);
			lstate.initialized_device_path.clear();
			lstate.pending_results.clear();
			lstate.pending_results_idx = 0;
		}
	}

	output.SetCardinality(output_count);
}

static unique_ptr<NodeStatistics> LustreLinkObjectsCardinality(ClientContext &context,
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

TableFunction LustreLinkObjectsFunction::GetFunction(bool multi_device) {
	TableFunction func("lustre_link_objects_internal",
	                   {multi_device ? LogicalType::LIST(LogicalType::VARCHAR) : LogicalType::VARCHAR},
	                   LustreLinkObjectsExecute,
	                   multi_device ? LustreLinkObjectsBindMulti : LustreLinkObjectsBindSingle,
	                   LustreLinkObjectsInitGlobal, LustreLinkObjectsInitLocal);
	func.named_parameters["skip_no_fid"] = LogicalType::BOOLEAN;
	func.named_parameters["skip_no_linkea"] = LogicalType::BOOLEAN;
	func.projection_pushdown = true;
	func.filter_pushdown = true;
	func.table_scan_progress = LustreScanProgress;
	func.cardinality = LustreLinkObjectsCardinality;
	return func;
}

TableFunctionSet LustreLinkObjectsFunction::GetFunctionSet() {
	TableFunctionSet set("lustre_link_objects_internal");
	set.AddFunction(GetFunction(false));
	set.AddFunction(GetFunction(true));
	return set;
}

} // namespace lustre
} // namespace duckdb
