//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_links.cpp
//
// Table function implementation for querying Lustre MDT hard link information
// Uses filter pushdown with OI (Object Index) B-tree lookup for targeted reads
//===----------------------------------------------------------------------===//

#include "lustre_links.hpp"
#include "mdt_scanner.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {
namespace lustre {

static constexpr idx_t LINK_EXACT_LOOKUP_BATCH_SIZE = 512;
static constexpr idx_t LINK_SEQ_SCAN_THRESHOLD = 8192;

static void SortAndUniqueFIDs(vector<LustreFID> &fid_values) {
	std::sort(fid_values.begin(), fid_values.end());
	fid_values.erase(std::unique(fid_values.begin(), fid_values.end()), fid_values.end());
}

static bool UseFIDPath(const LinkFilter &filter) {
	return filter.HasFIDPredicate() || filter.HasFIDFilter();
}

static bool ShouldUseSequentialScan(const LinkFilter &filter) {
	if (filter.RequiresGenericEvaluation()) {
		return true;
	}
	if (filter.HasFIDPredicate() && filter.HasParentFIDPredicate()) {
		return true;
	}
	if (UseFIDPath(filter)) {
		return !filter.HasFIDFilter() || filter.fid_values.size() > LINK_SEQ_SCAN_THRESHOLD;
	}
	return !filter.HasParentFIDFilter() || filter.parent_fid_values.size() > LINK_SEQ_SCAN_THRESHOLD;
}

//===----------------------------------------------------------------------===//
// Column Definitions
//===----------------------------------------------------------------------===//

static const vector<string> LINK_COLUMN_NAMES = {
    "fid",         // Lustre FID of the inode
    "parent_fid",  // Parent directory FID
    "name",        // File name in parent directory
    "device"       // Source device path
};

static const vector<LogicalType> LINK_COLUMN_TYPES = {
    LogicalType::VARCHAR,  // fid
    LogicalType::VARCHAR,  // parent_fid
    LogicalType::VARCHAR,  // name
    LogicalType::VARCHAR   // device
};

//===----------------------------------------------------------------------===//
// LinkFilter Implementation
//===----------------------------------------------------------------------===//

static void ParseLinkFilterColumn(idx_t actual_col, const TableFilter &filter,
                                  vector<LustreFID> &fid_values, vector<LustreFID> &parent_fid_values,
                                  vector<shared_ptr<DynamicFilterData>> *dynamic_fid_filters = nullptr,
                                  vector<shared_ptr<DynamicFilterData>> *dynamic_parent_fid_filters = nullptr) {
	// Only process fid (col 0) and parent_fid (col 1)
	if (actual_col != static_cast<idx_t>(LinkColumnIdx::FID) &&
	    actual_col != static_cast<idx_t>(LinkColumnIdx::PARENT_FID)) {
		return;
	}

	auto &target = (actual_col == static_cast<idx_t>(LinkColumnIdx::FID))
	                   ? fid_values
	                   : parent_fid_values;

	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &const_filter = filter.Cast<ConstantFilter>();
		// Only handle equality
		if (const_filter.comparison_type != ExpressionType::COMPARE_EQUAL) {
			return;
		}
		auto str = StringValue::Get(const_filter.constant);
		LustreFID fid;
		if (LustreFID::FromString(str, fid)) {
			target.push_back(fid);
		}
		break;
	}

	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		for (const auto &val : in_filter.values) {
			auto str = StringValue::Get(val);
			LustreFID fid;
			if (LustreFID::FromString(str, fid)) {
				target.push_back(fid);
			}
		}
		break;
	}

	case TableFilterType::CONJUNCTION_AND: {
		auto &and_filter = filter.Cast<ConjunctionAndFilter>();
		for (const auto &child : and_filter.child_filters) {
			ParseLinkFilterColumn(actual_col, *child, fid_values, parent_fid_values,
			                      dynamic_fid_filters, dynamic_parent_fid_filters);
		}
		break;
	}

	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		if (optional_filter.child_filter) {
			ParseLinkFilterColumn(actual_col, *optional_filter.child_filter, fid_values, parent_fid_values,
			                      dynamic_fid_filters, dynamic_parent_fid_filters);
		}
		break;
	}

	case TableFilterType::DYNAMIC_FILTER: {
		auto &dynamic_filter = filter.Cast<DynamicFilter>();
		if (dynamic_filter.filter_data) {
			lock_guard<mutex> lock(dynamic_filter.filter_data->lock);
			if (dynamic_filter.filter_data->initialized && dynamic_filter.filter_data->filter) {
				ParseLinkFilterColumn(actual_col, *dynamic_filter.filter_data->filter,
				                      fid_values, parent_fid_values,
				                      dynamic_fid_filters, dynamic_parent_fid_filters);
			} else {
				// Not yet initialized — store for deferred resolution
				if (actual_col == static_cast<idx_t>(LinkColumnIdx::FID) && dynamic_fid_filters) {
					dynamic_fid_filters->push_back(dynamic_filter.filter_data);
				} else if (actual_col == static_cast<idx_t>(LinkColumnIdx::PARENT_FID) && dynamic_parent_fid_filters) {
					dynamic_parent_fid_filters->push_back(dynamic_filter.filter_data);
				}
			}
		}
		break;
	}

	default:
		// Other filter types (OR, IS_NULL, !=, <, >) are ignored.
		// DuckDB will apply them as post-filters.
		break;
	}
}

unique_ptr<LinkFilter> LinkFilter::Create(const TableFilterSet *filters, const vector<idx_t> &column_ids) {
	auto result = make_uniq<LinkFilter>();

	if (!filters) {
		return result;
	}

	for (const auto &entry : filters->filters) {
		idx_t filter_col_idx = entry.first;
		if (filter_col_idx >= column_ids.size()) {
			continue;
		}
		idx_t actual_column_idx = column_ids[filter_col_idx];
		if (actual_column_idx == static_cast<idx_t>(LinkColumnIdx::FID) && !result->fid_predicate) {
			result->fid_predicate = LustreStringFilter::Create(*entry.second);
		}
		if (actual_column_idx == static_cast<idx_t>(LinkColumnIdx::PARENT_FID) && !result->parent_fid_predicate) {
			result->parent_fid_predicate = LustreStringFilter::Create(*entry.second);
		}
		ParseLinkFilterColumn(actual_column_idx, *entry.second,
		                      result->fid_values, result->parent_fid_values,
		                      &result->dynamic_fid_filters, &result->dynamic_parent_fid_filters);
	}

	SortAndUniqueFIDs(result->fid_values);
	SortAndUniqueFIDs(result->parent_fid_values);
	result->static_fid_count = result->fid_values.size();
	result->static_parent_fid_count = result->parent_fid_values.size();

	return result;
}

bool LinkFilter::ResolveDynamicFilters() {
	// Save previous dynamic values for change detection
	vector<LustreFID> old_dynamic_fids(fid_values.begin() + static_fid_count, fid_values.end());
	vector<LustreFID> old_dynamic_pfids(parent_fid_values.begin() + static_parent_fid_count, parent_fid_values.end());

	// Roll back to static values
	fid_values.resize(static_fid_count);
	parent_fid_values.resize(static_parent_fid_count);

	// Re-parse dynamic filters (pointers are never cleared)
	for (auto &fd : dynamic_fid_filters) {
		if (!fd) {
			continue;
		}
		lock_guard<mutex> lock(fd->lock);
		if (fd->initialized && fd->filter) {
			ParseLinkFilterColumn(static_cast<idx_t>(LinkColumnIdx::FID),
			                      *fd->filter, fid_values, parent_fid_values);
		}
	}
	for (auto &fd : dynamic_parent_fid_filters) {
		if (!fd) {
			continue;
		}
		lock_guard<mutex> lock(fd->lock);
		if (fd->initialized && fd->filter) {
			ParseLinkFilterColumn(static_cast<idx_t>(LinkColumnIdx::PARENT_FID),
			                      *fd->filter, fid_values, parent_fid_values);
		}
	}

	SortAndUniqueFIDs(fid_values);
	SortAndUniqueFIDs(parent_fid_values);
	// Detect whether values changed
	vector<LustreFID> new_dynamic_fids(fid_values.begin() + static_fid_count, fid_values.end());
	vector<LustreFID> new_dynamic_pfids(parent_fid_values.begin() + static_parent_fid_count, parent_fid_values.end());
	return old_dynamic_fids != new_dynamic_fids || old_dynamic_pfids != new_dynamic_pfids;
}

//===----------------------------------------------------------------------===//
// Bind Functions
//===----------------------------------------------------------------------===//

static unique_ptr<FunctionData> LustreLinksBindSingle(ClientContext &context,
                                                      TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types,
                                                      vector<string> &names) {
	auto result = make_uniq<LustreQueryBindData>();
	result->device_paths.push_back(StringValue::Get(input.inputs[0]));
	ParseNamedParameters(input.named_parameters, result->scan_config);
	names = LINK_COLUMN_NAMES;
	return_types = LINK_COLUMN_TYPES;
	return std::move(result);
}

static unique_ptr<FunctionData> LustreLinksBindMulti(ClientContext &context,
                                                     TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types,
                                                     vector<string> &names) {
	auto result = make_uniq<LustreQueryBindData>();
	auto &list_values = ListValue::GetChildren(input.inputs[0]);
	for (auto &val : list_values) {
		result->device_paths.push_back(StringValue::Get(val));
	}
	if (result->device_paths.empty()) {
		throw BinderException("lustre_links requires at least one device path");
	}
	ParseNamedParameters(input.named_parameters, result->scan_config);
	names = LINK_COLUMN_NAMES;
	return_types = LINK_COLUMN_TYPES;
	return std::move(result);
}

//===----------------------------------------------------------------------===//
// Init Functions
//===----------------------------------------------------------------------===//

static unique_ptr<GlobalTableFunctionState> LustreLinksInitGlobal(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<LustreQueryBindData>();

	auto gstate = make_uniq<LustreLinksGlobalState>();
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

	// Parse filters from WHERE clause
	gstate->link_filter = LinkFilter::Create(input.filters.get(), input.column_ids);
	gstate->use_sequential_scan = gstate->link_filter && ShouldUseSequentialScan(*gstate->link_filter);
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

static unique_ptr<LocalTableFunctionState> LustreLinksInitLocal(ExecutionContext &context,
                                                                 TableFunctionInitInput &input,
                                                                 GlobalTableFunctionState *global_state) {
	return make_uniq<LustreLinksLocalState>();
}

//===----------------------------------------------------------------------===//
// Helper: Write a link output row
//===----------------------------------------------------------------------===//

static void WriteLinkRow(DataChunk &output, idx_t row_idx, const vector<idx_t> &column_ids,
                         const LustreLink &link, const string &device_path) {
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto &vec = output.data[i];
		switch (column_ids[i]) {
		case 0: { // fid (VARCHAR)
			if (link.fid.IsValid()) {
				auto fid_str = link.fid.ToString();
				FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, fid_str);
			} else {
				FlatVector::SetNull(vec, row_idx, true);
			}
			break;
		}
		case 1: { // parent_fid (VARCHAR)
			auto pfid_str = link.parent_fid.ToString();
			FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, pfid_str);
			break;
		}
		case 2: { // name (VARCHAR)
			FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, link.name);
			break;
		}
		case 3: { // device (VARCHAR)
			FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, device_path);
			break;
		}
		default:
			break;
		}
	}
}

static void ResetLinksBlockGroupState(LustreLinksLocalState &lstate) {
	lstate.block_group_active = false;
	lstate.block_group_max_ino = 0;
	lstate.next_block_group_in_batch = 0;
	lstate.block_group_batch_end = 0;
}

//===----------------------------------------------------------------------===//
// Helper: Ensure local scanner is initialized for the current device
//===----------------------------------------------------------------------===//

static bool InitializeLinksDevice(LustreLinksGlobalState &gstate, idx_t dev_idx) {
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

static bool EnsureLinksLocalScanner(LustreLinksGlobalState &gstate, LustreLinksLocalState &lstate,
                                    bool sequential_scan) {
	idx_t dev_idx = gstate.current_device_idx.load();

	if (lstate.scanner_initialized && lstate.initialized_device_idx == dev_idx &&
	    lstate.sequential_scan_mode == sequential_scan) {
		return true;
	}

	if (lstate.scanner_initialized) {
		lstate.scanner->Close();
		lstate.scanner_initialized = false;
		ResetLinksBlockGroupState(lstate);
	}

	if (dev_idx >= gstate.device_paths.size() || gstate.finished.load()) {
		return false;
	}

	// Ensure device metadata is initialized (one thread does this)
	if (!gstate.device_initialized.load()) {
		lock_guard<mutex> lock(gstate.device_transition_lock);
		if (!gstate.device_initialized.load()) {
			if (!InitializeLinksDevice(gstate, dev_idx)) {
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
	lstate.sequential_scan_mode = sequential_scan;
	ResetLinksBlockGroupState(lstate);
	return true;
}

static bool ClaimNextLinksBlockGroup(LustreLinksGlobalState &gstate, LustreLinksLocalState &lstate) {
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

static bool MatchesLinkPredicate(const LinkFilter &filter, const LustreLink &link) {
	if (!filter.ContainsFID(link.fid)) {
		return false;
	}
	if (!filter.ContainsParentFID(link.parent_fid)) {
		return false;
	}
	if (filter.fid_predicate && filter.fid_predicate->RequiresGenericEvaluation() && !filter.EvaluateFID(link.fid)) {
		return false;
	}
	if (filter.parent_fid_predicate && filter.parent_fid_predicate->RequiresGenericEvaluation() &&
	    !filter.EvaluateParentFID(link.parent_fid)) {
		return false;
	}
	return true;
}

//===----------------------------------------------------------------------===//
// FID Path: batched lookup by fid → OI → inode → LinkEA → rows
//===----------------------------------------------------------------------===//

static bool ExecuteExactFIDPath(LustreLinksGlobalState &gstate, LustreLinksLocalState &lstate,
                                DataChunk &output, idx_t &output_count) {
	auto &filter = *gstate.link_filter;
	const auto &current_device = gstate.device_paths[lstate.initialized_device_idx];
	struct LookupEntry {
		ext2_ino_t ino;
		LustreFID fid;
	};

	while (output_count < STANDARD_VECTOR_SIZE) {
		// Drain pending results first
		while (lstate.pending_results_idx < lstate.pending_results.size() &&
		       output_count < STANDARD_VECTOR_SIZE) {
			WriteLinkRow(output, output_count, gstate.column_ids,
			             lstate.pending_results[lstate.pending_results_idx], current_device);
			lstate.pending_results_idx++;
			output_count++;
		}

		if (output_count >= STANDARD_VECTOR_SIZE) {
			return true;
		}

		vector<LookupEntry> lookup_batch;
		lookup_batch.reserve(LINK_EXACT_LOOKUP_BATCH_SIZE);

		while (lookup_batch.size() < LINK_EXACT_LOOKUP_BATCH_SIZE) {
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
			std::vector<LinkEntry> links;
			if (!lstate.scanner->ReadInodeLinkEA(entry.ino, actual_fid, links)) {
				continue;
			}

			for (auto &link_entry : links) {
				LustreLink link;
				link.fid = actual_fid;
				link.parent_fid = link_entry.parent_fid;
				link.name = link_entry.name;
				lstate.pending_results.push_back(std::move(link));
			}
		}
	}

	return true;
}

//===----------------------------------------------------------------------===//
// Parent FID Path: parent_fid → OI → dir inode → dir entries → child FIDs
//===----------------------------------------------------------------------===//

static bool ExecuteExactParentFIDPath(LustreLinksGlobalState &gstate, LustreLinksLocalState &lstate,
                                      DataChunk &output, idx_t &output_count) {
	auto &filter = *gstate.link_filter;
	const auto &current_device = gstate.device_paths[lstate.initialized_device_idx];
	struct LookupEntry {
		ext2_ino_t ino;
		LustreFID fid;
	};

	while (output_count < STANDARD_VECTOR_SIZE) {
		// Drain pending results first
		while (lstate.pending_results_idx < lstate.pending_results.size() &&
		       output_count < STANDARD_VECTOR_SIZE) {
			WriteLinkRow(output, output_count, gstate.column_ids,
			             lstate.pending_results[lstate.pending_results_idx], current_device);
			lstate.pending_results_idx++;
			output_count++;
		}

		if (output_count >= STANDARD_VECTOR_SIZE) {
			return true;
		}

		vector<LookupEntry> lookup_batch;
		lookup_batch.reserve(LINK_EXACT_LOOKUP_BATCH_SIZE);

		while (lookup_batch.size() < LINK_EXACT_LOOKUP_BATCH_SIZE) {
			idx_t fid_idx = gstate.next_fid_idx.fetch_add(1);
			if (fid_idx >= filter.parent_fid_values.size()) {
				break;
			}

			auto &parent_fid = filter.parent_fid_values[fid_idx];
			ext2_ino_t dir_ino;
			if (!lstate.scanner->LookupFID(parent_fid, dir_ino)) {
				continue;
			}
			lookup_batch.push_back({dir_ino, parent_fid});
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

		for (auto &lookup : lookup_batch) {
			if (have_last_ino && lookup.ino == last_ino) {
				continue;
			}
			have_last_ino = true;
			last_ino = lookup.ino;

			std::vector<DirEntry> entries;
			if (!lstate.scanner->ReadDirectoryEntries(lookup.ino, entries)) {
				continue;
			}

			std::sort(entries.begin(), entries.end(), [](const DirEntry &left, const DirEntry &right) {
				return left.ino < right.ino;
			});

			for (auto &entry : entries) {
				LustreFID child_fid;
				if (!lstate.scanner->ReadInodeFID(entry.ino, child_fid)) {
					continue;
				}

				LustreLink link;
				link.fid = child_fid;
				link.parent_fid = lookup.fid;
				link.name = entry.name;
				lstate.pending_results.push_back(std::move(link));
			}
		}
	}

	return true;
}

//===----------------------------------------------------------------------===//
// Sequential Scan Path - linear scan by block group
//===----------------------------------------------------------------------===//

static bool ExecuteSequentialPath(LustreLinksGlobalState &gstate, LustreLinksLocalState &lstate,
                                  DataChunk &output, idx_t &output_count) {
	auto &filter = *gstate.link_filter;
	const auto &current_device = gstate.device_paths[lstate.initialized_device_idx];

	while (output_count < STANDARD_VECTOR_SIZE) {
		if (!ClaimNextLinksBlockGroup(gstate, lstate)) {
			return false;
		}

		while (output_count < STANDARD_VECTOR_SIZE) {
			LustreLink link;
			if (!lstate.scanner->GetNextLink(link, gstate.scan_config, lstate.block_group_max_ino)) {
				lstate.block_group_active = false;
				gstate.active_block_groups.fetch_sub(1);
				break;
			}
			if (!MatchesLinkPredicate(filter, link)) {
				continue;
			}

			WriteLinkRow(output, output_count, gstate.column_ids, link, current_device);
			output_count++;
		}
	}

	return true;
}

//===----------------------------------------------------------------------===//
// Execute Function
//===----------------------------------------------------------------------===//

static void LustreLinksExecute(ClientContext &context, TableFunctionInput &data_p,
                               DataChunk &output) {
	auto &gstate = data_p.global_state->Cast<LustreLinksGlobalState>();
	auto &lstate = data_p.local_state->Cast<LustreLinksLocalState>();

	// Re-evaluate dynamic filters on every call (single-threaded when dynamic filters present)
	if (gstate.link_filter->HasDynamicFilter()) {
		bool changed = gstate.link_filter->ResolveDynamicFilters();
		if (changed) {
			gstate.use_sequential_scan = ShouldUseSequentialScan(*gstate.link_filter);
			// Filter values changed — reset scan state
			gstate.finished = false;
			gstate.next_fid_idx.store(0);
			gstate.next_block_group.store(0);
			gstate.active_block_groups.store(0);
			lstate.pending_results.clear();
			lstate.pending_results_idx = 0;
			gstate.current_device_idx = 0;
			gstate.device_initialized = false;
			lstate.scanner_initialized = false;
			ResetLinksBlockGroupState(lstate);
		}
	}

	if (gstate.finished) {
		output.SetCardinality(0);
		return;
	}

	idx_t output_count = 0;
	bool use_fid_path = UseFIDPath(*gstate.link_filter);

	while (output_count < STANDARD_VECTOR_SIZE) {
		if (!EnsureLinksLocalScanner(gstate, lstate, gstate.use_sequential_scan)) {
			break;
		}

		// Execute the appropriate path
		bool has_more;
		if (gstate.use_sequential_scan) {
			has_more = ExecuteSequentialPath(gstate, lstate, output, output_count);
		} else if (use_fid_path) {
			has_more = ExecuteExactFIDPath(gstate, lstate, output, output_count);
		} else {
			has_more = ExecuteExactParentFIDPath(gstate, lstate, output, output_count);
		}

		if (!has_more) {
			if (gstate.use_sequential_scan && gstate.active_block_groups.load() != 0) {
				break;
			}
			// Current device exhausted, move to next
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
			ResetLinksBlockGroupState(lstate);
		}
	}

	output.SetCardinality(output_count);
}

//===----------------------------------------------------------------------===//
// Cardinality Function
//===----------------------------------------------------------------------===//

static unique_ptr<NodeStatistics> LustreLinksCardinality(ClientContext &context,
                                                         const FunctionData *bind_data_p) {
	// Return a very large cardinality estimate to ensure the optimizer places
	// lustre_links on the PROBE side of hash joins. This enables DynamicFilter
	// pushdown from the BUILD side (e.g., REC_CTE_SCAN) into lustre_links,
	// which is critical for recursive CTE support.
	return make_uniq<NodeStatistics>(1000000000000);
}

//===----------------------------------------------------------------------===//
// GetFunctionSet
//===----------------------------------------------------------------------===//

static void SetLinkCommonProperties(TableFunction &func) {
	func.named_parameters["skip_no_fid"] = LogicalType::BOOLEAN;
	func.named_parameters["skip_no_linkea"] = LogicalType::BOOLEAN;
	func.projection_pushdown = true;
	func.filter_pushdown = true;
	func.cardinality = LustreLinksCardinality;
}

TableFunctionSet LustreLinksFunction::GetFunctionSet() {
	TableFunctionSet set("lustre_links");

	// Overload 1: single device path (VARCHAR)
	TableFunction single_func("lustre_links", {LogicalType::VARCHAR}, LustreLinksExecute,
	                          LustreLinksBindSingle, LustreLinksInitGlobal, LustreLinksInitLocal);
	SetLinkCommonProperties(single_func);
	set.AddFunction(std::move(single_func));

	// Overload 2: multiple device paths (LIST(VARCHAR))
	TableFunction multi_func("lustre_links", {LogicalType::LIST(LogicalType::VARCHAR)}, LustreLinksExecute,
	                         LustreLinksBindMulti, LustreLinksInitGlobal, LustreLinksInitLocal);
	SetLinkCommonProperties(multi_func);
	set.AddFunction(std::move(multi_func));

	return set;
}

} // namespace lustre
} // namespace duckdb
