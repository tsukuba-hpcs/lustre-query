//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_inode_links.cpp
//
// Internal fused table function for inode/link joins
//===----------------------------------------------------------------------===//

#include "lustre_inode_links.hpp"

#include "lustre_fid_filter.hpp"
#include "lustre_filter.hpp"
#include "lustre_links.hpp"
#include "lustre_scan_state.hpp"
#include "lustre_string_filter.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"

#include <algorithm>
#include <iterator>
#include <map>

namespace duckdb {
namespace lustre {

static constexpr idx_t INODE_LINK_EXACT_LOOKUP_BATCH_SIZE = 512;
static constexpr idx_t INODE_LINK_SEQ_SCAN_THRESHOLD = 8192;

static const vector<string> INODE_LINK_COLUMN_NAMES = {
    "inode_fid", "ino", "type", "mode", "nlink", "uid", "gid", "size", "blocks",
    "atime", "mtime", "ctime", "projid", "flags", "device", "link_fid", "parent_fid", "name"};

static const vector<LogicalType> INODE_LINK_COLUMN_TYPES = {
    LogicalType::VARCHAR,   LogicalType::UBIGINT,   LogicalType::VARCHAR,   LogicalType::UINTEGER,
    LogicalType::UINTEGER,  LogicalType::UINTEGER,  LogicalType::UINTEGER,  LogicalType::UBIGINT,
    LogicalType::UBIGINT,   LogicalType::TIMESTAMP, LogicalType::TIMESTAMP, LogicalType::TIMESTAMP,
    LogicalType::UINTEGER,  LogicalType::UINTEGER,  LogicalType::VARCHAR,   LogicalType::VARCHAR,
    LogicalType::VARCHAR,   LogicalType::VARCHAR};

struct LustreInodeLinksFilter {
	unique_ptr<LustreFilterSet> inode_filters;
	unique_ptr<FIDOnlyFilter> inode_fid_filter;
	unique_ptr<LinkFilter> link_filter;
	unique_ptr<LustreStringFilter> name_predicate;
	unique_ptr<LustreStringFilter> device_predicate;
	bool join_on_parent_fid = false;
	bool use_child_lookup = false;
	vector<LustreFID> lookup_fids;

	bool HasDynamicFilter() const {
		return (inode_fid_filter && inode_fid_filter->HasDynamicFilter()) ||
		       (link_filter && link_filter->HasDynamicFilter());
	}

	bool HasAnyFilter() const {
		return (inode_filters && inode_filters->HasFilters()) ||
		       (inode_fid_filter && inode_fid_filter->HasAnyFilter()) ||
		       (link_filter && link_filter->HasAnyFilter()) ||
		       (name_predicate && name_predicate->HasPredicate()) ||
		       (device_predicate && device_predicate->HasPredicate());
	}

	bool UseSequentialScan() const {
		return lookup_fids.empty() || lookup_fids.size() > INODE_LINK_SEQ_SCAN_THRESHOLD;
	}

	bool ResolveDynamicFilters() {
		bool changed = false;
		if (inode_fid_filter && inode_fid_filter->HasDynamicFilter()) {
			changed |= inode_fid_filter->ResolveDynamicFilters();
		}
		if (link_filter && link_filter->HasDynamicFilter()) {
			changed |= link_filter->ResolveDynamicFilters();
		}
		if (changed) {
			RebuildLookupFIDs();
		}
		return changed;
	}

	void RebuildLookupFIDs() {
		lookup_fids.clear();
		bool has_inode_fids = inode_fid_filter && !inode_fid_filter->fid_values.empty();
		bool has_link_fids = link_filter && !link_filter->fid_values.empty();
		use_child_lookup = false;

		if (!join_on_parent_fid) {
			if (has_inode_fids && has_link_fids) {
				lookup_fids.reserve(MinValue(inode_fid_filter->fid_values.size(), link_filter->fid_values.size()));
				std::set_intersection(inode_fid_filter->fid_values.begin(), inode_fid_filter->fid_values.end(),
				                      link_filter->fid_values.begin(), link_filter->fid_values.end(),
				                      std::back_inserter(lookup_fids));
			} else if (has_inode_fids) {
				lookup_fids.insert(lookup_fids.end(), inode_fid_filter->fid_values.begin(), inode_fid_filter->fid_values.end());
			} else if (has_link_fids) {
				lookup_fids.insert(lookup_fids.end(), link_filter->fid_values.begin(), link_filter->fid_values.end());
			}
			std::sort(lookup_fids.begin(), lookup_fids.end());
			lookup_fids.erase(std::unique(lookup_fids.begin(), lookup_fids.end()), lookup_fids.end());
			return;
		}

		vector<LustreFID> parent_lookup_fids;
		bool has_link_parent_fids = link_filter && !link_filter->parent_fid_values.empty();
		if (has_inode_fids && has_link_parent_fids) {
			parent_lookup_fids.reserve(
			    MinValue(inode_fid_filter->fid_values.size(), link_filter->parent_fid_values.size()));
			std::set_intersection(inode_fid_filter->fid_values.begin(), inode_fid_filter->fid_values.end(),
			                      link_filter->parent_fid_values.begin(), link_filter->parent_fid_values.end(),
			                      std::back_inserter(parent_lookup_fids));
		} else if (has_inode_fids) {
			parent_lookup_fids.insert(parent_lookup_fids.end(), inode_fid_filter->fid_values.begin(),
			                          inode_fid_filter->fid_values.end());
		} else if (has_link_parent_fids) {
			parent_lookup_fids.insert(parent_lookup_fids.end(), link_filter->parent_fid_values.begin(),
			                          link_filter->parent_fid_values.end());
		}
		std::sort(parent_lookup_fids.begin(), parent_lookup_fids.end());
		parent_lookup_fids.erase(std::unique(parent_lookup_fids.begin(), parent_lookup_fids.end()), parent_lookup_fids.end());

		if (!parent_lookup_fids.empty() && (!has_link_fids || parent_lookup_fids.size() <= link_filter->fid_values.size())) {
			lookup_fids = std::move(parent_lookup_fids);
			return;
		}
		if (has_link_fids) {
			lookup_fids.insert(lookup_fids.end(), link_filter->fid_values.begin(), link_filter->fid_values.end());
			std::sort(lookup_fids.begin(), lookup_fids.end());
			lookup_fids.erase(std::unique(lookup_fids.begin(), lookup_fids.end()), lookup_fids.end());
			use_child_lookup = true;
			return;
		}
		lookup_fids = std::move(parent_lookup_fids);
		std::sort(lookup_fids.begin(), lookup_fids.end());
		lookup_fids.erase(std::unique(lookup_fids.begin(), lookup_fids.end()), lookup_fids.end());
	}

	bool MatchesInode(const LustreInode &inode, const string &device_path) const {
		if (inode_filters && inode_filters->HasFilters() && !inode_filters->Evaluate(inode)) {
			return false;
		}
		if (device_predicate && device_predicate->HasPredicate() && !device_predicate->Evaluate(&device_path)) {
			return false;
		}
		if (inode_fid_filter) {
			if (!inode_fid_filter->ContainsFID(inode.fid)) {
				return false;
			}
			if (inode_fid_filter->RequiresGenericEvaluation() && !inode_fid_filter->EvaluateFID(inode.fid)) {
				return false;
			}
		}
		return true;
	}

	bool MatchesLink(const LustreFID &link_fid, const LustreFID &parent_fid, const string &name) const {
		if (link_filter) {
			if (!link_filter->ContainsFID(link_fid)) {
				return false;
			}
			if (!link_filter->ContainsParentFID(parent_fid)) {
				return false;
			}
			if (link_filter->fid_predicate && link_filter->fid_predicate->RequiresGenericEvaluation() &&
			    !link_filter->EvaluateFID(link_fid)) {
				return false;
			}
			if (link_filter->parent_fid_predicate && link_filter->parent_fid_predicate->RequiresGenericEvaluation() &&
			    !link_filter->EvaluateParentFID(parent_fid)) {
				return false;
			}
		}
		if (name_predicate && name_predicate->HasPredicate() && !name_predicate->Evaluate(&name)) {
			return false;
		}
		return true;
	}

	bool Matches(const LustreInodeLinkRow &row, const string &device_path) const {
		return MatchesInode(row.inode, device_path) && MatchesLink(row.link_fid, row.parent_fid, row.name);
	}

	static unique_ptr<LustreInodeLinksFilter> Create(const TableFilterSet *filters, const vector<idx_t> &column_ids,
	                                                 bool join_on_parent_fid) {
		auto result = make_uniq<LustreInodeLinksFilter>();
		result->join_on_parent_fid = join_on_parent_fid;
		result->inode_filters = LustreFilterSet::Create(filters, column_ids);
		result->inode_fid_filter = FIDOnlyFilter::Create(filters, column_ids, 0);

		auto normalized_link_filters = make_uniq<TableFilterSet>();
		vector<idx_t> normalized_link_column_ids = {0, 1, 2};

		if (filters) {
			for (const auto &entry : filters->filters) {
				if (entry.first >= column_ids.size()) {
					continue;
				}
				auto actual_column_idx = column_ids[entry.first];
				switch (actual_column_idx) {
				case 15:
					normalized_link_filters->filters[0] = entry.second->Copy();
					break;
				case 16:
					normalized_link_filters->filters[1] = entry.second->Copy();
					break;
				case 17:
					normalized_link_filters->filters[2] = entry.second->Copy();
					break;
				case 14:
					if (!result->device_predicate) {
						result->device_predicate = LustreStringFilter::Create(*entry.second);
					}
					break;
				default:
					break;
				}
			}
		}

		result->link_filter = LinkFilter::Create(normalized_link_filters.get(), normalized_link_column_ids);
		auto name_entry = normalized_link_filters->filters.find(2);
		if (name_entry != normalized_link_filters->filters.end()) {
			result->name_predicate = LustreStringFilter::Create(*name_entry->second);
		}
		result->RebuildLookupFIDs();
		return result;
	}
};

struct LustreInodeLinksGlobalState : public GlobalTableFunctionState {
	vector<string> device_paths;
	vector<idx_t> column_ids;
	MDTScanConfig scan_config;
	unique_ptr<LustreInodeLinksFilter> filters;
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
	bool join_on_parent_fid = false;
	bool use_sequential_scan = true;
	idx_t thread_count = 1;

	LustreInodeLinksGlobalState() : current_device_idx(0), finished(false), device_initialized(false), next_fid_idx(0),
	                                next_block_group(0), active_block_groups(0) {
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

struct LustreInodeLinksLocalState : public LocalTableFunctionState {
	unique_ptr<MDTScanner> scanner;
	bool scanner_initialized = false;
	idx_t initialized_device_idx = DConstants::INVALID_INDEX;
	string initialized_device_path;
	bool sequential_scan_mode = true;
	bool block_group_active = false;
	ext2_ino_t block_group_max_ino = 0;
	int next_block_group_in_batch = 0;
	int block_group_batch_end = 0;
	vector<LustreInodeLinkRow> pending_results;
	idx_t pending_results_idx = 0;

	LustreInodeLinksLocalState() {
		scanner = make_uniq<MDTScanner>();
	}
};

const vector<string> &LustreInodeLinksFunction::GetColumnNames() {
	return INODE_LINK_COLUMN_NAMES;
}

const vector<LogicalType> &LustreInodeLinksFunction::GetColumnTypes() {
	return INODE_LINK_COLUMN_TYPES;
}

static unique_ptr<FunctionData> LustreInodeLinksBindSingle(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types,
                                                           vector<string> &names) {
	auto result = make_uniq<LustreQueryBindData>();
	result->device_paths.push_back(StringValue::Get(input.inputs[0]));
	ParseNamedParameters(input.named_parameters, result->scan_config);
	EstimateCardinality(*result);
	names = LustreInodeLinksFunction::GetColumnNames();
	return_types = LustreInodeLinksFunction::GetColumnTypes();
	return std::move(result);
}

static unique_ptr<FunctionData> LustreInodeLinksBindMulti(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types,
                                                          vector<string> &names) {
	auto result = make_uniq<LustreQueryBindData>();
	auto &list_values = ListValue::GetChildren(input.inputs[0]);
	for (auto &val : list_values) {
		result->device_paths.push_back(StringValue::Get(val));
	}
	if (result->device_paths.empty()) {
		throw BinderException("lustre_inode_links_internal requires at least one device path");
	}
	ParseNamedParameters(input.named_parameters, result->scan_config);
	EstimateCardinality(*result);
	names = LustreInodeLinksFunction::GetColumnNames();
	return_types = LustreInodeLinksFunction::GetColumnTypes();
	return std::move(result);
}

static bool InitializeInodeLinksDevice(LustreInodeLinksGlobalState &gstate, idx_t device_idx) {
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

static unique_ptr<GlobalTableFunctionState> LustreInodeLinksInitGlobal(ClientContext &context,
                                                                       TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<LustreQueryBindData>();
	auto result = make_uniq<LustreInodeLinksGlobalState>();
	result->device_paths = bind_data.device_paths;
	result->column_ids = input.column_ids;
	result->scan_config = bind_data.scan_config;
	result->scan_config.read_link_names = ScanNeedsColumn(input, 17);
	result->join_on_parent_fid = bind_data.inode_link_join_on_parent_fid;
	result->filters = LustreInodeLinksFilter::Create(input.filters.get(), input.column_ids, result->join_on_parent_fid);
	result->use_sequential_scan = !result->filters || result->filters->UseSequentialScan();
	result->thread_count = NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());
	if (!result->device_paths.empty()) {
		InitializeInodeLinksDevice(*result, 0);
	}
	return std::move(result);
}

static unique_ptr<LocalTableFunctionState> LustreInodeLinksInitLocal(ExecutionContext &context,
                                                                     TableFunctionInitInput &input,
                                                                     GlobalTableFunctionState *global_state) {
	return make_uniq<LustreInodeLinksLocalState>();
}

static void WriteTimestamp(Vector &vec, idx_t row_idx, int64_t unix_timestamp) {
	if (unix_timestamp == 0) {
		FlatVector::SetNull(vec, row_idx, true);
	} else {
		FlatVector::GetData<timestamp_t>(vec)[row_idx] = Timestamp::FromEpochSeconds(unix_timestamp);
	}
}

static void WriteInodeLinkColumn(Vector &vec, idx_t col_idx, idx_t row_idx, const LustreInodeLinkRow &row,
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
		auto fid_str = row.link_fid.ToString();
		FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, fid_str);
		break;
	}
	case 16: {
		auto pfid_str = row.parent_fid.ToString();
		FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, pfid_str);
		break;
	}
	case 17:
		FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, row.name);
		break;
	default:
		break;
	}
}

static void ResetInodeLinksBlockGroupState(LustreInodeLinksLocalState &lstate) {
	lstate.block_group_active = false;
	lstate.block_group_max_ino = 0;
	lstate.next_block_group_in_batch = 0;
	lstate.block_group_batch_end = 0;
}

static bool EnsureInodeLinksScanner(LustreInodeLinksGlobalState &gstate, LustreInodeLinksLocalState &lstate,
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
		ResetInodeLinksBlockGroupState(lstate);
		lstate.initialized_device_path.clear();
	}

	if (dev_idx >= gstate.device_paths.size() || gstate.finished.load()) {
		return false;
	}

	if (!gstate.device_initialized.load()) {
		lock_guard<mutex> lock(gstate.device_transition_lock);
		if (!gstate.device_initialized.load()) {
			if (!InitializeInodeLinksDevice(gstate, dev_idx)) {
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
	ResetInodeLinksBlockGroupState(lstate);
	return true;
}

static void ResetInodeLinksPendingRows(LustreInodeLinksLocalState &lstate) {
	lstate.pending_results.clear();
	lstate.pending_results_idx = 0;
}

static bool FlushInodeLinkRows(LustreInodeLinksGlobalState &gstate, LustreInodeLinksLocalState &lstate,
                               DataChunk &output, idx_t &output_count) {
	const auto &current_device = lstate.initialized_device_path;
	while (lstate.pending_results_idx < lstate.pending_results.size() && output_count < STANDARD_VECTOR_SIZE) {
		auto &row = lstate.pending_results[lstate.pending_results_idx];
		for (idx_t i = 0; i < gstate.column_ids.size(); i++) {
			WriteInodeLinkColumn(output.data[i], gstate.column_ids[i], output_count, row, current_device);
		}
		lstate.pending_results_idx++;
		output_count++;
	}
	return output_count < STANDARD_VECTOR_SIZE;
}

static bool ClaimNextBlockGroup(LustreInodeLinksGlobalState &gstate, LustreInodeLinksLocalState &lstate) {
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

static bool ExecuteExactFIDPath(LustreInodeLinksGlobalState &gstate, LustreInodeLinksLocalState &lstate,
                                DataChunk &output, idx_t &output_count) {
	const auto &current_device = lstate.initialized_device_path;
	struct LookupEntry {
		ext2_ino_t ino;
		LustreFID fid;
	};

	while (output_count < STANDARD_VECTOR_SIZE) {
		if (!FlushInodeLinkRows(gstate, lstate, output, output_count)) {
			return true;
		}

		vector<LookupEntry> lookup_batch;
		lookup_batch.reserve(INODE_LINK_EXACT_LOOKUP_BATCH_SIZE);

		while (lookup_batch.size() < INODE_LINK_EXACT_LOOKUP_BATCH_SIZE) {
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

		ResetInodeLinksPendingRows(lstate);
		ext2_ino_t last_ino = 0;
		bool have_last_ino = false;

		for (auto &entry : lookup_batch) {
			if (have_last_ino && entry.ino == last_ino) {
				continue;
			}
			have_last_ino = true;
			last_ino = entry.ino;

			LustreInode inode;
			vector<LinkEntry> links;
			if (!lstate.scanner->ReadInodeLinks(entry.ino, inode, links, gstate.scan_config)) {
				continue;
			}

			for (auto &link_entry : links) {
				LustreInodeLinkRow row;
				row.inode = inode;
				row.link_fid = inode.fid;
				row.parent_fid = link_entry.parent_fid;
				row.name = link_entry.name;
				if (gstate.filters && !gstate.filters->Matches(row, current_device)) {
					continue;
				}
				lstate.pending_results.push_back(std::move(row));
			}
		}
	}

	return true;
}

static void ExpandParentDirectoryRows(LustreInodeLinksGlobalState &gstate, LustreInodeLinksLocalState &lstate,
                                      const LustreInode &inode, const LustreFID &parent_fid,
                                      vector<DirEntry> &entries) {
	std::sort(entries.begin(), entries.end(), [](const DirEntry &left, const DirEntry &right) {
		if (left.ino != right.ino) {
			return left.ino < right.ino;
		}
		return left.name < right.name;
	});

	ext2_ino_t last_child_ino = 0;
	bool have_last_child_ino = false;
	LustreFID last_child_fid;
	for (auto &entry : entries) {
		if (!have_last_child_ino || entry.ino != last_child_ino) {
			LustreFID child_fid;
			if (!lstate.scanner->ReadInodeFID(entry.ino, child_fid)) {
				have_last_child_ino = false;
				continue;
			}
			last_child_ino = entry.ino;
			last_child_fid = child_fid;
			have_last_child_ino = true;
		}
		if (gstate.filters && !gstate.filters->MatchesLink(last_child_fid, parent_fid, entry.name)) {
			continue;
		}

		LustreInodeLinkRow row;
		row.inode = inode;
		row.link_fid = last_child_fid;
		row.parent_fid = parent_fid;
		row.name = entry.name;
		lstate.pending_results.push_back(std::move(row));
	}
}

static bool ExecuteExactParentFIDPath(LustreInodeLinksGlobalState &gstate, LustreInodeLinksLocalState &lstate,
                                      DataChunk &output, idx_t &output_count) {
	const auto &current_device = lstate.initialized_device_path;
	struct LookupEntry {
		ext2_ino_t ino;
		LustreFID fid;
	};

	while (output_count < STANDARD_VECTOR_SIZE) {
		if (!FlushInodeLinkRows(gstate, lstate, output, output_count)) {
			return true;
		}

		vector<LookupEntry> lookup_batch;
		lookup_batch.reserve(INODE_LINK_EXACT_LOOKUP_BATCH_SIZE);

		while (lookup_batch.size() < INODE_LINK_EXACT_LOOKUP_BATCH_SIZE) {
			idx_t fid_idx = gstate.next_fid_idx.fetch_add(1);
			if (fid_idx >= gstate.filters->lookup_fids.size()) {
				break;
			}

			auto &parent_fid = gstate.filters->lookup_fids[fid_idx];
			ext2_ino_t parent_ino;
			if (!lstate.scanner->LookupFID(parent_fid, parent_ino)) {
				continue;
			}
			lookup_batch.push_back({parent_ino, parent_fid});
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

		ResetInodeLinksPendingRows(lstate);
		ext2_ino_t last_ino = 0;
		bool have_last_ino = false;

		for (auto &entry : lookup_batch) {
			if (have_last_ino && entry.ino == last_ino) {
				continue;
			}
			have_last_ino = true;
			last_ino = entry.ino;

			LustreInode inode;
			if (!lstate.scanner->ReadInode(entry.ino, inode, gstate.scan_config)) {
				continue;
			}
			if (gstate.filters && !gstate.filters->MatchesInode(inode, current_device)) {
				continue;
			}
			if (inode.type != FileType::DIRECTORY) {
				continue;
			}

			vector<DirEntry> entries;
			if (!lstate.scanner->ReadDirectoryEntries(entry.ino, entries)) {
				continue;
			}
			ExpandParentDirectoryRows(gstate, lstate, inode, entry.fid, entries);
		}
	}

	return true;
}

static bool ExecuteExactChildFIDPath(LustreInodeLinksGlobalState &gstate, LustreInodeLinksLocalState &lstate,
                                     DataChunk &output, idx_t &output_count) {
	const auto &current_device = lstate.initialized_device_path;
	struct LookupEntry {
		ext2_ino_t ino;
		LustreFID fid;
	};

	while (output_count < STANDARD_VECTOR_SIZE) {
		if (!FlushInodeLinkRows(gstate, lstate, output, output_count)) {
			return true;
		}

		vector<LookupEntry> lookup_batch;
		lookup_batch.reserve(INODE_LINK_EXACT_LOOKUP_BATCH_SIZE);

		while (lookup_batch.size() < INODE_LINK_EXACT_LOOKUP_BATCH_SIZE) {
			idx_t fid_idx = gstate.next_fid_idx.fetch_add(1);
			if (fid_idx >= gstate.filters->lookup_fids.size()) {
				break;
			}

			auto &child_fid = gstate.filters->lookup_fids[fid_idx];
			ext2_ino_t child_ino;
			if (!lstate.scanner->LookupFID(child_fid, child_ino)) {
				continue;
			}
			lookup_batch.push_back({child_ino, child_fid});
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

		ResetInodeLinksPendingRows(lstate);
		ext2_ino_t last_ino = 0;
		bool have_last_ino = false;
		std::map<LustreFID, std::pair<bool, LustreInode> > parent_inode_cache;

		for (auto &entry : lookup_batch) {
			if (have_last_ino && entry.ino == last_ino) {
				continue;
			}
			have_last_ino = true;
			last_ino = entry.ino;

			LustreFID child_fid;
			vector<LinkEntry> links;
			if (!lstate.scanner->ReadInodeLinkEA(entry.ino, child_fid, links, gstate.scan_config.read_link_names)) {
				continue;
			}

			for (auto &link_entry : links) {
				if (gstate.filters &&
				    !gstate.filters->MatchesLink(child_fid, link_entry.parent_fid, link_entry.name)) {
					continue;
				}

				LustreInode parent_inode;
				bool have_parent_inode = false;
				auto cache_entry = parent_inode_cache.find(link_entry.parent_fid);
				if (cache_entry != parent_inode_cache.end()) {
					have_parent_inode = cache_entry->second.first;
					if (have_parent_inode) {
						parent_inode = cache_entry->second.second;
					}
				} else {
					ext2_ino_t parent_ino;
					if (lstate.scanner->LookupFID(link_entry.parent_fid, parent_ino) &&
					    lstate.scanner->ReadInode(parent_ino, parent_inode, gstate.scan_config) &&
					    (!gstate.filters || gstate.filters->MatchesInode(parent_inode, current_device))) {
						parent_inode_cache[link_entry.parent_fid] = std::make_pair(true, parent_inode);
						have_parent_inode = true;
					} else {
						parent_inode_cache[link_entry.parent_fid] = std::make_pair(false, LustreInode());
					}
				}

				if (!have_parent_inode) {
					continue;
				}

				LustreInodeLinkRow row;
				row.inode = parent_inode;
				row.link_fid = child_fid;
				row.parent_fid = link_entry.parent_fid;
				row.name = link_entry.name;
				lstate.pending_results.push_back(std::move(row));
			}
		}
	}

	return true;
}

static bool ExecuteSequentialSameFIDPath(LustreInodeLinksGlobalState &gstate, LustreInodeLinksLocalState &lstate,
                                         DataChunk &output, idx_t &output_count) {
	const auto &current_device = lstate.initialized_device_path;
	while (output_count < STANDARD_VECTOR_SIZE) {
		if (!ClaimNextBlockGroup(gstate, lstate)) {
			return false;
		}

		while (output_count < STANDARD_VECTOR_SIZE) {
			LustreInodeLinkRow row;
			if (!lstate.scanner->GetNextInodeLink(row, gstate.scan_config, lstate.block_group_max_ino)) {
				lstate.block_group_active = false;
				gstate.active_block_groups.fetch_sub(1);
				break;
			}
			if (gstate.filters && !gstate.filters->Matches(row, current_device)) {
				continue;
			}

			for (idx_t i = 0; i < gstate.column_ids.size(); i++) {
				WriteInodeLinkColumn(output.data[i], gstate.column_ids[i], output_count, row, current_device);
			}
			output_count++;
		}
	}

	return true;
}

static bool ExecuteSequentialParentFIDPath(LustreInodeLinksGlobalState &gstate, LustreInodeLinksLocalState &lstate,
                                           DataChunk &output, idx_t &output_count) {
	const auto &current_device = lstate.initialized_device_path;
	while (output_count < STANDARD_VECTOR_SIZE) {
		if (!FlushInodeLinkRows(gstate, lstate, output, output_count)) {
			return true;
		}
		if (!ClaimNextBlockGroup(gstate, lstate)) {
			return false;
		}

		while (output_count < STANDARD_VECTOR_SIZE) {
			LustreInode inode;
			if (!lstate.scanner->GetNextInode(inode, gstate.scan_config, lstate.block_group_max_ino)) {
				lstate.block_group_active = false;
				gstate.active_block_groups.fetch_sub(1);
				break;
			}
			if (gstate.filters && !gstate.filters->MatchesInode(inode, current_device)) {
				continue;
			}
			if (inode.type != FileType::DIRECTORY) {
				continue;
			}

			vector<DirEntry> entries;
			if (!lstate.scanner->ReadDirectoryEntries(inode.ino, entries)) {
				continue;
			}

			ResetInodeLinksPendingRows(lstate);
			ExpandParentDirectoryRows(gstate, lstate, inode, inode.fid, entries);
			if (!FlushInodeLinkRows(gstate, lstate, output, output_count)) {
				return true;
			}
		}
	}

	return true;
}

static void LustreInodeLinksExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &gstate = data_p.global_state->Cast<LustreInodeLinksGlobalState>();
	auto &lstate = data_p.local_state->Cast<LustreInodeLinksLocalState>();

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
			ResetInodeLinksPendingRows(lstate);
			if (lstate.scanner_initialized) {
				lstate.scanner->CloseScan();
				lstate.scanner->CloseOI();
				lstate.scanner->Close();
			}
			lstate.scanner_initialized = false;
			ResetInodeLinksBlockGroupState(lstate);
			lstate.initialized_device_path.clear();
		}
	}

	if (gstate.finished) {
		output.SetCardinality(0);
		return;
	}

	idx_t output_count = 0;
	while (output_count < STANDARD_VECTOR_SIZE) {
		if (!EnsureInodeLinksScanner(gstate, lstate, gstate.use_sequential_scan)) {
			break;
		}

		bool has_more;
		if (gstate.use_sequential_scan) {
			has_more = gstate.join_on_parent_fid ? ExecuteSequentialParentFIDPath(gstate, lstate, output, output_count)
			                                     : ExecuteSequentialSameFIDPath(gstate, lstate, output, output_count);
		} else if (gstate.join_on_parent_fid) {
			has_more = gstate.filters && gstate.filters->use_child_lookup
			               ? ExecuteExactChildFIDPath(gstate, lstate, output, output_count)
			               : ExecuteExactParentFIDPath(gstate, lstate, output, output_count);
		} else {
			has_more = ExecuteExactFIDPath(gstate, lstate, output, output_count);
		}

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
			ResetInodeLinksBlockGroupState(lstate);
			lstate.initialized_device_path.clear();
			ResetInodeLinksPendingRows(lstate);
		}
	}

	output.SetCardinality(output_count);
}

static unique_ptr<NodeStatistics> LustreInodeLinksCardinality(ClientContext &context,
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

TableFunction LustreInodeLinksFunction::GetFunction(bool multi_device) {
	TableFunction func("lustre_inode_links_internal",
	                   {multi_device ? LogicalType::LIST(LogicalType::VARCHAR) : LogicalType::VARCHAR},
	                   LustreInodeLinksExecute,
	                   multi_device ? LustreInodeLinksBindMulti : LustreInodeLinksBindSingle,
	                   LustreInodeLinksInitGlobal, LustreInodeLinksInitLocal);
	func.named_parameters["skip_no_fid"] = LogicalType::BOOLEAN;
	func.named_parameters["skip_no_linkea"] = LogicalType::BOOLEAN;
	func.projection_pushdown = true;
	func.filter_pushdown = true;
	func.table_scan_progress = LustreScanProgress;
	func.cardinality = LustreInodeLinksCardinality;
	return func;
}

TableFunctionSet LustreInodeLinksFunction::GetFunctionSet() {
	TableFunctionSet set("lustre_inode_links_internal");
	set.AddFunction(GetFunction(false));
	set.AddFunction(GetFunction(true));
	return set;
}

} // namespace lustre
} // namespace duckdb
