//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_dirmap.cpp
//
// Table function implementation for querying Lustre DNE2-aware directory mapping.
// Maps physical parent FIDs (shard/namespace bearers) to logical directory FIDs.
//===----------------------------------------------------------------------===//

#include "lustre_dirmap.hpp"
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

static constexpr idx_t DIRMAP_EXACT_LOOKUP_BATCH_SIZE = 512;
static constexpr idx_t DIRMAP_SEQ_SCAN_THRESHOLD = 8192;

//===----------------------------------------------------------------------===//
// Column Definitions
//===----------------------------------------------------------------------===//

static const vector<string> DIRMAP_COLUMN_NAMES = {
    "dir_fid",
    "parent_fid",
    "dir_device",
    "parent_device",
    "master_mdt_index",
    "stripe_index",
    "stripe_count",
    "hash_type",
    "layout_version",
    "source",
    "lma_incompat"
};

static const vector<LogicalType> DIRMAP_COLUMN_TYPES = {
    LogicalType::VARCHAR,    // dir_fid
    LogicalType::VARCHAR,    // parent_fid
    LogicalType::VARCHAR,    // dir_device
    LogicalType::VARCHAR,    // parent_device
    LogicalType::UINTEGER,   // master_mdt_index
    LogicalType::UINTEGER,   // stripe_index
    LogicalType::UINTEGER,   // stripe_count
    LogicalType::UINTEGER,   // hash_type
    LogicalType::UINTEGER,   // layout_version
    LogicalType::VARCHAR,    // source
    LogicalType::UINTEGER    // lma_incompat
};

//===----------------------------------------------------------------------===//
// Bind Functions
//===----------------------------------------------------------------------===//

static unique_ptr<FunctionData> LustreDirMapBindSingle(ClientContext &context,
                                                       TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types,
                                                       vector<string> &names) {
	auto result = make_uniq<LustreQueryBindData>();
	result->device_paths.push_back(StringValue::Get(input.inputs[0]));
	ParseNamedParameters(input.named_parameters, result->scan_config);
	result->scan_config.skip_no_linkea = false;
	names = DIRMAP_COLUMN_NAMES;
	return_types = DIRMAP_COLUMN_TYPES;
	return std::move(result);
}

static unique_ptr<FunctionData> LustreDirMapBindMulti(ClientContext &context,
                                                      TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types,
                                                      vector<string> &names) {
	auto result = make_uniq<LustreQueryBindData>();
	auto &list_values = ListValue::GetChildren(input.inputs[0]);
	for (auto &val : list_values) {
		result->device_paths.push_back(StringValue::Get(val));
	}
	if (result->device_paths.empty()) {
		throw BinderException("lustre_dirmap requires at least one device path");
	}
	ParseNamedParameters(input.named_parameters, result->scan_config);
	result->scan_config.skip_no_linkea = false;
	names = DIRMAP_COLUMN_NAMES;
	return_types = DIRMAP_COLUMN_TYPES;
	return std::move(result);
}

//===----------------------------------------------------------------------===//
// Init Functions
//===----------------------------------------------------------------------===//

static unique_ptr<GlobalTableFunctionState> LustreDirMapInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<LustreQueryBindData>();

	auto gstate = make_uniq<LustreDirMapGlobalState>();
	gstate->device_paths = bind_data.device_paths;
	gstate->column_ids = input.column_ids;
	gstate->scan_config = bind_data.scan_config;
	gstate->current_device_idx.store(0);
	gstate->finished.store(false);
	gstate->device_initialized.store(false);
	gstate->next_fid_idx.store(0);
	gstate->next_block_group.store(0);
	gstate->active_block_groups.store(0);

	gstate->thread_count = NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());

	gstate->fid_filter = FIDOnlyFilter::Create(input.filters.get(), input.column_ids,
	                                           static_cast<idx_t>(DirMapColumnIdx::DIR_FID));
	gstate->use_sequential_scan = gstate->fid_filter &&
		(gstate->fid_filter->RequiresGenericEvaluation() ||
		 !gstate->fid_filter->HasFIDFilter() ||
		 gstate->fid_filter->fid_values.size() > DIRMAP_SEQ_SCAN_THRESHOLD);
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

static unique_ptr<LocalTableFunctionState> LustreDirMapInitLocal(ExecutionContext &context,
                                                                  TableFunctionInitInput &input,
                                                                  GlobalTableFunctionState *global_state) {
	return make_uniq<LustreDirMapLocalState>();
}

//===----------------------------------------------------------------------===//
// Helper: Write a dirmap output row
//===----------------------------------------------------------------------===//

static void WriteDirMapRow(DataChunk &output, idx_t row_idx, const vector<idx_t> &column_ids,
                           const LustreDirMapGlobalState::PendingRow &row) {
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto &vec = output.data[i];
		switch (column_ids[i]) {
		case static_cast<idx_t>(DirMapColumnIdx::DIR_FID): {
			auto fid_str = row.dir_fid.ToString();
			FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, fid_str);
			break;
		}
		case static_cast<idx_t>(DirMapColumnIdx::PARENT_FID): {
			auto fid_str = row.parent_fid.ToString();
			FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, fid_str);
			break;
		}
		case static_cast<idx_t>(DirMapColumnIdx::DIR_DEVICE):
			FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, row.dir_device);
			break;
		case static_cast<idx_t>(DirMapColumnIdx::PARENT_DEVICE):
			FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, row.parent_device);
			break;
		case static_cast<idx_t>(DirMapColumnIdx::MASTER_MDT_INDEX):
			FlatVector::GetData<uint32_t>(vec)[row_idx] = row.master_mdt_index;
			break;
		case static_cast<idx_t>(DirMapColumnIdx::STRIPE_INDEX):
			FlatVector::GetData<uint32_t>(vec)[row_idx] = row.stripe_index;
			break;
		case static_cast<idx_t>(DirMapColumnIdx::STRIPE_COUNT):
			FlatVector::GetData<uint32_t>(vec)[row_idx] = row.stripe_count;
			break;
		case static_cast<idx_t>(DirMapColumnIdx::HASH_TYPE):
			FlatVector::GetData<uint32_t>(vec)[row_idx] = row.hash_type;
			break;
		case static_cast<idx_t>(DirMapColumnIdx::LAYOUT_VERSION):
			FlatVector::GetData<uint32_t>(vec)[row_idx] = row.layout_version;
			break;
		case static_cast<idx_t>(DirMapColumnIdx::SOURCE):
			FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, row.source);
			break;
		case static_cast<idx_t>(DirMapColumnIdx::LMA_INCOMPAT):
			FlatVector::GetData<uint32_t>(vec)[row_idx] = row.lma_incompat;
			break;
		default:
			break;
		}
	}
}

//===----------------------------------------------------------------------===//
// Core: Generate dirmap rows from a directory inode's metadata
//===----------------------------------------------------------------------===//

static void GenerateDirMapRows(const LustreFID &fid, const LustreLMV &lmv,
                                const std::vector<LinkEntry> &links,
                                const std::vector<DirEntry> &dir_entries,
                                const std::string &device_path,
                                uint32_t lma_incompat,
                                LustreDirMapLocalState &lstate,
                                std::vector<LustreDirMapGlobalState::PendingRow> &pending) {
	pending.clear();

	if (!lmv.IsValid()) {
		// Plain directory: dir_fid = parent_fid = self
		LustreDirMapGlobalState::PendingRow row;
		row.dir_fid = fid;
		row.parent_fid = fid;
		row.dir_device = device_path;
		row.parent_device = device_path;
		row.master_mdt_index = 0;
		row.stripe_index = 0;
		row.stripe_count = 1;
		row.hash_type = 0;
		row.layout_version = 0;
		row.source = "plain";
		row.lma_incompat = lma_incompat;
		pending.push_back(std::move(row));
		return;
	}

	if (lmv.IsMaster()) {
		// Master striped directory: stripe 0 is the master object itself
		{
			LustreDirMapGlobalState::PendingRow row;
			row.dir_fid = fid;
			row.parent_fid = fid;
			row.dir_device = device_path;
			row.parent_device = device_path;
			row.master_mdt_index = lmv.lmv_master_mdt_index;
			row.stripe_index = 0;
			row.stripe_count = lmv.lmv_stripe_count;
			row.hash_type = lmv.lmv_hash_type;
			row.layout_version = lmv.lmv_layout_version;
			row.source = "master";
			row.lma_incompat = lma_incompat;
			pending.push_back(std::move(row));
		}

		// Parse shard entries from master's directory listing.
		// Shard entry format: "[0xSEQ:0xOID:0xVER]:INDEX"
		for (auto &entry : dir_entries) {
			if (entry.name.empty() || entry.name[0] != '[') {
				continue;
			}

			auto bracket_pos = entry.name.find(']');
			if (bracket_pos == std::string::npos || bracket_pos + 1 >= entry.name.size()) {
				continue;
			}
			if (entry.name[bracket_pos + 1] != ':') {
				continue;
			}

			std::string fid_str = entry.name.substr(0, bracket_pos + 1);
			LustreFID shard_fid;
			if (!LustreFID::FromString(fid_str, shard_fid)) {
				continue;
			}

			std::string idx_str = entry.name.substr(bracket_pos + 2);
			char *end = nullptr;
			unsigned long shard_idx = strtoul(idx_str.c_str(), &end, 10);
			if (end == idx_str.c_str() || shard_idx >= lmv.lmv_stripe_count) {
				continue;
			}

			// Resolve shard device via cross-MDT OI lookup
			std::string shard_device;
			lstate.ResolveFIDToDevice(shard_fid, shard_device);

			LustreDirMapGlobalState::PendingRow row;
			row.dir_fid = fid;
			row.parent_fid = shard_fid;
			row.dir_device = device_path;
			row.parent_device = shard_device;
			row.master_mdt_index = lmv.lmv_master_mdt_index;
			row.stripe_index = static_cast<uint32_t>(shard_idx);
			row.stripe_count = lmv.lmv_stripe_count;
			row.hash_type = lmv.lmv_hash_type;
			row.layout_version = lmv.lmv_layout_version;
			row.source = "master";
			row.lma_incompat = lma_incompat;
			pending.push_back(std::move(row));
		}
		return;
	}

	if (lmv.IsSlave()) {
		// Slave/shard: check if master is reachable on any scanned device.
		// If yes, skip (the master will emit this shard's mapping).
		LustreFID master_fid;
		if (!links.empty()) {
			master_fid = links[0].parent_fid;
		}

		if (master_fid.IsValid() && lstate.IsFIDReachable(master_fid)) {
			return;
		}

		// Fallback: master not in scanned device set. Emit from slave perspective.
		LustreDirMapGlobalState::PendingRow row;
		row.dir_fid = master_fid;
		row.parent_fid = fid;
		row.dir_device.clear();
		row.parent_device = device_path;
		row.master_mdt_index = 0;
		row.stripe_index = lmv.lmv_master_mdt_index;  // Reused as stripe index for slaves
		row.stripe_count = lmv.lmv_stripe_count;
		row.hash_type = lmv.lmv_hash_type;
		row.layout_version = lmv.lmv_layout_version;
		row.source = "slave";
		row.lma_incompat = lma_incompat;
		pending.push_back(std::move(row));
		return;
	}
}

//===----------------------------------------------------------------------===//
// Block Group Helpers
//===----------------------------------------------------------------------===//

static void ResetDirMapBlockGroupState(LustreDirMapLocalState &lstate) {
	lstate.block_group_active = false;
	lstate.block_group_max_ino = 0;
	lstate.next_block_group_in_batch = 0;
	lstate.block_group_batch_end = 0;
}

static bool InitializeDirMapDevice(LustreDirMapGlobalState &gstate, idx_t dev_idx) {
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

static bool EnsureDirMapLocalScanner(LustreDirMapGlobalState &gstate, LustreDirMapLocalState &lstate,
                                     bool sequential_scan) {
	idx_t dev_idx = gstate.current_device_idx.load();

	if (lstate.scanner_initialized && lstate.initialized_device_idx == dev_idx &&
	    lstate.sequential_scan_mode == sequential_scan) {
		return true;
	}

	if (lstate.scanner_initialized) {
		lstate.scanner->Close();
		lstate.scanner_initialized = false;
		ResetDirMapBlockGroupState(lstate);
		lstate.initialized_device_path.clear();
	}

	if (dev_idx >= gstate.device_paths.size() || gstate.finished.load()) {
		return false;
	}

	if (!gstate.device_initialized.load()) {
		lock_guard<mutex> lock(gstate.device_transition_lock);
		if (!gstate.device_initialized.load()) {
			if (!InitializeDirMapDevice(gstate, dev_idx)) {
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
	ResetDirMapBlockGroupState(lstate);

	// Initialize cross-MDT resolve scanners (lazily, once)
	lstate.EnsureResolveInitialized(gstate.device_paths);

	return true;
}

static bool ClaimNextDirMapBlockGroup(LustreDirMapGlobalState &gstate, LustreDirMapLocalState &lstate) {
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

static bool MatchesDirMapFIDPredicate(const FIDOnlyFilter &filter, const LustreFID &fid) {
	if (!filter.ContainsFID(fid)) {
		return false;
	}
	if (filter.RequiresGenericEvaluation()) {
		return filter.EvaluateFID(fid);
	}
	return true;
}

//===----------------------------------------------------------------------===//
// FID Path: batched lookup by dir_fid -> OI -> inode -> LMV -> dirmap rows
//===----------------------------------------------------------------------===//

static bool ExecuteExactFIDPath(LustreDirMapGlobalState &gstate, LustreDirMapLocalState &lstate,
                                DataChunk &output, idx_t &output_count) {
	auto &filter = *gstate.fid_filter;
	const auto &current_device = lstate.initialized_device_path;
	struct LookupEntry {
		ext2_ino_t ino;
		LustreFID fid;
	};

	while (output_count < STANDARD_VECTOR_SIZE) {
		// Drain pending results first
		while (lstate.pending_results_idx < lstate.pending_results.size() &&
		       output_count < STANDARD_VECTOR_SIZE) {
			auto &row = lstate.pending_results[lstate.pending_results_idx];
			WriteDirMapRow(output, output_count, gstate.column_ids, row);
			lstate.pending_results_idx++;
			output_count++;
		}

		if (output_count >= STANDARD_VECTOR_SIZE) {
			return true;
		}

		vector<LookupEntry> lookup_batch;
		lookup_batch.reserve(DIRMAP_EXACT_LOOKUP_BATCH_SIZE);

		while (lookup_batch.size() < DIRMAP_EXACT_LOOKUP_BATCH_SIZE) {
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

			// Read inode and check if directory
			LustreInode inode;
			if (!lstate.scanner->ReadInode(entry.ino, inode, gstate.scan_config)) {
				continue;
			}
			if (inode.type != FileType::DIRECTORY) {
				continue;
			}

			// Read LMA incompat and skip agent inodes
			LustreLMV lmv;
			lstate.scanner->ReadInodeLMV(entry.ino, lmv);

			// Read LinkEA
			std::vector<LinkEntry> links;
			LustreFID link_fid;
			lstate.scanner->ReadInodeLinkEA(entry.ino, link_fid, links);

			// Read directory entries if master
			std::vector<DirEntry> dir_entries;
			if (lmv.IsMaster()) {
				lstate.scanner->ReadDirectoryEntries(entry.ino, dir_entries);
			}

			// lma_incompat not easily available from FID lookup path; set to 0
			uint32_t lma_incompat = 0;
			GenerateDirMapRows(inode.fid, lmv, links, dir_entries, current_device,
			                   lma_incompat, lstate, lstate.pending_results);
		}
	}

	return true;
}

//===----------------------------------------------------------------------===//
// Sequential Scan Path - linear scan by block group
//===----------------------------------------------------------------------===//

static bool ExecuteSequentialPath(LustreDirMapGlobalState &gstate, LustreDirMapLocalState &lstate,
                                  DataChunk &output, idx_t &output_count) {
	auto &filter = *gstate.fid_filter;
	const auto &current_device = lstate.initialized_device_path;

	while (output_count < STANDARD_VECTOR_SIZE) {
		while (lstate.pending_results_idx < lstate.pending_results.size() &&
		       output_count < STANDARD_VECTOR_SIZE) {
			auto &row = lstate.pending_results[lstate.pending_results_idx];
			WriteDirMapRow(output, output_count, gstate.column_ids, row);
			lstate.pending_results_idx++;
			output_count++;
		}

		if (output_count >= STANDARD_VECTOR_SIZE) {
			return true;
		}

		if (!ClaimNextDirMapBlockGroup(gstate, lstate)) {
			return false;
		}

		while (output_count < STANDARD_VECTOR_SIZE) {
			ext2_ino_t ino;
			LustreFID fid;
			LustreLMV lmv;
			std::vector<LinkEntry> links;
			std::vector<DirEntry> dir_entries;
			uint32_t lma_incompat = 0;

			if (!lstate.scanner->GetNextDirMapEntries(ino, fid, lmv, links, dir_entries,
			                                          lma_incompat,
			                                          gstate.scan_config, lstate.block_group_max_ino)) {
				lstate.block_group_active = false;
				gstate.active_block_groups.fetch_sub(1);
				break;
			}

			if (!MatchesDirMapFIDPredicate(filter, fid)) {
				continue;
			}

			lstate.pending_results.clear();
			lstate.pending_results_idx = 0;
			GenerateDirMapRows(fid, lmv, links, dir_entries, current_device,
			                   lma_incompat, lstate, lstate.pending_results);
			break;
		}
	}

	return true;
}

//===----------------------------------------------------------------------===//
// Execute Function
//===----------------------------------------------------------------------===//

static void LustreDirMapExecute(ClientContext &context, TableFunctionInput &data_p,
                                DataChunk &output) {
	auto &gstate = data_p.global_state->Cast<LustreDirMapGlobalState>();
	auto &lstate = data_p.local_state->Cast<LustreDirMapLocalState>();

	if (gstate.fid_filter->HasDynamicFilter()) {
		bool changed = gstate.fid_filter->ResolveDynamicFilters();
		if (changed) {
			gstate.use_sequential_scan =
			    gstate.fid_filter->RequiresGenericEvaluation() || !gstate.fid_filter->HasFIDFilter() ||
			    gstate.fid_filter->fid_values.size() > DIRMAP_SEQ_SCAN_THRESHOLD;
			gstate.finished = false;
			gstate.next_fid_idx.store(0);
			gstate.next_block_group.store(0);
			gstate.active_block_groups.store(0);
			lstate.pending_results.clear();
			lstate.pending_results_idx = 0;
			gstate.current_device_idx = 0;
			gstate.device_initialized = false;
			lstate.scanner_initialized = false;
			ResetDirMapBlockGroupState(lstate);
			lstate.initialized_device_path.clear();
		}
	}

	if (gstate.finished) {
		output.SetCardinality(0);
		return;
	}

	idx_t output_count = 0;

	while (output_count < STANDARD_VECTOR_SIZE) {
		if (!EnsureDirMapLocalScanner(gstate, lstate, gstate.use_sequential_scan)) {
			break;
		}

		bool has_more = gstate.use_sequential_scan
		    ? ExecuteSequentialPath(gstate, lstate, output, output_count)
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
			ResetDirMapBlockGroupState(lstate);
			lstate.initialized_device_path.clear();
		}
	}

	output.SetCardinality(output_count);
}

//===----------------------------------------------------------------------===//
// Cardinality Function
//===----------------------------------------------------------------------===//

static unique_ptr<NodeStatistics> LustreDirMapCardinality(ClientContext &context,
                                                           const FunctionData *bind_data_p) {
	return make_uniq<NodeStatistics>(1000000000000);
}

//===----------------------------------------------------------------------===//
// GetFunctionSet
//===----------------------------------------------------------------------===//

static void SetDirMapCommonProperties(TableFunction &func) {
	func.named_parameters["skip_no_fid"] = LogicalType::BOOLEAN;
	func.named_parameters["skip_no_linkea"] = LogicalType::BOOLEAN;
	func.projection_pushdown = true;
	func.filter_pushdown = true;
	func.cardinality = LustreDirMapCardinality;
}

TableFunctionSet LustreDirMapFunction::GetFunctionSet() {
	TableFunctionSet set("lustre_dirmap");

	TableFunction single_func("lustre_dirmap", {LogicalType::VARCHAR}, LustreDirMapExecute,
	                          LustreDirMapBindSingle, LustreDirMapInitGlobal, LustreDirMapInitLocal);
	SetDirMapCommonProperties(single_func);
	set.AddFunction(std::move(single_func));

	TableFunction multi_func("lustre_dirmap", {LogicalType::LIST(LogicalType::VARCHAR)}, LustreDirMapExecute,
	                         LustreDirMapBindMulti, LustreDirMapInitGlobal, LustreDirMapInitLocal);
	SetDirMapCommonProperties(multi_func);
	set.AddFunction(std::move(multi_func));

	return set;
}

} // namespace lustre
} // namespace duckdb
