//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_link_inode_dirmap.cpp
//
// Fused 3-way link/dirmap/inode table function: scans links, resolves
// parent_fid → dir_fid via cached LMV, resolves dir_fid → inode via cached OI.
//===----------------------------------------------------------------------===//

#include "lustre_link_inode_dirmap.hpp"
#include "lustre_link_selection.hpp"
#include "lustre_output_string_cache.hpp"
#include "lustre_types.hpp"
#include "lustre_scan_state.hpp"
#include "lustre_fid_filter.hpp"
#include "mdt_scanner.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"

#include <algorithm>
#include <unordered_map>

namespace duckdb {
namespace lustre {

static constexpr idx_t LID_SEQ_SCAN_THRESHOLD = 8192;

//===----------------------------------------------------------------------===//
// Column Definitions (link 0-3, dirmap 4-13, inode 14-28)
//===----------------------------------------------------------------------===//

static const vector<string> LID_COLUMN_NAMES = {"fid",
                                                "parent_fid",
                                                "name",
                                                "link_device",
                                                "dir_fid",
                                                "dirmap_parent_fid",
                                                "dir_device",
                                                "parent_device",
                                                "master_mdt_index",
                                                "stripe_index",
                                                "stripe_count",
                                                "hash_type",
                                                "layout_version",
                                                "source",
                                                "inode_fid",
                                                "ino",
                                                "type",
                                                "mode",
                                                "nlink",
                                                "uid",
                                                "gid",
                                                "size",
                                                "blocks",
                                                "atime",
                                                "mtime",
                                                "ctime",
                                                "projid",
                                                "flags",
                                                "inode_device"};

static const vector<LogicalType> LID_COLUMN_TYPES = {
    LogicalType::VARCHAR,   LogicalType::VARCHAR,   LogicalType::VARCHAR,  LogicalType::VARCHAR,
    LogicalType::VARCHAR,   LogicalType::VARCHAR,   LogicalType::VARCHAR,  LogicalType::VARCHAR,
    LogicalType::UINTEGER,  LogicalType::UINTEGER,  LogicalType::UINTEGER, LogicalType::UINTEGER,
    LogicalType::UINTEGER,  LogicalType::VARCHAR,   LogicalType::VARCHAR,  LogicalType::UBIGINT,
    LogicalType::VARCHAR,   LogicalType::UINTEGER,  LogicalType::UINTEGER, LogicalType::UINTEGER,
    LogicalType::UINTEGER,  LogicalType::UBIGINT,   LogicalType::UBIGINT,  LogicalType::TIMESTAMP,
    LogicalType::TIMESTAMP, LogicalType::TIMESTAMP, LogicalType::UINTEGER, LogicalType::UINTEGER,
    LogicalType::VARCHAR};

//===----------------------------------------------------------------------===//
// Cache Structures
//===----------------------------------------------------------------------===//

struct DirMapCacheEntry {
	LustreFID dir_fid;
	std::string dir_device;
	uint32_t master_mdt_index = 0;
	uint32_t stripe_index = 0;
	uint32_t stripe_count = 1;
	uint32_t hash_type = 0;
	uint32_t layout_version = 0;
	std::string source = "plain";
};

struct FIDHash {
	size_t operator()(const LustreFID &fid) const {
		size_t h = std::hash<uint64_t> {}(fid.f_seq);
		h ^= std::hash<uint32_t> {}(fid.f_oid) + 0x9e3779b9 + (h << 6) + (h >> 2);
		h ^= std::hash<uint32_t> {}(fid.f_ver) + 0x9e3779b9 + (h << 6) + (h >> 2);
		return h;
	}
};

//===----------------------------------------------------------------------===//
// Global State
//===----------------------------------------------------------------------===//

struct LIDGlobalState : public GlobalTableFunctionState {
	vector<string> device_paths;
	vector<idx_t> column_ids;
	MDTScanConfig scan_config;
	unique_ptr<FIDOnlyFilter> fid_filter;
	std::unordered_map<uint32_t, idx_t> mdt_index_to_device_idx;
	vector<FLDRangeEntry> fld_ranges;

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
	bool use_sequential_scan = false;
	idx_t thread_count = 1;

	idx_t MaxThreads() const override {
		if (fid_filter && fid_filter->HasDynamicFilter())
			return 1;
		if (!fid_filter)
			return 1;
		if (use_sequential_scan)
			return MinValue<idx_t>(thread_count, static_cast<idx_t>(MaxValue(total_block_groups, 1)));
		if (!fid_filter->HasFIDFilter())
			return 1;
		return MinValue<idx_t>(fid_filter->fid_values.size(), thread_count);
	}
};

//===----------------------------------------------------------------------===//
// Local State with dual cache
//===----------------------------------------------------------------------===//

struct LIDLocalState : public LocalTableFunctionState {
	unique_ptr<MDTScanner> scanner;
	bool scanner_initialized = false;
	idx_t initialized_device_idx = DConstants::INVALID_INDEX;
	string initialized_device_path;
	bool block_group_active = false;
	ext2_ino_t block_group_max_ino = 0;
	int next_block_group_in_batch = 0;
	int block_group_batch_end = 0;

	vector<unique_ptr<MDTScanner>> resolve_scanners;

	std::unordered_map<LustreFID, DirMapCacheEntry, FIDHash> dirmap_cache;
	std::unordered_map<LustreFID, LustreInode, FIDHash> inode_cache;

	LIDLocalState() {
		scanner = make_uniq<MDTScanner>();
	}

	MDTScanner *GetResolveScanner(const LIDGlobalState &g, idx_t device_idx) {
		if (device_idx >= g.device_paths.size()) {
			return nullptr;
		}
		if (resolve_scanners.size() < g.device_paths.size()) {
			resolve_scanners.resize(g.device_paths.size());
		}
		if (!resolve_scanners[device_idx]) {
			resolve_scanners[device_idx] = make_uniq<MDTScanner>();
			resolve_scanners[device_idx]->Open(g.device_paths[device_idx]);
			resolve_scanners[device_idx]->InitOI();
		}
		return resolve_scanners[device_idx].get();
	}

	bool TryLookupFIDOnDevice(const LIDGlobalState &g, idx_t device_idx, const LustreFID &fid, ext2_ino_t &ino_out) {
		auto *resolve_scanner = GetResolveScanner(g, device_idx);
		if (!resolve_scanner) {
			return false;
		}
		if (!resolve_scanner->LookupFID(fid, ino_out)) {
			return false;
		}

		uint32_t incompat = 0;
		resolve_scanner->ReadInodeLMAIncompat(ino_out, incompat);
		return (incompat & LMAI_AGENT) == 0;
	}

	bool LookupDeviceBySeq(const LIDGlobalState &g, uint64_t seq, idx_t &device_idx_out) const {
		if (g.fld_ranges.empty()) {
			return false;
		}

		auto it = std::upper_bound(g.fld_ranges.begin(), g.fld_ranges.end(), seq,
		                           [](uint64_t value, const FLDRangeEntry &range) { return value < range.seq_start; });
		if (it == g.fld_ranges.begin()) {
			return false;
		}
		--it;
		if (seq >= it->seq_end) {
			return false;
		}

		auto mdt_it = g.mdt_index_to_device_idx.find(it->mdt_index);
		if (mdt_it == g.mdt_index_to_device_idx.end()) {
			return false;
		}
		device_idx_out = mdt_it->second;
		return true;
	}

	bool LookupFIDFallback(const LIDGlobalState &g, const LustreFID &fid, ext2_ino_t &ino_out, idx_t &scanner_idx) {
		for (idx_t device_idx = 0; device_idx < g.device_paths.size(); device_idx++) {
			if (TryLookupFIDOnDevice(g, device_idx, fid, ino_out)) {
				scanner_idx = device_idx;
				return true;
			}
		}
		return false;
	}

	bool LookupFIDCrossMDT(const LIDGlobalState &g, const LustreFID &fid, ext2_ino_t &ino_out, idx_t &scanner_idx) {
		idx_t device_idx = DConstants::INVALID_INDEX;
		if (LookupDeviceBySeq(g, fid.f_seq, device_idx)) {
			if (TryLookupFIDOnDevice(g, device_idx, fid, ino_out)) {
				scanner_idx = device_idx;
				return true;
			}
		}

		if (LookupFIDFallback(g, fid, ino_out, scanner_idx)) {
			return true;
		}
		return false;
	}

	bool ResolveDirMap(const LIDGlobalState &g, const LustreFID &parent_fid, DirMapCacheEntry &out) {
		auto it = dirmap_cache.find(parent_fid);
		if (it != dirmap_cache.end()) {
			out = it->second;
			return true;
		}

		ext2_ino_t ino;
		idx_t sidx;
		if (!LookupFIDCrossMDT(g, parent_fid, ino, sidx))
			return false;

		DirMapCacheEntry entry;
		entry.dir_fid = parent_fid;
		entry.dir_device = g.device_paths[sidx];

		LustreLMV lmv;
		auto *resolve_scanner = GetResolveScanner(g, sidx);
		resolve_scanner->ReadInodeLMV(ino, lmv);

		if (!lmv.IsValid()) {
			entry.source = "plain";
		} else if (lmv.IsMaster()) {
			entry.source = "master";
			entry.master_mdt_index = lmv.lmv_master_mdt_index;
			entry.stripe_count = lmv.lmv_stripe_count;
			entry.hash_type = lmv.lmv_hash_type;
			entry.layout_version = lmv.lmv_layout_version;
		} else if (lmv.IsSlave()) {
			LustreFID link_fid;
			std::vector<LinkEntry> links;
			resolve_scanner->ReadInodeLinkEA(ino, link_fid, links);
			auto *master_link = SelectStripedSlaveParentLink(links, parent_fid, lmv.lmv_master_mdt_index);
			if (!master_link) {
				return false;
			}

			entry.dir_fid = master_link->parent_fid;
			ext2_ino_t dir_ino;
			idx_t dir_sidx;
			if (LookupFIDCrossMDT(g, entry.dir_fid, dir_ino, dir_sidx))
				entry.dir_device = g.device_paths[dir_sidx];
			else
				entry.dir_device.clear();
			entry.source = "slave";
			entry.stripe_index = lmv.lmv_master_mdt_index;
			entry.stripe_count = lmv.lmv_stripe_count;
			entry.hash_type = lmv.lmv_hash_type;
			entry.layout_version = lmv.lmv_layout_version;
		}

		dirmap_cache[parent_fid] = entry;
		out = entry;
		return true;
	}

	bool ResolveInode(const LIDGlobalState &g, const LustreFID &dir_fid, LustreInode &out) {
		auto it = inode_cache.find(dir_fid);
		if (it != inode_cache.end()) {
			out = it->second;
			return true;
		}

		ext2_ino_t ino;
		idx_t sidx;
		if (!LookupFIDCrossMDT(g, dir_fid, ino, sidx))
			return false;

		MDTScanConfig cfg;
		cfg.skip_no_fid = false;
		cfg.skip_no_linkea = false;
		LustreInode inode;
		auto *resolve_scanner = GetResolveScanner(g, sidx);
		if (!resolve_scanner->ReadInode(ino, inode, cfg))
			return false;

		inode_cache[dir_fid] = inode;
		out = inode;
		return true;
	}
};

//===----------------------------------------------------------------------===//
// Bind
//===----------------------------------------------------------------------===//

static unique_ptr<FunctionData> BindSingle(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &ret,
                                           vector<string> &names) {
	auto r = make_uniq<LustreQueryBindData>();
	r->device_paths.push_back(StringValue::Get(input.inputs[0]));
	ParseNamedParameters(input.named_parameters, r->scan_config);
	names = LID_COLUMN_NAMES;
	ret = LID_COLUMN_TYPES;
	return std::move(r);
}

static unique_ptr<FunctionData> BindMulti(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &ret,
                                          vector<string> &names) {
	auto r = make_uniq<LustreQueryBindData>();
	for (auto &v : ListValue::GetChildren(input.inputs[0]))
		r->device_paths.push_back(StringValue::Get(v));
	if (r->device_paths.empty())
		throw BinderException("lustre_link_inode_dirmap requires at least one device path");
	ParseNamedParameters(input.named_parameters, r->scan_config);
	names = LID_COLUMN_NAMES;
	ret = LID_COLUMN_TYPES;
	return std::move(r);
}

//===----------------------------------------------------------------------===//
// Init
//===----------------------------------------------------------------------===//

static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &ctx, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<LustreQueryBindData>();
	auto g = make_uniq<LIDGlobalState>();
	g->device_paths = bind.device_paths;
	g->column_ids = input.column_ids;
	g->scan_config = bind.scan_config;
	g->scan_config.read_link_names = ScanNeedsColumn(input, 2);
	g->current_device_idx.store(0);
	g->finished.store(false);
	g->device_initialized.store(false);
	g->next_fid_idx.store(0);
	g->next_block_group.store(0);
	g->active_block_groups.store(0);
	g->thread_count = NumericCast<idx_t>(TaskScheduler::GetScheduler(ctx).NumberOfThreads());
	g->fid_filter = FIDOnlyFilter::Create(input.filters.get(), input.column_ids, 0);
	for (idx_t i = 0; i < g->device_paths.size(); i++) {
		MDTScanner probe;
		probe.Open(g->device_paths[i]);

		uint32_t mdt_index = 0;
		if (probe.GetMDTIndex(mdt_index)) {
			g->mdt_index_to_device_idx[mdt_index] = i;
			if (mdt_index == 0) {
				probe.LoadFLDRanges(g->fld_ranges);
			}
		}

		probe.Close();
	}
	g->use_sequential_scan =
	    g->fid_filter && (g->fid_filter->RequiresGenericEvaluation() || !g->fid_filter->HasFIDFilter() ||
	                      g->fid_filter->fid_values.size() > LID_SEQ_SCAN_THRESHOLD);
	if (g->use_sequential_scan && !g->device_paths.empty()) {
		MDTScanner probe;
		probe.Open(g->device_paths[0]);
		g->current_device_path = g->device_paths[0];
		g->total_block_groups = static_cast<int>(probe.GetBlockGroupCount());
		g->inodes_per_group = probe.GetInodesPerGroup();
		probe.Close();
		g->device_initialized = true;
	}
	return std::move(g);
}

static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext &, TableFunctionInitInput &,
                                                     GlobalTableFunctionState *) {
	return make_uniq<LIDLocalState>();
}

//===----------------------------------------------------------------------===//
// WriteRow — 29 columns
//===----------------------------------------------------------------------===//

static void WriteRow(DataChunk &output, idx_t row, const vector<idx_t> &cols, const LustreLink &link,
                     const DirMapCacheEntry &dm, bool dm_ok, const LustreInode &dir_inode, bool inode_ok,
                     const string &link_device, const string &inode_device, LustreOutputStringCache &string_cache) {
	for (idx_t i = 0; i < cols.size(); i++) {
		auto &vec = output.data[i];
		switch (cols[i]) {
		// Link columns 0-3
		case 0:
			FlatVector::GetData<string_t>(vec)[row] = string_cache.GetFID(vec, i, link.fid);
			break;
		case 1:
			FlatVector::GetData<string_t>(vec)[row] = string_cache.GetFID(vec, i, link.parent_fid);
			break;
		case 2:
			FlatVector::GetData<string_t>(vec)[row] = StringVector::AddString(vec, link.name);
			break;
		case 3:
			FlatVector::GetData<string_t>(vec)[row] = string_cache.GetString(vec, i, link_device);
			break;
		// DirMap columns 4-13
		case 4:
			if (dm_ok)
				FlatVector::GetData<string_t>(vec)[row] = string_cache.GetFID(vec, i, dm.dir_fid);
			else
				FlatVector::SetNull(vec, row, true);
			break;
		case 5:
			if (dm_ok)
				FlatVector::GetData<string_t>(vec)[row] = string_cache.GetFID(vec, i, link.parent_fid);
			else
				FlatVector::SetNull(vec, row, true);
			break;
		case 6:
			if (dm_ok)
				FlatVector::GetData<string_t>(vec)[row] = string_cache.GetString(vec, i, dm.dir_device);
			else
				FlatVector::SetNull(vec, row, true);
			break;
		case 7:
			if (dm_ok)
				FlatVector::GetData<string_t>(vec)[row] = string_cache.GetString(vec, i, link_device);
			else
				FlatVector::SetNull(vec, row, true);
			break;
		case 8:
			if (dm_ok)
				FlatVector::GetData<uint32_t>(vec)[row] = dm.master_mdt_index;
			else
				FlatVector::SetNull(vec, row, true);
			break;
		case 9:
			if (dm_ok)
				FlatVector::GetData<uint32_t>(vec)[row] = dm.stripe_index;
			else
				FlatVector::SetNull(vec, row, true);
			break;
		case 10:
			if (dm_ok)
				FlatVector::GetData<uint32_t>(vec)[row] = dm.stripe_count;
			else
				FlatVector::SetNull(vec, row, true);
			break;
		case 11:
			if (dm_ok)
				FlatVector::GetData<uint32_t>(vec)[row] = dm.hash_type;
			else
				FlatVector::SetNull(vec, row, true);
			break;
		case 12:
			if (dm_ok)
				FlatVector::GetData<uint32_t>(vec)[row] = dm.layout_version;
			else
				FlatVector::SetNull(vec, row, true);
			break;
		case 13:
			if (dm_ok)
				FlatVector::GetData<string_t>(vec)[row] = string_cache.GetString(vec, i, dm.source);
			else
				FlatVector::SetNull(vec, row, true);
			break;
		// Inode columns 14-28
		case 14:
			if (inode_ok)
				FlatVector::GetData<string_t>(vec)[row] = string_cache.GetFID(vec, i, dir_inode.fid);
			else
				FlatVector::SetNull(vec, row, true);
			break;
		case 15:
			if (inode_ok)
				FlatVector::GetData<uint64_t>(vec)[row] = dir_inode.ino;
			else
				FlatVector::SetNull(vec, row, true);
			break;
		case 16:
			if (inode_ok)
				FlatVector::GetData<string_t>(vec)[row] =
				    StringVector::AddString(vec, FileTypeToString(dir_inode.type));
			else
				FlatVector::SetNull(vec, row, true);
			break;
		case 17:
			if (inode_ok)
				FlatVector::GetData<uint32_t>(vec)[row] = dir_inode.mode;
			else
				FlatVector::SetNull(vec, row, true);
			break;
		case 18:
			if (inode_ok)
				FlatVector::GetData<uint32_t>(vec)[row] = dir_inode.nlink;
			else
				FlatVector::SetNull(vec, row, true);
			break;
		case 19:
			if (inode_ok)
				FlatVector::GetData<uint32_t>(vec)[row] = dir_inode.uid;
			else
				FlatVector::SetNull(vec, row, true);
			break;
		case 20:
			if (inode_ok)
				FlatVector::GetData<uint32_t>(vec)[row] = dir_inode.gid;
			else
				FlatVector::SetNull(vec, row, true);
			break;
		case 21:
			if (inode_ok)
				FlatVector::GetData<uint64_t>(vec)[row] = dir_inode.size;
			else
				FlatVector::SetNull(vec, row, true);
			break;
		case 22:
			if (inode_ok)
				FlatVector::GetData<uint64_t>(vec)[row] = dir_inode.blocks;
			else
				FlatVector::SetNull(vec, row, true);
			break;
		case 23:
			if (inode_ok)
				FlatVector::GetData<timestamp_t>(vec)[row] = Timestamp::FromEpochSeconds(dir_inode.atime);
			else
				FlatVector::SetNull(vec, row, true);
			break;
		case 24:
			if (inode_ok)
				FlatVector::GetData<timestamp_t>(vec)[row] = Timestamp::FromEpochSeconds(dir_inode.mtime);
			else
				FlatVector::SetNull(vec, row, true);
			break;
		case 25:
			if (inode_ok)
				FlatVector::GetData<timestamp_t>(vec)[row] = Timestamp::FromEpochSeconds(dir_inode.ctime);
			else
				FlatVector::SetNull(vec, row, true);
			break;
		case 26:
			if (inode_ok)
				FlatVector::GetData<uint32_t>(vec)[row] = dir_inode.projid;
			else
				FlatVector::SetNull(vec, row, true);
			break;
		case 27:
			if (inode_ok)
				FlatVector::GetData<uint32_t>(vec)[row] = dir_inode.flags;
			else
				FlatVector::SetNull(vec, row, true);
			break;
		case 28:
			if (inode_ok)
				FlatVector::GetData<string_t>(vec)[row] = string_cache.GetString(vec, i, inode_device);
			else
				FlatVector::SetNull(vec, row, true);
			break;
		default:
			break;
		}
	}
}

//===----------------------------------------------------------------------===//
// Block Group Helpers
//===----------------------------------------------------------------------===//

static void ResetBG(LIDLocalState &l) {
	l.block_group_active = false;
	l.block_group_max_ino = 0;
	l.next_block_group_in_batch = 0;
	l.block_group_batch_end = 0;
}

static bool InitDevice(LIDGlobalState &g, idx_t dev) {
	if (dev >= g.device_paths.size()) {
		g.finished = true;
		return false;
	}
	g.current_device_path = g.device_paths[dev];
	g.next_fid_idx.store(0);
	g.next_block_group.store(0);
	g.active_block_groups.store(0);
	if (!g.use_sequential_scan) {
		g.device_initialized = true;
		return true;
	}
	MDTScanner probe;
	probe.Open(g.current_device_path);
	g.total_block_groups = static_cast<int>(probe.GetBlockGroupCount());
	g.inodes_per_group = probe.GetInodesPerGroup();
	probe.Close();
	g.device_initialized = true;
	return true;
}

static bool EnsureScanner(LIDGlobalState &g, LIDLocalState &l) {
	idx_t dev = g.current_device_idx.load();
	if (l.scanner_initialized && l.initialized_device_idx == dev)
		return true;
	if (l.scanner_initialized) {
		l.scanner->Close();
		l.scanner_initialized = false;
		ResetBG(l);
	}
	if (dev >= g.device_paths.size() || g.finished.load())
		return false;
	if (!g.device_initialized.load()) {
		lock_guard<mutex> lock(g.device_transition_lock);
		if (!g.device_initialized.load()) {
			if (!InitDevice(g, dev))
				return false;
		}
	}
	l.scanner->Open(g.current_device_path);
	l.scanner->StartScan();
	l.scanner_initialized = true;
	l.initialized_device_idx = dev;
	l.initialized_device_path = g.current_device_path;
	ResetBG(l);
	return true;
}

static bool ClaimBG(LIDGlobalState &g, LIDLocalState &l) {
	static constexpr int BATCH = 8;
	if (l.block_group_active)
		return true;
	if (l.next_block_group_in_batch >= l.block_group_batch_end) {
		int s = g.next_block_group.fetch_add(BATCH);
		if (s >= g.total_block_groups)
			return false;
		int e = s + BATCH;
		if (e > g.total_block_groups)
			e = g.total_block_groups;
		g.active_block_groups.fetch_add(e - s);
		l.next_block_group_in_batch = s;
		l.block_group_batch_end = e;
	}
	int bg = l.next_block_group_in_batch++;
	l.scanner->GotoBlockGroup(bg);
	l.block_group_max_ino = static_cast<ext2_ino_t>(bg + 1) * g.inodes_per_group;
	l.block_group_active = true;
	return true;
}

//===----------------------------------------------------------------------===//
// Execute
//===----------------------------------------------------------------------===//

static void Execute(ClientContext &ctx, TableFunctionInput &data_p, DataChunk &output) {
	auto &g = data_p.global_state->Cast<LIDGlobalState>();
	auto &l = data_p.local_state->Cast<LIDLocalState>();

	if (g.fid_filter->HasDynamicFilter()) {
		bool changed = g.fid_filter->ResolveDynamicFilters();
		if (changed) {
			g.use_sequential_scan = g.fid_filter->RequiresGenericEvaluation() || !g.fid_filter->HasFIDFilter() ||
			                        g.fid_filter->fid_values.size() > LID_SEQ_SCAN_THRESHOLD;
			g.finished = false;
			g.next_fid_idx.store(0);
			g.next_block_group.store(0);
			g.active_block_groups.store(0);
			g.current_device_idx = 0;
			g.device_initialized = false;
			l.scanner_initialized = false;
			ResetBG(l);
		}
	}

	if (g.finished) {
		output.SetCardinality(0);
		return;
	}

	idx_t cnt = 0;
	LustreOutputStringCache string_cache(g.column_ids.size());
	while (cnt < STANDARD_VECTOR_SIZE) {
		if (!EnsureScanner(g, l))
			break;

		if (!ClaimBG(g, l)) {
			if (g.active_block_groups.load() != 0)
				break;
			lock_guard<mutex> lock(g.device_transition_lock);
			idx_t cur = g.current_device_idx.load();
			if (l.initialized_device_idx == cur && g.next_block_group.load() >= g.total_block_groups &&
			    g.active_block_groups.load() == 0) {
				g.device_initialized.store(false);
				g.current_device_idx++;
				if (g.current_device_idx >= g.device_paths.size())
					g.finished = true;
			}
			l.scanner_initialized = false;
			ResetBG(l);
			continue;
		}

		LustreLink link;
		if (!l.scanner->GetNextLink(link, g.scan_config, l.block_group_max_ino)) {
			l.block_group_active = false;
			l.dirmap_cache.clear();
			l.inode_cache.clear();
			g.active_block_groups.fetch_sub(1);
			continue;
		}

		if (g.fid_filter->HasFIDFilter() && !g.fid_filter->ContainsFID(link.fid))
			continue;
		if (g.fid_filter->RequiresGenericEvaluation() && !g.fid_filter->EvaluateFID(link.fid))
			continue;

		// Resolve dirmap
		DirMapCacheEntry dm;
		bool dm_ok = l.ResolveDirMap(g, link.parent_fid, dm);

		// Resolve directory inode
		LustreInode dir_inode;
		bool inode_ok = false;
		std::string inode_device;
		if (dm_ok) {
			inode_ok = l.ResolveInode(g, dm.dir_fid, dir_inode);
			if (inode_ok) {
				// Find the device for the inode
				ext2_ino_t tmp_ino;
				idx_t tmp_sidx;
				if (l.LookupFIDCrossMDT(g, dm.dir_fid, tmp_ino, tmp_sidx))
					inode_device = g.device_paths[tmp_sidx];
			}
		}

		WriteRow(output, cnt, g.column_ids, link, dm, dm_ok, dir_inode, inode_ok, l.initialized_device_path,
		         inode_device, string_cache);
		cnt++;
	}
	output.SetCardinality(cnt);
}

//===----------------------------------------------------------------------===//
// Cardinality / Registration
//===----------------------------------------------------------------------===//

static unique_ptr<NodeStatistics> Cardinality(ClientContext &, const FunctionData *) {
	return make_uniq<NodeStatistics>(1000000000000);
}

static void SetProps(TableFunction &f) {
	f.named_parameters["skip_no_fid"] = LogicalType::BOOLEAN;
	f.named_parameters["skip_no_linkea"] = LogicalType::BOOLEAN;
	f.projection_pushdown = true;
	f.filter_pushdown = true;
	f.cardinality = Cardinality;
}

static const vector<string> &GetNames() {
	static const vector<string> n = LID_COLUMN_NAMES;
	return n;
}
static const vector<LogicalType> &GetTypes() {
	static const vector<LogicalType> t = LID_COLUMN_TYPES;
	return t;
}

const vector<string> &LustreLinkInodeDirMapFunction::GetColumnNames() {
	return GetNames();
}
const vector<LogicalType> &LustreLinkInodeDirMapFunction::GetColumnTypes() {
	return GetTypes();
}

TableFunction LustreLinkInodeDirMapFunction::GetFunction(bool multi) {
	if (multi) {
		TableFunction f("lustre_link_inode_dirmap", {LogicalType::LIST(LogicalType::VARCHAR)}, Execute, BindMulti,
		                InitGlobal, InitLocal);
		SetProps(f);
		return f;
	}
	TableFunction f("lustre_link_inode_dirmap", {LogicalType::VARCHAR}, Execute, BindSingle, InitGlobal, InitLocal);
	SetProps(f);
	return f;
}

TableFunctionSet LustreLinkInodeDirMapFunction::GetFunctionSet() {
	TableFunctionSet set("lustre_link_inode_dirmap");
	set.AddFunction(GetFunction(false));
	set.AddFunction(GetFunction(true));
	return set;
}

} // namespace lustre
} // namespace duckdb
