//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_link_dirmap.cpp
//
// Fused link/dirmap table function: scans links and resolves parent_fid to
// dir_fid inline via cached LMV lookup.
//===----------------------------------------------------------------------===//

#include "lustre_link_dirmap.hpp"
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

#include <unordered_map>

namespace duckdb {
namespace lustre {

static constexpr idx_t LINK_DIRMAP_SEQ_SCAN_THRESHOLD = 8192;

//===----------------------------------------------------------------------===//
// Column Definitions
//===----------------------------------------------------------------------===//

static const vector<string> LINK_DIRMAP_COLUMN_NAMES = {
    "fid", "parent_fid", "name", "device",
    "dir_fid", "dirmap_parent_fid", "dir_device", "parent_device",
    "master_mdt_index", "stripe_index", "stripe_count",
    "hash_type", "layout_version", "source"
};

static const vector<LogicalType> LINK_DIRMAP_COLUMN_TYPES = {
    LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
    LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
    LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::UINTEGER,
    LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::VARCHAR
};

//===----------------------------------------------------------------------===//
// DirMap Resolution Cache
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
        size_t h = std::hash<uint64_t>{}(fid.f_seq);
        h ^= std::hash<uint32_t>{}(fid.f_oid) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<uint32_t>{}(fid.f_ver) + 0x9e3779b9 + (h << 6) + (h >> 2);
        return h;
    }
};

//===----------------------------------------------------------------------===//
// Global State
//===----------------------------------------------------------------------===//

struct LustreLinkDirMapGlobalState : public GlobalTableFunctionState {
    vector<string> device_paths;
    vector<idx_t> column_ids;
    MDTScanConfig scan_config;
    unique_ptr<FIDOnlyFilter> fid_filter;

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
        if (fid_filter && fid_filter->HasDynamicFilter()) return 1;
        if (!fid_filter) return 1;
        if (use_sequential_scan) {
            return MinValue<idx_t>(thread_count, static_cast<idx_t>(MaxValue(total_block_groups, 1)));
        }
        if (!fid_filter->HasFIDFilter()) return 1;
        return MinValue<idx_t>(fid_filter->fid_values.size(), thread_count);
    }
};

//===----------------------------------------------------------------------===//
// Local State
//===----------------------------------------------------------------------===//

struct LustreLinkDirMapLocalState : public LocalTableFunctionState {
    unique_ptr<MDTScanner> scanner;
    bool scanner_initialized = false;
    idx_t initialized_device_idx = DConstants::INVALID_INDEX;
    string initialized_device_path;
    bool block_group_active = false;
    ext2_ino_t block_group_max_ino = 0;
    int next_block_group_in_batch = 0;
    int block_group_batch_end = 0;

    vector<unique_ptr<MDTScanner>> resolve_scanners;
    vector<string> resolve_device_paths;
    bool resolve_initialized = false;
    std::unordered_map<LustreFID, DirMapCacheEntry, FIDHash> dirmap_cache;

    LustreLinkDirMapLocalState() { scanner = make_uniq<MDTScanner>(); }

    void EnsureResolveInitialized(const vector<string> &device_paths) {
        if (resolve_initialized) return;
        resolve_scanners.resize(device_paths.size());
        resolve_device_paths = device_paths;
        for (idx_t i = 0; i < device_paths.size(); i++) {
            resolve_scanners[i] = make_uniq<MDTScanner>();
            resolve_scanners[i]->Open(device_paths[i]);
            resolve_scanners[i]->InitOI();
        }
        resolve_initialized = true;
    }

    bool LookupFIDCrossMDT(const LustreFID &fid, ext2_ino_t &ino_out, idx_t &scanner_idx) {
        for (idx_t i = 0; i < resolve_scanners.size(); i++) {
            if (resolve_scanners[i]->LookupFID(fid, ino_out)) {
                // Skip agent inodes (DNE1 remote dir stubs)
                uint32_t incompat = 0;
                resolve_scanners[i]->ReadInodeLMAIncompat(ino_out, incompat);
                if (incompat & LMAI_AGENT) {
                    continue;  // Try next scanner for the real directory
                }
                scanner_idx = i;
                return true;
            }
        }
        return false;
    }

    bool ResolveParentFID(const LustreFID &parent_fid, DirMapCacheEntry &out) {
        auto it = dirmap_cache.find(parent_fid);
        if (it != dirmap_cache.end()) { out = it->second; return true; }

        ext2_ino_t ino;
        idx_t sidx;
        if (!LookupFIDCrossMDT(parent_fid, ino, sidx)) return false;

        DirMapCacheEntry entry;
        entry.dir_fid = parent_fid;
        entry.dir_device = resolve_device_paths[sidx];

        LustreLMV lmv;
        resolve_scanners[sidx]->ReadInodeLMV(ino, lmv);

        if (!lmv.IsValid()) {
            entry.source = "plain";
            entry.stripe_count = 1;
        } else if (lmv.IsMaster()) {
            entry.source = "master";
            entry.master_mdt_index = lmv.lmv_master_mdt_index;
            entry.stripe_index = 0;
            entry.stripe_count = lmv.lmv_stripe_count;
            entry.hash_type = lmv.lmv_hash_type;
            entry.layout_version = lmv.lmv_layout_version;
        } else if (lmv.IsSlave()) {
            LustreFID link_fid;
            std::vector<LinkEntry> links;
            resolve_scanners[sidx]->ReadInodeLinkEA(ino, link_fid, links);
            auto *master_link = SelectStripedSlaveParentLink(links, parent_fid, lmv.lmv_master_mdt_index);
            if (!master_link) {
                return false;
            }

            entry.dir_fid = master_link->parent_fid;
            // Resolve dir_fid device
            ext2_ino_t dir_ino;
            idx_t dir_sidx;
            if (LookupFIDCrossMDT(entry.dir_fid, dir_ino, dir_sidx)) {
                entry.dir_device = resolve_device_paths[dir_sidx];
            } else {
                entry.dir_device.clear();
            }
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
};

//===----------------------------------------------------------------------===//
// Bind
//===----------------------------------------------------------------------===//

static unique_ptr<FunctionData> BindSingle(ClientContext &ctx, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
    auto r = make_uniq<LustreQueryBindData>();
    r->device_paths.push_back(StringValue::Get(input.inputs[0]));
    ParseNamedParameters(input.named_parameters, r->scan_config);
    names = LINK_DIRMAP_COLUMN_NAMES;
    return_types = LINK_DIRMAP_COLUMN_TYPES;
    return std::move(r);
}

static unique_ptr<FunctionData> BindMulti(ClientContext &ctx, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
    auto r = make_uniq<LustreQueryBindData>();
    for (auto &v : ListValue::GetChildren(input.inputs[0])) r->device_paths.push_back(StringValue::Get(v));
    if (r->device_paths.empty()) throw BinderException("lustre_link_dirmap requires at least one device path");
    ParseNamedParameters(input.named_parameters, r->scan_config);
    names = LINK_DIRMAP_COLUMN_NAMES;
    return_types = LINK_DIRMAP_COLUMN_TYPES;
    return std::move(r);
}

//===----------------------------------------------------------------------===//
// Init
//===----------------------------------------------------------------------===//

static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &ctx, TableFunctionInitInput &input) {
    auto &bind = input.bind_data->Cast<LustreQueryBindData>();
    auto g = make_uniq<LustreLinkDirMapGlobalState>();
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
    g->use_sequential_scan = g->fid_filter &&
        (g->fid_filter->RequiresGenericEvaluation() || !g->fid_filter->HasFIDFilter() ||
         g->fid_filter->fid_values.size() > LINK_DIRMAP_SEQ_SCAN_THRESHOLD);
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

static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext &ctx, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *gs) {
    return make_uniq<LustreLinkDirMapLocalState>();
}

//===----------------------------------------------------------------------===//
// WriteRow
//===----------------------------------------------------------------------===//

static void WriteRow(DataChunk &output, idx_t row, const vector<idx_t> &col_ids,
                     const LustreLink &link, const DirMapCacheEntry &dm,
                     const string &device, bool dm_valid, LustreOutputStringCache &string_cache) {
    for (idx_t i = 0; i < col_ids.size(); i++) {
        auto &vec = output.data[i];
        switch (col_ids[i]) {
        case 0: FlatVector::GetData<string_t>(vec)[row] = string_cache.GetFID(vec, i, link.fid); break;
        case 1: FlatVector::GetData<string_t>(vec)[row] = string_cache.GetFID(vec, i, link.parent_fid); break;
        case 2: FlatVector::GetData<string_t>(vec)[row] = StringVector::AddString(vec, link.name); break;
        case 3: FlatVector::GetData<string_t>(vec)[row] = string_cache.GetString(vec, i, device); break;
        case 4:
            if (dm_valid) FlatVector::GetData<string_t>(vec)[row] = string_cache.GetFID(vec, i, dm.dir_fid);
            else FlatVector::SetNull(vec, row, true);
            break;
        case 5:
            if (dm_valid) FlatVector::GetData<string_t>(vec)[row] = string_cache.GetFID(vec, i, link.parent_fid);
            else FlatVector::SetNull(vec, row, true);
            break;
        case 6:
            if (dm_valid) FlatVector::GetData<string_t>(vec)[row] = string_cache.GetString(vec, i, dm.dir_device);
            else FlatVector::SetNull(vec, row, true);
            break;
        case 7:
            if (dm_valid) {
                FlatVector::GetData<string_t>(vec)[row] = string_cache.GetString(vec, i, device);
            } else FlatVector::SetNull(vec, row, true);
            break;
        case 8: if (dm_valid) FlatVector::GetData<uint32_t>(vec)[row] = dm.master_mdt_index; else FlatVector::SetNull(vec, row, true); break;
        case 9: if (dm_valid) FlatVector::GetData<uint32_t>(vec)[row] = dm.stripe_index; else FlatVector::SetNull(vec, row, true); break;
        case 10: if (dm_valid) FlatVector::GetData<uint32_t>(vec)[row] = dm.stripe_count; else FlatVector::SetNull(vec, row, true); break;
        case 11: if (dm_valid) FlatVector::GetData<uint32_t>(vec)[row] = dm.hash_type; else FlatVector::SetNull(vec, row, true); break;
        case 12: if (dm_valid) FlatVector::GetData<uint32_t>(vec)[row] = dm.layout_version; else FlatVector::SetNull(vec, row, true); break;
        case 13:
            if (dm_valid) FlatVector::GetData<string_t>(vec)[row] = string_cache.GetString(vec, i, dm.source);
            else FlatVector::SetNull(vec, row, true);
            break;
        default: break;
        }
    }
}

//===----------------------------------------------------------------------===//
// Block Group Helpers
//===----------------------------------------------------------------------===//

static void ResetBG(LustreLinkDirMapLocalState &l) {
    l.block_group_active = false;
    l.block_group_max_ino = 0;
    l.next_block_group_in_batch = 0;
    l.block_group_batch_end = 0;
}

static bool InitDevice(LustreLinkDirMapGlobalState &g, idx_t dev) {
    if (dev >= g.device_paths.size()) { g.finished = true; return false; }
    g.current_device_path = g.device_paths[dev];
    g.next_fid_idx.store(0);
    g.next_block_group.store(0);
    g.active_block_groups.store(0);
    if (!g.use_sequential_scan) { g.device_initialized = true; return true; }
    MDTScanner probe;
    probe.Open(g.current_device_path);
    g.total_block_groups = static_cast<int>(probe.GetBlockGroupCount());
    g.inodes_per_group = probe.GetInodesPerGroup();
    probe.Close();
    g.device_initialized = true;
    return true;
}

static bool EnsureScanner(LustreLinkDirMapGlobalState &g, LustreLinkDirMapLocalState &l) {
    idx_t dev = g.current_device_idx.load();
    if (l.scanner_initialized && l.initialized_device_idx == dev) return true;
    if (l.scanner_initialized) { l.scanner->Close(); l.scanner_initialized = false; ResetBG(l); }
    if (dev >= g.device_paths.size() || g.finished.load()) return false;
    if (!g.device_initialized.load()) {
        lock_guard<mutex> lock(g.device_transition_lock);
        if (!g.device_initialized.load()) { if (!InitDevice(g, dev)) return false; }
    }
    l.scanner->Open(g.current_device_path);
    l.scanner->StartScan();
    l.scanner_initialized = true;
    l.initialized_device_idx = dev;
    l.initialized_device_path = g.current_device_path;
    ResetBG(l);
    l.EnsureResolveInitialized(g.device_paths);
    return true;
}

static bool ClaimBG(LustreLinkDirMapGlobalState &g, LustreLinkDirMapLocalState &l) {
    static constexpr int BATCH = 8;
    if (l.block_group_active) return true;
    if (l.next_block_group_in_batch >= l.block_group_batch_end) {
        int s = g.next_block_group.fetch_add(BATCH);
        if (s >= g.total_block_groups) return false;
        int e = s + BATCH;
        if (e > g.total_block_groups) e = g.total_block_groups;
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
    auto &g = data_p.global_state->Cast<LustreLinkDirMapGlobalState>();
    auto &l = data_p.local_state->Cast<LustreLinkDirMapLocalState>();

    if (g.fid_filter->HasDynamicFilter()) {
        bool changed = g.fid_filter->ResolveDynamicFilters();
        if (changed) {
            g.use_sequential_scan = g.fid_filter->RequiresGenericEvaluation() || !g.fid_filter->HasFIDFilter() ||
                                    g.fid_filter->fid_values.size() > LINK_DIRMAP_SEQ_SCAN_THRESHOLD;
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

    if (g.finished) { output.SetCardinality(0); return; }

    idx_t cnt = 0;
    LustreOutputStringCache string_cache(g.column_ids.size());
    while (cnt < STANDARD_VECTOR_SIZE) {
        if (!EnsureScanner(g, l)) break;

        if (!ClaimBG(g, l)) {
            if (g.active_block_groups.load() != 0) break;
            lock_guard<mutex> lock(g.device_transition_lock);
            idx_t cur = g.current_device_idx.load();
            if (l.initialized_device_idx == cur &&
                g.next_block_group.load() >= g.total_block_groups &&
                g.active_block_groups.load() == 0) {
                g.device_initialized.store(false);
                g.current_device_idx++;
                if (g.current_device_idx >= g.device_paths.size()) g.finished = true;
            }
            l.scanner_initialized = false;
            ResetBG(l);
            continue;
        }

        LustreLink link;
        if (!l.scanner->GetNextLink(link, g.scan_config, l.block_group_max_ino)) {
            l.block_group_active = false;
            l.dirmap_cache.clear();
            g.active_block_groups.fetch_sub(1);
            continue;
        }

        if (g.fid_filter->HasFIDFilter() && !g.fid_filter->ContainsFID(link.fid)) continue;
        if (g.fid_filter->RequiresGenericEvaluation() && !g.fid_filter->EvaluateFID(link.fid)) continue;

        DirMapCacheEntry dm;
        bool dm_valid = l.ResolveParentFID(link.parent_fid, dm);
        WriteRow(output, cnt, g.column_ids, link, dm, l.initialized_device_path, dm_valid, string_cache);
        cnt++;
    }
    output.SetCardinality(cnt);
}

//===----------------------------------------------------------------------===//
// Cardinality / GetFunctionSet
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

static const vector<string> &GetNames() { static const vector<string> n = LINK_DIRMAP_COLUMN_NAMES; return n; }
static const vector<LogicalType> &GetTypes() { static const vector<LogicalType> t = LINK_DIRMAP_COLUMN_TYPES; return t; }

const vector<string> &LustreLinkDirMapFunction::GetColumnNames() { return GetNames(); }
const vector<LogicalType> &LustreLinkDirMapFunction::GetColumnTypes() { return GetTypes(); }

TableFunction LustreLinkDirMapFunction::GetFunction(bool multi) {
    if (multi) {
        TableFunction f("lustre_link_dirmap", {LogicalType::LIST(LogicalType::VARCHAR)}, Execute, BindMulti, InitGlobal, InitLocal);
        SetProps(f); return f;
    }
    TableFunction f("lustre_link_dirmap", {LogicalType::VARCHAR}, Execute, BindSingle, InitGlobal, InitLocal);
    SetProps(f); return f;
}

TableFunctionSet LustreLinkDirMapFunction::GetFunctionSet() {
    TableFunctionSet set("lustre_link_dirmap");
    set.AddFunction(GetFunction(false));
    set.AddFunction(GetFunction(true));
    return set;
}

} // namespace lustre
} // namespace duckdb
