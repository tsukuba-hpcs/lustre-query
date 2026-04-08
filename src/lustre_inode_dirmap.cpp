//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_inode_dirmap.cpp
//
// Fused inode/dirmap table function: co-scans directory inodes and generates
// dirmap rows inline. Join: inodes.fid = dirmap.dir_fid
//===----------------------------------------------------------------------===//

#include "lustre_inode_dirmap.hpp"
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

namespace duckdb {
namespace lustre {

static constexpr idx_t INODE_DIRMAP_SEQ_SCAN_THRESHOLD = 8192;

//===----------------------------------------------------------------------===//
// Column Definitions
//===----------------------------------------------------------------------===//

static const vector<string> INODE_DIRMAP_COLUMN_NAMES = {
    "inode_fid", "ino", "type", "mode", "nlink", "uid", "gid", "size", "blocks",
    "atime", "mtime", "ctime", "projid", "flags", "device",
    "dir_fid", "parent_fid", "dir_device", "parent_device",
    "master_mdt_index", "stripe_index", "stripe_count",
    "hash_type", "layout_version", "source"
};

static const vector<LogicalType> INODE_DIRMAP_COLUMN_TYPES = {
    LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR, LogicalType::UINTEGER,
    LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::UBIGINT,
    LogicalType::UBIGINT, LogicalType::TIMESTAMP, LogicalType::TIMESTAMP, LogicalType::TIMESTAMP,
    LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::VARCHAR,
    LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
    LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::UINTEGER,
    LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::VARCHAR
};

//===----------------------------------------------------------------------===//
// PendingRow
//===----------------------------------------------------------------------===//

struct InodeDirMapPendingRow {
    LustreInode inode;
    LustreFID dir_fid;
    LustreFID parent_fid;
    std::string dir_device;
    std::string parent_device;
    uint32_t master_mdt_index = 0;
    uint32_t stripe_index = 0;
    uint32_t stripe_count = 1;
    uint32_t hash_type = 0;
    uint32_t layout_version = 0;
    std::string source = "plain";
};

//===----------------------------------------------------------------------===//
// Global State
//===----------------------------------------------------------------------===//

struct InodeDirMapGlobalState : public GlobalTableFunctionState {
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
        if (use_sequential_scan)
            return MinValue<idx_t>(thread_count, static_cast<idx_t>(MaxValue(total_block_groups, 1)));
        if (!fid_filter->HasFIDFilter()) return 1;
        return MinValue<idx_t>(fid_filter->fid_values.size(), thread_count);
    }
};

//===----------------------------------------------------------------------===//
// Local State
//===----------------------------------------------------------------------===//

struct InodeDirMapLocalState : public LocalTableFunctionState {
    unique_ptr<MDTScanner> scanner;
    bool scanner_initialized = false;
    idx_t initialized_device_idx = DConstants::INVALID_INDEX;
    string initialized_device_path;
    bool block_group_active = false;
    ext2_ino_t block_group_max_ino = 0;
    int next_block_group_in_batch = 0;
    int block_group_batch_end = 0;

    vector<InodeDirMapPendingRow> pending;
    idx_t pending_idx = 0;

    vector<unique_ptr<MDTScanner>> resolve_scanners;
    vector<string> resolve_device_paths;
    bool resolve_initialized = false;

    InodeDirMapLocalState() { scanner = make_uniq<MDTScanner>(); }

    void EnsureResolveInitialized(const vector<string> &dp) {
        if (resolve_initialized) return;
        resolve_scanners.resize(dp.size());
        resolve_device_paths = dp;
        for (idx_t i = 0; i < dp.size(); i++) {
            resolve_scanners[i] = make_uniq<MDTScanner>();
            resolve_scanners[i]->Open(dp[i]);
            resolve_scanners[i]->InitOI();
        }
        resolve_initialized = true;
    }

    bool ResolveFIDToDevice(const LustreFID &fid, string &out) {
        for (idx_t i = 0; i < resolve_scanners.size(); i++) {
            ext2_ino_t ino;
            if (resolve_scanners[i]->LookupFID(fid, ino)) { out = resolve_device_paths[i]; return true; }
        }
        out.clear();
        return false;
    }
};

//===----------------------------------------------------------------------===//
// Bind
//===----------------------------------------------------------------------===//

static unique_ptr<FunctionData> BindSingle(ClientContext &, TableFunctionBindInput &input,
                                           vector<LogicalType> &ret, vector<string> &names) {
    auto r = make_uniq<LustreQueryBindData>();
    r->device_paths.push_back(StringValue::Get(input.inputs[0]));
    ParseNamedParameters(input.named_parameters, r->scan_config);
    r->scan_config.skip_no_linkea = false;
    names = INODE_DIRMAP_COLUMN_NAMES;
    ret = INODE_DIRMAP_COLUMN_TYPES;
    return std::move(r);
}

static unique_ptr<FunctionData> BindMulti(ClientContext &, TableFunctionBindInput &input,
                                          vector<LogicalType> &ret, vector<string> &names) {
    auto r = make_uniq<LustreQueryBindData>();
    for (auto &v : ListValue::GetChildren(input.inputs[0])) r->device_paths.push_back(StringValue::Get(v));
    if (r->device_paths.empty()) throw BinderException("lustre_inode_dirmap requires at least one device path");
    ParseNamedParameters(input.named_parameters, r->scan_config);
    r->scan_config.skip_no_linkea = false;
    names = INODE_DIRMAP_COLUMN_NAMES;
    ret = INODE_DIRMAP_COLUMN_TYPES;
    return std::move(r);
}

//===----------------------------------------------------------------------===//
// Init
//===----------------------------------------------------------------------===//

static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &ctx, TableFunctionInitInput &input) {
    auto &bind = input.bind_data->Cast<LustreQueryBindData>();
    auto g = make_uniq<InodeDirMapGlobalState>();
    g->device_paths = bind.device_paths;
    g->column_ids = input.column_ids;
    g->scan_config = bind.scan_config;
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
         g->fid_filter->fid_values.size() > INODE_DIRMAP_SEQ_SCAN_THRESHOLD);
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
    return make_uniq<InodeDirMapLocalState>();
}

//===----------------------------------------------------------------------===//
// WriteRow
//===----------------------------------------------------------------------===//

static void WriteRow(DataChunk &output, idx_t row, const vector<idx_t> &cols,
                     const InodeDirMapPendingRow &r, const string &dev, LustreOutputStringCache &string_cache) {
    for (idx_t i = 0; i < cols.size(); i++) {
        auto &vec = output.data[i];
        switch (cols[i]) {
        case 0: FlatVector::GetData<string_t>(vec)[row] = string_cache.GetFID(vec, i, r.inode.fid); break;
        case 1: FlatVector::GetData<uint64_t>(vec)[row] = r.inode.ino; break;
        case 2: FlatVector::GetData<string_t>(vec)[row] = StringVector::AddString(vec, FileTypeToString(r.inode.type)); break;
        case 3: FlatVector::GetData<uint32_t>(vec)[row] = r.inode.mode; break;
        case 4: FlatVector::GetData<uint32_t>(vec)[row] = r.inode.nlink; break;
        case 5: FlatVector::GetData<uint32_t>(vec)[row] = r.inode.uid; break;
        case 6: FlatVector::GetData<uint32_t>(vec)[row] = r.inode.gid; break;
        case 7: FlatVector::GetData<uint64_t>(vec)[row] = r.inode.size; break;
        case 8: FlatVector::GetData<uint64_t>(vec)[row] = r.inode.blocks; break;
        case 9: FlatVector::GetData<timestamp_t>(vec)[row] = Timestamp::FromEpochSeconds(r.inode.atime); break;
        case 10: FlatVector::GetData<timestamp_t>(vec)[row] = Timestamp::FromEpochSeconds(r.inode.mtime); break;
        case 11: FlatVector::GetData<timestamp_t>(vec)[row] = Timestamp::FromEpochSeconds(r.inode.ctime); break;
        case 12: FlatVector::GetData<uint32_t>(vec)[row] = r.inode.projid; break;
        case 13: FlatVector::GetData<uint32_t>(vec)[row] = r.inode.flags; break;
        case 14: FlatVector::GetData<string_t>(vec)[row] = string_cache.GetString(vec, i, dev); break;
        case 15: FlatVector::GetData<string_t>(vec)[row] = string_cache.GetFID(vec, i, r.dir_fid); break;
        case 16: FlatVector::GetData<string_t>(vec)[row] = string_cache.GetFID(vec, i, r.parent_fid); break;
        case 17: FlatVector::GetData<string_t>(vec)[row] = string_cache.GetString(vec, i, r.dir_device); break;
        case 18: FlatVector::GetData<string_t>(vec)[row] = string_cache.GetString(vec, i, r.parent_device); break;
        case 19: FlatVector::GetData<uint32_t>(vec)[row] = r.master_mdt_index; break;
        case 20: FlatVector::GetData<uint32_t>(vec)[row] = r.stripe_index; break;
        case 21: FlatVector::GetData<uint32_t>(vec)[row] = r.stripe_count; break;
        case 22: FlatVector::GetData<uint32_t>(vec)[row] = r.hash_type; break;
        case 23: FlatVector::GetData<uint32_t>(vec)[row] = r.layout_version; break;
        case 24: FlatVector::GetData<string_t>(vec)[row] = string_cache.GetString(vec, i, r.source); break;
        default: break;
        }
    }
}

//===----------------------------------------------------------------------===//
// GenerateRows — same logic as lustre_dirmap but attaches inode metadata
//===----------------------------------------------------------------------===//

static void GenerateRows(const LustreInode &inode, const LustreFID &fid, const LustreLMV &lmv,
                          const std::vector<LinkEntry> &links, const std::vector<DirEntry> &dir_entries,
                          const std::string &device, InodeDirMapLocalState &lstate,
                          vector<InodeDirMapPendingRow> &out) {
    out.clear();

    if (!lmv.IsValid()) {
        InodeDirMapPendingRow r;
        r.inode = inode;
        r.dir_fid = fid;
        r.parent_fid = fid;
        r.dir_device = device;
        r.parent_device = device;
        r.source = "plain";
        out.push_back(std::move(r));
        return;
    }

    if (lmv.IsMaster()) {
        // Stripe 0 = master itself
        {
            InodeDirMapPendingRow r;
            r.inode = inode;
            r.dir_fid = fid;
            r.parent_fid = fid;
            r.dir_device = device;
            r.parent_device = device;
            r.master_mdt_index = lmv.lmv_master_mdt_index;
            r.stripe_count = lmv.lmv_stripe_count;
            r.hash_type = lmv.lmv_hash_type;
            r.layout_version = lmv.lmv_layout_version;
            r.source = "master";
            out.push_back(std::move(r));
        }
        // Shard entries
        for (auto &entry : dir_entries) {
            if (entry.name.empty() || entry.name[0] != '[') continue;
            auto bp = entry.name.find(']');
            if (bp == std::string::npos || bp + 1 >= entry.name.size() || entry.name[bp + 1] != ':') continue;
            LustreFID shard_fid;
            if (!LustreFID::FromString(entry.name.substr(0, bp + 1), shard_fid)) continue;
            char *end = nullptr;
            unsigned long idx = strtoul(entry.name.c_str() + bp + 2, &end, 10);
            if (end == entry.name.c_str() + bp + 2 || idx >= lmv.lmv_stripe_count) continue;

            std::string shard_dev;
            lstate.ResolveFIDToDevice(shard_fid, shard_dev);

            InodeDirMapPendingRow r;
            r.inode = inode;
            r.dir_fid = fid;
            r.parent_fid = shard_fid;
            r.dir_device = device;
            r.parent_device = shard_dev;
            r.master_mdt_index = lmv.lmv_master_mdt_index;
            r.stripe_index = static_cast<uint32_t>(idx);
            r.stripe_count = lmv.lmv_stripe_count;
            r.hash_type = lmv.lmv_hash_type;
            r.layout_version = lmv.lmv_layout_version;
            r.source = "master";
            out.push_back(std::move(r));
        }
        return;
    }

    // Slave: skip — slave's dir_fid != slave's own fid, so no match in i.fid = d.dir_fid
}

//===----------------------------------------------------------------------===//
// Block Group Helpers
//===----------------------------------------------------------------------===//

static void ResetBG(InodeDirMapLocalState &l) {
    l.block_group_active = false;
    l.block_group_max_ino = 0;
    l.next_block_group_in_batch = 0;
    l.block_group_batch_end = 0;
}

static bool InitDevice(InodeDirMapGlobalState &g, idx_t dev) {
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

static bool EnsureScanner(InodeDirMapGlobalState &g, InodeDirMapLocalState &l, bool seq) {
    idx_t dev = g.current_device_idx.load();
    if (l.scanner_initialized && l.initialized_device_idx == dev) return true;
    if (l.scanner_initialized) { l.scanner->Close(); l.scanner_initialized = false; ResetBG(l); }
    if (dev >= g.device_paths.size() || g.finished.load()) return false;
    if (!g.device_initialized.load()) {
        lock_guard<mutex> lock(g.device_transition_lock);
        if (!g.device_initialized.load()) { if (!InitDevice(g, dev)) return false; }
    }
    l.scanner->Open(g.current_device_path);
    if (seq) l.scanner->StartScan(); else l.scanner->InitOI();
    l.scanner_initialized = true;
    l.initialized_device_idx = dev;
    l.initialized_device_path = g.current_device_path;
    ResetBG(l);
    l.EnsureResolveInitialized(g.device_paths);
    return true;
}

static bool ClaimBG(InodeDirMapGlobalState &g, InodeDirMapLocalState &l) {
    static constexpr int BATCH = 8;
    if (l.block_group_active) return true;
    if (l.next_block_group_in_batch >= l.block_group_batch_end) {
        int s = g.next_block_group.fetch_add(BATCH);
        if (s >= g.total_block_groups) return false;
        int e = s + BATCH; if (e > g.total_block_groups) e = g.total_block_groups;
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
    auto &g = data_p.global_state->Cast<InodeDirMapGlobalState>();
    auto &l = data_p.local_state->Cast<InodeDirMapLocalState>();

    if (g.fid_filter->HasDynamicFilter()) {
        bool changed = g.fid_filter->ResolveDynamicFilters();
        if (changed) {
            g.use_sequential_scan = g.fid_filter->RequiresGenericEvaluation() || !g.fid_filter->HasFIDFilter() ||
                                    g.fid_filter->fid_values.size() > INODE_DIRMAP_SEQ_SCAN_THRESHOLD;
            g.finished = false;
            g.next_fid_idx.store(0);
            g.next_block_group.store(0);
            g.active_block_groups.store(0);
            g.current_device_idx = 0;
            g.device_initialized = false;
            l.scanner_initialized = false;
            ResetBG(l);
            l.pending.clear();
            l.pending_idx = 0;
        }
    }

    if (g.finished) { output.SetCardinality(0); return; }
    idx_t cnt = 0;
    LustreOutputStringCache string_cache(g.column_ids.size());

    while (cnt < STANDARD_VECTOR_SIZE) {
        // Drain pending
        while (l.pending_idx < l.pending.size() && cnt < STANDARD_VECTOR_SIZE) {
            WriteRow(output, cnt, g.column_ids, l.pending[l.pending_idx], l.initialized_device_path, string_cache);
            l.pending_idx++;
            cnt++;
        }
        if (cnt >= STANDARD_VECTOR_SIZE) break;

        if (!EnsureScanner(g, l, g.use_sequential_scan)) break;

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

        ext2_ino_t ino;
        LustreFID fid;
        LustreLMV lmv;
        std::vector<LinkEntry> links;
        std::vector<DirEntry> dir_entries;
        uint32_t lma_incompat = 0;

        if (!l.scanner->GetNextDirMapEntries(ino, fid, lmv, links, dir_entries,
                                              lma_incompat,
                                              g.scan_config, l.block_group_max_ino)) {
            l.block_group_active = false;
            g.active_block_groups.fetch_sub(1);
            continue;
        }

        if (g.fid_filter->HasFIDFilter() && !g.fid_filter->ContainsFID(fid)) continue;
        if (g.fid_filter->RequiresGenericEvaluation() && !g.fid_filter->EvaluateFID(fid)) continue;

        // Get full inode metadata
        LustreInode inode;
        if (!l.scanner->ReadInode(ino, inode, g.scan_config)) continue;

        l.pending.clear();
        l.pending_idx = 0;
        GenerateRows(inode, fid, lmv, links, dir_entries, l.initialized_device_path, l, l.pending);
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

static const vector<string> &GetNames() { static const vector<string> n = INODE_DIRMAP_COLUMN_NAMES; return n; }
static const vector<LogicalType> &GetTypes() { static const vector<LogicalType> t = INODE_DIRMAP_COLUMN_TYPES; return t; }

const vector<string> &LustreInodeDirMapFunction::GetColumnNames() { return GetNames(); }
const vector<LogicalType> &LustreInodeDirMapFunction::GetColumnTypes() { return GetTypes(); }

TableFunction LustreInodeDirMapFunction::GetFunction(bool multi) {
    if (multi) {
        TableFunction f("lustre_inode_dirmap", {LogicalType::LIST(LogicalType::VARCHAR)}, Execute, BindMulti, InitGlobal, InitLocal);
        SetProps(f); return f;
    }
    TableFunction f("lustre_inode_dirmap", {LogicalType::VARCHAR}, Execute, BindSingle, InitGlobal, InitLocal);
    SetProps(f); return f;
}

TableFunctionSet LustreInodeDirMapFunction::GetFunctionSet() {
    TableFunctionSet set("lustre_inode_dirmap");
    set.AddFunction(GetFunction(false));
    set.AddFunction(GetFunction(true));
    return set;
}

} // namespace lustre
} // namespace duckdb
