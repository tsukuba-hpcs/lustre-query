# lustre_query

A DuckDB extension that scans Lustre MDT (Metadata Target) devices directly via SQL. Uses libext2fs to read metadata from ldiskfs-formatted MDT devices.

## Table Functions

| Function | Description |
|----------|-------------|
| `lustre_inodes(device)` | Inode metadata (FID, size, permissions, timestamps, etc.) |
| `lustre_links(device)` | Hard link information (FID, parent FID, filename) |
| `lustre_layouts(device)` | Layout components (stripe config, PFL/FLR/EC) |
| `lustre_objects(device)` | OST object placement (per-stripe OST assignment) |
| `lustre_dirmap(device)` | DNE2-aware directory mapping (physical parent FID to logical directory FID) |

Each function accepts a single device path (`VARCHAR`) or a list of devices (`LIST(VARCHAR)`).

## Scalar Functions

| Function | Description |
|----------|-------------|
| `lustre_fid2path(device, fid)` | Resolve FID to filesystem path via LinkEA traversal |
| `lustre_path2fid(device, path)` | Resolve filesystem path to FID via directory tree traversal |

## Examples

```sql
-- Scan all inodes from an MDT device
SELECT * FROM lustre_inodes('/dev/mapper/mdt0');

-- Find large files owned by a specific user
SELECT fid, size, mtime
FROM lustre_inodes('/dev/mapper/mdt0')
WHERE uid = 1000 AND size > 1e9
ORDER BY size DESC;

-- Look up hard links by FID (uses OI B-tree lookup)
SELECT * FROM lustre_links('/dev/mapper/mdt0')
WHERE fid = '[0x200000401:0x1:0x0]';

-- Join inodes with links (optimizer auto-rewrites to fused scan)
SELECT i.fid, i.size, l.name
FROM lustre_inodes('/dev/mapper/mdt0') i
JOIN lustre_links('/dev/mapper/mdt0') l ON i.fid = l.fid;

-- Resolve FID to path
SELECT lustre_fid2path('/dev/mapper/mdt0', fid) AS path
FROM lustre_inodes('/dev/mapper/mdt0')
WHERE uid = 0 AND type = 'file';

-- Scan multiple MDTs
SELECT * FROM lustre_inodes(['/dev/mapper/mdt0', '/dev/mapper/mdt1']);

-- DNE2-aware logical directory entry count (correct for striped directories)
SELECT
  d.dir_fid,
  count(*) AS entry_count
FROM lustre_links(['/dev/mapper/mdt0', '/dev/mapper/mdt1']) l
JOIN lustre_dirmap(['/dev/mapper/mdt0', '/dev/mapper/mdt1']) d
  ON l.parent_fid = d.parent_fid
JOIN lustre_inodes(['/dev/mapper/mdt0', '/dev/mapper/mdt1']) i
  ON i.fid = d.dir_fid
WHERE i.type = 'dir'
GROUP BY d.dir_fid;
```

## Key Features

- **Parallel scanning** — Partitions work by block group for multi-threaded scans of large MDTs
- **Filter pushdown** — Converts FID predicates in WHERE clauses to OI B-tree lookups, avoiding full scans
- **Fused scan optimization** — The optimizer detects joins between `lustre_inodes` and `lustre_links` / `lustre_layouts` / `lustre_objects`, rewriting them into single-pass fused scans
- **DNE2-aware directory mapping** — `lustre_dirmap` normalizes striped directory shards to logical directory identities, with cross-MDT cooperative FID resolution
- **Column pruning** — Reads only the requested columns, skipping unnecessary xattr parsing

## Dependencies

- DuckDB v1.5.1
- libext2fs (e2fsprogs development package)

This extension requires the Lustre-patched version of e2fsprogs from Whamcloud, not the standard distribution package.

Download from https://downloads.whamcloud.com/public/e2fsprogs/:

## Build

```bash
git clone --recurse-submodules https://github.com/tsukuba-hpcs/lustre-query.git
cd lustre-query
make
```

Build artifacts are output to `build/release/extension/lustre_query/`.

## Loading

```sql
LOAD 'build/release/extension/lustre_query/lustre_query.duckdb_extension';
```

## Column Reference

### lustre_inodes

| Column | Type | Description |
|--------|------|-------------|
| fid | VARCHAR | Lustre FID (`[seq:oid:ver]`) |
| ino | UBIGINT | Inode number |
| type | VARCHAR | File type (file/dir/link/...) |
| mode | UINTEGER | Permissions |
| nlink | UINTEGER | Hard link count |
| uid | UINTEGER | User ID |
| gid | UINTEGER | Group ID |
| size | UBIGINT | File size in bytes |
| blocks | UBIGINT | Block count |
| atime | TIMESTAMP | Access time |
| mtime | TIMESTAMP | Modification time |
| ctime | TIMESTAMP | Change time |
| projid | UINTEGER | Project ID |
| flags | UINTEGER | Inode flags |
| device | VARCHAR | Source device path |

### lustre_links

| Column | Type | Description |
|--------|------|-------------|
| fid | VARCHAR | Lustre FID |
| parent_fid | VARCHAR | Parent directory FID |
| name | VARCHAR | Filename |
| device | VARCHAR | Source device path |

### lustre_layouts

| Column | Type | Description |
|--------|------|-------------|
| fid | VARCHAR | Lustre FID |
| comp_index | UINTEGER | Component index |
| comp_id | UINTEGER | Component ID |
| mirror_id | UINTEGER | Mirror ID |
| comp_flags | UINTEGER | Component flags |
| extent_start | UBIGINT | Start offset |
| extent_end | UBIGINT | End offset |
| pattern | UINTEGER | Stripe pattern |
| stripe_size | UINTEGER | Stripe size in bytes |
| stripe_count | UINTEGER | Stripe count |
| stripe_offset | UINTEGER | First OST index |
| pool | VARCHAR | OST pool name |
| dstripe_count | UTINYINT | EC data stripe count (k) |
| cstripe_count | UTINYINT | EC code stripe count (p) |
| compr_type | UTINYINT | Compression type |
| compr_lvl | UTINYINT | Compression level |
| device | VARCHAR | Source device path |

### lustre_objects

| Column | Type | Description |
|--------|------|-------------|
| fid | VARCHAR | Lustre FID |
| comp_index | UINTEGER | Component index |
| stripe_index | UINTEGER | Stripe index |
| ost_idx | UINTEGER | OST index |
| ost_oi_id | UBIGINT | OST object ID |
| ost_oi_seq | UBIGINT | OST object sequence |
| device | VARCHAR | Source device path |

### lustre_dirmap

Maps physical namespace-bearing parent objects (shards or plain directories) to logical directory identities. Under DNE2, `lustre_links.parent_fid` may point to a shard rather than the master directory. This table provides the normalization layer.

| Column | Type | Description |
|--------|------|-------------|
| dir_fid | VARCHAR | Logical directory FID (master FID for striped, self for plain) |
| parent_fid | VARCHAR | Physical namespace-bearing object FID (shard FID or self for plain) |
| dir_device | VARCHAR | Device where dir_fid lives |
| parent_device | VARCHAR | Device where parent_fid lives |
| master_mdt_index | UINTEGER | MDT index of master directory |
| stripe_index | UINTEGER | Stripe index (0 for master/plain) |
| stripe_count | UINTEGER | Total stripe count (1 for plain) |
| hash_type | UINTEGER | LMV hash type and flags |
| layout_version | UINTEGER | LMV layout version |
| source | VARCHAR | Row source: `plain`, `master`, or `slave` |

When multiple devices are provided, cross-MDT OI lookup resolves all device columns. Masters emit all shard mappings authoritatively; slaves are skipped when their master is reachable (emitted as fallback only for partial scans).
