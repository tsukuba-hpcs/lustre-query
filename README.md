# lustre_query

A DuckDB extension for scanning Lustre MDT (Metadata Target) devices directly from SQL. It reads ldiskfs-formatted MDT devices through the Lustre-patched `libext2fs` from Whamcloud `e2fsprogs`.

## Dependencies

- DuckDB v1.5.1
- Lustre-patched `libext2fs` (Whamcloud `e2fsprogs`)

This extension requires the Lustre-patched `e2fsprogs`, not the standard distribution package.

Download: https://downloads.whamcloud.com/public/e2fsprogs/

## Build

```bash
git clone --recurse-submodules https://github.com/tsukuba-hpcs/lustre-query.git
cd lustre-query
make
```

Build artifacts are written to `build/release/extension/lustre_query/`.

## Load

```sql
LOAD 'build/release/extension/lustre_query/lustre_query.duckdb_extension';
```

## API Summary

### Table Functions

All table functions accept either a single device path (`VARCHAR`) or a list of device paths (`LIST(VARCHAR)`).

| Signature | Description |
|-----------|-------------|
| `lustre_inodes(device)` / `lustre_inodes(devices)` | Inode metadata: FID, size, ownership, permissions, timestamps, and flags |
| `lustre_links(device)` / `lustre_links(devices)` | Hard-link entries: inode FID, parent directory FID, and filename |
| `lustre_layouts(device)` / `lustre_layouts(devices)` | LOV layout components: stripe layout, PFL/FLR/EC metadata, and pool information |
| `lustre_objects(device)` / `lustre_objects(devices)` | Per-stripe OST object placement |
| `lustre_dirmap(device)` / `lustre_dirmap(devices)` | Logical-directory mapping for plain directories, DNE1 remote-parent cases, and DNE2 striped directories |

### Scalar Functions

Scalar functions take the lookup target as the first argument and the device or device list as the second argument.

| Signature | Returns | Description |
|-----------|---------|-------------|
| `lustre_fid2path(fid, device)` / `lustre_fid2path(fid, devices)` | `LIST(VARCHAR)` | Resolves a FID to one or more filesystem paths by traversing LinkEA. Multiple paths can be returned for hard-linked files. |
| `lustre_path2fid(path, device)` / `lustre_path2fid(path, devices)` | `VARCHAR` | Resolves a filesystem path to a Lustre FID by walking the directory tree from the MDT root. |

### Common Named Parameters

The public table functions accept these named parameters:

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `skip_no_fid` | `BOOLEAN` | `true` | Skip inodes that do not have a valid Lustre FID |
| `skip_no_linkea` | `BOOLEAN` | `true` | Skip inodes that do not have LinkEA metadata |

`lustre_dirmap` and the optimizer's internal dirmap fused scans force `skip_no_linkea = false`, because directory mapping depends on link metadata.

## Examples

```sql
-- Scan all inodes from one MDT
SELECT *
FROM lustre_inodes('/dev/mapper/mdt0');

-- Find large files owned by a specific user
SELECT fid, size, mtime
FROM lustre_inodes('/dev/mapper/mdt0')
WHERE uid = 1000 AND size > 1e9
ORDER BY size DESC;

-- Look up hard-link entries for a specific FID
SELECT *
FROM lustre_links('/dev/mapper/mdt0')
WHERE fid = '[0x200000401:0x1:0x0]';

-- Join inode metadata with directory entries
SELECT i.fid, i.size, l.name
FROM lustre_inodes('/dev/mapper/mdt0') AS i
JOIN lustre_links('/dev/mapper/mdt0') AS l
  ON i.fid = l.fid;

-- Resolve a FID to one or more paths
SELECT fid, lustre_fid2path(fid, '/dev/mapper/mdt0') AS paths
FROM lustre_inodes('/dev/mapper/mdt0')
WHERE uid = 0 AND type = 'file';

-- Resolve a path across multiple MDTs
SELECT lustre_path2fid('/project/a/b/file.dat',
                       ['/dev/mapper/mdt0', '/dev/mapper/mdt1']) AS fid;

-- Scan multiple MDTs
SELECT *
FROM lustre_inodes(['/dev/mapper/mdt0', '/dev/mapper/mdt1']);

-- DNE-aware logical directory entry counts
SELECT d.dir_fid, count(*) AS entry_count
FROM lustre_links(['/dev/mapper/mdt0', '/dev/mapper/mdt1']) AS l
JOIN lustre_dirmap(['/dev/mapper/mdt0', '/dev/mapper/mdt1']) AS d
  ON l.parent_fid = d.parent_fid
JOIN lustre_inodes(['/dev/mapper/mdt0', '/dev/mapper/mdt1']) AS i
  ON i.fid = d.dir_fid
WHERE i.type = 'dir'
GROUP BY d.dir_fid;

-- Find directories whose parent lives on another MDT (DNE1)
SELECT dir_fid, dir_device, lma_incompat
FROM lustre_dirmap(['/dev/mapper/mdt0', '/dev/mapper/mdt1'])
WHERE lma_incompat & 4 != 0; -- LMAI_REMOTE_PARENT
```

## Execution Notes

- Parallel scans partition work by block group.
- Projection pushdown and column pruning avoid decoding unused fields.
- Equality and `IN` predicates on FIDs are pushed down to OI lookups when possible.
- `lustre_layouts` and `lustre_objects` are most effective when the query filters on `fid`.
- `lustre_dirmap` normalizes physical namespace-bearing objects to logical directory identities and skips DNE1 agent inodes automatically.

## Optimizer Rewrites

The optimizer can replace common join shapes with internal fused scans. These internal table functions are implementation details; normal queries should use the public functions above.

| Query Pattern | Internal Rewrite | Effect |
|---------------|------------------|--------|
| `lustre_links JOIN lustre_dirmap ON parent_fid` | `lustre_link_dirmap` | Resolves parent-directory mapping inline |
| `lustre_inodes JOIN lustre_dirmap ON fid = dir_fid` | `lustre_inode_dirmap` | Scans directory inodes together with dirmap rows |
| `lustre_links JOIN lustre_dirmap JOIN lustre_inodes` | `lustre_link_inode_dirmap` | Replaces the multi-stage join with a single fused scan plus cached lookups |

## Column Reference

### lustre_inodes

| Column | Type | Description |
|--------|------|-------------|
| `fid` | `VARCHAR` | Lustre FID (`[seq:oid:ver]`) |
| `ino` | `UBIGINT` | Inode number |
| `type` | `VARCHAR` | File type (`file`, `dir`, `link`, ...) |
| `mode` | `UINTEGER` | Permission bits |
| `nlink` | `UINTEGER` | Hard-link count |
| `uid` | `UINTEGER` | User ID |
| `gid` | `UINTEGER` | Group ID |
| `size` | `UBIGINT` | File size in bytes |
| `blocks` | `UBIGINT` | Block count |
| `atime` | `TIMESTAMP` | Access time |
| `mtime` | `TIMESTAMP` | Modification time |
| `ctime` | `TIMESTAMP` | Change time |
| `projid` | `UINTEGER` | Project ID |
| `flags` | `UINTEGER` | Inode flags |
| `device` | `VARCHAR` | Source device path |

### lustre_links

| Column | Type | Description |
|--------|------|-------------|
| `fid` | `VARCHAR` | Lustre FID |
| `parent_fid` | `VARCHAR` | Parent directory FID |
| `name` | `VARCHAR` | Filename within `parent_fid` |
| `device` | `VARCHAR` | Source device path |

### lustre_layouts

| Column | Type | Description |
|--------|------|-------------|
| `fid` | `VARCHAR` | Lustre FID |
| `comp_index` | `UINTEGER` | Component index |
| `comp_id` | `UINTEGER` | Component ID |
| `mirror_id` | `USMALLINT` | Mirror ID |
| `comp_flags` | `UINTEGER` | Component flags |
| `extent_start` | `UBIGINT` | Start offset |
| `extent_end` | `UBIGINT` | End offset |
| `pattern` | `UINTEGER` | Stripe pattern |
| `stripe_size` | `UINTEGER` | Stripe size in bytes |
| `stripe_count` | `USMALLINT` | Stripe count |
| `stripe_offset` | `USMALLINT` | First OST index |
| `pool` | `VARCHAR` | OST pool name |
| `dstripe_count` | `UTINYINT` | EC data stripe count (`k`) |
| `cstripe_count` | `UTINYINT` | EC code stripe count (`p`) |
| `compr_type` | `UTINYINT` | Compression type |
| `compr_lvl` | `UTINYINT` | Compression level |
| `device` | `VARCHAR` | Source device path |

### lustre_objects

| Column | Type | Description |
|--------|------|-------------|
| `fid` | `VARCHAR` | Lustre FID |
| `comp_index` | `UINTEGER` | Component index |
| `stripe_index` | `UINTEGER` | Stripe index within the component |
| `ost_idx` | `UINTEGER` | OST index |
| `ost_oi_id` | `UBIGINT` | OST object ID |
| `ost_oi_seq` | `UBIGINT` | OST object sequence |
| `device` | `VARCHAR` | Source device path |

### lustre_dirmap

`lustre_dirmap` maps physical namespace-bearing parent objects (plain directories or striped shards) to logical directory identities.

| Column | Type | Description |
|--------|------|-------------|
| `dir_fid` | `VARCHAR` | Logical directory FID (master FID for striped directories, self for plain directories) |
| `parent_fid` | `VARCHAR` | Physical namespace-bearing object FID (shard FID, or self for plain directories) |
| `dir_device` | `VARCHAR` | Device where `dir_fid` lives |
| `parent_device` | `VARCHAR` | Device where `parent_fid` lives |
| `master_mdt_index` | `UINTEGER` | MDT index of the master directory |
| `stripe_index` | `UINTEGER` | Stripe index (`0` for master/plain directories) |
| `stripe_count` | `UINTEGER` | Total stripe count (`1` for plain directories) |
| `hash_type` | `UINTEGER` | LMV hash type and flags |
| `layout_version` | `UINTEGER` | LMV layout version |
| `source` | `VARCHAR` | Row source: `plain`, `master`, or `slave` |
| `lma_incompat` | `UINTEGER` | LMA incompat flags |

When multiple devices are provided, cross-MDT OI lookup resolves the device columns. Masters emit authoritative shard mappings; slaves are emitted only as fallback when the master cannot be reached. DNE1 agent inodes (`LMAI_AGENT`) are skipped automatically.

### LMA incompat flags

| Flag | Value | Meaning |
|------|-------|---------|
| `LMAI_RELEASED` | `0x01` | File released (HSM) |
| `LMAI_AGENT` | `0x02` | Agent inode (DNE1 remote directory stub); skipped by `lustre_dirmap` |
| `LMAI_REMOTE_PARENT` | `0x04` | Parent directory is on a remote MDT |
| `LMAI_STRIPED` | `0x08` | Striped directory (DNE2) |
| `LMAI_ORPHAN` | `0x10` | Orphan inode |
