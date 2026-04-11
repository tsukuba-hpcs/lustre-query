/*
 * oi_lookup.c - Lustre OI (Object Index) lookup using libext2fs
 *
 * This tool reads OI files from a Lustre ldiskfs filesystem and
 * performs FID to inode number translation.
 *
 * Build: gcc -o oi_lookup oi_lookup.c -lext2fs -lcom_err
 */

#include "oi_lookup.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <ext2fs/ext2fs.h>

/* ---------- On-disk structures (from Lustre source) ---------- */

/* IAM lfix root header - 16 bytes */
struct iam_lfix_root {
	uint64_t ilr_magic; /* 0xbedabb1ed (little-endian on disk) */
	uint16_t ilr_keysize;
	uint16_t ilr_recsize;
	uint16_t ilr_ptrsize;
	uint8_t ilr_indirect_levels;
	uint8_t ilr_padding;
} __attribute__((packed));

#define IAM_LFIX_ROOT_MAGIC 0xbedabb1edULL

/* dx_countlimit - 4 bytes, follows root header */
struct dx_countlimit {
	uint16_t limit;
	uint16_t count;
} __attribute__((packed));

/* IAM leaf header - 4 bytes */
struct iam_leaf_head {
	uint16_t ill_magic;
	uint16_t ill_count;
} __attribute__((packed));

#define IAM_LEAF_HEADER_MAGIC 0x1976

/* OSD inode ID - 8 bytes, stored big-endian in OI */
struct osd_inode_id {
	uint32_t oii_ino;
	uint32_t oii_gen;
};

/* ---------- Byte order helpers ---------- */

static inline uint16_t le16_to_cpu(uint16_t x) {
	return x; /* Assuming little-endian host */
}

static inline uint32_t le32_to_cpu(uint32_t x) {
	return x;
}

static inline uint64_t le64_to_cpu(uint64_t x) {
	return x;
}

static inline uint32_t be32_to_cpu(uint32_t x) {
	return __builtin_bswap32(x);
}

static inline uint64_t be64_to_cpu(uint64_t x) {
	return __builtin_bswap64(x);
}

static inline void cpu_to_be_fid(const struct lu_fid *src, void *dst) {
	uint8_t *p = dst;
	uint64_t seq = __builtin_bswap64(src->f_seq);
	uint32_t oid = __builtin_bswap32(src->f_oid);
	uint32_t ver = __builtin_bswap32(src->f_ver);
	memcpy(p, &seq, 8);
	memcpy(p + 8, &oid, 4);
	memcpy(p + 12, &ver, 4);
}

/* ---------- OI context ---------- */

struct oi_context {
	ext2_filsys fs;
	int oi_count;          /* Number of OI files (1, 2, 4, ..., 64) */
	ext2_ino_t *oi_inodes; /* Array of OI file inodes */
	ext2_file_t *oi_files; /* Persistent file handles for OI files */

	/* Cached parameters from root (same for all OI files) */
	uint16_t keysize; /* Should be 16 for FID */
	uint16_t recsize; /* Should be 8 for osd_inode_id */
	uint16_t ptrsize; /* Should be 4 */

	/* Small set-associative cache for OI B-tree blocks */
	uint64_t cache_clock;
	void *cache_storage;
	struct oi_block_cache_entry *cache_entries;

	int verbose; /* Verbose output flag */
};

#define OI_BLOCK_CACHE_WAYS    4
#define OI_BLOCK_CACHE_SETS    64
#define OI_BLOCK_CACHE_ENTRIES (OI_BLOCK_CACHE_WAYS * OI_BLOCK_CACHE_SETS)

struct oi_block_cache_entry {
	uint64_t block;
	uint64_t last_used;
	int oi_idx;
	int valid;
	void *data;
};

/* ---------- Directory scanning for OI files ---------- */

struct oi_scan_ctx {
	ext2_filsys fs;
	ext2_ino_t oi_inodes[64];
	int max_idx;
};

static int oi_dir_iterate_cb(struct ext2_dir_entry *dirent, int offset, int blocksize, char *buf, void *priv_data) {
	struct oi_scan_ctx *ctx = priv_data;
	char name[256];
	int namelen = ext2fs_dirent_name_len(dirent);
	int idx;

	(void)offset;
	(void)blocksize;
	(void)buf;

	if ((size_t)namelen >= sizeof(name))
		return 0;

	memcpy(name, dirent->name, namelen);
	name[namelen] = '\0';

	/* Match "oi.16" or "oi.16.N" */
	if (strcmp(name, "oi.16") == 0) {
		ctx->oi_inodes[0] = dirent->inode;
		if (ctx->max_idx < 0)
			ctx->max_idx = 0;
	} else if (strncmp(name, "oi.16.", 6) == 0) {
		idx = atoi(name + 6);
		if (idx >= 0 && idx < 64) {
			ctx->oi_inodes[idx] = dirent->inode;
			if (idx > ctx->max_idx)
				ctx->max_idx = idx;
		}
	}

	return 0;
}

static int oi_scan_files(struct oi_context *ctx) {
	struct oi_scan_ctx scan = {.fs = ctx->fs, .max_idx = -1};
	errcode_t err;
	int i;

	memset(scan.oi_inodes, 0, sizeof(scan.oi_inodes));

	/* Iterate root directory */
	err = ext2fs_dir_iterate(ctx->fs, EXT2_ROOT_INO, 0, NULL, oi_dir_iterate_cb, &scan);
	if (err) {
		fprintf(stderr, "Error iterating root directory: %s\n", error_message(err));
		return -EIO;
	}

	if (scan.max_idx < 0) {
		fprintf(stderr, "No OI files found\n");
		return -ENOENT;
	}

	/* Determine oi_count: must be power of 2 */
	if (scan.max_idx == 0) {
		ctx->oi_count = 1;
	} else {
		/* Round up to next power of 2 */
		ctx->oi_count = 1;
		while (ctx->oi_count <= scan.max_idx)
			ctx->oi_count *= 2;
	}

	ctx->oi_inodes = malloc(ctx->oi_count * sizeof(ext2_ino_t));
	if (!ctx->oi_inodes)
		return -ENOMEM;

	for (i = 0; i < ctx->oi_count; i++) {
		ctx->oi_inodes[i] = scan.oi_inodes[i];
		if (ctx->oi_inodes[i] == 0 && ctx->verbose) {
			fprintf(stderr, "Warning: OI file index %d not found\n", i);
		}
	}

	return 0;
}

/* ---------- Block reading helpers ---------- */

static int ensure_oi_file_open(struct oi_context *ctx, int oi_idx) {
	errcode_t err;
	ext2_ino_t oi_ino;

	if (oi_idx < 0 || oi_idx >= ctx->oi_count)
		return -EINVAL;

	if (ctx->oi_files[oi_idx])
		return 0;

	oi_ino = ctx->oi_inodes[oi_idx];
	if (oi_ino == 0)
		return -ENOENT;

	err = ext2fs_file_open(ctx->fs, oi_ino, 0, &ctx->oi_files[oi_idx]);
	if (err) {
		fprintf(stderr, "Cannot open OI file inode %u: %s\n", oi_ino, error_message(err));
		return -EIO;
	}

	return 0;
}

static int read_oi_block_uncached(struct oi_context *ctx, int oi_idx, blk64_t lblk, void *buf) {
	errcode_t err;
	unsigned int got;
	uint64_t offset;
	int ret;

	ret = ensure_oi_file_open(ctx, oi_idx);
	if (ret)
		return ret;

	offset = lblk * ctx->fs->blocksize;
	err = ext2fs_file_llseek(ctx->oi_files[oi_idx], offset, EXT2_SEEK_SET, NULL);
	if (err)
		return -EIO;

	err = ext2fs_file_read(ctx->oi_files[oi_idx], buf, ctx->fs->blocksize, &got);

	if (err || got != ctx->fs->blocksize) {
		fprintf(stderr, "Cannot read block %llu: %s\n", (unsigned long long)lblk,
		        err ? error_message(err) : "short read");
		return -EIO;
	}

	return 0;
}

static unsigned int oi_block_cache_hash(int oi_idx, blk64_t lblk) {
	uint64_t hash = (uint64_t)(unsigned int)oi_idx * 11400714819323198485ull;
	hash ^= (uint64_t)lblk * 14029467366897019727ull;
	return (unsigned int)(hash & (OI_BLOCK_CACHE_SETS - 1));
}

static int read_oi_block_cached(struct oi_context *ctx, int oi_idx, blk64_t lblk, const void **buf_out) {
	struct oi_block_cache_entry *set;
	struct oi_block_cache_entry *victim;
	unsigned int set_idx;
	int way;
	int ret;

	set_idx = oi_block_cache_hash(oi_idx, lblk);
	set = ctx->cache_entries + (set_idx * OI_BLOCK_CACHE_WAYS);
	victim = &set[0];

	for (way = 0; way < OI_BLOCK_CACHE_WAYS; way++) {
		struct oi_block_cache_entry *entry = &set[way];
		if (entry->valid && entry->oi_idx == oi_idx && entry->block == (uint64_t)lblk) {
			entry->last_used = ++ctx->cache_clock;
			*buf_out = entry->data;
			return 0;
		}
		if (!entry->valid || entry->last_used < victim->last_used)
			victim = entry;
	}

	ret = read_oi_block_uncached(ctx, oi_idx, lblk, victim->data);
	if (ret)
		return ret;

	victim->valid = 1;
	victim->oi_idx = oi_idx;
	victim->block = (uint64_t)lblk;
	victim->last_used = ++ctx->cache_clock;
	*buf_out = victim->data;
	return 0;
}

/* ---------- IAM B-tree traversal ---------- */

/*
 * Get pointer value from index entry.
 * Entry layout: [ikey (keysize bytes)] [ptr (ptrsize bytes)]
 */
static uint32_t get_entry_ptr(struct oi_context *ctx, void *entry) {
	void *ptr_addr = (char *)entry + ctx->keysize;
	if (ctx->ptrsize == 4) {
		uint32_t val;
		memcpy(&val, ptr_addr, 4);
		return le32_to_cpu(val);
	}
	/* ptrsize == 8 case (unlikely for OI) */
	uint64_t val;
	memcpy(&val, ptr_addr, 8);
	return (uint32_t)le64_to_cpu(val);
}

/*
 * Compare FID keys (big-endian comparison = memcmp).
 * Returns: <0 if k1 < k2, 0 if equal, >0 if k1 > k2
 */
static int keycmp(const void *k1, const void *k2, size_t keysize) {
	return memcmp(k1, k2, keysize);
}

/*
 * Binary search in index node entries.
 * Returns the entry whose key is <= target key.
 *
 * For lfix format:
 * - Entry 0: dx_countlimit + idle_blocks + padding (20 bytes)
 * - Entry 1+: real index entries
 *
 * The kernel's iam_find_position starts search at entry 2 for lfix (non-compat),
 * but with count=2 (countlimit + 1 real entry), it returns entry 1.
 *
 * entries: points to start of entries area (after root header)
 * count: from dx_countlimit.count (includes countlimit itself)
 */
static void *find_index_entry(struct oi_context *ctx, void *entries, int count, const void *key) {
	int entry_size = ctx->keysize + ctx->ptrsize;
	void *p, *q, *m;

	/*
	 * For lfix format, first real entry is at index 1.
	 * Index 0 contains dx_countlimit + idle_blocks + padding.
	 *
	 * Kernel uses start_idx=2 for the binary search algorithm, but
	 * since it returns (p-1), with count=2 it effectively returns entry 1.
	 *
	 * We simplify: if count <= 1, no real entries. Otherwise, real entries
	 * are at indices 1 through (count-1).
	 */
	if (count <= 1) {
		/* No real entries, shouldn't happen in valid OI */
		return entries;
	}

	/* First real entry at index 1, last at index (count-1) */
	p = (char *)entries + 1 * entry_size;
	q = (char *)entries + (count - 1) * entry_size;

	/* If only one real entry, return it */
	if (p == q) {
		return p;
	}

	/* If target key is less than first entry's key, return first entry */
	if (keycmp(key, p, ctx->keysize) < 0) {
		return p;
	}

	/* If target key is >= last entry's key, return last entry */
	if (keycmp(key, q, ctx->keysize) >= 0) {
		return q;
	}

	/* Binary search: find largest entry with key <= target */
	while ((char *)p + entry_size < (char *)q) {
		m = (char *)p + (((char *)q - (char *)p) / entry_size / 2) * entry_size;
		if (keycmp(m, key, ctx->keysize) <= 0)
			p = (char *)m + entry_size;
		else
			q = m;
	}

	/* Now p points to the entry we want, or just past it */
	if (keycmp(p, key, ctx->keysize) <= 0)
		return p;
	return (char *)p - entry_size;
}

/*
 * Binary search in leaf node.
 * Returns 0 if found (and fills *rec), -ENOENT otherwise.
 */
static int search_leaf(struct oi_context *ctx, void *buf, const void *key, struct osd_inode_id *rec) {
	struct iam_leaf_head *head = buf;
	int count, entry_size;
	void *entries, *p, *q, *m;

	if (le16_to_cpu(head->ill_magic) != IAM_LEAF_HEADER_MAGIC) {
		fprintf(stderr, "Bad leaf magic: 0x%x\n", le16_to_cpu(head->ill_magic));
		return -EIO;
	}

	count = le16_to_cpu(head->ill_count);
	if (count == 0)
		return -ENOENT;

	entry_size = ctx->keysize + ctx->recsize;
	entries = (char *)buf + sizeof(struct iam_leaf_head);

	p = entries;
	q = (char *)entries + (count - 1) * entry_size;

	/* Binary search for exact match */
	while (p <= q) {
		m = (char *)p + (((char *)q - (char *)p) / entry_size / 2) * entry_size;
		int cmp = keycmp(key, m, ctx->keysize);
		if (cmp == 0) {
			/* Found! Extract record */
			void *rec_ptr = (char *)m + ctx->keysize;
			memcpy(rec, rec_ptr, sizeof(*rec));
			/* Convert from big-endian */
			rec->oii_ino = be32_to_cpu(rec->oii_ino);
			rec->oii_gen = be32_to_cpu(rec->oii_gen);
			return 0;
		} else if (cmp < 0) {
			q = (char *)m - entry_size;
		} else {
			p = (char *)m + entry_size;
		}
	}

	return -ENOENT;
}

/* ---------- Main lookup function ---------- */

int oi_lookup(struct oi_context *ctx, const struct lu_fid *fid, struct oi_result *result) {
	int oi_idx;
	ext2_ino_t oi_ino;
	const void *buf;
	struct iam_lfix_root *root;
	struct dx_countlimit *cl;
	uint8_t key[16]; /* FID in big-endian */
	int indirect_levels;
	uint32_t block;
	int level;
	void *entries;
	void *entry;
	struct osd_inode_id rec;
	int ret;
	int verbose = ctx->verbose;

	/* Select OI file based on FID sequence */
	oi_idx = fid->f_seq & (ctx->oi_count - 1);
	oi_ino = ctx->oi_inodes[oi_idx];

	if (oi_ino == 0) {
		fprintf(stderr, "OI file %d not available\n", oi_idx);
		return -ENOENT;
	}

	/* Convert FID to big-endian key */
	cpu_to_be_fid(fid, key);

	/* Read root block (block 0) */
	ret = read_oi_block_cached(ctx, oi_idx, 0, &buf);
	if (ret) {
		return ret;
	}

	/* Parse root header */
	root = (struct iam_lfix_root *)buf;
	if (le64_to_cpu(root->ilr_magic) != IAM_LFIX_ROOT_MAGIC) {
		fprintf(stderr, "Bad root magic: 0x%llx (expected 0x%llx)\n", (unsigned long long)le64_to_cpu(root->ilr_magic),
		        (unsigned long long)IAM_LFIX_ROOT_MAGIC);
		return -EIO;
	}

	/* Cache parameters (first call only in practice) */
	ctx->keysize = le16_to_cpu(root->ilr_keysize);
	ctx->recsize = le16_to_cpu(root->ilr_recsize);
	ctx->ptrsize = le16_to_cpu(root->ilr_ptrsize);
	indirect_levels = root->ilr_indirect_levels;

	if (verbose)
		printf("  OI params: keysize=%d recsize=%d ptrsize=%d levels=%d\n", ctx->keysize, ctx->recsize, ctx->ptrsize,
		       indirect_levels);

	/* Verify expected sizes for OI */
	if (ctx->keysize != 16 || ctx->recsize != 8 || ctx->ptrsize != 4) {
		fprintf(stderr, "Unexpected OI parameters\n");
		return -EIO;
	}

	/* Index entries start after root header (id_root_gap = sizeof(iam_lfix_root)) */
	entries = (char *)buf + sizeof(struct iam_lfix_root);
	cl = (struct dx_countlimit *)entries;

	if (verbose)
		printf("  Root: count=%d limit=%d\n", le16_to_cpu(cl->count), le16_to_cpu(cl->limit));

	/* Find entry in root index */
	entry = find_index_entry(ctx, entries, le16_to_cpu(cl->count), key);
	block = get_entry_ptr(ctx, entry);

	if (verbose) {
		printf("  Root search -> block %u (entry at offset %ld)\n", block, (char *)entry - (char *)buf);
		/* Dump entry bytes for debugging */
		printf("  Entry hex: ");
		for (int i = 0; i < ctx->keysize + ctx->ptrsize; i++)
			printf("%02x ", ((unsigned char *)entry)[i]);
		printf("\n");
	}

	/* Traverse intermediate index levels */
	for (level = 0; level < indirect_levels; level++) {
		ret = read_oi_block_cached(ctx, oi_idx, block, &buf);
		if (ret) {
			return ret;
		}

		/* Index node: entries start at offset 0 (id_node_gap = 0) */
		entries = (void *)buf;
		cl = (struct dx_countlimit *)entries;

		if (verbose)
			printf("  Level %d: count=%d, ", level, le16_to_cpu(cl->count));

		entry = find_index_entry(ctx, entries, le16_to_cpu(cl->count), key);
		block = get_entry_ptr(ctx, entry);

		if (verbose)
			printf("-> block %u\n", block);
	}

	/* Now 'block' points to leaf */
	ret = read_oi_block_cached(ctx, oi_idx, block, &buf);
	if (ret) {
		return ret;
	}

	ret = search_leaf(ctx, (void *)buf, key, &rec);

	if (ret == 0) {
		result->ino = rec.oii_ino;
		result->gen = rec.oii_gen;
		if (verbose)
			printf("  Found: ino=%u gen=%u\n", result->ino, result->gen);
	}

	return ret;
}

/* ---------- Public API ---------- */

void oi_set_verbose(struct oi_context *ctx, int verbose) {
	ctx->verbose = verbose;
}

int oi_get_count(struct oi_context *ctx) {
	return ctx->oi_count;
}

struct oi_context *oi_open(const char *device) {
	struct oi_context *ctx;
	errcode_t err;
	size_t block_bytes;
	int i;
	int ret;

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx)
		return NULL;

	err = ext2fs_open(device, 0, 0, 0, unix_io_manager, &ctx->fs);
	if (err) {
		fprintf(stderr, "Cannot open filesystem %s: %s\n", device, error_message(err));
		free(ctx);
		return NULL;
	}

	ret = oi_scan_files(ctx);
	if (ret) {
		ext2fs_close(ctx->fs);
		free(ctx);
		return NULL;
	}

	ctx->oi_files = calloc(ctx->oi_count, sizeof(ext2_file_t));
	ctx->cache_entries = calloc(OI_BLOCK_CACHE_ENTRIES, sizeof(struct oi_block_cache_entry));
	block_bytes = (size_t)ctx->fs->blocksize * OI_BLOCK_CACHE_ENTRIES;
	ctx->cache_storage = malloc(block_bytes);
	if (!ctx->oi_files || !ctx->cache_entries || !ctx->cache_storage) {
		if (ctx->oi_files)
			free(ctx->oi_files);
		if (ctx->cache_entries)
			free(ctx->cache_entries);
		if (ctx->cache_storage)
			free(ctx->cache_storage);
		if (ctx->oi_inodes)
			free(ctx->oi_inodes);
		ext2fs_close(ctx->fs);
		free(ctx);
		return NULL;
	}

	for (i = 0; i < OI_BLOCK_CACHE_ENTRIES; i++) {
		ctx->cache_entries[i].data = (char *)ctx->cache_storage + ((size_t)i * ctx->fs->blocksize);
	}

	return ctx;
}

void oi_close(struct oi_context *ctx) {
	int i;

	if (ctx) {
		if (ctx->oi_files) {
			for (i = 0; i < ctx->oi_count; i++) {
				if (ctx->oi_files[i])
					ext2fs_file_close(ctx->oi_files[i]);
			}
			free(ctx->oi_files);
		}
		if (ctx->cache_entries)
			free(ctx->cache_entries);
		if (ctx->cache_storage)
			free(ctx->cache_storage);
		if (ctx->oi_inodes)
			free(ctx->oi_inodes);
		if (ctx->fs)
			ext2fs_close(ctx->fs);
		free(ctx);
	}
}
