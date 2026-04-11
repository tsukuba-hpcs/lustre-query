/*
 * oi_lookup.h - Lustre OI lookup library interface
 */

#ifndef OI_LOOKUP_H
#define OI_LOOKUP_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Lustre FID structure */
struct lu_fid {
	uint64_t f_seq; /* Sequence number */
	uint32_t f_oid; /* Object ID */
	uint32_t f_ver; /* Version */
};

/* OI lookup result */
struct oi_result {
	uint32_t ino; /* Inode number */
	uint32_t gen; /* Generation number */
};

/* Opaque context */
struct oi_context;

/*
 * Open an OI context for the given filesystem.
 *
 * @device: Path to block device or image file containing ldiskfs/ext4 filesystem
 * @returns: OI context on success, NULL on failure
 *
 * Note: The filesystem should NOT be mounted, or mounted read-only.
 */
struct oi_context *oi_open(const char *device);

/*
 * Close OI context and release resources.
 */
void oi_close(struct oi_context *ctx);

/*
 * Lookup inode number for a given FID.
 *
 * @ctx: OI context from oi_open()
 * @fid: FID to lookup
 * @result: Output - inode number and generation
 * @returns: 0 on success, -ENOENT if not found, other negative errno on error
 */
int oi_lookup(struct oi_context *ctx, const struct lu_fid *fid, struct oi_result *result);

/*
 * Get the number of OI files.
 */
int oi_get_count(struct oi_context *ctx);

/*
 * Enable/disable verbose output.
 */
void oi_set_verbose(struct oi_context *ctx, int verbose);

#ifdef __cplusplus
}
#endif

#endif /* OI_LOOKUP_H */