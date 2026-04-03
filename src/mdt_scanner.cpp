//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// mdt_scanner.cpp
//
// MDT scanner implementation using libext2fs
//===----------------------------------------------------------------------===//

#include "mdt_scanner.hpp"

#include <cstring>
#include <sys/types.h>

namespace duckdb {
namespace lustre {

//===----------------------------------------------------------------------===//
// Endian conversion helpers
//===----------------------------------------------------------------------===//

static inline uint64_t ReadBE64(const uint8_t *p) {
	return (static_cast<uint64_t>(p[0]) << 56) | (static_cast<uint64_t>(p[1]) << 48) |
	       (static_cast<uint64_t>(p[2]) << 40) | (static_cast<uint64_t>(p[3]) << 32) |
	       (static_cast<uint64_t>(p[4]) << 24) | (static_cast<uint64_t>(p[5]) << 16) |
	       (static_cast<uint64_t>(p[6]) << 8)  | static_cast<uint64_t>(p[7]);
}

static inline uint32_t ReadBE32(const uint8_t *p) {
	return (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
	       (static_cast<uint32_t>(p[2]) << 8)  | static_cast<uint32_t>(p[3]);
}

static inline uint16_t ReadBE16(const uint8_t *p) {
	return (static_cast<uint16_t>(p[0]) << 8) | static_cast<uint16_t>(p[1]);
}

static void PopulateInodeMetadata(LustreInode &out, ext2_ino_t ino, const struct ext2_inode_large &inode) {
	out = LustreInode();
	out.ino = ino;
	out.mode = inode.i_mode;
	out.nlink = inode.i_links_count;
	out.uid = inode.i_uid | ((uint32_t)inode.osd2.linux2.l_i_uid_high << 16);
	out.gid = inode.i_gid | ((uint32_t)inode.osd2.linux2.l_i_gid_high << 16);
	out.size = inode.i_size;
	if (ModeToFileType(out.mode) != FileType::DIRECTORY) {
		out.size |= ((uint64_t)inode.i_size_high << 32);
	}
	out.blocks = inode.i_blocks;
	out.atime = inode.i_atime;
	out.mtime = inode.i_mtime;
	out.ctime = inode.i_ctime;
	out.type = ModeToFileType(out.mode);
	out.flags = inode.i_flags;
	out.projid = inode.i_projid;
}

//===----------------------------------------------------------------------===//
// Constructor / Destructor
//===----------------------------------------------------------------------===//

MDTScanner::MDTScanner()
    : fs_(nullptr), scan_(nullptr), oi_ctx_(nullptr), next_block_group_(0),
      scanned_inodes_(0), valid_inodes_(0) {
}

MDTScanner::~MDTScanner() {
	Close();
}

//===----------------------------------------------------------------------===//
// Open / Close
//===----------------------------------------------------------------------===//

void MDTScanner::Open(const std::string &device_path) {
	if (fs_) {
		Close();
	}

	device_path_ = device_path;

	// Open the filesystem in read-only mode
	errcode_t err = ext2fs_open(
	    device_path.c_str(),
	    EXT2_FLAG_64BITS | EXT2_FLAG_SOFTSUPP_FEATURES,  // flags
	    0,              // superblock (0 = default)
	    0,              // block_size (0 = auto-detect)
	    unix_io_manager,
	    &fs_
	);

	if (err) {
		throw IOException("Failed to open filesystem '%s': %s", device_path, error_message(err));
	}

	// Reset counters
	next_block_group_.store(0);
	scanned_inodes_.store(0);
	valid_inodes_.store(0);

	// Reset link iteration state
	pending_inode_ = LustreInode();
	has_pending_links_ = false;
	pending_link_idx_ = 0;
	pending_links_.clear();
	has_pending_objects_ = false;
	pending_object_idx_ = 0;
	pending_objects_.clear();
	has_pending_layouts_ = false;
	pending_layout_idx_ = 0;
	pending_layouts_.clear();
	skip_current_group_ = false;
}

void MDTScanner::Close() {
	CloseScan();
	CloseOI();

	if (fs_) {
		ext2fs_close(fs_);
		fs_ = nullptr;
	}
}

//===----------------------------------------------------------------------===//
// Filesystem Info
//===----------------------------------------------------------------------===//

uint64_t MDTScanner::GetTotalInodes() const {
	if (!fs_ || !fs_->super) return 0;
	return fs_->super->s_inodes_count;
}

uint64_t MDTScanner::GetUsedInodes() const {
	if (!fs_ || !fs_->super) return 0;
	return fs_->super->s_inodes_count - fs_->super->s_free_inodes_count;
}

uint32_t MDTScanner::GetBlockGroupCount() const {
	if (!fs_) return 0;
	return fs_->group_desc_count;
}

uint32_t MDTScanner::GetInodesPerGroup() const {
	if (!fs_ || !fs_->super) return 0;
	return fs_->super->s_inodes_per_group;
}

uint32_t MDTScanner::GetBlockSize() const {
	if (!fs_) return 0;
	return fs_->blocksize;
}

uint32_t MDTScanner::GetInodeSize() const {
	if (!fs_ || !fs_->super) return 0;
	return EXT2_INODE_SIZE(fs_->super);
}

//===----------------------------------------------------------------------===//
// Sequential Scan
//===----------------------------------------------------------------------===//

void MDTScanner::StartScan(int buffer_blocks) {
	if (!fs_) {
		throw IOException("Filesystem not open for '%s'", device_path_);
	}

	CloseScan();

	errcode_t err = ext2fs_open_inode_scan(fs_, buffer_blocks, &scan_);
	if (err) {
		throw IOException("Failed to open inode scan for '%s': %s", device_path_, error_message(err));
	}
}

void MDTScanner::CloseScan() {
	if (scan_) {
		ext2fs_close_inode_scan(scan_);
		scan_ = nullptr;
	}
	pending_inode_ = LustreInode();
	has_pending_links_ = false;
	pending_link_idx_ = 0;
	pending_links_.clear();
	has_pending_objects_ = false;
	pending_object_idx_ = 0;
	pending_objects_.clear();
	has_pending_layouts_ = false;
	pending_layout_idx_ = 0;
	pending_layouts_.clear();
	skip_current_group_ = false;
}

//===----------------------------------------------------------------------===//
// Block Group Based Scan
//===----------------------------------------------------------------------===//

void MDTScanner::GotoBlockGroup(int group) {
	if (!scan_) {
		throw IOException("Inode scan not started for '%s'", device_path_);
	}

	if (group < 0 || group >= (int)GetBlockGroupCount()) {
		throw IOException("Invalid block group %d for '%s'", group, device_path_);
	}

	if (!BlockGroupHasAllocatedInodes(group)) {
		skip_current_group_ = true;
		return;
	}

	skip_current_group_ = false;
	errcode_t err = ext2fs_inode_scan_goto_blockgroup(scan_, group);
	if (err) {
		throw IOException("Failed to goto block group %d for '%s': %s",
		                  group, device_path_, error_message(err));
	}
}

int MDTScanner::GetNextBlockGroup() {
	while (true) {
		int group = next_block_group_.fetch_add(1);
		if (group >= (int)GetBlockGroupCount()) {
			return -1;  // No more block groups
		}
		if (BlockGroupHasAllocatedInodes(group)) {
			return group;
		}
	}
}

void MDTScanner::ResetBlockGroupCounter() {
	next_block_group_.store(0);
}

//===----------------------------------------------------------------------===//
// Internal Helpers
//===----------------------------------------------------------------------===//

bool MDTScanner::GetNextRawInode(ext2_ino_t &ino, struct ext2_inode_large &raw) {
	if (!scan_) {
		throw IOException("Inode scan not started for '%s'", device_path_);
	}

	while (true) {
		errcode_t err = ext2fs_get_next_inode_full(scan_, &ino,
			(struct ext2_inode *)&raw, sizeof(raw));
		if (err) {
			throw IOException("Failed to get next inode for '%s': %s", device_path_, error_message(err));
		}

		// ino == 0 means scan is complete
		if (ino == 0) {
			return false;
		}

		scanned_inodes_++;

		// Skip unused inodes (all zeros)
		if (raw.i_mode == 0 && raw.i_links_count == 0) {
			continue;
		}

		// Skip deleted inodes
		if (raw.i_dtime != 0) {
			continue;
		}

		return true;
	}
}

bool MDTScanner::GetNextRawInode(ext2_ino_t &ino, struct ext2_inode_large &raw, ext2_ino_t max_ino) {
	if (!scan_) {
		throw IOException("Inode scan not started for '%s'", device_path_);
	}

	if (skip_current_group_) {
		skip_current_group_ = false;
		return false;
	}

	while (true) {
		errcode_t err = ext2fs_get_next_inode_full(scan_, &ino,
			(struct ext2_inode *)&raw, sizeof(raw));
		if (err) {
			throw IOException("Failed to get next inode for '%s': %s", device_path_, error_message(err));
		}

		// ino == 0 means scan is complete
		if (ino == 0) {
			return false;
		}

		// Past our block group boundary
		if (ino > max_ino) {
			return false;
		}

		scanned_inodes_++;

		// Skip unused inodes (all zeros)
		if (raw.i_mode == 0 && raw.i_links_count == 0) {
			continue;
		}

		// Skip deleted inodes
		if (raw.i_dtime != 0) {
			continue;
		}

		return true;
	}
}

bool MDTScanner::BlockGroupHasAllocatedInodes(int group) const {
	if (!fs_ || group < 0 || group >= static_cast<int>(GetBlockGroupCount())) {
		return false;
	}

	auto inodes_per_group = static_cast<uint64_t>(GetInodesPerGroup());
	auto total_inodes = static_cast<uint64_t>(GetTotalInodes());
	auto first_ino = static_cast<uint64_t>(group) * inodes_per_group + 1;
	if (first_ino > total_inodes) {
		return false;
	}

	auto inodes_in_group = total_inodes - first_ino + 1;
	if (inodes_in_group > inodes_per_group) {
		inodes_in_group = inodes_per_group;
	}
	auto free_inodes = static_cast<uint64_t>(ext2fs_bg_free_inodes_count(fs_, static_cast<dgrp_t>(group)));
	return free_inodes < inodes_in_group;
}

bool MDTScanner::OpenAndReadXattrs(ext2_ino_t ino, struct ext2_xattr_handle *&h) {
	h = nullptr;
	errcode_t err = ext2fs_xattrs_open(fs_, ino, &h);
	if (err || !h) {
		return false;
	}

	err = ext2fs_xattrs_read(h);
	if (err) {
		ext2fs_xattrs_close(&h);
		h = nullptr;
		return false;
	}

	return true;
}

//===----------------------------------------------------------------------===//
// GetNextInode - full metadata scan (for lustre_inodes)
//===----------------------------------------------------------------------===//

bool MDTScanner::GetNextInode(LustreInode &out, const MDTScanConfig &config) {
	ext2_ino_t ino;
	struct ext2_inode_large inode;

	while (true) {
		if (!GetNextRawInode(ino, inode)) {
			return false;
		}

		// Convert all inode metadata
		out = LustreInode();
		out.ino = ino;
		out.mode = inode.i_mode;
		out.nlink = inode.i_links_count;

		// UID/GID with high bits (32-bit support)
		out.uid = inode.i_uid | ((uint32_t)inode.osd2.linux2.l_i_uid_high << 16);
		out.gid = inode.i_gid | ((uint32_t)inode.osd2.linux2.l_i_gid_high << 16);

		// Size (64-bit) - initial value from inode
		out.size = inode.i_size;
		if (ModeToFileType(out.mode) != FileType::DIRECTORY) {
			out.size |= ((uint64_t)inode.i_size_high << 32);
		}
		out.blocks = inode.i_blocks;

		// Timestamps
		out.atime = inode.i_atime;
		out.mtime = inode.i_mtime;
		out.ctime = inode.i_ctime;

		// File type
		out.type = ModeToFileType(out.mode);

		// Flags
		out.flags = inode.i_flags;

		// Projid
		out.projid = inode.i_projid;

		// Read Lustre xattrs using ext2fs_xattrs_read_inode to avoid double inode read
		struct ext2_xattr_handle *h = nullptr;
		if (!OpenAndReadXattrs(ino, h)) {
			// No xattrs — apply skip checks and continue
			if (config.skip_no_fid || config.skip_no_linkea) {
				continue;
			}
			valid_inodes_++;
			return true;
		}

		// Parse FID
		ParseFID(h, out.fid);

		// Lightweight LinkEA check (no full parse)
		bool has_linkea = HasLinkEA(h);

		// Parse SOM (size on MDS) — overwrites size/blocks if valid
		uint64_t som_size, som_blocks;
		if (ParseSOM(h, som_size, som_blocks)) {
			out.size = som_size;
			out.blocks = som_blocks;
		}

		ext2fs_xattrs_close(&h);

		// Apply config-based skip checks
		if (config.skip_no_fid && !out.fid.IsValid()) {
			continue;
		}
		if (config.skip_no_linkea && !has_linkea) {
			continue;
		}

		valid_inodes_++;
		return true;
	}
}

bool MDTScanner::GetNextInode(LustreInode &out, const MDTScanConfig &config, ext2_ino_t max_ino) {
	ext2_ino_t ino;
	struct ext2_inode_large inode;

	while (true) {
		if (!GetNextRawInode(ino, inode, max_ino)) {
			return false;
		}

		// Convert all inode metadata
		out = LustreInode();
		out.ino = ino;
		out.mode = inode.i_mode;
		out.nlink = inode.i_links_count;

		// UID/GID with high bits (32-bit support)
		out.uid = inode.i_uid | ((uint32_t)inode.osd2.linux2.l_i_uid_high << 16);
		out.gid = inode.i_gid | ((uint32_t)inode.osd2.linux2.l_i_gid_high << 16);

		// Size (64-bit) - initial value from inode
		out.size = inode.i_size;
		if (ModeToFileType(out.mode) != FileType::DIRECTORY) {
			out.size |= ((uint64_t)inode.i_size_high << 32);
		}
		out.blocks = inode.i_blocks;

		// Timestamps
		out.atime = inode.i_atime;
		out.mtime = inode.i_mtime;
		out.ctime = inode.i_ctime;

		// File type
		out.type = ModeToFileType(out.mode);

		// Flags
		out.flags = inode.i_flags;

		// Projid
		out.projid = inode.i_projid;

		// Read Lustre xattrs
		struct ext2_xattr_handle *h = nullptr;
		if (!OpenAndReadXattrs(ino, h)) {
			if (config.skip_no_fid || config.skip_no_linkea) {
				continue;
			}
			valid_inodes_++;
			return true;
		}

		// Parse FID
		ParseFID(h, out.fid);

		// Lightweight LinkEA check (no full parse)
		bool has_linkea = HasLinkEA(h);

		// Parse SOM (size on MDS) — overwrites size/blocks if valid
		uint64_t som_size, som_blocks;
		if (ParseSOM(h, som_size, som_blocks)) {
			out.size = som_size;
			out.blocks = som_blocks;
		}

		ext2fs_xattrs_close(&h);

		// Apply config-based skip checks
		if (config.skip_no_fid && !out.fid.IsValid()) {
			continue;
		}
		if (config.skip_no_linkea && !has_linkea) {
			continue;
		}

		valid_inodes_++;
		return true;
	}
}

//===----------------------------------------------------------------------===//
// GetNextLink - link scan (for lustre_links)
//===----------------------------------------------------------------------===//

bool MDTScanner::GetNextLink(LustreLink &link, const MDTScanConfig &config) {
	// If we have pending links from a previous inode, emit the next one
	if (has_pending_links_) {
		auto &entry = pending_links_[pending_link_idx_];
		link.fid = pending_fid_;
		link.parent_fid = entry.parent_fid;
		link.name = entry.name;
		pending_link_idx_++;

		if (pending_link_idx_ >= pending_links_.size()) {
			has_pending_links_ = false;
		}
		return true;
	}

	// Scan for the next inode with LinkEA
	ext2_ino_t ino;
	struct ext2_inode_large inode;

	while (true) {
		if (!GetNextRawInode(ino, inode)) {
			return false;
		}

		// Read Lustre xattrs
		struct ext2_xattr_handle *h = nullptr;
		if (!OpenAndReadXattrs(ino, h)) {
			if (config.skip_no_fid || config.skip_no_linkea) {
				continue;
			}
			valid_inodes_++;
			continue;
		}

		// Parse FID
		LustreFID fid;
		ParseFID(h, fid);

		// Full LinkEA parse (need actual link entries)
		std::vector<LinkEntry> links;
		bool has_linkea = ParseLinkEA(h, links);

		ext2fs_xattrs_close(&h);

		// Apply config-based skip checks
		if (config.skip_no_fid && !fid.IsValid()) {
			continue;
		}
		if (config.skip_no_linkea && !has_linkea) {
			continue;
		}

		valid_inodes_++;

		if (links.empty()) {
			continue;
		}

		// Emit the first link
		link.fid = fid;
		link.parent_fid = links[0].parent_fid;
		link.name = links[0].name;

		// If there are more links, save pending state
		if (links.size() > 1) {
			pending_fid_ = fid;
			pending_links_ = std::move(links);
			pending_link_idx_ = 1;
			has_pending_links_ = true;
		}

		return true;
	}
}

bool MDTScanner::GetNextLink(LustreLink &link, const MDTScanConfig &config, ext2_ino_t max_ino) {
	// If we have pending links from a previous inode, emit the next one
	if (has_pending_links_) {
		auto &entry = pending_links_[pending_link_idx_];
		link.fid = pending_fid_;
		link.parent_fid = entry.parent_fid;
		link.name = entry.name;
		pending_link_idx_++;

		if (pending_link_idx_ >= pending_links_.size()) {
			has_pending_links_ = false;
		}
		return true;
	}

	// Scan for the next inode with LinkEA
	ext2_ino_t ino;
	struct ext2_inode_large inode;

	while (true) {
		if (!GetNextRawInode(ino, inode, max_ino)) {
			return false;
		}

		// Read Lustre xattrs
		struct ext2_xattr_handle *h = nullptr;
		if (!OpenAndReadXattrs(ino, h)) {
			if (config.skip_no_fid || config.skip_no_linkea) {
				continue;
			}
			valid_inodes_++;
			continue;
		}

		// Parse FID
		LustreFID fid;
		ParseFID(h, fid);

		// Full LinkEA parse (need actual link entries)
		std::vector<LinkEntry> links;
		bool has_linkea = ParseLinkEA(h, links);

		ext2fs_xattrs_close(&h);

		// Apply config-based skip checks
		if (config.skip_no_fid && !fid.IsValid()) {
			continue;
		}
		if (config.skip_no_linkea && !has_linkea) {
			continue;
		}

		valid_inodes_++;

		if (links.empty()) {
			continue;
		}

		// Emit the first link
		link.fid = fid;
		link.parent_fid = links[0].parent_fid;
		link.name = links[0].name;

		// If there are more links, save pending state
		if (links.size() > 1) {
			pending_fid_ = fid;
			pending_links_ = std::move(links);
			pending_link_idx_ = 1;
			has_pending_links_ = true;
		}

		return true;
	}
}

//===----------------------------------------------------------------------===//
// OI (Object Index) Targeted Lookup
//===----------------------------------------------------------------------===//

void MDTScanner::InitOI() {
	if (oi_ctx_) {
		return;  // Already initialized
	}
	oi_ctx_ = oi_open(device_path_.c_str());
	if (!oi_ctx_) {
		throw IOException("Failed to open OI context for '%s'", device_path_);
	}
}

void MDTScanner::CloseOI() {
	if (oi_ctx_) {
		oi_close(oi_ctx_);
		oi_ctx_ = nullptr;
	}
}

bool MDTScanner::LookupFID(const LustreFID &fid, ext2_ino_t &ino_out) {
	if (!oi_ctx_) {
		throw IOException("OI context not initialized for '%s'", device_path_);
	}

	struct lu_fid c_fid;
	c_fid.f_seq = fid.f_seq;
	c_fid.f_oid = fid.f_oid;
	c_fid.f_ver = fid.f_ver;

	struct oi_result result;
	int ret = oi_lookup(oi_ctx_, &c_fid, &result);
	if (ret != 0) {
		return false;
	}

	ino_out = result.ino;
	return true;
}

bool MDTScanner::ReadInode(ext2_ino_t ino, LustreInode &out, const MDTScanConfig &config) {
	if (!fs_) {
		throw IOException("Filesystem not open for '%s'", device_path_);
	}

	// Read the raw inode
	struct ext2_inode_large inode;
	memset(&inode, 0, sizeof(inode));
	errcode_t err = ext2fs_read_inode_full(fs_, ino, (struct ext2_inode *)&inode, sizeof(inode));
	if (err) {
		return false;
	}

	// Skip unused/deleted inodes
	if (inode.i_mode == 0 && inode.i_links_count == 0) {
		return false;
	}
	if (inode.i_dtime != 0) {
		return false;
	}

	// Convert to LustreInode (same logic as GetNextInode)
	out = LustreInode();
	out.ino = ino;
	out.mode = inode.i_mode;
	out.nlink = inode.i_links_count;

	// UID/GID with high bits (32-bit support)
	out.uid = inode.i_uid | ((uint32_t)inode.osd2.linux2.l_i_uid_high << 16);
	out.gid = inode.i_gid | ((uint32_t)inode.osd2.linux2.l_i_gid_high << 16);

	// Size (64-bit)
	out.size = inode.i_size;
	if (ModeToFileType(out.mode) != FileType::DIRECTORY) {
		out.size |= ((uint64_t)inode.i_size_high << 32);
	}
	out.blocks = inode.i_blocks;

	// Timestamps
	out.atime = inode.i_atime;
	out.mtime = inode.i_mtime;
	out.ctime = inode.i_ctime;

	// File type
	out.type = ModeToFileType(out.mode);

	// Flags
	out.flags = inode.i_flags;

	// Projid
	out.projid = inode.i_projid;

	// Read Lustre xattrs
	struct ext2_xattr_handle *h = nullptr;
	if (!OpenAndReadXattrs(ino, h)) {
		if (config.skip_no_fid || config.skip_no_linkea) {
			return false;
		}
		return true;
	}

	// Parse FID
	ParseFID(h, out.fid);

	// Lightweight LinkEA check
	bool has_linkea = HasLinkEA(h);

	// Parse SOM (size on MDS)
	uint64_t som_size, som_blocks;
	if (ParseSOM(h, som_size, som_blocks)) {
		out.size = som_size;
		out.blocks = som_blocks;
	}

	ext2fs_xattrs_close(&h);

	// Apply config-based skip checks
	if (config.skip_no_fid && !out.fid.IsValid()) {
		return false;
	}
	if (config.skip_no_linkea && !has_linkea) {
		return false;
	}

	return true;
}

bool MDTScanner::ReadInodeLinks(ext2_ino_t ino, LustreInode &inode_out, std::vector<LinkEntry> &links_out,
                                const MDTScanConfig &config) {
	if (!fs_) {
		throw IOException("Filesystem not open for '%s'", device_path_);
	}

	links_out.clear();

	struct ext2_inode_large inode;
	memset(&inode, 0, sizeof(inode));
	errcode_t err = ext2fs_read_inode_full(fs_, ino, (struct ext2_inode *)&inode, sizeof(inode));
	if (err) {
		return false;
	}

	if (inode.i_mode == 0 && inode.i_links_count == 0) {
		return false;
	}
	if (inode.i_dtime != 0) {
		return false;
	}

	inode_out = LustreInode();
	inode_out.ino = ino;
	inode_out.mode = inode.i_mode;
	inode_out.nlink = inode.i_links_count;
	inode_out.uid = inode.i_uid | ((uint32_t)inode.osd2.linux2.l_i_uid_high << 16);
	inode_out.gid = inode.i_gid | ((uint32_t)inode.osd2.linux2.l_i_gid_high << 16);
	inode_out.size = inode.i_size;
	if (ModeToFileType(inode_out.mode) != FileType::DIRECTORY) {
		inode_out.size |= ((uint64_t)inode.i_size_high << 32);
	}
	inode_out.blocks = inode.i_blocks;
	inode_out.atime = inode.i_atime;
	inode_out.mtime = inode.i_mtime;
	inode_out.ctime = inode.i_ctime;
	inode_out.type = ModeToFileType(inode_out.mode);
	inode_out.flags = inode.i_flags;
	inode_out.projid = inode.i_projid;

	struct ext2_xattr_handle *h = nullptr;
	if (!OpenAndReadXattrs(ino, h)) {
		return false;
	}

	ParseFID(h, inode_out.fid);
	ParseLinkEA(h, links_out);

	uint64_t som_size, som_blocks;
	if (ParseSOM(h, som_size, som_blocks)) {
		inode_out.size = som_size;
		inode_out.blocks = som_blocks;
	}

	ext2fs_xattrs_close(&h);

	if (config.skip_no_fid && !inode_out.fid.IsValid()) {
		return false;
	}
	if (config.skip_no_linkea && links_out.empty()) {
		return false;
	}

	return true;
}

bool MDTScanner::ReadInodeLinkLayouts(ext2_ino_t ino, LustreFID &fid_out, std::vector<LinkEntry> &links_out,
                                      std::vector<LustreLayoutComponent> &components_out,
                                      const MDTScanConfig &config) {
	links_out.clear();
	components_out.clear();

	struct ext2_xattr_handle *h = nullptr;
	if (!OpenAndReadXattrs(ino, h)) {
		return false;
	}

	fid_out = LustreFID();
	ParseFID(h, fid_out);
	ParseLinkEA(h, links_out);
	ParseLOVDetailed(h, &components_out, nullptr);

	ext2fs_xattrs_close(&h);

	if (config.skip_no_fid && !fid_out.IsValid()) {
		return false;
	}
	if (config.skip_no_linkea && links_out.empty()) {
		return false;
	}
	if (components_out.empty()) {
		return false;
	}

	return true;
}

bool MDTScanner::ReadInodeLinkObjects(ext2_ino_t ino, LustreFID &fid_out, std::vector<LinkEntry> &links_out,
                                      std::vector<LustreOSTObject> &objects_out, const MDTScanConfig &config) {
	links_out.clear();
	objects_out.clear();

	struct ext2_xattr_handle *h = nullptr;
	if (!OpenAndReadXattrs(ino, h)) {
		return false;
	}

	fid_out = LustreFID();
	ParseFID(h, fid_out);
	ParseLinkEA(h, links_out);
	ParseLOVDetailed(h, nullptr, &objects_out);

	ext2fs_xattrs_close(&h);

	if (config.skip_no_fid && !fid_out.IsValid()) {
		return false;
	}
	if (config.skip_no_linkea && links_out.empty()) {
		return false;
	}
	if (objects_out.empty()) {
		return false;
	}

	return true;
}

bool MDTScanner::GetNextInodeLink(LustreInodeLinkRow &row, const MDTScanConfig &config) {
	// If we have pending links from a previous inode, emit the next joined row.
	if (has_pending_links_) {
		auto &entry = pending_links_[pending_link_idx_];
		row.inode = pending_inode_;
		row.link_fid = pending_inode_.fid;
		row.parent_fid = entry.parent_fid;
		row.name = entry.name;
		pending_link_idx_++;

		if (pending_link_idx_ >= pending_links_.size()) {
			has_pending_links_ = false;
		}
		return true;
	}

	ext2_ino_t ino;
	struct ext2_inode_large inode;

	while (true) {
		if (!GetNextRawInode(ino, inode)) {
			return false;
		}

		LustreInode inode_row;
		inode_row.ino = ino;
		inode_row.mode = inode.i_mode;
		inode_row.nlink = inode.i_links_count;
		inode_row.uid = inode.i_uid | ((uint32_t)inode.osd2.linux2.l_i_uid_high << 16);
		inode_row.gid = inode.i_gid | ((uint32_t)inode.osd2.linux2.l_i_gid_high << 16);
		inode_row.size = inode.i_size;
		if (ModeToFileType(inode_row.mode) != FileType::DIRECTORY) {
			inode_row.size |= ((uint64_t)inode.i_size_high << 32);
		}
		inode_row.blocks = inode.i_blocks;
		inode_row.atime = inode.i_atime;
		inode_row.mtime = inode.i_mtime;
		inode_row.ctime = inode.i_ctime;
		inode_row.type = ModeToFileType(inode_row.mode);
		inode_row.flags = inode.i_flags;
		inode_row.projid = inode.i_projid;

		struct ext2_xattr_handle *h = nullptr;
		if (!OpenAndReadXattrs(ino, h)) {
			continue;
		}

		ParseFID(h, inode_row.fid);

		std::vector<LinkEntry> links;
		ParseLinkEA(h, links);

		uint64_t som_size, som_blocks;
		if (ParseSOM(h, som_size, som_blocks)) {
			inode_row.size = som_size;
			inode_row.blocks = som_blocks;
		}

		ext2fs_xattrs_close(&h);

		// Inner join on fid only matches rows with a valid FID and at least one link entry.
		if (!inode_row.fid.IsValid() || links.empty()) {
			continue;
		}
		if (config.skip_no_fid && !inode_row.fid.IsValid()) {
			continue;
		}
		if (config.skip_no_linkea && links.empty()) {
			continue;
		}

		valid_inodes_++;

		row.inode = inode_row;
		row.link_fid = inode_row.fid;
		row.parent_fid = links[0].parent_fid;
		row.name = links[0].name;

		if (links.size() > 1) {
			pending_inode_ = inode_row;
			pending_fid_ = inode_row.fid;
			pending_links_ = std::move(links);
			pending_link_idx_ = 1;
			has_pending_links_ = true;
		}
		return true;
	}
}

bool MDTScanner::GetNextInodeLink(LustreInodeLinkRow &row, const MDTScanConfig &config, ext2_ino_t max_ino) {
	// If we have pending links from a previous inode, emit the next joined row.
	if (has_pending_links_) {
		auto &entry = pending_links_[pending_link_idx_];
		row.inode = pending_inode_;
		row.link_fid = pending_inode_.fid;
		row.parent_fid = entry.parent_fid;
		row.name = entry.name;
		pending_link_idx_++;

		if (pending_link_idx_ >= pending_links_.size()) {
			has_pending_links_ = false;
		}
		return true;
	}

	ext2_ino_t ino;
	struct ext2_inode_large inode;

	while (true) {
		if (!GetNextRawInode(ino, inode, max_ino)) {
			return false;
		}

		LustreInode inode_row;
		inode_row.ino = ino;
		inode_row.mode = inode.i_mode;
		inode_row.nlink = inode.i_links_count;
		inode_row.uid = inode.i_uid | ((uint32_t)inode.osd2.linux2.l_i_uid_high << 16);
		inode_row.gid = inode.i_gid | ((uint32_t)inode.osd2.linux2.l_i_gid_high << 16);
		inode_row.size = inode.i_size;
		if (ModeToFileType(inode_row.mode) != FileType::DIRECTORY) {
			inode_row.size |= ((uint64_t)inode.i_size_high << 32);
		}
		inode_row.blocks = inode.i_blocks;
		inode_row.atime = inode.i_atime;
		inode_row.mtime = inode.i_mtime;
		inode_row.ctime = inode.i_ctime;
		inode_row.type = ModeToFileType(inode_row.mode);
		inode_row.flags = inode.i_flags;
		inode_row.projid = inode.i_projid;

		struct ext2_xattr_handle *h = nullptr;
		if (!OpenAndReadXattrs(ino, h)) {
			continue;
		}

		ParseFID(h, inode_row.fid);

		std::vector<LinkEntry> links;
		ParseLinkEA(h, links);

		uint64_t som_size, som_blocks;
		if (ParseSOM(h, som_size, som_blocks)) {
			inode_row.size = som_size;
			inode_row.blocks = som_blocks;
		}

		ext2fs_xattrs_close(&h);

		// Inner join on fid only matches rows with a valid FID and at least one link entry.
		if (!inode_row.fid.IsValid() || links.empty()) {
			continue;
		}
		if (config.skip_no_fid && !inode_row.fid.IsValid()) {
			continue;
		}
		if (config.skip_no_linkea && links.empty()) {
			continue;
		}

		valid_inodes_++;

		row.inode = inode_row;
		row.link_fid = inode_row.fid;
		row.parent_fid = links[0].parent_fid;
		row.name = links[0].name;

		if (links.size() > 1) {
			pending_inode_ = inode_row;
			pending_fid_ = inode_row.fid;
			pending_links_ = std::move(links);
			pending_link_idx_ = 1;
			has_pending_links_ = true;
		}
		return true;
	}
}

bool MDTScanner::ReadInodeLinkEA(ext2_ino_t ino, LustreFID &fid_out, std::vector<LinkEntry> &links_out) {
	links_out.clear();

	struct ext2_xattr_handle *h = nullptr;
	if (!OpenAndReadXattrs(ino, h)) {
		return false;
	}

	bool has_fid = ParseFID(h, fid_out);
	ParseLinkEA(h, links_out);

	ext2fs_xattrs_close(&h);
	return has_fid;
}

bool MDTScanner::ReadInodeLayouts(ext2_ino_t ino, LustreFID &fid_out,
                                  std::vector<LustreLayoutComponent> &components) {
	components.clear();

	struct ext2_xattr_handle *h = nullptr;
	if (!OpenAndReadXattrs(ino, h)) {
		return false;
	}

	bool has_fid = ParseFID(h, fid_out);
	ParseLOVDetailed(h, &components, nullptr);

	ext2fs_xattrs_close(&h);
	return has_fid;
}

bool MDTScanner::ReadInodeLayouts(ext2_ino_t ino, LustreInode &inode_out,
                                  std::vector<LustreLayoutComponent> &components, const MDTScanConfig &config) {
	if (!fs_) {
		throw IOException("Filesystem not open for '%s'", device_path_);
	}

	components.clear();

	struct ext2_inode_large inode;
	memset(&inode, 0, sizeof(inode));
	errcode_t err = ext2fs_read_inode_full(fs_, ino, (struct ext2_inode *)&inode, sizeof(inode));
	if (err) {
		return false;
	}
	if (inode.i_mode == 0 && inode.i_links_count == 0) {
		return false;
	}
	if (inode.i_dtime != 0) {
		return false;
	}

	PopulateInodeMetadata(inode_out, ino, inode);

	struct ext2_xattr_handle *h = nullptr;
	if (!OpenAndReadXattrs(ino, h)) {
		return false;
	}

	ParseFID(h, inode_out.fid);
	bool has_linkea = HasLinkEA(h);
	ParseLOVDetailed(h, &components, nullptr);

	uint64_t som_size, som_blocks;
	if (ParseSOM(h, som_size, som_blocks)) {
		inode_out.size = som_size;
		inode_out.blocks = som_blocks;
	}

	ext2fs_xattrs_close(&h);

	if (config.skip_no_fid && !inode_out.fid.IsValid()) {
		return false;
	}
	if (config.skip_no_linkea && !has_linkea) {
		return false;
	}
	if (components.empty()) {
		return false;
	}
	return true;
}

bool MDTScanner::ReadInodeLayoutObjects(ext2_ino_t ino, LustreFID &fid_out,
                                        std::vector<LustreLayoutComponent> &components,
                                        std::vector<LustreOSTObject> &objects, const MDTScanConfig &config) {
	components.clear();
	objects.clear();

	struct ext2_xattr_handle *h = nullptr;
	if (!OpenAndReadXattrs(ino, h)) {
		return false;
	}

	fid_out = LustreFID();
	ParseFID(h, fid_out);
	bool has_linkea = HasLinkEA(h);
	ParseLOVDetailed(h, &components, &objects);
	ext2fs_xattrs_close(&h);

	if (config.skip_no_fid && !fid_out.IsValid()) {
		return false;
	}
	if (config.skip_no_linkea && !has_linkea) {
		return false;
	}
	if (components.empty() || objects.empty()) {
		return false;
	}
	return true;
}

bool MDTScanner::GetNextInodeLayouts(LustreFID &fid_out, std::vector<LustreLayoutComponent> &components,
                                     const MDTScanConfig &config) {
	ext2_ino_t ino;
	struct ext2_inode_large inode;

	while (true) {
		if (!GetNextRawInode(ino, inode)) {
			return false;
		}

		struct ext2_xattr_handle *h = nullptr;
		if (!OpenAndReadXattrs(ino, h)) {
			if (config.skip_no_fid) {
				continue;
			}
			continue;
		}

		fid_out = LustreFID();
		components.clear();
		ParseFID(h, fid_out);
		ParseLOVDetailed(h, &components, nullptr);
		ext2fs_xattrs_close(&h);

		if (config.skip_no_fid && !fid_out.IsValid()) {
			continue;
		}
		if (components.empty()) {
			continue;
		}
		return true;
	}
}

bool MDTScanner::GetNextInodeLinkLayouts(LustreFID &fid_out, std::vector<LinkEntry> &links_out,
                                         std::vector<LustreLayoutComponent> &components_out,
                                         const MDTScanConfig &config) {
	ext2_ino_t ino;
	struct ext2_inode_large inode;

	while (true) {
		if (!GetNextRawInode(ino, inode)) {
			return false;
		}
		if (ReadInodeLinkLayouts(ino, fid_out, links_out, components_out, config)) {
			return true;
		}
	}
}

bool MDTScanner::GetNextInodeLayout(LustreInodeLayoutRow &row, const MDTScanConfig &config) {
	if (has_pending_layouts_) {
		row.inode = pending_inode_;
		row.layout_fid = pending_inode_.fid;
		row.layout = pending_layouts_[pending_layout_idx_];
		pending_layout_idx_++;
		if (pending_layout_idx_ >= pending_layouts_.size()) {
			has_pending_layouts_ = false;
		}
		return true;
	}

	ext2_ino_t ino;
	struct ext2_inode_large inode;

	while (true) {
		if (!GetNextRawInode(ino, inode)) {
			return false;
		}

		LustreInode inode_row;
		PopulateInodeMetadata(inode_row, ino, inode);

		struct ext2_xattr_handle *h = nullptr;
		if (!OpenAndReadXattrs(ino, h)) {
			continue;
		}

		ParseFID(h, inode_row.fid);
		bool has_linkea = HasLinkEA(h);
		std::vector<LustreLayoutComponent> components;
		ParseLOVDetailed(h, &components, nullptr);

		uint64_t som_size, som_blocks;
		if (ParseSOM(h, som_size, som_blocks)) {
			inode_row.size = som_size;
			inode_row.blocks = som_blocks;
		}

		ext2fs_xattrs_close(&h);

		if (!inode_row.fid.IsValid() || components.empty()) {
			continue;
		}
		if (config.skip_no_fid && !inode_row.fid.IsValid()) {
			continue;
		}
		if (config.skip_no_linkea && !has_linkea) {
			continue;
		}

		valid_inodes_++;

		row.inode = inode_row;
		row.layout_fid = inode_row.fid;
		row.layout = components[0];

		if (components.size() > 1) {
			pending_inode_ = inode_row;
			pending_layouts_ = std::move(components);
			pending_layout_idx_ = 1;
			has_pending_layouts_ = true;
		}
		return true;
	}
}

bool MDTScanner::GetNextInodeLayout(LustreInodeLayoutRow &row, const MDTScanConfig &config, ext2_ino_t max_ino) {
	if (has_pending_layouts_) {
		row.inode = pending_inode_;
		row.layout_fid = pending_inode_.fid;
		row.layout = pending_layouts_[pending_layout_idx_];
		pending_layout_idx_++;
		if (pending_layout_idx_ >= pending_layouts_.size()) {
			has_pending_layouts_ = false;
		}
		return true;
	}

	ext2_ino_t ino;
	struct ext2_inode_large inode;

	while (true) {
		if (!GetNextRawInode(ino, inode, max_ino)) {
			return false;
		}

		LustreInode inode_row;
		PopulateInodeMetadata(inode_row, ino, inode);

		struct ext2_xattr_handle *h = nullptr;
		if (!OpenAndReadXattrs(ino, h)) {
			continue;
		}

		ParseFID(h, inode_row.fid);
		bool has_linkea = HasLinkEA(h);
		std::vector<LustreLayoutComponent> components;
		ParseLOVDetailed(h, &components, nullptr);

		uint64_t som_size, som_blocks;
		if (ParseSOM(h, som_size, som_blocks)) {
			inode_row.size = som_size;
			inode_row.blocks = som_blocks;
		}

		ext2fs_xattrs_close(&h);

		if (!inode_row.fid.IsValid() || components.empty()) {
			continue;
		}
		if (config.skip_no_fid && !inode_row.fid.IsValid()) {
			continue;
		}
		if (config.skip_no_linkea && !has_linkea) {
			continue;
		}

		valid_inodes_++;

		row.inode = inode_row;
		row.layout_fid = inode_row.fid;
		row.layout = components[0];

		if (components.size() > 1) {
			pending_inode_ = inode_row;
			pending_layouts_ = std::move(components);
			pending_layout_idx_ = 1;
			has_pending_layouts_ = true;
		}
		return true;
	}
}

bool MDTScanner::GetNextInodeLayouts(LustreFID &fid_out, std::vector<LustreLayoutComponent> &components,
                                     const MDTScanConfig &config, ext2_ino_t max_ino) {
	ext2_ino_t ino;
	struct ext2_inode_large inode;

	while (true) {
		if (!GetNextRawInode(ino, inode, max_ino)) {
			return false;
		}

		struct ext2_xattr_handle *h = nullptr;
		if (!OpenAndReadXattrs(ino, h)) {
			if (config.skip_no_fid) {
				continue;
			}
			continue;
		}

		fid_out = LustreFID();
		components.clear();
		ParseFID(h, fid_out);
		ParseLOVDetailed(h, &components, nullptr);
		ext2fs_xattrs_close(&h);

		if (config.skip_no_fid && !fid_out.IsValid()) {
			continue;
		}
		if (components.empty()) {
			continue;
		}
		return true;
	}
}

bool MDTScanner::GetNextInodeLinkLayouts(LustreFID &fid_out, std::vector<LinkEntry> &links_out,
                                         std::vector<LustreLayoutComponent> &components_out,
                                         const MDTScanConfig &config, ext2_ino_t max_ino) {
	ext2_ino_t ino;
	struct ext2_inode_large inode;

	while (true) {
		if (!GetNextRawInode(ino, inode, max_ino)) {
			return false;
		}
		if (ReadInodeLinkLayouts(ino, fid_out, links_out, components_out, config)) {
			return true;
		}
	}
}

bool MDTScanner::GetNextInodeLayoutObjects(LustreFID &fid_out, std::vector<LustreLayoutComponent> &components,
                                           std::vector<LustreOSTObject> &objects, const MDTScanConfig &config) {
	ext2_ino_t ino;
	struct ext2_inode_large inode;

	while (true) {
		if (!GetNextRawInode(ino, inode)) {
			return false;
		}

		struct ext2_xattr_handle *h = nullptr;
		if (!OpenAndReadXattrs(ino, h)) {
			continue;
		}

		fid_out = LustreFID();
		components.clear();
		objects.clear();
		ParseFID(h, fid_out);
		bool has_linkea = HasLinkEA(h);
		ParseLOVDetailed(h, &components, &objects);
		ext2fs_xattrs_close(&h);

		if (config.skip_no_fid && !fid_out.IsValid()) {
			continue;
		}
		if (config.skip_no_linkea && !has_linkea) {
			continue;
		}
		if (components.empty() || objects.empty()) {
			continue;
		}
		return true;
	}
}

bool MDTScanner::GetNextInodeLayoutObjects(LustreFID &fid_out, std::vector<LustreLayoutComponent> &components,
                                           std::vector<LustreOSTObject> &objects, const MDTScanConfig &config,
                                           ext2_ino_t max_ino) {
	ext2_ino_t ino;
	struct ext2_inode_large inode;

	while (true) {
		if (!GetNextRawInode(ino, inode, max_ino)) {
			return false;
		}

		struct ext2_xattr_handle *h = nullptr;
		if (!OpenAndReadXattrs(ino, h)) {
			continue;
		}

		fid_out = LustreFID();
		components.clear();
		objects.clear();
		ParseFID(h, fid_out);
		bool has_linkea = HasLinkEA(h);
		ParseLOVDetailed(h, &components, &objects);
		ext2fs_xattrs_close(&h);

		if (config.skip_no_fid && !fid_out.IsValid()) {
			continue;
		}
		if (config.skip_no_linkea && !has_linkea) {
			continue;
		}
		if (components.empty() || objects.empty()) {
			continue;
		}
		return true;
	}
}

bool MDTScanner::ReadInodeObjects(ext2_ino_t ino, LustreFID &fid_out,
                                  std::vector<LustreOSTObject> &objects) {
	objects.clear();

	struct ext2_xattr_handle *h = nullptr;
	if (!OpenAndReadXattrs(ino, h)) {
		return false;
	}

	bool has_fid = ParseFID(h, fid_out);
	ParseLOVDetailed(h, nullptr, &objects);

	ext2fs_xattrs_close(&h);
	return has_fid;
}

bool MDTScanner::ReadInodeObjects(ext2_ino_t ino, LustreInode &inode_out,
                                  std::vector<LustreOSTObject> &objects, const MDTScanConfig &config) {
	if (!fs_) {
		throw IOException("Filesystem not open for '%s'", device_path_);
	}

	objects.clear();

	struct ext2_inode_large inode;
	memset(&inode, 0, sizeof(inode));
	errcode_t err = ext2fs_read_inode_full(fs_, ino, (struct ext2_inode *)&inode, sizeof(inode));
	if (err) {
		return false;
	}
	if (inode.i_mode == 0 && inode.i_links_count == 0) {
		return false;
	}
	if (inode.i_dtime != 0) {
		return false;
	}

	PopulateInodeMetadata(inode_out, ino, inode);

	struct ext2_xattr_handle *h = nullptr;
	if (!OpenAndReadXattrs(ino, h)) {
		return false;
	}

	ParseFID(h, inode_out.fid);
	bool has_linkea = HasLinkEA(h);
	ParseLOVDetailed(h, nullptr, &objects);

	uint64_t som_size, som_blocks;
	if (ParseSOM(h, som_size, som_blocks)) {
		inode_out.size = som_size;
		inode_out.blocks = som_blocks;
	}

	ext2fs_xattrs_close(&h);

	if (config.skip_no_fid && !inode_out.fid.IsValid()) {
		return false;
	}
	if (config.skip_no_linkea && !has_linkea) {
		return false;
	}
	if (objects.empty()) {
		return false;
	}
	return true;
}

bool MDTScanner::GetNextInodeObjects(LustreFID &fid_out, std::vector<LustreOSTObject> &objects,
                                     const MDTScanConfig &config) {
	ext2_ino_t ino;
	struct ext2_inode_large inode;

	while (true) {
		if (!GetNextRawInode(ino, inode)) {
			return false;
		}

		struct ext2_xattr_handle *h = nullptr;
		if (!OpenAndReadXattrs(ino, h)) {
			if (config.skip_no_fid) {
				continue;
			}
			continue;
		}

		fid_out = LustreFID();
		objects.clear();
		ParseFID(h, fid_out);
		ParseLOVDetailed(h, nullptr, &objects);
		ext2fs_xattrs_close(&h);

		if (config.skip_no_fid && !fid_out.IsValid()) {
			continue;
		}
		if (objects.empty()) {
			continue;
		}
		return true;
	}
}

bool MDTScanner::GetNextInodeLinkObjects(LustreFID &fid_out, std::vector<LinkEntry> &links_out,
                                         std::vector<LustreOSTObject> &objects_out, const MDTScanConfig &config) {
	ext2_ino_t ino;
	struct ext2_inode_large inode;

	while (true) {
		if (!GetNextRawInode(ino, inode)) {
			return false;
		}
		if (ReadInodeLinkObjects(ino, fid_out, links_out, objects_out, config)) {
			return true;
		}
	}
}

bool MDTScanner::GetNextInodeObject(LustreInodeObjectRow &row, const MDTScanConfig &config) {
	if (has_pending_objects_) {
		row.inode = pending_inode_;
		row.object_fid = pending_inode_.fid;
		row.object = pending_objects_[pending_object_idx_];
		pending_object_idx_++;
		if (pending_object_idx_ >= pending_objects_.size()) {
			has_pending_objects_ = false;
		}
		return true;
	}

	ext2_ino_t ino;
	struct ext2_inode_large inode;

	while (true) {
		if (!GetNextRawInode(ino, inode)) {
			return false;
		}

		LustreInode inode_row;
		PopulateInodeMetadata(inode_row, ino, inode);

		struct ext2_xattr_handle *h = nullptr;
		if (!OpenAndReadXattrs(ino, h)) {
			continue;
		}

		ParseFID(h, inode_row.fid);
		bool has_linkea = HasLinkEA(h);
		std::vector<LustreOSTObject> objects;
		ParseLOVDetailed(h, nullptr, &objects);

		uint64_t som_size, som_blocks;
		if (ParseSOM(h, som_size, som_blocks)) {
			inode_row.size = som_size;
			inode_row.blocks = som_blocks;
		}

		ext2fs_xattrs_close(&h);

		if (!inode_row.fid.IsValid() || objects.empty()) {
			continue;
		}
		if (config.skip_no_fid && !inode_row.fid.IsValid()) {
			continue;
		}
		if (config.skip_no_linkea && !has_linkea) {
			continue;
		}

		valid_inodes_++;

		row.inode = inode_row;
		row.object_fid = inode_row.fid;
		row.object = objects[0];

		if (objects.size() > 1) {
			pending_inode_ = inode_row;
			pending_objects_ = std::move(objects);
			pending_object_idx_ = 1;
			has_pending_objects_ = true;
		}
		return true;
	}
}

bool MDTScanner::GetNextInodeObject(LustreInodeObjectRow &row, const MDTScanConfig &config, ext2_ino_t max_ino) {
	if (has_pending_objects_) {
		row.inode = pending_inode_;
		row.object_fid = pending_inode_.fid;
		row.object = pending_objects_[pending_object_idx_];
		pending_object_idx_++;
		if (pending_object_idx_ >= pending_objects_.size()) {
			has_pending_objects_ = false;
		}
		return true;
	}

	ext2_ino_t ino;
	struct ext2_inode_large inode;

	while (true) {
		if (!GetNextRawInode(ino, inode, max_ino)) {
			return false;
		}

		LustreInode inode_row;
		PopulateInodeMetadata(inode_row, ino, inode);

		struct ext2_xattr_handle *h = nullptr;
		if (!OpenAndReadXattrs(ino, h)) {
			continue;
		}

		ParseFID(h, inode_row.fid);
		bool has_linkea = HasLinkEA(h);
		std::vector<LustreOSTObject> objects;
		ParseLOVDetailed(h, nullptr, &objects);

		uint64_t som_size, som_blocks;
		if (ParseSOM(h, som_size, som_blocks)) {
			inode_row.size = som_size;
			inode_row.blocks = som_blocks;
		}

		ext2fs_xattrs_close(&h);

		if (!inode_row.fid.IsValid() || objects.empty()) {
			continue;
		}
		if (config.skip_no_fid && !inode_row.fid.IsValid()) {
			continue;
		}
		if (config.skip_no_linkea && !has_linkea) {
			continue;
		}

		valid_inodes_++;

		row.inode = inode_row;
		row.object_fid = inode_row.fid;
		row.object = objects[0];

		if (objects.size() > 1) {
			pending_inode_ = inode_row;
			pending_objects_ = std::move(objects);
			pending_object_idx_ = 1;
			has_pending_objects_ = true;
		}
		return true;
	}
}

bool MDTScanner::GetNextInodeObjects(LustreFID &fid_out, std::vector<LustreOSTObject> &objects,
                                     const MDTScanConfig &config, ext2_ino_t max_ino) {
	ext2_ino_t ino;
	struct ext2_inode_large inode;

	while (true) {
		if (!GetNextRawInode(ino, inode, max_ino)) {
			return false;
		}

		struct ext2_xattr_handle *h = nullptr;
		if (!OpenAndReadXattrs(ino, h)) {
			if (config.skip_no_fid) {
				continue;
			}
			continue;
		}

		fid_out = LustreFID();
		objects.clear();
		ParseFID(h, fid_out);
		ParseLOVDetailed(h, nullptr, &objects);
		ext2fs_xattrs_close(&h);

		if (config.skip_no_fid && !fid_out.IsValid()) {
			continue;
		}
		if (objects.empty()) {
			continue;
		}
		return true;
	}
}

bool MDTScanner::GetNextInodeLinkObjects(LustreFID &fid_out, std::vector<LinkEntry> &links_out,
                                         std::vector<LustreOSTObject> &objects_out, const MDTScanConfig &config,
                                         ext2_ino_t max_ino) {
	ext2_ino_t ino;
	struct ext2_inode_large inode;

	while (true) {
		if (!GetNextRawInode(ino, inode, max_ino)) {
			return false;
		}
		if (ReadInodeLinkObjects(ino, fid_out, links_out, objects_out, config)) {
			return true;
		}
	}
}

bool MDTScanner::ReadInodeFID(ext2_ino_t ino, LustreFID &fid_out) {
	struct ext2_xattr_handle *h = nullptr;
	if (!OpenAndReadXattrs(ino, h)) {
		return false;
	}

	bool has_fid = ParseFID(h, fid_out);
	ext2fs_xattrs_close(&h);
	return has_fid;
}

struct DirIterateCallbackData {
	std::vector<lustre::DirEntry> *entries;
};

static int dir_iterate_callback(struct ext2_dir_entry *dirent,
                                int offset, int blocksize,
                                char *buf, void *priv_data) {
	auto *cb_data = static_cast<DirIterateCallbackData *>(priv_data);
	int namelen = ext2fs_dirent_name_len(dirent);

	// Skip . and ..
	if (namelen == 1 && dirent->name[0] == '.') {
		return 0;
	}
	if (namelen == 2 && dirent->name[0] == '.' && dirent->name[1] == '.') {
		return 0;
	}

	lustre::DirEntry entry;
	entry.ino = dirent->inode;
	entry.name.assign(dirent->name, namelen);
	entry.file_type = ext2fs_dirent_file_type(dirent);
	cb_data->entries->push_back(std::move(entry));

	return 0;
}

bool MDTScanner::ReadDirectoryEntries(ext2_ino_t dir_ino, std::vector<DirEntry> &entries_out) {
	entries_out.clear();

	if (!fs_) {
		return false;
	}

	DirIterateCallbackData cb_data;
	cb_data.entries = &entries_out;

	errcode_t err = ext2fs_dir_iterate(fs_, dir_ino, 0, nullptr,
	                                   dir_iterate_callback, &cb_data);
	if (err) {
		return false;
	}

	return !entries_out.empty();
}

bool MDTScanner::LookupName(ext2_ino_t dir_ino, const std::string &name, ext2_ino_t &child_ino) {
	if (!fs_) {
		return false;
	}

	errcode_t err = ext2fs_lookup(fs_, dir_ino, name.c_str(), name.size(), nullptr, &child_ino);
	return err == 0;
}

//===----------------------------------------------------------------------===//
// Lustre Extended Attribute Parsing
//===----------------------------------------------------------------------===//

bool MDTScanner::ParseFID(struct ext2_xattr_handle *xattr_handle, LustreFID &fid) {
	void *value = nullptr;
	size_t value_len = 0;
	errcode_t err = ext2fs_xattr_get(xattr_handle, xattr::LMA, &value, &value_len);
	if (err || !value || value_len < sizeof(LustreLMA)) {
		return false;
	}

	// Parse LMA structure
	const uint8_t *data = static_cast<const uint8_t *>(value);

	// Skip lma_compat and lma_incompat
	const uint8_t *fid_data = data + LMA_FID_OFFSET;

	// Read FID fields (little-endian on x86)
	memcpy(&fid.f_seq, fid_data, sizeof(fid.f_seq));
	memcpy(&fid.f_oid, fid_data + 8, sizeof(fid.f_oid));
	memcpy(&fid.f_ver, fid_data + 12, sizeof(fid.f_ver));

	ext2fs_free_mem(&value);
	return true;
}

bool MDTScanner::ParseLinkEA(struct ext2_xattr_handle *xattr_handle, std::vector<LinkEntry> &links) {
	links.clear();

	void *value = nullptr;
	size_t value_len = 0;
	errcode_t err = ext2fs_xattr_get(xattr_handle, xattr::LINK, &value, &value_len);
	if (err || !value || value_len < sizeof(LinkEAHeader)) {
		return false;
	}

	const uint8_t *data = static_cast<const uint8_t *>(value);
	const uint8_t *end = data + value_len;

	// Parse LinkEA header
	LinkEAHeader header;
	memcpy(&header.leh_magic, data, sizeof(header.leh_magic));
	memcpy(&header.leh_reccount, data + 4, sizeof(header.leh_reccount));
	memcpy(&header.leh_len, data + 8, sizeof(header.leh_len));

	// Verify magic
	if (header.leh_magic != LINK_EA_MAGIC) {
		ext2fs_free_mem(&value);
		return false;
	}

	// Skip header (24 bytes)
	const uint8_t *entry_ptr = data + sizeof(LinkEAHeader);

	// Parse each entry
	for (uint32_t i = 0; i < header.leh_reccount && entry_ptr + LINK_EA_RECLEN_SIZE <= end; i++) {
		uint16_t reclen = ReadBE16(entry_ptr);

		if (reclen < LINK_EA_MIN_ENTRY_SIZE || entry_ptr + reclen > end) {
			break;
		}

		// Parse parent FID (after reclen field) - stored as big-endian
		const uint8_t *fid_ptr = entry_ptr + LINK_EA_RECLEN_SIZE;
		LustreFID parent_fid;
		parent_fid.f_seq = ReadBE64(fid_ptr);
		parent_fid.f_oid = ReadBE32(fid_ptr + 8);
		parent_fid.f_ver = ReadBE32(fid_ptr + 12);

		// Extract name (remaining bytes after FID), trimming trailing null bytes
		size_t name_len = reclen - LINK_EA_MIN_ENTRY_SIZE;
		const char *name_ptr = reinterpret_cast<const char *>(entry_ptr + LINK_EA_MIN_ENTRY_SIZE);
		while (name_len > 0 && name_ptr[name_len - 1] == '\0') {
			name_len--;
		}
		std::string name(name_ptr, name_len);

		links.emplace_back(parent_fid, name);

		// Move to next entry
		entry_ptr += reclen;
	}

	ext2fs_free_mem(&value);
	return !links.empty();
}

//===----------------------------------------------------------------------===//
// ParseLOVDetailed - full LOV xattr parsing with composite layout support
//===----------------------------------------------------------------------===//

// V1/V3 blob binary layout (little-endian):
//   Offset 0:  u32 lmm_magic
//   Offset 4:  u32 lmm_pattern
//   Offset 8:  u64 lmm_oi_id
//   Offset 16: u64 lmm_oi_seq
//   Offset 24: u32 lmm_stripe_size
//   Offset 28: u16 lmm_stripe_count
//   Offset 30: u16 lmm_layout_gen
//   Offset 32: char[16] lmm_pool_name (V3 only)
//   V1: offset 32, V3: offset 48: lov_user_ost_data_v1[stripe_count]
//
// lov_user_ost_data_v1 (24 bytes each):
//   Offset 0:  u64 l_ost_oi.oi_id
//   Offset 8:  u64 l_ost_oi.oi_seq
//   Offset 16: u32 l_ost_gen
//   Offset 20: u32 l_ost_idx

static bool ParseV1V3Blob(const uint8_t *data, size_t data_len,
                           uint32_t comp_index, uint32_t comp_id,
                           uint16_t mirror_id, uint32_t comp_flags,
                           uint64_t extent_start, uint64_t extent_end,
                           uint8_t dstripe_count, uint8_t cstripe_count,
                           uint8_t compr_type, uint8_t compr_lvl,
                           LustreLayoutComponent *out_component,
                           std::vector<LustreOSTObject> *out_objects) {
	if (data_len < 32) {
		return false;
	}

	uint32_t magic;
	memcpy(&magic, data, sizeof(magic));

	if (magic != LOV_MAGIC_V1 && magic != LOV_MAGIC_V3) {
		return false;
	}

	uint32_t pattern;
	memcpy(&pattern, data + 4, sizeof(pattern));

	uint32_t stripe_size;
	memcpy(&stripe_size, data + 24, sizeof(stripe_size));

	uint16_t stripe_count;
	memcpy(&stripe_count, data + 28, sizeof(stripe_count));

	// Determine objects offset based on V1 vs V3
	size_t objects_offset = 32; // V1
	std::string pool_name;
	if (magic == LOV_MAGIC_V3) {
		if (data_len < 48) {
			return false;
		}
		const char *pool_ptr = reinterpret_cast<const char *>(data + 32);
		size_t pool_len = strnlen(pool_ptr, 16);
		if (pool_len > 0) {
			pool_name.assign(pool_ptr, pool_len);
		}
		objects_offset = 48;
	}

	// Parse stripe_offset from first OST object (if available)
	uint16_t stripe_offset = 0xFFFF;
	size_t objects_end = objects_offset + static_cast<size_t>(stripe_count) * 24;

	if (stripe_count > 0 && objects_end <= data_len) {
		uint32_t first_ost_idx;
		memcpy(&first_ost_idx, data + objects_offset + 20, sizeof(first_ost_idx));
		stripe_offset = static_cast<uint16_t>(first_ost_idx);
	}

	if (out_component) {
		out_component->comp_index = comp_index;
		out_component->comp_id = comp_id;
		out_component->mirror_id = mirror_id;
		out_component->comp_flags = comp_flags;
		out_component->extent_start = extent_start;
		out_component->extent_end = extent_end;
		out_component->pattern = pattern;
		out_component->stripe_size = stripe_size;
		out_component->stripe_count = stripe_count;
		out_component->stripe_offset = stripe_offset;
		out_component->pool_name = pool_name;
		out_component->dstripe_count = dstripe_count;
		out_component->cstripe_count = cstripe_count;
		out_component->compr_type = compr_type;
		out_component->compr_lvl = compr_lvl;
	}

	// Parse OST objects only for initialized components with valid object data
	if (out_objects && stripe_count > 0 && objects_end <= data_len) {
		// Skip uninitialized composite components (objects may be garbage)
		if (comp_flags != 0 && !(comp_flags & LCME_FL_INIT)) {
			return true;
		}

		for (uint16_t i = 0; i < stripe_count; i++) {
			const uint8_t *obj_ptr = data + objects_offset + static_cast<size_t>(i) * 24;
			LustreOSTObject obj;
			obj.comp_index = comp_index;
			obj.stripe_index = i;
			memcpy(&obj.ost_oi_id, obj_ptr, sizeof(obj.ost_oi_id));
			memcpy(&obj.ost_oi_seq, obj_ptr + 8, sizeof(obj.ost_oi_seq));
			memcpy(&obj.ost_idx, obj_ptr + 20, sizeof(obj.ost_idx));
			out_objects->push_back(obj);
		}
	}

	return true;
}

// lov_comp_md_v1 header (32 bytes, packed):
//   Offset 0:  u32 lcm_magic
//   Offset 4:  u32 lcm_size
//   Offset 8:  u32 lcm_layout_gen
//   Offset 12: u16 lcm_flags
//   Offset 14: u16 lcm_entry_count
//   Offset 16: u16 lcm_mirror_count
//   Offset 18: u8  lcm_ec_count
//   Offset 19: u8  padding
//   Offset 20: u16[2] padding
//   Offset 24: u64 padding
//
// lov_comp_md_entry_v1 (48 bytes, packed):
//   Offset 0:  u32 lcme_id
//   Offset 4:  u32 lcme_flags
//   Offset 8:  u64 lcme_extent.e_start
//   Offset 16: u64 lcme_extent.e_end
//   Offset 24: u32 lcme_offset
//   Offset 28: u32 lcme_size
//   Offset 32: u32 lcme_layout_gen
//   Offset 36: u64 lcme_timestamp
//   Offset 44: u8  lcme_dstripe_count
//   Offset 45: u8  lcme_cstripe_count
//   Offset 46: u8  lcme_compr_type
//   Offset 47: u8  lcme_compr_lvl:4 | lcme_compr_chunk_log_bits:4

static constexpr size_t COMP_V1_HEADER_SIZE = 32;
static constexpr size_t COMP_ENTRY_SIZE = 48;

bool MDTScanner::ParseLOVDetailed(struct ext2_xattr_handle *xattr_handle,
                                  std::vector<LustreLayoutComponent> *components,
                                  std::vector<LustreOSTObject> *objects) {
	void *value = nullptr;
	size_t value_len = 0;
	errcode_t err = ext2fs_xattr_get(xattr_handle, xattr::LOV, &value, &value_len);
	if (err || !value) {
		return false;
	}

	if (value_len < 4) {
		ext2fs_free_mem(&value);
		return false;
	}

	const uint8_t *data = static_cast<const uint8_t *>(value);

	uint32_t magic;
	memcpy(&magic, data, sizeof(magic));

	bool result = false;

	if (magic == LOV_MAGIC_V1 || magic == LOV_MAGIC_V3) {
		// Simple (non-composite) layout — emit as a single component
		LustreLayoutComponent comp;
		result = ParseV1V3Blob(data, value_len, 0, 0, 0, 0, 0, LUSTRE_EOF,
		                       0, 0, 0, 0,
		                       components ? &comp : nullptr, objects);
		if (result && components) {
			components->push_back(std::move(comp));
		}
	} else if (magic == LOV_MAGIC_COMP_V1) {
		// Composite layout (PFL, FLR, EC)
		if (value_len < COMP_V1_HEADER_SIZE) {
			ext2fs_free_mem(&value);
			return false;
		}

		uint16_t entry_count;
		memcpy(&entry_count, data + 14, sizeof(entry_count));

		// Validate that all entries fit within the header area
		size_t entries_end = COMP_V1_HEADER_SIZE + static_cast<size_t>(entry_count) * COMP_ENTRY_SIZE;
		if (entries_end > value_len) {
			ext2fs_free_mem(&value);
			return false;
		}

		result = true;
		for (uint16_t i = 0; i < entry_count; i++) {
			const uint8_t *entry = data + COMP_V1_HEADER_SIZE + static_cast<size_t>(i) * COMP_ENTRY_SIZE;

			uint32_t lcme_id;
			memcpy(&lcme_id, entry, sizeof(lcme_id));

			uint32_t lcme_flags;
			memcpy(&lcme_flags, entry + 4, sizeof(lcme_flags));

			uint64_t extent_start, extent_end;
			memcpy(&extent_start, entry + 8, sizeof(extent_start));
			memcpy(&extent_end, entry + 16, sizeof(extent_end));

			uint32_t lcme_offset, lcme_size;
			memcpy(&lcme_offset, entry + 24, sizeof(lcme_offset));
			memcpy(&lcme_size, entry + 28, sizeof(lcme_size));

			uint8_t dstripe_count = entry[44];
			uint8_t cstripe_count = entry[45];
			uint8_t compr_type = entry[46];
			uint8_t compr_lvl = entry[47] & 0x0F;

			uint16_t mirror_id = static_cast<uint16_t>((lcme_id & MIRROR_ID_MASK) >> MIRROR_ID_SHIFT);

			// Validate blob offset and size
			if (lcme_offset + lcme_size > value_len) {
				continue;
			}

			LustreLayoutComponent comp;
			ParseV1V3Blob(data + lcme_offset, lcme_size, i, lcme_id, mirror_id,
			              lcme_flags, extent_start, extent_end,
			              dstripe_count, cstripe_count, compr_type, compr_lvl,
			              components ? &comp : nullptr, objects);

			if (components) {
				components->push_back(std::move(comp));
			}
		}
	}
	// Other magic values (LOV_MAGIC_FOREIGN, etc.) are silently skipped

	ext2fs_free_mem(&value);
	return result;
}

bool MDTScanner::ParseSOM(struct ext2_xattr_handle *xattr_handle, uint64_t &size, uint64_t &blocks) {
	void *value = nullptr;
	size_t value_len = 0;
	errcode_t err = ext2fs_xattr_get(xattr_handle, xattr::SOM, &value, &value_len);
	if (err || !value || value_len < sizeof(LustreSOM)) {
		return false;
	}

	// Parse SOM structure (little-endian on x86)
	const uint8_t *data = static_cast<const uint8_t *>(value);
	uint16_t lsa_valid;
	memcpy(&lsa_valid, data, sizeof(lsa_valid));

	// Check if SOM is valid (lsa_valid should be non-zero)
	if (lsa_valid == 0) {
		ext2fs_free_mem(&value);
		return false;
	}

	// Read size and blocks (offset: 2 + 6 = 8 bytes for lsa_valid + reserved)
	memcpy(&size, data + 8, sizeof(size));
	memcpy(&blocks, data + 16, sizeof(blocks));

	ext2fs_free_mem(&value);
	return true;
}

bool MDTScanner::HasLinkEA(struct ext2_xattr_handle *xattr_handle) {
	void *value = nullptr;
	size_t value_len = 0;
	errcode_t err = ext2fs_xattr_get(xattr_handle, xattr::LINK, &value, &value_len);
	if (err || !value || value_len < sizeof(LinkEAHeader)) {
		return false;
	}

	// Only check magic and reccount — no full entry parsing
	const uint8_t *data = static_cast<const uint8_t *>(value);
	uint32_t magic;
	uint32_t reccount;
	memcpy(&magic, data, sizeof(magic));
	memcpy(&reccount, data + 4, sizeof(reccount));

	ext2fs_free_mem(&value);

	return magic == LINK_EA_MAGIC && reccount > 0;
}

} // namespace lustre
} // namespace duckdb
