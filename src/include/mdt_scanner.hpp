//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// mdt_scanner.hpp
//
// MDT (Metadata Target) scanner using libext2fs
// Concrete class providing GetNextInode and GetNextLink
//===----------------------------------------------------------------------===//

#pragma once

#include "lustre_types.hpp"
#include "oi_lookup.h"
#include "duckdb/common/exception.hpp"

#include <ext2fs/ext2fs.h>
#include <memory>
#include <string>
#include <vector>
#include <atomic>
#include <mutex>

namespace duckdb {
namespace lustre {

//===----------------------------------------------------------------------===//
// MDT Scan Configuration
//===----------------------------------------------------------------------===//
struct MDTScanConfig {
	//! Skip inodes without a valid Lustre FID (default: true)
	bool skip_no_fid = true;
	//! Skip inodes without LinkEA (no hard link information) (default: true)
	bool skip_no_linkea = true;
};

//===----------------------------------------------------------------------===//
// Directory Entry (for targeted directory reads)
//===----------------------------------------------------------------------===//
struct DirEntry {
	ext2_ino_t ino;
	std::string name;
	uint8_t file_type;
};

//===----------------------------------------------------------------------===//
// MDT Scanner - concrete class for scanning inodes from ldiskfs-formatted MDT device
//===----------------------------------------------------------------------===//
class MDTScanner {
public:
	MDTScanner();
	~MDTScanner();

	// Disable copy
	MDTScanner(const MDTScanner&) = delete;
	MDTScanner& operator=(const MDTScanner&) = delete;

	//! Open the MDT device for scanning
	void Open(const std::string &device_path);

	//! Close the device
	void Close();

	//! Check if device is open
	bool IsOpen() const { return fs_ != nullptr; }

	//! Get total number of inodes in filesystem
	uint64_t GetTotalInodes() const;

	//! Get number of used inodes
	uint64_t GetUsedInodes() const;

	//! Get number of block groups
	uint32_t GetBlockGroupCount() const;

	//! Get inodes per block group (from superblock)
	uint32_t GetInodesPerGroup() const;

	//! Get filesystem block size
	uint32_t GetBlockSize() const;

	//! Get inode size
	uint32_t GetInodeSize() const;

	//===----------------------------------------------------------------------===//
	// Sequential Scan Interface
	//===----------------------------------------------------------------------===//

	//! Start a new inode scan
	void StartScan(int buffer_blocks = 0);

	//! Full metadata scan (for lustre_inodes)
	bool GetNextInode(LustreInode &inode, const MDTScanConfig &config = MDTScanConfig{});

	//! Full metadata scan with inode number upper bound (for block-group-bounded parallel scan)
	bool GetNextInode(LustreInode &inode, const MDTScanConfig &config, ext2_ino_t max_ino);

	//! Link scan (for lustre_links) - returns one link (one row) per call
	bool GetNextLink(LustreLink &link, const MDTScanConfig &config = MDTScanConfig{});

	//! Link scan with inode number upper bound (for block-group-bounded parallel scan)
	bool GetNextLink(LustreLink &link, const MDTScanConfig &config, ext2_ino_t max_ino);

	//! Internal fused inode+link scan used to avoid double full scans for joins
	bool GetNextInodeLink(LustreInodeLinkRow &row, const MDTScanConfig &config = MDTScanConfig{});

	//! Internal fused inode+link scan with inode number upper bound
	bool GetNextInodeLink(LustreInodeLinkRow &row, const MDTScanConfig &config, ext2_ino_t max_ino);

	//! Close current scan
	void CloseScan();

	//===----------------------------------------------------------------------===//
	// Block Group Based Scan (for parallel scanning)
	//===----------------------------------------------------------------------===//

	//! Jump to a specific block group
	void GotoBlockGroup(int group);

	//! Get next block group to scan (thread-safe)
	int GetNextBlockGroup();

	//! Reset block group counter
	void ResetBlockGroupCounter();

	//===----------------------------------------------------------------------===//
	// OI (Object Index) Targeted Lookup Interface
	//===----------------------------------------------------------------------===//

	//! Initialize OI context for the current device
	void InitOI();

	//! Close OI context
	void CloseOI();

	//! Lookup inode number for a given FID via OI B-tree
	//! Returns true if found, false otherwise
	bool LookupFID(const LustreFID &fid, ext2_ino_t &ino_out);

	//! Read a specific inode by number and populate full metadata (for OI lookup path)
	bool ReadInode(ext2_ino_t ino, LustreInode &out, const MDTScanConfig &config = MDTScanConfig{});

	//! Read a specific inode's metadata and parsed LinkEA in a single xattr pass
	bool ReadInodeLinks(ext2_ino_t ino, LustreInode &inode_out, std::vector<LinkEntry> &links_out,
	                    const MDTScanConfig &config = MDTScanConfig{});

	//! Read a specific inode's FID, LinkEA, and layout components in a single xattr pass
	bool ReadInodeLinkLayouts(ext2_ino_t ino, LustreFID &fid_out, std::vector<LinkEntry> &links_out,
	                          std::vector<LustreLayoutComponent> &components_out,
	                          const MDTScanConfig &config = MDTScanConfig{});

	//! Read a specific inode's FID, LinkEA, and OST objects in a single xattr pass
	bool ReadInodeLinkObjects(ext2_ino_t ino, LustreFID &fid_out, std::vector<LinkEntry> &links_out,
	                          std::vector<LustreOSTObject> &objects_out,
	                          const MDTScanConfig &config = MDTScanConfig{});

	//! Read a specific inode's FID and LinkEA
	bool ReadInodeLinkEA(ext2_ino_t ino, LustreFID &fid_out, std::vector<LinkEntry> &links_out);

	//! Read a specific inode's layout components (for lustre_layouts)
	bool ReadInodeLayouts(ext2_ino_t ino, LustreFID &fid_out,
	                      std::vector<LustreLayoutComponent> &components);

	//! Read a specific inode's metadata and layout components in a single xattr pass
	bool ReadInodeLayouts(ext2_ino_t ino, LustreInode &inode_out, std::vector<LustreLayoutComponent> &components,
	                      const MDTScanConfig &config = MDTScanConfig{});

	//! Read a specific inode's FID, layout components, and OST objects in a single xattr pass
	bool ReadInodeLayoutObjects(ext2_ino_t ino, LustreFID &fid_out, std::vector<LustreLayoutComponent> &components,
	                            std::vector<LustreOSTObject> &objects,
	                            const MDTScanConfig &config = MDTScanConfig{});

	//! Sequential scan of the next inode that has layout components
	bool GetNextInodeLayouts(LustreFID &fid_out, std::vector<LustreLayoutComponent> &components,
	                         const MDTScanConfig &config = MDTScanConfig{});

	//! Sequential scan of the next inode that has both LinkEA and layout components
	bool GetNextInodeLinkLayouts(LustreFID &fid_out, std::vector<LinkEntry> &links_out,
	                             std::vector<LustreLayoutComponent> &components_out,
	                             const MDTScanConfig &config = MDTScanConfig{});

	//! Sequential scan of the next inode with an upper inode bound that has layout components
	bool GetNextInodeLayouts(LustreFID &fid_out, std::vector<LustreLayoutComponent> &components,
	                         const MDTScanConfig &config, ext2_ino_t max_ino);

	//! Sequential scan of the next inode with an upper inode bound that has both LinkEA and layout components
	bool GetNextInodeLinkLayouts(LustreFID &fid_out, std::vector<LinkEntry> &links_out,
	                             std::vector<LustreLayoutComponent> &components_out,
	                             const MDTScanConfig &config, ext2_ino_t max_ino);

	//! Sequential scan of the next inode that has both layout components and OST objects
	bool GetNextInodeLayoutObjects(LustreFID &fid_out, std::vector<LustreLayoutComponent> &components,
	                               std::vector<LustreOSTObject> &objects,
	                               const MDTScanConfig &config = MDTScanConfig{});

	//! Sequential scan of the next inode with an upper inode bound that has both layouts and objects
	bool GetNextInodeLayoutObjects(LustreFID &fid_out, std::vector<LustreLayoutComponent> &components,
	                               std::vector<LustreOSTObject> &objects,
	                               const MDTScanConfig &config, ext2_ino_t max_ino);

	//! Internal fused inode+layout scan used to avoid double full scans for joins
	bool GetNextInodeLayout(LustreInodeLayoutRow &row, const MDTScanConfig &config = MDTScanConfig{});

	//! Internal fused inode+layout scan with inode number upper bound
	bool GetNextInodeLayout(LustreInodeLayoutRow &row, const MDTScanConfig &config, ext2_ino_t max_ino);

	//! Read a specific inode's OST objects (for lustre_objects)
	bool ReadInodeObjects(ext2_ino_t ino, LustreFID &fid_out,
	                      std::vector<LustreOSTObject> &objects);

	//! Read a specific inode's metadata and OST objects in a single xattr pass
	bool ReadInodeObjects(ext2_ino_t ino, LustreInode &inode_out, std::vector<LustreOSTObject> &objects,
	                      const MDTScanConfig &config = MDTScanConfig{});

	//! Sequential scan of the next inode that has OST objects
	bool GetNextInodeObjects(LustreFID &fid_out, std::vector<LustreOSTObject> &objects,
	                         const MDTScanConfig &config = MDTScanConfig{});

	//! Sequential scan of the next inode that has both LinkEA and OST objects
	bool GetNextInodeLinkObjects(LustreFID &fid_out, std::vector<LinkEntry> &links_out,
	                             std::vector<LustreOSTObject> &objects_out,
	                             const MDTScanConfig &config = MDTScanConfig{});

	//! Sequential scan of the next inode with an upper inode bound that has OST objects
	bool GetNextInodeObjects(LustreFID &fid_out, std::vector<LustreOSTObject> &objects,
	                         const MDTScanConfig &config, ext2_ino_t max_ino);

	//! Sequential scan of the next inode with an upper inode bound that has both LinkEA and OST objects
	bool GetNextInodeLinkObjects(LustreFID &fid_out, std::vector<LinkEntry> &links_out,
	                             std::vector<LustreOSTObject> &objects_out,
	                             const MDTScanConfig &config, ext2_ino_t max_ino);

	//! Internal fused inode+object scan used to avoid double full scans for joins
	bool GetNextInodeObject(LustreInodeObjectRow &row, const MDTScanConfig &config = MDTScanConfig{});

	//! Internal fused inode+object scan with inode number upper bound
	bool GetNextInodeObject(LustreInodeObjectRow &row, const MDTScanConfig &config, ext2_ino_t max_ino);

	//! Read a specific inode's FID only
	bool ReadInodeFID(ext2_ino_t ino, LustreFID &fid_out);

	//! Read directory entries (children) of a directory inode
	bool ReadDirectoryEntries(ext2_ino_t dir_ino, std::vector<DirEntry> &entries_out);

	//! Lookup a single name in a directory (wraps ext2fs_lookup)
	bool LookupName(ext2_ino_t dir_ino, const std::string &name, ext2_ino_t &child_ino);

private:
	//===----------------------------------------------------------------------===//
	// Internal helpers
	//===----------------------------------------------------------------------===//

	//! Get next raw inode from ext2fs scan (skips unused/deleted)
	bool GetNextRawInode(ext2_ino_t &ino, struct ext2_inode_large &raw);

	//! Get next raw inode with upper bound (stops when ino > max_ino)
	bool GetNextRawInode(ext2_ino_t &ino, struct ext2_inode_large &raw, ext2_ino_t max_ino);

	//! Return true when the block group's inode bitmap contains at least one allocated inode
	bool BlockGroupHasAllocatedInodes(int group) const;

	//! Open xattr handle and read xattrs for an inode
	bool OpenAndReadXattrs(ext2_ino_t ino, struct ext2_xattr_handle *&h);

	//===----------------------------------------------------------------------===//
	// Lustre Extended Attribute Parsing
	//===----------------------------------------------------------------------===//

	//! Parse FID from trusted.lma xattr
	bool ParseFID(struct ext2_xattr_handle *xattr_handle, LustreFID &fid);

	//! Parse hard link information from trusted.link xattr (LinkEA)
	bool ParseLinkEA(struct ext2_xattr_handle *xattr_handle, std::vector<LinkEntry> &links);

	//! Parse LOV (stripe/layout info) from trusted.lov xattr into components and/or objects.
	//! Either pointer may be nullptr if that output is not needed.
	bool ParseLOVDetailed(struct ext2_xattr_handle *xattr_handle,
	                      std::vector<LustreLayoutComponent> *components,
	                      std::vector<LustreOSTObject> *objects);

	//! Parse SOM (Size on MDS) from trusted.som xattr
	bool ParseSOM(struct ext2_xattr_handle *xattr_handle, uint64_t &size, uint64_t &blocks);

	//! Lightweight check for LinkEA presence (no full parse)
	bool HasLinkEA(struct ext2_xattr_handle *xattr_handle);

	ext2_filsys fs_;              // Filesystem handle
	ext2_inode_scan scan_;        // Inode scan handle
	std::string device_path_;     // Device path
	struct oi_context *oi_ctx_;   // OI lookup context (separate ext2_filsys handle)

	// For parallel block group scanning
	std::atomic<int> next_block_group_;
	std::mutex mutex_;

	// Scan statistics
	std::atomic<uint64_t> scanned_inodes_;
	std::atomic<uint64_t> valid_inodes_;

	// Link iteration state (link expansion managed internally)
	LustreInode pending_inode_;
	LustreFID pending_fid_;
	std::vector<LinkEntry> pending_links_;
	size_t pending_link_idx_ = 0;
	bool has_pending_links_ = false;

	// Object iteration state (object expansion managed internally)
	std::vector<LustreOSTObject> pending_objects_;
	size_t pending_object_idx_ = 0;
	bool has_pending_objects_ = false;

	// Layout iteration state (component expansion managed internally)
	std::vector<LustreLayoutComponent> pending_layouts_;
	size_t pending_layout_idx_ = 0;
	bool has_pending_layouts_ = false;

	// When GotoBlockGroup() sees an empty inode bitmap range, bounded scans return
	// without touching the inode table.
	bool skip_current_group_ = false;
};

} // namespace lustre
} // namespace duckdb
