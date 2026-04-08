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
	//! Internal optimization flag: false when raw inode metadata is sufficient
	bool read_xattrs = true;
	//! Whether LinkEA parsing should materialize link names
	bool read_link_names = true;
};

//===----------------------------------------------------------------------===//
// Directory Entry (for targeted directory reads)
//===----------------------------------------------------------------------===//
struct DirEntry {
	ext2_ino_t ino;
	std::string name;
	uint8_t file_type;
};

struct FLDRangeEntry {
	uint64_t seq_start;
	uint64_t seq_end;
	uint32_t mdt_index;
	uint32_t flags;
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

	//! Return this MDT's numeric index parsed from the filesystem label
	bool GetMDTIndex(uint32_t &mdt_index_out);

	//! Resolve a FID sequence to its home MDT index via MDT0's FLD index
	bool LookupFIDHomeMDTIndex(const LustreFID &fid, uint32_t &mdt_index_out);

	//! Load all MDT FLD ranges from MDT0's FLD index
	bool LoadFLDRanges(std::vector<FLDRangeEntry> &ranges_out);

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
	bool ReadInodeLinkEA(ext2_ino_t ino, LustreFID &fid_out, std::vector<LinkEntry> &links_out,
	                    bool read_names = true);

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

	//! Read a specific inode's LMA incompat flags
	bool ReadInodeLMAIncompat(ext2_ino_t ino, uint32_t &incompat_out);

	//! Read a specific inode's LMV xattr
	bool ReadInodeLMV(ext2_ino_t ino, LustreLMV &lmv_out);

	//! Sequential scan: next directory inode with FID, LMV, LinkEA, dir entries, and LMA incompat flags.
	//! Skips agent inodes (LMAI_AGENT).
	bool GetNextDirMapEntries(ext2_ino_t &ino_out, LustreFID &fid_out, LustreLMV &lmv_out,
	                          std::vector<LinkEntry> &links_out,
	                          std::vector<DirEntry> &dir_entries_out,
	                          uint32_t &lma_incompat_out,
	                          const MDTScanConfig &config);

	//! Same with upper bound for block-group-bounded parallel scan
	bool GetNextDirMapEntries(ext2_ino_t &ino_out, LustreFID &fid_out, LustreLMV &lmv_out,
	                          std::vector<LinkEntry> &links_out,
	                          std::vector<DirEntry> &dir_entries_out,
	                          uint32_t &lma_incompat_out,
	                          const MDTScanConfig &config, ext2_ino_t max_ino);

	//! Read directory entries (children) of a directory inode
	bool ReadDirectoryEntries(ext2_ino_t dir_ino, std::vector<DirEntry> &entries_out);

	//! Lookup a single name in a directory (wraps ext2fs_lookup)
	bool LookupName(ext2_ino_t dir_ino, const std::string &name, ext2_ino_t &child_ino);

private:
	//===----------------------------------------------------------------------===//
	// Internal helpers
	//===----------------------------------------------------------------------===//

	//! Get next raw inode from ext2fs scan (skips unused/deleted)
	bool GetNextRawInode(ext2_ino_t &ino);

	//! Get next raw inode with upper bound (stops when ino > max_ino)
	bool GetNextRawInode(ext2_ino_t &ino, ext2_ino_t max_ino);

	//! Read a specific inode into the reusable full-size inode buffer
	bool ReadRawInode(ext2_ino_t ino);

	//! Return true when the block group's inode bitmap contains at least one allocated inode
	bool BlockGroupHasAllocatedInodes(int group) const;

	//! Ensure the reusable inode buffer matches the current filesystem's inode size
	void EnsureInodeBuffer();

	//! Ensure the reusable xattr block buffer matches the current filesystem's block size
	void EnsureXattrBlockBuffer();

	//! Clear per-inode cached xattr state
	void InvalidateBufferedXattrState();

	//! Load the current inode's external EA block into the reusable buffer
	bool LoadBufferedExternalXattrBlock();

	//! Fast path for skip_no_fid/skip_no_linkea without opening a full xattr handle
	bool PassesXattrSkipChecksFast(ext2_ino_t ino, const MDTScanConfig &config);

	//! Find an xattr value in the current buffered inode / EA block without allocating
	bool FindBufferedXattrValue(uint8_t name_index, const char *short_name, size_t short_name_len,
	                            const uint8_t *&value_ptr, size_t &value_len, ext2_ino_t &value_inum);

	//! Read a prefix of an external EA inode value into the caller-provided buffer
	bool ReadExternalXattrPrefix(ext2_ino_t value_ino, void *buf, unsigned int wanted, unsigned int &got);

	//! Read a complete external EA inode value into the reusable scratch buffer
	bool ReadExternalXattrValue(ext2_ino_t value_ino, const uint8_t *&value_ptr, size_t &value_len);

	//! Read the prefix of a buffered xattr value into a caller-provided scratch buffer when needed
	bool ReadBufferedXattrPrefix(uint8_t name_index, const char *short_name, size_t short_name_len,
	                             void *scratch, unsigned int wanted, const uint8_t *&value_ptr, size_t &value_len);

	//! Read the complete buffered xattr value into a reusable scratch buffer when needed
	bool ReadBufferedXattrValue(uint8_t name_index, const char *short_name, size_t short_name_len,
	                            const uint8_t *&value_ptr, size_t &value_len);

	//! Direct parsers for lustre_inodes that avoid ext2_xattr_handle allocations
	bool ParseBufferedFID(LustreFID &fid);
	bool ParseBufferedSOM(uint64_t &size, uint64_t &blocks);
	bool HasBufferedLinkEA();
	bool ParseBufferedLinkEA(std::vector<LinkEntry> &links, bool read_names = true);
	bool ParseBufferedLOVDetailed(std::vector<LustreLayoutComponent> *components,
	                              std::vector<LustreOSTObject> *objects);
	bool ParseBufferedLMV(LustreLMV &lmv);
	bool ParseBufferedLMAIncompat(uint32_t &incompat);
	bool EnsureFLDReady();
	void CloseFLD();

	ext2_filsys fs_;              // Filesystem handle
	ext2_inode_scan scan_;        // Inode scan handle
	std::string device_path_;     // Device path
	struct oi_context *oi_ctx_;   // OI lookup context (separate ext2_filsys handle)
	ext2_ino_t fld_ino_ = 0;
	ext2_file_t fld_file_ = nullptr;
	bool fld_initialized_ = false;
	bool fld_available_ = false;
	uint16_t fld_keysize_ = 0;
	uint16_t fld_recsize_ = 0;
	uint16_t fld_ptrsize_ = 0;
	uint8_t fld_indirect_levels_ = 0;
	std::vector<uint8_t> fld_root_buffer_;
	std::vector<uint8_t> fld_block_buffer_;
	uint64_t total_inodes_ = 0;
	uint32_t block_group_count_ = 0;
	uint32_t inodes_per_group_ = 0;
	uint32_t block_size_ = 0;
	uint32_t inode_size_ = 0;
	std::vector<char> inode_buffer_;
	std::vector<char> xattr_block_buffer_;
	std::vector<char> xattr_value_buffer_;
	ext2_ino_t buffered_inode_ = 0;
	bool buffered_xattr_block_loaded_ = false;
	bool buffered_xattr_block_valid_ = false;

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
