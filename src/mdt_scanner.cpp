//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// mdt_scanner.cpp
//
// MDT scanner implementation using libext2fs
//===----------------------------------------------------------------------===//

#include "mdt_scanner.hpp"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <limits>
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

static inline uint64_t ReadLE64(const uint8_t *p) {
	return static_cast<uint64_t>(p[0]) | (static_cast<uint64_t>(p[1]) << 8) |
	       (static_cast<uint64_t>(p[2]) << 16) | (static_cast<uint64_t>(p[3]) << 24) |
	       (static_cast<uint64_t>(p[4]) << 32) | (static_cast<uint64_t>(p[5]) << 40) |
	       (static_cast<uint64_t>(p[6]) << 48) | (static_cast<uint64_t>(p[7]) << 56);
}

static inline uint32_t ReadLE32(const uint8_t *p) {
	return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
	       (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}

static inline uint16_t ReadLE16(const uint8_t *p) {
	return static_cast<uint16_t>(p[0]) | (static_cast<uint16_t>(p[1]) << 8);
}

static inline void WriteBE64(uint8_t *p, uint64_t value) {
	p[0] = static_cast<uint8_t>(value >> 56);
	p[1] = static_cast<uint8_t>(value >> 48);
	p[2] = static_cast<uint8_t>(value >> 40);
	p[3] = static_cast<uint8_t>(value >> 32);
	p[4] = static_cast<uint8_t>(value >> 24);
	p[5] = static_cast<uint8_t>(value >> 16);
	p[6] = static_cast<uint8_t>(value >> 8);
	p[7] = static_cast<uint8_t>(value);
}

static void PopulateInodeMetadata(LustreInode &out, ext2_ino_t ino, const struct ext2_inode_large &inode) {
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
	out.fid = LustreFID();
	out.parent_fid = LustreFID();
	out.type = ModeToFileType(out.mode);
	out.flags = inode.i_flags;
	out.projid = inode.i_projid;
}

static inline struct ext2_inode_large *GetBufferedInode(std::vector<char> &inode_buffer) {
	return reinterpret_cast<struct ext2_inode_large *>(inode_buffer.data());
}

static constexpr uint8_t TRUSTED_XATTR_INDEX = 4;

struct XattrValueRef {
	const uint8_t *value_ptr = nullptr;
	size_t value_len = 0;
	ext2_ino_t value_inum = 0;
	bool found = false;
};

static bool MatchXattrEntry(const struct ext2_ext_attr_entry *entry, uint8_t name_index,
                            const char *short_name, size_t short_name_len) {
	return entry->e_name_index == name_index &&
	       entry->e_name_len == short_name_len &&
	       memcmp(EXT2_EXT_ATTR_NAME(entry), short_name, short_name_len) == 0;
}

static bool FindXattrEntryInRegion(uint8_t name_index, const char *short_name, size_t short_name_len,
                                   struct ext2_ext_attr_entry *entries, unsigned int storage_size,
                                   char *value_start, size_t values_size, XattrValueRef &result) {
	auto *entry = entries;
	unsigned int remain = storage_size;

	while (remain >= sizeof(struct ext2_ext_attr_entry) && !EXT2_EXT_IS_LAST_ENTRY(entry)) {
		remain -= sizeof(struct ext2_ext_attr_entry);
		auto name_storage = static_cast<unsigned int>(EXT2_EXT_ATTR_SIZE(entry->e_name_len));
		if (name_storage > remain) {
			return false;
		}
		remain -= name_storage;

		if (MatchXattrEntry(entry, name_index, short_name, short_name_len)) {
			result.found = true;
			result.value_len = entry->e_value_size;
			result.value_inum = entry->e_value_inum;
			if (entry->e_value_inum == 0) {
				if (static_cast<size_t>(entry->e_value_offs) + entry->e_value_size > values_size) {
					return false;
				}
				result.value_ptr = reinterpret_cast<const uint8_t *>(value_start + entry->e_value_offs);
			}
			return true;
		}

		entry = EXT2_EXT_ATTR_NEXT(entry);
	}

	return false;
}

static constexpr uint64_t IAM_LFIX_ROOT_MAGIC = 0xbedabb1edULL;
static constexpr uint16_t IAM_LEAF_HEADER_MAGIC = 0x1976;
static constexpr const char *FLD_INDEX_NAME = "fld";

struct IAMLfixRoot {
	uint64_t magic;
	uint16_t keysize;
	uint16_t recsize;
	uint16_t ptrsize;
	uint8_t indirect_levels;
	uint8_t padding;
} __attribute__((packed));

struct IAMDxCountLimit {
	uint16_t limit;
	uint16_t count;
} __attribute__((packed));

struct IAMLeafHead {
	uint16_t magic;
	uint16_t count;
} __attribute__((packed));

static bool ReadIndexedFileBlock(ext2_file_t file, uint32_t block_size, blk64_t lblk, std::vector<uint8_t> &buffer) {
	buffer.resize(block_size);

	errcode_t err = ext2fs_file_llseek(file, lblk * block_size, EXT2_SEEK_SET, nullptr);
	if (err) {
		return false;
	}

	unsigned int got = 0;
	err = ext2fs_file_read(file, buffer.data(), block_size, &got);
	return err == 0 && got == block_size;
}

static const uint8_t *FindIAMIndexEntry(const uint8_t *entries, uint16_t count, uint16_t keysize,
                                        uint16_t ptrsize, const uint8_t *target_key) {
	const auto entry_size = static_cast<idx_t>(keysize + ptrsize);
	if (count <= 1 || entry_size == 0) {
		return nullptr;
	}

	const auto *first = entries + entry_size;
	const auto *last = entries + static_cast<idx_t>(count - 1) * entry_size;

	if (memcmp(target_key, first, keysize) < 0) {
		return first;
	}
	if (memcmp(target_key, last, keysize) >= 0) {
		return last;
	}

	const auto *low = first;
	const auto *high = last;
	while (low + entry_size < high) {
		const auto slots = static_cast<idx_t>((high - low) / entry_size);
		const auto *mid = low + (slots / 2) * entry_size;
		if (memcmp(mid, target_key, keysize) <= 0) {
			low = mid + entry_size;
		} else {
			high = mid;
		}
	}

	if (memcmp(low, target_key, keysize) <= 0) {
		return low;
	}
	return low - entry_size;
}

static uint32_t ReadIAMIndexPointer(const uint8_t *entry, uint16_t keysize, uint16_t ptrsize) {
	const auto *ptr = entry + keysize;
	if (ptrsize == sizeof(uint32_t)) {
		return ReadLE32(ptr);
	}
	if (ptrsize == sizeof(uint64_t)) {
		return static_cast<uint32_t>(ReadLE64(ptr));
	}
	return 0;
}

static bool ParseMDTIndexLabel(const char *raw_label, uint32_t &mdt_index_out) {
	size_t label_len = 0;
	while (label_len < EXT2_LABEL_LEN && raw_label[label_len] != '\0') {
		label_len++;
	}
	if (label_len < 4) {
		return false;
	}

	for (size_t pos = 0; pos + 3 <= label_len; pos++) {
		if (raw_label[pos] != 'M' || raw_label[pos + 1] != 'D' || raw_label[pos + 2] != 'T') {
			continue;
		}
		if (pos != 0 && raw_label[pos - 1] != '-') {
			continue;
		}

		size_t digits_start = pos + 3;
		size_t digits_end = digits_start;
		while (digits_end < label_len && std::isxdigit(static_cast<unsigned char>(raw_label[digits_end]))) {
			digits_end++;
		}
		if (digits_end == digits_start || digits_end != label_len) {
			continue;
		}

		uint32_t value = 0;
		for (size_t i = digits_start; i < digits_end; i++) {
			value <<= 4;
			if (raw_label[i] >= '0' && raw_label[i] <= '9') {
				value |= static_cast<uint32_t>(raw_label[i] - '0');
			} else {
				value |= static_cast<uint32_t>(std::toupper(static_cast<unsigned char>(raw_label[i])) - 'A' + 10);
			}
		}

		mdt_index_out = value;
		return true;
	}

	return false;
}

static constexpr uint32_t LU_SEQ_RANGE_MASK = 0x3;
static constexpr uint32_t LU_SEQ_RANGE_MDT = 0x0;

static bool CollectFLDRangesFromBlock(ext2_file_t file, uint32_t block_size, uint16_t keysize, uint16_t recsize,
                                      uint16_t ptrsize, uint8_t remaining_levels, blk64_t block,
                                      std::vector<uint8_t> &buffer, std::vector<FLDRangeEntry> &ranges_out) {
	if (!ReadIndexedFileBlock(file, block_size, block, buffer)) {
		return false;
	}

	if (remaining_levels == 0) {
		const auto *leaf = reinterpret_cast<const IAMLeafHead *>(buffer.data());
		if (ReadLE16(reinterpret_cast<const uint8_t *>(&leaf->magic)) != IAM_LEAF_HEADER_MAGIC) {
			return false;
		}

		const auto leaf_count = ReadLE16(reinterpret_cast<const uint8_t *>(&leaf->count));
		const auto entry_size = static_cast<idx_t>(keysize + recsize);
		const auto *leaf_entries = buffer.data() + sizeof(IAMLeafHead);
		for (idx_t i = 0; i < leaf_count; i++) {
			const auto *entry = leaf_entries + i * entry_size;
			const auto *record = entry + keysize;

			FLDRangeEntry range;
			range.seq_start = ReadBE64(record);
			range.seq_end = ReadBE64(record + sizeof(uint64_t));
			range.mdt_index = ReadBE32(record + 2 * sizeof(uint64_t));
			range.flags = ReadBE32(record + 2 * sizeof(uint64_t) + sizeof(uint32_t));

			if (range.seq_start >= range.seq_end) {
				continue;
			}
			if ((range.flags & LU_SEQ_RANGE_MASK) != LU_SEQ_RANGE_MDT) {
				continue;
			}
			ranges_out.push_back(range);
		}
		return true;
	}

	const auto *entries = buffer.data();
	const auto *countlimit = reinterpret_cast<const IAMDxCountLimit *>(entries);
	const auto count = ReadLE16(reinterpret_cast<const uint8_t *>(&countlimit->count));
	const auto entry_size = static_cast<idx_t>(keysize + ptrsize);
	for (idx_t i = 1; i < count; i++) {
		const auto *entry = entries + i * entry_size;
		const auto child = ReadIAMIndexPointer(entry, keysize, ptrsize);
		if (child == 0) {
			continue;
		}
		if (!CollectFLDRangesFromBlock(file, block_size, keysize, recsize, ptrsize, remaining_levels - 1,
		                               child, buffer, ranges_out)) {
			return false;
		}
	}

	return true;
}

bool MDTScanner::EnsureFLDReady() {
	if (fld_initialized_) {
		return fld_available_;
	}

	fld_initialized_ = true;
	fld_available_ = false;
	fld_ino_ = 0;
	fld_keysize_ = 0;
	fld_recsize_ = 0;
	fld_ptrsize_ = 0;
	fld_indirect_levels_ = 0;
	fld_root_buffer_.clear();
	fld_block_buffer_.clear();

	if (!fs_ || block_size_ == 0) {
		return false;
	}
	if (!LookupName(EXT2_ROOT_INO, FLD_INDEX_NAME, fld_ino_)) {
		return false;
	}
	if (ext2fs_file_open(fs_, fld_ino_, 0, &fld_file_) != 0) {
		fld_file_ = nullptr;
		return false;
	}
	if (!ReadIndexedFileBlock(fld_file_, block_size_, 0, fld_root_buffer_)) {
		ext2fs_file_close(fld_file_);
		fld_file_ = nullptr;
		return false;
	}

	const auto *root = reinterpret_cast<const IAMLfixRoot *>(fld_root_buffer_.data());
	if (ReadLE64(reinterpret_cast<const uint8_t *>(&root->magic)) != IAM_LFIX_ROOT_MAGIC) {
		ext2fs_file_close(fld_file_);
		fld_file_ = nullptr;
		fld_root_buffer_.clear();
		return false;
	}

	fld_keysize_ = ReadLE16(reinterpret_cast<const uint8_t *>(&root->keysize));
	fld_recsize_ = ReadLE16(reinterpret_cast<const uint8_t *>(&root->recsize));
	fld_ptrsize_ = ReadLE16(reinterpret_cast<const uint8_t *>(&root->ptrsize));
	fld_indirect_levels_ = root->indirect_levels;
	if (fld_keysize_ != sizeof(uint64_t) || fld_recsize_ != 24 || fld_ptrsize_ != sizeof(uint32_t)) {
		ext2fs_file_close(fld_file_);
		fld_file_ = nullptr;
		fld_root_buffer_.clear();
		fld_keysize_ = 0;
		fld_recsize_ = 0;
		fld_ptrsize_ = 0;
		fld_indirect_levels_ = 0;
		return false;
	}

	fld_available_ = true;
	return true;
}

void MDTScanner::CloseFLD() {
	if (fld_file_) {
		ext2fs_file_close(fld_file_);
		fld_file_ = nullptr;
	}

	fld_ino_ = 0;
	fld_initialized_ = false;
	fld_available_ = false;
	fld_keysize_ = 0;
	fld_recsize_ = 0;
	fld_ptrsize_ = 0;
	fld_indirect_levels_ = 0;
	fld_root_buffer_.clear();
	fld_block_buffer_.clear();
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
	total_inodes_ = fs_->super ? fs_->super->s_inodes_count : 0;
	block_group_count_ = fs_->group_desc_count;
	inodes_per_group_ = fs_->super ? fs_->super->s_inodes_per_group : 0;
	block_size_ = fs_->blocksize;
	inode_size_ = fs_->super ? EXT2_INODE_SIZE(fs_->super) : 0;
	buffered_inode_ = 0;
	inode_buffer_.clear();
	xattr_block_buffer_.clear();
	xattr_value_buffer_.clear();
	buffered_xattr_block_loaded_ = false;
	buffered_xattr_block_valid_ = false;
	CloseFLD();

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
	buffered_inode_ = 0;
	inode_buffer_.clear();
	xattr_block_buffer_.clear();
	buffered_xattr_block_loaded_ = false;
	buffered_xattr_block_valid_ = false;
}

void MDTScanner::Close() {
	CloseScan();
	CloseFLD();
	CloseOI();

	if (fs_) {
		ext2fs_close(fs_);
		fs_ = nullptr;
	}
	total_inodes_ = 0;
	block_group_count_ = 0;
	inodes_per_group_ = 0;
	block_size_ = 0;
	inode_size_ = 0;
}

//===----------------------------------------------------------------------===//
// Filesystem Info
//===----------------------------------------------------------------------===//

uint64_t MDTScanner::GetTotalInodes() const {
	return total_inodes_;
}

uint64_t MDTScanner::GetUsedInodes() const {
	if (!fs_ || !fs_->super) return 0;
	return fs_->super->s_inodes_count - fs_->super->s_free_inodes_count;
}

uint32_t MDTScanner::GetBlockGroupCount() const {
	return block_group_count_;
}

uint32_t MDTScanner::GetInodesPerGroup() const {
	return inodes_per_group_;
}

uint32_t MDTScanner::GetBlockSize() const {
	return block_size_;
}

uint32_t MDTScanner::GetInodeSize() const {
	return inode_size_;
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

void MDTScanner::EnsureInodeBuffer() {
	auto inode_size = static_cast<size_t>(inode_size_);
	if (inode_size < sizeof(struct ext2_inode_large)) {
		inode_size = sizeof(struct ext2_inode_large);
	}
	if (inode_buffer_.size() != inode_size) {
		inode_buffer_.resize(inode_size);
	}
}

void MDTScanner::EnsureXattrBlockBuffer() {
	auto block_size = static_cast<size_t>(block_size_);
	if (block_size == 0) {
		return;
	}
	if (xattr_block_buffer_.size() != block_size) {
		xattr_block_buffer_.resize(block_size);
	}
}

void MDTScanner::InvalidateBufferedXattrState() {
	buffered_xattr_block_loaded_ = false;
	buffered_xattr_block_valid_ = false;
}

bool MDTScanner::LoadBufferedExternalXattrBlock() {
	buffered_xattr_block_loaded_ = true;
	buffered_xattr_block_valid_ = false;

	if (!fs_ || inode_buffer_.empty()) {
		return false;
	}

	auto *inode = GetBufferedInode(inode_buffer_);
	auto blk = ext2fs_file_acl_block(fs_, EXT2_INODE(inode));
	if (blk == 0) {
		return false;
	}
	if (blk < fs_->super->s_first_data_block || blk >= ext2fs_blocks_count(fs_->super)) {
		return false;
	}

	EnsureXattrBlockBuffer();
	errcode_t err = ext2fs_read_ext_attr3(fs_, blk, xattr_block_buffer_.data(), buffered_inode_);
	if (err) {
		return false;
	}

	auto *header = reinterpret_cast<struct ext2_ext_attr_header *>(xattr_block_buffer_.data());
	if (header->h_magic != EXT2_EXT_ATTR_MAGIC) {
		return false;
	}

	buffered_xattr_block_valid_ = true;
	return true;
}

bool MDTScanner::GetNextRawInode(ext2_ino_t &ino) {
	if (!scan_) {
		throw IOException("Inode scan not started for '%s'", device_path_);
	}

	EnsureInodeBuffer();
	auto inode_size = static_cast<int>(inode_size_);

	while (true) {
		errcode_t err = ext2fs_get_next_inode_full(scan_, &ino,
			reinterpret_cast<struct ext2_inode *>(inode_buffer_.data()), inode_size);
		if (err) {
			throw IOException("Failed to get next inode for '%s': %s", device_path_, error_message(err));
		}

		// ino == 0 means scan is complete
		if (ino == 0) {
			return false;
		}

		auto *raw = GetBufferedInode(inode_buffer_);
		buffered_inode_ = ino;
		InvalidateBufferedXattrState();
		scanned_inodes_++;

		// Skip unused inodes (all zeros)
		if (raw->i_mode == 0 && raw->i_links_count == 0) {
			continue;
		}

		// Skip deleted inodes
		if (raw->i_dtime != 0) {
			continue;
		}

		return true;
	}
}

bool MDTScanner::GetNextRawInode(ext2_ino_t &ino, ext2_ino_t max_ino) {
	if (!scan_) {
		throw IOException("Inode scan not started for '%s'", device_path_);
	}

	if (skip_current_group_) {
		skip_current_group_ = false;
		return false;
	}

	EnsureInodeBuffer();
	auto inode_size = static_cast<int>(inode_size_);

	while (true) {
		errcode_t err = ext2fs_get_next_inode_full(scan_, &ino,
			reinterpret_cast<struct ext2_inode *>(inode_buffer_.data()), inode_size);
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

		auto *raw = GetBufferedInode(inode_buffer_);
		buffered_inode_ = ino;
		InvalidateBufferedXattrState();
		scanned_inodes_++;

		// Skip unused inodes (all zeros)
		if (raw->i_mode == 0 && raw->i_links_count == 0) {
			continue;
		}

		// Skip deleted inodes
		if (raw->i_dtime != 0) {
			continue;
		}

		return true;
	}
}

bool MDTScanner::ReadRawInode(ext2_ino_t ino) {
	if (!fs_) {
		throw IOException("Filesystem not open for '%s'", device_path_);
	}

	EnsureInodeBuffer();
	errcode_t err = ext2fs_read_inode_full(fs_, ino,
		reinterpret_cast<struct ext2_inode *>(inode_buffer_.data()), static_cast<int>(inode_size_));
	if (err) {
		return false;
	}

	auto *raw = GetBufferedInode(inode_buffer_);
	buffered_inode_ = ino;
	InvalidateBufferedXattrState();

	if (raw->i_mode == 0 && raw->i_links_count == 0) {
		return false;
	}
	if (raw->i_dtime != 0) {
		return false;
	}
	return true;
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

bool MDTScanner::FindBufferedXattrValue(uint8_t name_index, const char *short_name, size_t short_name_len,
                                        const uint8_t *&value_ptr, size_t &value_len, ext2_ino_t &value_inum) {
	value_ptr = nullptr;
	value_len = 0;
	value_inum = 0;

	if (!fs_ || inode_buffer_.empty()) {
		return false;
	}

	auto *inode = GetBufferedInode(inode_buffer_);
	XattrValueRef result;

	if (inode->i_extra_isize >= sizeof(inode->i_extra_isize) &&
	    inode_size_ > EXT2_GOOD_OLD_INODE_SIZE + inode->i_extra_isize + sizeof(__u32) &&
	    (inode->i_extra_isize & 3) == 0) {
		__u32 magic = 0;
		auto *ibody_start = inode_buffer_.data() + EXT2_GOOD_OLD_INODE_SIZE + inode->i_extra_isize;
		memcpy(&magic, ibody_start, sizeof(magic));
		if (magic == EXT2_EXT_ATTR_MAGIC) {
			auto storage_size = static_cast<unsigned int>(inode_size_ - EXT2_GOOD_OLD_INODE_SIZE -
			                                             inode->i_extra_isize - sizeof(__u32));
			auto *entries = reinterpret_cast<struct ext2_ext_attr_entry *>(ibody_start + sizeof(__u32));
			if (FindXattrEntryInRegion(name_index, short_name, short_name_len,
			                           entries, storage_size, reinterpret_cast<char *>(entries),
			                           storage_size, result)) {
				value_ptr = result.value_ptr;
				value_len = result.value_len;
				value_inum = result.value_inum;
				return result.found;
			}
		}
	}

	if (!buffered_xattr_block_loaded_ && !LoadBufferedExternalXattrBlock()) {
		return false;
	}
	if (!buffered_xattr_block_valid_) {
		return false;
	}

	auto *entries = reinterpret_cast<struct ext2_ext_attr_entry *>(xattr_block_buffer_.data() +
	                                                               sizeof(struct ext2_ext_attr_header));
	auto storage_size = static_cast<unsigned int>(block_size_ - sizeof(struct ext2_ext_attr_header));
	if (FindXattrEntryInRegion(name_index, short_name, short_name_len,
	                           entries, storage_size, xattr_block_buffer_.data(),
	                           xattr_block_buffer_.size(), result)) {
		value_ptr = result.value_ptr;
		value_len = result.value_len;
		value_inum = result.value_inum;
		return result.found;
	}

	return false;
}

bool MDTScanner::ReadExternalXattrPrefix(ext2_ino_t value_ino, void *buf, unsigned int wanted, unsigned int &got) {
	got = 0;
	if (!fs_) {
		return false;
	}

	ext2_file_t ea_file;
	errcode_t err = ext2fs_file_open(fs_, value_ino, 0, &ea_file);
	if (err) {
		return false;
	}

	auto *ea_inode = ext2fs_file_get_inode(ea_file);
	bool ok = false;
	if (!(ea_inode->i_flags & EXT4_INLINE_DATA_FL) &&
	    (ea_inode->i_flags & EXT4_EA_INODE_FL) &&
	    ea_inode->i_links_count != 0 &&
	    static_cast<__u64>(ext2fs_file_get_size(ea_file)) >= wanted &&
	    ext2fs_file_read(ea_file, buf, wanted, &got) == 0 &&
	    got == wanted) {
		ok = true;
	}

	ext2fs_file_close(ea_file);
	return ok;
}

bool MDTScanner::ReadExternalXattrValue(ext2_ino_t value_ino, const uint8_t *&value_ptr, size_t &value_len) {
	value_ptr = nullptr;
	value_len = 0;
	if (!fs_) {
		return false;
	}

	ext2_file_t ea_file;
	errcode_t err = ext2fs_file_open(fs_, value_ino, 0, &ea_file);
	if (err) {
		return false;
	}

	auto *ea_inode = ext2fs_file_get_inode(ea_file);
	auto file_size = static_cast<size_t>(ext2fs_file_get_size(ea_file));
	bool ok = false;
	if (!(ea_inode->i_flags & EXT4_INLINE_DATA_FL) &&
	    (ea_inode->i_flags & EXT4_EA_INODE_FL) &&
	    ea_inode->i_links_count != 0 &&
	    file_size != 0 &&
	    file_size <= std::numeric_limits<unsigned int>::max()) {
		xattr_value_buffer_.resize(file_size);
		unsigned int got = 0;
		if (ext2fs_file_read(ea_file, xattr_value_buffer_.data(), static_cast<unsigned int>(file_size), &got) == 0 &&
		    got == file_size) {
			value_ptr = reinterpret_cast<const uint8_t *>(xattr_value_buffer_.data());
			value_len = file_size;
			ok = true;
		}
	}

	ext2fs_file_close(ea_file);
	return ok;
}

bool MDTScanner::ReadBufferedXattrPrefix(uint8_t name_index, const char *short_name, size_t short_name_len,
                                         void *scratch, unsigned int wanted, const uint8_t *&value_ptr,
                                         size_t &value_len) {
	ext2_ino_t value_inum = 0;
	if (!FindBufferedXattrValue(name_index, short_name, short_name_len, value_ptr, value_len, value_inum)) {
		return false;
	}

	if (value_len < wanted) {
		return false;
	}

	if (value_inum == 0) {
		return true;
	}

	unsigned int got = 0;
	if (!ReadExternalXattrPrefix(value_inum, scratch, wanted, got)) {
		return false;
	}

	value_ptr = static_cast<const uint8_t *>(scratch);
	value_len = got;
	return true;
}

bool MDTScanner::ReadBufferedXattrValue(uint8_t name_index, const char *short_name, size_t short_name_len,
                                        const uint8_t *&value_ptr, size_t &value_len) {
	ext2_ino_t value_inum = 0;
	if (!FindBufferedXattrValue(name_index, short_name, short_name_len, value_ptr, value_len, value_inum)) {
		return false;
	}

	if (value_inum == 0) {
		return value_ptr != nullptr && value_len != 0;
	}

	return ReadExternalXattrValue(value_inum, value_ptr, value_len);
}

bool MDTScanner::ParseBufferedFID(LustreFID &fid) {
	fid = LustreFID();

	uint8_t scratch[sizeof(LustreLMA)];
	const uint8_t *value_ptr = nullptr;
	size_t value_len = 0;
	if (!ReadBufferedXattrPrefix(TRUSTED_XATTR_INDEX, "lma", 3, scratch, sizeof(scratch), value_ptr, value_len)) {
		return false;
	}
	if (!value_ptr || value_len < LMA_FID_OFFSET + sizeof(fid.f_seq) + sizeof(fid.f_oid) + sizeof(fid.f_ver)) {
		return false;
	}

	memcpy(&fid.f_seq, value_ptr + LMA_FID_OFFSET, sizeof(fid.f_seq));
	memcpy(&fid.f_oid, value_ptr + LMA_FID_OFFSET + 8, sizeof(fid.f_oid));
	memcpy(&fid.f_ver, value_ptr + LMA_FID_OFFSET + 12, sizeof(fid.f_ver));
	return fid.IsValid();
}

bool MDTScanner::ParseBufferedLMAIncompat(uint32_t &incompat) {
	incompat = 0;
	uint8_t scratch[sizeof(LustreLMA)];
	const uint8_t *value_ptr = nullptr;
	size_t value_len = 0;
	if (!ReadBufferedXattrPrefix(TRUSTED_XATTR_INDEX, "lma", 3, scratch, sizeof(scratch), value_ptr, value_len)) {
		return false;
	}
	if (!value_ptr || value_len < 8) {
		return false;
	}
	memcpy(&incompat, value_ptr + 4, sizeof(uint32_t));
	return true;
}

bool MDTScanner::ParseBufferedSOM(uint64_t &size, uint64_t &blocks) {
	uint8_t scratch[sizeof(LustreSOM)];
	const uint8_t *value_ptr = nullptr;
	size_t value_len = 0;
	if (!ReadBufferedXattrPrefix(TRUSTED_XATTR_INDEX, "som", 3, scratch, sizeof(scratch), value_ptr, value_len)) {
		return false;
	}
	if (!value_ptr || value_len < sizeof(LustreSOM)) {
		return false;
	}

	uint16_t valid;
	memcpy(&valid, value_ptr, sizeof(valid));
	if (valid == 0) {
		return false;
	}

	memcpy(&size, value_ptr + 8, sizeof(size));
	memcpy(&blocks, value_ptr + 16, sizeof(blocks));
	return true;
}

bool MDTScanner::HasBufferedLinkEA() {
	uint8_t scratch[sizeof(LinkEAHeader)];
	const uint8_t *value_ptr = nullptr;
	size_t value_len = 0;
	if (!ReadBufferedXattrPrefix(TRUSTED_XATTR_INDEX, "link", 4, scratch, sizeof(scratch), value_ptr, value_len)) {
		return false;
	}
	if (!value_ptr || value_len < sizeof(LinkEAHeader)) {
		return false;
	}

	uint32_t magic;
	uint32_t reccount;
	memcpy(&magic, value_ptr, sizeof(magic));
	memcpy(&reccount, value_ptr + 4, sizeof(reccount));
	return magic == LINK_EA_MAGIC && reccount != 0;
}

bool MDTScanner::PassesXattrSkipChecksFast(ext2_ino_t ino, const MDTScanConfig &config) {
	if (!config.skip_no_fid && !config.skip_no_linkea) {
		return true;
	}

	if (buffered_inode_ != ino) {
		if (!ReadRawInode(ino)) {
			return false;
		}
	}

	if (config.skip_no_fid) {
		LustreFID fid;
		if (!ParseBufferedFID(fid)) {
			return false;
		}
	}

	if (config.skip_no_linkea && !HasBufferedLinkEA()) {
		return false;
	}

	return true;
}

//===----------------------------------------------------------------------===//
// GetNextInode - full metadata scan (for lustre_inodes)
//===----------------------------------------------------------------------===//

bool MDTScanner::GetNextInode(LustreInode &out, const MDTScanConfig &config) {
	ext2_ino_t ino;

	while (true) {
		if (!GetNextRawInode(ino)) {
			return false;
		}
		const auto &inode = *GetBufferedInode(inode_buffer_);

		PopulateInodeMetadata(out, ino, inode);

		if (!config.read_xattrs) {
			if (!PassesXattrSkipChecksFast(ino, config)) {
				continue;
			}
			valid_inodes_++;
			return true;
		}

		bool has_linkea = HasBufferedLinkEA();
		ParseBufferedFID(out.fid);
		uint64_t som_size, som_blocks;
		if (ParseBufferedSOM(som_size, som_blocks)) {
			out.size = som_size;
			out.blocks = som_blocks;
		}

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

	while (true) {
		if (!GetNextRawInode(ino, max_ino)) {
			return false;
		}
		const auto &inode = *GetBufferedInode(inode_buffer_);

		PopulateInodeMetadata(out, ino, inode);

		if (!config.read_xattrs) {
			if (!PassesXattrSkipChecksFast(ino, config)) {
				continue;
			}
			valid_inodes_++;
			return true;
		}

		bool has_linkea = HasBufferedLinkEA();
		ParseBufferedFID(out.fid);
		uint64_t som_size, som_blocks;
		if (ParseBufferedSOM(som_size, som_blocks)) {
			out.size = som_size;
			out.blocks = som_blocks;
		}

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

	while (true) {
		if (!GetNextRawInode(ino)) {
			return false;
		}

		LustreFID fid;
		std::vector<LinkEntry> links;
		ParseBufferedFID(fid);
		bool has_linkea = ParseBufferedLinkEA(links);

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

	while (true) {
		if (!GetNextRawInode(ino, max_ino)) {
			return false;
		}

		LustreFID fid;
		std::vector<LinkEntry> links;
		ParseBufferedFID(fid);
		bool has_linkea = ParseBufferedLinkEA(links);

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

bool MDTScanner::GetMDTIndex(uint32_t &mdt_index_out) {
	if (!fs_ || !fs_->super) {
		return false;
	}

	return ParseMDTIndexLabel(reinterpret_cast<const char *>(fs_->super->s_volume_name), mdt_index_out);
}

bool MDTScanner::LookupFIDHomeMDTIndex(const LustreFID &fid, uint32_t &mdt_index_out) {
	if (!EnsureFLDReady()) {
		return false;
	}

	bool found = false;
	uint8_t key[sizeof(uint64_t)];
	WriteBE64(key, fid.f_seq);

	do {
		const auto *entries = fld_root_buffer_.data() + sizeof(IAMLfixRoot);
		const auto *root_countlimit = reinterpret_cast<const IAMDxCountLimit *>(entries);
		const auto root_count = ReadLE16(reinterpret_cast<const uint8_t *>(&root_countlimit->count));
		const auto *entry = FindIAMIndexEntry(entries, root_count, fld_keysize_, fld_ptrsize_, key);
		if (!entry) {
			break;
		}

		auto block = ReadIAMIndexPointer(entry, fld_keysize_, fld_ptrsize_);
		for (uint8_t level = 0; level < fld_indirect_levels_; level++) {
			if (!ReadIndexedFileBlock(fld_file_, block_size_, block, fld_block_buffer_)) {
				block = 0;
				break;
			}

			const auto *node_entries = fld_block_buffer_.data();
			const auto *countlimit = reinterpret_cast<const IAMDxCountLimit *>(node_entries);
			const auto count = ReadLE16(reinterpret_cast<const uint8_t *>(&countlimit->count));
			entry = FindIAMIndexEntry(node_entries, count, fld_keysize_, fld_ptrsize_, key);
			if (!entry) {
				block = 0;
				break;
			}
			block = ReadIAMIndexPointer(entry, fld_keysize_, fld_ptrsize_);
		}
		if (block == 0) {
			break;
		}

		if (!ReadIndexedFileBlock(fld_file_, block_size_, block, fld_block_buffer_)) {
			break;
		}

		const auto *leaf = reinterpret_cast<const IAMLeafHead *>(fld_block_buffer_.data());
		if (ReadLE16(reinterpret_cast<const uint8_t *>(&leaf->magic)) != IAM_LEAF_HEADER_MAGIC) {
			break;
		}

		const auto leaf_count = ReadLE16(reinterpret_cast<const uint8_t *>(&leaf->count));
		if (leaf_count == 0) {
			break;
		}

		const auto entry_size = static_cast<idx_t>(fld_keysize_ + fld_recsize_);
		const auto *leaf_entries = fld_block_buffer_.data() + sizeof(IAMLeafHead);

		idx_t low = 0;
		idx_t high = leaf_count;
		while (low < high) {
			const auto mid = low + (high - low) / 2;
			const auto *mid_entry = leaf_entries + mid * entry_size;
			if (memcmp(mid_entry, key, fld_keysize_) <= 0) {
				low = mid + 1;
			} else {
				high = mid;
			}
		}
		if (low == 0) {
			break;
		}

		const auto *leaf_entry = leaf_entries + (low - 1) * entry_size;
		const auto *record = leaf_entry + fld_keysize_;
		const auto range_start = ReadBE64(record);
		const auto range_end = ReadBE64(record + sizeof(uint64_t));
		if (range_start >= range_end || fid.f_seq < range_start || fid.f_seq >= range_end) {
			break;
		}

		mdt_index_out = ReadBE32(record + 2 * sizeof(uint64_t));
		found = true;
	} while (false);

	return found;
}

bool MDTScanner::LoadFLDRanges(std::vector<FLDRangeEntry> &ranges_out) {
	ranges_out.clear();
	if (!EnsureFLDReady()) {
		return false;
	}

	const auto *entries = fld_root_buffer_.data() + sizeof(IAMLfixRoot);
	const auto *root_countlimit = reinterpret_cast<const IAMDxCountLimit *>(entries);
	const auto root_count = ReadLE16(reinterpret_cast<const uint8_t *>(&root_countlimit->count));
	const auto entry_size = static_cast<idx_t>(fld_keysize_ + fld_ptrsize_);

	for (idx_t i = 1; i < root_count; i++) {
		const auto *entry = entries + i * entry_size;
		const auto block = ReadIAMIndexPointer(entry, fld_keysize_, fld_ptrsize_);
		if (block == 0) {
			continue;
		}
		if (!CollectFLDRangesFromBlock(fld_file_, block_size_, fld_keysize_, fld_recsize_, fld_ptrsize_,
		                               fld_indirect_levels_, block, fld_block_buffer_, ranges_out)) {
			ranges_out.clear();
			return false;
		}
	}

	std::sort(ranges_out.begin(), ranges_out.end(), [](const FLDRangeEntry &lhs, const FLDRangeEntry &rhs) {
		if (lhs.seq_start != rhs.seq_start) {
			return lhs.seq_start < rhs.seq_start;
		}
		if (lhs.seq_end != rhs.seq_end) {
			return lhs.seq_end < rhs.seq_end;
		}
		return lhs.mdt_index < rhs.mdt_index;
	});
	return !ranges_out.empty();
}

bool MDTScanner::ReadInode(ext2_ino_t ino, LustreInode &out, const MDTScanConfig &config) {
	if (!fs_) {
		throw IOException("Filesystem not open for '%s'", device_path_);
	}

	// Read the raw inode
	if (!ReadRawInode(ino)) {
		return false;
	}
	const auto &inode = *GetBufferedInode(inode_buffer_);

	// Convert to LustreInode (same logic as GetNextInode)
	PopulateInodeMetadata(out, ino, inode);

	if (!config.read_xattrs) {
		return PassesXattrSkipChecksFast(ino, config);
	}

	bool has_linkea = HasBufferedLinkEA();
	ParseBufferedFID(out.fid);
	uint64_t som_size, som_blocks;
	if (ParseBufferedSOM(som_size, som_blocks)) {
		out.size = som_size;
		out.blocks = som_blocks;
	}

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

	if (!ReadRawInode(ino)) {
		return false;
	}
	const auto &inode = *GetBufferedInode(inode_buffer_);

	PopulateInodeMetadata(inode_out, ino, inode);

	ParseBufferedFID(inode_out.fid);
	ParseBufferedLinkEA(links_out);

	uint64_t som_size = 0;
	uint64_t som_blocks = 0;
	if (ParseBufferedSOM(som_size, som_blocks)) {
		inode_out.size = som_size;
		inode_out.blocks = som_blocks;
	}

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

	if (!ReadRawInode(ino)) {
		return false;
	}

	fid_out = LustreFID();
	ParseBufferedFID(fid_out);
	ParseBufferedLinkEA(links_out);
	ParseBufferedLOVDetailed(&components_out, nullptr);

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

	if (!ReadRawInode(ino)) {
		return false;
	}

	fid_out = LustreFID();
	ParseBufferedFID(fid_out);
	ParseBufferedLinkEA(links_out);
	ParseBufferedLOVDetailed(nullptr, &objects_out);

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

	while (true) {
		if (!GetNextRawInode(ino)) {
			return false;
		}
		const auto &inode = *GetBufferedInode(inode_buffer_);

		LustreInode inode_row;
		PopulateInodeMetadata(inode_row, ino, inode);

		std::vector<LinkEntry> links;
		ParseBufferedFID(inode_row.fid);
		ParseBufferedLinkEA(links);

		uint64_t som_size = 0;
		uint64_t som_blocks = 0;
		if (ParseBufferedSOM(som_size, som_blocks)) {
			inode_row.size = som_size;
			inode_row.blocks = som_blocks;
		}

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

	while (true) {
		if (!GetNextRawInode(ino, max_ino)) {
			return false;
		}
		const auto &inode = *GetBufferedInode(inode_buffer_);

		LustreInode inode_row;
		PopulateInodeMetadata(inode_row, ino, inode);

		std::vector<LinkEntry> links;
		ParseBufferedFID(inode_row.fid);
		ParseBufferedLinkEA(links);

		uint64_t som_size = 0;
		uint64_t som_blocks = 0;
		if (ParseBufferedSOM(som_size, som_blocks)) {
			inode_row.size = som_size;
			inode_row.blocks = som_blocks;
		}

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

	if (!ReadRawInode(ino)) {
		return false;
	}

	return ParseBufferedFID(fid_out) && ParseBufferedLinkEA(links_out);
}

bool MDTScanner::ReadInodeLayouts(ext2_ino_t ino, LustreFID &fid_out,
                                  std::vector<LustreLayoutComponent> &components) {
	components.clear();

	if (!ReadRawInode(ino)) {
		return false;
	}

	bool has_fid = ParseBufferedFID(fid_out);
	ParseBufferedLOVDetailed(&components, nullptr);
	return has_fid;
}

bool MDTScanner::ReadInodeLayouts(ext2_ino_t ino, LustreInode &inode_out,
                                  std::vector<LustreLayoutComponent> &components, const MDTScanConfig &config) {
	if (!fs_) {
		throw IOException("Filesystem not open for '%s'", device_path_);
	}

	components.clear();

	if (!ReadRawInode(ino)) {
		return false;
	}
	const auto &inode = *GetBufferedInode(inode_buffer_);

	PopulateInodeMetadata(inode_out, ino, inode);

	ParseBufferedFID(inode_out.fid);
	bool has_linkea = HasBufferedLinkEA();
	ParseBufferedLOVDetailed(&components, nullptr);

	uint64_t som_size = 0;
	uint64_t som_blocks = 0;
	if (ParseBufferedSOM(som_size, som_blocks)) {
		inode_out.size = som_size;
		inode_out.blocks = som_blocks;
	}

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

	if (!ReadRawInode(ino)) {
		return false;
	}

	fid_out = LustreFID();
	ParseBufferedFID(fid_out);
	bool has_linkea = HasBufferedLinkEA();
	ParseBufferedLOVDetailed(&components, &objects);

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

	while (true) {
		if (!GetNextRawInode(ino)) {
			return false;
		}

		fid_out = LustreFID();
		components.clear();
		ParseBufferedFID(fid_out);
		ParseBufferedLOVDetailed(&components, nullptr);

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

	while (true) {
		if (!GetNextRawInode(ino)) {
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

	while (true) {
		if (!GetNextRawInode(ino)) {
			return false;
		}
		const auto &inode = *GetBufferedInode(inode_buffer_);

		LustreInode inode_row;
		PopulateInodeMetadata(inode_row, ino, inode);

		std::vector<LustreLayoutComponent> components;
		ParseBufferedFID(inode_row.fid);
		bool has_linkea = HasBufferedLinkEA();
		ParseBufferedLOVDetailed(&components, nullptr);

		uint64_t som_size = 0;
		uint64_t som_blocks = 0;
		if (ParseBufferedSOM(som_size, som_blocks)) {
			inode_row.size = som_size;
			inode_row.blocks = som_blocks;
		}

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

	while (true) {
		if (!GetNextRawInode(ino, max_ino)) {
			return false;
		}
		const auto &inode = *GetBufferedInode(inode_buffer_);

		LustreInode inode_row;
		PopulateInodeMetadata(inode_row, ino, inode);

		std::vector<LustreLayoutComponent> components;
		ParseBufferedFID(inode_row.fid);
		bool has_linkea = HasBufferedLinkEA();
		ParseBufferedLOVDetailed(&components, nullptr);

		uint64_t som_size = 0;
		uint64_t som_blocks = 0;
		if (ParseBufferedSOM(som_size, som_blocks)) {
			inode_row.size = som_size;
			inode_row.blocks = som_blocks;
		}

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

	while (true) {
		if (!GetNextRawInode(ino, max_ino)) {
			return false;
		}

		fid_out = LustreFID();
		components.clear();
		ParseBufferedFID(fid_out);
		ParseBufferedLOVDetailed(&components, nullptr);

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

	while (true) {
		if (!GetNextRawInode(ino, max_ino)) {
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

	while (true) {
		if (!GetNextRawInode(ino)) {
			return false;
		}

		fid_out = LustreFID();
		components.clear();
		objects.clear();
		ParseBufferedFID(fid_out);
		bool has_linkea = HasBufferedLinkEA();
		ParseBufferedLOVDetailed(&components, &objects);

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

	while (true) {
		if (!GetNextRawInode(ino, max_ino)) {
			return false;
		}

		fid_out = LustreFID();
		components.clear();
		objects.clear();
		ParseBufferedFID(fid_out);
		bool has_linkea = HasBufferedLinkEA();
		ParseBufferedLOVDetailed(&components, &objects);

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

	if (!ReadRawInode(ino)) {
		return false;
	}

	bool has_fid = ParseBufferedFID(fid_out);
	ParseBufferedLOVDetailed(nullptr, &objects);
	return has_fid;
}

bool MDTScanner::ReadInodeObjects(ext2_ino_t ino, LustreInode &inode_out,
                                  std::vector<LustreOSTObject> &objects, const MDTScanConfig &config) {
	if (!fs_) {
		throw IOException("Filesystem not open for '%s'", device_path_);
	}

	objects.clear();

	if (!ReadRawInode(ino)) {
		return false;
	}
	const auto &inode = *GetBufferedInode(inode_buffer_);

	PopulateInodeMetadata(inode_out, ino, inode);

	ParseBufferedFID(inode_out.fid);
	bool has_linkea = HasBufferedLinkEA();
	ParseBufferedLOVDetailed(nullptr, &objects);

	uint64_t som_size = 0;
	uint64_t som_blocks = 0;
	if (ParseBufferedSOM(som_size, som_blocks)) {
		inode_out.size = som_size;
		inode_out.blocks = som_blocks;
	}

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

	while (true) {
		if (!GetNextRawInode(ino)) {
			return false;
		}

		fid_out = LustreFID();
		objects.clear();
		ParseBufferedFID(fid_out);
		ParseBufferedLOVDetailed(nullptr, &objects);

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

	while (true) {
		if (!GetNextRawInode(ino)) {
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

	while (true) {
		if (!GetNextRawInode(ino)) {
			return false;
		}
		const auto &inode = *GetBufferedInode(inode_buffer_);

		LustreInode inode_row;
		PopulateInodeMetadata(inode_row, ino, inode);

		std::vector<LustreOSTObject> objects;
		ParseBufferedFID(inode_row.fid);
		bool has_linkea = HasBufferedLinkEA();
		ParseBufferedLOVDetailed(nullptr, &objects);

		uint64_t som_size = 0;
		uint64_t som_blocks = 0;
		if (ParseBufferedSOM(som_size, som_blocks)) {
			inode_row.size = som_size;
			inode_row.blocks = som_blocks;
		}

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

	while (true) {
		if (!GetNextRawInode(ino, max_ino)) {
			return false;
		}
		const auto &inode = *GetBufferedInode(inode_buffer_);

		LustreInode inode_row;
		PopulateInodeMetadata(inode_row, ino, inode);

		std::vector<LustreOSTObject> objects;
		ParseBufferedFID(inode_row.fid);
		bool has_linkea = HasBufferedLinkEA();
		ParseBufferedLOVDetailed(nullptr, &objects);

		uint64_t som_size = 0;
		uint64_t som_blocks = 0;
		if (ParseBufferedSOM(som_size, som_blocks)) {
			inode_row.size = som_size;
			inode_row.blocks = som_blocks;
		}

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

	while (true) {
		if (!GetNextRawInode(ino, max_ino)) {
			return false;
		}

		fid_out = LustreFID();
		objects.clear();
		ParseBufferedFID(fid_out);
		ParseBufferedLOVDetailed(nullptr, &objects);

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

	while (true) {
		if (!GetNextRawInode(ino, max_ino)) {
			return false;
		}
		if (ReadInodeLinkObjects(ino, fid_out, links_out, objects_out, config)) {
			return true;
		}
	}
}

bool MDTScanner::ReadInodeFID(ext2_ino_t ino, LustreFID &fid_out) {
	if (!ReadRawInode(ino)) {
		return false;
	}

	return ParseBufferedFID(fid_out);
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
// GetNextDirMapEntries - sequential scan for directory inodes with LMV/LinkEA
//===----------------------------------------------------------------------===//

bool MDTScanner::GetNextDirMapEntries(ext2_ino_t &ino_out, LustreFID &fid_out, LustreLMV &lmv_out,
                                      std::vector<LinkEntry> &links_out,
                                      std::vector<DirEntry> &dir_entries_out,
                                      uint32_t &lma_incompat_out,
                                      const MDTScanConfig &config) {
	ext2_ino_t ino;

	while (true) {
		if (!GetNextRawInode(ino)) {
			return false;
		}

		// Only process directory inodes
		auto *raw = reinterpret_cast<const struct ext2_inode *>(inode_buffer_.data());
		if ((raw->i_mode & 0xF000) != 0x4000) {
			continue;
		}

		fid_out = LustreFID();
		lmv_out = LustreLMV();
		links_out.clear();
		dir_entries_out.clear();
		lma_incompat_out = 0;

		ParseBufferedFID(fid_out);
		if (config.skip_no_fid && !fid_out.IsValid()) {
			continue;
		}

		// Read LMA incompat flags and skip agent inodes (DNE1 remote dir stubs)
		ParseBufferedLMAIncompat(lma_incompat_out);
		if (lma_incompat_out & LMAI_AGENT) {
			continue;
		}

		// Parse LMV (may not exist for plain directories)
		ParseBufferedLMV(lmv_out);

		// Parse LinkEA (needed for slave LMV to locate master)
		ParseBufferedLinkEA(links_out);

		// For master striped dirs, read directory entries to find shard sub-entries
		if (lmv_out.IsMaster()) {
			ReadDirectoryEntries(ino, dir_entries_out);
		}

		ino_out = ino;
		return true;
	}
}

bool MDTScanner::GetNextDirMapEntries(ext2_ino_t &ino_out, LustreFID &fid_out, LustreLMV &lmv_out,
                                      std::vector<LinkEntry> &links_out,
                                      std::vector<DirEntry> &dir_entries_out,
                                      uint32_t &lma_incompat_out,
                                      const MDTScanConfig &config, ext2_ino_t max_ino) {
	ext2_ino_t ino;

	while (true) {
		if (!GetNextRawInode(ino, max_ino)) {
			return false;
		}

		// Only process directory inodes
		auto *raw = reinterpret_cast<const struct ext2_inode *>(inode_buffer_.data());
		if ((raw->i_mode & 0xF000) != 0x4000) {
			continue;
		}

		fid_out = LustreFID();
		lmv_out = LustreLMV();
		links_out.clear();
		dir_entries_out.clear();
		lma_incompat_out = 0;

		ParseBufferedFID(fid_out);
		if (config.skip_no_fid && !fid_out.IsValid()) {
			continue;
		}

		// Read LMA incompat flags and skip agent inodes (DNE1 remote dir stubs)
		ParseBufferedLMAIncompat(lma_incompat_out);
		if (lma_incompat_out & LMAI_AGENT) {
			continue;
		}

		// Parse LMV (may not exist for plain directories)
		ParseBufferedLMV(lmv_out);

		// Parse LinkEA (needed for slave LMV to locate master)
		ParseBufferedLinkEA(links_out);

		// For master striped dirs, read directory entries to find shard sub-entries
		if (lmv_out.IsMaster()) {
			ReadDirectoryEntries(ino, dir_entries_out);
		}

		ino_out = ino;
		return true;
	}
}

//===----------------------------------------------------------------------===//
// Lustre Extended Attribute Parsing
//===----------------------------------------------------------------------===//

static bool ParseLinkEAValue(const uint8_t *data, size_t value_len, std::vector<LinkEntry> &links) {
	links.clear();
	if (!data || value_len < sizeof(LinkEAHeader)) {
		return false;
	}

	const uint8_t *end = data + value_len;

	// Parse LinkEA header
	LinkEAHeader header;
	memcpy(&header.leh_magic, data, sizeof(header.leh_magic));
	memcpy(&header.leh_reccount, data + 4, sizeof(header.leh_reccount));
	memcpy(&header.leh_len, data + 8, sizeof(header.leh_len));

	// Verify magic
	if (header.leh_magic != LINK_EA_MAGIC) {
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

	return !links.empty();
}

bool MDTScanner::ParseBufferedLinkEA(std::vector<LinkEntry> &links) {
	const uint8_t *value_ptr = nullptr;
	size_t value_len = 0;
	if (!ReadBufferedXattrValue(TRUSTED_XATTR_INDEX, "link", 4, value_ptr, value_len)) {
		links.clear();
		return false;
	}
	return ParseLinkEAValue(value_ptr, value_len, links);
}

//===----------------------------------------------------------------------===//
// ParseBufferedLMV - parse trusted.lmv xattr from buffered inode
//===----------------------------------------------------------------------===//

bool MDTScanner::ParseBufferedLMV(LustreLMV &lmv) {
	lmv = LustreLMV();

	const uint8_t *value_ptr = nullptr;
	size_t value_len = 0;
	if (!ReadBufferedXattrValue(TRUSTED_XATTR_INDEX, "lmv", 3, value_ptr, value_len)) {
		return false;
	}
	if (!value_ptr || value_len < LMV_HEADER_SIZE) {
		return false;
	}

	// Parse header fields (little-endian on disk)
	// Offset 0:  lmv_magic (u32)
	// Offset 4:  lmv_stripe_count (u32)
	// Offset 8:  lmv_master_mdt_index (u32) - master: MDT index, slave: stripe index
	// Offset 12: lmv_hash_type (u32)
	// Offset 16: lmv_layout_version (u32)
	// Offset 20: lmv_migrate_offset (u32)
	// Offset 24: lmv_migrate_hash (u32)
	// Offset 28: lmv_padding2 (u32)
	// Offset 32: lmv_padding3 (u64)
	// Offset 40: lmv_pool_name[16]
	memcpy(&lmv.lmv_magic,            value_ptr + 0,  sizeof(uint32_t));
	memcpy(&lmv.lmv_stripe_count,     value_ptr + 4,  sizeof(uint32_t));
	memcpy(&lmv.lmv_master_mdt_index, value_ptr + 8,  sizeof(uint32_t));
	memcpy(&lmv.lmv_hash_type,        value_ptr + 12, sizeof(uint32_t));
	memcpy(&lmv.lmv_layout_version,   value_ptr + 16, sizeof(uint32_t));
	memcpy(&lmv.lmv_migrate_offset,   value_ptr + 20, sizeof(uint32_t));
	memcpy(&lmv.lmv_migrate_hash,     value_ptr + 24, sizeof(uint32_t));

	const char *pool_ptr = reinterpret_cast<const char *>(value_ptr + 40);
	size_t pool_len = strnlen(pool_ptr, LMV_POOL_NAME_SIZE);
	if (pool_len > 0) {
		lmv.lmv_pool_name.assign(pool_ptr, pool_len);
	}

	if (!lmv.IsValid()) {
		lmv = LustreLMV();
		return false;
	}

	return true;
}

bool MDTScanner::ReadInodeLMAIncompat(ext2_ino_t ino, uint32_t &incompat_out) {
	if (!ReadRawInode(ino)) {
		return false;
	}
	return ParseBufferedLMAIncompat(incompat_out);
}

bool MDTScanner::ReadInodeLMV(ext2_ino_t ino, LustreLMV &lmv_out) {
	if (!ReadRawInode(ino)) {
		return false;
	}
	return ParseBufferedLMV(lmv_out);
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

static bool ParseLOVValue(const uint8_t *data, size_t value_len,
                         std::vector<LustreLayoutComponent> *components,
                         std::vector<LustreOSTObject> *objects) {
	if (!data) {
		return false;
	}

	if (value_len < 4) {
		return false;
	}

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
			return false;
		}

		uint16_t entry_count;
		memcpy(&entry_count, data + 14, sizeof(entry_count));

		// Validate that all entries fit within the header area
		size_t entries_end = COMP_V1_HEADER_SIZE + static_cast<size_t>(entry_count) * COMP_ENTRY_SIZE;
		if (entries_end > value_len) {
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

	return result;
}

bool MDTScanner::ParseBufferedLOVDetailed(std::vector<LustreLayoutComponent> *components,
                                          std::vector<LustreOSTObject> *objects) {
	const uint8_t *value_ptr = nullptr;
	size_t value_len = 0;
	if (!ReadBufferedXattrValue(TRUSTED_XATTR_INDEX, "lov", 3, value_ptr, value_len)) {
		if (components) {
			components->clear();
		}
		if (objects) {
			objects->clear();
		}
		return false;
	}
	return ParseLOVValue(value_ptr, value_len, components, objects);
}

} // namespace lustre
} // namespace duckdb
