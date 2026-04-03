//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_types.hpp
//
// Lustre filesystem type definitions
//===----------------------------------------------------------------------===//

#pragma once

#include <cinttypes>
#include <cstdint>
#include <string>
#include <vector>

namespace duckdb {
namespace lustre {

//===----------------------------------------------------------------------===//
// Lustre FID (File Identifier) - 128-bit unique identifier
//===----------------------------------------------------------------------===//
struct LustreFID {
	uint64_t f_seq;  // Sequence number
	uint32_t f_oid;  // Object ID
	uint32_t f_ver;  // Version

	LustreFID() : f_seq(0), f_oid(0), f_ver(0) {}
	LustreFID(uint64_t seq, uint32_t oid, uint32_t ver) : f_seq(seq), f_oid(oid), f_ver(ver) {}

	std::string ToString() const {
		char buf[64];
		snprintf(buf, sizeof(buf), "[0x%" PRIx64 ":0x%" PRIx32 ":0x%" PRIx32 "]",
		         f_seq, f_oid, f_ver);
		return std::string(buf);
	}

	bool IsValid() const {
		// FID with all zeros is invalid
		return f_seq != 0 || f_oid != 0;
	}

	bool operator==(const LustreFID &other) const {
		return f_seq == other.f_seq && f_oid == other.f_oid && f_ver == other.f_ver;
	}

	bool operator!=(const LustreFID &other) const {
		return !(*this == other);
	}

	bool operator<(const LustreFID &other) const {
		if (f_seq != other.f_seq) {
			return f_seq < other.f_seq;
		}
		if (f_oid != other.f_oid) {
			return f_oid < other.f_oid;
		}
		return f_ver < other.f_ver;
	}

	//! Parse FID from string format "[0xSEQ:0xOID:0xVER]"
	static bool FromString(const std::string &str, LustreFID &out) {
		out = LustreFID();
		if (str.size() < 5 || str.front() != '[' || str.back() != ']') {
			return false;
		}
		// Strip brackets
		std::string inner = str.substr(1, str.size() - 2);
		// Find the two colons
		auto pos1 = inner.find(':');
		if (pos1 == std::string::npos) {
			return false;
		}
		auto pos2 = inner.find(':', pos1 + 1);
		if (pos2 == std::string::npos) {
			return false;
		}
		std::string seq_str = inner.substr(0, pos1);
		std::string oid_str = inner.substr(pos1 + 1, pos2 - pos1 - 1);
		std::string ver_str = inner.substr(pos2 + 1);
		char *end = nullptr;
		out.f_seq = strtoull(seq_str.c_str(), &end, 0);
		if (end == seq_str.c_str()) {
			return false;
		}
		out.f_oid = static_cast<uint32_t>(strtoul(oid_str.c_str(), &end, 0));
		if (end == oid_str.c_str()) {
			return false;
		}
		out.f_ver = static_cast<uint32_t>(strtoul(ver_str.c_str(), &end, 0));
		if (end == ver_str.c_str()) {
			return false;
		}
		return true;
	}
};

//===----------------------------------------------------------------------===//
// Lustre Metadata Attributes (LMA) - stored in trusted.lma xattr
//===----------------------------------------------------------------------===//
struct LustreLMA {
	uint32_t lma_compat;      // Compatible flags
	uint32_t lma_incompat;    // Incompatible flags
	LustreFID lma_self_fid;   // Self FID
};

// LMA magic and flags
constexpr uint32_t LUSTRE_LMA_MAGIC = 0x0BD30BD0;

//===----------------------------------------------------------------------===//
// LOV (Lustre Object Volume) - stripe information
//===----------------------------------------------------------------------===//
struct LustreLOV {
	uint32_t lmm_magic;        // LOV_MAGIC_V1 or LOV_MAGIC_V3
	uint32_t lmm_pattern;      // Stripe pattern
	uint64_t lmm_oi_id;        // Object ID
	uint64_t lmm_oi_seq;       // Object sequence
	uint32_t lmm_stripe_size;  // Stripe size in bytes
	uint16_t lmm_stripe_count; // Number of stripes
	uint16_t lmm_layout_gen;   // Layout generation
	// Pool name follows in V3
	char lmm_pool_name[16];    // OST pool name (LOV_MAGIC_V3)
};

// LOV magic numbers
constexpr uint32_t LOV_MAGIC_V1 = 0x0BD10BD0;
constexpr uint32_t LOV_MAGIC_V3 = 0x0BD30BD0;
constexpr uint32_t LOV_MAGIC_COMP_V1 = 0x0BD60BD0;

// Composite layout entry flags
constexpr uint32_t LCME_FL_INIT = 0x00000010;

// Special extent value for end-of-file
constexpr uint64_t LUSTRE_EOF = 0xFFFFFFFFFFFFFFFFULL;

// Mirror ID encoding within lcme_id
constexpr uint16_t MIRROR_ID_SHIFT = 16;
constexpr uint32_t MIRROR_ID_MASK = 0x7FFF0000;

//===----------------------------------------------------------------------===//
// SOM (Size on MDS) - stored in trusted.som xattr
//===----------------------------------------------------------------------===//
struct LustreSOM {
	uint16_t lsa_valid;         // SOM flags
	uint16_t lsa_reserved[3];   // Reserved
	uint64_t lsa_size;          // File size
	uint64_t lsa_blocks;        // Block count
};

//===----------------------------------------------------------------------===//
// File type constants (from mode)
//===----------------------------------------------------------------------===//
enum class FileType : uint8_t {
	UNKNOWN = 0,
	REGULAR = 1,
	DIRECTORY = 2,
	SYMLINK = 3,
	BLOCK_DEV = 4,
	CHAR_DEV = 5,
	FIFO = 6,
	SOCKET = 7
};

inline FileType ModeToFileType(uint16_t mode) {
	switch (mode & 0xF000) {
	case 0x8000: return FileType::REGULAR;    // S_IFREG
	case 0x4000: return FileType::DIRECTORY;  // S_IFDIR
	case 0xA000: return FileType::SYMLINK;    // S_IFLNK
	case 0x6000: return FileType::BLOCK_DEV;  // S_IFBLK
	case 0x2000: return FileType::CHAR_DEV;   // S_IFCHR
	case 0x1000: return FileType::FIFO;       // S_IFIFO
	case 0xC000: return FileType::SOCKET;     // S_IFSOCK
	default:     return FileType::UNKNOWN;
	}
}

inline const char* FileTypeToString(FileType type) {
	switch (type) {
	case FileType::REGULAR:   return "file";
	case FileType::DIRECTORY: return "dir";
	case FileType::SYMLINK:   return "link";
	case FileType::BLOCK_DEV: return "blk";
	case FileType::CHAR_DEV:  return "chr";
	case FileType::FIFO:      return "fifo";
	case FileType::SOCKET:    return "sock";
	default:                  return "unknown";
	}
}

//===----------------------------------------------------------------------===//
// Link Extended Attribute (LinkEA) - stored in trusted.link xattr
// Contains all hard link information for a file
//===----------------------------------------------------------------------===//

// LinkEA magic number and layout constants
constexpr uint32_t LINK_EA_MAGIC = 0x11EAF1DF;
constexpr size_t LINK_EA_RECLEN_SIZE = 2;       // lee_reclen field size (big-endian uint16)
constexpr size_t LINK_EA_FID_SIZE = 16;          // lee_parent_fid field size
constexpr size_t LINK_EA_MIN_ENTRY_SIZE = LINK_EA_RECLEN_SIZE + LINK_EA_FID_SIZE; // 18 bytes

// LMA layout constants
constexpr size_t LMA_FID_OFFSET = 8;            // Offset of FID within LMA (after lma_compat + lma_incompat)

struct LinkEAHeader {
	uint32_t leh_magic;           // Magic number (LINK_EA_MAGIC)
	uint32_t leh_reccount;        // Number of entries (link count)
	uint64_t leh_len;             // Total size in bytes
	uint32_t leh_overflow_time;   // Overflow timestamp
	uint32_t leh_padding;         // Padding
};

// LinkEA entry is packed and variable-length
// struct link_ea_entry {
//     unsigned char lee_reclen[2];      // Record length (big-endian)
//     unsigned char lee_parent_fid[16]; // Parent directory FID
//     char          lee_name[];         // File name (not null-terminated)
// } __attribute__((packed));

// Parsed link entry
struct LinkEntry {
	LustreFID parent_fid;   // Parent directory FID
	std::string name;       // File name in parent directory

	LinkEntry() = default;
	LinkEntry(const LustreFID &fid, const std::string &n)
	    : parent_fid(fid), name(n) {}
};

//===----------------------------------------------------------------------===//
// Lustre Link - a single (fid, parent_fid, name) row for lustre_links output
//===----------------------------------------------------------------------===//
struct LustreLink {
	LustreFID fid;         // Inode's FID
	LustreFID parent_fid;  // Parent directory FID
	std::string name;      // File name in parent directory
};

//===----------------------------------------------------------------------===//
// Lustre Layout Component - one per component in a composite (PFL/FLR/EC) layout,
// or a single entry for simple V1/V3 layouts
//===----------------------------------------------------------------------===//
struct LustreLayoutComponent {
	uint32_t comp_index;       // 0-based component index
	uint32_t comp_id;          // lcme_id (0 for non-composite)
	uint16_t mirror_id;        // (lcme_id & MIRROR_ID_MASK) >> MIRROR_ID_SHIFT
	uint32_t comp_flags;       // lcme_flags (0 for non-composite)
	uint64_t extent_start;     // Byte offset start (0 for non-composite)
	uint64_t extent_end;       // Byte offset end (LUSTRE_EOF for non-composite)
	uint32_t pattern;          // lmm_pattern (RAID0, MDT, etc.)
	uint32_t stripe_size;      // lmm_stripe_size in bytes
	uint16_t stripe_count;     // lmm_stripe_count
	uint16_t stripe_offset;    // First OST index from objects, or 0xFFFF if unknown
	std::string pool_name;     // OST pool name (V3 only)
	uint8_t  dstripe_count;    // EC data stripe count (k)
	uint8_t  cstripe_count;    // EC code stripe count (p)
	uint8_t  compr_type;       // Compression type
	uint8_t  compr_lvl;        // Compression level (4 bits)

	LustreLayoutComponent() : comp_index(0), comp_id(0), mirror_id(0), comp_flags(0),
	                          extent_start(0), extent_end(LUSTRE_EOF), pattern(0),
	                          stripe_size(0), stripe_count(0), stripe_offset(0xFFFF),
	                          dstripe_count(0), cstripe_count(0), compr_type(0), compr_lvl(0) {}
};

//===----------------------------------------------------------------------===//
// Lustre OST Object - per-stripe object placement information
//===----------------------------------------------------------------------===//
struct LustreOSTObject {
	uint32_t comp_index;       // Component this object belongs to
	uint32_t stripe_index;     // 0-based stripe index within the component
	uint32_t ost_idx;          // OST index
	uint64_t ost_oi_id;        // Object ID on the OST
	uint64_t ost_oi_seq;       // Object sequence on the OST

	LustreOSTObject() : comp_index(0), stripe_index(0), ost_idx(0), ost_oi_id(0), ost_oi_seq(0) {}
};

//===----------------------------------------------------------------------===//
// Lustre Inode - parsed metadata from MDT
//===----------------------------------------------------------------------===//
struct LustreInode {
	// Basic inode fields
	uint64_t ino;           // Inode number
	uint16_t mode;          // File mode (type + permissions)
	uint32_t nlink;         // Hard link count
	uint32_t uid;           // User ID
	uint32_t gid;           // Group ID
	uint64_t size;          // File size in bytes
	uint64_t blocks;        // Number of 512-byte blocks

	// Timestamps (Unix epoch seconds)
	int64_t atime;          // Last access time
	int64_t mtime;          // Last modification time
	int64_t ctime;          // Last status change time

	// Lustre-specific fields
	LustreFID fid;          // Lustre FID
	LustreFID parent_fid;   // Parent directory FID

	// Extended attributes
	uint32_t projid;        // Project ID
	uint32_t flags;         // Inode flags

	// Derived fields
	FileType type;          // File type

	LustreInode() : ino(0), mode(0), nlink(0), uid(0), gid(0), size(0), blocks(0),
	                atime(0), mtime(0), ctime(0),
	                projid(0), flags(0),
	                type(FileType::UNKNOWN) {}
};

//===----------------------------------------------------------------------===//
// Lustre Inode+Link Row - one joined row for internal fused scans
//===----------------------------------------------------------------------===//
struct LustreInodeLinkRow {
	LustreInode inode;      // Full inode metadata
	LustreFID link_fid;     // FID exposed by lustre_links
	LustreFID parent_fid;   // Parent directory FID
	std::string name;       // File name in parent directory
};

//===----------------------------------------------------------------------===//
// Lustre Inode+Object Row - one joined row for internal fused scans
//===----------------------------------------------------------------------===//
struct LustreInodeObjectRow {
	LustreInode inode;      // Full inode metadata
	LustreFID object_fid;   // FID exposed by lustre_objects
	LustreOSTObject object; // OST object placement row
};

//===----------------------------------------------------------------------===//
// Lustre Inode+Layout Row - one joined row for internal fused scans
//===----------------------------------------------------------------------===//
struct LustreInodeLayoutRow {
	LustreInode inode;             // Full inode metadata
	LustreFID layout_fid;          // FID exposed by lustre_layouts
	LustreLayoutComponent layout;  // Layout component row
};

//===----------------------------------------------------------------------===//
// Extended Attribute names used by Lustre
//===----------------------------------------------------------------------===//
namespace xattr {
	constexpr const char* LMA = "trusted.lma";      // Lustre Metadata Attributes
	constexpr const char* LOV = "trusted.lov";      // LOV stripe info
	constexpr const char* LMV = "trusted.lmv";      // LMV directory stripe
	constexpr const char* LINK = "trusted.link";    // Hard link info
	constexpr const char* FID = "trusted.fid";      // FID
	constexpr const char* SOM = "trusted.som";      // Size on MDS
}

} // namespace lustre
} // namespace duckdb
