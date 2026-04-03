//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_inodes.hpp
//
// Table function for querying Lustre MDT inodes
//===----------------------------------------------------------------------===//

#pragma once

#include "lustre_scan_state.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {
namespace lustre {

//===----------------------------------------------------------------------===//
// lustre_inodes Table Function
//===----------------------------------------------------------------------===//
class LustreInodesFunction {
public:
	//! Get the table function set (VARCHAR and LIST(VARCHAR) overloads)
	static TableFunctionSet GetFunctionSet();

	//! Column names
	static const vector<string> &GetColumnNames();

	//! Column types
	static const vector<LogicalType> &GetColumnTypes();
};

//===----------------------------------------------------------------------===//
// Local State - per-thread state
//===----------------------------------------------------------------------===//
struct LustreQueryLocalState : public LocalTableFunctionState {
	//! Per-thread MDT scanner (each thread has its own ext2fs handle)
	unique_ptr<MDTScanner> scanner;

	//! Whether this thread's scanner is open for the current device
	bool scanner_initialized = false;

	//! The device index this scanner is initialized for
	idx_t initialized_device_idx = DConstants::INVALID_INDEX;

	//! Stable device path for the currently initialized scanner
	string initialized_device_path;

	//! Whether a claimed block group is still being scanned across output chunks
	bool block_group_active = false;

	//! Upper inode bound for the active block group
	ext2_ino_t block_group_max_ino = 0;

	//! Next claimed block group in the local batch
	int next_block_group_in_batch = 0;

	//! Exclusive upper bound of the local block group batch
	int block_group_batch_end = 0;

	//! Deferred statistics for the active block group
	uint64_t pending_scanned = 0;
	uint64_t pending_returned = 0;

	//! Buffer for scanned inodes
	vector<LustreInode> inode_buffer;

	//! Current position in buffer
	idx_t buffer_pos;

	//! Buffer size
	static constexpr idx_t BUFFER_SIZE = 1024;

	LustreQueryLocalState() : buffer_pos(0) {
		scanner = make_uniq<MDTScanner>();
		inode_buffer.reserve(BUFFER_SIZE);
	}
};

} // namespace lustre
} // namespace duckdb
