//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_filter.hpp
//
// Filter pushdown support for Lustre Query
//===----------------------------------------------------------------------===//

#pragma once

#include "lustre_types.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/table_filter.hpp"

#include <unordered_set>

namespace duckdb {
namespace lustre {

//===----------------------------------------------------------------------===//
// Column Index Constants
//===----------------------------------------------------------------------===//
enum class LustreColumnIdx : idx_t {
	FID = 0,
	INO = 1,
	TYPE = 2,
	MODE = 3,
	NLINK = 4,
	UID = 5,
	GID = 6,
	SIZE = 7,
	BLOCKS = 8,
	ATIME = 9,
	MTIME = 10,
	CTIME = 11,
	PROJID = 12,
	FLAGS = 13,
	DEVICE = 14
};

//===----------------------------------------------------------------------===//
// Filter Operator
//===----------------------------------------------------------------------===//
enum class FilterOp : uint8_t {
	EQUAL,
	NOT_EQUAL,
	GREATER_THAN,
	GREATER_THAN_OR_EQUAL,
	LESS_THAN,
	LESS_THAN_OR_EQUAL,
	IN,
	IS_NULL,
	IS_NOT_NULL
};

//===----------------------------------------------------------------------===//
// Single Filter Condition
//===----------------------------------------------------------------------===//
struct LustreFilterCondition {
	//! Column index to filter on
	idx_t column_idx;

	//! Filter operator
	FilterOp op;

	//! Value storage (type depends on column)
	uint64_t uint_value = 0;
	int64_t int_value = 0;
	string string_value;
	std::unordered_set<uint64_t> uint_set;    // For IN operator (unsigned columns)
	std::unordered_set<int64_t> int_set;      // For IN operator (signed columns, e.g. timestamps)
	std::unordered_set<string> string_set;    // For IN operator (string columns)

	//! Evaluate this condition against an inode
	bool Evaluate(const LustreInode &inode) const;

private:
	//! Get uint64 value from inode for the column
	uint64_t GetUIntValue(const LustreInode &inode) const;

	//! Get int64 value from inode for the column (for timestamps)
	int64_t GetIntValue(const LustreInode &inode) const;

	//! Get string value from inode for the column
	string GetStringValue(const LustreInode &inode) const;

	//! Compare numeric values
	template <typename T>
	bool CompareNumeric(T inode_value) const;

	//! Compare string values
	bool CompareString(const string &inode_value) const;
};

//===----------------------------------------------------------------------===//
// Compiled Filter Set
//===----------------------------------------------------------------------===//
class LustreFilterSet {
public:
	//! Create a filter set from DuckDB TableFilterSet
	static unique_ptr<LustreFilterSet> Create(const TableFilterSet *filters, const vector<idx_t> &column_ids);

	//! Evaluate all filters against an inode
	bool Evaluate(const LustreInode &inode) const;

	//! Check if there are any filters
	bool HasFilters() const;

private:
	//! All filter conditions (ANDed together)
	vector<LustreFilterCondition> conditions_;

	//! Parse a single TableFilter into conditions
	static void ParseFilter(idx_t column_idx, const TableFilter &filter, vector<LustreFilterCondition> &conditions);

	//! Check if a column is filterable
	static bool IsFilterableColumn(idx_t column_idx);
};

} // namespace lustre
} // namespace duckdb
