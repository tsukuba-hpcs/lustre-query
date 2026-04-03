//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_filter.cpp
//
// Filter pushdown implementation for Lustre Query
//===----------------------------------------------------------------------===//

#include "lustre_filter.hpp"

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"

namespace duckdb {
namespace lustre {

//===----------------------------------------------------------------------===//
// LustreFilterCondition Implementation
//===----------------------------------------------------------------------===//

uint64_t LustreFilterCondition::GetUIntValue(const LustreInode &inode) const {
	switch (column_idx) {
	case static_cast<idx_t>(LustreColumnIdx::INO):
		return inode.ino;
	case static_cast<idx_t>(LustreColumnIdx::MODE):
		return inode.mode;
	case static_cast<idx_t>(LustreColumnIdx::NLINK):
		return inode.nlink;
	case static_cast<idx_t>(LustreColumnIdx::UID):
		return inode.uid;
	case static_cast<idx_t>(LustreColumnIdx::GID):
		return inode.gid;
	case static_cast<idx_t>(LustreColumnIdx::SIZE):
		return inode.size;
	case static_cast<idx_t>(LustreColumnIdx::BLOCKS):
		return inode.blocks;
	case static_cast<idx_t>(LustreColumnIdx::PROJID):
		return inode.projid;
	case static_cast<idx_t>(LustreColumnIdx::FLAGS):
		return inode.flags;
	default:
		return 0;
	}
}

int64_t LustreFilterCondition::GetIntValue(const LustreInode &inode) const {
	switch (column_idx) {
	case static_cast<idx_t>(LustreColumnIdx::ATIME):
		return inode.atime;
	case static_cast<idx_t>(LustreColumnIdx::MTIME):
		return inode.mtime;
	case static_cast<idx_t>(LustreColumnIdx::CTIME):
		return inode.ctime;
	default:
		return 0;
	}
}

string LustreFilterCondition::GetStringValue(const LustreInode &inode) const {
	switch (column_idx) {
	case static_cast<idx_t>(LustreColumnIdx::TYPE):
		return FileTypeToString(inode.type);
	default:
		return "";
	}
}

template <typename T>
bool LustreFilterCondition::CompareNumeric(T inode_value) const {
	T compare_value = static_cast<T>(std::is_signed<T>::value ? int_value : uint_value);

	switch (op) {
	case FilterOp::EQUAL:
		return inode_value == compare_value;
	case FilterOp::NOT_EQUAL:
		return inode_value != compare_value;
	case FilterOp::GREATER_THAN:
		return inode_value > compare_value;
	case FilterOp::GREATER_THAN_OR_EQUAL:
		return inode_value >= compare_value;
	case FilterOp::LESS_THAN:
		return inode_value < compare_value;
	case FilterOp::LESS_THAN_OR_EQUAL:
		return inode_value <= compare_value;
	case FilterOp::IN:
		if constexpr (std::is_signed<T>::value) {
			return int_set.find(static_cast<int64_t>(inode_value)) != int_set.end();
		} else {
			return uint_set.find(static_cast<uint64_t>(inode_value)) != uint_set.end();
		}
	default:
		return true;
	}
}

bool LustreFilterCondition::CompareString(const string &inode_value) const {
	switch (op) {
	case FilterOp::EQUAL:
		return inode_value == string_value;
	case FilterOp::NOT_EQUAL:
		return inode_value != string_value;
	case FilterOp::IN:
		return string_set.find(inode_value) != string_set.end();
	default:
		// String columns don't support <, <=, >, >=
		return true;
	}
}

static bool IsColumnNull(idx_t column_idx, const LustreInode &inode) {
	switch (column_idx) {
	case static_cast<idx_t>(LustreColumnIdx::FID):
		return !inode.fid.IsValid();
	case static_cast<idx_t>(LustreColumnIdx::ATIME):
		return inode.atime == 0;
	case static_cast<idx_t>(LustreColumnIdx::MTIME):
		return inode.mtime == 0;
	case static_cast<idx_t>(LustreColumnIdx::CTIME):
		return inode.ctime == 0;
	default:
		return false;
	}
}

bool LustreFilterCondition::Evaluate(const LustreInode &inode) const {
	// Handle IS_NULL / IS_NOT_NULL for any column
	if (op == FilterOp::IS_NULL) {
		return IsColumnNull(column_idx, inode);
	}
	if (op == FilterOp::IS_NOT_NULL) {
		return !IsColumnNull(column_idx, inode);
	}

	switch (column_idx) {
	// Unsigned integer columns
	case static_cast<idx_t>(LustreColumnIdx::INO):
	case static_cast<idx_t>(LustreColumnIdx::SIZE):
	case static_cast<idx_t>(LustreColumnIdx::BLOCKS):
		return CompareNumeric<uint64_t>(GetUIntValue(inode));

	case static_cast<idx_t>(LustreColumnIdx::MODE):
	case static_cast<idx_t>(LustreColumnIdx::NLINK):
	case static_cast<idx_t>(LustreColumnIdx::UID):
	case static_cast<idx_t>(LustreColumnIdx::GID):
	case static_cast<idx_t>(LustreColumnIdx::PROJID):
	case static_cast<idx_t>(LustreColumnIdx::FLAGS):
		return CompareNumeric<uint32_t>(static_cast<uint32_t>(GetUIntValue(inode)));

	// Timestamp columns (signed int64)
	case static_cast<idx_t>(LustreColumnIdx::ATIME):
	case static_cast<idx_t>(LustreColumnIdx::MTIME):
	case static_cast<idx_t>(LustreColumnIdx::CTIME):
		return CompareNumeric<int64_t>(GetIntValue(inode));

	// String columns
	case static_cast<idx_t>(LustreColumnIdx::TYPE):
		return CompareString(GetStringValue(inode));

	default:
		// Non-filterable columns always pass
		return true;
	}
}

//===----------------------------------------------------------------------===//
// LustreFilterSet Implementation
//===----------------------------------------------------------------------===//

bool LustreFilterSet::IsFilterableColumn(idx_t column_idx) {
	switch (column_idx) {
	case static_cast<idx_t>(LustreColumnIdx::INO):
	case static_cast<idx_t>(LustreColumnIdx::TYPE):
	case static_cast<idx_t>(LustreColumnIdx::MODE):
	case static_cast<idx_t>(LustreColumnIdx::NLINK):
	case static_cast<idx_t>(LustreColumnIdx::UID):
	case static_cast<idx_t>(LustreColumnIdx::GID):
	case static_cast<idx_t>(LustreColumnIdx::SIZE):
	case static_cast<idx_t>(LustreColumnIdx::BLOCKS):
	case static_cast<idx_t>(LustreColumnIdx::ATIME):
	case static_cast<idx_t>(LustreColumnIdx::MTIME):
	case static_cast<idx_t>(LustreColumnIdx::CTIME):
	case static_cast<idx_t>(LustreColumnIdx::PROJID):
	case static_cast<idx_t>(LustreColumnIdx::FLAGS):
		return true;
	default:
		// DEVICE is not filterable
		return false;
	}
}

static FilterOp ExpressionTypeToFilterOp(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return FilterOp::EQUAL;
	case ExpressionType::COMPARE_NOTEQUAL:
		return FilterOp::NOT_EQUAL;
	case ExpressionType::COMPARE_GREATERTHAN:
		return FilterOp::GREATER_THAN;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return FilterOp::GREATER_THAN_OR_EQUAL;
	case ExpressionType::COMPARE_LESSTHAN:
		return FilterOp::LESS_THAN;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return FilterOp::LESS_THAN_OR_EQUAL;
	default:
		return FilterOp::EQUAL;
	}
}

static bool IsStringColumn(idx_t column_idx) {
	return column_idx == static_cast<idx_t>(LustreColumnIdx::TYPE);
}

static bool IsTimestampColumn(idx_t column_idx) {
	return column_idx == static_cast<idx_t>(LustreColumnIdx::ATIME) ||
	       column_idx == static_cast<idx_t>(LustreColumnIdx::MTIME) ||
	       column_idx == static_cast<idx_t>(LustreColumnIdx::CTIME);
}

void LustreFilterSet::ParseFilter(idx_t column_idx, const TableFilter &filter,
                                  vector<LustreFilterCondition> &conditions) {
	if (!IsFilterableColumn(column_idx)) {
		return;
	}

	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &const_filter = filter.Cast<ConstantFilter>();
		LustreFilterCondition condition;
		condition.column_idx = column_idx;
		condition.op = ExpressionTypeToFilterOp(const_filter.comparison_type);

		if (IsStringColumn(column_idx)) {
			condition.string_value = StringValue::Get(const_filter.constant);
		} else if (IsTimestampColumn(column_idx)) {
			// Convert timestamp to epoch seconds
			auto ts = TimestampValue::Get(const_filter.constant);
			condition.int_value = Timestamp::GetEpochSeconds(ts);
		} else {
			// Numeric column
			switch (const_filter.constant.type().id()) {
			case LogicalTypeId::UBIGINT:
				condition.uint_value = UBigIntValue::Get(const_filter.constant);
				break;
			case LogicalTypeId::BIGINT:
				condition.int_value = BigIntValue::Get(const_filter.constant);
				condition.uint_value = static_cast<uint64_t>(condition.int_value);
				break;
			case LogicalTypeId::UINTEGER:
				condition.uint_value = UIntegerValue::Get(const_filter.constant);
				break;
			case LogicalTypeId::INTEGER:
				condition.int_value = IntegerValue::Get(const_filter.constant);
				condition.uint_value = static_cast<uint64_t>(condition.int_value);
				break;
			case LogicalTypeId::USMALLINT:
				condition.uint_value = USmallIntValue::Get(const_filter.constant);
				break;
			case LogicalTypeId::SMALLINT:
				condition.int_value = SmallIntValue::Get(const_filter.constant);
				condition.uint_value = static_cast<uint64_t>(condition.int_value);
				break;
			default:
				// Try to cast to bigint
				condition.int_value = const_filter.constant.GetValue<int64_t>();
				condition.uint_value = static_cast<uint64_t>(condition.int_value);
				break;
			}
		}
		conditions.push_back(std::move(condition));
		break;
	}

	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		LustreFilterCondition condition;
		condition.column_idx = column_idx;
		condition.op = FilterOp::IN;

		if (IsStringColumn(column_idx)) {
			for (const auto &val : in_filter.values) {
				condition.string_set.insert(StringValue::Get(val));
			}
		} else if (IsTimestampColumn(column_idx)) {
			for (const auto &val : in_filter.values) {
				auto ts = TimestampValue::Get(val);
				condition.int_set.insert(Timestamp::GetEpochSeconds(ts));
			}
		} else {
			for (const auto &val : in_filter.values) {
				condition.uint_set.insert(val.GetValue<uint64_t>());
			}
		}
		conditions.push_back(std::move(condition));
		break;
	}

	case TableFilterType::CONJUNCTION_AND: {
		auto &and_filter = filter.Cast<ConjunctionAndFilter>();
		for (const auto &child : and_filter.child_filters) {
			ParseFilter(column_idx, *child, conditions);
		}
		break;
	}

	case TableFilterType::CONJUNCTION_OR: {
		// OR filters on the same column are not directly supported
		// They would need to be handled as a disjunction of conditions
		// For now, we skip OR filters and let DuckDB handle them post-filter
		break;
	}

	case TableFilterType::IS_NULL: {
		LustreFilterCondition condition;
		condition.column_idx = column_idx;
		condition.op = FilterOp::IS_NULL;
		conditions.push_back(std::move(condition));
		break;
	}

	case TableFilterType::IS_NOT_NULL: {
		LustreFilterCondition condition;
		condition.column_idx = column_idx;
		condition.op = FilterOp::IS_NOT_NULL;
		conditions.push_back(std::move(condition));
		break;
	}

	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		if (optional_filter.child_filter) {
			ParseFilter(column_idx, *optional_filter.child_filter, conditions);
		}
		break;
	}

	case TableFilterType::DYNAMIC_FILTER: {
		auto &dynamic_filter = filter.Cast<DynamicFilter>();
		if (dynamic_filter.filter_data) {
			lock_guard<mutex> lock(dynamic_filter.filter_data->lock);
			if (dynamic_filter.filter_data->initialized && dynamic_filter.filter_data->filter) {
				ParseFilter(column_idx, *dynamic_filter.filter_data->filter, conditions);
			}
		}
		break;
	}

	default:
		break;
	}
}

unique_ptr<LustreFilterSet> LustreFilterSet::Create(const TableFilterSet *filters, const vector<idx_t> &column_ids) {
	auto result = make_uniq<LustreFilterSet>();

	if (!filters) {
		return result;
	}

	for (const auto &entry : filters->filters) {
		idx_t filter_col_idx = entry.first;

		// The filter column index refers to the position in column_ids
		// We need to get the actual column index
		if (filter_col_idx >= column_ids.size()) {
			continue;
		}
		idx_t actual_column_idx = column_ids[filter_col_idx];

		ParseFilter(actual_column_idx, *entry.second, result->conditions_);
	}

	return result;
}

bool LustreFilterSet::HasFilters() const {
	return !conditions_.empty();
}

bool LustreFilterSet::Evaluate(const LustreInode &inode) const {
	// All conditions are ANDed together
	for (const auto &condition : conditions_) {
		if (!condition.Evaluate(inode)) {
			return false;
		}
	}
	return true;
}

} // namespace lustre
} // namespace duckdb
