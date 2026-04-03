//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_string_filter.hpp
//
// Compiled string predicate support for pushed-down TableFilters
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/enums/expression_type.hpp"

#include <unordered_set>

namespace duckdb {
namespace lustre {

enum class LustreStringFilterKind : uint8_t {
	NONE,
	CONSTANT,
	IN_FILTER,
	IS_NULL,
	IS_NOT_NULL,
	CONJUNCTION_AND,
	CONJUNCTION_OR,
	OPTIONAL,
	BLOOM,
	DYNAMIC
};

class LustreStringFilter {
public:
	static unique_ptr<LustreStringFilter> Create(const TableFilter &filter);

	bool Evaluate(const string *value) const;

	bool HasPredicate() const {
		return has_predicate;
	}

	bool RequiresGenericEvaluation() const {
		return requires_generic_evaluation;
	}

private:
	LustreStringFilterKind kind = LustreStringFilterKind::NONE;
	ExpressionType comparison_type = ExpressionType::INVALID;
	string constant_value;
	std::unordered_set<string> string_values;
	vector<unique_ptr<LustreStringFilter>> children;
	unique_ptr<TableFilter> raw_filter;
	bool has_predicate = false;
	bool requires_generic_evaluation = false;

	bool EvaluateConstant(const string *value) const;
	static void Populate(const TableFilter &filter, LustreStringFilter &result);
};

} // namespace lustre
} // namespace duckdb
