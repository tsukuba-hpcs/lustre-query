//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_string_filter.cpp
//
// Compiled string predicate support for pushed-down TableFilters
//===----------------------------------------------------------------------===//

#include "lustre_string_filter.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/filter/bloom_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"

namespace duckdb {
namespace lustre {

unique_ptr<LustreStringFilter> LustreStringFilter::Create(const TableFilter &filter) {
	auto result = make_uniq<LustreStringFilter>();
	Populate(filter, *result);
	return result;
}

void LustreStringFilter::Populate(const TableFilter &filter, LustreStringFilter &result) {
	result.has_predicate = true;

	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &const_filter = filter.Cast<ConstantFilter>();
		result.kind = LustreStringFilterKind::CONSTANT;
		result.comparison_type = const_filter.comparison_type;
		result.constant_value = StringValue::Get(const_filter.constant);
		result.requires_generic_evaluation = const_filter.comparison_type != ExpressionType::COMPARE_EQUAL;
		break;
	}
	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		result.kind = LustreStringFilterKind::IN_FILTER;
		for (const auto &value : in_filter.values) {
			result.string_values.insert(StringValue::Get(value));
		}
		result.requires_generic_evaluation = false;
		break;
	}
	case TableFilterType::IS_NULL:
		result.kind = LustreStringFilterKind::IS_NULL;
		result.requires_generic_evaluation = true;
		break;
	case TableFilterType::IS_NOT_NULL:
		result.kind = LustreStringFilterKind::IS_NOT_NULL;
		result.requires_generic_evaluation = true;
		break;
	case TableFilterType::CONJUNCTION_AND: {
		auto &and_filter = filter.Cast<ConjunctionAndFilter>();
		result.kind = LustreStringFilterKind::CONJUNCTION_AND;
		result.requires_generic_evaluation = false;
		for (const auto &child : and_filter.child_filters) {
			auto child_filter = LustreStringFilter::Create(*child);
			result.requires_generic_evaluation |= child_filter->RequiresGenericEvaluation();
			result.children.push_back(std::move(child_filter));
		}
		break;
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &or_filter = filter.Cast<ConjunctionOrFilter>();
		result.kind = LustreStringFilterKind::CONJUNCTION_OR;
		result.requires_generic_evaluation = true;
		for (const auto &child : or_filter.child_filters) {
			result.children.push_back(LustreStringFilter::Create(*child));
		}
		break;
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		result.kind = LustreStringFilterKind::OPTIONAL;
		result.requires_generic_evaluation = false;
		if (optional_filter.child_filter) {
			auto child_filter = LustreStringFilter::Create(*optional_filter.child_filter);
			result.requires_generic_evaluation = child_filter->RequiresGenericEvaluation();
			result.children.push_back(std::move(child_filter));
		}
		break;
	}
	case TableFilterType::BLOOM_FILTER:
		result.kind = LustreStringFilterKind::BLOOM;
		result.raw_filter = filter.Copy();
		result.requires_generic_evaluation = true;
		break;
	case TableFilterType::DYNAMIC_FILTER:
		result.kind = LustreStringFilterKind::DYNAMIC;
		result.raw_filter = filter.Copy();
		result.requires_generic_evaluation = true;
		break;
	default:
		result.kind = LustreStringFilterKind::NONE;
		result.has_predicate = false;
		result.requires_generic_evaluation = false;
		break;
	}
}

bool LustreStringFilter::EvaluateConstant(const string *value) const {
	if (!value) {
		return false;
	}

	auto cmp = value->compare(constant_value);
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		return cmp == 0;
	case ExpressionType::COMPARE_NOTEQUAL:
		return cmp != 0;
	case ExpressionType::COMPARE_GREATERTHAN:
		return cmp > 0;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return cmp >= 0;
	case ExpressionType::COMPARE_LESSTHAN:
		return cmp < 0;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return cmp <= 0;
	default:
		return true;
	}
}

bool LustreStringFilter::Evaluate(const string *value) const {
	switch (kind) {
	case LustreStringFilterKind::NONE:
		return true;
	case LustreStringFilterKind::CONSTANT:
		return EvaluateConstant(value);
	case LustreStringFilterKind::IN_FILTER:
		return value && string_values.find(*value) != string_values.end();
	case LustreStringFilterKind::IS_NULL:
		return value == nullptr;
	case LustreStringFilterKind::IS_NOT_NULL:
		return value != nullptr;
	case LustreStringFilterKind::CONJUNCTION_AND:
		for (const auto &child : children) {
			if (child && !child->Evaluate(value)) {
				return false;
			}
		}
		return true;
	case LustreStringFilterKind::CONJUNCTION_OR:
		for (const auto &child : children) {
			if (child && child->Evaluate(value)) {
				return true;
			}
		}
		return children.empty();
	case LustreStringFilterKind::OPTIONAL:
		return children.empty() || children[0]->Evaluate(value);
	case LustreStringFilterKind::BLOOM: {
		auto &bf_filter = raw_filter->Cast<BFTableFilter>();
		if (!value) {
			return !bf_filter.FiltersNullValues();
		}
		return bf_filter.FilterValue(Value(*value));
	}
	case LustreStringFilterKind::DYNAMIC: {
		auto &dynamic_filter = raw_filter->Cast<DynamicFilter>();
		if (!dynamic_filter.filter_data) {
			return true;
		}
		lock_guard<mutex> lock(dynamic_filter.filter_data->lock);
		if (!dynamic_filter.filter_data->initialized || !dynamic_filter.filter_data->filter) {
			return true;
		}
		if (!value) {
			return false;
		}
		auto &const_filter = *dynamic_filter.filter_data->filter;
		return const_filter.Compare(Value(*value));
	}
	default:
		return true;
	}
}

} // namespace lustre
} // namespace duckdb
