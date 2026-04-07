//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_optimizer.cpp
//
// Optimizer rewrite for fused inode/link joins
//===----------------------------------------------------------------------===//

#include "lustre_optimizer.hpp"

#include "lustre_inode_links.hpp"
#include "lustre_inode_layouts.hpp"
#include "lustre_inode_objects.hpp"
#include "lustre_inode_dirmap.hpp"
#include "lustre_link_dirmap.hpp"
#include "lustre_link_inode_dirmap.hpp"
#include "lustre_link_layouts.hpp"
#include "lustre_link_objects.hpp"
#include "lustre_object_layouts.hpp"
#include "lustre_scan_state.hpp"
#include "lustre_table_filter_eval.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {
namespace lustre {

enum class FusedRelationKind : uint8_t {
	INODE,
	LINK,
	OBJECT,
	LAYOUT,
	DIRMAP,
	LINK_DIRMAP,
	OTHER
};

class LustreFusedInodeLinkJoin final : public LogicalExtensionOperator {
public:
	LustreFusedInodeLinkJoin(unique_ptr<LogicalOperator> child_p, vector<ColumnBinding> column_bindings_p,
	                         vector<LogicalType> output_types_p, vector<idx_t> table_indexes_p)
	    : column_bindings(std::move(column_bindings_p)), output_types(std::move(output_types_p)),
	      table_indexes(std::move(table_indexes_p)) {
		children.push_back(std::move(child_p));
	}

	vector<ColumnBinding> GetColumnBindings() override {
		return column_bindings;
	}

	vector<idx_t> GetTableIndex() const override {
		return table_indexes;
	}

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		D_ASSERT(children.size() == 1);
		return planner.CreatePlan(*children[0]);
	}

	string GetName() const override {
		return "LUSTRE_FUSED_JOIN";
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override {
		InsertionOrderPreservingMap<string> result;
		result["Rewrite"] = "lustre fused scan";
		SetParamsEstimatedCardinality(result);
		return result;
	}

	string GetExtensionName() const override {
		return "lustre_query";
	}

	bool SupportSerialization() const override {
		return false;
	}

	unique_ptr<LogicalOperator> Copy(ClientContext &context) const override {
		auto result = make_uniq<LustreFusedInodeLinkJoin>(children[0]->Copy(context), column_bindings, output_types,
		                                                  table_indexes);
		if (has_estimated_cardinality) {
			result->SetEstimatedCardinality(estimated_cardinality);
		}
		return std::move(result);
	}

protected:
	void ResolveTypes() override {
		types = output_types;
	}

private:
	vector<ColumnBinding> column_bindings;
	vector<LogicalType> output_types;
	vector<idx_t> table_indexes;
};

struct SidePlanInfo {
	LogicalGet *get = nullptr;
	vector<ColumnBinding> root_bindings;
	vector<idx_t> visible_actual_columns;
	vector<unique_ptr<Expression>> hoisted_filters;
	column_binding_map_t<idx_t> binding_to_actual_column;
};

class ColumnBindingRewriter final : public LogicalOperatorVisitor {
public:
	explicit ColumnBindingRewriter(const column_binding_map_t<ColumnBinding> &binding_map_p)
	    : binding_map(binding_map_p) {
	}

	bool success = true;

	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		auto entry = binding_map.find(expr.binding);
		if (entry == binding_map.end()) {
			success = false;
			return nullptr;
		}
		expr.binding = entry->second;
		return nullptr;
	}

private:
	const column_binding_map_t<ColumnBinding> &binding_map;
};

class LustreOptimizerExtension : public OptimizerExtension {
public:
	LustreOptimizerExtension() {
		optimize_function = RewritePlan;
	}

private:
	static FusedRelationKind GetRelationKind(const LogicalGet &get) {
		if (StringUtil::CIEquals(get.function.name, "lustre_inodes")) {
			return FusedRelationKind::INODE;
		}
		if (StringUtil::CIEquals(get.function.name, "lustre_links")) {
			return FusedRelationKind::LINK;
		}
		if (StringUtil::CIEquals(get.function.name, "lustre_objects")) {
			return FusedRelationKind::OBJECT;
		}
		if (StringUtil::CIEquals(get.function.name, "lustre_layouts")) {
			return FusedRelationKind::LAYOUT;
		}
		if (StringUtil::CIEquals(get.function.name, "lustre_dirmap")) {
			return FusedRelationKind::DIRMAP;
		}
		if (StringUtil::CIEquals(get.function.name, "lustre_link_dirmap")) {
			return FusedRelationKind::LINK_DIRMAP;
		}
		return FusedRelationKind::OTHER;
	}

	static idx_t MapRelationColumn(idx_t column_idx, FusedRelationKind kind) {
		switch (kind) {
		case FusedRelationKind::INODE:
			return MapInodeColumn(column_idx);
		case FusedRelationKind::LINK:
			return MapLinkColumn(column_idx);
		case FusedRelationKind::OBJECT:
			return MapObjectColumn(column_idx);
		case FusedRelationKind::LAYOUT:
			return MapLayoutColumn(column_idx);
		case FusedRelationKind::DIRMAP:
			// DIRMAP in inode_dirmap context: inode(0-14) + dirmap(15-24)
			return MapInodeDirMapDirMapColumn(column_idx);
		default:
			return DConstants::INVALID_INDEX;
		}
	}

	static const vector<LogicalType> &GetFusedColumnTypes(FusedRelationKind kind) {
		switch (kind) {
		case FusedRelationKind::LINK:
			return LustreInodeLinksFunction::GetColumnTypes();
		case FusedRelationKind::OBJECT:
			return LustreInodeObjectsFunction::GetColumnTypes();
		case FusedRelationKind::LAYOUT:
			return LustreInodeLayoutsFunction::GetColumnTypes();
		default:
			throw InternalException("unsupported fused relation kind");
		}
	}

	static const vector<string> &GetFusedColumnNames(FusedRelationKind kind) {
		switch (kind) {
		case FusedRelationKind::LINK:
			return LustreInodeLinksFunction::GetColumnNames();
		case FusedRelationKind::OBJECT:
			return LustreInodeObjectsFunction::GetColumnNames();
		case FusedRelationKind::LAYOUT:
			return LustreInodeLayoutsFunction::GetColumnNames();
		default:
			throw InternalException("unsupported fused relation kind");
		}
	}

	static TableFunction GetFusedFunction(FusedRelationKind kind, bool multi_device) {
		switch (kind) {
		case FusedRelationKind::LINK:
			return LustreInodeLinksFunction::GetFunction(multi_device);
		case FusedRelationKind::OBJECT:
			return LustreInodeObjectsFunction::GetFunction(multi_device);
		case FusedRelationKind::LAYOUT:
			return LustreInodeLayoutsFunction::GetFunction(multi_device);
		default:
			throw InternalException("unsupported fused relation kind");
		}
	}

	static bool IsStringFilterRewritable(const TableFilter &filter, bool allow_dynamic) {
		switch (filter.filter_type) {
		case TableFilterType::CONSTANT_COMPARISON:
		case TableFilterType::IN_FILTER:
		case TableFilterType::IS_NULL:
		case TableFilterType::IS_NOT_NULL:
		case TableFilterType::BLOOM_FILTER:
			return true;
		case TableFilterType::DYNAMIC_FILTER:
			return allow_dynamic;
		case TableFilterType::CONJUNCTION_AND: {
			auto &and_filter = filter.Cast<ConjunctionAndFilter>();
			for (const auto &child : and_filter.child_filters) {
				if (!IsStringFilterRewritable(*child, allow_dynamic)) {
					return false;
				}
			}
			return true;
		}
		case TableFilterType::CONJUNCTION_OR: {
			auto &or_filter = filter.Cast<ConjunctionOrFilter>();
			for (const auto &child : or_filter.child_filters) {
				if (!IsStringFilterRewritable(*child, allow_dynamic)) {
					return false;
				}
			}
			return true;
		}
		case TableFilterType::OPTIONAL_FILTER: {
			auto &optional_filter = filter.Cast<OptionalFilter>();
			return !optional_filter.child_filter || IsStringFilterRewritable(*optional_filter.child_filter, allow_dynamic);
		}
		default:
			return false;
		}
	}

	static bool IsInodeColumnFilterRewritable(const TableFilter &filter) {
		switch (filter.filter_type) {
		case TableFilterType::CONSTANT_COMPARISON:
		case TableFilterType::IN_FILTER:
		case TableFilterType::IS_NULL:
		case TableFilterType::IS_NOT_NULL:
			return true;
		case TableFilterType::CONJUNCTION_AND: {
			auto &and_filter = filter.Cast<ConjunctionAndFilter>();
			for (const auto &child : and_filter.child_filters) {
				if (!IsInodeColumnFilterRewritable(*child)) {
					return false;
				}
			}
			return true;
		}
		case TableFilterType::OPTIONAL_FILTER: {
			auto &optional_filter = filter.Cast<OptionalFilter>();
			return !optional_filter.child_filter || IsInodeColumnFilterRewritable(*optional_filter.child_filter);
		}
		default:
			return false;
		}
	}

	static bool HasOnlyRewriteSafeFilters(const LogicalGet &get, FusedRelationKind kind) {
		for (const auto &entry : get.table_filters.filters) {
			bool supported = false;
			if (kind == FusedRelationKind::INODE) {
				switch (entry.first) {
				case 0:
					supported = IsStringFilterRewritable(*entry.second, true);
					break;
				case 14:
					supported = IsStringFilterRewritable(*entry.second, false);
					break;
				default:
					supported = IsInodeColumnFilterRewritable(*entry.second);
					break;
				}
			} else if (kind == FusedRelationKind::LINK) {
				switch (entry.first) {
				case 0:
				case 1:
					supported = IsStringFilterRewritable(*entry.second, true);
					break;
				case 2:
					supported = IsStringFilterRewritable(*entry.second, false);
					break;
				default:
					supported = false;
					break;
				}
			} else if (kind == FusedRelationKind::OBJECT || kind == FusedRelationKind::LAYOUT) {
				supported = IsGenericTableFilterRewritable(*entry.second);
			} else if (kind == FusedRelationKind::DIRMAP) {
				switch (entry.first) {
				case 0:
				case 1:
					supported = IsStringFilterRewritable(*entry.second, true);
					break;
				default:
					supported = IsGenericTableFilterRewritable(*entry.second);
					break;
				}
			}
			if (!supported) {
				return false;
			}
		}
		return true;
	}

	static idx_t MapFilterColumn(idx_t column_idx, FusedRelationKind kind) {
		return MapRelationColumn(column_idx, kind);
	}

	static unique_ptr<TableFilterSet> TranslateFilters(const LogicalGet &source, FusedRelationKind kind) {
		auto result = make_uniq<TableFilterSet>();
		for (const auto &entry : source.table_filters.filters) {
			auto mapped_idx = MapFilterColumn(entry.first, kind);
			if (mapped_idx == DConstants::INVALID_INDEX) {
				return nullptr;
			}
			result->filters[mapped_idx] = entry.second->Copy();
		}
		return result;
	}

	static void MergeFilters(TableFilterSet &target, const TableFilterSet &source) {
		for (const auto &entry : source.filters) {
			target.filters[entry.first] = entry.second->Copy();
		}
	}

	static idx_t GetOrAddColumn(vector<idx_t> &scan_columns, idx_t column_idx) {
		for (idx_t i = 0; i < scan_columns.size(); i++) {
			if (scan_columns[i] == column_idx) {
				return i;
			}
		}
		scan_columns.push_back(column_idx);
		return scan_columns.size() - 1;
	}

	static vector<idx_t> GetVisibleActualColumns(const LogicalGet &get) {
		vector<idx_t> result;
		const auto &column_ids = get.GetColumnIds();
		if (column_ids.empty()) {
			return result;
		}
		if (get.projection_ids.empty()) {
			result.reserve(column_ids.size());
			for (const auto &column_id : column_ids) {
				result.push_back(column_id.GetPrimaryIndex());
			}
			return result;
		}
		result.reserve(get.projection_ids.size());
		for (auto proj_id : get.projection_ids) {
			if (proj_id >= column_ids.size()) {
				return {};
			}
			result.push_back(column_ids[proj_id].GetPrimaryIndex());
		}
		return result;
	}

	static vector<ColumnBinding> GetVisibleBindings(const LogicalGet &get) {
		vector<ColumnBinding> result;
		const auto &column_ids = get.GetColumnIds();
		if (column_ids.empty()) {
			return result;
		}
		if (get.projection_ids.empty()) {
			result.reserve(column_ids.size());
			for (idx_t i = 0; i < column_ids.size(); i++) {
				result.emplace_back(get.table_index, i);
			}
			return result;
		}
		result.reserve(get.projection_ids.size());
		for (auto proj_id : get.projection_ids) {
			result.emplace_back(get.table_index, proj_id);
		}
		return result;
	}

	static vector<idx_t> GetJoinOutputActualColumns(const vector<idx_t> &visible_columns,
	                                                const vector<idx_t> &projection_map) {
		if (visible_columns.empty()) {
			return {};
		}
		if (projection_map.empty()) {
			return visible_columns;
		}
		vector<idx_t> result;
		result.reserve(projection_map.size());
		for (auto proj_id : projection_map) {
			if (proj_id >= visible_columns.size()) {
				return {};
			}
			result.push_back(visible_columns[proj_id]);
		}
		return result;
	}

	static bool RewriteBindingReferences(unique_ptr<Expression> &expr,
	                                     const column_binding_map_t<ColumnBinding> &binding_map) {
		ColumnBindingRewriter rewriter(binding_map);
		rewriter.VisitExpression(&expr);
		return rewriter.success;
	}

	static void EnumerateBoundColumnRefs(Expression &expr,
	                                     const std::function<void(BoundColumnRefExpression &column_ref)> &callback) {
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			callback(expr.Cast<BoundColumnRefExpression>());
		}
		ExpressionIterator::EnumerateChildren(expr,
		                                      [&](Expression &child) { EnumerateBoundColumnRefs(child, callback); });
	}

	static bool ExtractSidePlanInternal(LogicalOperator &op, SidePlanInfo &result,
	                                    column_binding_map_t<idx_t> &output_binding_to_actual) {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_GET: {
			auto &get = op.Cast<LogicalGet>();
			auto visible_bindings = GetVisibleBindings(get);
			auto visible_columns = GetVisibleActualColumns(get);
			if (visible_bindings.size() != visible_columns.size()) {
				return false;
			}
			output_binding_to_actual.clear();
			for (idx_t i = 0; i < visible_bindings.size(); i++) {
				output_binding_to_actual[visible_bindings[i]] = visible_columns[i];
				result.binding_to_actual_column[visible_bindings[i]] = visible_columns[i];
			}

			result.get = &get;
			return true;
		}
		case LogicalOperatorType::LOGICAL_FILTER: {
			auto &filter = op.Cast<LogicalFilter>();
			if (op.children.size() != 1) {
				return false;
			}
			column_binding_map_t<idx_t> child_binding_to_actual;
			if (!ExtractSidePlanInternal(*op.children[0], result, child_binding_to_actual)) {
				return false;
			}
			for (const auto &expr : filter.expressions) {
				result.hoisted_filters.push_back(expr->Copy());
			}
			if (!filter.HasProjectionMap()) {
				output_binding_to_actual = std::move(child_binding_to_actual);
				return true;
			}
			auto child_bindings = op.children[0]->GetColumnBindings();
			auto filter_bindings = filter.GetColumnBindings();
			if (filter_bindings.size() != filter.projection_map.size()) {
				return false;
			}
			output_binding_to_actual.clear();
			for (idx_t i = 0; i < filter.projection_map.size(); i++) {
				auto proj_id = filter.projection_map[i];
				if (proj_id >= child_bindings.size()) {
					return false;
				}
				auto child_entry = child_binding_to_actual.find(child_bindings[proj_id]);
				if (child_entry == child_binding_to_actual.end()) {
					return false;
				}
				output_binding_to_actual[filter_bindings[i]] = child_entry->second;
				result.binding_to_actual_column[filter_bindings[i]] = child_entry->second;
			}
			return true;
		}
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			auto &projection = op.Cast<LogicalProjection>();
			if (op.children.size() != 1) {
				return false;
			}
			column_binding_map_t<idx_t> child_binding_to_actual;
			if (!ExtractSidePlanInternal(*op.children[0], result, child_binding_to_actual)) {
				return false;
			}
			auto projection_bindings = projection.GetColumnBindings();
			if (projection_bindings.size() != projection.expressions.size()) {
				return false;
			}

			output_binding_to_actual.clear();
			for (idx_t i = 0; i < projection.expressions.size(); i++) {
				auto &expr = projection.expressions[i];
				if (!expr || expr->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
					return false;
				}
				auto &column_ref = expr->Cast<BoundColumnRefExpression>();
				auto child_entry = child_binding_to_actual.find(column_ref.binding);
				if (child_entry == child_binding_to_actual.end()) {
					return false;
				}
				output_binding_to_actual[projection_bindings[i]] = child_entry->second;
				result.binding_to_actual_column[projection_bindings[i]] = child_entry->second;
			}
			return true;
		}
		default:
			return false;
		}
	}

	static bool ExtractSidePlan(LogicalOperator &op, SidePlanInfo &result) {
		result.get = nullptr;
		result.root_bindings = op.GetColumnBindings();
		result.visible_actual_columns.clear();
		result.hoisted_filters.clear();
		result.binding_to_actual_column.clear();
		column_binding_map_t<idx_t> output_binding_to_actual;
		if (!ExtractSidePlanInternal(op, result, output_binding_to_actual)) {
			return false;
		}
		result.visible_actual_columns.reserve(result.root_bindings.size());
		for (const auto &binding : result.root_bindings) {
			auto entry = output_binding_to_actual.find(binding);
			if (entry == output_binding_to_actual.end()) {
				return false;
			}
			result.visible_actual_columns.push_back(entry->second);
		}
		return true;
	}

	static bool IsActualColumnReference(const unique_ptr<Expression> &expr, const SidePlanInfo &side, idx_t actual_column) {
		if (!expr || expr->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return false;
		}
		auto &column_ref = expr->Cast<BoundColumnRefExpression>();
		if (side.root_bindings.size() != side.visible_actual_columns.size()) {
			return false;
		}
		for (idx_t i = 0; i < side.root_bindings.size(); i++) {
			if (side.root_bindings[i] == column_ref.binding && side.visible_actual_columns[i] == actual_column) {
				return true;
			}
		}
		return false;
	}

	static bool MatchesJoinColumnEquality(const JoinCondition &condition, const SidePlanInfo &left_side, idx_t left_column,
	                                      const SidePlanInfo &right_side, idx_t right_column) {
		if (condition.comparison != ExpressionType::COMPARE_EQUAL) {
			return false;
		}
		bool direct = IsActualColumnReference(condition.left, left_side, left_column) &&
		              IsActualColumnReference(condition.right, right_side, right_column);
		bool reverse = IsActualColumnReference(condition.right, left_side, left_column) &&
		               IsActualColumnReference(condition.left, right_side, right_column);
		return direct || reverse;
	}

	static idx_t MapInodeColumn(idx_t column_idx) {
		return column_idx <= 14 ? column_idx : DConstants::INVALID_INDEX;
	}

	static idx_t MapLinkColumn(idx_t column_idx) {
		switch (column_idx) {
		case 0:
			return 15;
		case 1:
			return 16;
		case 2:
			return 17;
		default:
			return DConstants::INVALID_INDEX;
		}
	}

	static idx_t MapObjectColumn(idx_t column_idx) {
		return column_idx <= 6 ? 15 + column_idx : DConstants::INVALID_INDEX;
	}

	static idx_t MapLayoutColumn(idx_t column_idx) {
		return column_idx <= 16 ? 15 + column_idx : DConstants::INVALID_INDEX;
	}

	static idx_t MapObjectLayoutObjectColumn(idx_t column_idx) {
		return column_idx <= 6 ? column_idx : DConstants::INVALID_INDEX;
	}

	static idx_t MapObjectLayoutLayoutColumn(idx_t column_idx) {
		return column_idx <= 16 ? 7 + column_idx : DConstants::INVALID_INDEX;
	}

	static idx_t MapLinkObjectLinkColumn(idx_t column_idx) {
		return column_idx <= 2 ? column_idx : DConstants::INVALID_INDEX;
	}

	static idx_t MapLinkObjectObjectColumn(idx_t column_idx) {
		return column_idx <= 6 ? 3 + column_idx : DConstants::INVALID_INDEX;
	}

	static idx_t MapLinkLayoutLinkColumn(idx_t column_idx) {
		return column_idx <= 2 ? column_idx : DConstants::INVALID_INDEX;
	}

	static idx_t MapLinkLayoutLayoutColumn(idx_t column_idx) {
		return column_idx <= 16 ? 3 + column_idx : DConstants::INVALID_INDEX;
	}

	// Level 1: lustre_link_dirmap — link(0-3) + dirmap(4-13)
	static idx_t MapLinkDirMapLinkColumn(idx_t column_idx) {
		return column_idx <= 3 ? column_idx : DConstants::INVALID_INDEX;
	}

	// Fused functions don't include lma_incompat (col 10) — only map dirmap cols 0-9.
	// If a query references lma_incompat, the optimizer won't rewrite (falls back to hash join).
	static idx_t MapLinkDirMapDirMapColumn(idx_t column_idx) {
		return column_idx <= 9 ? 4 + column_idx : DConstants::INVALID_INDEX;
	}

	// Level 2: lustre_inode_dirmap — inode(0-14) + dirmap(15-24)
	static idx_t MapInodeDirMapInodeColumn(idx_t column_idx) {
		return column_idx <= 14 ? column_idx : DConstants::INVALID_INDEX;
	}

	static idx_t MapInodeDirMapDirMapColumn(idx_t column_idx) {
		return column_idx <= 9 ? 15 + column_idx : DConstants::INVALID_INDEX;
	}

	// Level 3: lustre_link_inode_dirmap — link(0-3) + dirmap(4-13) + inode(14-28)
	static idx_t MapLinkInodeDirMapLinkColumn(idx_t column_idx) {
		return column_idx <= 3 ? column_idx : DConstants::INVALID_INDEX;
	}

	static idx_t MapLinkInodeDirMapDirMapColumn(idx_t column_idx) {
		return column_idx <= 9 ? 4 + column_idx : DConstants::INVALID_INDEX;
	}

	static idx_t MapLinkInodeDirMapInodeColumn(idx_t column_idx) {
		return column_idx <= 14 ? 14 + column_idx : DConstants::INVALID_INDEX;
	}

	// Level 3 via cascading: lustre_link_dirmap(0-13) + inode(14-28)
	static idx_t MapLinkInodeDirMapFromLinkDirMapColumn(idx_t column_idx) {
		return column_idx <= 13 ? column_idx : DConstants::INVALID_INDEX;
	}

	static idx_t FindMaxTableIndex(const LogicalOperator &op) {
		idx_t max_index = 0;
		for (auto table_index : op.GetTableIndex()) {
			max_index = MaxValue(max_index, table_index);
		}
		for (const auto &child : op.children) {
			max_index = MaxValue(max_index, FindMaxTableIndex(*child));
		}
		return max_index;
	}

	static bool BindDataMatches(const LogicalGet &inode_get, const LogicalGet &link_get) {
		auto &inode_bind = inode_get.bind_data->Cast<LustreQueryBindData>();
		auto &link_bind = link_get.bind_data->Cast<LustreQueryBindData>();
		return inode_bind.device_paths == link_bind.device_paths &&
		       inode_bind.scan_config.skip_no_fid == link_bind.scan_config.skip_no_fid &&
		       inode_bind.scan_config.skip_no_linkea == link_bind.scan_config.skip_no_linkea;
	}

	//! Relaxed match for dirmap joins — only device_paths must match,
	//! scan config may differ (dirmap uses skip_no_linkea=false)
	static bool BindDataDevicePathsMatch(const LogicalGet &a, const LogicalGet &b) {
		auto &a_bind = a.bind_data->Cast<LustreQueryBindData>();
		auto &b_bind = b.bind_data->Cast<LustreQueryBindData>();
		return a_bind.device_paths == b_bind.device_paths;
	}

	static vector<idx_t> CollectTableIndexes(const vector<ColumnBinding> &bindings) {
		vector<idx_t> result;
		unordered_set<idx_t> seen;
		for (const auto &binding : bindings) {
			if (seen.insert(binding.table_index).second) {
				result.push_back(binding.table_index);
			}
		}
		return result;
	}

	static unique_ptr<LogicalOperator> TryRewriteLinkRelationJoin(LogicalComparisonJoin &join, idx_t &next_table_index) {
		if (join.join_type != JoinType::INNER || join.predicate || join.conditions.size() != 1) {
			return nullptr;
		}
		if (join.children.size() != 2) {
			return nullptr;
		}

		SidePlanInfo left_side;
		SidePlanInfo right_side;
		if (!ExtractSidePlan(*join.children[0], left_side) || !ExtractSidePlan(*join.children[1], right_side)) {
			return nullptr;
		}
		if (!left_side.get || !right_side.get) {
			return nullptr;
		}

		auto &left_get = *left_side.get;
		auto &right_get = *right_side.get;
		auto left_kind = GetRelationKind(left_get);
		auto right_kind = GetRelationKind(right_get);
		bool left_is_link = left_kind == FusedRelationKind::LINK;
		bool right_is_link = right_kind == FusedRelationKind::LINK;
		if (left_is_link == right_is_link) {
			return nullptr;
		}

		auto relation_kind = left_is_link ? right_kind : left_kind;
		if (relation_kind != FusedRelationKind::OBJECT && relation_kind != FusedRelationKind::LAYOUT) {
			return nullptr;
		}

		auto &link_get = left_is_link ? left_get : right_get;
		auto &relation_get = left_is_link ? right_get : left_get;
		auto &link_side = left_is_link ? left_side : right_side;
		auto &relation_side = left_is_link ? right_side : left_side;

		if (!link_get.bind_data || !relation_get.bind_data) {
			return nullptr;
		}
		if (!BindDataMatches(link_get, relation_get)) {
			return nullptr;
		}
		if (!HasOnlyRewriteSafeFilters(link_get, FusedRelationKind::LINK) ||
		    !HasOnlyRewriteSafeFilters(relation_get, relation_kind)) {
			return nullptr;
		}
		if (!MatchesJoinColumnEquality(join.conditions[0], link_side, 0, relation_side, 0)) {
			return nullptr;
		}

		auto left_columns = GetJoinOutputActualColumns(left_side.visible_actual_columns, join.left_projection_map);
		auto right_columns = GetJoinOutputActualColumns(right_side.visible_actual_columns, join.right_projection_map);
		if ((join.left_projection_map.empty() && left_side.root_bindings.empty()) ||
		    (join.right_projection_map.empty() && right_side.root_bindings.empty())) {
			return nullptr;
		}

		auto map_relation_column = [&](idx_t column_idx, bool is_link_side) -> idx_t {
			if (is_link_side) {
				return relation_kind == FusedRelationKind::OBJECT ? MapLinkObjectLinkColumn(column_idx)
				                                                 : MapLinkLayoutLinkColumn(column_idx);
			}
			return relation_kind == FusedRelationKind::OBJECT ? MapLinkObjectObjectColumn(column_idx)
			                                                 : MapLinkLayoutLayoutColumn(column_idx);
		};

		auto &fused_types = relation_kind == FusedRelationKind::OBJECT ? LustreLinkObjectsFunction::GetColumnTypes()
		                                                               : LustreLinkLayoutsFunction::GetColumnTypes();
		auto &fused_names = relation_kind == FusedRelationKind::OBJECT ? LustreLinkObjectsFunction::GetColumnNames()
		                                                               : LustreLinkLayoutsFunction::GetColumnNames();
		auto fused_function = [&](bool multi_device) {
			return relation_kind == FusedRelationKind::OBJECT ? LustreLinkObjectsFunction::GetFunction(multi_device)
			                                                  : LustreLinkLayoutsFunction::GetFunction(multi_device);
		};

		vector<idx_t> fused_output_columns;
		fused_output_columns.reserve(left_columns.size() + right_columns.size());
		auto append_columns = [&](const vector<idx_t> &columns, bool is_link_side) {
			for (auto column_idx : columns) {
				auto fused_idx = map_relation_column(column_idx, is_link_side);
				if (fused_idx == DConstants::INVALID_INDEX) {
					return false;
				}
				fused_output_columns.push_back(fused_idx);
			}
			return true;
		};
		if (!append_columns(left_columns, left_is_link) || !append_columns(right_columns, right_is_link)) {
			return nullptr;
		}

		auto output_bindings = join.GetColumnBindings();
		if (output_bindings.empty() || output_bindings.size() != fused_output_columns.size() ||
		    output_bindings.size() != join.types.size()) {
			return nullptr;
		}

		auto translate_filters = [&](const LogicalGet &source, bool is_link_side) -> unique_ptr<TableFilterSet> {
			auto result = make_uniq<TableFilterSet>();
			for (const auto &entry : source.table_filters.filters) {
				auto mapped_idx = map_relation_column(entry.first, is_link_side);
				if (mapped_idx == DConstants::INVALID_INDEX) {
					return nullptr;
				}
				result->filters[mapped_idx] = entry.second->Copy();
			}
			return result;
		};

		auto link_filters = translate_filters(link_get, true);
		auto relation_filters = translate_filters(relation_get, false);
		if (!link_filters || !relation_filters) {
			return nullptr;
		}

		TableFilterSet fused_filters;
		MergeFilters(fused_filters, *link_filters);
		MergeFilters(fused_filters, *relation_filters);

		column_binding_map_t<idx_t> root_binding_to_fused_column;
		auto register_side_columns = [&](const SidePlanInfo &side, bool is_link_side) {
			for (const auto &entry : side.binding_to_actual_column) {
				auto fused_idx = map_relation_column(entry.second, is_link_side);
				if (fused_idx == DConstants::INVALID_INDEX) {
					return false;
				}
				root_binding_to_fused_column[entry.first] = fused_idx;
			}
			return true;
		};
		if (!register_side_columns(link_side, true) || !register_side_columns(relation_side, false)) {
			return nullptr;
		}

		vector<idx_t> scan_columns;
		scan_columns.reserve(fused_output_columns.size() + fused_filters.filters.size());
		for (auto fused_idx : fused_output_columns) {
			GetOrAddColumn(scan_columns, fused_idx);
		}
		for (const auto &entry : fused_filters.filters) {
			GetOrAddColumn(scan_columns, entry.first);
		}
		auto add_filter_columns = [&](const vector<unique_ptr<Expression>> &filters) {
			for (const auto &expr : filters) {
				bool success = true;
				EnumerateBoundColumnRefs(*expr, [&](BoundColumnRefExpression &column_ref) {
					auto entry = root_binding_to_fused_column.find(column_ref.binding);
					if (entry == root_binding_to_fused_column.end()) {
						success = false;
						return;
					}
					GetOrAddColumn(scan_columns, entry->second);
				});
				if (!success) {
					return false;
				}
			}
			return true;
		};
		if (!add_filter_columns(left_side.hoisted_filters) || !add_filter_columns(right_side.hoisted_filters)) {
			return nullptr;
		}

		auto fused_bind_data = link_get.bind_data->Copy();
		auto &bind_data = fused_bind_data->Cast<LustreQueryBindData>();
		auto fused_get = make_uniq<LogicalGet>(next_table_index, fused_function(bind_data.device_paths.size() > 1),
		                                       std::move(fused_bind_data), fused_types, fused_names);
		next_table_index++;
		vector<ColumnIndex> column_ids;
		column_ids.reserve(scan_columns.size());
		for (auto fused_idx : scan_columns) {
			column_ids.emplace_back(fused_idx);
		}
		fused_get->SetColumnIds(std::move(column_ids));
		fused_get->table_filters = std::move(fused_filters);
		if (join.has_estimated_cardinality) {
			fused_get->SetEstimatedCardinality(join.estimated_cardinality);
		}

		unordered_map<idx_t, idx_t> fused_column_to_scan_position;
		for (idx_t i = 0; i < scan_columns.size(); i++) {
			fused_column_to_scan_position[scan_columns[i]] = i;
		}

		column_binding_map_t<ColumnBinding> root_binding_to_child_binding;
		for (const auto &entry : root_binding_to_fused_column) {
			auto scan_entry = fused_column_to_scan_position.find(entry.second);
			if (scan_entry == fused_column_to_scan_position.end()) {
				continue;
			}
			root_binding_to_child_binding[entry.first] = ColumnBinding(fused_get->table_index, scan_entry->second);
		}

		unique_ptr<LogicalOperator> child_plan = std::move(fused_get);
		vector<unique_ptr<Expression>> hoisted_filters;
		auto append_filters = [&](const vector<unique_ptr<Expression>> &source_filters) {
			for (const auto &expr : source_filters) {
				auto expr_copy = expr->Copy();
				if (!RewriteBindingReferences(expr_copy, root_binding_to_child_binding)) {
					return false;
				}
				hoisted_filters.push_back(std::move(expr_copy));
			}
			return true;
		};
		if (!append_filters(left_side.hoisted_filters) || !append_filters(right_side.hoisted_filters)) {
			return nullptr;
		}
		if (!hoisted_filters.empty()) {
			auto filter = make_uniq<LogicalFilter>();
			filter->expressions = std::move(hoisted_filters);
			filter->children.push_back(std::move(child_plan));
			child_plan = std::move(filter);
		}

		if (scan_columns.size() > fused_output_columns.size()) {
			vector<unique_ptr<Expression>> select_list;
			select_list.reserve(fused_output_columns.size());
			for (idx_t i = 0; i < fused_output_columns.size(); i++) {
				select_list.push_back(
				    make_uniq<BoundColumnRefExpression>(fused_types[scan_columns[i]], ColumnBinding(next_table_index - 1, i)));
			}
			auto projection = make_uniq<LogicalProjection>(next_table_index++, std::move(select_list));
			projection->children.push_back(std::move(child_plan));
			child_plan = std::move(projection);
		}

		auto replacement = make_uniq<LustreFusedInodeLinkJoin>(std::move(child_plan), std::move(output_bindings),
		                                                       join.types, CollectTableIndexes(join.GetColumnBindings()));
		if (join.has_estimated_cardinality) {
			replacement->SetEstimatedCardinality(join.estimated_cardinality);
		}
		return std::move(replacement);
	}

	static unique_ptr<LogicalOperator> TryRewriteObjectLayoutJoin(LogicalComparisonJoin &join, idx_t &next_table_index) {
		if (join.join_type != JoinType::INNER || join.predicate || join.conditions.size() != 2) {
			return nullptr;
		}
		if (join.children.size() != 2) {
			return nullptr;
		}

		SidePlanInfo left_side;
		SidePlanInfo right_side;
		if (!ExtractSidePlan(*join.children[0], left_side) || !ExtractSidePlan(*join.children[1], right_side)) {
			return nullptr;
		}
		if (!left_side.get || !right_side.get) {
			return nullptr;
		}

		auto &left_get = *left_side.get;
		auto &right_get = *right_side.get;
		auto left_kind = GetRelationKind(left_get);
		auto right_kind = GetRelationKind(right_get);
		bool left_is_object = left_kind == FusedRelationKind::OBJECT;
		bool right_is_object = right_kind == FusedRelationKind::OBJECT;
		bool left_is_layout = left_kind == FusedRelationKind::LAYOUT;
		bool right_is_layout = right_kind == FusedRelationKind::LAYOUT;
		if (!(left_is_object && right_is_layout) && !(left_is_layout && right_is_object)) {
			return nullptr;
		}

		auto &object_get = left_is_object ? left_get : right_get;
		auto &layout_get = left_is_object ? right_get : left_get;
		auto &object_side = left_is_object ? left_side : right_side;
		auto &layout_side = left_is_object ? right_side : left_side;

		if (!object_get.bind_data || !layout_get.bind_data) {
			return nullptr;
		}
		if (!BindDataMatches(object_get, layout_get)) {
			return nullptr;
		}
		if (!HasOnlyRewriteSafeFilters(object_get, FusedRelationKind::OBJECT) ||
		    !HasOnlyRewriteSafeFilters(layout_get, FusedRelationKind::LAYOUT)) {
			return nullptr;
		}

		bool has_fid_join = false;
		bool has_comp_index_join = false;
		for (const auto &condition : join.conditions) {
			if (MatchesJoinColumnEquality(condition, object_side, 0, layout_side, 0)) {
				has_fid_join = true;
				continue;
			}
			if (MatchesJoinColumnEquality(condition, object_side, 1, layout_side, 1)) {
				has_comp_index_join = true;
				continue;
			}
			return nullptr;
		}
		if (!has_fid_join || !has_comp_index_join) {
			return nullptr;
		}

		auto left_columns = GetJoinOutputActualColumns(left_side.visible_actual_columns, join.left_projection_map);
		auto right_columns = GetJoinOutputActualColumns(right_side.visible_actual_columns, join.right_projection_map);
		if ((join.left_projection_map.empty() && left_side.root_bindings.empty()) ||
		    (join.right_projection_map.empty() && right_side.root_bindings.empty())) {
			return nullptr;
		}

		vector<idx_t> fused_output_columns;
		fused_output_columns.reserve(left_columns.size() + right_columns.size());

		auto append_columns = [&](const vector<idx_t> &columns, bool is_object_side) {
			for (auto column_idx : columns) {
				auto fused_idx = is_object_side ? MapObjectLayoutObjectColumn(column_idx)
				                                : MapObjectLayoutLayoutColumn(column_idx);
				if (fused_idx == DConstants::INVALID_INDEX) {
					return false;
				}
				fused_output_columns.push_back(fused_idx);
			}
			return true;
		};

		if (!append_columns(left_columns, left_is_object) || !append_columns(right_columns, right_is_object)) {
			return nullptr;
		}

		auto output_bindings = join.GetColumnBindings();
		if (output_bindings.empty() || output_bindings.size() != fused_output_columns.size() ||
		    output_bindings.size() != join.types.size()) {
			return nullptr;
		}

		auto translate_filters = [&](const LogicalGet &source, bool is_object_side) -> unique_ptr<TableFilterSet> {
			auto result = make_uniq<TableFilterSet>();
			for (const auto &entry : source.table_filters.filters) {
				auto mapped_idx = is_object_side ? MapObjectLayoutObjectColumn(entry.first)
				                                 : MapObjectLayoutLayoutColumn(entry.first);
				if (mapped_idx == DConstants::INVALID_INDEX) {
					return nullptr;
				}
				result->filters[mapped_idx] = entry.second->Copy();
			}
			return result;
		};

		auto object_filters = translate_filters(object_get, true);
		auto layout_filters = translate_filters(layout_get, false);
		if (!object_filters || !layout_filters) {
			return nullptr;
		}

		TableFilterSet fused_filters;
		MergeFilters(fused_filters, *object_filters);
		MergeFilters(fused_filters, *layout_filters);

		column_binding_map_t<idx_t> root_binding_to_fused_column;
		auto register_side_columns = [&](const SidePlanInfo &side, bool is_object_side) {
			for (const auto &entry : side.binding_to_actual_column) {
				auto fused_idx = is_object_side ? MapObjectLayoutObjectColumn(entry.second)
				                                : MapObjectLayoutLayoutColumn(entry.second);
				if (fused_idx == DConstants::INVALID_INDEX) {
					return false;
				}
				root_binding_to_fused_column[entry.first] = fused_idx;
			}
			return true;
		};
		if (!register_side_columns(object_side, true) || !register_side_columns(layout_side, false)) {
			return nullptr;
		}

		vector<idx_t> scan_columns;
		scan_columns.reserve(fused_output_columns.size() + fused_filters.filters.size());
		for (auto fused_idx : fused_output_columns) {
			GetOrAddColumn(scan_columns, fused_idx);
		}
		for (const auto &entry : fused_filters.filters) {
			GetOrAddColumn(scan_columns, entry.first);
		}
		auto add_filter_columns = [&](const vector<unique_ptr<Expression>> &filters) {
			for (const auto &expr : filters) {
				bool success = true;
				EnumerateBoundColumnRefs(*expr, [&](BoundColumnRefExpression &column_ref) {
					auto entry = root_binding_to_fused_column.find(column_ref.binding);
					if (entry == root_binding_to_fused_column.end()) {
						success = false;
						return;
					}
					GetOrAddColumn(scan_columns, entry->second);
				});
				if (!success) {
					return false;
				}
			}
			return true;
		};
		if (!add_filter_columns(left_side.hoisted_filters) || !add_filter_columns(right_side.hoisted_filters)) {
			return nullptr;
		}

		auto fused_bind_data = object_get.bind_data->Copy();
		auto &bind_data = fused_bind_data->Cast<LustreQueryBindData>();
		auto fused_function = LustreObjectLayoutsFunction::GetFunction(bind_data.device_paths.size() > 1);
		auto fused_get = make_uniq<LogicalGet>(next_table_index, fused_function, std::move(fused_bind_data),
		                                       LustreObjectLayoutsFunction::GetColumnTypes(),
		                                       LustreObjectLayoutsFunction::GetColumnNames());
		next_table_index++;
		vector<ColumnIndex> column_ids;
		column_ids.reserve(scan_columns.size());
		for (auto fused_idx : scan_columns) {
			column_ids.emplace_back(fused_idx);
		}
		fused_get->SetColumnIds(std::move(column_ids));
		fused_get->table_filters = std::move(fused_filters);
		if (join.has_estimated_cardinality) {
			fused_get->SetEstimatedCardinality(join.estimated_cardinality);
		}

		unordered_map<idx_t, idx_t> fused_column_to_scan_position;
		for (idx_t i = 0; i < scan_columns.size(); i++) {
			fused_column_to_scan_position[scan_columns[i]] = i;
		}

		column_binding_map_t<ColumnBinding> root_binding_to_child_binding;
		for (const auto &entry : root_binding_to_fused_column) {
			auto scan_entry = fused_column_to_scan_position.find(entry.second);
			if (scan_entry == fused_column_to_scan_position.end()) {
				continue;
			}
			root_binding_to_child_binding[entry.first] = ColumnBinding(fused_get->table_index, scan_entry->second);
		}

		unique_ptr<LogicalOperator> child_plan = std::move(fused_get);
		vector<unique_ptr<Expression>> hoisted_filters;
		auto append_filters = [&](const vector<unique_ptr<Expression>> &source_filters) {
			for (const auto &expr : source_filters) {
				auto expr_copy = expr->Copy();
				if (!RewriteBindingReferences(expr_copy, root_binding_to_child_binding)) {
					return false;
				}
				hoisted_filters.push_back(std::move(expr_copy));
			}
			return true;
		};
		if (!append_filters(left_side.hoisted_filters) || !append_filters(right_side.hoisted_filters)) {
			return nullptr;
		}
		if (!hoisted_filters.empty()) {
			auto filter = make_uniq<LogicalFilter>();
			filter->expressions = std::move(hoisted_filters);
			filter->children.push_back(std::move(child_plan));
			child_plan = std::move(filter);
		}

		if (scan_columns.size() > fused_output_columns.size()) {
			vector<unique_ptr<Expression>> select_list;
			select_list.reserve(fused_output_columns.size());
			for (idx_t i = 0; i < fused_output_columns.size(); i++) {
				select_list.push_back(make_uniq<BoundColumnRefExpression>(
				    LustreObjectLayoutsFunction::GetColumnTypes()[scan_columns[i]], ColumnBinding(next_table_index - 1, i)));
			}
			auto projection = make_uniq<LogicalProjection>(next_table_index++, std::move(select_list));
			projection->children.push_back(std::move(child_plan));
			child_plan = std::move(projection);
		}

		auto replacement = make_uniq<LustreFusedInodeLinkJoin>(std::move(child_plan), std::move(output_bindings),
		                                                       join.types, CollectTableIndexes(join.GetColumnBindings()));
		if (join.has_estimated_cardinality) {
			replacement->SetEstimatedCardinality(join.estimated_cardinality);
		}
		return std::move(replacement);
	}

	static unique_ptr<LogicalOperator> TryRewriteJoin(LogicalComparisonJoin &join, idx_t &next_table_index) {
		if (join.join_type != JoinType::INNER || join.predicate || join.conditions.size() != 1) {
			return nullptr;
		}
		if (join.children.size() != 2) {
			return nullptr;
		}

		auto &condition = join.conditions[0];
		if (condition.comparison != ExpressionType::COMPARE_EQUAL) {
			return nullptr;
		}

		SidePlanInfo left_side;
		SidePlanInfo right_side;
		if (!ExtractSidePlan(*join.children[0], left_side) || !ExtractSidePlan(*join.children[1], right_side)) {
			return nullptr;
		}
		if (!left_side.get || !right_side.get) {
			return nullptr;
		}

		auto &left_get = *left_side.get;
		auto &right_get = *right_side.get;
		auto left_kind = GetRelationKind(left_get);
		auto right_kind = GetRelationKind(right_get);
		if (left_kind == FusedRelationKind::OTHER || right_kind == FusedRelationKind::OTHER) {
			return nullptr;
		}
		auto left_is_inode = left_kind == FusedRelationKind::INODE;
		auto right_is_inode = right_kind == FusedRelationKind::INODE;
		if (left_is_inode == right_is_inode) {
			return nullptr;
		}

		auto &inode_get = left_is_inode ? left_get : right_get;
		auto &relation_get = left_is_inode ? right_get : left_get;
		auto &inode_side = left_is_inode ? left_side : right_side;
		auto &relation_side = left_is_inode ? right_side : left_side;
		auto relation_kind = left_is_inode ? right_kind : left_kind;

		if (!inode_get.bind_data || !relation_get.bind_data) {
			return nullptr;
		}
		if (!BindDataMatches(inode_get, relation_get)) {
			return nullptr;
		}
		if (!HasOnlyRewriteSafeFilters(inode_get, FusedRelationKind::INODE) ||
		    !HasOnlyRewriteSafeFilters(relation_get, relation_kind)) {
			return nullptr;
		}

		bool join_on_parent_fid = false;
		if (relation_kind == FusedRelationKind::LINK) {
			if (MatchesJoinColumnEquality(condition, inode_side, 0, relation_side, 0)) {
				join_on_parent_fid = false;
			} else if (MatchesJoinColumnEquality(condition, inode_side, 0, relation_side, 1)) {
				join_on_parent_fid = true;
			} else {
				return nullptr;
			}
		} else if (!MatchesJoinColumnEquality(condition, inode_side, 0, relation_side, 0)) {
			return nullptr;
		}

		auto left_columns = GetJoinOutputActualColumns(left_side.visible_actual_columns, join.left_projection_map);
		auto right_columns = GetJoinOutputActualColumns(right_side.visible_actual_columns, join.right_projection_map);
		if ((join.left_projection_map.empty() && left_side.root_bindings.empty()) ||
		    (join.right_projection_map.empty() && right_side.root_bindings.empty())) {
			return nullptr;
		}

		vector<idx_t> fused_output_columns;
		fused_output_columns.reserve(left_columns.size() + right_columns.size());

		auto append_columns = [&](const vector<idx_t> &columns, FusedRelationKind kind) {
			for (auto column_idx : columns) {
				auto fused_idx = MapRelationColumn(column_idx, kind);
				if (fused_idx == DConstants::INVALID_INDEX) {
					return false;
				}
				fused_output_columns.push_back(fused_idx);
			}
			return true;
		};

		if (!append_columns(left_columns, left_kind)) {
			return nullptr;
		}
		if (!append_columns(right_columns, right_kind)) {
			return nullptr;
		}

		auto output_bindings = join.GetColumnBindings();
		if (output_bindings.empty() || output_bindings.size() != fused_output_columns.size() ||
		    output_bindings.size() != join.types.size()) {
			return nullptr;
		}

		auto inode_filters = TranslateFilters(inode_get, FusedRelationKind::INODE);
		auto relation_filters = TranslateFilters(relation_get, relation_kind);
		if (!inode_filters || !relation_filters) {
			return nullptr;
		}

		TableFilterSet fused_filters;
		MergeFilters(fused_filters, *inode_filters);
		MergeFilters(fused_filters, *relation_filters);

		column_binding_map_t<idx_t> root_binding_to_fused_column;
		auto register_side_columns = [&](const SidePlanInfo &side, FusedRelationKind kind) {
			for (const auto &entry : side.binding_to_actual_column) {
				auto fused_idx = MapRelationColumn(entry.second, kind);
				if (fused_idx == DConstants::INVALID_INDEX) {
					return false;
				}
				root_binding_to_fused_column[entry.first] = fused_idx;
			}
			return true;
		};
		if (!register_side_columns(inode_side, FusedRelationKind::INODE) ||
		    !register_side_columns(relation_side, relation_kind)) {
			return nullptr;
		}

		vector<idx_t> scan_columns;
		scan_columns.reserve(fused_output_columns.size() + fused_filters.filters.size());
		for (auto fused_idx : fused_output_columns) {
			GetOrAddColumn(scan_columns, fused_idx);
		}
		for (const auto &entry : fused_filters.filters) {
			GetOrAddColumn(scan_columns, entry.first);
		}
		auto add_filter_columns = [&](const vector<unique_ptr<Expression>> &filters) {
			for (const auto &expr : filters) {
				bool success = true;
				EnumerateBoundColumnRefs(*expr, [&](BoundColumnRefExpression &column_ref) {
					auto entry = root_binding_to_fused_column.find(column_ref.binding);
					if (entry == root_binding_to_fused_column.end()) {
						success = false;
						return;
					}
					GetOrAddColumn(scan_columns, entry->second);
				});
				if (!success) {
					return false;
				}
			}
			return true;
		};
		if (!add_filter_columns(left_side.hoisted_filters) || !add_filter_columns(right_side.hoisted_filters)) {
			return nullptr;
		}

		auto fused_bind_data = inode_get.bind_data->Copy();
		auto &bind_data = fused_bind_data->Cast<LustreQueryBindData>();
		bind_data.inode_link_join_on_parent_fid = relation_kind == FusedRelationKind::LINK && join_on_parent_fid;
		auto fused_function = GetFusedFunction(relation_kind, bind_data.device_paths.size() > 1);
		auto fused_get = make_uniq<LogicalGet>(next_table_index, fused_function, std::move(fused_bind_data),
		                                       GetFusedColumnTypes(relation_kind),
		                                       GetFusedColumnNames(relation_kind));
		next_table_index++;
		vector<ColumnIndex> column_ids;
		column_ids.reserve(scan_columns.size());
		for (auto fused_idx : scan_columns) {
			column_ids.emplace_back(fused_idx);
		}
		fused_get->SetColumnIds(std::move(column_ids));
		fused_get->table_filters = std::move(fused_filters);
		if (join.has_estimated_cardinality) {
			fused_get->SetEstimatedCardinality(join.estimated_cardinality);
		}

		unordered_map<idx_t, idx_t> fused_column_to_scan_position;
		for (idx_t i = 0; i < scan_columns.size(); i++) {
			fused_column_to_scan_position[scan_columns[i]] = i;
		}

		column_binding_map_t<ColumnBinding> root_binding_to_child_binding;
		for (const auto &entry : root_binding_to_fused_column) {
			auto scan_entry = fused_column_to_scan_position.find(entry.second);
			if (scan_entry == fused_column_to_scan_position.end()) {
				continue;
			}
			root_binding_to_child_binding[entry.first] = ColumnBinding(fused_get->table_index, scan_entry->second);
		}

		unique_ptr<LogicalOperator> child_plan = std::move(fused_get);
		vector<unique_ptr<Expression>> hoisted_filters;
		auto append_filters = [&](const vector<unique_ptr<Expression>> &source_filters) {
			for (const auto &expr : source_filters) {
				auto expr_copy = expr->Copy();
				if (!RewriteBindingReferences(expr_copy, root_binding_to_child_binding)) {
					return false;
				}
				hoisted_filters.push_back(std::move(expr_copy));
			}
			return true;
		};
		if (!append_filters(left_side.hoisted_filters) || !append_filters(right_side.hoisted_filters)) {
			return nullptr;
		}
		if (!hoisted_filters.empty()) {
			auto filter = make_uniq<LogicalFilter>();
			filter->expressions = std::move(hoisted_filters);
			filter->children.push_back(std::move(child_plan));
			child_plan = std::move(filter);
		}

		if (scan_columns.size() > fused_output_columns.size()) {
			vector<unique_ptr<Expression>> select_list;
			select_list.reserve(fused_output_columns.size());
			for (idx_t i = 0; i < fused_output_columns.size(); i++) {
				select_list.push_back(make_uniq<BoundColumnRefExpression>(GetFusedColumnTypes(relation_kind)[scan_columns[i]],
				                                                         ColumnBinding(next_table_index - 1, i)));
			}
			auto projection = make_uniq<LogicalProjection>(next_table_index++, std::move(select_list));
			projection->children.push_back(std::move(child_plan));
			child_plan = std::move(projection);
		}

		auto replacement = make_uniq<LustreFusedInodeLinkJoin>(std::move(child_plan), std::move(output_bindings),
		                                                       join.types, CollectTableIndexes(join.GetColumnBindings()));
		if (join.has_estimated_cardinality) {
			replacement->SetEstimatedCardinality(join.estimated_cardinality);
		}
		return std::move(replacement);
	}

	// Helper: build a fused LogicalGet + wrapper from a 2-way dirmap join.
	// map_left_fn / map_right_fn translate original column indices to fused column indices.
	// fused_types / fused_names / fused_function describe the target fused table function.
	static unique_ptr<LogicalOperator> BuildDirMapFusedPlan(
	    LogicalComparisonJoin &join, SidePlanInfo &left_side, SidePlanInfo &right_side,
	    LogicalGet &left_get, LogicalGet &right_get,
	    const std::function<idx_t(idx_t, bool)> &map_column,
	    const vector<LogicalType> &fused_types, const vector<string> &fused_names,
	    TableFunction fused_function, idx_t &next_table_index) {

		auto left_columns = GetJoinOutputActualColumns(left_side.visible_actual_columns, join.left_projection_map);
		auto right_columns = GetJoinOutputActualColumns(right_side.visible_actual_columns, join.right_projection_map);
		if ((join.left_projection_map.empty() && left_side.root_bindings.empty()) ||
		    (join.right_projection_map.empty() && right_side.root_bindings.empty())) {
			return nullptr;
		}

		vector<idx_t> fused_output_columns;
		fused_output_columns.reserve(left_columns.size() + right_columns.size());
		auto append = [&](const vector<idx_t> &cols, bool is_left) {
			for (auto c : cols) {
				auto f = map_column(c, is_left);
				if (f == DConstants::INVALID_INDEX) return false;
				fused_output_columns.push_back(f);
			}
			return true;
		};
		if (!append(left_columns, true) || !append(right_columns, false)) return nullptr;

		auto output_bindings = join.GetColumnBindings();
		if (output_bindings.empty() || output_bindings.size() != fused_output_columns.size()) return nullptr;

		auto translate_filters = [&](const LogicalGet &src, bool is_left) -> unique_ptr<TableFilterSet> {
			auto r = make_uniq<TableFilterSet>();
			for (const auto &e : src.table_filters.filters) {
				auto m = map_column(e.first, is_left);
				if (m == DConstants::INVALID_INDEX) return nullptr;
				r->filters[m] = e.second->Copy();
			}
			return r;
		};
		auto lf = translate_filters(left_get, true);
		auto rf = translate_filters(right_get, false);
		if (!lf || !rf) return nullptr;

		TableFilterSet fused_filters;
		MergeFilters(fused_filters, *lf);
		MergeFilters(fused_filters, *rf);

		column_binding_map_t<idx_t> root_binding_to_fused;
		auto reg = [&](const SidePlanInfo &side, bool is_left) {
			for (const auto &e : side.binding_to_actual_column) {
				auto f = map_column(e.second, is_left);
				if (f == DConstants::INVALID_INDEX) return false;
				root_binding_to_fused[e.first] = f;
			}
			return true;
		};
		if (!reg(left_side, true) || !reg(right_side, false)) return nullptr;

		vector<idx_t> scan_columns;
		scan_columns.reserve(fused_output_columns.size() + fused_filters.filters.size());
		for (auto f : fused_output_columns) GetOrAddColumn(scan_columns, f);
		for (const auto &e : fused_filters.filters) GetOrAddColumn(scan_columns, e.first);
		auto add_fc = [&](const vector<unique_ptr<Expression>> &filters) {
			for (const auto &expr : filters) {
				bool ok = true;
				EnumerateBoundColumnRefs(*expr, [&](BoundColumnRefExpression &cr) {
					auto it = root_binding_to_fused.find(cr.binding);
					if (it == root_binding_to_fused.end()) { ok = false; return; }
					GetOrAddColumn(scan_columns, it->second);
				});
				if (!ok) return false;
			}
			return true;
		};
		if (!add_fc(left_side.hoisted_filters) || !add_fc(right_side.hoisted_filters)) return nullptr;

		auto fused_bind = left_get.bind_data->Copy();
		auto fused_get = make_uniq<LogicalGet>(next_table_index, fused_function, std::move(fused_bind),
		                                       fused_types, fused_names);
		next_table_index++;
		vector<ColumnIndex> col_ids;
		for (auto f : scan_columns) col_ids.emplace_back(f);
		fused_get->SetColumnIds(std::move(col_ids));
		fused_get->table_filters = std::move(fused_filters);
		if (join.has_estimated_cardinality) fused_get->SetEstimatedCardinality(join.estimated_cardinality);

		unordered_map<idx_t, idx_t> fused_to_scan;
		for (idx_t i = 0; i < scan_columns.size(); i++) fused_to_scan[scan_columns[i]] = i;

		column_binding_map_t<ColumnBinding> root_to_child;
		for (const auto &e : root_binding_to_fused) {
			auto it = fused_to_scan.find(e.second);
			if (it != fused_to_scan.end())
				root_to_child[e.first] = ColumnBinding(fused_get->table_index, it->second);
		}

		unique_ptr<LogicalOperator> child_plan = std::move(fused_get);
		vector<unique_ptr<Expression>> hoisted;
		auto app = [&](const vector<unique_ptr<Expression>> &src) {
			for (const auto &expr : src) {
				auto c = expr->Copy();
				if (!RewriteBindingReferences(c, root_to_child)) return false;
				hoisted.push_back(std::move(c));
			}
			return true;
		};
		if (!app(left_side.hoisted_filters) || !app(right_side.hoisted_filters)) return nullptr;
		if (!hoisted.empty()) {
			auto f = make_uniq<LogicalFilter>();
			f->expressions = std::move(hoisted);
			f->children.push_back(std::move(child_plan));
			child_plan = std::move(f);
		}

		if (scan_columns.size() > fused_output_columns.size()) {
			vector<unique_ptr<Expression>> sl;
			sl.reserve(fused_output_columns.size());
			for (idx_t i = 0; i < fused_output_columns.size(); i++) {
				sl.push_back(make_uniq<BoundColumnRefExpression>(fused_types[scan_columns[i]],
				             ColumnBinding(next_table_index - 1, i)));
			}
			auto proj = make_uniq<LogicalProjection>(next_table_index++, std::move(sl));
			proj->children.push_back(std::move(child_plan));
			child_plan = std::move(proj);
		}

		auto replacement = make_uniq<LustreFusedInodeLinkJoin>(std::move(child_plan), std::move(output_bindings),
		                                                       join.types, CollectTableIndexes(join.GetColumnBindings()));
		if (join.has_estimated_cardinality) replacement->SetEstimatedCardinality(join.estimated_cardinality);
		return std::move(replacement);
	}

	// Level 1: links JOIN dirmap ON parent_fid → link_dirmap
	// Level 2: inodes JOIN dirmap ON fid = dir_fid → inode_dirmap
	static unique_ptr<LogicalOperator> TryRewriteDirMapJoin(LogicalComparisonJoin &join, idx_t &next_table_index) {
		if (join.join_type != JoinType::INNER || join.predicate || join.conditions.size() != 1) return nullptr;
		if (join.children.size() != 2) return nullptr;

		SidePlanInfo left_side, right_side;
		if (!ExtractSidePlan(*join.children[0], left_side) || !ExtractSidePlan(*join.children[1], right_side)) return nullptr;
		if (!left_side.get || !right_side.get) return nullptr;

		auto &left_get = *left_side.get;
		auto &right_get = *right_side.get;
		auto left_kind = GetRelationKind(left_get);
		auto right_kind = GetRelationKind(right_get);

		bool left_is_dirmap = left_kind == FusedRelationKind::DIRMAP;
		bool right_is_dirmap = right_kind == FusedRelationKind::DIRMAP;
		if (left_is_dirmap == right_is_dirmap) return nullptr;

		auto other_kind = left_is_dirmap ? right_kind : left_kind;
		auto &dirmap_get = left_is_dirmap ? left_get : right_get;
		auto &other_get = left_is_dirmap ? right_get : left_get;
		auto &dirmap_side = left_is_dirmap ? left_side : right_side;
		auto &other_side = left_is_dirmap ? right_side : left_side;

		if (!dirmap_get.bind_data || !other_get.bind_data) return nullptr;
		if (!BindDataDevicePathsMatch(dirmap_get, other_get)) return nullptr;
		if (!HasOnlyRewriteSafeFilters(dirmap_get, FusedRelationKind::DIRMAP)) return nullptr;

		auto &cond = join.conditions[0];
		if (cond.comparison != ExpressionType::COMPARE_EQUAL) return nullptr;

		if (other_kind == FusedRelationKind::LINK) {
			// Level 1: links.parent_fid(1) = dirmap.parent_fid(1)
			if (!HasOnlyRewriteSafeFilters(other_get, FusedRelationKind::LINK)) return nullptr;
			if (!MatchesJoinColumnEquality(cond, other_side, 1, dirmap_side, 1)) return nullptr;

			auto &bind = other_get.bind_data->Cast<LustreQueryBindData>();
			auto map_col = [&](idx_t c, bool is_left) -> idx_t {
				bool is_link = (is_left && !left_is_dirmap) || (!is_left && left_is_dirmap);
				return is_link ? MapLinkDirMapLinkColumn(c) : MapLinkDirMapDirMapColumn(c);
			};
			return BuildDirMapFusedPlan(join, left_side, right_side, left_get, right_get, map_col,
			                            LustreLinkDirMapFunction::GetColumnTypes(),
			                            LustreLinkDirMapFunction::GetColumnNames(),
			                            LustreLinkDirMapFunction::GetFunction(bind.device_paths.size() > 1),
			                            next_table_index);
		}

		if (other_kind == FusedRelationKind::INODE) {
			// Level 2: inodes.fid(0) = dirmap.dir_fid(0)
			if (!HasOnlyRewriteSafeFilters(other_get, FusedRelationKind::INODE)) return nullptr;
			if (!MatchesJoinColumnEquality(cond, other_side, 0, dirmap_side, 0)) return nullptr;

			auto &bind = other_get.bind_data->Cast<LustreQueryBindData>();
			auto map_col = [&](idx_t c, bool is_left) -> idx_t {
				bool is_inode = (is_left && !left_is_dirmap) || (!is_left && left_is_dirmap);
				return is_inode ? MapInodeDirMapInodeColumn(c) : MapInodeDirMapDirMapColumn(c);
			};
			return BuildDirMapFusedPlan(join, left_side, right_side, left_get, right_get, map_col,
			                            LustreInodeDirMapFunction::GetColumnTypes(),
			                            LustreInodeDirMapFunction::GetColumnNames(),
			                            LustreInodeDirMapFunction::GetFunction(bind.device_paths.size() > 1),
			                            next_table_index);
		}

		return nullptr;
	}

	// Level 3: 3-way detection. Detect before children are rewritten.
	// Pattern: JOIN { JOIN { links, dirmap }, inodes } or any permutation.
	static unique_ptr<LogicalOperator> TryRewrite3WayDirMapJoin(LogicalComparisonJoin &outer_join,
	                                                            idx_t &next_table_index) {
		if (outer_join.join_type != JoinType::INNER || outer_join.predicate || outer_join.conditions.size() != 1) {
			return nullptr;
		}
		if (outer_join.children.size() != 2) return nullptr;

		// One child must be another comparison join
		LogicalComparisonJoin *inner_join = nullptr;
		LogicalOperator *third_op = nullptr;
		bool inner_is_left = false;

		if (outer_join.children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			inner_join = &outer_join.children[0]->Cast<LogicalComparisonJoin>();
			third_op = outer_join.children[1].get();
			inner_is_left = true;
		} else if (outer_join.children[1]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			inner_join = &outer_join.children[1]->Cast<LogicalComparisonJoin>();
			third_op = outer_join.children[0].get();
			inner_is_left = false;
		} else {
			return nullptr;
		}

		if (inner_join->join_type != JoinType::INNER || inner_join->predicate || inner_join->conditions.size() != 1) {
			return nullptr;
		}

		// Extract all three leaf tables
		SidePlanInfo side_a, side_b, side_c;
		if (!ExtractSidePlan(*inner_join->children[0], side_a) ||
		    !ExtractSidePlan(*inner_join->children[1], side_b) ||
		    !ExtractSidePlan(*third_op, side_c)) {
			return nullptr;
		}
		if (!side_a.get || !side_b.get || !side_c.get) return nullptr;

		auto kind_a = GetRelationKind(*side_a.get);
		auto kind_b = GetRelationKind(*side_b.get);
		auto kind_c = GetRelationKind(*side_c.get);

		// Identify link, dirmap, inode sides
		SidePlanInfo *link_side = nullptr, *dirmap_side = nullptr, *inode_side = nullptr;
		struct { SidePlanInfo *s; FusedRelationKind k; } sides[] = {
			{&side_a, kind_a}, {&side_b, kind_b}, {&side_c, kind_c}
		};
		for (auto &e : sides) {
			if (e.k == FusedRelationKind::LINK && !link_side) link_side = e.s;
			else if (e.k == FusedRelationKind::DIRMAP && !dirmap_side) dirmap_side = e.s;
			else if (e.k == FusedRelationKind::INODE && !inode_side) inode_side = e.s;
		}
		if (!link_side || !dirmap_side || !inode_side) return nullptr;

		auto *link_get = link_side->get;
		auto *dirmap_get = dirmap_side->get;
		auto *inode_get = inode_side->get;
		if (!link_get->bind_data || !dirmap_get->bind_data || !inode_get->bind_data) return nullptr;
		if (!BindDataDevicePathsMatch(*link_get, *dirmap_get) || !BindDataDevicePathsMatch(*link_get, *inode_get)) return nullptr;
		if (!HasOnlyRewriteSafeFilters(*link_get, FusedRelationKind::LINK) ||
		    !HasOnlyRewriteSafeFilters(*dirmap_get, FusedRelationKind::DIRMAP) ||
		    !HasOnlyRewriteSafeFilters(*inode_get, FusedRelationKind::INODE)) {
			return nullptr;
		}

		// Verify join conditions:
		// Need: links.parent_fid(1) = dirmap.parent_fid(1)  AND  dirmap.dir_fid(0) = inodes.fid(0)
		// These may be split across inner/outer joins in any order.
		auto &inner_cond = inner_join->conditions[0];
		auto &outer_cond = outer_join.conditions[0];
		if (inner_cond.comparison != ExpressionType::COMPARE_EQUAL ||
		    outer_cond.comparison != ExpressionType::COMPARE_EQUAL) {
			return nullptr;
		}

		// The inner join connects two of: {side_a, side_b}
		// The outer join connects {inner result, side_c}
		// We need to find which condition is link-dirmap and which is dirmap-inode.

		// Inner join sides are side_a and side_b. Check both possible conditions.
		bool inner_has_link_dirmap = false;
		bool inner_has_dirmap_inode = false;

		// Check if inner connects link.parent_fid = dirmap.parent_fid
		if (MatchesJoinColumnEquality(inner_cond, *link_side, 1, *dirmap_side, 1) &&
		    (link_side == &side_a || link_side == &side_b) &&
		    (dirmap_side == &side_a || dirmap_side == &side_b)) {
			inner_has_link_dirmap = true;
		}
		// Check if inner connects dirmap.dir_fid = inode.fid
		if (MatchesJoinColumnEquality(inner_cond, *dirmap_side, 0, *inode_side, 0) &&
		    (dirmap_side == &side_a || dirmap_side == &side_b) &&
		    (inode_side == &side_a || inode_side == &side_b)) {
			inner_has_dirmap_inode = true;
		}

		// Now check the outer condition connects {inner_result, side_c}
		// The outer condition must reference columns from both the inner and outer sides.
		// The inner result exposes bindings from side_a and side_b (through the inner join).
		// side_c is the third table.

		bool outer_has_link_dirmap = false;
		bool outer_has_dirmap_inode = false;

		// For outer condition, the "inner join side" includes both side_a and side_b bindings.
		// We need to check if the outer condition matches the remaining pattern.
		// The outer join's left = inner join result, right = side_c (or vice versa).
		// side_c's root_bindings are accessible. For inner result, we use side_a and side_b.

		// Check outer: link.parent_fid(1) = dirmap.parent_fid(1)
		if (MatchesJoinColumnEquality(outer_cond, *link_side, 1, *dirmap_side, 1)) {
			outer_has_link_dirmap = true;
		}
		// Check outer: dirmap.dir_fid(0) = inode.fid(0)
		if (MatchesJoinColumnEquality(outer_cond, *dirmap_side, 0, *inode_side, 0)) {
			outer_has_dirmap_inode = true;
		}

		// We need exactly one link-dirmap and one dirmap-inode condition
		bool has_both = (inner_has_link_dirmap && outer_has_dirmap_inode) ||
		                (inner_has_dirmap_inode && outer_has_link_dirmap);
		if (!has_both) return nullptr;

		// Build the 3-way fused function
		// Output: link columns from outer join + dirmap columns + inode columns
		// We need to compute output_columns for all three sides.
		auto &bind = link_get->bind_data->Cast<LustreQueryBindData>();

		// Compute the outer join's output column mapping
		auto outer_left_columns = GetJoinOutputActualColumns(
			(inner_is_left ? inner_join->GetColumnBindings().size() > 0 ? std::vector<idx_t>{} : std::vector<idx_t>{} : side_c.visible_actual_columns),
			{});

		// This is getting very complex for a general solution.
		// Let's take a pragmatic shortcut: if the pattern matches, build the fused plan
		// by mapping all three sides' visible columns to the fused layout.

		auto map_col_3way = [&](idx_t c, FusedRelationKind kind) -> idx_t {
			switch (kind) {
			case FusedRelationKind::LINK: return MapLinkInodeDirMapLinkColumn(c);
			case FusedRelationKind::DIRMAP: return MapLinkInodeDirMapDirMapColumn(c);
			case FusedRelationKind::INODE: return MapLinkInodeDirMapInodeColumn(c);
			default: return DConstants::INVALID_INDEX;
			}
		};

		// Gather all output columns from the outer join
		auto outer_output_bindings = outer_join.GetColumnBindings();
		if (outer_output_bindings.empty()) return nullptr;

		// Map each outer output binding to a fused column
		// The outer join's output bindings reference the inner join's output + side_c's output
		// But inner join's output references side_a + side_b
		// So ultimately all bindings trace back to one of the three leaf tables.

		// Build a unified mapping: any binding from any side → fused column
		column_binding_map_t<idx_t> all_binding_to_fused;
		for (const auto &e : link_side->binding_to_actual_column) {
			auto f = map_col_3way(e.second, FusedRelationKind::LINK);
			if (f != DConstants::INVALID_INDEX) all_binding_to_fused[e.first] = f;
		}
		for (const auto &e : dirmap_side->binding_to_actual_column) {
			auto f = map_col_3way(e.second, FusedRelationKind::DIRMAP);
			if (f != DConstants::INVALID_INDEX) all_binding_to_fused[e.first] = f;
		}
		for (const auto &e : inode_side->binding_to_actual_column) {
			auto f = map_col_3way(e.second, FusedRelationKind::INODE);
			if (f != DConstants::INVALID_INDEX) all_binding_to_fused[e.first] = f;
		}

		// Trace outer join output bindings through inner join to leaf tables
		// outer join left side = inner join output or side_c
		// We need the projection maps from both joins.
		auto inner_output_bindings = inner_join->GetColumnBindings();
		auto outer_left_bindings_raw = inner_is_left ? inner_output_bindings : side_c.root_bindings;
		auto outer_right_bindings_raw = inner_is_left ? side_c.root_bindings : inner_output_bindings;

		// Apply outer join projection maps
		auto get_proj = [](const vector<ColumnBinding> &bindings, const vector<idx_t> &proj_map) {
			if (proj_map.empty()) return bindings;
			vector<ColumnBinding> result;
			for (auto p : proj_map) {
				if (p < bindings.size()) result.push_back(bindings[p]);
			}
			return result;
		};
		auto outer_left_bindings = get_proj(outer_left_bindings_raw, outer_join.left_projection_map);
		auto outer_right_bindings = get_proj(outer_right_bindings_raw, outer_join.right_projection_map);

		vector<idx_t> fused_output_columns;
		auto trace_binding = [&](const ColumnBinding &b) -> idx_t {
			auto it = all_binding_to_fused.find(b);
			if (it != all_binding_to_fused.end()) return it->second;

			// May be from inner join output - trace through inner join
			for (idx_t i = 0; i < inner_output_bindings.size(); i++) {
				if (inner_output_bindings[i] == b) {
					// This inner output came from side_a or side_b
					// Inner join's left/right projection maps
					auto inner_left_bindings_raw = side_a.root_bindings;
					auto inner_right_bindings_raw = side_b.root_bindings;
					auto inner_left = get_proj(inner_left_bindings_raw, inner_join->left_projection_map);
					auto inner_right = get_proj(inner_right_bindings_raw, inner_join->right_projection_map);
					if (i < inner_left.size()) {
						auto it2 = all_binding_to_fused.find(inner_left[i]);
						if (it2 != all_binding_to_fused.end()) return it2->second;
					} else if (i - inner_left.size() < inner_right.size()) {
						auto it2 = all_binding_to_fused.find(inner_right[i - inner_left.size()]);
						if (it2 != all_binding_to_fused.end()) return it2->second;
					}
				}
			}
			return DConstants::INVALID_INDEX;
		};

		for (auto &b : outer_left_bindings) {
			auto f = trace_binding(b);
			if (f == DConstants::INVALID_INDEX) return nullptr;
			fused_output_columns.push_back(f);
		}
		for (auto &b : outer_right_bindings) {
			auto f = trace_binding(b);
			if (f == DConstants::INVALID_INDEX) return nullptr;
			fused_output_columns.push_back(f);
		}

		if (fused_output_columns.size() != outer_output_bindings.size()) return nullptr;

		// Build the fused LogicalGet
		auto &fused_types_ref = LustreLinkInodeDirMapFunction::GetColumnTypes();
		auto &fused_names_ref = LustreLinkInodeDirMapFunction::GetColumnNames();
		auto fused_function = LustreLinkInodeDirMapFunction::GetFunction(bind.device_paths.size() > 1);

		vector<idx_t> scan_columns;
		for (auto f : fused_output_columns) GetOrAddColumn(scan_columns, f);

		// Translate filters from all three tables
		for (const auto &e : link_get->table_filters.filters) {
			auto m = map_col_3way(e.first, FusedRelationKind::LINK);
			if (m != DConstants::INVALID_INDEX) GetOrAddColumn(scan_columns, m);
		}
		for (const auto &e : dirmap_get->table_filters.filters) {
			auto m = map_col_3way(e.first, FusedRelationKind::DIRMAP);
			if (m != DConstants::INVALID_INDEX) GetOrAddColumn(scan_columns, m);
		}
		for (const auto &e : inode_get->table_filters.filters) {
			auto m = map_col_3way(e.first, FusedRelationKind::INODE);
			if (m != DConstants::INVALID_INDEX) GetOrAddColumn(scan_columns, m);
		}

		auto fused_bind = link_get->bind_data->Copy();
		auto fused_get = make_uniq<LogicalGet>(next_table_index, fused_function, std::move(fused_bind),
		                                       fused_types_ref, fused_names_ref);
		next_table_index++;

		vector<ColumnIndex> col_ids;
		for (auto f : scan_columns) col_ids.emplace_back(f);
		fused_get->SetColumnIds(std::move(col_ids));

		// Merge all filters
		TableFilterSet fused_filters;
		for (const auto &e : link_get->table_filters.filters) {
			auto m = map_col_3way(e.first, FusedRelationKind::LINK);
			if (m != DConstants::INVALID_INDEX) fused_filters.filters[m] = e.second->Copy();
		}
		for (const auto &e : dirmap_get->table_filters.filters) {
			auto m = map_col_3way(e.first, FusedRelationKind::DIRMAP);
			if (m != DConstants::INVALID_INDEX) fused_filters.filters[m] = e.second->Copy();
		}
		for (const auto &e : inode_get->table_filters.filters) {
			auto m = map_col_3way(e.first, FusedRelationKind::INODE);
			if (m != DConstants::INVALID_INDEX) fused_filters.filters[m] = e.second->Copy();
		}
		fused_get->table_filters = std::move(fused_filters);

		if (outer_join.has_estimated_cardinality) {
			fused_get->SetEstimatedCardinality(outer_join.estimated_cardinality);
		}

		// Build scan position map
		unordered_map<idx_t, idx_t> fused_to_scan;
		for (idx_t i = 0; i < scan_columns.size(); i++) fused_to_scan[scan_columns[i]] = i;

		// Map outer output bindings to fused scan positions
		// Use the traced fused_output_columns
		unique_ptr<LogicalOperator> child_plan = std::move(fused_get);

		// Add projection if scan has extra columns
		if (scan_columns.size() > fused_output_columns.size()) {
			vector<unique_ptr<Expression>> sl;
			for (idx_t i = 0; i < fused_output_columns.size(); i++) {
				auto it = fused_to_scan.find(fused_output_columns[i]);
				if (it == fused_to_scan.end()) return nullptr;
				sl.push_back(make_uniq<BoundColumnRefExpression>(fused_types_ref[fused_output_columns[i]],
				             ColumnBinding(next_table_index - 1, it->second)));
			}
			auto proj = make_uniq<LogicalProjection>(next_table_index++, std::move(sl));
			proj->children.push_back(std::move(child_plan));
			child_plan = std::move(proj);
		}

		auto replacement = make_uniq<LustreFusedInodeLinkJoin>(std::move(child_plan), std::move(outer_output_bindings),
		                                                       outer_join.types,
		                                                       CollectTableIndexes(outer_join.GetColumnBindings()));
		if (outer_join.has_estimated_cardinality) {
			replacement->SetEstimatedCardinality(outer_join.estimated_cardinality);
		}
		return std::move(replacement);
	}

	static void RewriteJoins(unique_ptr<LogicalOperator> &op, idx_t &next_table_index) {
		// Try 3-way dirmap rewrite BEFORE processing children (pre-emptive)
		if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto replacement = TryRewrite3WayDirMapJoin(op->Cast<LogicalComparisonJoin>(), next_table_index);
			if (replacement) {
				op = std::move(replacement);
				return;
			}
		}

		for (auto &child : op->children) {
			RewriteJoins(child, next_table_index);
		}
		if (op->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			return;
		}
		auto replacement = TryRewriteDirMapJoin(op->Cast<LogicalComparisonJoin>(), next_table_index);
		if (!replacement) {
			replacement = TryRewriteLinkRelationJoin(op->Cast<LogicalComparisonJoin>(), next_table_index);
		}
		if (!replacement) {
			replacement = TryRewriteObjectLayoutJoin(op->Cast<LogicalComparisonJoin>(), next_table_index);
		}
		if (!replacement) {
			replacement = TryRewriteJoin(op->Cast<LogicalComparisonJoin>(), next_table_index);
		}
		if (replacement) {
			op = std::move(replacement);
		}
	}

	static void RewritePlan(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
		idx_t next_table_index = FindMaxTableIndex(*plan) + 1;
		RewriteJoins(plan, next_table_index);
	}
};

void RegisterLustreOptimizer(ExtensionLoader &loader) {
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	OptimizerExtension::Register(config, LustreOptimizerExtension());
}

} // namespace lustre
} // namespace duckdb
