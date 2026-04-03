//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_fid2path.cpp
//
// Scalar function: FID -> filesystem path via LinkEA traversal
//===----------------------------------------------------------------------===//

#include "lustre_fid2path.hpp"
#include "lustre_types.hpp"
#include "mdt_scanner.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {
namespace lustre {

//===----------------------------------------------------------------------===//
// Root FID: [0x200000007:1:0]
//===----------------------------------------------------------------------===//
static const LustreFID ROOT_FID = {0x200000007ULL, 1, 0};
static constexpr idx_t MAX_PATH_DEPTH = 4096;

//===----------------------------------------------------------------------===//
// Bind Data - stores device paths (shared across threads)
//===----------------------------------------------------------------------===//
struct Fid2PathBindData : public FunctionData {
	vector<string> device_paths;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<Fid2PathBindData>();
		copy->device_paths = device_paths;
		return std::move(copy);
	}

	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<Fid2PathBindData>();
		return device_paths == o.device_paths;
	}
};

//===----------------------------------------------------------------------===//
// Local State - per-thread MDTScanner instances (lazily opened)
//===----------------------------------------------------------------------===//
struct Fid2PathLocalState : public FunctionLocalState {
	vector<unique_ptr<MDTScanner>> scanners;
	bool initialized = false;

	void EnsureInitialized(const Fid2PathBindData &bind_data) {
		if (initialized) {
			return;
		}
		scanners.resize(bind_data.device_paths.size());
		for (idx_t i = 0; i < bind_data.device_paths.size(); i++) {
			scanners[i] = make_uniq<MDTScanner>();
			scanners[i]->Open(bind_data.device_paths[i]);
			scanners[i]->InitOI();
		}
		initialized = true;
	}

	//! Try OI lookup across all devices, return the scanner index that succeeded
	bool LookupFID(const LustreFID &fid, ext2_ino_t &ino_out, idx_t &scanner_idx) {
		for (idx_t i = 0; i < scanners.size(); i++) {
			if (scanners[i]->LookupFID(fid, ino_out)) {
				scanner_idx = i;
				return true;
			}
		}
		return false;
	}
};

//===----------------------------------------------------------------------===//
// Bind Functions
//===----------------------------------------------------------------------===//

static unique_ptr<FunctionData> Fid2PathBindSingle(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	auto result = make_uniq<Fid2PathBindData>();
	if (arguments[1]->IsFoldable()) {
		auto val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
		if (!val.IsNull()) {
			result->device_paths.push_back(StringValue::Get(val));
		}
	}
	return std::move(result);
}

static unique_ptr<FunctionData> Fid2PathBindMulti(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {
	auto result = make_uniq<Fid2PathBindData>();
	if (arguments[1]->IsFoldable()) {
		auto val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
		if (!val.IsNull()) {
			auto &children = ListValue::GetChildren(val);
			for (auto &child : children) {
				result->device_paths.push_back(StringValue::Get(child));
			}
		}
	}
	return std::move(result);
}

//===----------------------------------------------------------------------===//
// Local State Init
//===----------------------------------------------------------------------===//

static unique_ptr<FunctionLocalState> Fid2PathInitLocal(ExpressionState &state,
                                                        const BoundFunctionExpression &expr,
                                                        FunctionData *bind_data) {
	return make_uniq<Fid2PathLocalState>();
}

//===----------------------------------------------------------------------===//
// Helper: build a single path from a LinkEntry up to the root
//===----------------------------------------------------------------------===//

static bool BuildPathFromLink(Fid2PathLocalState &lstate, const string &name, LustreFID parent_fid, string &path_out) {
	vector<string> components;
	components.push_back(name);

	LustreFID current_fid = parent_fid;
	idx_t depth = 0;

	while (!(current_fid == ROOT_FID)) {
		if (depth++ >= MAX_PATH_DEPTH) {
			return false;
		}

		ext2_ino_t ino;
		idx_t scanner_idx;
		if (!lstate.LookupFID(current_fid, ino, scanner_idx)) {
			return false;
		}

		LustreFID inode_fid;
		vector<LinkEntry> links;
		if (!lstate.scanners[scanner_idx]->ReadInodeLinkEA(ino, inode_fid, links) || links.empty()) {
			return false;
		}

		// Directories cannot have hardlinks on Linux, so first entry is sufficient
		components.push_back(links[0].name);
		current_fid = links[0].parent_fid;
	}

	// Build path: reverse components and join with /
	path_out = "/";
	for (auto it = components.rbegin(); it != components.rend(); ++it) {
		if (path_out.size() > 1) {
			path_out += "/";
		}
		path_out += *it;
	}
	return true;
}

//===----------------------------------------------------------------------===//
// Execute Function
//===----------------------------------------------------------------------===//

static void Fid2PathExecute(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.bind_info->Cast<Fid2PathBindData>();
	auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<Fid2PathLocalState>();

	lstate.EnsureInitialized(bind_data);

	auto &fid_vec = args.data[0];
	idx_t count = args.size();

	UnifiedVectorFormat fid_data;
	fid_vec.ToUnifiedFormat(count, fid_data);

	result.SetVectorType(VectorType::FLAT_VECTOR);

	// First pass: resolve all paths for each input FID
	vector<vector<string>> all_paths(count);

	for (idx_t i = 0; i < count; i++) {
		auto fid_idx = fid_data.sel->get_index(i);

		if (!fid_data.validity.RowIsValid(fid_idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		auto fid_str = UnifiedVectorFormat::GetData<string_t>(fid_data)[fid_idx];
		LustreFID current_fid;
		if (!LustreFID::FromString(fid_str.GetString(), current_fid)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		// OI lookup: FID -> inode
		ext2_ino_t ino;
		idx_t scanner_idx;
		if (!lstate.LookupFID(current_fid, ino, scanner_idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		// Read LinkEA: inode -> all (parent_fid, name) entries
		LustreFID inode_fid;
		vector<LinkEntry> links;
		if (!lstate.scanners[scanner_idx]->ReadInodeLinkEA(ino, inode_fid, links) || links.empty()) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		// Build a path for each link entry (hardlink)
		bool any_success = false;
		for (auto &link : links) {
			string path;
			if (BuildPathFromLink(lstate, link.name, link.parent_fid, path)) {
				all_paths[i].push_back(std::move(path));
				any_success = true;
			}
		}

		if (!any_success) {
			FlatVector::SetNull(result, i, true);
		}
	}

	// Second pass: populate the LIST(VARCHAR) result vector
	auto list_data = FlatVector::GetData<list_entry_t>(result);
	auto &child_vector = ListVector::GetEntry(result);
	idx_t total_paths = 0;

	for (idx_t i = 0; i < count; i++) {
		list_data[i].offset = total_paths;
		list_data[i].length = all_paths[i].size();
		total_paths += all_paths[i].size();
	}

	ListVector::Reserve(result, total_paths);

	for (idx_t i = 0; i < count; i++) {
		for (idx_t j = 0; j < all_paths[i].size(); j++) {
			FlatVector::GetData<string_t>(child_vector)[list_data[i].offset + j] =
			    StringVector::AddString(child_vector, all_paths[i][j]);
		}
	}

	ListVector::SetListSize(result, total_paths);
}

//===----------------------------------------------------------------------===//
// GetFunctionSet
//===----------------------------------------------------------------------===//

ScalarFunctionSet LustreFid2PathFunction::GetFunctionSet() {
	ScalarFunctionSet set("lustre_fid2path");

	// Overload 1: lustre_fid2path(fid VARCHAR, device VARCHAR) -> LIST(VARCHAR)
	ScalarFunction single_func({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::LIST(LogicalType::VARCHAR),
	                           Fid2PathExecute, Fid2PathBindSingle, nullptr, nullptr,
	                           Fid2PathInitLocal);
	single_func.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	set.AddFunction(std::move(single_func));

	// Overload 2: lustre_fid2path(fid VARCHAR, devices LIST(VARCHAR)) -> LIST(VARCHAR)
	ScalarFunction multi_func({LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)}, LogicalType::LIST(LogicalType::VARCHAR),
	                          Fid2PathExecute, Fid2PathBindMulti, nullptr, nullptr,
	                          Fid2PathInitLocal);
	multi_func.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	set.AddFunction(std::move(multi_func));

	return set;
}

} // namespace lustre
} // namespace duckdb
