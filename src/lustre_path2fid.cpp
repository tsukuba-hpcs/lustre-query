//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_path2fid.cpp
//
// Scalar function: filesystem path -> FID via directory tree traversal
//===----------------------------------------------------------------------===//

#include "lustre_path2fid.hpp"
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

//===----------------------------------------------------------------------===//
// Bind Data - stores device paths (shared across threads)
//===----------------------------------------------------------------------===//
struct Path2FidBindData : public FunctionData {
	vector<string> device_paths;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<Path2FidBindData>();
		copy->device_paths = device_paths;
		return std::move(copy);
	}

	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<Path2FidBindData>();
		return device_paths == o.device_paths;
	}
};

//===----------------------------------------------------------------------===//
// Local State - per-thread MDTScanner instances (lazily opened)
//===----------------------------------------------------------------------===//
struct Path2FidLocalState : public FunctionLocalState {
	vector<unique_ptr<MDTScanner>> scanners;
	bool initialized = false;

	void EnsureInitialized(const Path2FidBindData &bind_data) {
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

static unique_ptr<FunctionData> Path2FidBindSingle(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	auto result = make_uniq<Path2FidBindData>();
	if (arguments[1]->IsFoldable()) {
		auto val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
		if (!val.IsNull()) {
			result->device_paths.push_back(StringValue::Get(val));
		}
	}
	return std::move(result);
}

static unique_ptr<FunctionData> Path2FidBindMulti(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {
	auto result = make_uniq<Path2FidBindData>();
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

static unique_ptr<FunctionLocalState> Path2FidInitLocal(ExpressionState &state, const BoundFunctionExpression &expr,
                                                        FunctionData *bind_data) {
	return make_uniq<Path2FidLocalState>();
}

//===----------------------------------------------------------------------===//
// Helper: Split path into components
//===----------------------------------------------------------------------===//

static vector<string> SplitPath(const string &path) {
	vector<string> components;
	size_t start = 0;
	size_t len = path.size();

	while (start < len) {
		// Skip leading slashes
		while (start < len && path[start] == '/') {
			start++;
		}
		if (start >= len) {
			break;
		}
		// Find next slash
		size_t end = path.find('/', start);
		if (end == string::npos) {
			end = len;
		}
		components.push_back(path.substr(start, end - start));
		start = end;
	}

	return components;
}

//===----------------------------------------------------------------------===//
// Execute Function
//===----------------------------------------------------------------------===//

static void Path2FidExecute(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.bind_info->Cast<Path2FidBindData>();
	auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<Path2FidLocalState>();

	lstate.EnsureInitialized(bind_data);

	auto &path_vec = args.data[0];
	idx_t count = args.size();

	UnifiedVectorFormat path_data;
	path_vec.ToUnifiedFormat(count, path_data);

	result.SetVectorType(VectorType::FLAT_VECTOR);

	for (idx_t i = 0; i < count; i++) {
		auto path_idx = path_data.sel->get_index(i);

		if (!path_data.validity.RowIsValid(path_idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		auto path_str = UnifiedVectorFormat::GetData<string_t>(path_data)[path_idx];
		auto components = SplitPath(path_str.GetString());

		// Start from root FID
		LustreFID current_fid = ROOT_FID;
		bool success = true;

		for (auto &component : components) {
			// OI lookup: current_fid -> inode
			ext2_ino_t dir_ino;
			idx_t scanner_idx;
			if (!lstate.LookupFID(current_fid, dir_ino, scanner_idx)) {
				success = false;
				break;
			}

			// Directory lookup: find child inode by name
			ext2_ino_t child_ino;
			if (!lstate.scanners[scanner_idx]->LookupName(dir_ino, component, child_ino)) {
				success = false;
				break;
			}

			// Read child inode's FID from trusted.lma xattr
			LustreFID child_fid;
			if (!lstate.scanners[scanner_idx]->ReadInodeFID(child_ino, child_fid)) {
				success = false;
				break;
			}

			current_fid = child_fid;
		}

		if (!success) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		auto fid_str = current_fid.ToString();
		FlatVector::GetData<string_t>(result)[i] = StringVector::AddString(result, fid_str);
	}
}

//===----------------------------------------------------------------------===//
// GetFunctionSet
//===----------------------------------------------------------------------===//

ScalarFunctionSet LustrePath2FidFunction::GetFunctionSet() {
	ScalarFunctionSet set("lustre_path2fid");

	// Overload 1: lustre_path2fid(path VARCHAR, device VARCHAR) -> VARCHAR
	ScalarFunction single_func({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR, Path2FidExecute,
	                           Path2FidBindSingle, nullptr, nullptr, Path2FidInitLocal);
	single_func.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	set.AddFunction(std::move(single_func));

	// Overload 2: lustre_path2fid(path VARCHAR, devices LIST(VARCHAR)) -> VARCHAR
	ScalarFunction multi_func({LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)}, LogicalType::VARCHAR,
	                          Path2FidExecute, Path2FidBindMulti, nullptr, nullptr, Path2FidInitLocal);
	multi_func.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	set.AddFunction(std::move(multi_func));

	return set;
}

} // namespace lustre
} // namespace duckdb
