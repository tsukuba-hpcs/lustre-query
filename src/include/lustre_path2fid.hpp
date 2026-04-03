//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_path2fid.hpp
//
// Scalar function: filesystem path -> FID via directory tree traversal
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set.hpp"

namespace duckdb {
namespace lustre {

class LustrePath2FidFunction {
public:
	static ScalarFunctionSet GetFunctionSet();
};

} // namespace lustre
} // namespace duckdb
