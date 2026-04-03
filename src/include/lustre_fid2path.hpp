//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_fid2path.hpp
//
// Scalar function: FID -> filesystem path via LinkEA traversal
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set.hpp"

namespace duckdb {
namespace lustre {

class LustreFid2PathFunction {
public:
	static ScalarFunctionSet GetFunctionSet();
};

} // namespace lustre
} // namespace duckdb
