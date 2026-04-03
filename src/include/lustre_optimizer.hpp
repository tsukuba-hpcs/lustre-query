//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_optimizer.hpp
//
// Optimizer rewrites for Lustre query patterns
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class ExtensionLoader;

namespace lustre {

void RegisterLustreOptimizer(ExtensionLoader &loader);

} // namespace lustre
} // namespace duckdb
