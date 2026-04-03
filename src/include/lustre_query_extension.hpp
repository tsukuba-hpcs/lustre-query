//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_query_extension.hpp
//
// Extension header
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/extension.hpp"

namespace duckdb {

class LustreQueryExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb
