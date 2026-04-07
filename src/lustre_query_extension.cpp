//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_query_extension.cpp
//
// Extension entry point
//===----------------------------------------------------------------------===//

#include "lustre_query_extension.hpp"
#include "lustre_inodes.hpp"
#include "lustre_inode_links.hpp"
#include "lustre_inode_layouts.hpp"
#include "lustre_inode_objects.hpp"
#include "lustre_link_layouts.hpp"
#include "lustre_link_objects.hpp"
#include "lustre_object_layouts.hpp"
#include "lustre_links.hpp"
#include "lustre_layouts.hpp"
#include "lustre_objects.hpp"
#include "lustre_dirmap.hpp"
#include "lustre_link_dirmap.hpp"
#include "lustre_inode_dirmap.hpp"
#include "lustre_link_inode_dirmap.hpp"
#include "lustre_fid2path.hpp"
#include "lustre_path2fid.hpp"
#include "lustre_optimizer.hpp"

#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Register the lustre_inodes table function set
	loader.RegisterFunction(lustre::LustreInodesFunction::GetFunctionSet());

	// Register the lustre_links table function set
	loader.RegisterFunction(lustre::LustreLinksFunction::GetFunctionSet());

	// Register the internal fused inode+link table function set
	loader.RegisterFunction(lustre::LustreInodeLinksFunction::GetFunctionSet());

	// Register the internal fused inode+layout table function set
	loader.RegisterFunction(lustre::LustreInodeLayoutsFunction::GetFunctionSet());

	// Register the internal fused inode+object table function set
	loader.RegisterFunction(lustre::LustreInodeObjectsFunction::GetFunctionSet());

	// Register the internal fused link+object table function set
	loader.RegisterFunction(lustre::LustreLinkObjectsFunction::GetFunctionSet());

	// Register the internal fused link+layout table function set
	loader.RegisterFunction(lustre::LustreLinkLayoutsFunction::GetFunctionSet());

	// Register the internal fused object+layout table function set
	loader.RegisterFunction(lustre::LustreObjectLayoutsFunction::GetFunctionSet());

	// Register the lustre_layouts table function set
	loader.RegisterFunction(lustre::LustreLayoutsFunction::GetFunctionSet());

	// Register the lustre_objects table function set
	loader.RegisterFunction(lustre::LustreObjectsFunction::GetFunctionSet());

	// Register the lustre_dirmap table function set
	loader.RegisterFunction(lustre::LustreDirMapFunction::GetFunctionSet());

	// Register the fused link/dirmap table function set
	loader.RegisterFunction(lustre::LustreLinkDirMapFunction::GetFunctionSet());

	// Register the fused inode/dirmap table function set
	loader.RegisterFunction(lustre::LustreInodeDirMapFunction::GetFunctionSet());

	// Register the fused link/inode/dirmap table function set
	loader.RegisterFunction(lustre::LustreLinkInodeDirMapFunction::GetFunctionSet());

	// Register the lustre_fid2path scalar function set
	loader.RegisterFunction(lustre::LustreFid2PathFunction::GetFunctionSet());

	// Register the lustre_path2fid scalar function set
	loader.RegisterFunction(lustre::LustrePath2FidFunction::GetFunctionSet());

	// Register optimizer rewrites for fused scans
	lustre::RegisterLustreOptimizer(loader);

	// Set extension description
	loader.SetDescription("Scan Lustre MDT (Metadata Target) devices directly using SQL");
}

void LustreQueryExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string LustreQueryExtension::Name() {
	return "lustre_query";
}

std::string LustreQueryExtension::Version() const {
#ifdef EXT_VERSION_LUSTRE_QUERY
	return EXT_VERSION_LUSTRE_QUERY;
#else
	return "0.1.0";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(lustre_query, loader) {
	duckdb::LoadInternal(loader);
}

}
