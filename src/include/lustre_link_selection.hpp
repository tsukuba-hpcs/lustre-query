#pragma once

#include "lustre_types.hpp"

#include <string>
#include <vector>

namespace duckdb {
namespace lustre {

inline std::string BuildStripedShardLinkName(const LustreFID &shard_fid, uint32_t stripe_index) {
	return shard_fid.ToString() + ":" + std::to_string(stripe_index);
}

inline const LinkEntry *FindStripedShardLink(const std::vector<LinkEntry> &links, const LustreFID &shard_fid,
                                             uint32_t stripe_index) {
	const auto expected_name = BuildStripedShardLinkName(shard_fid, stripe_index);
	for (const auto &link : links) {
		if (link.parent_fid.IsValid() && link.name == expected_name) {
			return &link;
		}
	}
	return nullptr;
}

inline const LinkEntry *FindUniqueParentLink(const std::vector<LinkEntry> &links) {
	const LinkEntry *candidate = nullptr;
	LustreFID candidate_parent;

	for (const auto &link : links) {
		if (!link.parent_fid.IsValid()) {
			continue;
		}
		if (!candidate) {
			candidate = &link;
			candidate_parent = link.parent_fid;
			continue;
		}
		if (link.parent_fid != candidate_parent) {
			return nullptr;
		}
	}
	return candidate;
}

inline const LinkEntry *FindUniqueDirectoryLink(const std::vector<LinkEntry> &links) {
	const LinkEntry *candidate = nullptr;

	for (const auto &link : links) {
		if (!link.parent_fid.IsValid()) {
			continue;
		}
		if (!candidate) {
			candidate = &link;
			continue;
		}
		if (link.parent_fid != candidate->parent_fid || link.name != candidate->name) {
			return nullptr;
		}
	}
	return candidate;
}

inline const LinkEntry *SelectDirectoryParentLink(const std::vector<LinkEntry> &links) {
	return FindUniqueDirectoryLink(links);
}

inline const LinkEntry *SelectStripedSlaveParentLink(const std::vector<LinkEntry> &links, const LustreFID &shard_fid,
                                                     uint32_t stripe_index) {
	if (auto *match = FindStripedShardLink(links, shard_fid, stripe_index)) {
		return match;
	}
	return FindUniqueParentLink(links);
}

} // namespace lustre
} // namespace duckdb
