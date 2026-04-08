//===----------------------------------------------------------------------===//
//                         LustreQuery Extension
//
// lustre_output_string_cache.hpp
//
// Helpers for writing repeated FID/device strings into DuckDB output vectors
// without re-formatting or re-copying them for every row in a chunk.
//===----------------------------------------------------------------------===//

#pragma once

#include "lustre_types.hpp"

#include "duckdb/common/types/vector.hpp"

#include <utility>
#include <unordered_map>

namespace duckdb {
namespace lustre {

inline string_t MaterializeFIDString(Vector &vector, const LustreFID &fid) {
	auto result = StringVector::EmptyString(vector, fid.StringLength());
	fid.WriteTo(result.GetDataWriteable());
	result.Finalize();
	return result;
}

struct LustreOutputStringCache {
	explicit LustreOutputStringCache(idx_t column_count) : fid_cache(column_count), string_cache(column_count) {
	}

	string_t GetFID(Vector &vector, idx_t output_column_idx, const LustreFID &fid) {
		auto &cache = fid_cache[output_column_idx];
		auto entry = cache.find(fid);
		if (entry != cache.end()) {
			return entry->second;
		}
		auto materialized = MaterializeFIDString(vector, fid);
		cache.emplace(fid, materialized);
		return materialized;
	}

	string_t GetString(Vector &vector, idx_t output_column_idx, const string &value) {
		if (value.size() <= string_t::INLINE_LENGTH) {
			return StringVector::AddString(vector, value);
		}
		auto &cache = string_cache[output_column_idx];
		for (auto &entry : cache) {
			if (entry.first == value) {
				return entry.second;
			}
		}
		auto materialized = StringVector::AddString(vector, value);
		cache.emplace_back(value, materialized);
		return materialized;
	}

private:
	vector<std::unordered_map<LustreFID, string_t, LustreFIDHash>> fid_cache;
	vector<vector<std::pair<string, string_t>>> string_cache;
};

} // namespace lustre
} // namespace duckdb
