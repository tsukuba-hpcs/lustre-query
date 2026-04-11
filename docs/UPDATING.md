# Updating Lustre Query

This note is for maintainers updating `lustre_query` to a newer DuckDB release.

## What to bump

- Update the `duckdb` submodule to the target DuckDB tag or commit.
- Update the `extension-ci-tools` submodule to the matching release branch or tag.
- Update the pinned versions in [MainDistributionPipeline.yml](../.github/workflows/MainDistributionPipeline.yml):
  - `uses: duckdb/extension-ci-tools/...@...`
  - `duckdb_version`
  - `ci_tools_version`

## What to verify after the bump

- `make` still builds the extension.
- The optimizer rewrite code still matches DuckDB planner and binder APIs.
- Table/scalar function registration still compiles against the new extension loader APIs.
- `clang-format` / `clang-tidy` checks in CI still pass.

## Likely breakage areas

`lustre_query` uses DuckDB's internal C++ extension APIs, so source-level compatibility is not guaranteed across releases. In practice, breakage is most likely in:

- planner and optimizer classes used by [lustre_optimizer.cpp](../src/lustre_optimizer.cpp)
- table-function bind/init/execute signatures used throughout [src](../src)
- extension loading and registration entry points in [lustre_query_extension.cpp](../src/lustre_query_extension.cpp)

## Useful references

- DuckDB releases: https://github.com/duckdb/duckdb/releases
- DuckDB extension-related patches: https://github.com/duckdb/duckdb/commits/main/.github/patches/extensions
- `extension-ci-tools` release branches matching the DuckDB version you target
