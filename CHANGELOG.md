# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.1] - 2026-03-19

### Added

- `metricviews.__version__` attribute — lazy-loaded via `importlib.metadata`
- Shared pytest fixtures in `conftest.py`: `fixtures_dir`, `minimal_spec`, `valid_yaml_path`
- Parameterized format type tests — all 6 valid and 5 invalid types exercised
- Test for `_format_pydantic_errors` fallback branch (unknown Pydantic error types)

### Changed

- Branch coverage enabled (`branch = true` in `[tool.coverage.run]`)

[0.2.1]: https://github.com/barath2904/uc-metric-views/compare/v0.2.0...v0.2.1

## [0.2.0] - 2026-03-19

### Added

- Three-tier severity in `YamlValidationError`: `error`, `warning`, and `suggestion`
- Human-readable Pydantic error messages — no more raw `input_value` dumps or `pydantic.dev` URLs
- Colored CLI output: `[ERROR]` in red, `[WARN ]` in yellow, `[HINT ]` in cyan
- `--strict` now escalates both warnings **and** suggestions to errors (updated help text)
- 14 new validation checks:
  - `extra="forbid"` on all sub-models (`DimensionDef`, `MeasureDef`, `JoinDef`, `WindowSpec`, `MaterializationConfig`, `MaterializedViewDef`) — typos in field names are now errors instead of being silently dropped
  - `min_length=1` on required string fields — empty `name` / `expr` / `source` are now errors
  - `min_length=1` on `MaterializationConfig.materialized_views` — empty list is now an error
  - Whitespace-only `on:` join condition treated as missing (stripped before truthy check)
  - Join `source` validated as FQN (warning)
  - Placeholder `???` detected in `using` list as well as `on` condition
  - `format` dict without a `type` key is now an error (not silently passed)
  - Synonyms: empty strings are errors; duplicates within a column are warnings
  - Materialized view `dimensions`/`measures` cross-referenced against declared names
  - FQN pattern updated to allow hyphens in catalog/schema/table names (`my-catalog.my-schema.tbl`)
- Informational findings reclassified from `warning` → `suggestion`: non-FQN source, missing aggregate function, experimental `materialization`, experimental `window`

[0.2.0]: https://github.com/barath2904/uc-metric-views/compare/v0.1.1...v0.2.0

## [0.1.1] - 2026-03-19

### Changed

- Version test no longer hardcodes version string — matches any semver (`\d+\.\d+\.\d+`)
- Expanded docstrings for `_find_join_key()` (3-step heuristic) and `_render_join()` (recursive indentation)
- Added one-liner docstrings to Pydantic models: `DimensionDef`, `MeasureDef`, `JoinDef`, `MaterializationConfig`, `MaterializedViewDef`
- Added comment above `_AGG_FUNCTIONS` constant

### Fixed

- Type annotations in `validator.py`: `dict` → `dict[str, Any]`, `list` → `list[Any]`

[0.1.1]: https://github.com/barath2904/uc-metric-views/compare/v0.1.0...v0.1.1

## [0.1.0] - 2026-03-18

### Added

- `ucm generate` — scaffold metric view YAML from a fact table + dimension joins
- `ucm inspect` — read-only column inventory with suggested roles (dimension / measure / ignore)
- `ucm validate` — 11-check YAML validation with `--strict` mode for CI
- `ucm deploy` — deploy YAML to Databricks Unity Catalog with `--dry-run` support
- Pydantic models covering the full Databricks metric view YAML spec (v1.1 + experimental features)
- Column classification heuristics (name patterns + type inference)
- Star-schema join support with auto-detected join keys
- YAML `on:` boolean key detection and rewrite (YAML 1.1 gotcha)
- SQL injection protection in DDL generation (identifier validation + dollar-quote escaping)
- Example YAML files using Databricks `samples` catalog (works on any workspace)
- CI workflow (lint, format, type check, tests on Python 3.10–3.13)
- Release workflows (TestPyPI on version tag, PyPI via manual dispatch)

[0.1.0]: https://github.com/barath2904/uc-metric-views/releases/tag/v0.1.0
