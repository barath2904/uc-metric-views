# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

## [0.1.1] - 2026-03-19

### Changed

- Version test no longer hardcodes version string — matches any semver (`\d+\.\d+\.\d+`)

### Fixed

- Type annotations in `validator.py`: `dict` → `dict[str, Any]`, `list` → `list[Any]`

### Docs

- Expanded docstrings for `_find_join_key()` (3-step heuristic) and `_render_join()` (recursive indentation)
- Added one-liner docstrings to Pydantic models: `DimensionDef`, `MeasureDef`, `JoinDef`, `MaterializationConfig`, `MaterializedViewDef`
- Added comment above `_AGG_FUNCTIONS` constant

[0.1.1]: https://github.com/barath2904/uc-metric-views/compare/v0.1.0...v0.1.1
