# CLAUDE.md — Project Guidelines for uc-metric-views

## What is this project?

A Python CLI tool (`ucm`) that generates, validates, and deploys Databricks Unity Catalog metric view YAML definitions. See `docs/ARCHITECTURE.md` for full design and `docs/plans/plan_v0.1_release.md` for the implementation plan.

## Tech Stack

- Python 3.10+, Pydantic 2, Click, PyYAML, databricks-sdk
- Dev: pytest, ruff, mypy (strict mode)
- Build: hatchling

## Commands

```bash
pip install -e ".[dev]"       # install with dev deps
pytest tests/ -v              # run tests (coverage auto-enabled via pyproject.toml)
ruff check src/ tests/        # lint
ruff format --check src/ tests/ # format check
mypy src/                     # type check
ucm --version                 # verify CLI
ucm validate ./examples/      # validate YAML files
```

---

## Design Principles

Follow these throughout the codebase. They are non-negotiable.

### YAGNI — You Aren't Gonna Need It

Do not build for hypothetical future requirements. The right amount of complexity is the minimum needed for the current task. Three similar lines of code is better than a premature abstraction.

- No helpers, utilities, or abstractions for one-time operations
- No feature flags or backwards-compatibility shims
- No config file support (v0.1 uses CLI args + env vars only)
- If something is deferred to a future release (see `ROADMAP.md`), do not scaffold or stub it

### Functions over Classes

Most of this package is data transformation (column metadata in, YAML out, DDL out). Use plain functions. Classes are reserved for:

- Pydantic models (data validation / domain schema)
- Click CLI group (framework requirement)
- `DatabricksError` (exception subclass)

Do not create service classes, manager classes, or base classes. If you're writing `class FooService:` with only methods and no state, use functions instead.

### One File, One Job

Each module has a single responsibility. Do not cross boundaries:

| Module | Responsibility | Does NOT do |
|--------|---------------|-------------|
| `models.py` | Pydantic models (the schema) | Business logic |
| `heuristics.py` | Column classification | SDK calls, file I/O |
| `introspector.py` | Databricks SDK calls | Classification, YAML |
| `generator.py` | Build MetricViewSpec + serialize to YAML | Validation, deployment |
| `validator.py` | Check YAML against spec | Deployment, generation |
| `deployer.py` | Wrap YAML in DDL + execute via SDK | Validation logic (delegates to validator) |
| `cli.py` | Click commands (thin delegation) | Business logic |

### YAML Passes Through Verbatim

The deployer never parses or transforms YAML content. It wraps it in DDL and sends it. This means:

- Deployer does not import `yaml` or `MetricViewSpec`
- Deployer reads the file as a string and embeds it in `CREATE OR REPLACE VIEW ... AS $$ {content} $$`
- Any YAML transformation happens in `generator.py` or by the user

### Generate Scaffolds, Humans Refine

The generator produces the 80% case. These features are NEVER auto-generated — they require human judgment:

- Window measures (trailing, cumulative, YTD)
- Materialization config
- Snowflake joins (deeply nested dimensions)
- Semantic metadata (display_name, format, synonyms)

### Validate Before Deploy

Always. The deployer calls `validate_file()` before each deploy. `--strict` mode in CI treats warnings as errors.

---

## Coding Standards

### Naming

- `YamlValidationError` (not `ValidationError`) — avoids collision with `pydantic.ValidationError`
- Snake case for all Python identifiers
- Humanized names in generated YAML: `total_order_amount` → `Total Order Amount`

### Python 3.10 Compatibility

- Minimum supported version is Python 3.10
- Every module that uses `str | None`, `list[str]`, or other PEP 604/585 syntax at runtime MUST have `from __future__ import annotations` as the first import
- Do not use `match`/`case` statements (Python 3.10 supports them, but they are not needed in this codebase)
- Do not use `ExceptionGroup`, `TaskGroup`, or other 3.11+ stdlib additions

### Type Safety

- `mypy --strict` must pass
- Use `str | None` (not `Optional[str]`) — with `from __future__ import annotations`
- Use `-> NoReturn` for functions that always raise
- `format` fields on dimensions/measures are `dict | None` (not deeply typed) — the Databricks format spec may evolve

### Error Handling

- CLI wraps SDK exceptions via `_handle_sdk_error() -> NoReturn` for human-readable messages
- Validator returns `list[YamlValidationError]` — never raises
- Deployer returns `DeployResult` with status field — never raises on deployment failure

### Security

- **SQL injection in DDL:** The deployer validates YAML content does not contain `$$` (dollar-quote escape) and sanitizes catalog/schema/view identifiers against `^[\w-]+$` before constructing DDL
- **Credential safety:** `_handle_sdk_error()` only shows raw exception details in `--verbose` mode to avoid leaking tokens in CI logs
- **YAML deserialization:** Always use `yaml.safe_load()`, never `yaml.load()`
- **GitHub Actions:** Pass `vars.*` through `env:` blocks with quoted shell variables, never inline `${{ vars.* }}` in `run:` commands

### YAML Gotcha: The `on` Key

YAML 1.1 parses bare `on:` as boolean `True`. This corrupts join definitions silently.

- Generator always outputs `"on":` (quoted)
- Validator pre-checks for boolean keys in join dicts BEFORE Pydantic parsing and rewrites them with a warning
- Tests must cover this case (`tests/fixtures/unquoted_on_key.yaml`)

### Databricks SDK Gotchas

These are validated against `databricks-sdk >= 0.30.0`. Do not change without re-checking the SDK source.

- **`ColumnInfo.type_name` is a `ColumnTypeName` enum, not a string.** Always use `.value` to get the string (e.g., `col.type_name.value if col.type_name else "STRING"`).
- **`execute_statement(wait_timeout=...)` max is `"50s"`.** Valid range: `"0s"` (async) or `"5s"` to `"50s"`. Never use `"120s"` — it will be rejected by the API.
- **`StatementState` values:** `PENDING`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELED`, `CLOSED`. Our code checks for `SUCCEEDED` explicitly; everything else is treated as failure.
- **`TableType` includes `METRIC_VIEW`, `MATERIALIZED_VIEW`, `STREAMING_TABLE`** — `list_tables` must exclude all of these (not just `VIEW`) when `include_views=False`.
- **`ColumnTypeName` enum names differ from SQL aliases.** SDK uses `LONG` (not `BIGINT`), `SHORT` (not `SMALLINT`), `BYTE` (not `TINYINT`), `INT` (not `INTEGER`). The heuristics type sets include both SDK names and SQL aliases for compatibility.

### Dependency Pinning

- Runtime deps use compatible version ranges with upper bounds: `>=X.Y,<Z.0` (e.g., `pydantic>=2.0,<3.0`)
- Dev deps also use upper bounds: `>=X.Y,<Z.0`
- Never use unpinned deps (bare package names) or open-ended `>=X.Y` without an upper bound
- Upper bounds are set at the next major version to avoid unexpected breaking changes

### Testing

- TDD: write failing test first, then implement
- Tests do not require a Databricks connection — all SDK calls are mocked
- Fixtures live in `tests/fixtures/`
- Use `tmp_path` (pytest built-in) for file I/O tests
- Test file naming matches source: `heuristics.py` → `test_heuristics.py`

### Line Length and Formatting

- `ruff` with `line-length = 100`
- `ruff check` with lint rules: `E`, `F`, `W`, `I` (isort), `UP` (pyupgrade), `B` (bugbear), `SIM`, `RET`, `PERF`, `RUF`
- `ruff format` for consistent code formatting (Black-compatible)
- No docstrings required on internal/private functions
- Docstrings on public API functions only when the behavior is not obvious from the signature

### Git Discipline

- One commit per logical unit of work (one module + its tests)
- Conventional commit messages: `feat:`, `test:`, `fix:`, `docs:`, `ci:`, `chore:`
- Do not amend published commits

---

## Architecture Quick Reference

```
Databricks UC → introspector.py → heuristics.py → generator.py → .yaml file
                                                                      ↓
                                                   validator.py ← human edits
                                                        ↓
                                                   deployer.py → CREATE OR REPLACE VIEW
```

Full details: `docs/ARCHITECTURE.md`
Implementation plan: `docs/plans/plan_v0.1_release.md`
Future releases: `ROADMAP.md`

---

## Plugin & Skill Usage Guide

These installed plugins should be leveraged during implementation. Use them — don't reinvent their workflows.

### Implementation Workflow

1. **Start execution:** Use `superpowers:subagent-driven-development` or `superpowers:executing-plans` against `docs/plans/plan_v0.1_release.md`
2. **Per task:** Follow `superpowers:test-driven-development` — write failing tests first, then implement
3. **After each module:** Run `code-simplifier` to clean up, then `commit-commands:commit` for git
4. **Debugging:** Use `superpowers:systematic-debugging` before guessing at fixes
5. **Before PR:** Run `pr-review-toolkit:review-pr` and `code-review:code-review` for comprehensive review
6. **After merge:** Use `claude-md-management:revise-claude-md` to capture implementation learnings

### Databricks Reference Skills

When implementing modules that interact with the Databricks SDK, consult these for up-to-date patterns:

- `databricks-ai-dev-kit:databricks-metric-views` — metric view YAML spec, CREATE syntax, query patterns
- `databricks-ai-dev-kit:databricks-python-sdk` — SDK client patterns, auth, error handling
- `databricks-ai-dev-kit:databricks-unity-catalog` — catalog/schema/table introspection patterns

### Active Hooks (automatic)

- **security-guidance** — auto-warns about injection, XSS, and unsafe patterns when editing files
- **pyright-lsp** — Python type checking LSP server for `.py` files
