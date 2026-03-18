# uc-metric-views — Architecture

## Overview

`uc-metric-views` is a Python package (with CLI) that solves three problems: **generating** metric view YAML definitions from Unity Catalog table schemas, **validating** them against the Databricks spec, and **deploying** them as metric views to Unity Catalog.

A metric view is a business concept — not a 1:1 mapping to a table. It typically sits on a fact table joined to one or more dimension tables, with measures (aggregations) defined over the fact table and dimensions drawn from both the fact and joined dimension tables. The tool is designed around this reality.

---

## Pain Points → Solutions

```
Pain Point                              Solution
─────────────────────────────────────── ──────────────────────────────────
1. No automation for MV deployment   →  deployer.py + CLI deploy command
2. YAML creation doesn't scale       →  introspector.py + heuristics.py + generator.py
3. Deploy via Python pkg or GHA       →  CLI + GHA workflows
4. Generation from CLI                →  CLI generate command (per-metric-view scaffolding)
5. Best practices                     →  Pydantic models, single responsibility, testable functions
```

---

## Databricks Metric View YAML Spec — Complete Reference

Based on the official Databricks documentation (spec version 1.1), the full YAML schema is:

```yaml
# ── REQUIRED ───────────────────────────────────────────────────────────
version: "1.1"                              # "0.1" (DBR 16.4-17.1) or "1.1" (DBR 17.2+)

source: catalog.schema.table                # FQN table/view, SQL query, or another metric view
                                            # SQL query example:
                                            #   source: SELECT * FROM fct LEFT JOIN dim ON ...

dimensions:                                 # At least one required
  - name: Order Month
    expr: "DATE_TRUNC('MONTH', o_orderdate)"
    comment: "Month of order"               # v1.1 — stored in Unity Catalog
    display_name: "Order Month"             # v1.1 — semantic metadata for viz tools
    format:                                 # v1.1 — display format
      type: date
      date_format: year_month_day
      leading_zeros: true
    synonyms:                               # v1.1 — for Genie/LLM discovery (max 10)
      - "order month"
      - "month of order"

measures:                                   # At least one required
  - name: Total Revenue
    expr: "SUM(o_totalprice)"
    comment: "Sum of all order prices"
    display_name: "Total Revenue"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "revenue"
      - "total sales"
    window:                                 # Experimental — windowed/cumulative/semiadditive
      - order: date_dim                     # Dimension that orders the window
        range: trailing 7 day               # current | cumulative | trailing N unit | leading N unit | all
        semiadditive: last                  # first | last — how to summarize when order dim not in GROUP BY

# ── OPTIONAL ───────────────────────────────────────────────────────────
comment: "Orders KPIs for sales analysis"   # v1.1 — metric view description

filter: "o_orderdate > '1990-01-01'"        # SQL WHERE applied to all queries

joins:                                      # Star schema and snowflake schema
  - name: customer
    source: catalog.schema.dim_customer
    "on": source.o_custkey = customer.c_custkey   # MUST quote 'on' — YAML 1.1 parses bare on as boolean
    # OR use:
    # using:
    #   - shared_key_column
    joins:                                  # Nested joins = snowflake schema (DBR 17.1+)
      - name: nation
        source: catalog.schema.dim_nation
        "on": customer.c_nationkey = nation.n_nationkey

materialization:                            # Experimental — performance acceleration
  schedule: every 6 hours
  mode: relaxed                             # Only supported value currently
  materialized_views:
    - name: baseline
      type: unaggregated                    # Materializes full data model (source + joins + filter)
    - name: revenue_by_category
      type: aggregated                      # Pre-computes specific dim/measure combos
      dimensions:
        - Order Month                       # References by name, not expression
      measures:
        - Total Revenue
```

### Format Types Reference

| Format Type | Required Fields | Optional Fields |
|-------------|----------------|-----------------|
| `number` | `type: number` | `decimal_places` (`max`/`exact`/`all` + `places`), `hide_group_separator`, `abbreviation` (`none`/`compact`/`scientific`) |
| `currency` | `type: currency`, `currency_code` (ISO-4217) | `decimal_places`, `hide_group_separator`, `abbreviation` |
| `percentage` | `type: percentage` | `decimal_places`, `hide_group_separator` |
| `byte` | `type: byte` | `decimal_places`, `hide_group_separator` |
| `date` | `type: date`, `date_format` | `leading_zeros` |
| `date_time` | `type: date_time`, at least one of `date_format`/`time_format` | `leading_zeros` |

### Window Measure Range Options

| Range | Meaning |
|-------|---------|
| `current` | Only rows where window ordering value = current row |
| `cumulative` | All rows ≤ current row's value (running total) |
| `trailing N unit` | N time units before current, excluding current unit |
| `leading N unit` | N time units after current |
| `all` | All rows regardless of window value |

### Deployment DDL

```sql
CREATE OR REPLACE VIEW `catalog`.`schema`.`view_name`
WITH METRICS LANGUAGE YAML AS $$
{yaml_content}
$$
```

---

## Architecture

### Design Principles

- **Functions over classes** — most of this package is data transformation (YAML in, SQL out). Classes are reserved for Pydantic models (validation) and the thin SDK wrapper.
- **One file, one job** — each module does exactly one thing.
- **Generate scaffolds, humans refine** — the tool generates the 80% case. Advanced features (window measures, materialization, snowflake joins, semantic metadata) are hand-authored.
- **Validate before deploy** — always. Cheaper to fail locally than on a SQL warehouse.
- **YAML passes through verbatim** — the deployer doesn't parse or transform YAML content. It wraps it in DDL and sends it.

### Data Flow

```
                    ┌─────────────────────────────────────────────────┐
                    │              GENERATE FLOW                       │
                    │                                                  │
                    │  Databricks UC ──→ introspector.py                │
                    │       │              (list tables, get columns)   │
                    │       ▼                                          │
                    │  Column metadata ──→ heuristics.py                │
                    │       │              (classify dim / measure)     │
                    │       ▼                                          │
                    │  MetricViewSpec ──→ generator.py                  │
                    │       │              (spec → YAML string → file)  │
                    │       ▼                                          │
                    │  ./metric_views/order_metrics.yaml                │
                    └─────────────────────────────────────────────────┘
                                         │
                                         │ human reviews, edits, adds:
                                         │   - semantic metadata
                                         │   - window measures
                                         │   - materialization
                                         │   - snowflake joins
                                         ▼
                    ┌─────────────────────────────────────────────────┐
                    │              VALIDATE FLOW                       │
                    │                                                  │
                    │  ./metric_views/*.yaml ──→ validator.py           │
                    │       │                     (parse + check)       │
                    │       ▼                                          │
                    │  List[YamlValidationError]                           │
                    └─────────────────────────────────────────────────┘
                                         │
                                         ▼
                    ┌─────────────────────────────────────────────────┐
                    │              DEPLOY FLOW                         │
                    │                                                  │
                    │  ./metric_views/*.yaml ──→ deployer.py            │
                    │       │                     (yaml → DDL → API)    │
                    │       ▼                                          │
                    │  CREATE OR REPLACE VIEW `cat`.`sch`.`view`       │
                    │  WITH METRICS LANGUAGE YAML AS $$ ... $$         │
                    │       │                                          │
                    │       ▼                                          │
                    │  databricks-sdk statement_execution              │
                    └─────────────────────────────────────────────────┘
```

### Feature Responsibility Matrix

```
Feature              generate     validate     deploy
───────────────────  ───────────  ───────────  ───────────
dimensions           ✅ auto      ✅ check     ✅ pass-through
measures             ✅ auto      ✅ check     ✅ pass-through
joins (star)         ✅ auto      ✅ check     ✅ pass-through
joins (snowflake)    ❌ manual    ✅ check     ✅ pass-through
semantic metadata    ❌ manual    ✅ check     ✅ pass-through
format specs         ❌ manual    ✅ check     ✅ pass-through
window measures      ❌ manual    ✅ + warn    ✅ pass-through
materialization      ❌ manual    ✅ + warn    ✅ pass-through
```

---

## Package Structure

```
uc-metric-views/
│
├── src/
│   └── metricviews/
│       ├── __init__.py          # Public API: generate, validate, deploy
│       ├── py.typed             # PEP 561 marker for downstream type checking
│       ├── models.py            # ALL Pydantic models (one file)
│       ├── introspector.py      # UC connectivity: list tables, get columns
│       ├── heuristics.py        # Column → dimension/measure classification
│       ├── generator.py         # MetricViewSpec → YAML file on disk
│       ├── validator.py         # YAML file → list of errors
│       ├── deployer.py          # YAML file → SQL DDL → Databricks API call
│       └── cli.py               # Click entrypoint (thin — delegates to above)
│
├── tests/
│   ├── test_models.py
│   ├── test_heuristics.py
│   ├── test_generator.py
│   ├── test_validator.py
│   ├── test_introspector.py    # SDK mock tests
│   ├── test_deployer.py        # Mocked SDK — no real Databricks needed
│   ├── test_cli.py             # Click CliRunner
│   └── fixtures/
│       ├── sample_orders.yaml
│       ├── star_schema_with_joins.yaml
│       ├── with_window_measures.yaml
│       ├── with_materialization.yaml
│       ├── invalid_missing_measures.yaml
│       └── unquoted_on_key.yaml
│
├── examples/                    # Ready-to-use samples for Databricks free tier
│   ├── README.md
│   ├── basic_single_table.yaml
│   ├── star_schema_joins.yaml
│   └── with_semantic_metadata.yaml
│
├── .github/
│   └── workflows/
│       ├── ci.yml              # Lint + unit test on PR
│       └── deploy.yml          # Validate + deploy on merge to main
│
├── pyproject.toml
├── README.md
├── ROADMAP.md
└── LICENSE                     # MIT
```

---

## Module Design

### models.py — Domain Model

Everything flows through these Pydantic models. They encode the complete Databricks metric view YAML spec v1.1 plus the experimental features. `MetricViewSpec` IS the schema — no separate JSON schema file needed. Validation happens automatically on instantiation.

```python
from __future__ import annotations
from pydantic import BaseModel, Field, model_validator
from enum import Enum


class ColumnRole(str, Enum):
    DIMENSION = "dimension"
    MEASURE = "measure"
    IGNORE = "ignore"


class WindowSpec(BaseModel):
    order: str                              # dimension name that orders the window
    range: str                              # "current", "cumulative", "trailing 7 day", etc.
    semiadditive: str | None = None         # "first" or "last"


class DimensionDef(BaseModel):
    name: str
    expr: str
    comment: str | None = None
    display_name: str | None = None         # v1.1 semantic metadata
    format: dict | None = None              # v1.1 — passed through as dict (spec may evolve)
    synonyms: list[str] | None = None       # v1.1 (max 10)


class MeasureDef(BaseModel):
    name: str
    expr: str
    comment: str | None = None
    display_name: str | None = None
    format: dict | None = None
    synonyms: list[str] | None = None
    window: list[WindowSpec] | None = None  # Experimental


class JoinDef(BaseModel):
    name: str
    source: str
    on: str | None = None                   # MUST be quoted in YAML ("on":)
    using: list[str] | None = None
    joins: list[JoinDef] | None = None      # Recursive — snowflake schema (DBR 17.1+)

    @model_validator(mode="after")
    def exactly_one_join_key(self) -> JoinDef: ...

JoinDef.model_rebuild()  # Required for recursive Pydantic models


class MaterializedViewDef(BaseModel):
    name: str
    type: str                               # "aggregated" or "unaggregated"
    dimensions: list[str] | None = None     # ref by name
    measures: list[str] | None = None       # ref by name


class MaterializationConfig(BaseModel):
    schedule: str                           # e.g. "every 6 hours"
    mode: str = "relaxed"
    materialized_views: list[MaterializedViewDef]


class MetricViewSpec(BaseModel):
    version: str = "1.1"
    source: str
    comment: str | None = None
    filter: str | None = None
    joins: list[JoinDef] | None = None
    dimensions: list[DimensionDef] = Field(min_length=1)
    measures: list[MeasureDef] = Field(min_length=1)
    materialization: MaterializationConfig | None = None

    @model_validator(mode="after")
    def no_duplicate_names(self) -> MetricViewSpec: ...


class DiscoveredColumn(BaseModel):
    name: str
    type_name: str                          # e.g. "STRING", "BIGINT", "DECIMAL(10,2)"
    comment: str | None = None
    role: ColumnRole | None = None          # Assigned by heuristics


class DiscoveredTable(BaseModel):
    catalog: str
    schema_name: str
    table_name: str
    columns: list[DiscoveredColumn]
    comment: str | None = None

    @property
    def fqn(self) -> str:
        return f"{self.catalog}.{self.schema_name}.{self.table_name}"


class DeployResult(BaseModel):
    yaml_file: str
    view_fqn: str
    status: str                             # "success", "failed", "dry_run"
    sql: str
    error: str | None = None
```

**Design decisions:**
- `format` is `dict | None` — not deeply typed. The format spec has many nested options and Databricks may add new format types. Passing as dict avoids breaking when the spec evolves.
- `JoinDef` is recursive via `joins: list[JoinDef] | None` — supports snowflake schema nesting.
- `WindowSpec` and `MaterializationConfig` are modeled as first-class types for structural validation, but the generator never produces them — they are hand-authored.

---

### introspector.py — Unity Catalog Discovery

Connects to Databricks, returns typed column metadata. Functions only — no state between calls.

```python
def create_client(host: str | None = None, token: str | None = None) -> WorkspaceClient
    """Falls back to DATABRICKS_HOST / DATABRICKS_TOKEN env vars and ~/.databrickscfg."""

def list_tables(
    client: WorkspaceClient,
    catalog: str,
    schema: str,
    table_filter: str | None = None,    # glob, e.g. "fct_*"
    include_views: bool = False,
) -> list[str]

def discover_table(
    client: WorkspaceClient,
    catalog: str,
    schema: str,
    table_name: str,
) -> DiscoveredTable
```

---

### heuristics.py — Column Classification

Classifies columns as dimension, measure, or ignore. Deterministic and stateless — fully testable without Databricks.

**Priority order (highest wins):**
1. Name pattern match (ignore patterns first, then dimension, then measure)
2. Type-based inference
3. Conservative default → DIMENSION

| Column | Type | Name Match | Type Match | Result |
|--------|------|------------|------------|--------|
| `customer_id` | BIGINT | `_id$` → DIM | BIGINT → MEASURE | **DIMENSION** (name wins) |
| `order_date` | DATE | `_date$` → DIM | DATE → DIM | **DIMENSION** |
| `total_amount` | DECIMAL(10,2) | `_amount$` → MEASURE | DECIMAL → MEASURE | **MEASURE** |
| `_fivetran_synced` | TIMESTAMP | `^_` → IGNORE | — | **IGNORE** |

```python
def classify_column(col: DiscoveredColumn) -> ColumnRole
    """Classify a single column. Deterministic, stateless."""

def suggest_aggregation(col_name: str, col_type: str) -> str
    """Suggest default aggregation: SUM / AVG / MAX / MIN / COUNT."""

def classify_table(columns: list[DiscoveredColumn]) -> list[DiscoveredColumn]
    """Classify all columns. Returns copies with role assigned."""
```

---

### generator.py — Spec to YAML File

Produces one YAML file per business concept (fact table + optional dimension joins). Manual YAML serialization — not `yaml.dump()` — to control key ordering and ensure `"on":` is always quoted.

```python
def spec_from_tables(
    source: DiscoveredTable,
    dim_tables: list[DiscoveredTable] | None = None,
) -> MetricViewSpec
    """Build MetricViewSpec from fact + dim tables.
    - Source columns → dimensions OR measures via heuristics
    - Dimension table columns → all treated as dimension candidates
    - Join keys auto-detected; placeholder '???' inserted if not found
    - Row Count measure always inserted first
    """

def spec_to_yaml(spec: MetricViewSpec) -> str
    """Serialize MetricViewSpec to YAML string. Manual formatting — not yaml.dump()."""

def write_yaml_file(
    spec: MetricViewSpec,
    output_path: str | Path,
    overwrite: bool = False,
) -> Path | None
    """Write YAML file. Returns path if written, None if skipped."""
```

---

### validator.py — Pre-Deployment Checks

Runs 11 checks against each YAML file. Returns `list[YamlValidationError]` (empty = valid). Separates errors (block deploy) from warnings (informational).

**Critical: YAML `on` boolean pre-check.** `yaml.safe_load()` parses unquoted `on:` as boolean `True`, corrupting the dict key before Pydantic ever sees it. The validator detects and rewrites boolean keys in the raw dict **before** `MetricViewSpec(**raw)`, so users get a clear warning instead of a cryptic Pydantic error.

| # | Check | Severity |
|---|-------|----------|
| 1 | Valid YAML syntax | error |
| 2 | YAML `on` boolean key rewrite (pre-Pydantic) | warning |
| 3 | Pydantic structural validation (required fields, types) | error |
| 4 | Version is supported (`"1.1"`) | error |
| 5 | Source is FQN or SQL query | warning |
| 6 | Measures contain aggregate functions | warning |
| 7 | Experimental: `materialization` present | warning |
| 8 | Experimental: `window` measures present | warning |
| 9 | Format type is valid | error |
| 10 | Synonym count ≤ 10 per column | error |
| 11 | Join keys contain no `???` placeholders | error |

```python
def validate_file(yaml_path: str | Path) -> list[YamlValidationError]
def validate_directory(yaml_dir: str | Path) -> list[YamlValidationError]
```

---

### deployer.py — YAML to Databricks

Reads YAML, wraps in DDL, executes via SDK. YAML content passes through verbatim — no parsing, no transformation. View name derived from filename stem.

```python
def build_ddl(yaml_content: str, catalog: str, schema: str, view_name: str) -> str
    """Produces: CREATE OR REPLACE VIEW `cat`.`sch`.`view` WITH METRICS LANGUAGE YAML AS $$ ... $$"""

def deploy_file(
    client: WorkspaceClient,
    yaml_path: str | Path,
    catalog: str,
    schema: str,
    warehouse_id: str,
    view_name: str | None = None,
    dry_run: bool = False,
) -> DeployResult

def deploy_directory(
    client: WorkspaceClient,
    yaml_dir: str | Path,
    catalog: str,
    schema: str,
    warehouse_id: str,
    dry_run: bool = False,
) -> list[DeployResult]
    """Validates each file (errors only) before deploying."""
```

---

### cli.py — User Interface

Four commands; thin delegation layer. All Databricks auth via `--host` / `--token` or env vars.

| Command | Required args | Key flags |
|---------|--------------|-----------|
| `ucm --version` | — | Prints version and exits |
| `ucm generate` | `--source`, `--output` | `--join` (repeatable), `--overwrite` |
| `ucm inspect` | `--source` | `--join` (repeatable) |
| `ucm validate` | `PATH` (file or dir) | `--strict` (warnings → errors) |
| `ucm deploy` | `PATH`, `--catalog`, `--schema`, `--warehouse-id` | `--dry-run` |

All Databricks commands wrap SDK exceptions with human-readable error messages (auth failures, unreachable host, warehouse errors).

---

## End-to-End Operational Flow

```
 INITIAL SETUP
 ═════════════

 1. pip install uc-metric-views

 2. Inspect available tables (read-only):
    ucm inspect --source analytics.gold.fct_orders \
                --join analytics.gold.dim_customer

 3. Generate a metric view YAML (per business concept):
    ucm generate --source analytics.gold.fct_orders \
                 --join analytics.gold.dim_customer \
                 --join analytics.gold.dim_product \
                 --output ./metric_views/order_metrics.yaml

 4. Review and enrich the generated YAML:
    ├── Fix any mis-classified columns
    ├── Add custom measures (ratios, filtered aggregations)
    ├── Add semantic metadata (display_name, format, synonyms)
    ├── Add window measures if needed (trailing, cumulative, YTD)
    ├── Add materialization config if performance tuning needed
    └── Add snowflake joins for deeply normalized dim tables

 5. Validate:
    ucm validate ./metric_views/ --strict

 6. Deploy to dev:
    ucm deploy ./metric_views/ \
      --catalog dev --schema metrics --warehouse-id abc123

 7. Commit and push → PR → CI validates → merge → CD deploys

 ────────────────────────────────────────────────────────────────

 ONGOING: NEW METRIC VIEWS
 ═════════════════════════

 For each new business concept:
   1. ucm generate --source ... --join ... --output ./metric_views/new_concept.yaml
   2. Review and edit
   3. ucm validate → ucm deploy --dry-run → commit → PR → merge

 ────────────────────────────────────────────────────────────────

 CI/CD PIPELINE
 ═════════════

 PR opened:
   ├── ruff + mypy (code quality)
   ├── pytest (unit tests)
   └── ucm validate --strict (YAML integrity)

 Merged to main:
   ├── ucm validate --strict
   └── ucm deploy → target environment
```

---

## Scope: What's In and What's Not

### In scope (v0.1)

- `ucm generate` — per-metric-view scaffolding with star schema join support
- `ucm inspect` — read-only column inventory
- `ucm validate` — full spec validation including experimental feature warnings
- `ucm deploy` — DDL deployment via SDK with dry-run support
- Pydantic models covering the complete YAML spec (v0.1 + v1.1 + experimental)
- GitHub Actions for CI/CD

### Intentionally deferred

See [ROADMAP.md](ROADMAP.md) for features planned for future releases and their rationale.
