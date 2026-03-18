# uc-metric-views

A Python CLI tool (`ucm`) that generates, validates, and deploys [Databricks Unity Catalog metric view](https://docs.databricks.com/en/sql/language-manual/sql-ref-metric-view.html) YAML definitions.

## Installation

```bash
pip install uc-metric-views
```

For development:

```bash
git clone https://github.com/barath2904/uc-metric-views.git
cd uc-metric-views
pip install -e ".[dev]"
```

## Quick Start

### 1. Inspect a table

See column types and suggested roles (dimension / measure / ignore) before generating:

```bash
ucm inspect --source analytics.gold.fct_orders \
            --join analytics.gold.dim_customer
```

### 2. Generate a metric view YAML

Scaffold a YAML file from a fact table and optional dimension joins:

```bash
ucm generate --source analytics.gold.fct_orders \
             --join analytics.gold.dim_customer \
             --output ./examples/order_metrics.yaml
```

Review and refine the generated file — add semantic metadata, window measures, or materialization config as needed.

### 3. Validate

Check YAML files against the Databricks metric view spec:

```bash
ucm validate ./examples/                # validate a directory
ucm validate ./examples/order_metrics.yaml  # validate a single file
ucm validate ./examples/ --strict       # treat warnings as errors (CI mode)
```

### 4. Deploy

Deploy to a Databricks Unity Catalog schema:

```bash
ucm deploy ./examples/ \
  --catalog my_catalog --schema my_schema \
  --warehouse-id abc123

# Preview the generated SQL without executing:
ucm deploy ./examples/ \
  --catalog my_catalog --schema my_schema \
  --dry-run
```

## Authentication

All commands that connect to Databricks accept `--host` and `--token` flags, or read from environment variables:

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
export DATABRICKS_WAREHOUSE_ID="abc123"  # optional, for deploy
```

The SDK also supports `~/.databrickscfg` profiles.

## Example YAML

```yaml
version: "1.1"
comment: "NYC taxi trip metrics"
source: samples.nyctaxi.trips

dimensions:
  - name: Pickup Date
    expr: "DATE_TRUNC('DAY', tpep_pickup_datetime)"
  - name: Pickup Zip
    expr: "pickup_zip"

measures:
  - name: Trip Count
    expr: "COUNT(1)"
  - name: Total Fare
    expr: "SUM(fare_amount)"
  - name: Average Distance
    expr: "AVG(trip_distance)"
```

More examples in the [`examples/`](examples/) directory. You can point `ucm` at any directory of YAML files.

## Validation Checks

The validator runs 11 checks on each YAML file:

| # | Check | Severity |
|---|-------|----------|
| 1 | Valid YAML syntax | error |
| 2 | YAML `on` boolean key rewrite | warning |
| 3 | Pydantic structural validation | error |
| 4 | Version is supported (`"1.1"`) | error |
| 5 | Source is FQN or SQL query | warning |
| 6 | Measures contain aggregate functions | warning |
| 7 | Experimental: `materialization` present | warning |
| 8 | Experimental: `window` measures present | warning |
| 9 | Format type is valid | error |
| 10 | Synonym count ≤ 10 per column | error |
| 11 | Join keys contain no `???` placeholders | error |

## Requirements

- Python 3.10+
- A Databricks workspace (for `generate`, `inspect`, and `deploy` commands)
- No Databricks connection needed for `validate`

## License

[MIT](LICENSE)
