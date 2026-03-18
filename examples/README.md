# uc-metric-views Examples

Ready-to-use metric view YAML files for trying `uc-metric-views` on any Databricks workspace (including the free Community Edition).

## Prerequisites

1. A Databricks workspace with a SQL warehouse
2. A catalog and schema where you have `CREATE VIEW` permission
3. `uc-metric-views` installed: `pip install uc-metric-views`
4. Auth configured via environment variables:
   ```bash
   export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
   export DATABRICKS_TOKEN="dapi..."
   export DATABRICKS_WAREHOUSE_ID="abc123..."
   ```

## Examples

| File | What it demonstrates |
|------|---------------------|
| `basic_single_table.yaml` | Simplest metric view — NYC taxi data, no joins |
| `star_schema_joins.yaml` | Fact + dimension join (TPC-H orders + customers) |
| `with_semantic_metadata.yaml` | display_name, format, synonyms for AI/BI tools |

## Quick Start

```bash
# 1. Validate all examples
ucm validate ./examples/

# 2. Deploy one example (dry run first)
ucm deploy examples/basic_single_table.yaml \
  --catalog your_catalog \
  --schema your_schema \
  --warehouse-id $DATABRICKS_WAREHOUSE_ID \
  --dry-run

# 3. Deploy for real
ucm deploy examples/basic_single_table.yaml \
  --catalog your_catalog \
  --schema your_schema \
  --warehouse-id $DATABRICKS_WAREHOUSE_ID

# 4. Deploy all examples at once
ucm deploy examples/ \
  --catalog your_catalog \
  --schema your_schema \
  --warehouse-id $DATABRICKS_WAREHOUSE_ID
```

## Sample Data Sources

These examples use datasets available on every Databricks workspace:

- **`samples.nyctaxi.trips`** — NYC taxi trip records
- **`samples.tpch.orders`** — TPC-H benchmark order data
- **`samples.tpch.customer`** — TPC-H benchmark customer data
