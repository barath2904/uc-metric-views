# Examples

Ready-to-use metric view YAML files for trying `uc-metric-views` on any Databricks workspace.

For installation, authentication, and full usage see the [main README](../README.md).

## Files

| File | What it demonstrates |
|------|---------------------|
| `basic_single_table.yaml` | Simplest metric view — NYC taxi data, no joins |
| `star_schema_joins.yaml` | Fact + dimension join (TPC-H orders + customers) |
| `with_semantic_metadata.yaml` | `display_name`, `format`, `synonyms` for AI/BI tools |

All examples use the `samples` catalog available on every Databricks workspace:

- **`samples.nyctaxi.trips`** — NYC taxi trip records
- **`samples.tpch.orders`** — TPC-H benchmark order data
- **`samples.tpch.customer`** — TPC-H benchmark customer data

## Try them

```bash
# Validate
ucm validate ./examples/

# Dry run a single file
ucm deploy examples/basic_single_table.yaml \
  --catalog your_catalog --schema your_schema --dry-run

# Deploy all
ucm deploy examples/ \
  --catalog your_catalog --schema your_schema
```
