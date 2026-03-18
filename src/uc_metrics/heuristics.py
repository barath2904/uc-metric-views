"""Heuristics for classifying columns as dimension, measure, or ignore.

Priority order (highest wins):
  1. Name pattern match
  2. Type-based inference
  3. Conservative default → DIMENSION
"""
from __future__ import annotations

import re

from .models import ColumnRole, DiscoveredColumn

# ── NAME PATTERNS ──────────────────────────────────────────────────
_IGNORE_PATTERNS: list[str] = [
    r"^_.*",
    r".*_(created|updated|modified|loaded|ingested)_at$",
    r"^etl_.*", r"^dwh_.*", r"^meta_.*",
    r"^__.*",
]

_DIMENSION_NAME_PATTERNS: list[str] = [
    r".*_id$", r".*_key$", r".*_sk$",
    r".*_code$", r".*_name$", r".*_desc$",
    r".*_type$", r".*_status$", r".*_state$",
    r".*_category$", r".*_class$", r".*_group$",
    r".*_region$", r".*_country$", r".*_city$",
    r"^is_.*", r"^has_.*", r"^flag_.*",
    r".*_date$", r".*_month$", r".*_year$",
    r".*_at$",
]

_MEASURE_NAME_PATTERNS: list[str] = [
    r".*_amount$", r".*_amt$",
    r".*_total$", r".*_sum$",
    r".*_count$", r".*_cnt$",
    r".*_qty$", r".*_quantity$",
    r".*_price$", r".*_cost$", r".*_revenue$",
    r".*_rate$", r".*_ratio$", r".*_pct$", r".*_percent$",
    r".*_balance$", r".*_fee$", r".*_tax$",
    r".*_weight$", r".*_volume$",
    r".*_score$", r".*_value$",
]

# ── TYPE SETS ──────────────────────────────────────────────────────
# SDK ColumnTypeName enum values + common SQL aliases for compatibility
_DIMENSION_TYPES: set[str] = {
    "STRING", "VARCHAR", "CHAR", "TEXT",        # SDK: STRING, CHAR
    "DATE", "TIMESTAMP", "TIMESTAMP_NTZ",       # SDK: DATE, TIMESTAMP, TIMESTAMP_NTZ
    "BOOLEAN", "BINARY",                        # SDK: BOOLEAN, BINARY
}

_MEASURE_TYPES: set[str] = {
    "INT", "INTEGER",                           # SDK: INT
    "LONG", "BIGINT",                           # SDK: LONG
    "SHORT", "SMALLINT",                        # SDK: SHORT
    "BYTE", "TINYINT",                          # SDK: BYTE
    "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC",    # SDK: FLOAT, DOUBLE, DECIMAL
}


def _matches_any(name: str, patterns: list[str]) -> bool:
    return any(re.match(p, name) for p in patterns)


def _base_type(type_name: str) -> str:
    """'DECIMAL(10,2)' → 'DECIMAL'"""
    return type_name.upper().split("(")[0].strip()


def classify_column(col: DiscoveredColumn) -> ColumnRole:
    """Classify a single column. Deterministic, stateless, testable."""
    name = col.name.lower()

    if _matches_any(name, _IGNORE_PATTERNS):
        return ColumnRole.IGNORE
    if _matches_any(name, _DIMENSION_NAME_PATTERNS):
        return ColumnRole.DIMENSION
    if _matches_any(name, _MEASURE_NAME_PATTERNS):
        return ColumnRole.MEASURE

    base = _base_type(col.type_name)
    if base in _DIMENSION_TYPES:
        return ColumnRole.DIMENSION
    if base in _MEASURE_TYPES:
        return ColumnRole.MEASURE

    return ColumnRole.DIMENSION


def suggest_aggregation(col_name: str, col_type: str) -> str:
    """Suggest a default aggregation function for a measure column."""
    name = col_name.lower()
    if any(kw in name for kw in ("count", "cnt")):
        return f"SUM({col_name})"
    if any(kw in name for kw in ("avg", "rate", "ratio", "pct", "percent")):
        return f"AVG({col_name})"
    if "max" in name:
        return f"MAX({col_name})"
    if "min" in name:
        return f"MIN({col_name})"
    return f"SUM({col_name})"


def classify_table(columns: list[DiscoveredColumn]) -> list[DiscoveredColumn]:
    """Classify all columns. Returns copies with role assigned."""
    classified = []
    for col in columns:
        col_copy = col.model_copy()
        col_copy.role = classify_column(col)
        classified.append(col_copy)
    return classified
