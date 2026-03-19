"""Validate metric view YAML files before deployment."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import pydantic
import yaml  # type: ignore[import-untyped]

from .models import JoinDef, MetricViewSpec


@dataclass
class YamlValidationError:
    """A validation finding. Named to avoid collision with pydantic.ValidationError."""

    file: str
    message: str
    severity: Literal["error", "warning"] = "error"


# Aggregate function prefixes used to heuristically validate measure expressions.
_AGG_FUNCTIONS = (
    "SUM(",
    "COUNT(",
    "AVG(",
    "MIN(",
    "MAX(",
    "COUNT_IF(",
    "APPROX_COUNT_DISTINCT(",
    "COLLECT_SET(",
    "COLLECT_LIST(",
    "PERCENTILE(",
    "STDDEV(",
)

_FQN_PATTERN = re.compile(r"^[\w]+\.[\w]+\.[\w]+$")

_VALID_FORMAT_TYPES = {"number", "currency", "percentage", "byte", "date", "date_time"}


def _fix_yaml_on_boolean_keys(raw: dict[str, Any]) -> list[YamlValidationError]:
    """Pre-Pydantic fix: yaml.safe_load parses unquoted 'on:' as boolean True.

    Detects {True: "join condition"} in join dicts and rewrites to {"on": "value"}.
    Returns warnings for each rewrite.
    """
    warnings: list[YamlValidationError] = []

    def _fix_joins(joins_list: list[Any]) -> None:
        for join_dict in joins_list:
            if not isinstance(join_dict, dict):
                continue
            if True in join_dict:
                join_dict["on"] = join_dict.pop(True)
                join_name = join_dict.get("name", "unknown")
                warnings.append(
                    YamlValidationError(
                        file="",  # caller fills in
                        message=(
                            f"Join '{join_name}': bare 'on:' was parsed as boolean True "
                            f"by YAML 1.1 — it must be quoted as '\"on\":' in your YAML file"
                        ),
                        severity="warning",
                    )
                )
            if "joins" in join_dict and isinstance(join_dict["joins"], list):
                _fix_joins(join_dict["joins"])

    if "joins" in raw and isinstance(raw["joins"], list):
        _fix_joins(raw["joins"])

    return warnings


def validate_file(yaml_path: str | Path) -> list[YamlValidationError]:
    """Validate a single YAML file. Returns empty list if valid."""
    path = Path(yaml_path)
    errors: list[YamlValidationError] = []
    name = path.name

    if not path.exists():
        return [YamlValidationError(name, f"File not found: {path}")]

    # 1. Valid YAML syntax
    try:
        raw = yaml.safe_load(path.read_text())
    except yaml.YAMLError as e:
        return [YamlValidationError(name, f"Invalid YAML: {e}")]
    except (OSError, UnicodeDecodeError) as e:
        return [YamlValidationError(name, f"Cannot read file: {e}")]

    if not isinstance(raw, dict):
        return [YamlValidationError(name, "YAML must be a mapping (dict)")]

    # 2. Fix YAML 'on' boolean keys BEFORE Pydantic parsing
    on_warnings = _fix_yaml_on_boolean_keys(raw)
    for w in on_warnings:
        w.file = name
    errors.extend(on_warnings)

    # 3. Pydantic structural validation
    try:
        spec = MetricViewSpec(**raw)
    except pydantic.ValidationError as e:
        return [*errors, YamlValidationError(name, f"Schema validation failed: {e}")]

    # 4. Version check
    if spec.version != "1.1":
        errors.append(
            YamlValidationError(
                name,
                f"Unsupported version '{spec.version}' — only '1.1' is supported",
            )
        )

    # 5. Source should be FQN or SQL query
    if not _FQN_PATTERN.match(spec.source) and "SELECT" not in spec.source.upper():
        errors.append(
            YamlValidationError(
                name,
                f"Source '{spec.source}' should be fully-qualified "
                "(catalog.schema.table) or a SQL query",
                "warning",
            )
        )

    # 6. Measures should contain aggregate functions
    for m in spec.measures:
        expr_upper = m.expr.upper()
        has_agg = any(fn in expr_upper for fn in _AGG_FUNCTIONS)
        has_measure_ref = "MEASURE(" in expr_upper
        if not has_agg and not has_measure_ref and "/" not in m.expr:
            errors.append(
                YamlValidationError(
                    name,
                    f"Measure '{m.name}' may be missing aggregate function: {m.expr}",
                    "warning",
                )
            )

    # 7. Experimental: materialization
    if spec.materialization:
        errors.append(
            YamlValidationError(
                name,
                "materialization is an Experimental feature — behavior may change",
                "warning",
            )
        )

    # 8. Experimental: window measures
    for m in spec.measures:
        if m.window:
            errors.append(
                YamlValidationError(
                    name,
                    f"Measure '{m.name}' uses window (Experimental feature)",
                    "warning",
                )
            )
            break

    # 9. Format type validation
    for col in list(spec.dimensions) + list(spec.measures):
        if col.format and isinstance(col.format, dict):
            fmt_type = col.format.get("type")
            if fmt_type and fmt_type not in _VALID_FORMAT_TYPES:
                errors.append(
                    YamlValidationError(
                        name,
                        f"Column '{col.name}' has unknown format type '{fmt_type}'. "
                        f"Valid: {', '.join(sorted(_VALID_FORMAT_TYPES))}",
                    )
                )

    # 10. Synonym count (max 10 per column)
    errors.extend(
        YamlValidationError(
            name,
            f"Column '{col.name}' has {len(col.synonyms)} synonyms (max 10)",
        )
        for col in list(spec.dimensions) + list(spec.measures)
        if col.synonyms and len(col.synonyms) > 10
    )

    # 11. Placeholder join keys
    if spec.joins:
        errors.extend(
            YamlValidationError(
                name,
                f"Join '{j.name}' has placeholder key — replace '???' with actual column names",
            )
            for j in _flatten_joins(spec.joins)
            if j.on and "???" in j.on
        )

    return errors


def _flatten_joins(joins: list[JoinDef]) -> list[JoinDef]:
    """Recursively flatten nested joins for validation."""
    result = []
    for j in joins:
        result.append(j)
        if j.joins:
            result.extend(_flatten_joins(j.joins))
    return result


def validate_directory(yaml_dir: str | Path) -> list[YamlValidationError]:
    """Validate all YAML files in a directory."""
    path = Path(yaml_dir)
    all_errors: list[YamlValidationError] = []

    yaml_files = sorted(path.glob("*.yaml")) + sorted(path.glob("*.yml"))
    if not yaml_files:
        return [YamlValidationError(str(path), "No YAML files found in directory")]

    for f in yaml_files:
        all_errors.extend(validate_file(f))

    return all_errors
