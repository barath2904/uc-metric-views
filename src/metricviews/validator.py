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
    severity: Literal["error", "warning", "suggestion"] = "error"


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

# Allows hyphens in catalog/schema/table names (e.g. my-catalog.my-schema.orders).
# Sister pattern: deployer._IDENTIFIER_PATTERN uses the same [\w-]+ alphabet for single segments.
_FQN_PATTERN = re.compile(r"^[\w-]+\.[\w-]+\.[\w-]+$")

_VALID_FORMAT_TYPES = {"number", "currency", "percentage", "byte", "date", "date_time"}

_PYDANTIC_TYPE_MESSAGES: dict[str, str] = {
    "missing": "required field missing",
    "extra_forbidden": "unknown field — check for typos",
    "string_type": "must be a string",
    "string_too_short": "must not be empty",
    "list_type": "must be a list",
    "list_too_short": "must have at least 1 item",
    "dict_type": "must be a mapping",
    "bool_type": "must be true or false",
    "int_type": "must be an integer",
    "value_error": "",  # use msg as-is
}


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


def _format_pydantic_errors(
    filename: str, exc: pydantic.ValidationError
) -> list[YamlValidationError]:
    """Convert a Pydantic ValidationError into clean, user-facing error messages.

    Drops input_value dumps, pydantic.dev URLs, and internal type codes.
    Returns one YamlValidationError per Pydantic error.
    """
    out = []
    for err in exc.errors():
        loc = " → ".join(str(p) for p in err["loc"]) if err["loc"] else "root"
        err_type = err["type"]
        if err_type in _PYDANTIC_TYPE_MESSAGES:
            detail = _PYDANTIC_TYPE_MESSAGES[err_type] or err["msg"]
        else:
            detail = err["msg"]
        out.append(YamlValidationError(filename, f"'{loc}': {detail}"))
    return out


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
        errors.extend(_format_pydantic_errors(name, e))
        return errors

    # 4. Version check
    if spec.version != "1.1":
        errors.append(
            YamlValidationError(
                name,
                f"Unsupported version '{spec.version}' — only '1.1' is supported",
            )
        )

    # Precompute shared traversals used by multiple checks below.
    all_columns = spec.dimensions + spec.measures
    flat_joins = _flatten_joins(spec.joins) if spec.joins else []

    # 5. Source should be FQN or SQL query (suggestion — may be intentional)
    if not _FQN_PATTERN.match(spec.source) and "SELECT" not in spec.source.upper():
        errors.append(
            YamlValidationError(
                name,
                f"Source '{spec.source}' should be fully-qualified "
                "(catalog.schema.table) or a SQL query",
                "suggestion",
            )
        )

    # 6. Join source should be FQN
    errors.extend(
        YamlValidationError(
            name,
            f"Join '{j.name}' source '{j.source}' should be fully-qualified (catalog.schema.table)",
            "warning",
        )
        for j in flat_joins
        if not _FQN_PATTERN.match(j.source)
    )

    # 7. Measures should contain aggregate functions (suggestion — may be intentional)
    for m in spec.measures:
        expr_upper = m.expr.upper()
        has_agg = any(fn in expr_upper for fn in _AGG_FUNCTIONS)
        has_measure_ref = "MEASURE(" in expr_upper
        if not has_agg and not has_measure_ref and "/" not in m.expr:
            errors.append(
                YamlValidationError(
                    name,
                    f"Measure '{m.name}' may be missing aggregate function: {m.expr}",
                    "suggestion",
                )
            )

    # 8. Experimental: materialization (suggestion — valid but advisory)
    if spec.materialization:
        errors.append(
            YamlValidationError(
                name,
                "materialization is an Experimental feature — behavior may change",
                "suggestion",
            )
        )

    # 9. Experimental: window measures (suggestion — valid but advisory)
    for m in spec.measures:
        if m.window:
            errors.append(
                YamlValidationError(
                    name,
                    f"Measure '{m.name}' uses window (Experimental feature)",
                    "suggestion",
                )
            )
            break

    # 10. Format type validation
    for col in all_columns:
        if col.format is not None and isinstance(col.format, dict):
            fmt_type = col.format.get("type")
            if fmt_type is None:
                errors.append(
                    YamlValidationError(
                        name,
                        f"Column '{col.name}' format must include a 'type' key. "
                        f"Valid types: {', '.join(sorted(_VALID_FORMAT_TYPES))}",
                    )
                )
            elif fmt_type not in _VALID_FORMAT_TYPES:
                errors.append(
                    YamlValidationError(
                        name,
                        f"Column '{col.name}' has unknown format type '{fmt_type}'. "
                        f"Valid: {', '.join(sorted(_VALID_FORMAT_TYPES))}",
                    )
                )

    # 11. Synonym validation: empty strings and duplicates
    for col in all_columns:
        if col.synonyms:
            if any(s == "" for s in col.synonyms):
                errors.append(
                    YamlValidationError(
                        name,
                        f"Column '{col.name}' has empty synonym — remove blank entries",
                    )
                )
            dupes = {s for s in col.synonyms if col.synonyms.count(s) > 1}
            if dupes:
                errors.append(
                    YamlValidationError(
                        name,
                        f"Column '{col.name}' has duplicate synonyms: {dupes}",
                        "warning",
                    )
                )
            if len(col.synonyms) > 10:
                errors.append(
                    YamlValidationError(
                        name,
                        f"Column '{col.name}' has {len(col.synonyms)} synonyms (max 10)",
                    )
                )

    # 12. Placeholder join keys ('???') in on and using
    errors.extend(
        YamlValidationError(
            name,
            f"Join '{j.name}' has placeholder key — replace '???' with actual column names",
        )
        for j in flat_joins
        if (j.on and "???" in j.on) or (j.using and any("???" in u for u in j.using))
    )

    # 13. Materialized view dimension/measure name cross-reference
    if spec.materialization:
        known_names = {d.name for d in spec.dimensions} | {m.name for m in spec.measures}
        errors.extend(
            YamlValidationError(
                name,
                f"Materialized view '{mv.name}' references unknown dimension '{dim_name}'",
            )
            for mv in spec.materialization.materialized_views
            for dim_name in mv.dimensions or []
            if dim_name not in known_names
        )
        errors.extend(
            YamlValidationError(
                name,
                f"Materialized view '{mv.name}' references unknown measure '{measure_name}'",
            )
            for mv in spec.materialization.materialized_views
            for measure_name in mv.measures or []
            if measure_name not in known_names
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
