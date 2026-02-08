"""
VTagger DSL Parser.

Generic DSL parser for dimension mapping rules.
Supports TAG['key'] and DIMENSION['key'] accessors with == and CONTAINS operators.
"""

import re
from typing import Any, Dict, List, Optional, Set, Tuple


# Regex patterns - compiled once at module level
_TAG_PATTERN = re.compile(r"TAG\['([^']+)'\]\s*(==|CONTAINS)\s*'([^']*)'")
_DIM_PATTERN = re.compile(r"(?:BUSINESS_)?DIMENSION\['([^']+)'\]\s*(==|CONTAINS)\s*'([^']*)'")
_VALUE_PATTERN = re.compile(r"'([^']*)'")


def _parse_single_expr(expr: str) -> Optional[Dict]:
    """Parse a single comparison expression."""
    tag_match = _TAG_PATTERN.search(expr)
    if tag_match:
        key, op, value = tag_match.groups()
        return {"type": "TAG", "key": key, "op": op, "value": value.lower()}

    dim_match = _DIM_PATTERN.search(expr)
    if dim_match:
        key, op, value = dim_match.groups()
        return {"type": "DIM", "key": key, "op": op, "value": value.lower()}

    return None


def parse_expression(match_expr: str) -> List[Dict]:
    """Parse a match expression into structured conditions.

    Returns list of condition dicts (OR'd together):
    [{"type": "TAG"|"DIM", "key": "...", "op": "=="|"CONTAINS", "value": "..."}, ...]
    """
    if " || " in match_expr:
        parts = [_parse_single_expr(p.strip()) for p in match_expr.split(" || ")]
        return [p for p in parts if p]

    single = _parse_single_expr(match_expr)
    return [single] if single else []


def parse_value_expression(value_expr: str) -> str:
    """Extract literal string from value expression. Input: "'Some Value'" -> Output: "Some Value" """
    match = _VALUE_PATTERN.search(value_expr)
    if match:
        return match.group(1)
    return value_expr.strip("'\"")


def extract_tag_keys(statements: List[Dict]) -> Set[str]:
    """Scan all statements and return set of TAG['key'] keys referenced."""
    keys = set()
    for stmt in statements:
        match_expr = stmt.get("matchExpression", "")
        for m in _TAG_PATTERN.finditer(match_expr):
            keys.add(m.group(1))
    return keys


def extract_dimension_keys(statements: List[Dict]) -> Set[str]:
    """Scan all statements and return set of DIMENSION['key'] keys referenced."""
    keys = set()
    for stmt in statements:
        match_expr = stmt.get("matchExpression", "")
        for m in _DIM_PATTERN.finditer(match_expr):
            keys.add(m.group(1))
    return keys


def validate_dimension_json(content: Dict) -> List[str]:
    """Validate a dimension JSON blob. Returns list of error messages (empty = valid)."""
    errors = []

    if not content.get("vtag_name") and not content.get("name"):
        errors.append("Missing required field: vtag_name or name")

    statements = content.get("statements", [])
    if not isinstance(statements, list):
        errors.append("'statements' must be a list")
        return errors

    for i, stmt in enumerate(statements):
        if not isinstance(stmt, dict):
            errors.append(f"Statement {i}: must be an object")
            continue

        match_expr = stmt.get("matchExpression")
        value_expr = stmt.get("valueExpression")

        if not match_expr:
            errors.append(f"Statement {i}: missing matchExpression")
            continue
        if not value_expr:
            errors.append(f"Statement {i}: missing valueExpression")
            continue

        # Validate matchExpression parses
        conditions = parse_expression(match_expr)
        if not conditions:
            errors.append(f"Statement {i}: cannot parse matchExpression: {match_expr}")

        # Validate valueExpression
        parsed_value = parse_value_expression(value_expr)
        if not parsed_value:
            errors.append(f"Statement {i}: empty valueExpression")

    return errors


def build_indexes(statements: List[Dict]) -> Dict:
    """Pre-parse all statements into fast-lookup indexes.

    Returns:
    {
        "tag_exact": {(key, value_lower): result_value},
        "dim_exact": {(key, value_lower): result_value},
        "tag_contains": [(key, substring_lower, result_value)],
        "dim_contains": [(key, substring_lower, result_value)],
        "tag_keys_used": set(),
        "dim_keys_used": set(),
    }
    """
    tag_exact = {}
    dim_exact = {}
    tag_contains = []
    dim_contains = []
    tag_keys_used = set()
    dim_keys_used = set()

    for stmt in statements:
        match_expr = stmt.get("matchExpression", "")
        value_expr = stmt.get("valueExpression", "")
        result = parse_value_expression(value_expr)

        # Parse conditions
        conditions = parse_expression(match_expr)

        for cond in conditions:
            ctype = cond["type"]
            ckey = cond["key"]
            cop = cond["op"]
            cvalue = cond["value"]

            if ctype == "TAG":
                tag_keys_used.add(ckey)
                if cop == "==":
                    if (ckey, cvalue) not in tag_exact:
                        tag_exact[(ckey, cvalue)] = result
                elif cop == "CONTAINS":
                    # Also index as exact for fast path
                    if (ckey, cvalue) not in tag_exact:
                        tag_contains.append((ckey, cvalue, result))
            else:  # DIM
                dim_keys_used.add(ckey)
                if cop == "==":
                    if (ckey, cvalue) not in dim_exact:
                        dim_exact[(ckey, cvalue)] = result
                elif cop == "CONTAINS":
                    dim_contains.append((ckey, cvalue, result))

    return {
        "tag_exact": tag_exact,
        "dim_exact": dim_exact,
        "tag_contains": tag_contains,
        "dim_contains": dim_contains,
        "tag_keys_used": tag_keys_used,
        "dim_keys_used": dim_keys_used,
    }
