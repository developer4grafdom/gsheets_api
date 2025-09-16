
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from datetime import datetime


Scalar = Union[str, int, float, bool, None]


def _is_nullish(value: Any) -> bool:
    return value is None or value == ""


def _to_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        s = value.strip().lower()
        if s in ("true", "t", "yes", "y", "1"):
            return True
        if s in ("false", "f", "no", "n", "0"):
            return False
    return None


def _to_number(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        s = value.strip()
        try:
            return float(s)
        except Exception:
            return None
    return None


_DATE_FORMATS = (
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%m/%d/%Y",
    "%d/%m/%Y",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
)


def _to_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        # Reject pure numbers to avoid mis-parsing scores as timestamps
        return None
    if isinstance(value, str):
        s = value.strip()
        # Try ISO first
        try:
            return datetime.fromisoformat(s)
        except Exception:
            pass
        for fmt in _DATE_FORMATS:
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                continue
    return None


def _coerce_pair(lhs: Any, rhs: Any) -> Tuple[Scalar, Scalar, str]:
    """
    Coerce cell and value into a comparable pair with a type tag: number|datetime|string|bool|null.
    The strategy is simple and fast: try datetime, then number, then bool, else string.
    """
    if _is_nullish(lhs) and _is_nullish(rhs):
        return (None, None, "null")

    ldt = _to_datetime(lhs)
    rdt = _to_datetime(rhs)
    if ldt is not None and rdt is not None:
        return (ldt, rdt, "datetime")

    ln = _to_number(lhs)
    rn = _to_number(rhs)
    if ln is not None and rn is not None:
        return (ln, rn, "number")

    lb = _to_bool(lhs)
    rb = _to_bool(rhs)
    if lb is not None and rb is not None:
        return (lb, rb, "bool")

    return ("" if lhs is None else str(lhs), "" if rhs is None else str(rhs), "string")


def _cmp(lhs: Any, rhs: Any, op: str) -> bool:
    a, b, kind = _coerce_pair(lhs, rhs)
    if op == "eq":
        return a == b
    if op == "ne":
        return a != b
    if kind in ("number", "datetime", "string"):
        if op == "gt":
            return a > b
        if op == "gte":
            return a >= b
        if op == "lt":
            return a < b
        if op == "lte":
            return a <= b
    return False


def _like(cell: Any, pattern: Any) -> bool:
    if _is_nullish(cell) or _is_nullish(pattern):
        return False
    return str(pattern).lower() in str(cell).lower()


def _between(cell: Any, low: Any, high: Any) -> bool:
    a_l, b_l, k1 = _coerce_pair(cell, low)
    a_h, b_h, k2 = _coerce_pair(cell, high)
    # Ensure same coercion kind for both comparisons
    if k1 != k2:
        return False
    return (a_l >= b_l) and (a_h <= b_h) if False else (a_l >= b_l and a_h <= b_h)


def _in_list(cell: Any, options: List[Any]) -> bool:
    for opt in options:
        if _cmp(cell, opt, "eq"):
            return True
    return False


def _not_in_list(cell: Any, options: List[Any]) -> bool:
    for opt in options:
        if _cmp(cell, opt, "eq"):
            return False
    return True


def build_predicate(payload: Any) -> Callable[[Dict[str, Any]], bool]:
    """
    Supports a simple AND/OR with flat lists of conditions and operators:
      eq, ne, gt, gte, lt, lte, like, between, in, not_in, is_null, is_not_null.

    Payload can be either the whole body { sheet, limit, where } or just where.
    Filter where format:
      { "and": [ ... ], "or": [ ... ] }
    If both are present: match = (all AND) and (any OR). If only one present, use it.
    """

    # Extract where from either the full payload or direct where object
    where = payload.get("where") if isinstance(payload, dict) and "where" in payload else payload

    if not isinstance(where, dict):
        return lambda row: True

    and_conds = where.get("and") or []
    or_conds = where.get("or") or []
    if not isinstance(and_conds, list):
        and_conds = [and_conds]
    if not isinstance(or_conds, list):
        or_conds = [or_conds]

    def match_one(row: Dict[str, Any], cond: Dict[str, Any]) -> bool:
        field = cond.get("field")
        operator = str(cond.get("operator", "eq")).lower()
        value = cond.get("value")
        values = cond.get("values")
        cell = row.get(field)

        if operator == "is_null":
            return _is_nullish(cell)
        if operator == "is_not_null":
            return not _is_nullish(cell)

        if operator == "like":
            return _like(cell, value)

        if operator == "between":
            pair: Optional[List[Any]] = None
            if isinstance(values, list) and len(values) >= 2:
                pair = values[:2]
            elif isinstance(value, (list, tuple)) and len(value) >= 2:
                pair = list(value)[:2]
            if not pair:
                return False
            return _between(cell, pair[0], pair[1])

        if operator in ("in", "not_in"):
            opts: List[Any] = []
            if isinstance(values, list):
                opts = values
            elif isinstance(value, (list, tuple)):
                opts = list(value)
            else:
                opts = [value]
            return _in_list(cell, opts) if operator == "in" else _not_in_list(cell, opts)

        # Comparators and equality
        if operator in ("eq", "ne", "gt", "gte", "lt", "lte"):
            return _cmp(cell, value, operator)

        return False

    def predicate(row: Dict[str, Any]) -> bool:
        and_ok = True
        if and_conds:
            for cond in and_conds:
                if not isinstance(cond, dict) or not match_one(row, cond):
                    and_ok = False
                    break

        or_ok = True if not or_conds else False
        if or_conds:
            for cond in or_conds:
                if isinstance(cond, dict) and match_one(row, cond):
                    or_ok = True
                    break

        if and_conds and or_conds:
            # When both AND and OR are supplied, treat as union: (all AND) OR (any OR)
            return and_ok or or_ok
        if and_conds:
            return and_ok
        if or_conds:
            return or_ok
        return True

    return predicate
