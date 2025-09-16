import re
from typing import List

def col_idx_to_a1(n: int) -> str:
    """0-based index -> A1 column letters."""
    s = ""
    n += 1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(r + 65) + s
    return s

def normalize_rows(headers: List[str], raw_rows: List[List[str]]) -> List[dict]:
    """Pad rows to headers length and return dicts keyed by header."""
    out = []
    for r in raw_rows:
        padded = r + [""] * (len(headers) - len(r))
        out.append(dict(zip(headers, padded)))
    return out
