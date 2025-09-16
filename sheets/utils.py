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
    if not raw_rows:
        return []

    hdr_len = len(headers)
    hdr_tuple = tuple(headers)
    pad_cache = None

    out = []
    append = out.append
    for r in raw_rows:
        r_len = len(r)
        if r_len < hdr_len:
            need = hdr_len - r_len
            if pad_cache is None or len(pad_cache) < need:
                pad_cache = [""] * need
            padded = list(r) + pad_cache[:need]
        else:
            if r_len > hdr_len:
                padded = r[:hdr_len]
            else:
                padded = r

        append(dict(zip(hdr_tuple, padded)))

    return out
