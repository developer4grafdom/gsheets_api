import os, json, base64, time
from typing import List, Dict, Tuple, Optional, Any
from django.conf import settings
from google.oauth2 import service_account
from googleapiclient.discovery import build
from .utils import col_idx_to_a1, normalize_rows
from .filters import build_predicate

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Module-level caches
_CACHED_CREDS: Optional[Any] = None
_CACHED_SERVICE: Optional[Any] = None
_READ_CACHE: Dict[Tuple[str, str], Tuple[float, List[List[str]]]] = {}
_READ_CACHE_TTL_SECONDS = 10.0

def _load_credentials():
    global _CACHED_CREDS
    if _CACHED_CREDS is not None:
        return _CACHED_CREDS
    if settings.GOOGLE_SERVICE_ACCOUNT_INFO_B64:
        info = json.loads(base64.b64decode(settings.GOOGLE_SERVICE_ACCOUNT_INFO_B64))
        _CACHED_CREDS = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
        return _CACHED_CREDS
    raise RuntimeError("No Google credentials found. Configure one of the env vars in .env.")

def get_sheets_service():
    global _CACHED_SERVICE
    if _CACHED_SERVICE is not None:
        return _CACHED_SERVICE
    creds = _load_credentials()
    _CACHED_SERVICE = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return _CACHED_SERVICE

def read_values(spreadsheet_id: str, a1_range: str, use_cache: bool = True) -> List[List[str]]:
    """Fast read helper with tiny TTL cache."""
    cache_key = (spreadsheet_id, a1_range)
    now = time.time()

    if use_cache and cache_key in _READ_CACHE:
        ts, cached = _READ_CACHE[cache_key]
        if (now - ts) <= _READ_CACHE_TTL_SECONDS:
            return cached

    svc = get_sheets_service()
    req = (
        svc.spreadsheets()
        .values()
        .get(
            spreadsheetId=spreadsheet_id,
            range=a1_range,
            valueRenderOption="UNFORMATTED_VALUE",
            dateTimeRenderOption="FORMATTED_STRING",
            majorDimension="ROWS",
            fields="values"
        )
    )
    resp = req.execute()
    values = resp.get("values", [])

    if use_cache:
        _READ_CACHE[cache_key] = (now, values)
    return values

def filter_rows(headers: List[str], rows: List[dict], filters: Dict[str, str]) -> List[Tuple[int, dict]]:
    """
    Return [(row_index, row_dict)] where row_index is 0-based within provided rows (not including header).
    All filters must match exactly (string compare).
    """
    if not filters:
        return list(enumerate(rows))
    selected = []
    for i, r in enumerate(rows):
        ok = True
        for k, v in filters.items():
            if str(r.get(k, "")) != str(v):
                ok = False
                break
        if ok:
            selected.append((i, r))
    return selected


def upsert_rows(
    spreadsheet_id: str,
    sheet_name: str,
    where: Any,
    data: Dict[str, Any],
    update_all: bool = False,
) -> Dict[str, Any]:
    """
    Update matched rows using 'where' filter. If no match, append a new row.
    Returns a payload with headers, updated/appended counts and affected rows.
    """
    values = read_values(spreadsheet_id, sheet_name, use_cache=False)
    if not values:
        raise Exception("Sheet appears empty or unreadable")

    headers = values[0]
    rows = normalize_rows(headers, values[1:])

    predicate = build_predicate(where) if where is not None else (lambda r: True)
    selected_indices = [i for i, r in enumerate(rows) if predicate(r)]

    svc = get_sheets_service()
    updated = 0
    appended = 0

    if selected_indices:
        target_indices = selected_indices if update_all else [selected_indices[0]]
        for idx in target_indices:
            sheet_row_num = idx + 2  # include header row
            end_col = col_idx_to_a1(len(headers) - 1)
            a1_range = f"{sheet_name}!A{sheet_row_num}:{end_col}{sheet_row_num}"

            existing_dict = rows[idx]
            updated_row = [existing_dict.get(h, "") for h in headers]
            for k, v in data.items():
                if k in headers:
                    col_pos = headers.index(k)
                    updated_row[col_pos] = v

            req = (
                svc.spreadsheets()
                .values()
                .update(
                    spreadsheetId=spreadsheet_id,
                    range=a1_range,
                    valueInputOption="RAW",
                    body={"values": [updated_row]},
                )
            )
            req.execute()
            updated += 1
    else:
        # Append new row with provided fields only
        new_row = [""] * len(headers)
        for k, v in data.items():
            if k in headers:
                new_row[headers.index(k)] = v
        req = (
            svc.spreadsheets()
            .values()
            .append(
                spreadsheetId=spreadsheet_id,
                range=sheet_name,
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": [new_row]},
            )
        )
        req.execute()
        appended = 1

    return {
        "headers": headers,
        "updated": updated,
        "appended": appended,
    }
