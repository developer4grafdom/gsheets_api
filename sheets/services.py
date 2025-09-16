import os, json, base64, time
import threading
import sys
from typing import List, Dict, Tuple, Optional, Any
from django.conf import settings
from google.oauth2 import service_account
from googleapiclient.discovery import build
from .utils import col_idx_to_a1, normalize_rows
from .filters import build_predicate

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

_CACHED_CREDS: Optional[Any] = None
_CACHED_SERVICE: Optional[Any] = None
_READ_CACHE: Dict[Tuple[str, str], Tuple[float, List[List[str]]]] = {}
_READ_CACHE_TTL_SECONDS = 10.0

def _progress_indicator(stop_event, message="Processing"):
    """Show an animated progress indicator while an operation is running."""
    i = 0
    while not stop_event.is_set():
        dots = "." * (i % 4)
        sys.stdout.write(f"\r{message}{dots.ljust(3)}")
        sys.stdout.flush()
        i += 1
        time.sleep(0.5)
    sys.stdout.write("\r")
    sys.stdout.flush()

def warmup_sheets_service():
    """
    Initialize sheets service and make a minimal API call to warm up all lazy-loaded
    components. This reduces the first-request latency.
    Only runs in the main Django process to avoid duplicate initialization.
    """
    import os
    import sys
    
    # Skip warmup in Django's autoreloader process
    if os.environ.get('RUN_MAIN') != 'true':
        return
        
    # Skip if not running under Django manage.py
    if not any('manage.py' in arg for arg in sys.argv):
        return
        
    try:
        print("Initializing sheets service (main process)...")
        svc = get_sheets_service()
        
        print("Building request...")
        req = (
            svc.spreadsheets()
            .values()
            .get(
                spreadsheetId=settings.DUMMY_SHEET_ID,
                range=settings.DUMMY_RANGE,
                fields="values",
            )
        )
        
        # Start progress indicator in background
        stop_event = threading.Event()
        progress_thread = threading.Thread(
            target=_progress_indicator,
            args=(stop_event, "Executing request")
        )
        progress_thread.daemon = True
        progress_thread.start()
        
        try:
            req.execute()
        finally:
            # Stop progress indicator
            stop_event.set()
            progress_thread.join()
        
        print("\nSheets service warmup complete!")
        
    except Exception as e:
        print("\nWarning: Sheets service warmup failed:", str(e))


def _load_credentials():
    global _CACHED_CREDS
    if _CACHED_CREDS is not None:
        return _CACHED_CREDS
    if settings.GOOGLE_SERVICE_ACCOUNT_INFO_B64:
        info = json.loads(base64.b64decode(settings.GOOGLE_SERVICE_ACCOUNT_INFO_B64))
        _CACHED_CREDS = service_account.Credentials.from_service_account_info(
            info, scopes=SCOPES
        )
        return _CACHED_CREDS
    raise RuntimeError(
        "No Google credentials found. Configure one of the env vars in .env."
    )


def get_sheets_service():
    global _CACHED_SERVICE
    if _CACHED_SERVICE is not None:
        return _CACHED_SERVICE
    creds = _load_credentials()
    _CACHED_SERVICE = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return _CACHED_SERVICE


def read_values(
    spreadsheet_id: str, a1_range: str, use_cache: bool = True
) -> List[List[str]]:
    """Fast read helper with tiny TTL cache."""
    cache_key = (spreadsheet_id, a1_range)
    now = time.time()

    # cache check
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
            fields="values",
        )
    )

    resp = req.execute()
    values = resp.get("values", [])
    if use_cache:
        _READ_CACHE[cache_key] = (now, values)

    return values


def apply_filters(rows: List[dict], where: Optional[dict] = None) -> List[dict]:
    """
    Apply filters to rows using the where clause.
    
    Args:
        rows: List of row dictionaries to filter
        where: Filter specification dictionary (optional)
    
    Returns:
        List of rows that match the filter criteria
    """
    if where is None:
        return rows
    
    predicate = build_predicate(where)
    return [r for r in rows if predicate(r)]

def apply_pagination(
    rows: List[dict],
    page: Optional[int] = None,
    limit: Optional[int] = None
) -> Tuple[List[dict], dict]:
    """
    Apply pagination to a list of rows.
    
    Args:
        rows: List of rows to paginate
        page: Page number (1-based, default: 1)
        limit: Items per page (default: 50)
    
    Returns:
        Tuple of (paginated_rows, pagination_info)
        pagination_info includes: total, page, limit, hasNextPage
    """
    try:
        page = int(page) if page is not None else 1
    except (ValueError, TypeError):
        page = 1
        
    try:
        limit = int(limit) if limit is not None else 50
    except (ValueError, TypeError):
        limit = 50
        
    if page < 1:
        page = 1
    if limit < 0:
        limit = 0
        
    total = len(rows)
    start = (page - 1) * limit if limit > 0 else 0
    end = start + limit if limit > 0 else total
    
    paginated = rows[start:end]
    has_next = (end < total)
    
    return paginated, {
        "total": total,
        "page": page,
        "limit": limit,
        "hasNextPage": has_next
    }

def filter_rows(
    headers: List[str], rows: List[dict], filters: Dict[str, str]
) -> List[Tuple[int, dict]]:
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
        
        if len(target_indices) == 1:
            idx = target_indices[0]
            sheet_row_num = idx + 2
            end_col = col_idx_to_a1(len(headers) - 1)
            range_spec = f"{sheet_name}!A{sheet_row_num}:{end_col}{sheet_row_num}"
            
            existing_dict = rows[idx]
            needs_update = False
            updated_row = [existing_dict.get(h, "") for h in headers]
            for k, v in data.items():
                if k in headers:
                    col_pos = headers.index(k)
                    if str(updated_row[col_pos]) != str(v):
                        updated_row[col_pos] = v
                        needs_update = True
            
            if needs_update:
                svc.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=range_spec,
                    valueInputOption="RAW",
                    body={"values": [updated_row]}
                ).execute()
                updated = 1
            else:
                print("upsert_rows: skipped update - no changes needed")
                updated = 0
        else:
            data_rows = []
            ranges = []
            
            for idx in target_indices:
                existing_dict = rows[idx]
                updated_row = [existing_dict.get(h, "") for h in headers]
                needs_update = False
                
                for k, v in data.items():
                    if k in headers:
                        col_pos = headers.index(k)
                        if str(updated_row[col_pos]) != str(v):
                            updated_row[col_pos] = v
                            needs_update = True
                
                if needs_update:
                    sheet_row_num = idx + 2 
                    end_col = col_idx_to_a1(len(headers) - 1)
                    ranges.append(f"{sheet_name}!A{sheet_row_num}:{end_col}{sheet_row_num}")
                    data_rows.append(updated_row)
            
            if data_rows:
                batch_body = {
                    "valueInputOption": "RAW",
                    "data": [
                        {"range": range_spec, "values": [row]}
                        for range_spec, row in zip(ranges, data_rows)
                    ]
                }
                
                svc.spreadsheets().values().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body=batch_body
                ).execute()
                updated = len(data_rows)
            else:
                print("upsert_rows: skipped batch update - no changes needed")
                updated = 0
    else:
        new_row = [""] * len(headers)
        for k, v in data.items():
            if k in headers:
                new_row[headers.index(k)] = v
        
        last_row = len(values)
        end_col = col_idx_to_a1(len(headers) - 1)
        range_spec = f"{sheet_name}!A{last_row}:{end_col}{last_row}"
        
        req = (
            svc.spreadsheets()
            .values()
            .append(
                spreadsheetId=spreadsheet_id,
                range=range_spec,
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": [new_row]},
            )
        )
        req.execute()
        appended = 1

    return {
        "updated": updated,
        "appended": appended,
    }
