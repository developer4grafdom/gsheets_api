import json
import time
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from .validators import require_sheet, require_data, require_PUT
from .services import read_values, upsert_rows
from .utils import normalize_rows
from .filters import build_predicate

@csrf_exempt
@require_POST
def read_sheet(request, spreadsheet_id: str):
    """
    Read rows from a Google Sheet using a rich JSON filter.
    Method: POST only
    Body:
      {
        "sheet": "Sheet1",
        "where": { ... }   # optional, see filter DSL
      }
    """
    try:

        body = json.loads(request.body.decode("utf-8") or "{}")
        sheet_name = require_sheet(request.GET)
        values = read_values(spreadsheet_id, sheet_name)
        headers = values[0]
        rows = normalize_rows(headers, values[1:])

        where = body.get("where")
        if where is None:
            selected = rows
        else:
            predicate = build_predicate(where)
            selected = [r for r in rows if predicate(r)]

        # Pagination: page (1-based) and limit
        try:
            page = int(body.get("page")) if body.get("page") is not None else 1
        except Exception:
            page = 1
        try:
            limit = int(body.get("limit")) if body.get("limit") is not None else 50
        except Exception:
            limit = 50
        if page < 1:
            page = 1
        if limit < 0:
            limit = 0

        total = len(selected)
        start = (page - 1) * limit if limit > 0 else 0
        end = start + limit if limit > 0 else total
        page_rows = selected[start:end]
        has_next = (end < total)

        resp = JsonResponse(
            {
                "sheet": sheet_name,
                "headers": headers,
                "rows": page_rows,
                "total": total,
                "hasNextPage": has_next,
                "limit": limit,
            }
        )
        return resp
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=404)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=400)


@csrf_exempt
@require_PUT
def update_sheet(request, spreadsheet_id: str):
   
    try:
        t0 = time.perf_counter()

        body = json.loads(request.body.decode("utf-8") or "{}")
        t1 = time.perf_counter()
        print("update_sheet: parsed body in %.4f ms" % ((t1 - t0) * 1000))

        sheet_name = require_sheet(request.GET)
        t2 = time.perf_counter()
        print("update_sheet: validated sheet param in %.4f ms" % ((t2 - t1) * 1000))

        data = require_data(body)
        if not isinstance(data, dict):
            raise Exception("'data' must be an object")

        where = body.get("where")
        multiple_param = str(request.GET.get("multiple", "false")).strip().lower()
        update_all = multiple_param in ("1", "true", "t", "yes", "y")

        result = upsert_rows(
            spreadsheet_id=spreadsheet_id,
            sheet_name=sheet_name,
            where=where,
            data=data,
            update_all=update_all,
        )

        resp = JsonResponse(
            {
                "sheet": sheet_name,
                "headers": result["headers"],
                "updated": result["updated"],
                "appended": result["appended"],
            }
        )
        t_end = time.perf_counter()
        print("update_sheet: total time %.4f ms" % ((t_end - t0) * 1000))
        return resp
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=400)
