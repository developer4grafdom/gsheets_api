import json
import time
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from .validators import require_sheet, require_data, require_PUT
from .services import (
    read_values, upsert_rows, apply_filters, 
    apply_pagination
)
from .utils import normalize_rows

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

        filtered_rows = apply_filters(rows, body.get("where"))
        page_rows, pagination_info = apply_pagination(
            filtered_rows,
            page=body.get("page"),
            limit=body.get("limit")
        )

        resp = JsonResponse({
            "sheet": sheet_name,
            "headers": headers,
            "rows": page_rows,
            **pagination_info  # Includes total, hasNextPage, limit
        })

        return resp
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=404)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=400)


@csrf_exempt
@require_PUT
def update_sheet(request, spreadsheet_id: str):
   
    try:
        body = json.loads(request.body.decode("utf-8") or "{}")
        sheet_name = require_sheet(request.GET)
        data = require_data(body)
        if not isinstance(data, dict):
            raise Exception("'data' must be an object")

        where = body.get("where")
        multiple_param = str(request.GET.get("multiple", "false")).strip().lower()
        update_all = multiple_param == 'true'

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
                "updated": result["updated"],
                "appended": result["appended"],
            }
        )
        return resp
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=400)
