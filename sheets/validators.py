from django.views.decorators.http import require_http_methods

def require_sheet(payload):
    name = payload.get("sheet")
    if not name:
        raise Exception("Missing 'sheet' name.")
    return name

def require_data(payload):
    data = payload.get("data")
    if not isinstance(data, dict) or not data:
        raise Exception("'data' must be a non-empty object")
    return data

def require_PUT(view_func):
    return require_http_methods(["PUT"])(view_func)