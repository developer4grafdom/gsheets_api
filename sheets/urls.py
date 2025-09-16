from django.urls import path
from .views import read_sheet, update_sheet

urlpatterns = [
    path("api/sheets/<str:spreadsheet_id>/read", read_sheet, name="sheets-read"),
    path("api/sheets/<str:spreadsheet_id>/update", update_sheet, name="sheets-update"),
]
