from django.apps import AppConfig

class SheetsConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'sheets'
    
    def ready(self):
        """
        Called when Django starts. Perfect place to initialize
        the sheets service to avoid first-request latency.
        """
        from .services import warmup_sheets_service
        warmup_sheets_service()
