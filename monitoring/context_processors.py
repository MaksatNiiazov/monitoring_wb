from monitoring.services.config import get_monitoring_settings


def workspace_settings(request):
    return {"workspace_settings": get_monitoring_settings()}
