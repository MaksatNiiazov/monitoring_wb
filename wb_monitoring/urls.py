from django.contrib import admin
from django.urls import include, path

from monitoring import views as monitoring_views

urlpatterns = [
    path("admin/", admin.site.urls),
    path("products/", monitoring_views.products_workspace, name="products"),
    path("", include(("monitoring.urls", "monitoring"), namespace="monitoring")),
]
