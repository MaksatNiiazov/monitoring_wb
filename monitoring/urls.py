from django.urls import path

from . import views

app_name = "monitoring"

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("reports/", views.reports, name="reports"),
    path("settings/", views.workspace_settings, name="workspace_settings"),
    path("settings/save/", views.update_workspace_settings, name="update_workspace_settings"),
    path("settings/demo/", views.load_demo_data, name="load_demo_data"),
    path("settings/workbook/", views.download_monitoring_workbook, name="download_monitoring_workbook"),
    path("settings/google-sync/", views.sync_google_sheets, name="sync_google_sheets"),
    path("settings/templates/<str:template_key>/", views.download_customer_template, name="download_customer_template"),
    path("products/add/", views.add_product, name="add_product"),
    path("campaigns/add/", views.add_campaign, name="add_campaign"),
    path("sync/", views.sync_all, name="sync_all"),
    path("products/<int:pk>/", views.product_detail, name="product_detail"),
    path("products/<int:pk>/sync/", views.sync_product, name="sync_product"),
    path("products/<int:pk>/settings/", views.update_product_settings, name="update_product_settings"),
    path("products/<int:pk>/note/", views.update_daily_note, name="update_daily_note"),
    path("products/<int:pk>/export/", views.export_product_csv, name="export_product_csv"),
]
