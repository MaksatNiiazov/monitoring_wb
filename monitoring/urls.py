from django.urls import path

from . import views

app_name = "monitoring"

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("table/", views.table_workspace, name="table"),
    path("products/", views.products_workspace, name="products"),
    path("campaigns/", views.campaigns_workspace, name="campaigns"),
    path("campaigns/<int:pk>/", views.campaign_detail, name="campaign_detail"),
    path("reports/", views.reports, name="reports"),
    path("settings/", views.workspace_settings, name="workspace_settings"),
    path("settings/save/", views.update_workspace_settings, name="update_workspace_settings"),
    path("settings/workbook/", views.download_monitoring_workbook, name="download_monitoring_workbook"),
    path("products/add/", views.add_product, name="add_product"),
    path("campaigns/add/", views.add_campaign, name="add_campaign"),
    path("campaigns/<int:pk>/settings/", views.update_campaign, name="update_campaign"),
    path("campaigns/<int:pk>/toggle/", views.toggle_campaign_active, name="toggle_campaign_active"),
    path("sync/", views.sync_all, name="sync_all"),
    path("sync/cancel/", views.sync_cancel, name="sync_cancel"),
    path("sync/status/", views.sync_status, name="sync_status"),
    path("table/note-cell/", views.update_table_note_cell, name="update_table_note_cell"),
    path("products/<int:pk>/", views.product_detail, name="product_detail"),
    path("products/<int:pk>/sync/", views.sync_product, name="sync_product"),
    path("products/<int:pk>/settings/", views.update_product_settings, name="update_product_settings"),
    path("products/<int:pk>/note/", views.update_daily_note, name="update_daily_note"),
    path("products/<int:pk>/export/", views.export_product_csv, name="export_product_csv"),
]
