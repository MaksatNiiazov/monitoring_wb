from django.contrib import admin

from .models import (
    Campaign,
    DailyCampaignProductStat,
    DailyProductKeywordStat,
    DailyProductMetrics,
    DailyProductNote,
    DailyProductStock,
    DailyWarehouseStock,
    MonitoringSettings,
    Product,
    ProductCampaign,
    SyncLog,
    Warehouse,
)


@admin.register(Product)
class ProductAdmin(admin.ModelAdmin):
    list_display = ("nm_id", "title", "vendor_code", "brand_name", "is_active")
    list_filter = ("is_active", "brand_name", "subject_name")
    search_fields = ("title", "vendor_code", "nm_id")


class ProductCampaignInline(admin.TabularInline):
    model = ProductCampaign
    extra = 0


@admin.register(Campaign)
class CampaignAdmin(admin.ModelAdmin):
    list_display = ("external_id", "name", "monitoring_group", "bid_type", "payment_type", "status", "is_active")
    list_filter = ("monitoring_group", "bid_type", "payment_type", "status", "is_active")
    search_fields = ("name", "external_id")
    inlines = [ProductCampaignInline]


@admin.register(Warehouse)
class WarehouseAdmin(admin.ModelAdmin):
    list_display = ("office_id", "name", "region_name", "is_visible_in_monitoring")
    list_filter = ("is_visible_in_monitoring", "region_name")
    search_fields = ("name", "office_id")


@admin.register(DailyProductMetrics)
class DailyProductMetricsAdmin(admin.ModelAdmin):
    list_display = ("product", "stats_date", "open_count", "add_to_cart_count", "order_count", "order_sum")
    list_filter = ("stats_date",)
    search_fields = ("product__title", "product__nm_id")


@admin.register(DailyProductStock)
class DailyProductStockAdmin(admin.ModelAdmin):
    list_display = ("product", "stats_date", "total_stock", "in_way_to_client", "in_way_from_client")
    list_filter = ("stats_date",)
    search_fields = ("product__title", "product__nm_id")


@admin.register(DailyWarehouseStock)
class DailyWarehouseStockAdmin(admin.ModelAdmin):
    list_display = ("product", "warehouse", "stats_date", "stock_count", "in_way_to_client", "in_way_from_client")
    list_filter = ("stats_date", "warehouse")
    search_fields = ("product__title", "product__nm_id", "warehouse__name")


@admin.register(DailyCampaignProductStat)
class DailyCampaignProductStatAdmin(admin.ModelAdmin):
    list_display = ("campaign", "product", "stats_date", "zone", "impressions", "clicks", "order_count", "spend")
    list_filter = ("stats_date", "zone", "campaign__monitoring_group")
    search_fields = ("campaign__name", "campaign__external_id", "product__title", "product__nm_id")


@admin.register(DailyProductKeywordStat)
class DailyProductKeywordStatAdmin(admin.ModelAdmin):
    list_display = ("product", "stats_date", "query_text", "frequency", "organic_position", "boosted_position", "boosted_ctr")
    list_filter = ("stats_date",)
    search_fields = ("product__title", "product__nm_id", "query_text")


@admin.register(DailyProductNote)
class DailyProductNoteAdmin(admin.ModelAdmin):
    list_display = ("product", "note_date", "spp_percent", "seller_price", "wb_price", "price_changed")
    list_filter = ("note_date", "price_changed", "unified_enabled", "manual_search_enabled", "manual_shelves_enabled")
    search_fields = ("product__title", "product__nm_id")


@admin.register(SyncLog)
class SyncLogAdmin(admin.ModelAdmin):
    list_display = ("kind", "status", "target_date", "created_at", "finished_at")
    list_filter = ("kind", "status", "target_date")
    readonly_fields = ("payload",)


@admin.register(MonitoringSettings)
class MonitoringSettingsAdmin(admin.ModelAdmin):
    list_display = (
        "project_name",
        "report_timezone",
        "sync_hour",
        "sync_minute",
        "overwrite_within_day",
    )
