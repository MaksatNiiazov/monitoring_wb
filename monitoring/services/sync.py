from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
import zlib

from django.db import transaction
from django.utils import timezone

from monitoring.models import (
    Campaign,
    CampaignMonitoringGroup,
    DailyCampaignProductStat,
    DailyProductMetrics,
    DailyProductStock,
    DailyWarehouseStock,
    Product,
    ProductCampaign,
    SyncKind,
    SyncLog,
    SyncStatus,
    Warehouse,
)
from monitoring.services.config import get_monitoring_settings
from monitoring.services.reports import decimalize, map_app_type_to_zone, quantize_money
from monitoring.services.wb_client import AnalyticsWBClient, PromotionWBClient, WBApiError


class SyncServiceError(Exception):
    pass


@dataclass
class SyncDates:
    advertising_date: date
    stock_date: date


def resolve_sync_dates(reference_date: date | None = None) -> SyncDates:
    current = reference_date or timezone.localdate()
    return SyncDates(advertising_date=current - timedelta(days=1), stock_date=current)


def resolve_campaign_group(payload: dict[str, Any]) -> str:
    bid_type = (payload.get("bid_type") or "").strip().lower()
    settings = payload.get("settings") or {}
    placements = settings.get("placements") or {}
    if bid_type == "unified":
        return CampaignMonitoringGroup.UNIFIED
    if placements.get("search") and placements.get("recommendations"):
        return CampaignMonitoringGroup.UNIFIED
    if placements.get("search"):
        return CampaignMonitoringGroup.MANUAL_SEARCH
    if placements.get("recommendations"):
        return CampaignMonitoringGroup.MANUAL_SHELVES
    return CampaignMonitoringGroup.OTHER


def update_product_from_payload(product: Product, payload: dict[str, Any]) -> Product:
    product.title = payload.get("name") or payload.get("title") or product.title
    product.vendor_code = payload.get("vendorCode") or product.vendor_code
    product.brand_name = payload.get("brandName") or product.brand_name
    product.subject_name = payload.get("subjectName") or product.subject_name
    product.save()
    return product


def refresh_product_metadata(product: Product, *, analytics_client: AnalyticsWBClient | None = None) -> Product:
    analytics_client = analytics_client or AnalyticsWBClient()
    snapshot_date = timezone.localdate()
    response = analytics_client.get_product_stocks(nm_ids=[product.nm_id], snapshot_date=snapshot_date)
    items = response.get("data", {}).get("items", [])
    payload = next((item for item in items if item.get("nmID") == product.nm_id), None)
    if payload:
        update_product_from_payload(product, payload)
    return product


def refresh_campaign_metadata(campaign: Campaign, *, promotion_client: PromotionWBClient | None = None) -> Campaign:
    promotion_client = promotion_client or PromotionWBClient()
    response = promotion_client.get_campaigns(ids=[campaign.external_id])
    adverts = response.get("adverts", [])
    payload = next((item for item in adverts if item.get("id") == campaign.external_id), None)
    if not payload:
        return campaign

    settings = payload.get("settings") or {}
    placements = settings.get("placements") or {}
    campaign.name = settings.get("name") or campaign.name
    campaign.bid_type = payload.get("bid_type") or campaign.bid_type
    campaign.payment_type = settings.get("payment_type") or campaign.payment_type
    campaign.status = str(payload.get("status") or campaign.status)
    if campaign.monitoring_group == CampaignMonitoringGroup.OTHER:
        campaign.monitoring_group = resolve_campaign_group(payload)
    campaign.placements = placements
    campaign.raw_payload = payload
    campaign.save()

    nm_settings = payload.get("nm_settings") or []
    for nm_setting in nm_settings:
        nm_id = nm_setting.get("nm_id")
        if not nm_id:
            continue
        product, _ = Product.objects.get_or_create(nm_id=nm_id)
        update_product_from_payload(product, nm_setting)
        ProductCampaign.objects.get_or_create(product=product, campaign=campaign)

    return campaign


def upsert_product_metrics(*, product: Product, stats_date: date, history_entry: dict[str, Any], currency: str, overwrite: bool) -> None:
    defaults = {
        "open_count": history_entry.get("openCount", 0),
        "add_to_cart_count": history_entry.get("cartCount", 0),
        "order_count": history_entry.get("orderCount", 0),
        "order_sum": quantize_money(decimalize(history_entry.get("orderSum"))),
        "buyout_count": history_entry.get("buyoutCount", 0),
        "buyout_sum": quantize_money(decimalize(history_entry.get("buyoutSum"))),
        "add_to_wishlist_count": history_entry.get("addToWishlistCount", 0),
        "currency": currency,
        "raw_payload": history_entry,
    }
    if overwrite:
        DailyProductMetrics.objects.update_or_create(product=product, stats_date=stats_date, defaults=defaults)
    else:
        DailyProductMetrics.objects.get_or_create(product=product, stats_date=stats_date, defaults=defaults)


def upsert_product_stock(*, product: Product, stats_date: date, item_payload: dict[str, Any], overwrite: bool) -> None:
    metrics = item_payload.get("metrics") or {}
    defaults = {
        "total_stock": metrics.get("stockCount", 0),
        "in_way_to_client": metrics.get("toClientCount", 0),
        "in_way_from_client": metrics.get("fromClientCount", 0),
        "currency": item_payload.get("currency", "RUB"),
        "raw_payload": item_payload,
    }
    if overwrite:
        DailyProductStock.objects.update_or_create(product=product, stats_date=stats_date, defaults=defaults)
    else:
        DailyProductStock.objects.get_or_create(product=product, stats_date=stats_date, defaults=defaults)


def iter_size_payloads(sizes_payload: dict[str, Any]) -> list[dict[str, Any]]:
    return list(((sizes_payload.get("data") or {}).get("sizes")) or [])


def build_product_stock_payload_from_sizes(sizes_payload: dict[str, Any]) -> dict[str, Any]:
    data = sizes_payload.get("data") or {}
    total_stock = 0
    in_way_to_client = 0
    in_way_from_client = 0
    for size_payload in iter_size_payloads(sizes_payload):
        metrics = size_payload.get("metrics") or {}
        total_stock += int(metrics.get("stockCount") or 0)
        in_way_to_client += int(metrics.get("toClientCount") or 0)
        in_way_from_client += int(metrics.get("fromClientCount") or 0)
    return {
        "currency": data.get("currency", "RUB"),
        "metrics": {
            "stockCount": total_stock,
            "toClientCount": in_way_to_client,
            "fromClientCount": in_way_from_client,
        },
        "derivedFrom": "sizes",
        "raw_payload": sizes_payload,
    }


def resolve_office_id(office_payload: dict[str, Any]) -> int | None:
    office_id = office_payload.get("officeID")
    if office_id not in (None, ""):
        try:
            normalized = int(office_id)
        except (TypeError, ValueError):
            normalized = 0
        if normalized > 0:
            return normalized
    office_name = (office_payload.get("officeName") or "").strip()
    region_name = (office_payload.get("regionName") or "").strip()
    synthetic_key = f"{office_name}|{region_name}"
    if not synthetic_key.strip("|"):
        return None
    return 9_000_000_000 + zlib.crc32(synthetic_key.encode("utf-8"))


def aggregate_offices_from_sizes(sizes_payload: dict[str, Any]) -> list[dict[str, Any]]:
    aggregated: dict[int, dict[str, Any]] = {}
    for size_payload in iter_size_payloads(sizes_payload):
        size_name = size_payload.get("name") or ""
        for office in size_payload.get("offices") or []:
            office_id = resolve_office_id(office)
            if office_id is None:
                continue
            entry = aggregated.setdefault(
                office_id,
                {
                    "officeID": office_id,
                    "officeName": office.get("officeName") or "Маркетплейс",
                    "regionName": office.get("regionName") or "",
                    "metrics": {
                        "stockCount": 0,
                        "toClientCount": 0,
                        "fromClientCount": 0,
                        "avgOrders": Decimal("0"),
                    },
                    "sizeNames": [],
                },
            )
            metrics = office.get("metrics") or {}
            entry["metrics"]["stockCount"] += int(metrics.get("stockCount") or 0)
            entry["metrics"]["toClientCount"] += int(metrics.get("toClientCount") or 0)
            entry["metrics"]["fromClientCount"] += int(metrics.get("fromClientCount") or 0)
            entry["metrics"]["avgOrders"] += decimalize(metrics.get("avgOrders"))
            if size_name and size_name not in entry["sizeNames"]:
                entry["sizeNames"].append(size_name)
    for office in aggregated.values():
        office["metrics"]["avgOrders"] = str(office["metrics"]["avgOrders"])
    return list(aggregated.values())


def upsert_warehouse_stocks(*, product: Product, stats_date: date, sizes_payload: dict[str, Any], overwrite: bool) -> None:
    offices = aggregate_offices_from_sizes(sizes_payload)
    if overwrite:
        DailyWarehouseStock.objects.filter(product=product, stats_date=stats_date).delete()
    for office in offices:
        metrics = office.get("metrics") or {}
        warehouse, _ = Warehouse.objects.update_or_create(
            office_id=office.get("officeID") or 0,
            defaults={
                "name": office.get("officeName") or "Маркетплейс",
                "region_name": office.get("regionName") or "",
            },
        )
        defaults = {
            "stock_count": metrics.get("stockCount", 0),
            "in_way_to_client": metrics.get("toClientCount", 0),
            "in_way_from_client": metrics.get("fromClientCount", 0),
            "avg_orders": quantize_money(decimalize(metrics.get("avgOrders"))),
            "raw_payload": office,
        }
        if overwrite:
            DailyWarehouseStock.objects.update_or_create(
                product=product,
                warehouse=warehouse,
                stats_date=stats_date,
                defaults=defaults,
            )
        else:
            DailyWarehouseStock.objects.get_or_create(
                product=product,
                warehouse=warehouse,
                stats_date=stats_date,
                defaults=defaults,
            )


def upsert_campaign_stats(*, product_map: dict[int, Product], campaign_map: dict[int, Campaign], stat_payload: dict[str, Any], overwrite: bool) -> None:
    campaign = campaign_map.get(stat_payload.get("advertId"))
    if not campaign:
        return
    for day_payload in stat_payload.get("days", []):
        stats_date = datetime.fromisoformat(day_payload.get("date", "").replace("Z", "+00:00")).date()
        for app_payload in day_payload.get("apps", []):
            zone = map_app_type_to_zone(app_payload.get("appType"))
            for item in app_payload.get("nms", []):
                product = product_map.get(item.get("nmId"))
                if not product:
                    continue
                defaults = {
                    "impressions": item.get("views", 0),
                    "clicks": item.get("clicks", 0),
                    "spend": quantize_money(decimalize(item.get("sum"))),
                    "add_to_cart_count": item.get("atbs", 0),
                    "order_count": item.get("orders", 0),
                    "units_ordered": item.get("shks", 0),
                    "order_sum": quantize_money(decimalize(item.get("sum_price"))),
                    "raw_payload": item,
                }
                if overwrite:
                    DailyCampaignProductStat.objects.update_or_create(
                        campaign=campaign,
                        product=product,
                        stats_date=stats_date,
                        zone=zone,
                        defaults=defaults,
                    )
                else:
                    DailyCampaignProductStat.objects.get_or_create(
                        campaign=campaign,
                        product=product,
                        stats_date=stats_date,
                        zone=zone,
                        defaults=defaults,
                    )


def run_sync(*, product_ids: list[int] | None = None, reference_date: date | None = None, overwrite: bool = True, kind: str = SyncKind.FULL) -> SyncLog:
    dates = resolve_sync_dates(reference_date)
    runtime_settings = get_monitoring_settings()
    log = SyncLog.objects.create(
        kind=kind,
        status=SyncStatus.RUNNING,
        target_date=dates.advertising_date,
        payload={
            "advertising_date": dates.advertising_date.isoformat(),
            "stock_date": dates.stock_date.isoformat(),
            "product_ids": product_ids or [],
        },
    )
    try:
        analytics_client = AnalyticsWBClient()
        promotion_client = PromotionWBClient()
        queryset = Product.objects.filter(is_active=True)
        if product_ids:
            queryset = queryset.filter(id__in=product_ids)
        products = list(queryset.order_by("id"))
        if not products:
            raise SyncServiceError("Нет активных товаров для синхронизации.")

        product_map = {product.nm_id: product for product in products}
        campaigns = list(Campaign.objects.filter(is_active=True, products__in=products).distinct())
        campaign_map = {campaign.external_id: campaign for campaign in campaigns}

        with transaction.atomic():
            funnel_rows = analytics_client.get_sales_funnel_history(
                nm_ids=list(product_map.keys()),
                start_date=dates.advertising_date,
                end_date=dates.advertising_date,
            )
            for row in funnel_rows:
                product_data = row.get("product") or {}
                product = product_map.get(product_data.get("nmId"))
                if not product:
                    continue
                update_product_from_payload(product, product_data)
                for history_entry in row.get("history", []):
                    history_date = date.fromisoformat(history_entry["date"])
                    upsert_product_metrics(
                        product=product,
                        stats_date=history_date,
                        history_entry=history_entry,
                        currency=row.get("currency", "RUB"),
                        overwrite=overwrite,
                    )

            stock_items_by_nm_id: dict[int, dict[str, Any]] = {}
            try:
                stock_response = analytics_client.get_product_stocks(
                    nm_ids=list(product_map.keys()),
                    snapshot_date=dates.stock_date,
                )
                stock_items_by_nm_id = {
                    item.get("nmID"): item
                    for item in stock_response.get("data", {}).get("items", [])
                    if item.get("nmID")
                }
            except WBApiError as exc:
                if "WB API 500" not in str(exc):
                    raise

            for product in products:
                sizes_payload = analytics_client.get_product_sizes(nm_id=product.nm_id, snapshot_date=dates.stock_date)
                stock_item = stock_items_by_nm_id.get(product.nm_id)
                if stock_item:
                    update_product_from_payload(product, stock_item)
                    upsert_product_stock(
                        product=product,
                        stats_date=dates.stock_date,
                        item_payload=stock_item,
                        overwrite=overwrite,
                    )
                else:
                    upsert_product_stock(
                        product=product,
                        stats_date=dates.stock_date,
                        item_payload=build_product_stock_payload_from_sizes(sizes_payload),
                        overwrite=overwrite,
                    )
                upsert_warehouse_stocks(
                    product=product,
                    stats_date=dates.stock_date,
                    sizes_payload=sizes_payload,
                    overwrite=overwrite,
                )

            if campaigns:
                for campaign in campaigns:
                    refresh_campaign_metadata(campaign, promotion_client=promotion_client)
                stats_payload = promotion_client.get_campaign_stats(
                    ids=[campaign.external_id for campaign in campaigns],
                    start_date=dates.advertising_date,
                    end_date=dates.advertising_date,
                )
                for item in stats_payload:
                    upsert_campaign_stats(
                        product_map=product_map,
                        campaign_map=campaign_map,
                        stat_payload=item,
                        overwrite=overwrite,
                    )

        google_sync_note = ""
        if (
            hasattr(runtime_settings, "google_sheets_enabled")
            and runtime_settings.google_sheets_enabled
            and runtime_settings.google_sheets_auto_sync
        ):
            from monitoring.services.google_sheets import GoogleSheetsSyncError, sync_reports_to_google_sheets

            try:
                sheets_count = sync_reports_to_google_sheets(
                    reference_date=dates.stock_date,
                    product_ids=product_ids,
                )
                google_sync_note = f" Google Sheets обновлён: {sheets_count} листов."
            except GoogleSheetsSyncError as exc:
                google_sync_note = f" Google Sheets не обновлён: {exc}"

        log.status = SyncStatus.SUCCESS
        log.finished_at = timezone.now()
        log.message = f"Синхронизация завершена.{google_sync_note}".strip()
        log.save(update_fields=["status", "finished_at", "message", "updated_at"])
        return log
    except (WBApiError, SyncServiceError, ValueError) as exc:
        log.status = SyncStatus.ERROR
        log.finished_at = timezone.now()
        log.message = str(exc)
        log.save(update_fields=["status", "finished_at", "message", "updated_at"])
        raise SyncServiceError(str(exc)) from exc


def next_run_at(now: datetime, hour: int, minute: int) -> datetime:
    candidate = datetime.combine(now.date(), time(hour=hour, minute=minute), tzinfo=now.tzinfo)
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate
