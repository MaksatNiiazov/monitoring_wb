from __future__ import annotations

from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Callable
import logging
import threading
import zlib

from django.db import close_old_connections
from django.utils import timezone

from monitoring.models import (
    Campaign,
    CampaignMonitoringGroup,
    DailyCampaignProductStat,
    DailyCampaignSearchClusterStat,
    DailyProductKeywordStat,
    DailyProductMetrics,
    DailyProductNote,
    DailyProductStock,
    DailyWarehouseStock,
    Product,
    ProductKeyword,
    ProductCampaign,
    SyncKind,
    SyncLog,
    SyncStatus,
    Warehouse,
)
from monitoring.services.config import get_monitoring_settings
from monitoring.services.reports import decimalize, map_app_type_to_zone, normalize_search_text, quantize_money
from monitoring.services.wb_client import (
    AnalyticsWBClient,
    FeedbacksWBClient,
    PricesWBClient,
    PromotionWBClient,
    StatisticsWBClient,
    WBApiError,
)


class SyncServiceError(Exception):
    pass


class SyncCancelledError(SyncServiceError):
    pass


logger = logging.getLogger(__name__)


def _sync_console(message: str) -> None:
    timestamp = timezone.localtime().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[SYNC {timestamp}] {message}", flush=True)


@dataclass
class SyncDates:
    stats_date: date
    stock_date: date


def resolve_sync_dates(reference_date: date | None = None) -> SyncDates:
    current = reference_date or timezone.localdate()
    return SyncDates(stats_date=current, stock_date=current)


def resolve_sync_range(
    *,
    date_from: date | None = None,
    date_to: date | None = None,
    reference_date: date | None = None,
) -> tuple[date, date]:
    fallback_date = reference_date or timezone.localdate()
    range_start = date_from or date_to or fallback_date
    range_end = date_to or date_from or fallback_date
    if range_start > range_end:
        raise SyncServiceError("Дата начала периода не может быть позже даты окончания.")
    return range_start, range_end


def iter_sync_dates(*, date_from: date, date_to: date) -> list[SyncDates]:
    sync_dates: list[SyncDates] = []
    current = date_from
    while current <= date_to:
        sync_dates.append(resolve_sync_dates(current))
        current += timedelta(days=1)
    return sync_dates


def stage_progress_percent(*, step_index: int, total_steps: int, start_percent: int = 3, end_percent: int = 96) -> int:
    if total_steps <= 0:
        return end_percent
    normalized_step = max(0, min(step_index, total_steps))
    ratio = normalized_step / total_steps
    return int(round(start_percent + (end_percent - start_percent) * ratio))


def day_stage_detail(*, detail: str, sync_date: date, day_index: int, total_days: int) -> str:
    if total_days <= 1:
        return detail
    return f"День {day_index}/{total_days} ({sync_date:%d.%m.%Y}). {detail}"


def is_wb_start_day_limit_error(message: str) -> bool:
    normalized = (message or "").lower()
    return (
        ("invalid start day" in normalized and "excess limit on days" in normalized)
        or "не позволяет запрашивать эту дату" in normalized
    )


def humanize_sync_error_message(message: str) -> str:
    if is_wb_start_day_limit_error(message):
        return (
            "WB Analytics ограничивает глубину ретроспективы для этого отчёта. "
            "Сократите диапазон на более свежие даты и запустите синхронизацию повторно."
        )
    return message


def batched(items: list[Any], size: int) -> list[list[Any]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def fetch_product_sizes_payloads(
    *,
    nm_ids: list[int],
    snapshot_date: date,
    max_workers: int = 4,
    on_item_done: Callable[[int, int], None] | None = None,
) -> dict[int, dict[str, Any]]:
    if not nm_ids:
        return {}

    total = len(nm_ids)
    worker_count = max(1, min(max_workers, len(nm_ids)))
    if worker_count == 1:
        client = AnalyticsWBClient()
        payloads: dict[int, dict[str, Any]] = {}
        for index, nm_id in enumerate(nm_ids, start=1):
            payloads[nm_id] = client.get_product_sizes(nm_id=nm_id, snapshot_date=snapshot_date)
            if on_item_done:
                on_item_done(index, total)
        return payloads

    def _fetch_one(target_nm_id: int) -> tuple[int, dict[str, Any]]:
        thread_client = AnalyticsWBClient()
        return target_nm_id, thread_client.get_product_sizes(nm_id=target_nm_id, snapshot_date=snapshot_date)

    payloads: dict[int, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="wb-sizes") as executor:
        future_map = {executor.submit(_fetch_one, nm_id): nm_id for nm_id in nm_ids}
        done = 0
        for future in as_completed(future_map):
            nm_id, payload = future.result()
            payloads[nm_id] = payload
            done += 1
            if on_item_done:
                on_item_done(done, total)
    return payloads


def collect_product_keywords(product: Product) -> list[str]:
    saved_keywords = list(
        ProductKeyword.objects.filter(product=product)
        .order_by("position", "query_text", "id")
        .values_list("query_text", flat=True)
    )
    keyword_texts = [item.strip() for item in saved_keywords if item and item.strip()]
    if not keyword_texts:
        keyword_texts = [
            item.strip()
            for item in [product.primary_keyword, product.secondary_keyword]
            if item and item.strip()
        ]
    return list(dict.fromkeys(keyword_texts))


def parse_wb_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def mode_decimal(values: list[Any]) -> Decimal:
    normalized = [decimalize(value) for value in values if value not in (None, "")]
    if not normalized:
        return Decimal("0")
    counts = Counter(normalized)
    best_value, _ = max(counts.items(), key=lambda item: (item[1], item[0]))
    return best_value


def average_decimal(values: list[Any]) -> Decimal:
    normalized = [decimalize(value) for value in values if value not in (None, "")]
    if not normalized:
        return Decimal("0")
    return sum(normalized, Decimal("0")) / Decimal(len(normalized))


def compute_days_until_zero(*, total_stock: int, avg_orders_per_day: Decimal) -> Decimal:
    if avg_orders_per_day <= 0:
        return Decimal("0")
    return decimalize(total_stock) / avg_orders_per_day


def extract_avg_orders(item_payload: dict[str, Any]) -> Decimal:
    metrics = item_payload.get("metrics") or {}
    return decimalize(metrics.get("avgOrders"))


def build_price_lookup(price_payload: dict[str, Any]) -> dict[int, dict[str, Decimal]]:
    result: dict[int, dict[str, Decimal]] = {}
    for item in ((price_payload.get("data") or {}).get("listGoods") or []):
        nm_id = item.get("nmID")
        if not nm_id:
            continue
        sizes = item.get("sizes") or []
        seller_price = mode_decimal([size.get("discountedPrice") for size in sizes])
        wb_price = mode_decimal([size.get("clubDiscountedPrice") for size in sizes]) or seller_price
        result[nm_id] = {
            "seller_price": quantize_money(seller_price),
            "wb_price": quantize_money(wb_price),
            "raw_payload": item,
        }
    return result


def build_supplier_orders_lookup(order_rows: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in order_rows:
        nm_id = row.get("nmId")
        if nm_id:
            grouped[nm_id].append(row)
    return dict(grouped)


def extract_supplier_order_date(order_row: dict[str, Any]) -> date | None:
    for field_name in (
        "date",
        "lastChangeDate",
        "lastChangeDateTime",
        "dateCreated",
        "createdAt",
        "createdDate",
    ):
        raw_value = order_row.get(field_name)
        if not raw_value:
            continue
        if isinstance(raw_value, datetime):
            return raw_value.date()
        if isinstance(raw_value, date):
            return raw_value
        parsed = parse_wb_datetime(str(raw_value))
        if parsed:
            return parsed.date()
        try:
            return date.fromisoformat(str(raw_value)[:10])
        except ValueError:
            continue
    return None


def split_supplier_orders_lookup_by_date(
    order_rows: list[dict[str, Any]],
) -> tuple[dict[date, dict[int, list[dict[str, Any]]]], dict[int, list[dict[str, Any]]]]:
    dated_lookup: dict[date, dict[int, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    undated_lookup: dict[int, list[dict[str, Any]]] = defaultdict(list)

    for row in order_rows:
        nm_id = row.get("nmId")
        if not nm_id:
            continue
        order_date = extract_supplier_order_date(row)
        if order_date:
            dated_lookup[order_date][nm_id].append(row)
        else:
            undated_lookup[nm_id].append(row)

    dated_result = {stats_date: dict(product_rows) for stats_date, product_rows in dated_lookup.items()}
    return dated_result, dict(undated_lookup)


def summarize_supplier_orders(order_rows: list[dict[str, Any]]) -> dict[str, Decimal]:
    spp_percent = quantize_money(average_decimal([row.get("spp") for row in order_rows]))
    seller_price = quantize_money(mode_decimal([row.get("priceWithDisc") for row in order_rows]))
    wb_price = quantize_money(mode_decimal([row.get("finishedPrice") for row in order_rows]))
    return {
        "spp_percent": spp_percent,
        "seller_price": seller_price,
        "wb_price": wb_price,
    }


def resolve_campaign_group(payload: dict[str, Any]) -> str:
    bid_type = (payload.get("bid_type") or "").strip().lower()
    campaign_type = int(payload.get("type") or 0)
    settings = payload.get("settings") or {}
    placements = settings.get("placements") or {}
    placement_types = {str(value).strip().lower() for value in (payload.get("placement_types") or []) if str(value).strip()}
    if "search" in placement_types:
        placements["search"] = True
    if "recommendations" in placement_types:
        placements["recommendations"] = True
    if bid_type == "unified":
        return CampaignMonitoringGroup.UNIFIED
    if campaign_type == 4:
        return CampaignMonitoringGroup.MANUAL_CATALOG
    if campaign_type == 6:
        return CampaignMonitoringGroup.MANUAL_SEARCH
    if campaign_type in {5, 7}:
        return CampaignMonitoringGroup.MANUAL_SHELVES
    if placements.get("search") and placements.get("recommendations"):
        return CampaignMonitoringGroup.UNIFIED
    if placements.get("search"):
        return CampaignMonitoringGroup.MANUAL_SEARCH
    if placements.get("recommendations"):
        return CampaignMonitoringGroup.MANUAL_SHELVES
    return CampaignMonitoringGroup.OTHER


def update_product_from_payload(product: Product, payload: dict[str, Any]) -> Product:
    updated_fields: list[str] = []
    payload_map = {
        "title": payload.get("name") or payload.get("title"),
        "vendor_code": payload.get("vendorCode"),
        "brand_name": payload.get("brandName"),
        "subject_name": payload.get("subjectName"),
    }
    for field_name, next_value in payload_map.items():
        if next_value in (None, ""):
            continue
        if getattr(product, field_name) != next_value:
            setattr(product, field_name, next_value)
            updated_fields.append(field_name)
    if updated_fields:
        product.save(update_fields=[*updated_fields, "updated_at"])
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


def _apply_campaign_metadata_payload(campaign: Campaign, payload: dict[str, Any]) -> Campaign:
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


def refresh_campaign_metadata(campaign: Campaign, *, promotion_client: PromotionWBClient | None = None) -> Campaign:
    promotion_client = promotion_client or PromotionWBClient()
    response = promotion_client.get_campaigns(ids=[campaign.external_id])
    adverts = response.get("adverts", [])
    payload = next((item for item in adverts if item.get("id") == campaign.external_id), None)
    if not payload:
        return campaign
    return _apply_campaign_metadata_payload(campaign, payload)


def refresh_campaigns_metadata(
    campaigns: list[Campaign],
    *,
    promotion_client: PromotionWBClient | None = None,
) -> None:
    if not campaigns:
        return
    promotion_client = promotion_client or PromotionWBClient()
    campaigns_by_external_id = {campaign.external_id: campaign for campaign in campaigns}
    for id_chunk in batched(list(campaigns_by_external_id.keys()), 100):
        response = promotion_client.get_campaigns(ids=id_chunk)
        for payload in response.get("adverts", []) or []:
            campaign = campaigns_by_external_id.get(payload.get("id"))
            if not campaign:
                continue
            _apply_campaign_metadata_payload(campaign, payload)


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
    avg_orders_per_day = extract_avg_orders(item_payload)
    defaults = {
        "total_stock": metrics.get("stockCount", 0),
        "in_way_to_client": metrics.get("toClientCount", 0),
        "in_way_from_client": metrics.get("fromClientCount", 0),
        "avg_orders_per_day": quantize_money(avg_orders_per_day),
        "days_until_zero": quantize_money(
            compute_days_until_zero(
                total_stock=int(metrics.get("stockCount") or 0),
                avg_orders_per_day=avg_orders_per_day,
            )
        ),
        "currency": item_payload.get("currency", "RUB"),
        "raw_payload": item_payload,
    }
    if overwrite:
        DailyProductStock.objects.update_or_create(product=product, stats_date=stats_date, defaults=defaults)
    else:
        DailyProductStock.objects.get_or_create(product=product, stats_date=stats_date, defaults=defaults)


def iter_size_payloads(sizes_payload: dict[str, Any]) -> list[dict[str, Any]]:
    return list(((sizes_payload.get("data") or {}).get("sizes")) or [])


def aggregate_avg_orders_from_sizes(sizes_payload: dict[str, Any]) -> Decimal:
    total = Decimal("0")
    for size_payload in iter_size_payloads(sizes_payload):
        metrics = size_payload.get("metrics") or {}
        size_avg_orders = decimalize(metrics.get("avgOrders"))
        if size_avg_orders:
            total += size_avg_orders
            continue
        for office in size_payload.get("offices") or []:
            total += decimalize((office.get("metrics") or {}).get("avgOrders"))
    return total


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
            "avgOrders": str(aggregate_avg_orders_from_sizes(sizes_payload)),
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


def upsert_warehouse_stocks(
    *,
    product: Product,
    stats_date: date,
    sizes_payload: dict[str, Any],
    overwrite: bool,
    warehouse_cache: dict[int, Warehouse] | None = None,
) -> None:
    offices = aggregate_offices_from_sizes(sizes_payload)
    if overwrite:
        DailyWarehouseStock.objects.filter(product=product, stats_date=stats_date).delete()
    rows_to_create: list[DailyWarehouseStock] = []
    for office in offices:
        metrics = office.get("metrics") or {}
        office_id = int(office.get("officeID") or 0)
        if office_id <= 0:
            continue
        office_name = office.get("officeName") or "Маркетплейс"
        region_name = office.get("regionName") or ""
        warehouse = warehouse_cache.get(office_id) if warehouse_cache is not None else None
        if warehouse is None:
            warehouse, _ = Warehouse.objects.update_or_create(
                office_id=office_id,
                defaults={
                    "name": office_name,
                    "region_name": region_name,
                },
            )
            if warehouse_cache is not None:
                warehouse_cache[office_id] = warehouse
        elif warehouse.name != office_name or warehouse.region_name != region_name:
            warehouse.name = office_name
            warehouse.region_name = region_name
            warehouse.save(update_fields=["name", "region_name", "updated_at"])
        defaults = {
            "stock_count": metrics.get("stockCount", 0),
            "in_way_to_client": metrics.get("toClientCount", 0),
            "in_way_from_client": metrics.get("fromClientCount", 0),
            "avg_orders": quantize_money(decimalize(metrics.get("avgOrders"))),
            "raw_payload": office,
        }
        if overwrite:
            rows_to_create.append(
                DailyWarehouseStock(
                    product=product,
                    warehouse=warehouse,
                    stats_date=stats_date,
                    **defaults,
                )
            )
        else:
            DailyWarehouseStock.objects.get_or_create(
                product=product,
                warehouse=warehouse,
                stats_date=stats_date,
                defaults=defaults,
            )
    if overwrite and rows_to_create:
        DailyWarehouseStock.objects.bulk_create(rows_to_create, batch_size=500)


def upsert_campaign_stats(
    *,
    product_map: dict[int, Product],
    campaign_map: dict[int, Campaign],
    stat_payload: dict[str, Any],
    overwrite: bool,
    allowed_dates: set[date] | None = None,
    linked_products_by_campaign_id: dict[int, set[int]] | None = None,
    tracked_product_ids: list[int] | None = None,
) -> None:
    campaign = campaign_map.get(stat_payload.get("advertId"))
    if not campaign:
        return
    linked_product_ids = (
        linked_products_by_campaign_id.get(campaign.id, set()) if linked_products_by_campaign_id is not None else set()
    )
    if linked_products_by_campaign_id is None:
        linked_product_ids = set(campaign.products.values_list("id", flat=True))
    tracked_ids = tracked_product_ids if tracked_product_ids is not None else [product.id for product in product_map.values()]
    for day_payload in stat_payload.get("days", []):
        stats_date = datetime.fromisoformat(day_payload.get("date", "").replace("Z", "+00:00")).date()
        if allowed_dates is not None and stats_date not in allowed_dates:
            continue
        if overwrite and tracked_ids:
            DailyCampaignProductStat.objects.filter(
                campaign=campaign,
                stats_date=stats_date,
                product_id__in=tracked_ids,
            ).delete()
        aggregated_rows: dict[tuple[int, str], dict[str, Any]] = {}
        for app_payload in day_payload.get("apps", []):
            app_type = app_payload.get("appType")
            zone = map_app_type_to_zone(app_type)
            for item in app_payload.get("nms", []):
                product = product_map.get(item.get("nmId"))
                if not product:
                    continue
                if linked_product_ids and product.id not in linked_product_ids:
                    continue
                key = (product.id, zone)
                row = aggregated_rows.setdefault(
                    key,
                    {
                        "impressions": 0,
                        "clicks": 0,
                        "spend": Decimal("0"),
                        "add_to_cart_count": 0,
                        "order_count": 0,
                        "units_ordered": 0,
                        "order_sum": Decimal("0"),
                        "raw_payload": [],
                    },
                )
                row["impressions"] += int(item.get("views") or 0)
                row["clicks"] += int(item.get("clicks") or 0)
                row["spend"] += decimalize(item.get("sum"))
                row["add_to_cart_count"] += int(item.get("atbs") or 0)
                row["order_count"] += int(item.get("orders") or 0)
                row["units_ordered"] += int(item.get("shks") or 0)
                row["order_sum"] += decimalize(item.get("sum_price"))
                row["raw_payload"].append(
                    {
                        "appType": app_type,
                        "item": item,
                    }
                )

        if overwrite:
            rows_to_create: list[DailyCampaignProductStat] = []
            for (product_id, zone), row in aggregated_rows.items():
                rows_to_create.append(
                    DailyCampaignProductStat(
                        campaign=campaign,
                        product_id=product_id,
                        stats_date=stats_date,
                        zone=zone,
                        impressions=row["impressions"],
                        clicks=row["clicks"],
                        spend=quantize_money(row["spend"]),
                        add_to_cart_count=row["add_to_cart_count"],
                        order_count=row["order_count"],
                        units_ordered=row["units_ordered"],
                        order_sum=quantize_money(row["order_sum"]),
                        raw_payload={"items": row["raw_payload"]},
                    )
                )
            if rows_to_create:
                DailyCampaignProductStat.objects.bulk_create(rows_to_create, batch_size=500)
        else:
            for (product_id, zone), row in aggregated_rows.items():
                DailyCampaignProductStat.objects.get_or_create(
                    campaign=campaign,
                    product_id=product_id,
                    stats_date=stats_date,
                    zone=zone,
                    defaults={
                        "impressions": row["impressions"],
                        "clicks": row["clicks"],
                        "spend": quantize_money(row["spend"]),
                        "add_to_cart_count": row["add_to_cart_count"],
                        "order_count": row["order_count"],
                        "units_ordered": row["units_ordered"],
                        "order_sum": quantize_money(row["order_sum"]),
                        "raw_payload": {"items": row["raw_payload"]},
                    },
                )


def fetch_negative_feedback_count(*, feedback_client: FeedbacksWBClient, product: Product, stats_date: date) -> int:
    feedbacks_by_id: dict[str, dict[str, Any]] = {}
    for is_answered in (False, True):
        skip = 0
        while True:
            payload = feedback_client.get_feedbacks(
                nm_id=product.nm_id,
                is_answered=is_answered,
                take=100,
                skip=skip,
            )
            feedbacks = ((payload.get("data") or {}).get("feedbacks")) or []
            if not feedbacks:
                break
            should_stop = False
            for item in feedbacks:
                created_at = parse_wb_datetime(item.get("createdDate"))
                if not created_at:
                    continue
                created_date = created_at.astimezone(timezone.get_current_timezone()).date()
                if created_date < stats_date:
                    should_stop = True
                    continue
                if created_date > stats_date:
                    continue
                feedback_id = item.get("id")
                if feedback_id:
                    feedbacks_by_id[feedback_id] = item
            if should_stop or len(feedbacks) < 100:
                break
            skip += len(feedbacks)
    return sum(1 for item in feedbacks_by_id.values() if int(item.get("productValuation") or 0) <= 3)


def fetch_product_enrichment_payloads(
    *,
    products: list[Product],
    stats_date: date,
    max_workers: int = 4,
    analytics_client: AnalyticsWBClient | None = None,
    feedbacks_client: FeedbacksWBClient | None = None,
) -> tuple[dict[int, dict[str, Any]], list[str]]:
    payloads_by_product_id: dict[int, dict[str, Any]] = {}
    warnings: list[str] = []

    if not products:
        return payloads_by_product_id, warnings

    def _fetch_one(
        *,
        product: Product,
        local_analytics_client: AnalyticsWBClient,
        local_feedbacks_client: FeedbacksWBClient,
    ) -> tuple[int, dict[str, Any], list[str]]:
        local_warnings: list[str] = []
        keywords = collect_product_keywords(product)
        organic_keyword_payload: dict[str, Any] | None = None
        negative_feedback_count: int | None = None

        if keywords:
            try:
                organic_keyword_payload = local_analytics_client.get_search_orders(
                    nm_id=product.nm_id,
                    start_date=stats_date,
                    end_date=stats_date,
                    search_texts=keywords,
                )
            except WBApiError as exc:
                local_warnings.append(f"organic_keywords:{product.nm_id}: {exc}")

        try:
            negative_feedback_count = fetch_negative_feedback_count(
                feedback_client=local_feedbacks_client,
                product=product,
                stats_date=stats_date,
            )
        except WBApiError as exc:
            local_warnings.append(f"feedbacks:{product.nm_id}: {exc}")

        return (
            product.id,
            {
                "keywords": keywords,
                "organic_keyword_payload": organic_keyword_payload,
                "negative_feedback_count": negative_feedback_count,
            },
            local_warnings,
        )

    worker_count = max(1, min(max_workers, len(products)))
    if worker_count == 1:
        local_analytics_client = analytics_client or AnalyticsWBClient()
        local_feedbacks_client = feedbacks_client or FeedbacksWBClient()
        for product in products:
            product_id, payload, local_warnings = _fetch_one(
                product=product,
                local_analytics_client=local_analytics_client,
                local_feedbacks_client=local_feedbacks_client,
            )
            payloads_by_product_id[product_id] = payload
            warnings.extend(local_warnings)
        return payloads_by_product_id, warnings

    def _worker(product: Product) -> tuple[int, dict[str, Any], list[str]]:
        return _fetch_one(
            product=product,
            local_analytics_client=AnalyticsWBClient(),
            local_feedbacks_client=FeedbacksWBClient(),
        )

    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="wb-product-enrichment") as executor:
        future_map = {executor.submit(_worker, product): product.id for product in products}
        for future in as_completed(future_map):
            product_id, payload, local_warnings = future.result()
            payloads_by_product_id[product_id] = payload
            warnings.extend(local_warnings)

    return payloads_by_product_id, warnings


def upsert_keyword_stats(
    *,
    product: Product,
    stats_date: date,
    keywords: list[str],
    organic_payload: dict[str, Any] | None,
    boosted_stats: dict[str, dict[str, Any]],
    overwrite: bool,
) -> None:
    cleaned_keywords = [keyword.strip() for keyword in keywords if keyword.strip()]
    if not cleaned_keywords:
        return

    organic_map: dict[str, dict[str, Any]] = {}
    if organic_payload:
        for item in ((organic_payload.get("data") or {}).get("items")) or []:
            organic_map[normalize_search_text(item.get("text") or "")] = item

    if overwrite:
        DailyProductKeywordStat.objects.filter(
            product=product,
            stats_date=stats_date,
            query_text__in=cleaned_keywords,
        ).delete()

    for keyword in cleaned_keywords:
        normalized_keyword = normalize_search_text(keyword)
        organic_item = organic_map.get(normalized_keyword) or {}
        boosted_item = boosted_stats.get(normalized_keyword) or {}
        organic_date_item = {}
        for date_item in organic_item.get("dateItems") or []:
            raw_date = date_item.get("dt") or date_item.get("date")
            if not raw_date:
                continue
            if str(raw_date)[:10] == stats_date.isoformat():
                organic_date_item = date_item
                break
        defaults = {
            "frequency": int(organic_item.get("frequency") or 0),
            "organic_position": quantize_money(decimalize(organic_date_item.get("avgPosition"))),
            "organic_orders": int(organic_date_item.get("orders") or 0),
            "boosted_position": quantize_money(decimalize(boosted_item.get("avg_position"))),
            "boosted_ctr": quantize_money(decimalize(boosted_item.get("ctr"))),
            "boosted_views": int(boosted_item.get("views") or 0),
            "boosted_clicks": int(boosted_item.get("clicks") or 0),
            "raw_payload": {
                "organic": organic_item,
                "boosted": boosted_item.get("raw_payload") or [],
            },
        }
        if overwrite:
            DailyProductKeywordStat.objects.update_or_create(
                product=product,
                stats_date=stats_date,
                query_text=keyword,
                defaults=defaults,
            )
        else:
            DailyProductKeywordStat.objects.get_or_create(
                product=product,
                stats_date=stats_date,
                query_text=keyword,
                defaults=defaults,
            )


def upsert_product_note(
    *,
    product: Product,
    stats_date: date,
    overwrite: bool,
    supplier_order_summary: dict[str, Decimal] | None = None,
    price_summary: dict[str, Decimal] | None = None,
    negative_feedback_count: int | None = None,
    enabled_groups: set[str] | None = None,
) -> None:
    note, created = DailyProductNote.objects.get_or_create(
        product=product,
        note_date=stats_date,
    )
    if not overwrite and not created:
        return

    updated_fields: set[str] = set()

    # Действия и комментарии в блоке "Обзор" остаются ручными:
    # синхронизация заполняет только фактические числовые поля.
    if supplier_order_summary:
        spp_percent = supplier_order_summary.get("spp_percent", Decimal("0"))
        if note.spp_percent != spp_percent:
            note.spp_percent = spp_percent
            updated_fields.add("spp_percent")

        seller_price = supplier_order_summary.get("seller_price")
        if seller_price and note.seller_price != seller_price:
            note.seller_price = seller_price
            updated_fields.add("seller_price")

        wb_price = supplier_order_summary.get("wb_price")
        if wb_price and note.wb_price != wb_price:
            note.wb_price = wb_price
            updated_fields.add("wb_price")

    if price_summary:
        if not decimalize(note.seller_price):
            seller_price = price_summary.get("seller_price")
            if seller_price and note.seller_price != seller_price:
                note.seller_price = seller_price
                updated_fields.add("seller_price")
        if not decimalize(note.wb_price):
            wb_price = price_summary.get("wb_price")
            if wb_price and note.wb_price != wb_price:
                note.wb_price = wb_price
                updated_fields.add("wb_price")

    if negative_feedback_count is not None:
        current_feedback = (note.negative_feedback or "").strip()
        auto_feedback = "Без изменений" if negative_feedback_count == 0 else str(negative_feedback_count)
        if not current_feedback or current_feedback.isdigit() or current_feedback == "Без изменений":
            if note.negative_feedback != auto_feedback:
                note.negative_feedback = auto_feedback
                updated_fields.add("negative_feedback")

    if created:
        if not note.promo_status:
            note.promo_status = "Не участвуем"
            updated_fields.add("promo_status")
        if not note.negative_feedback:
            note.negative_feedback = "Без изменений"
            updated_fields.add("negative_feedback")

    if updated_fields:
        note.save(update_fields=[*sorted(updated_fields), "updated_at"])


def mark_stale_running_syncs(*, stale_after_hours: int = 6) -> int:
    cutoff = timezone.now() - timedelta(hours=stale_after_hours)
    stale_logs = SyncLog.objects.filter(
        status=SyncStatus.RUNNING,
        finished_at__isnull=True,
        created_at__lt=cutoff,
    )
    stale_count = stale_logs.count()
    if stale_count:
        stale_logs.update(
            status=SyncStatus.ERROR,
            finished_at=timezone.now(),
            message="Синхронизация была прервана и помечена как зависший запуск.",
        )
    return stale_count


def get_running_sync() -> SyncLog | None:
    return (
        SyncLog.objects.filter(status=SyncStatus.RUNNING, finished_at__isnull=True)
        .order_by("-created_at")
        .first()
    )


def request_cancel_running_sync() -> SyncLog | None:
    log = get_running_sync()
    if not log:
        return None

    payload = dict(log.payload or {})
    progress = dict(payload.get("progress") or {})
    now = timezone.now()
    now_iso = now.isoformat()
    progress.update(
        {
            "percent": 100,
            "stage": "Отменено",
            "detail": "Синхронизация остановлена пользователем. Можно запускать новый sync.",
            "updated_at": now_iso,
        }
    )
    payload.update(
        {
            "cancel_requested": True,
            "cancel_requested_at": now_iso,
            "progress": progress,
        }
    )
    log.status = SyncStatus.CANCELED
    log.finished_at = now
    log.payload = payload
    log.message = "Синхронизация отменена пользователем."
    log.save(update_fields=["status", "finished_at", "payload", "message", "updated_at"])
    return log


def _is_cancel_requested(log: SyncLog) -> bool:
    fresh = SyncLog.objects.filter(pk=log.pk).values("status", "payload").first()
    if not fresh:
        return True
    if fresh["status"] != SyncStatus.RUNNING:
        return True
    payload = fresh["payload"] if isinstance(fresh["payload"], dict) else {}
    return bool(payload.get("cancel_requested"))


def _assert_not_cancelled(log: SyncLog) -> None:
    if _is_cancel_requested(log):
        raise SyncCancelledError("Синхронизация отменена пользователем.")


def _update_sync_progress(log: SyncLog, *, percent: int, stage: str, detail: str | None = None) -> None:
    normalized_percent = max(0, min(int(percent), 100))
    progress_payload = {
        "percent": normalized_percent,
        "stage": stage,
        "detail": detail or "",
        "updated_at": timezone.now().isoformat(),
    }
    log.payload = {
        **(log.payload or {}),
        "progress": progress_payload,
    }
    log.message = detail or stage
    log.save(update_fields=["payload", "message", "updated_at"])
    summary = detail or stage
    _sync_console(f"log={log.id} progress={normalized_percent}% stage={stage} detail={summary}")


def run_sync_in_background(
    *,
    product_ids: list[int] | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    reference_date: date | None = None,
    overwrite: bool = True,
    kind: str = SyncKind.FULL,
) -> threading.Thread:
    def _worker() -> None:
        close_old_connections()
        _sync_console(
            "background worker started "
            f"kind={kind} date_from={date_from} date_to={date_to} reference_date={reference_date}"
        )
        try:
            run_sync(
                product_ids=product_ids,
                date_from=date_from,
                date_to=date_to,
                reference_date=reference_date,
                overwrite=overwrite,
                kind=kind,
            )
            _sync_console("background worker finished successfully")
        except SyncServiceError as exc:
            logger.warning("Фоновая синхронизация завершилась ошибкой сервиса: %s", exc)
        except Exception:
            logger.exception("Фоновая синхронизация завершилась непредвиденной ошибкой.")
        finally:
            close_old_connections()

    thread_name = f"wb-sync-{kind}-{timezone.now().strftime('%Y%m%d-%H%M%S')}"
    thread = threading.Thread(target=_worker, daemon=True, name=thread_name)
    thread.start()
    return thread


def run_sync(
    *,
    product_ids: list[int] | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    reference_date: date | None = None,
    overwrite: bool = True,
    kind: str = SyncKind.FULL,
) -> SyncLog:
    range_start, range_end = resolve_sync_range(date_from=date_from, date_to=date_to, reference_date=reference_date)
    sync_dates = iter_sync_dates(date_from=range_start, date_to=range_end)
    _sync_console(
        "sync requested "
        f"kind={kind} overwrite={overwrite} range={range_start.isoformat()}..{range_end.isoformat()} "
        f"days={len(sync_dates)} products={'all' if not product_ids else len(product_ids)}"
    )
    if not sync_dates:
        raise SyncServiceError("Не удалось определить даты синхронизации.")

    if len(sync_dates) == 1:
        return _run_sync_single_day(
            product_ids=product_ids,
            reference_date=sync_dates[0].stats_date,
            range_start=range_start,
            range_end=range_end,
            days_count=1,
            overwrite=overwrite,
            kind=kind,
        )

    last_log: SyncLog | None = None
    skipped_dates_due_api_limit: list[str] = []
    for sync_day in sync_dates:
        _sync_console(f"sync day started date={sync_day.stats_date.isoformat()}")
        try:
            last_log = _run_sync_single_day(
                product_ids=product_ids,
                reference_date=sync_day.stats_date,
                range_start=range_start,
                range_end=range_end,
                days_count=len(sync_dates),
                overwrite=overwrite,
                kind=kind,
            )
        except SyncServiceError as exc:
            if is_wb_start_day_limit_error(str(exc)):
                skipped_dates_due_api_limit.append(sync_day.stats_date.isoformat())
                _sync_console(
                    "sync day skipped by WB API limit "
                    f"date={sync_day.stats_date.isoformat()} reason={exc}"
                )
                continue
            raise

    if not last_log:
        if skipped_dates_due_api_limit:
            raise SyncServiceError(
                "WB Analytics ограничивает глубину исторических данных: выбранный диапазон целиком вне допустимого окна API."
            )
        raise SyncServiceError("Не удалось завершить синхронизацию диапазона.")

    payload = dict(last_log.payload or {})
    payload.update(
        {
            "stats_date_from": range_start.isoformat(),
            "stats_date_to": range_end.isoformat(),
            "days_count": len(sync_dates),
        }
    )
    if skipped_dates_due_api_limit:
        payload["skipped_dates_due_api_limit"] = skipped_dates_due_api_limit
        warnings_payload = payload.get("warnings")
        warnings: list[str] = list(warnings_payload) if isinstance(warnings_payload, list) else []
        warnings.append(
            f"Пропущены даты из-за ограничения WB Analytics: {', '.join(skipped_dates_due_api_limit)}."
        )
        payload["warnings"] = warnings
        last_log.message = (
            f"{(last_log.message or '').strip()} Пропущено дат из-за ограничения WB Analytics: {len(skipped_dates_due_api_limit)}."
        ).strip()
    last_log.payload = payload
    update_fields = ["payload", "updated_at"]
    if skipped_dates_due_api_limit:
        update_fields.append("message")
    last_log.save(update_fields=update_fields)
    _sync_console(
        "sync completed "
        f"log={last_log.id} status={last_log.status} skipped_days={len(skipped_dates_due_api_limit)}"
    )
    return last_log


def _run_sync_single_day(
    *,
    product_ids: list[int] | None = None,
    reference_date: date | None = None,
    range_start: date | None = None,
    range_end: date | None = None,
    days_count: int = 1,
    overwrite: bool = True,
    kind: str = SyncKind.FULL,
) -> SyncLog:
    dates = resolve_sync_dates(reference_date)
    runtime_settings = get_monitoring_settings()

    mark_stale_running_syncs()
    running_sync = get_running_sync()
    if running_sync:
        raise SyncServiceError("Синхронизация уже выполняется. Дождитесь завершения текущего запуска и повторите позже.")

    log = SyncLog.objects.create(
        kind=kind,
        status=SyncStatus.RUNNING,
        target_date=dates.stats_date,
        payload={
            "stats_date": dates.stats_date.isoformat(),
            "stock_date": dates.stock_date.isoformat(),
            "stats_date_from": (range_start or dates.stats_date).isoformat(),
            "stats_date_to": (range_end or dates.stats_date).isoformat(),
            "days_count": max(1, days_count),
            "product_ids": product_ids or [],
        },
    )
    _sync_console(
        f"log={log.id} started kind={kind} date={dates.stats_date.isoformat()} "
        f"days_count={max(1, days_count)} overwrite={overwrite}"
    )
    try:
        _update_sync_progress(
            log,
            percent=3,
            stage="Подготовка",
            detail="Инициализация клиентов WB и подготовка списка товаров.",
        )
        analytics_client = AnalyticsWBClient()
        promotion_client = PromotionWBClient()
        feedbacks_client = FeedbacksWBClient()
        queryset = Product.objects.filter(is_active=True)
        if product_ids:
            queryset = queryset.filter(id__in=product_ids)
        products = list(queryset.order_by("id"))
        if not products:
            raise SyncServiceError("Нет активных товаров для синхронизации.")
        _assert_not_cancelled(log)

        optional_errors: list[str] = []
        product_map = {product.nm_id: product for product in products}
        tracked_product_ids = [product.id for product in products]
        campaigns = list(Campaign.objects.filter(is_active=True, products__in=products).distinct())
        campaign_map = {campaign.external_id: campaign for campaign in campaigns}
        campaign_pairs = (
            list(ProductCampaign.objects.filter(campaign__in=campaigns, product__in=products).select_related("campaign", "product"))
            if campaigns
            else []
        )
        linked_products_by_campaign_id: dict[int, set[int]] = defaultdict(set)
        for pair in campaign_pairs:
            linked_products_by_campaign_id[pair.campaign_id].add(pair.product_id)
        warehouse_cache: dict[int, Warehouse] = {}
        _update_sync_progress(
            log,
            percent=12,
            stage="Воронка",
            detail=f"Сбор воронки продаж за {dates.stats_date:%d.%m.%Y}.",
        )

        # SQLite блокирует всю БД на запись во время длинной транзакции.
        # Здесь много сетевых вызовов, поэтому держать один giant-atomic нельзя:
        # это провоцирует "database is locked" в веб-запросах.
        with nullcontext():
            _assert_not_cancelled(log)
            funnel_rows = analytics_client.get_sales_funnel_history(
                nm_ids=list(product_map.keys()),
                start_date=dates.stats_date,
                end_date=dates.stats_date,
            )
            for row in funnel_rows:
                _assert_not_cancelled(log)
                product_data = row.get("product") or {}
                product = product_map.get(product_data.get("nmId"))
                if not product:
                    continue
                update_product_from_payload(product, product_data)
                for history_entry in row.get("history", []):
                    _assert_not_cancelled(log)
                    history_date = date.fromisoformat(history_entry["date"])
                    if history_date != dates.stats_date:
                        continue
                    upsert_product_metrics(
                        product=product,
                        stats_date=history_date,
                        history_entry=history_entry,
                        currency=row.get("currency", "RUB"),
                        overwrite=overwrite,
                    )

            _update_sync_progress(
                log,
                percent=28,
                stage="Остатки",
                detail=f"Сбор остатков и складов ({len(products)} товаров).",
            )
            _assert_not_cancelled(log)
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

            _assert_not_cancelled(log)
            sizes_total = max(1, len(products))
            sizes_progress_step = max(1, sizes_total // 5)

            def _on_sizes_item_done(done: int, total: int) -> None:
                if done == 1 or done == total or done % sizes_progress_step == 0:
                    stage_percent = min(43, 28 + int((done / max(1, total)) * 15))
                    _update_sync_progress(
                        log,
                        percent=stage_percent,
                        stage="Stocks",
                        detail=f"Loading stock sizes: {done}/{total} SKU.",
                    )

            sizes_payload_by_nm_id = fetch_product_sizes_payloads(
                nm_ids=[product.nm_id for product in products],
                snapshot_date=dates.stock_date,
                max_workers=4,
                on_item_done=_on_sizes_item_done,
            )

            for product in products:
                _assert_not_cancelled(log)
                sizes_payload = sizes_payload_by_nm_id.get(product.nm_id)
                if sizes_payload is None:
                    sizes_payload = analytics_client.get_product_sizes(nm_id=product.nm_id, snapshot_date=dates.stock_date)
                derived_stock_payload = build_product_stock_payload_from_sizes(sizes_payload)
                stock_item = stock_items_by_nm_id.get(product.nm_id)
                if stock_item:
                    update_product_from_payload(product, stock_item)
                upsert_product_stock(
                    product=product,
                    stats_date=dates.stock_date,
                    item_payload=derived_stock_payload,
                    overwrite=overwrite,
                )
                upsert_warehouse_stocks(
                    product=product,
                    stats_date=dates.stock_date,
                    sizes_payload=sizes_payload,
                    overwrite=overwrite,
                    warehouse_cache=warehouse_cache,
                )

            _update_sync_progress(
                log,
                percent=45,
                stage="Реклама",
                detail="Обновление статусов кампаний и рекламной статистики.",
            )
            _assert_not_cancelled(log)
            if campaigns:
                refresh_campaigns_metadata(campaigns, promotion_client=promotion_client)
                stats_payload = promotion_client.get_campaign_stats(
                    ids=[campaign.external_id for campaign in campaigns],
                    start_date=dates.stats_date,
                    end_date=dates.stats_date,
                )
                for item in stats_payload:
                    _assert_not_cancelled(log)
                    upsert_campaign_stats(
                        product_map=product_map,
                        campaign_map=campaign_map,
                        stat_payload=item,
                        overwrite=overwrite,
                        allowed_dates={dates.stats_date},
                        linked_products_by_campaign_id=linked_products_by_campaign_id,
                        tracked_product_ids=tracked_product_ids,
                    )

            _update_sync_progress(
                log,
                percent=60,
                stage="Цены и заказы",
                detail="Сбор цен и заказов поставщика.",
            )
            _assert_not_cancelled(log)
            def _fetch_price_lookup() -> tuple[dict[int, dict[str, Decimal]], str | None]:
                try:
                    local_prices_client = PricesWBClient()
                    lookup: dict[int, dict[str, Decimal]] = {}
                    for nm_chunk in batched(list(product_map.keys()), 1000):
                        if not nm_chunk:
                            continue
                        lookup.update(build_price_lookup(local_prices_client.get_goods_prices(nm_ids=nm_chunk)))
                    return lookup, None
                except WBApiError as exc:
                    return {}, f"prices: {exc}"

            def _fetch_supplier_orders_lookup() -> tuple[dict[int, list[dict[str, Any]]], str | None]:
                try:
                    local_statistics_client = StatisticsWBClient()
                    supplier_rows = local_statistics_client.get_supplier_orders(date_from=dates.stats_date)
                    supplier_lookup_by_date, undated_supplier_lookup = split_supplier_orders_lookup_by_date(supplier_rows)
                    supplier_lookup = supplier_lookup_by_date.get(dates.stats_date, {})
                    if not supplier_lookup:
                        supplier_lookup = undated_supplier_lookup
                    return supplier_lookup, None
                except WBApiError as exc:
                    return {}, f"supplier_orders: {exc}"

            with ThreadPoolExecutor(max_workers=2, thread_name_prefix="wb-sync-stage") as executor:
                price_future = executor.submit(_fetch_price_lookup)
                supplier_future = executor.submit(_fetch_supplier_orders_lookup)
                price_lookup, price_error = price_future.result()
                supplier_orders_lookup, supplier_error = supplier_future.result()

            if price_error:
                optional_errors.append(price_error)
            if supplier_error:
                optional_errors.append(supplier_error)

            _update_sync_progress(
                log,
                percent=74,
                stage="Ключевые запросы",
                detail="Сбор органики и буст-статистики по ключам.",
            )
            _assert_not_cancelled(log)
            boosted_keyword_aggregates: dict[int, dict[str, dict[str, Any]]] = defaultdict(dict)
            search_cluster_totals_by_pair: dict[tuple[int, int], dict[str, Any]] = {}
            if campaigns:
                current_date_iso = dates.stats_date.isoformat()
                normquery_items = [
                    {"advertId": pair.campaign.external_id, "nmId": pair.product.nm_id}
                    for pair in campaign_pairs
                ]
                try:
                    for item_chunk in batched(normquery_items, 100):
                        _assert_not_cancelled(log)
                        if not item_chunk:
                            continue
                        nm_ids_by_advert: dict[int, list[int]] = defaultdict(list)
                        for pair_payload in item_chunk:
                            nm_ids_by_advert[pair_payload["advertId"]].append(pair_payload["nmId"])
                        payload = promotion_client.get_daily_search_cluster_stats(
                            items=item_chunk,
                            start_date=dates.stats_date,
                            end_date=dates.stats_date,
                        )
                        for item in payload.get("items") or []:
                            _assert_not_cancelled(log)
                            advert_id = item.get("advertId")
                            campaign = campaign_map.get(advert_id)
                            nm_id = item.get("nmId")
                            if not nm_id:
                                candidates = nm_ids_by_advert.get(advert_id, [])
                                if len(candidates) == 1:
                                    nm_id = candidates[0]
                            product = product_map.get(nm_id)
                            if not campaign or not product:
                                continue
                            pair_key = (campaign.id, product.id)
                            pair_totals = search_cluster_totals_by_pair.setdefault(
                                pair_key,
                                {
                                    "impressions": 0,
                                    "clicks": 0,
                                    "spend": Decimal("0"),
                                    "add_to_cart_count": 0,
                                    "order_count": 0,
                                    "units_ordered": 0,
                                    "raw_payload": [],
                                },
                            )
                            product_bucket = boosted_keyword_aggregates[product.id]
                            for daily_stat in item.get("dailyStats") or []:
                                _assert_not_cancelled(log)
                                stat_date = (daily_stat.get("date") or "")[:10]
                                if stat_date and stat_date != current_date_iso:
                                    continue
                                stat = daily_stat.get("stat") or {}
                                views = int(stat.get("views") or 0)
                                clicks = int(stat.get("clicks") or 0)
                                pair_totals["impressions"] += views
                                pair_totals["clicks"] += clicks
                                pair_totals["spend"] += decimalize(stat.get("spend"))
                                pair_totals["add_to_cart_count"] += int(stat.get("atbs") or 0)
                                pair_totals["order_count"] += int(stat.get("orders") or 0)
                                pair_totals["units_ordered"] += int(stat.get("shks") or 0)
                                pair_totals["raw_payload"].append(daily_stat)

                                normalized_query = normalize_search_text(stat.get("normQuery") or "")
                                if not normalized_query:
                                    continue
                                entry = product_bucket.setdefault(
                                    normalized_query,
                                    {
                                        "weighted_position_sum": Decimal("0"),
                                        "weight": Decimal("0"),
                                        "views": 0,
                                        "clicks": 0,
                                        "raw_payload": [],
                                    },
                                )
                                weight = Decimal(views or 1)
                                entry["weighted_position_sum"] += decimalize(stat.get("avgPos")) * weight
                                entry["weight"] += weight
                                entry["views"] += views
                                entry["clicks"] += clicks
                                entry["raw_payload"].append(stat)
                except WBApiError as exc:
                    optional_errors.append(f"boosted_keywords: {exc}")

                if overwrite:
                    DailyCampaignSearchClusterStat.objects.filter(
                        campaign__in=campaigns,
                        product__in=products,
                        stats_date=dates.stats_date,
                    ).delete()
                if search_cluster_totals_by_pair:
                    cluster_rows = [
                        DailyCampaignSearchClusterStat(
                            campaign_id=campaign_id,
                            product_id=product_id,
                            stats_date=dates.stats_date,
                            impressions=payload["impressions"],
                            clicks=payload["clicks"],
                            spend=quantize_money(payload["spend"]),
                            add_to_cart_count=payload["add_to_cart_count"],
                            order_count=payload["order_count"],
                            units_ordered=payload["units_ordered"],
                            raw_payload={"daily_stats": payload["raw_payload"]},
                        )
                        for (campaign_id, product_id), payload in search_cluster_totals_by_pair.items()
                    ]
                    if overwrite:
                        DailyCampaignSearchClusterStat.objects.bulk_create(cluster_rows, batch_size=500)
                    else:
                        for row in cluster_rows:
                            DailyCampaignSearchClusterStat.objects.get_or_create(
                                campaign_id=row.campaign_id,
                                product_id=row.product_id,
                                stats_date=row.stats_date,
                                defaults={
                                    "impressions": row.impressions,
                                    "clicks": row.clicks,
                                    "spend": row.spend,
                                    "add_to_cart_count": row.add_to_cart_count,
                                    "order_count": row.order_count,
                                    "units_ordered": row.units_ordered,
                                    "raw_payload": row.raw_payload,
                                },
                            )

            boosted_keyword_stats: dict[int, dict[str, dict[str, Any]]] = {}
            for product_id, query_map in boosted_keyword_aggregates.items():
                boosted_keyword_stats[product_id] = {}
                for query_text, aggregate in query_map.items():
                    weight = aggregate["weight"]
                    views = aggregate["views"]
                    clicks = aggregate["clicks"]
                    boosted_keyword_stats[product_id][query_text] = {
                        "avg_position": quantize_money(
                            aggregate["weighted_position_sum"] / weight if weight else Decimal("0")
                        ),
                        "ctr": quantize_money(decimalize(clicks) * Decimal("100") / Decimal(views)) if views else Decimal("0"),
                        "views": views,
                        "clicks": clicks,
                        "raw_payload": aggregate["raw_payload"],
                    }

            enabled_groups_by_product: dict[int, set[str]] = defaultdict(set)
            groups_rows = (
                DailyCampaignProductStat.objects.filter(product_id__in=tracked_product_ids, stats_date=dates.stats_date)
                .values("product_id", "campaign__monitoring_group")
                .distinct()
            )
            for row in groups_rows:
                enabled_groups_by_product[row["product_id"]].add(
                    row["campaign__monitoring_group"] or CampaignMonitoringGroup.OTHER
                )

            _update_sync_progress(
                log,
                percent=88,
                stage="Обзор по товарам",
                detail="Заполнение заметок и итоговых полей по карточкам товаров.",
            )
            _assert_not_cancelled(log)
            product_enrichment_by_id, enrichment_warnings = fetch_product_enrichment_payloads(
                products=products,
                stats_date=dates.stats_date,
                max_workers=4,
                analytics_client=analytics_client,
                feedbacks_client=feedbacks_client,
            )
            optional_errors.extend(enrichment_warnings)
            for product in products:
                _assert_not_cancelled(log)
                enrichment_payload = product_enrichment_by_id.get(product.id) or {}
                keywords = enrichment_payload.get("keywords") or []
                organic_keyword_payload = enrichment_payload.get("organic_keyword_payload")
                negative_feedback_count = enrichment_payload.get("negative_feedback_count")

                if keywords:
                    upsert_keyword_stats(
                        product=product,
                        stats_date=dates.stats_date,
                        keywords=keywords,
                        organic_payload=organic_keyword_payload,
                        boosted_stats=boosted_keyword_stats.get(product.id, {}),
                        overwrite=overwrite,
                    )

                supplier_order_rows = supplier_orders_lookup.get(product.nm_id) or []
                supplier_order_summary = summarize_supplier_orders(supplier_order_rows) if supplier_order_rows else None

                upsert_product_note(
                    product=product,
                    stats_date=dates.stats_date,
                    overwrite=overwrite,
                    supplier_order_summary=supplier_order_summary,
                    price_summary=price_lookup.get(product.nm_id),
                    negative_feedback_count=negative_feedback_count,
                    enabled_groups=enabled_groups_by_product.get(product.id, set()),
                )

        _assert_not_cancelled(log)
        _update_sync_progress(
            log,
            percent=96,
            stage="Финализация",
            detail="Формирование итогов синхронизации.",
        )
        _assert_not_cancelled(log)
        log.status = SyncStatus.SUCCESS
        log.finished_at = timezone.now()
        final_payload = {
            **(log.payload or {}),
            "progress": {
                "percent": 100,
                "stage": "Завершено",
                "detail": "Синхронизация завершена успешно.",
                "updated_at": timezone.now().isoformat(),
            },
        }
        if optional_errors:
            final_payload["warnings"] = optional_errors
        log.payload = final_payload
        optional_note = f" Предупреждений: {len(optional_errors)}." if optional_errors else ""
        log.message = f"Синхронизация завершена.{optional_note}".strip()
        log.save(update_fields=["status", "finished_at", "message", "payload", "updated_at"])
        _sync_console(
            f"log={log.id} finished status={log.status} warnings={len(optional_errors)} "
            f"date={dates.stats_date.isoformat()}"
        )
        return log
    except SyncCancelledError as exc:
        log.status = SyncStatus.CANCELED
        log.finished_at = timezone.now()
        log.message = str(exc)
        log.payload = {
            **(log.payload or {}),
            "cancel_requested": True,
            "progress": {
                "percent": 100,
                "stage": "Отменено",
                "detail": str(exc),
                "updated_at": timezone.now().isoformat(),
            },
        }
        log.save(update_fields=["status", "finished_at", "message", "payload", "updated_at"])
        _sync_console(
            f"log={log.id} finished status={log.status} reason=cancelled date={dates.stats_date.isoformat()}"
        )
        raise
    except (WBApiError, SyncServiceError, ValueError) as exc:
        friendly_message = humanize_sync_error_message(str(exc))
        log.status = SyncStatus.ERROR
        log.finished_at = timezone.now()
        log.message = friendly_message
        log.payload = {
            **(log.payload or {}),
            "progress": {
                "percent": 100,
                "stage": "Ошибка",
                "detail": friendly_message,
                "updated_at": timezone.now().isoformat(),
            },
        }
        log.save(update_fields=["status", "finished_at", "message", "payload", "updated_at"])
        _sync_console(
            f"log={log.id} finished status={log.status} reason=error date={dates.stats_date.isoformat()} "
            f"detail={friendly_message}"
        )
        raise SyncServiceError(friendly_message) from exc


def next_run_at(now: datetime, hour: int, minute: int) -> datetime:
    candidate = datetime.combine(now.date(), time(hour=hour, minute=minute), tzinfo=now.tzinfo)
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate
