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
import time
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

WB_RATE_LIMIT_MARKERS = ("429", "461", "global limiter", "rate limit", "limited by")
API_FAMILY_ANALYTICS = "analytics"
API_FAMILY_PROMOTION = "promotion"
API_FAMILY_PRICES = "prices"
API_FAMILY_STATISTICS = "statistics"
API_FAMILY_FEEDBACKS = "feedbacks"
RATE_LIMIT_SECTION_HINTS = {
    "sales_funnel_history": "Воронка продаж: показы, корзины, заказы, выкупы",
    "product_stocks": "Остатки по товарам",
    "product_sizes": "Остатки и склады по размерам",
    "campaign_stats": "Реклама: расходы, корзины и заказы РК",
    "campaign_metadata": "Метаданные рекламных кампаний",
    "boosted_keywords": "Ключевые запросы РК",
    "prices": "Цены из WB Prices",
    "supplier_orders": "Заказы поставщика: СПП, Цена WBSELLER, Цена WB",
    "organic_keywords": "Органика по ключам",
    "product_enrichment": "Обзор по товарам",
    "feedbacks": "Негативные отзывы",
}


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


def is_wb_start_day_limit_error(message: str) -> bool:
    normalized = (message or "").lower()
    return (
        ("invalid start day" in normalized and "excess limit on days" in normalized)
        or "не позволяет запрашивать эту дату" in normalized
    )


def is_wb_rate_limit_error(message: str) -> bool:
    normalized = (message or "").lower()
    return any(marker in normalized for marker in WB_RATE_LIMIT_MARKERS)


def configure_sync_wb_client(client: Any, *, max_retries: int | None = None) -> Any:
    if max_retries is not None:
        client.max_retries = max_retries
    client.max_retry_delay_seconds = min(float(getattr(client, "max_retry_delay_seconds", 30.0)), 30.0)
    client.fast_fail_rate_limit = True
    client.update_shared_rate_limit_on_429 = False
    return client


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
    max_workers: int = 1,
    on_item_done: Callable[[int, int], None] | None = None,
    client_factory: Callable[[], AnalyticsWBClient] | None = None,
) -> dict[int, dict[str, Any]]:
    if not nm_ids:
        return {}

    total = len(nm_ids)
    worker_count = max(1, min(max_workers, len(nm_ids)))
    resolved_client_factory = client_factory or AnalyticsWBClient
    if worker_count == 1:
        client = resolved_client_factory()
        payloads: dict[int, dict[str, Any]] = {}
        for index, nm_id in enumerate(nm_ids, start=1):
            payloads[nm_id] = client.get_product_sizes(nm_id=nm_id, snapshot_date=snapshot_date)
            if on_item_done:
                on_item_done(index, total)
        return payloads

    def _fetch_one(target_nm_id: int) -> tuple[int, dict[str, Any]]:
        thread_client = resolved_client_factory()
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
        latest_note_keywords = (
            DailyProductNote.objects.filter(product=product)
            .exclude(keywords=[])
            .order_by("-note_date", "-id")
            .values_list("keywords", flat=True)
            .first()
        )
        if isinstance(latest_note_keywords, list):
            keyword_texts = [
                str(item).strip()
                for item in latest_note_keywords
                if str(item).strip()
            ]
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


def refresh_available_campaigns_metadata(*, promotion_client: PromotionWBClient | None = None) -> list[Campaign]:
    promotion_client = promotion_client or PromotionWBClient()
    response = promotion_client.get_campaigns()
    campaigns: list[Campaign] = []
    for payload in response.get("adverts", []) or []:
        external_id = payload.get("id")
        if not external_id:
            continue
        campaign, _ = Campaign.objects.get_or_create(external_id=external_id)
        campaigns.append(_apply_campaign_metadata_payload(campaign, payload))
    return campaigns


def _upsert(
    model,
    *,
    lookup: dict,
    defaults: dict,
    overwrite: bool,
    bulk_create: bool = False,
) -> Any | None:
    """Универсальный upsert: update_or_create при overwrite, иначе get_or_create."""
    if overwrite:
        if bulk_create:
            return None  # caller сам делает bulk_create
        obj, _ = model.objects.update_or_create(**lookup, defaults=defaults)
        return obj
    obj, _ = model.objects.get_or_create(**lookup, defaults=defaults)
    return obj


def upsert_product_metrics(*, product: Product, stats_date: date, history_entry: dict[str, Any], currency: str, overwrite: bool) -> None:
    _upsert(
        DailyProductMetrics,
        lookup={"product": product, "stats_date": stats_date},
        defaults={
            "open_count": history_entry.get("openCount", 0),
            "add_to_cart_count": history_entry.get("cartCount", 0),
            "order_count": history_entry.get("orderCount", 0),
            "order_sum": quantize_money(decimalize(history_entry.get("orderSum"))),
            "buyout_count": history_entry.get("buyoutCount", 0),
            "buyout_sum": quantize_money(decimalize(history_entry.get("buyoutSum"))),
            "add_to_wishlist_count": history_entry.get("addToWishlistCount", 0),
            "currency": currency,
            "raw_payload": history_entry,
        },
        overwrite=overwrite,
    )


def upsert_product_stock(*, product: Product, stats_date: date, item_payload: dict[str, Any], overwrite: bool) -> None:
    metrics = item_payload.get("metrics") or {}
    avg_orders_per_day = extract_avg_orders(item_payload)
    _upsert(
        DailyProductStock,
        lookup={"product": product, "stats_date": stats_date},
        defaults={
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
        },
        overwrite=overwrite,
    )


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


def _get_or_create_warehouse(
    office_id: int,
    office_name: str,
    region_name: str,
    warehouse_cache: dict[int, Warehouse] | None,
) -> Warehouse:
    """Получить или создать склад с кэшированием."""
    if warehouse_cache and office_id in warehouse_cache:
        wh = warehouse_cache[office_id]
        if wh.name != office_name or wh.region_name != region_name:
            wh.name = office_name
            wh.region_name = region_name
            wh.save(update_fields=["name", "region_name", "updated_at"])
        return wh
    wh, _ = Warehouse.objects.update_or_create(
        office_id=office_id,
        defaults={"name": office_name, "region_name": region_name},
    )
    if warehouse_cache is not None:
        warehouse_cache[office_id] = wh
    return wh


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
        office_id = int(office.get("officeID") or 0)
        if office_id <= 0:
            continue
        metrics = office.get("metrics") or {}
        warehouse = _get_or_create_warehouse(
            office_id,
            office.get("officeName") or "Маркетплейс",
            office.get("regionName") or "",
            warehouse_cache,
        )
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


def _aggregate_campaign_day_stats(
    day_payload: dict[str, Any],
    *,
    product_map: dict[int, Product],
    linked_product_ids: set[int],
) -> dict[tuple[int, str], dict[str, Any]]:
    """Агрегирует статистику кампании за день по продуктам и зонам.
    
    СУММИРУЕТ ВСЕ товары в зоне (как cmp.wildberries.ru), а не только известные продукты.
    Это даёт правильные данные по Корзинам, Заказам и Заказам (руб.) для Единой Ставки.
    """
    aggregated: dict[tuple[int, str], dict[str, Any]] = {}
    zone_totals: dict[str, dict[str, Any]] = {}
    
    # Сначала суммируем ВСЕ товары по зонам (все nmId в кампании)
    for app_payload in day_payload.get("apps", []):
        app_type = app_payload.get("appType")
        zone = map_app_type_to_zone(app_type)
        
        if zone not in zone_totals:
            zone_totals[zone] = {
                "impressions": 0,
                "clicks": 0,
                "spend": Decimal("0"),
                "add_to_cart_count": 0,
                "order_count": 0,
                "units_ordered": 0,
                "order_sum": Decimal("0"),
                "raw_payload": [],
            }
        
        for item in app_payload.get("nms", []):
            zone_totals[zone]["impressions"] += int(item.get("views") or 0)
            zone_totals[zone]["clicks"] += int(item.get("clicks") or 0)
            zone_totals[zone]["spend"] += decimalize(item.get("sum"))
            zone_totals[zone]["add_to_cart_count"] += int(item.get("atbs") or 0)
            zone_totals[zone]["order_count"] += int(item.get("orders") or 0)
            zone_totals[zone]["units_ordered"] += int(item.get("shks") or 0)
            zone_totals[zone]["order_sum"] += decimalize(item.get("sum_price"))
            zone_totals[zone]["raw_payload"].append({"appType": app_type, "item": item})
    
    # Нормализуем данные для Единой ставки - делим на количество товаров
    # чтобы избежать умножения данных при суммировании в отчётах
    linked_count = len(linked_product_ids) if linked_product_ids else len(product_map)
    divisor = max(1, linked_count)

    for nm_id, product in product_map.items():
        # Проверяем linked_product_ids если они есть
        if linked_product_ids and product.id not in linked_product_ids:
            continue

        for zone, totals in zone_totals.items():
            key = (product.id, zone)
            aggregated[key] = {
                "impressions": totals["impressions"] // divisor,
                "clicks": totals["clicks"] // divisor,
                "spend": quantize_money(totals["spend"] / divisor),
                "add_to_cart_count": totals["add_to_cart_count"] // divisor,
                "order_count": totals["order_count"] // divisor,
                "units_ordered": totals["units_ordered"] // divisor,
                "order_sum": quantize_money(totals["order_sum"] / divisor),
                "raw_payload": totals["raw_payload"],
            }

    return aggregated


def _save_campaign_stats(
    aggregated: dict[tuple[int, str], dict[str, Any]],
    *,
    campaign: Campaign,
    stats_date: date,
    overwrite: bool,
) -> None:
    """Сохраняет агрегированную статистику кампании."""
    if overwrite:
        rows_to_create = [
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
            for (product_id, zone), row in aggregated.items()
        ]
        if rows_to_create:
            DailyCampaignProductStat.objects.bulk_create(rows_to_create, batch_size=500)
        return

    for (product_id, zone), row in aggregated.items():
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
        linked_products_by_campaign_id.get(campaign.id, set())
        if linked_products_by_campaign_id is not None
        else set(campaign.products.values_list("id", flat=True))
    )
    tracked_ids = tracked_product_ids if tracked_product_ids is not None else [p.id for p in product_map.values()]

    for day_payload in stat_payload.get("days", []):
        stats_date = datetime.fromisoformat(day_payload.get("date", "").replace("Z", "+00:00")).date()
        if allowed_dates is not None and stats_date not in allowed_dates:
            continue

        if overwrite and tracked_ids:
            DailyCampaignProductStat.objects.filter(
                campaign=campaign, stats_date=stats_date, product_id__in=tracked_ids
            ).delete()

        aggregated = _aggregate_campaign_day_stats(
            day_payload, product_map=product_map, linked_product_ids=linked_product_ids
        )
        _save_campaign_stats(aggregated, campaign=campaign, stats_date=stats_date, overwrite=overwrite)


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
    period_start: date | None = None,
    period_end: date | None = None,
    max_workers: int = 1,
    analytics_client: AnalyticsWBClient | None = None,
    feedbacks_client: FeedbacksWBClient | None = None,
    skip_analytics: bool = False,
    skip_feedbacks: bool = False,
) -> tuple[dict[int, dict[str, Any]], list[str]]:
    payloads_by_product_id: dict[int, dict[str, Any]] = {}
    warnings: list[str] = []
    organic_start_date = period_start or stats_date
    organic_end_date = period_end or stats_date

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

        if keywords and not skip_analytics:
            try:
                organic_keyword_payload = local_analytics_client.get_search_orders(
                    nm_id=product.nm_id,
                    start_date=organic_start_date,
                    end_date=organic_end_date,
                    search_texts=keywords,
                )
            except WBApiError as exc:
                local_warnings.append(f"organic_keywords:{product.nm_id}: {exc}")
        elif keywords and skip_analytics:
            local_warnings.append(f"organic_keywords:{product.nm_id}: skipped because analytics API is rate-limited")

        if not skip_feedbacks:
            try:
                negative_feedback_count = fetch_negative_feedback_count(
                    feedback_client=local_feedbacks_client,
                    product=product,
                    stats_date=stats_date,
                )
            except WBApiError as exc:
                local_warnings.append(f"feedbacks:{product.nm_id}: {exc}")
        else:
            local_warnings.append(f"feedbacks:{product.nm_id}: skipped because feedbacks API is rate-limited")

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


def _update_sync_progress(
    log: SyncLog,
    *,
    percent: int,
    stage: str,
    detail: str | None = None,
    retry_until: str | None = None,
) -> None:
    normalized_percent = max(0, min(int(percent), 100))
    progress_payload = {
        "percent": normalized_percent,
        "stage": stage,
        "detail": detail or "",
        "updated_at": timezone.now().isoformat(),
    }
    if retry_until:
        progress_payload["retry_until"] = retry_until
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

    skipped_dates_due_api_limit: list[date] = []
    current_range_start = range_start
    while current_range_start <= range_end:
        current_sync_dates = iter_sync_dates(date_from=current_range_start, date_to=range_end)
        try:
            log = _run_sync_single_day(
                product_ids=product_ids,
                reference_date=range_end,
                range_start=current_range_start,
                range_end=range_end,
                days_count=len(current_sync_dates),
                overwrite=overwrite,
                kind=kind,
            )
        except SyncServiceError as exc:
            if not is_wb_start_day_limit_error(str(exc)):
                raise
            skipped_dates_due_api_limit.append(current_range_start)
            _sync_console(
                "WB Analytics не принимает начало диапазона "
                f"{current_range_start.isoformat()}. Сдвигаем старт на следующий день."
            )
            current_range_start += timedelta(days=1)
            continue

        if skipped_dates_due_api_limit:
            payload = dict(log.payload or {})
            payload["original_stats_date_from"] = range_start.isoformat()
            payload["skipped_dates_due_api_limit"] = [
                skipped_date.isoformat()
                for skipped_date in skipped_dates_due_api_limit
            ]
            log.payload = payload
            skip_note = (
                "Пропущено дат из-за ограничения WB Analytics: "
                f"{len(skipped_dates_due_api_limit)}."
            )
            log.message = f"{log.message or ''} {skip_note}".strip()
            log.save(update_fields=["payload", "message", "updated_at"])
        return log

    raise SyncServiceError(
        "Выбранный диапазон целиком вне допустимого окна API WB Analytics. "
        "Выберите более свежие даты."
    )


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
    period_start = range_start or dates.stats_date
    period_end = range_end or dates.stats_date
    period_dates = iter_sync_dates(date_from=period_start, date_to=period_end)
    allowed_stats_dates = {item.stats_date for item in period_dates}
    period_label = (
        f"{period_start:%d.%m.%Y}-{period_end:%d.%m.%Y}"
        if period_start != period_end
        else f"{dates.stats_date:%d.%m.%Y}"
    )

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
    optional_errors: list[str] = []
    skipped_sections: list[str] = []
    limited_api_families: set[str] = set()

    def _family_limited(family: str) -> bool:
        return family in limited_api_families

    def _record_rate_limit(family: str, source: str, exc: Exception | str) -> None:
        limited_api_families.add(family)
        warning = f"{source} rate limit: {str(exc)[:120]}"
        if warning not in optional_errors:
            optional_errors.append(warning)
        skipped_section = RATE_LIMIT_SECTION_HINTS.get(source, source)
        if skipped_section not in skipped_sections:
            skipped_sections.append(skipped_section)
        _sync_console(f"WB API family limited family={family} source={source}. Further calls in this family are skipped.")

    try:
        _update_sync_progress(
            log,
            percent=3,
            stage="Подготовка",
            detail="Инициализация клиентов WB и подготовка списка товаров.",
        )
        analytics_client = configure_sync_wb_client(AnalyticsWBClient())
        promotion_client = configure_sync_wb_client(PromotionWBClient(), max_retries=1)
        feedbacks_client = configure_sync_wb_client(FeedbacksWBClient(), max_retries=1)

        def _bind_retry_progress(client, *, percent: int, stage: str) -> None:
            def _on_retry(event: dict[str, Any]) -> None:
                remaining = max(0, int(round(float(event.get("remaining_seconds") or 0))))
                next_attempt = int(event.get("next_attempt") or 0)
                max_retries = int(event.get("max_retries") or 0)
                path = str(event.get("path") or "")
                # Рассчитываем timestamp когда retry завершится (для live countdown на фронте)
                retry_until = (timezone.now() + timezone.timedelta(seconds=remaining)).isoformat()
                if event.get("is_global_limiter"):
                    detail = (
                        f"WB вернул 429/461 на {path}. Ждём {remaining} сек перед повтором "
                        f"{next_attempt}/{max_retries}. Синхронизация не зависла."
                    )
                else:
                    detail = (
                        f"WB API временно недоступен на {path}. Ждём {remaining} сек перед повтором "
                        f"{next_attempt}/{max_retries}."
                    )
                _update_sync_progress(log, percent=percent, stage=stage, detail=detail, retry_until=retry_until)
                _assert_not_cancelled(log)

            client.retry_callback = _on_retry

        _bind_retry_progress(analytics_client, percent=18, stage="WB лимит")
        _bind_retry_progress(promotion_client, percent=46, stage="WB лимит рекламы")
        _bind_retry_progress(feedbacks_client, percent=90, stage="WB лимит")

        queryset = Product.objects.filter(is_active=True)
        if product_ids:
            queryset = queryset.filter(id__in=product_ids)
        products = list(queryset.order_by("id"))
        if not products:
            raise SyncServiceError("Нет активных товаров для синхронизации.")
        _assert_not_cancelled(log)

        product_map = {product.nm_id: product for product in products}
        tracked_product_ids = [product.id for product in products]
        try:
            refresh_available_campaigns_metadata(promotion_client=configure_sync_wb_client(PromotionWBClient(), max_retries=1))
        except WBApiError as exc:
            err_str = str(exc).lower()
            if is_wb_rate_limit_error(err_str):
                _sync_console("ПРЕДУПРЕЖДЕНИЕ: WB API rate limit (429/461) на списке рекламных кампаний. Используем уже сохранённые РК.")
                _record_rate_limit(API_FAMILY_PROMOTION, "campaign_metadata", exc)
            else:
                raise
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
            detail=f"Сбор воронки продаж за {period_label}.",
        )

        # SQLite блокирует всю БД на запись во время длинной транзакции.
        # Здесь много сетевых вызовов, поэтому держать один giant-atomic нельзя:
        # это провоцирует "database is locked" в веб-запросах.
        with nullcontext():
            _assert_not_cancelled(log)
            # Разбиваем запросы на chunks, чтобы избежать зависания WB API
            funnel_rows: list[dict[str, Any]] = []
            nm_ids_list = list(product_map.keys())
            funnel_chunk_size = 20  # WB limit for /sales-funnel/products/history nmIds
            total_funnel_chunks = max(1, (len(nm_ids_list) + funnel_chunk_size - 1) // funnel_chunk_size)
            _sync_console(f"Начинаем сбор воронки: {len(nm_ids_list)} товаров, {total_funnel_chunks} чанков по {funnel_chunk_size}")
            if _family_limited(API_FAMILY_ANALYTICS):
                _sync_console("Пропускаем воронку: Analytics API уже ограничен в этом запуске.")
            else:
                for chunk_index, nm_chunk in enumerate(batched(nm_ids_list, funnel_chunk_size), start=1):
                    _assert_not_cancelled(log)
                    if not nm_chunk:
                        continue
                    _sync_console(f"Запрос воронки: чанк {chunk_index}/{total_funnel_chunks} ({len(nm_chunk)} товаров)")
                    import time
                    start_time = time.monotonic()
                    try:
                        chunk_rows = analytics_client.get_sales_funnel_history(
                            nm_ids=list(nm_chunk),
                            start_date=period_start,
                            end_date=period_end,
                        )
                        elapsed = time.monotonic() - start_time
                        _sync_console(f"Воронка чанк {chunk_index}/{total_funnel_chunks} получено {len(chunk_rows)} строк за {elapsed:.1f}с")
                        funnel_rows.extend(chunk_rows)
                    except WBApiError as exc:
                        elapsed = time.monotonic() - start_time
                        err_str = str(exc).lower()
                        # При 429/461 (rate limit / global limiter) продолжаем без данных воронки, чтобы не зависать
                        is_rate_limit = is_wb_rate_limit_error(err_str)
                        if is_rate_limit:
                            _sync_console(f"ПРЕДУПРЕЖДЕНИЕ: WB API rate limit (429/461) на воронке после {elapsed:.1f}с. Пропускаем.")
                            _record_rate_limit(API_FAMILY_ANALYTICS, "sales_funnel_history", exc)
                            break
                        _sync_console(f"ОШИБКА воронки чанк {chunk_index}/{total_funnel_chunks} после {elapsed:.1f}с: {exc}")
                        raise
                    # Обновляем прогресс внутри этапа "Воронка" (12% -> 28%)
                    sub_percent = 12 + int((chunk_index / total_funnel_chunks) * 15)
                    _update_sync_progress(
                        log,
                        percent=sub_percent,
                        stage="Воронка",
                        detail=f"Сбор воронки: chunk {chunk_index}/{total_funnel_chunks} ({len(nm_chunk)} товаров).",
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
                    if history_date not in allowed_stats_dates:
                        continue
                    upsert_product_metrics(
                        product=product,
                        stats_date=history_date,
                        history_entry=history_entry,
                        currency=row.get("currency", "RUB"),
                        overwrite=overwrite,
                    )

            # БЕЗОПАСНЫЙ РЕЖИМ: пауза между этапами для "отдыха" API
            time.sleep(1.0)
            _update_sync_progress(
                log,
                percent=28,
                stage="Остатки",
                detail=f"Сбор остатков и складов ({len(products)} товаров).",
            )
            _assert_not_cancelled(log)
            stock_items_by_nm_id: dict[int, dict[str, Any]] = {}
            if _family_limited(API_FAMILY_ANALYTICS):
                _sync_console("Пропускаем остатки: Analytics API уже ограничен в этом запуске.")
            else:
                try:
                    # Разбиваем запросы остатков на chunks для стабильности
                    stock_chunk_size = 50  # БЕЗОПАСНЫЙ РЕЖИМ: уменьшили с 200 до 50
                    for nm_chunk in batched(list(product_map.keys()), stock_chunk_size):
                        _assert_not_cancelled(log)
                        if not nm_chunk:
                            continue
                        stock_response = analytics_client.get_product_stocks(
                            nm_ids=list(nm_chunk),
                            snapshot_date=dates.stock_date,
                        )
                        for item in stock_response.get("data", {}).get("items", []) or []:
                            nm_id = item.get("nmID")
                            if nm_id:
                                stock_items_by_nm_id[nm_id] = item
                except WBApiError as exc:
                    err_str = str(exc).lower()
                    if is_wb_rate_limit_error(err_str):
                        _sync_console(f"ПРЕДУПРЕЖДЕНИЕ: WB API rate limit (429/461) на остатках. Пропускаем.")
                        _record_rate_limit(API_FAMILY_ANALYTICS, "product_stocks", exc)
                    elif "wb api 500" not in err_str:
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

            sizes_payload_by_nm_id: dict[int, dict[str, Any]] = {}
            if _family_limited(API_FAMILY_ANALYTICS):
                _sync_console("Пропускаем размеры: Analytics API уже ограничен в этом запуске.")
            else:
                try:
                    sizes_payload_by_nm_id = fetch_product_sizes_payloads(
                        nm_ids=[product.nm_id for product in products],
                        snapshot_date=dates.stock_date,
                        max_workers=1,
                        on_item_done=_on_sizes_item_done,
                        client_factory=lambda: configure_sync_wb_client(AnalyticsWBClient()),
                    )
                except WBApiError as exc:
                    err_str = str(exc).lower()
                    if is_wb_rate_limit_error(err_str):
                        _sync_console(f"ПРЕДУПРЕЖДЕНИЕ: WB API rate limit (429/461) на размерах. Пропускаем.")
                        _record_rate_limit(API_FAMILY_ANALYTICS, "product_sizes", exc)
                    else:
                        raise

            for product in products:
                _assert_not_cancelled(log)
                sizes_payload = sizes_payload_by_nm_id.get(product.nm_id)
                # Если нет sizes_payload (rate limit или нет данных), используем минимальный payload
                if sizes_payload is None:
                    if _family_limited(API_FAMILY_ANALYTICS):
                        sizes_payload = {}
                    else:
                        try:
                            sizes_payload = analytics_client.get_product_sizes(nm_id=product.nm_id, snapshot_date=dates.stock_date)
                        except WBApiError as exc:
                            err_str = str(exc).lower()
                            if is_wb_rate_limit_error(err_str):
                                _sync_console(f"ПРЕДУПРЕЖДЕНИЕ: Пропуск размеров для nm_id={product.nm_id} из-за rate limit")
                                _record_rate_limit(API_FAMILY_ANALYTICS, "product_sizes", exc)
                                sizes_payload = {}
                            else:
                                raise
                derived_stock_payload = build_product_stock_payload_from_sizes(sizes_payload) if sizes_payload else {"metrics": {}}
                stock_item = stock_items_by_nm_id.get(product.nm_id)
                if stock_item:
                    update_product_from_payload(product, stock_item)
                if sizes_payload:
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

            # БЕЗОПАСНЫЙ РЕЖИМ: пауза перед рекламой (чувствительный API)
            time.sleep(10.0)
            _update_sync_progress(
                log,
                percent=45,
                stage="Реклама",
                detail="Обновление статусов кампаний и рекламной статистики.",
            )
            _assert_not_cancelled(log)
            if campaigns and _family_limited(API_FAMILY_PROMOTION):
                _sync_console("Пропускаем рекламу: Promotion API уже ограничен в этом запуске.")
            elif campaigns:
                try:
                    _update_sync_progress(
                        log,
                        percent=45,
                        stage="Реклама",
                        detail="Получение рекламной статистики WB.",
                    )
                    stats_payload = promotion_client.get_campaign_stats(
                        ids=[campaign.external_id for campaign in campaigns],
                        start_date=period_start,
                        end_date=period_end,
                    )
                    for item in stats_payload:
                        _assert_not_cancelled(log)
                        upsert_campaign_stats(
                            product_map=product_map,
                            campaign_map=campaign_map,
                            stat_payload=item,
                            overwrite=overwrite,
                            allowed_dates=allowed_stats_dates,
                            linked_products_by_campaign_id=linked_products_by_campaign_id,
                            tracked_product_ids=tracked_product_ids,
                        )
                    try:
                        _update_sync_progress(
                            log,
                            percent=58,
                            stage="Реклама",
                            detail="Обновление метаданных рекламных кампаний.",
                        )
                        metadata_client = configure_sync_wb_client(PromotionWBClient(), max_retries=1)
                        refresh_campaigns_metadata(campaigns, promotion_client=metadata_client)
                    except WBApiError as exc:
                        err_str = str(exc).lower()
                        if is_wb_rate_limit_error(err_str):
                            _sync_console("ПРЕДУПРЕЖДЕНИЕ: WB API rate limit (429/461) на метаданных кампаний. Пропускаем.")
                            _record_rate_limit(API_FAMILY_PROMOTION, "campaign_metadata", exc)
                        else:
                            raise
                except WBApiError as exc:
                    err_str = str(exc).lower()
                    if is_wb_rate_limit_error(err_str):
                        _sync_console(f"ПРЕДУПРЕЖДЕНИЕ: WB API rate limit (429/461) на рекламе. Пропускаем.")
                        _record_rate_limit(API_FAMILY_PROMOTION, "campaign_stats", exc)
                    else:
                        raise

            _update_sync_progress(
                log,
                percent=60,
                stage="Цены и заказы",
                detail="Сбор цен и заказов поставщика.",
            )
            _assert_not_cancelled(log)
            def _fetch_price_lookup() -> tuple[dict[int, dict[str, Decimal]], str | None]:
                try:
                    local_prices_client = configure_sync_wb_client(PricesWBClient())
                    lookup: dict[int, dict[str, Decimal]] = {}
                    for nm_chunk in batched(list(product_map.keys()), 1000):
                        if not nm_chunk:
                            continue
                        lookup.update(build_price_lookup(local_prices_client.get_goods_prices(nm_ids=nm_chunk)))
                    return lookup, None
                except WBApiError as exc:
                    return {}, f"prices: {exc}"

            def _fetch_supplier_orders_lookup() -> tuple[
                dict[date, dict[int, list[dict[str, Any]]]],
                dict[int, list[dict[str, Any]]],
                str | None,
            ]:
                try:
                    local_statistics_client = configure_sync_wb_client(StatisticsWBClient())
                    supplier_rows = local_statistics_client.get_supplier_orders(date_from=period_start)
                    supplier_lookup_by_date, undated_supplier_lookup = split_supplier_orders_lookup_by_date(supplier_rows)
                    supplier_lookup_by_date = {
                        stats_date: lookup
                        for stats_date, lookup in supplier_lookup_by_date.items()
                        if stats_date in allowed_stats_dates
                    }
                    return supplier_lookup_by_date, undated_supplier_lookup, None
                except WBApiError as exc:
                    return {}, {}, f"supplier_orders: {exc}"

            if _family_limited(API_FAMILY_PRICES):
                _sync_console("Пропускаем цены: Prices API уже ограничен в этом запуске.")
                price_lookup, price_error = {}, None
            else:
                price_lookup, price_error = _fetch_price_lookup()
            if price_error:
                optional_errors.append(price_error)
                if is_wb_rate_limit_error(price_error):
                    _record_rate_limit(API_FAMILY_PRICES, "prices", price_error)
                    price_lookup = {}

            if _family_limited(API_FAMILY_STATISTICS):
                _sync_console("Пропускаем заказы поставщика: Statistics API уже ограничен в этом запуске.")
                supplier_orders_lookup_by_date, undated_supplier_orders_lookup, supplier_error = {}, {}, None
            else:
                supplier_orders_lookup_by_date, undated_supplier_orders_lookup, supplier_error = _fetch_supplier_orders_lookup()
            if supplier_error:
                optional_errors.append(supplier_error)
                if is_wb_rate_limit_error(supplier_error):
                    _record_rate_limit(API_FAMILY_STATISTICS, "supplier_orders", supplier_error)
                    supplier_orders_lookup_by_date = {}
                    undated_supplier_orders_lookup = {}

            _update_sync_progress(
                log,
                percent=74,
                stage="Ключевые запросы",
                detail="Сбор органики и буст-статистики по ключам.",
            )
            _assert_not_cancelled(log)
            boosted_keyword_aggregates: dict[date, dict[int, dict[str, dict[str, Any]]]] = defaultdict(
                lambda: defaultdict(dict)
            )
            search_cluster_totals_by_pair: dict[tuple[date, int, int], dict[str, Any]] = {}
            search_cluster_refresh_ok = False
            if campaigns and _family_limited(API_FAMILY_PROMOTION):
                _sync_console("Пропускаем ключевые запросы рекламы: Promotion API уже ограничен в этом запуске.")
            elif campaigns:
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
                            start_date=period_start,
                            end_date=period_end,
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
                            for daily_stat in item.get("dailyStats") or []:
                                _assert_not_cancelled(log)
                                raw_stat_date = (daily_stat.get("date") or "")[:10]
                                try:
                                    target_stats_date = date.fromisoformat(raw_stat_date) if raw_stat_date else dates.stats_date
                                except ValueError:
                                    target_stats_date = dates.stats_date
                                if target_stats_date not in allowed_stats_dates:
                                    continue
                                stat = daily_stat.get("stat") or {}
                                pair_key = (target_stats_date, campaign.id, product.id)
                                pair_totals = search_cluster_totals_by_pair.setdefault(
                                    pair_key,
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
                                product_bucket = boosted_keyword_aggregates[target_stats_date][product.id]
                                views = int(stat.get("views") or 0)
                                clicks = int(stat.get("clicks") or 0)
                                pair_totals["impressions"] += views
                                pair_totals["clicks"] += clicks
                                pair_totals["spend"] += decimalize(stat.get("spend"))
                                pair_totals["add_to_cart_count"] += int(stat.get("atbs") or 0)
                                pair_totals["order_count"] += int(stat.get("orders") or 0)
                                pair_totals["units_ordered"] += int(stat.get("shks") or 0)
                                pair_totals["order_sum"] += decimalize(stat.get("sum_price"))
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
                    search_cluster_refresh_ok = True
                except WBApiError as exc:
                    err_str = str(exc).lower()
                    if is_wb_rate_limit_error(err_str):
                        _sync_console(f"ПРЕДУПРЕЖДЕНИЕ: WB API rate limit (429/461) на ключевых словах. Пропускаем.")
                        _record_rate_limit(API_FAMILY_PROMOTION, "boosted_keywords", exc)
                    else:
                        optional_errors.append(f"boosted_keywords: {exc}")

                if search_cluster_refresh_ok and overwrite:
                    DailyCampaignSearchClusterStat.objects.filter(
                        campaign__in=campaigns,
                        product__in=products,
                        stats_date__in=allowed_stats_dates,
                    ).delete()
                if search_cluster_refresh_ok and search_cluster_totals_by_pair:
                    cluster_rows = [
                        DailyCampaignSearchClusterStat(
                            campaign_id=campaign_id,
                            product_id=product_id,
                            stats_date=stats_date,
                            impressions=payload["impressions"],
                            clicks=payload["clicks"],
                            spend=quantize_money(payload["spend"]),
                            add_to_cart_count=payload["add_to_cart_count"],
                            order_count=payload["order_count"],
                            units_ordered=payload["units_ordered"],
                            order_sum=quantize_money(payload["order_sum"]),
                            raw_payload={"daily_stats": payload["raw_payload"]},
                        )
                        for (stats_date, campaign_id, product_id), payload in search_cluster_totals_by_pair.items()
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

            boosted_keyword_stats_by_date: dict[date, dict[int, dict[str, dict[str, Any]]]] = {}
            for stats_date, product_query_map in boosted_keyword_aggregates.items():
                boosted_keyword_stats_by_date[stats_date] = {}
                for product_id, query_map in product_query_map.items():
                    boosted_keyword_stats_by_date[stats_date][product_id] = {}
                    for query_text, aggregate in query_map.items():
                        weight = aggregate["weight"]
                        views = aggregate["views"]
                        clicks = aggregate["clicks"]
                        boosted_keyword_stats_by_date[stats_date][product_id][query_text] = {
                            "avg_position": quantize_money(
                                aggregate["weighted_position_sum"] / weight if weight else Decimal("0")
                            ),
                            "ctr": quantize_money(decimalize(clicks) * Decimal("100") / Decimal(views)) if views else Decimal("0"),
                            "views": views,
                            "clicks": clicks,
                            "raw_payload": aggregate["raw_payload"],
                        }

            enabled_groups_by_date_product: dict[date, dict[int, set[str]]] = defaultdict(lambda: defaultdict(set))
            groups_rows = (
                DailyCampaignProductStat.objects.filter(product_id__in=tracked_product_ids, stats_date__in=allowed_stats_dates)
                .values("stats_date", "product_id", "campaign__monitoring_group")
                .distinct()
            )
            for row in groups_rows:
                enabled_groups_by_date_product[row["stats_date"]][row["product_id"]].add(
                    row["campaign__monitoring_group"] or CampaignMonitoringGroup.OTHER
                )

            _update_sync_progress(
                log,
                percent=88,
                stage="Обзор по товарам",
                detail="Заполнение заметок и итоговых полей по карточкам товаров.",
            )
            _assert_not_cancelled(log)
            product_enrichment_by_id: dict[int, dict[str, Any]] = {}
            enrichment_warnings: list[str] = []
            try:
                product_enrichment_by_id, enrichment_warnings = fetch_product_enrichment_payloads(
                    products=products,
                    stats_date=dates.stats_date,
                    period_start=period_start,
                    period_end=period_end,
                    max_workers=1,
                    analytics_client=analytics_client,
                    feedbacks_client=feedbacks_client,
                    skip_analytics=_family_limited(API_FAMILY_ANALYTICS),
                    skip_feedbacks=_family_limited(API_FAMILY_FEEDBACKS),
                )
                optional_errors.extend(enrichment_warnings)
                for warning in enrichment_warnings:
                    if warning.startswith("organic_keywords:") and is_wb_rate_limit_error(warning):
                        _record_rate_limit(API_FAMILY_ANALYTICS, "organic_keywords", warning)
                    if warning.startswith("feedbacks:") and is_wb_rate_limit_error(warning):
                        _record_rate_limit(API_FAMILY_FEEDBACKS, "feedbacks", warning)
            except WBApiError as exc:
                err_str = str(exc).lower()
                if is_wb_rate_limit_error(err_str):
                    _sync_console(f"ПРЕДУПРЕЖДЕНИЕ: WB API rate limit (429/461) на обзоре по товарам. Пропускаем.")
                    _record_rate_limit(API_FAMILY_ANALYTICS, "product_enrichment", exc)
                else:
                    raise
            for product in products:
                _assert_not_cancelled(log)
                enrichment_payload = product_enrichment_by_id.get(product.id) or {}
                keywords = enrichment_payload.get("keywords") or []
                organic_keyword_payload = enrichment_payload.get("organic_keyword_payload")
                negative_feedback_count = enrichment_payload.get("negative_feedback_count")

                for stats_date in sorted(allowed_stats_dates):
                    if keywords:
                        upsert_keyword_stats(
                            product=product,
                            stats_date=stats_date,
                            keywords=keywords,
                            organic_payload=organic_keyword_payload,
                            boosted_stats=boosted_keyword_stats_by_date.get(stats_date, {}).get(product.id, {}),
                            overwrite=overwrite,
                        )

                    supplier_lookup = supplier_orders_lookup_by_date.get(stats_date, {})
                    if not supplier_lookup and not supplier_orders_lookup_by_date:
                        supplier_lookup = undated_supplier_orders_lookup
                    supplier_order_rows = supplier_lookup.get(product.nm_id) or []
                    supplier_order_summary = summarize_supplier_orders(supplier_order_rows) if supplier_order_rows else None

                    upsert_product_note(
                        product=product,
                        stats_date=stats_date,
                        overwrite=overwrite,
                        supplier_order_summary=supplier_order_summary,
                        price_summary=price_lookup.get(product.nm_id),
                        negative_feedback_count=negative_feedback_count if stats_date == dates.stats_date else None,
                        enabled_groups=enabled_groups_by_date_product.get(stats_date, {}).get(product.id, set()),
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
        success_detail = "Синхронизация завершена успешно."
        success_stage = "Завершено"
        if limited_api_families:
            success_stage = "Завершено частично"
            skipped_note = f" Не обновились: {', '.join(skipped_sections)}." if skipped_sections else ""
            success_detail = (
                "Синхронизация завершена частично: часть WB API была ограничена 429/461, "
                "поэтому эти разделы пропущены без повторного спама запросами."
                f"{skipped_note}"
            )
        final_payload = {
            **(log.payload or {}),
            "progress": {
                "percent": 100,
                "stage": success_stage,
                "detail": success_detail,
                "updated_at": timezone.now().isoformat(),
            },
        }
        if optional_errors:
            final_payload["warnings"] = optional_errors
        if skipped_sections:
            final_payload["skipped_sections"] = skipped_sections
        if limited_api_families:
            final_payload["wb_limited_families"] = sorted(limited_api_families)
            final_payload["partial_sync"] = True
        log.payload = final_payload
        optional_note = f" Предупреждений: {len(optional_errors)}." if optional_errors else ""
        log.message = f"{success_detail}{optional_note}".strip()
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
