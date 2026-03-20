from __future__ import annotations

from collections import Counter, defaultdict
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
    DailyProductKeywordStat,
    DailyProductMetrics,
    DailyProductNote,
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


@dataclass
class SyncDates:
    stats_date: date
    stock_date: date


def resolve_sync_dates(reference_date: date | None = None) -> SyncDates:
    current = reference_date or timezone.localdate()
    return SyncDates(stats_date=current, stock_date=current)


def batched(items: list[Any], size: int) -> list[list[Any]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


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
    linked_product_ids = set(campaign.products.values_list("id", flat=True))
    tracked_product_ids = [product.id for product in product_map.values()]
    for day_payload in stat_payload.get("days", []):
        stats_date = datetime.fromisoformat(day_payload.get("date", "").replace("Z", "+00:00")).date()
        if overwrite and tracked_product_ids:
            DailyCampaignProductStat.objects.filter(
                campaign=campaign,
                stats_date=stats_date,
                product_id__in=tracked_product_ids,
            ).delete()
        for app_payload in day_payload.get("apps", []):
            zone = map_app_type_to_zone(app_payload.get("appType"))
            for item in app_payload.get("nms", []):
                product = product_map.get(item.get("nmId"))
                if not product:
                    continue
                if linked_product_ids and product.id not in linked_product_ids:
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
        organic_date_item = next(iter(organic_item.get("dateItems") or []), {})
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
    note_defaults: dict[str, Any] = {}
    if supplier_order_summary:
        note_defaults["spp_percent"] = supplier_order_summary.get("spp_percent", Decimal("0"))
        if supplier_order_summary.get("seller_price"):
            note_defaults["seller_price"] = supplier_order_summary["seller_price"]
        if supplier_order_summary.get("wb_price"):
            note_defaults["wb_price"] = supplier_order_summary["wb_price"]
    if price_summary:
        if not note_defaults.get("seller_price") and price_summary.get("seller_price"):
            note_defaults["seller_price"] = price_summary["seller_price"]
        if not note_defaults.get("wb_price") and price_summary.get("wb_price"):
            note_defaults["wb_price"] = price_summary["wb_price"]
    if negative_feedback_count is not None:
        note_defaults["negative_feedback"] = str(negative_feedback_count)

    active_groups = enabled_groups or set()
    note_defaults["unified_enabled"] = CampaignMonitoringGroup.UNIFIED in active_groups
    note_defaults["manual_search_enabled"] = CampaignMonitoringGroup.MANUAL_SEARCH in active_groups
    note_defaults["manual_shelves_enabled"] = CampaignMonitoringGroup.MANUAL_SHELVES in active_groups

    previous_note = (
        DailyProductNote.objects.filter(product=product, note_date__lt=stats_date)
        .order_by("-note_date")
        .first()
    )
    current_seller_price = decimalize(note_defaults.get("seller_price"))
    previous_seller_price = decimalize(previous_note.seller_price if previous_note else 0)
    note_defaults["price_changed"] = bool(previous_note and current_seller_price and current_seller_price != previous_seller_price)

    if overwrite:
        DailyProductNote.objects.update_or_create(
            product=product,
            note_date=stats_date,
            defaults=note_defaults,
        )
    else:
        DailyProductNote.objects.get_or_create(
            product=product,
            note_date=stats_date,
            defaults=note_defaults,
        )


def run_sync(*, product_ids: list[int] | None = None, reference_date: date | None = None, overwrite: bool = True, kind: str = SyncKind.FULL) -> SyncLog:
    dates = resolve_sync_dates(reference_date)
    runtime_settings = get_monitoring_settings()
    log = SyncLog.objects.create(
        kind=kind,
        status=SyncStatus.RUNNING,
        target_date=dates.stats_date,
        payload={
            "stats_date": dates.stats_date.isoformat(),
            "stock_date": dates.stock_date.isoformat(),
            "product_ids": product_ids or [],
        },
    )
    try:
        analytics_client = AnalyticsWBClient()
        promotion_client = PromotionWBClient()
        statistics_client = StatisticsWBClient()
        prices_client = PricesWBClient()
        feedbacks_client = FeedbacksWBClient()
        queryset = Product.objects.filter(is_active=True)
        if product_ids:
            queryset = queryset.filter(id__in=product_ids)
        products = list(queryset.order_by("id"))
        if not products:
            raise SyncServiceError("Нет активных товаров для синхронизации.")

        optional_errors: list[str] = []
        product_map = {product.nm_id: product for product in products}
        campaigns = list(Campaign.objects.filter(is_active=True, products__in=products).distinct())
        campaign_map = {campaign.external_id: campaign for campaign in campaigns}

        with transaction.atomic():
            funnel_rows = analytics_client.get_sales_funnel_history(
                nm_ids=list(product_map.keys()),
                start_date=dates.stats_date,
                end_date=dates.stats_date,
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
                    start_date=dates.stats_date,
                    end_date=dates.stats_date,
                )
                for item in stats_payload:
                    upsert_campaign_stats(
                        product_map=product_map,
                        campaign_map=campaign_map,
                        stat_payload=item,
                        overwrite=overwrite,
                    )

            price_lookup: dict[int, dict[str, Decimal]] = {}
            try:
                for nm_chunk in batched(list(product_map.keys()), 1000):
                    if not nm_chunk:
                        continue
                    price_lookup.update(build_price_lookup(prices_client.get_goods_prices(nm_ids=nm_chunk)))
            except WBApiError as exc:
                optional_errors.append(f"prices: {exc}")

            supplier_orders_lookup: dict[int, list[dict[str, Any]]] = {}
            try:
                supplier_orders_lookup = build_supplier_orders_lookup(
                    statistics_client.get_supplier_orders(date_from=dates.stats_date)
                )
            except WBApiError as exc:
                optional_errors.append(f"supplier_orders: {exc}")

            boosted_keyword_aggregates: dict[int, dict[str, dict[str, Any]]] = defaultdict(dict)
            if campaigns:
                campaign_pairs = list(
                    ProductCampaign.objects.filter(campaign__in=campaigns, product__in=products).select_related("campaign", "product")
                )
                normquery_items = [
                    {"advertId": pair.campaign.external_id, "nmId": pair.product.nm_id}
                    for pair in campaign_pairs
                ]
                try:
                    for item_chunk in batched(normquery_items, 100):
                        if not item_chunk:
                            continue
                        payload = promotion_client.get_daily_search_cluster_stats(
                            items=item_chunk,
                            start_date=dates.stats_date,
                            end_date=dates.stats_date,
                        )
                        for item in payload.get("items") or []:
                            product = product_map.get(item.get("nmId"))
                            if not product:
                                continue
                            product_bucket = boosted_keyword_aggregates[product.id]
                            for daily_stat in item.get("dailyStats") or []:
                                stat = daily_stat.get("stat") or {}
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
                                views = int(stat.get("views") or 0)
                                clicks = int(stat.get("clicks") or 0)
                                weight = Decimal(views or 1)
                                entry["weighted_position_sum"] += decimalize(stat.get("avgPos")) * weight
                                entry["weight"] += weight
                                entry["views"] += views
                                entry["clicks"] += clicks
                                entry["raw_payload"].append(stat)
                except WBApiError as exc:
                    optional_errors.append(f"boosted_keywords: {exc}")

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
            for stat in DailyCampaignProductStat.objects.filter(product__in=products, stats_date=dates.stats_date).select_related("campaign"):
                enabled_groups_by_product[stat.product_id].add(stat.campaign.monitoring_group)

            for product in products:
                keywords = list(
                    dict.fromkeys(
                        [item.strip() for item in [product.primary_keyword, product.secondary_keyword] if item.strip()]
                    )
                )
                organic_keyword_payload: dict[str, Any] | None = None
                if keywords:
                    try:
                        organic_keyword_payload = analytics_client.get_search_orders(
                            nm_id=product.nm_id,
                            start_date=dates.stats_date,
                            end_date=dates.stats_date,
                            search_texts=keywords,
                        )
                    except WBApiError as exc:
                        optional_errors.append(f"organic_keywords:{product.nm_id}: {exc}")
                    upsert_keyword_stats(
                        product=product,
                        stats_date=dates.stats_date,
                        keywords=keywords,
                        organic_payload=organic_keyword_payload,
                        boosted_stats=boosted_keyword_stats.get(product.id, {}),
                        overwrite=overwrite,
                    )

                negative_feedback_count: int | None = None
                try:
                    negative_feedback_count = fetch_negative_feedback_count(
                        feedback_client=feedbacks_client,
                        product=product,
                        stats_date=dates.stats_date,
                    )
                except WBApiError as exc:
                    optional_errors.append(f"feedbacks:{product.nm_id}: {exc}")

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
        if optional_errors:
            log.payload = {
                **log.payload,
                "warnings": optional_errors,
            }
        optional_note = f" Предупреждений: {len(optional_errors)}." if optional_errors else ""
        log.message = f"Синхронизация завершена.{optional_note}{google_sync_note}".strip()
        log.save(update_fields=["status", "finished_at", "message", "payload", "updated_at"])
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
