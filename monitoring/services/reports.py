from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from functools import lru_cache
from typing import Any

from django.conf import settings
from django.db.models import Avg, Count, Max, Sum
from django.utils import timezone

from monitoring.models import (
    CampaignMonitoringGroup,
    CampaignZone,
    DailyCampaignProductStat,
    DailyCampaignSearchClusterStat,
    DailyProductKeywordStat,
    DailyProductMetrics,
    DailyProductNote,
    DailyProductStock,
    DailyWarehouseStock,
    Product,
    ProductEconomicsVersion,
    ProductCampaign,
    SyncLog,
)

ZERO = Decimal("0")
ONE = Decimal("1")
ONE_HUNDRED = Decimal("100")


def decimalize(value: Any) -> Decimal:
    if value in (None, ""):
        return ZERO
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def percent_points(value: Any) -> Decimal:
    number = decimalize(value)
    if number == ZERO:
        return ZERO
    if -ONE <= number <= ONE:
        return number * ONE_HUNDRED
    return number


def percent_fraction(value: Any) -> Decimal:
    return safe_divide(percent_points(value), ONE_HUNDRED)


def safe_divide(numerator: Decimal | int | float, denominator: Decimal | int | float) -> Decimal:
    denominator_value = decimalize(denominator)
    if denominator_value == 0:
        return ZERO
    return decimalize(numerator) / denominator_value


def quantize_money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"))


def normalize_search_text(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


@lru_cache(maxsize=1)
def parse_zone_map() -> dict[int, str]:
    mapping: dict[int, str] = {}
    for chunk in settings.WB_APP_TYPE_ZONE_MAP.split(","):
        if ":" not in chunk:
            continue
        key, value = chunk.split(":", 1)
        try:
            mapping[int(key.strip())] = value.strip()
        except ValueError:
            continue
    return mapping or {1: CampaignZone.RECOMMENDATION, 32: CampaignZone.SEARCH, 64: CampaignZone.CATALOG}


def map_app_type_to_zone(app_type: int | None) -> str:
    app_type = app_type or 0
    return parse_zone_map().get(app_type, CampaignZone.UNKNOWN)


@dataclass
class MetricCell:
    impressions: int = 0
    clicks: int = 0
    spend: Decimal = ZERO
    carts: int = 0
    orders: int = 0
    order_sum: Decimal = ZERO
    units: int = 0

    def add(self, stat: DailyCampaignProductStat) -> None:
        self.impressions += stat.impressions
        self.clicks += stat.clicks
        self.spend += decimalize(stat.spend)
        self.carts += stat.add_to_cart_count
        self.orders += stat.order_count
        self.order_sum += decimalize(stat.order_sum)
        self.units += stat.units_ordered

    @property
    def ctr(self) -> Decimal:
        return safe_divide(self.clicks * 100, self.impressions)

    @property
    def cpc(self) -> Decimal:
        return safe_divide(self.spend, self.clicks)

    @property
    def cpm(self) -> Decimal:
        return safe_divide(self.spend * 1000, self.impressions)

    @property
    def order_cost(self) -> Decimal:
        return safe_divide(self.spend, self.orders)

    @property
    def cart_cost(self) -> Decimal:
        return safe_divide(self.spend, self.carts)

    def traffic_share(self, total_impressions: int) -> Decimal:
        return safe_divide(self.impressions * 100, total_impressions)


def clone_metric_cell(cell: MetricCell) -> MetricCell:
    return MetricCell(
        impressions=cell.impressions,
        clicks=cell.clicks,
        spend=decimalize(cell.spend),
        carts=cell.carts,
        orders=cell.orders,
        order_sum=decimalize(cell.order_sum),
        units=cell.units,
    )


def metric_cell_from_search_clusters(cluster_rows: list[DailyCampaignSearchClusterStat]) -> MetricCell:
    cell = MetricCell()
    for row in cluster_rows:
        cell.impressions += int(row.impressions or 0)
        cell.clicks += int(row.clicks or 0)
        cell.spend += decimalize(row.spend)
        cell.carts += int(row.add_to_cart_count or 0)
        cell.orders += int(row.order_count or 0)
        cell.units += int(row.units_ordered or 0)
    return cell


def clamp_metric_cell_to_total(cell: MetricCell, total: MetricCell) -> MetricCell:
    clamped = clone_metric_cell(cell)
    clamped.impressions = max(0, min(clamped.impressions, total.impressions))
    clamped.clicks = max(0, min(clamped.clicks, total.clicks))
    clamped.spend = max(ZERO, min(decimalize(clamped.spend), decimalize(total.spend)))
    clamped.carts = max(0, min(clamped.carts, total.carts))
    clamped.orders = max(0, min(clamped.orders, total.orders))
    clamped.units = max(0, min(clamped.units, total.units))
    clamped.order_sum = max(ZERO, min(decimalize(clamped.order_sum), decimalize(total.order_sum)))
    return clamped


def subtract_metric_cells(total: MetricCell, part: MetricCell) -> MetricCell:
    return MetricCell(
        impressions=max(total.impressions - part.impressions, 0),
        clicks=max(total.clicks - part.clicks, 0),
        spend=max(decimalize(total.spend) - decimalize(part.spend), ZERO),
        carts=max(total.carts - part.carts, 0),
        orders=max(total.orders - part.orders, 0),
        order_sum=max(decimalize(total.order_sum) - decimalize(part.order_sum), ZERO),
        units=max(total.units - part.units, 0),
    )


@dataclass(frozen=True)
class ResolvedEconomics:
    effective_from: date | None
    buyout_percent: Decimal
    unit_cost: Decimal
    logistics_cost: Decimal


def normalize_warehouse_name(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


def resolve_product_economics(product: Product, effective_date: date) -> ResolvedEconomics:
    snapshot = (
        ProductEconomicsVersion.objects.filter(product=product, effective_from__lte=effective_date)
        .order_by("-effective_from", "-id")
        .first()
    )
    if snapshot:
        return ResolvedEconomics(
            effective_from=snapshot.effective_from,
            buyout_percent=decimalize(snapshot.buyout_percent),
            unit_cost=decimalize(snapshot.unit_cost),
            logistics_cost=decimalize(snapshot.logistics_cost),
        )
    return ResolvedEconomics(
        effective_from=None,
        buyout_percent=decimalize(product.buyout_percent),
        unit_cost=decimalize(product.unit_cost),
        logistics_cost=decimalize(product.logistics_cost),
    )


def get_default_dates(product: Product | None = None) -> tuple[date, date]:
    metrics_qs = DailyProductMetrics.objects
    stock_qs = DailyProductStock.objects
    if product is not None:
        metrics_qs = metrics_qs.filter(product=product)
        stock_qs = stock_qs.filter(product=product)

    latest_metrics = metrics_qs.aggregate(latest=Max("stats_date"))["latest"]
    latest_stock = stock_qs.aggregate(latest=Max("stats_date"))["latest"]
    today = timezone.localdate()
    return latest_metrics or today, latest_stock or today


def estimate_buyout_sum(product_or_economics: Product | ResolvedEconomics, order_sum: Decimal, effective_date: date | None = None) -> Decimal:
    economics = (
        resolve_product_economics(product_or_economics, effective_date or timezone.localdate())
        if isinstance(product_or_economics, Product)
        else product_or_economics
    )
    return quantize_money(order_sum * percent_fraction(economics.buyout_percent))


def estimate_profit(
    product_or_economics: Product | ResolvedEconomics,
    order_count: int,
    order_sum: Decimal,
    spend: Decimal,
    effective_date: date | None = None,
) -> Decimal:
    economics = (
        resolve_product_economics(product_or_economics, effective_date or timezone.localdate())
        if isinstance(product_or_economics, Product)
        else product_or_economics
    )
    buyout_units = decimalize(order_count) * percent_fraction(economics.buyout_percent)
    estimated_sales = estimate_buyout_sum(economics, order_sum)
    goods_cost = buyout_units * decimalize(economics.unit_cost)
    logistics = buyout_units * decimalize(economics.logistics_cost)
    return quantize_money(estimated_sales - spend - goods_cost - logistics)


def build_dashboard_context(*, stats_date: date | None = None, stock_date: date | None = None) -> dict[str, Any]:
    stats_date = stats_date or get_default_dates()[0]
    stock_date = stock_date or get_default_dates()[1]
    products = list(Product.objects.filter(is_active=True).order_by("title", "nm_id"))
    product_ids = [product.id for product in products]

    metrics_by_product_id = {
        item.product_id: item
        for item in DailyProductMetrics.objects.filter(
            product_id__in=product_ids,
            stats_date=stats_date,
        )
    }
    stocks_by_product_id = {
        item.product_id: item
        for item in DailyProductStock.objects.filter(
            product_id__in=product_ids,
            stats_date=stock_date,
        )
    }
    spend_by_product_id = {
        row["product_id"]: quantize_money(decimalize(row["spend"]))
        for row in DailyCampaignProductStat.objects.filter(
            product_id__in=product_ids,
            stats_date=stats_date,
        )
        .values("product_id")
        .annotate(spend=Sum("spend"))
    }
    campaigns_count_by_product_id = {
        row["product_id"]: int(row["campaigns_count"] or 0)
        for row in ProductCampaign.objects.filter(
            product_id__in=product_ids,
            campaign__is_active=True,
        )
        .values("product_id")
        .annotate(campaigns_count=Count("campaign_id", distinct=True))
    }

    cards: list[dict[str, Any]] = []
    total_orders = 0
    total_order_sum = ZERO
    dashboard_spend = ZERO
    total_stock = 0
    total_campaigns = 0

    for product in products:
        metrics = metrics_by_product_id.get(product.id)
        stock = stocks_by_product_id.get(product.id)
        product_spend = spend_by_product_id.get(product.id, ZERO)
        campaigns_count = campaigns_count_by_product_id.get(product.id, 0)
        cards.append(
            {
                "product": product,
                "metrics": metrics,
                "stock": stock,
                "total_spend": quantize_money(product_spend),
                "campaigns_count": campaigns_count,
            }
        )
        total_orders += metrics.order_count if metrics else 0
        total_order_sum += decimalize(metrics.order_sum if metrics else ZERO)
        total_stock += stock.total_stock if stock else 0
        total_campaigns += campaigns_count
        dashboard_spend += product_spend

    return {
        "cards": cards,
        "stats_date": stats_date,
        "stock_date": stock_date,
        "totals": {
            "products": len(products),
            "orders": total_orders,
            "order_sum": quantize_money(total_order_sum),
            "spend": quantize_money(dashboard_spend),
            "stock": total_stock,
            "campaigns": total_campaigns,
        },
        "sync_logs": SyncLog.objects.all()[:10],
    }


def build_product_report(
    *,
    product: Product,
    stats_date: date | None = None,
    stock_date: date | None = None,
    create_note: bool = True,
    preloaded_metrics: dict[date, DailyProductMetrics] | None = None,
    preloaded_stocks: dict[date, DailyProductStock] | None = None,
    preloaded_notes: dict[date, DailyProductNote] | None = None,
    preloaded_warehouse_rows: dict[date, list[DailyWarehouseStock]] | None = None,
    preloaded_campaign_stats: dict[date, list[DailyCampaignProductStat]] | None = None,
    preloaded_keyword_stats: dict[date, list[DailyProductKeywordStat]] | None = None,
    preloaded_search_cluster_stats: dict[date, list[DailyCampaignSearchClusterStat]] | None = None,
    preloaded_economics: dict[date, ResolvedEconomics] | None = None,
    preloaded_visible_warehouse_names: set[str] | None = None,
    preloaded_active_campaign_exists: bool | None = None,
    preloaded_history: list[DailyProductMetrics] | None = None,
    preloaded_rolling_avg_orders: Decimal | int | float | None = None,
) -> dict[str, Any]:
    stats_date = stats_date or get_default_dates(product)[0]
    stock_date = stock_date or get_default_dates(product)[1]
    economics = (
        preloaded_economics.get(stock_date)
        if preloaded_economics is not None
        else resolve_product_economics(product, stock_date)
    ) or resolve_product_economics(product, stock_date)
    metrics = (
        preloaded_metrics.get(stats_date)
        if preloaded_metrics is not None
        else product.daily_metrics.filter(stats_date=stats_date).first()
    )
    stock = (
        preloaded_stocks.get(stock_date)
        if preloaded_stocks is not None
        else product.daily_stocks.filter(stats_date=stock_date).first()
    )
    if create_note:
        if preloaded_notes is not None and stats_date in preloaded_notes:
            daily_note = preloaded_notes[stats_date]
        else:
            daily_note, _ = DailyProductNote.objects.get_or_create(product=product, note_date=stats_date)
    else:
        daily_note = (
            preloaded_notes.get(stats_date) if preloaded_notes is not None else None
        ) or DailyProductNote.objects.filter(product=product, note_date=stats_date).first() or DailyProductNote(
            product=product, note_date=stats_date
        )
    preferred_warehouse_names = (
        set(preloaded_visible_warehouse_names)
        if preloaded_visible_warehouse_names is not None
        else {normalize_warehouse_name(item) for item in product.visible_warehouse_names()}
    )
    warehouse_rows = (
        list(preloaded_warehouse_rows.get(stock_date) or [])
        if preloaded_warehouse_rows is not None
        else list(
            DailyWarehouseStock.objects.filter(
                product=product,
                stats_date=stock_date,
            ).select_related("warehouse")
        )
    )
    if preferred_warehouse_names:
        warehouse_rows = [
            row for row in warehouse_rows if normalize_warehouse_name(row.warehouse.name) in preferred_warehouse_names
        ]
    else:
        warehouse_rows = [row for row in warehouse_rows if row.warehouse.is_visible_in_monitoring]
    campaign_stats = (
        list(preloaded_campaign_stats.get(stats_date) or [])
        if preloaded_campaign_stats is not None
        else list(
            DailyCampaignProductStat.objects.filter(
                product=product,
                stats_date=stats_date,
                campaign__products=product,
            )
            .select_related("campaign")
            .order_by("campaign__monitoring_group", "zone")
        )
    )
    keyword_stats = (
        list(preloaded_keyword_stats.get(stats_date) or [])
        if preloaded_keyword_stats is not None
        else list(
            DailyProductKeywordStat.objects.filter(
                product=product,
                stats_date=stats_date,
            ).order_by("query_text")
        )
    )

    total_ad = MetricCell()
    for stat in campaign_stats:
        total_ad.add(stat)

    legacy_cells: dict[tuple[str, str], MetricCell] = {}
    for stat in campaign_stats:
        key = (stat.campaign.monitoring_group, stat.zone)
        legacy_cells.setdefault(key, MetricCell()).add(stat)

    search_cluster_stats = (
        list(preloaded_search_cluster_stats.get(stats_date) or [])
        if preloaded_search_cluster_stats is not None
        else list(
            DailyCampaignSearchClusterStat.objects.filter(
                product=product,
                stats_date=stats_date,
                campaign__products=product,
            ).select_related("campaign")
        )
    )
    search_clusters_by_group: dict[str, list[DailyCampaignSearchClusterStat]] = defaultdict(list)
    for row in search_cluster_stats:
        search_clusters_by_group[row.campaign.monitoring_group].append(row)

    group_totals: dict[str, MetricCell] = defaultdict(MetricCell)
    for stat in campaign_stats:
        group_totals[stat.campaign.monitoring_group].add(stat)

    def resolve_group_cells(group: str) -> tuple[MetricCell, MetricCell, MetricCell]:
        cluster_rows = search_clusters_by_group.get(group, [])
        if not cluster_rows:
            return (
                legacy_cells.get((group, CampaignZone.SEARCH), MetricCell()),
                legacy_cells.get((group, CampaignZone.RECOMMENDATION), MetricCell()),
                legacy_cells.get((group, CampaignZone.CATALOG), MetricCell()),
            )
        total = group_totals.get(group, MetricCell())
        legacy_search = legacy_cells.get((group, CampaignZone.SEARCH), MetricCell())
        search = clone_metric_cell(legacy_search)
        search_cluster = metric_cell_from_search_clusters(cluster_rows)
        search.impressions = search_cluster.impressions
        search.clicks = search_cluster.clicks
        search.spend = search_cluster.spend
        search = clamp_metric_cell_to_total(search, total)
        shelves = subtract_metric_cells(total, search)
        return search, shelves, MetricCell()

    cells: dict[tuple[str, str], MetricCell] = {}
    for group in (
        CampaignMonitoringGroup.UNIFIED,
        CampaignMonitoringGroup.MANUAL_SEARCH,
        CampaignMonitoringGroup.MANUAL_SHELVES,
        CampaignMonitoringGroup.MANUAL_CATALOG,
    ):
        search_cell, shelves_cell, catalog_cell = resolve_group_cells(group)
        cells[(group, CampaignZone.SEARCH)] = search_cell
        cells[(group, CampaignZone.RECOMMENDATION)] = shelves_cell
        cells[(group, CampaignZone.CATALOG)] = catalog_cell

    traffic_totals = {
        CampaignMonitoringGroup.UNIFIED: sum(
            cell.impressions for key, cell in cells.items() if key[0] == CampaignMonitoringGroup.UNIFIED
        ),
        CampaignMonitoringGroup.MANUAL_SEARCH: sum(
            cell.impressions for key, cell in cells.items() if key[0] == CampaignMonitoringGroup.MANUAL_SEARCH
        ),
        CampaignMonitoringGroup.MANUAL_SHELVES: sum(
            cell.impressions for key, cell in cells.items() if key[0] == CampaignMonitoringGroup.MANUAL_SHELVES
        ),
        CampaignMonitoringGroup.MANUAL_CATALOG: sum(
            cell.impressions for key, cell in cells.items() if key[0] == CampaignMonitoringGroup.MANUAL_CATALOG
        ),
    }
    blocks = {
        "unified_search": cells.get((CampaignMonitoringGroup.UNIFIED, CampaignZone.SEARCH), MetricCell()),
        "unified_shelves": cells.get((CampaignMonitoringGroup.UNIFIED, CampaignZone.RECOMMENDATION), MetricCell()),
        "unified_catalog": cells.get((CampaignMonitoringGroup.UNIFIED, CampaignZone.CATALOG), MetricCell()),
        "manual_search": cells.get((CampaignMonitoringGroup.MANUAL_SEARCH, CampaignZone.SEARCH), MetricCell()),
        "manual_shelves": cells.get((CampaignMonitoringGroup.MANUAL_SHELVES, CampaignZone.RECOMMENDATION), MetricCell()),
        "manual_catalog": cells.get((CampaignMonitoringGroup.MANUAL_CATALOG, CampaignZone.CATALOG), MetricCell()),
    }

    overall_open = metrics.open_count if metrics else 0
    overall_carts = metrics.add_to_cart_count if metrics else 0
    overall_orders = metrics.order_count if metrics else 0
    overall_sum = decimalize(metrics.order_sum if metrics else ZERO)

    organic = {
        "open_count": max(overall_open - total_ad.clicks, 0),
        "cart_count": max(overall_carts - total_ad.carts, 0),
        "order_count": max(overall_orders - total_ad.orders, 0),
        "order_sum": quantize_money(max(overall_sum - total_ad.order_sum, ZERO)),
    }
    traffic_cards = [
        {
            "label": "Единая · Поиск",
            "cell": blocks["unified_search"],
            "share": blocks["unified_search"].traffic_share(traffic_totals[CampaignMonitoringGroup.UNIFIED]),
        },
        {
            "label": "Единая · Полки",
            "cell": blocks["unified_shelves"],
            "share": blocks["unified_shelves"].traffic_share(traffic_totals[CampaignMonitoringGroup.UNIFIED]),
        },
        {
            "label": "Единая · Каталог",
            "cell": blocks["unified_catalog"],
            "share": blocks["unified_catalog"].traffic_share(traffic_totals[CampaignMonitoringGroup.UNIFIED]),
        },
        {
            "label": "Руч. поиск",
            "cell": blocks["manual_search"],
            "share": blocks["manual_search"].traffic_share(traffic_totals[CampaignMonitoringGroup.MANUAL_SEARCH]),
        },
        {
            "label": "Руч. полки",
            "cell": blocks["manual_shelves"],
            "share": blocks["manual_shelves"].traffic_share(traffic_totals[CampaignMonitoringGroup.MANUAL_SHELVES]),
        },
    ]
    if any(
        [
            blocks["manual_catalog"].impressions,
            blocks["manual_catalog"].clicks,
            blocks["manual_catalog"].orders,
            blocks["manual_catalog"].spend,
        ]
    ):
        traffic_cards.append(
            {
                "label": "Руч. каталог",
                "cell": blocks["manual_catalog"],
                "share": blocks["manual_catalog"].traffic_share(traffic_totals[CampaignMonitoringGroup.MANUAL_CATALOG]),
            }
        )
    insights = {
        "ad_orders_share": safe_divide(total_ad.orders * 100, overall_orders),
        "organic_orders_share": safe_divide(organic["order_count"] * 100, overall_orders),
        "ad_revenue_share": safe_divide(total_ad.order_sum * 100, overall_sum),
        "organic_revenue_share": safe_divide(organic["order_sum"] * 100, overall_sum),
        "avg_order_value": safe_divide(overall_sum, overall_orders),
        "avg_spend_per_click": total_ad.cpc,
    }

    alerts: list[dict[str, str]] = []
    if metrics is None:
        alerts.append(
            {
                "tone": "warning",
                "title": "Нет общей воронки за выбранную дату",
                "detail": "По этой дате в базе пока нет переходов, корзин и заказов из Sales Funnel.",
            }
        )
    if stock is None:
        alerts.append(
            {
                "tone": "warning",
                "title": "Нет среза остатков за выбранную дату",
                "detail": "Складские остатки для этой даты ещё не были собраны или были очищены.",
            }
        )
    active_campaign_exists = (
        bool(preloaded_active_campaign_exists)
        if preloaded_active_campaign_exists is not None
        else product.campaigns.filter(is_active=True).exists()
    )
    if not campaign_stats and active_campaign_exists:
        alerts.append(
            {
                "tone": "warning",
                "title": "Нет рекламного среза по привязанным РК",
                "detail": "Кампании у товара есть, но по выбранной дате статистика не сохранена.",
            }
        )
    if any(
        [
            blocks["manual_catalog"].impressions,
            blocks["manual_catalog"].clicks,
            blocks["manual_catalog"].orders,
            blocks["manual_catalog"].spend,
        ]
    ):
        alerts.append(
            {
                "tone": "info",
                "title": "Есть данные по ручному каталогу",
                "detail": "Они включены в общие итоги и быстрый срез по зонам, но в шаблонной матрице не выделены отдельной колонкой.",
            }
        )

    history = (
        list(preloaded_history)
        if preloaded_history is not None
        else list(product.daily_metrics.order_by("-stats_date")[:14])
    )
    rolling_avg_orders_per_day = (
        preloaded_rolling_avg_orders
        if preloaded_rolling_avg_orders is not None
        else product.daily_metrics.order_by("-stats_date")[:7].aggregate(avg=Avg("order_count"))["avg"] or 0
    )
    avg_orders_per_day = decimalize(stock.avg_orders_per_day if stock else 0) or decimalize(rolling_avg_orders_per_day)
    days_until_zero = decimalize(stock.days_until_zero if stock else 0)
    if days_until_zero == 0 and avg_orders_per_day:
        days_until_zero = safe_divide(stock.total_stock if stock else 0, avg_orders_per_day)

    keyword_stats_map = {
        normalize_search_text(item.query_text): item
        for item in keyword_stats
    }
    keyword_rows: list[dict[str, Any]] = []
    for query_text in [product.primary_keyword or "", product.secondary_keyword or ""]:
        normalized_query = normalize_search_text(query_text)
        keyword_stat = keyword_stats_map.get(normalized_query)
        keyword_rows.append(
            {
                "query_text": query_text,
                "has_data": keyword_stat is not None,
                "frequency": keyword_stat.frequency if keyword_stat else None,
                "organic_position": keyword_stat.organic_position if keyword_stat else None,
                "boosted_position": keyword_stat.boosted_position if keyword_stat else None,
                "boosted_ctr": keyword_stat.boosted_ctr if keyword_stat else None,
            }
        )

    report = {
        "product": product,
        "stats_date": stats_date,
        "stock_date": stock_date,
        "metrics": metrics,
        "stock": stock,
        "note": daily_note,
        "economics": economics,
        "warehouse_rows": warehouse_rows,
        "history": history,
        "keyword_rows": keyword_rows,
        "cells": cells,
        "blocks": blocks,
        "total_ad": total_ad,
        "organic": organic,
        "traffic_cards": traffic_cards,
        "insights": insights,
        "avg_orders_per_day": avg_orders_per_day.quantize(Decimal("0.01")) if avg_orders_per_day else ZERO,
        "days_until_zero": days_until_zero.quantize(Decimal("0.01")) if days_until_zero else ZERO,
        "traffic_totals": traffic_totals,
        "alerts": alerts,
        "campaign_stats_count": len(campaign_stats),
    }

    return report
