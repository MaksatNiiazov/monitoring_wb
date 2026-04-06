from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal

from monitoring.models import Campaign, CampaignZone, DailyCampaignProductStat, DailyCampaignSearchClusterStat, Product
from monitoring.services.reports import MetricCell, decimalize, metric_cell_from_search_clusters, quantize_money, safe_divide

ZERO = Decimal("0")

ZONE_ORDER = [
    CampaignZone.SEARCH,
    CampaignZone.RECOMMENDATION,
    CampaignZone.CATALOG,
    CampaignZone.UNKNOWN,
]
ZONE_LABELS = dict(CampaignZone.choices)


def _float(value: Decimal | int | float) -> float:
    return float(decimalize(value).quantize(Decimal("0.01")))


def _money(value: Decimal | int | float) -> Decimal:
    return quantize_money(decimalize(value))


def _has_metric_values(cell: MetricCell) -> bool:
    return any(
        [
            cell.impressions,
            cell.clicks,
            cell.carts,
            cell.orders,
            cell.units,
            decimalize(cell.spend),
            decimalize(cell.order_sum),
        ]
    )


def _metric_ratio(numerator: Decimal | int | float, denominator: Decimal | int | float) -> Decimal:
    return safe_divide(decimalize(numerator) * Decimal("100"), denominator)


def _timeline_dataset(series_points: list[dict[str, object]]) -> dict[str, object]:
    return {
        "defaultMetric": "spend",
        "defaultType": "line",
        "labels": [point["label"] for point in series_points],
        "series": {
            "spend": {
                "label": "Расход",
                "format": "money",
                "values": [point["spend"] for point in series_points],
            },
            "orders": {
                "label": "Заказы",
                "format": "int",
                "values": [point["orders"] for point in series_points],
            },
            "revenue": {
                "label": "Сумма заказов",
                "format": "money",
                "values": [point["revenue"] for point in series_points],
            },
            "impressions": {
                "label": "Показы",
                "format": "int",
                "values": [point["impressions"] for point in series_points],
            },
            "clicks": {
                "label": "Клики",
                "format": "int",
                "values": [point["clicks"] for point in series_points],
            },
            "ctr": {
                "label": "CTR",
                "format": "percent",
                "values": [point["ctr"] for point in series_points],
            },
            "cpc": {
                "label": "CPC",
                "format": "money",
                "values": [point["cpc"] for point in series_points],
            },
            "drr": {
                "label": "ДРР",
                "format": "percent",
                "values": [point["drr"] for point in series_points],
            },
        },
    }


def _format_placements(placements: dict) -> list[str]:
    if not isinstance(placements, dict):
        return []
    result: list[str] = []
    for key, value in placements.items():
        key_label = str(key).replace("_", " ").strip() or "placement"
        if isinstance(value, bool):
            if value:
                result.append(key_label)
            continue
        if value in (None, "", [], {}):
            continue
        if isinstance(value, list):
            rendered = ", ".join(str(item) for item in value if item not in (None, ""))
            if rendered:
                result.append(f"{key_label}: {rendered}")
            continue
        if isinstance(value, dict):
            rendered = ", ".join(f"{inner_key}={inner_value}" for inner_key, inner_value in value.items())
            if rendered:
                result.append(f"{key_label}: {rendered}")
            continue
        result.append(f"{key_label}: {value}")
    return result


def build_campaign_detail_context(*, campaign: Campaign, date_from: date, date_to: date) -> dict[str, object]:
    linked_products = list(campaign.products.order_by("title", "nm_id"))
    stats_rows = list(
        DailyCampaignProductStat.objects.filter(
            campaign=campaign,
            stats_date__range=(date_from, date_to),
        )
        .select_related("product")
        .order_by("stats_date", "product__title", "zone")
    )
    cluster_rows = list(
        DailyCampaignSearchClusterStat.objects.filter(
            campaign=campaign,
            stats_date__range=(date_from, date_to),
        )
        .select_related("product")
        .order_by("stats_date", "product__title")
    )

    product_map: dict[int, Product] = {product.id: product for product in linked_products}
    for row in stats_rows:
        product_map.setdefault(row.product_id, row.product)
    for row in cluster_rows:
        product_map.setdefault(row.product_id, row.product)

    total_cell = MetricCell()
    total_cluster_cell = metric_cell_from_search_clusters(cluster_rows)
    day_cells: dict[date, MetricCell] = defaultdict(MetricCell)
    day_cluster_cells: dict[date, MetricCell] = defaultdict(MetricCell)
    zone_cells: dict[str, MetricCell] = defaultdict(MetricCell)
    product_cells: dict[int, MetricCell] = defaultdict(MetricCell)
    product_zone_cells: dict[int, dict[str, MetricCell]] = defaultdict(lambda: defaultdict(MetricCell))
    product_cluster_cells: dict[int, MetricCell] = defaultdict(MetricCell)

    for row in stats_rows:
        total_cell.add(row)
        day_cells[row.stats_date].add(row)
        zone_cells[row.zone].add(row)
        product_cells[row.product_id].add(row)
        product_zone_cells[row.product_id][row.zone].add(row)

    for row in cluster_rows:
        day_cluster_cells[row.stats_date].impressions += int(row.impressions or 0)
        day_cluster_cells[row.stats_date].clicks += int(row.clicks or 0)
        day_cluster_cells[row.stats_date].spend += decimalize(row.spend)
        day_cluster_cells[row.stats_date].carts += int(row.add_to_cart_count or 0)
        day_cluster_cells[row.stats_date].orders += int(row.order_count or 0)
        day_cluster_cells[row.stats_date].units += int(row.units_ordered or 0)

        product_cluster_cells[row.product_id].impressions += int(row.impressions or 0)
        product_cluster_cells[row.product_id].clicks += int(row.clicks or 0)
        product_cluster_cells[row.product_id].spend += decimalize(row.spend)
        product_cluster_cells[row.product_id].carts += int(row.add_to_cart_count or 0)
        product_cluster_cells[row.product_id].orders += int(row.order_count or 0)
        product_cluster_cells[row.product_id].units += int(row.units_ordered or 0)

    total_days = max((date_to - date_from).days + 1, 1)
    all_dates = [date_from + timedelta(days=offset) for offset in range(total_days)]

    daily_rows: list[dict[str, object]] = []
    timeline_points: list[dict[str, object]] = []
    for stats_date in all_dates:
        cell = day_cells.get(stats_date, MetricCell())
        cluster_cell = day_cluster_cells.get(stats_date, MetricCell())
        drr = _metric_ratio(cell.spend, cell.order_sum)
        row = {
            "date": stats_date,
            "label": stats_date.strftime("%d.%m"),
            "cell": cell,
            "search_cluster_cell": cluster_cell,
            "ctr": cell.ctr,
            "cpc": cell.cpc,
            "cpo": cell.order_cost,
            "drr": drr,
            "has_stats": _has_metric_values(cell),
        }
        daily_rows.append(row)
        timeline_points.append(
            {
                "label": stats_date.strftime("%d.%m"),
                "spend": _float(cell.spend),
                "orders": int(cell.orders or 0),
                "revenue": _float(cell.order_sum),
                "impressions": int(cell.impressions or 0),
                "clicks": int(cell.clicks or 0),
                "ctr": _float(cell.ctr),
                "cpc": _float(cell.cpc),
                "drr": _float(drr),
            }
        )

    zone_rows: list[dict[str, object]] = []
    for zone in ZONE_ORDER:
        cell = zone_cells.get(zone, MetricCell())
        if not _has_metric_values(cell) and zone != CampaignZone.SEARCH:
            continue
        zone_rows.append(
            {
                "key": zone,
                "label": ZONE_LABELS.get(zone, zone),
                "cell": cell,
                "traffic_share": _metric_ratio(cell.impressions, total_cell.impressions),
                "orders_share": _metric_ratio(cell.orders, total_cell.orders),
                "revenue_share": _metric_ratio(cell.order_sum, total_cell.order_sum),
                "drr": _metric_ratio(cell.spend, cell.order_sum),
                "cpm": cell.cpm,
            }
        )

    product_rows: list[dict[str, object]] = []
    for product_id, product in product_map.items():
        cell = product_cells.get(product_id, MetricCell())
        cluster_cell = product_cluster_cells.get(product_id, MetricCell())
        search_zone_cell = product_zone_cells.get(product_id, {}).get(CampaignZone.SEARCH, MetricCell())
        product_rows.append(
            {
                "product": product,
                "is_linked": any(item.id == product_id for item in linked_products),
                "cell": cell,
                "search_cluster_cell": cluster_cell,
                "traffic_share": _metric_ratio(cell.impressions, total_cell.impressions),
                "orders_share": _metric_ratio(cell.orders, total_cell.orders),
                "revenue_share": _metric_ratio(cell.order_sum, total_cell.order_sum),
                "spend_share": _metric_ratio(cell.spend, total_cell.spend),
                "ctr": cell.ctr,
                "cpc": cell.cpc,
                "drr": _metric_ratio(cell.spend, cell.order_sum),
                "search_cluster_share": _metric_ratio(cluster_cell.impressions, search_zone_cell.impressions),
                "search_zone_cell": search_zone_cell,
            }
        )

    product_rows.sort(
        key=lambda item: (
            decimalize(item["cell"].spend),
            item["cell"].orders,
            decimalize(item["cell"].order_sum),
            item["product"].title or "",
            item["product"].nm_id,
        ),
        reverse=True,
    )

    cluster_product_rows = [row for row in product_rows if _has_metric_values(row["search_cluster_cell"])]
    cluster_product_rows.sort(
        key=lambda item: (
            decimalize(item["search_cluster_cell"].spend),
            item["search_cluster_cell"].orders,
            item["search_cluster_cell"].clicks,
        ),
        reverse=True,
    )

    products_with_stats = sum(1 for row in product_rows if _has_metric_values(row["cell"]))
    days_with_stats = sum(1 for row in daily_rows if row["has_stats"])
    latest_stats_date = max((row.stats_date for row in stats_rows), default=None)
    search_zone_total = zone_cells.get(CampaignZone.SEARCH, MetricCell())
    top_product = next((row for row in product_rows if _has_metric_values(row["cell"])), None)
    top_zone = next((row for row in zone_rows if _has_metric_values(row["cell"])), None)

    alerts: list[dict[str, str]] = []
    if not linked_products:
        alerts.append(
            {
                "tone": "warning",
                "title": "У кампании нет привязанных товаров",
                "detail": "Свяжите РК с товарами, чтобы sync мог раскладывать статистику по карточкам и отчётам.",
            }
        )
    if not stats_rows:
        alerts.append(
            {
                "tone": "warning",
                "title": "За выбранный период нет рекламной статистики",
                "detail": "Проверьте даты, ID кампании и наличие sync. Метаданные кампании можно видеть уже сейчас, но цифры появляются только после рекламного среза.",
            }
        )
    elif days_with_stats < total_days:
        alerts.append(
            {
                "tone": "info",
                "title": "История заполнена не полностью",
                "detail": f"В диапазоне {total_days} дн. статистика есть только за {days_with_stats} дн.",
            }
        )

    return {
        "campaign": campaign,
        "date_from": date_from,
        "date_to": date_to,
        "linked_products": linked_products,
        "total": total_cell,
        "search_cluster_total": total_cluster_cell,
        "latest_stats_date": latest_stats_date,
        "products_with_stats": products_with_stats,
        "days_with_stats": days_with_stats,
        "days_without_stats": max(total_days - days_with_stats, 0),
        "timeline_chart": _timeline_dataset(timeline_points),
        "daily_rows": list(reversed(daily_rows)),
        "zone_rows": zone_rows,
        "product_rows": product_rows,
        "search_cluster_rows": cluster_product_rows,
        "alerts": alerts,
        "placements_items": _format_placements(campaign.placements or {}),
        "search_cluster_share": _metric_ratio(total_cluster_cell.impressions, search_zone_total.impressions),
        "total_drr": _metric_ratio(total_cell.spend, total_cell.order_sum),
        "avg_daily_spend": _money(safe_divide(total_cell.spend, total_days)),
        "avg_daily_orders": safe_divide(total_cell.orders, total_days),
        "top_product": top_product,
        "top_zone": top_zone,
    }
