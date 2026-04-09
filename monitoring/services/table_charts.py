from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal
from typing import Any

from monitoring.models import DailyCampaignProductStat, Product
from monitoring.services.monitoring_table import _build_prefetched_product_report_context
from monitoring.services.reporting_hub import build_reports_context
from monitoring.services.reports import (
    build_product_report,
    decimalize,
    estimate_buyout_sum,
    estimate_monitoring_profit,
    safe_divide,
)


ZERO = Decimal("0")
CAMPAIGN_PALETTE = [
    "#0f6a5c",
    "#22724d",
    "#986322",
    "#9d483d",
    "#345c8a",
    "#7a5af8",
    "#00838f",
    "#5f6b7a",
    "#ff7a59",
    "#d97706",
]
STANDARD_PALETTE = [
    "#0f6a5c",
    "#22724d",
    "#986322",
    "#9d483d",
    "#345c8a",
    "#7a5af8",
    "#00838f",
    "#5f6b7a",
    "#ff7a59",
    "#d97706",
    "#2f855a",
    "#c026d3",
    "#2563eb",
    "#0ea5e9",
    "#9333ea",
    "#dc2626",
    "#475569",
    "#6b7280",
]


def _float(value: Decimal | int | float | str | None) -> float:
    return float(decimalize(value).quantize(Decimal("0.01")))


def _int(value: Decimal | int | float | str | None) -> int:
    return int(decimalize(value))


def _timeline_dataset(series_points: list[dict[str, object]]) -> dict[str, object]:
    return {
        "defaultMetric": "revenue",
        "defaultType": "line",
        "labels": [str(point["label"]) for point in series_points],
        "series": {
            "orders": {
                "label": "Заказы",
                "format": "int",
                "values": [point["orders"] for point in series_points],
            },
            "revenue": {
                "label": "Оборот",
                "format": "money",
                "values": [point["revenue"] for point in series_points],
            },
            "spend": {
                "label": "Расход рекламы",
                "format": "money",
                "values": [point["spend"] for point in series_points],
            },
            "stock": {
                "label": "Остаток WB",
                "format": "int",
                "values": [point["stock"] for point in series_points],
            },
            "organic_share": {
                "label": "Доля органики",
                "format": "percent",
                "values": [point["organic_share"] for point in series_points],
            },
            "cpo": {
                "label": "CPO",
                "format": "money",
                "values": [point["cpo"] for point in series_points],
            },
            "drr": {
                "label": "ДРР",
                "format": "percent",
                "values": [point["drr"] for point in series_points],
            },
        },
    }


def _product_metrics_dataset(
    series_points: list[dict[str, object]],
    *,
    start_label: str,
    end_label: str,
) -> dict[str, object]:
    series_order = [
        "stock",
        "spend",
        "impressions",
        "ctr",
        "cpm",
        "cpc",
        "clicks",
        "carts",
        "conversion_cart",
        "orders",
        "conversion_order",
        "order_sum",
        "buyouts",
        "cost_per_order",
        "cost_per_cart",
        "drr_orders",
        "drr_sales",
        "profit",
    ]
    return {
        "mode": "multi",
        "defaultType": "line",
        "labels": [str(point["label"]) for point in series_points],
        "seriesOrder": series_order,
        "defaultSeries": series_order,
        "windowLabel": f"{start_label} — {end_label}",
        "series": {
            "stock": {"label": "Остатки WB", "format": "int", "values": [point["stock"] for point in series_points], "color": STANDARD_PALETTE[0]},
            "spend": {"label": "Затраты (руб)", "format": "money", "values": [point["spend"] for point in series_points], "color": STANDARD_PALETTE[1]},
            "impressions": {"label": "Показы", "format": "int", "values": [point["impressions"] for point in series_points], "color": STANDARD_PALETTE[2]},
            "ctr": {"label": "CTR", "format": "percent", "values": [point["ctr"] for point in series_points], "color": STANDARD_PALETTE[3]},
            "cpm": {"label": "CPM", "format": "money", "values": [point["cpm"] for point in series_points], "color": STANDARD_PALETTE[4]},
            "cpc": {"label": "CPC", "format": "money", "values": [point["cpc"] for point in series_points], "color": STANDARD_PALETTE[5]},
            "clicks": {"label": "Клики", "format": "int", "values": [point["clicks"] for point in series_points], "color": STANDARD_PALETTE[6]},
            "carts": {"label": "Корзины", "format": "int", "values": [point["carts"] for point in series_points], "color": STANDARD_PALETTE[7]},
            "conversion_cart": {"label": "Конверсия в корзину", "format": "percent", "values": [point["conversion_cart"] for point in series_points], "color": STANDARD_PALETTE[8]},
            "orders": {"label": "Заказы", "format": "int", "values": [point["orders"] for point in series_points], "color": STANDARD_PALETTE[9]},
            "conversion_order": {"label": "Конверсия в заказ", "format": "percent", "values": [point["conversion_order"] for point in series_points], "color": STANDARD_PALETTE[10]},
            "order_sum": {"label": "Заказы (руб.)", "format": "money", "values": [point["order_sum"] for point in series_points], "color": STANDARD_PALETTE[11]},
            "buyouts": {"label": "Выкупы ≈ (руб.)", "format": "money", "values": [point["buyouts"] for point in series_points], "color": STANDARD_PALETTE[12]},
            "cost_per_order": {"label": "Стоимость заказа", "format": "money", "values": [point["cost_per_order"] for point in series_points], "color": STANDARD_PALETTE[13]},
            "cost_per_cart": {"label": "Стоимость корзины", "format": "money", "values": [point["cost_per_cart"] for point in series_points], "color": STANDARD_PALETTE[14]},
            "drr_orders": {"label": "ДРР от заказов (%)", "format": "percent", "values": [point["drr_orders"] for point in series_points], "color": STANDARD_PALETTE[15]},
            "drr_sales": {"label": "ДРР от продаж ≈ (%)", "format": "percent", "values": [point["drr_sales"] for point in series_points], "color": STANDARD_PALETTE[16]},
            "profit": {"label": "Прибыль", "format": "money", "values": [point["profit"] for point in series_points], "color": STANDARD_PALETTE[17]},
        },
    }


def _campaign_metrics_dataset(
    campaign_series: dict[str, Any],
    *,
    labels: list[str],
    start_label: str,
    end_label: str,
) -> dict[str, object]:
    metric_order = ["spend", "impressions", "clicks", "carts", "orders", "order_sum"]
    metric_labels = {
        "spend": ("Затраты", "money"),
        "impressions": ("Показы", "int"),
        "clicks": ("Клики", "int"),
        "carts": ("Корзины", "int"),
        "orders": ("Заказы", "int"),
        "order_sum": ("Заказы (руб.)", "money"),
    }
    metrics: dict[str, dict[str, object]] = {}
    for metric_key in metric_order:
        label, format_name = metric_labels[metric_key]
        series = {}
        for index, (campaign_key, payload) in enumerate(campaign_series.items()):
            series[campaign_key] = {
                "label": payload["label"],
                "color": payload["color"],
                "values": payload["metrics"][metric_key],
            }
        metrics[metric_key] = {
            "label": label,
            "format": format_name,
            "seriesOrder": list(campaign_series.keys()),
            "series": series,
        }
    return {
        "mode": "campaigns",
        "defaultMetric": "spend",
        "metricOrder": metric_order,
        "defaultSeries": list(campaign_series.keys()),
        "labels": labels,
        "metrics": metrics,
        "windowLabel": f"{start_label} — {end_label}",
        "emptyText": "По рекламным кампаниям пока нет сохранённого среза за выбранный период.",
        "emptyCaption": "Запустите синхронизацию РК или выберите период, где по кампаниям уже есть статистика.",
    }


def _build_product_series_points(*, product: Product, stock_dates: list[date]) -> list[dict[str, object]]:
    if not stock_dates:
        return []

    prefetched_context = _build_prefetched_product_report_context(product=product, stock_dates=stock_dates)
    reports = [
        build_product_report(
            product=product,
            stats_date=stock_date,
            stock_date=stock_date,
            create_note=False,
            **prefetched_context,
        )
        for stock_date in stock_dates
    ]

    series_points: list[dict[str, object]] = []
    for stock_date, report in zip(stock_dates, reports):
        metrics = report["metrics"]
        total_ad = report["total_ad"]
        stock = report["stock"]
        insights = report["insights"]
        economics = report["economics"]
        note = report["note"]
        ad_spend = decimalize(total_ad.spend)
        order_sum = decimalize(metrics.order_sum if metrics else 0)
        clicks = _int(metrics.open_count if metrics else 0)
        carts = _int(metrics.add_to_cart_count if metrics else 0)
        orders = _int(metrics.order_count if metrics else 0)
        buyouts = estimate_buyout_sum(economics, order_sum)
        ad_orders = _int(total_ad.orders)
        ad_revenue = decimalize(total_ad.order_sum)
        drr_sales_ratio = safe_divide(ad_spend, buyouts)
        profit = estimate_monitoring_profit(
            seller_price=getattr(note, "seller_price", None),
            unit_cost=economics.unit_cost,
            logistics_cost=economics.logistics_cost,
            buyout_percent=economics.buyout_percent,
            drr_sales_percent=drr_sales_ratio,
            total_orders=orders,
        )

        series_points.append(
            {
                "label": stock_date.strftime("%d.%m"),
                "orders": orders,
                "revenue": _float(order_sum),
                "spend": _float(ad_spend),
                "stock": _int(stock.total_stock if stock else 0),
                "organic_share": _float(insights.get("organic_orders_share") or 0),
                "cpo": _float(safe_divide(ad_spend, ad_orders)),
                "drr": _float(safe_divide(ad_spend * 100, ad_revenue)),
                "impressions": _int(total_ad.impressions),
                "ctr": _float(total_ad.ctr),
                "cpm": _float(total_ad.cpm),
                "cpc": _float(total_ad.cpc),
                "clicks": clicks,
                "carts": carts,
                "conversion_cart": _float(safe_divide(carts * 100, clicks)),
                "conversion_order": _float(safe_divide(orders * 100, carts)),
                "order_sum": _float(order_sum),
                "buyouts": _float(buyouts),
                "cost_per_order": _float(safe_divide(ad_spend, orders)),
                "cost_per_cart": _float(safe_divide(ad_spend, carts)),
                "drr_orders": _float(safe_divide(ad_spend * 100, order_sum)),
                "drr_sales": _float(safe_divide(ad_spend * 100, buyouts)),
                "profit": _float(profit),
            }
        )
    return series_points


def _build_campaign_series_points(*, product: Product, stock_dates: list[date]) -> dict[str, Any]:
    if not stock_dates:
        return {}

    labels = [stock_date.strftime("%d.%m") for stock_date in stock_dates]
    rows = list(
        DailyCampaignProductStat.objects.filter(
            product=product,
            stats_date__in=stock_dates,
            campaign__products=product,
            campaign__is_active=True,
        )
        .select_related("campaign")
        .order_by("campaign__name", "campaign__external_id", "stats_date", "zone")
    )

    campaign_order: list[int] = []
    campaign_meta: dict[int, dict[str, Any]] = {}
    daily_totals: dict[tuple[int, date], dict[str, Decimal | int]] = defaultdict(
        lambda: {
            "spend": ZERO,
            "impressions": 0,
            "clicks": 0,
            "carts": 0,
            "orders": 0,
            "order_sum": ZERO,
        }
    )

    for row in rows:
        if row.campaign_id not in campaign_meta:
            campaign_order.append(row.campaign_id)
            campaign_meta[row.campaign_id] = {
                "key": f"campaign-{row.campaign_id}",
                "label": row.campaign.name or f"РК {row.campaign.external_id}",
                "color": CAMPAIGN_PALETTE[(len(campaign_order) - 1) % len(CAMPAIGN_PALETTE)],
            }
        bucket = daily_totals[(row.campaign_id, row.stats_date)]
        bucket["spend"] = decimalize(bucket["spend"]) + decimalize(row.spend)
        bucket["impressions"] = int(bucket["impressions"]) + int(row.impressions or 0)
        bucket["clicks"] = int(bucket["clicks"]) + int(row.clicks or 0)
        bucket["carts"] = int(bucket["carts"]) + int(row.add_to_cart_count or 0)
        bucket["orders"] = int(bucket["orders"]) + int(row.order_count or 0)
        bucket["order_sum"] = decimalize(bucket["order_sum"]) + decimalize(row.order_sum)

    series: dict[str, Any] = {}
    for campaign_id in campaign_order:
        meta = campaign_meta[campaign_id]
        series[meta["key"]] = {
            "label": meta["label"],
            "color": meta["color"],
            "labels": labels,
            "metrics": {
                "spend": [],
                "impressions": [],
                "clicks": [],
                "carts": [],
                "orders": [],
                "order_sum": [],
            },
        }
        for stock_date in stock_dates:
            totals = daily_totals[(campaign_id, stock_date)]
            series[meta["key"]]["metrics"]["spend"].append(_float(totals["spend"]))
            series[meta["key"]]["metrics"]["impressions"].append(int(totals["impressions"]))
            series[meta["key"]]["metrics"]["clicks"].append(int(totals["clicks"]))
            series[meta["key"]]["metrics"]["carts"].append(int(totals["carts"]))
            series[meta["key"]]["metrics"]["orders"].append(int(totals["orders"]))
            series[meta["key"]]["metrics"]["order_sum"].append(_float(totals["order_sum"]))

    return series


def build_table_timeline_context(
    *,
    active_sheet: dict[str, object] | None,
    reference_date: date,
    history_days: int,
) -> dict[str, object] | None:
    if not active_sheet:
        return None

    if active_sheet.get("kind") == "dashboard":
        reports_context = build_reports_context(reference_date=reference_date, range_days=history_days)
        return {
            "title": "Динамика по сводке",
            "subtitle": "График использует тот же период, что выбран для таблицы мониторинга.",
            "chart": reports_context["timeline_chart"],
        }

    if active_sheet.get("kind") != "product":
        return None

    product_id = active_sheet.get("product_id")
    block_dates = list(active_sheet.get("block_dates") or [])
    if not product_id or not block_dates:
        return None

    product = Product.objects.filter(pk=product_id).first()
    if product is None:
        return None

    standard_points = _build_product_series_points(product=product, stock_dates=block_dates)
    campaign_series = _build_campaign_series_points(product=product, stock_dates=block_dates)
    has_campaign_view = product.campaigns.filter(is_active=True).exists()
    product_label = product.vendor_code or product.title or f"WB {product.nm_id}"

    if has_campaign_view:
        chart: dict[str, Any] = {
            "defaultView": "standard",
            "viewOptions": [
                {"key": "standard", "label": "Стандартный"},
                {"key": "campaigns", "label": "По рекламным кампаниям"},
            ],
            "views": {
                "standard": _product_metrics_dataset(
                    standard_points,
                    start_label=block_dates[0].strftime("%d.%m.%Y"),
                    end_label=block_dates[-1].strftime("%d.%m.%Y"),
                ),
                "campaigns": _campaign_metrics_dataset(
                    campaign_series,
                    labels=[stock_date.strftime("%d.%m") for stock_date in block_dates],
                    start_label=block_dates[0].strftime("%d.%m.%Y"),
                    end_label=block_dates[-1].strftime("%d.%m.%Y"),
                ),
            },
        }
    else:
        chart = _product_metrics_dataset(
            standard_points,
            start_label=block_dates[0].strftime("%d.%m.%Y"),
            end_label=block_dates[-1].strftime("%d.%m.%Y"),
        )

    return {
        "title": f"Динамика SKU · {product_label}",
        "subtitle": "Выберите стандартный режим для общей динамики товара или режим по рекламным кампаниям для сравнения РК между собой.",
        "chart": chart,
    }
