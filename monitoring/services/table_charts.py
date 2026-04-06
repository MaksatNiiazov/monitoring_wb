from __future__ import annotations

from datetime import date
from decimal import Decimal

from monitoring.models import Product
from monitoring.services.monitoring_table import _build_prefetched_product_report_context
from monitoring.services.reporting_hub import build_reports_context
from monitoring.services.reports import build_product_report, decimalize, safe_divide


ZERO = Decimal("0")


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
        ad_orders = _int(total_ad.orders)
        ad_revenue = decimalize(total_ad.order_sum)
        ad_spend = decimalize(total_ad.spend)

        series_points.append(
            {
                "label": stock_date.strftime("%d.%m"),
                "orders": _int(metrics.order_count if metrics else 0),
                "revenue": _float(metrics.order_sum if metrics else 0),
                "spend": _float(ad_spend),
                "stock": _int(stock.total_stock if stock else 0),
                "organic_share": _float(insights.get("organic_orders_share") or 0),
                "cpo": _float(safe_divide(ad_spend, ad_orders)),
                "drr": _float(safe_divide(ad_spend * 100, ad_revenue)),
            }
        )
    return series_points


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

    series_points = _build_product_series_points(product=product, stock_dates=block_dates)
    product_label = product.vendor_code or product.title or f"WB {product.nm_id}"
    return {
        "title": f"Динамика SKU · {product_label}",
        "subtitle": "Показывает выбранные метрики по тем же датам, которые сейчас открыты в таблице.",
        "chart": _timeline_dataset(series_points),
    }
