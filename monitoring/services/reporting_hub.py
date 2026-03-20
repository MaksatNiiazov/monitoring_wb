from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from django.db.models import Sum

from monitoring.models import (
    CampaignMonitoringGroup,
    DailyCampaignProductStat,
    DailyProductMetrics,
    DailyProductStock,
    DailyWarehouseStock,
    Product,
)
from monitoring.services.reports import build_product_report, decimalize, quantize_money, safe_divide


def _float(value) -> float:
    return float(decimalize(value).quantize(Decimal("0.01")))


def _int(value) -> int:
    return int(decimalize(value))


def _format_money(value) -> str:
    return f"{quantize_money(decimalize(value)):,}".replace(",", " ").replace(".", ",") + " ₽"


def _format_signed_money(value) -> str:
    amount = quantize_money(decimalize(value))
    prefix = "+" if amount > 0 else ""
    return prefix + _format_money(amount)


def _format_decimal(value) -> str:
    return f"{decimalize(value).quantize(Decimal('0.01'))}".replace(".", ",")


def _format_percent(value) -> str:
    return f"{decimalize(value).quantize(Decimal('0.01'))}%".replace(".", ",")


def _series_point(*, stock_date: date) -> dict:
    stats_date = stock_date
    funnel = DailyProductMetrics.objects.filter(stats_date=stats_date).aggregate(
        opens=Sum("open_count"),
        carts=Sum("add_to_cart_count"),
        orders=Sum("order_count"),
        revenue=Sum("order_sum"),
    )
    ad = DailyCampaignProductStat.objects.filter(stats_date=stats_date).aggregate(
        clicks=Sum("clicks"),
        carts=Sum("add_to_cart_count"),
        orders=Sum("order_count"),
        spend=Sum("spend"),
        revenue=Sum("order_sum"),
        impressions=Sum("impressions"),
    )
    stock = DailyProductStock.objects.filter(stats_date=stock_date).aggregate(
        total_stock=Sum("total_stock"),
        to_client=Sum("in_way_to_client"),
    )

    total_orders = _int(funnel.get("orders"))
    total_revenue = quantize_money(decimalize(funnel.get("revenue")))
    ad_orders = _int(ad.get("orders"))
    ad_revenue = quantize_money(decimalize(ad.get("revenue")))
    organic_orders = max(total_orders - ad_orders, 0)
    organic_revenue = quantize_money(max(total_revenue - ad_revenue, Decimal("0")))

    return {
        "stock_date": stock_date,
        "stats_date": stats_date,
        "label": stock_date.strftime("%d.%m"),
        "orders": total_orders,
        "revenue": _float(total_revenue),
        "spend": _float(ad.get("spend")),
        "stock": _int(stock.get("total_stock")),
        "clicks": _int(ad.get("clicks")) + max(_int(funnel.get("opens")) - _int(ad.get("clicks")), 0),
        "organic_orders": organic_orders,
        "organic_revenue": _float(organic_revenue),
        "organic_share": _float(safe_divide(organic_orders * 100, total_orders)),
        "ad_orders": ad_orders,
        "impressions": _int(ad.get("impressions")),
        "to_client": _int(stock.get("to_client")),
    }


def _timeline_dataset(series_points: list[dict]) -> dict:
    return {
        "defaultMetric": "revenue",
        "defaultType": "area",
        "labels": [point["label"] for point in series_points],
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
        },
    }


def _product_dataset(product_rows: list[dict]) -> dict:
    return {
        "defaultMetric": "orders",
        "defaultType": "bar",
        "labels": [row["label"] for row in product_rows],
        "series": {
            "orders": {
                "label": "Заказы",
                "format": "int",
                "values": [row["orders"] for row in product_rows],
            },
            "revenue": {
                "label": "Оборот",
                "format": "money",
                "values": [row["revenue"] for row in product_rows],
            },
            "spend": {
                "label": "Расход рекламы",
                "format": "money",
                "values": [row["spend"] for row in product_rows],
            },
            "stock": {
                "label": "Остаток WB",
                "format": "int",
                "values": [row["stock"] for row in product_rows],
            },
            "organic_share": {
                "label": "Доля органики",
                "format": "percent",
                "values": [row["organic_share"] for row in product_rows],
            },
        },
    }


def _product_spotlights(product_rows: list[dict], reference_date: date) -> list[dict]:
    non_zero_rows = [row for row in product_rows if row["orders"] or row["revenue"] or row["spend"] or row["stock"]]
    if not non_zero_rows:
        return [
            {
                "label": "Нет накопленных срезов",
                "value": "Добавьте данные",
                "detail": "После первой синхронизации здесь появятся сигналы по продажам, рекламе и остаткам.",
                "tone": "neutral",
            }
        ]

    revenue_leader = max(non_zero_rows, key=lambda item: (item["revenue"], item["orders"], item["label"]))
    spend_leader = max(non_zero_rows, key=lambda item: (item["spend"], item["revenue"], item["label"]))
    organic_leader = max(non_zero_rows, key=lambda item: (item["organic_share"], item["orders"], item["label"]))

    stock_candidates = [
        row
        for row in non_zero_rows
        if row["days_until_zero"] not in (None, 0) and row["stock"] > 0 and row["orders"] > 0
    ]
    stock_risk = min(stock_candidates, key=lambda item: (item["days_until_zero"], item["stock"], item["label"])) if stock_candidates else None

    cards = [
        {
            "label": "Лидер по обороту",
            "value": revenue_leader["label"],
            "detail": f"{revenue_leader['revenue_label']} за срез {reference_date:%d.%m.%Y}.",
            "tone": "success",
        },
        {
            "label": "Максимальная рекламная нагрузка",
            "value": spend_leader["label"],
            "detail": f"{spend_leader['spend_label']} расхода на дату РК {reference_date:%d.%m.%Y}.",
            "tone": "warning",
        },
        {
            "label": "Сильнейшая органика",
            "value": organic_leader["label"],
            "detail": f"{organic_leader['organic_share_label']} органических заказов в текущем срезе.",
            "tone": "neutral",
        },
    ]
    if stock_risk:
        cards.append(
            {
                "label": "Риск по остаткам",
                "value": stock_risk["label"],
                "detail": f"Запаса примерно на {stock_risk['days_until_zero_label']} дня по текущему темпу заказов.",
                "tone": "danger",
            }
        )
    return cards


def build_reports_context(*, reference_date: date, range_days: int = 14) -> dict:
    range_days = max(7, min(range_days, 60))
    stock_dates = [reference_date - timedelta(days=offset) for offset in reversed(range(range_days))]
    series_points = [_series_point(stock_date=stock_date) for stock_date in stock_dates]

    products = list(Product.objects.filter(is_active=True).order_by("vendor_code", "title", "nm_id"))
    product_rows: list[dict] = []
    for product in products:
        report = build_product_report(
            product=product,
            stats_date=reference_date,
            stock_date=reference_date,
            create_note=False,
        )
        days_until_zero = decimalize(report["days_until_zero"])
        product_rows.append(
            {
                "label": product.vendor_code or product.title or f"WB {product.nm_id}",
                "product": product,
                "orders": report["metrics"].order_count if report["metrics"] else 0,
                "revenue": _float(report["metrics"].order_sum if report["metrics"] else 0),
                "revenue_label": _format_money(report["metrics"].order_sum if report["metrics"] else 0),
                "spend": _float(report["total_ad"].spend),
                "spend_label": _format_money(report["total_ad"].spend),
                "stock": report["stock"].total_stock if report["stock"] else 0,
                "organic_share": _float(report["insights"]["organic_orders_share"]),
                "organic_share_label": _format_percent(report["insights"]["organic_orders_share"]),
                "days_until_zero": _float(days_until_zero),
                "days_until_zero_label": _format_decimal(days_until_zero),
                "campaigns_count": product.campaigns.filter(is_active=True).count(),
            }
        )

    ranked_products = sorted(product_rows, key=lambda item: (-item["revenue"], -item["orders"], item["label"]))

    group_labels = dict(CampaignMonitoringGroup.choices)
    total_campaign_spend = Decimal("0")
    campaign_mix_raw = list(
        DailyCampaignProductStat.objects.filter(stats_date=reference_date)
        .values("campaign__monitoring_group")
        .annotate(
            spend=Sum("spend"),
            orders=Sum("order_count"),
            revenue=Sum("order_sum"),
        )
        .order_by("-spend")
    )
    for row in campaign_mix_raw:
        total_campaign_spend += decimalize(row["spend"])

    campaign_mix_rows = []
    for row in campaign_mix_raw:
        group = row["campaign__monitoring_group"] or CampaignMonitoringGroup.OTHER
        spend = decimalize(row["spend"])
        orders = _int(row["orders"])
        revenue = decimalize(row["revenue"])
        campaign_mix_rows.append(
            {
                "label": group_labels.get(group, group),
                "spend": _float(spend),
                "spend_share": _float(safe_divide(spend * 100, total_campaign_spend)),
                "orders": orders,
                "revenue": _float(revenue),
                "cpo": _float(safe_divide(spend, orders)),
            }
        )

    warehouse_raw = list(
        DailyWarehouseStock.objects.filter(stats_date=reference_date)
        .values("warehouse__name")
        .annotate(stock=Sum("stock_count"), to_client=Sum("in_way_to_client"))
        .order_by("-stock")[:10]
    )
    total_visible_stock = sum((_int(row["stock"]) for row in warehouse_raw), 0)
    warehouse_rows = [
        {
            "label": row["warehouse__name"],
            "stock": _int(row["stock"]),
            "to_client": _int(row["to_client"]),
            "stock_share": _float(safe_divide(_int(row["stock"]) * 100, total_visible_stock)),
        }
        for row in warehouse_raw
    ]

    latest_point = series_points[-1] if series_points else {}
    previous_point = series_points[-2] if len(series_points) > 1 else latest_point
    revenue_total = sum((Decimal(str(point["revenue"])) for point in series_points), Decimal("0"))
    spend_total = sum((Decimal(str(point["spend"])) for point in series_points), Decimal("0"))
    orders_total = sum(point["orders"] for point in series_points)
    stock_total = latest_point.get("stock", 0)
    latest_orders = latest_point.get("orders", 0)
    previous_orders = previous_point.get("orders", 0)
    latest_revenue = Decimal(str(latest_point.get("revenue", 0)))
    previous_revenue = Decimal(str(previous_point.get("revenue", 0)))

    summary_cards = [
        {
            "label": "Заказы за период",
            "value": orders_total,
            "format": "int",
            "detail": f"Последний день: {latest_orders} ({latest_orders - previous_orders:+d}).",
        },
        {
            "label": "Оборот за период",
            "value": _float(revenue_total),
            "format": "money",
            "detail": f"Последний день: {_format_money(latest_revenue)} ({_format_signed_money(latest_revenue - previous_revenue)} к предыдущему).",
        },
        {
            "label": "Расход рекламы",
            "value": _float(spend_total),
            "format": "money",
            "detail": "Сумма по всем кампаниям внутри выбранного окна аналитики.",
        },
        {
            "label": "Текущий остаток WB",
            "value": stock_total,
            "format": "int",
            "detail": f"Срез по складам на дату {reference_date:%d.%m.%Y}.",
        },
        {
            "label": "Доля органики",
            "value": _float(latest_point.get("organic_share", 0)),
            "format": "percent",
            "detail": f"По рекламному дню {reference_date:%d.%m.%Y}.",
        },
    ]

    return {
        "reference_date": reference_date,
        "range_days": range_days,
        "range_options": [7, 14, 30, 60],
        "summary_cards": summary_cards,
        "spotlight_cards": _product_spotlights(ranked_products, reference_date),
        "timeline_points": series_points,
        "timeline_chart": _timeline_dataset(series_points),
        "product_rows": ranked_products,
        "product_chart": _product_dataset(sorted(product_rows, key=lambda item: (-item["orders"], -item["revenue"], item["label"]))[:10]),
        "campaign_mix_rows": campaign_mix_rows,
        "warehouse_rows": warehouse_rows,
        "latest_point": latest_point,
        "latest_stats_date": reference_date,
    }
