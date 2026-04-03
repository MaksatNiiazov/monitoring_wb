from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from django.db.models import Count, Sum

from monitoring.models import (
    CampaignMonitoringGroup,
    DailyCampaignProductStat,
    DailyProductMetrics,
    DailyProductStock,
    DailyWarehouseStock,
    Product,
    ProductCampaign,
)
from monitoring.services.reports import decimalize, quantize_money, safe_divide


ZERO = Decimal("0")


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


def _sum_decimal_points(series_points: list[dict], key: str) -> Decimal:
    return sum((decimalize(point.get(key)) for point in series_points), ZERO)


def _sum_int_points(series_points: list[dict], key: str) -> int:
    return sum((int(point.get(key) or 0) for point in series_points), 0)


def _series_point_from_aggregates(*, stock_date: date, funnel: dict, ad: dict, stock: dict) -> dict:
    stats_date = stock_date
    total_orders = _int(funnel.get("orders"))
    total_revenue = quantize_money(decimalize(funnel.get("revenue")))
    ad_orders = _int(ad.get("orders"))
    ad_revenue = quantize_money(decimalize(ad.get("revenue")))
    ad_spend = quantize_money(decimalize(ad.get("spend")))
    organic_orders = max(total_orders - ad_orders, 0)
    organic_revenue = quantize_money(max(total_revenue - ad_revenue, ZERO))
    ad_clicks = _int(ad.get("clicks"))
    funnel_opens = _int(funnel.get("opens"))

    return {
        "stock_date": stock_date,
        "stats_date": stats_date,
        "label": stock_date.strftime("%d.%m"),
        "orders": total_orders,
        "revenue": _float(total_revenue),
        "spend": _float(ad_spend),
        "stock": _int(stock.get("total_stock")),
        "clicks": ad_clicks + max(funnel_opens - ad_clicks, 0),
        "funnel_opens": funnel_opens,
        "ad_clicks": ad_clicks,
        "ad_carts": _int(ad.get("carts")),
        "ad_revenue": _float(ad_revenue),
        "organic_orders": organic_orders,
        "organic_revenue": _float(organic_revenue),
        "organic_share": _float(safe_divide(organic_orders * 100, total_orders)),
        "ad_orders": ad_orders,
        "impressions": _int(ad.get("impressions")),
        "to_client": _int(stock.get("to_client")),
        "cpo": _float(safe_divide(ad_spend, ad_orders)),
        "drr": _float(safe_divide(ad_spend * 100, ad_revenue)),
    }


def _series_points_for_dates(*, stock_dates: list[date]) -> list[dict]:
    if not stock_dates:
        return []

    start_date = min(stock_dates)
    end_date = max(stock_dates)

    funnel_by_date = {
        row["stats_date"]: row
        for row in DailyProductMetrics.objects.filter(stats_date__range=(start_date, end_date))
        .values("stats_date")
        .annotate(
            opens=Sum("open_count"),
            carts=Sum("add_to_cart_count"),
            orders=Sum("order_count"),
            revenue=Sum("order_sum"),
        )
    }
    ad_by_date = {
        row["stats_date"]: row
        for row in DailyCampaignProductStat.objects.filter(stats_date__range=(start_date, end_date))
        .values("stats_date")
        .annotate(
            clicks=Sum("clicks"),
            carts=Sum("add_to_cart_count"),
            orders=Sum("order_count"),
            spend=Sum("spend"),
            revenue=Sum("order_sum"),
            impressions=Sum("impressions"),
        )
    }
    stock_by_date = {
        row["stats_date"]: row
        for row in DailyProductStock.objects.filter(stats_date__range=(start_date, end_date))
        .values("stats_date")
        .annotate(total_stock=Sum("total_stock"), to_client=Sum("in_way_to_client"))
    }

    series_points: list[dict] = []
    for stock_date in stock_dates:
        series_points.append(
            _series_point_from_aggregates(
                stock_date=stock_date,
                funnel=funnel_by_date.get(stock_date, {}),
                ad=ad_by_date.get(stock_date, {}),
                stock=stock_by_date.get(stock_date, {}),
            )
        )
    return series_points


def _timeline_dataset(series_points: list[dict]) -> dict:
    return {
        "defaultMetric": "revenue",
        "defaultType": "line",
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
            "cpo": {
                "label": "CPO",
                "format": "money",
                "values": [row["cpo"] for row in product_rows],
            },
            "drr": {
                "label": "ДРР",
                "format": "percent",
                "values": [row["drr"] for row in product_rows],
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


def _kpi_cards(series_points: list[dict]) -> list[dict]:
    if not series_points:
        return []

    days = max(len(series_points), 1)
    total_orders = _sum_int_points(series_points, "orders")
    ad_orders = _sum_int_points(series_points, "ad_orders")
    ad_clicks = _sum_int_points(series_points, "ad_clicks")
    ad_impressions = _sum_int_points(series_points, "impressions")
    ad_spend = _sum_decimal_points(series_points, "spend")
    ad_revenue = _sum_decimal_points(series_points, "ad_revenue")
    stock_latest = int(series_points[-1].get("stock") or 0)
    avg_daily_orders = safe_divide(total_orders, days)

    roas = safe_divide(ad_revenue, ad_spend)
    drr = safe_divide(ad_spend * 100, ad_revenue)
    cpo = safe_divide(ad_spend, ad_orders)
    cpc = safe_divide(ad_spend, ad_clicks)
    ctr = safe_divide(ad_clicks * 100, ad_impressions)
    ad_order_share = safe_divide(ad_orders * 100, total_orders)
    coverage_days = safe_divide(stock_latest, avg_daily_orders)

    return [
        {
            "label": "ROAS",
            "value": _float(roas),
            "format": "decimal",
            "detail": f"Доход на 1 вложенный рубль за период ({days} дн.).",
            "tone": "success" if roas >= 4 else "warning",
        },
        {
            "label": "ДРР",
            "value": _float(drr),
            "format": "percent",
            "detail": "Доля рекламного расхода в рекламной выручке.",
            "tone": "success" if drr <= 25 else "warning",
        },
        {
            "label": "CPO",
            "value": _float(cpo),
            "format": "money",
            "detail": "Средняя стоимость одного рекламного заказа.",
            "tone": "neutral",
        },
        {
            "label": "CTR",
            "value": _float(ctr),
            "format": "percent",
            "detail": "Кликабельность всех рекламных зон.",
            "tone": "neutral",
        },
        {
            "label": "CPC",
            "value": _float(cpc),
            "format": "money",
            "detail": "Средняя цена клика по всем РК.",
            "tone": "neutral",
        },
        {
            "label": "Доля заказов из РК",
            "value": _float(ad_order_share),
            "format": "percent",
            "detail": "Какую часть общих заказов формирует реклама.",
            "tone": "neutral",
        },
        {
            "label": "Покрытие остатком",
            "value": _float(coverage_days),
            "format": "decimal",
            "detail": "Запас в днях при текущем среднем темпе.",
            "tone": "danger" if coverage_days and coverage_days < 7 else "success",
        },
    ]


def _trend_rows(series_points: list[dict]) -> list[dict]:
    if len(series_points) < 4:
        return []

    pivot = max(1, len(series_points) // 2)
    previous = series_points[:pivot]
    current = series_points[pivot:]
    if not previous or not current:
        return []

    def metric_delta(label: str, key: str, fmt: str, lower_is_better: bool = False) -> dict:
        current_value = _sum_decimal_points(current, key)
        previous_value = _sum_decimal_points(previous, key)
        delta = current_value - previous_value
        delta_pct = safe_divide(delta * 100, previous_value) if previous_value else ZERO
        positive = delta_pct <= 0 if lower_is_better else delta_pct >= 0
        return {
            "label": label,
            "current_value": _float(current_value),
            "previous_value": _float(previous_value),
            "delta_value": _float(delta_pct),
            "format": fmt,
            "tone": "success" if positive else "warning",
        }

    return [
        metric_delta("Оборот", "revenue", "money"),
        metric_delta("Заказы", "orders", "int"),
        metric_delta("Расход рекламы", "spend", "money", lower_is_better=True),
        metric_delta("Органическая выручка", "organic_revenue", "money"),
    ]


def _diagnostic_rows(product_rows: list[dict]) -> list[dict]:
    rows = [row for row in product_rows if row["orders"] or row["spend"] or row["stock"]]
    if not rows:
        return []

    cpo_candidates = [decimalize(row["cpo"]) for row in rows if row["orders"] > 0 and row["spend"] > 0]
    avg_cpo = safe_divide(sum(cpo_candidates, ZERO), len(cpo_candidates)) if cpo_candidates else ZERO
    diagnostics: list[dict] = []

    for row in sorted(rows, key=lambda item: (-item["spend"], item["label"])):
        if row["spend"] >= 1500 and row["orders"] == 0:
            diagnostics.append(
                {
                    "label": row["label"],
                    "title": "Расход без заказов",
                    "detail": f"{row['spend_label']} расхода без заказов за текущий срез.",
                    "tone": "danger",
                }
            )
        elif avg_cpo > 0 and row["orders"] >= 3 and decimalize(row["cpo"]) >= avg_cpo * Decimal("1.7"):
            diagnostics.append(
                {
                    "label": row["label"],
                    "title": "Дорогой заказ относительно среднего",
                    "detail": f"CPO {row['cpo_label']} при среднем по витрине {_format_money(avg_cpo)}.",
                    "tone": "warning",
                }
            )
        elif row["days_until_zero"] > 0 and row["days_until_zero"] <= 7 and row["orders"] > 0:
            diagnostics.append(
                {
                    "label": row["label"],
                    "title": "Риск по остаткам",
                    "detail": f"Запас на {row['days_until_zero_label']} дня при текущем темпе.",
                    "tone": "warning",
                }
            )
        elif row["organic_share"] <= 20 and row["spend"] >= 2000:
            diagnostics.append(
                {
                    "label": row["label"],
                    "title": "Высокая зависимость от рекламы",
                    "detail": f"Органика {row['organic_share_label']} при расходе {row['spend_label']}.",
                    "tone": "neutral",
                }
            )

        if len(diagnostics) >= 8:
            break

    if diagnostics:
        return diagnostics
    return [
        {
            "label": "Сезон без явных перекосов",
            "title": "Критичных аномалий не найдено",
            "detail": "По ключевым сигналам CPO, расходу и остаткам всё в рабочем коридоре.",
            "tone": "success",
        }
    ]


def build_reports_context(*, reference_date: date, range_days: int = 14) -> dict:
    range_days = max(1, min(range_days, 60))
    stock_dates = [reference_date - timedelta(days=offset) for offset in reversed(range(range_days))]
    series_points = _series_points_for_dates(stock_dates=stock_dates)

    products = list(Product.objects.filter(is_active=True).order_by("vendor_code", "title", "nm_id"))
    product_ids = [product.id for product in products]

    metrics_by_product_id = {
        row.product_id: row
        for row in DailyProductMetrics.objects.filter(
            product_id__in=product_ids,
            stats_date=reference_date,
        )
    }
    stocks_by_product_id = {
        row.product_id: row
        for row in DailyProductStock.objects.filter(
            product_id__in=product_ids,
            stats_date=reference_date,
        )
    }
    campaign_totals_by_product_id = {
        row["product_id"]: {
            "spend": decimalize(row["spend"]),
            "orders": int(row["orders"] or 0),
            "revenue": decimalize(row["revenue"]),
        }
        for row in DailyCampaignProductStat.objects.filter(
            product_id__in=product_ids,
            stats_date=reference_date,
        )
        .values("product_id")
        .annotate(
            spend=Sum("spend"),
            orders=Sum("order_count"),
            revenue=Sum("order_sum"),
        )
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

    product_rows: list[dict] = []
    for product in products:
        metrics = metrics_by_product_id.get(product.id)
        stock = stocks_by_product_id.get(product.id)
        campaign_totals = campaign_totals_by_product_id.get(
            product.id,
            {"spend": ZERO, "orders": 0, "revenue": ZERO},
        )
        ad_spend = campaign_totals["spend"]
        ad_orders = int(campaign_totals["orders"] or 0)
        ad_revenue = campaign_totals["revenue"]
        total_orders = int(metrics.order_count if metrics else 0)
        organic_orders = max(total_orders - ad_orders, 0)
        organic_share = safe_divide(organic_orders * 100, total_orders)
        drr = safe_divide(ad_spend * 100, ad_revenue)
        cpo = safe_divide(ad_spend, ad_orders)
        days_until_zero = decimalize(stock.days_until_zero if stock else 0)

        product_rows.append(
            {
                "label": product.vendor_code or product.title or f"WB {product.nm_id}",
                "product": product,
                "orders": total_orders,
                "revenue": _float(metrics.order_sum if metrics else 0),
                "revenue_label": _format_money(metrics.order_sum if metrics else 0),
                "spend": _float(ad_spend),
                "spend_label": _format_money(ad_spend),
                "stock": stock.total_stock if stock else 0,
                "organic_share": _float(organic_share),
                "organic_share_label": _format_percent(organic_share),
                "drr": _float(drr),
                "drr_label": _format_percent(drr),
                "cpo": _float(cpo),
                "cpo_label": _format_money(cpo),
                "days_until_zero": _float(days_until_zero),
                "days_until_zero_label": _format_decimal(days_until_zero),
                "campaigns_count": campaigns_count_by_product_id.get(product.id, 0),
            }
        )

    ranked_products = sorted(product_rows, key=lambda item: (-item["revenue"], -item["orders"], item["label"]))

    group_labels = dict(CampaignMonitoringGroup.choices)
    total_campaign_spend = ZERO
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
    revenue_total = sum((Decimal(str(point["revenue"])) for point in series_points), ZERO)
    spend_total = sum((Decimal(str(point["spend"])) for point in series_points), ZERO)
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
        "kpi_cards": _kpi_cards(series_points),
        "trend_rows": _trend_rows(series_points),
        "diagnostic_rows": _diagnostic_rows(ranked_products),
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
