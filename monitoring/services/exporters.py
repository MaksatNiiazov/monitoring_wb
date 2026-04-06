from __future__ import annotations

from decimal import Decimal

from monitoring.models import CampaignMonitoringGroup, CampaignZone
from monitoring.services.reports import (
    MetricCell,
    decimalize,
    estimate_buyout_sum,
    percent_fraction,
    percent_points,
    safe_divide,
)


def format_decimal(value: Decimal | int | float | str | None) -> str:
    if value in (None, ""):
        return ""
    number = decimalize(value)
    text = f"{number.quantize(Decimal('0.01'))}".replace(".", ",")
    if "," in text:
        text = text.rstrip("0").rstrip(",")
    return text


def format_percent(value: Decimal | int | float | None) -> str:
    if value is None:
        return "-"
    text = f"{decimalize(value).quantize(Decimal('0.01'))}".replace(".", ",")
    if "," in text:
        text = text.rstrip("0").rstrip(",")
    return text + "%"


def format_int(value: int | float | Decimal | None) -> str:
    if value is None:
        return ""
    return str(int(decimalize(value)))


def format_optional_decimal(value: Decimal | int | float | str | None) -> str:
    number = decimalize(value)
    if number == 0:
        return ""
    return format_decimal(number)


def format_optional_percent(value: Decimal | int | float | None) -> str:
    number = decimalize(value)
    if number == 0:
        return ""
    return format_percent(number)


def format_keyword_int(value: int | None, *, has_data: bool) -> str:
    if value is None and not has_data:
        return ""
    return format_int(value or 0)


def format_keyword_decimal(value: Decimal | int | float | None, *, has_data: bool) -> str:
    if value is None and not has_data:
        return ""
    return format_decimal(value or 0)


def spp_change_parts(report: dict, previous_report: dict | None = None) -> tuple[str, str]:
    note = report["note"]
    current = decimalize(note.spp_percent)
    if current == 0:
        return ("", "")
    if previous_report is None:
        return ("Без изменений", "")
    previous = decimalize(previous_report["note"].spp_percent)
    delta = current - previous
    if delta == 0:
        return ("Без изменений", "")
    label = "Вырос на" if delta > 0 else "Упал на"
    return (label, format_percent(abs(delta)))


def spp_change_label(report: dict, previous_report: dict | None = None) -> str:
    label, value = spp_change_parts(report, previous_report)
    if not label:
        return value
    if not value:
        return label
    return f"{label} {value}"


def cell_for(report: dict, group: str, zone: str) -> MetricCell:
    return report["cells"].get((group, zone), MetricCell())


def estimate_monitoring_profit(
    *,
    seller_price: Decimal | int | float | None,
    unit_cost: Decimal | int | float,
    logistics_cost: Decimal | int | float,
    buyout_percent: Decimal | int | float,
    drr_sales_percent: Decimal | int | float,
    total_orders: int | float,
) -> Decimal:
    if seller_price in (None, ""):
        return Decimal("0")
    seller_price_value = decimalize(seller_price)
    buyout_fraction = percent_fraction(buyout_percent)
    if buyout_fraction <= 0:
        return Decimal("0")
    logistics_adjustment = safe_divide(logistics_cost, buyout_fraction) - Decimal("50") if buyout_fraction else Decimal("0")
    margin_per_buyout_unit = (
        seller_price_value
        - decimalize(unit_cost)
        - (seller_price_value * decimalize(drr_sales_percent) / Decimal("100"))
        - (seller_price_value * Decimal("0.25"))
        - logistics_adjustment
    )
    return (margin_per_buyout_unit * decimalize(total_orders) * buyout_fraction).quantize(Decimal("0.01"))


def exporter_rows(report: dict, previous_report: dict | None = None) -> list[list[str]]:
    product = report["product"]
    economics = report["economics"]
    metrics = report["metrics"]
    stock = report["stock"]
    note = report["note"]
    promo_status_value = (note.promo_status or "").strip() or "Не участвуем"
    negative_feedback_value = (note.negative_feedback or "").strip() or "Без изменений"
    total_ad = report["total_ad"]
    organic = report["organic"]

    unified_search = cell_for(report, CampaignMonitoringGroup.UNIFIED, CampaignZone.SEARCH)
    unified_shelves = cell_for(report, CampaignMonitoringGroup.UNIFIED, CampaignZone.RECOMMENDATION)
    unified_catalog = cell_for(report, CampaignMonitoringGroup.UNIFIED, CampaignZone.CATALOG)
    manual_search = cell_for(report, CampaignMonitoringGroup.MANUAL_SEARCH, CampaignZone.SEARCH)
    manual_shelves = cell_for(report, CampaignMonitoringGroup.MANUAL_SHELVES, CampaignZone.RECOMMENDATION)

    columns = [unified_search, unified_shelves, unified_catalog, manual_search, manual_shelves]

    def is_active(cell: MetricCell) -> bool:
        return any(
            [
                cell.impressions,
                cell.clicks,
                cell.carts,
                cell.orders,
                decimalize(cell.order_sum),
                decimalize(cell.spend),
            ]
        )

    active_columns = [is_active(cell) for cell in columns]

    def pick(metric_name: str) -> list[str]:
        values: list[str] = []
        for index, cell in enumerate(columns):
            if not active_columns[index]:
                values.append("")
                continue
            value = getattr(cell, metric_name)
            if isinstance(value, Decimal):
                values.append(format_decimal(value))
            else:
                values.append(format_int(value))
        return values

    def traffic_unified(cell: MetricCell) -> str:
        if not is_active(cell):
            return ""
        unified_total = unified_search.impressions + unified_shelves.impressions
        if unified_total <= 0:
            return ""
        return format_percent(cell.traffic_share(unified_total))

    def traffic_manual(cell: MetricCell) -> str:
        if not is_active(cell):
            return ""
        return ""

    def derived_decimal(cell: MetricCell, value: Decimal | int | float | str | None) -> str:
        if not is_active(cell):
            return ""
        return format_decimal(value)

    def derived_percent(cell: MetricCell, value: Decimal | int | float | None) -> str:
        if not is_active(cell):
            return ""
        return format_percent(value)

    overall_clicks = metrics.open_count if metrics else 0
    overall_carts = metrics.add_to_cart_count if metrics else 0
    overall_orders = metrics.order_count if metrics else 0
    overall_order_sum = decimalize(metrics.order_sum if metrics else 0)
    conversion_cart = safe_divide(decimalize(overall_carts) * 100, overall_clicks)
    conversion_order = safe_divide(decimalize(overall_orders) * 100, overall_carts)
    estimated_buyout_overall = estimate_buyout_sum(economics, decimalize(metrics.order_sum if metrics else 0))
    drr_sales_ratio = safe_divide(total_ad.spend, estimated_buyout_overall)
    profit_overall = estimate_monitoring_profit(
        seller_price=note.seller_price,
        unit_cost=economics.unit_cost,
        logistics_cost=economics.logistics_cost,
        buyout_percent=economics.buyout_percent,
        drr_sales_percent=drr_sales_ratio,
        total_orders=overall_orders,
    )

    spp_delta_label, spp_delta_value = spp_change_parts(report, previous_report)

    rows = [
        ["", f"{report['stock_date']:%d.%m.%Y}", "", "", "", "", "", ""],
        ["Тип рекламной кампании", "Единая ставка", "", "", "Руч. Поиск", "Руч. Полки", "Общая", "Органика"],
        ["Зоны показов", "Поиск", "Полки", "Каталог", "Поиск", "Полки", "Общая", "Органика"],
        ["Доля трафика (%)", traffic_unified(unified_search), traffic_unified(unified_shelves), "", "", "", "-", "-"],
        ["Затраты (руб)", *pick("spend"), format_decimal(total_ad.spend), "-"],
        ["Показы ", *pick("impressions"), format_int(total_ad.impressions), "-"],
        ["CTR", derived_decimal(unified_search, unified_search.ctr), derived_decimal(unified_shelves, unified_shelves.ctr), derived_decimal(unified_catalog, unified_catalog.ctr), derived_decimal(manual_search, manual_search.ctr), derived_decimal(manual_shelves, manual_shelves.ctr), "-", "-"],
        ["CPM", derived_decimal(unified_search, unified_search.cpm), derived_decimal(unified_shelves, unified_shelves.cpm), derived_decimal(unified_catalog, unified_catalog.cpm), derived_decimal(manual_search, manual_search.cpm), derived_decimal(manual_shelves, manual_shelves.cpm), "-", "-"],
        ["CPC", derived_decimal(unified_search, unified_search.cpc), derived_decimal(unified_shelves, unified_shelves.cpc), derived_decimal(unified_catalog, unified_catalog.cpc), derived_decimal(manual_search, manual_search.cpc), derived_decimal(manual_shelves, manual_shelves.cpc), "-", "-"],
        ["Клики ", *pick("clicks"), format_int(overall_clicks), format_int(organic["open_count"])],
        ["Корзины ", *pick("carts"), format_int(overall_carts), format_int(organic["cart_count"])],
        ["Конверсия в корзину", "", "", "", "", "", format_percent(conversion_cart), ""],
        ["Заказы", *pick("orders"), format_int(overall_orders), format_int(organic["order_count"])],
        ["Конверсия в заказ", "", "", "", "", "", format_percent(conversion_order), ""],
        ["Заказы (руб.)", *pick("order_sum"), format_decimal(overall_order_sum), format_decimal(organic["order_sum"])],
        [
            "Выкупы ≈ (руб.)",
            derived_decimal(unified_search, estimate_buyout_sum(economics, unified_search.order_sum)),
            derived_decimal(unified_shelves, estimate_buyout_sum(economics, unified_shelves.order_sum)),
            derived_decimal(unified_catalog, estimate_buyout_sum(economics, unified_catalog.order_sum)),
            derived_decimal(manual_search, estimate_buyout_sum(economics, manual_search.order_sum)),
            derived_decimal(manual_shelves, estimate_buyout_sum(economics, manual_shelves.order_sum)),
            format_decimal(estimated_buyout_overall),
            "-",
        ],
        [
            "Стоимость заказа",
            derived_decimal(unified_search, unified_search.order_cost),
            derived_decimal(unified_shelves, unified_shelves.order_cost),
            derived_decimal(unified_catalog, unified_catalog.order_cost),
            derived_decimal(manual_search, manual_search.order_cost),
            derived_decimal(manual_shelves, manual_shelves.order_cost),
            format_decimal(safe_divide(total_ad.spend, overall_orders)),
            "-",
        ],
        [
            "Стоимость корзины",
            derived_decimal(unified_search, unified_search.cart_cost),
            derived_decimal(unified_shelves, unified_shelves.cart_cost),
            derived_decimal(unified_catalog, unified_catalog.cart_cost),
            derived_decimal(manual_search, manual_search.cart_cost),
            derived_decimal(manual_shelves, manual_shelves.cart_cost),
            format_decimal(safe_divide(total_ad.spend, overall_carts)),
            "-",
        ],
        [
            "ДРР от заказов (%)",
            derived_percent(unified_search, safe_divide(unified_search.spend, unified_search.order_sum) * 100),
            derived_percent(unified_shelves, safe_divide(unified_shelves.spend, unified_shelves.order_sum) * 100),
            derived_percent(unified_catalog, safe_divide(unified_catalog.spend, unified_catalog.order_sum) * 100),
            derived_percent(manual_search, safe_divide(manual_search.spend, manual_search.order_sum) * 100),
            derived_percent(manual_shelves, safe_divide(manual_shelves.spend, manual_shelves.order_sum) * 100),
            format_percent(safe_divide(total_ad.spend, overall_order_sum) * 100),
            "-",
        ],
        [
            "ДРР от продаж ≈ (%)",
            derived_percent(unified_search, safe_divide(unified_search.spend, estimate_buyout_sum(economics, unified_search.order_sum)) * 100),
            derived_percent(unified_shelves, safe_divide(unified_shelves.spend, estimate_buyout_sum(economics, unified_shelves.order_sum)) * 100),
            derived_percent(unified_catalog, safe_divide(unified_catalog.spend, estimate_buyout_sum(economics, unified_catalog.order_sum)) * 100),
            derived_percent(manual_search, safe_divide(manual_search.spend, estimate_buyout_sum(economics, manual_search.order_sum)) * 100),
            derived_percent(manual_shelves, safe_divide(manual_shelves.spend, estimate_buyout_sum(economics, manual_shelves.order_sum)) * 100),
            format_percent(drr_sales_ratio * 100),
            "-",
        ],
        ["Прибыль", format_decimal(profit_overall), "", "", "", "", "", ""],
        ["Процент выкупа %", format_percent(percent_points(economics.buyout_percent)), "", "", "", "", "", ""],
        ["Себестоимость", format_decimal(economics.unit_cost), "", "", "", "", "", ""],
        ["Логистика", format_decimal(economics.logistics_cost), "", "", "", "", "", ""],
        ["", "Остатки:", "", "", "", "", "", ""],
        ["", "Остатки на складах WB", "", "", format_int(stock.total_stock if stock else 0), "", "", ""],
        ["", "Едут к клиенту", "", "", format_int(stock.in_way_to_client if stock else 0), "", "", ""],
        ["", "Возвращаются на склад", "", "", format_int(stock.in_way_from_client if stock else 0), "", "", ""],
        ["", "Ср. кол-во заказов/день", "", "", format_decimal(report["avg_orders_per_day"]), "", "", ""],
        ["", "Ср. убыль остатков/день", "", "", format_optional_decimal(report["avg_stock_drop_per_day"]), "", "", ""],
        ["", "Дней до АУТА", "", "", format_optional_decimal(report["days_until_zero_from_stock_drop"]), "", "", ""],
        ["", "", "", "", "", "", "", ""],
        ["", "", "", "", "", "", "", ""],
    ]
    rows.extend(
        [
            ["", "Обзор:", "", "", "", "", "", ""],
            ["", "СПП", "", format_optional_percent(note.spp_percent), "", spp_delta_label, "", spp_delta_value],
            ["", "Цена WBSELLER (наша)", "", "", "", format_optional_decimal(note.seller_price), "", ""],
            ["", "Цена WB (на сайте)", "", "", "", format_optional_decimal(note.wb_price), "", ""],
            ["", "Акция", "", "", "", promo_status_value, "", ""],
            ["", "Негативные отзывы", "", "", "", negative_feedback_value, "", ""],
            ["", "Действия:", "", "", "", "", "", ""],
            ["", "Включили рекламу?", "", "", "Да" if (note.unified_enabled or note.manual_search_enabled or note.manual_shelves_enabled) else "Нет", "", "", ""],
            ["", "Меняли цену?(WBSeller)", "", "", "Да" if note.price_changed else "Нет", "", "", ""],
            ["Комментарий:", note.comment, "", "", "", "", "", ""],
        ]
    )
    return rows
