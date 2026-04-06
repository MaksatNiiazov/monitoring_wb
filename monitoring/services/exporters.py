from __future__ import annotations

from decimal import Decimal

from monitoring.services.reports import (
    decimalize,
    estimate_buyout_sum,
    has_metric_cell_data,
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
    number = decimalize(value)
    if number != 0 and abs(number) < Decimal("0.01"):
        return "<0,01%" if number > 0 else ">-0,01%"
    text = f"{number.quantize(Decimal('0.01'))}".replace(".", ",")
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
        - (seller_price_value * decimalize(drr_sales_percent))
        - (seller_price_value * Decimal("0.25"))
        - logistics_adjustment
    )
    return (margin_per_buyout_unit * decimalize(total_orders) * buyout_fraction).quantize(Decimal("0.01"))


def exporter_rows(report: dict, previous_report: dict | None = None) -> list[list[str]]:
    economics = report["economics"]
    metrics = report["metrics"]
    stock = report["stock"]
    note = report["note"]
    promo_status_value = (note.promo_status or "").strip() or "Не участвуем"
    negative_feedback_value = (note.negative_feedback or "").strip() or "Без изменений"
    total_ad = report["table_blocks"]["ad_total"]
    organic = report["table_organic"]
    search = report["table_blocks"]["search"]
    shelves = report["table_blocks"]["shelves"]
    manual = report["table_blocks"]["manual"]

    columns = [search, shelves, manual]
    active_columns = [has_metric_cell_data(cell) for cell in columns]

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

    def traffic_value(cell) -> str:
        if not has_metric_cell_data(cell):
            return ""
        total_unified_impressions = search.impressions + shelves.impressions
        if total_unified_impressions <= 0:
            return ""
        return format_percent(cell.traffic_share(total_unified_impressions))

    def derived_decimal(cell, value: Decimal | int | float | str | None) -> str:
        if not has_metric_cell_data(cell):
            return ""
        return format_decimal(value)

    def derived_percent(cell, value: Decimal | int | float | None) -> str:
        if not has_metric_cell_data(cell):
            return ""
        return format_percent(value)

    def format_ratio(
        numerator: Decimal | int | float,
        denominator: Decimal | int | float,
        *,
        scale: Decimal | int = 1,
        treat_zero_numerator_as_empty: bool = False,
    ) -> str:
        numerator_value = decimalize(numerator)
        denominator_value = decimalize(denominator)
        if denominator_value == 0:
            return "-"
        if treat_zero_numerator_as_empty and numerator_value == 0:
            return "-"
        return format_decimal(safe_divide(numerator_value, denominator_value) * decimalize(scale))

    def format_percent_ratio(
        numerator: Decimal | int | float,
        denominator: Decimal | int | float,
        *,
        treat_zero_numerator_as_empty: bool = False,
    ) -> str:
        numerator_value = decimalize(numerator)
        denominator_value = decimalize(denominator)
        if denominator_value == 0:
            return "-"
        if treat_zero_numerator_as_empty and numerator_value == 0:
            return "-"
        return format_percent(safe_divide(numerator_value, denominator_value) * 100)

    def derived_ratio_decimal(
        cell,
        numerator: Decimal | int | float,
        denominator: Decimal | int | float,
        *,
        scale: Decimal | int = 1,
        treat_zero_numerator_as_empty: bool = False,
    ) -> str:
        if not has_metric_cell_data(cell):
            return ""
        return format_ratio(
            numerator,
            denominator,
            scale=scale,
            treat_zero_numerator_as_empty=treat_zero_numerator_as_empty,
        )

    def derived_ratio_percent(
        cell,
        numerator: Decimal | int | float,
        denominator: Decimal | int | float,
        *,
        treat_zero_numerator_as_empty: bool = False,
    ) -> str:
        if not has_metric_cell_data(cell):
            return ""
        return format_percent_ratio(
            numerator,
            denominator,
            treat_zero_numerator_as_empty=treat_zero_numerator_as_empty,
        )

    overall_clicks = metrics.open_count if metrics else 0
    overall_carts = metrics.add_to_cart_count if metrics else 0
    overall_orders = metrics.order_count if metrics else 0
    overall_order_sum = decimalize(metrics.order_sum if metrics else 0)
    conversion_cart = safe_divide(decimalize(overall_carts) * 100, overall_clicks)
    conversion_order = safe_divide(decimalize(overall_orders) * 100, overall_carts)
    estimated_buyout_overall = estimate_buyout_sum(economics, overall_order_sum)
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
    spp_delta_text = spp_delta_value or spp_delta_label

    rows = [
        ["", f"{report['stock_date']:%d.%m.%Y}", "", "", "", ""],
        ["Тип рекламной кампании", "Единая ставка", "", "Руч. поиск", "Общая", "ОРГ"],
        ["Зоны показов", "Поиск", "Полки", "", "", ""],
        ["Доля трафика (%)", traffic_value(search), traffic_value(shelves), "", "100%", "-"],
        ["Затраты (руб)", *pick("spend"), format_decimal(total_ad.spend), "-"],
        ["Показы ", *pick("impressions"), format_int(total_ad.impressions), "-"],
        ["CTR", derived_ratio_decimal(search, search.clicks, search.impressions, scale=100), derived_ratio_decimal(shelves, shelves.clicks, shelves.impressions, scale=100), derived_ratio_decimal(manual, manual.clicks, manual.impressions, scale=100), "-", "-"],
        ["CPM", derived_ratio_decimal(search, decimalize(search.spend) * 1000, search.impressions), derived_ratio_decimal(shelves, decimalize(shelves.spend) * 1000, shelves.impressions), derived_ratio_decimal(manual, decimalize(manual.spend) * 1000, manual.impressions), "-", "-"],
        ["CPC", derived_ratio_decimal(search, search.spend, search.clicks), derived_ratio_decimal(shelves, shelves.spend, shelves.clicks), derived_ratio_decimal(manual, manual.spend, manual.clicks), format_ratio(total_ad.spend, overall_clicks), "-"],
        ["Клики ", *pick("clicks"), format_int(overall_clicks), format_int(organic["open_count"])],
        ["Корзины ", *pick("carts"), format_int(overall_carts), format_int(organic["cart_count"])],
        ["Конверсия в корзину", "", "", "", format_percent_ratio(overall_carts, overall_clicks), ""],
        ["Заказы", *pick("orders"), format_int(overall_orders), format_int(organic["order_count"])],
        ["Конверсия в заказ", "", "", "", format_percent_ratio(overall_orders, overall_carts), ""],
        ["Заказы (руб.)", *pick("order_sum"), format_decimal(overall_order_sum), format_decimal(organic["order_sum"])],
        [
            "Выкупы ≈ (руб.)",
            derived_decimal(search, estimate_buyout_sum(economics, search.order_sum)),
            derived_decimal(shelves, estimate_buyout_sum(economics, shelves.order_sum)),
            "",
            format_decimal(estimated_buyout_overall),
            "-",
        ],
        [
            "Стоимость заказа",
            derived_ratio_decimal(search, search.spend, search.orders, treat_zero_numerator_as_empty=True),
            derived_ratio_decimal(shelves, shelves.spend, shelves.orders, treat_zero_numerator_as_empty=True),
            "",
            format_ratio(total_ad.spend, overall_orders, treat_zero_numerator_as_empty=True),
            "-",
        ],
        [
            "Стоимость корзины",
            derived_ratio_decimal(search, search.spend, search.carts, treat_zero_numerator_as_empty=True),
            derived_ratio_decimal(shelves, shelves.spend, shelves.carts, treat_zero_numerator_as_empty=True),
            "",
            format_ratio(total_ad.spend, overall_carts, treat_zero_numerator_as_empty=True),
            "-",
        ],
        [
            "ДРР от заказов (%)",
            derived_ratio_percent(search, search.spend, search.order_sum, treat_zero_numerator_as_empty=True),
            derived_ratio_percent(shelves, shelves.spend, shelves.order_sum, treat_zero_numerator_as_empty=True),
            "",
            format_percent_ratio(total_ad.spend, overall_order_sum, treat_zero_numerator_as_empty=True),
            "-",
        ],
        [
            "ДРР от продаж ≈ (%)",
            derived_ratio_percent(
                search,
                search.spend,
                estimate_buyout_sum(economics, search.order_sum),
                treat_zero_numerator_as_empty=True,
            ),
            derived_ratio_percent(
                shelves,
                shelves.spend,
                estimate_buyout_sum(economics, shelves.order_sum),
                treat_zero_numerator_as_empty=True,
            ),
            "",
            format_percent_ratio(total_ad.spend, estimated_buyout_overall, treat_zero_numerator_as_empty=True),
            "-",
        ],
        ["Прибыль", format_decimal(profit_overall), "", "", "", ""],
        ["Процент выкупа %", format_percent(percent_points(economics.buyout_percent)), "", "", "", ""],
        ["Себестоимость", format_decimal(economics.unit_cost), "", "", "", ""],
        ["Логистика", format_decimal(economics.logistics_cost), "", "", "", ""],
        ["", "Остатки:", "", "", "", ""],
        ["", "Остатки на складах WB", "", "", format_int(stock.total_stock if stock else 0), ""],
        ["", "Едут к клиенту", "", "", format_int(stock.in_way_to_client if stock else 0), ""],
        ["", "Возвращаются на склад", "", "", format_int(stock.in_way_from_client if stock else 0), ""],
        ["", "Ср. кол-во заказов/день", "", "", format_decimal(report["avg_orders_per_day"]), ""],
        ["", "Ср. убыль остатков/день", "", "", format_optional_decimal(report["avg_stock_drop_per_day"]), ""],
        ["", "Дней до АУТА", "", "", format_optional_decimal(report["days_until_zero_from_stock_drop"]), ""],
        ["", "", "", "", "", ""],
        ["", "", "", "", "", ""],
        ["", "Обзор:", "", "", "", ""],
        ["", "СПП", "", format_optional_percent(note.spp_percent), "", spp_delta_text],
        ["", "Цена WBSELLER (наша)", "", "", "", format_optional_decimal(note.seller_price)],
        ["", "Цена WB (на сайте)", "", "", "", format_optional_decimal(note.wb_price)],
        ["", "Акция", "", "", "", promo_status_value],
        ["", "Негативные отзывы", "", "", "", negative_feedback_value],
        ["", "Действия:", "", "", "", ""],
        ["", "Включили рекламу?", "", "", "Да" if (note.unified_enabled or note.manual_search_enabled or note.manual_shelves_enabled) else "Нет", ""],
        ["", "Меняли цену?(WBSeller)", "", "", "Да" if note.price_changed else "Нет", ""],
        ["Комментарий:", note.comment, "", "", "", ""],
    ]
    return rows
