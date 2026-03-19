from __future__ import annotations

from decimal import Decimal

from monitoring.models import CampaignMonitoringGroup, CampaignZone
from monitoring.services.reports import (
    MetricCell,
    decimalize,
    estimate_buyout_sum,
    estimate_profit,
    safe_divide,
)


def format_decimal(value: Decimal | int | float | str | None) -> str:
    if value in (None, ""):
        return ""
    number = decimalize(value)
    return f"{number.quantize(Decimal('0.01'))}".replace(".", ",")


def format_percent(value: Decimal | int | float | None) -> str:
    if value is None:
        return "-"
    return f"{decimalize(value).quantize(Decimal('0.01'))}%".replace(".", ",")


def format_int(value: int | float | Decimal | None) -> str:
    if value is None:
        return ""
    return str(int(decimalize(value)))


def cell_for(report: dict, group: str, zone: str) -> MetricCell:
    return report["cells"].get((group, zone), MetricCell())


def exporter_rows(report: dict) -> list[list[str]]:
    product = report["product"]
    economics = report["economics"]
    metrics = report["metrics"]
    stock = report["stock"]
    note = report["note"]
    total_ad = report["total_ad"]
    organic = report["organic"]
    traffic_totals = report["traffic_totals"]

    unified_search = cell_for(report, CampaignMonitoringGroup.UNIFIED, CampaignZone.SEARCH)
    unified_shelves = cell_for(report, CampaignMonitoringGroup.UNIFIED, CampaignZone.RECOMMENDATION)
    unified_catalog = cell_for(report, CampaignMonitoringGroup.UNIFIED, CampaignZone.CATALOG)
    manual_search = cell_for(report, CampaignMonitoringGroup.MANUAL_SEARCH, CampaignZone.SEARCH)
    manual_shelves = cell_for(report, CampaignMonitoringGroup.MANUAL_SHELVES, CampaignZone.RECOMMENDATION)

    columns = [unified_search, unified_shelves, unified_catalog, manual_search, manual_shelves]

    def pick(metric_name: str) -> list[str]:
        values: list[str] = []
        for cell in columns:
            value = getattr(cell, metric_name)
            if isinstance(value, Decimal):
                values.append(format_decimal(value))
            else:
                values.append(format_int(value))
        return values

    def traffic_for(cell: MetricCell, group: str) -> str:
        return format_percent(cell.traffic_share(traffic_totals.get(group, 0)))

    estimated_buyout_overall = estimate_buyout_sum(economics, decimalize(metrics.order_sum if metrics else 0))
    profit_overall = estimate_profit(
        economics,
        metrics.order_count if metrics else 0,
        decimalize(metrics.order_sum if metrics else 0),
        decimalize(total_ad.spend),
    )

    return [
        ["", "", "ОБРАЗЕЦ", "", "", "", "", "", ""],
        ["Тип рекламной кампании", "", "Единая ставка", "", "", "Руч. Поиск", "Руч. Полки", "Общая", "ОРГ"],
        ["Зоны показов", "", "Поиск", "Полки", "Каталог", "Поиск", "Полки", "Общая", "ОРГ"],
        ["Доля трафика (%)", "", traffic_for(unified_search, CampaignMonitoringGroup.UNIFIED), traffic_for(unified_shelves, CampaignMonitoringGroup.UNIFIED), traffic_for(unified_catalog, CampaignMonitoringGroup.UNIFIED), traffic_for(manual_search, CampaignMonitoringGroup.MANUAL_SEARCH), traffic_for(manual_shelves, CampaignMonitoringGroup.MANUAL_SHELVES), "-", "-"],
        ["Затраты (руб)", "", *pick("spend"), format_decimal(total_ad.spend), "-"],
        ["Показы", "", *pick("impressions"), format_int(total_ad.impressions), "-"],
        ["CTR", "", format_percent(unified_search.ctr), format_percent(unified_shelves.ctr), format_percent(unified_catalog.ctr), format_percent(manual_search.ctr), format_percent(manual_shelves.ctr), "-", "-"],
        ["CPM", "", format_decimal(unified_search.cpm), format_decimal(unified_shelves.cpm), format_decimal(unified_catalog.cpm), format_decimal(manual_search.cpm), format_decimal(manual_shelves.cpm), "-", "-"],
        ["CPC", "", format_decimal(unified_search.cpc), format_decimal(unified_shelves.cpc), format_decimal(unified_catalog.cpc), format_decimal(manual_search.cpc), format_decimal(manual_shelves.cpc), "-", "-"],
        ["Клики", "", *pick("clicks"), format_int(total_ad.clicks), format_int(organic["open_count"])],
        ["Корзины", "", *pick("carts"), format_int(total_ad.carts), format_int(organic["cart_count"])],
        ["Заказы", "", *pick("orders"), format_int(total_ad.orders), format_int(organic["order_count"])],
        ["Заказы (руб.)", "", *pick("order_sum"), format_decimal(total_ad.order_sum), format_decimal(organic["order_sum"])],
        ["Выкупы ≈ (руб.)", "", format_decimal(estimate_buyout_sum(economics, unified_search.order_sum)), format_decimal(estimate_buyout_sum(economics, unified_shelves.order_sum)), format_decimal(estimate_buyout_sum(economics, unified_catalog.order_sum)), format_decimal(estimate_buyout_sum(economics, manual_search.order_sum)), format_decimal(estimate_buyout_sum(economics, manual_shelves.order_sum)), format_decimal(estimated_buyout_overall), "-"],
        ["Стоимость заказа", "", format_decimal(unified_search.order_cost), format_decimal(unified_shelves.order_cost), format_decimal(unified_catalog.order_cost), format_decimal(manual_search.order_cost), format_decimal(manual_shelves.order_cost), format_decimal(total_ad.order_cost), "-"],
        ["Стоимость корзины", "", format_decimal(unified_search.cart_cost), format_decimal(unified_shelves.cart_cost), format_decimal(unified_catalog.cart_cost), format_decimal(manual_search.cart_cost), format_decimal(manual_shelves.cart_cost), format_decimal(total_ad.cart_cost), "-"],
        ["ДРР от заказов (%)", "", format_percent(safe_divide(unified_search.spend * 100, unified_search.order_sum)), format_percent(safe_divide(unified_shelves.spend * 100, unified_shelves.order_sum)), format_percent(safe_divide(unified_catalog.spend * 100, unified_catalog.order_sum)), format_percent(safe_divide(manual_search.spend * 100, manual_search.order_sum)), format_percent(safe_divide(manual_shelves.spend * 100, manual_shelves.order_sum)), format_percent(safe_divide(total_ad.spend * 100, total_ad.order_sum)), "-"],
        ["ДРР от продаж ≈ (%)", "", format_percent(safe_divide(unified_search.spend * 100, estimate_buyout_sum(economics, unified_search.order_sum))), format_percent(safe_divide(unified_shelves.spend * 100, estimate_buyout_sum(economics, unified_shelves.order_sum))), format_percent(safe_divide(unified_catalog.spend * 100, estimate_buyout_sum(economics, unified_catalog.order_sum))), format_percent(safe_divide(manual_search.spend * 100, estimate_buyout_sum(economics, manual_search.order_sum))), format_percent(safe_divide(manual_shelves.spend * 100, estimate_buyout_sum(economics, manual_shelves.order_sum))), format_percent(safe_divide(total_ad.spend * 100, estimated_buyout_overall)), "-"],
        ["Прибыль (без налогов и костов вне ВБ)", "", format_decimal(estimate_profit(economics, unified_search.orders, unified_search.order_sum, unified_search.spend)), format_decimal(estimate_profit(economics, unified_shelves.orders, unified_shelves.order_sum, unified_shelves.spend)), format_decimal(estimate_profit(economics, unified_catalog.orders, unified_catalog.order_sum, unified_catalog.spend)), format_decimal(estimate_profit(economics, manual_search.orders, manual_search.order_sum, manual_search.spend)), format_decimal(estimate_profit(economics, manual_shelves.orders, manual_shelves.order_sum, manual_shelves.spend)), format_decimal(profit_overall), ""],
        ["Процент выкупа %", "", format_percent(economics.buyout_percent), "", "", "", "", "", ""],
        ["Себестоимость", "", format_decimal(economics.unit_cost), "", "", "", "", "", ""],
        ["Логистика", "", format_decimal(economics.logistics_cost), "", "", "", "", "", ""],
        ["Себес", "", "Остатки:", "", "", "", "", "", ""],
        ["Логистика", "", "", "", "", "", "", "", ""],
        ["", "", "", "", "", "", "", "", ""],
        ["", "", "", "", "", "", "", "", ""],
        ["", "", "", "", "", "", "", "", ""],
        ["", "", "Остатки на складах WB", "", "", "", "", format_int(stock.total_stock if stock else 0), ""],
        ["", "", "Едут к клиенту", "", "", "", "", format_int(stock.in_way_to_client if stock else 0), ""],
        ["", "", "Возвращаются на склад", "", "", "", "", format_int(stock.in_way_from_client if stock else 0), ""],
        ["", "", "Ср. кол-во заказов/день", "", "", "", "", format_decimal(report["avg_orders_per_day"]), ""],
        ["", "", "Дней до распродажи в 0", "", "", "", "", format_decimal(report["days_until_zero"]), ""],
        ["Ключи", "", "Частота", "поз. ОРГ", "", "", "", "поз. БУСТ", "CTR (%)"],
        [product.primary_keyword or "", "", "", "", "", "", "", "", ""],
        [product.secondary_keyword or "", "", "", "", "", "", "", "", ""],
        ["", "", "Обзор:", "", "", "", "", "", ""],
        ["", "", "СПП", format_percent(note.spp_percent), "", "", "Упал на", "", ""],
        ["", "", "Цена WBSELLER (наша)", "", "", "", format_decimal(note.seller_price), "", ""],
        ["", "", "Цена WB (на сайте)", "", "", "", format_decimal(note.wb_price), "", ""],
        ["", "", "Акция", "", "", "", note.promo_status, "", ""],
        ["", "", "Негативные отзывы", "", "", "", note.negative_feedback, "", ""],
        ["", "", "Действия:", "", "", "", "", "", ""],
        ["", "", "Включили РК единая ставка?", "", "", "", "Да" if note.unified_enabled else "Нет", "", ""],
        ["", "", "Включили РК руч. поиск?", "", "", "", "Да" if note.manual_search_enabled else "Нет", "", ""],
        ["", "", "Включили РК руч. полки?", "", "", "", "Да" if note.manual_shelves_enabled else "Нет", "", ""],
        ["", "", "Меняли цену? (WBSeller)", "", "", "", "Да" if note.price_changed else "Нет", "", ""],
        ["", "", "Комментарии:", "", "", "", "", "", ""],
        ["", "", note.comment, "", "", "", "", "", ""],
    ]
