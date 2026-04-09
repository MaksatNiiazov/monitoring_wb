from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from io import BytesIO
import re
from typing import Any

from django.utils import timezone
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

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
)
from monitoring.services.config import get_monitoring_settings
from monitoring.services.exporters import exporter_rows
from monitoring.services.reports import (
    MetricCell,
    ResolvedEconomics,
    build_dashboard_context,
    build_product_report,
    decimalize,
    has_metric_cell_data,
    normalize_warehouse_name,
)

BASE_BLOCK_HEIGHT = 43
BASE_KEYWORD_ROWS = 0
BLOCK_WIDTH = 7
BLOCK_GAP = 1
SHEETS_MAX_TITLE_LENGTH = 100


@dataclass
class MonitoringSheetPayload:
    title: str
    rows: list[list[Any]]
    kind: str = "product"
    product_id: int | None = None
    block_dates: list[date] | None = None


THIN_SIDE = Side(style="thin", color="D4D8E4")
THIN_BORDER = Border(left=THIN_SIDE, right=THIN_SIDE, top=THIN_SIDE, bottom=THIN_SIDE)
BLOCK_EDGE_SIDE = Side(style="medium", color="9AA6BF")
BLOCK_LEFT_BORDER = Border(left=BLOCK_EDGE_SIDE, right=THIN_SIDE, top=THIN_SIDE, bottom=THIN_SIDE)
BLOCK_RIGHT_BORDER = Border(left=THIN_SIDE, right=BLOCK_EDGE_SIDE, top=THIN_SIDE, bottom=THIN_SIDE)
NO_BORDER = Border()
HEADER_FILL = PatternFill("solid", fgColor="15324D")
SUBHEADER_FILL = PatternFill("solid", fgColor="E7EEF8")
SECTION_FILL = PatternFill("solid", fgColor="EEF3F7")
FORMULA_FILL = PatternFill("solid", fgColor="F1EAFE")
META_FILL = PatternFill("solid", fgColor="F7F9FC")
BLOCK_SEPARATOR_FILL = PatternFill("solid", fgColor="DDE5F2")
HEADER_FONT = Font(color="FFFFFF", bold=True)
SECTION_FONT = Font(color="15324D", bold=True)


def normalize_title(value: str) -> str:
    normalized = re.sub(r"[\\/?*\[\]:]", "_", value).strip()
    return normalized[:SHEETS_MAX_TITLE_LENGTH] or "Sheet1"


def monitoring_sheet_title(product: Product) -> str:
    base = product.vendor_code or product.title or f"WB {product.nm_id}"
    return normalize_title(base)


def _money(value: Decimal | int | float | str | None) -> float:
    return float(decimalize(value).quantize(Decimal("0.01")))


def _percent_points(value: Decimal | int | float | str | None) -> Decimal:
    number = decimalize(value)
    if number == 0:
        return number
    if Decimal("-1") <= number <= Decimal("1"):
        return number * Decimal("100")
    return number


def _fraction(value: Decimal | int | float | str | None) -> float:
    return float((_percent_points(value) / Decimal("100")).quantize(Decimal("0.0001")))


def _int(value: Decimal | int | float | str | None) -> int:
    return int(decimalize(value))


def _bool_label(value: bool) -> str:
    return "Да" if value else "Нет"


def _optional_money(value: Decimal | int | float | str | None) -> float | str:
    number = decimalize(value)
    if number == 0:
        return ""
    return _money(number)


def _optional_fraction(value: Decimal | int | float | str | None) -> float | str:
    number = decimalize(value)
    if number == 0:
        return ""
    return _fraction(number)


def _keyword_int(value: int | None, *, has_data: bool) -> int | str:
    if value is None and not has_data:
        return ""
    return _int(value or 0)


def _keyword_money(value: Decimal | int | float | str | None, *, has_data: bool) -> float | str:
    if value is None and not has_data:
        return ""
    return _money(value or 0)


def _keyword_offset(keyword_rows: list[dict[str, Any]]) -> int:
    return 0


def _block_height(keyword_rows: list[dict[str, Any]]) -> int:
    return BASE_BLOCK_HEIGHT + _keyword_offset(keyword_rows)


def _normalize_keyword_rows_count(reports: list[dict[str, Any]]) -> None:
    if not reports:
        return
    target = max(len(report.get("keyword_rows") or []) for report in reports)
    for report in reports:
        keyword_rows = report.get("keyword_rows") or []
        missing = max(0, target - len(keyword_rows))
        for _ in range(missing):
            keyword_rows.append(
                {
                    "query_text": "",
                    "has_data": False,
                    "frequency": None,
                    "organic_position": None,
                    "boosted_position": None,
                    "boosted_ctr": None,
                }
            )
        report["keyword_rows"] = keyword_rows


def _cell_ref(*, start_row: int, start_col: int, relative_row: int, relative_col: int) -> str:
    return f"{get_column_letter(start_col + relative_col - 1)}{start_row + relative_row - 1}"


def _cell_formula(
    *,
    start_row: int,
    start_col: int,
    template: str,
    refs: dict[str, tuple[int, int]],
) -> str:
    resolved = {
        key: _cell_ref(start_row=start_row, start_col=start_col, relative_row=rel_row, relative_col=rel_col)
        for key, (rel_row, rel_col) in refs.items()
    }
    return "=" + template.format(**resolved)


def _metric_cell(report: dict[str, Any], group: str, zone: str) -> MetricCell:
    return report["cells"].get((group, zone), MetricCell())


def _block_header(report: dict[str, Any]) -> str:
    stock_date = report["stock_date"]
    return f"{stock_date:%d.%m.%Y}"


def _spp_delta_parts(
    report: dict[str, Any],
    previous_report: dict[str, Any] | None,
) -> tuple[str, float | str]:
    current = decimalize(report["note"].spp_percent)
    if current == 0:
        return ("", "")
    if previous_report is None:
        return ("Без изменений", "")
    previous = decimalize(previous_report["note"].spp_percent)
    delta = current - previous
    if delta == 0:
        return ("Без изменений", "")
    label = "Вырос на" if delta > 0 else "Упал на"
    return (label, _optional_fraction(delta.copy_abs()))


def build_day_block(
    report: dict[str, Any],
    *,
    previous_report: dict[str, Any] | None = None,
    start_row: int = 1,
    start_col: int = 1,
) -> list[list[Any]]:
    economics = report["economics"]
    note = report["note"]
    stock = report["stock"]
    metrics = report["metrics"]
    promo_status_value = (note.promo_status or "").strip() or "Не участвуем"
    negative_feedback_value = (note.negative_feedback or "").strip() or "Без изменений"

    search = report["table_blocks"]["search"]
    shelves = report["table_blocks"]["shelves"]
    catalog = report["table_blocks"]["catalog"]
    manual = report["table_blocks"]["manual"]

    columns = [search, shelves, catalog, manual]
    active_columns = [has_metric_cell_data(cell) for cell in columns]

    def pick_numbers(metric_name: str) -> list[int | float | str]:
        values: list[int | float | str] = []
        for index, cell in enumerate(columns):
            if not active_columns[index]:
                values.append("")
                continue
            value = getattr(cell, metric_name)
            if isinstance(value, Decimal):
                values.append(_money(value))
            else:
                values.append(_int(value))
        return values

    def traffic_value(relative_col: int) -> float | str:
        if relative_col == 5:
            return ""
        if relative_col == 6:
            return 1.0
        if relative_col == 7:
            return "-"
        cell = columns[relative_col - 2]
        if not has_metric_cell_data(cell):
            return ""
        unified_total = search.impressions + shelves.impressions + catalog.impressions
        if unified_total <= 0:
            return ""
        return _fraction(decimalize(cell.impressions) * Decimal("100") / decimalize(unified_total))

    def row_after_keywords(base_row: int) -> int:
        return base_row

    seller_price_ref = _cell_ref(
        start_row=start_row,
        start_col=start_col,
        relative_row=row_after_keywords(36),
        relative_col=7,
    )
    buyout_percent_ref = _cell_ref(start_row=start_row, start_col=start_col, relative_row=22, relative_col=2)
    unit_cost_ref = _cell_ref(start_row=start_row, start_col=start_col, relative_row=23, relative_col=2)
    logistics_ref = _cell_ref(start_row=start_row, start_col=start_col, relative_row=24, relative_col=2)

    def row_value_ref(relative_row: int, relative_col: int) -> str:
        return _cell_ref(
            start_row=start_row,
            start_col=start_col,
            relative_row=row_after_keywords(relative_row),
            relative_col=relative_col,
        )

    def buyout_formula(relative_col: int) -> str:
        return f"={row_value_ref(15, relative_col)}*{buyout_percent_ref}"

    def cost_per_order_formula(relative_col: int) -> str:
        return safe_divide_formula(
            row_value_ref(5, relative_col),
            row_value_ref(13, relative_col),
            treat_zero_numerator_as_empty=True,
        )

    def cost_per_cart_formula(relative_col: int) -> str:
        return safe_divide_formula(
            row_value_ref(5, relative_col),
            row_value_ref(11, relative_col),
            treat_zero_numerator_as_empty=True,
        )

    def ratio_formula(relative_col: int, denominator_row: int) -> str:
        return safe_divide_formula(
            row_value_ref(5, relative_col),
            row_value_ref(denominator_row, relative_col),
            scale=100,
            treat_zero_numerator_as_empty=True,
        )

    def safe_divide_formula(
        numerator_ref: str,
        denominator_ref: str,
        *,
        scale: int | None = None,
        fallback: str = '"-"',
        treat_zero_numerator_as_empty: bool = False,
    ) -> str:
        expression = f"{numerator_ref}/{denominator_ref}"
        if scale is not None:
            expression = f"({expression})*{scale}"
        conditions = [f"{denominator_ref}=\"\"", f"{denominator_ref}=0"]
        if treat_zero_numerator_as_empty:
            conditions.append(f"{numerator_ref}=\"\"")
            conditions.append(f"{numerator_ref}=0")
        joined_conditions = ",".join(conditions)
        return f'=IFERROR(IF(OR({joined_conditions}),{fallback},{expression}),{fallback})'

    def overall_profit_formula() -> str:
        buyout_fraction = f"IF({buyout_percent_ref}>1,{buyout_percent_ref}/100,{buyout_percent_ref})"
        return (
            f"=IFERROR(IF({seller_price_ref}=0,0,"
            f"(({seller_price_ref}-{unit_cost_ref}-({seller_price_ref}*{row_value_ref(20, 6)}))-"
            f"({seller_price_ref}*25/100)-(({logistics_ref}/{buyout_fraction})-50))*"
            f"({row_value_ref(13, 6)}*{buyout_fraction})),0)"
        )

    def maybe_formula(relative_col: int, formula: str) -> str:
        return formula if active_columns[relative_col - 2] else ""

    def average_orders_formula() -> float | str:
        if stock and decimalize(stock.avg_orders_per_day):
            return _money(stock.avg_orders_per_day)
        block_span = BLOCK_WIDTH + BLOCK_GAP
        refs: list[str] = []
        for offset in range(7):
            block_start_col = start_col - offset * block_span
            if block_start_col < 1:
                break
            refs.append(
                _cell_ref(
                    start_row=start_row,
                    start_col=block_start_col,
                    relative_row=13,
                    relative_col=6,
                )
            )
        if len(refs) <= 1:
            return _money(report["avg_orders_per_day"])
        return f"=(({'+'.join(refs)})/{len(refs)})"

    def average_stock_drop_value() -> float | str:
        return _optional_money(report["avg_stock_drop_per_day"])

    def days_until_zero_value() -> float | str:
        return _optional_money(report["days_until_zero_from_stock_drop"])

    spp_delta_label, spp_delta_value = _spp_delta_parts(report, previous_report)
    spp_delta_text = spp_delta_value or spp_delta_label

    rows: list[list[Any]] = [
        ["", _block_header(report), "", "", "", "", ""],
        ["Тип рекламной кампании", "Единая ставка", "", "", "Руч. поиск", "Общая", "ОРГ"],
        ["Зоны показов", "Поиск", "Полки", "Каталог", "", "", ""],
        ["Доля трафика (%)", traffic_value(2), traffic_value(3), traffic_value(4), "", 1.0, "-"],
        ["Затраты (руб)", *pick_numbers("spend"), f"=SUM({row_value_ref(5, 2)}:{row_value_ref(5, 5)})", "-"],
        ["Показы ", *pick_numbers("impressions"), f"=SUM({row_value_ref(6, 2)}:{row_value_ref(6, 5)})", "-"],
        [
            "CTR",
            maybe_formula(2, safe_divide_formula(row_value_ref(10, 2), row_value_ref(6, 2), scale=100)),
            maybe_formula(3, safe_divide_formula(row_value_ref(10, 3), row_value_ref(6, 3), scale=100)),
            maybe_formula(4, safe_divide_formula(row_value_ref(10, 4), row_value_ref(6, 4), scale=100)),
            maybe_formula(5, safe_divide_formula(row_value_ref(10, 5), row_value_ref(6, 5), scale=100)),
            safe_divide_formula(f"SUM({row_value_ref(10, 2)}:{row_value_ref(10, 5)})", row_value_ref(6, 6), scale=100),
            "-",
        ],
        [
            "CPM",
            maybe_formula(2, safe_divide_formula(f"{row_value_ref(5, 2)}*1000", row_value_ref(6, 2))),
            maybe_formula(3, safe_divide_formula(f"{row_value_ref(5, 3)}*1000", row_value_ref(6, 3))),
            maybe_formula(4, safe_divide_formula(f"{row_value_ref(5, 4)}*1000", row_value_ref(6, 4))),
            maybe_formula(5, safe_divide_formula(f"{row_value_ref(5, 5)}*1000", row_value_ref(6, 5))),
            safe_divide_formula(f"{row_value_ref(5, 6)}*1000", row_value_ref(6, 6)),
            "-",
        ],
        [
            "CPC",
            maybe_formula(2, safe_divide_formula(row_value_ref(5, 2), row_value_ref(10, 2))),
            maybe_formula(3, safe_divide_formula(row_value_ref(5, 3), row_value_ref(10, 3))),
            maybe_formula(4, safe_divide_formula(row_value_ref(5, 4), row_value_ref(10, 4))),
            maybe_formula(5, safe_divide_formula(row_value_ref(5, 5), row_value_ref(10, 5))),
            safe_divide_formula(row_value_ref(5, 6), f"SUM({row_value_ref(10, 2)}:{row_value_ref(10, 5)})"),
            "-",
        ],
        ["Клики ", *pick_numbers("clicks"), _int(metrics.open_count if metrics else 0), f'={row_value_ref(10, 6)}-SUM({row_value_ref(10, 2)}:{row_value_ref(10, 5)})'],
        ["Корзины ", *pick_numbers("carts"), _int(metrics.add_to_cart_count if metrics else 0), f'={row_value_ref(11, 6)}-SUM({row_value_ref(11, 2)}:{row_value_ref(11, 5)})'],
        ["Конверсия в корзину", "", "", "", "", safe_divide_formula(row_value_ref(11, 6), row_value_ref(10, 6), scale=100), ""],
        ["Заказы", *pick_numbers("orders"), _int(metrics.order_count if metrics else 0), f'={row_value_ref(13, 6)}-SUM({row_value_ref(13, 2)}:{row_value_ref(13, 5)})'],
        ["Конверсия в заказ", "", "", "", "", safe_divide_formula(row_value_ref(13, 6), row_value_ref(11, 6), scale=100), ""],
        ["Заказы (руб.)", *pick_numbers("order_sum"), _money(metrics.order_sum if metrics else 0), f'={row_value_ref(15, 6)}-SUM({row_value_ref(15, 2)}:{row_value_ref(15, 5)})'],
        ["Выкупы ≈ (руб.)", maybe_formula(2, buyout_formula(2)), maybe_formula(3, buyout_formula(3)), maybe_formula(4, buyout_formula(4)), maybe_formula(5, buyout_formula(5)), buyout_formula(6), "-"],
        ["Стоимость заказа", maybe_formula(2, cost_per_order_formula(2)), maybe_formula(3, cost_per_order_formula(3)), maybe_formula(4, cost_per_order_formula(4)), maybe_formula(5, cost_per_order_formula(5)), cost_per_order_formula(6), "-"],
        ["Стоимость корзины", maybe_formula(2, cost_per_cart_formula(2)), maybe_formula(3, cost_per_cart_formula(3)), maybe_formula(4, cost_per_cart_formula(4)), maybe_formula(5, cost_per_cart_formula(5)), cost_per_cart_formula(6), "-"],
        ["ДРР от заказов (%)", maybe_formula(2, ratio_formula(2, 15)), maybe_formula(3, ratio_formula(3, 15)), maybe_formula(4, ratio_formula(4, 15)), maybe_formula(5, ratio_formula(5, 15)), ratio_formula(6, 15), "-"],
        ["ДРР от продаж ≈ (%)", maybe_formula(2, ratio_formula(2, 16)), maybe_formula(3, ratio_formula(3, 16)), maybe_formula(4, ratio_formula(4, 16)), maybe_formula(5, ratio_formula(5, 16)), ratio_formula(6, 16), "-"],
        ["Прибыль", overall_profit_formula(), "", "", "", "", ""],
        ["Процент выкупа %", _fraction(economics.buyout_percent), "", "", "", "", ""],
        ["Себестоимость", _money(economics.unit_cost), "", "", "", "", ""],
        ["Логистика", _money(economics.logistics_cost), "", "", "", "", ""],
        ["", "Остатки:", "", "", "", "", ""],
        ["", "Остатки на складах WB", "", "", "", _int(stock.total_stock if stock else 0), ""],
        ["", "Едут к клиенту", "", "", "", _int(stock.in_way_to_client if stock else 0), ""],
        ["", "Возвращаются на склад", "", "", "", _int(stock.in_way_from_client if stock else 0), ""],
        ["", "Ср. кол-во заказов/день", "", "", "", average_orders_formula(), ""],
        ["", "Ср. убыль остатков/день", "", "", "", average_stock_drop_value(), ""],
        ["", "Дней до АУТА", "", "", "", days_until_zero_value(), ""],
        ["", "", "", "", "", "", ""],
        ["", "", "", "", "", "", ""],
    ]
    rows.extend(
        [
            ["", "Обзор:", "", "", "", "", ""],
            ["", "СПП", "", "", "", _optional_fraction(note.spp_percent), spp_delta_text],
            ["", "Цена WBSELLER (наша)", "", "", "", "", _optional_money(note.seller_price)],
            ["", "Цена WB (на сайте)", "", "", "", "", _optional_money(note.wb_price)],
            ["", "Акция", "", "", "", "", promo_status_value],
            ["", "Негативные отзывы", "", "", "", "", negative_feedback_value],
            ["", "Действия:", "", "", "", "", ""],
            ["", "Включили рекламу?", "", "", "", _bool_label(note.unified_enabled or note.manual_search_enabled or note.manual_shelves_enabled or getattr(note, "manual_catalog_enabled", False)), ""],
            ["", "Меняли цену?(WBSeller)", "", "", "", _bool_label(note.price_changed), ""],
            ["Комментарий:", note.comment, "", "", "", "", ""],
        ]
    )
    return rows

    rows: list[list[Any]] = [
        ["", _block_header(report), "", "", "", "", ""],
        ["Тип рекламной кампании", "Единая ставка", "", "", "Руч. поиск", "Общая", "ОРГ"],
        ["Зоны показов", "Поиск", "Полки", "Каталог", "", "", ""],
        ["Доля трафика (%)", traffic_value(2), traffic_value(3), traffic_value(4), "", 1.0, "-"],
        ["Затраты (руб)", *pick_numbers("spend"), f"=SUM({row_value_ref(5, 2)}:{row_value_ref(5, 5)})", "-"],
        ["Показы ", *pick_numbers("impressions"), f"=SUM({row_value_ref(6, 2)}:{row_value_ref(6, 5)})", "-"],
        ["CTR", maybe_formula(2, safe_divide_formula(row_value_ref(10, 2), row_value_ref(6, 2), scale=100)), maybe_formula(3, safe_divide_formula(row_value_ref(10, 3), row_value_ref(6, 3), scale=100)), maybe_formula(4, safe_divide_formula(row_value_ref(10, 4), row_value_ref(6, 4), scale=100)), maybe_formula(5, safe_divide_formula(row_value_ref(10, 5), row_value_ref(6, 5), scale=100)), "-", "-"],
        ["CPM", maybe_formula(2, safe_divide_formula(f"{row_value_ref(5, 2)}*1000", row_value_ref(6, 2))), maybe_formula(3, safe_divide_formula(f"{row_value_ref(5, 3)}*1000", row_value_ref(6, 3))), maybe_formula(4, safe_divide_formula(f"{row_value_ref(5, 4)}*1000", row_value_ref(6, 4))), maybe_formula(5, safe_divide_formula(f"{row_value_ref(5, 5)}*1000", row_value_ref(6, 5))), "-", "-"],
        ["CPC", maybe_formula(2, safe_divide_formula(row_value_ref(5, 2), row_value_ref(10, 2))), maybe_formula(3, safe_divide_formula(row_value_ref(5, 3), row_value_ref(10, 3))), maybe_formula(4, safe_divide_formula(row_value_ref(5, 4), row_value_ref(10, 4))), maybe_formula(5, safe_divide_formula(row_value_ref(5, 5), row_value_ref(10, 5))), safe_divide_formula(row_value_ref(5, 6), f"SUM({row_value_ref(10, 2)}:{row_value_ref(10, 5)})"), "-"],
        ["Клики ", *pick_numbers("clicks"), _int(metrics.open_count if metrics else 0), f'={row_value_ref(10, 6)}-SUM({row_value_ref(10, 2)}:{row_value_ref(10, 5)})'],
        ["Корзины ", *pick_numbers("carts"), _int(metrics.add_to_cart_count if metrics else 0), f'={row_value_ref(11, 6)}-SUM({row_value_ref(11, 2)}:{row_value_ref(11, 5)})'],
        ["Конверсия в корзину", "", "", "", "", safe_divide_formula(row_value_ref(11, 6), row_value_ref(10, 6), scale=100), ""],
        ["Заказы", *pick_numbers("orders"), _int(metrics.order_count if metrics else 0), f'={row_value_ref(13, 6)}-SUM({row_value_ref(13, 2)}:{row_value_ref(13, 5)})'],
        ["Конверсия в заказ", "", "", "", "", safe_divide_formula(row_value_ref(13, 6), row_value_ref(11, 6), scale=100), ""],
        ["Заказы (руб.)", *pick_numbers("order_sum"), _money(metrics.order_sum if metrics else 0), f'={row_value_ref(15, 6)}-SUM({row_value_ref(15, 2)}:{row_value_ref(15, 5)})'],
        ["Выкупы ≈ (руб.)", maybe_formula(2, buyout_formula(2)), maybe_formula(3, buyout_formula(3)), maybe_formula(4, buyout_formula(4)), maybe_formula(5, buyout_formula(5)), buyout_formula(6), "-"],
        ["Стоимость заказа", maybe_formula(2, cost_per_order_formula(2)), maybe_formula(3, cost_per_order_formula(3)), maybe_formula(4, cost_per_order_formula(4)), maybe_formula(5, cost_per_order_formula(5)), cost_per_order_formula(6), "-"],
        ["Стоимость корзины", maybe_formula(2, cost_per_cart_formula(2)), maybe_formula(3, cost_per_cart_formula(3)), maybe_formula(4, cost_per_cart_formula(4)), maybe_formula(5, cost_per_cart_formula(5)), cost_per_cart_formula(6), "-"],
        ["ДРР от заказов (%)", maybe_formula(2, ratio_formula(2, 15)), maybe_formula(3, ratio_formula(3, 15)), maybe_formula(4, ratio_formula(4, 15)), maybe_formula(5, ratio_formula(5, 15)), ratio_formula(6, 15), "-"],
        ["ДРР от продаж ≈ (%)", maybe_formula(2, ratio_formula(2, 16)), maybe_formula(3, ratio_formula(3, 16)), maybe_formula(4, ratio_formula(4, 16)), maybe_formula(5, ratio_formula(5, 16)), ratio_formula(6, 16), "-"],
        ["Прибыль", overall_profit_formula(), "", "", "", "", ""],
        ["Процент выкупа %", _fraction(economics.buyout_percent), "", "", "", "", ""],
        ["Себестоимость", _money(economics.unit_cost), "", "", "", "", ""],
        ["Логистика", _money(economics.logistics_cost), "", "", "", "", ""],
        ["", "Остатки:", "", "", "", "", ""],
        ["", "Остатки на складах WB", "", "", "", _int(stock.total_stock if stock else 0), ""],
        ["", "Едут к клиенту", "", "", "", _int(stock.in_way_to_client if stock else 0), ""],
        ["", "Возвращаются на склад", "", "", "", _int(stock.in_way_from_client if stock else 0), ""],
        ["", "Ср. кол-во заказов/день", "", "", "", average_orders_formula(), ""],
        ["", "Ср. убыль остатков/день", "", "", "", average_stock_drop_value(), ""],
        ["", "Дней до АУТА", "", "", "", days_until_zero_value(), ""],
        ["", "", "", "", "", "", ""],
        ["", "", "", "", "", "", ""],
    ]
    rows.extend(
        [
            ["", "Обзор:", "", "", "", "", ""],
            ["", "СПП", "", "", "", _optional_fraction(note.spp_percent), spp_delta_text],
            ["", "Цена WBSELLER (наша)", "", "", "", "", _optional_money(note.seller_price)],
            ["", "Цена WB (на сайте)", "", "", "", "", _optional_money(note.wb_price)],
            ["", "Акция", "", "", "", "", promo_status_value],
            ["", "Негативные отзывы", "", "", "", "", negative_feedback_value],
            ["", "Действия:", "", "", "", "", ""],
            ["", "Включили рекламу?", "", "", "", _bool_label(note.unified_enabled or note.manual_search_enabled or note.manual_shelves_enabled), ""],
            ["", "Меняли цену?(WBSeller)", "", "", "", _bool_label(note.price_changed), ""],
            ["Комментарий:", note.comment, "", "", "", "", ""],
        ]
    )
    return rows


def _build_prefetched_product_report_context(*, product: Product, stock_dates: list[date]) -> dict[str, Any]:
    if not stock_dates:
        return {}

    normalized_dates = sorted(set(stock_dates))
    max_stock_date = normalized_dates[-1]

    metrics_by_date = {
        row.stats_date: row
        for row in DailyProductMetrics.objects.filter(
            product=product,
            stats_date__in=normalized_dates,
        )
    }
    stocks_by_date = {
        row.stats_date: row
        for row in DailyProductStock.objects.filter(
            product=product,
            stats_date__in=normalized_dates,
        )
    }
    notes_by_date = {
        row.note_date: row
        for row in DailyProductNote.objects.filter(
            product=product,
            note_date__in=normalized_dates,
        )
    }

    warehouse_rows_by_date: dict[date, list[DailyWarehouseStock]] = defaultdict(list)
    for row in DailyWarehouseStock.objects.filter(
        product=product,
        stats_date__in=normalized_dates,
    ).select_related("warehouse"):
        warehouse_rows_by_date[row.stats_date].append(row)

    campaign_stats_by_date: dict[date, list[DailyCampaignProductStat]] = defaultdict(list)
    for row in (
        DailyCampaignProductStat.objects.filter(
            product=product,
            stats_date__in=normalized_dates,
        )
        .select_related("campaign")
        .order_by("stats_date", "campaign__monitoring_group", "zone")
    ):
        campaign_stats_by_date[row.stats_date].append(row)

    keyword_stats_by_date: dict[date, list[DailyProductKeywordStat]] = defaultdict(list)
    for row in DailyProductKeywordStat.objects.filter(
        product=product,
        stats_date__in=normalized_dates,
    ).order_by("stats_date", "query_text"):
        keyword_stats_by_date[row.stats_date].append(row)

    search_cluster_stats_by_date: dict[date, list[DailyCampaignSearchClusterStat]] = defaultdict(list)
    for row in DailyCampaignSearchClusterStat.objects.filter(
        product=product,
        stats_date__in=normalized_dates,
    ).select_related("campaign"):
        search_cluster_stats_by_date[row.stats_date].append(row)

    economics_versions = list(
        ProductEconomicsVersion.objects.filter(
            product=product,
            effective_from__lte=max_stock_date,
        ).order_by("-effective_from", "-id")
    )
    fallback_buyout = decimalize(product.buyout_percent)
    fallback_unit_cost = decimalize(product.unit_cost)
    fallback_logistics = decimalize(product.logistics_cost)
    economics_by_date: dict[date, ResolvedEconomics] = {}
    for current_date in normalized_dates:
        economics_snapshot = next(
            (version for version in economics_versions if version.effective_from <= current_date),
            None,
        )
        if economics_snapshot:
            resolved = ResolvedEconomics(
                effective_from=economics_snapshot.effective_from,
                buyout_percent=decimalize(economics_snapshot.buyout_percent),
                unit_cost=decimalize(economics_snapshot.unit_cost),
                logistics_cost=decimalize(economics_snapshot.logistics_cost),
            )
        else:
            resolved = ResolvedEconomics(
                effective_from=None,
                buyout_percent=decimalize(product.buyout_percent),
                unit_cost=decimalize(product.unit_cost),
                logistics_cost=decimalize(product.logistics_cost),
            )
        economics_by_date[current_date] = ResolvedEconomics(
            effective_from=resolved.effective_from,
            buyout_percent=fallback_buyout if resolved.buyout_percent == 0 and fallback_buyout != 0 else resolved.buyout_percent,
            unit_cost=fallback_unit_cost if resolved.unit_cost == 0 and fallback_unit_cost != 0 else resolved.unit_cost,
            logistics_cost=fallback_logistics if resolved.logistics_cost == 0 and fallback_logistics != 0 else resolved.logistics_cost,
        )

    visible_warehouse_names = {
        normalize_warehouse_name(name)
        for name in product.visible_warehouse_names()
    }
    active_campaign_exists = product.campaigns.filter(is_active=True).exists()

    history_rows = list(product.daily_metrics.order_by("-stats_date")[:14])
    rolling_avg_orders = Decimal("0")
    if history_rows:
        window = history_rows[:7]
        rolling_avg_orders = sum((Decimal(item.order_count) for item in window), Decimal("0")) / Decimal(len(window))

    return {
        "preloaded_metrics": metrics_by_date,
        "preloaded_stocks": stocks_by_date,
        "preloaded_notes": notes_by_date,
        "preloaded_warehouse_rows": warehouse_rows_by_date,
        "preloaded_campaign_stats": campaign_stats_by_date,
        "preloaded_keyword_stats": keyword_stats_by_date,
        "preloaded_search_cluster_stats": search_cluster_stats_by_date,
        "preloaded_economics": economics_by_date,
        "preloaded_visible_warehouse_names": visible_warehouse_names,
        "preloaded_active_campaign_exists": active_campaign_exists,
        "preloaded_history": history_rows,
        "preloaded_rolling_avg_orders": rolling_avg_orders,
    }


def build_product_monitoring_rows(*, product: Product, reference_date: date, history_days: int) -> list[list[Any]]:
    stock_dates = [reference_date - timedelta(days=offset) for offset in reversed(range(history_days))]
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
    _normalize_keyword_rows_count(reports)

    block_height = _block_height(reports[0]["keyword_rows"]) if reports else BASE_BLOCK_HEIGHT
    total_width = history_days * BLOCK_WIDTH + max(history_days - 1, 0) * BLOCK_GAP
    matrix = [["" for _ in range(total_width)] for _ in range(block_height)]

    for index, report in enumerate(reports):
        start_col = 1 + index * (BLOCK_WIDTH + BLOCK_GAP)
        block = build_day_block(
            report,
            previous_report=reports[index - 1] if index > 0 else None,
            start_row=1,
            start_col=start_col,
        )
        for row_offset, block_row in enumerate(block):
            if row_offset >= block_height:
                break
            for col_offset, value in enumerate(block_row):
                matrix[row_offset][start_col + col_offset - 1] = value
    return matrix


def build_product_monitoring_rows_display(
    *,
    product: Product,
    reference_date: date,
    history_days: int,
    stock_dates: list[date] | None = None,
) -> list[list[Any]]:
    resolved_stock_dates = stock_dates or [reference_date - timedelta(days=offset) for offset in reversed(range(history_days))]
    resolved_history_days = len(resolved_stock_dates)
    prefetched_context = _build_prefetched_product_report_context(product=product, stock_dates=resolved_stock_dates)
    reports = [
        build_product_report(
            product=product,
            stats_date=stock_date,
            stock_date=stock_date,
            create_note=False,
            **prefetched_context,
        )
        for stock_date in resolved_stock_dates
    ]
    _normalize_keyword_rows_count(reports)

    block_height = _block_height(reports[0]["keyword_rows"]) if reports else BASE_BLOCK_HEIGHT
    total_width = resolved_history_days * BLOCK_WIDTH + max(resolved_history_days - 1, 0) * BLOCK_GAP
    matrix = [["" for _ in range(total_width)] for _ in range(block_height)]

    for index, report in enumerate(reports):
        previous_report = reports[index - 1] if index > 0 else None
        start_col = index * (BLOCK_WIDTH + BLOCK_GAP)
        block = exporter_rows(report, previous_report=previous_report)
        for row_offset, block_row in enumerate(block):
            if row_offset >= block_height:
                break
            for col_offset, value in enumerate(block_row):
                target_col = start_col + col_offset
                if target_col >= total_width:
                    break
                matrix[row_offset][target_col] = value

    return matrix


def build_dashboard_rows(*, reference_date: date, history_days: int, product_ids: list[int] | None = None) -> list[list[Any]]:
    stats_date = reference_date
    context = build_dashboard_context(stats_date=stats_date, stock_date=reference_date)
    settings = get_monitoring_settings()
    rows: list[list[Any]] = [
        [settings.project_name, "", "", "", "", ""],
        ["Сформировано", timezone.localtime().strftime("%Y-%m-%d %H:%M"), "", "", "", ""],
        ["Глубина по дням", reference_date.isoformat(), "Окно, дней", history_days, "", ""],
        ["Дата рекламной статистики", stats_date.isoformat(), "", "", "", ""],
        [],
        ["Лист", "nmID", "Товар", "Артикул продавца", "Заказы", "Расход РК", "Остаток WB", "К клиенту", "Кампаний"],
    ]
    for card in context["cards"]:
        if product_ids and card["product"].id not in product_ids:
            continue
        rows.append(
            [
                monitoring_sheet_title(card["product"]),
                card["product"].nm_id,
                card["product"].title,
                card["product"].vendor_code,
                card["metrics"].order_count if card["metrics"] else 0,
                _money(card["total_spend"]),
                card["stock"].total_stock if card["stock"] else 0,
                card["stock"].in_way_to_client if card["stock"] else 0,
                card["campaigns_count"],
            ]
        )
    return rows


def _products_for_export(product_ids: list[int] | None = None) -> list[Product]:
    queryset = Product.objects.filter(is_active=True)
    if product_ids:
        queryset = queryset.filter(id__in=product_ids)
    return list(queryset.order_by("vendor_code", "title", "nm_id"))


def build_monitoring_sheet_payloads(
    *,
    reference_date: date,
    history_days: int | None = None,
    product_ids: list[int] | None = None,
) -> list[MonitoringSheetPayload]:
    settings = get_monitoring_settings()
    resolved_history_days = history_days or getattr(settings, "monitoring_history_days", 14) or 14
    products = _products_for_export(product_ids)
    payloads = [
        MonitoringSheetPayload(
            title=normalize_title("Dashboard"),
            rows=build_dashboard_rows(reference_date=reference_date, history_days=resolved_history_days, product_ids=product_ids),
            kind="dashboard",
        )
    ]
    for product in products:
        payloads.append(
            MonitoringSheetPayload(
                title=monitoring_sheet_title(product),
                rows=build_product_monitoring_rows(
                    product=product,
                    reference_date=reference_date,
                    history_days=resolved_history_days,
                ),
                kind="product",
            )
        )
    return payloads


def build_table_view_payloads(
    *,
    reference_date: date,
    history_days: int | None = None,
    product_ids: list[int] | None = None,
    active_sheet_key: str | None = None,
) -> list[MonitoringSheetPayload]:
    settings = get_monitoring_settings()
    resolved_history_days = history_days or getattr(settings, "monitoring_history_days", 14) or 14
    products = _products_for_export(product_ids)
    active_key = (active_sheet_key or "").strip()

    def is_active(sheet_index: int) -> bool:
        if not active_key:
            return True
        return active_key == f"sheet-{sheet_index}"

    payloads = [
        MonitoringSheetPayload(
            title=normalize_title("Dashboard"),
            rows=(
                build_dashboard_rows(
                    reference_date=reference_date,
                    history_days=resolved_history_days,
                    product_ids=product_ids,
                )
                if is_active(0)
                else []
            ),
            kind="dashboard",
        )
    ]
    stock_dates = [reference_date - timedelta(days=offset) for offset in reversed(range(resolved_history_days))]
    for product in products:
        sheet_index = len(payloads)
        payloads.append(
            MonitoringSheetPayload(
                title=monitoring_sheet_title(product),
                rows=(
                    build_product_monitoring_rows_display(
                        product=product,
                        reference_date=reference_date,
                        history_days=resolved_history_days,
                        stock_dates=stock_dates,
                    )
                    if is_active(sheet_index)
                    else []
                ),
                kind="product",
                product_id=product.id,
                block_dates=stock_dates,
            )
        )
    return payloads


def _apply_dashboard_style(sheet) -> None:
    sheet.freeze_panes = "A6"
    widths = {1: 24, 2: 14, 3: 32, 4: 18, 5: 12, 6: 14, 7: 12, 8: 12, 9: 10}
    for col_idx, width in widths.items():
        sheet.column_dimensions[get_column_letter(col_idx)].width = width

    for row in sheet.iter_rows():
        for cell in row:
            cell.border = THIN_BORDER
            cell.alignment = Alignment(vertical="center", horizontal="left", wrap_text=True)
            if cell.row == 1:
                cell.fill = HEADER_FILL
                cell.font = HEADER_FONT
            elif cell.row in (2, 3, 4, 6):
                cell.fill = SUBHEADER_FILL
                cell.font = SECTION_FONT if cell.row == 6 else Font(bold=True)


def _apply_product_sheet_style(sheet, history_days: int, block_height: int) -> None:
    sheet.freeze_panes = "B4"
    keyword_offset = 0
    percent_rows = {4, 12, 14, 19, 20, 22, 35}
    money_rows = {5, 15, 16, 21, 23, 24, 36, 37}
    integer_rows = {6, 10, 11, 13, 26, 27, 28}
    decimal_rows = {7, 8, 9, 17, 18, 29, 30, 31}
    section_rows = {25, 32, 34, 40}

    for row_idx in range(1, block_height + 1):
        if row_idx in (1, 2, 3):
            sheet.row_dimensions[row_idx].height = 24
        elif row_idx in section_rows:
            sheet.row_dimensions[row_idx].height = 22
        else:
            sheet.row_dimensions[row_idx].height = 19

    for block_index in range(history_days):
        start_col = 1 + block_index * (BLOCK_WIDTH + BLOCK_GAP)
        widths = [24, 12, 12, 12, 12, 12]
        for offset, width in enumerate(widths):
            sheet.column_dimensions[get_column_letter(start_col + offset)].width = width
        if block_index < history_days - 1:
            sheet.column_dimensions[get_column_letter(start_col + BLOCK_WIDTH)].width = 4

        for row_idx in range(1, block_height + 1):
            for col_offset in range(BLOCK_WIDTH):
                cell = sheet.cell(row=row_idx, column=start_col + col_offset)
                if col_offset == 0:
                    cell.border = BLOCK_LEFT_BORDER
                elif col_offset == BLOCK_WIDTH - 1:
                    cell.border = BLOCK_RIGHT_BORDER
                else:
                    cell.border = THIN_BORDER
                cell.alignment = Alignment(vertical="center", horizontal="center", wrap_text=True)

                if row_idx in (1, 2, 3):
                    cell.fill = HEADER_FILL if row_idx in (1, 2) else SUBHEADER_FILL
                    cell.font = HEADER_FONT if row_idx in (1, 2) else SECTION_FONT
                elif row_idx in section_rows:
                    cell.fill = SECTION_FILL
                    cell.font = SECTION_FONT
                elif isinstance(cell.value, str) and cell.value.startswith("="):
                    cell.fill = FORMULA_FILL
                else:
                    cell.fill = META_FILL

                if row_idx in percent_rows:
                    cell.number_format = "0.##%"
                elif row_idx in money_rows:
                    cell.number_format = "#,##0.##"
                elif row_idx in integer_rows:
                    cell.number_format = "#,##0"
                elif row_idx in decimal_rows:
                    cell.number_format = "#,##0.##"

                if col_offset == 0 or row_idx in section_rows:
                    cell.alignment = Alignment(vertical="center", horizontal="left", wrap_text=True)

        if block_index < history_days - 1:
            separator_col = start_col + BLOCK_WIDTH
            for row_idx in range(1, block_height + 1):
                separator_cell = sheet.cell(row=row_idx, column=separator_col)
                separator_cell.value = ""
                separator_cell.fill = BLOCK_SEPARATOR_FILL
                separator_cell.border = NO_BORDER
    sheet.row_dimensions[43].height = 42


def build_monitoring_workbook(
    *,
    reference_date: date,
    history_days: int | None = None,
    product_ids: list[int] | None = None,
) -> Workbook:
    payloads = build_monitoring_sheet_payloads(
        reference_date=reference_date,
        history_days=history_days,
        product_ids=product_ids,
    )
    workbook = Workbook()
    default_sheet = workbook.active
    workbook.remove(default_sheet)

    resolved_history_days = history_days or getattr(get_monitoring_settings(), "monitoring_history_days", 14) or 14
    for payload in payloads:
        sheet = workbook.create_sheet(title=payload.title)
        for row_idx, row in enumerate(payload.rows, start=1):
            for col_idx, value in enumerate(row, start=1):
                sheet.cell(row=row_idx, column=col_idx, value=value)
        if payload.kind == "dashboard":
            _apply_dashboard_style(sheet)
        else:
            _apply_product_sheet_style(sheet, resolved_history_days, sheet.max_row)
    return workbook


def export_monitoring_workbook_bytes(
    *,
    reference_date: date,
    history_days: int | None = None,
    product_ids: list[int] | None = None,
) -> bytes:
    workbook = build_monitoring_workbook(
        reference_date=reference_date,
        history_days=history_days,
        product_ids=product_ids,
    )
    buffer = BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()
