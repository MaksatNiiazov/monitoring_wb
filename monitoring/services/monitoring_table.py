from __future__ import annotations

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

from monitoring.models import CampaignMonitoringGroup, CampaignZone, Product
from monitoring.services.config import get_monitoring_settings
from monitoring.services.reports import MetricCell, build_dashboard_context, build_product_report, decimalize

BLOCK_HEIGHT = 48
BLOCK_WIDTH = 9
BLOCK_GAP = 1
SHEETS_MAX_TITLE_LENGTH = 100


@dataclass
class MonitoringSheetPayload:
    title: str
    rows: list[list[Any]]
    kind: str = "product"


THIN_SIDE = Side(style="thin", color="D4D8E4")
THIN_BORDER = Border(left=THIN_SIDE, right=THIN_SIDE, top=THIN_SIDE, bottom=THIN_SIDE)
HEADER_FILL = PatternFill("solid", fgColor="15324D")
SUBHEADER_FILL = PatternFill("solid", fgColor="E7EEF8")
SECTION_FILL = PatternFill("solid", fgColor="EEF3F7")
FORMULA_FILL = PatternFill("solid", fgColor="F1EAFE")
META_FILL = PatternFill("solid", fgColor="F7F9FC")
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


def _fraction(value: Decimal | int | float | str | None) -> float:
    return float((decimalize(value) / Decimal("100")).quantize(Decimal("0.0001")))


def _int(value: Decimal | int | float | str | None) -> int:
    return int(decimalize(value))


def _bool_label(value: bool) -> str:
    return "Да" if value else "Нет"


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
    stats_date = report["stats_date"]
    stock_date = report["stock_date"]
    return f"РК {stats_date:%d.%m.%Y} / Остатки {stock_date:%d.%m.%Y}"


def _spp_delta_value(report: dict[str, Any], previous_report: dict[str, Any] | None) -> float | str:
    if previous_report is None:
        return ""
    current = decimalize(report["note"].spp_percent)
    previous = decimalize(previous_report["note"].spp_percent)
    return float((current - previous).quantize(Decimal("0.0001")) / Decimal("100"))


def build_day_block(
    report: dict[str, Any],
    *,
    previous_report: dict[str, Any] | None = None,
    start_row: int = 1,
    start_col: int = 1,
) -> list[list[Any]]:
    product = report["product"]
    economics = report["economics"]
    note = report["note"]
    stock = report["stock"]
    total_ad = report["total_ad"]
    organic = report["organic"]

    unified_search = _metric_cell(report, CampaignMonitoringGroup.UNIFIED, CampaignZone.SEARCH)
    unified_shelves = _metric_cell(report, CampaignMonitoringGroup.UNIFIED, CampaignZone.RECOMMENDATION)
    unified_catalog = _metric_cell(report, CampaignMonitoringGroup.UNIFIED, CampaignZone.CATALOG)
    manual_search = _metric_cell(report, CampaignMonitoringGroup.MANUAL_SEARCH, CampaignZone.SEARCH)
    manual_shelves = _metric_cell(report, CampaignMonitoringGroup.MANUAL_SHELVES, CampaignZone.RECOMMENDATION)

    columns = [unified_search, unified_shelves, unified_catalog, manual_search, manual_shelves]

    def pick_numbers(metric_name: str) -> list[int | float]:
        values: list[int | float] = []
        for cell in columns:
            value = getattr(cell, metric_name)
            if isinstance(value, Decimal):
                values.append(_money(value))
            else:
                values.append(_int(value))
        return values

    def traffic_formula(relative_col: int) -> str:
        if relative_col in (6, 7):
            impressions_ref = _cell_ref(start_row=start_row, start_col=start_col, relative_row=6, relative_col=relative_col)
            return f'=IF({impressions_ref}>0,1,0)'
        return _cell_formula(
            start_row=start_row,
            start_col=start_col,
            template="IFERROR({current}/SUM({start}:{end}),0)",
            refs={
                "current": (6, relative_col),
                "start": (6, 3),
                "end": (6, 5),
            },
        )

    seller_price_ref = _cell_ref(start_row=start_row, start_col=start_col, relative_row=38, relative_col=7)
    buyout_percent_ref = _cell_ref(start_row=start_row, start_col=start_col, relative_row=20, relative_col=3)
    unit_cost_ref = _cell_ref(start_row=start_row, start_col=start_col, relative_row=21, relative_col=3)
    logistics_ref = _cell_ref(start_row=start_row, start_col=start_col, relative_row=22, relative_col=3)

    def row_value_ref(relative_row: int, relative_col: int) -> str:
        return _cell_ref(start_row=start_row, start_col=start_col, relative_row=relative_row, relative_col=relative_col)

    def order_sum_formula(relative_col: int) -> str:
        return f"={row_value_ref(12, relative_col)}*{seller_price_ref}"

    def buyout_formula(relative_col: int) -> str:
        return f"={row_value_ref(13, relative_col)}*{buyout_percent_ref}"

    def cost_per_order_formula(relative_col: int) -> str:
        return f'=IFERROR({row_value_ref(5, relative_col)}/{row_value_ref(12, relative_col)},0)'

    def cost_per_cart_formula(relative_col: int) -> str:
        return f'=IFERROR({row_value_ref(5, relative_col)}/{row_value_ref(11, relative_col)},0)'

    def ratio_formula(relative_col: int, denominator_row: int) -> str:
        return f'=IFERROR({row_value_ref(5, relative_col)}/{row_value_ref(denominator_row, relative_col)},0)'

    def profit_formula(relative_col: int) -> str:
        return (
            f"={row_value_ref(14, relative_col)}-{row_value_ref(5, relative_col)}-"
            f"({row_value_ref(12, relative_col)}*{buyout_percent_ref}*({unit_cost_ref}+{logistics_ref}))"
        )

    rows: list[list[Any]] = [
        ["", "", _block_header(report), "", "", "", "", "", ""],
        ["Тип рекламной кампании", "", "Единая ставка", "", "", "Руч. поиск", "Руч. полки", "Итого реклама", "Органика"],
        ["Зоны показов", "", "Поиск", "Полки", "Каталог", "Поиск", "Полки", "Общая", "ОРГ"],
        ["Доля трафика (%)", "", traffic_formula(3), traffic_formula(4), traffic_formula(5), traffic_formula(6), traffic_formula(7), "", ""],
        ["Затраты (руб)", "", *pick_numbers("spend"), _money(total_ad.spend), ""],
        ["Показы", "", *pick_numbers("impressions"), _int(total_ad.impressions), ""],
        ["CTR", "", f'=IFERROR({row_value_ref(10, 3)}/{row_value_ref(6, 3)},0)', f'=IFERROR({row_value_ref(10, 4)}/{row_value_ref(6, 4)},0)', f'=IFERROR({row_value_ref(10, 5)}/{row_value_ref(6, 5)},0)', f'=IFERROR({row_value_ref(10, 6)}/{row_value_ref(6, 6)},0)', f'=IFERROR({row_value_ref(10, 7)}/{row_value_ref(6, 7)},0)', f'=IFERROR({row_value_ref(10, 8)}/{row_value_ref(6, 8)},0)', ""],
        ["CPM", "", f'=IFERROR({row_value_ref(5, 3)}*1000/{row_value_ref(6, 3)},0)', f'=IFERROR({row_value_ref(5, 4)}*1000/{row_value_ref(6, 4)},0)', f'=IFERROR({row_value_ref(5, 5)}*1000/{row_value_ref(6, 5)},0)', f'=IFERROR({row_value_ref(5, 6)}*1000/{row_value_ref(6, 6)},0)', f'=IFERROR({row_value_ref(5, 7)}*1000/{row_value_ref(6, 7)},0)', f'=IFERROR({row_value_ref(5, 8)}*1000/{row_value_ref(6, 8)},0)', ""],
        ["CPC", "", f'=IFERROR({row_value_ref(5, 3)}/{row_value_ref(10, 3)},0)', f'=IFERROR({row_value_ref(5, 4)}/{row_value_ref(10, 4)},0)', f'=IFERROR({row_value_ref(5, 5)}/{row_value_ref(10, 5)},0)', f'=IFERROR({row_value_ref(5, 6)}/{row_value_ref(10, 6)},0)', f'=IFERROR({row_value_ref(5, 7)}/{row_value_ref(10, 7)},0)', f'=IFERROR({row_value_ref(5, 8)}/{row_value_ref(10, 8)},0)', ""],
        ["Клики", "", *pick_numbers("clicks"), _int(total_ad.clicks), _int(organic["open_count"])],
        ["Корзины", "", *pick_numbers("carts"), _int(total_ad.carts), _int(organic["cart_count"])],
        ["Заказы", "", *pick_numbers("orders"), _int(total_ad.orders), _int(organic["order_count"])],
        ["Заказы (руб.)", "", order_sum_formula(3), order_sum_formula(4), order_sum_formula(5), order_sum_formula(6), order_sum_formula(7), order_sum_formula(8), order_sum_formula(9)],
        ["Выкупы ≈ (руб.)", "", buyout_formula(3), buyout_formula(4), buyout_formula(5), buyout_formula(6), buyout_formula(7), buyout_formula(8), ""],
        ["Стоимость заказа", "", cost_per_order_formula(3), cost_per_order_formula(4), cost_per_order_formula(5), cost_per_order_formula(6), cost_per_order_formula(7), cost_per_order_formula(8), ""],
        ["Стоимость корзины", "", cost_per_cart_formula(3), cost_per_cart_formula(4), cost_per_cart_formula(5), cost_per_cart_formula(6), cost_per_cart_formula(7), cost_per_cart_formula(8), ""],
        ["ДРР от заказов (%)", "", ratio_formula(3, 13), ratio_formula(4, 13), ratio_formula(5, 13), ratio_formula(6, 13), ratio_formula(7, 13), ratio_formula(8, 13), ""],
        ["ДРР от продаж ≈ (%)", "", ratio_formula(3, 14), ratio_formula(4, 14), ratio_formula(5, 14), ratio_formula(6, 14), ratio_formula(7, 14), ratio_formula(8, 14), ""],
        ["Прибыль (без налогов и костов вне ВБ)", "", profit_formula(3), profit_formula(4), profit_formula(5), profit_formula(6), profit_formula(7), profit_formula(8), ""],
        ["Процент выкупа %", "", _fraction(economics.buyout_percent), "", "", "", "", "", ""],
        ["Себестоимость", "", _money(economics.unit_cost), "", "", "", "", "", ""],
        ["Логистика", "", _money(economics.logistics_cost), "", "", "", "", "", ""],
        ["Себес", "", "Остатки:", "", "", "", "", "", ""],
        ["Логистика", "", "", "", "", "", "", "", ""],
        ["", "", "", "", "", "", "", "", ""],
        ["", "", "", "", "", "", "", "", ""],
        ["", "", "", "", "", "", "", "", ""],
        ["", "", "Остатки на складах WB", "", "", "", "", _int(stock.total_stock if stock else 0), ""],
        ["", "", "Едут к клиенту", "", "", "", "", _int(stock.in_way_to_client if stock else 0), ""],
        ["", "", "Возвращаются на склад", "", "", "", "", _int(stock.in_way_from_client if stock else 0), ""],
        ["", "", "Ср. кол-во заказов/день", "", "", "", "", _money(report["avg_orders_per_day"]), ""],
        ["", "", "Дней до распродажи в 0", "", "", "", "", f'=IFERROR({row_value_ref(28, 8)}/{row_value_ref(31, 8)},0)', ""],
        ["Ключи", "", "Частота", "поз. ОРГ", "", "", "", "поз. БУСТ", "CTR (%)"],
        [product.primary_keyword or "", "", "", "", "", "", "", "", ""],
        [product.secondary_keyword or "", "", "", "", "", "", "", "", ""],
        ["", "", "Обзор:", "", "", "", "", "", ""],
        ["", "", "СПП", _fraction(note.spp_percent), "", "", "Изм. к пред.", "", _spp_delta_value(report, previous_report)],
        ["", "", "Цена WBSELLER (наша)", "", "", "", _money(note.seller_price), "", ""],
        ["", "", "Цена WB (на сайте)", "", "", "", _money(note.wb_price), "", ""],
        ["", "", "Акция", "", "", "", note.promo_status, "", ""],
        ["", "", "Негативные отзывы", "", "", "", note.negative_feedback, "", ""],
        ["", "", "Действия:", "", "", "", "", "", ""],
        ["", "", "Включили РК единая ставка?", "", "", "", _bool_label(note.unified_enabled), "", ""],
        ["", "", "Включили РК руч. поиск?", "", "", "", _bool_label(note.manual_search_enabled), "", ""],
        ["", "", "Включили РК руч. полки?", "", "", "", _bool_label(note.manual_shelves_enabled), "", ""],
        ["", "", "Меняли цену? (WBSeller)", "", "", "", _bool_label(note.price_changed), "", ""],
        ["", "", "Комментарии:", "", "", "", "", "", ""],
        ["", "", note.comment, "", "", "", "", "", ""],
    ]
    return rows


def build_product_monitoring_rows(*, product: Product, reference_date: date, history_days: int) -> list[list[Any]]:
    stock_dates = [reference_date - timedelta(days=offset) for offset in reversed(range(history_days))]
    reports = [
        build_product_report(
            product=product,
            stats_date=stock_date - timedelta(days=1),
            stock_date=stock_date,
            create_note=False,
        )
        for stock_date in stock_dates
    ]

    total_width = history_days * BLOCK_WIDTH + max(history_days - 1, 0) * BLOCK_GAP
    matrix = [["" for _ in range(total_width)] for _ in range(BLOCK_HEIGHT)]

    for index, report in enumerate(reports):
        start_col = 1 + index * (BLOCK_WIDTH + BLOCK_GAP)
        block = build_day_block(
            report,
            previous_report=reports[index - 1] if index > 0 else None,
            start_row=1,
            start_col=start_col,
        )
        for row_offset, block_row in enumerate(block):
            for col_offset, value in enumerate(block_row):
                matrix[row_offset][start_col + col_offset - 1] = value
    return matrix


def build_dashboard_rows(*, reference_date: date, history_days: int, product_ids: list[int] | None = None) -> list[list[Any]]:
    stats_date = reference_date - timedelta(days=1)
    context = build_dashboard_context(stats_date=stats_date, stock_date=reference_date)
    settings = get_monitoring_settings()
    rows: list[list[Any]] = [
        [settings.project_name, "", "", "", "", ""],
        ["Сформировано", timezone.localtime().strftime("%Y-%m-%d %H:%M"), "", "", "", ""],
        ["Опорная дата остатков", reference_date.isoformat(), "История, дней", history_days, "", ""],
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
            title=normalize_title(getattr(settings, "google_dashboard_sheet_name", "Dashboard") or "Dashboard"),
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


def _apply_product_sheet_style(sheet, history_days: int) -> None:
    sheet.freeze_panes = "C4"
    percent_rows = {4, 7, 17, 18, 20, 37}
    money_rows = {5, 8, 9, 13, 14, 15, 16, 19, 21, 22, 38, 39}
    integer_rows = {6, 10, 11, 12, 28, 29, 30}
    decimal_rows = {31, 32}
    section_rows = {33, 36, 42, 47}

    for block_index in range(history_days):
        start_col = 1 + block_index * (BLOCK_WIDTH + BLOCK_GAP)
        widths = [24, 4, 14, 14, 14, 14, 14, 14, 14]
        for offset, width in enumerate(widths):
            sheet.column_dimensions[get_column_letter(start_col + offset)].width = width
        if block_index < history_days - 1:
            sheet.column_dimensions[get_column_letter(start_col + BLOCK_WIDTH)].width = 3

        for row_idx in range(1, BLOCK_HEIGHT + 1):
            for col_offset in range(BLOCK_WIDTH):
                cell = sheet.cell(row=row_idx, column=start_col + col_offset)
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
                    cell.number_format = "0.00%"
                elif row_idx in money_rows:
                    cell.number_format = '#,##0.00'
                elif row_idx in integer_rows:
                    cell.number_format = "#,##0"
                elif row_idx in decimal_rows:
                    cell.number_format = "#,##0.00"

                if col_offset == 0 or row_idx in section_rows:
                    cell.alignment = Alignment(vertical="center", horizontal="left", wrap_text=True)

    sheet.row_dimensions[48].height = 42


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
            _apply_product_sheet_style(sheet, resolved_history_days)
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
