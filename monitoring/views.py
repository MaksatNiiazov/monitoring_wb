from __future__ import annotations

import csv
import json
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation

from django import forms as django_forms
from django.contrib import messages
from django.db.models import Count, Max
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.utils.dateparse import parse_date

from .forms import (
    CampaignWorkspaceCreateForm,
    CampaignWorkspaceSettingsForm,
    DailyNoteForm,
    MonitoringWorkbookForm,
    MonitoringSettingsForm,
    ProductCreateForm,
    ProductSettingsForm,
    SyncForm,
)
from .models import (
    Campaign,
    DailyProductKeywordStat,
    DailyProductMetrics,
    DailyProductNote,
    DailyProductStock,
    DailyWarehouseStock,
    Product,
    ProductCampaign,
    ProductEconomicsVersion,
    ProductKeyword,
    SyncKind,
    SyncLog,
    SyncStatus,
    WBApiRateLimit,
)
from .services.config import (
    build_readiness_summary,
    build_workspace_overview,
    clear_monitoring_settings_cache,
    get_monitoring_settings,
)
from .services.exporters import exporter_rows
from .services.monitoring_table import (
    BLOCK_GAP,
    BLOCK_WIDTH,
    build_table_view_payloads,
    export_monitoring_workbook_bytes,
)
from .services.campaigns import build_campaign_detail_context
from .services.table_charts import build_table_timeline_context
from .services.reports import (
    build_product_metrics_chart,
    build_product_report,
    decimalize,
    get_default_dates,
    normalize_keyword_texts,
    normalize_search_text,
    normalize_warehouse_name,
    resolve_product_economics,
)
from .services.sync import (
    get_running_sync,
    mark_stale_running_syncs,
    request_cancel_running_sync,
    refresh_campaign_metadata,
    refresh_product_metadata,
    run_sync_in_background,
)


def _selected_date(raw: str | None, fallback: date) -> date:
    parsed = parse_date(raw or "")
    return parsed or fallback


def _selected_history_days(raw: str | None, fallback: int) -> int:
    try:
        parsed = int(raw or "")
    except (TypeError, ValueError):
        parsed = fallback
    return max(1, min(parsed, 90))


def _selected_campaign_period(
    request: HttpRequest,
    *,
    fallback_end: date,
    fallback_days: int,
) -> tuple[date, date]:
    date_to = _selected_date(request.GET.get("date_to"), fallback_end)
    date_from_raw = request.GET.get("date_from")
    if date_from_raw:
        date_from = _selected_date(date_from_raw, date_to - timedelta(days=max(fallback_days - 1, 0)))
    else:
        date_from = date_to - timedelta(days=max(fallback_days - 1, 0))
    if date_from > date_to:
        date_from = date_to
    return date_from, date_to


def _parse_decimal_input(raw_value: str | int | bool | None) -> Decimal:
    text = str(raw_value or "").strip()
    if text in {"", "-", "—", "–"}:
        return Decimal("0")
    normalized = (
        text.replace("\u00a0", "")
        .replace(" ", "")
        .replace("₽", "")
        .replace("руб.", "")
        .replace("руб", "")
        .replace("р.", "")
        .replace("%", "")
        .replace(",", ".")
    )
    try:
        return Decimal(normalized)
    except InvalidOperation as exc:
        raise ValueError("invalid decimal") from exc


def _normalize_percent_points(value: Decimal, raw_value: str | int | bool | None) -> Decimal:
    text = str(raw_value or "").strip()
    if not text:
        return Decimal("0")
    normalized = value
    if "%" not in text and Decimal("-1") <= value <= Decimal("1"):
        normalized = value * Decimal("100")
    return normalized.quantize(Decimal("0.01"))


def _format_decimal_input(value: Decimal) -> str:
    text = f"{value.quantize(Decimal('0.01'))}".replace(".", ",")
    if "," in text:
        text = text.rstrip("0").rstrip(",")
    return text


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = str(value or "").strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
    return result


def _product_keyword_values(product: Product) -> list[str]:
    return normalize_keyword_texts(
        list(
            ProductKeyword.objects.filter(product=product)
            .order_by("position", "query_text", "id")
            .values_list("query_text", flat=True)
        )
    )


def _note_keyword_values(note: DailyProductNote) -> list[str]:
    if not isinstance(note.keywords, list):
        return []
    return normalize_keyword_texts([str(item) for item in note.keywords])


def _effective_keyword_values(product: Product, note: DailyProductNote) -> list[str]:
    return normalize_keyword_texts([*_note_keyword_values(note), *_product_keyword_values(product)])


def _sync_product_keywords(product: Product, keyword_texts: list[str]) -> None:
    cleaned_keywords = normalize_keyword_texts(keyword_texts)
    existing_keywords = list(ProductKeyword.objects.filter(product=product).order_by("position", "query_text", "id"))
    existing_by_normalized: dict[str, ProductKeyword] = {}
    duplicate_ids: list[int] = []
    for keyword in existing_keywords:
        normalized_keyword = normalize_search_text(keyword.query_text)
        if normalized_keyword in existing_by_normalized:
            duplicate_ids.append(keyword.id)
            continue
        existing_by_normalized[normalized_keyword] = keyword
    if duplicate_ids:
        ProductKeyword.objects.filter(id__in=duplicate_ids).delete()

    kept_ids: list[int] = []
    for position, keyword_text in enumerate(cleaned_keywords):
        normalized_text = normalize_search_text(keyword_text)
        keyword = existing_by_normalized.get(normalized_text)
        if keyword is None:
            keyword = ProductKeyword.objects.create(
                product=product,
                query_text=keyword_text,
                position=position,
            )
        else:
            update_fields: list[str] = []
            if keyword.query_text != keyword_text:
                keyword.query_text = keyword_text
                update_fields.append("query_text")
            if keyword.position != position:
                keyword.position = position
                update_fields.append("position")
            if update_fields:
                keyword.save(update_fields=[*update_fields, "updated_at"])
        kept_ids.append(keyword.id)

    queryset = ProductKeyword.objects.filter(product=product)
    if kept_ids:
        queryset.exclude(id__in=kept_ids).delete()
    else:
        queryset.delete()


def _sync_product_note_keywords(
    *,
    product: Product,
    source_note: DailyProductNote,
    keyword_texts: list[str],
    rows_count: int,
    preserve_larger_rows: bool,
) -> None:
    cleaned_keywords = normalize_keyword_texts(keyword_texts)
    rows_target = max(int(rows_count or 0), len(cleaned_keywords), 0)
    notes = list(DailyProductNote.objects.filter(product=product).exclude(pk=source_note.pk))
    if not notes:
        return
    now = timezone.now()
    for item in notes:
        item.keywords = cleaned_keywords
        item.keyword_rows_count = (
            max(int(item.keyword_rows_count or 0), rows_target) if preserve_larger_rows else rows_target
        )
        item.updated_at = now
    DailyProductNote.objects.bulk_update(notes, ["keywords", "keyword_rows_count", "updated_at"])


def _format_duration_hms(total_seconds: int) -> str:
    seconds = max(0, int(total_seconds or 0))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours} ч {minutes} мин {seconds} сек"


def _current_wb_sync_cooldown() -> dict[str, object]:
    now = timezone.now()
    record = (
        WBApiRateLimit.objects.filter(next_request_at__gt=now)
        .order_by("-next_request_at", "path")
        .first()
    )
    if record is None or record.next_request_at is None:
        return {
            "is_blocked": False,
            "remaining_seconds": 0,
            "retry_until": None,
            "source": "",
            "detail": "",
        }

    remaining_seconds = max(1, int((record.next_request_at - now).total_seconds()))
    remaining_display = _format_duration_hms(remaining_seconds)
    retry_local = timezone.localtime(record.next_request_at)
    source = f"{record.method} {record.path}"
    return {
        "is_blocked": True,
        "remaining_seconds": remaining_seconds,
        "remaining_display": remaining_display,
        "retry_until": record.next_request_at.isoformat(),
        "source": source,
        "detail": record.last_detail or "",
        "status": record.last_status,
        "token_type": record.token_type,
        "retry_at_display": retry_local.strftime("%d.%m.%Y %H:%M:%S"),
        "message": (
            f"WB лимит активен: {source}. "
            f"Следующая полная синхронизация будет доступна через {remaining_display} "
            f"(после {retry_local:%d.%m.%Y %H:%M:%S})."
        ),
    }


def _warn_about_wb_sync_cooldown(request: HttpRequest) -> bool:
    cooldown = _current_wb_sync_cooldown()
    if not cooldown.get("is_blocked"):
        return False
    messages.warning(
        request,
        str(
            cooldown.get("message")
            or "WB API временно ограничил запросы. Повторите синхронизацию позже."
        ),
    )
    return True


def _parse_stock_int(raw_value: object) -> int:
    try:
        value = int(raw_value or 0)
    except (TypeError, ValueError):
        return 0
    return max(value, 0)


def _collect_size_payloads(stock_row: DailyProductStock | None) -> list[dict[str, object]]:
    if stock_row is None or not isinstance(stock_row.raw_payload, dict):
        return []
    nested_payload = stock_row.raw_payload.get("raw_payload")
    payload_data = nested_payload if isinstance(nested_payload, dict) else stock_row.raw_payload
    return list((((payload_data.get("data") or {}).get("sizes")) or []))


def _build_stock_popup_payload(
    *,
    product: Product,
    stock_row: DailyProductStock | None,
    warehouse_rows: list[dict[str, object]],
    visible_warehouse_names: list[str],
    preferred_warehouse_names: set[str],
) -> dict[str, object]:
    warehouse_total = sum(_parse_stock_int(item.get("stock")) for item in warehouse_rows)
    flat_rows: list[dict[str, object]] = []
    for row in warehouse_rows:
        size_names = [str(value).strip() for value in (row.get("size_names") or []) if str(value).strip()]
        size_preview = ", ".join(size_names[:3])
        if len(size_names) > 3:
            size_preview = f"{size_preview} и ещё {len(size_names) - 3}"
        flat_rows.append(
            {
                "warehouse": str(row.get("warehouse") or "").strip(),
                "stock": _parse_stock_int(row.get("stock")),
                "to_client": _parse_stock_int(row.get("to_client")),
                "from_client": _parse_stock_int(row.get("from_client")),
                "sizes": size_preview,
            }
        )

    size_payloads = _collect_size_payloads(stock_row)
    if not size_payloads:
        return {
            "mode": "flat",
            "title": "Остатки по складам",
            "summary_text": (
                f"Итого: {warehouse_total} шт. Нажмите для деталей"
                if flat_rows
                else "Нет данных по складам на эту дату"
            ),
            "payload_json": json.dumps(
                {
                    "mode": "flat",
                    "columns": [
                        {"id": "warehouse", "label": "Склад"},
                        {"id": "stock", "label": "Остаток", "numeric": True},
                        {"id": "to_client", "label": "К клиенту", "numeric": True},
                        {"id": "from_client", "label": "Возвраты", "numeric": True},
                        {"id": "sizes", "label": "Размеры"},
                    ],
                    "rows": flat_rows,
                    "empty_message": "Нет данных по складам для выбранной даты.",
                },
                ensure_ascii=False,
            ),
            "total": warehouse_total,
            "has_rows": bool(flat_rows),
        }

    visible_names_in_order = _dedupe_preserve_order(visible_warehouse_names)
    allowed_names = {normalize_warehouse_name(row.get("warehouse") or "") for row in warehouse_rows if row.get("warehouse")}
    discovered_names: dict[str, str] = {}
    warehouse_order: list[str] = []
    matrix_rows: list[dict[str, object]] = []
    used_warehouse_names: set[str] = set()

    def register_warehouse(display_name: str) -> str:
        normalized_name = normalize_warehouse_name(display_name)
        if not normalized_name:
            return ""
        discovered_names.setdefault(normalized_name, display_name)
        return normalized_name

    for warehouse_name in visible_names_in_order:
        normalized_name = register_warehouse(warehouse_name)
        if normalized_name and normalized_name not in warehouse_order:
            warehouse_order.append(normalized_name)

    for warehouse_row in warehouse_rows:
        normalized_name = register_warehouse(str(warehouse_row.get("warehouse") or ""))
        if normalized_name and normalized_name not in warehouse_order:
            warehouse_order.append(normalized_name)

    for size_payload in size_payloads:
        size_name = str(size_payload.get("name") or "").strip()
        if not size_name:
            continue
        row_values: dict[str, object] = {
            "vendor_code": product.vendor_code or str(product.nm_id),
            "size": size_name,
        }
        row_total = 0
        offices = size_payload.get("offices") or []
        for office in offices:
            office_name = str((office or {}).get("officeName") or "").strip()
            normalized_name = register_warehouse(office_name)
            if not normalized_name:
                continue
            if preferred_warehouse_names and normalized_name not in preferred_warehouse_names:
                continue
            if allowed_names and normalized_name not in allowed_names:
                continue
            if normalized_name not in warehouse_order:
                warehouse_order.append(normalized_name)
            stock_value = _parse_stock_int(((office or {}).get("metrics") or {}).get("stockCount"))
            if stock_value > 0:
                row_values[normalized_name] = stock_value
                row_total += stock_value
                used_warehouse_names.add(normalized_name)
        if row_total > 0:
            matrix_rows.append(row_values)

    warehouse_columns = [name for name in warehouse_order if name in used_warehouse_names]
    if not matrix_rows or not warehouse_columns:
        return {
            "mode": "flat",
            "title": "Остатки по складам",
            "summary_text": (
                f"Итого: {warehouse_total} шт. Нажмите для деталей"
                if flat_rows
                else "Нет данных по складам на эту дату"
            ),
            "payload_json": json.dumps(
                {
                    "mode": "flat",
                    "columns": [
                        {"id": "warehouse", "label": "Склад"},
                        {"id": "stock", "label": "Остаток", "numeric": True},
                        {"id": "to_client", "label": "К клиенту", "numeric": True},
                        {"id": "from_client", "label": "Возвраты", "numeric": True},
                        {"id": "sizes", "label": "Размеры"},
                    ],
                    "rows": flat_rows,
                    "empty_message": "Нет данных по складам для выбранной даты.",
                },
                ensure_ascii=False,
            ),
            "total": warehouse_total,
            "has_rows": bool(flat_rows),
        }

    matrix_column_specs = [
        {"id": "vendor_code", "label": "Артикул продавца"},
        {"id": "size", "label": "Размер вещи"},
    ]
    for warehouse_name in warehouse_columns:
        matrix_column_specs.append(
            {
                "id": warehouse_name,
                "label": discovered_names.get(warehouse_name, warehouse_name),
                "numeric": True,
                "blank_zero": True,
            }
        )

    matrix_total = sum(
        sum(_parse_stock_int(row.get(warehouse_name)) for warehouse_name in warehouse_columns)
        for row in matrix_rows
    )
    return {
        "mode": "matrix",
        "title": "Остатки",
        "summary_text": f"Итого: {matrix_total} шт. По размерам и складам",
        "payload_json": json.dumps(
            {
                "mode": "matrix",
                "columns": matrix_column_specs,
                "rows": matrix_rows,
                "empty_message": "Нет детализированных остатков по размерам для выбранной даты.",
            },
            ensure_ascii=False,
        ),
        "total": matrix_total,
        "has_rows": bool(matrix_rows),
    }


def _build_product_stock_popup_payload_for_date(*, product: Product, stock_date: date) -> dict[str, object]:
    visible_warehouse_names = product.visible_warehouse_names()
    preferred_warehouse_names = {
        normalize_warehouse_name(warehouse_name)
        for warehouse_name in visible_warehouse_names
    }
    stock_row = DailyProductStock.objects.filter(
        product=product,
        stats_date=stock_date,
    ).first()
    warehouse_rows: list[dict[str, object]] = []
    warehouse_queryset = (
        DailyWarehouseStock.objects.filter(
            product=product,
            stats_date=stock_date,
        )
        .select_related("warehouse")
        .order_by("warehouse__name")
    )
    for warehouse_row in warehouse_queryset:
        warehouse_name = warehouse_row.warehouse.name
        if preferred_warehouse_names:
            if normalize_warehouse_name(warehouse_name) not in preferred_warehouse_names:
                continue
        elif not warehouse_row.warehouse.is_visible_in_monitoring:
            continue
        warehouse_rows.append(
            {
                "warehouse": warehouse_name,
                "stock": int(warehouse_row.stock_count or 0),
                "to_client": int(warehouse_row.in_way_to_client or 0),
                "from_client": int(warehouse_row.in_way_from_client or 0),
                "size_names": list((warehouse_row.raw_payload or {}).get("sizeNames") or []),
            }
        )

    return _build_stock_popup_payload(
        product=product,
        stock_row=stock_row,
        warehouse_rows=warehouse_rows,
        visible_warehouse_names=visible_warehouse_names,
        preferred_warehouse_names=preferred_warehouse_names,
    )


def _resolve_daily_keyword_stat(
    *,
    product: Product,
    note_date: date,
    keyword_text: str,
    keyword_prev: str = "",
) -> DailyProductKeywordStat:
    resolved_text = keyword_text.strip()[:255]
    previous_text = keyword_prev.strip()[:255]
    stat = DailyProductKeywordStat.objects.filter(
        product=product,
        stats_date=note_date,
        query_text=resolved_text,
    ).first()
    if stat is not None:
        if previous_text and previous_text != resolved_text:
            DailyProductKeywordStat.objects.filter(
                product=product,
                stats_date=note_date,
                query_text=previous_text,
            ).exclude(pk=stat.pk).delete()
        return stat

    if previous_text:
        previous_stat = DailyProductKeywordStat.objects.filter(
            product=product,
            stats_date=note_date,
            query_text=previous_text,
        ).first()
        if previous_stat is not None:
            previous_stat.query_text = resolved_text
            previous_stat.save(update_fields=["query_text", "updated_at"])
            return previous_stat

    return DailyProductKeywordStat.objects.create(
        product=product,
        stats_date=note_date,
        query_text=resolved_text,
    )


def _keyword_stat_has_values(stat: DailyProductKeywordStat) -> bool:
    return any(
        [
            int(stat.frequency or 0),
            decimalize(stat.organic_position),
            int(stat.organic_orders or 0),
            decimalize(stat.boosted_position),
            decimalize(stat.boosted_ctr),
            int(stat.boosted_views or 0),
            int(stat.boosted_clicks or 0),
        ]
    )


def _sync_keyword_stat_query_texts(*, product: Product, previous_text: str, resolved_text: str) -> None:
    previous_text = previous_text.strip()[:255]
    resolved_text = resolved_text.strip()[:255]
    if not previous_text:
        return

    previous_stats = list(DailyProductKeywordStat.objects.filter(product=product, query_text=previous_text))
    if not previous_stats:
        return
    if not resolved_text:
        DailyProductKeywordStat.objects.filter(id__in=[stat.id for stat in previous_stats]).delete()
        return

    existing_by_date = {
        stat.stats_date: stat
        for stat in DailyProductKeywordStat.objects.filter(product=product, query_text=resolved_text)
    }
    for stat in previous_stats:
        existing_stat = existing_by_date.get(stat.stats_date)
        if existing_stat is not None and existing_stat.pk != stat.pk:
            stat.delete()
            continue
        if stat.query_text != resolved_text:
            stat.query_text = resolved_text
            stat.save(update_fields=["query_text", "updated_at"])


def _safe_next_url(raw: str | None, fallback: str) -> str:
    candidate = (raw or "").strip()
    if candidate.startswith("/") and not candidate.startswith("//"):
        return candidate
    return fallback


def _table_row_visual_key(value: object) -> str:
    normalized = " ".join(str(value or "").strip().lower().split())
    exact_mapping = {
        "затраты": "spend",
        "затраты (руб)": "spend",
        "затраты (руб.)": "spend",
        "корзины": "carts",
        "конверсия в корзину": "conversion-cart",
        "конверсия в корзину (%)": "conversion-cart",
        "заказы": "orders",
        "конверсия в заказ": "conversion-order",
        "конверсия в заказ (%)": "conversion-order",
    }
    return exact_mapping.get(normalized, "")


def _table_row_style_key(
    *,
    row_number: int,
    keyword_header_row: int | None,
    overview_row: int | None,
) -> str:
    if row_number == 1:
        return "date"
    if row_number == 2:
        return "campaign-header"
    if row_number == 3:
        return "zone-header"
    if 4 <= row_number <= 21:
        if row_number == 17:
            return "metric-muted-input"
        if row_number in {7, 19, 21}:
            return "metric-muted-formula"
        if row_number in {9, 16, 18, 20}:
            return "metric-formula"
        if row_number in {5, 8, 11, 12, 15}:
            return "metric-muted"
        return "metric"
    if 27 <= row_number <= 32:
        return "stock"
    if keyword_header_row is not None and row_number == keyword_header_row:
        return "keyword-header"
    if (
        keyword_header_row is not None
        and overview_row is not None
        and keyword_header_row < row_number < overview_row
    ):
        return "keyword-row"
    if overview_row is not None and row_number >= overview_row:
        overview_offset = row_number - overview_row
        if overview_offset in {0, 6, 10}:
            return "section-header"
        if 1 <= overview_offset <= 5:
            return "overview-field"
        if 7 <= overview_offset <= 9:
            return "action-field"
        if overview_offset == 11:
            return "comment-row"
    return ""


def dashboard(request: HttpRequest) -> HttpResponse:
    target = reverse("monitoring:table")
    query_string = request.META.get("QUERY_STRING", "").strip()
    if query_string:
        target = f"{target}?{query_string}"
    return redirect(target)


def table_workspace(request: HttpRequest) -> HttpResponse:
    workspace_settings = get_monitoring_settings()
    default_stock_date = DailyProductStock.objects.aggregate(latest=Max("stats_date"))["latest"] or timezone.localdate()
    initial_reference_date = _selected_date(request.GET.get("reference_date"), default_stock_date)
    initial_history_days = _selected_history_days(
        request.GET.get("history_days"),
        getattr(workspace_settings, "monitoring_history_days", 14) or 14,
    )
    filters_form = MonitoringWorkbookForm(
        request.GET or None,
        initial={
            "reference_date": initial_reference_date,
            "history_days": initial_history_days,
        },
    )
    if filters_form.is_valid():
        reference_date = filters_form.cleaned_data["reference_date"] or initial_reference_date
        history_days = filters_form.cleaned_data["history_days"] or initial_history_days
    else:
        reference_date = initial_reference_date
        history_days = initial_history_days
    filters_form.fields["reference_date"].widget = django_forms.HiddenInput()
    filters_form.fields["history_days"].widget = django_forms.HiddenInput()
    table_period_start = reference_date - timedelta(days=max(history_days - 1, 0))

    requested_sheet = (request.GET.get("sheet") or "").strip()
    products_count = Product.objects.filter(is_active=True).count()
    max_sheet_index = products_count
    default_sheet_key = "sheet-1" if products_count > 0 else "sheet-0"
    active_sheet_key = requested_sheet
    if not active_sheet_key.startswith("sheet-"):
        active_sheet_key = default_sheet_key
    else:
        try:
            active_sheet_index = int(active_sheet_key.split("-", 1)[1])
        except (TypeError, ValueError, IndexError):
            active_sheet_index = -1
        if active_sheet_index < 0 or active_sheet_index > max_sheet_index:
            active_sheet_key = default_sheet_key

    payloads = build_table_view_payloads(
        reference_date=reference_date,
        history_days=history_days,
        active_sheet_key=active_sheet_key,
    )
    sheet_tabs: list[dict] = []
    for index, payload in enumerate(payloads):
        sheet_tabs.append(
            {
                "key": f"sheet-{index}",
                "title": payload.title,
                "label": "Сводка" if payload.kind == "dashboard" else payload.title,
                "kind": payload.kind,
                "rows": payload.rows,
                "product_id": payload.product_id,
                "block_dates": payload.block_dates or [],
            }
        )

    active_sheet = next((item for item in sheet_tabs if item["key"] == active_sheet_key), None)
    if active_sheet is None:
        active_sheet = next((item for item in sheet_tabs if item["kind"] == "product"), sheet_tabs[0] if sheet_tabs else None)

    if active_sheet is not None:
        keyword_header_row = None
        overview_row = None
        keyword_rows_count = 0
        keyword_offset = 0

        def row_after_keywords(base_row: int) -> int:
            return base_row + keyword_offset

        if active_sheet["kind"] == "product":
            for row_index, row in enumerate(active_sheet["rows"], start=1):
                first_value = str(row[0] if row else "").strip()
                second_value = str(row[1] if len(row) > 1 else "").strip()
                if first_value == "Ключи":
                    keyword_header_row = row_index
                if second_value == "Обзор:":
                    overview_row = row_index
            if keyword_header_row and overview_row and overview_row > keyword_header_row:
                keyword_rows_count = max(0, overview_row - keyword_header_row - 1)
                keyword_offset = keyword_rows_count

        editable_controls: dict[tuple[int, int], dict[str, object]] = {
            (
                17,
                1,
            ): {
                "type": "input",
                "field": "buyout_percent",
                "percent": True,
                "placeholder": "%",
                "colspan": 7,
                "centered": True,
            },
            (27, 7): {"type": "stock_popup"},
            (row_after_keywords(35), 2): {"type": "input", "field": "spp_percent", "percent": True, "placeholder": "%", "colspan": 5},
            (row_after_keywords(36), 7): {"type": "input", "field": "seller_price", "placeholder": "0,00", "colspan": 2},
            (row_after_keywords(37), 7): {"type": "input", "field": "wb_price", "placeholder": "0,00", "colspan": 2},
            (row_after_keywords(38), 7): {
                "type": "select",
                "field": "promo_status",
                "options": ["Не участвуем", "Участвуем", "Тест", "Акция"],
                "colspan": 2,
            },
            (row_after_keywords(39), 7): {
                "type": "select",
                "field": "negative_feedback",
                "options": ["Без изменений", "Есть негатив", "Нужна проверка", "Критично"],
                "colspan": 2,
            },
            (row_after_keywords(41), 7): {"type": "bool", "field": "unified_enabled"},
            (row_after_keywords(42), 7): {"type": "bool", "field": "manual_search_enabled"},
            (row_after_keywords(43), 7): {"type": "bool", "field": "price_changed"},
            (row_after_keywords(45), 1): {"type": "textarea", "field": "comment", "placeholder": "Комментарий"},
        }
        if keyword_header_row:
            editable_controls[(keyword_header_row, 0)] = {
                "type": "keyword_rows",
                "field": "keyword_rows_count_delta",
            }
            for keyword_row_number in range(keyword_header_row + 1, keyword_header_row + keyword_rows_count + 1):
                editable_controls[(keyword_row_number, 0)] = {
                    "type": "input",
                    "field": "keyword_query",
                    "input_mode": "text",
                    "placeholder": "Ключ",
                }
                editable_controls[(keyword_row_number, 1)] = {
                    "type": "input",
                    "field": "keyword_frequency",
                    "input_mode": "numeric",
                    "placeholder": "0",
                    "centered": True,
                }
                editable_controls[(keyword_row_number, 2)] = {
                    "type": "input",
                    "field": "keyword_organic_position",
                    "placeholder": "0,00",
                    "centered": True,
                }
                editable_controls[(keyword_row_number, 7)] = {
                    "type": "input",
                    "field": "keyword_boosted_position",
                    "placeholder": "0,00",
                    "centered": True,
                }
                editable_controls[(keyword_row_number, 8)] = {
                    "type": "input",
                    "field": "keyword_boosted_ctr",
                    "placeholder": "0,00",
                    "centered": True,
                }

        display_spans: dict[tuple[int, int], dict[str, object]] = {}
        display_spans[(1, 1)] = {"colspan": 8, "centered": True}
        display_spans[(2, 1)] = {"colspan": 3, "centered": True}
        display_spans[(2, 4)] = {"colspan": 2, "centered": True}
        display_spans[(12, 1)] = {"colspan": 6}
        display_spans[(14, 1)] = {"colspan": 6}
        for blank_row_number in range(22, 27):
            display_spans[(blank_row_number, 1)] = {"colspan": 8}
        for stock_row_number in range(27, 33):
            display_spans[(stock_row_number, 1)] = {"colspan": 2}
            display_spans[(stock_row_number, 7)] = {"colspan": 2, "centered": True}
        if overview_row:
            display_spans[(overview_row, 1)] = {"colspan": 8, "centered": True}
            display_spans[(overview_row + 1, 2)] = {"colspan": 5}
            for overview_field_row in range(overview_row + 2, overview_row + 6):
                display_spans[(overview_field_row, 1)] = {"colspan": 2}
                display_spans[(overview_field_row, 7)] = {"colspan": 2}
            display_spans[(overview_row + 6, 1)] = {"colspan": 8, "centered": True}
            for action_field_row in range(overview_row + 7, overview_row + 10):
                display_spans[(action_field_row, 1)] = {"colspan": 2}
            display_spans[(overview_row + 10, 1)] = {"colspan": 8, "centered": True}
        block_dates = active_sheet.get("block_dates") or []
        product_id = active_sheet.get("product_id")
        block_span = BLOCK_WIDTH + BLOCK_GAP
        label_anchor_block_index = 0 if block_dates else -1
        prepared_rows: list[dict] = []
        for row_index, row in enumerate(active_sheet["rows"]):
            prepared_cells: list[dict] = []
            row_number = row_index + 1
            row_visual_key = _table_row_visual_key(row[0] if row else "")
            row_style_key = _table_row_style_key(
                row_number=row_number,
                keyword_header_row=keyword_header_row,
                overview_row=overview_row,
            )

            column_index = 0
            while column_index < len(row):
                value = row[column_index]
                block_index = column_index // block_span
                in_block_col = column_index % block_span
                is_gap_col = active_sheet["kind"] == "product" and in_block_col == BLOCK_WIDTH
                is_keyword_section_row = (
                    active_sheet["kind"] == "product"
                    and keyword_header_row is not None
                    and overview_row is not None
                    and keyword_header_row <= row_number < overview_row
                )
                is_repeat_label_col = (
                    active_sheet["kind"] == "product"
                    and in_block_col == 0
                    and label_anchor_block_index >= 0
                    and block_index != label_anchor_block_index
                )
                control = None
                colspan = 1
                centered_value = False

                if active_sheet["kind"] == "product" and product_id and not is_gap_col:
                    control_spec = editable_controls.get((row_number, in_block_col))
                    display_span_spec = display_spans.get((row_number, in_block_col))
                    if is_keyword_section_row and in_block_col == 0 and block_index != label_anchor_block_index:
                        control_spec = None
                else:
                    control_spec = None
                    display_span_spec = None

                if control_spec:
                    note_date = block_dates[block_index] if block_index < len(block_dates) else None
                    if note_date:
                        control_type = str(control_spec.get("type") or "")
                        field_name = str(control_spec.get("field") or "")
                        if control_type == "bool":
                            normalized = str(value or "").strip().lower()
                            bool_value = normalized in {"да", "true", "1", "yes", "on"}
                            control = {
                                "type": "bool",
                                "field": field_name,
                                "value": bool_value,
                                "note_date": note_date.isoformat(),
                                "product_id": product_id,
                            }
                        elif control_type == "select":
                            current_value = str(value or "").strip()
                            options = [str(option) for option in control_spec.get("options", []) if str(option)]
                            if current_value and current_value not in options:
                                options = [current_value, *options]
                            control = {
                                "type": "select",
                                "field": field_name,
                                "value": current_value or options[0],
                                "options": options,
                                "note_date": note_date.isoformat(),
                                "product_id": product_id,
                            }
                        elif control_type == "input":
                            current_value = str(value or "").strip()
                            if control_spec.get("percent") and current_value.endswith("%"):
                                current_value = current_value[:-1].strip()
                            control = {
                                "type": "input",
                                "field": field_name,
                                "value": current_value,
                                "placeholder": str(control_spec.get("placeholder") or ""),
                                "input_mode": str(control_spec.get("input_mode") or "decimal"),
                                "note_date": note_date.isoformat(),
                                "product_id": product_id,
                                "centered": bool(control_spec.get("centered")),
                            }
                            if field_name.startswith("keyword_"):
                                keyword_prev_value = str(row[0] if row else "").strip()
                                control["keyword_prev"] = keyword_prev_value
                            elif control_spec.get("keyword_prev") is not None:
                                control["keyword_prev"] = str(control_spec.get("keyword_prev") or "")
                            if control_spec.get("span_to_block_end") and in_block_col < BLOCK_WIDTH:
                                colspan = max(1, BLOCK_WIDTH - in_block_col)
                        elif control_type == "keyword_rows":
                            control = {
                                "type": "keyword_rows",
                                "field": field_name,
                                "note_date": note_date.isoformat(),
                                "product_id": product_id,
                            }
                        elif control_type == "textarea":
                            control = {
                                "type": "textarea",
                                "field": field_name,
                                "value": str(value or ""),
                                "placeholder": str(control_spec.get("placeholder") or ""),
                                "note_date": note_date.isoformat(),
                                "product_id": product_id,
                            }
                            # Spread the comment editor across the entire data part of the day block.
                            if in_block_col < BLOCK_WIDTH:
                                colspan = max(1, BLOCK_WIDTH - in_block_col)
                        elif control_type == "stock_popup":
                            control = {
                                "type": "stock_popup",
                                "stock_date": note_date.isoformat(),
                                "stock_date_label": note_date.strftime("%d.%m.%Y"),
                                "title": "Остатки по складам",
                                "summary_text": str(value or "Нет данных по складам на эту дату"),
                                "payload_url": (
                                    f"{reverse('monitoring:table_stock_popup_payload')}"
                                    f"?product_id={product_id}&stats_date={note_date.isoformat()}"
                                ),
                            }
                            if in_block_col < BLOCK_WIDTH:
                                colspan = max(1, BLOCK_WIDTH - in_block_col)
                        explicit_colspan = control_spec.get("colspan")
                        if control and explicit_colspan and in_block_col < BLOCK_WIDTH:
                            try:
                                colspan = max(1, min(BLOCK_WIDTH - in_block_col, int(explicit_colspan)))
                            except (TypeError, ValueError):
                                colspan = 1
                elif display_span_spec and in_block_col < BLOCK_WIDTH:
                    explicit_colspan = display_span_spec.get("colspan")
                    if explicit_colspan:
                        try:
                            colspan = max(1, min(BLOCK_WIDTH - in_block_col, int(explicit_colspan)))
                        except (TypeError, ValueError):
                            colspan = 1
                    elif display_span_spec.get("span_to_block_end"):
                        colspan = max(1, BLOCK_WIDTH - in_block_col)
                    centered_value = bool(display_span_spec.get("centered"))

                prepared_cells.append(
                    {
                        "value": value,
                        "row_index": row_number,
                        "block_index": block_index,
                        "in_block_col": in_block_col,
                        "is_gap_col": is_gap_col,
                        "is_block_start": active_sheet["kind"] == "product" and in_block_col == 0,
                        "is_spacer_col": False,
                        "is_label_col": active_sheet["kind"] == "product" and in_block_col == 0 and not is_repeat_label_col,
                        "is_repeat_label_col": is_repeat_label_col,
                        "control": control,
                        "colspan": colspan,
                        "is_comment_span": bool(control and control.get("type") == "textarea" and colspan > 1),
                        "is_stock_span": bool(control and control.get("type") == "stock_popup" and colspan > 1),
                        "is_input_span": bool(control and control.get("type") == "input" and colspan > 1),
                        "is_centered_value_span": bool(centered_value and colspan > 1),
                    }
                )
                column_index += colspan
            prepared_rows.append(
                {
                    "cells": prepared_cells,
                    "row_visual_key": row_visual_key,
                    "row_style_key": row_style_key,
                }
            )
        active_sheet = {**active_sheet, "rows": prepared_rows, "keyword_offset": keyword_offset}

    table_timeline = build_table_timeline_context(
        active_sheet=active_sheet,
        reference_date=reference_date,
        history_days=history_days,
    )

    context = {
        "reference_date": reference_date,
        "history_days": history_days,
        "table_period_start": table_period_start,
        "table_period_end": reference_date,
        "table_filters_form": filters_form,
        "sheet_tabs": sheet_tabs,
        "active_sheet": active_sheet,
        "table_timeline": table_timeline,
        "product_form": ProductCreateForm(),
        "campaign_form": CampaignWorkspaceCreateForm(),
        "sync_form": SyncForm(
            initial={
                "reference_date": reference_date,
                "date_from": reference_date,
                "date_to": reference_date,
            }
        ),
        "workspace_settings": workspace_settings,
        "readiness": build_readiness_summary(),
        "workspace_overview": build_workspace_overview(),
        "table_note_update_url": reverse("monitoring:update_table_note_cell"),
        "hide_workspace_header": True,
        "body_class": "is-table-fullscreen" if getattr(workspace_settings, "table_default_fullscreen_mode", False) else "",
    }
    return render(request, "monitoring/table_workspace.html", context)


def table_stock_popup_payload(request: HttpRequest) -> JsonResponse:
    product_raw = (request.GET.get("product_id") or "").strip()
    stats_date = parse_date((request.GET.get("stats_date") or "").strip())
    if not product_raw.isdigit() or stats_date is None:
        return JsonResponse({"ok": False, "detail": "Некорректные параметры."}, status=400)

    product = get_object_or_404(Product, pk=int(product_raw))
    payload = _build_product_stock_popup_payload_for_date(product=product, stock_date=stats_date)
    return JsonResponse(
        {
            "ok": True,
            "title": str(payload.get("title") or "Остатки по складам"),
            "date_label": stats_date.strftime("%d.%m.%Y"),
            "total": int(payload.get("total") or 0),
            "payload": json.loads(
                str(payload.get("payload_json") or "{\"mode\":\"flat\",\"columns\":[],\"rows\":[]}")
            ),
        }
    )
def products_workspace(request: HttpRequest) -> HttpResponse:
    settings_obj = get_monitoring_settings()
    today = timezone.localdate()
    latest_metrics_date = DailyProductMetrics.objects.aggregate(latest=Max("stats_date"))["latest"] or today
    latest_stock_date = DailyProductStock.objects.aggregate(latest=Max("stats_date"))["latest"] or today

    products = list(Product.objects.order_by("-is_active", "vendor_code", "title", "nm_id"))
    product_ids = [product.id for product in products]

    metrics_by_product_id = {
        row.product_id: row
        for row in DailyProductMetrics.objects.filter(product_id__in=product_ids, stats_date=latest_metrics_date)
    }
    stocks_by_product_id = {
        row.product_id: row
        for row in DailyProductStock.objects.filter(product_id__in=product_ids, stats_date=latest_stock_date)
    }
    campaigns_count_by_product_id = {
        row["product_id"]: int(row["campaigns_count"] or 0)
        for row in ProductCampaign.objects.filter(product_id__in=product_ids, campaign__is_active=True)
        .values("product_id")
        .annotate(campaigns_count=Count("campaign_id", distinct=True))
    }

    product_rows: list[dict[str, object]] = []
    for product in products:
        metrics = metrics_by_product_id.get(product.id)
        stock = stocks_by_product_id.get(product.id)
        product_rows.append(
            {
                "product": product,
                "metrics": metrics,
                "stock": stock,
                "campaigns_count": campaigns_count_by_product_id.get(product.id, 0),
                "stats_date": latest_metrics_date,
                "stock_date": latest_stock_date,
            }
        )

    edit_raw = (request.GET.get("edit") or "").strip()
    modal_raw = (request.GET.get("modal") or "").strip().lower()
    selected_product = None
    if edit_raw.isdigit():
        selected_product = next((item["product"] for item in product_rows if item["product"].id == int(edit_raw)), None)
    if selected_product is None and products:
        selected_product = products[0]

    context = {
        "workspace_settings": settings_obj,
        "workspace_overview": build_workspace_overview(),
        "product_rows": product_rows,
        "latest_metrics_date": latest_metrics_date,
        "latest_stock_date": latest_stock_date,
        "selected_product": selected_product,
        "open_product_edit_modal": modal_raw == "edit" and selected_product is not None,
        "product_form": ProductCreateForm(),
        "selected_product_form": ProductSettingsForm(instance=selected_product) if selected_product else None,
    }
    return render(request, "monitoring/products_workspace.html", context)


def campaigns_workspace(request: HttpRequest) -> HttpResponse:
    settings_obj = get_monitoring_settings()
    campaigns = list(
        Campaign.objects.annotate(
            products_count=Count("products", distinct=True),
            latest_stats_date=Max("daily_stats__stats_date"),
        )
        .prefetch_related("products")
        .order_by("-is_active", "monitoring_group", "name", "external_id")
    )

    campaign_rows: list[dict[str, object]] = []
    for campaign in campaigns:
        linked_products = list(campaign.products.all())
        preview_items = [product.vendor_code or str(product.nm_id) for product in linked_products[:3]]
        remainder = max(0, len(linked_products) - len(preview_items))
        campaign_rows.append(
            {
                "campaign": campaign,
                "linked_products": linked_products,
                "products_preview": ", ".join(preview_items),
                "products_remainder": remainder,
            }
        )

    edit_raw = (request.GET.get("edit") or "").strip()
    modal_raw = (request.GET.get("modal") or "").strip().lower()
    selected_campaign = None
    if edit_raw.isdigit():
        selected_campaign = next(
            (item["campaign"] for item in campaign_rows if item["campaign"].id == int(edit_raw)),
            None,
        )
    if selected_campaign is None and campaigns:
        selected_campaign = campaigns[0]

    context = {
        "workspace_settings": settings_obj,
        "workspace_overview": build_workspace_overview(),
        "campaign_rows": campaign_rows,
        "active_campaigns_count": sum(1 for campaign in campaigns if campaign.is_active),
        "campaign_form": CampaignWorkspaceCreateForm(),
        "selected_campaign": selected_campaign,
        "selected_campaign_form": CampaignWorkspaceSettingsForm(instance=selected_campaign) if selected_campaign else None,
        "open_campaign_edit_modal": modal_raw == "edit" and selected_campaign is not None,
    }
    return render(request, "monitoring/campaigns_workspace.html", context)


def campaign_detail(request: HttpRequest, pk: int) -> HttpResponse:
    campaign = get_object_or_404(Campaign.objects.prefetch_related("products"), pk=pk)
    settings_obj = get_monitoring_settings()
    latest_stats_date = campaign.daily_stats.aggregate(latest=Max("stats_date"))["latest"] or timezone.localdate()
    date_from, date_to = _selected_campaign_period(
        request,
        fallback_end=latest_stats_date,
        fallback_days=getattr(settings_obj, "monitoring_history_days", 14) or 14,
    )
    context = build_campaign_detail_context(campaign=campaign, date_from=date_from, date_to=date_to)
    context.update(
        {
            "workspace_settings": settings_obj,
            "workspace_overview": build_workspace_overview(),
            "settings_form": CampaignWorkspaceSettingsForm(instance=campaign),
        }
    )
    return render(request, "monitoring/campaign_detail.html", context)


def add_product(request: HttpRequest) -> HttpResponse:
    if request.method != "POST":
        return redirect("monitoring:dashboard")

    fallback_target = reverse("monitoring:dashboard")
    redirect_target = _safe_next_url(request.POST.get("next"), fallback_target)

    form = ProductCreateForm(request.POST)
    if not form.is_valid():
        for error in form.errors.values():
            messages.error(request, error.as_text())
        return redirect(redirect_target)

    product = form.save()
    try:
        refresh_product_metadata(product)
        messages.success(request, f"Товар {product.nm_id} добавлен и обогащён данными WB.")
    except Exception as exc:
        messages.warning(request, f"Товар {product.nm_id} добавлен, но данные WB не подтянулись: {exc}")
    if redirect_target != fallback_target:
        return redirect(redirect_target)
    return redirect("monitoring:product_detail", pk=product.pk)


def add_campaign(request: HttpRequest) -> HttpResponse:
    fallback_target = reverse("monitoring:dashboard")
    redirect_target = _safe_next_url(request.POST.get("next"), fallback_target)
    if request.method != "POST":
        return redirect(fallback_target)

    form = CampaignWorkspaceCreateForm(request.POST)
    if not form.is_valid():
        for error in form.errors.values():
            messages.error(request, error.as_text())
        return redirect(redirect_target)

    campaign = form.save(commit=False)
    campaign.save()
    form.save_m2m()
    try:
        refresh_campaign_metadata(campaign)
        messages.success(request, f"Кампания {campaign.external_id} добавлена.")
    except Exception as exc:
        messages.warning(request, f"Кампания сохранена, но данные WB не подтянулись: {exc}")
    return redirect(redirect_target)


def update_campaign(request: HttpRequest, pk: int) -> HttpResponse:
    campaign = get_object_or_404(Campaign, pk=pk)
    detail_target = reverse("monitoring:campaign_detail", kwargs={"pk": campaign.pk})
    redirect_target = _safe_next_url(request.POST.get("next"), detail_target)
    if request.method != "POST":
        return redirect(detail_target)

    form = CampaignWorkspaceSettingsForm(request.POST, instance=campaign)
    if form.is_valid():
        form.save()
        messages.success(request, "Настройки кампании обновлены.")
    else:
        messages.error(request, "Не удалось сохранить изменения кампании.")
    return redirect(redirect_target)


def toggle_campaign_active(request: HttpRequest, pk: int) -> HttpResponse:
    campaign = get_object_or_404(Campaign, pk=pk)
    fallback_target = reverse("monitoring:campaigns")
    redirect_target = _safe_next_url(request.POST.get("next"), fallback_target)
    if request.method != "POST":
        return redirect(redirect_target)

    campaign.is_active = not campaign.is_active
    campaign.save(update_fields=["is_active", "updated_at"])
    if campaign.is_active:
        messages.success(request, f"Кампания {campaign.external_id} снова участвует в мониторинге.")
    else:
        messages.success(request, f"Кампания {campaign.external_id} отключена от мониторинга.")
    return redirect(redirect_target)


def sync_all(request: HttpRequest) -> HttpResponse:
    if request.method != "POST":
        return redirect("monitoring:dashboard")

    form = SyncForm(request.POST)
    if not form.is_valid():
        messages.error(request, "Невозможно запустить синхронизацию: проверьте дату.")
        return redirect("monitoring:dashboard")

    mark_stale_running_syncs()
    running_sync = get_running_sync()
    if running_sync:
        messages.warning(
            request,
            "Синхронизация уже запущена. Дождитесь завершения текущего обновления и повторите позже.",
        )
        return redirect("monitoring:dashboard")
    if _warn_about_wb_sync_cooldown(request):
        return redirect("monitoring:dashboard")

    selected_products = list(form.cleaned_data.get("product_ids") or [])
    product_ids = [product.id for product in selected_products] if selected_products else None
    sync_kind = SyncKind.PRODUCT if product_ids else SyncKind.FULL
    run_sync_in_background(
        product_ids=product_ids,
        date_from=form.cleaned_data["date_from"],
        date_to=form.cleaned_data["date_to"],
        reference_date=form.cleaned_data["reference_date"],
        overwrite=True,
        kind=sync_kind,
    )
    if product_ids:
        messages.success(request, "Синхронизация выбранных товаров запущена в фоне. Статус обновляется автоматически.")
    else:
        messages.success(request, "Полная синхронизация запущена в фоне. Статус обновляется автоматически.")
    return redirect("monitoring:dashboard")


def product_detail(request: HttpRequest, pk: int) -> HttpResponse:
    product = get_object_or_404(Product, pk=pk)
    settings_obj = get_monitoring_settings()
    default_stats_date, default_stock_date = get_default_dates(product)
    stats_date = _selected_date(request.GET.get("stats_date"), default_stats_date)
    stock_date = _selected_date(request.GET.get("stock_date"), default_stock_date)
    report = build_product_report(product=product, stats_date=stats_date, stock_date=stock_date)
    product_chart = build_product_metrics_chart(
        product=product,
        reference_date=stats_date,
        history_days=getattr(settings_obj, "monitoring_history_days", 14) or 14,
    )
    note = report["note"]
    linked_campaigns = list(
        product.campaigns.filter(is_active=True)
        .annotate(latest_stats_date=Max("daily_stats__stats_date"))
        .order_by("monitoring_group", "name", "external_id")
    )
    economics_history = list(product.economics_versions.order_by("-effective_from", "-id")[:8])
    context = {
        "report": report,
        "product": product,
        "settings_form": ProductSettingsForm(instance=product),
        "note_form": DailyNoteForm(instance=note, initial={"note_date": stats_date}),
        "linked_campaigns": linked_campaigns,
        "economics_history": economics_history,
        "product_chart": product_chart,
        "sync_form": SyncForm(
            initial={
                "reference_date": stock_date,
                "date_from": stock_date,
                "date_to": stock_date,
            },
            show_products=False,
        ),
    }
    return render(request, "monitoring/product_detail.html", context)


def sync_product(request: HttpRequest, pk: int) -> HttpResponse:
    product = get_object_or_404(Product, pk=pk)
    if request.method != "POST":
        return redirect("monitoring:product_detail", pk=product.pk)

    form = SyncForm(request.POST, show_products=False)
    if not form.is_valid():
        messages.error(request, "Невозможно запустить синхронизацию товара: проверьте дату.")
        return redirect("monitoring:product_detail", pk=product.pk)

    mark_stale_running_syncs()
    running_sync = get_running_sync()
    if running_sync:
        messages.warning(
            request,
            "Синхронизация уже выполняется. Новый запуск будет доступен после завершения текущего.",
        )
    elif _warn_about_wb_sync_cooldown(request):
        pass
    else:
        run_sync_in_background(
            product_ids=[product.id],
            date_from=form.cleaned_data["date_from"],
            date_to=form.cleaned_data["date_to"],
            reference_date=form.cleaned_data["reference_date"],
            overwrite=True,
            kind=SyncKind.PRODUCT,
        )
        messages.success(
            request,
            f"Синхронизация товара {product.nm_id} запущена в фоне. Вы можете продолжать работу в системе.",
        )
    stock_date = form.cleaned_data["date_to"] or form.cleaned_data["reference_date"] or get_default_dates(product)[1]
    stats_date = stock_date
    return redirect(
        f"{reverse('monitoring:product_detail', kwargs={'pk': product.pk})}?stats_date={stats_date}&stock_date={stock_date}"
    )


def sync_cancel(request: HttpRequest) -> JsonResponse | HttpResponse:
    expects_json = (
        request.headers.get("X-Requested-With") == "XMLHttpRequest"
        or "application/json" in request.headers.get("Accept", "")
    )
    if request.method != "POST":
        if expects_json:
            return JsonResponse({"ok": False, "detail": "Method not allowed."}, status=405)
        return redirect("monitoring:dashboard")

    mark_stale_running_syncs()
    log = request_cancel_running_sync()
    if not log:
        if expects_json:
            return JsonResponse(
                {
                    "ok": False,
                    "has_running_sync": False,
                    "detail": "Активная синхронизация не найдена.",
                }
            )
        messages.info(request, "Активная синхронизация не найдена.")
        return redirect(request.META.get("HTTP_REFERER") or reverse("monitoring:dashboard"))

    if expects_json:
        return JsonResponse(
            {
                "ok": True,
                "has_running_sync": True,
                "sync_id": log.id,
                "detail": "Синхронизация остановлена. Можно запускать новый sync.",
            }
        )
    messages.warning(request, "Синхронизация остановлена. Можно запускать новый sync.")
    return redirect(request.META.get("HTTP_REFERER") or reverse("monitoring:dashboard"))


def sync_status(request: HttpRequest) -> JsonResponse:
    mark_stale_running_syncs()
    log = get_running_sync() or SyncLog.objects.order_by("-created_at").first()
    sync_cooldown = _current_wb_sync_cooldown()
    if not log:
        return JsonResponse(
            {
                "has_sync": False,
                "is_running": False,
                "status": "idle",
                "status_display": "Ожидание",
                "kind_display": "",
                "message": "Синхронизация ещё не запускалась.",
                "progress": {
                    "percent": 0,
                    "stage": "Ожидание запуска",
                    "detail": "",
                },
                "sync_cooldown": sync_cooldown,
            }
        )

    payload = log.payload if isinstance(log.payload, dict) else {}
    progress = payload.get("progress") if isinstance(payload.get("progress"), dict) else {}
    is_running = log.status == SyncStatus.RUNNING and log.finished_at is None
    cancel_requested = bool(payload.get("cancel_requested"))
    raw_percent = progress.get("percent")
    try:
        progress_percent = int(raw_percent)
    except (TypeError, ValueError):
        progress_percent = 0 if is_running else 100
    progress_percent = max(0, min(progress_percent, 100))

    return JsonResponse(
        {
            "has_sync": True,
            "id": log.id,
            "is_running": is_running,
            "status": log.status,
            "status_display": log.get_status_display(),
            "kind": log.kind,
            "kind_display": log.get_kind_display(),
            "message": log.message or "",
            "cancel_requested": cancel_requested,
            "can_cancel": is_running and not cancel_requested,
            "target_date": log.target_date.isoformat() if log.target_date else None,
            "target_date_from": payload.get("stats_date_from"),
            "target_date_to": payload.get("stats_date_to"),
            "days_count": payload.get("days_count") or 1,
            "created_at": log.created_at.isoformat() if log.created_at else None,
            "finished_at": log.finished_at.isoformat() if log.finished_at else None,
            "progress": {
                "percent": progress_percent,
                "stage": progress.get("stage")
                or ("Отмена запрошена" if is_running and cancel_requested else ("Выполняется" if is_running else "Завершено")),
                "detail": progress.get("detail") or "",
                "updated_at": progress.get("updated_at") or (log.updated_at.isoformat() if log.updated_at else None),
                "retry_until": progress.get("retry_until"),
            },
            "sync_cooldown": sync_cooldown,
        }
    )


def update_product_settings(request: HttpRequest, pk: int) -> HttpResponse:
    product = get_object_or_404(Product, pk=pk)
    detail_target = reverse("monitoring:product_detail", kwargs={"pk": product.pk})
    redirect_target = _safe_next_url(request.POST.get("next"), detail_target)
    if request.method != "POST":
        return redirect(detail_target)

    form = ProductSettingsForm(request.POST, instance=product)
    if form.is_valid():
        form.save()
        messages.success(request, "Настройки товара обновлены.")
    else:
        messages.error(request, "Не удалось сохранить настройки товара.")
    return redirect(redirect_target)


def update_daily_note(request: HttpRequest, pk: int) -> HttpResponse:
    product = get_object_or_404(Product, pk=pk)
    if request.method != "POST":
        return redirect("monitoring:product_detail", pk=product.pk)

    note_date = _selected_date(request.POST.get("note_date"), get_default_dates(product)[0])
    note, _ = DailyProductNote.objects.get_or_create(product=product, note_date=note_date)
    form = DailyNoteForm(request.POST, instance=note)
    if form.is_valid():
        form.save()
        messages.success(request, "Ежедневная заметка сохранена.")
    else:
        messages.error(request, "Не удалось сохранить заметку.")
    stock_date = get_default_dates(product)[1]
    return redirect(
        f"{reverse('monitoring:product_detail', kwargs={'pk': product.pk})}?stats_date={note_date}&stock_date={stock_date}"
    )


def update_table_note_cell(request: HttpRequest) -> JsonResponse:
    # New handler supports bool/select/text/decimal fields and dated economics snapshots.
    return _update_table_note_cell_v2(request)


def _update_table_note_cell_v2(request: HttpRequest) -> JsonResponse:
    if request.method != "POST":
        return JsonResponse({"ok": False, "detail": "Method not allowed."}, status=405)

    payload: dict[str, str | int | bool | None]
    if request.content_type and "application/json" in request.content_type:
        try:
            payload = json.loads(request.body.decode("utf-8") or "{}")
        except (UnicodeDecodeError, json.JSONDecodeError):
            return JsonResponse({"ok": False, "detail": "Некорректный JSON."}, status=400)
    else:
        payload = request.POST

    product_id_raw = payload.get("product_id")
    note_date_raw = payload.get("note_date")
    field = str(payload.get("field") or "").strip()
    raw_value = payload.get("value")
    keyword_prev = str(payload.get("keyword_prev") or "").strip()
    keyword_query = str(payload.get("keyword_query") or "").strip()

    try:
        product_id = int(str(product_id_raw))
    except (TypeError, ValueError):
        return JsonResponse({"ok": False, "detail": "Некорректный product_id."}, status=400)

    note_date = parse_date(str(note_date_raw or ""))
    if not note_date:
        return JsonResponse({"ok": False, "detail": "Некорректная дата."}, status=400)

    product = get_object_or_404(Product, pk=product_id)
    note, _ = DailyProductNote.objects.get_or_create(product=product, note_date=note_date)

    bool_fields = {
        "unified_enabled",
        "manual_search_enabled",
        "manual_shelves_enabled",
        "price_changed",
    }
    product_text_fields = {
        "primary_keyword",
        "secondary_keyword",
    }
    select_defaults = {
        "promo_status": "Не участвуем",
        "negative_feedback": "Без изменений",
    }
    note_decimal_fields = {"spp_percent", "seller_price", "wb_price"}
    note_text_fields = {"comment"}
    economics_decimal_fields = {"buyout_percent", "unit_cost", "logistics_cost"}
    keyword_int_fields = {"keyword_frequency"}
    keyword_decimal_fields = {
        "keyword_organic_position",
        "keyword_boosted_position",
        "keyword_boosted_ctr",
    }
    percent_fields = {"spp_percent", "buyout_percent"}

    note_update_fields: list[str] = []
    display_value = ""

    if field == "keyword_query":
        resolved_text = str(raw_value or "").strip()
        prev_text = keyword_prev.strip()
        keyword_list = _effective_keyword_values(product, note)
        updated = False
        if prev_text:
            try:
                prev_index = next(
                    index
                    for index, item in enumerate(keyword_list)
                    if normalize_search_text(item) == normalize_search_text(prev_text)
                )
            except StopIteration:
                prev_index = None
            if prev_index is not None:
                if resolved_text:
                    if keyword_list[prev_index] != resolved_text:
                        keyword_list[prev_index] = resolved_text
                        updated = True
                else:
                    keyword_list.pop(prev_index)
                    updated = True
            elif resolved_text:
                keyword_list.append(resolved_text)
                updated = True
        elif resolved_text:
            keyword_list.append(resolved_text)
            updated = True

        if updated:
            note.keywords = normalize_keyword_texts(keyword_list)
            note.keyword_rows_count = max(int(note.keyword_rows_count or 0), len(note.keywords), 0)
            note_update_fields = ["keywords", "keyword_rows_count", "updated_at"]
            _sync_product_keywords(product, note.keywords)
            _sync_product_note_keywords(
                product=product,
                source_note=note,
                keyword_texts=note.keywords,
                rows_count=note.keyword_rows_count,
                preserve_larger_rows=True,
            )
            _sync_keyword_stat_query_texts(
                product=product,
                previous_text=prev_text,
                resolved_text=resolved_text,
            )
        display_value = resolved_text[:255]
    elif field in keyword_int_fields or field in keyword_decimal_fields:
        resolved_keyword = keyword_query or keyword_prev
        if not resolved_keyword.strip():
            return JsonResponse({"ok": False, "detail": "Сначала заполните ключ."}, status=400)
        normalized_keywords = normalize_keyword_texts([*_effective_keyword_values(product, note), resolved_keyword])
        if normalized_keywords != _note_keyword_values(note):
            note.keywords = normalized_keywords
            note.keyword_rows_count = max(int(note.keyword_rows_count or 0), len(normalized_keywords), 0)
            note_update_fields = ["keywords", "keyword_rows_count", "updated_at"]
        _sync_product_keywords(product, normalized_keywords)
        _sync_product_note_keywords(
            product=product,
            source_note=note,
            keyword_texts=normalized_keywords,
            rows_count=note.keyword_rows_count,
            preserve_larger_rows=True,
        )

        stat = _resolve_daily_keyword_stat(
            product=product,
            note_date=note_date,
            keyword_text=resolved_keyword,
            keyword_prev=keyword_prev,
        )
        if field in keyword_int_fields:
            try:
                resolved_decimal = _parse_decimal_input(raw_value)
            except ValueError:
                return JsonResponse({"ok": False, "detail": "Invalid numeric value."}, status=400)
            resolved_int = int(resolved_decimal)
            if resolved_int < 0:
                return JsonResponse({"ok": False, "detail": "Value cannot be negative."}, status=400)
            stat.frequency = resolved_int
            stat.save(update_fields=["frequency", "updated_at"])
            display_value = str(resolved_int) if resolved_int else ""
        else:
            try:
                resolved_decimal = _parse_decimal_input(raw_value)
            except ValueError:
                return JsonResponse({"ok": False, "detail": "Invalid numeric value."}, status=400)
            resolved_decimal = resolved_decimal.quantize(Decimal("0.01"))
            if resolved_decimal < 0:
                return JsonResponse({"ok": False, "detail": "Value cannot be negative."}, status=400)
            stat_field_map = {
                "keyword_organic_position": "organic_position",
                "keyword_boosted_position": "boosted_position",
                "keyword_boosted_ctr": "boosted_ctr",
            }
            setattr(stat, stat_field_map[field], resolved_decimal)
            stat.save(update_fields=[stat_field_map[field], "updated_at"])
            display_value = _format_decimal_input(resolved_decimal) if resolved_decimal else ""

        if not _keyword_stat_has_values(stat):
            stat.delete()
    elif field == "keyword_delete":
        keyword_list = _effective_keyword_values(product, note)
        current_rows = max(int(note.keyword_rows_count or 0), len(keyword_list), 0)
        target_text = str(keyword_prev or raw_value or keyword_query or "").strip()
        target_normalized = normalize_search_text(target_text)
        removed_keywords: list[str] = []

        if target_normalized:
            remaining_keywords: list[str] = []
            removed = False
            for keyword_text in keyword_list:
                if not removed and normalize_search_text(keyword_text) == target_normalized:
                    removed_keywords.append(keyword_text)
                    removed = True
                    continue
                remaining_keywords.append(keyword_text)
            keyword_list = remaining_keywords
        elif current_rows <= len(keyword_list) and keyword_list:
            removed_keywords.append(keyword_list[-1])
            keyword_list = keyword_list[:-1]

        next_rows = max(0, current_rows - 1)
        next_rows = max(next_rows, len(keyword_list))
        note.keywords = normalize_keyword_texts(keyword_list)
        note.keyword_rows_count = next_rows
        note_update_fields = ["keywords", "keyword_rows_count", "updated_at"]
        _sync_product_keywords(product, note.keywords)
        _sync_product_note_keywords(
            product=product,
            source_note=note,
            keyword_texts=note.keywords,
            rows_count=next_rows,
            preserve_larger_rows=False,
        )
        for removed_keyword in removed_keywords:
            DailyProductKeywordStat.objects.filter(
                product=product,
                query_text=removed_keyword,
            ).delete()
        display_value = str(next_rows)
    elif field == "keyword_rows_count_delta":
        try:
            delta = int(str(raw_value or "0"))
        except (TypeError, ValueError):
            return JsonResponse({"ok": False, "detail": "Некорректное изменение строк."}, status=400)
        if delta not in {-1, 1}:
            return JsonResponse({"ok": False, "detail": "Некорректное изменение строк."}, status=400)

        keyword_list = _effective_keyword_values(product, note)
        current_rows = max(int(note.keyword_rows_count or 0), len(keyword_list), 0)
        next_rows = max(0, current_rows + delta)
        removed_keywords: list[str] = []
        if next_rows < len(keyword_list):
            removed_keywords = keyword_list[next_rows:]
            keyword_list = keyword_list[:next_rows]
            note.keywords = keyword_list
        note.keyword_rows_count = next_rows
        note_update_fields = ["keyword_rows_count", "updated_at"]
        if removed_keywords or _note_keyword_values(note) != keyword_list:
            note.keywords = normalize_keyword_texts(keyword_list)
            note_update_fields = ["keywords", "keyword_rows_count", "updated_at"]
            _sync_product_keywords(product, note.keywords)
            _sync_product_note_keywords(
                product=product,
                source_note=note,
                keyword_texts=note.keywords,
                rows_count=next_rows,
                preserve_larger_rows=False,
            )
        else:
            _sync_product_note_keywords(
                product=product,
                source_note=note,
                keyword_texts=keyword_list,
                rows_count=next_rows,
                preserve_larger_rows=False,
            )
        if removed_keywords:
            DailyProductKeywordStat.objects.filter(
                product=product,
                query_text__in=removed_keywords,
            ).delete()
        display_value = str(next_rows)
    elif field in product_text_fields:
        resolved_text = str(raw_value or "").strip()
        setattr(product, field, resolved_text[:255])
        product.save(update_fields=[field, "updated_at"])
        display_value = resolved_text[:255]
    elif field == "ads_enabled":
        normalized = str(raw_value or "").strip().lower()
        resolved_bool = normalized in {"1", "true", "yes", "on", "да"}
        note.unified_enabled = resolved_bool
        note.manual_search_enabled = resolved_bool
        note.manual_shelves_enabled = resolved_bool
        display_value = "Да" if resolved_bool else "Нет"
        note_update_fields = [
            "unified_enabled",
            "manual_search_enabled",
            "manual_shelves_enabled",
            "updated_at",
        ]
    elif field in bool_fields:
        normalized = str(raw_value or "").strip().lower()
        resolved_bool = normalized in {"1", "true", "yes", "on", "да"}
        setattr(note, field, resolved_bool)
        display_value = "Да" if resolved_bool else "Нет"
        note_update_fields = [field, "updated_at"]
    elif field in select_defaults:
        resolved_text = str(raw_value or "").strip() or select_defaults[field]
        setattr(note, field, resolved_text[:255])
        display_value = resolved_text[:255]
        note_update_fields = [field, "updated_at"]
    elif field in note_decimal_fields:
        try:
            resolved_decimal = _parse_decimal_input(raw_value)
        except ValueError:
            return JsonResponse({"ok": False, "detail": "Invalid numeric value."}, status=400)
        if field in percent_fields:
            resolved_decimal = _normalize_percent_points(resolved_decimal, raw_value)
            if resolved_decimal < 0 or resolved_decimal > 100:
                return JsonResponse({"ok": False, "detail": "Percent must be in 0..100 range."}, status=400)
        else:
            resolved_decimal = resolved_decimal.quantize(Decimal("0.01"))
            if resolved_decimal < 0:
                return JsonResponse({"ok": False, "detail": "Value cannot be negative."}, status=400)
        setattr(note, field, resolved_decimal)
        display_value = _format_decimal_input(resolved_decimal)
        note_update_fields = [field, "updated_at"]
    elif field in note_text_fields:
        resolved_text = str(raw_value or "").strip()
        setattr(note, field, resolved_text)
        display_value = resolved_text
        note_update_fields = [field, "updated_at"]
    elif field in economics_decimal_fields:
        try:
            resolved_decimal = _parse_decimal_input(raw_value)
        except ValueError:
            return JsonResponse({"ok": False, "detail": "Invalid numeric value."}, status=400)
        if field in percent_fields:
            resolved_decimal = _normalize_percent_points(resolved_decimal, raw_value)
            if resolved_decimal < 0 or resolved_decimal > 100:
                return JsonResponse({"ok": False, "detail": "Percent must be in 0..100 range."}, status=400)
        else:
            resolved_decimal = resolved_decimal.quantize(Decimal("0.01"))
            if resolved_decimal < 0:
                return JsonResponse({"ok": False, "detail": "Value cannot be negative."}, status=400)

        if resolved_decimal == 0:
            current_defaults = resolve_product_economics(product, note_date)
            fallback_value = getattr(current_defaults, field, Decimal("0"))
            if fallback_value != 0:
                resolved_decimal = fallback_value
            else:
                return JsonResponse({"ok": False, "detail": "Value cannot be 0."}, status=400)

        economics = ProductEconomicsVersion.objects.filter(product=product, effective_from=note_date).first()
        if economics:
            setattr(economics, field, resolved_decimal)
            economics.save(update_fields=[field, "updated_at"])
        else:
            previous_version = (
                ProductEconomicsVersion.objects.filter(product=product, effective_from__lte=note_date)
                .order_by("-effective_from", "-id")
                .first()
            )
            seed_values = {
                "buyout_percent": previous_version.buyout_percent if previous_version else product.buyout_percent,
                "unit_cost": previous_version.unit_cost if previous_version else product.unit_cost,
                "logistics_cost": previous_version.logistics_cost if previous_version else product.logistics_cost,
            }
            seed_values[field] = resolved_decimal
            ProductEconomicsVersion.objects.create(
                product=product,
                effective_from=note_date,
                buyout_percent=seed_values["buyout_percent"],
                unit_cost=seed_values["unit_cost"],
                logistics_cost=seed_values["logistics_cost"],
            )
        display_value = _format_decimal_input(resolved_decimal)
    else:
        return JsonResponse({"ok": False, "detail": "Field is not supported."}, status=400)

    if note_update_fields:
        note.save(update_fields=note_update_fields)

    return JsonResponse(
        {
            "ok": True,
            "field": field,
            "value": display_value,
            "note_date": note_date.isoformat(),
            "product_id": product_id,
        }
    )

    bool_fields = {
        "unified_enabled",
        "manual_search_enabled",
        "manual_shelves_enabled",
        "price_changed",
    }
    select_defaults = {
        "promo_status": "Не участвуем",
        "negative_feedback": "Без изменений",
    }

    if field in product_text_fields:
        resolved = str(raw_value or "").strip()
        setattr(product, field, resolved[:255])
        product.save(update_fields=[field, "updated_at"])
        display_value = resolved[:255]
    elif field in bool_fields:
        normalized = str(raw_value or "").strip().lower()
        resolved = normalized in {"1", "true", "yes", "on", "да"}
        setattr(note, field, resolved)
        display_value = "Да" if resolved else "Нет"
    elif field in select_defaults:
        resolved = str(raw_value or "").strip() or select_defaults[field]
        setattr(note, field, resolved[:255])
        display_value = resolved[:255]
    else:
        return JsonResponse({"ok": False, "detail": "Поле не поддерживается."}, status=400)

    note.save(update_fields=[field, "updated_at"])
    return JsonResponse(
        {
            "ok": True,
            "field": field,
            "value": display_value,
            "note_date": note_date.isoformat(),
            "product_id": product_id,
        }
    )


def export_product_csv(request: HttpRequest, pk: int) -> HttpResponse:
    product = get_object_or_404(Product, pk=pk)
    default_stats_date, default_stock_date = get_default_dates(product)
    stats_date = _selected_date(request.GET.get("stats_date"), default_stats_date)
    stock_date = _selected_date(request.GET.get("stock_date"), default_stock_date)
    report = build_product_report(product=product, stats_date=stats_date, stock_date=stock_date)
    response = HttpResponse(content_type="text/csv; charset=utf-8-sig")
    filename = f"monitoring_wb_{product.nm_id}_{stats_date.isoformat()}.csv"
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    response.write("\ufeff")
    writer = csv.writer(response)
    for row in exporter_rows(report):
        writer.writerow(row)
    return response


def download_monitoring_workbook(request: HttpRequest) -> HttpResponse:
    settings_obj = get_monitoring_settings()
    form = MonitoringWorkbookForm(
        request.GET or None,
        initial={
            "reference_date": date.today(),
            "history_days": getattr(settings_obj, "monitoring_history_days", 14),
        },
    )
    if not form.is_valid():
        messages.error(request, "Невозможно собрать книгу мониторинга: проверьте дату и глубину истории.")
        return redirect("monitoring:workspace_settings")

    reference_date = form.cleaned_data["reference_date"] or date.today()
    history_days = form.cleaned_data["history_days"] or getattr(settings_obj, "monitoring_history_days", 14)
    workbook_bytes = export_monitoring_workbook_bytes(reference_date=reference_date, history_days=history_days)
    filename = f"monitoring_wb_{reference_date.isoformat()}_{history_days}d.xlsx"
    response = HttpResponse(
        workbook_bytes,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


def workspace_settings(request: HttpRequest) -> HttpResponse:
    settings_obj = get_monitoring_settings()
    if hasattr(settings_obj, "_meta"):
        settings_form = MonitoringSettingsForm(instance=settings_obj)
    else:
        settings_form = MonitoringSettingsForm(
            initial={
                "project_name": settings_obj.project_name,
                "report_timezone": settings_obj.report_timezone,
                "sync_hour": settings_obj.sync_hour,
                "sync_minute": settings_obj.sync_minute,
                "monitoring_history_days": getattr(settings_obj, "monitoring_history_days", 14),
                "table_default_compact_mode": getattr(settings_obj, "table_default_compact_mode", True),
                "table_default_fullscreen_mode": getattr(settings_obj, "table_default_fullscreen_mode", False),
                "visible_warehouses_note": settings_obj.visible_warehouses_note,
                "campaign_grouping_note": settings_obj.campaign_grouping_note,
            }
        )
    context = {
        "settings_form": settings_form,
        "workspace_settings": settings_obj,
        "readiness": build_readiness_summary(),
        "workspace_overview": build_workspace_overview(),
        "workbook_form": MonitoringWorkbookForm(
            initial={
                "reference_date": date.today(),
                "history_days": getattr(settings_obj, "monitoring_history_days", 14),
            }
        ),
    }
    return render(request, "monitoring/workspace_settings.html", context)


def update_workspace_settings(request: HttpRequest) -> HttpResponse:
    settings_obj = get_monitoring_settings()
    if request.method != "POST":
        return redirect("monitoring:workspace_settings")

    form = MonitoringSettingsForm(request.POST, instance=settings_obj)
    if form.is_valid():
        form.save()
        clear_monitoring_settings_cache()
        messages.success(request, "Настройки мониторинга сохранены.")
    else:
        messages.error(request, "Не удалось сохранить настройки мониторинга.")
    return redirect("monitoring:workspace_settings")
