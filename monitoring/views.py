from __future__ import annotations

from collections import defaultdict
import csv
import json
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation

from django.contrib import messages
from django.db.models import Count, Max
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.utils.dateparse import parse_date

from .forms import (
    CampaignCreateForm,
    DailyNoteForm,
    MonitoringWorkbookForm,
    MonitoringSettingsForm,
    ProductCreateForm,
    ProductSettingsForm,
    ReportsFilterForm,
    SyncForm,
)
from .models import (
    DailyProductMetrics,
    DailyProductNote,
    DailyProductStock,
    DailyWarehouseStock,
    Product,
    ProductCampaign,
    ProductEconomicsVersion,
    SyncKind,
    SyncLog,
    SyncStatus,
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
from .services.reporting_hub import build_reports_context
from .services.reports import build_product_report, get_default_dates, normalize_warehouse_name, resolve_product_economics
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


def _safe_next_url(raw: str | None, fallback: str) -> str:
    candidate = (raw or "").strip()
    if candidate.startswith("/") and not candidate.startswith("//"):
        return candidate
    return fallback


def dashboard(request: HttpRequest) -> HttpResponse:
    return table_workspace(request)


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
        for row_index, row in enumerate(active_sheet.get("rows", []), start=1):
            if not row:
                continue
            first_cell = str(row[0] or "").strip()
            if keyword_header_row is None and first_cell == "Ключи":
                keyword_header_row = row_index
                continue
            if keyword_header_row is not None and first_cell == "Обзор:":
                overview_row = row_index
                break
        keyword_rows_count = (
            max(0, (overview_row - keyword_header_row - 1))
            if keyword_header_row is not None and overview_row is not None
            else 0
        )
        keyword_offset = max(0, keyword_rows_count - 2)

        def row_after_keywords(base_row: int) -> int:
            return base_row + keyword_offset if base_row >= 36 else base_row

        editable_controls: dict[tuple[int, int], dict[str, object]] = {
            (
                22,
                2,
            ): {
                "type": "input",
                "field": "buyout_percent",
                "percent": True,
                "placeholder": "%",
                "span_to_block_end": True,
                "centered": True,
            },
            (
                23,
                2,
            ): {
                "type": "input",
                "field": "unit_cost",
                "placeholder": "0,00",
                "span_to_block_end": True,
                "centered": True,
            },
            (
                24,
                2,
            ): {
                "type": "input",
                "field": "logistics_cost",
                "placeholder": "0,00",
                "span_to_block_end": True,
                "centered": True,
            },
            (25, 2): {"type": "stock_popup"},
            (row_after_keywords(37), 3): {"type": "input", "field": "spp_percent", "percent": True, "placeholder": "%"},
            (row_after_keywords(38), 6): {"type": "input", "field": "seller_price", "placeholder": "0,00"},
            (row_after_keywords(39), 6): {"type": "input", "field": "wb_price", "placeholder": "0,00"},
            (row_after_keywords(40), 6): {
                "type": "select",
                "field": "promo_status",
                "options": ["Не участвуем", "Участвуем", "Тест", "Акция"],
            },
            (row_after_keywords(41), 6): {
                "type": "select",
                "field": "negative_feedback",
                "options": ["Без изменений", "Есть негатив", "Нужна проверка", "Критично"],
            },
            (row_after_keywords(43), 6): {"type": "bool", "field": "unified_enabled"},
            (row_after_keywords(44), 6): {"type": "bool", "field": "manual_search_enabled"},
            (row_after_keywords(45), 6): {"type": "bool", "field": "manual_shelves_enabled"},
            (row_after_keywords(46), 6): {"type": "bool", "field": "price_changed"},
            (row_after_keywords(48), 2): {"type": "textarea", "field": "comment", "placeholder": "Комментарий"},
        }
        if keyword_header_row is not None and overview_row is not None:
            for row_number in range(keyword_header_row + 1, overview_row):
                current_value = str(active_sheet["rows"][row_number - 1][0] or "").strip()
                editable_controls[(row_number, 0)] = {
                    "type": "input",
                    "field": "keyword_query",
                    "placeholder": "Ключ",
                    "input_mode": "text",
                    "keyword_prev": current_value,
                }

        display_spans: dict[tuple[int, int], dict[str, bool]] = {
            (21, 2): {"span_to_block_end": True, "centered": True},
        }
        block_dates = active_sheet.get("block_dates") or []
        product_id = active_sheet.get("product_id")
        block_span = BLOCK_WIDTH + BLOCK_GAP
        label_anchor_block_index = 0 if block_dates else -1
        stock_popup_payloads: dict[str, dict[str, object]] = {}

        if active_sheet["kind"] == "product" and product_id and block_dates:
            product = Product.objects.filter(pk=product_id).first()
            if product is not None:
                preferred_warehouse_names = {
                    normalize_warehouse_name(warehouse_name)
                    for warehouse_name in product.visible_warehouse_names()
                }
                warehouse_rows_by_date: dict[date, list[dict[str, object]]] = defaultdict(list)
                warehouse_rows = (
                    DailyWarehouseStock.objects.filter(
                        product_id=product_id,
                        stats_date__in=block_dates,
                    )
                    .select_related("warehouse")
                    .order_by("stats_date", "warehouse__name")
                )
                for warehouse_row in warehouse_rows:
                    warehouse_name = warehouse_row.warehouse.name
                    if preferred_warehouse_names:
                        if normalize_warehouse_name(warehouse_name) not in preferred_warehouse_names:
                            continue
                    elif not warehouse_row.warehouse.is_visible_in_monitoring:
                        continue
                    warehouse_rows_by_date[warehouse_row.stats_date].append(
                        {
                            "warehouse": warehouse_name,
                            "stock": int(warehouse_row.stock_count or 0),
                        }
                    )
                for stock_date in block_dates:
                    rows = warehouse_rows_by_date.get(stock_date, [])
                    stock_popup_payloads[stock_date.isoformat()] = {
                        "rows": rows,
                        "rows_json": json.dumps(rows, ensure_ascii=False),
                        "total": sum(int(item.get("stock") or 0) for item in rows),
                    }

        prepared_rows: list[dict] = []
        for row_index, row in enumerate(active_sheet["rows"]):
            prepared_cells: list[dict] = []
            row_number = row_index + 1
            column_index = 0
            while column_index < len(row):
                value = row[column_index]
                block_index = column_index // block_span
                in_block_col = column_index % block_span
                is_gap_col = active_sheet["kind"] == "product" and in_block_col == BLOCK_WIDTH
                is_repeat_label_col = (
                    active_sheet["kind"] == "product"
                    and in_block_col in (0, 1)
                    and label_anchor_block_index >= 0
                    and block_index != label_anchor_block_index
                )
                control = None
                colspan = 1
                centered_value = False

                if active_sheet["kind"] == "product" and product_id and not is_gap_col:
                    control_spec = editable_controls.get((row_number, in_block_col))
                    display_span_spec = display_spans.get((row_number, in_block_col))
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
                            if control_spec.get("keyword_prev") is not None:
                                control["keyword_prev"] = str(control_spec.get("keyword_prev") or "")
                            if control_spec.get("span_to_block_end") and in_block_col < BLOCK_WIDTH:
                                colspan = max(1, BLOCK_WIDTH - in_block_col)
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
                            popup_payload = stock_popup_payloads.get(
                                note_date.isoformat(),
                                {
                                    "rows": [],
                                    "rows_json": "[]",
                                    "total": 0,
                                },
                            )
                            control = {
                                "type": "stock_popup",
                                "stock_date": note_date.isoformat(),
                                "stock_date_label": note_date.strftime("%d.%m.%Y"),
                                "rows_json": str(popup_payload.get("rows_json") or "[]"),
                                "total": int(popup_payload.get("total") or 0),
                                "has_rows": bool(popup_payload.get("rows")),
                            }
                            if in_block_col < BLOCK_WIDTH:
                                colspan = max(1, BLOCK_WIDTH - in_block_col)
                elif display_span_spec and in_block_col < BLOCK_WIDTH:
                    if display_span_spec.get("span_to_block_end"):
                        colspan = max(1, BLOCK_WIDTH - in_block_col)
                    centered_value = bool(display_span_spec.get("centered"))

                prepared_cells.append(
                    {
                        "value": value,
                        "row_index": row_number,
                        "col_index": column_index,
                        "block_index": block_index,
                        "in_block_col": in_block_col,
                        "is_gap_col": is_gap_col,
                        "is_block_start": active_sheet["kind"] == "product" and in_block_col == 0,
                        "is_spacer_col": active_sheet["kind"] == "product" and in_block_col == 1 and not is_repeat_label_col,
                        "is_label_col": active_sheet["kind"] == "product" and in_block_col in (0, 1) and not is_repeat_label_col,
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
            prepared_rows.append({"cells": prepared_cells})
        active_sheet = {**active_sheet, "rows": prepared_rows, "keyword_offset": keyword_offset}

    context = {
        "reference_date": reference_date,
        "history_days": history_days,
        "table_filters_form": filters_form,
        "sheet_tabs": sheet_tabs,
        "active_sheet": active_sheet,
        "product_form": ProductCreateForm(),
        "campaign_form": CampaignCreateForm(),
        "sync_form": SyncForm(
            initial={
                "reference_date": reference_date,
                "date_from": reference_date,
                "date_to": reference_date,
                "force": workspace_settings.overwrite_within_day,
            }
        ),
        "workspace_settings": workspace_settings,
        "readiness": build_readiness_summary(),
        "workspace_overview": build_workspace_overview(),
        "table_note_update_url": reverse("monitoring:update_table_note_cell"),
        "hide_workspace_header": True,
        "body_class": "is-table-fullscreen",
    }
    return render(request, "monitoring/table_workspace.html", context)


def reports(request: HttpRequest) -> HttpResponse:
    default_reference_date = DailyProductStock.objects.aggregate(latest=Max("stats_date"))["latest"] or timezone.localdate()
    default_range_days = 14
    default_date_from = default_reference_date - timedelta(days=default_range_days - 1)
    form_data = request.GET.copy() if request.GET else None
    if form_data is not None and not form_data.get("date_from") and not form_data.get("date_to"):
        fallback_reference = _selected_date(form_data.get("reference_date"), default_reference_date)
        fallback_range_days = _selected_history_days(form_data.get("range_days"), default_range_days)
        form_data["date_to"] = fallback_reference.isoformat()
        form_data["date_from"] = (fallback_reference - timedelta(days=max(1, fallback_range_days) - 1)).isoformat()
    form = ReportsFilterForm(
        form_data or None,
        initial={
            "date_from": default_date_from,
            "date_to": default_reference_date,
            "range_days": default_range_days,
        },
    )
    if form.is_valid():
        date_from = form.cleaned_data["date_from"] or default_date_from
        date_to = form.cleaned_data["date_to"] or default_reference_date
    else:
        date_from = default_date_from
        date_to = default_reference_date

    range_days = max(1, (date_to - date_from).days + 1)
    context = build_reports_context(reference_date=date_to, range_days=range_days)
    context.update(
        {
            "filters_form": form,
            "workspace_overview": build_workspace_overview(),
            "date_from": date_from,
            "date_to": date_to,
        }
    )
    return render(request, "monitoring/reports.html", context)


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
    if request.method != "POST":
        return redirect("monitoring:dashboard")

    form = CampaignCreateForm(request.POST)
    if not form.is_valid():
        for error in form.errors.values():
            messages.error(request, error.as_text())
        return redirect("monitoring:dashboard")

    campaign = form.save(commit=False)
    campaign.save()
    form.save_m2m()
    try:
        refresh_campaign_metadata(campaign)
        messages.success(request, f"Кампания {campaign.external_id} добавлена.")
    except Exception as exc:
        messages.warning(request, f"Кампания сохранена, но данные WB не подтянулись: {exc}")
    return redirect("monitoring:dashboard")


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

    selected_products = list(form.cleaned_data.get("product_ids") or [])
    product_ids = [product.id for product in selected_products] if selected_products else None
    sync_kind = SyncKind.PRODUCT if product_ids else SyncKind.FULL
    run_sync_in_background(
        product_ids=product_ids,
        date_from=form.cleaned_data["date_from"],
        date_to=form.cleaned_data["date_to"],
        reference_date=form.cleaned_data["reference_date"],
        overwrite=form.cleaned_data["force"],
        kind=sync_kind,
    )
    if product_ids:
        messages.success(request, "Синхронизация выбранных товаров запущена в фоне. Статус обновляется автоматически.")
    else:
        messages.success(request, "Полная синхронизация запущена в фоне. Статус обновляется автоматически.")
    return redirect("monitoring:dashboard")


def product_detail(request: HttpRequest, pk: int) -> HttpResponse:
    product = get_object_or_404(Product, pk=pk)
    default_stats_date, default_stock_date = get_default_dates(product)
    stats_date = _selected_date(request.GET.get("stats_date"), default_stats_date)
    stock_date = _selected_date(request.GET.get("stock_date"), default_stock_date)
    report = build_product_report(product=product, stats_date=stats_date, stock_date=stock_date)
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
        "sync_form": SyncForm(
            initial={
                "reference_date": stock_date,
                "date_from": stock_date,
                "date_to": stock_date,
                "force": get_monitoring_settings().overwrite_within_day,
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
    else:
        run_sync_in_background(
            product_ids=[product.id],
            date_from=form.cleaned_data["date_from"],
            date_to=form.cleaned_data["date_to"],
            reference_date=form.cleaned_data["reference_date"],
            overwrite=form.cleaned_data["force"],
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
            },
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
    percent_fields = {"spp_percent", "buyout_percent"}

    note_update_fields: list[str] = []
    display_value = ""

    if field == "keyword_query":
        resolved_text = str(raw_value or "").strip()
        prev_text = keyword_prev.strip()
        keyword_list = [str(item).strip() for item in (note.keywords or []) if str(item).strip()]
        updated = False
        if prev_text:
            try:
                prev_index = keyword_list.index(prev_text)
            except ValueError:
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
            note.keywords = keyword_list
            note_update_fields = ["keywords", "updated_at"]
        display_value = resolved_text[:255]
    elif field in product_text_fields:
        resolved_text = str(raw_value or "").strip()
        setattr(product, field, resolved_text[:255])
        product.save(update_fields=[field, "updated_at"])
        display_value = resolved_text[:255]
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
                "overwrite_within_day": settings_obj.overwrite_within_day,
                "monitoring_history_days": getattr(settings_obj, "monitoring_history_days", 14),
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
