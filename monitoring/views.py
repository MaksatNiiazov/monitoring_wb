from __future__ import annotations

import csv
from datetime import date, timedelta

from django.contrib import messages
from django.db.models import Max
from django.http import HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
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
from .models import DailyProductNote, Product, SyncKind
from .services.config import (
    build_campaign_overview,
    build_readiness_summary,
    build_workspace_overview,
    get_monitoring_settings,
)
from .services.customer_templates import TEMPLATE_DEFINITIONS
from .services.demo import seed_demo_dataset
from .services.exporters import exporter_rows
from .services.google_sheets import GoogleSheetsSyncError, google_service_account_email, sync_reports_to_google_sheets
from .services.monitoring_table import export_monitoring_workbook_bytes
from .services.reporting_hub import build_reports_context
from .services.reports import build_dashboard_context, build_product_report, get_default_dates
from .services.sync import SyncServiceError, refresh_campaign_metadata, refresh_product_metadata, run_sync


def _selected_date(raw: str | None, fallback: date) -> date:
    parsed = parse_date(raw or "")
    return parsed or fallback


def dashboard(request: HttpRequest) -> HttpResponse:
    workspace_settings = get_monitoring_settings()
    default_stats_date, default_stock_date = get_default_dates()
    stats_date = _selected_date(request.GET.get("stats_date"), default_stats_date)
    stock_date = _selected_date(request.GET.get("stock_date"), default_stock_date)
    context = build_dashboard_context(stats_date=stats_date, stock_date=stock_date)
    context.update(
        {
            "product_form": ProductCreateForm(),
            "campaign_form": CampaignCreateForm(),
            "sync_form": SyncForm(
                initial={
                    "reference_date": stock_date,
                    "force": workspace_settings.overwrite_within_day,
                }
            ),
            "workspace_settings": workspace_settings,
            "readiness": build_readiness_summary(),
            "workspace_overview": build_workspace_overview(),
            "campaign_overview": build_campaign_overview(),
            "template_definitions": TEMPLATE_DEFINITIONS,
        }
    )
    return render(request, "monitoring/dashboard.html", context)


def reports(request: HttpRequest) -> HttpResponse:
    default_reference_date = get_default_dates()[1]
    form = ReportsFilterForm(
        request.GET or None,
        initial={
            "reference_date": default_reference_date,
            "range_days": 14,
        },
    )
    if form.is_valid():
        reference_date = form.cleaned_data["reference_date"] or default_reference_date
        range_days = form.cleaned_data["range_days"] or 14
    else:
        reference_date = default_reference_date
        range_days = 14
    context = build_reports_context(reference_date=reference_date, range_days=range_days)
    context.update(
        {
            "filters_form": form,
            "workspace_overview": build_workspace_overview(),
        }
    )
    return render(request, "monitoring/reports.html", context)


def add_product(request: HttpRequest) -> HttpResponse:
    if request.method != "POST":
        return redirect("monitoring:dashboard")

    form = ProductCreateForm(request.POST)
    if not form.is_valid():
        for error in form.errors.values():
            messages.error(request, error.as_text())
        return redirect("monitoring:dashboard")

    product = form.save()
    try:
        refresh_product_metadata(product)
        messages.success(request, f"Товар {product.nm_id} добавлен и обогащён данными WB.")
    except Exception as exc:
        messages.warning(request, f"Товар {product.nm_id} добавлен, но данные WB не подтянулись: {exc}")
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

    try:
        run_sync(
            reference_date=form.cleaned_data["reference_date"],
            overwrite=form.cleaned_data["force"],
            kind=SyncKind.FULL,
        )
        messages.success(request, "Полная синхронизация завершена.")
    except SyncServiceError as exc:
        messages.error(request, f"Синхронизация завершилась ошибкой: {exc}")
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
                "force": get_monitoring_settings().overwrite_within_day,
            }
        ),
    }
    return render(request, "monitoring/product_detail.html", context)


def sync_product(request: HttpRequest, pk: int) -> HttpResponse:
    product = get_object_or_404(Product, pk=pk)
    if request.method != "POST":
        return redirect("monitoring:product_detail", pk=product.pk)

    form = SyncForm(request.POST)
    if not form.is_valid():
        messages.error(request, "Невозможно запустить синхронизацию товара: проверьте дату.")
        return redirect("monitoring:product_detail", pk=product.pk)

    try:
        run_sync(
            product_ids=[product.id],
            reference_date=form.cleaned_data["reference_date"],
            overwrite=form.cleaned_data["force"],
            kind=SyncKind.PRODUCT,
        )
        messages.success(request, f"Данные товара {product.nm_id} обновлены.")
    except SyncServiceError as exc:
        messages.error(request, f"Ошибка синхронизации товара: {exc}")
    stock_date = _selected_date(request.POST.get("reference_date"), get_default_dates(product)[1])
    stats_date = stock_date - timedelta(days=1)
    return redirect(
        f"{reverse('monitoring:product_detail', kwargs={'pk': product.pk})}?stats_date={stats_date}&stock_date={stock_date}"
    )


def update_product_settings(request: HttpRequest, pk: int) -> HttpResponse:
    product = get_object_or_404(Product, pk=pk)
    if request.method != "POST":
        return redirect("monitoring:product_detail", pk=product.pk)

    form = ProductSettingsForm(request.POST, instance=product)
    if form.is_valid():
        form.save()
        messages.success(request, "Настройки товара обновлены.")
    else:
        messages.error(request, "Не удалось сохранить настройки товара.")
    return redirect("monitoring:product_detail", pk=product.pk)


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


def sync_google_sheets(request: HttpRequest) -> HttpResponse:
    if request.method != "POST":
        return redirect("monitoring:workspace_settings")
    settings_obj = get_monitoring_settings()
    form = MonitoringWorkbookForm(
        request.POST,
        prefix="sheets",
        initial={
            "reference_date": date.today(),
            "history_days": getattr(settings_obj, "monitoring_history_days", 14),
        },
    )
    if not form.is_valid():
        messages.error(request, "Невозможно обновить Google Sheets: проверьте дату и глубину истории.")
        return redirect("monitoring:workspace_settings")
    reference_date = form.cleaned_data["reference_date"] or date.today()
    history_days = form.cleaned_data["history_days"] or getattr(settings_obj, "monitoring_history_days", 14)
    try:
        updated = sync_reports_to_google_sheets(reference_date=reference_date, history_days=history_days)
        messages.success(
            request,
            f"Google Sheets обновлён: {updated} листов за дату {reference_date}, глубина {history_days} дн.",
        )
    except GoogleSheetsSyncError as exc:
        messages.error(request, f"Ошибка обновления Google Sheets: {exc}")
    return redirect("monitoring:workspace_settings")


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
        "template_definitions": TEMPLATE_DEFINITIONS,
        "workbook_form": MonitoringWorkbookForm(
            initial={
                "reference_date": date.today(),
                "history_days": getattr(settings_obj, "monitoring_history_days", 14),
            }
        ),
        "google_sync_form": MonitoringWorkbookForm(
            prefix="sheets",
            initial={
                "reference_date": date.today(),
                "history_days": getattr(settings_obj, "monitoring_history_days", 14),
            }
        ),
        "google_service_account_email": google_service_account_email(),
    }
    return render(request, "monitoring/workspace_settings.html", context)


def update_workspace_settings(request: HttpRequest) -> HttpResponse:
    settings_obj = get_monitoring_settings()
    if request.method != "POST":
        return redirect("monitoring:workspace_settings")

    form = MonitoringSettingsForm(request.POST, instance=settings_obj)
    if form.is_valid():
        form.save()
        messages.success(request, "Настройки мониторинга сохранены.")
    else:
        messages.error(request, "Не удалось сохранить настройки мониторинга.")
    return redirect("monitoring:workspace_settings")


def load_demo_data(request: HttpRequest) -> HttpResponse:
    if request.method != "POST":
        return redirect("monitoring:workspace_settings")
    seed_demo_dataset()
    messages.success(request, "Демо-данные загружены.")
    return redirect("monitoring:workspace_settings")


def download_customer_template(request: HttpRequest, template_key: str) -> HttpResponse:
    definition = TEMPLATE_DEFINITIONS.get(template_key)
    if not definition:
        messages.error(request, "Такой шаблон не найден.")
        return redirect("monitoring:workspace_settings")

    response = HttpResponse(content_type="text/csv; charset=utf-8-sig")
    response["Content-Disposition"] = f'attachment; filename="{definition["filename"]}"'
    response.write("\ufeff")
    writer = csv.writer(response)
    for row in definition["rows"]:
        writer.writerow(row)
    return response
