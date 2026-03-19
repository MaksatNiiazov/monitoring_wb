from __future__ import annotations

from datetime import datetime, time, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from django.conf import settings
from django.db.models import Count, Max, Q
from django.db.utils import OperationalError, ProgrammingError
from django.utils import timezone

from monitoring.models import (
    Campaign,
    DailyProductMetrics,
    DailyProductStock,
    MonitoringSettings,
    Product,
    SyncLog,
    SyncStatus,
    Warehouse,
)


def get_monitoring_settings():
    try:
        return MonitoringSettings.get_solo()
    except (OperationalError, ProgrammingError):
        return SimpleNamespace(
            project_name="Мониторинг WB",
            report_timezone=settings.WB_REPORT_TIMEZONE,
            sync_hour=settings.WB_SYNC_HOUR,
            sync_minute=settings.WB_SYNC_MINUTE,
            overwrite_within_day=True,
            monitoring_history_days=14,
            google_sheets_enabled=False,
            google_sheets_auto_sync=True,
            google_spreadsheet_id="",
            google_dashboard_sheet_name="Dashboard",
            visible_warehouses_note="",
            campaign_grouping_note="",
        )


def _report_now(runtime_settings) -> datetime:
    timezone_name = getattr(runtime_settings, "report_timezone", "") or settings.WB_REPORT_TIMEZONE
    try:
        target_zone = ZoneInfo(timezone_name)
    except Exception:
        return timezone.localtime()
    return timezone.now().astimezone(target_zone)


def _next_sync_run(runtime_settings, report_now: datetime) -> datetime:
    scheduled = datetime.combine(
        report_now.date(),
        time(
            hour=getattr(runtime_settings, "sync_hour", settings.WB_SYNC_HOUR),
            minute=getattr(runtime_settings, "sync_minute", settings.WB_SYNC_MINUTE),
        ),
        tzinfo=report_now.tzinfo,
    )
    if scheduled <= report_now:
        scheduled += timedelta(days=1)
    return scheduled


def _compact_sync_message(message: str | None) -> str:
    if not message:
        return "Последний успешный запуск завершился без критических ошибок."
    normalized = " ".join(message.split())
    first_sentence = normalized.split(". ", 1)[0].strip()
    compact = first_sentence or normalized
    if len(compact) > 120:
        compact = compact[:117].rstrip() + "..."
    return compact


def build_workspace_overview() -> dict:
    runtime_settings = get_monitoring_settings()
    report_now = _report_now(runtime_settings)
    latest_metrics_date = DailyProductMetrics.objects.aggregate(latest=Max("stats_date"))["latest"]
    latest_stock_date = DailyProductStock.objects.aggregate(latest=Max("stats_date"))["latest"]
    last_success = SyncLog.objects.filter(status=SyncStatus.SUCCESS).first()
    last_error = SyncLog.objects.filter(status=SyncStatus.ERROR).first()
    configured_products = Product.objects.filter(is_active=True).count()
    configured_campaigns = Campaign.objects.filter(is_active=True).count()

    google_ready = bool(
        (settings.GOOGLE_SERVICE_ACCOUNT_FILE or settings.GOOGLE_SERVICE_ACCOUNT_JSON)
        and getattr(runtime_settings, "google_spreadsheet_id", "")
    )
    google_spreadsheet_id = getattr(runtime_settings, "google_spreadsheet_id", "")
    spreadsheet_url = (
        f"https://docs.google.com/spreadsheets/d/{google_spreadsheet_id}/edit"
        if google_spreadsheet_id
        else ""
    )
    next_run = _next_sync_run(runtime_settings, report_now)
    today_sync_cutoff = datetime.combine(
        report_now.date(),
        time(
            hour=getattr(runtime_settings, "sync_hour", settings.WB_SYNC_HOUR),
            minute=getattr(runtime_settings, "sync_minute", settings.WB_SYNC_MINUTE),
        ),
        tzinfo=report_now.tzinfo,
    )
    if report_now < today_sync_cutoff:
        stock_expected_date = report_now.date() - timedelta(days=1)
        stats_expected_date = report_now.date() - timedelta(days=2)
    else:
        stock_expected_date = report_now.date()
        stats_expected_date = report_now.date() - timedelta(days=1)

    warnings: list[str] = []
    if not latest_metrics_date:
        warnings.append("В базе ещё нет рекламной статистики и общей воронки.")
    elif latest_metrics_date < stats_expected_date:
        warnings.append(
            f"Последняя рекламная статистика в базе от {latest_metrics_date:%d.%m.%Y}. "
            f"Ожидалась не старше {stats_expected_date:%d.%m.%Y}."
        )

    if not latest_stock_date:
        warnings.append("В базе ещё нет среза остатков и складских данных.")
    elif latest_stock_date < stock_expected_date:
        warnings.append(
            f"Последний срез остатков в базе от {latest_stock_date:%d.%m.%Y}. "
            f"Ожидался срез за {stock_expected_date:%d.%m.%Y}."
        )

    if not google_ready and getattr(runtime_settings, "google_sheets_enabled", False):
        warnings.append("Google Sheets включён в настройках, но не хватает service account или ID таблицы.")
    if last_error and (not last_success or last_error.created_at > last_success.created_at):
        warnings.append(f"Последний запуск завершился ошибкой: {last_error.message or 'без текста ошибки'}.")

    signals = [
        {
            "label": "Последний успешный sync",
            "value": last_success.finished_at.astimezone(report_now.tzinfo).strftime("%d.%m.%Y %H:%M")
            if last_success and last_success.finished_at
            else "Пока не было",
            "detail": _compact_sync_message(last_success.message) if last_success else "Ещё не было успешного полного обновления.",
            "tone": "positive" if last_success else "warning",
        },
        {
            "label": "Следующий автозапуск",
            "value": next_run.strftime("%d.%m.%Y %H:%M"),
            "detail": f"Пояс проекта: {getattr(runtime_settings, 'report_timezone', settings.WB_REPORT_TIMEZONE)}",
            "tone": "neutral",
        },
        {
            "label": "Свежесть данных",
            "value": f"РК {latest_metrics_date:%d.%m}" if latest_metrics_date else "РК —",
            "detail": f"Остатки {latest_stock_date:%d.%m.%Y}" if latest_stock_date else "Остатки ещё не собраны",
            "tone": "positive" if latest_metrics_date and latest_stock_date else "warning",
        },
        {
            "label": "Google Sheets",
            "value": "Подключён" if google_ready else "Ожидает настройки",
            "detail": "Таблица подключена и готова к синку." if google_ready else "Нужны service account и spreadsheet ID.",
            "tone": "positive" if google_ready else "warning",
        },
    ]

    return {
        "report_now": report_now,
        "next_run": next_run,
        "latest_metrics_date": latest_metrics_date,
        "latest_stock_date": latest_stock_date,
        "last_success": last_success,
        "last_error": last_error,
        "configured_products": configured_products,
        "configured_campaigns": configured_campaigns,
        "google_ready": google_ready,
        "google_spreadsheet_id": google_spreadsheet_id,
        "spreadsheet_url": spreadsheet_url,
        "warnings": warnings,
        "signals": signals,
    }


def build_campaign_overview(limit: int = 12) -> list[Campaign]:
    return list(
        Campaign.objects.filter(is_active=True)
        .annotate(
            products_count=Count("products", distinct=True),
            latest_stats_date=Max("daily_stats__stats_date"),
        )
        .order_by("monitoring_group", "name", "external_id")[:limit]
    )


def build_readiness_summary() -> list[dict]:
    configured_products = Product.objects.filter(is_active=True).count()
    configured_campaigns = Campaign.objects.filter(is_active=True).count()
    priced_products = (
        Product.objects.filter(is_active=True)
        .filter(Q(economics_versions__isnull=False) | Q(unit_cost__gt=0, logistics_cost__gt=0))
        .distinct()
        .count()
    )
    warehouses_total = Warehouse.objects.count()
    products_with_warehouse_rules = (
        Product.objects.filter(is_active=True, visible_warehouse_rules__isnull=False).distinct().count()
    )
    return [
        {
            "title": "Токен Analytics",
            "ready": bool(settings.WB_ANALYTICS_API_TOKEN),
            "detail": "Нужен для воронки и остатков.",
        },
        {
            "title": "Токен Promotion",
            "ready": bool(settings.WB_PROMOTION_API_TOKEN),
            "detail": "Нужен для статистики рекламных кампаний.",
        },
        {
            "title": "Товары",
            "ready": configured_products > 0,
            "detail": f"Заведено {configured_products} товаров.",
        },
        {
            "title": "Экономика товаров",
            "ready": priced_products == configured_products and configured_products > 0,
            "detail": f"Заполнено {priced_products} из {configured_products}.",
        },
        {
            "title": "Рекламные кампании",
            "ready": configured_campaigns > 0,
            "detail": f"Заведено {configured_campaigns} кампаний.",
        },
        {
            "title": "Google Sheets",
            "ready": bool(settings.GOOGLE_SERVICE_ACCOUNT_FILE or settings.GOOGLE_SERVICE_ACCOUNT_JSON),
            "detail": "Нужен service account JSON для записи в таблицу.",
        },
        {
            "title": "Склады показа",
            "ready": configured_products > 0 and products_with_warehouse_rules == configured_products,
            "detail": f"Настроено для {products_with_warehouse_rules} из {configured_products} товаров. Складов в базе: {warehouses_total}.",
        },
    ]
