from __future__ import annotations

import time
from zoneinfo import ZoneInfo

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from monitoring.models import SyncKind
from monitoring.services.config import get_monitoring_settings
from monitoring.services.sync import SyncServiceError, next_run_at, run_sync


class Command(BaseCommand):
    help = "Запускает фоновый цикл ежедневной синхронизации без внешнего планировщика."

    def _report_now(self, timezone_name: str):
        try:
            report_zone = ZoneInfo(timezone_name)
        except Exception:
            return timezone.localtime()
        return timezone.now().astimezone(report_zone)

    def handle(self, *args, **options) -> None:
        self.stdout.write(
            "Планировщик запущен. Расписание и часовой пояс будут перечитываться из настроек перед каждым циклом."
        )
        try:
            while True:
                runtime_settings = get_monitoring_settings()
                now = self._report_now(runtime_settings.report_timezone)
                scheduled_at = next_run_at(now, runtime_settings.sync_hour, runtime_settings.sync_minute)
                wait_seconds = max(int((scheduled_at - now).total_seconds()), 0)
                self.stdout.write(f"Ожидание до {scheduled_at:%d.%m.%Y %H:%M:%S} ({wait_seconds} сек.)")
                time.sleep(wait_seconds)
                try:
                    runtime_settings = get_monitoring_settings()
                    report_now = self._report_now(runtime_settings.report_timezone)
                    run_sync(
                        kind=SyncKind.FULL,
                        overwrite=True,
                        reference_date=report_now.date(),
                    )
                    self.stdout.write(self.style.SUCCESS("Ежедневная синхронизация выполнена."))
                except SyncServiceError as exc:
                    self.stderr.write(self.style.ERROR(f"Ошибка ежедневной синхронизации: {exc}"))
                time.sleep(settings.WB_SYNC_SLEEP_SECONDS)
        except KeyboardInterrupt as exc:
            raise CommandError("Планировщик остановлен пользователем.") from exc
