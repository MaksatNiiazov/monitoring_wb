from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand, CommandError
from django.utils.dateparse import parse_date

from monitoring.services.google_sheets import GoogleSheetsSyncError, sync_reports_to_google_sheets


class Command(BaseCommand):
    help = "Выгружает данные мониторинга в Google Sheets за выбранную дату."

    def add_arguments(self, parser) -> None:
        parser.add_argument("--reference-date", type=str, help="Дата среза остатков в формате YYYY-MM-DD")
        parser.add_argument("--history-days", type=int, default=None, help="Глубина истории в днях.")

    def handle(self, *args, **options):
        reference_date: date | None = None
        if options["reference_date"]:
            reference_date = parse_date(options["reference_date"])
            if reference_date is None:
                raise CommandError("reference-date должен быть в формате YYYY-MM-DD")
        else:
            reference_date = date.today()

        try:
            updated_count = sync_reports_to_google_sheets(
                reference_date=reference_date,
                history_days=options["history_days"],
            )
        except GoogleSheetsSyncError as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(self.style.SUCCESS(f"Google Sheets обновлён: {updated_count} листов."))
