from __future__ import annotations

from datetime import date
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.utils.dateparse import parse_date

from monitoring.services.config import get_monitoring_settings
from monitoring.services.monitoring_table import export_monitoring_workbook_bytes


class Command(BaseCommand):
    help = "Собирает итоговую книгу мониторинга в формате XLSX."

    def add_arguments(self, parser) -> None:
        parser.add_argument("--reference-date", type=str, help="Дата среза остатков в формате YYYY-MM-DD")
        parser.add_argument("--history-days", type=int, default=None, help="Глубина истории в днях.")
        parser.add_argument(
            "--output",
            type=str,
            default=None,
            help="Куда сохранить файл. По умолчанию workbook кладётся в корень проекта.",
        )

    def handle(self, *args, **options):
        reference_date: date | None = None
        if options["reference_date"]:
            reference_date = parse_date(options["reference_date"])
            if reference_date is None:
                raise CommandError("reference-date должен быть в формате YYYY-MM-DD")
        else:
            reference_date = date.today()

        settings = get_monitoring_settings()
        history_days = options["history_days"] or getattr(settings, "monitoring_history_days", 14)
        output_path = Path(options["output"] or f"monitoring_wb_{reference_date.isoformat()}_{history_days}d.xlsx")

        workbook_bytes = export_monitoring_workbook_bytes(
            reference_date=reference_date,
            history_days=history_days,
        )
        output_path.write_bytes(workbook_bytes)
        self.stdout.write(self.style.SUCCESS(f"Книга мониторинга сохранена: {output_path}"))
