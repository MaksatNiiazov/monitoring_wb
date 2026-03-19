from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand, CommandError
from django.utils.dateparse import parse_date

from monitoring.models import SyncKind
from monitoring.services.sync import SyncServiceError, run_sync


class Command(BaseCommand):
    help = "Синхронизирует статистику WB по товарам, рекламе и остаткам."

    def add_arguments(self, parser) -> None:
        parser.add_argument("--reference-date", type=str, help="Дата запуска в формате YYYY-MM-DD")
        parser.add_argument("--product-id", action="append", dest="product_ids", type=int, help="ID товара в БД")
        parser.add_argument("--no-overwrite", action="store_true", help="Не перезаписывать существующие записи")

    def handle(self, *args, **options) -> None:
        reference_date: date | None = None
        if options["reference_date"]:
            reference_date = parse_date(options["reference_date"])
            if reference_date is None:
                raise CommandError("reference-date должен быть в формате YYYY-MM-DD")

        try:
            log = run_sync(
                product_ids=options.get("product_ids"),
                reference_date=reference_date,
                overwrite=not options["no_overwrite"],
                kind=SyncKind.PRODUCT if options.get("product_ids") else SyncKind.FULL,
            )
        except SyncServiceError as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(self.style.SUCCESS(f"Синхронизация завершена: {log.message}"))
