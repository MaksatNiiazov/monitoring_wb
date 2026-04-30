from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db.models import Count, Q

from monitoring.models import Product


class Command(BaseCommand):
    help = "Remove empty auto-created WB products that have no monitoring data."

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Actually delete rows. Without this flag the command only prints candidates.",
        )

    def handle(self, *args, **options) -> None:
        queryset = (
            Product.objects.annotate(
                daily_metrics_count=Count("daily_metrics", distinct=True),
                daily_stocks_count=Count("daily_stocks", distinct=True),
                warehouse_stocks_count=Count("warehouse_stocks", distinct=True),
                campaign_stats_count=Count("campaign_stats", distinct=True),
                search_cluster_stats_count=Count("search_cluster_stats", distinct=True),
                daily_keyword_stats_count=Count("daily_keyword_stats", distinct=True),
                daily_notes_count=Count("daily_notes", distinct=True),
                keywords_count=Count("keywords", distinct=True),
                economics_versions_count=Count("economics_versions", distinct=True),
                visible_warehouse_rules_count=Count("visible_warehouse_rules", distinct=True),
            )
            .filter(
                Q(vendor_code="") | Q(vendor_code__isnull=True),
                title__startswith="WB ",
                daily_metrics_count=0,
                daily_stocks_count=0,
                warehouse_stocks_count=0,
                campaign_stats_count=0,
                search_cluster_stats_count=0,
                daily_keyword_stats_count=0,
                daily_notes_count=0,
                keywords_count=0,
                economics_versions_count=0,
                visible_warehouse_rules_count=0,
            )
            .order_by("nm_id")
        )
        ids = list(queryset.values_list("id", flat=True))
        names = list(queryset.values_list("nm_id", "title"))
        if not names:
            self.stdout.write(self.style.SUCCESS("Empty auto-created WB products not found."))
            return

        for nm_id, title in names:
            self.stdout.write(f"{nm_id}: {title}")
        if not options["apply"]:
            self.stdout.write(self.style.WARNING(f"Dry run: {len(ids)} products found. Add --apply to delete."))
            return

        deleted, _ = Product.objects.filter(id__in=ids).delete()
        self.stdout.write(self.style.SUCCESS(f"Deleted {deleted} rows."))
