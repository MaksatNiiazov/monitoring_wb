from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from django.core.management.base import BaseCommand
from django.db import OperationalError, ProgrammingError

from monitoring.models import Product
from monitoring.services.reports import build_product_report, decimalize


METRIC_FIELDS = ("spend", "impressions", "clicks", "carts", "orders", "order_sum")


def _as_decimal(value) -> Decimal:
    return decimalize(value or 0)


class Command(BaseCommand):
    help = (
        "Проверяет консистентность данных мониторинга по дням: "
        "суммы по блокам рекламы, общие метрики и доли трафика."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--date",
            dest="target_date",
            help="Дата проверки в формате YYYY-MM-DD. По умолчанию — сегодня.",
        )
        parser.add_argument(
            "--days",
            type=int,
            default=1,
            help="Сколько последних дней проверить (включая --date). По умолчанию 1.",
        )
        parser.add_argument(
            "--tolerance",
            type=float,
            default=0.01,
            help="Допустимая погрешность сравнения. По умолчанию 0.01.",
        )
        parser.add_argument(
            "--product-id",
            type=int,
            dest="product_id",
            help="Проверить только один товар.",
        )

    def handle(self, *args, **options):
        target_date_raw = options.get("target_date")
        target_date = date.fromisoformat(target_date_raw) if target_date_raw else date.today()
        days = max(1, int(options["days"]))
        tolerance = Decimal(str(options["tolerance"]))
        product_id = options.get("product_id")

        products_qs = Product.objects.filter(is_active=True).order_by("id")
        if product_id:
            products_qs = products_qs.filter(id=product_id)
        try:
            products = list(products_qs)
        except (OperationalError, ProgrammingError) as exc:
            self.stdout.write(
                self.style.WARNING(
                    "Не удалось получить товары для проверки: "
                    f"{exc}. Возможно, база не инициализирована в текущем окружении."
                )
            )
            return
        if not products:
            self.stdout.write(self.style.WARNING("Нет активных товаров для проверки."))
            return

        checked = 0
        issues: list[str] = []

        for offset in range(days):
            current_date = target_date - timedelta(days=offset)
            for product in products:
                report = build_product_report(
                    product=product,
                    stats_date=current_date,
                    stock_date=current_date,
                    create_note=False,
                )
                checked += 1
                issues.extend(self._check_report(product=product, report=report, tolerance=tolerance))

        self.stdout.write(
            self.style.SUCCESS(
                f"Проверено {checked} отчётов "
                f"(товаров: {len(products)}, дней: {days}, tolerance: {tolerance})."
            )
        )

        if issues:
            self.stdout.write(self.style.ERROR(f"Найдено расхождений: {len(issues)}"))
            for item in issues:
                self.stdout.write(self.style.ERROR(f"- {item}"))
            raise SystemExit(1)

        self.stdout.write(self.style.SUCCESS("Критичных расхождений не найдено."))

    def _check_report(self, *, product: Product, report: dict, tolerance: Decimal) -> list[str]:
        stats_date = report["stats_date"]
        table_blocks = report["table_blocks"]
        search = table_blocks["search"]
        shelves = table_blocks["shelves"]
        catalog = table_blocks["catalog"]
        manual = table_blocks["manual"]
        ad_total = table_blocks["ad_total"]
        metrics = report.get("metrics")

        issues: list[str] = []
        prefix = f"product_id={product.id}, date={stats_date}"

        components = (search, shelves, catalog, manual)
        for field in METRIC_FIELDS:
            total_value = _as_decimal(getattr(ad_total, field))
            sum_value = sum((_as_decimal(getattr(cell, field)) for cell in components), start=Decimal("0"))
            if abs(total_value - sum_value) > tolerance:
                issues.append(
                    f"{prefix}: ad_total.{field}={total_value} != "
                    f"sum(blocks)={sum_value} (Δ={total_value - sum_value})"
                )

        if metrics is not None:
            checks = {
                "clicks_vs_open_count": (_as_decimal(ad_total.clicks), _as_decimal(metrics.open_count)),
                "carts_vs_add_to_cart_count": (_as_decimal(ad_total.carts), _as_decimal(metrics.add_to_cart_count)),
                "orders_vs_order_count": (_as_decimal(ad_total.orders), _as_decimal(metrics.order_count)),
            }
            for check_name, (ad_value, metric_value) in checks.items():
                if ad_value > metric_value + tolerance:
                    issues.append(
                        f"{prefix}: {check_name} ad={ad_value} > overall_metrics={metric_value}"
                    )

        unified_total_impressions = (
            _as_decimal(search.impressions) + _as_decimal(shelves.impressions) + _as_decimal(catalog.impressions)
        )
        if unified_total_impressions > 0:
            unified_traffic_sum = (
                _as_decimal(search.traffic_share(unified_total_impressions))
                + _as_decimal(shelves.traffic_share(unified_total_impressions))
                + _as_decimal(catalog.traffic_share(unified_total_impressions))
            )
            if abs(unified_traffic_sum - Decimal("100")) > Decimal("0.5"):
                issues.append(
                    f"{prefix}: unified traffic sum={unified_traffic_sum} (ожидалось ~100)"
                )

        return issues
