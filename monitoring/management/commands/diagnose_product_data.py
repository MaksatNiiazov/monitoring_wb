from __future__ import annotations

from datetime import date
from decimal import Decimal
from django.core.management.base import BaseCommand
from django.db.models import Sum
from monitoring.models import (
    Campaign,
    DailyCampaignProductStat,
    DailyProductMetrics,
    Product,
)
from monitoring.services.reports import build_product_report


class Command(BaseCommand):
    help = "Диагностика данных для конкретного товара за дату"

    def add_arguments(self, parser):
        parser.add_argument("--product-id", type=int, required=True, help="ID товара в БД")
        parser.add_argument("--date", type=str, required=True, help="Дата в формате YYYY-MM-DD")

    def handle(self, *args, **options):
        product_id = options["product_id"]
        stats_date = date.fromisoformat(options["date"])

        try:
            product = Product.objects.get(id=product_id)
        except Product.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"Товар с ID {product_id} не найден"))
            return

        self.stdout.write(self.style.NOTICE(f"=== Диагностика товара {product.nm_id} (ID: {product.id}) за {stats_date} ===\n"))

        # 1. Проверяем сырые данные из БД
        self.stdout.write(self.style.NOTICE("1. Данные из DailyCampaignProductStat:"))
        campaign_stats = DailyCampaignProductStat.objects.filter(
            product=product,
            stats_date=stats_date,
        ).select_related("campaign")

        if not campaign_stats:
            self.stdout.write(self.style.WARNING("   Нет записей в DailyCampaignProductStat!"))
        else:
            for stat in campaign_stats:
                self.stdout.write(
                    f"   Кампания: {stat.campaign.name} (группа: {stat.campaign.monitoring_group}, зона: {stat.zone})\n"
                    f"   - Корзины: {stat.add_to_cart_count}\n"
                    f"   - Заказы: {stat.order_count}\n"
                    f"   - Заказы (руб.): {stat.order_sum}\n"
                    f"   - Затраты: {stat.spend}\n"
                )

        # Суммы по группам
        self.stdout.write(self.style.NOTICE("\n2. Суммы по группам кампаний:"))
        by_group = {}
        for stat in campaign_stats:
            group = stat.campaign.monitoring_group
            zone = stat.zone
            key = f"{group} / {zone}"
            if key not in by_group:
                by_group[key] = {"carts": 0, "orders": 0, "order_sum": Decimal("0"), "spend": Decimal("0")}
            by_group[key]["carts"] += stat.add_to_cart_count
            by_group[key]["orders"] += stat.order_count
            by_group[key]["order_sum"] += stat.order_sum
            by_group[key]["spend"] += stat.spend

        for key, values in by_group.items():
            self.stdout.write(f"   {key}:")
            self.stdout.write(f"     Корзины: {values['carts']}, Заказы: {values['orders']}, Сумма: {values['order_sum']}")

        # 3. Общие метрики (Sales Funnel)
        self.stdout.write(self.style.NOTICE("\n3. Общие метрики (DailyProductMetrics):"))
        metrics = DailyProductMetrics.objects.filter(
            product=product,
            stats_date=stats_date,
        ).first()

        if metrics:
            self.stdout.write(
                f"   - Переходы: {metrics.open_count}\n"
                f"   - Корзины: {metrics.add_to_cart_count}\n"
                f"   - Заказы: {metrics.order_count}\n"
                f"   - Сумма заказов: {metrics.order_sum}\n"
            )
        else:
            self.stdout.write(self.style.WARNING("   Нет записей в DailyProductMetrics!"))

        # 4. Проверяем отчет
        self.stdout.write(self.style.NOTICE("\n4. Данные в отчете (build_product_report):"))
        report = build_product_report(
            product=product,
            stats_date=stats_date,
            stock_date=stats_date,
            create_note=False,
        )

        blocks = report["table_blocks"]
        self.stdout.write("   Блоки таблицы:")
        self.stdout.write(f"   - unified_search: Корзины={blocks['search'].carts}, Заказы={blocks['search'].orders}")
        self.stdout.write(f"   - unified_shelves: Корзины={blocks['shelves'].carts}, Заказы={blocks['shelves'].orders}")
        self.stdout.write(f"   - unified_catalog: Корзины={blocks['catalog'].carts}, Заказы={blocks['catalog'].orders}")
        self.stdout.write(f"   - manual: Корзины={blocks['manual'].carts}, Заказы={blocks['manual'].orders}")
        self.stdout.write(f"   - ad_total: Корзины={blocks['ad_total'].carts}, Заказы={blocks['ad_total'].orders}")

        # 5. Проверка сумм
        self.stdout.write(self.style.NOTICE("\n5. Проверка сумм:"))
        total_from_blocks = (
            blocks["search"].carts +
            blocks["shelves"].carts +
            blocks["catalog"].carts +
            blocks["manual"].carts
        )
        self.stdout.write(f"   Сумма корзин по блокам: {total_from_blocks}")
        self.stdout.write(f"   ad_total.carts: {blocks['ad_total'].carts}")
        if metrics:
            self.stdout.write(f"   metrics.add_to_cart_count: {metrics.add_to_cart_count}")

        self.stdout.write(self.style.NOTICE("\n=== Диагностика завершена ==="))
