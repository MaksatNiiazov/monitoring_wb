from django.core.management.base import BaseCommand
from monitoring.models import DailyCampaignProductStat, Product


class Command(BaseCommand):
    help = "Проверка зон для товара"

    def add_arguments(self, parser):
        parser.add_argument("--nm-id", type=int, required=True)
        parser.add_argument("--date", type=str, required=True)

    def handle(self, *args, **options):
        nm_id = options["nm_id"]
        stats_date = options["date"]

        try:
            product = Product.objects.get(nm_id=nm_id)
        except Product.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"Товар {nm_id} не найден"))
            return

        stats = DailyCampaignProductStat.objects.filter(
            product=product,
            stats_date=stats_date,
        ).select_related("campaign")

        self.stdout.write(f"=== Товар {nm_id} за {stats_date} ===\n")

        total_carts = 0
        total_orders = 0
        total_sum = 0

        for s in stats:
            self.stdout.write(
                f"Кампания: {s.campaign.name}\n"
                f"  Группа: {s.campaign.monitoring_group}\n"
                f"  Зона: {s.zone}\n"
                f"  Корзины: {s.add_to_cart_count}\n"
                f"  Заказы: {s.order_count}\n"
                f"  Сумма: {s.order_sum}\n"
                f"  Затраты: {s.spend}\n"
            )
            total_carts += s.add_to_cart_count
            total_orders += s.order_count
            total_sum += s.order_sum

        self.stdout.write(f"\nИТОГО ВСЕХ ЗАПИСЕЙ:\n")
        self.stdout.write(f"  Корзины: {total_carts}\n")
        self.stdout.write(f"  Заказы: {total_orders}\n")
        self.stdout.write(f"  Сумма: {total_sum}\n")

        # Подбор комбинаций для целевых значений
        self.stdout.write(f"\n=== ПОДБОР КОМБИНАЦИЙ для 135/77/273310 ===\n")

        # Группировка по зонам
        by_zone = {"search": {"carts": 0, "orders": 0, "sum": 0},
                   "recommendation": {"carts": 0, "orders": 0, "sum": 0},
                   "catalog": {"carts": 0, "orders": 0, "sum": 0},
                   "unknown": {"carts": 0, "orders": 0, "sum": 0}}

        for s in stats:
            zone = s.zone
            by_zone[zone]["carts"] += s.add_to_cart_count
            by_zone[zone]["orders"] += s.order_count
            by_zone[zone]["sum"] += s.order_sum

        self.stdout.write("По зонам (все кампании):\n")
        for zone, data in by_zone.items():
            self.stdout.write(f"  {zone}: Корзины={data['carts']}, Заказы={data['orders']}, Сумма={data['sum']}\n")

        # Проверка комбинаций
        combos = [
            ("catalog", "recommendation"),
            ("catalog", "recommendation", "unknown"),
            ("catalog", "recommendation", "search"),
            ("catalog", "unknown"),
            ("recommendation", "search"),
            ("catalog", "search"),
        ]

        for combo in combos:
            carts = sum(by_zone[z]["carts"] for z in combo)
            orders = sum(by_zone[z]["orders"] for z in combo)
            sum_val = sum(by_zone[z]["sum"] for z in combo)
            match = "✓ ПОХОЖЕ" if abs(carts - 135) <= 5 and abs(orders - 77) <= 5 else ""
            self.stdout.write(f"  {'+'.join(combo)}: {carts}/{orders}/{sum_val} {match}\n")

        self.stdout.write(f"\nОжидалось: Корзины=135, Заказы=77, Сумма=273310")
