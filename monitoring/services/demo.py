from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from monitoring.models import (
    Campaign,
    CampaignMonitoringGroup,
    CampaignZone,
    DailyCampaignProductStat,
    DailyProductMetrics,
    DailyProductNote,
    DailyProductStock,
    Product,
    ProductEconomicsVersion,
    ProductCampaign,
    ProductVisibleWarehouse,
)


def seed_demo_dataset() -> None:
    first_product, _ = Product.objects.update_or_create(
        nm_id=111000111,
        defaults={
            "title": "Костюм Urban Motion",
            "vendor_code": "UM-001",
            "brand_name": "Urban Motion",
            "subject_name": "Костюм",
            "buyout_percent": Decimal("24.00"),
            "unit_cost": Decimal("1500.00"),
            "logistics_cost": Decimal("336.00"),
            "primary_keyword": "костюмы спортивные женский",
            "secondary_keyword": "весенний женский костюм",
        },
    )
    second_product, _ = Product.objects.update_or_create(
        nm_id=111000222,
        defaults={
            "title": "Лонгслив Mono Line",
            "vendor_code": "ML-002",
            "brand_name": "Mono Line",
            "subject_name": "Лонгслив",
            "buyout_percent": Decimal("28.00"),
            "unit_cost": Decimal("620.00"),
            "logistics_cost": Decimal("140.00"),
            "primary_keyword": "лонгслив женский",
            "secondary_keyword": "лонгслив базовый",
        },
    )
    unified, _ = Campaign.objects.update_or_create(
        external_id=28150154,
        defaults={"name": "Единая ставка", "monitoring_group": CampaignMonitoringGroup.UNIFIED},
    )
    manual_search, _ = Campaign.objects.update_or_create(
        external_id=28150155,
        defaults={"name": "РС Поиск", "monitoring_group": CampaignMonitoringGroup.MANUAL_SEARCH},
    )
    manual_shelves, _ = Campaign.objects.update_or_create(
        external_id=28150156,
        defaults={"name": "РС Полки", "monitoring_group": CampaignMonitoringGroup.MANUAL_SHELVES},
    )
    manual_catalog, _ = Campaign.objects.update_or_create(
        external_id=28150157,
        defaults={"name": "РС Каталог", "monitoring_group": CampaignMonitoringGroup.MANUAL_CATALOG},
    )

    for product in (first_product, second_product):
        for campaign in (unified, manual_search, manual_catalog, manual_shelves):
            ProductCampaign.objects.get_or_create(product=product, campaign=campaign)

    for product in (first_product, second_product):
        ProductEconomicsVersion.objects.update_or_create(
            product=product,
            effective_from=date.today(),
            defaults={
                "buyout_percent": product.buyout_percent,
                "unit_cost": product.unit_cost,
                "logistics_cost": product.logistics_cost,
            },
        )
        ProductVisibleWarehouse.objects.filter(product=product).delete()
        ProductVisibleWarehouse.objects.bulk_create(
            [
                ProductVisibleWarehouse(product=product, warehouse_name=name)
                for name in ["Коледино", "Казань", "Электросталь", "Краснодар", "Тула"]
            ]
        )

    today = date.today()
    for offset in range(7):
        stats_date = today - timedelta(days=offset + 1)
        stock_date = today - timedelta(days=offset)

        first_orders = 14 - offset
        second_orders = 10 - offset

        DailyProductMetrics.objects.update_or_create(
            product=first_product,
            stats_date=stats_date,
            defaults={
                "open_count": 430 - offset * 18,
                "add_to_cart_count": 91 - offset * 4,
                "order_count": first_orders,
                "order_sum": Decimal(str(59000 - offset * 2600)),
                "buyout_count": max(first_orders - 4, 0),
                "buyout_sum": Decimal(str(41000 - offset * 1800)),
            },
        )
        DailyProductMetrics.objects.update_or_create(
            product=second_product,
            stats_date=stats_date,
            defaults={
                "open_count": 350 - offset * 14,
                "add_to_cart_count": 67 - offset * 3,
                "order_count": second_orders,
                "order_sum": Decimal(str(31000 - offset * 1700)),
                "buyout_count": max(second_orders - 2, 0),
                "buyout_sum": Decimal(str(23000 - offset * 1500)),
            },
        )

        DailyProductStock.objects.update_or_create(
            product=first_product,
            stats_date=stock_date,
            defaults={
                "total_stock": 99 - offset * 2,
                "in_way_to_client": 139 - offset * 2,
                "in_way_from_client": 76 - offset,
            },
        )
        DailyProductStock.objects.update_or_create(
            product=second_product,
            stats_date=stock_date,
            defaults={
                "total_stock": 144 - offset * 3,
                "in_way_to_client": 63 - offset,
                "in_way_from_client": 22,
            },
        )

        DailyProductNote.objects.update_or_create(
            product=first_product,
            note_date=stats_date,
            defaults={
                "spp_percent": Decimal("18.00"),
                "seller_price": Decimal("5025.00"),
                "wb_price": Decimal("4145.00"),
                "promo_status": "Не участвуем",
                "negative_feedback": "Без изменений",
                "unified_enabled": True,
                "manual_search_enabled": False,
                "manual_shelves_enabled": False,
                "ads_budget": Decimal("10000.00"),
                "price_change_status": "Нет",
                "price_change_amount": Decimal("0.00"),
                "comment": "Демо-сценарий: держим органику и наблюдаем за остатком.",
            },
        )

        for campaign, zone, views, clicks, orders, spend, order_sum in (
            (unified, CampaignZone.SEARCH, 1800 - offset * 70, 162 - offset * 6, 8 - offset // 2, Decimal("3400.00") - Decimal(offset * 120), Decimal("32000.00") - Decimal(offset * 1500)),
            (unified, CampaignZone.RECOMMENDATION, 960 - offset * 40, 76 - offset * 3, 3, Decimal("1250.00") - Decimal(offset * 30), Decimal("11200.00") - Decimal(offset * 450)),
            (unified, CampaignZone.CATALOG, 720 - offset * 35, 48 - offset * 2, 2, Decimal("840.00") - Decimal(offset * 25), Decimal("6900.00") - Decimal(offset * 300)),
            (manual_search, CampaignZone.SEARCH, 670 - offset * 28, 58 - offset * 2, 2, Decimal("980.00") - Decimal(offset * 20), Decimal("8900.00") - Decimal(offset * 280)),
            (manual_catalog, CampaignZone.CATALOG, 410 - offset * 18, 33 - offset, 1, Decimal("610.00") - Decimal(offset * 14), Decimal("5200.00") - Decimal(offset * 160)),
            (manual_shelves, CampaignZone.RECOMMENDATION, 320 - offset * 16, 29 - offset, 1, Decimal("420.00") - Decimal(offset * 10), Decimal("4300.00") - Decimal(offset * 120)),
        ):
            DailyCampaignProductStat.objects.update_or_create(
                campaign=campaign,
                product=first_product,
                stats_date=stats_date,
                zone=zone,
                defaults={
                    "impressions": max(int(views), 0),
                    "clicks": max(int(clicks), 0),
                    "spend": max(spend, Decimal("0.00")),
                    "add_to_cart_count": max(int(orders * 2), 0),
                    "order_count": max(int(orders), 0),
                    "units_ordered": max(int(orders), 0),
                    "order_sum": max(order_sum, Decimal("0.00")),
                },
            )
