from io import BytesIO
import json
import re
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch

from django.test import TestCase
from django.utils import timezone
from openpyxl import load_workbook

from monitoring.models import (
    Campaign,
    CampaignMonitoringGroup,
    CampaignZone,
    DailyCampaignProductStat,
    DailyCampaignSearchClusterStat,
    DailyProductKeywordStat,
    DailyProductMetrics,
    DailyProductNote,
    DailyProductStock,
    DailyWarehouseStock,
    MonitoringSettings,
    Product,
    ProductEconomicsVersion,
    ProductVisibleWarehouse,
    SyncKind,
    SyncLog,
    SyncStatus,
    Warehouse,
)
from monitoring.forms import ProductSettingsForm, ReportsFilterForm, SyncForm
from monitoring.services.config import build_workspace_overview
from monitoring.services.demo import seed_demo_dataset
from monitoring.services.exporters import exporter_rows
from monitoring.services.monitoring_table import (
    build_day_block,
    build_monitoring_sheet_payloads,
    build_table_view_payloads,
    export_monitoring_workbook_bytes,
)
from monitoring.services.reporting_hub import build_reports_context
from monitoring.services.reports import build_dashboard_context, build_product_report
from monitoring.services.sync import (
    aggregate_offices_from_sizes,
    build_product_stock_payload_from_sizes,
    next_run_at,
    resolve_office_id,
    run_sync,
    SyncServiceError,
)
from monitoring.templatetags.monitoring_extras import css_percent
from monitoring.views import _build_stock_popup_payload
from monitoring.services.wb_client import AnalyticsWBClient, WBApiError


class ReportingTests(TestCase):
    def setUp(self) -> None:
        self.product = Product.objects.create(
            nm_id=123456,
            title="Женский спортивный костюм",
            vendor_code="SKU-001",
            buyout_percent=Decimal("24.00"),
            unit_cost=Decimal("1500.00"),
            logistics_cost=Decimal("336.00"),
            primary_keyword="костюмы спортивные женский",
            secondary_keyword="весенний женский костюм",
        )
        self.campaign = Campaign.objects.create(
            external_id=28150154,
            name="Единая ставка",
            monitoring_group=CampaignMonitoringGroup.UNIFIED,
        )
        self.campaign.products.add(self.product)
        ProductEconomicsVersion.objects.create(
            product=self.product,
            effective_from=date(2026, 3, 1),
            buyout_percent=Decimal("24.00"),
            unit_cost=Decimal("1500.00"),
            logistics_cost=Decimal("336.00"),
        )
        DailyProductMetrics.objects.create(
            product=self.product,
            stats_date=date(2026, 3, 16),
            open_count=100,
            add_to_cart_count=50,
            order_count=10,
            order_sum=Decimal("10000.00"),
            buyout_count=5,
            buyout_sum=Decimal("5000.00"),
        )
        DailyProductStock.objects.create(
            product=self.product,
            stats_date=date(2026, 3, 17),
            total_stock=99,
            in_way_to_client=139,
            in_way_from_client=76,
        )
        DailyCampaignProductStat.objects.create(
            campaign=self.campaign,
            product=self.product,
            stats_date=date(2026, 3, 16),
            zone=CampaignZone.SEARCH,
            impressions=1000,
            clicks=100,
            spend=Decimal("2000.00"),
            add_to_cart_count=20,
            order_count=6,
            order_sum=Decimal("6000.00"),
        )

    def test_product_report_calculates_organic_metrics(self) -> None:
        report = build_product_report(
            product=self.product,
            stats_date=date(2026, 3, 16),
            stock_date=date(2026, 3, 17),
        )
        self.assertEqual(report["organic"]["open_count"], 0)
        self.assertEqual(report["organic"]["cart_count"], 30)
        self.assertEqual(report["organic"]["order_count"], 4)
        self.assertEqual(report["organic"]["order_sum"], Decimal("4000.00"))

    def test_product_report_includes_saved_keyword_metrics(self) -> None:
        DailyProductNote.objects.create(
            product=self.product,
            note_date=date(2026, 3, 16),
            keywords=[self.product.primary_keyword],
        )
        DailyProductKeywordStat.objects.create(
            product=self.product,
            stats_date=date(2026, 3, 16),
            query_text=self.product.primary_keyword,
            frequency=120,
            organic_position=Decimal("5.50"),
            boosted_position=Decimal("12.30"),
            boosted_ctr=Decimal("8.40"),
        )
        report = build_product_report(
            product=self.product,
            stats_date=date(2026, 3, 16),
            stock_date=date(2026, 3, 17),
        )

        self.assertEqual(report["keyword_rows"][0]["frequency"], 120)
        self.assertEqual(report["keyword_rows"][0]["organic_position"], Decimal("5.50"))
        self.assertEqual(report["keyword_rows"][0]["boosted_position"], Decimal("12.30"))
        self.assertEqual(report["keyword_rows"][0]["boosted_ctr"], Decimal("8.40"))

    def test_product_report_starts_with_empty_daily_keyword_rows(self) -> None:
        report = build_product_report(
            product=self.product,
            stats_date=date(2026, 3, 16),
            stock_date=date(2026, 3, 17),
        )

        self.assertEqual(len(report["keyword_rows"]), 3)
        self.assertTrue(all(not row["query_text"] for row in report["keyword_rows"]))

    def test_export_contains_sample_header(self) -> None:
        report = build_product_report(
            product=self.product,
            stats_date=date(2026, 3, 16),
            stock_date=date(2026, 3, 17),
        )
        rows = exporter_rows(report)
        self.assertEqual(rows[0][1], "17.03.2026")
        self.assertEqual(rows[4][0], "Затраты (руб)")

    def test_exporter_uses_overall_funnel_in_summary_column(self) -> None:
        report = build_product_report(
            product=self.product,
            stats_date=date(2026, 3, 16),
            stock_date=date(2026, 3, 17),
        )
        rows = exporter_rows(report)
        self.assertEqual(rows[10][6], "50")
        self.assertEqual(rows[12][6], "10")
        self.assertEqual(rows[14][6], "10000")

    def test_exporter_normalizes_buyout_percent_when_saved_as_fraction(self) -> None:
        ProductEconomicsVersion.objects.filter(
            product=self.product,
            effective_from=date(2026, 3, 1),
        ).update(buyout_percent=Decimal("0.24"))
        report = build_product_report(
            product=self.product,
            stats_date=date(2026, 3, 16),
            stock_date=date(2026, 3, 17),
        )
        rows = exporter_rows(report)
        self.assertEqual(rows[21][1], "24%")
        self.assertEqual(rows[15][6], "2400")

    def test_dashboard_context_builds_aggregated_totals(self) -> None:
        context = build_dashboard_context(stats_date=date(2026, 3, 16), stock_date=date(2026, 3, 17))
        self.assertEqual(context["totals"]["products"], 1)
        self.assertEqual(context["totals"]["orders"], 10)
        self.assertEqual(context["totals"]["stock"], 99)

    def test_product_report_contains_insights_and_zone_cards(self) -> None:
        report = build_product_report(
            product=self.product,
            stats_date=date(2026, 3, 16),
            stock_date=date(2026, 3, 17),
        )
        self.assertIn("insights", report)
        self.assertEqual(len(report["traffic_cards"]), 5)

    def test_product_report_does_not_return_negative_days_until_zero_from_stock_drop(self) -> None:
        DailyProductStock.objects.create(
            product=self.product,
            stats_date=date(2026, 3, 16),
            total_stock=80,
            in_way_to_client=0,
            in_way_from_client=0,
        )
        DailyProductStock.objects.create(
            product=self.product,
            stats_date=date(2026, 3, 15),
            total_stock=70,
            in_way_to_client=0,
            in_way_from_client=0,
        )

        report = build_product_report(
            product=self.product,
            stats_date=date(2026, 3, 16),
            stock_date=date(2026, 3, 17),
        )

        self.assertEqual(report["avg_stock_drop_per_day"], Decimal("-14.50"))
        self.assertEqual(report["days_until_zero_from_stock_drop"], Decimal("0"))

    def test_product_report_uses_search_cluster_stats_for_zone_split(self) -> None:
        DailyCampaignSearchClusterStat.objects.create(
            campaign=self.campaign,
            product=self.product,
            stats_date=date(2026, 3, 16),
            impressions=200,
            clicks=20,
            spend=Decimal("400.00"),
            add_to_cart_count=5,
            order_count=2,
            units_ordered=2,
        )
        report = build_product_report(
            product=self.product,
            stats_date=date(2026, 3, 16),
            stock_date=date(2026, 3, 17),
        )
        unified_search = report["blocks"]["unified_search"]
        unified_shelves = report["blocks"]["unified_shelves"]

        self.assertEqual(unified_search.impressions, 200)
        self.assertEqual(unified_search.clicks, 20)
        self.assertEqual(unified_search.spend, Decimal("400.00"))
        self.assertEqual(unified_search.carts, 20)
        self.assertEqual(unified_search.orders, 6)
        self.assertEqual(unified_search.order_sum, Decimal("6000.00"))

        self.assertEqual(unified_shelves.impressions, 800)
        self.assertEqual(unified_shelves.clicks, 80)
        self.assertEqual(unified_shelves.spend, Decimal("1600.00"))
        self.assertEqual(unified_shelves.carts, 0)
        self.assertEqual(unified_shelves.orders, 0)
        self.assertEqual(unified_shelves.order_sum, Decimal("0.00"))

    def test_product_report_keeps_catalog_zone_when_cluster_stats_exist(self) -> None:
        DailyCampaignProductStat.objects.create(
            campaign=self.campaign,
            product=self.product,
            stats_date=date(2026, 3, 16),
            zone=CampaignZone.RECOMMENDATION,
            impressions=500,
            clicks=50,
            spend=Decimal("900.00"),
            add_to_cart_count=10,
            order_count=2,
            order_sum=Decimal("1800.00"),
        )
        DailyCampaignProductStat.objects.create(
            campaign=self.campaign,
            product=self.product,
            stats_date=date(2026, 3, 16),
            zone=CampaignZone.CATALOG,
            impressions=3000,
            clicks=120,
            spend=Decimal("1400.00"),
            add_to_cart_count=8,
            order_count=1,
            order_sum=Decimal("900.00"),
        )
        DailyCampaignSearchClusterStat.objects.create(
            campaign=self.campaign,
            product=self.product,
            stats_date=date(2026, 3, 16),
            impressions=200,
            clicks=20,
            spend=Decimal("400.00"),
            add_to_cart_count=5,
            order_count=2,
            units_ordered=2,
        )

        report = build_product_report(
            product=self.product,
            stats_date=date(2026, 3, 16),
            stock_date=date(2026, 3, 17),
        )

        unified_search = report["blocks"]["unified_search"]
        unified_shelves = report["blocks"]["unified_shelves"]
        unified_catalog = report["blocks"]["unified_catalog"]

        self.assertEqual(unified_search.impressions, 200)
        self.assertEqual(unified_search.clicks, 20)
        self.assertEqual(unified_search.spend, Decimal("400.00"))
        self.assertEqual(unified_shelves.impressions, 500)
        self.assertEqual(unified_shelves.clicks, 50)
        self.assertEqual(unified_shelves.spend, Decimal("900.00"))
        self.assertEqual(unified_catalog.impressions, 3000)
        self.assertEqual(unified_catalog.clicks, 120)
        self.assertEqual(unified_catalog.spend, Decimal("1400.00"))

    def test_monitoring_day_block_uses_overall_totals_and_single_profit_cell(self) -> None:
        report = build_product_report(
            product=self.product,
            stats_date=date(2026, 3, 16),
            stock_date=date(2026, 3, 17),
        )
        block = build_day_block(report, start_row=1, start_col=1)
        self.assertEqual(block[12][6], 10)
        self.assertEqual(block[12][7], "=G13-SUM(B13:F13)")
        self.assertTrue(block[20][1].startswith("=IFERROR(IF("))
        self.assertIn("*25/100", block[20][1])
        self.assertEqual(block[20][2:], ["", "", "", "", "", ""])

    def test_monitoring_day_block_traffic_share_matches_template_logic(self) -> None:
        manual_search_campaign = Campaign.objects.create(
            external_id=28150155,
            name="Руч. Поиск",
            monitoring_group=CampaignMonitoringGroup.MANUAL_SEARCH,
        )
        manual_search_campaign.products.add(self.product)
        manual_shelves_campaign = Campaign.objects.create(
            external_id=28150156,
            name="Руч. Полки",
            monitoring_group=CampaignMonitoringGroup.MANUAL_SHELVES,
        )
        manual_shelves_campaign.products.add(self.product)

        DailyCampaignProductStat.objects.create(
            campaign=self.campaign,
            product=self.product,
            stats_date=date(2026, 3, 16),
            zone=CampaignZone.RECOMMENDATION,
            impressions=500,
            clicks=50,
            spend=Decimal("900.00"),
            add_to_cart_count=10,
            order_count=2,
            order_sum=Decimal("1800.00"),
        )
        DailyCampaignProductStat.objects.create(
            campaign=self.campaign,
            product=self.product,
            stats_date=date(2026, 3, 16),
            zone=CampaignZone.CATALOG,
            impressions=3000,
            clicks=120,
            spend=Decimal("1400.00"),
            add_to_cart_count=8,
            order_count=1,
            order_sum=Decimal("900.00"),
        )
        DailyCampaignProductStat.objects.create(
            campaign=manual_search_campaign,
            product=self.product,
            stats_date=date(2026, 3, 16),
            zone=CampaignZone.SEARCH,
            impressions=200,
            clicks=15,
            spend=Decimal("300.00"),
            add_to_cart_count=3,
            order_count=1,
            order_sum=Decimal("700.00"),
        )
        DailyCampaignProductStat.objects.create(
            campaign=manual_shelves_campaign,
            product=self.product,
            stats_date=date(2026, 3, 16),
            zone=CampaignZone.RECOMMENDATION,
            impressions=180,
            clicks=12,
            spend=Decimal("260.00"),
            add_to_cart_count=2,
            order_count=1,
            order_sum=Decimal("500.00"),
        )

        report = build_product_report(
            product=self.product,
            stats_date=date(2026, 3, 16),
            stock_date=date(2026, 3, 17),
        )
        block = build_day_block(report, start_row=1, start_col=1)
        traffic_row = block[3]

        self.assertAlmostEqual(traffic_row[1], 0.6667, places=4)
        self.assertAlmostEqual(traffic_row[2], 0.3333, places=4)
        self.assertEqual(traffic_row[3], "")
        self.assertEqual(traffic_row[4], "")
        self.assertEqual(traffic_row[5], "")

    def test_exporter_traffic_share_matches_template_logic(self) -> None:
        manual_search_campaign = Campaign.objects.create(
            external_id=28150157,
            name="Руч. Поиск 2",
            monitoring_group=CampaignMonitoringGroup.MANUAL_SEARCH,
        )
        manual_search_campaign.products.add(self.product)
        manual_shelves_campaign = Campaign.objects.create(
            external_id=28150158,
            name="Руч. Полки 2",
            monitoring_group=CampaignMonitoringGroup.MANUAL_SHELVES,
        )
        manual_shelves_campaign.products.add(self.product)

        DailyCampaignProductStat.objects.create(
            campaign=self.campaign,
            product=self.product,
            stats_date=date(2026, 3, 16),
            zone=CampaignZone.RECOMMENDATION,
            impressions=500,
            clicks=50,
            spend=Decimal("900.00"),
            add_to_cart_count=10,
            order_count=2,
            order_sum=Decimal("1800.00"),
        )
        DailyCampaignProductStat.objects.create(
            campaign=self.campaign,
            product=self.product,
            stats_date=date(2026, 3, 16),
            zone=CampaignZone.CATALOG,
            impressions=3000,
            clicks=120,
            spend=Decimal("1400.00"),
            add_to_cart_count=8,
            order_count=1,
            order_sum=Decimal("900.00"),
        )
        DailyCampaignProductStat.objects.create(
            campaign=manual_search_campaign,
            product=self.product,
            stats_date=date(2026, 3, 16),
            zone=CampaignZone.SEARCH,
            impressions=200,
            clicks=15,
            spend=Decimal("300.00"),
            add_to_cart_count=3,
            order_count=1,
            order_sum=Decimal("700.00"),
        )
        DailyCampaignProductStat.objects.create(
            campaign=manual_shelves_campaign,
            product=self.product,
            stats_date=date(2026, 3, 16),
            zone=CampaignZone.RECOMMENDATION,
            impressions=180,
            clicks=12,
            spend=Decimal("260.00"),
            add_to_cart_count=2,
            order_count=1,
            order_sum=Decimal("500.00"),
        )

        report = build_product_report(
            product=self.product,
            stats_date=date(2026, 3, 16),
            stock_date=date(2026, 3, 17),
        )
        rows = exporter_rows(report)
        traffic_row = rows[3]

        self.assertEqual(traffic_row[1], "66,67%")
        self.assertEqual(traffic_row[2], "33,33%")
        self.assertEqual(traffic_row[3], "")
        self.assertEqual(traffic_row[4], "")
        self.assertEqual(traffic_row[5], "")

    def test_monitoring_day_block_offsets_organic_formula_for_later_blocks(self) -> None:
        report = build_product_report(
            product=self.product,
            stats_date=date(2026, 3, 16),
            stock_date=date(2026, 3, 17),
        )
        block = build_day_block(report, start_row=1, start_col=11)
        self.assertEqual(block[9][7], "=Q10-SUM(L10:P10)")
        self.assertEqual(block[12][7], "=Q13-SUM(L13:P13)")

    def test_product_report_ignores_stats_from_unlinked_campaigns(self) -> None:
        other_campaign = Campaign.objects.create(
            external_id=99887755,
            name="Foreign campaign",
            monitoring_group=CampaignMonitoringGroup.UNIFIED,
        )
        other_product = Product.objects.create(nm_id=999000, title="Other")
        other_campaign.products.add(other_product)
        DailyCampaignProductStat.objects.create(
            campaign=other_campaign,
            product=self.product,
            stats_date=date(2026, 3, 16),
            zone=CampaignZone.SEARCH,
            impressions=999,
            clicks=99,
            spend=Decimal("999.00"),
            add_to_cart_count=9,
            order_count=9,
            order_sum=Decimal("9999.00"),
        )

        report = build_product_report(
            product=self.product,
            stats_date=date(2026, 3, 16),
            stock_date=date(2026, 3, 17),
        )

        self.assertEqual(report["total_ad"].clicks, 100)
        self.assertEqual(report["total_ad"].order_sum, Decimal("6000.00"))

    def test_product_report_uses_economics_effective_for_stock_date(self) -> None:
        ProductEconomicsVersion.objects.create(
            product=self.product,
            effective_from=date(2026, 3, 18),
            buyout_percent=Decimal("30.00"),
            unit_cost=Decimal("1800.00"),
            logistics_cost=Decimal("420.00"),
        )
        report_old = build_product_report(
            product=self.product,
            stats_date=date(2026, 3, 16),
            stock_date=date(2026, 3, 17),
        )
        report_new = build_product_report(
            product=self.product,
            stats_date=date(2026, 3, 17),
            stock_date=date(2026, 3, 18),
            create_note=False,
        )
        self.assertEqual(report_old["economics"].unit_cost, Decimal("1500.00"))
        self.assertEqual(report_new["economics"].unit_cost, Decimal("1800.00"))

    def test_product_report_filters_warehouses_per_product(self) -> None:
        preferred = Warehouse.objects.create(office_id=1, name="Коледино")
        hidden = Warehouse.objects.create(office_id=2, name="Рязань")
        ProductVisibleWarehouse.objects.create(product=self.product, warehouse_name="Коледино")
        DailyProductStock.objects.update_or_create(
            product=self.product,
            stats_date=date(2026, 3, 18),
            defaults={"total_stock": 10},
        )
        DailyWarehouseStock.objects.create(
            product=self.product,
            warehouse=preferred,
            stats_date=date(2026, 3, 18),
            stock_count=5,
        )
        DailyWarehouseStock.objects.create(
            product=self.product,
            warehouse=hidden,
            stats_date=date(2026, 3, 18),
            stock_count=4,
        )
        report = build_product_report(
            product=self.product,
            stats_date=date(2026, 3, 17),
            stock_date=date(2026, 3, 18),
            create_note=False,
        )
        self.assertEqual(len(report["warehouse_rows"]), 1)
        self.assertEqual(report["warehouse_rows"][0].warehouse.name, "Коледино")


class SyncTests(TestCase):
    def setUp(self) -> None:
        self.product = Product.objects.create(
            nm_id=987654,
            title="SKU-987654",
            vendor_code="SKU-987654",
            buyout_percent=Decimal("25.00"),
            unit_cost=Decimal("1200.00"),
            logistics_cost=Decimal("300.00"),
        )
        ProductEconomicsVersion.objects.create(
            product=self.product,
            effective_from=date(2026, 3, 1),
            buyout_percent=Decimal("25.00"),
            unit_cost=Decimal("1200.00"),
            logistics_cost=Decimal("300.00"),
        )
        self.campaign = Campaign.objects.create(
            external_id=44556677,
            name="Test campaign",
            monitoring_group=CampaignMonitoringGroup.OTHER,
        )
        self.campaign.products.add(self.product)

    def test_run_sync_rejects_parallel_run_when_running_log_exists(self) -> None:
        SyncLog.objects.create(
            kind=SyncKind.FULL,
            status=SyncStatus.RUNNING,
            target_date=date(2026, 3, 17),
            payload={},
        )

        with self.assertRaisesMessage(SyncServiceError, "Синхронизация уже выполняется"):
            run_sync(reference_date=date(2026, 3, 17), overwrite=True)

    def test_build_product_stock_payload_from_sizes_sums_nested_metrics(self) -> None:
        payload = build_product_stock_payload_from_sizes(
            {
                "data": {
                    "currency": "RUB",
                    "sizes": [
                        {"metrics": {"stockCount": 5, "toClientCount": 2, "fromClientCount": 1}},
                        {"metrics": {"stockCount": 7, "toClientCount": 3, "fromClientCount": 4}},
                    ],
                }
            }
        )
        self.assertEqual(payload["metrics"]["stockCount"], 12)
        self.assertEqual(payload["metrics"]["toClientCount"], 5)
        self.assertEqual(payload["metrics"]["fromClientCount"], 5)
        self.assertEqual(payload["derivedFrom"], "sizes")

    def test_aggregate_offices_from_sizes_merges_same_warehouse_across_sizes(self) -> None:
        offices = aggregate_offices_from_sizes(
            {
                "data": {
                    "sizes": [
                        {
                            "name": "S",
                            "offices": [
                                {
                                    "officeID": 1,
                                    "officeName": "Коледино",
                                    "regionName": "Москва",
                                    "metrics": {"stockCount": 2, "toClientCount": 1, "fromClientCount": 0, "avgOrders": "1.5"},
                                }
                            ],
                        },
                        {
                            "name": "M",
                            "offices": [
                                {
                                    "officeID": 1,
                                    "officeName": "Коледино",
                                    "regionName": "Москва",
                                    "metrics": {"stockCount": 3, "toClientCount": 2, "fromClientCount": 1, "avgOrders": "2.5"},
                                },
                                {
                                    "officeID": 2,
                                    "officeName": "Казань",
                                    "regionName": "Казань",
                                    "metrics": {"stockCount": 4, "toClientCount": 0, "fromClientCount": 1, "avgOrders": "0.5"},
                                },
                            ],
                        },
                    ]
                }
            }
        )
        aggregated = {item["officeID"]: item for item in offices}
        self.assertEqual(aggregated[1]["metrics"]["stockCount"], 5)
        self.assertEqual(aggregated[1]["metrics"]["toClientCount"], 3)
        self.assertEqual(aggregated[1]["metrics"]["fromClientCount"], 1)
        self.assertEqual(aggregated[1]["metrics"]["avgOrders"], "4.0")
        self.assertEqual(sorted(aggregated[1]["sizeNames"]), ["M", "S"])
        self.assertEqual(aggregated[2]["metrics"]["stockCount"], 4)

    def test_resolve_office_id_builds_stable_synthetic_id_when_wb_omits_it(self) -> None:
        office_payload = {"officeID": "0", "officeName": "Коледино", "regionName": "Москва"}
        resolved = resolve_office_id(office_payload)
        self.assertIsNotNone(resolved)
        self.assertEqual(resolved, resolve_office_id(office_payload))
        self.assertGreater(resolved, 0)

    def test_run_sync_falls_back_to_sizes_when_bulk_stock_endpoint_returns_500(self) -> None:
        reference_date = date(2026, 3, 17)
        product = self.product
        campaign = self.campaign
        product.primary_keyword = "брюки мужские черные"
        product.secondary_keyword = "мужские классические брюки"
        product.save(update_fields=["primary_keyword", "secondary_keyword"])

        class FakeAnalyticsClient:
            def get_sales_funnel_history(self, *, nm_ids, start_date, end_date):
                return [
                    {
                        "product": {"nmId": product.nm_id, "name": "Updated title"},
                        "history": [
                            {
                                "date": "2026-03-17",
                                "openCount": 10,
                                "cartCount": 5,
                                "orderCount": 2,
                                "orderSum": "2500",
                                "buyoutCount": 1,
                                "buyoutSum": "1200",
                                "addToWishlistCount": 3,
                            }
                        ],
                        "currency": "RUB",
                    }
                ]

            def get_product_stocks(self, *, nm_ids, snapshot_date):
                raise WBApiError("WB API 500: internal server error")

            def get_product_sizes(self, *, nm_id, snapshot_date):
                return {
                    "data": {
                        "currency": "RUB",
                        "sizes": [
                            {
                                "name": "S",
                                "metrics": {"stockCount": 4, "toClientCount": 1, "fromClientCount": 0},
                                "offices": [
                                    {
                                        "officeID": 10,
                                        "officeName": "Коледино",
                                        "regionName": "Москва",
                                        "metrics": {"stockCount": 3, "toClientCount": 1, "fromClientCount": 0, "avgOrders": "1.2"},
                                    }
                                ],
                            },
                            {
                                "name": "M",
                                "metrics": {"stockCount": 6, "toClientCount": 2, "fromClientCount": 1},
                                "offices": [
                                    {
                                        "officeID": 10,
                                        "officeName": "Коледино",
                                        "regionName": "Москва",
                                        "metrics": {"stockCount": 4, "toClientCount": 1, "fromClientCount": 1, "avgOrders": "1.8"},
                                    },
                                    {
                                        "officeID": 11,
                                        "officeName": "Казань",
                                        "regionName": "Казань",
                                        "metrics": {"stockCount": 2, "toClientCount": 1, "fromClientCount": 0, "avgOrders": "0.7"},
                                    },
                                ],
                            },
                        ],
                    }
                }

            def get_search_orders(self, *, nm_id, start_date, end_date, search_texts):
                return {
                    "data": {
                        "items": [
                            {
                                "text": product.primary_keyword,
                                "frequency": 202,
                                "dateItems": [{"dt": "2026-03-17", "avgPosition": 4, "orders": 0}],
                            },
                            {
                                "text": product.secondary_keyword,
                                "frequency": 72,
                                "dateItems": [{"dt": "2026-03-17", "avgPosition": 3, "orders": 0}],
                            },
                        ]
                    }
                }

        class FakePromotionClient:
            def get_campaigns(self, *, ids=None, statuses=None):
                return {
                    "adverts": [
                        {
                            "id": campaign.external_id,
                            "bid_type": "unified",
                            "status": 9,
                            "settings": {
                                "name": "Campaign refreshed",
                                "payment_type": "cpm",
                                "placements": {"search": True, "recommendations": True},
                            },
                            "nm_settings": [{"nm_id": product.nm_id, "vendorCode": product.vendor_code, "name": product.title}],
                        }
                    ]
                }

            def get_campaign_stats(self, *, ids, start_date, end_date):
                return [
                    {
                        "advertId": campaign.external_id,
                        "days": [
                            {
                                "date": "2026-03-17T00:00:00+00:00",
                                "apps": [
                                    {
                                        "appType": 32,
                                        "nms": [
                                            {
                                                "nmId": product.nm_id,
                                                "views": 100,
                                                "clicks": 10,
                                                "sum": "55",
                                                "atbs": 2,
                                                "orders": 1,
                                                "shks": 1,
                                                "sum_price": "1300",
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                ]

            def get_daily_search_cluster_stats(self, *, items, start_date, end_date):
                return {
                    "items": [
                        {
                            "advertId": campaign.external_id,
                            "nmId": product.nm_id,
                            "dailyStats": [
                                {
                                    "date": "2026-03-17",
                                    "stat": {
                                        "normQuery": product.primary_keyword,
                                        "avgPos": 15.68,
                                        "ctr": 9.21,
                                        "views": 239,
                                        "clicks": 22,
                                    },
                                },
                                {
                                    "date": "2026-03-17",
                                    "stat": {
                                        "normQuery": product.secondary_keyword,
                                        "avgPos": 10.25,
                                        "ctr": 9.02,
                                        "views": 2185,
                                        "clicks": 197,
                                    },
                                },
                            ],
                        }
                    ]
                }

        class FakeStatisticsClient:
            def get_supplier_orders(self, *, date_from, flag=1):
                return [
                    {"nmId": product.nm_id, "spp": 17, "priceWithDisc": 3395, "finishedPrice": 2783},
                    {"nmId": product.nm_id, "spp": 18, "priceWithDisc": 3395, "finishedPrice": 2783},
                ]

        class FakePricesClient:
            def get_goods_prices(self, *, nm_ids):
                return {
                    "data": {
                        "listGoods": [
                            {
                                "nmID": product.nm_id,
                                "sizes": [
                                    {"discountedPrice": 3424.75, "clubDiscountedPrice": 2783},
                                ],
                            }
                        ]
                    }
                }

        class FakeFeedbacksClient:
            def get_feedbacks(self, *, nm_id, is_answered, take=100, skip=0):
                if skip:
                    return {"data": {"feedbacks": []}}
                if is_answered:
                    return {"data": {"feedbacks": []}}
                return {
                    "data": {
                        "feedbacks": [
                            {"id": "f1", "productValuation": 2, "createdDate": "2026-03-17T09:00:00Z"},
                            {"id": "f2", "productValuation": 5, "createdDate": "2026-03-17T12:00:00Z"},
                        ]
                    }
                }

        with patch("monitoring.services.sync.AnalyticsWBClient", return_value=FakeAnalyticsClient()):
            with patch("monitoring.services.sync.PromotionWBClient", return_value=FakePromotionClient()):
                with patch("monitoring.services.sync.StatisticsWBClient", return_value=FakeStatisticsClient()):
                    with patch("monitoring.services.sync.PricesWBClient", return_value=FakePricesClient()):
                        with patch("monitoring.services.sync.FeedbacksWBClient", return_value=FakeFeedbacksClient()):
                            log = run_sync(reference_date=reference_date, overwrite=True)

        self.assertEqual(log.status, "success")
        stock = DailyProductStock.objects.get(product=product, stats_date=reference_date)
        self.assertEqual(stock.total_stock, 10)
        self.assertEqual(stock.in_way_to_client, 3)
        self.assertEqual(stock.in_way_from_client, 1)
        self.assertEqual(stock.avg_orders_per_day, Decimal("3.70"))
        self.assertEqual(stock.days_until_zero, Decimal("2.70"))
        self.assertEqual(DailyWarehouseStock.objects.filter(product=product, stats_date=reference_date).count(), 2)
        kole = DailyWarehouseStock.objects.get(product=product, stats_date=reference_date, warehouse__office_id=10)
        self.assertEqual(kole.stock_count, 7)
        self.assertEqual(kole.in_way_to_client, 2)
        self.assertEqual(kole.in_way_from_client, 1)
        note = DailyProductNote.objects.get(product=product, note_date=reference_date)
        self.assertEqual(note.spp_percent, Decimal("17.50"))
        self.assertEqual(note.seller_price, Decimal("3395.00"))
        self.assertEqual(note.wb_price, Decimal("2783.00"))
        self.assertEqual(note.negative_feedback, "1")
        self.assertFalse(note.unified_enabled)
        campaign_stat = DailyCampaignProductStat.objects.get(
            campaign=campaign,
            product=product,
            stats_date=reference_date,
            zone=CampaignZone.SEARCH,
        )
        self.assertEqual(campaign_stat.impressions, 100)
        self.assertEqual(campaign_stat.clicks, 10)
        self.assertEqual(campaign_stat.spend, Decimal("55.00"))
        self.assertEqual(campaign_stat.add_to_cart_count, 2)
        self.assertEqual(campaign_stat.order_count, 1)
        self.assertEqual(campaign_stat.units_ordered, 1)
        self.assertEqual(campaign_stat.order_sum, Decimal("1300.00"))
        cluster_stat = DailyCampaignSearchClusterStat.objects.get(
            campaign=campaign,
            product=product,
            stats_date=reference_date,
        )
        self.assertEqual(cluster_stat.impressions, 2424)
        self.assertEqual(cluster_stat.clicks, 219)
        self.assertEqual(cluster_stat.spend, Decimal("0.00"))
        self.assertEqual(cluster_stat.add_to_cart_count, 0)
        self.assertEqual(cluster_stat.order_count, 0)
        self.assertEqual(cluster_stat.units_ordered, 0)
        primary_keyword = DailyProductKeywordStat.objects.get(
            product=product,
            stats_date=reference_date,
            query_text=product.primary_keyword,
        )
        self.assertEqual(primary_keyword.frequency, 202)
        self.assertEqual(primary_keyword.organic_position, Decimal("4.00"))
        self.assertEqual(primary_keyword.boosted_position, Decimal("15.68"))
        self.assertEqual(primary_keyword.boosted_ctr, Decimal("9.21"))

    def test_run_sync_range_updates_only_selected_dates(self) -> None:
        range_start = date(2026, 3, 17)
        range_end = date(2026, 3, 18)
        product = self.product
        campaign = self.campaign

        DailyProductMetrics.objects.create(
            product=product,
            stats_date=date(2026, 3, 16),
            open_count=999,
        )

        class FakeAnalyticsClient:
            def get_sales_funnel_history(self, *, nm_ids, start_date, end_date):
                return [
                    {
                        "product": {"nmId": product.nm_id, "name": "Updated title"},
                        "history": [
                            {
                                "date": start_date.isoformat(),
                                "openCount": start_date.day,
                                "cartCount": 1,
                                "orderCount": 1,
                                "orderSum": "100",
                                "buyoutCount": 1,
                                "buyoutSum": "100",
                                "addToWishlistCount": 0,
                            },
                            {
                                "date": "2026-03-01",
                                "openCount": 777,
                                "cartCount": 0,
                                "orderCount": 0,
                                "orderSum": "0",
                                "buyoutCount": 0,
                                "buyoutSum": "0",
                                "addToWishlistCount": 0,
                            },
                        ],
                        "currency": "RUB",
                    }
                ]

            def get_product_stocks(self, *, nm_ids, snapshot_date):
                return {"data": {"items": []}}

            def get_product_sizes(self, *, nm_id, snapshot_date):
                return {"data": {"currency": "RUB", "sizes": []}}

            def get_search_orders(self, *, nm_id, start_date, end_date, search_texts):
                return {"data": {"items": []}}

        class FakePromotionClient:
            def get_campaigns(self, *, ids=None, statuses=None):
                return {
                    "adverts": [
                        {
                            "id": campaign.external_id,
                            "bid_type": "unified",
                            "status": 9,
                            "settings": {
                                "name": "Campaign refreshed",
                                "payment_type": "cpm",
                                "placements": {"search": True, "recommendations": True},
                            },
                            "nm_settings": [{"nm_id": product.nm_id, "vendorCode": product.vendor_code, "name": product.title}],
                        }
                    ]
                }

            def get_campaign_stats(self, *, ids, start_date, end_date):
                return [
                    {
                        "advertId": campaign.external_id,
                        "days": [
                            {
                                "date": f"{start_date.isoformat()}T00:00:00+00:00",
                                "apps": [],
                            }
                        ],
                    }
                ]

            def get_daily_search_cluster_stats(self, *, items, start_date, end_date):
                return {"items": []}

        class FakeStatisticsClient:
            def get_supplier_orders(self, *, date_from, flag=1):
                return [
                    {"nmId": product.nm_id, "date": "2026-03-17T12:00:00Z", "spp": 10, "priceWithDisc": 100, "finishedPrice": 90},
                    {"nmId": product.nm_id, "date": "2026-03-18T12:00:00Z", "spp": 11, "priceWithDisc": 110, "finishedPrice": 99},
                ]

        class FakePricesClient:
            def get_goods_prices(self, *, nm_ids):
                return {"data": {"listGoods": []}}

        class FakeFeedbacksClient:
            def get_feedbacks(self, *, nm_id, is_answered, take=100, skip=0):
                return {"data": {"feedbacks": []}}

        with patch("monitoring.services.sync.AnalyticsWBClient", return_value=FakeAnalyticsClient()):
            with patch("monitoring.services.sync.PromotionWBClient", return_value=FakePromotionClient()):
                with patch("monitoring.services.sync.StatisticsWBClient", return_value=FakeStatisticsClient()):
                    with patch("monitoring.services.sync.PricesWBClient", return_value=FakePricesClient()):
                        with patch("monitoring.services.sync.FeedbacksWBClient", return_value=FakeFeedbacksClient()):
                            log = run_sync(
                                date_from=range_start,
                                date_to=range_end,
                                overwrite=True,
                            )

        self.assertEqual(log.status, SyncStatus.SUCCESS)
        self.assertEqual(log.payload.get("stats_date_from"), "2026-03-17")
        self.assertEqual(log.payload.get("stats_date_to"), "2026-03-18")
        self.assertEqual(log.payload.get("days_count"), 2)

        metric_16 = DailyProductMetrics.objects.get(product=product, stats_date=date(2026, 3, 16))
        self.assertEqual(metric_16.open_count, 999)
        self.assertTrue(DailyProductMetrics.objects.filter(product=product, stats_date=range_start).exists())
        self.assertTrue(DailyProductMetrics.objects.filter(product=product, stats_date=range_end).exists())
        self.assertFalse(DailyProductMetrics.objects.filter(product=product, stats_date=date(2026, 3, 1)).exists())

    def test_run_sync_range_skips_days_with_wb_api_day_limit(self) -> None:
        def fake_single_day(*, reference_date, **kwargs):
            if reference_date == date(2026, 3, 9):
                raise SyncServiceError(
                    "WB API 400: code=400, message={Invalid request body validate: invalid start day: excess limit on days}"
                )
            return SyncLog.objects.create(
                kind=SyncKind.FULL,
                status=SyncStatus.SUCCESS,
                target_date=reference_date,
                finished_at=timezone.now(),
                message="ok",
                payload={
                    "stats_date": reference_date.isoformat(),
                    "stock_date": reference_date.isoformat(),
                    "progress": {"percent": 100, "stage": "Завершено", "detail": "ok"},
                },
            )

        with patch("monitoring.services.sync._run_sync_single_day", side_effect=fake_single_day):
            log = run_sync(
                date_from=date(2026, 3, 9),
                date_to=date(2026, 3, 10),
                overwrite=True,
            )

        self.assertEqual(log.status, SyncStatus.SUCCESS)
        self.assertEqual(log.payload.get("skipped_dates_due_api_limit"), ["2026-03-09"])
        self.assertIn("Пропущено дат из-за ограничения WB Analytics", log.message)

    def test_run_sync_range_raises_if_all_days_outside_wb_api_window(self) -> None:
        with patch(
            "monitoring.services.sync._run_sync_single_day",
            side_effect=SyncServiceError(
                "WB API 400: code=400, message={Invalid request body validate: invalid start day: excess limit on days}"
            ),
        ):
            with self.assertRaisesMessage(SyncServiceError, "целиком вне допустимого окна API"):
                run_sync(
                    date_from=date(2026, 3, 9),
                    date_to=date(2026, 3, 10),
                    overwrite=True,
                )


class WBClientTests(TestCase):
    def test_client_retries_on_429_and_returns_json(self) -> None:
        class FakeResponse:
            def __init__(self, status_code: int, text: str, payload: object, headers: dict[str, str] | None = None) -> None:
                self.status_code = status_code
                self.text = text
                self._payload = payload
                self.headers = headers or {}

            def json(self):
                return self._payload

        responses = [
            FakeResponse(429, '{"detail":"Too Many Requests"}', {}, {"Retry-After": "0.1"}),
            FakeResponse(200, '[{"ok": true}]', [{"ok": True}]),
        ]

        with patch("monitoring.services.wb_client.requests.request", side_effect=responses) as request_mock:
            with patch("monitoring.services.wb_client.time.sleep") as sleep_mock:
                client = AnalyticsWBClient(token="test-token")
                client.min_interval_seconds = 0
                result = client.get_sales_funnel_history(
                    nm_ids=[123],
                    start_date=date(2026, 3, 16),
                    end_date=date(2026, 3, 16),
                )

        self.assertEqual(result, [{"ok": True}])
        self.assertEqual(request_mock.call_count, 2)
        sleep_mock.assert_called()

    def test_client_hides_raw_429_payload_when_limit_does_not_clear(self) -> None:
        class FakeResponse:
            def __init__(self, status_code: int, text: str, payload: object, headers: dict[str, str] | None = None) -> None:
                self.status_code = status_code
                self.text = text
                self._payload = payload
                self.headers = headers or {}

            def json(self):
                return self._payload

        response = FakeResponse(
            429,
            '{"detail":"Limited by global limiter"}',
            {"detail": "Limited by global limiter"},
            {"X-Ratelimit-Retry": "0.1"},
        )

        with patch("monitoring.services.wb_client.requests.request", side_effect=[response, response]):
            with patch("monitoring.services.wb_client.time.sleep"):
                client = AnalyticsWBClient(token="test-token")
                client.min_interval_seconds = 0
                client.max_retries = 2
                with self.assertRaisesMessage(WBApiError, "Wildberries временно ограничил запросы по кабинету"):
                    client.get_sales_funnel_history(
                        nm_ids=[123],
                        start_date=date(2026, 3, 16),
                        end_date=date(2026, 3, 16),
                    )


class SchedulerTests(TestCase):
    def test_next_run_at_moves_to_next_day_for_past_time(self) -> None:
        now = timezone.make_aware(datetime(2026, 3, 17, 12, 0, 0))
        result = next_run_at(now, 9, 15)
        self.assertEqual(result.date(), date(2026, 3, 18))
        self.assertEqual(result.hour, 9)
        self.assertEqual(result.minute, 15)


class PreparationTests(TestCase):
    def test_monitoring_settings_singleton(self) -> None:
        first = MonitoringSettings.get_solo()
        second = MonitoringSettings.get_solo()
        self.assertEqual(first.pk, second.pk)

    def test_seed_demo_data_creates_products(self) -> None:
        seed_demo_dataset()
        self.assertGreaterEqual(Product.objects.count(), 2)

    def test_workspace_settings_page_renders(self) -> None:
        response = self.client.get("/settings/")
        self.assertEqual(response.status_code, 200)

    def test_build_sheet_payloads_contains_dashboard_and_product(self) -> None:
        seed_demo_dataset()
        payloads = build_monitoring_sheet_payloads(reference_date=date.today(), history_days=3)
        self.assertGreaterEqual(len(payloads), 2)
        self.assertEqual(payloads[0].title, "Dashboard")
        self.assertEqual(len(payloads[1].rows[0]), 26)

    def test_workbook_download_returns_xlsx(self) -> None:
        seed_demo_dataset()
        response = self.client.get("/settings/workbook/?reference_date=2026-03-17&history_days=3")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response["Content-Type"],
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    def test_export_monitoring_workbook_returns_bytes(self) -> None:
        seed_demo_dataset()
        payload = export_monitoring_workbook_bytes(reference_date=date(2026, 3, 17), history_days=2)
        self.assertTrue(payload.startswith(b"PK"))

    def test_sheet_payload_dates_match_reference_date_logic(self) -> None:
        seed_demo_dataset()
        payloads = build_monitoring_sheet_payloads(reference_date=date(2026, 3, 18), history_days=3)
        dashboard = payloads[0]
        product_payload = next(payload for payload in payloads if payload.kind == "product")
        self.assertEqual(dashboard.rows[2][1], "2026-03-18")
        self.assertEqual(dashboard.rows[3][1], "2026-03-18")
        self.assertEqual(product_payload.rows[0][1], "16.03.2026")
        self.assertEqual(product_payload.rows[0][10], "17.03.2026")
        self.assertEqual(product_payload.rows[0][19], "18.03.2026")

    def test_workbook_headers_match_generated_sheet_payloads(self) -> None:
        seed_demo_dataset()
        payloads = build_monitoring_sheet_payloads(reference_date=date(2026, 3, 18), history_days=3)
        product_payload = next(payload for payload in payloads if payload.kind == "product")
        workbook = load_workbook(BytesIO(export_monitoring_workbook_bytes(reference_date=date(2026, 3, 18), history_days=3)))
        sheet = workbook[product_payload.title]
        self.assertEqual(sheet["B1"].value, product_payload.rows[0][1])
        self.assertEqual(sheet["K1"].value, product_payload.rows[0][10])
        self.assertEqual(sheet["T1"].value, product_payload.rows[0][19])

    def test_table_view_payloads_render_only_active_sheet_rows(self) -> None:
        seed_demo_dataset()
        payloads = build_table_view_payloads(
            reference_date=date(2026, 3, 18),
            history_days=3,
            active_sheet_key="sheet-1",
        )
        self.assertGreaterEqual(len(payloads), 2)
        self.assertEqual(payloads[0].rows, [])
        self.assertTrue(payloads[1].rows)
        if len(payloads) > 2:
            self.assertEqual(payloads[2].rows, [])


class ProductOperationsTests(TestCase):
    def test_product_settings_form_saves_checkbox_and_manual_warehouses(self) -> None:
        product = Product.objects.create(
            nm_id=7654321,
            title="Warehouse test",
            vendor_code="WH-01",
            buyout_percent=Decimal("25.00"),
            unit_cost=Decimal("1000.00"),
            logistics_cost=Decimal("250.00"),
        )
        warehouse = Warehouse.objects.create(office_id=100, name="Коледино")
        DailyWarehouseStock.objects.create(
            product=product,
            warehouse=warehouse,
            stats_date=date(2026, 3, 18),
            stock_count=5,
        )
        form = ProductSettingsForm(
            data={
                "title": "Warehouse test",
                "vendor_code": "WH-01",
                "brand_name": "",
                "subject_name": "",
                "buyout_percent": "25.00",
                "unit_cost": "1000.00",
                "logistics_cost": "250.00",
                "economics_effective_from": "2026-03-18",
                "primary_keyword": "",
                "secondary_keyword": "",
                "visible_warehouses": ["Коледино"],
                "visible_warehouse_names_extra": "Казань",
                "is_active": "on",
            },
            instance=product,
        )
        self.assertTrue(form.is_valid(), form.errors)
        saved = form.save()
        self.assertEqual(saved.visible_warehouse_names(), ["Казань", "Коледино"])

    def test_product_report_surfaces_manual_catalog_block(self) -> None:
        product = Product.objects.create(
            nm_id=456789,
            title="Manual catalog product",
            vendor_code="MAN-CAT",
            buyout_percent=Decimal("24.00"),
            unit_cost=Decimal("1500.00"),
            logistics_cost=Decimal("336.00"),
        )
        ProductEconomicsVersion.objects.create(
            product=product,
            effective_from=date(2026, 3, 1),
            buyout_percent=Decimal("24.00"),
            unit_cost=Decimal("1500.00"),
            logistics_cost=Decimal("336.00"),
        )
        DailyProductMetrics.objects.create(
            product=product,
            stats_date=date(2026, 3, 16),
            open_count=40,
            add_to_cart_count=10,
            order_count=4,
            order_sum=Decimal("8000.00"),
        )
        DailyProductStock.objects.create(
            product=product,
            stats_date=date(2026, 3, 17),
            total_stock=50,
        )
        campaign = Campaign.objects.create(
            external_id=99887766,
            name="Manual catalog",
            monitoring_group=CampaignMonitoringGroup.MANUAL_CATALOG,
        )
        campaign.products.add(product)
        DailyCampaignProductStat.objects.create(
            campaign=campaign,
            product=product,
            stats_date=date(2026, 3, 16),
            zone=CampaignZone.CATALOG,
            impressions=350,
            clicks=30,
            spend=Decimal("450.00"),
            add_to_cart_count=6,
            order_count=2,
            order_sum=Decimal("2100.00"),
        )

        report = build_product_report(
            product=product,
            stats_date=date(2026, 3, 16),
            stock_date=date(2026, 3, 17),
            create_note=False,
        )

        self.assertEqual(report["blocks"]["manual_catalog"].impressions, 350)
        self.assertTrue(any(card["label"] == "Руч. каталог" for card in report["traffic_cards"]))
        self.assertTrue(any(alert["tone"] == "info" for alert in report["alerts"]))

    def test_add_product_redirects_to_next_when_provided(self) -> None:
        with patch("monitoring.views.refresh_product_metadata"):
            response = self.client.post(
                "/products/add/",
                {
                    "nm_id": "22334455",
                    "buyout_percent": "24.00",
                    "unit_cost": "1000.00",
                    "logistics_cost": "250.00",
                    "primary_keyword": "",
                    "secondary_keyword": "",
                    "next": "/products/",
                },
            )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/products/")
        self.assertTrue(Product.objects.filter(nm_id=22334455).exists())

    def test_update_product_settings_redirects_to_next_when_provided(self) -> None:
        product = Product.objects.create(
            nm_id=91919191,
            title="Redirect target",
            vendor_code="RT-1",
            buyout_percent=Decimal("24.00"),
            unit_cost=Decimal("1200.00"),
            logistics_cost=Decimal("300.00"),
        )
        response = self.client.post(
            f"/products/{product.pk}/settings/",
            {
                "title": "Redirect target updated",
                "vendor_code": "RT-2",
                "brand_name": "",
                "subject_name": "",
                "buyout_percent": "25.00",
                "unit_cost": "1300.00",
                "logistics_cost": "320.00",
                "economics_effective_from": "2026-03-18",
                "primary_keyword": "",
                "secondary_keyword": "",
                "visible_warehouses": [],
                "visible_warehouse_names_extra": "",
                "is_active": "on",
                "next": f"/products/?edit={product.pk}",
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], f"/products/?edit={product.pk}")


class WorkspaceOverviewTests(TestCase):
    def test_workspace_overview_contains_schedule_and_sync_state(self) -> None:
        settings_obj = MonitoringSettings.get_solo()
        settings_obj.report_timezone = "Asia/Bishkek"
        settings_obj.sync_hour = 10
        settings_obj.sync_minute = 0
        settings_obj.save()
        product = Product.objects.create(nm_id=444555, title="Overview product")
        DailyProductMetrics.objects.create(
            product=product,
            stats_date=date(2026, 3, 18),
            order_count=1,
            order_sum=Decimal("1000.00"),
        )
        DailyProductStock.objects.create(
            product=product,
            stats_date=date(2026, 3, 19),
            total_stock=10,
        )
        SyncLog.objects.create(
            kind=SyncKind.FULL,
            status=SyncStatus.SUCCESS,
            target_date=date(2026, 3, 18),
            finished_at=timezone.now(),
            message="Sync OK",
        )

        overview = build_workspace_overview()

        self.assertIn("signals", overview)
        self.assertEqual(len(overview["signals"]), 3)
        self.assertIsNotNone(overview["next_run"])


class ReportingHubTests(TestCase):
    def test_build_reports_context_contains_chart_payloads(self) -> None:
        seed_demo_dataset()
        context = build_reports_context(reference_date=date(2026, 3, 18), range_days=14)

        self.assertEqual(context["range_days"], 14)
        self.assertIn("timeline_chart", context)
        self.assertIn("product_chart", context)
        self.assertEqual(context["timeline_chart"]["defaultMetric"], "revenue")
        self.assertEqual(context["timeline_chart"]["defaultType"], "line")
        self.assertTrue(context["summary_cards"])
        self.assertTrue(context["kpi_cards"])
        self.assertIn("trend_rows", context)
        self.assertIn("diagnostic_rows", context)
        self.assertIn("cpo", context["timeline_chart"]["series"])
        self.assertIn("drr", context["timeline_chart"]["series"])
        self.assertIn("cpo", context["product_chart"]["series"])
        self.assertIn("drr", context["product_chart"]["series"])


class StockPopupPayloadTests(TestCase):
    def test_stock_popup_builds_size_matrix_when_raw_sizes_exist(self) -> None:
        product = Product.objects.create(
            nm_id=998001,
            title="Stock matrix product",
            vendor_code="W-RSS01-GRY",
        )
        ProductVisibleWarehouse.objects.bulk_create(
            [
                ProductVisibleWarehouse(product=product, warehouse_name="Коледино"),
                ProductVisibleWarehouse(product=product, warehouse_name="Казань"),
                ProductVisibleWarehouse(product=product, warehouse_name="Электросталь"),
            ]
        )
        stock_row = DailyProductStock.objects.create(
            product=product,
            stats_date=date(2026, 4, 5),
            total_stock=29,
            raw_payload={
                "raw_payload": {
                    "data": {
                        "sizes": [
                            {
                                "name": "42",
                                "offices": [
                                    {"officeName": "Коледино", "metrics": {"stockCount": 1}},
                                    {"officeName": "Казань", "metrics": {"stockCount": 12}},
                                    {"officeName": "Электросталь", "metrics": {"stockCount": 1}},
                                ],
                            },
                            {
                                "name": "44",
                                "offices": [
                                    {"officeName": "Коледино", "metrics": {"stockCount": 1}},
                                    {"officeName": "Казань", "metrics": {"stockCount": 5}},
                                ],
                            },
                        ]
                    }
                }
            },
        )

        payload = _build_stock_popup_payload(
            product=product,
            stock_row=stock_row,
            warehouse_rows=[
                {"warehouse": "Коледино", "stock": 2, "to_client": 0, "from_client": 0, "size_names": ["42", "44"]},
                {"warehouse": "Казань", "stock": 17, "to_client": 0, "from_client": 0, "size_names": ["42", "44"]},
                {"warehouse": "Электросталь", "stock": 1, "to_client": 0, "from_client": 0, "size_names": ["42"]},
            ],
            visible_warehouse_names=["Коледино", "Казань", "Электросталь"],
            preferred_warehouse_names={"коледино", "казань", "электросталь"},
        )

        self.assertEqual(payload["mode"], "matrix")
        self.assertEqual(payload["title"], "Остатки")
        parsed = json.loads(payload["payload_json"])
        self.assertEqual(parsed["columns"][0]["label"], "Артикул продавца")
        self.assertEqual(parsed["columns"][1]["label"], "Размер вещи")
        self.assertEqual([column["label"] for column in parsed["columns"][2:]], ["Коледино", "Казань", "Электросталь"])
        self.assertEqual(parsed["rows"][0]["vendor_code"], "W-RSS01-GRY")
        self.assertEqual(parsed["rows"][0]["size"], "42")
        self.assertEqual(parsed["rows"][0]["коледино"], 1)
        self.assertEqual(parsed["rows"][0]["казань"], 12)
        self.assertEqual(parsed["rows"][0]["электросталь"], 1)
        self.assertEqual(parsed["rows"][1]["size"], "44")
        self.assertEqual(parsed["rows"][1]["коледино"], 1)
        self.assertEqual(parsed["rows"][1]["казань"], 5)
        self.assertNotIn("электросталь", parsed["rows"][1])


class PageRenderTests(TestCase):
    def test_dashboard_and_product_detail_pages_render(self) -> None:
        seed_demo_dataset()
        product = Product.objects.filter(is_active=True).first()
        dashboard = self.client.get("/")
        products_page = self.client.get("/products/")
        detail = self.client.get(f"/products/{product.pk}/")
        reports = self.client.get("/reports/")
        self.assertEqual(dashboard.status_code, 200)
        self.assertEqual(products_page.status_code, 200)
        self.assertEqual(detail.status_code, 200)
        self.assertEqual(reports.status_code, 200)
        self.assertContains(products_page, "Список товаров")
        self.assertContains(detail, 'data-detail-tab-trigger="overview"')
        self.assertContains(reports, 'data-reports-tab-trigger="overview"')
        self.assertNotContains(reports, 'data-reports-tab-trigger="analytics"')
        self.assertNotContains(reports, 'data-reports-panel="analytics"')

    def test_table_page_renders_monitoring_grid(self) -> None:
        seed_demo_dataset()
        response = self.client.get("/table/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "monitoring-grid-table")
        self.assertContains(response, "data-sync-indicator")

    def test_campaigns_page_renders_management_workspace(self) -> None:
        seed_demo_dataset()
        response = self.client.get("/campaigns/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "campaigns-modal-add")
        self.assertContains(response, "campaigns-modal-edit")
        self.assertContains(response, 'href="/campaigns/"')

    def test_campaign_detail_page_renders_stats_workspace(self) -> None:
        seed_demo_dataset()
        campaign = Campaign.objects.order_by("id").first()
        response = self.client.get(f"/campaigns/{campaign.pk}/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "campaign-timeline-chart")
        self.assertContains(response, 'data-detail-tab-trigger="overview"')
        self.assertContains(response, "Товары внутри кампании")

    def test_table_page_renders_inline_note_controls(self) -> None:
        seed_demo_dataset()
        response = self.client.get("/table/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'data-note-control="bool"')
        self.assertContains(response, 'data-note-control="select"')
        self.assertContains(response, 'data-note-control="input"')
        self.assertContains(response, 'data-note-control="text"')
        self.assertContains(response, "is-comment-span")
        self.assertContains(response, 'colspan="7"')
        self.assertContains(response, "is-stock-span")
        self.assertContains(response, "data-stock-popup-button")
        self.assertContains(response, "data-stock-payload")
        self.assertNotContains(response, "\\u0022mode\\u0022")
        self.assertContains(response, 'id="table-modal-stocks"')
        self.assertContains(response, 'data-table-action="fullscreen"')
        self.assertContains(response, 'data-inline-status')
        self.assertNotContains(response, "data-table-row-search")
        self.assertContains(response, 'data-day-step="-1"')
        self.assertContains(response, 'data-day-step="1"')
        self.assertContains(response, 'data-day-meta')
        self.assertContains(response, "Компактный режим")
        self.assertNotContains(response, "Плотный режим")

    def test_core_pages_render_utf8_without_mojibake(self) -> None:
        seed_demo_dataset()
        campaign = Campaign.objects.order_by("id").first()
        for url in ("/table/", "/products/", "/campaigns/", f"/campaigns/{campaign.pk}/", "/reports/", "/settings/"):
            response = self.client.get(url)
            self.assertEqual(response.status_code, 200)
            self.assertIn("charset=utf-8", response["Content-Type"].lower())
            html = response.content.decode("utf-8")
            self.assertNotIn("РќР", html)

    def test_reports_bar_width_styles_use_dot_separator(self) -> None:
        seed_demo_dataset()
        response = self.client.get("/reports/")
        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIsNone(re.search(r'bar-fill (?:accent|warm)" style="width:\\s*\\d+,\\d+%;"', html))

    def test_reports_page_accepts_explicit_date_range(self) -> None:
        seed_demo_dataset()
        response = self.client.get("/reports/?date_from=2026-03-12&date_to=2026-03-18")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["date_from"], date(2026, 3, 12))
        self.assertEqual(response.context["date_to"], date(2026, 3, 18))
        self.assertEqual(response.context["range_days"], 7)
        self.assertContains(response, "12.03.2026 — 18.03.2026")

    def test_reports_page_prefills_range_fields_from_reference_and_window_query(self) -> None:
        seed_demo_dataset()
        response = self.client.get("/reports/?reference_date=2026-03-18&range_days=14")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["date_from"], date(2026, 3, 5))
        self.assertEqual(response.context["date_to"], date(2026, 3, 18))
        self.assertContains(response, 'name="date_from"')
        self.assertContains(response, 'value="2026-03-05"')
        self.assertContains(response, 'name="date_to"')
        self.assertContains(response, 'value="2026-03-18"')

    def test_products_page_uses_modals_for_add_and_edit(self) -> None:
        seed_demo_dataset()
        product = Product.objects.filter(is_active=True).first()
        response = self.client.get(f"/products/?edit={product.pk}&modal=edit")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'id="products-modal-add"')
        self.assertContains(response, 'id="products-modal-edit"')
        self.assertContains(response, 'data-modal-open="products-modal-add"')
        self.assertContains(response, f'href="/products/?edit={product.pk}&modal=edit"')
        self.assertContains(response, "table_grid_controls.js")
        self.assertNotContains(response, 'id="product-edit"')

    def test_table_page_preserves_current_url_in_add_campaign_form(self) -> None:
        seed_demo_dataset()
        response = self.client.get("/table/?sheet=sheet-2")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'name="next" value="/table/?sheet=sheet-2"')


class TemplateFilterTests(TestCase):
    def test_css_percent_formats_for_inline_styles(self) -> None:
        self.assertEqual(css_percent(Decimal("31.93")), "31.93")
        self.assertEqual(css_percent("28,13"), "28.13")
        self.assertEqual(css_percent(Decimal("-3.5")), "0.00")
        self.assertEqual(css_percent(Decimal("130.777")), "100.00")


class SyncFormTests(TestCase):
    def test_sync_form_normalizes_single_side_range(self) -> None:
        form = SyncForm(
            data={
                "date_from": "2026-03-18",
            }
        )
        self.assertTrue(form.is_valid(), form.errors)
        self.assertEqual(form.cleaned_data["date_from"], date(2026, 3, 18))
        self.assertEqual(form.cleaned_data["date_to"], date(2026, 3, 18))

    def test_sync_form_rejects_invalid_range(self) -> None:
        form = SyncForm(
            data={
                "date_from": "2026-03-19",
                "date_to": "2026-03-18",
            }
        )
        self.assertFalse(form.is_valid())
        self.assertIn("date_to", form.errors)


class CampaignViewTests(TestCase):
    @patch("monitoring.views.refresh_campaign_metadata")
    def test_add_campaign_redirects_back_to_next_url(self, refresh_mock) -> None:
        seed_demo_dataset()
        product = Product.objects.filter(is_active=True).first()

        response = self.client.post(
            "/campaigns/add/",
            {
                "external_id": 99123456,
                "monitoring_group": CampaignMonitoringGroup.UNIFIED,
                "products": [product.pk],
                "next": "/table/?sheet=sheet-2",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/table/?sheet=sheet-2")
        campaign = Campaign.objects.get(external_id=99123456)
        self.assertTrue(campaign.products.filter(pk=product.pk).exists())
        refresh_mock.assert_called_once_with(campaign)

    def test_update_campaign_saves_group_and_active_flag(self) -> None:
        seed_demo_dataset()
        campaign = Campaign.objects.first()
        product = Product.objects.filter(is_active=True).order_by("id").last()

        response = self.client.post(
            f"/campaigns/{campaign.pk}/settings/",
            {
                "external_id": campaign.external_id,
                "name": "Manual name",
                "monitoring_group": CampaignMonitoringGroup.MANUAL_SEARCH,
                "products": [product.pk],
                "next": f"/campaigns/?edit={campaign.pk}",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], f"/campaigns/?edit={campaign.pk}")
        campaign.refresh_from_db()
        self.assertEqual(campaign.name, "Manual name")
        self.assertEqual(campaign.monitoring_group, CampaignMonitoringGroup.MANUAL_SEARCH)
        self.assertFalse(campaign.is_active)
        self.assertEqual(list(campaign.products.values_list("pk", flat=True)), [product.pk])

    def test_toggle_campaign_active_disables_campaign_in_monitoring(self) -> None:
        seed_demo_dataset()
        campaign = Campaign.objects.filter(is_active=True).first()

        response = self.client.post(
            f"/campaigns/{campaign.pk}/toggle/",
            {"next": "/campaigns/"},
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/campaigns/")
        campaign.refresh_from_db()
        self.assertFalse(campaign.is_active)


class ReportsFilterFormTests(TestCase):
    def test_reports_filter_form_accepts_explicit_date_range(self) -> None:
        form = ReportsFilterForm(
            data={
                "date_from": "2026-03-11",
                "date_to": "2026-03-18",
                "reference_date": "2026-03-18",
                "range_days": "14",
            }
        )
        self.assertTrue(form.is_valid(), form.errors)
        self.assertEqual(form.cleaned_data["date_from"], date(2026, 3, 11))
        self.assertEqual(form.cleaned_data["date_to"], date(2026, 3, 18))
        self.assertEqual(form.cleaned_data["reference_date"], date(2026, 3, 18))
        self.assertEqual(form.cleaned_data["range_days"], 8)

    def test_reports_filter_form_builds_period_from_date_to_and_window(self) -> None:
        form = ReportsFilterForm(
            data={
                "date_to": "2026-03-18",
                "range_days": "14",
            }
        )
        self.assertTrue(form.is_valid(), form.errors)
        self.assertEqual(form.cleaned_data["date_to"], date(2026, 3, 18))
        self.assertEqual(form.cleaned_data["date_from"], date(2026, 3, 5))


class SyncViewsTests(TestCase):
    def test_sync_status_returns_idle_payload_when_no_logs(self) -> None:
        response = self.client.get("/sync/status/")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertFalse(payload["has_sync"])
        self.assertFalse(payload["is_running"])
        self.assertEqual(payload["progress"]["percent"], 0)

    def test_sync_status_returns_running_log_progress(self) -> None:
        log = SyncLog.objects.create(
            kind=SyncKind.FULL,
            status=SyncStatus.RUNNING,
            target_date=date(2026, 3, 18),
            payload={
                "progress": {
                    "percent": 37,
                    "stage": "Остатки",
                    "detail": "Сбор остатков",
                }
            },
        )
        response = self.client.get("/sync/status/")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["has_sync"])
        self.assertTrue(payload["is_running"])
        self.assertEqual(payload["id"], log.id)
        self.assertEqual(payload["progress"]["percent"], 37)
        self.assertEqual(payload["progress"]["stage"], "Остатки")
        self.assertTrue(payload["can_cancel"])
        self.assertFalse(payload["cancel_requested"])

    def test_sync_cancel_requests_running_sync_stop(self) -> None:
        log = SyncLog.objects.create(
            kind=SyncKind.FULL,
            status=SyncStatus.RUNNING,
            target_date=date(2026, 3, 18),
            payload={
                "progress": {
                    "percent": 96,
                    "stage": "Финализация",
                    "detail": "Формирование итогов.",
                }
            },
        )
        response = self.client.post(
            "/sync/cancel/",
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
            HTTP_ACCEPT="application/json",
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["sync_id"], log.id)

        log.refresh_from_db()
        self.assertEqual(log.status, SyncStatus.CANCELED)
        self.assertIsNotNone(log.finished_at)
        self.assertTrue(log.payload.get("cancel_requested"))
        self.assertEqual((log.payload.get("progress") or {}).get("stage"), "Отменено")

    def test_sync_cancel_returns_info_when_no_running_sync(self) -> None:
        response = self.client.post(
            "/sync/cancel/",
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
            HTTP_ACCEPT="application/json",
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertFalse(payload["ok"])
        self.assertFalse(payload["has_running_sync"])

    def test_sync_all_starts_background_worker(self) -> None:
        with patch("monitoring.views.run_sync_in_background") as start_mock:
            response = self.client.post(
                "/sync/",
                {
                    "reference_date": "2026-03-18",
                },
            )
        self.assertEqual(response.status_code, 302)
        start_mock.assert_called_once()
        self.assertEqual(start_mock.call_args.kwargs["date_from"], date(2026, 3, 18))
        self.assertEqual(start_mock.call_args.kwargs["date_to"], date(2026, 3, 18))
        self.assertTrue(start_mock.call_args.kwargs["overwrite"])

    def test_sync_all_accepts_date_range(self) -> None:
        with patch("monitoring.views.run_sync_in_background") as start_mock:
            response = self.client.post(
                "/sync/",
                {
                    "date_from": "2026-03-17",
                    "date_to": "2026-03-18",
                },
            )
        self.assertEqual(response.status_code, 302)
        start_mock.assert_called_once()
        self.assertEqual(start_mock.call_args.kwargs["date_from"], date(2026, 3, 17))
        self.assertEqual(start_mock.call_args.kwargs["date_to"], date(2026, 3, 18))

    def test_sync_product_starts_background_worker(self) -> None:
        product = Product.objects.create(
            nm_id=20260322,
            title="Async sync product",
            vendor_code="ASYNC-1",
            buyout_percent=Decimal("24.00"),
            unit_cost=Decimal("1200.00"),
            logistics_cost=Decimal("300.00"),
        )
        with patch("monitoring.views.run_sync_in_background") as start_mock:
            response = self.client.post(
                f"/products/{product.pk}/sync/",
                {
                    "reference_date": "2026-03-18",
                },
            )
        self.assertEqual(response.status_code, 302)
        start_mock.assert_called_once()
        self.assertEqual(start_mock.call_args.kwargs["date_from"], date(2026, 3, 18))
        self.assertEqual(start_mock.call_args.kwargs["date_to"], date(2026, 3, 18))


class TableInlineNoteUpdateTests(TestCase):
    def setUp(self) -> None:
        self.product = Product.objects.create(
            nm_id=99887766,
            title="Inline note product",
            vendor_code="INLINE-1",
            buyout_percent=Decimal("24.00"),
            unit_cost=Decimal("500.00"),
            logistics_cost=Decimal("100.00"),
        )
        self.note = DailyProductNote.objects.create(
            product=self.product,
            note_date=date(2026, 3, 18),
            promo_status="Не участвуем",
            negative_feedback="Без изменений",
            unified_enabled=False,
        )

    def test_table_note_cell_updates_boolean_field(self) -> None:
        response = self.client.post(
            "/table/note-cell/",
            data=json.dumps(
                {
                    "product_id": self.product.id,
                    "note_date": "2026-03-18",
                    "field": "unified_enabled",
                    "value": True,
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["value"], "Да")

        self.note.refresh_from_db()
        self.assertTrue(self.note.unified_enabled)

    def test_table_note_cell_updates_select_field(self) -> None:
        response = self.client.post(
            "/table/note-cell/",
            data=json.dumps(
                {
                    "product_id": self.product.id,
                    "note_date": "2026-03-18",
                    "field": "promo_status",
                    "value": "Тест",
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["value"], "Тест")

        self.note.refresh_from_db()
        self.assertEqual(self.note.promo_status, "Тест")
    def test_table_note_cell_updates_decimal_note_field(self) -> None:
        response = self.client.post(
            "/table/note-cell/",
            data=json.dumps(
                {
                    "product_id": self.product.id,
                    "note_date": "2026-03-18",
                    "field": "spp_percent",
                    "value": "29,00",
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["value"], "29")

        self.note.refresh_from_db()
        self.assertEqual(self.note.spp_percent, Decimal("29.00"))

    def test_table_note_cell_updates_economics_for_selected_date(self) -> None:
        response = self.client.post(
            "/table/note-cell/",
            data=json.dumps(
                {
                    "product_id": self.product.id,
                    "note_date": "2026-03-18",
                    "field": "unit_cost",
                    "value": "700,00",
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["value"], "700")

        version = ProductEconomicsVersion.objects.get(product=self.product, effective_from=date(2026, 3, 18))
        self.assertEqual(version.unit_cost, Decimal("700.00"))

    def test_table_note_cell_updates_product_keywords(self) -> None:
        response = self.client.post(
            "/table/note-cell/",
            data=json.dumps(
                {
                    "product_id": self.product.id,
                    "note_date": "2026-03-18",
                    "field": "primary_keyword",
                    "value": "брюки мужские",
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["value"], "брюки мужские")

        self.product.refresh_from_db()
        self.assertEqual(self.product.primary_keyword, "брюки мужские")

    def test_table_note_cell_updates_daily_keyword_query(self) -> None:
        response = self.client.post(
            "/table/note-cell/",
            data=json.dumps(
                {
                    "product_id": self.product.id,
                    "note_date": "2026-03-18",
                    "field": "keyword_query",
                    "value": "брюки мужские черные",
                    "keyword_prev": "",
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["value"], "брюки мужские черные")

        self.note.refresh_from_db()
        self.assertEqual(self.note.keywords, ["брюки мужские черные"])

    def test_table_note_cell_updates_daily_keyword_metrics(self) -> None:
        self.note.keywords = ["брюки мужские черные"]
        self.note.save(update_fields=["keywords", "updated_at"])

        response = self.client.post(
            "/table/note-cell/",
            data=json.dumps(
                {
                    "product_id": self.product.id,
                    "note_date": "2026-03-18",
                    "field": "keyword_frequency",
                    "value": "120",
                    "keyword_prev": "брюки мужские черные",
                    "keyword_query": "брюки мужские черные",
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["value"], "120")

        response = self.client.post(
            "/table/note-cell/",
            data=json.dumps(
                {
                    "product_id": self.product.id,
                    "note_date": "2026-03-18",
                    "field": "keyword_boosted_ctr",
                    "value": "8,40",
                    "keyword_prev": "брюки мужские черные",
                    "keyword_query": "брюки мужские черные",
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["value"], "8,4")

        stat = DailyProductKeywordStat.objects.get(
            product=self.product,
            stats_date=date(2026, 3, 18),
            query_text="брюки мужские черные",
        )
        self.assertEqual(stat.frequency, 120)
        self.assertEqual(stat.boosted_ctr, Decimal("8.40"))

    def test_table_note_cell_changes_keyword_rows_count(self) -> None:
        self.note.keywords = ["ключ 1", "ключ 2"]
        self.note.keyword_rows_count = 3
        self.note.save(update_fields=["keywords", "keyword_rows_count", "updated_at"])

        response = self.client.post(
            "/table/note-cell/",
            data=json.dumps(
                {
                    "product_id": self.product.id,
                    "note_date": "2026-03-18",
                    "field": "keyword_rows_count_delta",
                    "value": "1",
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        self.note.refresh_from_db()
        self.assertEqual(self.note.keyword_rows_count, 4)

        response = self.client.post(
            "/table/note-cell/",
            data=json.dumps(
                {
                    "product_id": self.product.id,
                    "note_date": "2026-03-18",
                    "field": "keyword_rows_count_delta",
                    "value": "-1",
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        self.note.refresh_from_db()
        self.assertEqual(self.note.keyword_rows_count, 3)

    def test_table_note_cell_trims_keywords_when_rows_are_removed(self) -> None:
        self.note.keywords = ["ключ 1", "ключ 2", "ключ 3"]
        self.note.keyword_rows_count = 3
        self.note.save(update_fields=["keywords", "keyword_rows_count", "updated_at"])
        DailyProductKeywordStat.objects.create(
            product=self.product,
            stats_date=date(2026, 3, 18),
            query_text="ключ 3",
            frequency=50,
        )

        response = self.client.post(
            "/table/note-cell/",
            data=json.dumps(
                {
                    "product_id": self.product.id,
                    "note_date": "2026-03-18",
                    "field": "keyword_rows_count_delta",
                    "value": "-1",
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)

        self.note.refresh_from_db()
        self.assertEqual(self.note.keyword_rows_count, 2)
        self.assertEqual(self.note.keywords, ["ключ 1", "ключ 2"])
        self.assertFalse(
            DailyProductKeywordStat.objects.filter(
                product=self.product,
                stats_date=date(2026, 3, 18),
                query_text="ключ 3",
            ).exists()
        )
