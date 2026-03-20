from io import BytesIO
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
from monitoring.forms import ProductSettingsForm
from monitoring.services.config import build_workspace_overview
from monitoring.services.demo import seed_demo_dataset
from monitoring.services.exporters import exporter_rows
from monitoring.services.monitoring_table import export_monitoring_workbook_bytes
from monitoring.services.monitoring_table import build_day_block
from monitoring.services.reporting_hub import build_reports_context
from monitoring.services.google_sheets import (
    GoogleSheetsSyncError,
    _ensure_google_dependencies,
    _sheet_values_for_locale,
    _sparse_value_updates,
    build_sheet_payloads,
)
from monitoring.services.reports import build_dashboard_context, build_product_report
from monitoring.services.sync import (
    aggregate_offices_from_sizes,
    build_product_stock_payload_from_sizes,
    next_run_at,
    resolve_office_id,
    run_sync,
)
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

    def test_export_contains_sample_header(self) -> None:
        report = build_product_report(
            product=self.product,
            stats_date=date(2026, 3, 16),
            stock_date=date(2026, 3, 17),
        )
        rows = exporter_rows(report)
        self.assertEqual(rows[0][2], "ОБРАЗЕЦ (17.03.2026)")
        self.assertEqual(rows[4][0], "Затраты (руб)")

    def test_exporter_uses_overall_funnel_in_summary_column(self) -> None:
        report = build_product_report(
            product=self.product,
            stats_date=date(2026, 3, 16),
            stock_date=date(2026, 3, 17),
        )
        rows = exporter_rows(report)
        self.assertEqual(rows[10][7], "50")
        self.assertEqual(rows[11][7], "10")
        self.assertEqual(rows[12][7], "10000")

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

    def test_monitoring_day_block_uses_overall_totals_and_single_profit_cell(self) -> None:
        report = build_product_report(
            product=self.product,
            stats_date=date(2026, 3, 16),
            stock_date=date(2026, 3, 17),
        )
        block = build_day_block(report, start_row=1, start_col=1)
        self.assertEqual(block[11][7], 10)
        self.assertEqual(block[11][8], "=H12-SUM(C12:G12)")
        self.assertTrue(block[18][2].startswith("=(("))
        self.assertEqual(block[18][3:], ["", "", "", "", "", ""])

    def test_monitoring_day_block_offsets_organic_formula_for_later_blocks(self) -> None:
        report = build_product_report(
            product=self.product,
            stats_date=date(2026, 3, 16),
            stock_date=date(2026, 3, 17),
        )
        block = build_day_block(report, start_row=1, start_col=11)
        self.assertEqual(block[9][8], "=R10-SUM(M10:Q10)")
        self.assertEqual(block[12][8], "=R13-SUM(M13:Q13)")

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
        self.assertTrue(note.unified_enabled)
        primary_keyword = DailyProductKeywordStat.objects.get(
            product=product,
            stats_date=reference_date,
            query_text=product.primary_keyword,
        )
        self.assertEqual(primary_keyword.frequency, 202)
        self.assertEqual(primary_keyword.organic_position, Decimal("4.00"))
        self.assertEqual(primary_keyword.boosted_position, Decimal("15.68"))
        self.assertEqual(primary_keyword.boosted_ctr, Decimal("9.21"))


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
        payloads = build_sheet_payloads(reference_date=date.today(), history_days=3)
        self.assertGreaterEqual(len(payloads), 2)
        self.assertEqual(payloads[0].title, "Dashboard")
        self.assertEqual(len(payloads[1].rows[0]), 29)

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
        payloads = build_sheet_payloads(reference_date=date(2026, 3, 18), history_days=3)
        dashboard = payloads[0]
        product_payload = next(payload for payload in payloads if payload.kind == "product")
        self.assertEqual(dashboard.rows[2][1], "2026-03-18")
        self.assertEqual(dashboard.rows[3][1], "2026-03-18")
        self.assertEqual(product_payload.rows[0][2], "ОБРАЗЕЦ (16.03.2026)")
        self.assertEqual(product_payload.rows[0][12], "ОБРАЗЕЦ (17.03.2026)")
        self.assertEqual(product_payload.rows[0][22], "ОБРАЗЕЦ (18.03.2026)")

    def test_workbook_headers_match_generated_sheet_payloads(self) -> None:
        seed_demo_dataset()
        payloads = build_sheet_payloads(reference_date=date(2026, 3, 18), history_days=3)
        product_payload = next(payload for payload in payloads if payload.kind == "product")
        workbook = load_workbook(BytesIO(export_monitoring_workbook_bytes(reference_date=date(2026, 3, 18), history_days=3)))
        sheet = workbook[product_payload.title]
        self.assertEqual(sheet["C1"].value, product_payload.rows[0][2])
        self.assertEqual(sheet["M1"].value, product_payload.rows[0][12])
        self.assertEqual(sheet["W1"].value, product_payload.rows[0][22])

    def test_google_dependencies_error_is_lazy_and_explicit(self) -> None:
        with patch("monitoring.services.google_sheets.GOOGLE_CLIENTS_AVAILABLE", False):
            with self.assertRaisesMessage(GoogleSheetsSyncError, "python -m pip install -r requirements.txt"):
                _ensure_google_dependencies()

    def test_sparse_value_updates_preserve_non_contiguous_cells(self) -> None:
        updates = _sparse_value_updates(
            "Sheet1",
            [
                ["", "", "Header", "", "", "Tail"],
                ["Value", "Dense", "Row"],
            ],
        )
        self.assertEqual(
            updates,
            [
                {"range": "'Sheet1'!C1:C1", "values": [["Header"]]},
                {"range": "'Sheet1'!F1:F1", "values": [["Tail"]]},
            ],
        )

    def test_sheet_values_for_locale_converts_formulas_for_ru_locale(self) -> None:
        rows = [["=IFERROR(C6/SUM(C6:E6),0)", "=IF(F6>0,1,0)", "Text"]]
        converted = _sheet_values_for_locale(rows, "ru_RU")
        self.assertEqual(converted[0][0], "=IFERROR(C6/SUM(C6:E6);0)")
        self.assertEqual(converted[0][1], "=IF(F6>0;1;0)")
        self.assertEqual(converted[0][2], "Text")


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


class WorkspaceOverviewTests(TestCase):
    def test_workspace_overview_contains_schedule_and_sync_state(self) -> None:
        settings_obj = MonitoringSettings.get_solo()
        settings_obj.report_timezone = "Asia/Bishkek"
        settings_obj.sync_hour = 10
        settings_obj.sync_minute = 0
        settings_obj.google_spreadsheet_id = "test-sheet-id"
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
        self.assertEqual(len(overview["signals"]), 4)
        self.assertIsNotNone(overview["next_run"])


class ReportingHubTests(TestCase):
    def test_build_reports_context_contains_chart_payloads(self) -> None:
        seed_demo_dataset()
        context = build_reports_context(reference_date=date(2026, 3, 18), range_days=14)

        self.assertEqual(context["range_days"], 14)
        self.assertIn("timeline_chart", context)
        self.assertIn("product_chart", context)
        self.assertEqual(context["timeline_chart"]["defaultMetric"], "revenue")
        self.assertTrue(context["summary_cards"])


class PageRenderTests(TestCase):
    def test_dashboard_and_product_detail_pages_render(self) -> None:
        seed_demo_dataset()
        product = Product.objects.filter(is_active=True).first()
        dashboard = self.client.get("/")
        detail = self.client.get(f"/products/{product.pk}/")
        reports = self.client.get("/reports/")
        self.assertEqual(dashboard.status_code, 200)
        self.assertEqual(detail.status_code, 200)
        self.assertEqual(reports.status_code, 200)
