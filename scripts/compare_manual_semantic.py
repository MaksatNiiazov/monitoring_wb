from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "wb_monitoring.settings")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import django

django.setup()

from monitoring.models import Product
from monitoring.services.google_sheets import GoogleSheetsGateway
from monitoring.services.reports import build_product_report, decimalize


MANUAL_SPREADSHEET_ID = "12xidvaeYnq_RKHfLOISs2GrwEDirvCJ7f32kpPL7gSg"
COMMON_TITLES = [
    "M-BNN01-BLK",
    "M-SHR01-BLK",
    "M-JBN01-BLK",
    "M-JBG01-BLK",
    "W-LNG01-DBL",
    "W-RSS01-GRY",
    "W-WSS01-SGR",
    "W-TWD01-BEJ",
    "W-TWD01-BLK",
]
TARGET_DATES = [date(2026, 3, 16), date(2026, 3, 17), date(2026, 3, 18)]


def parse_manual_date(value: str) -> date | None:
    text = str(value).strip()
    if not text or "." not in text:
        return None
    parts = text.split(".")
    if len(parts) != 3:
        return None
    try:
        day, month, year = (int(part) for part in parts)
    except ValueError:
        return None
    return date(year, month, day)


def parse_number(value: object) -> Decimal | None:
    text = str(value).strip()
    if text in {"", "-", "#DIV/0!", "#VALUE!", "None"}:
        return None
    text = text.replace("\xa0", "").replace("р.", "").replace("%", "").replace("+", "").replace(",", ".")
    try:
        return Decimal(text)
    except Exception:
        return None


def fetch_manual_sheet(title: str) -> list[list[str]]:
    service = GoogleSheetsGateway(MANUAL_SPREADSHEET_ID).service
    values = (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=MANUAL_SPREADSHEET_ID,
            range=f"'{title}'!A1:ZZ12",
            valueRenderOption="FORMATTED_VALUE",
        )
        .execute()
        .get("values", [])
    )
    return values


def manual_metrics_for_date(rows: list[list[str]], target: date) -> dict[str, Decimal | None] | None:
    if not rows:
        return None
    header = rows[0]
    target_indexes = [idx for idx, value in enumerate(header) if parse_manual_date(value) == target]
    if not target_indexes:
        return None
    idx = target_indexes[-1]

    def row_value(row_number: int, offset: int) -> Decimal | None:
        row = rows[row_number - 1] if len(rows) >= row_number else []
        absolute_index = idx + offset
        if absolute_index < 0 or absolute_index >= len(row):
            return None
        return parse_number(row[absolute_index])

    return {
        "search_spend": row_value(4, -4),
        "shelves_spend": row_value(4, -3),
        "total_spend": row_value(4, -2),
        "total_impressions": row_value(5, -2),
        "total_clicks": row_value(9, -2),
        "total_carts": row_value(10, -2),
        "total_orders": row_value(11, -2),
        "total_order_sum": row_value(12, -2),
        "organic_orders": row_value(11, -1),
        "organic_order_sum": row_value(12, -1),
    }


def our_metrics_for_date(title: str, target: date) -> dict[str, Decimal]:
    product = Product.objects.get(vendor_code=title)
    report = build_product_report(
        product=product,
        stats_date=target - timedelta(days=1),
        stock_date=target,
        create_note=False,
    )
    return {
        "search_spend": decimalize(report["blocks"]["unified_search"].spend),
        "shelves_spend": decimalize(report["blocks"]["unified_shelves"].spend),
        "total_spend": decimalize(report["total_ad"].spend),
        "total_impressions": Decimal(report["total_ad"].impressions),
        "total_clicks": Decimal(report["total_ad"].clicks),
        "total_carts": Decimal(report["total_ad"].carts),
        "total_orders": Decimal(report["total_ad"].orders),
        "total_order_sum": decimalize(report["total_ad"].order_sum),
        "organic_orders": Decimal(report["organic"]["order_count"]),
        "organic_order_sum": decimalize(report["organic"]["order_sum"]),
    }


def main() -> None:
    for title in COMMON_TITLES:
        rows = fetch_manual_sheet(title)
        print(f"## {title}")
        for target in TARGET_DATES:
            manual_metrics = manual_metrics_for_date(rows, target)
            if manual_metrics is None:
                print(f"{target}: manual date missing")
                continue
            our_metrics = our_metrics_for_date(title, target)
            print(f"{target}:")
            for key, manual_value in manual_metrics.items():
                our_value = our_metrics[key]
                print(f"  {key}: manual={manual_value} ours={our_value}")


if __name__ == "__main__":
    main()
