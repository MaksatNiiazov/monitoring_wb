from __future__ import annotations

from typing import Iterable

import os
import sys
from pathlib import Path

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "wb_monitoring.settings")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import django

django.setup()

from monitoring.models import MonitoringSettings
from monitoring.services.google_sheets import GoogleSheetsGateway


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
CHECKS: list[tuple[str, str]] = [
    ("header_c1", "C1"),
    ("header_m1", "M1"),
    ("header_w1", "W1"),
    ("buyout_c20", "C20"),
    ("buyout_m20", "M20"),
    ("buyout_w20", "W20"),
    ("seller_g38", "G38"),
    ("seller_q38", "Q38"),
    ("seller_aa38", "AA38"),
    ("traffic_c4", "C4"),
    ("orders_h12", "H12"),
    ("orders_r12", "R12"),
    ("orders_ab12", "AB12"),
    ("stock_h28", "H28"),
    ("stock_r28", "R28"),
    ("stock_ab28", "AB28"),
]


def cell_value(service, spreadsheet_id: str, title: str, a1: str, render: str) -> str:
    quoted_range = f"'{title}'!{a1}"
    values = (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=spreadsheet_id,
            range=quoted_range,
            valueRenderOption=render,
        )
        .execute()
        .get("values", [])
    )
    if not values or not values[0]:
        return ""
    return str(values[0][0])


def diff_rows(title: str, manual_service, our_service, our_spreadsheet_id: str) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    for check_name, a1 in CHECKS:
        render = "FORMULA" if check_name == "traffic_c4" else "FORMATTED_VALUE"
        manual_value = cell_value(manual_service, MANUAL_SPREADSHEET_ID, title, a1, render)
        our_value = cell_value(our_service, our_spreadsheet_id, title, a1, render)
        if manual_value != our_value:
            rows.append((a1, manual_value, our_value))
    return rows


def main() -> None:
    settings = MonitoringSettings.get_solo()
    manual_gateway = GoogleSheetsGateway(MANUAL_SPREADSHEET_ID)
    our_gateway = GoogleSheetsGateway(settings.google_spreadsheet_id)

    total_diffs = 0
    for title in COMMON_TITLES:
        diffs = diff_rows(title, manual_gateway.service, our_gateway.service, settings.google_spreadsheet_id)
        print(f"## {title}")
        if not diffs:
            print("MATCH")
            continue
        total_diffs += len(diffs)
        for a1, manual_value, our_value in diffs:
            print(f"{a1}\n  manual: {manual_value}\n  ours:   {our_value}")
    print(f"\nTOTAL_DIFFS={total_diffs}")


if __name__ == "__main__":
    main()
