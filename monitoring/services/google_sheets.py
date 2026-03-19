from __future__ import annotations

import json
import re
from datetime import date
from typing import Any

from django.conf import settings

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    GOOGLE_CLIENTS_AVAILABLE = True
except ImportError:
    service_account = None
    build = None

    class HttpError(Exception):
        pass

    GOOGLE_CLIENTS_AVAILABLE = False

from monitoring.services.config import get_monitoring_settings
from monitoring.services.monitoring_table import (
    MonitoringSheetPayload,
    build_monitoring_sheet_payloads,
)

SHEETS_SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]


class GoogleSheetsSyncError(Exception):
    pass


def _ensure_google_dependencies() -> None:
    if GOOGLE_CLIENTS_AVAILABLE:
        return
    raise GoogleSheetsSyncError(
        "Не установлены зависимости для Google Sheets. Установите их командой: python -m pip install -r requirements.txt"
    )


def _service_account_info() -> dict[str, Any]:
    if settings.GOOGLE_SERVICE_ACCOUNT_JSON:
        return json.loads(settings.GOOGLE_SERVICE_ACCOUNT_JSON)
    if settings.GOOGLE_SERVICE_ACCOUNT_FILE:
        with open(settings.GOOGLE_SERVICE_ACCOUNT_FILE, "r", encoding="utf-8") as source:
            return json.load(source)
    raise GoogleSheetsSyncError("Не настроен service account для Google Sheets.")


def _service_account_email() -> str:
    return _service_account_info().get("client_email", "")


def _build_service():
    _ensure_google_dependencies()
    credentials = service_account.Credentials.from_service_account_info(
        _service_account_info(),
        scopes=SHEETS_SCOPE,
    )
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def normalize_title(value: str) -> str:
    normalized = re.sub(r"[\\/?*\[\]:]", "_", value).strip()
    return normalized[:100] or "Sheet1"


def _sheet_values(rows: list[list[Any]]) -> list[list[Any]]:
    converted: list[list[Any]] = []
    for row in rows:
        converted_row: list[Any] = []
        for value in row:
            if value is None:
                converted_row.append("")
            else:
                converted_row.append(value)
        converted.append(converted_row)
    return converted


def _convert_formula_for_locale(value: Any, locale: str) -> Any:
    if not isinstance(value, str) or not value.startswith("="):
        return value
    if locale.lower().startswith(("ru", "uk", "de", "fr", "es", "it", "pt", "pl")):
        return value.replace(",", ";")
    return value


def _sheet_values_for_locale(rows: list[list[Any]], locale: str) -> list[list[Any]]:
    converted: list[list[Any]] = []
    for row in rows:
        converted_row: list[Any] = []
        for value in row:
            if value is None:
                converted_row.append("")
            else:
                converted_row.append(_convert_formula_for_locale(value, locale))
        converted.append(converted_row)
    return converted


def _column_letter(index: int) -> str:
    result = ""
    current = index
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _sparse_value_updates(title: str, rows: list[list[Any]]) -> list[dict[str, Any]]:
    updates: list[dict[str, Any]] = []
    for row_index, row in enumerate(rows, start=1):
        last_non_empty = 0
        for col_index, value in enumerate(row, start=1):
            if value not in ("", None):
                last_non_empty = col_index
        if not last_non_empty:
            continue

        relevant = row[:last_non_empty]
        if all(value not in ("", None) for value in relevant):
            continue

        segment_start: int | None = None
        segment_values: list[Any] = []
        for col_index, value in enumerate(relevant, start=1):
            if value in ("", None):
                if segment_start is not None:
                    end_col = segment_start + len(segment_values) - 1
                    updates.append(
                        {
                            "range": f"'{title}'!{_column_letter(segment_start)}{row_index}:{_column_letter(end_col)}{row_index}",
                            "values": [segment_values],
                        }
                    )
                    segment_start = None
                    segment_values = []
                continue
            if segment_start is None:
                segment_start = col_index
            segment_values.append(value)

        if segment_start is not None:
            end_col = segment_start + len(segment_values) - 1
            updates.append(
                {
                    "range": f"'{title}'!{_column_letter(segment_start)}{row_index}:{_column_letter(end_col)}{row_index}",
                    "values": [segment_values],
                }
            )
    return updates


def build_sheet_payloads(
    *,
    reference_date: date,
    product_ids: list[int] | None = None,
    history_days: int | None = None,
) -> list[MonitoringSheetPayload]:
    return build_monitoring_sheet_payloads(
        reference_date=reference_date,
        history_days=history_days,
        product_ids=product_ids,
    )


def _grid_range(*, sheet_id: int, start_row: int, end_row: int, start_col: int, end_col: int) -> dict[str, int]:
    return {
        "sheetId": sheet_id,
        "startRowIndex": start_row,
        "endRowIndex": end_row,
        "startColumnIndex": start_col,
        "endColumnIndex": end_col,
    }


def _hex_color(value: str) -> dict[str, float]:
    value = value.lstrip("#")
    return {
        "red": int(value[0:2], 16) / 255,
        "green": int(value[2:4], 16) / 255,
        "blue": int(value[4:6], 16) / 255,
    }


def _repeat_cell(
    *,
    sheet_id: int,
    start_row: int,
    end_row: int,
    start_col: int,
    end_col: int,
    cell_format: dict[str, Any],
    fields: str,
) -> dict[str, Any]:
    return {
        "repeatCell": {
            "range": _grid_range(
                sheet_id=sheet_id,
                start_row=start_row,
                end_row=end_row,
                start_col=start_col,
                end_col=end_col,
            ),
            "cell": {"userEnteredFormat": cell_format},
            "fields": fields,
        }
    }


def _dashboard_format_requests(sheet_id: int, rows: list[list[Any]]) -> list[dict[str, Any]]:
    used_columns = max((len(row) for row in rows), default=1)
    return [
        {
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 5}},
                "fields": "gridProperties.frozenRowCount",
            }
        },
        _repeat_cell(
            sheet_id=sheet_id,
            start_row=0,
            end_row=1,
            start_col=0,
            end_col=used_columns,
            cell_format={
                "backgroundColor": _hex_color("#15324D"),
                "textFormat": {"foregroundColor": _hex_color("#FFFFFF"), "bold": True},
            },
            fields="userEnteredFormat.backgroundColor,userEnteredFormat.textFormat",
        ),
        _repeat_cell(
            sheet_id=sheet_id,
            start_row=5,
            end_row=6,
            start_col=0,
            end_col=used_columns,
            cell_format={
                "backgroundColor": _hex_color("#E7EEF8"),
                "textFormat": {"bold": True},
            },
            fields="userEnteredFormat.backgroundColor,userEnteredFormat.textFormat",
        ),
        _repeat_cell(
            sheet_id=sheet_id,
            start_row=5,
            end_row=len(rows),
            start_col=4,
            end_col=9,
            cell_format={"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}},
            fields="userEnteredFormat.numberFormat",
        ),
    ]


def _product_format_requests(sheet_id: int, rows: list[list[Any]]) -> list[dict[str, Any]]:
    used_columns = max((len(row) for row in rows), default=1)
    requests: list[dict[str, Any]] = [
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": 3, "frozenColumnCount": 2},
                },
                "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
            }
        },
        _repeat_cell(
            sheet_id=sheet_id,
            start_row=0,
            end_row=2,
            start_col=0,
            end_col=used_columns,
            cell_format={
                "backgroundColor": _hex_color("#15324D"),
                "textFormat": {"foregroundColor": _hex_color("#FFFFFF"), "bold": True},
            },
            fields="userEnteredFormat.backgroundColor,userEnteredFormat.textFormat",
        ),
        _repeat_cell(
            sheet_id=sheet_id,
            start_row=2,
            end_row=3,
            start_col=0,
            end_col=used_columns,
            cell_format={
                "backgroundColor": _hex_color("#E7EEF8"),
                "textFormat": {"bold": True},
            },
            fields="userEnteredFormat.backgroundColor,userEnteredFormat.textFormat",
        ),
    ]

    for row_index in (32, 35, 41, 46):
        requests.append(
            _repeat_cell(
                sheet_id=sheet_id,
                start_row=row_index,
                end_row=row_index + 1,
                start_col=0,
                end_col=used_columns,
                cell_format={
                    "backgroundColor": _hex_color("#EEF3F7"),
                    "textFormat": {"bold": True},
                },
                fields="userEnteredFormat.backgroundColor,userEnteredFormat.textFormat",
            )
        )

    for row_index in (3, 6, 16, 17, 19, 36):
        requests.append(
            _repeat_cell(
                sheet_id=sheet_id,
                start_row=row_index,
                end_row=row_index + 1,
                start_col=2,
                end_col=used_columns,
                cell_format={"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}},
                fields="userEnteredFormat.numberFormat",
            )
        )

    for row_index in (4, 7, 8, 12, 13, 14, 15, 18, 20, 21, 37, 38):
        requests.append(
            _repeat_cell(
                sheet_id=sheet_id,
                start_row=row_index,
                end_row=row_index + 1,
                start_col=2,
                end_col=used_columns,
                cell_format={"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}},
                fields="userEnteredFormat.numberFormat",
            )
        )

    for row_index in (5, 9, 10, 11, 27, 28, 29):
        requests.append(
            _repeat_cell(
                sheet_id=sheet_id,
                start_row=row_index,
                end_row=row_index + 1,
                start_col=2,
                end_col=used_columns,
                cell_format={"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},
                fields="userEnteredFormat.numberFormat",
            )
        )

    for row_index in (30, 31):
        requests.append(
            _repeat_cell(
                sheet_id=sheet_id,
                start_row=row_index,
                end_row=row_index + 1,
                start_col=2,
                end_col=used_columns,
                cell_format={"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}},
                fields="userEnteredFormat.numberFormat",
            )
        )

    return requests


class GoogleSheetsGateway:
    def __init__(self, spreadsheet_id: str) -> None:
        if not spreadsheet_id:
            raise GoogleSheetsSyncError("Не задан ID Google таблицы.")
        self.spreadsheet_id = spreadsheet_id
        self.service = _build_service()
        self._titles_cache: set[str] | None = None
        self._sheet_ids_cache: dict[str, int] | None = None
        self._spreadsheet_locale_cache: str | None = None

    def _spreadsheet_metadata(self) -> dict[str, Any]:
        return self.service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()

    def _spreadsheet_locale(self) -> str:
        if self._spreadsheet_locale_cache is None:
            metadata = self._spreadsheet_metadata()
            self._spreadsheet_locale_cache = metadata.get("properties", {}).get("locale", "en_US")
        return self._spreadsheet_locale_cache

    def _sheet_titles(self) -> set[str]:
        if self._titles_cache is None:
            metadata = self._spreadsheet_metadata()
            self._titles_cache = {sheet["properties"]["title"] for sheet in metadata.get("sheets", [])}
        return self._titles_cache

    def _sheet_ids(self) -> dict[str, int]:
        if self._sheet_ids_cache is None:
            metadata = self._spreadsheet_metadata()
            self._sheet_ids_cache = {
                sheet["properties"]["title"]: sheet["properties"]["sheetId"]
                for sheet in metadata.get("sheets", [])
            }
        return self._sheet_ids_cache

    def _invalidate_cache(self) -> None:
        self._titles_cache = None
        self._sheet_ids_cache = None
        self._spreadsheet_locale_cache = None

    def ensure_sheet(self, title: str) -> None:
        if title in self._sheet_titles():
            return
        body = {"requests": [{"addSheet": {"properties": {"title": title}}}]}
        self.service.spreadsheets().batchUpdate(spreadsheetId=self.spreadsheet_id, body=body).execute()
        self._invalidate_cache()

    def _sheet_id(self, title: str) -> int:
        self.ensure_sheet(title)
        return self._sheet_ids()[title]

    def _sheet_metadata(self, title: str) -> dict[str, Any]:
        metadata = self._spreadsheet_metadata()
        return next(sheet for sheet in metadata.get("sheets", []) if sheet["properties"]["title"] == title)

    def _clear_sheet_merges(self, title: str) -> None:
        metadata = self._sheet_metadata(title)
        if not metadata.get("merges"):
            return
        properties = metadata["properties"]
        grid = properties.get("gridProperties", {})
        request = {
            "unmergeCells": {
                "range": {
                    "sheetId": properties["sheetId"],
                    "startRowIndex": 0,
                    "endRowIndex": grid.get("rowCount", 1000),
                    "startColumnIndex": 0,
                    "endColumnIndex": grid.get("columnCount", 26),
                }
            }
        }
        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body={"requests": [request]},
        ).execute()
        self._invalidate_cache()

    def write_sheet(self, title: str, rows: list[list[Any]]) -> None:
        self.ensure_sheet(title)
        self._clear_sheet_merges(title)
        locale = self._spreadsheet_locale()
        localized_rows = _sheet_values_for_locale(rows, locale)
        clear_range = f"'{title}'!A:ZZZ"
        self.service.spreadsheets().values().clear(
            spreadsheetId=self.spreadsheet_id,
            range=clear_range,
            body={},
        ).execute()
        self.service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"'{title}'!A1",
            valueInputOption="USER_ENTERED",
            body={"values": localized_rows},
        ).execute()
        sparse_updates = _sparse_value_updates(title, localized_rows)
        if sparse_updates:
            self.service.spreadsheets().values().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={
                    "valueInputOption": "USER_ENTERED",
                    "data": sparse_updates,
                },
            ).execute()

    def format_payload(self, payload: MonitoringSheetPayload) -> None:
        sheet_id = self._sheet_id(payload.title)
        if payload.kind == "dashboard":
            requests = _dashboard_format_requests(sheet_id, payload.rows)
        else:
            requests = _product_format_requests(sheet_id, payload.rows)
        if not requests:
            return
        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body={"requests": requests},
        ).execute()


def sync_reports_to_google_sheets(
    *,
    reference_date: date,
    product_ids: list[int] | None = None,
    history_days: int | None = None,
) -> int:
    runtime_settings = get_monitoring_settings()
    if not runtime_settings.google_sheets_enabled:
        raise GoogleSheetsSyncError("Интеграция Google Sheets отключена в настройках мониторинга.")
    if not runtime_settings.google_spreadsheet_id:
        raise GoogleSheetsSyncError("Не заполнен ID Google таблицы.")

    payloads = build_sheet_payloads(
        reference_date=reference_date,
        product_ids=product_ids,
        history_days=history_days,
    )
    try:
        gateway = GoogleSheetsGateway(runtime_settings.google_spreadsheet_id)
        for payload in payloads:
            gateway.write_sheet(payload.title, payload.rows)
            gateway.format_payload(payload)
    except HttpError as exc:
        raise GoogleSheetsSyncError(f"Google Sheets API error: {exc}") from exc
    return len(payloads)


def google_service_account_email() -> str:
    try:
        return _service_account_email()
    except Exception:
        return ""
