from __future__ import annotations

from datetime import date
import json
import time
from typing import Any

import requests
from django.conf import settings


class WBApiError(Exception):
    pass


class BaseWBClient:
    base_url: str = ""
    token: str = ""
    min_interval_seconds: float = 1.0
    max_retries: int = 12
    max_retry_delay_seconds: float = 90.0

    def __init__(self, token: str | None = None) -> None:
        self.token = token or self.token
        self._last_request_at = 0.0
        self._next_request_at = 0.0
        if not self.token:
            raise WBApiError("Не задан API-токен Wildberries.")

    def _wait_for_rate_window(self) -> None:
        now = time.monotonic()
        base_delay = self.min_interval_seconds - (now - self._last_request_at)
        rate_delay = self._next_request_at - now
        delay = max(base_delay, rate_delay)
        if delay > 0:
            time.sleep(delay)

    def _header_float(self, response: requests.Response, *names: str) -> float | None:
        for header_name in names:
            raw_value = response.headers.get(header_name)
            if not raw_value:
                continue
            try:
                parsed = float(raw_value)
            except (TypeError, ValueError):
                continue
            if parsed > 0:
                return parsed
        return None

    def _update_rate_window(self, response: requests.Response) -> None:
        now = time.monotonic()
        retry_after = self._header_float(response, "X-Ratelimit-Retry", "X-Ratelimit-Reset", "Retry-After")
        if retry_after:
            self._next_request_at = max(self._next_request_at, now + min(self.max_retry_delay_seconds, retry_after))
            return
        remaining = response.headers.get("X-Ratelimit-Remaining")
        reset_after = self._header_float(response, "X-Ratelimit-Reset")
        if remaining == "0" and reset_after:
            self._next_request_at = max(self._next_request_at, now + min(self.max_retry_delay_seconds, reset_after))
            return
        self._next_request_at = max(self._next_request_at, now + self.min_interval_seconds)

    def _retry_delay(self, response: requests.Response, attempt: int) -> float:
        retry_hint = self._header_float(response, "X-Ratelimit-Retry", "X-Ratelimit-Reset", "Retry-After")
        if retry_hint:
            return min(self.max_retry_delay_seconds, max(self.min_interval_seconds, retry_hint))
        return min(self.max_retry_delay_seconds, max(self.min_interval_seconds, 2 ** attempt))

    def _response_detail(self, response: requests.Response) -> str:
        try:
            payload = response.json()
        except (ValueError, json.JSONDecodeError):
            payload = None
        if isinstance(payload, dict):
            detail = payload.get("detail") or payload.get("title") or payload.get("message")
            if detail:
                return str(detail)
        text = (response.text or "").strip()
        return text[:200] if text else ""

    def _format_error(self, response: requests.Response) -> str:
        detail = self._response_detail(response)
        if response.status_code == 429:
            suffix = f" Деталь WB: {detail}" if detail else ""
            return (
                "Wildberries временно ограничил запросы по кабинету. "
                "Клиент автоматически ждал окно лимита, но оно не освободилось вовремя."
                f"{suffix}"
            )
        if detail:
            return f"WB API {response.status_code}: {detail}"
        return f"WB API {response.status_code}: request failed."

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        last_error = ""
        for attempt in range(self.max_retries):
            self._wait_for_rate_window()
            response = requests.request(
                method=method,
                url=f"{self.base_url}{path}",
                headers={
                    "Authorization": self.token,
                    "Content-Type": "application/json",
                },
                params=params,
                json=payload,
                timeout=45,
            )
            self._last_request_at = time.monotonic()
            self._update_rate_window(response)
            if response.status_code < 400:
                if not response.text.strip():
                    return None
                return response.json()

            last_error = self._format_error(response)
            if response.status_code in {429, 500, 502, 503, 504} and attempt < self.max_retries - 1:
                time.sleep(self._retry_delay(response, attempt))
                continue
            raise WBApiError(last_error)

        raise WBApiError(last_error or "WB API request failed.")


class AnalyticsWBClient(BaseWBClient):
    base_url = "https://seller-analytics-api.wildberries.ru"

    def __init__(self, token: str | None = None) -> None:
        super().__init__(token or settings.WB_ANALYTICS_API_TOKEN)

    def get_sales_funnel_history(self, *, nm_ids: list[int], start_date: date, end_date: date) -> list[dict[str, Any]]:
        return self._request(
            "POST",
            "/api/analytics/v3/sales-funnel/products/history",
            payload={
                "selectedPeriod": {
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat(),
                },
                "nmIds": nm_ids,
                "skipDeletedNm": True,
                "aggregationLevel": "day",
            },
        )

    def get_product_stocks(self, *, nm_ids: list[int], snapshot_date: date) -> dict[str, Any]:
        return self._request(
            "POST",
            "/api/v2/stocks-report/products/products",
            payload={
                "nmIDs": nm_ids,
                "currentPeriod": {
                    "start": snapshot_date.isoformat(),
                    "end": snapshot_date.isoformat(),
                },
                "stockType": settings.WB_STOCK_TYPE,
                "skipDeletedNm": True,
                "orderBy": {"field": "avgOrders", "mode": "desc"},
                "availabilityFilters": [
                    "deficient",
                    "actual",
                    "balanced",
                    "nonActual",
                    "nonLiquid",
                    "invalidData",
                ],
                "limit": max(100, len(nm_ids)),
                "offset": 0,
            },
        )

    def get_product_sizes(self, *, nm_id: int, snapshot_date: date) -> dict[str, Any]:
        return self._request(
            "POST",
            "/api/v2/stocks-report/products/sizes",
            payload={
                "nmID": nm_id,
                "currentPeriod": {
                    "start": snapshot_date.isoformat(),
                    "end": snapshot_date.isoformat(),
                },
                "stockType": settings.WB_STOCK_TYPE,
                "orderBy": {"field": "avgOrders", "mode": "desc"},
                "includeOffice": True,
            },
        )


class PromotionWBClient(BaseWBClient):
    base_url = "https://advert-api.wildberries.ru"

    def __init__(self, token: str | None = None) -> None:
        super().__init__(token or settings.WB_PROMOTION_API_TOKEN)

    def get_campaigns(self, *, ids: list[int] | None = None, statuses: list[int] | None = None) -> dict[str, Any]:
        params: dict[str, str] = {}
        if ids:
            params["ids"] = ",".join(str(item) for item in ids)
        if statuses:
            params["statuses"] = ",".join(str(item) for item in statuses)
        return self._request("GET", "/api/advert/v2/adverts", params=params)

    def get_campaign_stats(self, *, ids: list[int], start_date: date, end_date: date) -> list[dict[str, Any]]:
        return self._request(
            "GET",
            "/adv/v3/fullstats",
            params={
                "ids": ",".join(str(item) for item in ids),
                "beginDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
            },
        )
