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
    # Согласно документации WB: 300 запросов/мин = интервал 200ms, burst 20
    min_interval_seconds: float = 0.25  # 250ms между запросами (с запасом от 200ms)
    burst_limit: int = 20  # Можно сделать 20 запросов без задержки (burst)
    max_retries: int = 3
    max_retry_delay_seconds: float = 30.0  # Для кода 461 WB требует ждать дольше

    def __init__(self, token: str | None = None) -> None:
        self.token = token or self.token
        self._last_request_at = 0.0
        self._next_request_at = 0.0
        self._burst_remaining: int = self.burst_limit  # Счётчик burst-запросов
        if not self.token:
            raise WBApiError("Не задан API-токен Wildberries.")
        if len(self.token) < 20:
            raise WBApiError(f"API-токен Wildberries выглядит невалидным (длина {len(self.token)}). Проверьте настройки.")

    def _wait_for_rate_window(self) -> None:
        now = time.monotonic()
        # Если есть burst-запросы, не ждём (делаем запрос сразу)
        if self._burst_remaining > 0:
            self._burst_remaining -= 1
            return
        # Burst исчерпан — ждём согласно лимитам
        base_delay = self.min_interval_seconds - (now - self._last_request_at)
        rate_delay = self._next_request_at - now
        delay = max(base_delay, rate_delay, 0)
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
        
        # Проверяем global limiter (461) — отдельный жёсткий лимит
        if response.status_code == 429:
            detail = self._response_detail(response)
            if detail and ("global limiter" in detail.lower() or "461" in detail):
                # Global limiter — ждём 30 секунд
                self._next_request_at = max(self._next_request_at, now + 30.0)
                self._burst_remaining = 0
                return
        
        # Обрабатываем заголовки X-Ratelimit-* согласно документации WB
        remaining = response.headers.get("X-Ratelimit-Remaining")
        reset_after = self._header_float(response, "X-Ratelimit-Reset")
        retry_after = self._header_float(response, "X-Ratelimit-Retry")
        
        # Если WB говорит подождать — ждём
        if retry_after and retry_after > 0:
            self._next_request_at = max(self._next_request_at, now + retry_after)
            self._burst_remaining = 0  # Сбрасываем burst
            return
        
        # Если remaining = 0, ждём reset
        if remaining == "0" and reset_after:
            self._next_request_at = max(self._next_request_at, now + min(self.max_retry_delay_seconds, reset_after))
            self._burst_remaining = 0
            return
        
        # Обновляем burst из заголовка или восстанавливаем постепенно
        if remaining is not None:
            try:
                self._burst_remaining = max(0, int(remaining))
            except (ValueError, TypeError):
                pass
        
        # Стандартная задержка между запросами
        self._next_request_at = max(self._next_request_at, now + self.min_interval_seconds)

    def _retry_delay(self, response: requests.Response, attempt: int) -> float:
        # При 429 используем заголовки X-Ratelimit-* согласно документации
        if response.status_code == 429:
            # Сначала пробуем X-Ratelimit-Retry (через сколько можно повторить)
            retry_after = self._header_float(response, "X-Ratelimit-Retry")
            if retry_after and retry_after > 0:
                return min(self.max_retry_delay_seconds, retry_after)
            # Затем X-Ratelimit-Reset (через сколько восстановится burst)
            reset_after = self._header_float(response, "X-Ratelimit-Reset")
            if reset_after and reset_after > 0:
                return min(self.max_retry_delay_seconds, reset_after)
            # Fallback на Retry-After (стандартный HTTP заголовок)
            retry_after_std = self._header_float(response, "Retry-After")
            if retry_after_std and retry_after_std > 0:
                return min(self.max_retry_delay_seconds, retry_after_std)
        
        # Для global limiter (461) используем фиксированную задержку
        detail = self._response_detail(response)
        if detail and ("global limiter" in detail.lower() or "461" in detail):
            return 30.0  # WB требует 30+ секунд для global limiter
        
        # Экспоненциальный backoff для остальных ошибок
        return min(self.max_retry_delay_seconds, self.min_interval_seconds * (2 ** attempt))

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
            # WB возвращает 429 с code 461 для global limiter
            is_global_limit = detail and ("global limiter" in detail.lower() or "461" in detail)
            if is_global_limit:
                return f"WB API 429/461 (Global Rate Limit): {detail}. Нужно подождать 30-60 секунд между синхронизациями."
            if detail:
                return f"WB API 429 (Rate Limit): {detail}"
            return f"WB API 429: Rate Limit"
        if response.status_code == 401:
            return f"WB API 401: Неавторизован. Проверьте API-токен (длина {len(self.token) if self.token else 0})."
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
        import sys
        last_error = ""
        for attempt in range(self.max_retries):
            self._wait_for_rate_window()
            url = f"{self.base_url}{path}"
            print(f"[WB API] {method} {url} (attempt {attempt + 1}/{self.max_retries})", flush=True, file=sys.stderr)
            start_time = time.monotonic()
            try:
                response = requests.request(
                    method=method,
                    url=url,
                    headers={
                        "Authorization": self.token,
                        "Content-Type": "application/json",
                    },
                    params=params,
                    json=payload,
                    timeout=30,  # Уменьшили для быстрой отработки зависаний
                )
                elapsed = time.monotonic() - start_time
                body_preview = (response.text or "")[:200]
                print(f"[WB API] {method} {path} -> {response.status_code} in {elapsed:.1f}s | body: {body_preview}", flush=True, file=sys.stderr)
            except Exception as exc:
                elapsed = time.monotonic() - start_time
                print(f"[WB API] {method} {path} -> EXCEPTION after {elapsed:.1f}s: {exc}", flush=True, file=sys.stderr)
                raise
            self._last_request_at = time.monotonic()
            self._update_rate_window(response)
            if response.status_code < 400:
                if not response.text.strip():
                    return None
                return response.json()

            last_error = self._format_error(response)
            if response.status_code in {429, 500, 502, 503, 504} and attempt < self.max_retries - 1:
                # При 429 сбрасываем burst (документация WB)
                if response.status_code == 429:
                    self._burst_remaining = 0
                retry_delay = self._retry_delay(response, attempt)
                print(f"[WB API] Retry after {retry_delay:.1f}s: {last_error[:100]}", flush=True, file=sys.stderr)
                time.sleep(retry_delay)
                continue
            print(f"[WB API] ERROR: {last_error[:100]}", flush=True, file=sys.stderr)
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

    def get_search_orders(
        self,
        *,
        nm_id: int,
        start_date: date,
        end_date: date,
        search_texts: list[str],
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            "/api/v2/search-report/product/orders",
            payload={
                "period": {
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat(),
                },
                "nmId": nm_id,
                "searchTexts": search_texts,
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

    def get_daily_search_cluster_stats(
        self,
        *,
        items: list[dict[str, int]],
        start_date: date,
        end_date: date,
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            "/adv/v1/normquery/stats",
            payload={
                "from": start_date.isoformat(),
                "to": end_date.isoformat(),
                "items": items,
            },
        )


class StatisticsWBClient(BaseWBClient):
    base_url = "https://statistics-api.wildberries.ru"

    def __init__(self, token: str | None = None) -> None:
        super().__init__(token or settings.WB_ANALYTICS_API_TOKEN)

    def get_supplier_orders(self, *, date_from: date, flag: int = 1) -> list[dict[str, Any]]:
        return self._request(
            "GET",
            "/api/v1/supplier/orders",
            params={
                "dateFrom": date_from.isoformat(),
                "flag": flag,
            },
        )


class PricesWBClient(BaseWBClient):
    base_url = "https://discounts-prices-api.wildberries.ru"

    def __init__(self, token: str | None = None) -> None:
        super().__init__(token or settings.WB_ANALYTICS_API_TOKEN)

    def get_goods_prices(self, *, nm_ids: list[int]) -> dict[str, Any]:
        return self._request(
            "POST",
            "/api/v2/list/goods/filter",
            payload={
                "nmList": nm_ids,
            },
        )


class FeedbacksWBClient(BaseWBClient):
    base_url = "https://feedbacks-api.wildberries.ru"

    def __init__(self, token: str | None = None) -> None:
        super().__init__(token or settings.WB_ANALYTICS_API_TOKEN)

    def get_feedbacks(
        self,
        *,
        nm_id: int,
        is_answered: bool,
        take: int = 100,
        skip: int = 0,
    ) -> dict[str, Any]:
        return self._request(
            "GET",
            "/api/v1/feedbacks",
            params={
                "nmId": nm_id,
                "isAnswered": "true" if is_answered else "false",
                "take": take,
                "skip": skip,
            },
        )
