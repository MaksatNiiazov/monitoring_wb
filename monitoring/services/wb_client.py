from __future__ import annotations

from datetime import date
import json
import random
import threading
import time
from typing import Any, Callable, ClassVar

import requests
from django.conf import settings


class WBApiError(Exception):
    pass


class _RateLimitState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.next_request_at = 0.0
        self.consecutive_429_count = 0
        self.adaptive_delay_seconds = 0.0
        # Circuit breaker: если слишком много ошибок — "выключаем" эндпоинт
        self.circuit_broken_until = 0.0
        self.circuit_failure_count = 0


class BaseWBClient:
    base_url: str = ""
    token: str = ""
    # WB can return 429/code 461 from a global seller limiter. The limiter
    # below is process-wide, so parallel sync workers and separate client
    # instances cannot create bursts behind each other's backs.
    min_interval_seconds: float = 1.0
    burst_limit: int = 1
    max_retries: int = 5
    max_retry_delay_seconds: float = 300.0
    fast_fail_rate_limit: bool = False
    update_shared_rate_limit_on_429: bool = True
    _shared_rate_limit_state: ClassVar[_RateLimitState] = _RateLimitState()
    _consecutive_429_count: int = 0
    _adaptive_delay_seconds: float = 0.0

    def __init__(self, token: str | None = None) -> None:
        self.token = token or self.token
        self.retry_callback: Callable[[dict[str, Any]], None] | None = None
        self._last_request_at = 0.0
        self._next_request_at = 0.0
        self._burst_remaining: int = self.burst_limit
        self._consecutive_429_count = 0
        self._adaptive_delay_seconds = 0.0
        if not self.token:
            raise WBApiError("Не задан API-токен Wildberries.")
        if len(self.token) < 20:
            raise WBApiError(f"API-токен Wildberries выглядит невалидным (длина {len(self.token)}). Проверьте настройки.")

    def _wait_for_rate_window(self) -> None:
        state = BaseWBClient._shared_rate_limit_state
        while True:
            with state.lock:
                now = time.monotonic()
                delay = max(0.0, state.next_request_at - now)
                if delay <= 0:
                    interval = max(self.min_interval_seconds, state.adaptive_delay_seconds)
                    state.next_request_at = now + interval
                    return
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
        remaining = response.headers.get("X-Ratelimit-Remaining")
        reset_after = self._header_float(response, "X-Ratelimit-Reset")
        retry_after = self._header_float(response, "X-Ratelimit-Retry")
        state = BaseWBClient._shared_rate_limit_state
        with state.lock:
            # Circuit breaker: если 5+ consecutive 429 — "выключаем" на 5 минут
            CIRCUIT_BREAKER_THRESHOLD = 5
            CIRCUIT_BREAKER_COOLDOWN = 300.0  # 5 минут

            if response.status_code == 429:
                if not self.update_shared_rate_limit_on_429:
                    state.next_request_at = max(state.next_request_at, now + self.min_interval_seconds)
                    return
                state.consecutive_429_count += 1
                state.circuit_failure_count += 1
                state.adaptive_delay_seconds = min(
                    60.0,
                    max(self.min_interval_seconds, 2 ** (state.consecutive_429_count - 1)),
                )
                # Активируем circuit breaker
                if state.circuit_failure_count >= CIRCUIT_BREAKER_THRESHOLD:
                    state.circuit_broken_until = max(state.circuit_broken_until, now + CIRCUIT_BREAKER_COOLDOWN)
                if self._is_global_limiter_response(response):
                    delay = self._retry_delay(response, state.consecutive_429_count - 1)
                    state.next_request_at = max(state.next_request_at, now + delay)
                    return
            else:
                # Успешный запрос — сбрасываем failure count и circuit breaker
                state.circuit_failure_count = 0
                state.circuit_broken_until = 0.0
                if state.consecutive_429_count > 0:
                    state.consecutive_429_count = max(0, state.consecutive_429_count - 1)
                state.adaptive_delay_seconds = max(0.0, state.adaptive_delay_seconds * 0.5)

            # Проверяем circuit breaker перед планированием
            if state.circuit_broken_until > now:
                state.next_request_at = max(state.next_request_at, state.circuit_broken_until)
                return

            if retry_after and retry_after > 0:
                state.next_request_at = max(
                    state.next_request_at,
                    now + min(self.max_retry_delay_seconds, retry_after),
                )
                return

            if remaining == "0" and reset_after:
                state.next_request_at = max(
                    state.next_request_at,
                    now + min(self.max_retry_delay_seconds, reset_after),
                )
                return

            interval = max(self.min_interval_seconds, state.adaptive_delay_seconds)
            state.next_request_at = max(state.next_request_at, now + interval)

    def _retry_delay(self, response: requests.Response, attempt: int) -> float:
        # Jitter: добавляем случайный разброс 0-25% для предотвращения thundering herd
        jitter = 1.0 + random.uniform(0.0, 0.25)

        if response.status_code == 429:
            header_delay = (
                self._header_float(response, "X-Ratelimit-Retry")
                or self._header_float(response, "X-Ratelimit-Reset")
                or self._header_float(response, "Retry-After")
                or 0.0
            )
            if self._is_global_limiter_response(response):
                # 429/code 461: глобальный лимитер часто требует минуты ожидания
                global_delay = min(self.max_retry_delay_seconds, 60.0 * (2 ** min(attempt, 3)))
                return min(self.max_retry_delay_seconds, max(header_delay, global_delay) * jitter)
            if header_delay > 0:
                return min(self.max_retry_delay_seconds, header_delay * jitter)

        return min(self.max_retry_delay_seconds, self.min_interval_seconds * (2 ** attempt) * jitter)

    def _response_payload(self, response: requests.Response) -> Any:
        try:
            return response.json()
        except (ValueError, json.JSONDecodeError):
            return None

    def _response_detail(self, response: requests.Response) -> str:
        payload = self._response_payload(response)
        if isinstance(payload, dict):
            detail = payload.get("detail") or payload.get("title") or payload.get("message")
            if detail:
                return str(detail)
        text = (response.text or "").strip()
        return text[:200] if text else ""

    def _is_global_limiter_response(self, response: requests.Response) -> bool:
        if response.status_code != 429:
            return False
        payload = self._response_payload(response)
        code = str(payload.get("code") or "") if isinstance(payload, dict) else ""
        detail = self._response_detail(response).lower()
        body = (response.text or "").lower()
        return (
            code == "461"
            or "global limiter" in detail
            or "global limiter" in body
            or "limited by global" in detail
            or "limited by global" in body
        )

    def _format_error(self, response: requests.Response) -> str:
        detail = self._response_detail(response)
        if response.status_code == 429:
            if self._is_global_limiter_response(response):
                return f"WB API 429/461 (Global Rate Limit): {detail}. Нужно подождать 1-5 минут и повторить без параллельных запросов."
            if detail:
                return f"WB API 429 (Rate Limit): {detail}"
            return f"WB API 429: Rate Limit"
        if response.status_code == 401:
            return f"WB API 401: Неавторизован. Проверьте API-токен (длина {len(self.token) if self.token else 0})."
        if detail:
            return f"WB API {response.status_code}: {detail}"
        return f"WB API {response.status_code}: request failed."

    def _sleep_before_retry(
        self,
        delay_seconds: float,
        *,
        method: str,
        path: str,
        response: requests.Response,
        attempt: int,
        last_error: str,
    ) -> None:
        remaining = max(0.0, delay_seconds)
        while remaining > 0:
            if self.retry_callback:
                self.retry_callback(
                    {
                        "method": method,
                        "path": path,
                        "status_code": response.status_code,
                        "is_global_limiter": self._is_global_limiter_response(response),
                        "attempt": attempt + 1,
                        "next_attempt": attempt + 2,
                        "max_retries": self.max_retries,
                        "delay_seconds": delay_seconds,
                        "remaining_seconds": remaining,
                        "error": last_error,
                    }
                )
            sleep_seconds = min(15.0, remaining)
            time.sleep(sleep_seconds)
            remaining -= sleep_seconds

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
                # БЕЗОПАСНЫЙ РЕЖИМ: микро-пауза после успешного запроса для "дыхания" API
                time.sleep(0.1)
                if not response.text.strip():
                    return None
                return response.json()

            last_error = self._format_error(response)
            if response.status_code == 429 and self.fast_fail_rate_limit:
                print(f"[WB API] Fast fail on rate limit: {last_error[:100]}", flush=True, file=sys.stderr)
                raise WBApiError(last_error)
            if response.status_code in {429, 500, 502, 503, 504} and attempt < self.max_retries - 1:
                # При 429 сбрасываем burst (документация WB)
                if response.status_code == 429:
                    self._burst_remaining = 0
                retry_delay = self._retry_delay(response, attempt)
                print(f"[WB API] Retry after {retry_delay:.1f}s: {last_error[:100]}", flush=True, file=sys.stderr)
                self._sleep_before_retry(
                    retry_delay,
                    method=method,
                    path=path,
                    response=response,
                    attempt=attempt,
                    last_error=last_error,
                )
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
    min_interval_seconds = 3.0
    max_retries = 7
    max_retry_delay_seconds = 300.0

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
    # Отзывы не критичны для цифр таблицы: при 429/461 пропускаем их быстро.
    min_interval_seconds = 5.0
    max_retries = 1
    max_retry_delay_seconds = 30.0
    fast_fail_rate_limit = True
    update_shared_rate_limit_on_429 = False

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
