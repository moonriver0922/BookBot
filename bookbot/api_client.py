from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

import httpx
from loguru import logger

from bookbot.config import AppConfig


@dataclass
class ApiCallResult:
    ok: bool
    status_code: int
    payload: dict[str, Any] | None = None
    text: str = ""
    error: str = ""


@dataclass
class ApiSessionBridge:
    cookie_header: str
    csrf_tokens: dict[str, str]

    @property
    def is_empty(self) -> bool:
        return not self.cookie_header and not self.csrf_tokens


class BookingApiClient:
    """Protocol-level booking client for API-first/hybrid execution.

    The exact search/submit endpoints are site-specific and configurable.
    If endpoints are not configured, this client remains in a disabled state
    and callers should fall back to the UI workflow.
    """

    def __init__(self, config: AppConfig, session: ApiSessionBridge) -> None:
        self._config = config
        self._session = session
        self._timeout = max(500, int(config.api.request_timeout_ms)) / 1000.0
        self._retries = max(0, int(config.api.retry_count))
        self._base_url = config.api.base_url.rstrip("/") + "/"

    @property
    def enabled(self) -> bool:
        if not self._config.api.enabled:
            return False
        return bool(self._config.api.search_endpoint and self._config.api.submit_endpoint)

    def _headers(self) -> dict[str, str]:
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "User-Agent": "BookBot-Hybrid/1.0",
        }
        if self._session.cookie_header:
            headers["Cookie"] = self._session.cookie_header
        for k, v in self._session.csrf_tokens.items():
            headers[k] = v
        return headers

    async def _request(
        self,
        method: str,
        endpoint: str,
        *,
        payload: dict[str, Any],
    ) -> ApiCallResult:
        url = urljoin(self._base_url, endpoint.lstrip("/"))
        timeout = httpx.Timeout(self._timeout)
        headers = self._headers()
        for attempt in range(self._retries + 1):
            try:
                async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
                    response = await client.request(method, url, headers=headers, json=payload)
                body_text = response.text
                data: dict[str, Any] | None = None
                ctype = response.headers.get("content-type", "")
                if "json" in ctype.lower():
                    try:
                        parsed = response.json()
                        if isinstance(parsed, dict):
                            data = parsed
                    except Exception:
                        data = None
                return ApiCallResult(
                    ok=response.is_success,
                    status_code=response.status_code,
                    payload=data,
                    text=body_text[:2000],
                )
            except Exception as exc:
                if attempt >= self._retries:
                    return ApiCallResult(
                        ok=False,
                        status_code=0,
                        error=str(exc),
                    )
                await asyncio.sleep(0.1 * (attempt + 1))
        return ApiCallResult(ok=False, status_code=0, error="unreachable")

    async def search(self, payload: dict[str, Any]) -> ApiCallResult:
        return await self._request("POST", self._config.api.search_endpoint, payload=payload)

    async def submit(self, payload: dict[str, Any]) -> ApiCallResult:
        return await self._request("POST", self._config.api.submit_endpoint, payload=payload)


async def build_api_session_bridge(page, config: AppConfig) -> ApiSessionBridge:
    """Extract authenticated browser session data for protocol requests."""
    cookies = await page.context.cookies(config.api.base_url)
    cookie_header = "; ".join(
        f"{c.get('name', '')}={c.get('value', '')}"
        for c in cookies
        if c.get("name")
    )

    csrf_tokens = await page.evaluate(
        """() => {
            const tokens = {};
            const candidates = [
                ['meta[name="csrf-token"]', 'X-CSRF-Token', 'content'],
                ['meta[name="csrf_token"]', 'X-CSRF-Token', 'content'],
                ['input[name="_csrf"]', 'X-CSRF-Token', 'value'],
                ['input[name="csrfToken"]', 'X-CSRF-Token', 'value'],
            ];
            for (const [selector, header, attr] of candidates) {
                const el = document.querySelector(selector);
                if (el) {
                    const value = el.getAttribute(attr);
                    if (value) tokens[header] = value;
                }
            }
            return tokens;
        }"""
    )
    if not isinstance(csrf_tokens, dict):
        csrf_tokens = {}

    logger.debug(
        "API session bridge ready: cookies={}, csrf_headers={}",
        len(cookies),
        list(csrf_tokens.keys()),
    )
    return ApiSessionBridge(cookie_header=cookie_header, csrf_tokens=csrf_tokens)
