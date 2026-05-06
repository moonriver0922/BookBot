from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin, urlparse

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
    final_url: str = ""


@dataclass
class ApiSessionBridge:
    cookie_header: str
    csrf_tokens: dict[str, str]
    user_agent: str = ""
    accept_language: str = ""

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
        parsed = urlparse(self._base_url)
        self._origin = f"{parsed.scheme}://{parsed.netloc}"

    @property
    def enabled(self) -> bool:
        if not self._config.api.enabled:
            return False
        return bool(
            self._config.api.search_endpoint
            and self._config.api.prepare_endpoint
            and self._config.api.submit_endpoint
        )

    def _headers(
        self,
        *,
        accept_json: bool = True,
        xrw: bool = False,
        referer: str | None = None,
        form_encoded: bool = False,
    ) -> dict[str, str]:
        headers = {
            "Accept": "application/json, text/plain, */*" if accept_json else "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "User-Agent": self._session.user_agent or "BookBot-Hybrid/1.0",
            "Accept-Language": self._session.accept_language or "en-US,en;q=0.9",
            "Origin": self._origin,
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Dest": "empty",
        }
        if referer:
            headers["Referer"] = referer
        if xrw:
            headers["X-Requested-With"] = "XMLHttpRequest"
            headers["Sec-Fetch-Mode"] = "cors"
        else:
            headers["Sec-Fetch-Mode"] = "navigate"
        if form_encoded:
            headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
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
        query_params: dict[str, Any] | None = None,
        form_payload: dict[str, Any] | None = None,
        multipart_payload: dict[str, Any] | None = None,
        accept_json: bool = True,
        xrw: bool = False,
        referer: str | None = None,
    ) -> ApiCallResult:
        url = urljoin(self._base_url, endpoint.lstrip("/"))
        timeout = httpx.Timeout(self._timeout)
        headers = self._headers(accept_json=accept_json, xrw=xrw, referer=referer)
        files = None
        data = None
        if multipart_payload is not None:
            files = [(k, (None, "" if v is None else str(v))) for k, v in multipart_payload.items()]
        elif form_payload is not None:
            data = {k: "" if v is None else str(v) for k, v in form_payload.items()}
            headers = self._headers(
                accept_json=accept_json,
                xrw=xrw,
                referer=referer,
                form_encoded=True,
            )

        for attempt in range(self._retries + 1):
            try:
                async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
                    response = await client.request(
                        method,
                        url,
                        headers=headers,
                        params=query_params,
                        data=data,
                        files=files,
                    )
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
                    text=body_text[:20000],
                    final_url=str(response.url),
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

    async def search(self, *, csrf_token: str, payload: dict[str, Any]) -> ApiCallResult:
        return await self._request(
            "POST",
            self._config.api.search_endpoint,
            query_params={"CSRFToken": csrf_token},
            form_payload=payload,
            accept_json=True,
            xrw=True,
            referer=urljoin(self._base_url, "starspossfbstud/secure/ui_make_book/make_book.do"),
        )

    async def prepare_submit(self, payload: dict[str, Any]) -> ApiCallResult:
        return await self._request(
            "POST",
            self._config.api.prepare_endpoint,
            form_payload=payload,
            accept_json=False,
            referer=urljoin(self._base_url, "starspossfbstud/secure/ui_make_book/make_book.do"),
        )

    async def submit(self, payload: dict[str, Any]) -> ApiCallResult:
        return await self._request(
            "POST",
            self._config.api.submit_endpoint,
            multipart_payload=payload,
            accept_json=False,
            referer=urljoin(self._base_url, "starspossfbstud/secure/ui_make_book/make_book_submit.do"),
        )


_INPUT_RE = re.compile(
    r"<input[^>]*name=[\"'](?P<name>[^\"']+)[\"'][^>]*>",
    flags=re.IGNORECASE,
)
_VALUE_RE = re.compile(r"value=[\"']([^\"']*)[\"']", flags=re.IGNORECASE)
_CHECKED_RE = re.compile(r"\schecked(?:\s|>|=)", flags=re.IGNORECASE)


def extract_form_fields_from_html(html: str) -> dict[str, str]:
    """Best-effort extraction of HTML form input name/value pairs."""
    fields: dict[str, str] = {}
    for m in _INPUT_RE.finditer(html):
        tag = m.group(0)
        name = m.group("name").strip()
        lower = tag.lower()
        if "disabled" in lower:
            continue
        value_match = _VALUE_RE.search(tag)
        value = value_match.group(1) if value_match else ""
        if "type=\"checkbox\"" in lower or "type='checkbox'" in lower:
            if _CHECKED_RE.search(tag):
                fields[name] = value or "on"
            continue
        if "type=\"radio\"" in lower or "type='radio'" in lower:
            if _CHECKED_RE.search(tag):
                fields[name] = value
            continue
        fields[name] = value
    return fields


async def build_api_session_bridge(page, config: AppConfig) -> ApiSessionBridge:
    """Extract authenticated browser session data for protocol requests."""
    cookies = await page.context.cookies()
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

    user_agent = await page.evaluate("() => navigator.userAgent || ''")
    language = await page.evaluate("() => navigator.language || ''")
    accept_language = f"{language},en;q=0.9" if language else ""

    logger.debug(
        "API session bridge ready: cookies={}, csrf_headers={}",
        len(cookies),
        list(csrf_tokens.keys()),
    )
    return ApiSessionBridge(
        cookie_header=cookie_header,
        csrf_tokens=csrf_tokens,
        user_agent=user_agent if isinstance(user_agent, str) else "",
        accept_language=accept_language,
    )
