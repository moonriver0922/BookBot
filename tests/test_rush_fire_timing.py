"""Unit tests for rush fire-timing hardening (2026-09-13).

Context: the 08:30 rush fired ~4.2s late because the warm-up budget was
eroded - the "time to rush" was measured before the 3.4s server-time sync,
and a slow pre-open server made the warm-up itself take 5.6s.  The fire must
never depend on warm-up completion: the schedule is recomputed after the
sync, and the warm-up runs under a hard cap that always leaves margin before
the fire target.  Extra-center tab prep also gets bounded retries because a
transient prep timeout cost a whole center that morning.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from bookbot import booker


class TestWarmupSchedule:
    def test_normal_budget_keeps_lead_and_margin(self):
        sleep_s, cap_s = booker._warmup_schedule_s(300.0)
        assert sleep_s == pytest.approx(290.0)
        assert cap_s == pytest.approx(299.0)

    def test_sync_eroded_budget_skips_sleep(self):
        # 2026-09-13: the sync left only ~1.8s before the fire target.
        sleep_s, cap_s = booker._warmup_schedule_s(1.8)
        assert sleep_s == 0.0
        assert cap_s == pytest.approx(0.8)

    def test_already_late_still_gives_floor_cap(self):
        sleep_s, cap_s = booker._warmup_schedule_s(-5.0)
        assert sleep_s == 0.0
        assert cap_s == 0.5

    def test_cap_always_leaves_fire_margin(self):
        for remaining in (3.5, 8.0, 12.0, 600.0):
            sleep_s, cap_s = booker._warmup_schedule_s(remaining)
            assert sleep_s >= 0.0
            assert cap_s <= remaining - 1.0 + 1e-9
            assert cap_s >= 0.5


class TestRetryTabPrep:
    def test_succeeds_first_attempt_no_retry(self):
        calls = []

        async def make():
            calls.append(1)
            return "tab-a"

        result = asyncio.run(booker._retry_tab_prep(make, attempts=3, delay_s=0.0))
        assert result == "tab-a"
        assert len(calls) == 1

    def test_recovers_after_transient_failures(self):
        calls = []
        retries = []

        async def make():
            calls.append(1)
            if len(calls) < 3:
                raise TimeoutError("booking form controls not actionable")
            return "tab-b"

        result = asyncio.run(
            booker._retry_tab_prep(
                make,
                attempts=3,
                delay_s=0.0,
                on_retry=lambda attempt, exc: retries.append((attempt, type(exc).__name__)),
            )
        )
        assert result == "tab-b"
        assert len(calls) == 3
        assert retries == [(1, "TimeoutError"), (2, "TimeoutError")]

    def test_reraises_last_exception_after_exhaustion(self):
        calls = []

        async def make():
            calls.append(1)
            raise RuntimeError(f"boom {len(calls)}")

        with pytest.raises(RuntimeError, match="boom 3"):
            asyncio.run(booker._retry_tab_prep(make, attempts=3, delay_s=0.0))
        assert len(calls) == 3

    def test_delay_honored_between_attempts(self):
        async def make():
            raise ValueError("x")

        start = time.monotonic()
        with pytest.raises(ValueError):
            asyncio.run(booker._retry_tab_prep(make, attempts=3, delay_s=0.05))
        assert time.monotonic() - start >= 0.09
