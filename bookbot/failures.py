"""Failure taxonomy for rush/normal booking runs.

Maps flat feedback reasons into stable failure classes used for review
and experiment analysis.
"""
from __future__ import annotations

from typing import Any

# Top-level classes from the performance plan.
FAILURE_CLASSES = (
    "PREP_FAILURE",
    "TIMING_FAILURE",
    "SEARCH_FAILURE",
    "NO_INVENTORY",
    "BOT_LATENCY_LOSS",
    "COMPETITION_LOSS",
    "AUTOMATION_FAILURE",
    "SERVER_FAILURE",
    "SUCCESS",
    "UNKNOWN",
)

_REASON_TO_CLASS: dict[str, str] = {
    "login_failed": "PREP_FAILURE",
    "navigation_failed": "PREP_FAILURE",
    "form_not_ready": "PREP_FAILURE",
    "center_not_ready": "PREP_FAILURE",
    "session_expired": "PREP_FAILURE",
    "no_preferred_days": "PREP_FAILURE",
    "quota_full": "PREP_FAILURE",
    "fired_too_early": "TIMING_FAILURE",
    "fired_too_late": "TIMING_FAILURE",
    "inventory_not_open": "TIMING_FAILURE",
    "clock_sync_uncertain": "TIMING_FAILURE",
    "search_timeout": "SEARCH_FAILURE",
    "timetable_not_rendered": "SEARCH_FAILURE",
    "search_server_error": "SEARCH_FAILURE",
    "tab_scan_failed": "SEARCH_FAILURE",
    "no_slots": "NO_INVENTORY",
    "no_slots_visible": "NO_INVENTORY",
    "no_slots_after_0930": "NO_INVENTORY",
    "slot_visible_but_click_late": "BOT_LATENCY_LOSS",
    "next_enable_too_slow": "BOT_LATENCY_LOSS",
    "confirmation_page_too_slow": "BOT_LATENCY_LOSS",
    "booking_conflict": "COMPETITION_LOSS",
    "candidate_seen_then_conflict": "COMPETITION_LOSS",
    "slot_taken_before_submit": "COMPETITION_LOSS",
    "slot_taken_at_confirm": "COMPETITION_LOSS",
    "slot_cell_not_found": "AUTOMATION_FAILURE",
    "js_click_not_registered": "AUTOMATION_FAILURE",
    "native_click_failed": "AUTOMATION_FAILURE",
    "next_not_found": "AUTOMATION_FAILURE",
    "confirm_not_found": "AUTOMATION_FAILURE",
    "booking_failed": "AUTOMATION_FAILURE",
    "exception": "AUTOMATION_FAILURE",
    "all_attempts_exhausted": "AUTOMATION_FAILURE",
    "api_step_failed": "AUTOMATION_FAILURE",
    "maintenance": "SERVER_FAILURE",
    "rate_limit": "SERVER_FAILURE",
    "access_denied": "SERVER_FAILURE",
    "server_5xx": "SERVER_FAILURE",
    "booked": "SUCCESS",
    "no_bookings_made": "UNKNOWN",
}


def classify_reason(reason: str) -> str:
    """Map a single feedback reason to a failure class."""
    return _REASON_TO_CLASS.get(str(reason), "UNKNOWN")


def classify_run(
    *,
    success: bool,
    events: list[dict[str, Any]],
    metrics: dict[str, Any] | None = None,
) -> tuple[str, str]:
    """Classify a finished run.

    Returns ``(failure_class, primary_reason)``.
    Prefers competition loss over no-inventory when a candidate was seen
    and later conflicted.
    """
    metrics = metrics or {}
    if success:
        return "SUCCESS", "booked"

    reasons = [
        str(e.get("reason", "unknown"))
        for e in events
        if isinstance(e, dict) and e.get("reason")
    ]
    reason_set = set(reasons)

    if "booked" in reason_set:
        return "SUCCESS", "booked"

    saw_candidate = (
        isinstance(metrics.get("refresh_to_first_candidate_ms"), (int, float))
        or any(r == "booking_conflict" for r in reasons)
        or bool(metrics.get("visible_slots_unbooked"))
    )

    if "booking_conflict" in reason_set or (
        saw_candidate and "no_bookings_made" in reason_set
    ):
        return "COMPETITION_LOSS", "booking_conflict" if "booking_conflict" in reason_set else "candidate_seen_then_conflict"

    if "no_slots" in reason_set and not saw_candidate:
        return "NO_INVENTORY", "no_slots"

    if "no_bookings_made" in reason_set and not saw_candidate:
        return "NO_INVENTORY", "no_slots_visible"

    # Prefer the most specific non-unknown class among events.
    priority = [
        "PREP_FAILURE",
        "SERVER_FAILURE",
        "TIMING_FAILURE",
        "SEARCH_FAILURE",
        "AUTOMATION_FAILURE",
        "BOT_LATENCY_LOSS",
        "COMPETITION_LOSS",
        "NO_INVENTORY",
    ]
    classified = [(classify_reason(r), r) for r in reasons]
    for wanted in priority:
        for cls, reason in classified:
            if cls == wanted:
                return cls, reason

    if reasons:
        return classify_reason(reasons[0]), reasons[0]
    return "UNKNOWN", "unknown"
