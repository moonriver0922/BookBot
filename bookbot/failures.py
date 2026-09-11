"""Failure taxonomy for rush/normal booking runs.

Maps flat feedback reasons into stable failure classes used for review
and experiment analysis.

Classification follows an evidence hierarchy so automation bugs are not
miscounted as competition losses.
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
    "POSSIBLE_COMPETITION_LOSS",
    "AUTOMATION_FAILURE",
    "SERVER_FAILURE",
    "SUCCESS",
    "UNKNOWN",
)

_EXPLICIT_COMPETITION_REASONS = frozenset(
    {
        "booking_conflict",
        "candidate_seen_then_conflict",
        "slot_taken_before_submit",
        "slot_taken_at_confirm",
    }
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
    "possible_competition_loss": "POSSIBLE_COMPETITION_LOSS",
    "candidate_seen_no_booking": "POSSIBLE_COMPETITION_LOSS",
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


def _saw_candidate(reasons: list[str], metrics: dict[str, Any]) -> bool:
    if isinstance(metrics.get("refresh_to_first_candidate_ms"), (int, float)):
        return True
    if bool(metrics.get("visible_slots_unbooked")):
        return True
    if any(r in _EXPLICIT_COMPETITION_REASONS for r in reasons):
        return True
    return False


def classify_run(
    *,
    success: bool,
    events: list[dict[str, Any]],
    metrics: dict[str, Any] | None = None,
) -> tuple[str, str]:
    """Classify a finished run.

    Returns ``(failure_class, primary_reason)``.

    Evidence hierarchy (strict):
      1. Explicit server conflict / occupied → COMPETITION_LOSS
      2. Explicit click / Next / Confirm / latency faults → AUTOMATION / BOT_LATENCY / …
      3. Candidate seen + no booking, no hard evidence → POSSIBLE_COMPETITION_LOSS
      4. No candidate → NO_INVENTORY when inventory signals present
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

    # 1) Explicit competition evidence only.
    for reason in reasons:
        if reason in _EXPLICIT_COMPETITION_REASONS:
            return "COMPETITION_LOSS", reason

    # 2) Prefer specific non-ambiguous classes from events.
    # COMPETITION_LOSS is already handled above; do not infer it here.
    priority = [
        "PREP_FAILURE",
        "SERVER_FAILURE",
        "TIMING_FAILURE",
        "SEARCH_FAILURE",
        "AUTOMATION_FAILURE",
        "BOT_LATENCY_LOSS",
        "NO_INVENTORY",
        "POSSIBLE_COMPETITION_LOSS",
    ]
    classified = [(classify_reason(r), r) for r in reasons]
    for wanted in priority:
        for cls, reason in classified:
            if cls == wanted:
                return cls, reason

    saw_candidate = _saw_candidate(reasons, metrics)

    # 3) Ambiguous: saw inventory but no booking and no stronger signal.
    if saw_candidate and "no_bookings_made" in reason_set:
        return "POSSIBLE_COMPETITION_LOSS", "candidate_seen_no_booking"

    if "no_slots" in reason_set and not saw_candidate:
        return "NO_INVENTORY", "no_slots"

    if "no_bookings_made" in reason_set and not saw_candidate:
        return "NO_INVENTORY", "no_slots_visible"

    if reasons:
        return classify_reason(reasons[0]), reasons[0]
    return "UNKNOWN", "unknown"
