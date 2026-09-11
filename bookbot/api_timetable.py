"""Parse POSS timetable JSON payloads into TimeSlot candidates.

The live schema can vary slightly; this module walks nested JSON and
extracts objects that look like bookable facility time ranges.
"""
from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from bookbot.booker import TimeSlot

_TIME_RE = re.compile(r"^(\d{1,2}):(\d{2})$")
_DATE_DISPLAY_RE = re.compile(
    r"(\d{1,2})\s+([A-Za-z]{3})\s+(\d{4})",
)
_DATE_ISO_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "y", "available", "avail", "open", "free", "ok"}:
            return True
        if text in {"0", "false", "no", "n", "unavailable", "full", "booked", "occupied", "closed"}:
            return False
    return None


def _normalize_hhmm(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    # Handle "09:30:00" / "9:30 AM"
    text = text.replace(".", ":")
    m = re.match(r"^(\d{1,2}):(\d{2})", text)
    if not m:
        return None
    hour = int(m.group(1))
    minute = int(m.group(2))
    if "pm" in text.lower() and hour < 12:
        hour += 12
    if "am" in text.lower() and hour == 12:
        hour = 0
    if hour > 23 or minute > 59:
        return None
    return f"{hour:02d}:{minute:02d}"


def _parse_date_value(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    m = _DATE_ISO_RE.match(text)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    m = _DATE_DISPLAY_RE.search(text)
    if m:
        try:
            return datetime.strptime(
                f"{int(m.group(1)):02d} {m.group(2)} {m.group(3)}",
                "%d %b %Y",
            ).date()
        except ValueError:
            return None
    for fmt in ("%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(text[:10], fmt).date()
        except ValueError:
            continue
    return None


def _looks_available(node: dict[str, Any]) -> bool:
    for key in (
        "available",
        "isAvailable",
        "avail",
        "bookable",
        "canBook",
        "open",
        "free",
    ):
        if key in node:
            parsed = _as_bool(node.get(key))
            if parsed is not None:
                return parsed
    status = str(node.get("status") or node.get("slotStatus") or node.get("state") or "").lower()
    if status:
        if any(tok in status for tok in ("avail", "open", "free", "ok", "empty")):
            return True
        if any(tok in status for tok in ("full", "book", "occup", "close", "unavail", "disable")):
            return False
    # Absence of negative markers: treat as available when we have times + facility.
    return True


def _extract_from_object(node: dict[str, Any]) -> dict[str, Any] | None:
    start = (
        _normalize_hhmm(node.get("startTime"))
        or _normalize_hhmm(node.get("start"))
        or _normalize_hhmm(node.get("beginTime"))
        or _normalize_hhmm(node.get("fromTime"))
    )
    end = (
        _normalize_hhmm(node.get("endTime"))
        or _normalize_hhmm(node.get("end"))
        or _normalize_hhmm(node.get("toTime"))
        or _normalize_hhmm(node.get("finishTime"))
    )
    if not start or not end:
        # Combined "09:30-10:30"
        for key in ("time", "timeRange", "slotTime", "period"):
            raw = node.get(key)
            if not isinstance(raw, str) or "-" not in raw:
                continue
            left, right = raw.split("-", 1)
            start = _normalize_hhmm(left)
            end = _normalize_hhmm(right)
            if start and end:
                break
    if not start or not end:
        return None

    facility_id = (
        node.get("facilityId")
        or node.get("facility_id")
        or node.get("facId")
        or node.get("courtId")
        or ""
    )
    facility_name = (
        node.get("facilityName")
        or node.get("facility_name")
        or node.get("courtName")
        or node.get("court")
        or node.get("name")
        or ""
    )
    slot_date = (
        _parse_date_value(node.get("date"))
        or _parse_date_value(node.get("bookingDate"))
        or _parse_date_value(node.get("searchDate"))
        or _parse_date_value(node.get("slotDate"))
        or _parse_date_value(node.get("day"))
    )
    return {
        "start": start,
        "end": end,
        "facility_id": str(facility_id).strip(),
        "facility_name": str(facility_name).strip(),
        "date": slot_date,
        "available": _looks_available(node),
    }


def iter_timetable_nodes(payload: Any):
    """Yield nested dict nodes from a JSON payload."""
    stack: list[Any] = [payload]
    seen: set[int] = set()
    while stack:
        cur = stack.pop()
        obj_id = id(cur)
        if obj_id in seen:
            continue
        seen.add(obj_id)
        if isinstance(cur, dict):
            yield cur
            stack.extend(cur.values())
        elif isinstance(cur, list):
            stack.extend(cur)


def parse_timetable_payload(
    payload: Any,
    *,
    center_name: str,
    target_dates: list[date] | None = None,
) -> dict[date, list[TimeSlot]]:
    """Convert timetable JSON into ``{date: [TimeSlot, ...]}``."""
    from bookbot.booker import TimeSlot

    wanted = set(target_dates or [])
    by_date: dict[date, list[TimeSlot]] = {}
    dedupe: set[tuple[str, str, str, str]] = set()

    for node in iter_timetable_nodes(payload):
        extracted = _extract_from_object(node)
        if not extracted or not extracted["available"]:
            continue
        slot_date = extracted["date"]
        if slot_date is None:
            if len(wanted) == 1:
                slot_date = next(iter(wanted))
            else:
                continue
        if wanted and slot_date not in wanted:
            continue
        key = (
            str(slot_date),
            extracted["start"],
            extracted["end"],
            extracted["facility_id"],
        )
        if key in dedupe:
            continue
        dedupe.add(key)
        by_date.setdefault(slot_date, []).append(
            TimeSlot(
                start=extracted["start"],
                end=extracted["end"],
                center=center_name,
                court=extracted["facility_name"],
                available=True,
                facility_id=extracted["facility_id"],
            )
        )

    for slots in by_date.values():
        slots.sort(key=lambda s: (s.start, s.end, s.facility_id))
    return by_date
