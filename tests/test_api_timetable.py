"""Tests for timetable JSON parsing used by API Search race."""
from __future__ import annotations

from datetime import date

from bookbot.api_timetable import parse_timetable_payload


def test_parse_nested_available_slots():
    payload = {
        "result": {
            "slots": [
                {
                    "date": "18 Sep 2026",
                    "startTime": "08:30",
                    "endTime": "09:30",
                    "facilityId": "11",
                    "available": True,
                    "facilityName": "Court A",
                },
                {
                    "date": "18 Sep 2026",
                    "startTime": "09:30",
                    "endTime": "10:30",
                    "facilityId": "12",
                    "available": True,
                    "facilityName": "Court B",
                },
                {
                    "date": "18 Sep 2026",
                    "startTime": "10:30",
                    "endTime": "11:30",
                    "facilityId": "13",
                    "status": "FULL",
                },
            ]
        }
    }
    target = date(2026, 9, 18)
    parsed = parse_timetable_payload(
        payload,
        center_name="Shaw Sports Complex",
        target_dates=[target],
    )
    slots = parsed[target]
    assert [s.start for s in slots] == ["08:30", "09:30"]
    assert slots[1].facility_id == "12"
    assert slots[1].court == "Court B"


def test_parse_iso_date_and_list_root():
    payload = [
        {
            "bookingDate": "2026-09-19",
            "start": "15:30",
            "end": "16:30",
            "facility_id": 99,
            "status": "AVAILABLE",
        }
    ]
    target = date(2026, 9, 19)
    parsed = parse_timetable_payload(
        payload,
        center_name="Shaw",
        target_dates=[target],
    )
    assert len(parsed[target]) == 1
    assert parsed[target][0].facility_id == "99"
