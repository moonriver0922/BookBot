from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import yaml

CONFIG_SEARCH_PATHS = [
    Path("config.yaml"),
    Path(__file__).resolve().parent.parent / "config.yaml",
]
AUTO_TUNING_SEARCH_PATHS = [
    Path("auto_tuning.yaml"),
    Path(__file__).resolve().parent.parent / "auto_tuning.yaml",
]

DEFAULTS = {
    "preferences": {
        "activity": "Badminton",
        "center": "Shaw Sports Complex",
        "centers": ["Shaw Sports Complex"],
        "preferred_days": [1, 2, 4],
        "time_range": {"start": "14:00", "end": "18:00"},
        "fallback_time_range": None,
        "slot_priority_starts": [],
        "book_days_ahead": 7,
        "prefer_consecutive": 2,
        "weekly_max_slots": 4,
    },
    "settings": {
        "headless": True,
        "timeout": 30000,
        "retry_count": 6,
        "retry_interval": 5,
        "rush_retry_waves": 3,
        "rush_pre_fire_ms": 0,
        "rush_timetable_first_wait_ms": 24000,
        "rush_timetable_retry_wait_ms": 16000,
        "rush_timetable_probe_ms": [1200, 3200, 6800],
        "rush_reclick_guard_ms": 1400,
        "rush_warmup_mode": "mixed",
        "rush_retry_offsets_s": [3, 8],
        "same_slot_retry_limit": 3,
        "same_slot_retry_budget_ms": 3000,
        "next_click_backoff_ms": [150, 300, 500],
    },
    "stealth": {
        "human_delay_min": 0.3,
        "human_delay_max": 1.5,
        "typing_delay_min": 50,
        "typing_delay_max": 150,
        "use_real_chrome": True,
    },
    "selectors": {
        "search_date": "#searchDate",
        "activity": "#actvId",
        "center": "#ctrId",
        "search_button": "#searchButton",
        "next_button": "#nextButton",
        "timetable": "table.tt-timetable",
        "sports_facility_button": 'a:has-text("Sports Facility"), button:has-text("Sports Facility")',
        "unavailable_class_markers": ["not-avail", "unavail", "closed"],
    },
    "api": {
        "enabled": False,
        "base_url": "https://www40.polyu.edu.hk",
        "search_endpoint": "",
        "submit_endpoint": "",
        "request_timeout_ms": 2500,
        "retry_count": 2,
    },
}


@dataclass
class TimeRange:
    start: str = "14:00"
    end: str = "18:00"

    @property
    def start_hour(self) -> float:
        h, m = self.start.split(":")
        return int(h) + int(m) / 60

    @property
    def end_hour(self) -> float:
        h, m = self.end.split(":")
        return int(h) + int(m) / 60


@dataclass
class Preferences:
    activity: str = "Badminton"
    center: str = "Shaw Sports Complex"
    centers: List[str] = field(default_factory=lambda: ["Shaw Sports Complex"])
    preferred_days: List[int] = field(default_factory=lambda: [1, 2, 4])
    time_range: TimeRange = field(default_factory=TimeRange)
    fallback_time_range: TimeRange | None = None
    slot_priority_starts: List[str] = field(default_factory=list)
    book_days_ahead: int = 7
    prefer_consecutive: int = 2
    weekly_max_slots: int = 4


@dataclass
class Settings:
    headless: bool = True
    timeout: int = 30000
    retry_count: int = 6
    retry_interval: int = 5
    rush_retry_waves: int = 3
    rush_pre_fire_ms: int = 0
    rush_timetable_first_wait_ms: int = 24000
    rush_timetable_retry_wait_ms: int = 16000
    rush_timetable_probe_ms: List[int] = field(default_factory=lambda: [1200, 3200, 6800])
    rush_reclick_guard_ms: int = 1400
    rush_warmup_mode: str = "mixed"
    rush_retry_offsets_s: List[int] = field(default_factory=lambda: [3, 8])
    same_slot_retry_limit: int = 3
    same_slot_retry_budget_ms: int = 3000
    next_click_backoff_ms: List[int] = field(default_factory=lambda: [150, 300, 500])
    booking_mode: str = "ui"
    rush_time_sync_enabled: bool = True
    rush_time_sync_samples: int = 5
    rush_time_sync_timeout_ms: int = 1500


@dataclass
class StealthConfig:
    human_delay_min: float = 0.3
    human_delay_max: float = 1.5
    typing_delay_min: int = 50
    typing_delay_max: int = 150
    use_real_chrome: bool = True


@dataclass
class Credentials:
    username: str = ""
    password: str = ""


@dataclass
class Selectors:
    search_date: str = "#searchDate"
    activity: str = "#actvId"
    center: str = "#ctrId"
    search_button: str = "#searchButton"
    next_button: str = "#nextButton"
    timetable: str = "table.tt-timetable"
    sports_facility_button: str = 'a:has-text("Sports Facility"), button:has-text("Sports Facility")'
    unavailable_class_markers: List[str] = field(default_factory=lambda: ["not-avail", "unavail", "closed"])


@dataclass
class ApiSettings:
    enabled: bool = False
    base_url: str = "https://www40.polyu.edu.hk"
    search_endpoint: str = ""
    submit_endpoint: str = ""
    request_timeout_ms: int = 2500
    retry_count: int = 2


@dataclass
class AppConfig:
    credentials: Credentials = field(default_factory=Credentials)
    preferences: Preferences = field(default_factory=Preferences)
    settings: Settings = field(default_factory=Settings)
    stealth: StealthConfig = field(default_factory=StealthConfig)
    selectors: Selectors = field(default_factory=Selectors)
    api: ApiSettings = field(default_factory=ApiSettings)


def _deep_merge(base: dict, override: dict) -> dict:
    merged = base.copy()
    for k, v in override.items():
        if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
            merged[k] = _deep_merge(merged[k], v)
        else:
            merged[k] = v
    return merged


def _find_config_file(explicit_path: str | None = None) -> Path:
    if explicit_path:
        p = Path(explicit_path)
        if p.is_file():
            return p
        raise FileNotFoundError(f"Config file not found: {explicit_path}")

    env_path = os.environ.get("BOOKBOT_CONFIG")
    if env_path:
        p = Path(env_path)
        if p.is_file():
            return p

    for candidate in CONFIG_SEARCH_PATHS:
        if candidate.is_file():
            return candidate

    raise FileNotFoundError(
        "No config.yaml found. Copy config.example.yaml to config.yaml and fill in your credentials."
    )


def load_config(path: str | None = None) -> AppConfig:
    config_path = _find_config_file(path)
    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    merged = _deep_merge(DEFAULTS, raw)
    for tuning_path in AUTO_TUNING_SEARCH_PATHS:
        if tuning_path.is_file():
            with open(tuning_path, "r", encoding="utf-8") as tf:
                tuning_raw = yaml.safe_load(tf) or {}
            if isinstance(tuning_raw, dict):
                merged = _deep_merge(merged, tuning_raw)
            break

    creds = merged.get("credentials", {})
    prefs = merged.get("preferences", {})
    sett = merged.get("settings", {})
    stl = merged.get("stealth", {})
    sels = merged.get("selectors", {})
    api = merged.get("api", {})

    tr = prefs.pop("time_range", {})
    time_range = TimeRange(**tr)

    ftr = prefs.pop("fallback_time_range", None)
    fallback_time_range = TimeRange(**ftr) if ftr else None

    # If user only set "center" (single) but not "centers" (list),
    # populate centers from center so the fallback logic works.
    if "centers" not in raw.get("preferences", {}) and "center" in prefs:
        prefs["centers"] = [prefs["center"]]

    return AppConfig(
        credentials=Credentials(**creds),
        preferences=Preferences(
            time_range=time_range,
            fallback_time_range=fallback_time_range,
            **prefs,
        ),
        settings=Settings(**sett),
        stealth=StealthConfig(**stl),
        selectors=Selectors(**sels),
        api=ApiSettings(**api),
    )
