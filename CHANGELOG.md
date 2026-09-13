# Changelog

All notable review and optimization changes are recorded here.

## 2026-09-13

- Never fire late: recompute the warm-up schedule after the server-time sync
  and hard-cap the warm-up (`asyncio.wait_for`, >=1s fire margin). The
  2026-09-13 rush fired ~4.2s late because the sync ate the warm-up budget
  and the slow pre-open server stretched the warm-up to 5.6s
  (`warmup_timeout_count`, `warmup_completed_offset_ms`).
- Retry extra-center tab prep (3 attempts, fresh tab): a transient prep
  timeout cost the Sports Practice Hall tab on 2026-09-13
  (`prep_tab_retry_count`).
- Log API Search failures with status/error; record real network
  start/finish timestamps (rtt was stuck at 0.0) and add
  `api_search_ok_count`.
- Raise `api.request_timeout_ms` 2500 -> 15000 (example + live): open-time
  server latency exceeds the old timeout, so every API wave timed out on
  2026-09-13.
- Tests: warm-up schedule + tab-prep retry unit tests
  (`tests/test_rush_fire_timing.py`).

## 2026-09-12

- Fix `_api_search_wave` crash: `booking_claimed` was assigned inside the
  function but missing from its `nonlocal` declaration, so every API Search
  wave raised `UnboundLocalError` and the rush API Search race was silently
  dead. Add AST-based closure-scoping regression test
  (`tests/test_closure_scoping.py`).
- Keep the rush flow alive when a sibling center scan is cancelled after a
  claim: `asyncio.CancelledError` from `asyncio.as_completed` no longer
  aborts task cleanup and the Phase-5 retry waves
  (`sibling_scan_cancelled_count`).
- Rebuild the booking form when conflict recovery finds a stale post-submit
  page (`_refire_search_or_rebuild`); the same-slot lane can rescan real
  inventory and book the next acceptable slot on the same date instead of
  failing with "Could not parse timetable structure".
- Probe/re-click honesty: boundary probes report blocked tabs
  (`boundary_probe_blocked_count`, `tabs_blocked`); `reclick_count` counts
  real Search dispatches; inventory seen far outside the probe window no
  longer snaps `boundary_hit_offset_ms` to the latest probe
  (`boundary_hit_out_of_window`, `inventory_first_seen_ms`).
- Add unit tests for stale-page conflict recovery
  (`tests/test_conflict_retry.py`).

## 2026-09-11

- Add per-weekday time windows (`weekday_time_ranges`); Monday morning-only
  while keeping all other days and `min_slot_start=09:30`.
- Fix failure classification evidence hierarchy (`POSSIBLE_COMPETITION_LOSS`;
  automation faults no longer inflate competition loss).
- Add performance-budget alerts to daily review (competition_loss + latency SLOs).
- Add bounded adaptive rush tuning (`adaptive-report` + daily review auto-tune caps).
- Add rush API Search race (hybrid) with UI submit default and optional submit canary.
- Add rush open-boundary probe window (`[-200,0,200,500]` ms) with cancel-on-inventory
  and `bookbot timing-report` for offset recommendation.
- Add rush performance framework: first-acceptable booking, booking race lock,
  short rush timeouts, and observability (`run_id`, timeline, candidate events,
  failure taxonomy, war reports).
- Extend analyze/rollout reports with competition-loss funnel metrics and
  `experiment_id` grouping.
