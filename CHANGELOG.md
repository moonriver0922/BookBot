# Changelog

All notable review and optimization changes are recorded here.

## 2026-09-18

- Morning rush lost (`COMPETITION_LOSS`, run `20260918-080014-e953`, 5
  attempts for 2026-09-25) - but the new failure-evidence capture turned the
  post-mortem into a root cause instead of a guess.  The four "confirmation
  page never appeared" captures (`logs/booking_evidence/*-fail-confirm-missing*`)
  all show the site's actual confirm page ("Do you want to book the following
  facility? ... Submit / Back") rendered moments *after* the run gave up:
  the confirm-page wait budget (800 + 1500 + 1500 = 3.8s) is too tight when
  the congested server needs ~4-6s to render that page (today: API search rtt
  29.2s, first timetable +16.0s).  Bail timestamps vs capture timestamps show
  misses of ~0.2-2s per attempt.  Chain otherwise green
  (`actual_fire_delay_ms` 3.6ms, 0x403, API race 8/8 ok, keepalive 6), and
  both 09-17 additions worked live: failed-slot memory
  (`failed_slot_marked_count` 5 / `failed_slot_skip_count` 1 - wave 1 moved
  from the already-failed PH 09:30 cell to the untried PH 10:30-11:30) and
  the evidence capture itself (4 files).
- Confirm-wait budget: `rush_confirm_result_timeout_ms` 1500 -> 3500 (total
  wait 800 + 3500 + 3500 = 7.8s; the tight first budget still fast-paths
  healthy renders).  Defaults updated in `bookbot/config.py` + `booker.py`
  fallback; adaptive cap for the knob (500, 3000) -> (500, 6000); the run
  snapshot now records the value so future runs are self-verifying.
- Candidate bookkeeping: the api-search-hit lane's failure branch now closes
  a still-open candidate (`automation_failure`) - 09-18 attempt 2 left one
  'pending'.
- Tests: confirm-wait budget floor test (total >= 7s); suite 112 -> 113.

## 2026-09-17

- Morning rush lost (`COMPETITION_LOSS`, run `20260917-080012-25d7`, 4
  attempts for 2026-09-24, no bookings).  Attempt 1 (Shaw Sports Complex,
  via API search) died as `automation_failure` - the slot click never
  registered.  Attempts 2-4 (Sports Practice Hall, waves 2-4) selected the
  cell, armed Next and dispatched submits fine, but the server's
  confirmation page never rendered inside the wait window
  (`bot_latency_loss` x3).  The grid kept advertising 2 available cells for
  9/24 across re-scans, so every wave re-picked the same 09:30-10:30 cell;
  a read-only re-scan ~30 min later showed the date fully taken and My
  Record carries no 9/24 booking.  Execution chain was green:
  `actual_fire_delay_ms` 8.4ms, 0x403, API race 8/8 ok, all waves ran, no
  lane crashes.
- Failed-slot memory (same-run steering): `_note_failed_slots` remembers
  every cell that fails a booking attempt (keyed center + date +
  start-end); `_prefer_unfailed_slots` drops tried cells from later picks
  while an untried acceptable cell remains, and falls back to the tried
  cells when nothing else is left (retrying beats skipping - the 09-16 win
  came from re-trying the same slot).  Wired into the initial claim loop,
  the retry waves, and the API-search wave (`failed_slot_marked_count` /
  `failed_slot_skip_count` metrics); wave failures now also emit
  `booking_conflict` feedback with the wave number.
- Failure evidence capture: `_capture_failure_evidence` archives url +
  visible text (screenshot outside rush) under `logs/booking_evidence/`
  whenever the confirm page never appears, a conflict is detected, no
  confirm button is found, or a booking lane dies.  The 09-17 attribution
  ("likely beaten on the server") had to be guessed from network events;
  next time the server's own message is on disk (`failure_evidence_count` /
  `failure_evidence_last`).
- Scan logging lists the actual cells: "2 available slots
  [08:30-09:30, 09:30-10:30]" instead of just the count, so the next
  post-mortem can see which inventory was on screen.
- Tests: new `tests/test_rush_slot_blacklist.py` (21 cases; suite 91 -> 112
  passed) + live probe `tests/smoke_slot_memory.py` (steering, fallback,
  live + closed-page evidence capture: ALL PASS).

## 2026-09-16

- Morning rush WON on the crash-resilience build (`dac01da`): 9/23 Wed
  09:30-10:30 @ Shaw Sports Practice Hall, wave 2.  The mid-run flow hit
  `confirmation_page_too_slow` twice - both lanes degraded cleanly (one
  bailed as `bot_latency_loss`, the winner ticked its checkbox via the new
  helper and confirmed) instead of killing the attempt.  Server was slow
  again (first timetable +18.9s; Shaw-main API searches hung 4/4 for ~48s).
- Booking evidence: on success the confirmation page is archived as a
  screenshot + body text under `logs/booking_evidence/` (`booking_evidence`
  metric).  Motivation: the site's confirmation email can be delayed or
  missing, and a missing email made a real win look like a failure.
- Tests: `TestBookingEvidence` plus the crash-path regression now asserts
  the receipt is archived on a successful booking.

## 2026-09-15

- Crash resilience (mid-rush attempt killer): during the 08:30 rush a page
  navigation destroyed the JS context under the confirm-page checkbox
  evaluate; the exception bubbled from `book_slots` through the wave loop
  and aborted the whole attempt at +72s, so the late recovery waves (the
  09-14 safety net) never ran.  The 5s-later retry attempt had to re-fire
  ~105s late with one center skipped due to the deadline.  Fixes:
  - `_safe_evaluate`: retries when the JS context is lost to a navigation
    (waits for the page to settle between attempts) instead of raising;
    non-navigation errors still raise.
  - `_tick_confirm_checkboxes`: the rush checkbox tick falls back to
    Playwright locator clicks (which re-resolve after navigations) when the
    JS context is lost, and never raises.
  - `_book_slots_guarded`: every rush booking-lane call site is now wrapped
    — a lane fails locally (`lane_exception` feedback + candidate closed as
    `automation_failure`) while the wave loop keeps going; session-level
    errors (maintenance / form-not-ready) still propagate to the attempt
    handler.
  - Confirm-page grace window: when the first (tight) budget misses, keep
    waiting for the real confirm controls for one grace window
    (`rush_confirm_result_timeout_ms`) before the bare load-state fallback;
    `confirmation_page_seen` can now be marked inside the grace window.
  - First-occurrence metrics (`actual_fire_delay_ms`,
    `primary_boundary_offset_ms`, `refresh_to_first_candidate_ms`,
    `first_candidate_source`) are no longer overwritten by retry attempts —
    the 09-15 report showed `actual_fire_delay_ms=104875.6` (attempt 2)
    while the first fire was actually on time (-196ms).
- Tests: `tests/test_rush_crash_resilience.py` — safe-evaluate retries,
  checkbox fallback, lane isolation, confirm-page grace, sticky metrics,
  and an end-to-end regression of the exact 09-15 crash path (tick loses
  its context → flow still completes the booking).

## 2026-09-14

- CSRF token freshness (root cause of the run's ~59s search blackout): the
  POSS booking page freezes `CSRFToken: getCSRFToken()` into the Search click
  handler at page load, so the 08:00-prepared form held a token the server
  stopped accepting.  Every search - both browser tabs and the httpx API race
  (which reused the prep-time token) - returned 403 until a retry wave
  rebuilt the page and re-bound a fresh token.  Live off-peak probe: fresh
  token -> 200, dead token -> 403, a page re-render returns the current
  token, and the HTTP layer itself is not blocked (httpx works with a fresh
  token).
- Pre-fire form refresh: both booking tabs are re-rendered at fire-150s
  (`rush_form_refresh_before_s`, bounded budget, never delays the fire) so
  the fire runs with a fresh token binding; the API race now reads the
  CSRFToken from the tab at wave time instead of the prep-time snapshot.
- 403 heal: the first search 403 triggers an immediate rebuild + refire
  (max 2 rounds, 8s cooldown) instead of waiting ~59s for the retry waves
  (`search_403_count`, `search_403_heal_count` / `_refire` / `_fail`;
  the 2026-09-14 run recovered only at +59s through this path by accident).
- Scan hardening: `scan_available_slots_multi` tolerates a tab being
  navigated mid-scan by a concurrent rebuild.
- Form rebuild reliability: `_open_sports_facility_panel` retries the
  Sports Facility toggle (bounded) with a JS-click fallback.  A single click
  right after a page load can silently no-op while the page JS is still
  initializing; rebuilds then stalled ~20s and failed (found while
  validating the refresh/heal paths against the live site, fixed and
  re-verified live: rebuild + refire now succeeds from both the pre-fire and
  post-search states).
- Tests: token freshness suite `tests/test_token_freshness.py` (refresh plan
  bounds, form rebuild sequence, tab token read, heal round, API 403 retry).

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
- Keep the POSS session warm during long pre-open waits: the wait before the
  rush is chunked with a lightweight HEAD ping per tab every ~4 min
  (`keepalive_ping_count`; a login redirect raises
  `keepalive_session_suspect_count`). Local ops: LaunchAgent start moved
  08:18 -> 08:00 and the `caffeinate` wrapper window widened to `-t 2700` so
  the rush window stays covered.
- Endgame ("last 100m") hardening: the 2026-09-12 post-mortem showed the
  ~3.7s submission stall was dominated by the site's own selection-validation
  round trip (~3s); on our side the poll-based Next retries wasted that window
  and the JS/keyboard fallbacks could report success without a real click.
  Next is now clicked by an in-page MutationObserver within ms of the stable
  re-enable, verified against a submit-path request/navigation, re-armed once
  on a fake-enable flash, and fallbacks are honest (`next_click_via`,
  `next_click_wait_ms`, `next_click_enables`, `next_click_rearmed`,
  `rush_next_click_timeout_ms`).
- Wire-level timeline events from the Playwright listener
  (`prepare_request_seen` / `prepare_response_seen` / `submit_request_seen` /
  `submit_response_seen`, plus `*_request_seen_count` metrics); the post-Next
  surrender now waits up to `rush_confirm_result_timeout_ms` (1500ms) for the
  page instead of `rush_confirm_page_timeout_ms` (800ms).
- Tests: warm-up schedule + tab-prep retry + keepalive unit tests
  (`tests/test_rush_fire_timing.py`); armed Next click + network-event
  classification (`tests/test_rush_endgame.py`); observer smoke script
  (`tests/smoke_arm_next.py`).

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
