# PROJECT_STATUS

## Current Goal

Maximize PolyU badminton rush booking success for any acceptable slot at/after
09:30 during the 08:30 open window. Prefer success over slot quality.

## Implemented

- Rush first-acceptable selection (`find_rush_booking`)
- Rush default `prefer_consecutive=1` via `settings.rush_prefer_consecutive`
- Center race with booking claim lock
- Early return from scan when first acceptable slot is visible
- Short rush timeouts for slot→Next→confirm path
- Observability: `run_id`, timeline, candidate events, network RTT, failure taxonomy, war reports
- Analyze/rollout extensions for competition-loss funnel and experiment IDs
- P2 open-boundary probes (`rush_boundary_offsets_ms`) with cancel-on-inventory
- `bookbot timing-report` offset recommendation from historical runs
- P3 API Search race in rush hybrid (`api.rush_search_race`) with UI submit default
- Optional API submit canary (`api.submit_canary`)
- P4 bounded adaptive recommendations + daily-review auto-tune wiring
- Daily review performance-budget alerts (competition_loss / candidate→confirm P90)
- Failure evidence hierarchy: explicit conflict vs automation vs `POSSIBLE_COMPETITION_LOSS`
- Per-weekday rush windows via `preferences.weekday_time_ranges`

## Partially Implemented

- Network telemetry (response listener; timing quality depends on Playwright)
- API submit canary (implemented behind flag; needs live validation)
- Timetable JSON parser (best-effort; schema must be confirmed on live responses)

## Missing

- Performance budget alerting dashboards beyond review text
- Hardened API-only rush path after canary proves stable
- Tighten soft budgets toward final rush targets after 5–10 live samples

## Known Issues

- `config.example.yaml` keeps safe defaults (`booking_mode=ui`, `api.enabled=false`);
  live gitignored `config.yaml` must carry hybrid + API race for the next experiment.
- Smoke/report artifacts under `logs/` are local-only and should not be committed.
- HTTPS `gh` PAT cannot create PRs; use SSH for git push.
- API JSON field names are inferred; first live runs should inspect `api_search_*` metrics.
- Open-time timetable responses can take >10s. `api.request_timeout_ms` was
  raised 2500→15000 (2026-09-13) so the API race survives the open burst;
  watch `api_search_ok_count` / `api_search_fail_count` on the next runs.
- The pre-open warm-up used to be able to delay the fire (the sync ate its
  budget, then a slow warm-up stretched it — 2026-09-13 fired ~4.2s late). It
  is now scheduled after the sync and hard-capped (`warmup_timeout_count`);
  `actual_fire_delay_ms` should stay near 0 on the next runs.
- POSS disables Search while a request is in flight; UI probes/re-clicks cannot
  add parallel searches during that window (the API race is the parallel channel).
- Adaptive tuning needs >=5 rush runs before it becomes ready.
- Soft review budgets (e.g. candidate→confirm P90 2000ms) are monitoring thresholds,
  not the final beat-human targets (aim ~500–800ms once stable).

## Next Recommended Steps

1. Next 08:30 rush: collect one complete war report chain (probes → source → latencies → result).
2. Inspect `first_candidate_source` (API vs UI) before enabling `submit_canary`.
3. After >=5 rush samples, run `adaptive-report` / daily review `--auto-tune`.

## Recent Stage History

## 2026-09-14 (evening) — CSRF token freshness (403 blackout fix)

### Completed

- Forensics on the morning's win: the run succeeded despite a ~59s search
  blackout.  The POSS booking page freezes `CSRFToken: getCSRFToken()` into
  the Search click handler at page load, so the 08:00-prepared form held a
  token the server had stopped accepting: every search - both browser tabs
  AND the httpx API race (which reused the prep-time snapshot) - returned 403
  until a retry wave rebuilt the page at +56s and re-bound a fresh token
  (+59.1s -> 200 -> booked).  (2026-09-12 worked only because the
  prep-to-fire gap was ~11 min.)
- Live off-peak probes: fresh token -> 200; dead token -> 403 (httpx is NOT
  blocked - the API channel never had a fingerprint problem); continuous use
  keeps the token alive 40+ min; idle 29.5 min did NOT die off-peak, so the
  production trigger is not fully pinned - the fix is trigger-agnostic.
- Fix, three layers + two hardenings:
  1. Pre-fire form refresh: both booking tabs are re-rendered at fire-150s
     (`rush_form_refresh_before_s`, bounded budget, never delays the fire).
  2. API race reads the tab's CSRFToken at wave time (not the prep snapshot)
     and retries once on 403 with a re-read/render token.
  3. 403 heal: the first search 403 triggers an immediate rebuild + refire
     (max 2 rounds, 8s cooldown) instead of a ~59s wait.
  4. `_open_sports_facility_panel`: bounded click retries + JS-click
     fallback - a single click after page load can silently no-op while the
     page JS initializes (rebuilds stalled ~20s and failed; found while
     validating live, re-verified live: rebuild + refire succeeds from both
     the pre-fire and post-search states).
  5. `scan_available_slots_multi` tolerates a mid-scan navigation.
- Tests: 72/72 (`tests/test_token_freshness.py`, 24 new cases).

### Git

- Branch: `feature/token-freshness`
- Commit: `565d362`
- Push status: pushed

## 2026-09-14 — Live validation: first win on the hardened build

### Completed

- Run `20260914-080007-7211` (git `6754dcb`) — **SUCCESS**: booked 2026-09-21
  09:30–10:30 @ Shaw Sports Complex (wave 3, relaxed=False).
- Fire timing held: `actual_fire_delay_ms=1.5` (vs 4.2s late on 2026-09-13);
  warmup ready 9.96s before open; keepalive 7 pings during the 28-min wait.
- Endgame (last-100m fix): slot seen -> booked in 351ms — `next_click_via=armed`,
  site validation round trip 48ms, submit response 21ms (vs 3.68s on 2026-09-12).
- Server slower than ever (first search response 8.7s, timetable parseable at
  43.4s, first acceptable slot at +59.5s); retry wave 3 caught it.
- New issue logged: API race channel returned 403 on all 8 attempts (UI carried
  the win; investigate session/anti-bot separately).

### Git

- Branch: `feature/rush-fire-timing-hardening`
- Build validated: `6754dcb`
- Push status: pushed

## 2026-09-13 — Endgame (last-100m) hardening

### Completed

- Wire-level post-mortem of the 2026-09-12 loss: the network listener shows the
  site's own slot-validation round trip (make_book.do -> 302) took ~3.1s before
  Next re-enabled; the real submit (make_book_submit.do -> 200) left at
  ~T+15.1s and came back a conflict. Our poll-based Next retries burned that
  window and the JS/keyboard fallbacks could report success without a real
  click.
- `_click_next_fast` rewritten as an armed, event-driven click: an in-page
  MutationObserver clicks within ms of the stable re-enable, the click is
  verified against a submit-path request / navigation, re-armed once on a
  fake-enable flash, and fallbacks never report success without a dispatched
  click (`next_click_via`, `next_click_wait_ms`, `next_click_enables`,
  `next_click_rearmed`, `rush_next_click_timeout_ms`).
- Wire instrumentation: `prepare_request_seen` / `prepare_response_seen` /
  `submit_request_seen` / `submit_response_seen` timeline events plus
  `*_request_seen_count` metrics; post-Next surrender now waits up to
  `rush_confirm_result_timeout_ms` (1500ms) for the page.
- Smoke testing caught and fixed an effect-snapshot bug (a click whose effect
  landed before the poll started was misread as "no effect", triggering a
  needless re-arm + extra click).

### Changed Files

- `bookbot/booker.py`
- `bookbot/config.py`
- `config.example.yaml`
- `tests/test_rush_endgame.py`
- `tests/smoke_arm_next.py`
- `CHANGELOG.md`

### Validation

- Command: `.venv/bin/python -m pytest tests/ -q`
- Result: passed (48 tests)
- Notes: headless-chromium smoke (delayed / immediate / ignored-first-click
  re-arm / never-enabled) passes; the re-arm path clicks exactly twice (one
  ignored + one landing).

### Follow-Up Items

- Next rush: read `next_click_wait_ms` / `next_click_via` / `next_click_enables`
  to calibrate the stability gate against real enable->click latency.
- Consider the API submit canary (skips the UI validation chain) once payloads
  are validated against live responses.

### Git

- Branch: `feature/rush-fire-timing-hardening`
- Commit: `84d8c12`
- Push status: pushed

## 2026-09-13 — Fire-timing hardening (never fire late)

### Completed

- 2026-09-13 rush fired ~4.2s late (`actual_fire_delay_ms` 4197.7): the warm-up
  budget was measured before the 3.4s server-time sync and the slow pre-open
  server stretched the warm-up to 5.6s; all probes/searches fired at T+4s and
  no timetable data ever arrived (classified `NO_INVENTORY`, 0 slots seen).
- Warm-up schedule is recomputed after the sync and anchored to the first
  fire offset; the warm-up runs under `asyncio.wait_for` with a >=1s fire
  margin and is abandoned on overrun (`warmup_timeout_count`,
  `warmup_completed_offset_ms`).
- Extra-center tab prep retries up to 3 times with a fresh tab
  (`prep_tab_retry_count`): 2026-09-13 lost the Sports Practice Hall tab to a
  transient prep timeout.
- API Search failures are logged with status/error (were silent counters);
  `record_network` gets real start/finish timestamps (rtt was 0.0);
  `api_search_ok_count` added.
- Config: `api.request_timeout_ms` 2500 -> 15000 (example + live) — all four
  API waves timed out on 2026-09-13 vs ~10s+ server latency.

- Local ops: LaunchAgent start moved 08:18 -> 08:00 (more prep margin); the
  `caffeinate` window widened 1200 -> 2700s so the earlier start cannot leave
  the rush uncovered; machine runs `sleep 0` (no idle sleep); job reloaded and
  verified (`Minute=0`, `Hour=8`).
- Keepalive for the longer pre-open wait: the pre-fire wait is chunked with a
  lightweight HEAD ping per tab every ~4 min (`keepalive_ping_count`; login
  redirect -> `keepalive_session_suspect_count`) so the ~28-min idle window
  cannot expire the POSS session before the rush.

### Changed Files

- `bookbot/booker.py`
- `tests/test_rush_fire_timing.py`
- `config.example.yaml`
- `CHANGELOG.md`

### Validation

- Command: `.venv/bin/python -m pytest tests/ -q`
- Result: passed (34 tests)
- Notes: new unit tests cover the warm-up schedule math (sync-eroded budget,
  already-late start, fire margin) and tab-prep retries; the closure-scoping
  guard still passes.

### Follow-Up Items

- Next live rush: confirm `actual_fire_delay_ms` stays ~0 and
  `warmup_completed_offset_ms` is present; check `api_search_ok_count` /
  `api_search_fail_count` under the 15000ms timeout.
- Merge `feature/fix-rush-race-recovery` (still unmerged) together with this
  branch so both fix sets land on `main`.

### Git

- Branch: `feature/rush-fire-timing-hardening`
- Commits: `4608153` (fire timing), `7b41b83` (keepalive); docs `b4246b5`
- Push status: pushed

## 2026-09-12 — Rush race/retry recovery fixes

### Completed

- Fix API Search race crash: `booking_claimed` missing from `_api_search_wave`'s
  `nonlocal` declaration (every wave raised `UnboundLocalError` since P3).
- Cancelled sibling scans no longer abort the rush flow; task cleanup and
  Phase-5 retry waves now run after a failed claim
  (`sibling_scan_cancelled_count`).
- Conflict retry lane rebuilds the booking form on stale post-submit pages
  (`_refire_search_or_rebuild`); can rebook the next acceptable slot on the
  same date instead of failing to parse a result page.
- Probe/re-click honesty: blocked-tab diagnostics, real-dispatch counting,
  boundary-hit snap tolerance (`BOUNDARY_SNAP_TOLERANCE_MS`).
- Tests: closure-scoping guard (`tests/test_closure_scoping.py`); stale-page
  conflict recovery suite (`tests/test_conflict_retry.py`).

### Changed Files

- `bookbot/booker.py`
- `tests/test_closure_scoping.py`
- `tests/test_conflict_retry.py`
- `CHANGELOG.md`

### Validation

- Command: `.venv/bin/python -m pytest tests/ -q`
- Result: passed
- Notes: 26 tests. Extracted-function smoke run reproduced the pre-fix
  `UnboundLocalError` in `_api_search_wave`; post-fix version executes cleanly.
  Cancelled-sibling loop demo confirms cleanup/retry waves now run.

### Follow-Up Items

- Next live rush: confirm `api_search_wave_started` / `api_search_attempt_count`
  appear; compare `api_search_rtt_ms` with `api.request_timeout_ms` (2500ms).
- Consider not cancelling sibling scans at claim time so their data survives a
  failed booking attempt.
- Review `boundary_probe_blocked_count` distribution before deciding whether
  probe offsets stay worth firing.

### Git

- Branch: `feature/fix-rush-race-recovery`
- Commit: `9169f22`
- Push status: pushed

## 2026-09-11 — Weekday preference windows

### Completed

- `weekday_time_ranges` for per-day acceptance (Monday morning-only)
- Local live prefs: all 7 days, `min_slot_start=09:30`, Mon end `12:30`
- Re-enabled LaunchAgent `com.bookbot.polyu` (08:18 → wait for 08:30)

### Changed Files

- `bookbot/config.py`
- `bookbot/booker.py`
- `config.example.yaml`
- `tests/test_rush_framework.py`

### Validation

- Command: `python -m pytest tests/test_rush_framework.py -q`
- Result: passed
- Notes: 7 tests; launchctl lists `com.bookbot.polyu`

### Follow-Up Items

- Tomorrow live war report; confirm Monday afternoon is never claimed

### Git

- Branch: `feature/weekday-time-prefs` (merged to `main`)
- Commit: `ecbb043` / merge `784264a`
- Push status: pushed

## 2026-09-11 — Failure evidence hierarchy

### Completed

- Require explicit conflict for `COMPETITION_LOSS`
- Prefer automation / bot-latency reasons over inferred competition
- Add `POSSIBLE_COMPETITION_LOSS` for ambiguous candidate-seen losses
- Document soft budgets ≠ final rush targets

### Changed Files

- `bookbot/failures.py`
- `bookbot/tracker.py`
- `bookbot/budgets.py`
- `tests/test_rush_framework.py`
- `docs/decisions/0005-failure-evidence-hierarchy.md`

### Validation

- Command: `python -m pytest tests/test_rush_framework.py tests/test_budgets.py -q`
- Result: passed
- Notes: 11 tests

### Follow-Up Items

- Collect one high-quality live war report; pause large refactors

### Git

- Branch: `feature/fix-failure-evidence-hierarchy` (merged to `main`)
- Commit: `60344a3` / merge `197a6fa`
- Push status: pushed

## 2026-09-11 — Performance budget alerts (review)

### Completed

- Soft SLO budgets in `bookbot/budgets.py`
- Daily review highlights competition_loss_rate and candidate→confirm P90
- Warn/breach alerts when budgets exceeded

### Changed Files

- `bookbot/budgets.py`
- `bookbot/review.py`
- `tests/test_budgets.py`

### Validation

- Command: `python -m pytest tests/test_budgets.py tests/test_adaptive.py -q`
- Result: passed
- Notes: 6 tests

### Follow-Up Items

- Collect live rush samples under adaptive experiment
- Optional dashboards beyond review text

### Git

- Branch: `feature/rush-budget-alerts-p4b` (merged to `main`)
- Commit: `2ddc00f` / merge `d481e33`
- Push status: pushed

## 2026-09-11 — Bounded adaptive tuning (P4)

### Completed

- `bookbot/adaptive.py` with hard caps
- `adaptive-report` CLI
- Daily review adaptive section + auto_tune `set` actions
- Unit tests and ADR

### Changed Files

- `bookbot/adaptive.py`
- `bookbot/review.py`
- `bookbot/config.py`
- `run.py`
- `config.example.yaml`
- `tests/test_adaptive.py`
- `docs/decisions/0004-bounded-adaptive-tuning.md`

### Validation

- Command: `python -m pytest tests/test_adaptive.py tests/test_api_timetable.py tests/test_timing.py tests/test_rush_framework.py -q`
- Result: passed
- Notes: 13 tests

### Follow-Up Items

- Collect live rush samples
- Enable submit canary only after Search race validated

### Git

- Branch: `feature/rush-adaptive-p4` (merged to `main`)
- Commit: `7ebdfc1` / merge `202947e`
- Push status: pushed

## 2026-09-11 — API Search race in rush (P3)

### Completed

- Timetable JSON parser
- Rush hybrid API Search race with booking lock
- UI submit default + optional submit canary
- Local config switched to hybrid + api.enabled

### Changed Files

- `bookbot/api_timetable.py`
- `bookbot/api_client.py`
- `bookbot/booker.py`
- `bookbot/config.py`
- `bookbot/main.py`
- `config.example.yaml`
- `tests/test_api_timetable.py`
- `docs/decisions/0003-api-search-race.md`

### Validation

- Command: `python -m pytest tests/test_api_timetable.py tests/test_timing.py tests/test_rush_framework.py -q`
- Result: passed
- Notes: 11 tests

### Follow-Up Items

- Validate live JSON schema
- Enable submit canary carefully
- P4 adaptive tuning

### Git

- Branch: `feature/rush-api-hybrid-p3` (merged to `main`)
- Commit: `75ff76d` / main merge `95e022b`
- Push status: pushed via SSH
- Main: updated

## 2026-09-11 — Rush timing boundary probes (P2)

### Completed

- Signed boundary offsets with cancel-on-inventory
- Aligned rush T0 to official open
- timing-report recommendation CLI
- Unit tests and ADR

### Changed Files

- `bookbot/timing.py`
- `bookbot/booker.py`
- `bookbot/tracker.py`
- `bookbot/config.py`
- `bookbot/main.py`
- `run.py`
- `config.example.yaml`
- `tests/test_timing.py`
- `docs/decisions/0002-rush-boundary-probes.md`

### Validation

- Command: `python -m pytest tests/test_timing.py tests/test_rush_framework.py -q`
- Result: passed
- Notes: 9 tests

### Follow-Up Items

- Wire timing recommendation into daily review auto_tuning (bounded)
- P3 API hybrid rush

### Git

- Branch: `feature/rush-timing-p2` (merged to `main`)
- Commit: `f185ef1` / main merge `f762cad`
- Push status: pushed via SSH
- Main: updated

## 2026-09-11 — Rush performance framework (P0+P1)

### Completed

- Failure taxonomy module
- Tracker run_id / timeline / candidates / network / reports
- Rush first-acceptable path + booking lock + short timeouts
- Analyze and rollout funnel extensions
- Unit tests and ADR

### Changed Files

- `bookbot/failures.py`
- `bookbot/tracker.py`
- `bookbot/config.py`
- `bookbot/booker.py`
- `bookbot/main.py`
- `bookbot/analyze.py`
- `bookbot/rollout.py`
- `config.example.yaml`
- `tests/test_rush_framework.py`
- `docs/decisions/0001-rush-first-acceptable.md`
- `PROJECT_STATUS.md`
- `TODO.md`
- `CHANGELOG.md`

### Validation

- Command: `python -m pytest tests/test_rush_framework.py -q`
- Result: passed
- Notes: 5 unit tests

### Follow-Up Items

- Wire live config knobs
- Collect competition-window samples
- P2/P3 timing and API work

### Git

- Branch: `feature/rush-performance-framework` (also merged to `main`)
- Commit: `4181b15` (feature) / `f0e9711` (main merge)
- Push status: pushed via SSH; `origin` uses `git@github.com:moonriver0922/BookBot.git`
- Main: updated (`f0e9711`)
- PR status: skipped (PAT lacks `createPullRequest`); changes are already on `main`
