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
- 09-17 loss mode: attempts can end with the server never rendering the
  confirmation page (`bot_latency_loss`) and no ground truth of what the
  server returned.  Failure evidence now dumps the rendered page
  (`logs/booking_evidence/*-fail-*.txt`) so the next such loss is
  self-explaining.

## Next Recommended Steps

1. Next 08:30 rush: collect one complete war report chain (probes → source → latencies → result).
2. Inspect `first_candidate_source` (API vs UI) before enabling `submit_canary`.
3. After >=5 rush samples, run `adaptive-report` / daily review `--auto-tune`.
4. 09-18 rush: watch `failed_slot_skip_count` and wave `booking_conflict`
   feedback; on another confirm-page miss read the newest
   `logs/booking_evidence/*-fail-confirm-missing*.txt` first.

## Recent Stage History

## 2026-09-18 — Confirm-wait budget (4x "confirm page never appeared" was OUR window, not the server)

### Completed

- Morning run `20260918-080014-e953` FAILED — `COMPETITION_LOSS`, 5 attempts
  on 2026-09-25, no bookings; now with a proven root cause thanks to the new
  failure-evidence captures:
  - attempts 1/3/4/5 all died `bot_latency_loss` after `confirmation_page_too_slow`
    (800ms fast budget) + both grace/load-state stages expired at 3.8s total;
  - the four `fail-confirm-missing` captures show the server's actual confirm
    page ("Do you want to book the following facility? ... Submit/Back") for
    the exact slot each attempt had selected — rendered moments after the
    bail (attempt 3's PH Court 7 10:30-11:30 page was fully readable at
    bail+~0.1s; attempt 1's capture could not even read the body yet);
  - under today's load (API search rtt 29.2s; first timetable +16.0s; confirm
    pages ~4-6s to render) the 3.8s window consistently closed ~0.2-2s before
    the page arrived.  Substantively not a competition loss: the confirm
    pages were live offers we never reached tick+Submit on.
  - attempt 2 (api-search-hit lane, Main 09:30) died at slot selection and
    left its candidate 'pending' (bookkeeping fixed below).
  - both 09-17 additions worked live: failed-slot memory (marked 5, skipped 1
    — wave 1 moved from failed PH 09:30 to untried PH 10:30-11:30) and the
    evidence capture (4 files).
  - Chain green: fire 3.6ms, 0x403, API race 8/8 ok, warmup 2/2, keepalive 6.
- Fix: `rush_confirm_result_timeout_ms` 1500 -> 3500 (total 7.8s = 800 +
  2x3500; the tight first budget is unchanged for fast renders).  Live +
  example configs, `config.py` defaults (dict + dataclass), `booker.py`
  fallback, adaptive cap high 3000 -> 6000, and the run snapshot (now records
  the value) all updated.
- Candidate bookkeeping: the api-search-hit failure branch finishes a
  still-open candidate as `automation_failure`.

### Validation

- `pytest`: 113/113 (new confirm-wait budget floor test).
- Live smoke `tests/smoke_crash_resilience.py` re-run: A1/A2 context-loss
  survival, checkbox tick (1 on live page, 0 during nav race, no raises),
  confirm-wait returns without hanging — all pass.
- Evidence review: the four 09-18 captures read and matched to their attempt
  slots (PH 10:30-11:30, PH 09:30-10:30, Main 09:30-10:30).

### Git

- Branch: `feature/confirm-wait-budget`
- Commit: `4dfe7d5`
- Push status: pushed

## 2026-09-17 — Failed-slot memory + failure evidence (09-17 loss analysis)

### Completed

- Morning run `20260917-080012-25d7` FAILED — `COMPETITION_LOSS`, 4 attempts
  for 2026-09-24, no bookings:
  - attempt 1 (Shaw Sports Complex, via API search race): slot click never
    registered → `automation_failure`;
  - attempts 2-4 (Sports Practice Hall, waves 2-4): cell selected, armed
    Next clicked, submits dispatched — but the server's confirmation page
    never rendered inside the wait window → `bot_latency_loss` x3.
  - The grid kept advertising 2 available cells for 9/24 across re-scans,
    so every wave re-picked the same 09:30-10:30 cell.  A read-only re-scan
    ~30 min later showed the date fully taken; My Record shows no 9/24
    booking (no phantom success).
  - Chain green: `actual_fire_delay_ms` 8.4ms, 0x403, API race 8/8 ok,
    warmup 2/2, keepalive 6, all waves ran, no lane crashes.
- Failed-slot memory: `_note_failed_slots` / `_prefer_unfailed_slots` /
  `_choose_rush_slots` steer later picks to untried cells (keyed center +
  date + start-end), with an honest fallback to tried cells when no
  acceptable alternative remains.  Wired into the initial claim loop, the
  retry waves, and the API-search wave; metrics `failed_slot_marked_count`
  / `failed_slot_skip_count`; wave failures now emit `booking_conflict`
  feedback with the wave number.
- Failure evidence capture: `_capture_failure_evidence` archives url +
  visible text (+screenshot outside rush) under `logs/booking_evidence/`
  for confirm-missing / conflict / no-confirm-button / lane-exception
  failures (`failure_evidence_count` / `failure_evidence_last`).
- Scan logs now list the actual available cells per date instead of only
  the count.

### Validation

- `pytest`: 112/112 (new `tests/test_rush_slot_blacklist.py`, 21 cases).
- Live probe `tests/smoke_slot_memory.py`: ALL PASS — steering drops a
  seeded cell in favour of a real alternative, single-cell fallback still
  retries, live evidence capture wrote the real page, closed-page capture
  degrades gracefully (TargetClosedError recorded, no raise).
- Read-only: live re-scan of 9/24 after the loss (fully taken); My Record
  check via `tests/smoke_verify_booking.py` (no phantom booking).

### Git

- Branch: `feature/rush-failed-slot-memory`
- Commit: `0227708`
- Push status: pushed

## 2026-09-16 — Live win #2 + booking evidence archive

### Completed

- Morning run `20260916-080012-0111` SUCCESS on the crash-resilience build
  (`dac01da`): booked 2026-09-23 (Wed) 09:30-10:30 @ Shaw Sports Complex -
  Sports Practice Hall (BMT07), wave 2, single $10 deduction.  Verified on
  the site's My Record ledger (16 Sep 08:31 AM, "Individual Booking
  (Confirmed)"; balance $43).
- The 09-15 crash-resilience work was exercised live: two
  `confirmation_page_too_slow` events — one lane bailed cleanly
  (`bot_latency_loss`), the winning lane ticked its checkbox (new helper)
  and confirmed at +81s.  Server again slow (first timetable +18.9s;
  Shaw-main API searches hung ~48s, 4/4).
- Booking evidence archive: the success path saves a confirmation screenshot
  + body text under `logs/booking_evidence/` and records a `booking_evidence`
  metric, so wins have a local receipt independent of the (occasionally
  missing) confirmation email.
- Hermes-side companion: a daily (08:40) Discord morning report posts the
  result to the bookbot thread, attaching the evidence screenshot on wins.

### Validation

- `pytest`: 91/91 (`TestBookingEvidence` for the archive helper incl. the
  screenshot-failure path; the crash regression now asserts the receipt is
  archived on success).
- Live: booking verified against the site's My Record ledger;
  `tests/smoke_verify_booking.py` committed for future checks.

### Git

- Branch: `feature/rush-crash-resilience`
- Commit: `9f70a82`
- Push status: pushed

## 2026-09-15 — Rush crash containment (lane isolation + nav-race tolerant evaluates)

### Completed

- Morning run `20260915-080010-7aa1` FAILED — `COMPETITION_LOSS`
  (booking_conflict).  All 09-14 fixes validated live: zero 403s
  (`form_refresh_ok_count=2`), API race alive (`api_search_ok_count=8`,
  first candidate sourced from it), fire on time (-196ms), warmup 2/2,
  keepalive 6 pings, endgame clicks 13ms / armed wait 169ms.
  - Server was the worst yet: first search response +17.6s, Shaw-main API
    searches hung 48s with no payload (4/4), repeated "Could not parse
    timetable structure" pages.
  - The 09-22 09:30-10:30 slot: seen at +18.8s → submitted 540ms later →
    conflict.  3 submit attempts total, all conflicts; by +118s no
    acceptable slot was visible any more — the inventory went in ~2min.
  - Crash: at +72s a page navigation destroyed the JS context under the
    confirm-page checkbox evaluate and the exception aborted the ENTIRE
    attempt; the 5s-later retry re-fired ~105s late with one center skipped
    (deadline) and found nothing.  The lost 54s tail is exactly where the
    09-14 comeback wave won.
- Fixes (branch `feature/rush-crash-resilience`):
  - `_safe_evaluate` — retries evaluates that lose their context to a
    navigation (page settles between attempts); non-navigation errors still
    raise so real bugs stay visible.
  - `_tick_confirm_checkboxes` — checkbox tick with an honest locator-click
    fallback; never raises.
  - `_book_slots_guarded` — every rush lane call site wrapped: a lane fails
    locally (`lane_exception` feedback, candidate closed as
    `automation_failure`) while the wave loop keeps going; maintenance /
    form-not-ready errors still propagate to the attempt handler.
  - Confirmation-page grace window before the bare load-state fallback;
    `confirmation_page_seen` can now be marked inside the grace window.
  - First-occurrence metrics sticky (`actual_fire_delay_ms`,
    `primary_boundary_offset_ms`, `refresh_to_first_candidate_ms`,
    `first_candidate_source`) — retry attempts no longer overwrite the first
    fire's numbers (the 09-15 report showed `actual_fire_delay_ms=104875.6`
    from attempt 2 while attempt 1 fired on time).

### Validation

- `pytest`: 89/89 (new `tests/test_rush_crash_resilience.py`, 17 cases,
  including an end-to-end regression of the exact crash path).
- Live probe against the real site (off-peak): deterministic reproduction of
  the crash class — raw `page.evaluate` RAISED "Execution context was
  destroyed, most likely because of a navigation" while `_safe_evaluate` on
  the same destruction returned 'SURVIVED'; checkbox tick on a live page
  returned 1/0 without raising (twice, incl. during a nav race);
  confirm-page wait returned its fallback verdict without hanging.

### Git

- Branch: `feature/rush-crash-resilience`
- Commit: `6b82d4f`
- Push status: pushed

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
