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

## Partially Implemented

- Network telemetry (response listener; timing quality depends on Playwright)
- Adaptive offset recommendation (report exists; auto_tuning write not yet wired)
- API submit canary (implemented behind flag; needs live validation)
- Timetable JSON parser (best-effort; schema must be confirmed on live responses)

## Missing

- P4: full adaptive timeout/pre-fire with safety caps in daily review
- Performance budget alerting in daily review
- Hardened API-only rush path after canary proves stable

## Known Issues

- Live `config.yaml` may still carry old preference values; copy new keys from
  `config.example.yaml` when deploying the new strategy.
- Smoke/report artifacts under `logs/` are local-only and should not be committed.
- HTTPS `gh` PAT cannot create PRs; use SSH for git push.
- API JSON field names are inferred; first live runs should inspect `api_search_*` metrics.

## Next Recommended Steps

1. Run next rush under `rush-api-search-race-v1` and inspect whether `first_candidate_source=api_search`.
2. If API Search parses zero slots, capture one live `timetable.json` and tighten the parser.
3. Only then enable `api.submit_canary: true` as a one-variable experiment.
4. P4 adaptive tuning after enough offset/API samples exist.

## Recent Stage History

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

- Branch: `feature/rush-api-hybrid-p3`
- Commit: pending
- Push status: pending

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
