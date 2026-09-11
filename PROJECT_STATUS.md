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

## Partially Implemented

- Network telemetry (response listener; timing quality depends on Playwright)
- Pre-fire offset experimentation (knob exists; no auto optimizer yet)
- API hybrid rush path (scaffolded, still skipped in rush)

## Missing

- Adaptive pre-fire / probe offset optimizer (P4)
- API Search + Submit rush path (P3)
- Performance budget alerting in daily review

## Known Issues

- Live `config.yaml` may still carry old preference values; copy new keys from
  `config.example.yaml` when deploying the new strategy.
- Smoke/report artifacts under `logs/` are local-only and should not be committed.

## Next Recommended Steps

1. Copy new rush knobs into local `config.yaml` and tag `experiment_id`.
2. Collect 20–30 competitive rush windows and compare baseline vs new strategy.
3. Start P2 pre-fire offset A/B using `rush_pre_fire_ms`.
4. Resume API hybrid only after UI candidate→confirm P90 is inside budget.

## Recent Stage History

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

- Branch: `feature/rush-performance-framework`
- Commit: pending
- Push status: pending
