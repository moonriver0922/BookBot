# Decision: Rush first-acceptable booking with observability-first iteration

## Context

BookBot's goal shifted from selecting an ideal badminton slot to winning the
08:30 competition window for any acceptable slot at/after 09:30. Existing code
still ranked for afternoon quality and consecutive pairs, waited for fuller
timetable readiness, and lacked joinable run/candidate telemetry.

## Decision

1. Treat observability as P0: every rush run gets `run_id`, timeline events,
   candidate events, network RTT samples, failure taxonomy, and a war report.
2. Treat UI rush fast path as P1: `first acceptable slot wins`, default
   `rush_prefer_consecutive=1`, center race with booking lock, and short
   rush timeouts with fast fallback.
3. Keep API hybrid as a later stage (P3); do not block UI rush improvements.

## Consequences

- Success rate and competition-loss diagnosis become measurable.
- Slot quality may decrease in rush mode by design.
- Experiment tags (`experiment_id`, `strategy_version`, `config_hash`) enable
  one-variable A/B validation through `rollout-report`.
