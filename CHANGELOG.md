# Changelog

All notable review and optimization changes are recorded here.

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
