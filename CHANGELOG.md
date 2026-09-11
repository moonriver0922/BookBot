# Changelog

All notable review and optimization changes are recorded here.

## 2026-09-11

- Add rush API Search race (hybrid) with UI submit default and optional submit canary.
- Add rush open-boundary probe window (`[-200,0,200,500]` ms) with cancel-on-inventory
  and `bookbot timing-report` for offset recommendation.
- Add rush performance framework: first-acceptable booking, booking race lock,
  short rush timeouts, and observability (`run_id`, timeline, candidate events,
  failure taxonomy, war reports).
- Extend analyze/rollout reports with competition-loss funnel metrics and
  `experiment_id` grouping.
