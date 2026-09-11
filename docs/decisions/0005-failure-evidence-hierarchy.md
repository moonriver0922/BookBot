# ADR 0005: Evidence hierarchy for failure classification

## Status

Accepted

## Context

`classify_run()` previously treated `candidate_seen + no_bookings_made` as
`COMPETITION_LOSS`. That polluted competition metrics when UI steps failed
(`js_click_not_registered`, `next_not_found`, `confirm_not_found`).

## Decision

Require explicit conflict / occupied evidence for `COMPETITION_LOSS`. Prefer
automation and bot-latency classes when those reasons are present. Treat
ambiguous candidate-seen failures as `POSSIBLE_COMPETITION_LOSS`, which does
not inflate `competition_loss_rate`.

## Consequences

- Review competition rates become trustworthy for “human beat us” claims.
- Ambiguous losses stay visible without forcing a false competition diagnosis.
- Soft performance budgets remain monitoring thresholds, not final rush targets.
