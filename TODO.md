# TODO

## High Priority

- Copy new rush knobs into local `config.yaml` (`min_slot_start`, `rush_prefer_consecutive`, timeouts, `experiment_id`)
- Collect 20–30 competitive rush windows and compare success vs baseline
- Review `logs/reports/*.txt` after each rush for candidate→confirm bottlenecks

## Medium Priority

- P2: pre-fire offset A/B (`-300..+200ms`) with telemetry-driven selection
- Expand daily review to include competition_loss_rate and candidate_to_confirm P90
- Add small bounded open-boundary probe set (`T-200/T/T+200/T+500`) with cancel-on-hit

## Low Priority

- P3: API Search replay + hybrid submit canary
- P4: adaptive timeout/pre-fire recommendations with hard safety caps

## Research Questions

- Which earliest DOM/network signal most reliably means “slot is clickable”?
- What candidate→confirm budget is needed to beat typical human confirmation?

## Technical Debt

- `bookbot/booker.py` remains very large; consider extracting rush race/telemetry helpers
- Network RTT derivation from Playwright timing is approximate and should be validated against HAR
