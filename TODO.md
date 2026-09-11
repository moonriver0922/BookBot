# TODO

## High Priority

- Collect competitive rush windows under `rush-timing-boundary-v1`
- Review `python run.py timing-report` after enough samples and run one-variable offset A/B
- Review `logs/reports/*.txt` after each rush for candidate→confirm latency

## Medium Priority

- Wire bounded timing recommendation into daily review / `auto_tuning.yaml`
- P3: API Search replay + hybrid submit canary
- Expand daily review to include competition_loss_rate and candidate_to_confirm P90

## Low Priority

- P4: adaptive timeout/pre-fire recommendations with hard safety caps
- Extract rush race/telemetry helpers from `bookbot/booker.py`

## Research Questions

- Which earliest DOM/network signal most reliably means “slot is clickable”?
- Which signed boundary offset maximizes inventory-open + success without raising probe count?
- What candidate→confirm budget is needed to beat typical human confirmation?

## Technical Debt

- `bookbot/booker.py` remains very large; consider extracting rush race/telemetry helpers
- Network RTT derivation from Playwright timing is approximate and should be validated against HAR
- HTTPS GitHub PAT lacks pull-request create scope; SSH is required for push
