# TODO

## High Priority

- Validate live `timetable.json` parsing on the next rush (`first_candidate_source`, parsed slot counts)
- Keep `submit_canary: false` until Search race proves useful
- Review `logs/reports/*.txt` for API vs UI candidate latency

## Medium Priority

- One-variable enable of `api.submit_canary: true` after Search race is healthy
- Wire bounded timing recommendation into daily review / `auto_tuning.yaml`
- Expand daily review to include competition_loss_rate and candidate_to_confirm P90

## Low Priority

- P4: adaptive timeout/pre-fire recommendations with hard safety caps
- Extract rush race/telemetry helpers from `bookbot/booker.py`

## Research Questions

- Does API Search observe inventory before first timetable DOM?
- Which signed boundary offset maximizes inventory-open + success without raising probe count?
- What candidate→confirm budget is needed to beat typical human confirmation?

## Technical Debt

- `bookbot/booker.py` remains very large; consider extracting rush race/telemetry helpers
- Network RTT derivation from Playwright timing is approximate and should be validated against HAR
- HTTPS GitHub PAT lacks pull-request create scope; SSH is required for push
- API timetable parser is heuristic until a captured live payload is checked in as a fixture
