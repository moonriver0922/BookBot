# TODO

## High Priority

- Collect one complete live rush war report under `rush-adaptive-v1`
  (boundary probes → `first_candidate_source` → click/Next/confirm latencies → result)
- After >=5 rush runs, use `python run.py adaptive-report` and review auto-tune
- Validate live `timetable.json` parsing (`first_candidate_source`)

## Medium Priority

- One-variable enable of `api.submit_canary: true` after Search race is healthy
- Tighten soft budgets toward final targets (candidate→confirm <500–800ms) after samples
- Expand agent prompt with budget-alert context (optional)

## Low Priority

- Extract rush race/telemetry helpers from `bookbot/booker.py`
- Check in a redacted live timetable JSON fixture once captured
- Performance budget alerting dashboards beyond review text

## Research Questions

- Does API Search observe inventory before first timetable DOM?
- Which signed boundary offset maximizes inventory-open + success without raising probe count?
- Are adaptive timeout budgets reducing automation failures without increasing competition loss?

## Technical Debt

- `bookbot/booker.py` remains very large; consider extracting rush race/telemetry helpers
- Network RTT derivation from Playwright timing is approximate and should be validated against HAR
- HTTPS GitHub PAT lacks pull-request create scope; SSH is required for push
- API timetable parser is heuristic until a captured live payload is checked in as a fixture
