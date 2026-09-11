# Decision: API Search race with UI submit default

## Context

UI rush remains limited by DOM render. Protocol Search can discover inventory
earlier, but full API submit is still riskier (CSRF/form fields/facilityId).

## Decision

1. In rush hybrid mode, keep the UI multi-tab race as the booking authority.
2. When `api.enabled` and `api.rush_search_race`, race `timetable.json` Search
   across prepared centers at each boundary probe.
3. First acceptable API candidate claims the booking lock.
4. Default submit path is UI `book_slots` on the winning center tab.
5. `api.submit_canary` may attempt protocol prepare/submit first; on failure,
   fall back to UI once.

## Consequences

- Rush no longer skips API entirely under hybrid mode.
- JSON schema parsing is best-effort and must be validated against live payloads.
- API submit remains opt-in until canary metrics are stable.
