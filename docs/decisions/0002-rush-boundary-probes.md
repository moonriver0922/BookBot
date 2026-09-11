# Decision: Small open-boundary Search probe set

## Context

Single fire at a fixed `rush_pre_fire_ms` cannot absorb open-time and RTT
uncertainty. Flooding Search is unacceptable. We need a bounded probe window.

## Decision

1. Use signed boundary offsets relative to official open T0, default
   `[-200, 0, 200, 500]` ms, capped at 5 probes.
2. Align telemetry T0 to open even when the first probe fires early.
3. Cancel remaining probes as soon as an acceptable inventory candidate appears.
4. Recommend primary offset from historical `boundary_hit_offset_ms` via
   `bookbot timing-report`, without auto-raising request frequency.

## Consequences

- Open-window hit rate becomes measurable by offset.
- Legacy `rush_pre_fire_ms` remains as fallback when boundary mode is disabled.
- Further adaptive tuning must keep the probe count hard-capped.
