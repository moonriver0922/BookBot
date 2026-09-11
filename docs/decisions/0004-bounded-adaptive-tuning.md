# Decision: Bounded adaptive rush tuning

## Context

Manual one-variable experiments are required, but once enough rush samples
exist the bot should propose capped parameter updates instead of stagnating.

## Decision

1. Compute adaptive recommendations from runtime/feedback history.
2. Hard-cap pre-fire, boundary probe count, and rush timeouts.
3. Expose `bookbot adaptive-report`.
4. Wire recommendations into daily review auto-tuning via `auto_tuning.yaml`
   using explicit `set` operations (never unbounded increments).
5. Require a minimum rush-run sample size before applying adaptive changes.

## Consequences

- Timing and budgets can improve automatically without flooding Search.
- Center priority can shift from booked/conflict outcomes.
- Unsafe request-frequency growth remains blocked by probe caps.
