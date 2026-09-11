"""Performance budgets and alert evaluation for rush review.

Budgets are soft SLOs for daily review text. They do not change runtime
behavior; they surface regressions early.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from bookbot.analyze import AnalysisSummary


@dataclass(frozen=True)
class BudgetThreshold:
    key: str
    label: str
    limit: float
    unit: str
    higher_is_worse: bool = True


# Soft SLOs for the rush critical path. Tuned for review alerts, not hard fails.
DEFAULT_BUDGETS: tuple[BudgetThreshold, ...] = (
    BudgetThreshold("p90_open_to_candidate_ms", "P90 open→candidate", 1500.0, "ms"),
    BudgetThreshold("p90_candidate_to_confirm_ms", "P90 candidate→confirm", 2000.0, "ms"),
    BudgetThreshold("p90_candidate_to_submit_ms", "P90 candidate→submit", 2500.0, "ms"),
    BudgetThreshold("competition_loss_rate", "competition_loss_rate", 35.0, "%"),
    BudgetThreshold("automation_failure_rate", "automation_failure_rate", 20.0, "%"),
    BudgetThreshold("timetable_load_p90_s", "timetable_load P90", 8.0, "s"),
    BudgetThreshold("timetable_load_gt_8s_rate", "timetable_load >8s rate", 25.0, "%"),
)


@dataclass(frozen=True)
class BudgetAlert:
    key: str
    label: str
    value: float
    limit: float
    unit: str
    severity: str  # "warn" | "breach"

    def format_line(self) -> str:
        return (
            f"{self.severity}: {self.label}={self.value}{self.unit} "
            f"(budget {self.limit}{self.unit})"
        )


def _metric_value(summary: AnalysisSummary, key: str) -> float | None:
    value = getattr(summary, key, None)
    if isinstance(value, (int, float)):
        return float(value)
    return None


def evaluate_budgets(
    summary: AnalysisSummary,
    *,
    budgets: tuple[BudgetThreshold, ...] = DEFAULT_BUDGETS,
    min_runs: int = 3,
) -> list[BudgetAlert]:
    """Return alerts for metrics that exceed configured budgets.

    Requires ``min_runs`` so sparse windows do not spam false positives.
    """
    if summary.runs < min_runs:
        return []

    alerts: list[BudgetAlert] = []
    for budget in budgets:
        value = _metric_value(summary, budget.key)
        if value is None:
            continue
        if budget.higher_is_worse and value > budget.limit:
            # Soft warn until 1.5x budget, then breach.
            severity = "breach" if value >= budget.limit * 1.5 else "warn"
            alerts.append(
                BudgetAlert(
                    key=budget.key,
                    label=budget.label,
                    value=round(value, 1),
                    limit=budget.limit,
                    unit=budget.unit,
                    severity=severity,
                )
            )
    return alerts


def format_budget_section(
    summary: AnalysisSummary,
    *,
    days: int,
    alerts: list[BudgetAlert] | None = None,
) -> list[str]:
    """Build review lines highlighting key funnel metrics and budget alerts."""
    alerts = alerts if alerts is not None else evaluate_budgets(summary)

    def fmt_ms(value: float | None) -> str:
        return f"{value:.1f}ms" if value is not None else "n/a"

    def fmt_pct(value: float) -> str:
        return f"{value:.1f}%"

    lines = [
        f"- Performance window ({days}d, runs={summary.runs}):",
        f"  competition_loss_rate={fmt_pct(summary.competition_loss_rate)}",
        f"  automation_failure_rate={fmt_pct(summary.automation_failure_rate)}",
        f"  no_inventory_rate={fmt_pct(summary.no_inventory_rate)}",
        f"  P90 open→candidate={fmt_ms(summary.p90_open_to_candidate_ms)}",
        f"  P90 candidate→confirm={fmt_ms(summary.p90_candidate_to_confirm_ms)}",
        f"  P90 candidate→submit={fmt_ms(summary.p90_candidate_to_submit_ms)}",
    ]
    if not alerts:
        if summary.runs < 3:
            lines.append("- Performance budgets: insufficient samples (<3 runs)")
        else:
            lines.append("- Performance budgets: within soft SLOs")
        return lines

    lines.append("- Performance budget alerts:")
    for alert in alerts:
        lines.append(f"  - {alert.format_line()}")
    return lines


def summary_highlights(summary: AnalysisSummary) -> dict[str, Any]:
    """Compact dict useful for tests and agent prompts."""
    return {
        "runs": summary.runs,
        "competition_loss_rate": summary.competition_loss_rate,
        "p90_candidate_to_confirm_ms": summary.p90_candidate_to_confirm_ms,
        "p90_open_to_candidate_ms": summary.p90_open_to_candidate_ms,
        "alerts": [a.key for a in evaluate_budgets(summary)],
    }
