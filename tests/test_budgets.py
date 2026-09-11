from __future__ import annotations

from bookbot.analyze import AnalysisSummary
from bookbot.budgets import evaluate_budgets, format_budget_section
from collections import Counter


def _summary(**overrides) -> AnalysisSummary:
    base = dict(
        runs=10,
        success=6,
        failed=4,
        success_rate=60.0,
        technical_fail_rate=10.0,
        visible_slots_unbooked_rate=5.0,
        competition_loss_rate=10.0,
        no_inventory_rate=5.0,
        automation_failure_rate=5.0,
        p90_refresh_to_candidate_ms=800.0,
        p90_candidate_to_submit_ms=900.0,
        p50_search_rtt_ms=100.0,
        p90_search_rtt_ms=200.0,
        p95_search_rtt_ms=300.0,
        p50_open_to_candidate_ms=400.0,
        p90_open_to_candidate_ms=700.0,
        p95_open_to_candidate_ms=900.0,
        p50_candidate_to_click_ms=50.0,
        p90_candidate_to_click_ms=80.0,
        p50_click_to_next_ms=100.0,
        p90_click_to_next_ms=150.0,
        p50_candidate_to_confirm_ms=500.0,
        p90_candidate_to_confirm_ms=800.0,
        p95_candidate_to_confirm_ms=1000.0,
        timetable_load_p50_s=2.0,
        timetable_load_p90_s=4.0,
        timetable_load_p99_s=6.0,
        timetable_load_gt_8s_rate=0.0,
        reason_counts=Counter(),
        class_counts=Counter(),
    )
    base.update(overrides)
    return AnalysisSummary(**base)


def test_no_alerts_when_within_budget():
    assert evaluate_budgets(_summary()) == []


def test_alerts_on_competition_and_confirm_latency():
    alerts = evaluate_budgets(
        _summary(
            competition_loss_rate=40.0,
            p90_candidate_to_confirm_ms=3200.0,
        )
    )
    keys = {a.key for a in alerts}
    assert "competition_loss_rate" in keys
    assert "p90_candidate_to_confirm_ms" in keys
    confirm = next(a for a in alerts if a.key == "p90_candidate_to_confirm_ms")
    assert confirm.severity == "breach"


def test_insufficient_runs_skip_alerts():
    assert evaluate_budgets(_summary(runs=2, competition_loss_rate=99.0)) == []


def test_format_budget_section_includes_highlights():
    lines = format_budget_section(
        _summary(competition_loss_rate=40.0, p90_candidate_to_confirm_ms=2500.0),
        days=14,
    )
    text = "\n".join(lines)
    assert "competition_loss_rate=40.0%" in text
    assert "P90 candidate→confirm=" in text
    assert "Performance budget alerts:" in text
