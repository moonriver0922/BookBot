from __future__ import annotations

import json
import shutil
import subprocess
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from loguru import logger

RUNTIME_PATH = Path("logs/runtime.jsonl")
FEEDBACK_PATH = Path("logs/feedback.jsonl")
CHANGELOG_PATH = Path("CHANGELOG.md")
REVIEW_LOG_DIR = Path("logs")
AUTO_TUNING_PATH = Path("auto_tuning.yaml")
BACKUP_ROOT = REVIEW_LOG_DIR / "review-backups"

WHITELIST_PATHS = {AUTO_TUNING_PATH.resolve()}


@dataclass
class ReviewDecision:
    today_status: str
    today_runs: int
    today_success: int
    failure_reasons: Counter[str]


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def _parse_ts(text: str) -> datetime | None:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _is_same_day(row: dict[str, Any], target_date: datetime) -> bool:
    ts = _parse_ts(str(row.get("timestamp", "")))
    return bool(ts and ts.date() == target_date.date())


def _window_filter(rows: list[dict[str, Any]], days: int) -> list[dict[str, Any]]:
    if days <= 0:
        return rows
    cutoff = datetime.now().timestamp() - days * 86400
    out: list[dict[str, Any]] = []
    for row in rows:
        ts = _parse_ts(str(row.get("timestamp", "")))
        if ts and ts.timestamp() >= cutoff:
            out.append(row)
    return out


def _collect_failure_reasons(feedback_rows: list[dict[str, Any]]) -> Counter[str]:
    reasons: Counter[str] = Counter()
    for row in feedback_rows:
        if row.get("success") is True:
            continue
        events = row.get("events") or []
        for event in events:
            if not isinstance(event, dict):
                continue
            reason = str(event.get("reason", "unknown"))
            reasons[reason] += 1
    return reasons


def _build_decision(
    runtime_rows: list[dict[str, Any]],
    feedback_rows: list[dict[str, Any]],
    *,
    days: int,
) -> ReviewDecision:
    now = datetime.now()
    recent_runtime = _window_filter(runtime_rows, days)
    recent_feedback = _window_filter(feedback_rows, days)

    today_runtime = [r for r in recent_runtime if _is_same_day(r, now)]
    today_feedback = [r for r in recent_feedback if _is_same_day(r, now)]
    today_runs = len(today_runtime)
    today_success = sum(1 for r in today_runtime if r.get("success") is True)

    if today_runs == 0:
        status = "no_data"
    elif today_success > 0:
        status = "success"
    else:
        status = "failed"

    reasons = _collect_failure_reasons(recent_feedback)
    if status == "failed" and today_feedback:
        today_reasons = _collect_failure_reasons(today_feedback)
        if today_reasons:
            reasons = today_reasons + reasons

    return ReviewDecision(
        today_status=status,
        today_runs=today_runs,
        today_success=today_success,
        failure_reasons=reasons,
    )


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    return raw if isinstance(raw, dict) else {}


def _set_nested(root: dict[str, Any], dotted_key: str, value: Any) -> tuple[Any, Any]:
    parts = dotted_key.split(".")
    node = root
    for key in parts[:-1]:
        child = node.get(key)
        if not isinstance(child, dict):
            child = {}
            node[key] = child
        node = child
    leaf = parts[-1]
    old = node.get(leaf)
    node[leaf] = value
    return old, value


def _append_offset(existing: Any, offset: int) -> list[int]:
    vals = [3, 8] if not isinstance(existing, list) else [int(v) for v in existing if isinstance(v, int)]
    if offset not in vals:
        vals.append(offset)
    vals.sort()
    return vals[:5]


def _compute_actions(reasons: Counter[str], max_actions: int) -> list[tuple[str, Any, str]]:
    actions: list[tuple[str, Any, str]] = []
    top = [name for name, _ in reasons.most_common(8)]

    if any(r in top for r in ("login_failed", "navigation_failed")):
        actions.append(("settings.timeout", ("inc", 5000, 60000), "Increase timeout for login/navigation stability"))
        actions.append(("settings.retry_count", ("inc", 1, 10), "Increase retry count for transient failures"))
    if any(r in top for r in ("form_not_ready",)):
        actions.append(
            ("settings.rush_timetable_first_wait_ms", ("inc", 2000, 35000), "Increase first timetable wait budget"),
        )
    if any(r in top for r in ("exception", "all_attempts_exhausted")):
        actions.append(("settings.retry_interval", ("inc", 1, 12), "Increase retry interval for exception recovery"))
        actions.append(
            ("settings.same_slot_retry_budget_ms", ("inc", 500, 6000), "Increase same-slot retry budget for conflicts"),
        )
    if any(r in top for r in ("booking_conflict", "no_bookings_made")):
        actions.append(("settings.rush_retry_offsets_s", ("append_offset", 13), "Add a later retry wave at +13s"))

    return actions[:max_actions]


def _apply_actions_to_tuning(actions: list[tuple[str, Any, str]]) -> list[str]:
    if not actions:
        return []

    target = AUTO_TUNING_PATH.resolve()
    if target not in WHITELIST_PATHS:
        raise RuntimeError(f"Path not in auto-fix whitelist: {target}")

    now = datetime.now().strftime("%Y%m%d-%H%M%S")
    BACKUP_ROOT.mkdir(parents=True, exist_ok=True)
    backup_dir = BACKUP_ROOT / now
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_file = backup_dir / f"{AUTO_TUNING_PATH.name}.bak"
    if AUTO_TUNING_PATH.exists():
        shutil.copy2(AUTO_TUNING_PATH, backup_file)

    tuning = _load_yaml(AUTO_TUNING_PATH)
    applied: list[str] = []

    try:
        for key, op, comment in actions:
            curr = tuning
            parts = key.split(".")
            for p in parts[:-1]:
                if not isinstance(curr.get(p), dict):
                    curr[p] = {}
                curr = curr[p]
            leaf = parts[-1]
            old_val = curr.get(leaf)
            new_val: Any
            if isinstance(op, tuple) and op and op[0] == "inc":
                step = int(op[1])
                cap = int(op[2])
                base = int(old_val) if isinstance(old_val, int) else 0
                new_val = min(base + step, cap)
            elif isinstance(op, tuple) and op and op[0] == "append_offset":
                new_val = _append_offset(old_val, int(op[1]))
            else:
                new_val = op
            _set_nested(tuning, key, new_val)
            applied.append(f"{key}: {old_val!r} -> {new_val!r} ({comment})")

        rendered = yaml.safe_dump(tuning, sort_keys=True, allow_unicode=False)
        AUTO_TUNING_PATH.write_text(rendered, encoding="utf-8")
        # Lightweight validation: YAML can be parsed and remains dict.
        validated = _load_yaml(AUTO_TUNING_PATH)
        if not isinstance(validated, dict):
            raise RuntimeError("auto_tuning.yaml validation failed (not a mapping)")
        return applied
    except Exception:
        if backup_file.exists():
            shutil.copy2(backup_file, AUTO_TUNING_PATH)
        raise


def _ensure_changelog() -> None:
    if CHANGELOG_PATH.exists():
        return
    CHANGELOG_PATH.write_text(
        "# Changelog\n\nAll notable review/optimization changes are recorded here.\n",
        encoding="utf-8",
    )


def _append_changelog(lines: list[str]) -> None:
    _ensure_changelog()
    with CHANGELOG_PATH.open("a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def _write_review_report(lines: list[str]) -> Path:
    REVIEW_LOG_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REVIEW_LOG_DIR / f"review-{datetime.now().strftime('%Y-%m-%d')}.md"
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path


def _build_agent_digest(runtime_rows: list[dict[str, Any]], feedback_rows: list[dict[str, Any]]) -> str:
    total_runs = len(runtime_rows)
    succ = sum(1 for r in runtime_rows if r.get("success") is True)
    fail = total_runs - succ
    reasons = _collect_failure_reasons(feedback_rows)
    top_reasons = ", ".join(f"{k}:{v}" for k, v in reasons.most_common(10)) or "none"
    return (
        f"巡检日志摘要:\n"
        f"- runs={total_runs}, success={succ}, failed={fail}\n"
        f"- top_failure_reasons={top_reasons}\n"
    )


def _invoke_cursor_agent_analysis(
    *,
    runtime_path: Path,
    feedback_path: Path,
    runtime_rows: list[dict[str, Any]],
    feedback_rows: list[dict[str, Any]],
    model: str | None,
    timeout_s: int,
) -> tuple[bool, str]:
    digest = _build_agent_digest(runtime_rows, feedback_rows)
    prompt = (
        "你是自动化稳定性专家。请基于 runtime/feedback 日志输出失败排查与优化计划。\n"
        "要求：\n"
        "1) 先给根因排名（按影响度）\n"
        "2) 给逐步排查 checklist（可执行）\n"
        "3) 给最小风险优化动作（优先参数化，不做大改）\n"
        "4) 给验证指标与回滚条件\n"
        "5) 中文输出，条理清晰\n\n"
        f"参考文件:\n- {runtime_path}\n- {feedback_path}\n\n"
        f"{digest}"
    )

    agent_bin = shutil.which("cursor-agent")
    cursor_bin = shutil.which("cursor")

    def _build_cmd(model_name: str | None) -> list[str] | None:
        if agent_bin:
            cmd = [
                agent_bin,
                "--print",
                "--output-format",
                "text",
                "--force",
                "--approve-mcps",
                "--workspace",
                str(Path.cwd()),
            ]
            if model_name:
                cmd.extend(["--model", model_name])
            cmd.append(prompt)
            return cmd
        if cursor_bin:
            cmd = [
                cursor_bin,
                "agent",
                "--print",
                "--output-format",
                "text",
                "--force",
                "--approve-mcps",
                "--workspace",
                str(Path.cwd()),
            ]
            if model_name:
                cmd.extend(["--model", model_name])
            cmd.append(prompt)
            return cmd
        return None

    def _run_cmd(cmd: list[str]) -> tuple[int | None, str, str]:
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=str(Path.cwd()),
                timeout=max(15, timeout_s),
            )
            return proc.returncode, (proc.stdout or "").strip(), (proc.stderr or "").strip()
        except subprocess.TimeoutExpired:
            return None, "", f"agent invocation timed out after {max(15, timeout_s)}s"

    cmd = _build_cmd(model)
    if not cmd:
        return False, "Neither 'cursor-agent' nor 'cursor' CLI found in PATH"

    rc, out, err = _run_cmd(cmd)
    if rc is None:
        return False, err
    combined = out if out else err
    if rc != 0 and model and "Cannot use this model" in combined:
        fallback_cmd = _build_cmd(None)
        if fallback_cmd:
            f_rc, f_out, f_err = _run_cmd(fallback_cmd)
            if f_rc is None:
                return False, f_err
            if f_rc == 0:
                rc = 0
                out = f_out
                combined = out
            else:
                fallback_combined = f_out if f_out else f_err
                return False, f"model '{model}' unavailable; fallback(auto) failed: {fallback_combined[:1200]}"

    if rc != 0:
        msg = combined[:1200] if combined else f"agent exited with code {rc}"
        return False, msg

    REVIEW_LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d")
    model_tag = (model or "auto").replace("/", "_")
    output_path = REVIEW_LOG_DIR / f"review-agent-{stamp}-{model_tag}.md"
    output_path.write_text(out + "\n", encoding="utf-8")
    return True, str(output_path)


def run_daily_review(
    *,
    runtime_path: Path = RUNTIME_PATH,
    feedback_path: Path = FEEDBACK_PATH,
    days: int = 14,
    auto_fix: bool = False,
    max_auto_actions: int = 3,
    with_agent: bool = True,
    agent_model: str | None = "gpt-5.3-codex",
    agent_timeout_s: int = 120,
) -> int:
    runtime_rows = _read_jsonl(runtime_path)
    feedback_rows = _read_jsonl(feedback_path)
    decision = _build_decision(runtime_rows, feedback_rows, days=days)

    today = datetime.now().strftime("%Y-%m-%d")
    lines = [
        f"## {today}",
        "",
        "- Daily review at 09:00",
        f"- Today runtime rows: {decision.today_runs}",
        f"- Today success rows: {decision.today_success}",
        f"- Status: {decision.today_status}",
    ]

    top_reasons = ", ".join(f"{k}:{v}" for k, v in decision.failure_reasons.most_common(8)) or "none"
    lines.append(f"- Historical failure reasons ({days}d): {top_reasons}")

    if decision.today_status == "success":
        lines.append("- Action: Keep current strategy")
        report = _write_review_report(lines)
        _append_changelog(lines)
        logger.info("Review complete (success). Report: {}", report)
        return 0

    if decision.today_status == "no_data":
        lines.append("- Action: No runtime/feedback records for today, no optimization applied")
        report = _write_review_report(lines)
        _append_changelog(lines)
        logger.warning("Review complete (no_data). Report: {}", report)
        return 0

    lines.extend(
        [
            "- Step diagnosis:",
            "  1) Auth and navigation stability",
            "  2) Form readiness and timetable load budgets",
            "  3) Retry and conflict-recovery settings",
        ]
    )

    if with_agent:
        ok, detail = _invoke_cursor_agent_analysis(
            runtime_path=runtime_path,
            feedback_path=feedback_path,
            runtime_rows=_window_filter(runtime_rows, days),
            feedback_rows=_window_filter(feedback_rows, days),
            model=agent_model,
            timeout_s=agent_timeout_s,
        )
        if ok:
            lines.append(f"- Agent analysis: success ({detail})")
        else:
            lines.append(f"- Agent analysis: failed ({detail})")
    else:
        lines.append("- Agent analysis: skipped by CLI option")

    if not auto_fix:
        lines.append("- Auto-fix disabled; generated diagnosis only")
        report = _write_review_report(lines)
        _append_changelog(lines)
        logger.warning("Review complete (failed, no auto-fix). Report: {}", report)
        return 1

    actions = _compute_actions(decision.failure_reasons, max_actions=max(1, max_auto_actions))
    if not actions:
        lines.append("- Auto-fix: no matching optimization actions for observed failures")
        report = _write_review_report(lines)
        _append_changelog(lines)
        logger.warning("Review complete (failed, no action matched). Report: {}", report)
        return 1

    try:
        applied = _apply_actions_to_tuning(actions)
        lines.append("- Auto-fix applied to `auto_tuning.yaml`:")
        for item in applied:
            lines.append(f"  - {item}")
        lines.append("- Verification: YAML validated; rollback not needed")
        rc = 1
    except Exception as exc:
        lines.append(f"- Auto-fix failed and rolled back: {exc}")
        rc = 2

    report = _write_review_report(lines)
    _append_changelog(lines)
    logger.info("Review complete. Report: {}", report)
    return rc
