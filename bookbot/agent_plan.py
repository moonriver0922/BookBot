from __future__ import annotations

import json
import shutil
import subprocess
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


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


def _filter_recent(rows: list[dict[str, Any]], days: int) -> list[dict[str, Any]]:
    if days <= 0:
        return rows
    cutoff = datetime.now() - timedelta(days=days)
    kept: list[dict[str, Any]] = []
    for row in rows:
        ts = _parse_ts(str(row.get("timestamp", "")))
        if ts and ts >= cutoff:
            kept.append(row)
    return kept


def _build_digest(runtime_rows: list[dict[str, Any]], feedback_rows: list[dict[str, Any]]) -> str:
    total_runs = len(runtime_rows)
    succ = sum(1 for r in runtime_rows if r.get("success") is True)
    fail = total_runs - succ

    reason_counter: Counter[str] = Counter()
    failed_cases: list[str] = []
    for row in feedback_rows:
        if row.get("success") is True:
            continue
        events = row.get("events") or []
        reasons = [str(ev.get("reason", "unknown")) for ev in events if isinstance(ev, dict)]
        reason_counter.update(reasons)
        failed_cases.append(
            f"- {row.get('timestamp', '?')} reasons={reasons[:6]}"
        )

    top_reasons = ", ".join(f"{k}:{v}" for k, v in reason_counter.most_common(10)) or "none"
    sample_failed = "\n".join(failed_cases[-12:]) if failed_cases else "(no failed cases)"

    return (
        f"最近日志汇总:\n"
        f"- runs={total_runs}, success={succ}, failed={fail}\n"
        f"- top_failure_reasons={top_reasons}\n"
        f"- failed_case_samples:\n{sample_failed}\n"
    )


def run_plan_analysis_with_agent(
    *,
    workspace: Path,
    runtime_path: Path,
    feedback_path: Path,
    days: int,
    model: str | None = None,
) -> int:
    """Invoke cursor-agent in print mode with a plan-style prompt."""
    runtime_rows = _filter_recent(_read_jsonl(runtime_path), days)
    feedback_rows = _filter_recent(_read_jsonl(feedback_path), days)
    digest = _build_digest(runtime_rows, feedback_rows)

    prompt = (
        "你是一个资深自动化工程师。请以计划导向（Plan-style）方式分析日志，"
        "目标是提高抢位成功率。\n\n"
        "要求:\n"
        "1) 明确区分业务侧原因 vs 技术侧原因，并按影响度排序。\n"
        "2) 给出可执行改进方案，重点优化“刷新后抢不过”的链路。\n"
        "3) 只接受晚间固定时段，不建议扩大时段。\n"
        "4) 输出包含: 根因、证据、改进动作、验证指标、两周执行节奏。\n"
        "5) 用中文输出，简洁但有操作细节。\n\n"
        f"参考文件:\n- {runtime_path}\n- {feedback_path}\n\n"
        f"{digest}"
    )

    agent_bin = shutil.which("cursor-agent")
    cursor_bin = shutil.which("cursor")
    if agent_bin:
        cmd = [
            agent_bin,
            "--print",
            "--output-format",
            "text",
            "--workspace",
            str(workspace),
        ]
        if model:
            cmd.extend(["--model", model])
        cmd.append(prompt)
    elif cursor_bin:
        cmd = [
            cursor_bin,
            "agent",
            "--print",
            "--output-format",
            "text",
            "--workspace",
            str(workspace),
        ]
        if model:
            cmd.extend(["--model", model])
        cmd.append(prompt)
    else:
        raise RuntimeError("Neither 'cursor-agent' nor 'cursor' CLI found in PATH.")

    proc = subprocess.run(cmd, cwd=str(workspace))
    return proc.returncode
