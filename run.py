#!/usr/bin/env python3
"""BookBot – PolyU sports facility auto-booking CLI."""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

from loguru import logger


def _setup_logging(debug: bool = False) -> None:
    logger.remove()
    level = "DEBUG" if debug else "INFO"
    fmt = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    )
    logger.add(sys.stderr, format=fmt, level=level, colorize=True)
    logger.add(
        "bookbot.log",
        format=fmt,
        level="DEBUG",
        rotation="5 MB",
        retention="7 days",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="bookbot",
        description="PolyU POSS Sports Facility Auto-Booking Bot",
    )
    sub = parser.add_subparsers(dest="command")

    run_p = sub.add_parser("run", help="Run booking bot")
    run_p.add_argument(
        "--config", "-c",
        type=str,
        default=None,
        help="Path to config.yaml (default: auto-detect)",
    )
    run_p.add_argument(
        "--auto",
        action="store_true",
        help="Auto mode: wait until 08:30 then book",
    )
    run_p.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be booked without actually booking",
    )
    run_p.add_argument(
        "--debug",
        action="store_true",
        help="Run with browser visible (non-headless) and verbose logging",
    )

    analyze_p = sub.add_parser("analyze", help="Analyze runtime/feedback logs")
    analyze_p.add_argument("--days", type=int, default=14, help="Window size in days")
    analyze_p.add_argument(
        "--compare-days",
        type=int,
        default=14,
        help="Previous window size (days) for comparison",
    )
    analyze_p.add_argument(
        "--debug",
        action="store_true",
        help="Verbose logging",
    )

    plan_p = sub.add_parser("plan", help="Call Cursor Agent for plan-style log analysis")
    plan_p.add_argument("--days", type=int, default=14, help="Window size in days")
    plan_p.add_argument(
        "--agent-model",
        type=str,
        default="gpt-5.3-codex",
        help="Model passed to cursor-agent (default: gpt-5.3-codex)",
    )
    plan_p.add_argument(
        "--debug",
        action="store_true",
        help="Verbose logging",
    )

    review_p = sub.add_parser("review", help="Daily runtime/feedback review with auto-optimization")
    review_p.add_argument("--days", type=int, default=14, help="Historical window size in days")
    review_p.add_argument(
        "--auto-fix",
        action="store_true",
        help="Apply automatic optimization actions when today's run failed",
    )
    review_p.add_argument(
        "--max-auto-actions",
        type=int,
        default=3,
        help="Maximum automatic optimization actions in one review run",
    )
    review_p.add_argument(
        "--without-agent",
        action="store_true",
        help="Disable Cursor Agent analysis during failed-review diagnosis",
    )
    review_p.add_argument(
        "--agent-model",
        type=str,
        default="gpt-5.3-codex",
        help="Model passed to Cursor Agent for failed-review diagnosis",
    )
    review_p.add_argument(
        "--agent-timeout-s",
        type=int,
        default=120,
        help="Timeout seconds for Cursor Agent analysis invocation",
    )
    review_p.add_argument(
        "--debug",
        action="store_true",
        help="Verbose logging",
    )

    schedule_p = sub.add_parser("schedule", help="Manage schedule")
    schedule_p.add_argument(
        "action",
        choices=["install", "install-review", "install-all"],
        help="Schedule action",
    )
    schedule_p.add_argument(
        "--debug",
        action="store_true",
        help="Verbose logging",
    )

    # Backward-compat flags (deprecated style) to avoid breaking existing scripts.
    parser.add_argument("--config", "-c", type=str, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--auto", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--dry-run", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--debug", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--install-schedule", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--analyze-logs", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--agent-plan-logs", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--days", type=int, default=14, help=argparse.SUPPRESS)
    parser.add_argument("--compare-days", type=int, default=14, help=argparse.SUPPRESS)
    parser.add_argument("--agent-model", type=str, default="gpt-5.3-codex", help=argparse.SUPPRESS)
    parser.add_argument("--auto-fix", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--max-auto-actions", type=int, default=3, help=argparse.SUPPRESS)
    parser.add_argument("--without-agent", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--agent-timeout-s", type=int, default=120, help=argparse.SUPPRESS)

    args = parser.parse_args()

    if not args.command:
        # Map legacy flags to subcommands.
        if args.install_schedule:
            args.command = "schedule"
            args.action = "install"
        elif args.analyze_logs:
            args.command = "analyze"
        elif args.agent_plan_logs:
            args.command = "plan"
        else:
            args.command = "run"

    return args


def main() -> None:
    args = parse_args()
    _setup_logging(debug=args.debug)

    # Ensure we're in the project directory
    project_dir = Path(__file__).resolve().parent
    os.chdir(project_dir)

    if args.command == "schedule":
        from bookbot.scheduler import install_schedule, install_review_schedule
        if args.action == "install":
            install_schedule()
        elif args.action == "install-review":
            install_review_schedule()
        elif args.action == "install-all":
            install_schedule()
            install_review_schedule()
        return

    if args.command == "analyze":
        from bookbot.analyze import analyze_logs

        report = analyze_logs(
            Path("logs/runtime.jsonl"),
            Path("logs/feedback.jsonl"),
            days=args.days,
            compare_days=args.compare_days,
        )
        logger.info("\n{}", report.rstrip())
        return

    if args.command == "plan":
        from bookbot.agent_plan import run_plan_analysis_with_agent

        rc = run_plan_analysis_with_agent(
            workspace=project_dir,
            runtime_path=Path("logs/runtime.jsonl"),
            feedback_path=Path("logs/feedback.jsonl"),
            days=args.days,
            model=args.agent_model,
        )
        if rc != 0:
            logger.error("Agent plan analysis failed with exit code {}", rc)
            sys.exit(rc)
        return

    if args.command == "review":
        from bookbot.review import run_daily_review

        rc = run_daily_review(
            runtime_path=Path("logs/runtime.jsonl"),
            feedback_path=Path("logs/feedback.jsonl"),
            days=args.days,
            auto_fix=args.auto_fix,
            max_auto_actions=max(1, args.max_auto_actions),
            with_agent=not args.without_agent,
            agent_model=args.agent_model,
            agent_timeout_s=max(15, args.agent_timeout_s),
        )
        if rc != 0:
            sys.exit(rc)
        return

    from bookbot.config import load_config
    config = load_config(args.config)

    if args.debug:
        config.settings.headless = False

    rush_time = None
    if args.auto:
        rush_time = (8, 30, 0)
        logger.info(
            "Auto mode – starting preparation now, will click Search at {:02d}:{:02d}:{:02d}",
            *rush_time,
        )

    logger.info("Starting BookBot …")
    from bookbot.main import execute
    success = asyncio.run(execute(config, dry_run=args.dry_run, rush_time=rush_time))

    if success:
        logger.success("BookBot finished successfully")
    else:
        logger.warning("BookBot finished – no booking was made")
        sys.exit(1)


if __name__ == "__main__":
    main()
