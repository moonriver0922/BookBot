#!/usr/bin/env python3
"""BookBot – PolyU sports facility auto-booking CLI."""
from __future__ import annotations

import argparse
import asyncio
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
    parser.add_argument(
        "--config", "-c",
        type=str,
        default=None,
        help="Path to config.yaml (default: auto-detect)",
    )
    parser.add_argument(
        "--auto",
        action="store_true",
        help="Auto mode: wait until 08:30 then book",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be booked without actually booking",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Run with browser visible (non-headless) and verbose logging",
    )
    parser.add_argument(
        "--install-schedule",
        action="store_true",
        help="Install cron job / launchd agent for daily 08:29 runs",
    )
    parser.add_argument(
        "--analyze-logs",
        action="store_true",
        help="Analyze runtime/feedback logs and print baseline + trend metrics",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=14,
        help="Window size (days) for --analyze-logs",
    )
    parser.add_argument(
        "--compare-days",
        type=int,
        default=14,
        help="Previous window size (days) for comparison in --analyze-logs",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    _setup_logging(debug=args.debug)

    # Ensure we're in the project directory
    project_dir = Path(__file__).resolve().parent
    import os
    os.chdir(project_dir)

    if args.install_schedule:
        from bookbot.scheduler import install_schedule
        install_schedule()
        return

    if args.analyze_logs:
        from bookbot.analyze import analyze_logs

        report = analyze_logs(
            Path("logs/runtime.jsonl"),
            Path("logs/feedback.jsonl"),
            days=args.days,
            compare_days=args.compare_days,
        )
        logger.info("\n{}", report.rstrip())
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
