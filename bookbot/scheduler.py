from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from loguru import logger

PLIST_LABEL = "com.bookbot.polyu"
PLIST_DIR = Path.home() / "Library" / "LaunchAgents"
BOOKING_START_HOUR = 8
BOOKING_START_MINUTE = 18
CAFFEINATE_TIMEOUT_SECONDS = 20 * 60


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _booking_python() -> str:
    venv_python = _project_root() / ".venv" / "bin" / "python"
    if venv_python.exists():
        return str(venv_python)
    return sys.executable


def wait_until_target(hour: int = 8, minute: int = 30, second: int = 0) -> None:
    """Block until the next occurrence of HH:MM:SS, with sub-second precision."""
    now = datetime.now()
    target = now.replace(hour=hour, minute=minute, second=second, microsecond=0)
    if now >= target:
        target += timedelta(days=1)

    delta = (target - now).total_seconds()
    logger.info("Waiting {:.1f}s until {:02d}:{:02d}:{:02d} …", delta, hour, minute, second)

    # Coarse sleep (leave 2 seconds for spin-wait)
    if delta > 2:
        time.sleep(delta - 2)

    # Spin-wait for precision
    while datetime.now() < target:
        time.sleep(0.01)

    logger.info("Target time reached: {}", datetime.now().strftime("%H:%M:%S.%f"))


def generate_crontab_entry() -> str:
    python = _booking_python()
    script = str(_project_root() / "run.py")
    entry = (
        f"{BOOKING_START_MINUTE} {BOOKING_START_HOUR} * * * "
        f"cd \"{Path(script).parent}\" && /usr/bin/caffeinate -dimsu "
        f"-t {CAFFEINATE_TIMEOUT_SECONDS} {python} {script} --auto"
    )
    return entry


def generate_review_crontab_entry() -> str:
    python = sys.executable
    script = str(Path(__file__).resolve().parent.parent / "run.py")
    entry = f"0 9 * * * cd \"{Path(script).parent}\" && {python} {script} review --days 14 --auto-fix"
    return entry


def _install_crontab_entries(entries: list[str]) -> None:
    if not entries:
        return

    result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    existing = result.stdout if result.returncode == 0 else ""
    to_add: list[str] = []
    for entry in entries:
        if entry in existing:
            logger.info("Crontab entry already exists:\n  {}", entry)
            continue
        to_add.append(entry)

    if not to_add:
        logger.info("No new crontab entries to install")
        return

    new_crontab = existing.rstrip("\n") + "\n" + "\n".join(to_add) + "\n"
    proc = subprocess.run(
        ["crontab", "-"],
        input=new_crontab,
        capture_output=True,
        text=True,
    )
    if proc.returncode == 0:
        logger.success("Crontab entries installed successfully")
    else:
        logger.error("Failed to install crontab entries: {}", proc.stderr)


def install_crontab() -> None:
    entry = generate_crontab_entry()
    logger.info("Installing booking crontab entry:\n  {}", entry)
    _install_crontab_entries([entry])


def install_review_crontab() -> None:
    entry = generate_review_crontab_entry()
    logger.info("Installing review crontab entry:\n  {}", entry)
    _install_crontab_entries([entry])


def generate_launchd_plist() -> str:
    python = _booking_python()
    script = str(_project_root() / "run.py")
    work_dir = str(Path(script).parent)
    log_path = str(Path(work_dir) / "bookbot.log")

    plist = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{PLIST_LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/caffeinate</string>
        <string>-dimsu</string>
        <string>-t</string>
        <string>{CAFFEINATE_TIMEOUT_SECONDS}</string>
        <string>{python}</string>
        <string>{script}</string>
        <string>--auto</string>
    </array>
    <key>WorkingDirectory</key>
    <string>{work_dir}</string>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>{BOOKING_START_HOUR}</integer>
        <key>Minute</key>
        <integer>{BOOKING_START_MINUTE}</integer>
    </dict>
    <key>StandardOutPath</key>
    <string>{log_path}</string>
    <key>StandardErrorPath</key>
    <string>{log_path}</string>
</dict>
</plist>"""
    return plist


def install_launchd() -> None:
    PLIST_DIR.mkdir(parents=True, exist_ok=True)
    plist_path = PLIST_DIR / f"{PLIST_LABEL}.plist"

    plist_content = generate_launchd_plist()
    plist_path.write_text(plist_content, encoding="utf-8")
    logger.info("Wrote plist to {}", plist_path)

    subprocess.run(["launchctl", "unload", str(plist_path)], capture_output=True)
    result = subprocess.run(
        ["launchctl", "load", str(plist_path)],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        logger.success("LaunchAgent loaded: {}", plist_path)
    else:
        logger.error("Failed to load LaunchAgent: {}", result.stderr)


def install_schedule() -> None:
    """Install the scheduled task using the best method for the current OS."""
    if sys.platform == "darwin":
        install_launchd()
    else:
        install_crontab()
    logger.info(
        "Scheduled to run daily at {:02d}:{:02d} "
        "(caffeinate keeps Mac awake while bot waits until 08:30:00)",
        BOOKING_START_HOUR,
        BOOKING_START_MINUTE,
    )


def install_review_schedule() -> None:
    """Install the daily 09:00 review task (local crontab)."""
    install_review_crontab()
    logger.info("Scheduled review to run daily at 09:00")
