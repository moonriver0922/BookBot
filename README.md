# BookBot – PolyU Sports Facility Auto-Booking

Automated booking bot for PolyU POSS badminton courts. Books afternoon slots on preferred days with a single command or scheduled cron job.

## Features

- Automatic login and navigation through POSS
- Configurable day/time/venue preferences
- Prefers 2 consecutive slots (2 hours); falls back to 1 slot
- Weekly quota enforcement (max 4 slots/week)
- Anti-detection: stealth browser fingerprint, human-like delays, typing simulation
- Exponential-backoff retries with block detection
- Cron / macOS LaunchAgent scheduling
- Debug mode with visible browser and step-by-step screenshots
- Built-in log analytics for success-rate and latency baselines

## Quick Start

```bash
# 1. Clone and enter the project
cd BookBot

# 2. Create a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Install Playwright browsers
playwright install chromium

# 5. Copy and edit config
cp config.example.yaml config.yaml
# Fill in your NetID and password in config.yaml

# 6. Test with debug mode (browser visible)
python run.py --debug --dry-run

# 7. Run for real
python run.py
```

## Usage

```
python run.py                    # Manual run: book best available slot now
python run.py --auto             # Wait until 08:30 then book (for cron)
python run.py --dry-run          # Show what would be booked, no action
python run.py --debug            # Browser visible + verbose logging
python run.py --debug --dry-run  # Inspect everything without booking
python run.py --install-schedule # Install cron/launchd for daily runs
python run.py --analyze-logs     # Analyze logs (last 14d + previous 14d comparison)
python run.py -c /path/to/cfg   # Use a specific config file
```

## Configuration

Edit `config.yaml` (see `config.example.yaml` for all options):

```yaml
credentials:
  username: "your_net_id"
  password: "your_password"

preferences:
  activity: "Badminton"
  center: "Shaw Sports Complex"
  preferred_days: [1, 2, 4]       # Tue, Wed, Fri
  time_range:
    start: "14:00"
    end: "18:00"
  prefer_consecutive: 2           # 2 consecutive hours preferred
  weekly_max_slots: 4
```

### Preference Details

| Option | Description |
|--------|-------------|
| `preferred_days` | 0=Mon, 1=Tue, 2=Wed, 3=Thu, 4=Fri, 5=Sat, 6=Sun |
| `time_range` | Only book slots within this window |
| `prefer_consecutive` | Try to book N consecutive 1-hour slots |
| `weekly_max_slots` | Stop booking once this many slots are booked in the week |
| `book_days_ahead` | How many days ahead to book (default: 7) |

## Scheduling

The bot is designed to run at 08:29 and internally wait until exactly 08:30:00 for maximum speed when new slots become available.

```bash
# Install automatically (cron on Linux, launchd on macOS)
python run.py --install-schedule
```

Or manually add to crontab:

```
29 8 * * * cd /path/to/BookBot && /path/to/.venv/bin/python run.py --auto
```

## Debugging

- All runs generate screenshots in `screenshots/`
- Logs are written to `bookbot.log` (rotated at 5 MB)
- Use `--debug` to watch the browser in real-time
- Use `--dry-run` to see slot selection without booking
- Use `--analyze-logs --days 14 --compare-days 14` to track success-rate delta,
  technical failures, and rush latency P90 metrics

## Project Structure

```
BookBot/
├── run.py                 # CLI entry point
├── config.yaml            # Your config (git-ignored)
├── config.example.yaml    # Template config
├── requirements.txt       # Python dependencies
├── bookbot/
│   ├── main.py            # Orchestrator with retry logic
│   ├── config.py          # Config loader and validation
│   ├── auth.py            # Login and navigation
│   ├── booker.py          # Core booking logic and ranking
│   ├── stealth.py         # Anti-detection utilities
│   └── scheduler.py       # Cron/launchd helpers
└── screenshots/           # Auto-saved debug screenshots
```
