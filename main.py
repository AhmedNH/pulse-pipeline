#!/usr/bin/env python3
"""
main.py — Entry point for Pulse.

Usage
-----
    python main.py run                   # Fetch + store one round of data
    python main.py schedule              # Run on a schedule (Ctrl-C to stop)
    python main.py dashboard             # Print CLI dashboard from stored data
    python main.py weather [city]        # Latest weather (all cities or one)
    python main.py weather-stats <city>  # Aggregate stats for a city
    python main.py crypto [coin_id]      # Latest crypto prices (all or one)
    python main.py crypto-stats <coin>   # Aggregate stats for a coin
    python main.py api                   # Start the FastAPI REST server
"""

import argparse
import logging
import sys

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt= "%H:%M:%S",
)

from pulse import db

db.init_db()


def cmd_run(_args) -> None:
    from pulse.pipeline import run_all_jobs
    result = run_all_jobs()
    print(f"Stored {result['weather']} weather snapshots, {result['crypto']} crypto snapshots.")


def cmd_schedule(args) -> None:
    from pulse.pipeline import start_scheduler
    start_scheduler(
        weather_interval_minutes=args.weather_interval,
        crypto_interval_minutes =args.crypto_interval,
    )


def cmd_dashboard(_args) -> None:
    from pulse.cli import show_summary
    show_summary()


def cmd_weather(args) -> None:
    from pulse.cli import show_weather, show_weather_stats
    from pulse import db
    if args.city:
        row = db.get_latest_weather(args.city)
        if row is None:
            print(f"No data for '{args.city}'. Run: python main.py run")
        else:
            for key, val in dict(row).items():
                print(f"  {key:<14} {val}")
    else:
        show_weather()


def cmd_weather_stats(args) -> None:
    from pulse.cli import show_weather_stats
    show_weather_stats(args.city)


def cmd_crypto(args) -> None:
    from pulse.cli import show_crypto
    from pulse import db
    if args.coin_id:
        row = db.get_latest_crypto(args.coin_id)
        if row is None:
            print(f"No data for '{args.coin_id}'. Run: python main.py run")
        else:
            for key, val in dict(row).items():
                print(f"  {key:<14} {val}")
    else:
        show_crypto()


def cmd_crypto_stats(args) -> None:
    from pulse.cli import show_crypto_stats
    show_crypto_stats(args.coin)


def cmd_api(_args) -> None:
    try:
        import uvicorn
    except ImportError:
        print("uvicorn not installed. Run: pip install uvicorn")
        sys.exit(1)
    from pulse.api import app
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")


# ── Argument parser ────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog        = "pulse",
        description = "Real-time data aggregation pipeline.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("run",       help="Fetch and store one round of data")
    sub.add_parser("dashboard", help="Print CLI dashboard from stored data")
    sub.add_parser("api",       help="Start the FastAPI REST server on :8000")

    sched = sub.add_parser("schedule", help="Run on a schedule indefinitely")
    sched.add_argument("--weather-interval", type=int, default=30, metavar="MIN",
                       help="Weather fetch interval in minutes (default: 30)")
    sched.add_argument("--crypto-interval",  type=int, default=5,  metavar="MIN",
                       help="Crypto fetch interval in minutes (default: 5)")

    w = sub.add_parser("weather", help="Show latest weather data")
    w.add_argument("city", nargs="?", default=None,
                   help="City name (omit to show all)")

    ws = sub.add_parser("weather-stats", help="Show aggregate weather stats")
    ws.add_argument("city", help="City name")

    c = sub.add_parser("crypto", help="Show latest crypto prices")
    c.add_argument("coin_id", nargs="?", default=None,
                   help="CoinGecko coin ID (omit to show all)")

    cs = sub.add_parser("crypto-stats", help="Show aggregate crypto stats")
    cs.add_argument("coin", help="CoinGecko coin ID (e.g. bitcoin)")

    return parser


COMMANDS = {
    "run":           cmd_run,
    "schedule":      cmd_schedule,
    "dashboard":     cmd_dashboard,
    "weather":       cmd_weather,
    "weather-stats": cmd_weather_stats,
    "crypto":        cmd_crypto,
    "crypto-stats":  cmd_crypto_stats,
    "api":           cmd_api,
}

if __name__ == "__main__":
    parser = build_parser()
    args   = parser.parse_args()
    COMMANDS[args.command](args)
