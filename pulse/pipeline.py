"""
pipeline.py — Orchestration layer for Pulse.

Ties together fetching and storage.  Can be run once or on a schedule.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

import schedule

from pulse import db, fetcher

logger = logging.getLogger(__name__)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ── Single-run jobs ────────────────────────────────────────────────────────────

def run_weather_job(cities: list[str] | None = None) -> int:
    """
    Fetch and store weather snapshots.

    Parameters
    ----------
    cities : list of city names, or None to fetch all.

    Returns
    -------
    Number of snapshots successfully stored.
    """
    targets = cities or list(fetcher.CITIES.keys())
    stored  = 0
    ts      = _utc_now()

    for city in targets:
        try:
            snapshot = fetcher.fetch_weather(city)
            db.insert_weather(
                city         = snapshot["city"],
                latitude     = snapshot["latitude"],
                longitude    = snapshot["longitude"],
                temperature  = snapshot["temperature"],
                wind_speed   = snapshot["wind_speed"],
                weather_code = snapshot["weather_code"],
                fetched_at   = ts,
            )
            stored += 1
            logger.info("Weather stored: %s  %.1f°C", city, snapshot["temperature"])
        except Exception as exc:
            logger.error("Weather job failed for %s: %s", city, exc)

    return stored


def run_crypto_job(coin_ids: list[str] | None = None) -> int:
    """
    Fetch and store crypto market snapshots.

    Returns
    -------
    Number of snapshots successfully stored.
    """
    stored = 0
    ts     = _utc_now()

    try:
        snapshots = fetcher.fetch_crypto(coin_ids)
        for snap in snapshots:
            db.insert_crypto(
                coin_id    = snap["coin_id"],
                symbol     = snap["symbol"],
                price_usd  = snap["price_usd"],
                change_24h = snap["change_24h"],
                market_cap = snap["market_cap"],
                volume_24h = snap["volume_24h"],
                fetched_at = ts,
            )
            stored += 1
            logger.info(
                "Crypto stored: %s  $%.4f  (%+.2f%%)",
                snap["symbol"],
                snap["price_usd"],
                snap["change_24h"] or 0.0,
            )
    except Exception as exc:
        logger.error("Crypto job failed: %s", exc)

    return stored


def run_all_jobs() -> dict[str, int]:
    """Run both jobs and return a summary."""
    logger.info("=== Pipeline run started ===")
    weather_count = run_weather_job()
    crypto_count  = run_crypto_job()
    logger.info(
        "=== Pipeline run complete: %d weather, %d crypto ===",
        weather_count,
        crypto_count,
    )
    return {"weather": weather_count, "crypto": crypto_count}


# ── Scheduled runner ───────────────────────────────────────────────────────────

def start_scheduler(
    weather_interval_minutes: int = 30,
    crypto_interval_minutes: int  = 5,
) -> None:
    """
    Block and run the pipeline on a schedule indefinitely.

    Weather is fetched every `weather_interval_minutes` (default 30).
    Crypto  is fetched every `crypto_interval_minutes`  (default  5).

    Press Ctrl-C to stop.
    """
    logger.info(
        "Scheduler started — weather every %dm, crypto every %dm",
        weather_interval_minutes,
        crypto_interval_minutes,
    )

    # Run immediately on startup, then on schedule.
    run_all_jobs()

    schedule.every(weather_interval_minutes).minutes.do(run_weather_job)
    schedule.every(crypto_interval_minutes).minutes.do(run_crypto_job)

    try:
        while True:
            schedule.run_pending()
            time.sleep(10)
    except KeyboardInterrupt:
        logger.info("Scheduler stopped.")
        schedule.clear()
