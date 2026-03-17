"""
fetcher.py — Data fetching layer for Pulse.

All HTTP calls are isolated here so the rest of the application
never touches requests directly.  Each public function returns a
clean, typed dict — callers never parse raw JSON.

Data sources (both free, no API key required):
  • Open-Meteo   https://open-meteo.com/
  • CoinGecko    https://www.coingecko.com/en/api
"""

from __future__ import annotations

import time
import logging
from typing import Any

import requests

logger = logging.getLogger(__name__)

# ── HTTP helpers ───────────────────────────────────────────────────────────────

SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json", "User-Agent": "Pulse/1.0"})

_MAX_RETRIES = 3
_RETRY_DELAY = 2.0  # seconds


def _get(url: str, params: dict | None = None, timeout: int = 10) -> Any:
    """
    GET `url` with retry logic.  Raises RuntimeError after all retries fail.
    """
    last_exc: Exception | None = None
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            resp = SESSION.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            last_exc = exc
            logger.warning(
                "Request failed (attempt %d/%d): %s", attempt, _MAX_RETRIES, exc
            )
            if attempt < _MAX_RETRIES:
                time.sleep(_RETRY_DELAY * attempt)

    raise RuntimeError(f"All {_MAX_RETRIES} attempts failed for {url}") from last_exc


# ── Open-Meteo — weather ───────────────────────────────────────────────────────

# Named cities with their coordinates
CITIES: dict[str, tuple[float, float]] = {
    "Toronto":      (43.70,  -79.42),
    "New York":     (40.71,  -74.01),
    "London":       (51.51,   -0.13),
    "Tokyo":        (35.68,  139.69),
    "Paris":        (48.85,    2.35),
    "Dubai":        (25.20,   55.27),
    "Sydney":      (-33.87,  151.21),
    "Cairo":        (30.06,   31.25),
}

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_weather(city: str) -> dict:
    """
    Fetch current weather for a named city.

    Returns
    -------
    {
        "city":         str,
        "latitude":     float,
        "longitude":    float,
        "temperature":  float,   # °C
        "wind_speed":   float,   # km/h
        "weather_code": int,
    }

    Raises
    ------
    ValueError  if `city` is not in the CITIES registry.
    RuntimeError on network failure.
    """
    if city not in CITIES:
        raise ValueError(
            f"Unknown city '{city}'. Available: {', '.join(CITIES)}"
        )

    lat, lon = CITIES[city]
    data = _get(
        OPEN_METEO_URL,
        params={
            "latitude":  lat,
            "longitude": lon,
            "current":   "temperature_2m,wind_speed_10m,weather_code",
            "timezone":  "UTC",
        },
    )

    current = data["current"]
    return {
        "city":         city,
        "latitude":     lat,
        "longitude":    lon,
        "temperature":  current["temperature_2m"],
        "wind_speed":   current["wind_speed_10m"],
        "weather_code": current["weather_code"],
    }


def fetch_all_weather() -> list[dict]:
    """Fetch weather for every city in the registry."""
    results = []
    for city in CITIES:
        try:
            results.append(fetch_weather(city))
        except Exception as exc:
            logger.error("Weather fetch failed for %s: %s", city, exc)
    return results


# ── CoinGecko — crypto ─────────────────────────────────────────────────────────

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"

TRACKED_COINS = [
    "bitcoin",
    "ethereum",
    "solana",
    "cardano",
    "ripple",
    "dogecoin",
    "polkadot",
    "chainlink",
]


def fetch_crypto(coin_ids: list[str] | None = None) -> list[dict]:
    """
    Fetch current market data for one or more coins.

    Parameters
    ----------
    coin_ids : list of CoinGecko coin IDs, or None to use TRACKED_COINS.

    Returns
    -------
    List of dicts:
    {
        "coin_id":    str,
        "symbol":     str,
        "price_usd":  float,
        "change_24h": float | None,   # percentage
        "market_cap": float | None,
        "volume_24h": float | None,
    }
    """
    ids = coin_ids or TRACKED_COINS
    data = _get(
        COINGECKO_URL,
        params={
            "vs_currency":           "usd",
            "ids":                   ",".join(ids),
            "order":                 "market_cap_desc",
            "per_page":              len(ids),
            "page":                  1,
            "sparkline":             "false",
            "price_change_percentage": "24h",
        },
    )

    return [
        {
            "coin_id":    row["id"],
            "symbol":     row["symbol"].upper(),
            "price_usd":  row["current_price"],
            "change_24h": row.get("price_change_percentage_24h"),
            "market_cap": row.get("market_cap"),
            "volume_24h": row.get("total_volume"),
        }
        for row in data
    ]
