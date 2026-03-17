"""
db.py — SQLite storage layer for Pulse.

All schema creation, inserts, and queries live here.
The rest of the application never writes raw SQL.
"""

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

DB_PATH = Path(__file__).parent.parent / "pulse.db"

# Thread-local storage so each thread gets its own connection.
_local = threading.local()


def _get_connection() -> sqlite3.Connection:
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _local.conn.row_factory = sqlite3.Row
        _local.conn.execute("PRAGMA journal_mode=WAL")
        _local.conn.execute("PRAGMA foreign_keys=ON")
    return _local.conn


@contextmanager
def get_db() -> Generator[sqlite3.Connection, None, None]:
    """Yield a connection and commit on clean exit, rollback on error."""
    conn = _get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def init_db() -> None:
    """Create all tables if they do not already exist."""
    with get_db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS weather_snapshots (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                city          TEXT    NOT NULL,
                latitude      REAL    NOT NULL,
                longitude     REAL    NOT NULL,
                temperature   REAL    NOT NULL,   -- °C
                wind_speed    REAL    NOT NULL,   -- km/h
                weather_code  INTEGER NOT NULL,
                fetched_at    TEXT    NOT NULL    -- ISO-8601 UTC
            );

            CREATE TABLE IF NOT EXISTS crypto_snapshots (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                coin_id       TEXT    NOT NULL,
                symbol        TEXT    NOT NULL,
                price_usd     REAL    NOT NULL,
                change_24h    REAL,               -- percentage
                market_cap    REAL,
                volume_24h    REAL,
                fetched_at    TEXT    NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_weather_city_time
                ON weather_snapshots(city, fetched_at);

            CREATE INDEX IF NOT EXISTS idx_crypto_coin_time
                ON crypto_snapshots(coin_id, fetched_at);
            """
        )


# ── Weather queries ────────────────────────────────────────────────────────────

def insert_weather(
    city: str,
    latitude: float,
    longitude: float,
    temperature: float,
    wind_speed: float,
    weather_code: int,
    fetched_at: str,
) -> None:
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO weather_snapshots
                (city, latitude, longitude, temperature, wind_speed,
                 weather_code, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (city, latitude, longitude, temperature,
             wind_speed, weather_code, fetched_at),
        )


def get_latest_weather(city: str) -> sqlite3.Row | None:
    with get_db() as conn:
        return conn.execute(
            """
            SELECT * FROM weather_snapshots
            WHERE city = ?
            ORDER BY fetched_at DESC
            LIMIT 1
            """,
            (city,),
        ).fetchone()


def get_weather_history(city: str, limit: int = 48) -> list[sqlite3.Row]:
    """Return up to `limit` snapshots for a city, newest first."""
    with get_db() as conn:
        return conn.execute(
            """
            SELECT * FROM weather_snapshots
            WHERE city = ?
            ORDER BY fetched_at DESC
            LIMIT ?
            """,
            (city, limit),
        ).fetchall()


def get_weather_stats(city: str) -> sqlite3.Row | None:
    """Aggregate stats (min / max / avg temperature) for a city."""
    with get_db() as conn:
        return conn.execute(
            """
            SELECT
                city,
                COUNT(*)            AS readings,
                ROUND(MIN(temperature), 2) AS temp_min,
                ROUND(MAX(temperature), 2) AS temp_max,
                ROUND(AVG(temperature), 2) AS temp_avg,
                MIN(fetched_at)     AS first_seen,
                MAX(fetched_at)     AS last_seen
            FROM weather_snapshots
            WHERE city = ?
            GROUP BY city
            """,
            (city,),
        ).fetchone()


# ── Crypto queries ─────────────────────────────────────────────────────────────

def insert_crypto(
    coin_id: str,
    symbol: str,
    price_usd: float,
    change_24h: float | None,
    market_cap: float | None,
    volume_24h: float | None,
    fetched_at: str,
) -> None:
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO crypto_snapshots
                (coin_id, symbol, price_usd, change_24h,
                 market_cap, volume_24h, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (coin_id, symbol, price_usd, change_24h,
             market_cap, volume_24h, fetched_at),
        )


def get_latest_crypto(coin_id: str) -> sqlite3.Row | None:
    with get_db() as conn:
        return conn.execute(
            """
            SELECT * FROM crypto_snapshots
            WHERE coin_id = ?
            ORDER BY fetched_at DESC
            LIMIT 1
            """,
            (coin_id,),
        ).fetchone()


def get_all_latest_crypto() -> list[sqlite3.Row]:
    """Return the most recent snapshot for every tracked coin."""
    with get_db() as conn:
        return conn.execute(
            """
            SELECT c.*
            FROM crypto_snapshots c
            INNER JOIN (
                SELECT coin_id, MAX(fetched_at) AS latest
                FROM crypto_snapshots
                GROUP BY coin_id
            ) AS sub ON c.coin_id = sub.coin_id AND c.fetched_at = sub.latest
            ORDER BY c.market_cap DESC NULLS LAST
            """
        ).fetchall()


def get_crypto_history(coin_id: str, limit: int = 48) -> list[sqlite3.Row]:
    with get_db() as conn:
        return conn.execute(
            """
            SELECT * FROM crypto_snapshots
            WHERE coin_id = ?
            ORDER BY fetched_at DESC
            LIMIT ?
            """,
            (coin_id, limit),
        ).fetchall()


def get_crypto_stats(coin_id: str) -> sqlite3.Row | None:
    with get_db() as conn:
        return conn.execute(
            """
            SELECT
                coin_id,
                symbol,
                COUNT(*)                    AS readings,
                ROUND(MIN(price_usd), 4)    AS price_min,
                ROUND(MAX(price_usd), 4)    AS price_max,
                ROUND(AVG(price_usd), 4)    AS price_avg,
                MIN(fetched_at)             AS first_seen,
                MAX(fetched_at)             AS last_seen
            FROM crypto_snapshots
            WHERE coin_id = ?
            GROUP BY coin_id, symbol
            """,
            (coin_id,),
        ).fetchone()
