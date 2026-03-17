"""
tests/test_db.py — Unit tests for the database layer.

Uses an in-memory SQLite database so no files are created on disk.
"""

import sqlite3
import sys
import os
import unittest
from unittest.mock import patch
from contextlib import contextmanager

# Point DB_PATH at an in-memory database before importing pulse.db
import pulse.db as db_module

# ── In-memory DB fixture ───────────────────────────────────────────────────────

_mem_conn = sqlite3.connect(":memory:", check_same_thread=False)
_mem_conn.row_factory = sqlite3.Row
_mem_conn.execute("PRAGMA foreign_keys=ON")


@contextmanager
def _mock_get_db():
    try:
        yield _mem_conn
        _mem_conn.commit()
    except Exception:
        _mem_conn.rollback()
        raise


class TestDatabase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Create schema in the in-memory DB once for all tests."""
        with patch.object(db_module, "get_db", _mock_get_db):
            _mem_conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS weather_snapshots (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    city          TEXT    NOT NULL,
                    latitude      REAL    NOT NULL,
                    longitude     REAL    NOT NULL,
                    temperature   REAL    NOT NULL,
                    wind_speed    REAL    NOT NULL,
                    weather_code  INTEGER NOT NULL,
                    fetched_at    TEXT    NOT NULL
                );

                CREATE TABLE IF NOT EXISTS crypto_snapshots (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    coin_id       TEXT    NOT NULL,
                    symbol        TEXT    NOT NULL,
                    price_usd     REAL    NOT NULL,
                    change_24h    REAL,
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

    def _insert_weather(self, city="Toronto", temp=20.0, ts="2024-01-01T12:00:00+00:00"):
        with patch.object(db_module, "get_db", _mock_get_db):
            db_module.insert_weather(
                city=city, latitude=43.7, longitude=-79.4,
                temperature=temp, wind_speed=15.0,
                weather_code=1, fetched_at=ts,
            )

    def _insert_crypto(self, coin="bitcoin", price=50000.0, change=2.5,
                       ts="2024-01-01T12:00:00+00:00"):
        with patch.object(db_module, "get_db", _mock_get_db):
            db_module.insert_crypto(
                coin_id=coin, symbol=coin[:3].upper(),
                price_usd=price, change_24h=change,
                market_cap=1e12, volume_24h=5e10,
                fetched_at=ts,
            )

    # ── Weather tests ──────────────────────────────────────────────────────────

    def test_insert_and_retrieve_weather(self):
        self._insert_weather("Toronto", temp=22.5)
        with patch.object(db_module, "get_db", _mock_get_db):
            row = db_module.get_latest_weather("Toronto")
        self.assertIsNotNone(row)
        self.assertEqual(row["city"], "Toronto")
        self.assertAlmostEqual(row["temperature"], 22.5)

    def test_get_latest_weather_returns_newest(self):
        self._insert_weather("London", temp=10.0, ts="2024-01-01T10:00:00+00:00")
        self._insert_weather("London", temp=15.0, ts="2024-01-01T11:00:00+00:00")
        with patch.object(db_module, "get_db", _mock_get_db):
            row = db_module.get_latest_weather("London")
        self.assertAlmostEqual(row["temperature"], 15.0)

    def test_get_latest_weather_missing_city(self):
        with patch.object(db_module, "get_db", _mock_get_db):
            row = db_module.get_latest_weather("Atlantis")
        self.assertIsNone(row)

    def test_weather_history_limit(self):
        for i in range(5):
            self._insert_weather("Tokyo", temp=float(i),
                                 ts=f"2024-01-0{i+1}T00:00:00+00:00")
        with patch.object(db_module, "get_db", _mock_get_db):
            rows = db_module.get_weather_history("Tokyo", limit=3)
        self.assertEqual(len(rows), 3)

    def test_weather_stats(self):
        self._insert_weather("Paris", temp=5.0,  ts="2024-02-01T00:00:00+00:00")
        self._insert_weather("Paris", temp=15.0, ts="2024-02-02T00:00:00+00:00")
        self._insert_weather("Paris", temp=25.0, ts="2024-02-03T00:00:00+00:00")
        with patch.object(db_module, "get_db", _mock_get_db):
            stats = db_module.get_weather_stats("Paris")
        self.assertEqual(stats["city"], "Paris")
        self.assertAlmostEqual(stats["temp_min"], 5.0)
        self.assertAlmostEqual(stats["temp_max"], 25.0)
        self.assertAlmostEqual(stats["temp_avg"], 15.0)

    # ── Crypto tests ───────────────────────────────────────────────────────────

    def test_insert_and_retrieve_crypto(self):
        self._insert_crypto("bitcoin", price=60000.0)
        with patch.object(db_module, "get_db", _mock_get_db):
            row = db_module.get_latest_crypto("bitcoin")
        self.assertIsNotNone(row)
        self.assertEqual(row["coin_id"], "bitcoin")
        self.assertAlmostEqual(row["price_usd"], 60000.0)

    def test_get_latest_crypto_returns_newest(self):
        self._insert_crypto("ethereum", price=3000.0, ts="2024-01-01T10:00:00+00:00")
        self._insert_crypto("ethereum", price=3500.0, ts="2024-01-01T11:00:00+00:00")
        with patch.object(db_module, "get_db", _mock_get_db):
            row = db_module.get_latest_crypto("ethereum")
        self.assertAlmostEqual(row["price_usd"], 3500.0)

    def test_get_latest_crypto_missing(self):
        with patch.object(db_module, "get_db", _mock_get_db):
            row = db_module.get_latest_crypto("nonexistent_coin_xyz")
        self.assertIsNone(row)

    def test_crypto_history_limit(self):
        for i in range(6):
            self._insert_crypto("solana", price=float(100 + i),
                                ts=f"2024-03-0{i+1}T00:00:00+00:00")
        with patch.object(db_module, "get_db", _mock_get_db):
            rows = db_module.get_crypto_history("solana", limit=4)
        self.assertEqual(len(rows), 4)

    def test_crypto_stats(self):
        self._insert_crypto("dogecoin", price=0.10, ts="2024-04-01T00:00:00+00:00")
        self._insert_crypto("dogecoin", price=0.20, ts="2024-04-02T00:00:00+00:00")
        self._insert_crypto("dogecoin", price=0.30, ts="2024-04-03T00:00:00+00:00")
        with patch.object(db_module, "get_db", _mock_get_db):
            stats = db_module.get_crypto_stats("dogecoin")
        self.assertAlmostEqual(stats["price_min"], 0.10, places=4)
        self.assertAlmostEqual(stats["price_max"], 0.30, places=4)
        self.assertAlmostEqual(stats["price_avg"], 0.20, places=4)

    def test_crypto_stats_missing(self):
        with patch.object(db_module, "get_db", _mock_get_db):
            stats = db_module.get_crypto_stats("fake_coin_abc")
        self.assertIsNone(stats)


if __name__ == "__main__":
    unittest.main()
