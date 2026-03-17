"""
tests/test_fetcher.py — Unit tests for the fetcher layer.

All HTTP calls are mocked — no network access required.
"""

import unittest
from unittest.mock import patch, MagicMock

import pulse.fetcher as fetcher_module


MOCK_WEATHER_RESPONSE = {
    "current": {
        "temperature_2m": 18.5,
        "wind_speed_10m": 12.3,
        "weather_code":   1,
    }
}

MOCK_CRYPTO_RESPONSE = [
    {
        "id":                              "bitcoin",
        "symbol":                          "btc",
        "current_price":                   65000.0,
        "price_change_percentage_24h":     2.5,
        "market_cap":                      1_200_000_000_000,
        "total_volume":                    45_000_000_000,
    },
    {
        "id":                              "ethereum",
        "symbol":                          "eth",
        "current_price":                   3500.0,
        "price_change_percentage_24h":    -1.2,
        "market_cap":                      420_000_000_000,
        "total_volume":                    18_000_000_000,
    },
]


class TestFetcher(unittest.TestCase):

    @patch.object(fetcher_module, "_get", return_value=MOCK_WEATHER_RESPONSE)
    def test_fetch_weather_known_city(self, mock_get):
        result = fetcher_module.fetch_weather("Toronto")
        self.assertEqual(result["city"], "Toronto")
        self.assertAlmostEqual(result["temperature"], 18.5)
        self.assertAlmostEqual(result["wind_speed"], 12.3)
        self.assertEqual(result["weather_code"], 1)
        mock_get.assert_called_once()

    def test_fetch_weather_unknown_city(self):
        with self.assertRaises(ValueError):
            fetcher_module.fetch_weather("Narnia")

    @patch.object(fetcher_module, "_get", return_value=MOCK_WEATHER_RESPONSE)
    def test_fetch_all_weather_returns_list(self, _mock):
        results = fetcher_module.fetch_all_weather()
        self.assertIsInstance(results, list)
        self.assertEqual(len(results), len(fetcher_module.CITIES))

    @patch.object(fetcher_module, "_get", return_value=MOCK_CRYPTO_RESPONSE)
    def test_fetch_crypto_parses_correctly(self, mock_get):
        results = fetcher_module.fetch_crypto(["bitcoin", "ethereum"])
        self.assertEqual(len(results), 2)

        btc = results[0]
        self.assertEqual(btc["coin_id"],    "bitcoin")
        self.assertEqual(btc["symbol"],     "BTC")
        self.assertAlmostEqual(btc["price_usd"],  65000.0)
        self.assertAlmostEqual(btc["change_24h"],  2.5)

        eth = results[1]
        self.assertEqual(eth["coin_id"],    "ethereum")
        self.assertAlmostEqual(eth["change_24h"], -1.2)

    @patch.object(fetcher_module, "_get", side_effect=RuntimeError("network down"))
    def test_fetch_crypto_raises_on_failure(self, _mock):
        with self.assertRaises(RuntimeError):
            fetcher_module.fetch_crypto(["bitcoin"])

    @patch.object(fetcher_module, "_get", return_value=MOCK_WEATHER_RESPONSE)
    def test_fetch_all_weather_skips_failures(self, mock_get):
        """fetch_all_weather should catch per-city errors and continue."""
        call_count = [0]
        original = fetcher_module.fetch_weather

        def flaky_fetch(city):
            call_count[0] += 1
            if call_count[0] == 2:
                raise RuntimeError("simulated failure")
            return original(city)

        with patch.object(fetcher_module, "fetch_weather", side_effect=flaky_fetch):
            results = fetcher_module.fetch_all_weather()

        # One city failed; the rest should still be returned
        self.assertEqual(len(results), len(fetcher_module.CITIES) - 1)


if __name__ == "__main__":
    unittest.main()
