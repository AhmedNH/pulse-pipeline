# Pulse

A real-time data aggregation pipeline that continuously fetches, stores, and exposes weather and cryptocurrency market data through a REST API and a terminal dashboard.

---

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌──────────────┐
│   Data Sources  │     │    Pipeline       │     │   Storage    │
│                 │     │                  │     │              │
│  Open-Meteo API │────▶│  fetcher.py      │────▶│  SQLite      │
│  (weather)      │     │  (HTTP + retry)  │     │  (WAL mode)  │
│                 │     │                  │     │              │
│  CoinGecko API  │────▶│  pipeline.py     │────▶│  Indexed     │
│  (crypto)       │     │  (scheduling)    │     │  time-series │
└─────────────────┘     └──────────────────┘     └──────┬───────┘
                                                         │
                                          ┌──────────────┼──────────────┐
                                          │              │              │
                                    ┌─────▼──────┐ ┌────▼──────┐ ┌────▼──────┐
                                    │  REST API  │ │    CLI    │ │  Queries  │
                                    │ (FastAPI)  │ │  (Rich)   │ │  (db.py)  │
                                    └────────────┘ └───────────┘ └───────────┘
```

**Data sources** — both free, no API key required:
- [Open-Meteo](https://open-meteo.com/) — weather for 8 global cities, updated every 30 minutes
- [CoinGecko](https://www.coingecko.com/en/api) — prices for 8 cryptocurrencies, updated every 5 minutes

---

## Setup

```bash
pip install -r requirements.txt
```

---

## Usage

**Fetch and store one round of data:**
```bash
python main.py run
```

**Run on a schedule (weather every 30m, crypto every 5m):**
```bash
python main.py schedule
python main.py schedule --weather-interval 60 --crypto-interval 10
```

**Terminal dashboard:**
```bash
python main.py dashboard
```

**Query specific data:**
```bash
python main.py weather                    # All cities
python main.py weather Toronto            # One city
python main.py weather-stats Toronto      # Aggregate stats

python main.py crypto                     # All coins
python main.py crypto bitcoin             # One coin
python main.py crypto-stats bitcoin       # Aggregate stats
```

**Start the REST API server:**
```bash
python main.py api
# → http://localhost:8000
# → http://localhost:8000/docs  (auto-generated Swagger UI)
```

---

## REST API

| Method | Endpoint                        | Description                        |
|--------|---------------------------------|------------------------------------|
| GET    | `/weather/{city}`               | Latest weather snapshot            |
| GET    | `/weather/{city}/history`       | Recent snapshots (`?limit=24`)     |
| GET    | `/weather/{city}/stats`         | Min / max / avg temperature        |
| GET    | `/crypto`                       | Latest prices for all coins        |
| GET    | `/crypto/{coin_id}`             | Latest price for one coin          |
| GET    | `/crypto/{coin_id}/history`     | Recent snapshots (`?limit=24`)     |
| GET    | `/crypto/{coin_id}/stats`       | Min / max / avg price              |
| POST   | `/pipeline/run`                 | Trigger a manual pipeline run      |
| GET    | `/health`                       | Health check                       |

---

## Project Structure

```
pulse/
├── pulse/
│   ├── db.py           # SQLite storage layer — all schema and queries
│   ├── fetcher.py      # HTTP fetching with retry logic
│   ├── pipeline.py     # Orchestration and scheduling
│   ├── api.py          # FastAPI REST server
│   └── cli.py          # Rich terminal dashboard
├── tests/
│   ├── test_db.py      # 11 database unit tests (in-memory SQLite)
│   └── test_fetcher.py # 6 fetcher unit tests (fully mocked)
├── main.py             # Entry point
└── requirements.txt
```

### Key implementation decisions

| Concern | Decision |
|---|---|
| Storage | SQLite with WAL journal mode for concurrent read/write |
| Indexing | Composite index on `(city/coin_id, fetched_at)` for fast time-series queries |
| Concurrency | Thread-local connections — safe to call from API and scheduler simultaneously |
| Resilience | Exponential-backoff retry on all HTTP calls; per-source failures don't halt the pipeline |
| Scheduling | `schedule` library with 10-second polling loop; clean Ctrl-C shutdown |
| Testing | All tests run offline — HTTP is mocked, DB uses in-memory SQLite |

---

## Running Tests

```bash
python -m unittest discover -v tests/
# Ran 17 tests in 0.005s — OK
```
