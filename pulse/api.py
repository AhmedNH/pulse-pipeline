"""
api.py — FastAPI REST server for Pulse.

Run with:
    python main.py api

Endpoints:
    GET /weather/{city}              Latest snapshot for a city
    GET /weather/{city}/history      Recent snapshots (query: limit)
    GET /weather/{city}/stats        Aggregate stats
    GET /crypto                      Latest snapshot for all tracked coins
    GET /crypto/{coin_id}            Latest snapshot for one coin
    GET /crypto/{coin_id}/history    Recent snapshots (query: limit)
    GET /crypto/{coin_id}/stats      Aggregate stats
    POST /pipeline/run               Trigger a manual pipeline run
    GET  /health                     Health check
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from pulse import db, pipeline

app = FastAPI(
    title       = "Pulse API",
    description = "Real-time weather and crypto data pipeline",
    version     = "1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins  = ["*"],
    allow_methods  = ["*"],
    allow_headers  = ["*"],
)


def _row_to_dict(row) -> dict | None:
    if row is None:
        return None
    return dict(row)


# ── Health ─────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
def health() -> dict:
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


# ── Weather endpoints ──────────────────────────────────────────────────────────

@app.get("/weather/{city}", tags=["Weather"])
def get_weather(city: str) -> dict[str, Any]:
    """Return the most recent weather snapshot for a city."""
    row = db.get_latest_weather(city)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"No weather data found for '{city}'. "
                   f"Run the pipeline first: python main.py run",
        )
    return _row_to_dict(row)


@app.get("/weather/{city}/history", tags=["Weather"])
def get_weather_history(
    city:  str,
    limit: int = Query(default=24, ge=1, le=200),
) -> list[dict]:
    """Return recent weather snapshots for a city, newest first."""
    rows = db.get_weather_history(city, limit=limit)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No history for '{city}'.")
    return [_row_to_dict(r) for r in rows]


@app.get("/weather/{city}/stats", tags=["Weather"])
def get_weather_stats(city: str) -> dict[str, Any]:
    """Return aggregate temperature statistics for a city."""
    row = db.get_weather_stats(city)
    if row is None:
        raise HTTPException(status_code=404, detail=f"No data for '{city}'.")
    return _row_to_dict(row)


# ── Crypto endpoints ───────────────────────────────────────────────────────────

@app.get("/crypto", tags=["Crypto"])
def get_all_crypto() -> list[dict]:
    """Return the latest snapshot for every tracked coin."""
    rows = db.get_all_latest_crypto()
    if not rows:
        raise HTTPException(
            status_code=404,
            detail="No crypto data found. Run the pipeline first.",
        )
    return [_row_to_dict(r) for r in rows]


@app.get("/crypto/{coin_id}", tags=["Crypto"])
def get_crypto(coin_id: str) -> dict[str, Any]:
    """Return the most recent snapshot for a single coin."""
    row = db.get_latest_crypto(coin_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"No data for '{coin_id}'.")
    return _row_to_dict(row)


@app.get("/crypto/{coin_id}/history", tags=["Crypto"])
def get_crypto_history(
    coin_id: str,
    limit:   int = Query(default=24, ge=1, le=200),
) -> list[dict]:
    rows = db.get_crypto_history(coin_id, limit=limit)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No history for '{coin_id}'.")
    return [_row_to_dict(r) for r in rows]


@app.get("/crypto/{coin_id}/stats", tags=["Crypto"])
def get_crypto_stats(coin_id: str) -> dict[str, Any]:
    row = db.get_crypto_stats(coin_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"No data for '{coin_id}'.")
    return _row_to_dict(row)


# ── Pipeline control ───────────────────────────────────────────────────────────

@app.post("/pipeline/run", tags=["Pipeline"])
def trigger_pipeline() -> dict:
    """Manually trigger a full pipeline run (fetch + store)."""
    result = pipeline.run_all_jobs()
    return {"status": "ok", "stored": result}
