"""
Main application for openhands-data-feeder service.
"""

from contextlib import asynccontextmanager
from datetime import datetime, timezone
import os

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from config import PAIRS
from data_storage import data_storage
from scheduler import candle_scheduler
from api_key_manager import api_key_manager


# ─────────────────────────────────────────────────────────────
# Lifespan (startup / shutdown)
# ─────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Starting openhands-data-feeder service...")

    # BLOCKING initialization (must finish)
    await candle_scheduler.initialize_data()

    # Start scheduler only after init
    candle_scheduler.start()

    yield

    print("🛑 Shutting down openhands-data-feeder service...")
    candle_scheduler.stop()


# ─────────────────────────────────────────────────────────────
# FastAPI app
# ─────────────────────────────────────────────────────────────
app = FastAPI(
    title="openhands-data-feeder",
    description="Backend service for closed-candle Forex/Gold data",
    version="1.0.0",
    lifespan=lifespan
)

# ─────────────────────────────────────────────────────────────
# CORS
# ─────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {
        "status": "ok",
        "service": "openhands-data-feeder",
        "initialized": candle_scheduler.is_initialized
    }


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "initialized": candle_scheduler.is_initialized
    }


@app.get("/market-data")
async def get_market_data(
    pair: str = Query(..., description="Currency pair (e.g. GBPJPY)")
):
    pair = pair.upper()

    if pair not in PAIRS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid pair: {pair}. Valid pairs: {', '.join(PAIRS)}"
        )

    return {
        "pair": pair,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "timeframes": data_storage.get_pair_data(pair)
    }


@app.get("/market-data/all")
async def get_all_market_data():
    return {
        pair: {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeframes": data_storage.get_pair_data(pair)
        }
        for pair in PAIRS
    }


@app.get("/stats")
async def get_stats():
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "api_keys": api_key_manager.get_stats(),
        "storage": data_storage.get_stats()
    }


# ─────────────────────────────────────────────────────────────
# Local run
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
