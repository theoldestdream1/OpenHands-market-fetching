"""Main application for openhands-data-feeder service."""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from config import PAIRS, TIMEFRAME_DISPLAY
from data_storage import data_storage
from scheduler import candle_scheduler
from api_key_manager import api_key_manager


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    print("Starting openhands-data-feeder service...")

    # 🔒 BLOCKING historical initialization — MUST finish
    await candle_scheduler.initialize_data()

    # Start scheduler ONLY after full initialization
    candle_scheduler.start()

    yield

    # Shutdown
    print("Shutting down openhands-data-feeder service...")
    candle_scheduler.stop()


app = FastAPI(
    title="openhands-data-feeder",
    description="Backend service for fetching closed-candle Forex/Gold market data from TwelveData",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware for Lovable consumption
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/market-data")
async def get_market_data(pair: str = Query(..., description="Currency pair (e.g., GBPJPY)")):
    pair = pair.upper()

    if pair not in PAIRS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid pair: {pair}. Valid pairs: {', '.join(PAIRS)}"
        )

    timeframes_data = data_storage.get_pair_data(pair)

    return {
        "pair": pair,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "timeframes": timeframes_data
    }


@app.get("/market-data/all")
async def get_all_market_data():
    result = {}
    for pair in PAIRS:
        result[pair] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeframes": data_storage.get_pair_data(pair)
        }
    return result


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "initialized": candle_scheduler.is_initialized
    }


@app.get("/stats")
async def get_stats():
    """Get service statistics."""
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "api_keys": api_key_manager.get_stats(),
        "storage": data_storage.get_stats()
    }


@app.get("/")
async def root():
    return {
        "status": "ok",
        "service": "openhands-data-feeder",
        "initialized": candle_scheduler.is_initialized
    }
    

if __name__ == "__main__":
    import os
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
