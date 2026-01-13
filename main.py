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
    # Startup: Initialize data and start scheduler
    print("Starting openhands-data-feeder service...")
    
    # Start scheduler first
    candle_scheduler.start()
    
    # Initialize historical data in background
    asyncio.create_task(candle_scheduler.initialize_data())
    
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
    """Get market data for a specific pair.
    
    MODULE 9: External API endpoint for Lovable.
    
    Response format:
    {
        "pair": "GBPJPY",
        "timestamp": "ISO_UTC_TIME",
        "timeframes": {
            "1m": [],
            "5m": [],
            "15m": [],
            "1h": [],
            "4h": []
        }
    }
    """
    # Normalize pair to uppercase
    pair = pair.upper()
    
    # Validate pair
    if pair not in PAIRS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid pair: {pair}. Valid pairs: {', '.join(PAIRS)}"
        )
    
    # Get data for the pair
    timeframes_data = data_storage.get_pair_data(pair)
    
    return {
        "pair": pair,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "timeframes": timeframes_data
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=12000)
