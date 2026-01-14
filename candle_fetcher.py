"""Candle fetching logic for TwelveData - fetches ONLY closed candles."""

import httpx
import asyncio
from datetime import datetime, timezone, timedelta
# Ensure typing compatibility for Python < 3.10
from typing import List, Dict, Optional, Union

# Try/Except block handles if you forgot to push local config files
try:
    from config import TWELVEDATA_BASE_URL, TIMEFRAMES, ROLLING_LIMITS
    from api_key_manager import api_key_manager
except ImportError as e:
    raise ImportError(f"Missing configuration file on this machine: {e}")

# Use a context manager or a getter for the client to avoid global scope issues
# For simple scripts, a global is okay, but we will add a shutdown hook if needed.
# Ideally, pass the client into the function, but for this structure:
http_client = httpx.AsyncClient(timeout=30.0)

def is_candle_closed(timeframe: str, dt: datetime) -> bool:
    """Check if a candle should be closed at the given datetime (UTC)."""
    # Note: This logic triggers the fetch. It assumes standard clock alignment.
    minute = dt.minute
    hour = dt.hour

    if timeframe == "1min":
        return True
    elif timeframe == "5min":
        return minute % 5 == 0
    elif timeframe == "15min":
        return minute % 15 == 0
    elif timeframe == "1h":
        return minute == 0
    elif timeframe == "4h":
        return hour % 4 == 0 and minute == 0
    return False

def get_timeframes_to_fetch(dt: datetime) -> List[str]:
    """Return timeframes that have a candle closing at this time."""
    return [tf for tf in TIMEFRAMES if is_candle_closed(tf, dt)]

def _parse_timestamp(ts: str) -> datetime:
    """Parse TwelveData timestamp safely as UTC."""
    # TwelveData returns "2023-01-01 00:00:00". We force UTC.
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

def _is_confirmed_closed(candle_ts: datetime, now: datetime, timeframe: str) -> bool:
    """
    Check if the candle is definitively closed based on the current time.
    Logic: If current time >= candle_time + interval, it's closed.
    """
    # Define intervals in minutes
    intervals = {
        "1min": 1, "5min": 5, "15min": 15, "1h": 60, "4h": 240
    }
    minutes = intervals.get(timeframe)
    if not minutes:
        return False # Unknown timeframe, assume not closed/valid
    
    # Calculate when this specific candle closes
    candle_close_time = candle_ts + timedelta(minutes=minutes)
    
    # It is closed if NOW is past the close time
    return now >= candle_close_time

async def fetch_candles(pair: str, timeframe: str, outputsize: int = 1) -> Optional[List[Dict]]:
    """Fetch ONLY confirmed closed candles from TwelveData."""
    api_key = api_key_manager.get_available_key()
    if not api_key:
        print(f"No available API key for {pair}/{timeframe}")
        return None

    symbol = "XAU/USD" if pair == "XAUUSD" else f"{pair[:3]}/{pair[3:]}"

    params = {
        "symbol": symbol,
        "interval": timeframe,
        "outputsize": outputsize + 1, # Fetch 1 extra to ensure we get the last CLOSED one if the latest is OPEN
        "apikey": api_key,
        "timezone": "UTC"
    }

    try:
        response = await http_client.get(TWELVEDATA_BASE_URL, params=params)

        if response.status_code != 200:
            print(f"TwelveData HTTP {response.status_code} for {pair}/{timeframe}")
            return None

        data = response.json()

        if "values" not in data:
            print(f"TwelveData error for {pair}/{timeframe}: {data.get('message')}")
            return None

        now = datetime.now(timezone.utc)
        confirmed_candles = []

        # values usually come oldest to newest or newest to oldest. 
        # TwelveData default is newest first (index 0 is latest).
        for candle in data["values"]:
            candle_ts = _parse_timestamp(candle["datetime"])
            
            # Use strict time math to determine if closed
            if _is_confirmed_closed(candle_ts, now, timeframe):
                confirmed_candles.append(candle)
        
        # If we asked for N candles, ensure we return N closed candles
        # (Since we fetched N+1, we usually drop the first one if it was open)
        confirmed_candles = confirmed_candles[:outputsize]

        if confirmed_candles:
            api_key_manager.record_usage(api_key)
            return confirmed_candles

        return None

    except Exception as e:
        print(f"Fetch error {pair}/{timeframe}: {e}")
        return None

async def fetch_initial_history(pair: str, timeframe: str) -> Optional[List[Dict]]:
    """Fetch initial rolling history for a pair/timeframe."""
    outputsize = ROLLING_LIMITS.get(timeframe, 100)
    return await fetch_candles(pair, timeframe, outputsize)

# Optional: Helper to close client if you stop the app
async def close_client():
    await http_client.aclose()
