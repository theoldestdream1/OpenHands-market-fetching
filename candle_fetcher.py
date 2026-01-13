"""Candle fetching logic for TwelveData - fetches ONLY closed candles."""

import httpx
from datetime import datetime, timezone
from config import TWELVEDATA_BASE_URL, TIMEFRAMES, ROLLING_LIMITS
from api_key_manager import api_key_manager


def is_candle_closed(timeframe: str, dt: datetime) -> bool:
    """Check if a candle should be closed at the given datetime.
    
    MODULE 6: Candle closure enforcement rules.
    """
    minute = dt.minute
    hour = dt.hour
    
    if timeframe == "1min":
        return True  # Every minute
    elif timeframe == "5min":
        return minute % 5 == 0
    elif timeframe == "15min":
        return minute % 15 == 0
    elif timeframe == "1h":
        return minute == 0
    elif timeframe == "4h":
        return hour % 4 == 0 and minute == 0
    return False


def get_timeframes_to_fetch(dt: datetime) -> list[str]:
    """Get list of timeframes that have a candle closing at the given time."""
    return [tf for tf in TIMEFRAMES if is_candle_closed(tf, dt)]


async def fetch_candles(pair: str, timeframe: str, outputsize: int = 1) -> list[dict] | None:
    """Fetch closed candles from TwelveData API.
    
    Returns list of candle data or None if request failed.
    """
    api_key = api_key_manager.get_available_key()
    if not api_key:
        print(f"No available API key for {pair}/{timeframe}")
        return None
    
    # Format pair for TwelveData (e.g., EURUSD -> EUR/USD, XAUUSD -> XAU/USD)
    if pair == "XAUUSD":
        symbol = "XAU/USD"
    else:
        symbol = f"{pair[:3]}/{pair[3:]}"
    
    params = {
        "symbol": symbol,
        "interval": timeframe,
        "outputsize": outputsize,
        "apikey": api_key,
        "timezone": "UTC"
    }
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(TWELVEDATA_BASE_URL, params=params)
            api_key_manager.record_usage(api_key)
            
            if response.status_code != 200:
                print(f"API error for {pair}/{timeframe}: {response.status_code}")
                return None
            
            data = response.json()
            
            if "values" not in data:
                print(f"No values in response for {pair}/{timeframe}: {data.get('message', 'Unknown error')}")
                return None
            
            # Return candles (most recent first from API)
            return data["values"]
    
    except Exception as e:
        print(f"Error fetching {pair}/{timeframe}: {e}")
        return None


async def fetch_initial_history(pair: str, timeframe: str) -> list[dict] | None:
    """Fetch initial candle history for a pair/timeframe."""
    outputsize = ROLLING_LIMITS.get(timeframe, 100)
    return await fetch_candles(pair, timeframe, outputsize)
