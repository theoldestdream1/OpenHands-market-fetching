"""Candle fetching logic for TwelveData - fetches ONLY closed candles."""

import httpx
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional
import asyncio

try:
    from config import TWELVEDATA_BASE_URL, TIMEFRAMES, ROLLING_LIMITS
    from api_key_manager import api_key_manager
except ImportError as e:
    raise ImportError(f"Missing configuration file on this machine: {e}")

http_client = httpx.AsyncClient(timeout=30.0)


def is_candle_closed(timeframe: str, dt: datetime) -> bool:
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
    return [tf for tf in TIMEFRAMES if is_candle_closed(tf, dt)]


def _parse_timestamp(ts: str) -> datetime:
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _is_confirmed_closed(candle_ts: datetime, now: datetime, timeframe: str) -> bool:
    intervals = {"1min": 1, "5min": 5, "15min": 15, "1h": 60, "4h": 240}
    minutes = intervals.get(timeframe)
    if not minutes:
        return False
    return now >= candle_ts + timedelta(minutes=minutes)


async def fetch_candles(pair: str, timeframe: str, outputsize: int = 1) -> Optional[List[Dict]]:
    """
    Fetch ONLY confirmed closed candles from TwelveData.
    Handles key reservation and adaptive waiting internally.
    """
    # Reserve a key and wait if all keys are maxed
    key, wait_time = api_key_manager.reserve_key_for_request()
    while not key:
        await asyncio.sleep(wait_time)
        key, wait_time = api_key_manager.reserve_key_for_request()

    symbol = "XAU/USD" if pair == "XAUUSD" else f"{pair[:3]}/{pair[3:]}"
    params = {
        "symbol": symbol,
        "interval": timeframe,
        "outputsize": outputsize + 1,
        "apikey": key,
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

        for candle in data["values"]:
            candle_ts = _parse_timestamp(candle["datetime"])
            if _is_confirmed_closed(candle_ts, now, timeframe):
                confirmed_candles.append(candle)

        confirmed_candles = confirmed_candles[:outputsize]

        if confirmed_candles:
            # Record usage only after successful fetch
            api_key_manager.record_usage(key)
            return confirmed_candles

        return None

    except Exception as e:
        print(f"Fetch error {pair}/{timeframe}: {e}")
        return None


async def fetch_initial_history(pair: str, timeframe: str) -> Optional[List[Dict]]:
    outputsize = ROLLING_LIMITS.get(timeframe, 100)
    return await fetch_candles(pair, timeframe, outputsize)


async def close_client():
    await http_client.aclose()
