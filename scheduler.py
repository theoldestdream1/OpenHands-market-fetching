"""Scheduler for polling TwelveData at candle close times."""

import asyncio
from datetime import datetime, timezone
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from config import PAIRS, TIMEFRAMES
from candle_fetcher import fetch_candles, fetch_initial_history, get_timeframes_to_fetch
from data_storage import data_storage
from api_key_manager import api_key_manager


class CandleScheduler:
    def __init__(self):
        self.scheduler = AsyncIOScheduler(timezone="UTC")
        self.is_initialized = False
        self.started = False

    async def initialize_data(self):
        """Fetch initial historical data for all pairs and timeframes safely."""
        print("Initializing historical data (serialized with adaptive API key handling)...")

        for pair in PAIRS:
            for timeframe in TIMEFRAMES:
                while True:
                    key, wait_time = api_key_manager.reserve_key_for_request()
                    if key:
                        # Fetch initial history
                        candles = await fetch_initial_history(pair, timeframe)
                        if candles:
                            data_storage.set_initial_data(pair, timeframe, candles)
                            print(f"Loaded {len(candles)} candles for {pair}/{timeframe}")
                            # Record usage
                            api_key_manager.record_usage(key)
                            break
                        else:
                            print(f"Fetch failed for {pair}/{timeframe}, retrying in 10s...")
                            await asyncio.sleep(10)
                    else:
                        # Wait until some key is available
                        print(f"No API keys available, waiting {wait_time:.2f}s...")
                        await asyncio.sleep(wait_time)

                # Small delay to avoid hitting per-minute limits
                await asyncio.sleep(0.2)

        self.is_initialized = True
        print("Historical data initialization complete")

    async def fetch_closed_candles(self):
        if not self.is_initialized:
            return

        now = datetime.now(timezone.utc)
        timeframes_to_fetch = get_timeframes_to_fetch(now)

        if not timeframes_to_fetch:
            return

        print(f"[{now.isoformat()}] Fetching candles for timeframes: {timeframes_to_fetch}")

        for pair in PAIRS:
            for timeframe in timeframes_to_fetch:
                while True:
                    key, wait_time = api_key_manager.reserve_key_for_request()
                    if key:
                        # Fetch the most recent closed candle
                        candles = await fetch_candles(pair, timeframe, outputsize=1)
                        if candles and len(candles) > 0:
                            data_storage.add_single_candle(pair, timeframe, candles[0])
                        # Record usage
                        api_key_manager.record_usage(key)
                        break
                    else:
                        print(f"No API keys available, waiting {wait_time:.2f}s for {pair}/{timeframe}...")
                        await asyncio.sleep(wait_time)

                # Small delay to respect rate limits
                await asyncio.sleep(0.2)

    def start(self):
        if self.started:
            print("Scheduler already started, skipping")
            return

        self.started = True
        # Run every minute to check for candle closures
        self.scheduler.add_job(
            self.fetch_closed_candles,
            CronTrigger(second=5),  # Run at 5 seconds past each minute
            id="candle_fetcher",
            replace_existing=True
        )

        self.scheduler.start()
        print("Scheduler started - polling every minute at :05 seconds")

    def stop(self):
        """Stop the scheduler."""
        self.scheduler.shutdown()


# Global instance
candle_scheduler = CandleScheduler()
