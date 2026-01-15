"""Scheduler for polling TwelveData at candle close times with time slicing."""

import asyncio
from datetime import datetime, timezone
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from config import PAIRS, TIMEFRAMES
from candle_fetcher import fetch_candles, fetch_initial_history, get_timeframes_to_fetch
from data_storage import data_storage
from api_key_manager import api_key_manager


# Timeframe priority: lower index = higher priority
TIMEFRAME_PRIORITY = ["1min", "5min", "15min", "1h", "4h"]


class CandleScheduler:
    def __init__(self):
        self.scheduler = AsyncIOScheduler(timezone="UTC")
        self.is_initialized = False
        self.started = False

        # Precompute stable per-pair offsets for time slicing
        self.pair_offsets = self._compute_pair_offsets()

    def _compute_pair_offsets(self):
        """
        Assign each pair a stable second offset within the minute.
        This spreads requests evenly across the minute.
        """
        offsets = {}
        pair_count = len(PAIRS)
        if pair_count == 0:
            return offsets

        slot_size = max(1, 60 // pair_count)
        for idx, pair in enumerate(PAIRS):
            offsets[pair] = idx * slot_size

        return offsets

    async def initialize_data(self):
        """Fetch initial historical data for all pairs and timeframes safely."""
        print("Initializing historical data (serialized with adaptive API key handling)...")

        for pair in PAIRS:
            for timeframe in TIMEFRAMES:
                while True:
                    key, wait_time = api_key_manager.reserve_key_for_request()
                    if key:
                        candles = await fetch_initial_history(pair, timeframe)
                        if candles:
                            data_storage.set_initial_data(pair, timeframe, candles)
                            print(f"Loaded {len(candles)} candles for {pair}/{timeframe}")
                            api_key_manager.record_usage(key)
                            break
                        else:
                            print(f"Fetch failed for {pair}/{timeframe}, retrying in 10s...")
                            await asyncio.sleep(10)
                    else:
                        print(f"No API keys available, waiting {wait_time:.2f}s...")
                        await asyncio.sleep(wait_time)

                await asyncio.sleep(0.2)

        self.is_initialized = True
        print("Historical data initialization complete")

    async def _fetch_pair_at_offset(self, pair, timeframes, delay):
        """
        Fetch closed candles for a single pair after a delay.
        This function NEVER waits for API keys in live mode.
        """
        await asyncio.sleep(delay)

        # Sort timeframes by priority
        sorted_tfs = sorted(
            timeframes,
            key=lambda tf: TIMEFRAME_PRIORITY.index(tf)
            if tf in TIMEFRAME_PRIORITY else len(TIMEFRAME_PRIORITY)
        )

        for timeframe in sorted_tfs:
            key, _ = api_key_manager.reserve_key_for_request()
            if not key:
                print(f"[LIVE SKIP] No API key available for {pair}/{timeframe}")
                continue

            candles = await fetch_candles(pair, timeframe, outputsize=1)
            if candles and len(candles) > 0:
                data_storage.add_single_candle(pair, timeframe, candles[0])

            api_key_manager.record_usage(key)

    async def minute_tick(self):
        """
        Runs once per minute and schedules time-sliced fetches.
        """
        if not self.is_initialized:
            return

        now = datetime.now(timezone.utc)
        timeframes_to_fetch = get_timeframes_to_fetch(now)

        if not timeframes_to_fetch:
            return

        print(f"[{now.isoformat()}] Scheduling fetches for: {timeframes_to_fetch}")

        for pair in PAIRS:
            offset = self.pair_offsets.get(pair, 0)
            asyncio.create_task(
                self._fetch_pair_at_offset(pair, timeframes_to_fetch, offset)
            )

    def start(self):
        if self.started:
            print("Scheduler already started, skipping")
            return

        self.started = True

        self.scheduler.add_job(
            self.minute_tick,
            CronTrigger(second=5),
            id="minute_tick",
            replace_existing=True
        )

        self.scheduler.start()
        print("Scheduler started with time-sliced polling")

    def stop(self):
        """Stop the scheduler."""
        self.scheduler.shutdown()


# Global instance
candle_scheduler = CandleScheduler()
