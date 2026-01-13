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
    
    async def initialize_data(self):
        """Fetch initial historical data for all pairs and timeframes."""
        print("Initializing historical data...")
        
        for pair in PAIRS:
            for timeframe in TIMEFRAMES:
                # Check if we have an available key
                if not api_key_manager.get_available_key():
                    print("No available API keys, waiting 60 seconds...")
                    await asyncio.sleep(60)
                
                candles = await fetch_initial_history(pair, timeframe)
                if candles:
                    data_storage.set_initial_data(pair, timeframe, candles)
                    print(f"Loaded {len(candles)} candles for {pair}/{timeframe}")
                else:
                    print(f"Failed to load initial data for {pair}/{timeframe}")
                
                # Small delay to respect rate limits
                await asyncio.sleep(0.5)
        
        self.is_initialized = True
        print("Historical data initialization complete")
    
    async def fetch_closed_candles(self):
        """Fetch newly closed candles for all pairs and applicable timeframes."""
        now = datetime.now(timezone.utc)
        timeframes_to_fetch = get_timeframes_to_fetch(now)
        
        if not timeframes_to_fetch:
            return
        
        print(f"[{now.isoformat()}] Fetching candles for timeframes: {timeframes_to_fetch}")
        
        for pair in PAIRS:
            for timeframe in timeframes_to_fetch:
                # Check if we have an available key
                if not api_key_manager.get_available_key():
                    print("No available API keys, skipping this cycle")
                    return
                
                # Fetch only the most recent closed candle
                candles = await fetch_candles(pair, timeframe, outputsize=1)
                if candles and len(candles) > 0:
                    # The API returns most recent first, so take the first one
                    data_storage.add_single_candle(pair, timeframe, candles[0])
                
                # Small delay to respect rate limits
                await asyncio.sleep(0.2)
    
    def start(self):
        """Start the scheduler with cron jobs for each timeframe."""
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
