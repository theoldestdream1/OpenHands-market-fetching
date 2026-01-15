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
        """Fetch initial historical data for all pairs and timeframes in a safe, serialized way."""
        print("Initializing historical data (serialized to avoid API rate limits)...")
    
        for pair in PAIRS:
            for timeframe in TIMEFRAMES:
                # Check if there is an available API key
                key = api_key_manager.get_available_key()
                while not key:
                    print("No available API keys, waiting 10 seconds...")
                    await asyncio.sleep(10)
                    key = api_key_manager.get_available_key()
            
                # Fetch initial history for this pair/timeframe
                candles = await fetch_initial_history(pair, timeframe)
             
                if candles:
                    data_storage.set_initial_data(pair, timeframe, candles)
                    print(f"Loaded {len(candles)} candles for {pair}/{timeframe}")
                else:
                    print(f"Failed to load initial data for {pair}/{timeframe}")
            
                # Delay to prevent per-minute limit violations
                await asyncio.sleep(8)  # ~1 request per 8 seconds per key
        
        self.is_initialized = True
        print("Historical data initialization complete")
    
    async def fetch_closed_candles(self):
        if not self.is_initialized:
            return
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
