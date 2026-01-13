"""Data storage for candle history with rolling limits."""

from threading import Lock
from collections import deque
from config import PAIRS, TIMEFRAMES, ROLLING_LIMITS, TIMEFRAME_DISPLAY


class DataStorage:
    """In-memory storage for candle data with rolling history limits."""
    
    def __init__(self):
        self.lock = Lock()
        self.data = {}
        self._initialize_storage()
    
    def _initialize_storage(self):
        """Initialize storage structure for all pairs and timeframes."""
        for pair in PAIRS:
            self.data[pair] = {}
            for timeframe in TIMEFRAMES:
                limit = ROLLING_LIMITS.get(timeframe, 100)
                self.data[pair][timeframe] = deque(maxlen=limit)
    
    def add_candles(self, pair: str, timeframe: str, candles: list[dict]):
        """Add candles to storage, maintaining rolling limit.
        
        Candles should be in chronological order (oldest first).
        """
        if pair not in self.data or timeframe not in self.data[pair]:
            return
        
        with self.lock:
            storage = self.data[pair][timeframe]
            
            # Get existing timestamps to avoid duplicates
            existing_timestamps = {c.get("datetime") for c in storage}
            
            for candle in candles:
                if candle.get("datetime") not in existing_timestamps:
                    storage.append(candle)
                    existing_timestamps.add(candle.get("datetime"))
    
    def add_single_candle(self, pair: str, timeframe: str, candle: dict):
        """Add a single candle to storage."""
        if pair not in self.data or timeframe not in self.data[pair]:
            return
        
        with self.lock:
            storage = self.data[pair][timeframe]
            
            # Check for duplicate
            existing_timestamps = {c.get("datetime") for c in storage}
            if candle.get("datetime") not in existing_timestamps:
                storage.append(candle)
    
    def get_candles(self, pair: str, timeframe: str) -> list[dict]:
        """Get all candles for a pair/timeframe."""
        if pair not in self.data or timeframe not in self.data[pair]:
            return []
        
        with self.lock:
            return list(self.data[pair][timeframe])
    
    def get_pair_data(self, pair: str) -> dict:
        """Get all timeframe data for a pair in the API response format."""
        if pair not in self.data:
            return {}
        
        result = {}
        with self.lock:
            for timeframe in TIMEFRAMES:
                display_key = TIMEFRAME_DISPLAY.get(timeframe, timeframe)
                result[display_key] = list(self.data[pair][timeframe])
        
        return result
    
    def set_initial_data(self, pair: str, timeframe: str, candles: list[dict]):
        """Set initial candle data (replaces existing data).
        
        Candles from API are most recent first, so we reverse them.
        """
        if pair not in self.data or timeframe not in self.data[pair]:
            return
        
        with self.lock:
            storage = self.data[pair][timeframe]
            storage.clear()
            
            # Reverse to get chronological order (oldest first)
            for candle in reversed(candles):
                storage.append(candle)
    
    def get_stats(self) -> dict:
        """Get storage statistics."""
        with self.lock:
            stats = {}
            for pair in PAIRS:
                stats[pair] = {}
                for timeframe in TIMEFRAMES:
                    display_key = TIMEFRAME_DISPLAY.get(timeframe, timeframe)
                    stats[pair][display_key] = len(self.data[pair][timeframe])
            return stats


# Global instance
data_storage = DataStorage()
