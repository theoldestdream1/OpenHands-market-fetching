"""API Key Management for TwelveData with rotation and rate limiting."""

import os
import time
from datetime import datetime, timezone
from threading import Lock
from config import TWELVEDATA_DAILY_LIMIT, TWELVEDATA_MINUTE_LIMIT


class APIKeyManager:
    def __init__(self):
        self.keys = []
        self.key_stats = {}
        self.lock = Lock()
        self._load_keys()
    
    def _load_keys(self):
        """Dynamically load all available API keys from environment variables."""
        for i in range(1, 37):
            key = os.environ.get(f"TWELVEDATA_KEY_{i}")
            if key:
                self.keys.append(key)
                self.key_stats[key] = {
                    "requests_today": 0,
                    "requests_this_minute": 0,
                    "last_used_timestamp": 0,
                    "minute_window_start": 0
                }
        print(f"Loaded {len(self.keys)} TwelveData API keys")
    
    def _reset_daily_counters_if_needed(self):
        """Reset daily counters at 00:00 UTC."""
        now = datetime.now(timezone.utc)
        for key in self.keys:
            stats = self.key_stats[key]
            last_used = stats["last_used_timestamp"]
            if last_used > 0:
                last_used_dt = datetime.fromtimestamp(last_used, timezone.utc)
                if last_used_dt.date() < now.date():
                    stats["requests_today"] = 0
    
    def _reset_minute_counter_if_needed(self, key: str):
        """Reset minute counter if a new minute window has started."""
        now = time.time()
        stats = self.key_stats[key]
        if now - stats["minute_window_start"] >= 60:
            stats["requests_this_minute"] = 0
            stats["minute_window_start"] = now
    
    def get_available_key(self) -> str | None:
        """Get the least-used available key that is not near limits."""
        with self.lock:
            self._reset_daily_counters_if_needed()
            
            available_keys = []
            for key in self.keys:
                self._reset_minute_counter_if_needed(key)
                stats = self.key_stats[key]
                
                # Skip keys near limits
                if stats["requests_today"] >= TWELVEDATA_DAILY_LIMIT - 1:
                    continue
                if stats["requests_this_minute"] >= TWELVEDATA_MINUTE_LIMIT - 1:
                    continue
                
                available_keys.append((key, stats["requests_today"]))
            
            if not available_keys:
                return None
            
            # Return the least-used key
            available_keys.sort(key=lambda x: x[1])
            return available_keys[0][0]
    
    def record_usage(self, key: str):
        """Record that a key was used for a request."""
        with self.lock:
            if key in self.key_stats:
                stats = self.key_stats[key]
                stats["requests_today"] += 1
                stats["requests_this_minute"] += 1
                stats["last_used_timestamp"] = time.time()
                if stats["minute_window_start"] == 0:
                    stats["minute_window_start"] = time.time()
    
    def get_stats(self) -> dict:
        """Get current stats for all keys."""
        with self.lock:
            return {
                "total_keys": len(self.keys),
                "key_stats": {
                    f"key_{i+1}": {
                        "requests_today": self.key_stats[key]["requests_today"],
                        "requests_this_minute": self.key_stats[key]["requests_this_minute"]
                    }
                    for i, key in enumerate(self.keys)
                }
            }


# Global instance
api_key_manager = APIKeyManager()
