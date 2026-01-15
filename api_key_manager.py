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
        self.last_daily_reset = datetime.now(timezone.utc).date()
        self._load_keys()

    def _load_keys(self):
        """Dynamically load all available API keys from environment variables."""
        for i in range(1, 37):
            key = os.environ.get(f"TWELVEDATA_KEY_{i}")
            if key and key not in self.key_stats:
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
        now_date = datetime.now(timezone.utc).date()
        if self.last_daily_reset < now_date:
            for key in self.keys:
                self.key_stats[key]["requests_today"] = 0
            self.last_daily_reset = now_date

    def _reset_minute_counter_if_needed(self, key: str):
        """Reset minute counter if a new minute window has started."""
        now = time.time()
        stats = self.key_stats[key]
        if stats["minute_window_start"] == 0 or now - stats["minute_window_start"] >= 60:
            stats["requests_this_minute"] = 0
            stats["minute_window_start"] = now

    def get_available_key(self) -> str | None:
        """Get the least-used available key that is not over limits."""
        with self.lock:
            self._reset_daily_counters_if_needed()

            available_keys = []
            for key in self.keys:
                self._reset_minute_counter_if_needed(key)
                stats = self.key_stats[key]

                if stats["requests_today"] >= TWELVEDATA_DAILY_LIMIT:
                    continue
                if stats["requests_this_minute"] >= TWELVEDATA_MINUTE_LIMIT:
                    continue

                available_keys.append((key, stats["requests_today"]))

            if not available_keys:
                return None

            # Return least-used key (daily)
            available_keys.sort(key=lambda x: x[1])
            return available_keys[0][0]

    def record_usage(self, key: str):
        """Record that a key was used for a request."""
        with self.lock:
            if key in self.key_stats:
                stats = self.key_stats[key]

                if stats["minute_window_start"] == 0:
                    stats["minute_window_start"] = time.time()

                stats["requests_today"] += 1
                stats["requests_this_minute"] += 1
                stats["last_used_timestamp"] = time.time()

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

    # ================== NEW FUNCTION ==================
    def reserve_key_for_request(self) -> tuple[str | None, float]:
        """
        Atomically reserve a key for a request.

        Returns:
            (key, 0) if available, or (None, seconds_to_next_minute_window) if all keys are maxed.
        """
        with self.lock:
            self._reset_daily_counters_if_needed()
            now = time.time()

            # Reset minute counters as needed
            for key in self.keys:
                self._reset_minute_counter_if_needed(key)

            # Collect all keys under both daily and per-minute limits
            available_keys = [
                (key, self.key_stats[key]["requests_this_minute"])
                for key in self.keys
                if self.key_stats[key]["requests_today"] < TWELVEDATA_DAILY_LIMIT
                and self.key_stats[key]["requests_this_minute"] < TWELVEDATA_MINUTE_LIMIT
            ]

            if available_keys:
                # Return the key with least usage in current minute
                available_keys.sort(key=lambda x: x[1])
                return available_keys[0][0], 0.0

            # All keys maxed, compute time to next available window
            times_to_wait = [
                60 - (now - self.key_stats[key]["minute_window_start"])
                for key in self.keys
            ]
            wait_time = max(0.5, min(times_to_wait))
            return None, wait_time


# Global instance
api_key_manager = APIKeyManager()
