"""Market configuration for openhands-data-feeder."""

# MODULE 3: Market Configuration
PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD",
    "EURJPY", "GBPJPY", "AUDJPY", "EURCHF", "XAUUSD", "NZDUSD"
]

TIMEFRAMES = ["1min", "5min", "15min", "1h", "4h"]

# Mapping for API response keys
TIMEFRAME_DISPLAY = {
    "1min": "1m",
    "5min": "5m",
    "15min": "15m",
    "1h": "1h",
    "4h": "4h"
}

# MODULE 8: Rolling history limits per timeframe
ROLLING_LIMITS = {
    "1min": 500,
    "5min": 300,
    "15min": 200,
    "1h": 120,
    "4h": 100
}

# MODULE 4: TwelveData API limits
TWELVEDATA_DAILY_LIMIT = 800
TWELVEDATA_MINUTE_LIMIT = 8
TWELVEDATA_BASE_URL = "https://api.twelvedata.com/time_series"
