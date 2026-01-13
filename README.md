# openhands-data-feeder

Backend service for fetching closed-candle Forex/Gold market data from TwelveData.

## Features

- Fetches closed candles only (never forming candles)
- Rotates multiple TwelveData API keys safely
- Maintains rolling candle history per pair & timeframe
- Exposes REST API for Lovable consumption

## Supported Pairs (12)

EURUSD, GBPUSD, USDJPY, USDCHF, USDCAD, AUDUSD, EURJPY, GBPJPY, AUDJPY, EURCHF, XAUUSD, NZDUSD

## Supported Timeframes (5)

1m, 5m, 15m, 1h, 4h

## API Endpoints

### GET /market-data?pair=GBPJPY

Returns market data for a specific pair.

Response format:
```json
{
  "pair": "GBPJPY",
  "timestamp": "ISO_UTC_TIME",
  "timeframes": {
    "1m": [],
    "5m": [],
    "15m": [],
    "1h": [],
    "4h": []
  }
}
```

### GET /health

Health check endpoint.

### GET /stats

Service statistics including API key usage and storage stats.

## Environment Variables

Set TwelveData API keys as environment variables:
- `TWELVEDATA_KEY_1`
- `TWELVEDATA_KEY_2`
- ... up to `TWELVEDATA_KEY_36`

## Railway Deployment

1. Create a new project on Railway
2. Connect your GitHub repository
3. Add environment variables for your TwelveData API keys
4. Deploy

The service will automatically:
- Load all available API keys
- Initialize historical data on startup
- Poll TwelveData at candle close times
- Maintain rolling history per pair/timeframe

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export TWELVEDATA_KEY_1=your_api_key

# Run the service
uvicorn main:app --host 0.0.0.0 --port 8000
```
