# Beanance Market Backend

Realtime market data service for Beanance, powered by Binance USDⓈ-M Perpetual Futures.

The first release only includes active USDT-margined perpetual contracts. It does not place orders and does not require Binance API keys.

## Requirements

- Node.js 20 or newer
- npm

## Setup

```bash
git clone https://github.com/YosmerryGod/Beanance-market-backend.git
cd Beanance-market-backend
npm install
cp .env.example .env
npm start
```

The service listens on port `3030` by default.

## Market eligibility

The service loads Binance USDⓈ-M exchange information from:

`GET https://fapi.binance.com/fapi/v1/exchangeInfo`

A contract is included only when all conditions match:

- `status === "TRADING"`
- `contractType === "PERPETUAL"`
- `quoteAsset === "USDT"`
- `marginAsset === "USDT"`

Initial 24-hour ticker data is loaded from:

`GET https://fapi.binance.com/fapi/v1/ticker/24hr`

Live updates are consumed from Binance's USDⓈ-M all-market ticker stream:

`wss://fstream.binance.com/market/stream?streams=!ticker@arr`

Incoming ticker messages are matched against the active symbol set loaded from USDⓈ-M exchange information. This prevents non-USDT-M or inactive symbols from entering the Beanance market cache.

## REST API

### Health

`GET /health`

### Active futures markets

`GET /api/v1/market/futures`

Optional query parameters:

- `sort=symbol|quoteVolume|priceChangePercent`
- `order=asc|desc`

### One futures market

`GET /api/v1/market/futures/BTCUSDT`

### Tickers sorted by quote volume

`GET /api/v1/market/tickers`

### Top movers

`GET /api/v1/market/top-movers?limit=20`

Returns separate `gainers` and `losers` arrays.

## WebSocket

Connect to:

`ws://localhost:3030/ws/market`

The backend sends a full snapshot immediately after a client connects:

```json
{
  "type": "market.snapshot",
  "timestamp": 1793650000000,
  "data": []
}
```

Then it broadcasts updates:

```json
{
  "type": "market.ticker",
  "timestamp": 1793650000000,
  "data": {
    "symbol": "BTCUSDT",
    "lastPrice": 0,
    "priceChangePercent": 0,
    "quoteVolume": 0
  }
}
```

Market metadata and ticker values are held in memory. REST exchange information and the initial ticker snapshot are refreshed every five minutes by default. Binance WebSocket reconnects automatically with exponential backoff.

## Environment variables

| Variable | Default | Description |
| --- | --- | --- |
| `HOST` | `0.0.0.0` | HTTP bind host |
| `PORT` | `3030` | HTTP port |
| `LOG_LEVEL` | `info` | Fastify log level |
| `CORS_ORIGIN` | `*` | Allowed browser origins; comma-separated for multiple origins |
| `BINANCE_REST_BASE_URL` | `https://fapi.binance.com` | Binance USDⓈ-M REST base URL |
| `BINANCE_WS_BASE_URL` | `wss://fstream.binance.com/market/stream` | Binance market stream URL |
| `BINANCE_RECONNECT_BASE_DELAY_MS` | `1000` | Initial reconnect delay |
| `BINANCE_RECONNECT_MAX_DELAY_MS` | `30000` | Maximum reconnect delay |
| `MARKET_REFRESH_INTERVAL_MS` | `300000` | Exchange info and ticker refresh interval |

## Important notes

- All prices and volumes are normalized to JavaScript numbers for convenient frontend consumption.
- The 24-hour percentage is Binance's rolling 24-hour ticker change, not a UTC-day percentage.
- The WebSocket stream only emits changed ticker records. The backend cache retains the latest known value for each active market.
- This service is public market data only. Trading, account data, API keys, and order execution are out of scope.
