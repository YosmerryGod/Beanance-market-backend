# Beanance Market Backend

Realtime market data service for Beanance, powered by Binance USDⓈ-M Perpetual Futures.

The service exposes public market data only. It does not place orders, access user accounts, or require Binance API keys.

## Requirements

- Node.js 20 or newer
- npm
- Build tools supported by `better-sqlite3` when a prebuilt binary is unavailable

## Setup

```bash
git clone https://github.com/YosmerryGod/Beanance-market-backend.git
cd Beanance-market-backend
npm install
cp .env.example .env
npm start
```

The service listens on port `3030` by default. SQLite is created automatically at `./data/beanance-market.db`.

## Market eligibility

A contract is included only when all conditions match:

- `status === "TRADING"`
- `contractType === "PERPETUAL"`
- `quoteAsset === "USDT"`
- `marginAsset === "USDT"`

Exchange metadata is loaded from Binance `/fapi/v1/exchangeInfo`. Initial 24-hour ticker data is loaded from `/fapi/v1/ticker/24hr`.

The all-market ticker stream is shared by the backend:

`wss://fstream.binance.com/market/stream?streams=!ticker@arr`

## REST API

All endpoints return JSON.

| Endpoint | Purpose |
| --- | --- |
| `GET /health` | Service, SQLite and stream health |
| `GET /api/v1/market/futures` | Active USDT-M perpetual markets |
| `GET /api/v1/market/futures/:symbol` | One market and its latest ticker |
| `GET /api/v1/market/tickers` | Markets sorted by quote volume |
| `GET /api/v1/market/top-movers?limit=20` | Top gainers and losers |
| `GET /api/v1/market/futures/:symbol/klines?interval=1m&limit=500` | Historical OHLCV candles |
| `GET /api/v1/market/futures/:symbol/depth?limit=20` | Current order book snapshot |
| `GET /api/v1/market/futures/:symbol/trades?limit=50` | Recent public trades |
| `GET /api/v1/market/futures/:symbol/metrics` | Mark/index price, funding rate and open interest |

### Kline parameters

- Supported intervals: `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `3d`, `1w`, `1M`
- `limit`: 1–1500; defaults to 500
- `endTime`: optional Unix timestamp in milliseconds

The backend first checks SQLite. When the requested symbol/timeframe has no stored candles, it loads the requested history from Binance REST and stores it. Closed realtime candles are then upserted into SQLite.

## WebSocket

Connect to:

`ws://localhost:3030/ws/market`

The connection sends the existing `market.snapshot` message and continues to receive the existing `market.ticker` and `market.status` events.

A client can subscribe to detail streams by sending:

```json
{
  "type": "market.detail.subscribe",
  "symbol": "BTCUSDT",
  "streams": ["kline_1m", "depth20@100ms", "aggTrade"]
}
```

Supported detail streams:

- `kline_1m`
- `kline_5m`
- `kline_15m`
- `kline_30m`
- `kline_1h`
- `kline_4h`
- `kline_1d`
- `depth20@100ms`
- `aggTrade`

The server acknowledges subscriptions with `market.detail.subscribed`. Realtime messages use event types such as:

- `market.detail.kline`
- `market.detail.depthUpdate`
- `market.detail.aggTrade`

Unsubscribe with:

```json
{
  "type": "market.detail.unsubscribe",
  "symbol": "BTCUSDT",
  "streams": ["kline_1m", "depth20@100ms", "aggTrade"]
}
```

If a client disconnects, its subscriptions are released automatically.

### WebSocket efficiency

- The backend maintains one shared Binance detail WebSocket connection, not one Binance connection per frontend user.
- Streams are subscribed upstream only while at least one frontend client needs them.
- Multiple frontend clients share the same upstream stream.
- The existing all-market ticker stream remains separate and unchanged.
- Order book and trade data are sent only to clients subscribed to that symbol and stream.

## SQLite storage

SQLite uses WAL mode, normal synchronous mode, foreign keys, a busy timeout and prepared statements.

Tables:

- `markets`: active contract metadata
- `tickers`: latest REST ticker snapshot
- `klines`: historical and closed OHLCV candles
- `market_metrics`: latest funding, mark/index price and open interest

Ticker snapshots and market metadata are refreshed with the existing market refresh cycle. Interim ticker ticks remain in memory to avoid writing every market tick to disk. Closed kline candles are persisted, with a rolling retention limit of 2,000 candles per symbol/timeframe; order book snapshots and individual trades remain transient.

The database file and its WAL files are ignored by Git.

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
| `SQLITE_PATH` | `./data/beanance-market.db` | SQLite database file path |

## Notes

- Prices and volumes are normalized to JavaScript numbers.
- The 24-hour percentage is Binance's rolling 24-hour ticker change, not a UTC-day percentage.
- Funding and open-interest values are public market metrics, not trading signals.
- Binance public API limits still apply. Frontend clients should reuse the backend endpoints and WebSocket rather than calling Binance directly.

## Resource controls (updated)

Binance REST requests now pass through one central queue with bounded concurrency, estimated request-weight budgeting, request deduplication, endpoint-specific response caching, and a shared cooldown after HTTP 429/418. Retry is limited to server-side 5xx responses.

WebSocket connections use a shared lifecycle manager with exponential reconnect backoff and jitter, heartbeat/stale detection, and clean shutdown. Frontend detail subscriptions are indexed once and shared across clients. Default limits are 100 frontend clients, 20 detail subscriptions per client, and 800 active upstream streams.

Market ticker fanout is batched every 500 ms while preserving the existing market.ticker event shape. Slow clients are disconnected when their outbound buffer exceeds the configured limit. Depth updates are throttled per client.

SQLite stores only closed candles for the highest-volume 100 symbols and configured timeframes. Default retention is 2,000 candles per symbol/timeframe, 300,000 kline rows globally, and 30 days. A scheduled maintenance task prunes expired and excess rows and checkpoints the WAL.

Use ecosystem.config.cjs to run the service with PM2 memory and restart limits. Install pm2-logrotate and configure a 10 MB log size with seven retained files. Deployment log files are also pruned to the latest ten.

See .env.example for all queue, cache, WebSocket, HTTP rate-limit, and SQLite limits.
### PM2 commands

First-time setup:

    pm2 startOrReload ecosystem.config.cjs --update-env
    pm2 save
    pm2 install pm2-logrotate
    pm2 set pm2-logrotate:max_size 10M
    pm2 set pm2-logrotate:retain 7
    pm2 set pm2-logrotate:compress true

Subsequent deployments use node deploy.js. The deployment script pulls the selected branch, adds missing environment variables without replacing existing values, installs production dependencies, applies the PM2 ecosystem configuration, and checks /health.