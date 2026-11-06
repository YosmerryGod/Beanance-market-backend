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
| `GET /api/v1/market/exchange-info` | Complete persisted Binance USD-M exchange metadata, including global fields and every exchange symbol |
| `GET /api/v1/market/futures` | Active USDT-M perpetual markets, including Binance `exchangeInfo` and asset logo fields (`logo`, `logoSource`, `coingeckoId`) |
| `GET /api/v1/market/futures/:symbol` | One market and its latest ticker |
| `GET /api/v1/market/futures/:symbol/exchange-info` | Complete original Binance metadata for one contract, including all filters and trading rules |
| `GET /api/v1/market/tickers` | Markets sorted by quote volume |
| `GET /api/v1/market/top-movers?category=all&timeframe=1m&limit=500` | Live market-activity scanner by category and candle timeframe |
| `GET /api/v1/market/top-movers/history?limit=100` | Persisted Top Movers history; supports symbol, timeframe, from and to filters |
| `GET /api/v1/market/futures/:symbol/klines?interval=1m&limit=500` | Historical OHLCV candles |\n| `GET /api/v1/market/futures/:symbol/indicators?interval=1h&limit=500` | Configurable MA, EMA, SAR, Supertrend and Bollinger series |
| `GET /api/v1/market/futures/:symbol/depth?limit=20` | Current order book snapshot |
| `GET /api/v1/market/futures/:symbol/trades?limit=50` | Recent public trades |
| `GET /api/v1/market/futures/:symbol/metrics` | Latest mark/index price, estimated settle price, funding rate, interest rate, next funding time, Binance event time and open interest |

### Technical indicators

The indicator endpoint calculates values from the same Binance USDⓈ-M OHLCV candles used by the chart. It does not open additional Binance streams and does not store indicator values, keeping the calculation lightweight and allowing every chart to choose its own settings.

Example:

```text
GET /api/v1/market/futures/BTCUSDT/indicators?interval=1h&limit=500&maPeriods=20,50,200&emaPeriods=9,21,55&sarStep=0.02&sarMax=0.2&supertrendPeriod=10&supertrendMultiplier=3&bollingerPeriod=20&bollingerStdDev=2
```

Supported indicators and parameters:

| Indicator | Parameters | Defaults |
| --- | --- | --- |
| MA (SMA) | `maPeriods` (comma-separated, up to 10 periods) | `20,50` |
| EMA | `emaPeriods` (comma-separated, up to 10 periods) | `9,21` |
| Parabolic SAR | `sarStep`, `sarMax` | `0.02`, `0.2` |
| Supertrend | `supertrendPeriod`, `supertrendMultiplier` | `10`, `3` |
| Bollinger Bands | `bollingerPeriod`, `bollingerStdDev` | `20`, `2` |
| Volume | No extra parameters | Base and quote volume per candle |
| CVD | No extra parameters | Cumulative taker buy minus taker sell |
| Taker Long / Short | No extra parameters | Aggressive buy/sell base and quote volume |
| Funding Rate | No extra parameters | Historical funding events and candle-aligned rate series |

Periods are configurable from 1 to 500. MA/EMA accept up to 10 comma-separated periods, each from 1 to 500. SAR step and maximum, Supertrend multiplier, and Bollinger deviation are validated. Values before the required warm-up period are returned as `null`, so the frontend can safely draw gaps instead of misleading zero values.

The response contains `candles` plus aligned indicator arrays under `indicators`. Array index `i` always belongs to `candles[i]`. Supertrend also returns a direction array (`1` bullish, `-1` bearish). Volume and taker flow are returned in both base-asset and quote-asset units. Taker Long is Binance kline taker-buy volume; Taker Short is total candle volume minus taker-buy volume. CVD is the cumulative buy-minus-sell delta over the requested candle window and resets when the requested range changes. Funding Rate includes discrete historical settlement events and a candle-aligned step series; the response also includes the latest `currentFundingRate` and `nextFundingTime`. Historical funding retrieval is paginated up to 12 batches of 1,000 events per request and cached for 60 seconds. Use lowercase Binance interval values: `1m`, `5m`, `15m`, `1h`, `4h`, `1d`, and `1w` are supported by the kline service.

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
- Top Movers scans 1m, 5m, 15m, 1h, 4h, 1d and 1w candles for Kenaikan, Penurunan, Pullback, Rally, High Baru, Low Baru, and directional high-volume events. The displayed percentage is the candle's own timeframe move, not the rolling 24-hour ticker change.
- Top Movers history is stored in SQLite only when a candle closes and qualifies for at least one scanner category. Each history row includes its category IDs. Live updates continue to use the in-memory map and WebSocket.
- Top Movers restores its recent closed-candle context from locally persisted klines at startup, and `/health` reports per-timeframe subscription acknowledgements separately from open connections.
- Order book and trade data are sent only to clients subscribed to that symbol and stream.

## SQLite storage

SQLite uses WAL mode, normal synchronous mode, foreign keys, a busy timeout and prepared statements.

Tables:

- `exchange_info`: latest complete Binance `/fapi/v1/exchangeInfo` response, including timezone/server metadata, rate limits, assets, exchange filters, and the full symbols array
- `markets`: active USDT-M perpetual contract metadata, the complete original Binance symbol object in `exchange_info_json`, and asset logo metadata (`logo_url`, `logo_source`, `coingecko_id`) stored on each market row
- `tickers`: latest REST ticker snapshot
- `klines`: historical and closed OHLCV candles
- `market_metrics`: latest premium-index fields (mark/index price, estimated settle price, last funding rate, interest rate, next funding time and Binance event time) plus open interest
- The backend refreshes the bulk `/fapi/v1/premiumIndex` response every 10 seconds by default, stores matching active USDT-M perpetual symbols in one SQLite transaction, and preserves open-interest values between its independent updates. Configure with `MARKET_METRICS_REFRESH_INTERVAL_MS`.
- Asset logos are resolved once at startup and refreshed every 24 hours. CoinGecko is the primary source; Trust Wallet native-asset logos are checked as fallback. Only unique ticker-symbol matches are accepted to reduce incorrect logo assignments. Logo metadata is stored directly in `markets` and included in REST market lists and WebSocket market snapshots.
- `top_movers_history`: qualifying Top Movers events, unique per symbol/timeframe/candle

Ticker snapshots and market metadata are refreshed with the existing market refresh cycle. Interim ticker ticks remain in memory to avoid writing every market tick to disk. Closed kline candles are persisted, with a rolling retention limit of 2,000 candles per symbol/timeframe. Top Movers history is written once per qualifying closed candle (not on every realtime tick), with a hard global cap of 500 newest records; older records are deleted automatically after each insert. The existing SQLite retention period (30 days by default) also removes expired history. Order book snapshots and individual trades remain transient.

### Top Movers scanner API

- `GET /api/v1/market/top-movers?category=all&timeframe=1m&limit=500`
- Categories: `all`, `kenaikan`, `penurunan`, `pullback`, `rally`, `high_baru`, `low_baru`, `harga_naik_volume_tinggi`, and `harga_turun_volume_tinggi`.
- Timeframes: `1m`, `5m`, `15m`, `1h`, `4h`, `1d`, and `1w`. Omit `timeframe` to combine all scanner timeframes.
- Each live event includes `changePercent`, `timeframe`, `categories`, `severity`, candle OHLC bounds, candle quote volume, volume average/ratio, and event timestamps.

### Top Movers history API

- `GET /api/v1/market/top-movers/history?limit=100`
- Optional filters: `symbol=BTCUSDT`, `timeframe=1h`, `category=rally`, `from=UNIX_MS`, and `to=UNIX_MS`.
- `limit` accepts 1–1,000 and defaults to 100.
- Results are ordered newest first. The API returns closed-candle events only.
- Storage is capped globally at 500 newest records; older rows are deleted automatically.
- The unique key `symbol + timeframe + candleOpenTime` prevents duplicate rows if the same candle is processed again.

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
| `MARKET_REFRESH_INTERVAL_MS` | `21600000` | Exchange info and ticker refresh interval |
| `COINGECKO_API_KEY` | — | Optional CoinGecko Demo API key; kept server-side and sent as an API header |
| `SQLITE_PATH` | `./data/beanance-market.db` | SQLite database file path |

## Asset logo attribution

CoinGecko-sourced logos and data should be attributed in the frontend with a visible `Data provided by CoinGecko` credit linking to [CoinGecko API](https://www.coingecko.com/en/api). Trust Wallet fallback logos follow the Trust Wallet Assets repository structure.

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