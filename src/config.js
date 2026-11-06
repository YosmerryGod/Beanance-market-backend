import "dotenv/config";

function numberFromEnv(name, fallback, minimum = 1) {
  const value = Number(process.env[name]);
  return Number.isFinite(value) && value >= minimum ? value : fallback;
}

function parseCorsOrigin(value) {
  if (!value || value.trim() === "*") return true;
  return value.split(",").map((origin) => origin.trim()).filter(Boolean);
}

function parseList(value, fallback) {
  const values = String(value || fallback).split(",").map((item) => item.trim()).filter(Boolean);
  return [...new Set(values)];
}

const nodeEnv = process.env.NODE_ENV || "production";

export const config = {
  host: process.env.HOST || "0.0.0.0",
  port: numberFromEnv("PORT", 3030),
  logLevel: process.env.LOG_LEVEL || (nodeEnv === "production" ? "warn" : "info"),
  nodeEnv,
  sqlitePath: process.env.SQLITE_PATH || "./data/beanance-market.db",
  corsOrigin: parseCorsOrigin(process.env.CORS_ORIGIN),
  binanceRestBaseUrl: (process.env.BINANCE_REST_BASE_URL || "https://fapi.binance.com").replace(/\/$/, ""),
  binanceWsBaseUrl: process.env.BINANCE_WS_BASE_URL || "wss://fstream.binance.com/market/stream",
  reconnectBaseDelayMs: numberFromEnv("BINANCE_RECONNECT_BASE_DELAY_MS", 1000),
  reconnectMaxDelayMs: numberFromEnv("BINANCE_RECONNECT_MAX_DELAY_MS", 30000),
  marketRefreshIntervalMs: numberFromEnv("MARKET_REFRESH_INTERVAL_MS", 21600000),
  // [ADDED] Refresh the complete premium-index snapshot every 10 seconds by default.
  marketMetricsRefreshIntervalMs: numberFromEnv("MARKET_METRICS_REFRESH_INTERVAL_MS", 10000),
  rest: {
    timeoutMs: numberFromEnv("BINANCE_REST_TIMEOUT_MS", 10000),
    concurrency: numberFromEnv("BINANCE_REST_CONCURRENCY", 2),
    maxQueue: numberFromEnv("BINANCE_REST_MAX_QUEUE", 100),
    weightBudgetPerMinute: numberFromEnv("BINANCE_WEIGHT_BUDGET_PER_MINUTE", 850),
    cooldownFallbackMs: numberFromEnv("BINANCE_RATE_LIMIT_COOLDOWN_MS", 60000),
    cacheMaxEntries: numberFromEnv("BINANCE_CACHE_MAX_ENTRIES", 500),
    retry5xx: numberFromEnv("BINANCE_RETRY_5XX", 1, 0)
  },
  cache: {
    maxEntries: numberFromEnv("DETAIL_CACHE_MAX_ENTRIES", 300),
    depthTtlMs: numberFromEnv("DEPTH_CACHE_TTL_MS", 500),
    tradesTtlMs: numberFromEnv("TRADES_CACHE_TTL_MS", 1000),
    metricsTtlMs: numberFromEnv("METRICS_CACHE_TTL_MS", 10000),
    klineTtlMs: numberFromEnv("KLINE_CACHE_TTL_MS", 15000)
  },
  websocket: {
    maxClients: numberFromEnv("WS_MAX_CLIENTS", 100),
    maxSubscriptionsPerClient: numberFromEnv("WS_MAX_SUBSCRIPTIONS_PER_CLIENT", 20),
    maxActiveStreams: numberFromEnv("WS_MAX_ACTIVE_STREAMS", 800),
    maxBufferedBytes: numberFromEnv("WS_MAX_BUFFERED_BYTES", 1048576),
    marketBatchMs: numberFromEnv("WS_MARKET_BATCH_MS", 500),
    depthThrottleMs: numberFromEnv("WS_DEPTH_THROTTLE_MS", 200),
    heartbeatMs: numberFromEnv("WS_HEARTBEAT_MS", 30000),
    staleTimeoutMs: numberFromEnv("WS_STALE_TIMEOUT_MS", 90000),
    commandBatchSize: numberFromEnv("WS_COMMAND_BATCH_SIZE", 50),
    idleCloseMs: numberFromEnv("WS_IDLE_CLOSE_MS", 30000)
  },
  database: {
    cacheSizeKb: numberFromEnv("SQLITE_CACHE_SIZE_KB", 8192),
    pairRetention: numberFromEnv("SQLITE_KLINE_RETENTION_PER_PAIR", 2000),
    maxKlineRows: numberFromEnv("SQLITE_MAX_KLINE_ROWS", 300000),
    retentionDays: numberFromEnv("SQLITE_RETENTION_DAYS", 30),
    maxPersistedSymbols: numberFromEnv("SQLITE_MAX_PERSISTED_SYMBOLS", 100),
    maintenanceIntervalMs: numberFromEnv("SQLITE_MAINTENANCE_INTERVAL_MS", 3600000),
    maxDatabaseMb: numberFromEnv("SQLITE_MAX_DATABASE_MB", 256),
    allowedIntervals: parseList(process.env.SQLITE_KLINE_INTERVALS, "1m,5m,15m,1h,4h,1d")
  },
  http: {
    rateLimitMax: numberFromEnv("HTTP_RATE_LIMIT_MAX", 120),
    rateLimitWindow: process.env.HTTP_RATE_LIMIT_WINDOW || "1 minute"
  }
};
