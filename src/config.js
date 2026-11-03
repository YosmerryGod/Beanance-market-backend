import "dotenv/config";

function numberFromEnv(name, fallback) {
  const value = Number(process.env[name]);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

function parseCorsOrigin(value) {
  if (!value || value.trim() === "*") return true;

  return value
    .split(",")
    .map((origin) => origin.trim())
    .filter(Boolean);
}

export const config = {
  host: process.env.HOST || "0.0.0.0",
  port: numberFromEnv("PORT", 3030),
  logLevel: process.env.LOG_LEVEL || "info",
  sqlitePath: process.env.SQLITE_PATH || "./data/beanance-market.db",
  corsOrigin: parseCorsOrigin(process.env.CORS_ORIGIN),
  binanceRestBaseUrl: (
    process.env.BINANCE_REST_BASE_URL || "https://fapi.binance.com"
  ).replace(/\/$/, ""),
  binanceWsBaseUrl:
    process.env.BINANCE_WS_BASE_URL ||
    "wss://fstream.binance.com/market/stream",
  reconnectBaseDelayMs: numberFromEnv(
    "BINANCE_RECONNECT_BASE_DELAY_MS",
    1000
  ),
  reconnectMaxDelayMs: numberFromEnv(
    "BINANCE_RECONNECT_MAX_DELAY_MS",
    30000
  ),
  marketRefreshIntervalMs: numberFromEnv(
    "MARKET_REFRESH_INTERVAL_MS",
    300000
  )
};
