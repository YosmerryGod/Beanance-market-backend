import { closeDatabase, saveExchangeInfo, startDatabaseMaintenance } from "./database.js";
import { refreshAllMarketMetrics, backfillMarketKlines } from "./detail-service.js";
// [ADDED] Resolve market logos from CoinGecko with Trust Wallet fallback.
import { refreshMarketLogos } from "./market-logo-service.js";
import { stopDetailStreams } from "./detail-stream.js";
import { startTopMovers, stopTopMovers } from "./top-movers.js";
import { config } from "./config.js";
import { createBinanceWebSocketUrl, fetchAll24hrTickers, fetchExchangeInfo } from "./binance.js";
import { applyTicker, applyTickerSnapshot, getMarketStatus, getMarkets, hydrateMarketStore, replaceSymbols } from "./market-store.js";
import { publishMarketStatus, publishMarketUpdate, startHttpServer, stopHttpServer } from "./server.js";
import { ManagedWebSocket } from "./websocket-manager.js";

let refreshTimer = null;
// [ADDED] Independent timer and lock for bulk premium-index persistence.
let marketMetricsRefreshTimer = null;
// [ADDED] Daily asset-logo metadata refresh timer.
let marketLogoRefreshTimer = null;
let marketMetricsRefreshInProgress = false;
let refreshInProgress = false;
let shuttingDown = false;

const marketSocket = new ManagedWebSocket({
  name: "Binance market",
  url: createBinanceWebSocketUrl(),
  onMessage: handleBinancePayload,
  onOpen: () => publishMarketStatus({ ...getMarketStatus(), connection: "connected" }),
  onClose: () => publishMarketStatus({ ...getMarketStatus(), connection: "disconnected" })
});

async function refreshMarketData() {
  if (refreshInProgress) return;
  refreshInProgress = true;
  try {
    const exchangeInfo = await fetchExchangeInfo();
    // [ADDED] Persist the complete Binance exchangeInfo response before selecting active USDT-M perpetuals.
    saveExchangeInfo(exchangeInfo.payload);
    replaceSymbols(exchangeInfo.symbols);
    const tickers = await fetchAll24hrTickers();
    applyTickerSnapshot(tickers);
    const status = getMarketStatus();
    publishMarketStatus({ ...status, connection: marketSocket.connected ? "connected" : "disconnected" });
    console.info(`Loaded ${status.marketCount} active USDT-M perpetual markets and ${status.tickerCount} tickers`);
  } finally {
    refreshInProgress = false;
  }
}

// [ADDED] One bulk Binance request refreshes the latest premium-index fields for every active market.
async function refreshMarketMetrics() {
  if (marketMetricsRefreshInProgress) return;
  marketMetricsRefreshInProgress = true;
  try {
    const symbols = getMarkets().map((market) => market.symbol);
    const result = await refreshAllMarketMetrics(symbols);
    console.info(`Persisted premium-index metrics for ${result.saved}/${symbols.length} active markets`);
  } finally {
    marketMetricsRefreshInProgress = false;
  }
}

function handleBinancePayload(payload) {
  const data = payload?.data ?? payload;
  const tickers = Array.isArray(data) ? data : [data];
  for (const ticker of tickers) {
    const market = applyTicker(ticker);
    if (market) publishMarketUpdate(market);
  }
}

async function bootstrap() {
  const restored = hydrateMarketStore();
  if (restored) console.info(`Restored ${restored} markets from SQLite`);
  startDatabaseMaintenance();
  await startHttpServer();

  try {
    await refreshMarketData();
  } catch (error) {
    console.error("Initial Binance market snapshot failed:", error.message);
    console.warn("The API remains online and will retry during the next refresh cycle");
  }

  // [ADDED] Resolve logos after the first market snapshot is available.
  refreshMarketLogos().catch((error) => console.error("Initial market logo refresh failed:", error.message));
  marketLogoRefreshTimer = setInterval(() => {
    refreshMarketLogos({ force: true }).catch((error) => console.error("Scheduled market logo refresh failed:", error.message));
  }, 24 * 60 * 60 * 1000);
  marketLogoRefreshTimer.unref?.();

  marketSocket.connect();
  refreshMarketMetrics().catch((error) => console.error("Initial premium-index refresh failed:", error.message));
  marketMetricsRefreshTimer = setInterval(() => {
    refreshMarketMetrics().catch((error) => console.error("Scheduled premium-index refresh failed:", error.message));
  }, config.marketMetricsRefreshIntervalMs);
  marketMetricsRefreshTimer.unref?.();

  startTopMovers().catch((error) => console.error("Multi-timeframe Top Movers streams failed to start:", error.message));
  // [ADDED] Seed SQLite with 100 recent closed 15m candles for every active futures symbol.
  backfillMarketKlines(getMarkets().map((market) => market.symbol), "15m", 100)
    .then((result) => console.info(`15m candle backfill completed: ${result.savedCandles} candles saved for ${result.processedSymbols}/${result.requestedSymbols} symbols; ${result.failedSymbols.length} failed`))
    .catch((error) => console.error("15m candle backfill failed:", error.message));
  refreshTimer = setInterval(() => {
    refreshMarketData().catch((error) => console.error("Scheduled market refresh failed:", error.message));
  }, config.marketRefreshIntervalMs);
  refreshTimer.unref?.();
}

async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.info(`Received ${signal}; shutting down Beanance Market Backend`);
  if (refreshTimer) clearInterval(refreshTimer);
  if (marketMetricsRefreshTimer) clearInterval(marketMetricsRefreshTimer);
  if (marketLogoRefreshTimer) clearInterval(marketLogoRefreshTimer);
  marketSocket.stop();
  stopTopMovers();
  stopDetailStreams();
  await stopHttpServer();
  closeDatabase();
  process.exit(0);
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

bootstrap().catch(async (error) => {
  console.error("Beanance Market Backend failed to start:", error);
  await shutdown("BOOTSTRAP_ERROR");
});
