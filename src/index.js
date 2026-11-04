import { closeDatabase, startDatabaseMaintenance } from "./database.js";
import { stopDetailStreams } from "./detail-stream.js";
import { startTopMovers, stopTopMovers } from "./top-movers.js";
import { config } from "./config.js";
import { createBinanceWebSocketUrl, fetchAll24hrTickers, fetchExchangeInfo } from "./binance.js";
import { applyTicker, applyTickerSnapshot, getMarketStatus, hydrateMarketStore, replaceSymbols } from "./market-store.js";
import { publishMarketStatus, publishMarketUpdate, startHttpServer, stopHttpServer } from "./server.js";
import { ManagedWebSocket } from "./websocket-manager.js";

let refreshTimer = null;
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
    const symbols = await fetchExchangeInfo();
    replaceSymbols(symbols);
    const tickers = await fetchAll24hrTickers();
    applyTickerSnapshot(tickers);
    const status = getMarketStatus();
    publishMarketStatus({ ...status, connection: marketSocket.connected ? "connected" : "disconnected" });
    console.info(`Loaded ${status.marketCount} active USDT-M perpetual markets and ${status.tickerCount} tickers`);
  } finally {
    refreshInProgress = false;
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

  marketSocket.connect();
  startTopMovers().catch((error) => console.error("Multi-timeframe Top Movers streams failed to start:", error.message));
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
