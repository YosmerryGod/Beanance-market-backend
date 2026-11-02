import WebSocket from "ws";
import { config } from "./config.js";
import {
  createBinanceWebSocketUrl,
  fetchAll24hrTickers,
  fetchExchangeInfo
} from "./binance.js";
import {
  applyTicker,
  applyTickerSnapshot,
  getMarketStatus,
  replaceSymbols
} from "./market-store.js";
import {
  publishMarketStatus,
  publishMarketUpdate,
  startHttpServer,
  stopHttpServer
} from "./server.js";

let binanceSocket = null;
let reconnectTimer = null;
let refreshTimer = null;
let reconnectAttempts = 0;
let shuttingDown = false;

const sleep = (milliseconds) =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

async function refreshMarketData() {
  const symbols = await fetchExchangeInfo();
  replaceSymbols(symbols);

  const tickers = await fetchAll24hrTickers();
  applyTickerSnapshot(tickers);

  const status = getMarketStatus();
  publishMarketStatus({
    ...status,
    connection: binanceSocket?.readyState === WebSocket.OPEN
      ? "connected"
      : "disconnected"
  });

  console.log(
    `Loaded ${status.marketCount} active USDT-M perpetual markets and ${status.tickerCount} tickers`
  );
}

function scheduleReconnect() {
  if (shuttingDown || reconnectTimer) return;

  const exponentialDelay = Math.min(
    config.reconnectBaseDelayMs * 2 ** reconnectAttempts,
    config.reconnectMaxDelayMs
  );
  const jitter = Math.floor(Math.random() * 500);
  const delay = exponentialDelay + jitter;

  reconnectAttempts += 1;

  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectToBinance();
  }, delay);

  console.warn(`Binance WebSocket reconnect scheduled in ${delay}ms`);
}

function handleBinancePayload(payload) {
  const data = payload?.data ?? payload;
  const tickers = Array.isArray(data) ? data : [data];

  for (const ticker of tickers) {
    const market = applyTicker(ticker);

    if (market) {
      publishMarketUpdate(market);
    }
  }
}

function connectToBinance() {
  if (shuttingDown) return;

  const url = createBinanceWebSocketUrl();
  console.log(`Connecting to Binance market stream: ${url}`);

  binanceSocket = new WebSocket(url, {
    handshakeTimeout: 15000
  });

  binanceSocket.on("open", () => {
    reconnectAttempts = 0;
    console.log("Connected to Binance USD-M market stream");

    publishMarketStatus({
      ...getMarketStatus(),
      connection: "connected"
    });
  });

  binanceSocket.on("message", (rawMessage) => {
    try {
      handleBinancePayload(JSON.parse(rawMessage.toString()));
    } catch (error) {
      console.error("Failed to process Binance market message:", error);
    }
  });

  binanceSocket.on("error", (error) => {
    console.error("Binance WebSocket error:", error.message);
  });

  binanceSocket.on("close", (code, reason) => {
    console.warn(
      `Binance WebSocket closed: ${code} ${reason.toString()}`
    );

    publishMarketStatus({
      ...getMarketStatus(),
      connection: "disconnected"
    });

    scheduleReconnect();
  });
}

async function bootstrap() {
  await refreshMarketData();
  await startHttpServer();

  connectToBinance();

  refreshTimer = setInterval(async () => {
    try {
      await refreshMarketData();
    } catch (error) {
      console.error("Scheduled market refresh failed:", error);
    }
  }, config.marketRefreshIntervalMs);
}

async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;

  console.log(`Received ${signal}; shutting down Beanance Market Backend`);

  if (refreshTimer) clearInterval(refreshTimer);
  if (reconnectTimer) clearTimeout(reconnectTimer);

  if (binanceSocket) {
    binanceSocket.removeAllListeners();
    if (
      binanceSocket.readyState === WebSocket.OPEN ||
      binanceSocket.readyState === WebSocket.CONNECTING
    ) {
      binanceSocket.close();
    }
  }

  await stopHttpServer();
  process.exit(0);
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

bootstrap().catch(async (error) => {
  console.error("Beanance Market Backend failed to start:", error);
  await shutdown("BOOTSTRAP_ERROR");
});
