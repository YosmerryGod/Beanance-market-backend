import Fastify from "fastify";
import cors from "@fastify/cors";
import rateLimit from "@fastify/rate-limit";
import WebSocket, { WebSocketServer } from "ws";
import { config } from "./config.js";
import { getBinanceRestStatus } from "./binance.js";
import { getMarket, getMarkets, getMarketStatus } from "./market-store.js";
import { getDatabaseStatus, getStoredExchangeInfo, getStoredMarketExchangeInfo, getTopMoverHistory } from "./database.js";
import { getTopMovers, getTopMoversStatus, getTopMoverCategoryGroups, getTopMoversRevision, TOP_MOVER_CATEGORIES } from "./top-movers.js";
import { getMarketDepth, getMarketFundingRates, getMarketKlines, getMarketMetrics, getMarketTrades } from "./detail-service.js";
import { calculateIndicators, normalizeIndicatorOptions } from "./indicators.js";
import {
  getDetailStreamStatus,
  removeDetailClient,
  setDetailStreamHandler,
  subscribeDetailStreams,
  unsubscribeDetailStreams,
  forEachDetailStreamClient
} from "./detail-stream.js";

const app = Fastify({ logger: { level: config.logLevel } });
await app.register(cors, { origin: config.corsOrigin });
await app.register(rateLimit, {
  max: config.http.rateLimitMax,
  timeWindow: config.http.rateLimitWindow,
  hook: "onRequest"
});

const websocketServer = new WebSocketServer({
  noServer: true,
  path: "/ws/market",
  clientTracking: true,
  maxPayload: 32 * 1024,
  perMessageDeflate: false
});

const pendingMarketUpdates = new Map();
const depthLastSentAt = new WeakMap();
const messageWindows = new WeakMap();
let marketFlushTimer = null;
let heartbeatTimer = null;
let topMoversTimer = null;
// [ADDED] Broadcast only when the live scanner snapshot has changed.
let lastBroadcastTopMoversRevision = 0;

function sendJson(socket, payload) {
  if (socket.readyState !== WebSocket.OPEN) return false;
  if (socket.bufferedAmount > config.websocket.maxBufferedBytes) {
    socket.close(1013, "Client is too slow");
    return false;
  }
  socket.send(JSON.stringify(payload), (error) => {
    if (error) app.log.warn({ err: error }, "WebSocket send failed");
  });
  return true;
}

function broadcast(payload) {
  const message = JSON.stringify(payload);
  for (const client of websocketServer.clients) {
    if (client.readyState !== WebSocket.OPEN) continue;
    if (client.bufferedAmount > config.websocket.maxBufferedBytes) {
      client.close(1013, "Client is too slow");
      continue;
    }
    client.send(message);
  }
}

function flushMarketUpdates() {
  marketFlushTimer = null;
  if (!pendingMarketUpdates.size) return;
  const updates = [...pendingMarketUpdates.values()];
  pendingMarketUpdates.clear();

  const messages = updates.map((market) => JSON.stringify({
    type: "market.ticker",
    timestamp: Date.now(),
    data: market
  }));

  for (const client of websocketServer.clients) {
    if (client.readyState !== WebSocket.OPEN) continue;
    if (client.bufferedAmount > config.websocket.maxBufferedBytes) {
      client.close(1013, "Client is too slow");
      continue;
    }
    for (const message of messages) {
      if (client.bufferedAmount > config.websocket.maxBufferedBytes) {
        client.close(1013, "Client is too slow");
        break;
      }
      client.send(message);
    }
  }
}

function queueMarketUpdate(market) {
  pendingMarketUpdates.set(market.symbol, market);
  if (!marketFlushTimer) {
    marketFlushTimer = setTimeout(flushMarketUpdates, config.websocket.marketBatchMs);
    marketFlushTimer.unref?.();
  }
}

function parseLimit(value, fallback = 20, maximum = 200) {
  const limit = Number.parseInt(value, 10);
  if (!Number.isFinite(limit)) return fallback;
  return Math.min(Math.max(limit, 1), maximum);
}

function requireMarket(symbolValue, reply) {
  const market = getMarket(String(symbolValue || "").toUpperCase());
  if (!market) {
    reply.code(404).send({
      success: false,
      error: "MARKET_NOT_FOUND",
      message: "Futures market not found or not active"
    });
    return null;
  }
  return market;
}

app.get("/health", async () => ({
  success: true,
  service: "beanance-market-backend",
  status: "ok",
  timestamp: Date.now(),
  market: getMarketStatus(),
  database: getDatabaseStatus(),
  binanceRest: getBinanceRestStatus(),
  detailStreams: getDetailStreamStatus(),
  topMovers: getTopMoversStatus()
}));

// [ADDED] Expose the latest complete Binance USD-M exchangeInfo snapshot.
app.get("/api/v1/market/exchange-info", async (_request, reply) => {
  const data = getStoredExchangeInfo();
  if (!data) {
    return reply.code(503).send({
      success: false,
      error: "EXCHANGE_INFO_NOT_READY",
      message: "Binance exchange metadata has not been loaded yet"
    });
  }
  return { success: true, data };
});

// [ADDED] Return the original Binance metadata for one contract without adding it to live ticker messages.
app.get("/api/v1/market/futures/:symbol/exchange-info", async (request, reply) => {
  const market = requireMarket(request.params.symbol, reply);
  if (!market) return reply;
  const data = getStoredMarketExchangeInfo(market.symbol);
  if (!data) {
    return reply.code(404).send({
      success: false,
      error: "MARKET_EXCHANGE_INFO_NOT_FOUND",
      message: "Exchange metadata for this market is not stored"
    });
  }
  return { success: true, symbol: market.symbol, data };
});

app.get("/api/v1/market/futures", async (request) => {
  const sort = ["symbol", "quoteVolume", "priceChangePercent"].includes(request.query.sort) ? request.query.sort : "symbol";
  const order = request.query.order === "desc" ? "desc" : "asc";
  const markets = getMarkets({ sort, order });
  return { success: true, marketType: "USDT-M-PERPETUAL", count: markets.length, updatedAt: getMarketStatus().lastUpdateAt, data: markets };
});

app.get("/api/v1/market/futures/:symbol", async (request, reply) => {
  const market = requireMarket(request.params.symbol, reply);
  if (!market) return reply;
  return { success: true, data: market };
});

app.get("/api/v1/market/futures/:symbol/klines", async (request, reply) => {
  const market = requireMarket(request.params.symbol, reply);
  if (!market) return reply;
  const interval = String(request.query.interval || "1m");
  const limit = parseLimit(request.query.limit, 500, 1500);
  const endTime = request.query.endTime === undefined ? null : Number(request.query.endTime);
  if (endTime !== null && (!Number.isFinite(endTime) || endTime <= 0)) {
    return reply.code(400).send({ success: false, error: "INVALID_END_TIME", message: "endTime must be a valid Unix timestamp in milliseconds" });
  }
  const data = await getMarketKlines(market.symbol, interval, limit, endTime);
  return { success: true, symbol: market.symbol, interval, count: data.length, data };
});

app.get("/api/v1/market/futures/:symbol/indicators", async (request, reply) => {
  const market = requireMarket(request.params.symbol, reply);
  if (!market) return reply;

  const interval = String(request.query.interval || "1m"); // [MODIFIED] Preserve uppercase 1M because Binance uses it for monthly candles.
  const limit = parseLimit(request.query.limit, 500, 1500);
  const endTime = request.query.endTime === undefined ? null : Number(request.query.endTime);
  if (endTime !== null && (!Number.isFinite(endTime) || endTime <= 0)) {
    return reply.code(400).send({
      success: false,
      error: "INVALID_END_TIME",
      message: "endTime must be a valid Unix timestamp in milliseconds"
    });
  }

  try {
    const options = normalizeIndicatorOptions(request.query);
    const candles = await getMarketKlines(market.symbol, interval, limit, endTime);
    // [ADDED] Chart clients can skip funding history when they only need price overlays.
    const includeFunding = request.query.includeFunding !== "false";
    const fundingRates = includeFunding ? await getMarketFundingRates(market.symbol, candles) : [];
    const indicators = calculateIndicators(candles, options, fundingRates);
    const currentMetrics = includeFunding ? await getMarketMetrics(market.symbol) : null;
    return {
      success: true,
      symbol: market.symbol,
      interval,
      count: candles.length,
      candleSource: "Binance USDⓈ-M Futures",
      candles,
      currentFundingRate: currentMetrics?.fundingRate ?? null,
      nextFundingTime: currentMetrics?.nextFundingTime ?? null,
      indicators
    };
  } catch (error) {
    if (error.statusCode === 400) {
      return reply.code(400).send({
        success: false,
        error: "INVALID_INDICATOR_PARAMETERS",
        message: error.message
      });
    }
    throw error;
  }
});

app.get("/api/v1/market/futures/:symbol/depth", async (request, reply) => {
  const market = requireMarket(request.params.symbol, reply);
  if (!market) return reply;
  const data = await getMarketDepth(market.symbol, parseLimit(request.query.limit, 20, 100));
  return { success: true, data };
});

app.get("/api/v1/market/futures/:symbol/trades", async (request, reply) => {
  const market = requireMarket(request.params.symbol, reply);
  if (!market) return reply;
  const data = await getMarketTrades(market.symbol, parseLimit(request.query.limit, 50, 1000));
  return { success: true, symbol: market.symbol, count: data.length, data };
});

app.get("/api/v1/market/futures/:symbol/metrics", async (request, reply) => {
  const market = requireMarket(request.params.symbol, reply);
  if (!market) return reply;
  return { success: true, data: await getMarketMetrics(market.symbol) };
});

app.get("/api/v1/market/tickers", async () => {
  const markets = getMarkets({ sort: "quoteVolume", order: "desc" });
  return { success: true, count: markets.length, updatedAt: getMarketStatus().lastUpdateAt, data: markets };
});

app.get("/api/v1/market/top-movers", async (request, reply) => {
  const limit = parseLimit(request.query.limit, 500, 1000);
  const category = String(request.query.category || "all").toLowerCase();
  const timeframe = request.query.timeframe === undefined ? null : String(request.query.timeframe).toLowerCase();
  const allowedCategories = new Set(TOP_MOVER_CATEGORIES.map((item) => item.id));
  const allowedTimeframes = new Set(["1m", "5m", "15m", "1h", "4h", "1d", "1w"]);

  if (!allowedCategories.has(category)) {
    return reply.code(400).send({
      success: false,
      error: "INVALID_CATEGORY",
      message: "Unsupported Top Movers category",
      allowed: [...allowedCategories]
    });
  }
  if (timeframe !== null && !allowedTimeframes.has(timeframe)) {
    return reply.code(400).send({
      success: false,
      error: "INVALID_TIMEFRAME",
      message: "Unsupported Top Movers timeframe",
      allowed: [...allowedTimeframes]
    });
  }

  const events = getTopMovers(limit, { category, timeframe });
  const allEvents = getTopMovers(1000, { timeframe });
  return {
    success: true,
    updatedAt: Date.now(),
    revision: getTopMoversRevision(),
    filters: { category, timeframe, limit },
    data: {
      events,
      categories: getTopMoverCategoryGroups(allEvents)
    },
    meta: getTopMoversStatus()
  };
});

app.get("/api/v1/market/top-movers/history", async (request, reply) => {
  const limit = parseLimit(request.query.limit, 100, 1000);
  const symbol = request.query.symbol === undefined ? null : String(request.query.symbol).toUpperCase();
  const timeframe = request.query.timeframe === undefined ? null : String(request.query.timeframe);
  const category = request.query.category === undefined ? null : String(request.query.category).toLowerCase();
  const from = request.query.from === undefined ? null : Number(request.query.from);
  const to = request.query.to === undefined ? null : Number(request.query.to);
  const allowedTimeframes = new Set(["1m", "5m", "15m", "1h", "4h", "1d", "1w"]);

  if (symbol !== null && !/^[A-Z0-9]{3,30}$/.test(symbol)) {
    return reply.code(400).send({ success: false, error: "INVALID_SYMBOL", message: "symbol is invalid" });
  }
  if (timeframe !== null && !allowedTimeframes.has(timeframe)) {
    return reply.code(400).send({ success: false, error: "INVALID_TIMEFRAME", message: "Unsupported Top Movers timeframe" });
  }
  const allowedCategories = new Set(TOP_MOVER_CATEGORIES.filter((item) => item.id !== "all").map((item) => item.id));
  if (category !== null && !allowedCategories.has(category)) {
    return reply.code(400).send({ success: false, error: "INVALID_CATEGORY", message: "Unsupported Top Movers category", allowed: [...allowedCategories] });
  }
  if (from !== null && (!Number.isFinite(from) || from <= 0)) {
    return reply.code(400).send({ success: false, error: "INVALID_FROM", message: "from must be a Unix timestamp in milliseconds" });
  }
  if (to !== null && (!Number.isFinite(to) || to <= 0)) {
    return reply.code(400).send({ success: false, error: "INVALID_TO", message: "to must be a Unix timestamp in milliseconds" });
  }
  if (from !== null && to !== null && from > to) {
    return reply.code(400).send({ success: false, error: "INVALID_TIME_RANGE", message: "from must be less than or equal to to" });
  }

  const data = getTopMoverHistory({ limit, symbol, timeframe, category, from, to });
  return {
    success: true,
    count: data.length,
    filters: { symbol, timeframe, category, from, to, limit },
    data
  };
});

function isMessageRateAllowed(socket) {
  const now = Date.now();
  const current = messageWindows.get(socket);
  if (!current || now - current.startedAt >= 1000) {
    messageWindows.set(socket, { startedAt: now, count: 1 });
    return true;
  }
  current.count += 1;
  return current.count <= 10;
}

websocketServer.on("connection", (socket) => {
  socket.isAlive = true;
  socket.on("pong", () => { socket.isAlive = true; });

  sendJson(socket, { type: "market.snapshot", timestamp: Date.now(), data: getMarkets() });
  sendJson(socket, { type: "market.top-movers", timestamp: Date.now(), revision: getTopMoversRevision(), data: { events: getTopMovers(500), categories: getTopMoverCategoryGroups(getTopMovers(1000)) }, meta: getTopMoversStatus() });

  socket.on("message", (raw) => {
    if (!isMessageRateAllowed(socket)) {
      socket.close(1008, "Message rate limit exceeded");
      return;
    }
    let message;
    try {
      message = JSON.parse(raw.toString());
    } catch {
      sendJson(socket, { type: "market.error", error: "INVALID_JSON", message: "WebSocket message must be valid JSON" });
      return;
    }

    const symbol = String(message.symbol || "").toUpperCase();
    if (message.type === "market.detail.subscribe") {
      if (!getMarket(symbol)) {
        sendJson(socket, { type: "market.error", error: "MARKET_NOT_FOUND", symbol });
        return;
      }
      const streams = subscribeDetailStreams(socket, symbol, message.streams);
      sendJson(socket, {
        type: "market.detail.subscribed",
        symbol,
        streams,
        timestamp: Date.now()
      });
      return;
    }

    if (message.type === "market.detail.unsubscribe") {
      const removed = unsubscribeDetailStreams(socket, symbol, message.streams);
      sendJson(socket, {
        type: "market.detail.unsubscribed",
        symbol,
        streams: removed,
        timestamp: Date.now()
      });
    }
  });

  socket.on("close", () => removeDetailClient(socket));
  socket.on("error", (error) => app.log.warn({ err: error }, "Frontend WebSocket client error"));
});

setDetailStreamHandler((payload) => {
  const now = Date.now();
  const streamKey = `${payload.symbol.toLowerCase()}@${payload.stream}`;
  forEachDetailStreamClient(payload.symbol, payload.stream, (client) => {
    if (payload.stream === "depth20@100ms") {
      let clientMap = depthLastSentAt.get(client);
      if (!clientMap) {
        clientMap = new Map();
        depthLastSentAt.set(client, clientMap);
      }
      const lastSent = clientMap.get(streamKey) || 0;
      if (now - lastSent < config.websocket.depthThrottleMs) return;
      clientMap.set(streamKey, now);
    }
    sendJson(client, payload);
  });
});

websocketServer.on("error", (error) => app.log.error({ err: error }, "Frontend WebSocket server error"));

app.server.on("upgrade", (request, socket, head) => {
  const pathname = new URL(request.url || "/", "http://localhost").pathname;
  if (pathname !== "/ws/market") {
    socket.destroy();
    return;
  }
  if (websocketServer.clients.size >= config.websocket.maxClients) {
    socket.write("HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n");
    socket.destroy();
    return;
  }
  websocketServer.handleUpgrade(request, socket, head, (client) => {
    websocketServer.emit("connection", client, request);
  });
});

export function publishMarketUpdate(market) {
  queueMarketUpdate(market);
}

export function publishMarketStatus(status) {
  broadcast({ type: "market.status", timestamp: Date.now(), data: status });
}

export async function startHttpServer() {
  await app.listen({ host: config.host, port: config.port });
  lastBroadcastTopMoversRevision = getTopMoversRevision();
  topMoversTimer = setInterval(() => {
    if (!websocketServer.clients.size) return;
    const revision = getTopMoversRevision();
    if (revision === lastBroadcastTopMoversRevision) return;
    lastBroadcastTopMoversRevision = revision;
    broadcast({
      type: "market.top-movers",
      timestamp: Date.now(),
      revision,
      data: { events: getTopMovers(500), categories: getTopMoverCategoryGroups(getTopMovers(1000)) },
      meta: getTopMoversStatus()
    });
  }, 250);
  topMoversTimer.unref?.();

  heartbeatTimer = setInterval(() => {
    for (const client of websocketServer.clients) {
      if (client.isAlive === false) {
        client.terminate();
        continue;
      }
      client.isAlive = false;
      if (client.readyState === WebSocket.OPEN) client.ping();
    }
  }, config.websocket.heartbeatMs);
  heartbeatTimer.unref?.();
  app.log.info(`Beanance Market API listening on http://${config.host}:${config.port}`);
}

export async function stopHttpServer() {
  if (heartbeatTimer) clearInterval(heartbeatTimer);
  if (topMoversTimer) clearInterval(topMoversTimer);
  if (marketFlushTimer) clearTimeout(marketFlushTimer);
  heartbeatTimer = null;
  topMoversTimer = null;
  marketFlushTimer = null;
  pendingMarketUpdates.clear();
  for (const client of websocketServer.clients) {
    removeDetailClient(client);
    client.close(1001, "Server shutting down");
  }
  await app.close();
}
