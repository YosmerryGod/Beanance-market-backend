import Fastify from "fastify";
import cors from "@fastify/cors";
import rateLimit from "@fastify/rate-limit";
import WebSocket, { WebSocketServer } from "ws";
import { config } from "./config.js";
import { getBinanceRestStatus } from "./binance.js";
import { getMarket, getMarkets, getMarketStatus } from "./market-store.js";
import { getDatabaseStatus } from "./database.js";
import { getTopMovers, getTopMoversStatus } from "./top-movers.js";
import { getMarketDepth, getMarketKlines, getMarketMetrics, getMarketTrades } from "./detail-service.js";
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

app.get("/api/v1/market/top-movers", async (request) => {
  const limit = parseLimit(request.query.limit, 500, 1000);
  const events = getTopMovers(limit);
  const gainers = events.filter((item) => item.positive);
  const losers = events.filter((item) => !item.positive);
  return {
    success: true,
    updatedAt: Date.now(),
    data: { events, gainers, losers },
    meta: getTopMoversStatus()
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
  sendJson(socket, { type: "market.top-movers", timestamp: Date.now(), data: { events: getTopMovers(500) } });

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
  topMoversTimer = setInterval(() => {
    if (!websocketServer.clients.size) return;
    broadcast({
      type: "market.top-movers",
      timestamp: Date.now(),
      data: { events: getTopMovers(50) }
    });
  }, 1000);
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
