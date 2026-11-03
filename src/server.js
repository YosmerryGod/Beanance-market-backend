import Fastify from "fastify";
import cors from "@fastify/cors";
import WebSocket, { WebSocketServer } from "ws";
import { config } from "./config.js";
import {
  getMarket,
  getMarkets,
  getMarketStatus
} from "./market-store.js";
import { getDatabaseStatus } from "./database.js";
import {
  getMarketDepth,
  getMarketKlines,
  getMarketMetrics,
  getMarketTrades
} from "./detail-service.js";
import {
  getDetailStreamStatus,
  removeDetailClient,
  setDetailStreamHandler,
  subscribeDetailStreams,
  unsubscribeDetailStreams
} from "./detail-stream.js";

const app = Fastify({
  logger: {
    level: config.logLevel
  }
});

await app.register(cors, {
  origin: config.corsOrigin
});

const websocketServer = new WebSocketServer({
  noServer: true,
  path: "/ws/market",
  clientTracking: true,
  maxPayload: 64 * 1024
});

const clientSubscriptions = new Map();

function sendJson(socket, payload) {
  if (socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(payload));
  }
}

function broadcast(payload) {
  const message = JSON.stringify(payload);

  for (const client of websocketServer.clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  }
}

function parseLimit(value, fallback = 20, maximum = 200) {
  const limit = Number.parseInt(value, 10);
  if (!Number.isFinite(limit)) return fallback;
  return Math.min(Math.max(limit, 1), maximum);
}

function requireMarket(symbolValue, reply) {
  const symbol = String(symbolValue || "").toUpperCase();
  const market = getMarket(symbol);
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
  detailStreams: getDetailStreamStatus()
}));

app.get("/api/v1/market/futures", async (request) => {
  const sort = ["symbol", "quoteVolume", "priceChangePercent"].includes(
    request.query.sort
  )
    ? request.query.sort
    : "symbol";
  const order = request.query.order === "desc" ? "desc" : "asc";
  const markets = getMarkets({ sort, order });

  return {
    success: true,
    marketType: "USDT-M-PERPETUAL",
    count: markets.length,
    updatedAt: getMarketStatus().lastUpdateAt,
    data: markets
  };
});

app.get("/api/v1/market/futures/:symbol", async (request, reply) => {
  const market = getMarket(request.params.symbol);

  if (!market) {
    return reply.code(404).send({
      success: false,
      error: "MARKET_NOT_FOUND",
      message: "Futures market not found or not active"
    });
  }

  return {
    success: true,
    data: market
  };
});

app.get("/api/v1/market/futures/:symbol/klines", async (request, reply) => {
  const market = requireMarket(request.params.symbol, reply);
  if (!market) return reply;
  const interval = String(request.query.interval || "1m");
  const limit = parseLimit(request.query.limit, 500, 1500);
  const endTime = request.query.endTime === undefined
    ? null
    : Number(request.query.endTime);

  if (endTime !== null && !Number.isFinite(endTime)) {
    return reply.code(400).send({
      success: false,
      error: "INVALID_END_TIME",
      message: "endTime must be a valid Unix timestamp in milliseconds"
    });
  }

  const data = await getMarketKlines(
    market.symbol,
    interval,
    limit,
    endTime
  );

  return {
    success: true,
    symbol: market.symbol,
    interval,
    count: data.length,
    data
  };
});

app.get("/api/v1/market/futures/:symbol/depth", async (request, reply) => {
  const market = requireMarket(request.params.symbol, reply);
  if (!market) return reply;
  const limit = parseLimit(request.query.limit, 20, 100);
  const data = await getMarketDepth(market.symbol, limit);

  return { success: true, data };
});

app.get("/api/v1/market/futures/:symbol/trades", async (request, reply) => {
  const market = requireMarket(request.params.symbol, reply);
  if (!market) return reply;
  const limit = parseLimit(request.query.limit, 50, 1000);
  const data = await getMarketTrades(market.symbol, limit);

  return {
    success: true,
    symbol: market.symbol,
    count: data.length,
    data
  };
});

app.get("/api/v1/market/futures/:symbol/metrics", async (request, reply) => {
  const market = requireMarket(request.params.symbol, reply);
  if (!market) return reply;
  const data = await getMarketMetrics(market.symbol);

  return { success: true, data };
});

app.get("/api/v1/market/tickers", async () => {
  const markets = getMarkets({
    sort: "quoteVolume",
    order: "desc"
  });

  return {
    success: true,
    count: markets.length,
    updatedAt: getMarketStatus().lastUpdateAt,
    data: markets
  };
});

app.get("/api/v1/market/top-movers", async (request) => {
  const limit = parseLimit(request.query.limit);
  const markets = getMarkets();
  const withTicker = markets.filter(
    (market) => typeof market.priceChangePercent === "number"
  );

  const gainers = [...withTicker]
    .sort((a, b) => b.priceChangePercent - a.priceChangePercent)
    .slice(0, limit);

  const losers = [...withTicker]
    .sort((a, b) => a.priceChangePercent - b.priceChangePercent)
    .slice(0, limit);

  return {
    success: true,
    updatedAt: getMarketStatus().lastUpdateAt,
    data: {
      gainers,
      losers
    }
  };
});

websocketServer.on("connection", (socket) => {
  clientSubscriptions.set(socket, new Set());

  sendJson(socket, {
    type: "market.snapshot",
    timestamp: Date.now(),
    data: getMarkets()
  });

  socket.on("message", (raw) => {
    let message;
    try {
      message = JSON.parse(raw.toString());
    } catch {
      sendJson(socket, {
        type: "market.error",
        error: "INVALID_JSON",
        message: "WebSocket message must be valid JSON"
      });
      return;
    }

    const symbol = String(message.symbol || "").toUpperCase();
    if (message.type === "market.detail.subscribe") {
      if (!getMarket(symbol)) {
        sendJson(socket, {
          type: "market.error",
          error: "MARKET_NOT_FOUND",
          symbol
        });
        return;
      }

      const streams = subscribeDetailStreams(socket, symbol, message.streams);
      const clientSet = clientSubscriptions.get(socket);
      for (const stream of streams) {
        clientSet.add(`${symbol.toLowerCase()}@${stream}`);
      }

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
      const clientSet = clientSubscriptions.get(socket);
      for (const streamName of removed) clientSet.delete(streamName);

      sendJson(socket, {
        type: "market.detail.unsubscribed",
        symbol,
        streams: removed,
        timestamp: Date.now()
      });
    }
  });

  socket.on("close", () => {
    removeDetailClient(socket);
    clientSubscriptions.delete(socket);
  });

  socket.on("error", (error) => {
    app.log.warn({ err: error }, "Frontend WebSocket client error");
  });
});

setDetailStreamHandler((payload) => {
  const streamKey = `${payload.symbol.toLowerCase()}@${payload.stream}`;
  for (const client of websocketServer.clients) {
    if (
      client.readyState === WebSocket.OPEN &&
      clientSubscriptions.get(client)?.has(streamKey)
    ) {
      sendJson(client, payload);
    }
  }
});

app.server.on("upgrade", (request, socket, head) => {
  const pathname = new URL(
    request.url || "/",
    "http://localhost"
  ).pathname;

  if (pathname !== "/ws/market") {
    socket.destroy();
    return;
  }

  websocketServer.handleUpgrade(request, socket, head, (client) => {
    websocketServer.emit("connection", client, request);
  });
});

export function publishMarketUpdate(market) {
  broadcast({
    type: "market.ticker",
    timestamp: Date.now(),
    data: market
  });
}

export function publishMarketStatus(status) {
  broadcast({
    type: "market.status",
    timestamp: Date.now(),
    data: status
  });
}

export async function startHttpServer() {
  await app.listen({
    host: config.host,
    port: config.port
  });

  app.log.info(
    `Beanance Market API listening on http://${config.host}:${config.port}`
  );
}

export async function stopHttpServer() {
  for (const client of websocketServer.clients) {
    removeDetailClient(client);
    client.close(1001, "Server shutting down");
  }

  await app.close();
}
