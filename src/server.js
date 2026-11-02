import Fastify from "fastify";
import cors from "@fastify/cors";
import WebSocket, { WebSocketServer } from "ws";
import { config } from "./config.js";
import {
  getMarket,
  getMarkets,
  getMarketStatus
} from "./market-store.js";

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
  maxPayload: 1024 * 1024
});

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

function parseLimit(value, fallback = 20) {
  const limit = Number.parseInt(value, 10);
  if (!Number.isFinite(limit)) return fallback;
  return Math.min(Math.max(limit, 1), 200);
}

app.get("/health", async () => ({
  success: true,
  service: "beanance-market-backend",
  status: "ok",
  timestamp: Date.now(),
  market: getMarketStatus()
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

app.get("/api/v1/market/tickers", async (request) => {
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
  sendJson(socket, {
    type: "market.snapshot",
    timestamp: Date.now(),
    data: getMarkets()
  });

  socket.on("error", (error) => {
    app.log.warn({ err: error }, "Frontend WebSocket client error");
  });
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
    client.close(1001, "Server shutting down");
  }

  await app.close();
}
