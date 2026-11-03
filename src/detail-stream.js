import WebSocket from "ws";
import { config } from "./config.js";
import { persistRealtimeKline } from "./detail-service.js";

const allowedStreams = new Set([
  "kline_1m", "kline_5m", "kline_15m", "kline_30m",
  "kline_1h", "kline_4h", "kline_1d",
  "depth20@100ms",
  "aggTrade"
]);

const subscriptionsByClient = new Map();
const streamRefCounts = new Map();
let upstream = null;
let reconnectTimer = null;
let reconnectAttempts = 0;
let shuttingDown = false;
let messageId = 1;
let onDataHandler = () => {};

function upstreamUrl() {
  const base = config.binanceWsBaseUrl
    .replace(/\/market\/stream\/?$/, "")
    .replace(/\/$/, "");
  return `${base}/ws`;
}

function sendCommand(method, params) {
  if (!params.length || upstream?.readyState !== WebSocket.OPEN) return;

  upstream.send(JSON.stringify({
    method,
    params,
    id: messageId++
  }));
}

function streamName(symbol, stream) {
  return `${symbol.toLowerCase()}@${stream}`;
}

function scheduleReconnect() {
  if (shuttingDown || reconnectTimer) return;
  const delay = Math.min(
    config.reconnectBaseDelayMs * 2 ** reconnectAttempts,
    config.reconnectMaxDelayMs
  );
  reconnectAttempts += 1;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connect();
  }, delay);
}

function connect() {
  if (shuttingDown || upstream) return;

  upstream = new WebSocket(upstreamUrl(), { handshakeTimeout: 15000 });

  upstream.on("open", () => {
    reconnectAttempts = 0;
    const streams = [...streamRefCounts.keys()];
    for (let index = 0; index < streams.length; index += 100) {
      sendCommand("SUBSCRIBE", streams.slice(index, index + 100));
    }
  });

  upstream.on("message", (raw) => {
    let message;
    try {
      message = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (!message || !message.e) return;

    const event = message.e;
    const symbol = String(message.s || "").toUpperCase();
    let stream = null;
    let data = message;

    if (event === "kline" && message.k) {
      stream = `kline_${message.k.i}`;
      data = persistRealtimeKline(symbol, message.k.i, message.k);
    } else if (event === "depthUpdate") {
      stream = "depth20@100ms";
      data = {
        eventTime: message.E,
        transactionTime: message.T,
        firstUpdateId: message.U,
        finalUpdateId: message.u,
        bids: (message.b || []).map(([price, quantity]) => ({
          price: Number(price), quantity: Number(quantity)
        })),
        asks: (message.a || []).map(([price, quantity]) => ({
          price: Number(price), quantity: Number(quantity)
        }))
      };
    } else if (event === "aggTrade") {
      stream = "aggTrade";
      data = {
        id: message.a,
        price: Number(message.p),
        quantity: Number(message.q),
        firstTradeId: message.f,
        lastTradeId: message.l,
        time: message.T,
        isBuyerMaker: Boolean(message.m)
      };
    }

    if (!stream || !symbol) return;
    onDataHandler({
      type: `market.detail.${event}`,
      symbol,
      stream,
      timestamp: Date.now(),
      data
    });
  });

  upstream.on("error", (error) => {
    console.error("Binance detail WebSocket error:", error.message);
  });

  upstream.on("close", () => {
    upstream = null;
    scheduleReconnect();
  });
}

export function setDetailStreamHandler(handler) {
  onDataHandler = typeof handler === "function" ? handler : () => {};
}

export function subscribeDetailStreams(client, symbolValue, requestedStreams) {
  const symbol = String(symbolValue || "").toUpperCase();
  if (!/^[A-Z0-9]{3,30}$/.test(symbol)) return [];

  const streams = [...new Set(
    (Array.isArray(requestedStreams) ? requestedStreams : [])
      .filter((stream) => allowedStreams.has(stream))
  )];

  if (!subscriptionsByClient.has(client)) {
    subscriptionsByClient.set(client, new Set());
  }

  const clientSubscriptions = subscriptionsByClient.get(client);
  const added = [];

  for (const stream of streams) {
    const name = streamName(symbol, stream);
    if (clientSubscriptions.has(name)) continue;

    clientSubscriptions.add(name);
    const previousCount = streamRefCounts.get(name) || 0;
    streamRefCounts.set(name, previousCount + 1);
    if (previousCount === 0) added.push(name);
  }

  if (added.length) {
    if (!upstream) connect();
    sendCommand("SUBSCRIBE", added);
  }

  return streams;
}

export function unsubscribeDetailStreams(client, symbolValue, requestedStreams) {
  const clientSubscriptions = subscriptionsByClient.get(client);
  if (!clientSubscriptions) return [];

  const symbol = String(symbolValue || "").toUpperCase();
  const streams = Array.isArray(requestedStreams)
    ? requestedStreams.filter((stream) => allowedStreams.has(stream))
    : [...allowedStreams];
  const removed = [];

  for (const stream of streams) {
    const name = streamName(symbol, stream);
    if (!clientSubscriptions.delete(name)) continue;

    const nextCount = (streamRefCounts.get(name) || 1) - 1;
    if (nextCount <= 0) {
      streamRefCounts.delete(name);
      removed.push(name);
    } else {
      streamRefCounts.set(name, nextCount);
    }
  }

  if (removed.length) sendCommand("UNSUBSCRIBE", removed);
  return removed;
}

export function removeDetailClient(client) {
  const clientSubscriptions = subscriptionsByClient.get(client);
  if (!clientSubscriptions) return;

  const removed = [];
  for (const name of clientSubscriptions) {
    const nextCount = (streamRefCounts.get(name) || 1) - 1;
    if (nextCount <= 0) {
      streamRefCounts.delete(name);
      removed.push(name);
    } else {
      streamRefCounts.set(name, nextCount);
    }
  }

  subscriptionsByClient.delete(client);
  if (removed.length) sendCommand("UNSUBSCRIBE", removed);
}

export function getDetailStreamStatus() {
  return {
    connected: upstream?.readyState === WebSocket.OPEN,
    activeStreams: streamRefCounts.size,
    clients: subscriptionsByClient.size
  };
}

export function stopDetailStreams() {
  shuttingDown = true;
  if (reconnectTimer) clearTimeout(reconnectTimer);
  if (upstream) {
    upstream.removeAllListeners();
    upstream.close();
    upstream = null;
  }
  subscriptionsByClient.clear();
  streamRefCounts.clear();
}
