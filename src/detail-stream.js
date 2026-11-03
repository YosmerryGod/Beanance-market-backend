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
const upstreams = {
  market: null,
  public: null
};
const reconnectTimers = {
  market: null,
  public: null
};
const reconnectAttempts = {
  market: 0,
  public: 0
};
let shuttingDown = false;
let messageId = 1;
let onDataHandler = () => {};

function streamChannel(streamNameValue) {
  return streamNameValue.endsWith("@depth20@100ms") ? "public" : "market";
}

// [MODIFIED] Keep Binance's separate Market Streams and Public Streams endpoints.
function upstreamUrl(channel) {
  const base = config.binanceWsBaseUrl
    .replace(/\/(?:market|public)\/stream\/?$/, "")
    .replace(/\/$/, "");
  return `${base}/${channel}/stream`;
}

function sendCommand(channel, method, params) {
  const socket = upstreams[channel];
  if (!params.length || socket?.readyState !== WebSocket.OPEN) return;

  socket.send(JSON.stringify({
    method,
    params,
    id: messageId++
  }));
}

function streamName(symbol, stream) {
  return `${symbol.toLowerCase()}@${stream}`;
}

function scheduleReconnect(channel) {
  if (shuttingDown || reconnectTimers[channel]) return;

  const delay = Math.min(
    config.reconnectBaseDelayMs * 2 ** reconnectAttempts[channel],
    config.reconnectMaxDelayMs
  );
  reconnectAttempts[channel] += 1;

  reconnectTimers[channel] = setTimeout(() => {
    reconnectTimers[channel] = null;
    connect(channel);
  }, delay);

  console.warn(`Binance ${channel} WebSocket reconnect scheduled in ${delay}ms`);
}

function connect(channel) {
  if (shuttingDown || upstreams[channel]) return;

  const socket = new WebSocket(upstreamUrl(channel), {
    handshakeTimeout: 15000
  });
  upstreams[channel] = socket;

  socket.on("open", () => {
    reconnectAttempts[channel] = 0;
    console.info(`Binance ${channel} WebSocket connected: ${upstreamUrl(channel)}`);

    const streams = [...streamRefCounts.keys()].filter(
      (name) => streamChannel(name) === channel
    );

    for (let index = 0; index < streams.length; index += 100) {
      sendCommand(channel, "SUBSCRIBE", streams.slice(index, index + 100));
    }
  });

  socket.on("message", (raw) => {
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

  socket.on("error", (error) => {
    console.error(`Binance ${channel} WebSocket error:`, error.message);
  });

  socket.on("close", (code, reason) => {
    if (upstreams[channel] === socket) upstreams[channel] = null;
    console.warn(
      `Binance ${channel} WebSocket closed:`,
      code,
      reason?.toString() || ""
    );
    scheduleReconnect(channel);
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
  const addedByChannel = {
    market: [],
    public: []
  };

  for (const stream of streams) {
    const name = streamName(symbol, stream);
    if (clientSubscriptions.has(name)) continue;

    clientSubscriptions.add(name);
    const previousCount = streamRefCounts.get(name) || 0;
    streamRefCounts.set(name, previousCount + 1);

    if (previousCount === 0) {
      addedByChannel[streamChannel(name)].push(name);
    }
  }

  for (const channel of ["market", "public"]) {
    const added = addedByChannel[channel];
    if (!added.length) continue;
    if (!upstreams[channel]) connect(channel);
    sendCommand(channel, "SUBSCRIBE", added);
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
  const clientRemoved = [];
  const removedByChannel = {
    market: [],
    public: []
  };

  for (const stream of streams) {
    const name = streamName(symbol, stream);
    if (!clientSubscriptions.delete(name)) continue;
    clientRemoved.push(name);

    const nextCount = (streamRefCounts.get(name) || 1) - 1;
    if (nextCount <= 0) {
      streamRefCounts.delete(name);
      removedByChannel[streamChannel(name)].push(name);
    } else {
      streamRefCounts.set(name, nextCount);
    }
  }

  for (const channel of ["market", "public"]) {
    sendCommand(channel, "UNSUBSCRIBE", removedByChannel[channel]);
  }

  return clientRemoved;
}

export function removeDetailClient(client) {
  const clientSubscriptions = subscriptionsByClient.get(client);
  if (!clientSubscriptions) return;

  const removedByChannel = {
    market: [],
    public: []
  };

  for (const name of clientSubscriptions) {
    const nextCount = (streamRefCounts.get(name) || 1) - 1;
    if (nextCount <= 0) {
      streamRefCounts.delete(name);
      removedByChannel[streamChannel(name)].push(name);
    } else {
      streamRefCounts.set(name, nextCount);
    }
  }

  subscriptionsByClient.delete(client);

  for (const channel of ["market", "public"]) {
    sendCommand(channel, "UNSUBSCRIBE", removedByChannel[channel]);
  }
}

export function getDetailStreamStatus() {
  return {
    connected: Object.values(upstreams).some(
      (socket) => socket?.readyState === WebSocket.OPEN
    ),
    marketConnected: upstreams.market?.readyState === WebSocket.OPEN,
    publicConnected: upstreams.public?.readyState === WebSocket.OPEN,
    activeStreams: streamRefCounts.size,
    clients: subscriptionsByClient.size
  };
}

export function stopDetailStreams() {
  shuttingDown = true;

  for (const channel of ["market", "public"]) {
    if (reconnectTimers[channel]) {
      clearTimeout(reconnectTimers[channel]);
      reconnectTimers[channel] = null;
    }

    const socket = upstreams[channel];
    if (socket) {
      socket.removeAllListeners();
      socket.close();
      upstreams[channel] = null;
    }
  }

  subscriptionsByClient.clear();
  streamRefCounts.clear();
}
