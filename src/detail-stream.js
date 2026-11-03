import WebSocket from "ws";
import { config } from "./config.js";
import { persistRealtimeKline } from "./detail-service.js";
import { ManagedWebSocket } from "./websocket-manager.js";

const allowedStreams = new Set([
  "kline_1m", "kline_5m", "kline_15m", "kline_30m",
  "kline_1h", "kline_4h", "kline_1d", "depth20@100ms", "aggTrade"
]);
const subscriptionsByClient = new Map();
const clientsByStream = new Map();
const upstreams = { market: null, public: null };
const idleTimers = { market: null, public: null };
let messageId = 1;
let onDataHandler = () => {};
let shuttingDown = false;

function streamChannel(name) {
  return name.endsWith("@depth20@100ms") ? "public" : "market";
}

function streamName(symbol, stream) {
  return `${symbol.toLowerCase()}@${stream}`;
}

function baseWsUrl() {
  return config.binanceWsBaseUrl.replace(/\/(?:market|public)\/stream\/?$/, "").replace(/\/$/, "");
}

function sendCommand(channel, method, params) {
  const socket = upstreams[channel]?.getSocket();
  if (!params.length || socket?.readyState !== WebSocket.OPEN) return;
  socket.send(JSON.stringify({ method, params, id: messageId++ }));
}

function scheduleIdleClose(channel) {
  if (idleTimers[channel]) clearTimeout(idleTimers[channel]);
  if ([...clientsByStream.keys()].some((name) => streamChannel(name) === channel)) return;
  idleTimers[channel] = setTimeout(() => {
    idleTimers[channel] = null;
    if ([...clientsByStream.keys()].some((name) => streamChannel(name) === channel)) return;
    upstreams[channel]?.stop();
    upstreams[channel] = null;
    console.info(`Binance ${channel} upstream closed after idle period`);
  }, config.websocket.idleCloseMs);
  idleTimers[channel].unref?.();
}

function handleUpstreamMessage(message) {
  if (!message?.e) return;
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
      eventTime: message.E, transactionTime: message.T,
      firstUpdateId: message.U, finalUpdateId: message.u,
      bids: (message.b || []).map(([price, quantity]) => ({ price: Number(price), quantity: Number(quantity) })),
      asks: (message.a || []).map(([price, quantity]) => ({ price: Number(price), quantity: Number(quantity) }))
    };
  } else if (event === "aggTrade") {
    stream = "aggTrade";
    data = {
      id: message.a, price: Number(message.p), quantity: Number(message.q),
      firstTradeId: message.f, lastTradeId: message.l,
      time: message.T, isBuyerMaker: Boolean(message.m)
    };
  }

  if (!stream || !symbol) return;
  onDataHandler({ type: `market.detail.${event}`, symbol, stream, timestamp: Date.now(), data });
}

function connect(channel) {
  if (shuttingDown || upstreams[channel]) return;
  const url = `${baseWsUrl()}/${channel}/stream`;
  const manager = new ManagedWebSocket({
    name: `Binance ${channel}`,
    url,
    hasDemand: () => !shuttingDown && [...clientsByStream.keys()].some((name) => streamChannel(name) === channel),
    onOpen: async () => {
      const streams = [...clientsByStream.keys()].filter((name) => streamChannel(name) === channel);
      for (let index = 0; index < streams.length; index += config.websocket.commandBatchSize) {
        sendCommand(channel, "SUBSCRIBE", streams.slice(index, index + config.websocket.commandBatchSize));
        if (index + config.websocket.commandBatchSize < streams.length) {
          await new Promise((resolve) => setTimeout(resolve, 150));
        }
      }
    },
    onMessage: handleUpstreamMessage,
    onClose: () => {
      if (!shuttingDown && ![...clientsByStream.keys()].some((name) => streamChannel(name) === channel)) scheduleIdleClose(channel);
    }
  });
  upstreams[channel] = manager;
  manager.connect();
}

export function setDetailStreamHandler(handler) {
  onDataHandler = typeof handler === "function" ? handler : () => {};
}

export function subscribeDetailStreams(client, symbolValue, requestedStreams) {
  const symbol = String(symbolValue || "").toUpperCase();
  if (!/^[A-Z0-9]{3,30}$/.test(symbol)) return [];
  const requested = [...new Set((Array.isArray(requestedStreams) ? requestedStreams : []).filter((stream) => allowedStreams.has(stream)))];
  if (!subscriptionsByClient.has(client)) subscriptionsByClient.set(client, new Set());
  const clientSet = subscriptionsByClient.get(client);
  const accepted = [];
  const addedByChannel = { market: [], public: [] };

  for (const stream of requested) {
    const name = streamName(symbol, stream);
    if (clientSet.has(name)) {
      accepted.push(stream);
      continue;
    }
    if (clientSet.size >= config.websocket.maxSubscriptionsPerClient) break;
    if (!clientsByStream.has(name) && clientsByStream.size >= config.websocket.maxActiveStreams) break;
    clientSet.add(name);
    if (!clientsByStream.has(name)) clientsByStream.set(name, new Set());
    const subscribers = clientsByStream.get(name);
    const wasEmpty = subscribers.size === 0;
    subscribers.add(client);
    accepted.push(stream);

    if (wasEmpty) {
      const channel = streamChannel(name);
      if (idleTimers[channel]) {
        clearTimeout(idleTimers[channel]);
        idleTimers[channel] = null;
      }
      if (!upstreams[channel]) connect(channel);
      else addedByChannel[channel].push(name);
    }
  }
  for (const channel of ["market", "public"]) {
    if (addedByChannel[channel].length) sendCommand(channel, "SUBSCRIBE", addedByChannel[channel]);
  }
  return accepted;
}

export function unsubscribeDetailStreams(client, symbolValue, requestedStreams) {
  const clientSet = subscriptionsByClient.get(client);
  if (!clientSet) return [];
  const symbol = String(symbolValue || "").toUpperCase();
  const streams = Array.isArray(requestedStreams) ? requestedStreams.filter((stream) => allowedStreams.has(stream)) : [...allowedStreams];
  const removed = [];
  const removedByChannel = { market: [], public: [] };

  for (const stream of streams) {
    const name = streamName(symbol, stream);
    if (!clientSet.delete(name)) continue;
    removed.push(name);
    const subscribers = clientsByStream.get(name);
    subscribers?.delete(client);
    if (subscribers?.size === 0) {
      clientsByStream.delete(name);
      const channel = streamChannel(name);
      removedByChannel[channel].push(name);
      scheduleIdleClose(channel);
    }
  }
  for (const channel of ["market", "public"]) {
    if (removedByChannel[channel].length) sendCommand(channel, "UNSUBSCRIBE", removedByChannel[channel]);
  }
  if (clientSet.size === 0) subscriptionsByClient.delete(client);
  return removed;
}

export function removeDetailClient(client) {
  const clientSet = subscriptionsByClient.get(client);
  if (!clientSet) return;
  const removedByChannel = { market: [], public: [] };

  for (const name of clientSet) {
    const subscribers = clientsByStream.get(name);
    subscribers?.delete(client);
    if (subscribers?.size === 0) {
      clientsByStream.delete(name);
      const channel = streamChannel(name);
      removedByChannel[channel].push(name);
      scheduleIdleClose(channel);
    }
  }
  for (const channel of ["market", "public"]) {
    if (removedByChannel[channel].length) sendCommand(channel, "UNSUBSCRIBE", removedByChannel[channel]);
  }
  subscriptionsByClient.delete(client);
}

export function forEachDetailStreamClient(symbol, stream, callback) {
  const subscribers = clientsByStream.get(streamName(symbol, stream));
  if (!subscribers) return;
  for (const client of subscribers) callback(client);
}

export function getDetailStreamStatus() {
  return {
    connected: Object.values(upstreams).some((manager) => manager?.connected),
    marketConnected: Boolean(upstreams.market?.connected),
    publicConnected: Boolean(upstreams.public?.connected),
    activeStreams: clientsByStream.size,
    clients: subscriptionsByClient.size
  };
}

export function stopDetailStreams() {
  shuttingDown = true;
  for (const channel of ["market", "public"]) {
    if (idleTimers[channel]) clearTimeout(idleTimers[channel]);
    idleTimers[channel] = null;
    upstreams[channel]?.stop();
    upstreams[channel] = null;
  }
  subscriptionsByClient.clear();
  clientsByStream.clear();
}
