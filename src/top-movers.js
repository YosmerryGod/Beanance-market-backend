import { getMarket, getMarkets } from "./market-store.js";
import { config } from "./config.js";
import { ManagedWebSocket } from "./websocket-manager.js";
import { saveTopMoverHistory } from "./database.js";

const MAX_RESULTS = 1000;
const MIN_MOVE_PERCENT = 3;
const MEDIUM_MOVE_PERCENT = 7;
const HIGH_MOVE_PERCENT = 11;
const TIMEFRAMES = [
  { interval: "1m", label: "1 mnt" },
  { interval: "5m", label: "5 mnt" },
  { interval: "15m", label: "15 mnt" },
  { interval: "1h", label: "1 jam" },
  { interval: "4h", label: "4 jam" },
  { interval: "1d", label: "1 hari" },
  { interval: "1w", label: "1 minggu" }
];

const eventsByKey = new Map();
const sockets = new Map();
let trackedSymbols = 0;
let connectedStreams = 0;
let lastMessageAt = null;
let lastError = null;
let started = false;

function classifyMove(changePercent) {
  const magnitude = Math.abs(changePercent);
  if (magnitude < MIN_MOVE_PERCENT) return null;
  if (magnitude < MEDIUM_MOVE_PERCENT) return "Kecil";
  if (magnitude < HIGH_MOVE_PERCENT) return "Sedang";
  return "Tinggi";
}

function buildStreamUrl() {
  const origin = new URL(config.binanceWsBaseUrl).origin;
  return `${origin}/ws`;
}

function handleKlineMessage(payload, timeframe) {
  const data = payload?.data ?? payload;
  const candle = data?.k;
  if (!candle || !candle.s || !candle.o || !candle.c) return;

  const symbol = String(candle.s).toUpperCase();
  const openPrice = Number(candle.o);
  const lastPrice = Number(candle.c);
  if (!(openPrice > 0) || !(lastPrice > 0)) return;

  const changePercent = ((lastPrice - openPrice) / openPrice) * 100;
  const magnitude = Math.abs(changePercent);
  const positive = changePercent >= 0;
  const direction = positive ? "Kenaikan" : "Penurunan";
  const severity = classifyMove(changePercent);
  const key = `${symbol}:${timeframe.interval}`;
  const eventTime = Number(data.E || candle.T || Date.now());

  lastMessageAt = eventTime;

  if (!severity) {
    eventsByKey.delete(key);
    return;
  }

  const market = getMarket(symbol);
  if (!market) return;

  const status = `[${severity}] ${direction} ${timeframe.label}`;
  eventsByKey.set(key, {
    symbol,
    baseAsset: market.baseAsset,
    quoteAsset: market.quoteAsset,
    contractType: market.contractType,
    lastPrice,
    priceChangePercent: changePercent,
    changePercent,
    quoteVolume: Number(market.quoteVolume || 0),
    baseVolume: Number(market.baseVolume || 0),
    highPrice: Number(market.highPrice || 0),
    lowPrice: Number(market.lowPrice || 0),
    positive,
    categories: [direction],
    category: direction,
    status,
    timeframe: timeframe.interval,
    severity,
    eventTime
  });

  // Persist only closed candles so live kline updates do not cause repeated SQLite writes.
  if (candle.x === true) {
    saveTopMoverHistory({
      symbol,
      baseAsset: market.baseAsset,
      quoteAsset: market.quoteAsset,
      timeframe: timeframe.interval,
      direction,
      severity,
      changePercent,
      lastPrice,
      quoteVolume: Number(market.quoteVolume || 0),
      baseVolume: Number(market.baseVolume || 0),
      highPrice: Number(market.highPrice || 0),
      lowPrice: Number(market.lowPrice || 0),
      candleOpenTime: Number(candle.t),
      candleCloseTime: Number(candle.T),
      eventTime
    });
  }
}

function createTimeframeSocket(timeframe, symbols) {
  const streams = symbols.map((symbol) => `${symbol.toLowerCase()}@kline_${timeframe.interval}`);
  const manager = new ManagedWebSocket({
    name: `Binance Top Movers ${timeframe.interval}`,
    url: buildStreamUrl(),
    onOpen: (socket) => {
      socket.send(JSON.stringify({
        method: "SUBSCRIBE",
        params: streams,
        id: TIMEFRAMES.findIndex((item) => item.interval === timeframe.interval) + 1
      }));
      connectedStreams += 1;
      console.info(`Top Movers subscribed to ${streams.length} ${timeframe.interval} kline streams`);
    },
    onMessage: (payload) => handleKlineMessage(payload, timeframe),
    onClose: () => {
      connectedStreams = Math.max(0, connectedStreams - 1);
    }
  });
  sockets.set(timeframe.interval, manager);
  manager.connect();
}

export async function startTopMovers() {
  if (started) return;
  started = true;

  const markets = getMarkets().filter((market) => market.isTrading);
  const symbols = markets.map((market) => market.symbol);
  trackedSymbols = symbols.length;

  if (!symbols.length) {
    started = false;
    throw new Error("No active USDT-M perpetual symbols available for Top Movers");
  }

  for (const timeframe of TIMEFRAMES) {
    createTimeframeSocket(timeframe, symbols);
  }

  console.info(`Top Movers started for ${trackedSymbols} symbols across ${TIMEFRAMES.length} timeframes`);
}

export function stopTopMovers() {
  for (const manager of sockets.values()) manager.stop();
  sockets.clear();
  eventsByKey.clear();
  connectedStreams = 0;
  trackedSymbols = 0;
  started = false;
}

export function getTopMovers(limit = MAX_RESULTS) {
  const safeLimit = Math.min(Math.max(Number(limit) || MAX_RESULTS, 1), MAX_RESULTS);
  return [...eventsByKey.values()]
    .sort((a, b) => b.eventTime - a.eventTime)
    .slice(0, safeLimit);
}

export function getTopMoversStatus() {
  return {
    trackedSymbols,
    timeframes: TIMEFRAMES.map((item) => item.interval),
    connectedStreams,
    connected: connectedStreams > 0,
    eventCount: eventsByKey.size,
    lastMessageAt,
    lastError
  };
}
