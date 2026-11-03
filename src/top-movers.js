import WebSocket from "ws";
import { config } from "./config.js";
import { fetchKlines } from "./binance.js";
import { getMarkets } from "./market-store.js";
import { ManagedWebSocket } from "./websocket-manager.js";

const candleBySymbol = new Map();
let socketManager = null;
let seedPromise = null;
let lastMessageAt = null;
let lastError = null;
let stopped = false;

const MIN_ABSOLUTE_CHANGE_PERCENT = 0.15;
const MIN_QUOTE_VOLUME = 100000;
const MAX_RESULTS = 200;

function normalizeCandle(symbol, kline) {
  return {
    symbol,
    openTime: Number(kline.t),
    closeTime: Number(kline.T),
    open: Number(kline.o),
    high: Number(kline.h),
    low: Number(kline.l),
    close: Number(kline.c),
    volume: Number(kline.v),
    quoteVolume: Number(kline.q),
    tradeCount: Number(kline.n),
    takerBuyQuoteVolume: Number(kline.Q),
    isClosed: Boolean(kline.x)
  };
}

function applyKlineMessage(message) {
  const data = message?.data ?? message;
  const kline = data?.k;
  const symbol = String(data?.s || kline?.s || "").toUpperCase();
  if (!symbol || !kline || kline.i !== "1m") return;

  const current = normalizeCandle(symbol, kline);
  const existing = candleBySymbol.get(symbol);
  const priorCandle = existing?.current && existing.current.openTime < current.openTime
    ? existing.current
    : existing?.previous || null;
  candleBySymbol.set(symbol, {
    previous: current.isClosed ? current : priorCandle,
    current
  });
  lastMessageAt = Date.now();
  lastError = null;
}

function getEvent(candles, market) {
  const current = candles?.current;
  const previous = candles?.previous;
  if (!current || !previous || previous.close <= 0 || current.open <= 0) return null;

  const changePercent = ((current.close - previous.close) / previous.close) * 100;
  const candleChangePercent = ((current.close - current.open) / current.open) * 100;
  const previousRangePercent = previous.close > 0
    ? ((previous.high - previous.low) / previous.close) * 100
    : 0;
  const quoteVolume = current.quoteVolume || 0;
  const previousQuoteVolume = previous.quoteVolume || 0;
  const volumeRatio = previousQuoteVolume > 0 ? quoteVolume / previousQuoteVolume : null;

  if (!Number.isFinite(changePercent) || Math.abs(changePercent) < MIN_ABSOLUTE_CHANGE_PERCENT) return null;
  if (quoteVolume < MIN_QUOTE_VOLUME) return null;

  const positive = changePercent >= 0;
  const strongVolume = volumeRatio !== null && volumeRatio >= 1.5;
  const strongRange = Math.abs(candleChangePercent) >= Math.max(previousRangePercent, 0.1);
  let status = positive ? "Momentum naik" : "Momentum turun";
  if (strongVolume && positive) status = "Rally";
  if (strongVolume && !positive) status = "Sell-off";
  if (strongRange && positive && current.close >= current.high * 0.999) status = "Breakout";
  if (strongRange && !positive && current.close <= current.low * 1.001) status = "Breakdown";

  return {
    symbol: current.symbol,
    baseAsset: market?.baseAsset || current.symbol.replace(/USDT$/, ""),
    quoteAsset: market?.quoteAsset || "USDT",
    contractType: market?.contractType || "PERPETUAL",
    lastPrice: current.close,
    previousClose: previous.close,
    candleOpen: current.open,
    candleHigh: current.high,
    candleLow: current.low,
    candleVolume: current.volume,
    quoteVolume,
    previousQuoteVolume,
    volumeRatio,
    previousRangePercent,
    changePercent,
    candleChangePercent,
    positive,
    status,
    category: positive ? "Kenaikan" : "Penurunan",
    eventTime: Date.now(),
    candleOpenTime: current.openTime,
    candleCloseTime: current.closeTime,
    candleClosed: current.isClosed
  };
}

export function getTopMovers(limit = 50) {
  const markets = new Map(getMarkets().map((market) => [market.symbol, market]));
  return [...candleBySymbol.entries()]
    .map(([symbol, candles]) => getEvent(candles, markets.get(symbol)))
    .filter(Boolean)
    .sort((a, b) => Math.abs(b.changePercent) - Math.abs(a.changePercent))
    .slice(0, Math.min(Math.max(Number(limit) || 50, 1), MAX_RESULTS));
}

export function getTopMoversStatus() {
  return {
    trackedSymbols: candleBySymbol.size,
    connected: Boolean(socketManager?.connected),
    lastMessageAt,
    lastError
  };
}

async function seedPreviousCandles(symbols) {
  const queue = [...symbols];
  const workers = Array.from({ length: 6 }, async () => {
    while (queue.length && !stopped) {
      const symbol = queue.shift();
      try {
        const rows = await fetchKlines(symbol, "1m", 2);
        if (!Array.isArray(rows) || rows.length < 2) continue;
        const previous = {
          symbol,
          openTime: Number(rows[0][0]),
          open: Number(rows[0][1]),
          high: Number(rows[0][2]),
          low: Number(rows[0][3]),
          close: Number(rows[0][4]),
          volume: Number(rows[0][5]),
          closeTime: Number(rows[0][6]),
          quoteVolume: Number(rows[0][7]),
          tradeCount: Number(rows[0][8]),
          takerBuyQuoteVolume: Number(rows[0][10]),
          isClosed: true
        };
        const current = {
          symbol,
          openTime: Number(rows[1][0]),
          open: Number(rows[1][1]),
          high: Number(rows[1][2]),
          low: Number(rows[1][3]),
          close: Number(rows[1][4]),
          volume: Number(rows[1][5]),
          closeTime: Number(rows[1][6]),
          quoteVolume: Number(rows[1][7]),
          tradeCount: Number(rows[1][8]),
          takerBuyQuoteVolume: Number(rows[1][10]),
          isClosed: Number(rows[1][6]) < Date.now()
        };
        const existing = candleBySymbol.get(symbol);
        if (!existing || current.openTime >= existing.current.openTime) {
          candleBySymbol.set(symbol, { previous, current });
        }
      } catch (error) {
        lastError = error.message;
      }
    }
  });
  await Promise.all(workers);
}

function buildStreamUrl(symbols) {
  const base = config.binanceWsBaseUrl.replace(/\/$/, "");
  const streams = symbols.map((symbol) => `${symbol.toLowerCase()}@kline_1m`).join("/");
  return `${base}?streams=${streams}`;
}

export async function startTopMovers() {
  stopped = false;
  const symbols = getMarkets().map((market) => market.symbol);
  if (!symbols.length) return;
  if (seedPromise) await seedPromise;
  seedPromise = seedPreviousCandles(symbols).finally(() => { seedPromise = null; });
  await seedPromise;
  if (stopped || socketManager) return;

  socketManager = new ManagedWebSocket({
    name: "Binance top movers 1m",
    url: buildStreamUrl(symbols),
    onMessage: applyKlineMessage,
    onClose: () => { lastError = "Binance 1m kline stream disconnected"; }
  });
  socketManager.connect();
}

export function stopTopMovers() {
  stopped = true;
  socketManager?.stop();
  socketManager = null;
}
