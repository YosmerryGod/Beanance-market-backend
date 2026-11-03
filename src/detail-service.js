import {
  fetchKlines as fetchBinanceKlines,
  fetchOpenInterest,
  fetchOrderBook,
  fetchPremiumIndex,
  fetchRecentTrades
} from "./binance.js";
import {
  getStoredKlines,
  getStoredMetrics,
  saveKline,
  saveKlines,
  saveMarketMetrics
} from "./database.js";

export const KLINE_INTERVALS = new Set([
  "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h",
  "6h", "8h", "12h", "1d", "3d", "1w", "1M"
]);

const klineLoads = new Map();
const detailCache = new Map();
const DETAIL_CACHE_TTL_MS = 1000;

function normalizeSymbol(value) {
  return String(value || "").toUpperCase();
}

function normalizeKlineRow(symbol, interval, row) {
  return {
    symbol,
    interval,
    openTime: Number(row[0]),
    open: Number(row[1]),
    high: Number(row[2]),
    low: Number(row[3]),
    close: Number(row[4]),
    volume: Number(row[5]),
    closeTime: Number(row[6]),
    quoteVolume: Number(row[7]),
    tradeCount: Number(row[8]),
    takerBuyBaseVolume: Number(row[9]),
    takerBuyQuoteVolume: Number(row[10]),
    isClosed: Number(row[6]) < Date.now()
  };
}

export async function getMarketKlines(symbolValue, intervalValue, limitValue = 500, endTimeValue = null) {
  const symbol = normalizeSymbol(symbolValue);
  const interval = String(intervalValue || "1m");
  const limit = Math.min(Math.max(Number.parseInt(limitValue, 10) || 500, 1), 1500);
  const endTime = endTimeValue === null ? null : Number(endTimeValue);

  if (!/^[A-Z0-9]{3,30}$/.test(symbol)) {
    const error = new Error("Invalid market symbol");
    error.statusCode = 400;
    throw error;
  }
  if (!KLINE_INTERVALS.has(interval)) {
    const error = new Error("Unsupported kline interval");
    error.statusCode = 400;
    throw error;
  }

  const stored = getStoredKlines(symbol, interval, limit, endTime);
  if (stored.length >= limit) return stored;

  const loadKey = `${symbol}:${interval}:${endTime || "latest"}`;
  if (klineLoads.has(loadKey)) return klineLoads.get(loadKey);

  const loadPromise = (async () => {
    const rows = await fetchBinanceKlines(symbol, interval, limit, endTime);
    const normalized = rows.map((row) => normalizeKlineRow(symbol, interval, row));
    if (normalized.length) saveKlines(normalized);
    return normalized.map(({ symbol: _symbol, interval: _interval, ...candle }) => candle);
  })();

  klineLoads.set(loadKey, loadPromise);
  try {
    return await loadPromise;
  } finally {
    klineLoads.delete(loadKey);
  }
}

async function cached(key, loader) {
  const current = detailCache.get(key);
  if (current && current.expiresAt > Date.now()) return current.value;

  const value = await loader();
  detailCache.set(key, {
    value,
    expiresAt: Date.now() + DETAIL_CACHE_TTL_MS
  });
  return value;
}

export async function getMarketDepth(symbolValue, limitValue = 20) {
  const symbol = normalizeSymbol(symbolValue);
  const limit = [5, 10, 20, 50, 100].includes(Number(limitValue))
    ? Number(limitValue)
    : 20;

  return cached(`depth:${symbol}:${limit}`, async () => {
    const payload = await fetchOrderBook(symbol, limit);
    return {
      symbol,
      lastUpdateId: payload.lastUpdateId,
      bids: payload.bids.map(([price, quantity]) => ({
        price: Number(price),
        quantity: Number(quantity)
      })),
      asks: payload.asks.map(([price, quantity]) => ({
        price: Number(price),
        quantity: Number(quantity)
      })),
      timestamp: Date.now()
    };
  });
}

export async function getMarketTrades(symbolValue, limitValue = 50) {
  const symbol = normalizeSymbol(symbolValue);
  const limit = Math.min(Math.max(Number.parseInt(limitValue, 10) || 50, 1), 1000);

  return cached(`trades:${symbol}:${limit}`, async () => {
    const rows = await fetchRecentTrades(symbol, limit);
    return rows.map((trade) => ({
      id: trade.id,
      price: Number(trade.price),
      quantity: Number(trade.qty),
      quoteQuantity: Number(trade.quoteQty),
      time: trade.time,
      isBuyerMaker: Boolean(trade.isBuyerMaker)
    }));
  });
}

export async function getMarketMetrics(symbolValue, forceRefresh = false) {
  const symbol = normalizeSymbol(symbolValue);
  if (!forceRefresh) {
    const stored = getStoredMetrics(symbol);
    if (stored && Date.now() - stored.updatedAt < 15000) return stored;
  }

  const [premium, interest] = await Promise.all([
    fetchPremiumIndex(symbol),
    fetchOpenInterest(symbol).catch(() => null)
  ]);

  const metrics = {
    symbol,
    markPrice: Number(premium.markPrice),
    indexPrice: Number(premium.indexPrice),
    estimatedSettlePrice: Number(premium.estimatedSettlePrice),
    fundingRate: Number(premium.lastFundingRate),
    nextFundingTime: Number(premium.nextFundingTime),
    openInterest: interest ? Number(interest.openInterest) : null,
    openInterestUpdatedAt: interest ? Number(interest.time || Date.now()) : null
  };

  saveMarketMetrics(metrics);
  return { ...metrics, updatedAt: Date.now() };
}

export function persistRealtimeKline(symbol, interval, kline) {
  const candle = {
    symbol,
    interval,
    openTime: Number(kline.t),
    open: Number(kline.o),
    high: Number(kline.h),
    low: Number(kline.l),
    close: Number(kline.c),
    volume: Number(kline.v),
    closeTime: Number(kline.T),
    quoteVolume: Number(kline.q),
    tradeCount: Number(kline.n),
    takerBuyBaseVolume: Number(kline.V),
    takerBuyQuoteVolume: Number(kline.Q),
    isClosed: Boolean(kline.x)
  };

  if (candle.isClosed) saveKline(candle);

  return candle;
}
