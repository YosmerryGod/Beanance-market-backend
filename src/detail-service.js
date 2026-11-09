import {
  fetchKlines as fetchBinanceKlines,
  fetchFundingRates as fetchBinanceFundingRates,
  fetchAllPremiumIndex,
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
  saveMarketMetrics,
  savePremiumIndexSnapshots
} from "./database.js";
import { config } from "./config.js";

export const KLINE_INTERVALS = new Set([
  "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h",
  "6h", "8h", "12h", "1d", "3d", "1w", "1M"
]);

const detailCache = new Map();
const inFlight = new Map();

function normalizeSymbol(value) {
  const symbol = String(value || "").toUpperCase();
  // [MODIFIED] Accept Unicode letters and numbers in Binance symbols, including CJK symbols.
  if (!/^[\p{L}\p{N}]{3,30}$/u.test(symbol)) {
    const error = new Error("Invalid market symbol");
    error.statusCode = 400;
    throw error;
  }
  return symbol;
}

function cacheGet(key) {
  const entry = detailCache.get(key);
  if (!entry) return undefined;
  if (entry.expiresAt <= Date.now()) {
    detailCache.delete(key);
    return undefined;
  }
  detailCache.delete(key);
  detailCache.set(key, entry);
  return entry.value;
}

async function cached(key, ttlMs, loader) {
  const value = cacheGet(key);
  if (value !== undefined) return value;
  if (inFlight.has(key)) return inFlight.get(key);
  const promise = Promise.resolve().then(loader).then((result) => {
    detailCache.set(key, { value: result, expiresAt: Date.now() + ttlMs });
    while (detailCache.size > config.cache.maxEntries) {
      detailCache.delete(detailCache.keys().next().value);
    }
    return result;
  }).finally(() => inFlight.delete(key));
  inFlight.set(key, promise);
  return promise;
}

function normalizeKlineRow(symbol, interval, row) {
  const closeTime = Number(row[6]);
  return {
    symbol, interval, openTime: Number(row[0]), open: Number(row[1]),
    high: Number(row[2]), low: Number(row[3]), close: Number(row[4]),
    volume: Number(row[5]), closeTime, quoteVolume: Number(row[7]),
    tradeCount: Number(row[8]), takerBuyBaseVolume: Number(row[9]),
    takerBuyQuoteVolume: Number(row[10]), isClosed: closeTime < Date.now()
  };
}

export async function getMarketKlines(symbolValue, intervalValue, limitValue = 500, endTimeValue = null) {
  const symbol = normalizeSymbol(symbolValue);
  const interval = String(intervalValue || "1m");
  const limit = Math.min(Math.max(Number.parseInt(limitValue, 10) || 500, 1), 1500);
  const endTime = endTimeValue === null ? null : Number(endTimeValue);
  if (!KLINE_INTERVALS.has(interval)) {
    const error = new Error("Unsupported kline interval");
    error.statusCode = 400;
    throw error;
  }
  if (endTime !== null && (!Number.isFinite(endTime) || endTime <= 0)) {
    const error = new Error("Invalid endTime");
    error.statusCode = 400;
    throw error;
  }

  const key = `klines:${symbol}:${interval}:${limit}:${endTime ?? "latest"}`;
  return cached(key, config.cache.klineTtlMs, async () => {
    const stored = getStoredKlines(symbol, interval, limit, endTime);
    const latestStored = stored.at(-1);
    const storedIsFresh = endTime !== null || (
      latestStored && latestStored.closeTime >= Date.now() - 2 * 60 * 60 * 1000
    );
    if (stored.length >= limit && storedIsFresh) return stored;

    const rows = await fetchBinanceKlines(symbol, interval, limit, endTime);
    const normalized = rows.map((row) => normalizeKlineRow(symbol, interval, row));
    const closed = normalized.filter((candle) => candle.isClosed);
    if (closed.length) saveKlines(closed);
    return normalized.map(({ symbol: _symbol, interval: _interval, ...candle }) => candle);
  });
}

// [ADDED] Backfill recent closed 15m candles for every active market after startup.
export async function backfillMarketKlines(symbolValues, interval = "15m", limit = 100) {
  const symbols = [...new Set((Array.isArray(symbolValues) ? symbolValues : [])
    .map((value) => String(value || "").toUpperCase())
    .filter((value) => /^[\p{L}\p{N}]{3,30}$/u.test(value)))];
  const summary = { interval, requestedSymbols: symbols.length, processedSymbols: 0, savedCandles: 0, failedSymbols: [] };

  // [ADDED] Run requests sequentially so the Binance REST queue and weight budget stay predictable.
  for (const symbol of symbols) {
    try {
      const rows = await fetchBinanceKlines(symbol, interval, limit);
      const closed = rows.map((row) => normalizeKlineRow(symbol, interval, row))
        .filter((candle) => candle.isClosed);
      if (closed.length) summary.savedCandles += saveKlines(closed);
      summary.processedSymbols += 1;
    } catch (error) {
      summary.failedSymbols.push({ symbol, error: error.message });
      console.warn(`15m candle backfill failed for ${symbol}: ${error.message}`);
    }
  }

  return summary;
}

export async function getMarketFundingRates(symbolValue, candles) {
  const symbol = normalizeSymbol(symbolValue);
  if (!Array.isArray(candles) || candles.length === 0) return [];

  const startTime = Math.max(0, Number(candles[0].openTime) - 8 * 60 * 60 * 1000);
  const endTime = Number(candles[candles.length - 1].closeTime);
  if (!Number.isFinite(startTime) || !Number.isFinite(endTime) || endTime < startTime) return [];

  const key = `funding:${symbol}:${startTime}:${endTime}`;
  return cached(key, 60000, async () => {
    const rows = [];
    let cursor = startTime;

    for (let page = 0; page < 12 && cursor <= endTime; page += 1) {
      const batch = await fetchBinanceFundingRates(symbol, cursor, endTime, 1000);
      if (!Array.isArray(batch) || batch.length === 0) break;
      rows.push(...batch);
      const lastFundingTime = Number(batch[batch.length - 1].fundingTime);
      if (!Number.isFinite(lastFundingTime) || batch.length < 1000 || lastFundingTime >= endTime) break;
      cursor = lastFundingTime + 1;
    }

    return rows.map((item) => ({
      symbol: item.symbol,
      fundingTime: Number(item.fundingTime),
      fundingRate: Number(item.fundingRate),
      markPrice: item.markPrice === undefined ? null : Number(item.markPrice)
    })).filter((item) => Number.isFinite(item.fundingTime) && Number.isFinite(item.fundingRate));
  });
}

export async function getMarketDepth(symbolValue, limitValue = 20) {
  const symbol = normalizeSymbol(symbolValue);
  const limit = [5, 10, 20, 50, 100].includes(Number(limitValue)) ? Number(limitValue) : 20;
  return cached(`depth:${symbol}:${limit}`, config.cache.depthTtlMs, async () => {
    const payload = await fetchOrderBook(symbol, limit);
    return {
      symbol, lastUpdateId: payload.lastUpdateId,
      bids: (payload.bids || []).map(([price, quantity]) => ({ price: Number(price), quantity: Number(quantity) })),
      asks: (payload.asks || []).map(([price, quantity]) => ({ price: Number(price), quantity: Number(quantity) })),
      timestamp: Date.now()
    };
  });
}

export async function getMarketTrades(symbolValue, limitValue = 50) {
  const symbol = normalizeSymbol(symbolValue);
  const limit = Math.min(Math.max(Number.parseInt(limitValue, 10) || 50, 1), 1000);
  return cached(`trades:${symbol}:${limit}`, config.cache.tradesTtlMs, async () => {
    const rows = await fetchRecentTrades(symbol, limit);
    return rows.map((trade) => ({
      id: trade.id, price: Number(trade.price), quantity: Number(trade.qty),
      quoteQuantity: Number(trade.quoteQty), time: trade.time,
      isBuyerMaker: Boolean(trade.isBuyerMaker)
    }));
  });
}

export async function getMarketMetrics(symbolValue, forceRefresh = false) {
  const symbol = normalizeSymbol(symbolValue);
  if (!forceRefresh) {
    const stored = getStoredMetrics(symbol);
    if (stored && Date.now() - stored.updatedAt < config.cache.metricsTtlMs) return stored;
  }
  if (forceRefresh) detailCache.delete(`metrics:${symbol}`);
  return cached(`metrics:${symbol}`, config.cache.metricsTtlMs, async () => {
    const [premium, interest] = await Promise.all([
      fetchPremiumIndex(symbol),
      fetchOpenInterest(symbol).catch(() => null)
    ]);
    const metrics = {
      symbol, markPrice: Number(premium.markPrice), indexPrice: Number(premium.indexPrice),
      estimatedSettlePrice: Number(premium.estimatedSettlePrice),
      fundingRate: Number(premium.lastFundingRate), nextFundingTime: Number(premium.nextFundingTime),
      interestRate: Number(premium.interestRate), premiumIndexTime: Number(premium.time),
      openInterest: interest ? Number(interest.openInterest) : null,
      openInterestUpdatedAt: interest ? Number(interest.time || Date.now()) : null
    };
    saveMarketMetrics(metrics);
    return { ...metrics, updatedAt: Date.now() };
  });
}

// [ADDED] Refresh and persist the latest premium-index fields for all active USDT-M perpetual markets.
export async function refreshAllMarketMetrics(symbolValues) {
  const allowedSymbols = new Set(
    (Array.isArray(symbolValues) ? symbolValues : [])
      .map((value) => String(value || "").toUpperCase())
      // [MODIFIED] Keep valid Unicode market symbols in the bulk metrics allowlist.
      .filter((value) => /^[\p{L}\p{N}]{3,30}$/u.test(value))
  );
  if (!allowedSymbols.size) return { saved: 0, updatedAt: Date.now() };

  const payload = await fetchAllPremiumIndex();
  if (!Array.isArray(payload)) {
    throw new Error("Binance premiumIndex bulk response is not an array");
  }

  const snapshots = payload
    .filter((item) => item && allowedSymbols.has(String(item.symbol || "").toUpperCase()))
    .map((item) => ({
      symbol: String(item.symbol).toUpperCase(),
      markPrice: Number(item.markPrice),
      indexPrice: Number(item.indexPrice),
      estimatedSettlePrice: Number(item.estimatedSettlePrice),
      fundingRate: Number(item.lastFundingRate),
      nextFundingTime: Number(item.nextFundingTime),
      interestRate: Number(item.interestRate),
      premiumIndexTime: Number(item.time)
    }))
    .filter((item) => (
      Number.isFinite(item.markPrice) &&
      Number.isFinite(item.indexPrice) &&
      Number.isFinite(item.fundingRate) &&
      Number.isFinite(item.interestRate) &&
      Number.isFinite(item.nextFundingTime) &&
      Number.isFinite(item.premiumIndexTime) // [MODIFIED] Validate the mapped premiumIndexTime field.
    ));

  return savePremiumIndexSnapshots(snapshots);
}

export function persistRealtimeKline(symbol, interval, kline) {
  const closeTime = Number(kline.T);
  const candle = {
    symbol, interval, openTime: Number(kline.t), open: Number(kline.o),
    high: Number(kline.h), low: Number(kline.l), close: Number(kline.c),
    volume: Number(kline.v), closeTime, quoteVolume: Number(kline.q),
    tradeCount: Number(kline.n), takerBuyBaseVolume: Number(kline.V),
    takerBuyQuoteVolume: Number(kline.Q), isClosed: Boolean(kline.x) && closeTime < Date.now()
  };
  if (candle.isClosed) saveKline(candle);
  return candle;
}
