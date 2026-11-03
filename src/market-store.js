import {
  getStoredMarketSnapshot,
  saveMarkets,
  saveTickers
} from "./database.js";

const marketBySymbol = new Map();
let symbolsBySymbol = new Map();
let lastSnapshotAt = null;
let lastUpdateAt = null;
let lastSource = "initializing";
const sortedCache = new Map();
const SORT_CACHE_TTL_MS = 500;

function toNumber(value) {
  if (value === undefined || value === null || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function normalizeTicker(ticker) {
  const isRest = typeof ticker.symbol === "string";
  return {
    symbol: isRest ? ticker.symbol : ticker.s,
    priceChange: toNumber(isRest ? ticker.priceChange : ticker.p),
    priceChangePercent: toNumber(isRest ? ticker.priceChangePercent : ticker.P),
    weightedAvgPrice: toNumber(isRest ? ticker.weightedAvgPrice : ticker.w),
    lastPrice: toNumber(isRest ? ticker.lastPrice : ticker.c),
    lastQuantity: toNumber(isRest ? ticker.lastQty : ticker.Q),
    openPrice: toNumber(isRest ? ticker.openPrice : ticker.o),
    highPrice: toNumber(isRest ? ticker.highPrice : ticker.h),
    lowPrice: toNumber(isRest ? ticker.lowPrice : ticker.l),
    baseVolume: toNumber(isRest ? ticker.volume : ticker.v),
    quoteVolume: toNumber(isRest ? ticker.quoteVolume : ticker.q),
    openTime: toNumber(isRest ? ticker.openTime : ticker.O),
    closeTime: toNumber(isRest ? ticker.closeTime : ticker.C),
    tradeCount: toNumber(isRest ? ticker.count : ticker.n),
    eventTime: toNumber(isRest ? ticker.closeTime : ticker.E)
  };
}

function invalidateSortedCache() {
  sortedCache.clear();
}

function mergeMarketItem(symbolInfo, tickerInfo = {}) {
  return {
    ...symbolInfo,
    ...tickerInfo,
    symbol: symbolInfo.symbol,
    marketType: "USDT-M",
    isTrading: symbolInfo.status === "TRADING"
  };
}

export function hydrateMarketStore() {
  const rows = getStoredMarketSnapshot();
  symbolsBySymbol = new Map();
  marketBySymbol.clear();

  for (const row of rows) {
    const symbolInfo = {
      symbol: row.symbol,
      pair: row.pair,
      baseAsset: row.baseAsset,
      quoteAsset: row.quoteAsset,
      marginAsset: row.marginAsset,
      contractType: row.contractType,
      status: row.status,
      pricePrecision: row.pricePrecision,
      quantityPrecision: row.quantityPrecision,
      onboardDate: row.onboardDate,
      deliveryDate: row.deliveryDate
    };
    const tickerInfo = {
      symbol: row.symbol,
      priceChange: row.priceChange,
      priceChangePercent: row.priceChangePercent,
      weightedAvgPrice: row.weightedAvgPrice,
      lastPrice: row.lastPrice,
      lastQuantity: row.lastQuantity,
      openPrice: row.openPrice,
      highPrice: row.highPrice,
      lowPrice: row.lowPrice,
      baseVolume: row.baseVolume,
      quoteVolume: row.quoteVolume,
      openTime: row.openTime,
      closeTime: row.closeTime,
      tradeCount: row.tradeCount,
      eventTime: row.eventTime
    };
    symbolsBySymbol.set(row.symbol, symbolInfo);
    marketBySymbol.set(row.symbol, mergeMarketItem(symbolInfo, tickerInfo));
  }

  if (rows.length) {
    lastUpdateAt = Date.now();
    lastSource = "sqlite";
  }
  invalidateSortedCache();
  return rows.length;
}

export function replaceSymbols(symbols) {
  symbolsBySymbol = new Map(symbols.map((symbol) => [symbol.symbol, symbol]));
  saveMarkets(symbols);

  for (const [symbol, current] of marketBySymbol.entries()) {
    const symbolInfo = symbolsBySymbol.get(symbol);
    if (!symbolInfo) {
      marketBySymbol.delete(symbol);
      continue;
    }
    marketBySymbol.set(symbol, mergeMarketItem(symbolInfo, current));
  }
  lastUpdateAt = Date.now();
  invalidateSortedCache();
}

export function applyTicker(ticker) {
  if (!ticker || (typeof ticker.s !== "string" && typeof ticker.symbol !== "string")) return null;
  const normalized = normalizeTicker(ticker);
  const symbolInfo = symbolsBySymbol.get(normalized.symbol);
  if (!symbolInfo) return null;

  const marketItem = mergeMarketItem(symbolInfo, normalized);
  marketBySymbol.set(normalized.symbol, marketItem);
  lastUpdateAt = Date.now();
  lastSource = ticker.symbol ? "rest" : "websocket";
  invalidateSortedCache();
  return marketItem;
}

export function applyTickerSnapshot(tickers) {
  const updated = [];
  for (const ticker of tickers) {
    const item = applyTicker(ticker);
    if (item) updated.push(item);
  }
  saveTickers(updated);
  lastSnapshotAt = Date.now();
  lastSource = "rest";
  return updated;
}

export function getMarkets({ sort = "symbol", order = "asc" } = {}) {
  const key = `${sort}:${order}`;
  const cached = sortedCache.get(key);
  if (cached && cached.expiresAt > Date.now()) return cached.value.slice();

  const markets = Array.from(marketBySymbol.values());
  const direction = order === "desc" ? -1 : 1;
  markets.sort((a, b) => {
    if (sort === "quoteVolume" || sort === "priceChangePercent") {
      return ((a[sort] ?? 0) - (b[sort] ?? 0)) * direction;
    }
    return String(a.symbol).localeCompare(String(b.symbol)) * direction;
  });

  sortedCache.set(key, { value: markets, expiresAt: Date.now() + SORT_CACHE_TTL_MS });
  while (sortedCache.size > 6) sortedCache.delete(sortedCache.keys().next().value);
  return markets.slice();
}

export function getMarket(symbol) {
  return marketBySymbol.get(String(symbol).toUpperCase()) || null;
}

export function getMarketStatus() {
  return {
    marketCount: symbolsBySymbol.size,
    tickerCount: marketBySymbol.size,
    lastSnapshotAt,
    lastUpdateAt,
    lastSource
  };
}
