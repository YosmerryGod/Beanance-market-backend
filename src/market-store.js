import { saveMarkets, saveTickers } from "./database.js";

const marketBySymbol = new Map();
let symbolsBySymbol = new Map();
let lastSnapshotAt = null;
let lastUpdateAt = null;
let lastSource = "initializing";

function toNumber(value) {
  if (value === undefined || value === null || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function normalizeTicker(ticker) {
  return {
    symbol: ticker.s,
    priceChange: toNumber(ticker.p),
    priceChangePercent: toNumber(ticker.P),
    weightedAvgPrice: toNumber(ticker.w),
    lastPrice: toNumber(ticker.c),
    lastQuantity: toNumber(ticker.Q),
    openPrice: toNumber(ticker.o),
    highPrice: toNumber(ticker.h),
    lowPrice: toNumber(ticker.l),
    baseVolume: toNumber(ticker.v),
    quoteVolume: toNumber(ticker.q),
    openTime: toNumber(ticker.O),
    closeTime: toNumber(ticker.C),
    tradeCount: toNumber(ticker.n),
    eventTime: toNumber(ticker.E)
  };
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

export function replaceSymbols(symbols) {
  symbolsBySymbol = new Map(symbols.map((symbol) => [symbol.symbol, symbol]));
  saveMarkets(symbols);

  for (const [symbol, current] of marketBySymbol.entries()) {
    const symbolInfo = symbolsBySymbol.get(symbol);

    if (!symbolInfo) {
      marketBySymbol.delete(symbol);
      continue;
    }

    marketBySymbol.set(
      symbol,
      mergeMarketItem(symbolInfo, current)
    );
  }

  lastUpdateAt = Date.now();
}

export function applyTicker(ticker) {
  if (!ticker || typeof ticker.s !== "string") return null;

  const symbolInfo = symbolsBySymbol.get(ticker.s);
  if (!symbolInfo) return null;

  const normalized = normalizeTicker(ticker);
  const marketItem = mergeMarketItem(symbolInfo, normalized);

  marketBySymbol.set(ticker.s, marketItem);
  lastUpdateAt = Date.now();
  lastSource = "websocket";

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
  const markets = Array.from(marketBySymbol.values());
  const direction = order === "desc" ? -1 : 1;

  markets.sort((a, b) => {
    if (sort === "quoteVolume" || sort === "priceChangePercent") {
      return ((a[sort] ?? 0) - (b[sort] ?? 0)) * direction;
    }

    return String(a.symbol).localeCompare(String(b.symbol)) * direction;
  });

  return markets;
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
