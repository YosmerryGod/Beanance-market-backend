import { config } from "./config.js";

async function requestBinance(path) {
  const url = `${config.binanceRestBaseUrl}${path}`;
  const response = await fetch(url, {
    headers: {
      accept: "application/json",
      "user-agent": "BeananceMarketBackend/1.0"
    },
    signal: AbortSignal.timeout(15000)
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(
      `Binance REST request failed (${response.status}): ${body.slice(0, 300)}`
    );
  }

  return response.json();
}

export async function fetchExchangeInfo() {
  const payload = await requestBinance("/fapi/v1/exchangeInfo");

  if (!Array.isArray(payload.symbols)) {
    throw new Error("Unexpected Binance exchangeInfo response");
  }

  return payload.symbols
    .filter(
      (symbol) =>
        symbol.status === "TRADING" &&
        symbol.contractType === "PERPETUAL" &&
        symbol.quoteAsset === "USDT" &&
        symbol.marginAsset === "USDT"
    )
    .map((symbol) => ({
      symbol: symbol.symbol,
      pair: symbol.pair,
      baseAsset: symbol.baseAsset,
      quoteAsset: symbol.quoteAsset,
      marginAsset: symbol.marginAsset,
      contractType: symbol.contractType,
      status: symbol.status,
      pricePrecision: symbol.pricePrecision,
      quantityPrecision: symbol.quantityPrecision,
      onboardDate: symbol.onboardDate,
      deliveryDate: symbol.deliveryDate
    }))
    .sort((a, b) => a.symbol.localeCompare(b.symbol));
}

export async function fetchAll24hrTickers() {
  const payload = await requestBinance("/fapi/v1/ticker/24hr");

  if (!Array.isArray(payload)) {
    throw new Error("Unexpected Binance 24hr ticker response");
  }

  return payload;
}

export function createBinanceWebSocketUrl() {
  const baseUrl = config.binanceWsBaseUrl.replace(/\/$/, "");
  return `${baseUrl}?streams=!ticker@arr`;
}

function withQuery(path, params = {}) {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null) query.set(key, String(value));
  }
  const suffix = query.toString();
  return suffix ? `${path}?${suffix}` : path;
}

export async function fetchKlines(symbol, interval, limit = 500, endTime = null) {
  return requestBinance(withQuery("/fapi/v1/klines", {
    symbol,
    interval,
    limit,
    endTime
  }));
}

export async function fetchOrderBook(symbol, limit = 20) {
  return requestBinance(withQuery("/fapi/v1/depth", { symbol, limit }));
}

export async function fetchRecentTrades(symbol, limit = 50) {
  return requestBinance(withQuery("/fapi/v1/trades", { symbol, limit }));
}

export async function fetchPremiumIndex(symbol) {
  return requestBinance(withQuery("/fapi/v1/premiumIndex", { symbol }));
}

export async function fetchOpenInterest(symbol) {
  return requestBinance(withQuery("/fapi/v1/openInterest", { symbol }));
}
