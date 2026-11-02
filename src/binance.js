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
