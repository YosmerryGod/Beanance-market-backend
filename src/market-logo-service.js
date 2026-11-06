import { saveMarketLogo } from "./database.js";
import { getMarkets, applyMarketLogo } from "./market-store.js";

const COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3";
const TRUST_WALLET_BASE_URL = "https://raw.githubusercontent.com/trustwallet/assets/master/blockchains";
const REFRESH_INTERVAL_MS = 24 * 60 * 60 * 1000;
const PAGE_SIZE = 250;
const PAGE_COUNT = 4;

let refreshInProgress = false;
let lastRefreshAt = null;

function normalizeSymbol(value) {
  return String(value || "").trim().toUpperCase();
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJson(url) {
  const headers = { accept: "application/json" };
  if (process.env.COINGECKO_API_KEY) {
    headers["x-cg-demo-api-key"] = process.env.COINGECKO_API_KEY;
  }
  const response = await fetch(url, {
    headers,
    signal: AbortSignal.timeout(15000)
  });
  if (!response.ok) {
    throw new Error(`CoinGecko request failed with HTTP ${response.status}`);
  }
  return response.json();
}

async function fetchCoinGeckoMarkets() {
  const coins = [];
  for (let page = 1; page <= PAGE_COUNT; page += 1) {
    const params = new URLSearchParams({
      vs_currency: "usd",
      order: "market_cap_desc",
      per_page: String(PAGE_SIZE),
      page: String(page),
      sparkline: "false"
    });
    const result = await fetchJson(`${COINGECKO_BASE_URL}/coins/markets?${params}`);
    if (!Array.isArray(result) || result.length === 0) break;
    coins.push(...result);
    if (page < PAGE_COUNT) await delay(2500);
  }
  return coins;
}

async function fetchCoinList() {
  const params = new URLSearchParams({ include_platform: "false" });
  const result = await fetchJson(`${COINGECKO_BASE_URL}/coins/list?${params}`);
  return Array.isArray(result) ? result : [];
}

function buildUniqueCoinMap(coins) {
  const grouped = new Map();
  for (const coin of coins) {
    const symbol = normalizeSymbol(coin.symbol);
    if (!symbol || !coin.id) continue;
    const matches = grouped.get(symbol) || [];
    matches.push(coin);
    grouped.set(symbol, matches);
  }

  const unique = new Map();
  for (const [symbol, matches] of grouped) {
    // [ADDED] Avoid assigning a potentially unrelated logo when CoinGecko has duplicate tickers.
    if (matches.length === 1) unique.set(symbol, matches[0]);
  }
  return unique;
}

async function trustWalletNativeLogo(coinId) {
  if (!coinId || !/^[a-z0-9-]+$/.test(coinId)) return null;
  const url = `${TRUST_WALLET_BASE_URL}/${coinId}/info/logo.png`;
  try {
    const response = await fetch(url, {
      method: "HEAD",
      signal: AbortSignal.timeout(8000)
    });
    return response.ok ? url : null;
  } catch {
    return null;
  }
}

export async function refreshMarketLogos({ force = false } = {}) {
  if (refreshInProgress) return { skipped: true, reason: "refresh_in_progress" };
  if (!force && lastRefreshAt && Date.now() - lastRefreshAt < REFRESH_INTERVAL_MS) {
    return { skipped: true, reason: "already_refreshed" };
  }

  refreshInProgress = true;
  try {
    const markets = getMarkets();
    if (!markets.length) return { saved: 0, total: 0, reason: "markets_not_ready" };

    const coins = await fetchCoinGeckoMarkets();
    const coinBySymbol = buildUniqueCoinMap(coins);
    // [ADDED] The full CoinGecko coin list lets Trust Wallet fallback cover assets outside the top 1,000.
    const coinListBySymbol = buildUniqueCoinMap(await fetchCoinList());
    let saved = 0;
    let trustWalletFallbacks = 0;

    for (const market of markets) {
      const symbol = normalizeSymbol(market.baseAsset);
      const coin = coinBySymbol.get(symbol);
      const listedCoin = coin || coinListBySymbol.get(symbol);
      if (!listedCoin) continue;

      let logo = typeof coin?.image === "string" && coin.image.startsWith("https://")
        ? coin.image
        : null;
      let source = logo ? "coingecko" : null;

      // [ADDED] Trust Wallet native-asset logo is checked only when CoinGecko has no usable image.
      if (!logo) {
        logo = await trustWalletNativeLogo(listedCoin.id);
        if (logo) {
          source = "trustwallet";
          trustWalletFallbacks += 1;
        }
      }

      if (!logo) continue;
      const logoData = { logo, source, coingeckoId: listedCoin.id };
      saveMarketLogo(market.symbol, logoData);
      applyMarketLogo(market.symbol, logoData);
      saved += 1;
    }

    lastRefreshAt = Date.now();
    console.info(`Resolved logos for ${saved}/${markets.length} markets (Trust Wallet fallback: ${trustWalletFallbacks})`);
    return {
      saved,
      total: markets.length,
      coingeckoMatches: saved - trustWalletFallbacks,
      trustWalletFallbacks,
      updatedAt: lastRefreshAt
    };
  } finally {
    refreshInProgress = false;
  }
}
