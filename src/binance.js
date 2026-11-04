import { config } from "./config.js";

const requestQueue = [];
const inFlight = new Map();
const responseCache = new Map();
const weightEvents = [];
let activeRequests = 0;
let cooldownUntil = 0;
let usedWeightFromHeader = 0;
let headerWeightAt = 0;
let queueTimer = null;

const endpointWeights = {
  "/fapi/v1/exchangeInfo": 1,
  "/fapi/v1/ticker/24hr": 40,
  "/fapi/v1/klines": 1,
  "/fapi/v1/depth": 5,
  "/fapi/v1/trades": 5,
  "/fapi/v1/premiumIndex": 1,
  "/fapi/v1/openInterest": 1
};

function estimateWeight(path) {
  const pathname = path.split("?")[0];
  if (pathname === "/fapi/v1/depth") {
    const limit = Number(new URLSearchParams(path.split("?")[1] || "").get("limit") || 20);
    return limit <= 50 ? 2 : limit <= 100 ? 5 : 10;
  }
  return endpointWeights[pathname] || 1;
}

function pruneWeightEvents(now = Date.now()) {
  while (weightEvents.length && weightEvents[0].at <= now - 60000) weightEvents.shift();
}

function currentWeight(now = Date.now()) {
  pruneWeightEvents(now);
  const localWeight = weightEvents.reduce((total, item) => total + item.weight, 0);
  if (now - headerWeightAt > 60000) usedWeightFromHeader = 0;
  return Math.max(localWeight, usedWeightFromHeader);
}

function parseRetryAfter(response) {
  const value = response.headers.get("retry-after");
  if (!value) return config.rest.cooldownFallbackMs;
  const seconds = Number(value);
  if (Number.isFinite(seconds)) return Math.max(1000, seconds * 1000);
  const date = Date.parse(value);
  return Number.isFinite(date) ? Math.max(1000, date - Date.now()) : config.rest.cooldownFallbackMs;
}

function cacheGet(key) {
  const entry = responseCache.get(key);
  if (!entry) return undefined;
  if (entry.expiresAt <= Date.now()) {
    responseCache.delete(key);
    return undefined;
  }
  responseCache.delete(key);
  responseCache.set(key, entry);
  return entry.value;
}

function cacheSet(key, value, ttlMs) {
  if (ttlMs <= 0) return;
  responseCache.delete(key);
  responseCache.set(key, { value, expiresAt: Date.now() + ttlMs });
  while (responseCache.size > config.rest.cacheMaxEntries) {
    responseCache.delete(responseCache.keys().next().value);
  }
}

function scheduleQueue(delay = 0) {
  if (queueTimer) return;
  queueTimer = setTimeout(() => {
    queueTimer = null;
    drainQueue();
  }, Math.max(0, delay));
  queueTimer.unref?.();
}

function drainQueue() {
  if (!requestQueue.length) return;
  const now = Date.now();
  if (cooldownUntil > now) {
    scheduleQueue(cooldownUntil - now);
    return;
  }
  if (activeRequests >= config.rest.concurrency) return;
  if (currentWeight(now) >= config.rest.weightBudgetPerMinute) {
    pruneWeightEvents(now);
    const nextAvailable = weightEvents.length ? Math.max(100, weightEvents[0].at + 60000 - now) : 1000;
    scheduleQueue(nextAvailable);
    return;
  }

  while (requestQueue.length && activeRequests < config.rest.concurrency) {
    const task = requestQueue.shift();
    if (!task) break;
    activeRequests += 1;
    weightEvents.push({ at: Date.now(), weight: task.weight });
    executeTask(task).finally(() => {
      activeRequests -= 1;
      drainQueue();
    });
  }
}

async function executeTask(task) {
  try {
    const result = await performRequest(task.path, task.weight, 0);
    task.resolve(result);
  } catch (error) {
    task.reject(error);
  }
}

async function performRequest(path, weight, retryCount) {
  const response = await fetch(`${config.binanceRestBaseUrl}${path}`, {
    headers: {
      accept: "application/json",
      "user-agent": "BeananceMarketBackend/2.0"
    },
    signal: AbortSignal.timeout(config.rest.timeoutMs)
  });

  const usedWeight = Number(response.headers.get("x-mbx-used-weight-1m"));
  if (Number.isFinite(usedWeight)) {
    usedWeightFromHeader = usedWeight;
    headerWeightAt = Date.now();
  }

  if (response.status === 429 || response.status === 418) {
    const delay = parseRetryAfter(response);
    cooldownUntil = Math.max(cooldownUntil, Date.now() + delay);
    const body = await response.text();
    const error = new Error(`Binance REST rate limited (${response.status}); cooldown ${delay}ms: ${body.slice(0, 200)}`);
    error.statusCode = response.status;
    error.retryAfterMs = delay;
    throw error;
  }

  if (response.status >= 500 && retryCount < config.rest.retry5xx) {
    await new Promise((resolve) => setTimeout(resolve, 500 * (retryCount + 1)));
    return performRequest(path, weight, retryCount + 1);
  }

  if (!response.ok) {
    const body = await response.text();
    const error = new Error(`Binance REST request failed (${response.status}): ${body.slice(0, 300)}`);
    error.statusCode = response.status;
    throw error;
  }

  return response.json();
}

function requestBinance(path, { ttlMs = 0, weight = estimateWeight(path) } = {}) {
  const cached = cacheGet(path);
  if (cached !== undefined) return Promise.resolve(cached);
  if (inFlight.has(path)) return inFlight.get(path);
  if (requestQueue.length >= config.rest.maxQueue) {
    return Promise.reject(new Error("Binance REST request queue is full"));
  }

  const promise = new Promise((resolve, reject) => {
    requestQueue.push({
      path,
      weight,
      resolve: (value) => {
        cacheSet(path, value, ttlMs);
        resolve(value);
      },
      reject
    });
    scheduleQueue();
  }).finally(() => inFlight.delete(path));

  inFlight.set(path, promise);
  return promise;
}

export async function fetchExchangeInfo() {
  const payload = await requestBinance("/fapi/v1/exchangeInfo", { ttlMs: 21600000 });
  if (!Array.isArray(payload.symbols)) throw new Error("Unexpected Binance exchangeInfo response");
  return payload.symbols
    .filter((symbol) => symbol.status === "TRADING" && symbol.contractType === "PERPETUAL" && symbol.quoteAsset === "USDT" && symbol.marginAsset === "USDT")
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
  const payload = await requestBinance("/fapi/v1/ticker/24hr", { ttlMs: 2000, weight: 40 });
  if (!Array.isArray(payload)) throw new Error("Unexpected Binance 24hr ticker response");
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
  const path = withQuery("/fapi/v1/klines", { symbol, interval, limit, endTime });
  return requestBinance(path, { ttlMs: 15000, weight: 2 });
}

export async function fetchFundingRates(symbol, startTime, endTime, limit = 1000) {
  const path = withQuery("/fapi/v1/fundingRate", {
    symbol,
    startTime,
    endTime,
    limit: Math.min(Math.max(Number.parseInt(limit, 10) || 1000, 1), 1000)
  });
  return requestBinance(path, { ttlMs: 60000, weight: 5 });
}

export async function fetchOrderBook(symbol, limit = 20) {
  const path = withQuery("/fapi/v1/depth", { symbol, limit });
  return requestBinance(path, { ttlMs: 500, weight: estimateWeight(path) });
}

export async function fetchRecentTrades(symbol, limit = 50) {
  const path = withQuery("/fapi/v1/trades", { symbol, limit });
  return requestBinance(path, { ttlMs: 1000, weight: 5 });
}

export async function fetchPremiumIndex(symbol) {
  const path = withQuery("/fapi/v1/premiumIndex", { symbol });
  return requestBinance(path, { ttlMs: 10000, weight: 1 });
}

export async function fetchOpenInterest(symbol) {
  const path = withQuery("/fapi/v1/openInterest", { symbol });
  return requestBinance(path, { ttlMs: 10000, weight: 1 });
}

export function getBinanceRestStatus() {
  pruneWeightEvents();
  return {
    queuedRequests: requestQueue.length,
    activeRequests,
    usedWeightEstimate: currentWeight(),
    cooldownUntil: cooldownUntil || null,
    cacheEntries: responseCache.size
  };
}
