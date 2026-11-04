import { getMarket, getMarkets } from "./market-store.js";
import { config } from "./config.js";
import { ManagedWebSocket } from "./websocket-manager.js";
import { saveTopMoverHistory } from "./database.js";

const MAX_RESULTS = 1000;
const MIN_MOVE_PERCENT = 0.25;
const MEDIUM_MOVE_PERCENT = 1.5;
const HIGH_MOVE_PERCENT = 3;
const HISTORY_CANDLES = 20;
const VOLUME_SPIKE_MULTIPLIER = 2;
const TIMEFRAMES = [
  { interval: "1m", label: "1 mnt" },
  { interval: "5m", label: "5 mnt" },
  { interval: "15m", label: "15 mnt" },
  { interval: "1h", label: "1 jam" },
  { interval: "4h", label: "4 jam" },
  { interval: "1d", label: "1 hari" },
  { interval: "1w", label: "1 minggu" }
];

export const TOP_MOVER_CATEGORIES = [
  { id: "all", label: "Semua" },
  { id: "kenaikan", label: "Kenaikan" },
  { id: "penurunan", label: "Penurunan" },
  { id: "pullback", label: "Pullback" },
  { id: "rally", label: "Rally" },
  { id: "high_baru", label: "High Baru" },
  { id: "low_baru", label: "Low Baru" },
  { id: "harga_naik_volume_tinggi", label: "Harga Naik dengan Volume Tinggi" },
  { id: "harga_turun_volume_tinggi", label: "Harga Turun dengan Volume Tinggi" }
];

const eventsByKey = new Map();
const sockets = new Map();
const candleHistory = new Map();
const messageCounts = new Map(TIMEFRAMES.map(({ interval }) => [interval, 0]));
const symbolCounts = new Map(TIMEFRAMES.map(({ interval }) => [interval, new Set()]));
// [ADDED] Track Binance SUBSCRIBE acknowledgements separately from open sockets.
const subscriptionAcks = new Map(TIMEFRAMES.map(({ interval }) => [interval, false]));
const subscriptionErrors = new Map();
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

function getHistoryKey(symbol, interval) {
  return `${symbol}:${interval}`;
}

function getClosedHistory(symbol, interval) {
  return candleHistory.get(getHistoryKey(symbol, interval)) || [];
}

function getTrend(history) {
  if (history.length < 3) return null;
  const recent = history.slice(-3);
  const rising = recent.every((item, index) => index === 0 || item.close > recent[index - 1].close);
  const falling = recent.every((item, index) => index === 0 || item.close < recent[index - 1].close);
  if (rising) return "up";
  if (falling) return "down";
  return null;
}

function detectCategories({ changePercent, candle, history, volumeAverage }) {
  const categories = [];
  const positive = changePercent >= 0;
  const magnitude = Math.abs(changePercent);

  if (magnitude >= MIN_MOVE_PERCENT) {
    categories.push(positive ? "kenaikan" : "penurunan");
  }

  const trend = getTrend(history);
  if (!positive && trend === "up" && magnitude >= MIN_MOVE_PERCENT) categories.push("pullback");
  if (positive && trend === "up" && magnitude >= MIN_MOVE_PERCENT) categories.push("rally");

  const previousHigh = history.length
    ? Math.max(...history.slice(-HISTORY_CANDLES).map((item) => item.high))
    : 0;
  const previousLow = history.length
    ? Math.min(...history.slice(-HISTORY_CANDLES).map((item) => item.low))
    : 0;

  if (previousHigh > 0 && Number(candle.h) > previousHigh) categories.push("high_baru");
  if (previousLow > 0 && Number(candle.l) < previousLow) categories.push("low_baru");

  const currentQuoteVolume = Number(candle.q || 0);
  const volumeSpike = volumeAverage > 0 && currentQuoteVolume >= volumeAverage * VOLUME_SPIKE_MULTIPLIER;
  if (volumeSpike && positive) categories.push("harga_naik_volume_tinggi");
  if (volumeSpike && !positive) categories.push("harga_turun_volume_tinggi");

  return [...new Set(categories)];
}

function handleKlineMessage(payload, timeframe) {
  const data = payload?.data ?? payload;
  const candle = data?.k;
  if (!candle || !candle.s || candle.o === undefined || candle.c === undefined) return;

  const symbol = String(candle.s).toUpperCase();
  const openPrice = Number(candle.o);
  const lastPrice = Number(candle.c);
  if (!(openPrice > 0) || !(lastPrice > 0)) return;

  const changePercent = ((lastPrice - openPrice) / openPrice) * 100;
  const positive = changePercent >= 0;
  const direction = positive ? "Kenaikan" : "Penurunan";
  const severity = classifyMove(changePercent);
  const key = getHistoryKey(symbol, timeframe.interval);
  const eventTime = Number(data.E || candle.T || Date.now());
  const history = getClosedHistory(symbol, timeframe.interval);
  const volumeAverage = history.length
    ? history.slice(-HISTORY_CANDLES).reduce((sum, item) => sum + item.quoteVolume, 0) / Math.min(history.length, HISTORY_CANDLES)
    : 0;
  const categories = detectCategories({ changePercent, candle, history, volumeAverage });

  lastMessageAt = eventTime;
  messageCounts.set(timeframe.interval, (messageCounts.get(timeframe.interval) || 0) + 1);
  symbolCounts.get(timeframe.interval)?.add(symbol);

  const market = getMarket(symbol);
  if (!market) return;

  if (categories.length === 0) {
    eventsByKey.delete(key);
  } else {
    const categoryLabels = TOP_MOVER_CATEGORIES
      .filter((item) => categories.includes(item.id))
      .map((item) => item.label);
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
      candleQuoteVolume: Number(candle.q || 0),
      volumeAverage,
      volumeSpike: volumeAverage > 0 && Number(candle.q || 0) >= volumeAverage * VOLUME_SPIKE_MULTIPLIER,
      volumeRatio: volumeAverage > 0 ? Number((Number(candle.q || 0) / volumeAverage).toFixed(2)) : null,
      highPrice: Number(candle.h || market.highPrice || 0),
      lowPrice: Number(candle.l || market.lowPrice || 0),
      positive,
      categories,
      categoryLabels,
      category: categoryLabels[0] || null,
      status: `[${severity || "Terpantau"}] ${categoryLabels.join(", ")} ${timeframe.label}`,
      timeframe: timeframe.interval,
      severity: severity || "Kecil",
      eventTime,
      candleOpenTime: Number(candle.t),
      candleCloseTime: Number(candle.T),
      candleClosed: candle.x === true
    });
  }

  if (candle.x === true) {
    const closedCandle = {
      openTime: Number(candle.t),
      closeTime: Number(candle.T),
      open: openPrice,
      close: lastPrice,
      high: Number(candle.h),
      low: Number(candle.l),
      quoteVolume: Number(candle.q || 0)
    };
    const nextHistory = [...history.filter((item) => item.openTime !== closedCandle.openTime), closedCandle]
      .sort((a, b) => a.openTime - b.openTime)
      .slice(-HISTORY_CANDLES);
    candleHistory.set(key, nextHistory);

    if (categories.length > 0) {
      saveTopMoverHistory({
        symbol,
        baseAsset: market.baseAsset,
        quoteAsset: market.quoteAsset,
        timeframe: timeframe.interval,
        direction,
        severity: severity || "Kecil",
        categories,
        changePercent,
        lastPrice,
        quoteVolume: Number(market.quoteVolume || 0),
        baseVolume: Number(market.baseVolume || 0),
        highPrice: Number(candle.h || 0),
        lowPrice: Number(candle.l || 0),
        candleOpenTime: Number(candle.t),
        candleCloseTime: Number(candle.T),
        eventTime
      });
    }
  }
}

function createTimeframeSocket(timeframe, symbols) {
  const streams = symbols.map((symbol) => `${symbol.toLowerCase()}@kline_${timeframe.interval}`);
  const subscriptionId = TIMEFRAMES.findIndex((item) => item.interval === timeframe.interval) + 1;
  const manager = new ManagedWebSocket({
    name: `Binance Top Movers ${timeframe.interval}`,
    url: buildStreamUrl(),
    onOpen: (socket) => {
      socket.send(JSON.stringify({
        method: "SUBSCRIBE",
        params: streams,
        id: subscriptionId
      }));
      connectedStreams += 1;
      subscriptionAcks.set(timeframe.interval, false);
      subscriptionErrors.delete(timeframe.interval);
      console.info(`Top Movers subscription requested for ${streams.length} ${timeframe.interval} kline streams`);
    },
    onMessage: (payload) => {
      if (payload?.id === subscriptionId) {
        if (payload.error) {
          subscriptionAcks.set(timeframe.interval, false);
          subscriptionErrors.set(timeframe.interval, payload.error);
          lastError = `${timeframe.interval}: ${payload.error.msg || JSON.stringify(payload.error)}`;
          console.error(`Top Movers ${timeframe.interval} subscription rejected:`, payload.error);
        } else {
          subscriptionAcks.set(timeframe.interval, true);
          subscriptionErrors.delete(timeframe.interval);
          console.info(`Top Movers ${timeframe.interval} subscription acknowledged for ${streams.length} streams`);
        }
        return;
      }
      handleKlineMessage(payload, timeframe);
    },
    onClose: () => {
      connectedStreams = Math.max(0, connectedStreams - 1);
      subscriptionAcks.set(timeframe.interval, false);
    },
    onError: (error) => {
      lastError = error?.message || String(error);
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

  for (const timeframe of TIMEFRAMES) createTimeframeSocket(timeframe, symbols);
  console.info(`Top Movers scanner started for ${trackedSymbols} symbols across ${TIMEFRAMES.length} timeframes`);
}

export function stopTopMovers() {
  for (const manager of sockets.values()) manager.stop();
  sockets.clear();
  eventsByKey.clear();
  candleHistory.clear();
  for (const timeframe of TIMEFRAMES) {
    messageCounts.set(timeframe.interval, 0);
    symbolCounts.get(timeframe.interval)?.clear();
    subscriptionAcks.set(timeframe.interval, false);
    subscriptionErrors.delete(timeframe.interval);
  }
  connectedStreams = 0;
  trackedSymbols = 0;
  started = false;
}

export function getTopMovers(limit = MAX_RESULTS, { category = "all", timeframe = null } = {}) {
  const safeLimit = Math.min(Math.max(Number(limit) || MAX_RESULTS, 1), MAX_RESULTS);
  return [...eventsByKey.values()]
    .filter((event) => !timeframe || event.timeframe === timeframe)
    .filter((event) => category === "all" || event.categories.includes(category))
    .sort((a, b) => b.eventTime - a.eventTime)
    .slice(0, safeLimit);
}

export function getTopMoverCategoryGroups(events = getTopMovers(MAX_RESULTS)) {
  return TOP_MOVER_CATEGORIES.map((category) => ({
    ...category,
    count: category.id === "all"
      ? events.length
      : events.filter((event) => event.categories.includes(category.id)).length,
    events: category.id === "all"
      ? events
      : events.filter((event) => event.categories.includes(category.id))
  }));
}

export function getTopMoversStatus() {
  return {
    trackedSymbols,
    timeframes: TIMEFRAMES.map((item) => item.interval),
    connectedStreams,
    connected: connectedStreams > 0,
    acknowledgedTimeframes: [...subscriptionAcks].filter(([, acknowledged]) => acknowledged).map(([interval]) => interval),
    subscriptionAcks: Object.fromEntries(subscriptionAcks),
    subscriptionErrors: Object.fromEntries(subscriptionErrors),
    eventCount: eventsByKey.size,
    lastMessageAt,
    lastError,
    messagesByTimeframe: Object.fromEntries(messageCounts),
    activeSymbolsByTimeframe: Object.fromEntries(
      [...symbolCounts].map(([interval, symbols]) => [interval, symbols.size])
    ),
    categories: TOP_MOVER_CATEGORIES
  };
}
