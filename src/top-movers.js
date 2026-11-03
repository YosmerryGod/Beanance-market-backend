import { getMarkets } from "./market-store.js";

const MAX_RESULTS = 1000;
const HIGH_LOW_DISTANCE_PERCENT = 0.15;
const HIGH_VOLUME_RATIO = 1.5;

function median(values) {
  const sorted = values.filter((value) => Number.isFinite(value) && value > 0).sort((a, b) => a - b);
  if (!sorted.length) return 0;
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
}

// [MODIFIED] Build the complete Binance-style Top Movers list from all active 24h Futures tickers.
export function getTopMovers(limit = MAX_RESULTS) {
  const markets = getMarkets();
  const typicalQuoteVolume = median(markets.map((market) => Number(market.quoteVolume || 0)));

  const events = markets
    .filter((market) => market.isTrading && Number(market.lastPrice) > 0)
    .map((market) => {
      const lastPrice = Number(market.lastPrice || 0);
      const highPrice = Number(market.highPrice || 0);
      const lowPrice = Number(market.lowPrice || 0);
      const changePercent = Number(market.priceChangePercent || 0);
      const quoteVolume = Number(market.quoteVolume || 0);
      const highDistancePercent = highPrice > 0 ? ((highPrice - lastPrice) / highPrice) * 100 : null;
      const lowDistancePercent = lowPrice > 0 ? ((lastPrice - lowPrice) / lowPrice) * 100 : null;
      const volumeRatio = typicalQuoteVolume > 0 ? quoteVolume / typicalQuoteVolume : null;
      const positive = changePercent >= 0;
      const categories = [];

      if (positive) categories.push("Kenaikan");
      else categories.push("Penurunan");

      if (highDistancePercent !== null && highDistancePercent <= HIGH_LOW_DISTANCE_PERCENT) {
        categories.push("High Baru");
      }
      if (lowDistancePercent !== null && lowDistancePercent <= HIGH_LOW_DISTANCE_PERCENT) {
        categories.push("Low Baru");
      }

      const nearHigh = highDistancePercent !== null && highDistancePercent <= 2;
      const awayFromHigh = highDistancePercent !== null && highDistancePercent > 1;
      if (positive && awayFromHigh && !nearHigh) categories.push("Pullback");

      if (positive && changePercent >= 3) categories.push("Rally");

      const highVolume = volumeRatio !== null && volumeRatio >= HIGH_VOLUME_RATIO;
      if (highVolume && positive) categories.push("Harga Naik dengan Vol. Tinggi");
      if (highVolume && !positive) categories.push("Harga Turun dengan Vol. Tinggi");

      let status = positive ? "Kenaikan" : "Penurunan";
      if (categories.includes("High Baru")) status = "High Baru";
      else if (categories.includes("Low Baru")) status = "Low Baru";
      else if (categories.includes("Harga Naik dengan Vol. Tinggi")) status = "Harga Naik dengan Vol. Tinggi";
      else if (categories.includes("Harga Turun dengan Vol. Tinggi")) status = "Harga Turun dengan Vol. Tinggi";
      else if (categories.includes("Rally")) status = "Rally";
      else if (categories.includes("Pullback")) status = "Pullback";

      return {
        symbol: market.symbol,
        baseAsset: market.baseAsset,
        quoteAsset: market.quoteAsset,
        contractType: market.contractType,
        lastPrice,
        priceChangePercent: changePercent,
        changePercent,
        quoteVolume,
        baseVolume: Number(market.baseVolume || 0),
        highPrice,
        lowPrice,
        highDistancePercent,
        lowDistancePercent,
        volumeRatio,
        positive,
        categories,
        category: positive ? "Kenaikan" : "Penurunan",
        status,
        eventTime: Number(market.eventTime || market.closeTime || Date.now())
      };
    })
    .sort((a, b) => Math.abs(b.changePercent) - Math.abs(a.changePercent));

  const safeLimit = Math.min(Math.max(Number(limit) || MAX_RESULTS, 1), MAX_RESULTS);
  return events.slice(0, safeLimit);
}

// [MODIFIED] Report ticker coverage rather than the old one-minute candle subscription.
export function getTopMoversStatus() {
  const markets = getMarkets();
  return {
    trackedSymbols: markets.length,
    connected: markets.length > 0,
    lastMessageAt: markets.reduce((latest, market) => Math.max(latest, Number(market.eventTime || 0)), 0) || null,
    lastError: null
  };
}
