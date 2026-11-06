import Database from "better-sqlite3";
import { mkdirSync, statSync, existsSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { config } from "./config.js";

const databasePath = resolve(config.sqlitePath);
mkdirSync(dirname(databasePath), { recursive: true });

export const db = new Database(databasePath);
db.pragma("journal_mode = WAL");
db.pragma("synchronous = NORMAL");
db.pragma("foreign_keys = ON");
db.pragma("busy_timeout = 5000");
db.pragma(`cache_size = -${config.database.cacheSizeKb}`);
db.pragma("temp_store = MEMORY");
db.pragma("wal_autocheckpoint = 1000");

db.exec(`
  CREATE TABLE IF NOT EXISTS markets (
    symbol TEXT PRIMARY KEY, pair TEXT NOT NULL, base_asset TEXT NOT NULL,
    quote_asset TEXT NOT NULL, margin_asset TEXT NOT NULL, contract_type TEXT NOT NULL,
    status TEXT NOT NULL, price_precision INTEGER, quantity_precision INTEGER,
    onboard_date INTEGER, delivery_date INTEGER,
    exchange_info_json TEXT NOT NULL DEFAULT '{}', updated_at INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS exchange_info (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    payload_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS tickers (
    symbol TEXT PRIMARY KEY, price_change REAL, price_change_percent REAL,
    weighted_avg_price REAL, last_price REAL, last_quantity REAL, open_price REAL,
    high_price REAL, low_price REAL, base_volume REAL, quote_volume REAL,
    open_time INTEGER, close_time INTEGER, trade_count INTEGER, event_time INTEGER,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY(symbol) REFERENCES markets(symbol) ON DELETE CASCADE
  );
  CREATE TABLE IF NOT EXISTS klines (
    symbol TEXT NOT NULL, interval TEXT NOT NULL, open_time INTEGER NOT NULL,
    open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
    volume REAL NOT NULL, close_time INTEGER NOT NULL, quote_volume REAL NOT NULL,
    trade_count INTEGER NOT NULL, taker_buy_base_volume REAL NOT NULL,
    taker_buy_quote_volume REAL NOT NULL, is_closed INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL, PRIMARY KEY(symbol, interval, open_time)
  );
  CREATE TABLE IF NOT EXISTS market_metrics (
    symbol TEXT PRIMARY KEY, mark_price REAL, index_price REAL,
    estimated_settle_price REAL, funding_rate REAL, next_funding_time INTEGER,
    open_interest REAL, open_interest_updated_at INTEGER, updated_at INTEGER NOT NULL,
    FOREIGN KEY(symbol) REFERENCES markets(symbol) ON DELETE CASCADE
  );
  CREATE TABLE IF NOT EXISTS top_movers_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL, base_asset TEXT NOT NULL, quote_asset TEXT NOT NULL,
    timeframe TEXT NOT NULL, direction TEXT NOT NULL, severity TEXT NOT NULL,
    categories TEXT NOT NULL DEFAULT '[]',
    change_percent REAL NOT NULL, last_price REAL NOT NULL,
    quote_volume REAL NOT NULL DEFAULT 0, base_volume REAL NOT NULL DEFAULT 0,
    high_price REAL NOT NULL DEFAULT 0, low_price REAL NOT NULL DEFAULT 0,
    candle_open_time INTEGER NOT NULL, candle_close_time INTEGER NOT NULL,
    event_time INTEGER NOT NULL, created_at INTEGER NOT NULL,
    UNIQUE(symbol, timeframe, candle_open_time)
  );
  CREATE INDEX IF NOT EXISTS idx_klines_symbol_interval_time
    ON klines(symbol, interval, open_time DESC);
  CREATE INDEX IF NOT EXISTS idx_klines_updated_at ON klines(updated_at);
  CREATE INDEX IF NOT EXISTS idx_tickers_quote_volume ON tickers(quote_volume DESC);
  CREATE INDEX IF NOT EXISTS idx_top_movers_history_event_time ON top_movers_history(event_time DESC);
  CREATE INDEX IF NOT EXISTS idx_top_movers_history_symbol_time ON top_movers_history(symbol, timeframe, event_time DESC);
`);

// [ADDED] Migrate existing databases without dropping previously collected history.
const marketColumns = db.pragma("table_info(markets)");
if (!marketColumns.some((column) => column.name === "exchange_info_json")) {
  db.exec("ALTER TABLE markets ADD COLUMN exchange_info_json TEXT NOT NULL DEFAULT '{}'");
}

const topMoverHistoryColumns = db.pragma("table_info(top_movers_history)");
if (!topMoverHistoryColumns.some((column) => column.name === "categories")) {
  db.exec("ALTER TABLE top_movers_history ADD COLUMN categories TEXT NOT NULL DEFAULT '[]'");
}

const upsertMarket = db.prepare(`
  INSERT INTO markets (symbol,pair,base_asset,quote_asset,margin_asset,contract_type,status,price_precision,quantity_precision,onboard_date,delivery_date,exchange_info_json,updated_at)
  VALUES (@symbol,@pair,@baseAsset,@quoteAsset,@marginAsset,@contractType,@status,@pricePrecision,@quantityPrecision,@onboardDate,@deliveryDate,@exchangeInfoJson,@updatedAt)
  ON CONFLICT(symbol) DO UPDATE SET pair=excluded.pair,base_asset=excluded.base_asset,
    quote_asset=excluded.quote_asset,margin_asset=excluded.margin_asset,
    contract_type=excluded.contract_type,status=excluded.status,
    price_precision=excluded.price_precision,quantity_precision=excluded.quantity_precision,
    onboard_date=excluded.onboard_date,delivery_date=excluded.delivery_date,
    exchange_info_json=excluded.exchange_info_json,updated_at=excluded.updated_at
`);
const upsertTicker = db.prepare(`
  INSERT INTO tickers (symbol,price_change,price_change_percent,weighted_avg_price,last_price,last_quantity,open_price,high_price,low_price,base_volume,quote_volume,open_time,close_time,trade_count,event_time,updated_at)
  VALUES (@symbol,@priceChange,@priceChangePercent,@weightedAvgPrice,@lastPrice,@lastQuantity,@openPrice,@highPrice,@lowPrice,@baseVolume,@quoteVolume,@openTime,@closeTime,@tradeCount,@eventTime,@updatedAt)
  ON CONFLICT(symbol) DO UPDATE SET price_change=excluded.price_change,
    price_change_percent=excluded.price_change_percent,weighted_avg_price=excluded.weighted_avg_price,
    last_price=excluded.last_price,last_quantity=excluded.last_quantity,open_price=excluded.open_price,
    high_price=excluded.high_price,low_price=excluded.low_price,base_volume=excluded.base_volume,
    quote_volume=excluded.quote_volume,open_time=excluded.open_time,close_time=excluded.close_time,
    trade_count=excluded.trade_count,event_time=excluded.event_time,updated_at=excluded.updated_at
`);
const upsertKline = db.prepare(`
  INSERT INTO klines (symbol,interval,open_time,open,high,low,close,volume,close_time,quote_volume,trade_count,taker_buy_base_volume,taker_buy_quote_volume,is_closed,updated_at)
  VALUES (@symbol,@interval,@openTime,@open,@high,@low,@close,@volume,@closeTime,@quoteVolume,@tradeCount,@takerBuyBaseVolume,@takerBuyQuoteVolume,1,@updatedAt)
  ON CONFLICT(symbol,interval,open_time) DO UPDATE SET open=excluded.open,high=excluded.high,
    low=excluded.low,close=excluded.close,volume=excluded.volume,close_time=excluded.close_time,
    quote_volume=excluded.quote_volume,trade_count=excluded.trade_count,
    taker_buy_base_volume=excluded.taker_buy_base_volume,
    taker_buy_quote_volume=excluded.taker_buy_quote_volume,is_closed=1,updated_at=excluded.updated_at
`);
const upsertMetric = db.prepare(`
  INSERT INTO market_metrics (symbol,mark_price,index_price,estimated_settle_price,funding_rate,next_funding_time,open_interest,open_interest_updated_at,updated_at)
  VALUES (@symbol,@markPrice,@indexPrice,@estimatedSettlePrice,@fundingRate,@nextFundingTime,@openInterest,@openInterestUpdatedAt,@updatedAt)
  ON CONFLICT(symbol) DO UPDATE SET mark_price=excluded.mark_price,index_price=excluded.index_price,
    estimated_settle_price=excluded.estimated_settle_price,funding_rate=excluded.funding_rate,
    next_funding_time=excluded.next_funding_time,
    open_interest=COALESCE(excluded.open_interest,market_metrics.open_interest),
    open_interest_updated_at=COALESCE(excluded.open_interest_updated_at,market_metrics.open_interest_updated_at),
    updated_at=excluded.updated_at
`);

const selectKlinesLatest = db.prepare(`
  SELECT * FROM klines WHERE symbol=? AND interval=? ORDER BY open_time DESC LIMIT ?
`);
const selectKlinesUntil = db.prepare(`
  SELECT * FROM klines WHERE symbol=? AND interval=? AND open_time<=? ORDER BY open_time DESC LIMIT ?
`);
const selectMarketSnapshot = db.prepare(`
  SELECT m.symbol,m.pair,m.base_asset AS baseAsset,m.quote_asset AS quoteAsset,
    m.margin_asset AS marginAsset,m.contract_type AS contractType,m.status,
    m.price_precision AS pricePrecision,m.quantity_precision AS quantityPrecision,
    m.onboard_date AS onboardDate,m.delivery_date AS deliveryDate,
    m.exchange_info_json AS exchangeInfoJson,
    t.price_change AS priceChange,t.price_change_percent AS priceChangePercent,
    t.weighted_avg_price AS weightedAvgPrice,t.last_price AS lastPrice,
    t.last_quantity AS lastQuantity,t.open_price AS openPrice,t.high_price AS highPrice,
    t.low_price AS lowPrice,t.base_volume AS baseVolume,t.quote_volume AS quoteVolume,
    t.open_time AS openTime,t.close_time AS closeTime,t.trade_count AS tradeCount,t.event_time AS eventTime
  FROM markets m LEFT JOIN tickers t ON t.symbol=m.symbol
  WHERE m.status='TRADING' ORDER BY m.symbol ASC
`);
const selectMetric = db.prepare("SELECT * FROM market_metrics WHERE symbol=?");
const insertTopMoverHistory = db.prepare(`
  INSERT INTO top_movers_history (
    symbol,base_asset,quote_asset,timeframe,direction,severity,categories,change_percent,last_price,
    quote_volume,base_volume,high_price,low_price,candle_open_time,candle_close_time,event_time,created_at
  ) VALUES (
    @symbol,@baseAsset,@quoteAsset,@timeframe,@direction,@severity,@categories,@changePercent,@lastPrice,
    @quoteVolume,@baseVolume,@highPrice,@lowPrice,@candleOpenTime,@candleCloseTime,@eventTime,@createdAt
  )
  ON CONFLICT(symbol,timeframe,candle_open_time) DO UPDATE SET
    base_asset=excluded.base_asset,quote_asset=excluded.quote_asset,direction=excluded.direction,
    severity=excluded.severity,categories=excluded.categories,
    change_percent=excluded.change_percent,last_price=excluded.last_price,
    quote_volume=excluded.quote_volume,base_volume=excluded.base_volume,high_price=excluded.high_price,
    low_price=excluded.low_price,candle_close_time=excluded.candle_close_time,
    event_time=excluded.event_time,created_at=excluded.created_at
`);
const countTopMoverHistory = db.prepare("SELECT COUNT(*) AS count FROM top_movers_history");
const pruneTopMoverHistory = db.prepare("DELETE FROM top_movers_history WHERE event_time < ?");
const MAX_TOP_MOVER_HISTORY_ROWS = 500;
const pruneExcessTopMoverHistory = db.prepare(`
  DELETE FROM top_movers_history
  WHERE id NOT IN (
    SELECT id FROM top_movers_history
    ORDER BY event_time DESC, id DESC
    LIMIT ?
  )
`);
const prunePair = db.prepare(`
  DELETE FROM klines WHERE symbol=? AND interval=? AND open_time NOT IN (
    SELECT open_time FROM klines WHERE symbol=? AND interval=? ORDER BY open_time DESC LIMIT ?
  )
`);
const pruneExpired = db.prepare("DELETE FROM klines WHERE open_time < ?");
const countKlines = db.prepare("SELECT COUNT(*) AS count FROM klines");
const pruneGlobal = db.prepare(`
  DELETE FROM klines WHERE rowid IN (
    SELECT rowid FROM klines ORDER BY open_time ASC LIMIT ?
  )
`);

let persistedSymbols = new Set();
let maintenanceTimer = null;
let cachedKlineCount = countKlines.get().count;
let lastCountRefreshAt = Date.now();
let lastMaintenanceAt = null;

export function setPersistedSymbols(symbols) {
  persistedSymbols = new Set((Array.isArray(symbols) ? symbols : []).slice(0, config.database.maxPersistedSymbols));
}

export const saveMarkets = db.transaction((markets) => {
  const now = Date.now();
  const active = new Set();
  for (const market of markets) {
    upsertMarket.run({
      ...market,
      exchangeInfoJson: JSON.stringify(market.exchangeInfo ?? {}),
      updatedAt: now
    });
    active.add(market.symbol);
  }
  if (active.size) {
    const placeholders = [...active].map(() => "?").join(",");
    db.prepare(`DELETE FROM markets WHERE symbol NOT IN (${placeholders})`).run(...active);
  } else {
    db.prepare("DELETE FROM markets").run();
  }
});

export const saveTicker = (ticker) => {
  upsertTicker.run({ ...ticker, updatedAt: Date.now() });
};

export const saveTickers = db.transaction((tickers) => {
  const now = Date.now();
  for (const ticker of tickers) upsertTicker.run({ ...ticker, updatedAt: now });
});

function isPersistableKline(kline) {
  return Boolean(
    kline &&
    kline.isClosed === true &&
    persistedSymbols.has(kline.symbol) &&
    config.database.allowedIntervals.includes(kline.interval) &&
    Number.isFinite(kline.openTime) &&
    Number.isFinite(kline.closeTime) &&
    kline.closeTime >= Date.now() - config.database.retentionDays * 86400000 &&
    kline.closeTime < Date.now()
  );
}

export const saveKline = db.transaction((kline) => {
  if (!isPersistableKline(kline)) return false;
  upsertKline.run({ ...kline, updatedAt: Date.now() });
  prunePair.run(kline.symbol, kline.interval, kline.symbol, kline.interval, config.database.pairRetention);
  return true;
});

export const saveKlines = db.transaction((klines) => {
  const now = Date.now();
  const touchedPairs = new Set();
  let saved = 0;
  for (const kline of klines) {
    if (!isPersistableKline(kline)) continue;
    upsertKline.run({ ...kline, updatedAt: now });
    touchedPairs.add(`${kline.symbol}\u0000${kline.interval}`);
    saved += 1;
  }
  for (const pair of touchedPairs) {
    const [symbol, interval] = pair.split("\u0000");
    prunePair.run(symbol, interval, symbol, interval, config.database.pairRetention);
  }
  return saved;
});

export const saveMarketMetrics = (metrics) => {
  upsertMetric.run({ ...metrics, updatedAt: Date.now() });
};

export const saveTopMoverHistory = db.transaction((event) => {
  if (!event || !event.symbol || !event.timeframe || !Number.isFinite(event.candleOpenTime)) return false;
  insertTopMoverHistory.run({
    ...event,
    categories: JSON.stringify(Array.isArray(event.categories) ? event.categories : []),
    createdAt: Date.now()
  });
  pruneExcessTopMoverHistory.run(MAX_TOP_MOVER_HISTORY_ROWS);
  return true;
});

export function getTopMoverHistory({ limit = 100, symbol = null, timeframe = null, category = null, from = null, to = null } = {}) {
  const conditions = [];
  const params = [];
  if (symbol) {
    conditions.push("symbol = ?");
    params.push(symbol);
  }
  if (timeframe) {
    conditions.push("timeframe = ?");
    params.push(timeframe);
  }
  if (category) {
    conditions.push("categories LIKE ?");
    params.push(`%"${category}"%`);
  }
  if (from !== null) {
    conditions.push("event_time >= ?");
    params.push(from);
  }
  if (to !== null) {
    conditions.push("event_time <= ?");
    params.push(to);
  }
  const where = conditions.length ? " WHERE " + conditions.join(" AND ") : "";
  const query = `
    SELECT symbol,base_asset AS baseAsset,quote_asset AS quoteAsset,timeframe,direction,severity,
      change_percent AS changePercent,categories,last_price AS lastPrice,quote_volume AS quoteVolume,
      base_volume AS baseVolume,high_price AS highPrice,low_price AS lowPrice,
      candle_open_time AS candleOpenTime,candle_close_time AS candleCloseTime,event_time AS eventTime,
      created_at AS createdAt
    FROM top_movers_history${where}
    ORDER BY event_time DESC LIMIT ?
  `;
  return db.prepare(query).all(...params, limit).map((row) => {
    let categories = [];
    try {
      categories = JSON.parse(row.categories || "[]");
    } catch {
      categories = [];
    }
    return { ...row, categories: Array.isArray(categories) ? categories : [] };
  });
}

function mapKline(row) {
  return {
    openTime: row.open_time, open: row.open, high: row.high, low: row.low,
    close: row.close, volume: row.volume, closeTime: row.close_time,
    quoteVolume: row.quote_volume, tradeCount: row.trade_count,
    takerBuyBaseVolume: row.taker_buy_base_volume,
    takerBuyQuoteVolume: row.taker_buy_quote_volume, isClosed: true
  };
}

export function getStoredKlines(symbol, interval, limit = 500, endTime = null) {
  const rows = endTime !== null
    ? selectKlinesUntil.all(symbol, interval, endTime, limit)
    : selectKlinesLatest.all(symbol, interval, limit);
  return rows.reverse().map(mapKline);
}

export function getStoredMarketSnapshot() {
  return selectMarketSnapshot.all().map((row) => {
    const { exchangeInfoJson, ...market } = row;
    let exchangeInfo = {};
    try {
      exchangeInfo = JSON.parse(exchangeInfoJson || "{}");
    } catch {
      exchangeInfo = {};
    }
    return { ...market, exchangeInfo };
  });
}

// [ADDED] Preserve the complete Binance USD-M exchangeInfo payload, including global metadata.
export function saveExchangeInfo(payload) {
  if (!payload || typeof payload !== "object" || !Array.isArray(payload.symbols)) {
    throw new Error("Invalid Binance exchangeInfo payload");
  }
  db.prepare(`
    INSERT INTO exchange_info (id, payload_json, updated_at)
    VALUES (1, ?, ?)
    ON CONFLICT(id) DO UPDATE SET payload_json=excluded.payload_json, updated_at=excluded.updated_at
  `).run(JSON.stringify(payload), Date.now());
}

export function getStoredExchangeInfo() {
  const row = db.prepare("SELECT payload_json, updated_at FROM exchange_info WHERE id=1").get();
  if (!row) return null;
  try {
    return { ...JSON.parse(row.payload_json), updatedAt: row.updated_at };
  } catch {
    return null;
  }
}

export function getStoredMetrics(symbol) {
  const row = selectMetric.get(symbol);
  if (!row) return null;
  return {
    symbol: row.symbol, markPrice: row.mark_price, indexPrice: row.index_price,
    estimatedSettlePrice: row.estimated_settle_price, fundingRate: row.funding_rate,
    nextFundingTime: row.next_funding_time, openInterest: row.open_interest,
    openInterestUpdatedAt: row.open_interest_updated_at, updatedAt: row.updated_at
  };
}

function fileSize(path) {
  try {
    return existsSync(path) ? statSync(path).size : 0;
  } catch {
    return 0;
  }
}

export function runDatabaseMaintenance() {
  const cutoff = Date.now() - config.database.retentionDays * 86400000;
  pruneExpired.run(cutoff);
  pruneTopMoverHistory.run(cutoff);
  const count = countKlines.get().count;
  if (count > config.database.maxKlineRows) {
    pruneGlobal.run(count - config.database.maxKlineRows);
  }
  db.pragma("wal_checkpoint(PASSIVE)");
  db.pragma("optimize");
  cachedKlineCount = countKlines.get().count;
  lastCountRefreshAt = Date.now();
  lastMaintenanceAt = Date.now();
  const totalBytes = fileSize(databasePath) + fileSize(`${databasePath}-wal`);
  return { klineCount: cachedKlineCount, totalBytes, maxBytes: config.database.maxDatabaseMb * 1048576 };
}

export function startDatabaseMaintenance() {
  if (maintenanceTimer) return;
  runDatabaseMaintenance();
  maintenanceTimer = setInterval(() => {
    try {
      const status = runDatabaseMaintenance();
      if (status.totalBytes > status.maxBytes) {
        console.warn("SQLite database is above the configured size target; retention cleanup has run");
      }
    } catch (error) {
      console.error("SQLite maintenance failed:", error.message);
    }
  }, config.database.maintenanceIntervalMs);
  maintenanceTimer.unref?.();
}

export function getDatabaseStatus() {
  if (Date.now() - lastCountRefreshAt >= 60000) {
    cachedKlineCount = countKlines.get().count;
    lastCountRefreshAt = Date.now();
  }
  const totalBytes = fileSize(databasePath) + fileSize(`${databasePath}-wal`);
  return {
    path: config.sqlitePath,
    journalMode: db.pragma("journal_mode", { simple: true }),
    klineCount: cachedKlineCount,
    topMoverHistoryCount: countTopMoverHistory.get().count,
    totalBytes,
    maxBytes: config.database.maxDatabaseMb * 1048576,
    persistedSymbolCount: persistedSymbols.size,
    lastMaintenanceAt
  };
}

export function closeDatabase() {
  if (maintenanceTimer) clearInterval(maintenanceTimer);
  maintenanceTimer = null;
  if (db.open) {
    try {
      db.pragma("wal_checkpoint(TRUNCATE)");
    } catch (error) {
      console.warn("SQLite WAL checkpoint failed during shutdown:", error.message);
    }
    db.close();
  }
}
