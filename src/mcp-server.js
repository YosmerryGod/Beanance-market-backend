import "dotenv/config";
import { createServer } from "node:http";
import { timingSafeEqual } from "node:crypto";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";
import { db, getStoredKlines } from "./database.js";
import { calculateIndicators, normalizeIndicatorOptions } from "./indicators.js";

const HOST = process.env.MCP_HOST || "127.0.0.1";
const PORT = Number(process.env.MCP_PORT || 3031);
const AUTH_TOKEN = process.env.MCP_AUTH_TOKEN || "";
const ALLOWED_INTERVALS = new Set(["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"]);
const DEFAULT_INDICATOR_OPTIONS = normalizeIndicatorOptions({
  maPeriods: "20,50",
  emaPeriods: "9,21"
});

const AUTH_CONFIGURED = AUTH_TOKEN.length >= 32;

function jsonToolResult(value) {
  return {
    content: [{ type: "text", text: JSON.stringify(value) }]
  };
}

function jsonToolError(error) {
  return {
    isError: true,
    content: [{ type: "text", text: error instanceof Error ? error.message : String(error) }]
  };
}

function normalizeSymbol(value) {
  const symbol = String(value || "").trim().toUpperCase();
  if (!/^[A-Z0-9]{3,30}$/.test(symbol)) {
    throw new Error("Invalid futures symbol");
  }
  return symbol;
}

function normalizeInterval(value) {
  const interval = String(value || "15m");
  if (!ALLOWED_INTERVALS.has(interval)) {
    throw new Error("Unsupported interval");
  }
  return interval;
}

function getMarketOverview() {
  const marketCount = db.prepare("SELECT COUNT(*) AS count FROM markets WHERE status='TRADING'").get().count;
  const tickerCount = db.prepare("SELECT COUNT(*) AS count FROM tickers").get().count;
  const klineStats = db.prepare(`
    SELECT COUNT(*) AS rowCount, COUNT(DISTINCT symbol) AS symbolCount,
      COUNT(DISTINCT interval) AS intervalCount, MIN(open_time) AS oldestOpenTime,
      MAX(open_time) AS newestOpenTime
    FROM klines WHERE is_closed=1
  `).get();
  const intervals = db.prepare("SELECT interval, COUNT(*) AS rows, COUNT(DISTINCT symbol) AS symbols FROM klines WHERE is_closed=1 GROUP BY interval ORDER BY interval").all();
  const latestUpdate = db.prepare("SELECT MAX(updated_at) AS updatedAt FROM markets").get().updatedAt;

  return {
    service: "beanance-market-backend-mcp",
    status: "ok",
    readOnly: true,
    marketCount,
    tickerCount,
    storedKlines: klineStats,
    intervals,
    marketMetadataUpdatedAt: latestUpdate
  };
}

function createMcpServer() {
  const server = new McpServer({
    name: "beanance-market-mcp",
    version: "1.0.0"
  });

  server.tool(
    "get_market_overview",
    "Read-only overview of the Beanance market backend and its SQLite historical candle coverage.",
    {},
    async () => {
      try {
        return jsonToolResult(getMarketOverview());
      } catch (error) {
        return jsonToolError(error);
      }
    }
  );

  server.tool(
    "list_futures_markets",
    "List active Binance USDT-M perpetual futures markets stored by Beanance. Results are read-only.",
    {
      limit: z.number().int().min(1).max(200).default(100),
      sort: z.enum(["symbol", "quote_volume", "price_change_percent"]).default("quote_volume"),
      order: z.enum(["asc", "desc"]).default("desc")
    },
    async ({ limit, sort, order }) => {
      try {
        const orderBy = sort === "symbol"
          ? "m.symbol"
          : sort === "price_change_percent"
            ? "t.price_change_percent"
            : "t.quote_volume";
        const direction = order === "asc" ? "ASC" : "DESC";
        const rows = db.prepare(`
          SELECT m.symbol, m.base_asset AS baseAsset, m.quote_asset AS quoteAsset,
            m.status, t.last_price AS lastPrice, t.price_change_percent AS priceChangePercent,
            t.quote_volume AS quoteVolume, t.updated_at AS updatedAt
          FROM markets m LEFT JOIN tickers t ON t.symbol=m.symbol
          WHERE m.status='TRADING'
          ORDER BY ${orderBy} ${direction}
          LIMIT ?
        `).all(limit);
        return jsonToolResult({ count: rows.length, data: rows });
      } catch (error) {
        return jsonToolError(error);
      }
    }
  );

  server.tool(
    "get_historical_klines",
    "Read stored closed OHLCV candles for one active futures symbol. Does not fetch or modify Binance data.",
    {
      symbol: z.string().min(3).max(30),
      interval: z.string().default("15m"),
      limit: z.number().int().min(1).max(1500).default(500),
      endTime: z.number().int().positive().optional()
    },
    async ({ symbol: rawSymbol, interval: rawInterval, limit, endTime }) => {
      try {
        const symbol = normalizeSymbol(rawSymbol);
        const interval = normalizeInterval(rawInterval);
        const market = db.prepare("SELECT symbol FROM markets WHERE symbol=? AND status='TRADING'").get(symbol);
        if (!market) throw new Error("Active futures market not found");
        const candles = getStoredKlines(symbol, interval, limit, endTime ?? null);
        return jsonToolResult({ symbol, interval, count: candles.length, source: "sqlite", data: candles });
      } catch (error) {
        return jsonToolError(error);
      }
    }
  );

  server.tool(
    "get_market_indicators",
    "Calculate the backend's configured MA, EMA, SAR, Supertrend, Bollinger Bands, volume delta, CVD and taker flow from stored candles.",
    {
      symbol: z.string().min(3).max(30),
      interval: z.string().default("15m"),
      limit: z.number().int().min(30).max(1500).default(500),
      endTime: z.number().int().positive().optional()
    },
    async ({ symbol: rawSymbol, interval: rawInterval, limit, endTime }) => {
      try {
        const symbol = normalizeSymbol(rawSymbol);
        const interval = normalizeInterval(rawInterval);
        const market = db.prepare("SELECT symbol FROM markets WHERE symbol=? AND status='TRADING'").get(symbol);
        if (!market) throw new Error("Active futures market not found");
        const candles = getStoredKlines(symbol, interval, limit, endTime ?? null);
        const indicators = calculateIndicators(candles, DEFAULT_INDICATOR_OPTIONS);
        return jsonToolResult({
          symbol,
          interval,
          count: candles.length,
          source: "sqlite",
          candleAlignment: "Indicator array index i corresponds to candles[i]",
          candles,
          indicators
        });
      } catch (error) {
        return jsonToolError(error);
      }
    }
  );

  server.tool(
    "scan_early_bullish_pattern",
    "Scan stored candle history across active futures for the same four-candle early bullish pattern: positive taker volume delta, rising CVD, EMA9 below EMA21, bearish Supertrend, and price rising. Scans SQLite only and never places trades.",
    {
      interval: z.string().default("15m"),
      candlesBack: z.number().int().min(100).max(2000).default(2000),
      minPositiveDeltaCandles: z.number().int().min(1).max(4).default(4),
      minPriceRisePercent: z.number().min(0).max(10).default(0),
      maxResults: z.number().int().min(1).max(100).default(50)
    },
    async ({ interval: rawInterval, candlesBack, minPositiveDeltaCandles, minPriceRisePercent, maxResults }) => {
      try {
        const interval = normalizeInterval(rawInterval);
        const symbols = db.prepare("SELECT symbol FROM markets WHERE status='TRADING' ORDER BY symbol").all();
        const queryMatches = [];
        let scannedSymbols = 0;
        let symbolsWithCandles = 0;
        let matchingSymbols = new Set();
        let totalMatchingWindows = 0;

        for (const { symbol } of symbols) {
          scannedSymbols += 1;
          const candles = getStoredKlines(symbol, interval, candlesBack);
          if (candles.length < 60) continue;
          symbolsWithCandles += 1;

          const indicators = calculateIndicators(candles, DEFAULT_INDICATOR_OPTIONS);
          const ema9 = indicators.ema[9];
          const ema21 = indicators.ema[21];
          const delta = indicators.volumeDelta.base;
          const supertrendDirection = indicators.supertrend.direction;

          for (let endIndex = 3; endIndex < candles.length; endIndex += 1) {
            const startIndex = endIndex - 3;
            let positiveDeltaCount = 0;
            let emaBearish = true;
            let supertrendBearish = true;
            let cvdRising = true;

            for (let index = startIndex; index <= endIndex; index += 1) {
              if (Number(delta[index]) > 0) positiveDeltaCount += 1;
              if (ema9[index] === null || ema21[index] === null || !(ema9[index] < ema21[index])) emaBearish = false;
              if (supertrendDirection[index] !== -1) supertrendBearish = false;
              if (index > startIndex && !(indicators.cvd.base[index] > indicators.cvd.base[index - 1])) cvdRising = false;
            }

            const firstClose = candles[startIndex].close;
            const lastClose = candles[endIndex].close;
            const priceRisePercent = firstClose > 0 ? ((lastClose - firstClose) / firstClose) * 100 : 0;
            const priceRising = priceRisePercent > minPriceRisePercent;

            if (
              positiveDeltaCount < minPositiveDeltaCandles ||
              !emaBearish ||
              !supertrendBearish ||
              !cvdRising ||
              !priceRising
            ) continue;

            totalMatchingWindows += 1;
            matchingSymbols.add(symbol);
            queryMatches.push({
              symbol,
              interval,
              startTime: candles[startIndex].openTime,
              endTime: candles[endIndex].closeTime,
              startClose: firstClose,
              endClose: lastClose,
              priceRisePercent: Number(priceRisePercent.toFixed(4)),
              positiveDeltaCandles: positiveDeltaCount,
              ema9: ema9[endIndex],
              ema21: ema21[endIndex],
              supertrendDirection: supertrendDirection[endIndex],
              volumeDelta: delta.slice(startIndex, endIndex + 1)
            });
          }
        }

        queryMatches.sort((a, b) => b.endTime - a.endTime);
        return jsonToolResult({
          interval,
          pattern: {
            candleCount: 4,
            minPositiveDeltaCandles,
            cvdRising: true,
            ema9BelowEma21: true,
            supertrendBearish: true,
            priceRisePercentGreaterThan: minPriceRisePercent
          },
          scannedSymbols,
          symbolsWithCandles,
          matchingSymbols: matchingSymbols.size,
          matchingWindows: totalMatchingWindows,
          returnedMatches: Math.min(queryMatches.length, maxResults),
          truncated: queryMatches.length > maxResults,
          matches: queryMatches.slice(0, maxResults)
        });
      } catch (error) {
        return jsonToolError(error);
      }
    }
  );

  return server;
}

function isAuthorized(request) {
  const authorization = request.headers.authorization || "";
  const suppliedToken = authorization.startsWith("Bearer ") ? authorization.slice(7) : "";
  const expected = Buffer.from(AUTH_TOKEN);
  const supplied = Buffer.from(suppliedToken);
  return supplied.length === expected.length && timingSafeEqual(supplied, expected);
}

const httpServer = createServer(async (request, response) => {
  const url = new URL(request.url || "/", `http://${request.headers.host || "localhost"}`);

  if (url.pathname === "/health" && request.method === "GET") {
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({
      success: true,
      service: "beanance-market-mcp",
      status: AUTH_CONFIGURED ? "ok" : "not_configured",
      authenticated: AUTH_CONFIGURED
    }));
    return;
  }

  if (url.pathname !== "/mcp") {
    response.writeHead(404, { "content-type": "application/json" });
    response.end(JSON.stringify({ success: false, error: "NOT_FOUND" }));
    return;
  }

  if (!AUTH_CONFIGURED) {
    response.writeHead(503, { "content-type": "application/json" });
    response.end(JSON.stringify({ success: false, error: "MCP_AUTH_NOT_CONFIGURED" }));
    return;
  }

  if (!isAuthorized(request)) {
    response.writeHead(401, {
      "content-type": "application/json",
      "www-authenticate": "Bearer"
    });
    response.end(JSON.stringify({ success: false, error: "UNAUTHORIZED" }));
    return;
  }

  const mcpServer = createMcpServer();
  const transport = new StreamableHTTPServerTransport({
    sessionIdGenerator: undefined,
    enableJsonResponse: true
  });

  try {
    await mcpServer.connect(transport);
    await transport.handleRequest(request, response);
  } catch (error) {
    console.error("MCP request failed:", error);
    if (!response.headersSent) {
      response.writeHead(500, { "content-type": "application/json" });
      response.end(JSON.stringify({ success: false, error: "MCP_REQUEST_FAILED" }));
    } else if (!response.writableEnded) {
      response.end();
    }
  } finally {
    await transport.close().catch(() => {});
    await mcpServer.close().catch(() => {});
  }
});

httpServer.listen(PORT, HOST, () => {
  console.info(`Beanance Market MCP listening on http://${HOST}:${PORT}/mcp`);
  if (!AUTH_CONFIGURED) {
    console.warn("MCP_AUTH_TOKEN is missing or shorter than 32 characters; MCP requests are disabled until configured.");
  }
});

function shutdown(signal) {
  console.info(`Received ${signal}; shutting down Beanance Market MCP`);
  httpServer.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 10000).unref();
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
