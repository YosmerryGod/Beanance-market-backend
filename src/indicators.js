const DEFAULTS = {
  maPeriods: [20, 50],
  emaPeriods: [9, 21],
  sarStep: 0.02,
  sarMax: 0.2,
  supertrendPeriod: 10,
  supertrendMultiplier: 3,
  bollingerPeriod: 20,
  bollingerStdDev: 2
};

function finiteNumber(value, fallback, minimum, maximum) {
  const number = Number(value);
  if (!Number.isFinite(number) || number < minimum || number > maximum) return fallback;
  return number;
}

function parsePeriods(value, fallback) {
  if (value === undefined || value === null || value === "") return fallback;
  const values = String(value).split(",").map((item) => Number(item.trim()));
  if (!values.length || values.length > 10 || values.some((item) => !Number.isInteger(item) || item < 1 || item > 500)) {
    const error = new Error("Periods must be comma-separated integers between 1 and 500 (maximum 10 periods)");
    error.statusCode = 400;
    throw error;
  }
  return [...new Set(values)];
}

export function normalizeIndicatorOptions(query = {}) {
  const options = {
    maPeriods: parsePeriods(query.maPeriods, DEFAULTS.maPeriods),
    emaPeriods: parsePeriods(query.emaPeriods, DEFAULTS.emaPeriods),
    sarStep: finiteNumber(query.sarStep, DEFAULTS.sarStep, 0.001, 1),
    sarMax: finiteNumber(query.sarMax, DEFAULTS.sarMax, 0.01, 1),
    supertrendPeriod: finiteNumber(query.supertrendPeriod, DEFAULTS.supertrendPeriod, 1, 500),
    supertrendMultiplier: finiteNumber(query.supertrendMultiplier, DEFAULTS.supertrendMultiplier, 0.1, 20),
    bollingerPeriod: finiteNumber(query.bollingerPeriod, DEFAULTS.bollingerPeriod, 1, 500),
    bollingerStdDev: finiteNumber(query.bollingerStdDev, DEFAULTS.bollingerStdDev, 0.1, 10)
  };
  if (options.sarStep > options.sarMax) {
    const error = new Error("sarStep must be less than or equal to sarMax");
    error.statusCode = 400;
    throw error;
  }
  return options;
}

function sma(values, period) {
  const result = Array(values.length).fill(null);
  let sum = 0;
  for (let index = 0; index < values.length; index += 1) {
    sum += values[index];
    if (index >= period) sum -= values[index - period];
    if (index >= period - 1) result[index] = sum / period;
  }
  return result;
}

function ema(values, period) {
  const result = Array(values.length).fill(null);
  if (values.length < period) return result;
  const multiplier = 2 / (period + 1);
  let previous = values.slice(0, period).reduce((sum, value) => sum + value, 0) / period;
  result[period - 1] = previous;
  for (let index = period; index < values.length; index += 1) {
    previous = (values[index] - previous) * multiplier + previous;
    result[index] = previous;
  }
  return result;
}

function trueRanges(candles) {
  return candles.map((candle, index) => {
    if (index === 0) return candle.high - candle.low;
    const previousClose = candles[index - 1].close;
    return Math.max(
      candle.high - candle.low,
      Math.abs(candle.high - previousClose),
      Math.abs(candle.low - previousClose)
    );
  });
}

function atr(candles, period) {
  const ranges = trueRanges(candles);
  const result = Array(candles.length).fill(null);
  if (ranges.length < period) return result;
  let previous = ranges.slice(0, period).reduce((sum, value) => sum + value, 0) / period;
  result[period - 1] = previous;
  for (let index = period; index < ranges.length; index += 1) {
    previous = ((previous * (period - 1)) + ranges[index]) / period;
    result[index] = previous;
  }
  return result;
}

function parabolicSar(candles, step, maximum) {
  const length = candles.length;
  const result = Array(length).fill(null);
  if (length < 2) return result;

  let rising = candles[1].close >= candles[0].close;
  let acceleration = step;
  let extreme = rising
    ? Math.max(candles[0].high, candles[1].high)
    : Math.min(candles[0].low, candles[1].low);
  let sar = rising
    ? Math.min(candles[0].low, candles[1].low)
    : Math.max(candles[0].high, candles[1].high);
  result[1] = sar;

  for (let index = 2; index < length; index += 1) {
    sar += acceleration * (extreme - sar);
    if (rising) {
      sar = Math.min(sar, candles[index - 1].low, candles[index - 2].low);
      if (candles[index].low < sar) {
        rising = false;
        sar = extreme;
        extreme = candles[index].low;
        acceleration = step;
      } else if (candles[index].high > extreme) {
        extreme = candles[index].high;
        acceleration = Math.min(acceleration + step, maximum);
      }
    } else {
      sar = Math.max(sar, candles[index - 1].high, candles[index - 2].high);
      if (candles[index].high > sar) {
        rising = true;
        sar = extreme;
        extreme = candles[index].high;
        acceleration = step;
      } else if (candles[index].low < extreme) {
        extreme = candles[index].low;
        acceleration = Math.min(acceleration + step, maximum);
      }
    }
    result[index] = sar;
  }
  return result;
}

function supertrend(candles, period, multiplier) {
  const length = candles.length;
  const atrValues = atr(candles, period);
  const line = Array(length).fill(null);
  const direction = Array(length).fill(null);
  let upperBand = null;
  let lowerBand = null;
  let trend = 1;

  for (let index = 0; index < length; index += 1) {
    if (atrValues[index] === null) continue;
    const midpoint = (candles[index].high + candles[index].low) / 2;
    const basicUpper = midpoint + multiplier * atrValues[index];
    const basicLower = midpoint - multiplier * atrValues[index];

    if (upperBand === null) {
      upperBand = basicUpper;
      lowerBand = basicLower;
      trend = candles[index].close >= midpoint ? 1 : -1;
    } else {
      const previousClose = candles[index - 1].close;
      upperBand = basicUpper < upperBand || previousClose > upperBand ? basicUpper : upperBand;
      lowerBand = basicLower > lowerBand || previousClose < lowerBand ? basicLower : lowerBand;
      if (trend === 1 && candles[index].close < lowerBand) trend = -1;
      else if (trend === -1 && candles[index].close > upperBand) trend = 1;
    }
    line[index] = trend === 1 ? lowerBand : upperBand;
    direction[index] = trend;
  }
  return { line, direction };
}

function bollinger(values, period, deviation) {
  const middle = sma(values, period);
  const upper = Array(values.length).fill(null);
  const lower = Array(values.length).fill(null);
  for (let index = period - 1; index < values.length; index += 1) {
    const mean = middle[index];
    let squared = 0;
    for (let offset = index - period + 1; offset <= index; offset += 1) {
      squared += (values[offset] - mean) ** 2;
    }
    const standardDeviation = Math.sqrt(squared / period);
    upper[index] = mean + standardDeviation * deviation;
    lower[index] = mean - standardDeviation * deviation;
  }
  return { upper, middle, lower };
}

export function calculateIndicators(candles, options) {
  const closes = candles.map((candle) => Number(candle.close));
  const result = {
    ma: {},
    ema: {},
    sar: parabolicSar(candles, options.sarStep, options.sarMax),
    supertrend: (() => {
      const values = supertrend(candles, options.supertrendPeriod, options.supertrendMultiplier);
      return {
        value: values.line,
        direction: values.direction,
        period: options.supertrendPeriod,
        multiplier: options.supertrendMultiplier
      };
    })(),
    bollinger: (() => {
      const values = bollinger(closes, options.bollingerPeriod, options.bollingerStdDev);
      return {
        upper: values.upper,
        middle: values.middle,
        lower: values.lower,
        period: options.bollingerPeriod,
        stdDev: options.bollingerStdDev
      };
    })()
  };

  for (const period of options.maPeriods) result.ma[period] = sma(closes, period);
  for (const period of options.emaPeriods) result.ema[period] = ema(closes, period);

  result.settings = options;
  return result;
}
