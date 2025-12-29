-- Continuous aggregates for M5/M15/H1 (adjust as needed)

-- Helper macro-like comment:
-- 'first'/'last' require TimescaleDB toolkit or built-ins depending on version; fallback uses ORDER BY + DISTINCT ON.
-- Use built-in first/last aggregates if available. Otherwise use this typical OHLC pattern.

CREATE MATERIALIZED VIEW IF NOT EXISTS fx_m5
WITH (timescaledb.continuous) AS
SELECT
  symbol,
  time_bucket('5 minutes', ts) AS bucket,
  first(open, ts)  AS open,
  max(high)        AS high,
  min(low)         AS low,
  last(close, ts)  AS close,
  sum(volume)      AS volume
FROM fx_m1
GROUP BY symbol, bucket;

CREATE MATERIALIZED VIEW IF NOT EXISTS fx_m15
WITH (timescaledb.continuous) AS
SELECT
  symbol,
  time_bucket('15 minutes', ts) AS bucket,
  first(open, ts)  AS open,
  max(high)        AS high,
  min(low)         AS low,
  last(close, ts)  AS close,
  sum(volume)      AS volume
FROM fx_m1
GROUP BY symbol, bucket;

CREATE MATERIALIZED VIEW IF NOT EXISTS fx_h1
WITH (timescaledb.continuous) AS
SELECT
  symbol,
  time_bucket('1 hour', ts) AS bucket,
  first(open, ts)  AS open,
  max(high)        AS high,
  min(low)         AS low,
  last(close, ts)  AS close,
  sum(volume)      AS volume
FROM fx_m1
GROUP BY symbol, bucket;

-- Policies to refresh recent windows
SELECT add_continuous_aggregate_policy('fx_m5',
  start_offset => INTERVAL '7 days',
  end_offset   => INTERVAL '1 minute',
  schedule_interval => INTERVAL '5 minutes');

SELECT add_continuous_aggregate_policy('fx_m15',
  start_offset => INTERVAL '30 days',
  end_offset   => INTERVAL '5 minutes',
  schedule_interval => INTERVAL '15 minutes');

SELECT add_continuous_aggregate_policy('fx_h1',
  start_offset => INTERVAL '90 days',
  end_offset   => INTERVAL '15 minutes',
  schedule_interval => INTERVAL '1 hour');