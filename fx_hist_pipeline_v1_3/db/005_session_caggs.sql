-- Per-session continuous aggregates and QC'd per-session view

-- M5 per-session cagg
CREATE MATERIALIZED VIEW IF NOT EXISTS fx_session_m5
WITH (timescaledb.continuous) AS
SELECT
  f.symbol,
  s.session_name,
  time_bucket('5 minutes', f.ts) AS bucket,
  first(f.open, f.ts)  AS open,
  max(f.high)          AS high,
  min(f.low)           AS low,
  last(f.close, f.ts)  AS close,
  sum(f.volume)        AS volume
FROM fx_m1 f
JOIN fx_sessions s
  ON s.enabled
 AND (s.symbol IS NULL OR s.symbol='*' OR s.symbol = f.symbol)
WHERE
  CASE
    WHEN s.end_local >= s.start_local
      THEN (timezone(s.tz, f.ts))::time BETWEEN s.start_local AND s.end_local
    ELSE
      ((timezone(s.tz, f.ts))::time >= s.start_local OR (timezone(s.tz, f.ts))::time <= s.end_local)
  END
GROUP BY f.symbol, s.session_name, bucket;

-- M15 per-session cagg
CREATE MATERIALIZED VIEW IF NOT EXISTS fx_session_m15
WITH (timescaledb.continuous) AS
SELECT
  f.symbol,
  s.session_name,
  time_bucket('15 minutes', f.ts) AS bucket,
  first(f.open, f.ts)  AS open,
  max(f.high)          AS high,
  min(f.low)           AS low,
  last(f.close, f.ts)  AS close,
  sum(f.volume)        AS volume
FROM fx_m1 f
JOIN fx_sessions s
  ON s.enabled
 AND (s.symbol IS NULL OR s.symbol='*' OR s.symbol = f.symbol)
WHERE
  CASE
    WHEN s.end_local >= s.start_local
      THEN (timezone(s.tz, f.ts))::time BETWEEN s.start_local AND s.end_local
    ELSE
      ((timezone(s.tz, f.ts))::time >= s.start_local OR (timezone(s.tz, f.ts))::time <= s.end_local)
  END
GROUP BY f.symbol, s.session_name, bucket;

-- H1 per-session cagg (also stores bar counts & expected minutes per hour)
CREATE MATERIALIZED VIEW IF NOT EXISTS fx_session_h1
WITH (timescaledb.continuous) AS
SELECT
  f.symbol,
  s.session_name,
  time_bucket('1 hour', f.ts) AS bucket,
  first(f.open, f.ts)  AS open,
  max(f.high)          AS high,
  min(f.low)           AS low,
  last(f.close, f.ts)  AS close,
  sum(f.volume)        AS volume,
  COUNT(*)::int        AS bars,
  expected_minutes_in_session_hour(time_bucket('1 hour', f.ts), s.tz, s.start_local, s.end_local) AS expected
FROM fx_m1 f
JOIN fx_sessions s
  ON s.enabled
 AND (s.symbol IS NULL OR s.symbol='*' OR s.symbol = f.symbol)
WHERE
  CASE
    WHEN s.end_local >= s.start_local
      THEN (timezone(s.tz, f.ts))::time BETWEEN s.start_local AND s.end_local
    ELSE
      ((timezone(s.tz, f.ts))::time >= s.start_local OR (timezone(s.tz, f.ts))::time <= s.end_local)
  END
GROUP BY f.symbol, s.session_name, bucket, expected;

-- Policy refreshes
SELECT add_continuous_aggregate_policy('fx_session_m5',
  start_offset => INTERVAL '30 days',
  end_offset   => INTERVAL '5 minutes',
  schedule_interval => INTERVAL '5 minutes');

SELECT add_continuous_aggregate_policy('fx_session_m15',
  start_offset => INTERVAL '60 days',
  end_offset   => INTERVAL '5 minutes',
  schedule_interval => INTERVAL '15 minutes');

SELECT add_continuous_aggregate_policy('fx_session_h1',
  start_offset => INTERVAL '180 days',
  end_offset   => INTERVAL '15 minutes',
  schedule_interval => INTERVAL '1 hour');

-- QC'd M5 view by joining to hourly counts and thresholds from fx_sessions
DROP VIEW IF EXISTS fx_session_m5_qc CASCADE;
CREATE VIEW fx_session_m5_qc AS
WITH hour_qc AS (
  SELECT h.symbol, h.session_name, h.bucket AS hour_bucket,
         h.bars,
         h.expected,
         GREATEST(CEIL(h.expected * s.min_fill_ratio)::int, s.min_bars_abs) AS threshold,
         (h.bars >= GREATEST(CEIL(h.expected * s.min_fill_ratio)::int, s.min_bars_abs)) AS ok
  FROM fx_session_h1 h
  JOIN fx_sessions s
    ON s.enabled
   AND (s.symbol IS NULL OR s.symbol='*' OR s.symbol = h.symbol)
   AND s.session_name = h.session_name
)
SELECT m5.*
FROM fx_session_m5 m5
JOIN hour_qc q
  ON q.symbol = m5.symbol
 AND q.session_name = m5.session_name
 AND time_bucket('1 hour', m5.bucket) = q.hour_bucket
WHERE q.ok;