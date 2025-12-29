
-- v1.4: QC'd *materialized* per-session views + extra TFs (H4, D1)

-- Additional continuous aggregates: H4 and D1 per session
CREATE MATERIALIZED VIEW IF NOT EXISTS fx_session_h4
WITH (timescaledb.continuous) AS
SELECT
  f.symbol,
  s.session_name,
  time_bucket('4 hours', f.ts) AS bucket,
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

SELECT add_continuous_aggregate_policy('fx_session_h4',
  start_offset => INTERVAL '360 days',
  end_offset   => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour');

CREATE MATERIALIZED VIEW IF NOT EXISTS fx_session_d1
WITH (timescaledb.continuous) AS
SELECT
  f.symbol,
  s.session_name,
  time_bucket('1 day', f.ts) AS bucket,
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

SELECT add_continuous_aggregate_policy('fx_session_d1',
  start_offset => INTERVAL '1825 days',  -- ~5 years
  end_offset   => INTERVAL '1 day',
  schedule_interval => INTERVAL '1 day');


-- QC'd per-session *materialized* views (not continuous, explicit refresh)
-- Use the already-built fx_session_m5/m15/h1 CAGGs and hour-level QC from fx_session_h1 + fx_sessions.

CREATE OR REPLACE FUNCTION refresh_fx_session_qc_materialized()
RETURNS void LANGUAGE plpgsql AS
$$
BEGIN
  -- Create mats if missing (idempotent CREATEs)
  CREATE MATERIALIZED VIEW IF NOT EXISTS fx_session_m5_qc_mat AS
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
  WHERE q.ok
  WITH NO DATA;

  CREATE MATERIALIZED VIEW IF NOT EXISTS fx_session_m15_qc_mat AS
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
  SELECT m15.*
  FROM fx_session_m15 m15
  JOIN hour_qc q
    ON q.symbol = m15.symbol
   AND q.session_name = m15.session_name
   AND time_bucket('1 hour', m15.bucket) = q.hour_bucket
  WHERE q.ok
  WITH NO DATA;

  CREATE MATERIALIZED VIEW IF NOT EXISTS fx_session_h1_qc_mat AS
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
  SELECT h1.*
  FROM fx_session_h1 h1
  JOIN hour_qc q
    ON q.symbol = h1.symbol
   AND q.session_name = h1.session_name
   AND h1.bucket = q.hour_bucket
  WHERE q.ok
  WITH NO DATA;

  -- Create indexes if not present
  PERFORM 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='public' AND c.relname='fx_session_m5_qc_mat_idx';
  IF NOT FOUND THEN
    CREATE INDEX fx_session_m5_qc_mat_idx ON fx_session_m5_qc_mat(symbol, session_name, bucket DESC);
  END IF;

  PERFORM 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='public' AND c.relname='fx_session_m15_qc_mat_idx';
  IF NOT FOUND THEN
    CREATE INDEX fx_session_m15_qc_mat_idx ON fx_session_m15_qc_mat(symbol, session_name, bucket DESC);
  END IF;

  PERFORM 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='public' AND c.relname='fx_session_h1_qc_mat_idx';
  IF NOT FOUND THEN
    CREATE INDEX fx_session_h1_qc_mat_idx ON fx_session_h1_qc_mat(symbol, session_name, bucket DESC);
  END IF;

  -- Refresh concurrently for minimal lock time
  REFRESH MATERIALIZED VIEW CONCURRENTLY fx_session_m5_qc_mat;
  REFRESH MATERIALIZED VIEW CONCURRENTLY fx_session_m15_qc_mat;
  REFRESH MATERIALIZED VIEW CONCURRENTLY fx_session_h1_qc_mat;
END;
$$;

-- Kick an initial build (non-concurrent acceptable on first run)
SELECT refresh_fx_session_qc_materialized();
