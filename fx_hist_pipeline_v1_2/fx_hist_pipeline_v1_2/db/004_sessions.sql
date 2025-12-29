
-- Session windows, gap quality control, and views

-- Per-symbol (or wildcard) session definitions
DROP TABLE IF EXISTS fx_sessions CASCADE;
CREATE TABLE fx_sessions (
  id serial PRIMARY KEY,
  symbol text NULL,              -- NULL or '*' means applies to all symbols
  session_name text NOT NULL,    -- e.g., 'LDN', 'NY'
  tz text NOT NULL,              -- IANA zone, e.g., 'Europe/London'
  start_local time NOT NULL,     -- local session start (inclusive)
  end_local   time NOT NULL,     -- local session end (inclusive)
  enabled boolean NOT NULL DEFAULT true,
  -- Quality thresholds:
  -- Require bars >= max( ceil(expected * min_fill_ratio), min_bars_abs )
  min_fill_ratio numeric NOT NULL DEFAULT 0.97,
  min_bars_abs   int     NOT NULL DEFAULT 0
);

-- Example defaults (edit to taste). These apply to all symbols due to symbol='*'.
INSERT INTO fx_sessions (symbol, session_name, tz, start_local, end_local, min_fill_ratio, min_bars_abs)
VALUES
  ('*','LDN','Europe/London','07:00','17:00', 0.97, 0),
  ('*','NY' ,'America/New_York','08:00','17:00', 0.97, 0);

-- Helper: expected minutes within a given UTC hour that fall inside the session window.
-- Uses DST-aware conversion via time zone name.
DROP FUNCTION IF EXISTS expected_minutes_in_session_hour(timestamptz, text, time, time);
CREATE OR REPLACE FUNCTION expected_minutes_in_session_hour(hour_start timestamptz, tzname text, start_local time, end_local time)
RETURNS int
LANGUAGE sql STABLE AS
$$
WITH mins AS (
  SELECT generate_series(0,59) AS m
), local_minutes AS (
  SELECT ((timezone(tzname, hour_start + make_interval(mins => m)))::time) AS lt
  FROM mins
)
SELECT COUNT(*)::int
FROM local_minutes
WHERE CASE
  WHEN end_local >= start_local
    THEN lt BETWEEN start_local AND end_local
  ELSE  -- crosses midnight
    (lt >= start_local OR lt <= end_local)
END;
$$;

-- View: minute-level rows filtered to be in any enabled session.
DROP VIEW IF EXISTS fx_m1_sessioned CASCADE;
CREATE VIEW fx_m1_sessioned AS
SELECT f.symbol, f.ts, f.open, f.high, f.low, f.close, f.volume, f.source,
       s.session_name
FROM fx_m1 f
JOIN fx_sessions s
  ON s.enabled
 AND (s.symbol IS NULL OR s.symbol = '*' OR s.symbol = f.symbol)
WHERE
  CASE
    WHEN s.end_local >= s.start_local
      THEN (timezone(s.tz, f.ts))::time BETWEEN s.start_local AND s.end_local
    ELSE
      -- window crossing midnight
      ((timezone(s.tz, f.ts))::time >= s.start_local OR (timezone(s.tz, f.ts))::time <= s.end_local)
  END;

-- View: per-hour coverage and OK flag, per session & symbol.
DROP VIEW IF EXISTS fx_session_hour_quality CASCADE;
CREATE VIEW fx_session_hour_quality AS
WITH base AS (
  SELECT
    f.symbol,
    s.session_name,
    time_bucket('1 hour', f.ts) AS bucket,
    COUNT(*)::int AS bars,
    expected_minutes_in_session_hour(time_bucket('1 hour', f.ts), s.tz, s.start_local, s.end_local) AS expected,
    s.min_fill_ratio,
    s.min_bars_abs
  FROM fx_m1 f
  JOIN fx_sessions s
    ON s.enabled
   AND (s.symbol IS NULL OR s.symbol = '*' OR s.symbol = f.symbol)
  WHERE
    CASE
      WHEN s.end_local >= s.start_local
        THEN (timezone(s.tz, f.ts))::time BETWEEN s.start_local AND s.end_local
      ELSE
        ((timezone(s.tz, f.ts))::time >= s.start_local OR (timezone(s.tz, f.ts))::time <= s.end_local)
    END
  GROUP BY 1,2,3,5,6,7
)
SELECT *,
       GREATEST(CEIL(expected * min_fill_ratio)::int, min_bars_abs) AS threshold,
       (bars >= GREATEST(CEIL(expected * min_fill_ratio)::int, min_bars_abs)) AS ok
FROM base;

-- View: minute-level rows that belong to hours passing quality control.
DROP VIEW IF EXISTS fx_m1_sessioned_qc CASCADE;
CREATE VIEW fx_m1_sessioned_qc AS
SELECT f.*
FROM fx_m1_sessioned f
JOIN fx_session_hour_quality q
  ON q.symbol = f.symbol
 AND q.session_name = f.session_name
 AND time_bucket('1 hour', f.ts) = q.bucket
WHERE q.ok;

-- Convenience view: last close per symbol/session with gapfill+LOCF (example 5-minute buckets).
DROP VIEW IF EXISTS fx_session_m5_gapfill CASCADE;
CREATE VIEW fx_session_m5_gapfill AS
SELECT symbol,
       session_name,
       time_bucket_gapfill('5 minutes', ts) AS bucket,
       locf(last(close, ts)) AS close_ffill
FROM fx_m1_sessioned_qc
GROUP BY symbol, session_name, bucket;
