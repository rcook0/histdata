-- Enable extension (safe if already enabled)
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Base table
CREATE TABLE IF NOT EXISTS fx_m1 (
  symbol  text        NOT NULL,
  ts      timestamptz NOT NULL,
  open    double precision NOT NULL,
  high    double precision NOT NULL,
  low     double precision NOT NULL,
  close   double precision NOT NULL,
  volume  double precision NOT NULL DEFAULT 0,
  source  text        NOT NULL,
  PRIMARY KEY (symbol, ts)
);

-- Hypertable on time with space partitioning by symbol
SELECT create_hypertable('fx_m1', 'ts',
                         partitioning_column => 'symbol',
                         number_partitions  => 8,
                         chunk_time_interval => INTERVAL '7 days',
                         if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS fx_m1_ts_idx ON fx_m1 (ts);
CREATE INDEX IF NOT EXISTS fx_m1_symbol_ts_desc ON fx_m1 (symbol, ts DESC);