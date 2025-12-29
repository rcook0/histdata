-- Compression & retention policies
ALTER TABLE fx_m1 SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'symbol'
);

-- compress after 14 days
SELECT add_compression_policy('fx_m1', INTERVAL '14 days');

-- retain 5 years
SELECT add_retention_policy('fx_m1', INTERVAL '5 years');