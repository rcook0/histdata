# TimescaleDB quick homework (cheat sheet)

## Concepts you’ll actually use
- **Hypertables**: Postgres tables partitioned by time (and optionally by symbol). You still `SELECT` like normal SQL.
- **Chunk interval**: How big each time partition is. For FX M1, start with 7 days; adjust by ingest volume.
- **Space partition**: Additional partition on `symbol`. Great when you have many pairs. e.g., 8 partitions.
- **Compression**: Columnar storage on older chunks. Big savings; add a policy to compress after N days.
- **Retention**: Auto-drop data older than N years (or archive elsewhere first).
- **Continuous aggregates**: Materialized rollups that stay fresh (M5, M15, H1). Perfect for dashboards/backtests.

## Native install (Ubuntu/Debian, no Docker)
1) Install PostgreSQL and TimescaleDB packages (use your distro’s version numbers):
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib timescaledb-postgresql-16
# or timescaledb-postgresql-15 depending on your PG
```
2) Enable the extension in your DB:
```bash
sudo -u postgres createdb fx
sudo -u postgres psql -d fx -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"
```
3) Apply schema & policies from this repo:
```bash
psql -d fx -f ./db/001_schema.sql
psql -d fx -f ./db/002_policies.sql
psql -d fx -f ./db/003_continuous_aggs.sql
psql -d fx -f ./db/004_sessions.sql
psql -d fx -f ./db/005_session_caggs.sql
```

## Schema choices that matter
- Primary key on `(symbol, ts)` keeps writes idempotent.
- Hypertable on `ts`, **space partition on symbol** to parallelize and keep chunks smaller.
- Indexes: `(ts)` (already), `(symbol, ts DESC)` helps last-N queries per symbol.

## Policies you probably want
```sql
-- Compress after 14 days (adjust to your cadence)
SELECT add_compression_policy('fx_m1', INTERVAL '14 days');

-- Keep 5 years of M1; adjust as needed
SELECT add_retention_policy('fx_m1', INTERVAL '5 years');

-- Reorder by time/symbol to improve compression
SELECT reorder_chunk(i, 'fx_m1', order_by => 'symbol, ts')
FROM show_chunks('fx_m1') i;
```
TimescaleDB also supports `add_reorder_policy('fx_m1', 'symbol, ts');` on newer versions.

## Continuous aggregates
- M5/M15/H1 rollups speed up backtests and charts.
- Keep symbol and `time_bucket` as grouping keys; compute OHLC using Timescale’s `first/last` helpers.

## Session windows, DST & queries
Define “London”/“New York” by local hours (DST-aware) and filter in SQL:

```sql
-- London session example (08:00–17:00 Europe/London)
SELECT *
FROM fx_m1
WHERE ( (ts AT TIME ZONE 'Europe/London')::time >= TIME '08:00'
    AND (ts AT TIME ZONE 'Europe/London')::time <= TIME '17:00' )
  AND ts >= NOW() - INTERVAL '30 days'
  AND symbol = 'EURUSD';
```

Gap-aware backtests (skip windows with missing bars):
```sql
WITH hours AS (
  SELECT symbol,
         time_bucket('1 hour', ts) AS bucket,
         COUNT(*) AS bars
  FROM fx_m1
  WHERE symbol = 'EURUSD'
  GROUP BY 1,2
)
SELECT *
FROM hours
WHERE bars >= 58
ORDER BY bucket DESC;
```

Plot-friendly queries with gapfilling:
```sql
SELECT time_bucket_gapfill('5 minutes', ts) AS bucket,
       LOCF(last(close, ts)) AS close_ffill
FROM fx_m1
WHERE symbol='EURUSD'
  AND ts >= NOW() - INTERVAL '7 days'
GROUP BY bucket
ORDER BY bucket;
```

## Dockerd won’t start? Options
- **Skip Docker** (recommended now): native PostgreSQL + TimescaleDB (above).
- **Podman** as a drop-in: `sudo apt install podman` → alias `docker=podman`.
- **Lightweight VM**: run Docker inside Lima/Multipass if you must, but usually native PG is simpler.

Minimal triage if you want Docker anyway:
```bash
sudo systemctl status docker.service
journalctl -u docker --no-pager -n 200
getent group docker    # ensure your user is in 'docker' group (then re-login)
lsmod | grep overlay   # overlayfs present?
```