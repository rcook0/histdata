# FX Hist Pipeline v1.1
Adds **session windows** tooling and **TimescaleDB** helpers. Includes both **Docker Compose** and **native apt** setup.

### What’s new vs v1.0
- `sessions` subcommand: mark or filter London/NY (or any) sessions with correct DST handling via IANA time zones.
- `db/` with Timescale SQL: hypertable, compression, retention, and continuous aggregates (M5/M15/H1).
- `docker-compose.yml` for TimescaleDB + Adminer (optional). If Docker is broken, use the native install notes in `README_TIMESCALE.md`.

---

## Quick path (no Docker, native Postgres + TimescaleDB)
See **README_TIMESCALE.md** for apt-based setup. Then:

```bash
# Create database + extension + hypertable + policies
psql -d fx -f ./db/001_schema.sql
psql -d fx -f ./db/002_policies.sql
psql -d fx -f ./db/003_continuous_aggs.sql

# Load data as before
python pipeline.py to-pg --symbol EURUSD --csv ./work/EURUSD_backfilled.csv --dsn postgresql://user:pass@localhost:5432/fx --table fx_m1
```

## Session filtering
```bash
# keep only London+NY minutes (example windows are in README_TIMESCALE.md)
python pipeline.py sessions   --csv ./work/EURUSD_backfilled.csv   --out ./work/EURUSD_sessions_only.csv   --name LDN --tz Europe/London --start 07:00 --end 17:00   --name NY  --tz America/New_York --start 08:00 --end 17:00   --filter
```

## Docker path (if/when Docker works)
```bash
docker compose up -d
# then run the same psql -f files or put them in ./db/init to auto-run
```