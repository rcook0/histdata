
# FX Hist Pipeline v1.2
Adds **server-side (Timescale/Postgres) session windows**, **gap quality control**, and **ready-to-use views**.

## New in v1.2
- `db/004_sessions.sql` creates:
  - `fx_sessions` (per-symbol or wildcard session definitions with TZ + local start/end + quality thresholds)
  - helper function `expected_minutes_in_session_hour(...)`
  - views: `fx_m1_sessioned`, `fx_session_hour_quality`, `fx_m1_sessioned_qc`
- Use these in your backtests to query only DST-aware session minutes that pass a coverage threshold.

### Apply schema (after 001..003)
```bash
psql -d fx -f ./db/004_sessions.sql
```

### Typical backtest query (QC'd, London+NY defined via fx_sessions)
```sql
SELECT *
FROM fx_m1_sessioned_qc
WHERE symbol='EURUSD'
  AND ts >= '2024-01-01'
ORDER BY ts;
```

### Inspect hour coverage
```sql
SELECT *
FROM fx_session_hour_quality
WHERE symbol='EURUSD' AND session_name='LDN'
ORDER BY bucket DESC
LIMIT 50;
```

Edit `INSERT` rows in `fx_sessions` to match your symbols/windows.
