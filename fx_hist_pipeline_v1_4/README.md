# FX Hist Pipeline v1.4

Deliverables in this cut:
- **QC’d per-session materialized views** (not just views; refreshable): `fx_session_m5_qc_mat`, `fx_session_m15_qc_mat`, `fx_session_h1_qc_mat`.
- **Additional TFs**: per-session **H4** and **D1** continuous aggregates.
- **Refresh jobs**: bash helper + example cron line to refresh QC mats and CAGGs nightly (or on your cadence).

> We keep the existing session-aware continuous aggregates (`fx_session_m5/m15/h1`) and the fast QC view (`fx_session_m5_qc`) for real-time reads. The new *_qc_mat are persisted snapshots you can index and join cheaply for heavy backtests.

## Apply schema
```bash
psql -d fx -f ./db/001_schema.sql
psql -d fx -f ./db/002_policies.sql
psql -d fx -f ./db/003_continuous_aggs.sql
psql -d fx -f ./db/004_sessions.sql
psql -d fx -f ./db/005_session_caggs.sql
psql -d fx -f ./db/006_session_qc_matviews.sql
```

## Refresh materialized views (manual)
```bash
psql -d fx -c "SELECT refresh_fx_session_qc_materialized();"
```

## Cron (example, daily at 02:30)
```cron
30 2 * * * psql -d fx -c "SELECT refresh_fx_session_qc_materialized();"
```

Or use the helper:
```bash
bash ./jobs/refresh_qc.sh
```

## Query examples
```sql
-- QC'd, materialized M5
SELECT * FROM fx_session_m5_qc_mat
WHERE symbol='EURUSD' AND session_name='LDN'
  AND bucket >= NOW() - INTERVAL '14 days'
ORDER BY bucket;

-- H4 session rollups
SELECT * FROM fx_session_h4
WHERE symbol='GBPJPY' AND session_name='NY'
ORDER BY bucket DESC LIMIT 100;

-- D1 session rollups (helpful for swing filters)
SELECT * FROM fx_session_d1
WHERE symbol='XAUUSD'
ORDER BY bucket DESC LIMIT 90;
```