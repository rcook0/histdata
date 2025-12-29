# FX Hist Pipeline v1.3

Scope uplift:
- **Per-session Continuous Aggregates** (TimescaleDB): M5, M15, H1 per session + a QC'd M5 view.
- **YAML-driven Multi-Symbol Runner** on the client: end-to-end ingest with optional **session prefilter + hour-level QC** before loading to Postgres (reduces storage and noise).

---

## Fast start (native Postgres/Timescale)

1) Apply schema files in order:
```bash
psql -d fx -f ./db/001_schema.sql
psql -d fx -f ./db/002_policies.sql
psql -d fx -f ./db/003_continuous_aggs.sql
psql -d fx -f ./db/004_sessions.sql
psql -d fx -f ./db/005_session_caggs.sql
```

2) Configure sessions (edit `INSERT`s in 004) and verify:
```sql
SELECT * FROM fx_sessions;
```

3) Run the multi-symbol pipeline from YAML:
```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

python pipeline.py run --config ./config.example.v13.yaml
```

- Client prefilter/QC is optional per symbol. If disabled, load raw UTC bars and do session/QC downstream via the provided views/CAGGs.
- If enabled, we drop non-session minutes and hours failing QC **before** insertion.

4) Query per-session aggregates (QC'd M5 example):
```sql
SELECT *
FROM fx_session_m5_qc
WHERE symbol='EURUSD' AND session_name='LDN'
  AND bucket >= NOW() - INTERVAL '14 days'
ORDER BY bucket;
```

---

## YAML (v1.3) keys

```yaml
postgres:
  dsn: postgresql://user:pass@localhost:5432/fx
  table: fx_m1

symbols:
  EURUSD:
    input_globs: ["./data/EURUSD/*.csv"]
    impute_max: 5            # tiny-gap client imputation (min)
    dukascopy_max: 60        # attempt Dukas backfill up to N minutes
    prefilter:
      enabled: true          # client-side session/QC filter before load
      sessions:
        - { name: LDN, tz: Europe/London,    start: "07:00", end: "17:00" }
        - { name: NY,  tz: America/New_York, start: "08:00", end: "17:00" }
      qc:
        min_fill_ratio: 0.97 # bars must be >= ceil(expected*ratio)
        min_bars_abs: 58     # or at least N bars when expected ~60
    out_csv: "./work/EURUSD_v13.csv"

  GBPJPY:
    input_globs: ["./data/GBPJPY/*.csv"]
    impute_max: 5
    dukascopy_max: 60
    prefilter:
      enabled: false         # store full minute grid; filter later in SQL
    out_csv: "./work/GBPJPY_v13.csv"
```

**Tip:** If your Docker is down, run native Postgres/Timescale (see `README_TIMESCALE.md`). When Docker is healthy, `docker-compose.yml` stands up Timescale + Adminer; put SQL files in `db/init` to auto-apply.