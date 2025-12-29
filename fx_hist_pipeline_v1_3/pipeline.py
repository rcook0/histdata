
from __future__ import annotations
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
import io, sys, time, warnings

import numpy as np
import pandas as pd
import requests, lzma
from dateutil import tz
from tqdm import tqdm

# ------------ HistData I/O -------------

def read_histdata_csv(path: Path) -> pd.DataFrame:
    sample = path.read_text(encoding="utf-8", errors="ignore").splitlines()[:5]
    delim = ';' if any(';' in s for s in sample) else ','
    cols = ["dt", "open", "high", "low", "close", "volume"]
    df = pd.read_csv(path, header=None, names=cols, sep=delim, dtype={"dt": str})
    df["dt"] = pd.to_datetime(df["dt"], format="%Y%m%d %H%M%S", utc=False)
    df = df.set_index("dt")
    df.index = df.index.tz_localize("Etc/GMT+5").tz_convert("UTC")
    for c in ["open","high","low","close","volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def load_histdata_many(paths):
    dfs = [read_histdata_csv(Path(p)) for p in paths]
    if not dfs:
        return pd.DataFrame(columns=["open","high","low","close","volume"]).set_index(pd.DatetimeIndex([], tz="UTC"))
    df = pd.concat(dfs).sort_index()
    df = df[~df.index.duplicated(keep="last")]
    return df

def minute_grid(df: pd.DataFrame) -> pd.DatetimeIndex:
    if df.empty:
        return pd.DatetimeIndex([], tz="UTC")
    full = pd.date_range(df.index.min(), df.index.max(), freq="T", tz="UTC")
    full = full[~full.weekday.isin([5,6])]
    return full

def gap_report(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["start","len_min"])
    full = minute_grid(df)
    missing = full.difference(df.index)
    if len(missing)==0:
        return pd.DataFrame(columns=["start","len_min"])
    gaps = []
    run_start = missing[0]
    prev = missing[0]
    for ts in missing[1:]:
        if (ts - prev).total_seconds() != 60:
            gaps.append((run_start, int((prev - run_start).total_seconds()/60) + 1))
            run_start = ts
        prev = ts
    gaps.append((run_start, int((prev - run_start).total_seconds()/60) + 1))
    return pd.DataFrame(gaps, columns=["start","len_min"]).sort_values("start").reset_index(drop=True)

def impute_small_gaps(df: pd.DataFrame, max_gap_min: int) -> pd.DataFrame:
    if df.empty:
        return df
    full = minute_grid(df)
    out = df.reindex(full)
    out["source"] = out.get("source", pd.Series(index=out.index, dtype="object"))
    gaps = gap_report(df.dropna(subset=["close"]))
    small = gaps[gaps["len_min"] <= max_gap_min]
    mask = pd.Series(False, index=out.index)
    for _, r in small.iterrows():
        rng = pd.date_range(r["start"], periods=r["len_min"], freq="T", tz="UTC")
        mask.loc[rng] = True
    out.loc[mask, "close"] = out["close"].ffill().loc[mask]
    for c in ["open","high","low"]:
        out.loc[mask, c] = out["close"].loc[mask]
    out.loc[mask, "volume"] = 0
    out.loc[mask, "source"] = "imputed"
    return out

# ------------ Dukas backfill -------------

def dukascopy_tick_url(symbol: str, dt: datetime) -> str:
    y = dt.year; m = dt.month - 1; d = dt.day; h = dt.hour
    return f"https://datafeed.dukascopy.com/datafeed/{symbol}/{y:04d}/{m:02d}/{d:02d}/{h:02d}h_ticks.bi5"

def fetch_bi5(symbol: str, dt: datetime, timeout=20, retries=2) -> bytes | None:
    url = dukascopy_tick_url(symbol, dt)
    for _ in range(retries+1):
        try:
            r = requests.get(url, timeout=timeout)
            if r.status_code == 200 and r.content:
                return r.content
            time.sleep(0.4)
        except requests.RequestException:
            time.sleep(0.8)
    return None

def parse_bi5_to_ticks(raw: bytes, base_dt: datetime) -> pd.DataFrame:
    if not raw:
        return pd.DataFrame(columns=["ts","bid","ask","ask_vol","bid_vol"]).set_index("ts")
    try:
        data = lzma.decompress(raw)
    except lzma.LZMAError:
        return pd.DataFrame(columns=["ts","bid","ask","ask_vol","bid_vol"]).set_index("ts")
    if len(data) % (4*5) != 0:
        return pd.DataFrame(columns=["ts","bid","ask","ask_vol","bid_vol"]).set_index("ts")
    arr = np.frombuffer(data, dtype=">i4").reshape(-1,5)
    ms = arr[:,0].astype(np.int64)
    ask = arr[:,1].astype(np.float64) / 1e5
    bid = arr[:,2].astype(np.float64) / 1e5
    av  = arr[:,3].astype(np.float64)
    bv  = arr[:,4].astype(np.float64)
    ts = pd.to_datetime(base_dt.replace(minute=0, second=0, microsecond=0) + pd.to_timedelta(ms, unit="ms"), utc=True)
    df = pd.DataFrame({"bid": bid, "ask": ask, "ask_vol": av, "bid_vol": bv}, index=ts)
    df.index.name = "ts"
    return df

def dukascopy_m1(symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
    if start.tzinfo is None: start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:   end   = end.replace(tzinfo=timezone.utc)
    hours = int((end - start).total_seconds() // 3600) + 1
    dfs = []
    for i in tqdm(range(hours), desc=f"Dukas {symbol}", unit="h"):
        hdt = (start + timedelta(hours=i)).replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        raw = fetch_bi5(symbol, hdt)
        if not raw: continue
        ticks = parse_bi5_to_ticks(raw, hdt)
        if ticks.empty: continue
        m1 = pd.DataFrame({
            "open": ticks["bid"].resample("T").first(),
            "high": ticks["bid"].resample("T").max(),
            "low" : ticks["bid"].resample("T").min(),
            "close":ticks["bid"].resample("T").last(),
            "volume": ticks["bid_vol"].resample("T").sum(min_count=1)
        })
        dfs.append(m1.dropna(subset=["open","high","low","close"]))
    if not dfs:
        return pd.DataFrame(columns=["open","high","low","close","volume"]).set_index(pd.DatetimeIndex([], tz="UTC"))
    out = pd.concat(dfs).sort_index()
    out = out[~out.index.duplicated(keep="last")]
    return out

def apply_dukascopy_backfill(df: pd.DataFrame, symbol: str, max_gap_min: int) -> pd.DataFrame:
    if df.empty: return df
    full = minute_grid(df)
    out = df.reindex(full)
    out["source"] = out.get("source", pd.Series(index=out.index, dtype="object"))
    missing = out[out["close"].isna()].index
    if len(missing) == 0: return out
    groups = []
    run_start = None
    prev = None
    for ts in missing:
        if run_start is None:
            run_start = ts; prev = ts; continue
        if (ts - prev).total_seconds() != 60:
            groups.append((run_start, prev)); run_start = ts
        prev = ts
    if run_start is not None: groups.append((run_start, prev))
    for (gs, ge) in groups:
        nmin = int((ge - gs).total_seconds()/60) + 1
        if nmin > max_gap_min: continue
        start = (gs - timedelta(minutes=2)).to_pydatetime().astimezone(timezone.utc)
        end   = (ge + timedelta(minutes=2)).to_pydatetime().astimezone(timezone.utc) + timedelta(minutes=1)
        try:
            m1 = dukascopy_m1(symbol, start, end)
        except Exception:
            continue
        if m1.empty: continue
        gap_index = pd.date_range(gs, ge, freq="T", tz="UTC")
        m1_slice = m1.reindex(gap_index).dropna(subset=["open","high","low","close"])
        if not m1_slice.empty:
            out.loc[m1_slice.index, ["open","high","low","close","volume"]] = m1_slice[["open","high","low","close","volume"]]
            out.loc[m1_slice.index, "source"] = "dukascopy"
    return out

# ------------ Sessions & QC (client-side) -------------

def compute_session_mask(index_utc: pd.DatetimeIndex, session_specs):
    mask = pd.Series(False, index=index_utc)
    for s in session_specs:
        tzinfo = tz.gettz(s["tz"])
        start_h, start_m = map(int, s["start"].split(":"))
        end_h, end_m = map(int, s["end"].split(":"))
        local = index_utc.tz_convert(tzinfo)
        lt = (local.hour * 60 + local.minute)
        start_min = start_h*60 + start_m
        end_min = end_h*60 + end_m
        if end_min >= start_min:
            m = (lt >= start_min) & (lt <= end_min)
        else:
            m = (lt >= start_min) | (lt <= end_min)
        mask |= m
    return mask

def expected_minutes_by_hour(index_utc: pd.DatetimeIndex, session_specs):
    if len(index_utc) == 0:
        return pd.Series(dtype="int", index=pd.DatetimeIndex([], tz="UTC"))
    full = pd.date_range(index_utc.min(), index_utc.max(), freq="T", tz="UTC")
    mask = compute_session_mask(full, session_specs)
    hour = full.floor("H")
    expected = mask.groupby(hour).sum().astype(int)
    return expected

def prefilter_sessions_qc(df: pd.DataFrame, session_specs, min_fill_ratio: float, min_bars_abs: int):
    if df.empty: return df
    full = minute_grid(df)
    df = df.reindex(full)
    in_sess = compute_session_mask(df.index, session_specs)
    df["__in_session"] = in_sess.values
    hour = df.index.floor("H")
    expected = expected_minutes_by_hour(df.index, session_specs)
    observed = (~df["close"].isna()) & df["__in_session"]
    observed_counts = observed.groupby(hour).sum().astype(int)
    aligned = expected.to_frame("expected").join(observed_counts.to_frame("bars"), how="left").fillna(0)
    aligned["threshold"] = np.maximum(np.ceil(aligned["expected"]*min_fill_ratio).astype(int), int(min_bars_abs))
    aligned["ok"] = aligned["bars"] >= aligned["threshold"]
    ok_by_hour = aligned["ok"]
    keep = df["__in_session"] & ok_by_hour.reindex(hour).fillna(False).values
    out = df[keep].drop(columns=["__in_session"])
    return out

# ------------ PG COPY -------------

def to_postgres_copy(df: pd.DataFrame, symbol: str, dsn: str, table: str = "fx_m1", chunksize: int = 200_000):
    import psycopg2, io
    need = ["open","high","low","close","volume"]
    for c in need:
        if c not in df.columns:
            raise ValueError(f"Missing column {c}")
    cols = ["symbol","ts","open","high","low","close","volume","source"]
    if "source" not in df.columns:
        df["source"] = "histdata"
    def iter_rows(batch):
        for ts, row in batch.iterrows():
            yield "\\t".join([
                symbol,
                ts.isoformat(),
                f"{row['open']:.10f}" if pd.notna(row['open']) else "",
                f"{row['high']:.10f}" if pd.notna(row['high']) else "",
                f"{row['low']:.10f}"  if pd.notna(row['low']) else "",
                f"{row['close']:.10f}"if pd.notna(row['close']) else "",
                f"{row['volume']:.6f}" if pd.notna(row['volume']) else "",
                str(row.get("source","histdata") or "histdata")
            ]) + "\\n"
    import psycopg2
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {table} (
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
    CREATE INDEX IF NOT EXISTS {table}_ts_idx ON {table} (ts);
    """)
    n = len(df); i = 0
    while i < n:
        batch = df.iloc[i:i+chunksize]
        buf = io.StringIO()
        for line in iter_rows(batch):
            buf.write(line)
        buf.seek(0)
        cur.copy_from(buf, table, columns=cols)
        i += len(batch)
    cur.close(); conn.close()

# ------------ CLI -------------

def cmd_histdata_merge(args):
    paths = []
    for g in args.input:
        paths.extend([str(p) for p in Path().glob(g)])
    if not paths:
        print("No files matched.", file=sys.stderr); sys.exit(2)
    df = load_histdata_many(paths)
    df["source"] = "histdata"
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index_label="timestamp")
    print(f"Wrote {args.out} rows={len(df)} range=[{df.index.min()} .. {df.index.max()}]")

def cmd_gaps(args):
    df = pd.read_csv(args.csv, index_col=0, parse_dates=True)
    if df.index.tzinfo is None:
        df.index = df.index.tz_localize("UTC")
    rep = gap_report(df.dropna(subset=["close"]))
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    rep.to_csv(args.out, index=False)
    print(f"Gaps: {len(rep)} written to {args.out}")

def cmd_backfill(args):
    df = pd.read_csv(args.csv, index_col=0, parse_dates=True)
    if df.index.tzinfo is None:
        df.index = df.index.tz_localize("UTC")
    df1 = impute_small_gaps(df, args.impute_max)
    df2 = apply_dukascopy_backfill(df1, args.symbol, args.dukascopy_max)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df2.to_csv(args.out, index_label="timestamp")
    remaining = df2[df2.isna().any(axis=1)]
    print(f"Backfilled written to {args.out}. Remaining missing bars: {len(remaining)}")

def cmd_sessions(args):
    df = pd.read_csv(args.csv, index_col=0, parse_dates=True)
    if df.index.tzinfo is None:
        df.index = df.index.tz_localize("UTC")
    specs = []
    for name, tzname, start, end in zip(args.name, args.tz, args.start, args.end):
        specs.append({"name": name, "tz": tzname, "start": start, "end": end})
    mask = compute_session_mask(df.index, specs)
    if args.filter:
        out = df[mask].copy()
    else:
        out = df.copy(); out["in_session"] = mask.values
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index_label="timestamp")
    print(f"Wrote {args.out} rows={len(out)} (filtered={args.filter}).")

def cmd_to_pg(args):
    df = pd.read_csv(args.csv, index_col=0, parse_dates=True)
    if df.index.tzinfo is None:
        df.index = df.index.tz_localize("UTC")
    to_postgres_copy(df, args.symbol, args.dsn, table=args.table, chunksize=args.chunksize)

def cmd_run(args):
    import yaml
    cfg = yaml.safe_load(open(args.config, "r"))
    dsn = cfg["postgres"]["dsn"]
    table = cfg["postgres"].get("table", "fx_m1")

    for symbol, sdef in cfg["symbols"].items():
        print(f"\\n=== {symbol} ===")
        globs = sdef["input_globs"]
        paths = []
        for g in globs:
            paths.extend([str(p) for p in Path().glob(g)])
        if not paths:
            print(f"[{symbol}] No files matched {globs}", file=sys.stderr); continue
        df = load_histdata_many(paths); df["source"] = "histdata"
        df = impute_small_gaps(df, int(sdef.get("impute_max", 5)))
        df = apply_dukascopy_backfill(df, symbol, int(sdef.get("dukascopy_max", 60)))

        pf = sdef.get("prefilter", {})
        if pf.get("enabled", False):
            sess_specs = pf.get("sessions", [])
            qc = pf.get("qc", {"min_fill_ratio": 0.97, "min_bars_abs": 0})
            df = prefilter_sessions_qc(df, sess_specs, float(qc.get("min_fill_ratio", 0.97)), int(qc.get("min_bars_abs", 0)))
            df["source"] = df.get("source", "histdata")

        out_csv = sdef.get("out_csv")
        if out_csv:
            Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(out_csv, index_label="timestamp")
            print(f"[{symbol}] wrote {out_csv} rows={len(df)}")

        to_postgres_copy(df, symbol, dsn, table=table, chunksize=args.chunksize)
        print(f"[{symbol}] loaded to {table}")

def build_parser():
    p = argparse.ArgumentParser(description="HistData → Clean → (optional) Dukascopy backfill → Sessions → PostgreSQL")
    sub = p.add_subparsers(dest="cmd", required=True)

    m = sub.add_parser("histdata-merge", help="Merge & clean HistData CSVs to UTC")
    m.add_argument("--symbol", required=True)
    m.add_argument("--input", nargs="+", required=True, help="Glob patterns for HistData CSVs")
    m.add_argument("--out", required=True)
    m.set_defaults(func=cmd_histdata_merge)

    g = sub.add_parser("gaps", help="Report gaps")
    g.add_argument("--csv", required=True)
    g.add_argument("--out", required=True)
    g.set_defaults(func=cmd_gaps)

    b = sub.add_parser("backfill", help="Impute small gaps and try Dukascopy backfill")
    b.add_argument("--symbol", required=True)
    b.add_argument("--csv", required=True)
    b.add_argument("--out", required=True)
    b.add_argument("--impute-max", type=int, default=5)
    b.add_argument("--dukascopy-max", type=int, default=60)
    b.set_defaults(func=cmd_backfill)

    s = sub.add_parser("sessions", help="Mark or filter session windows (DST-aware)")
    s.add_argument("--csv", required=True)
    s.add_argument("--out", required=True)
    s.add_argument("--name", action="append", required=True, help="Session name label (repeatable)")
    s.add_argument("--tz", action="append", required=True, help="IANA timezone (repeatable)")
    s.add_argument("--start", action="append", required=True, help="HH:MM local (repeatable)")
    s.add_argument("--end", action="append", required=True, help="HH:MM local (repeatable)")
    s.add_argument("--filter", action="store_true", help="Keep only in-session minutes")
    s.set_defaults(func=cmd_sessions)

    t = sub.add_parser("to-pg", help="Load CSV to PostgreSQL")
    t.add_argument("--symbol", required=True)
    t.add_argument("--csv", required=True)
    t.add_argument("--dsn", required=True)
    t.add_argument("--table", default="fx_m1")
    t.add_argument("--chunksize", type=int, default=200_000)
    t.set_defaults(func=cmd_to_pg)

    r = sub.add_parser("run", help="Run multi-symbol ingest from YAML (client-side session/QC optional)")
    r.add_argument("--config", required=True)
    r.add_argument("--chunksize", type=int, default=200_000)
    r.set_defaults(func=cmd_run)

    return p

def main(argv=None):
    p = build_parser()
    args = p.parse_args(argv)
    args.func(args)

if __name__ == "__main__":
    main()
