#!/usr/bin/env -S uv run
# /// script
# dependencies = ["psycopg2-binary"]
# ///
"""Fetch FRED macro series (St. Louis Fed) into postgres.

Daily/weekly/monthly observations land in `fred_observations` (long/narrow:
one row per series×date). Mirrors scripts/fetch-netflow.py conventions:
incremental by default (watermark in `fred_fetch_state`), --backfill to
rebuild from a date, --freshness-only for gap reporting.

API: https://api.stlouisfed.org/fred/series/observations
  Free key from https://fred.stlouisfed.org/docs/api/api_key.html
  Rate limit 120 req/min. Missing observations are "." and are skipped.

Default universe is the 12-series macro regime set (rates, credit,
dollar, oil, Fed balance sheet, money, inflation, labor, vol):

  DFF, DTB3, DGS10, T10Y2Y, BAMLH0A0HYM2, DTWEXBGS, DCOILWTICO,
  WALCL, M2SL, CPIAUCSL, UNRATE, VIXCLS

Usage:
  just fred-fetch                          # incremental (from watermark)
  just fred-fetch -- --backfill 2020-01-01 # rebuild from DATE
  just fred-fetch -- --series DFF,T10Y2Y   # subset only
  just fred-fetch -- --freshness-only      # gap/coverage report, no fetch
  just fred-status                         # coverage per series (psql)

Schedule: daily after 18:00 ET (FRED updates EOD). Tiny volume, so a
single run backfills years in seconds.

Auth:
  FRED_API_KEY env var (see exchanges/.env). Also reads from Doppler.
"""

import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone, timedelta
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

PG_URL = os.environ.get("PG_URL", "postgres://postgres:postgres@localhost:5432/twain")
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
FRED_BASE = "https://api.stlouisfed.org"

# Curated regime set. All exist on FRED; verify at fred.stlouisfed.org/series/<ID>
DEFAULT_SERIES = [
    "DFF",            # Effective Federal Funds Rate (daily, %)
    "DTB3",           # 3-Month Treasury Bill (daily, %)
    "DGS10",          # 10-Year Treasury Constant Maturity (daily, %)
    "T10Y2Y",         # 10Y-2Y Treasury Spread (daily, %)
    "BAMLH0A0HYM2",   # ICE BofA US High Yield OAS (daily, %)
    "DTWEXBGS",       # Trade Weighted U.S. Dollar Index, Broad, Goods & Services (daily)
    "DCOILWTICO",     # WTI Crude Oil Price (daily, $/bbl)
    "WALCL",          # Fed Total Assets (weekly, $B, Wed)
    "M2SL",           # M2 Money Stock (monthly, $B, SA)
    "CPIAUCSL",       # CPI All Urban Consumers (monthly, SA)
    "UNRATE",         # Unemployment Rate (monthly, %)
    "VIXCLS",         # VIX Close (daily)
]

DEFAULT_LOOKBACK_DAYS = 365 * 5  # 5 years if no watermark (macro needs history)
RATE_LIMIT_DELAY = 0.15  # 120/min = 0.5s; be polite but fast for 12 series
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0


# ── FRED HTTP ────────────────────────────────────────────────────────────────

def fred_get(path: str, params: dict) -> dict:
    if not FRED_API_KEY:
        print("ERROR: FRED_API_KEY not set (exchanges/.env or env var)", file=sys.stderr)
        sys.exit(1)
    params = {**params, "api_key": FRED_API_KEY, "file_type": "json"}
    url = f"{FRED_BASE}{path}?{urllib.parse.urlencode(params)}"
    delay = RATE_LIMIT_DELAY
    for attempt in range(MAX_RETRIES):
        if attempt > 0:
            time.sleep(delay)
            delay *= RETRY_BACKOFF
        else:
            # small delay even on first try to stay under 120/min across series
            time.sleep(RATE_LIMIT_DELAY)
        req = urllib.request.Request(url, headers={"User-Agent": "mdx/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode()[:500]
            except Exception:
                pass
            if e.code == 429 and attempt < MAX_RETRIES - 1:
                print(f"  429 rate-limited, retrying ({body[:80]})", file=sys.stderr)
                continue
            raise RuntimeError(f"FRED HTTP {e.code} {e.reason} {body} (url {path})") from e
        except urllib.error.URLError as e:
            if attempt < MAX_RETRIES - 1:
                print(f"  URLError {e}, retrying", file=sys.stderr)
                continue
            raise


def fetch_series_info(series_id: str) -> dict:
    data = fred_get("/fred/series", {"series_id": series_id})
    seriess = data.get("seriess", [])
    if not seriess:
        return {}
    return seriess[0]


def fetch_observations(series_id: str, start: datetime | None, end: datetime | None) -> list[tuple[datetime, float]]:
    params: dict[str, str] = {
        "series_id": series_id,
        "sort_order": "asc",
        "limit": "100000",
    }
    if start:
        params["observation_start"] = start.strftime("%Y-%m-%d")
    if end:
        params["observation_end"] = end.strftime("%Y-%m-%d")
    data = fred_get("/fred/series/observations", params)
    obs = data.get("observations", [])
    out: list[tuple[datetime, float]] = []
    for o in obs:
        v = o.get("value", ".")
        if v == "." or v is None or v == "":
            continue
        try:
            val = float(v)
        except ValueError:
            continue
        # FRED dates are calendar dates; store as 00:00 UTC
        d = datetime.strptime(o["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        out.append((d, val))
    return out


# ── postgres ─────────────────────────────────────────────────────────────────

def ensure_series(conn, series_id: str):
    """Ensure fred_series row exists; hydrate metadata from FRED on first seen."""
    with conn.cursor() as cur:
        cur.execute("SELECT title FROM fred_series WHERE series_id = %s", (series_id,))
        row = cur.fetchone()
        if row and row[0]:
            return
        # fetch metadata (also handles empty-title repair from early 404 run)
        try:
            info = fetch_series_info(series_id)
        except Exception as e:
            print(f"  WARN {series_id} series info fetch failed: {e}", file=sys.stderr)
            info = {}
        if not info and row is not None:
            return  # already exists, nothing to repair
        with conn.cursor() as cur2:
            cur2.execute(
                """INSERT INTO fred_series (series_id, title, frequency, units, seasonal_adjustment, notes)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (series_id) DO UPDATE SET
                     title = COALESCE(NULLIF(EXCLUDED.title,''), fred_series.title),
                     frequency = COALESCE(NULLIF(EXCLUDED.frequency,''), fred_series.frequency),
                     units = COALESCE(NULLIF(EXCLUDED.units,''), fred_series.units),
                     seasonal_adjustment = COALESCE(NULLIF(EXCLUDED.seasonal_adjustment,''), fred_series.seasonal_adjustment),
                     notes = COALESCE(NULLIF(EXCLUDED.notes,''), fred_series.notes),
                     updated_at = now()""",
                (
                    series_id,
                    info.get("title", "")[:500] if info else "",
                    info.get("frequency", "") if info else "",
                    info.get("units", "") if info else "",
                    info.get("seasonal_adjustment", "") if info else "",
                    info.get("notes", "")[:2000] if info and info.get("notes") else "",
                ),
            )
    conn.commit()


def upsert_observations(conn, series_id: str, rows: list[tuple[datetime, float]]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO fred_observations (series_id, timestamp, value)
               VALUES %s
               ON CONFLICT (series_id, timestamp) DO UPDATE SET value = EXCLUDED.value""",
            [(series_id, ts, v) for ts, v in rows],
        )
    conn.commit()
    return len(rows)


def watermark(conn, series_id: str):
    with conn.cursor() as cur:
        cur.execute("SELECT last_ts FROM fred_fetch_state WHERE series_id = %s", (series_id,))
        row = cur.fetchone()
    return row[0] if row else None


def set_watermark(conn, series_id: str, ts: datetime):
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO fred_fetch_state (series_id, last_ts)
               VALUES (%s, %s)
               ON CONFLICT (series_id) DO UPDATE SET last_ts = EXCLUDED.last_ts, updated_at = now()""",
            (series_id, ts),
        )
    conn.commit()


def report_gaps(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT series_id, COUNT(*) AS n,
                   MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts
            FROM fred_observations GROUP BY series_id ORDER BY series_id
        """)
        rows = cur.fetchall()
        if not rows:
            print("  no fred_observations rows yet")
            return
        print(f"  {'series':14} {'rows':>6}  min_date    max_date    last_value")
        for series_id, n, min_ts, max_ts in rows:
            with conn.cursor() as cur2:
                cur2.execute(
                    "SELECT value FROM fred_observations WHERE series_id=%s ORDER BY timestamp DESC LIMIT 1",
                    (series_id,),
                )
                last_val = cur2.fetchone()
                lv = f"{last_val[0]:.4g}" if last_val else "—"
            print(f"  {series_id:14} {n:>6}  {min_ts.date()}  {max_ts.date()}  {lv}")

        # gap report: consecutive obs distance per series
        with conn.cursor() as cur3:
            cur3.execute("""
                WITH g AS (
                    SELECT series_id, timestamp,
                           LEAD(timestamp) OVER (PARTITION BY series_id ORDER BY timestamp) AS next_ts
                    FROM fred_observations
                )
                SELECT series_id,
                       COUNT(*) FILTER (WHERE next_ts IS NOT NULL AND next_ts - timestamp > interval '8 days') AS gaps_8d,
                       COUNT(*) FILTER (WHERE next_ts IS NOT NULL AND next_ts - timestamp > interval '35 days') AS gaps_35d,
                       COALESCE(MAX(EXTRACT(EPOCH FROM (next_ts - timestamp))/86400.0),0) AS max_gap_d
                FROM g GROUP BY series_id ORDER BY series_id
            """)
            for r in cur3.fetchall():
                # only warn if unexpectedly large for the series frequency
                print(f"    {r[0]:14} gaps>8d:{r[1]:>3} gaps>35d:{r[2]:>3} max_gap:{r[3]:.1f}d")

        with conn.cursor() as cur4:
            cur4.execute("SELECT series_id, last_ts FROM fred_fetch_state ORDER BY series_id")
            wms = cur4.fetchall()
            if wms:
                print("  watermarks:")
                for sid, ts in wms:
                    print(f"    {sid:14} {ts.date()}")


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--series", default="", help="Comma-separated subset of " + ",".join(DEFAULT_SERIES) + " (default: all)")
    parser.add_argument("--backfill", default="", help="Rebuild from DATE (e.g. 2020-01-01)")
    parser.add_argument("--end", default="", help="Fetch up to DATE (default: today)")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS, help=f"Days to fetch if no watermark (default {DEFAULT_LOOKBACK_DAYS})")
    parser.add_argument("--freshness-only", action="store_true", help="Coverage + gap report, no fetch")
    parser.add_argument("--list-series", action="store_true", help="List default series and exit")
    args = parser.parse_args()

    if args.list_series:
        print("Default series:")
        for s in DEFAULT_SERIES:
            print(f"  {s}")
        return

    series_ids = DEFAULT_SERIES
    if args.series:
        want = [s.strip().upper() for s in args.series.split(",") if s.strip()]
        # allow any valid FRED id, not just defaults
        series_ids = want

    # validate API key early
    if not FRED_API_KEY and not args.freshness_only:
        print("ERROR: FRED_API_KEY not set. Add to exchanges/.env or export it.", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(PG_URL)
    try:
        if args.freshness_only:
            report_gaps(conn)
            return

        now = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        end = now + timedelta(days=1)  # inclusive of today
        if args.end:
            end = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)

        total_rows = 0
        for sid in series_ids:
            ensure_series(conn, sid)

            wm = watermark(conn, sid)
            if args.backfill:
                start = datetime.strptime(args.backfill, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            elif wm:
                # resume from next day after watermark (watermark is last stored obs date)
                start = wm + timedelta(days=1)
            else:
                start = end - timedelta(days=args.lookback_days)

            if end <= start:
                print(f"{sid:14} nothing new (watermark {wm.date() if wm else 'none'}, start {start.date()})")
                continue

            print(f"{sid:14} [{start.date()} → {(end - timedelta(days=1)).date()}] ", end="", flush=True)
            try:
                obs = fetch_observations(sid, start, end - timedelta(days=1))
            except Exception as e:
                print(f"ERROR {e}", file=sys.stderr)
                continue

            if not obs:
                print("no new observations (weekend/holiday or pending)")
                continue

            n = upsert_observations(conn, sid, obs)
            set_watermark(conn, sid, max(ts for ts, _ in obs))
            total_rows += n
            print(f"{n} obs upserted (latest {obs[-1][0].date()} = {obs[-1][1]:.4g})")

        print(f"\nTotal: {total_rows} observations upserted across {len(series_ids)} series")
        report_gaps(conn)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
