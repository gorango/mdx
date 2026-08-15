#!/usr/bin/env -S uv run
# /// script
# dependencies = ["google-cloud-bigquery", "psycopg2-binary"]
# ///
"""Fetch BTC/ETH exchange netflow from Google BigQuery public datasets into postgres.

Hourly inflow/outflow into/out of labeled exchange addresses (Binance hot/cold
wallets from data/netflow/labels.json), landing in `flow_bars` (twain db).
Mirrors scripts/fetch-btcd.py conventions: incremental by default (watermark in
`netflow_fetch_state`), --backfill to rebuild from a date.

Sources (all free BigQuery public datasets, ~1 TB/month free tier):
  BTC  : bigquery-public-data.crypto_bitcoin.inputs / .outputs
  ETH  : bigquery-public-data.crypto_ethereum.transactions
  ERC20: bigquery-public-data.crypto_ethereum.token_transfers  (WBTC / USDT / USDC)

Auth: GOOGLE_APPLICATION_CREDENTIALS=<sa.json>, or `gcloud auth application-default
login`. Queries hit the project's free tier; the block_timestamp partition filter
plus address IN-list keep scanned bytes small.

Caveats:
  * BTC inputs carry the FULL UTXO value being spent, so outflow from a labeled
    address is upper-bounded by its spends (incl. internal Binance re-orgs). BTC
    netflow is an approximation — same as every public BTC flow feed.
  * `--lag-hours` (default 3) skips buckets BigQuery may not have finalized yet
    (BTC lags ~30 min). Fine for 1h/15m candles.

Usage:
  just netflow-fetch                        # incremental (from watermark; ~365d if none)
  just netflow-fetch -- --backfill 2024-01-01   # rebuild from DATE (idempotent upsert)
  just netflow-labels                       # only (re)load data/netflow/labels.json
  just netflow-status                       # coverage per asset
  just netflow-freshness                    # dataset staleness + flow_bars gaps

Freshness: each fetch clamps its horizon to the dataset's newest hour, so a
stale batch-fed dataset yields no new rows (and no watermark advance) instead
of baking gaps. Holes in flow_bars are reported and should be treated as NaN
by consumers, not zero.
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

from google.cloud import bigquery

PG_URL = os.environ.get(
    "PG_URL", "postgres://postgres:postgres@localhost:5432/twain"
)
LABELS_PATH = Path(__file__).resolve().parent.parent / "data" / "netflow" / "labels.json"

DEFAULT_LOOKBACK_DAYS = 365
DEFAULT_LAG_HOURS = 2   # skip buckets BigQuery may not have finalized yet
DEFAULT_STALE_HOURS = 24  # warn (and clamp fetch horizon) if dataset lags this much
DEFAULT_GAP_HOURS = 4     # flow_bars hole threshold for the continuity report

# asset -> (chain in labels.json, BQ dataset, ERC20 token address or None)
ASSETS = {
    "BTC":  ("bitcoin", "bigquery-public-data.crypto_bitcoin", None),
    "ETH":  ("ethereum", "bigquery-public-data.crypto_ethereum", None),
    "WBTC": ("ethereum", "bigquery-public-data.crypto_ethereum",
             "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"),
    "USDT": ("ethereum", "bigquery-public-data.crypto_ethereum",
             "0xdac17f958d2ee523a2206206994597c13d831ec7"),
    "USDC": ("ethereum", "bigquery-public-data.crypto_ethereum",
             "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
}

DIVISOR = {  # raw on-chain units -> asset units
    "BTC": 1e8, "ETH": 1e18, "WBTC": 1e8, "USDT": 1e6, "USDC": 1e6,
}


# ── BigQuery queries ──────────────────────────────────────────────────────────

def btc_query(dataset, table, start, end):
    """table in {inputs, outputs}; outputs -> inflow, inputs -> outflow."""
    return f"""
        SELECT TIMESTAMP_TRUNC(block_timestamp, HOUR) AS hour,
               COUNT(DISTINCT transaction_hash) AS tx_count,
               SUM(value) AS amount
        FROM `{dataset}.{table}`
        CROSS JOIN UNNEST(addresses) AS addr
        WHERE addr IN UNNEST(@labels)
          AND block_timestamp >= TIMESTAMP(@start)
          AND block_timestamp <  TIMESTAMP(@end)
        GROUP BY hour ORDER BY hour
    """


def eth_query(dataset, column, start, end):
    """column in {to_address, from_address}; to -> inflow, from -> outflow."""
    return f"""
        SELECT TIMESTAMP_TRUNC(block_timestamp, HOUR) AS hour,
               COUNT(*) AS tx_count,
               SUM(value) AS amount
        FROM `{dataset}.transactions`
        WHERE {column} IN UNNEST(@labels)
          AND block_timestamp >= TIMESTAMP(@start)
          AND block_timestamp <  TIMESTAMP(@end)
          AND value > 0
        GROUP BY hour ORDER BY hour
    """


def erc20_query(dataset, token, column, start, end):
    """column in {to_address, from_address}; to -> inflow, from -> outflow."""
    return f"""
        SELECT TIMESTAMP_TRUNC(block_timestamp, HOUR) AS hour,
               COUNT(*) AS tx_count,
               CAST(SUM(SAFE_CAST(value AS NUMERIC)) AS FLOAT64) AS amount
        FROM `{dataset}.token_transfers`
        WHERE token_address = @token
          AND {column} IN UNNEST(@labels)
          AND block_timestamp >= TIMESTAMP(@start)
          AND block_timestamp <  TIMESTAMP(@end)
        GROUP BY hour ORDER BY hour
    """


def run_query(client, sql, labels, start, end, params=None):
    qp = [
        bigquery.ArrayQueryParameter("labels", "STRING", labels),
        bigquery.ScalarQueryParameter("start", "STRING", start.isoformat()),
        bigquery.ScalarQueryParameter("end", "STRING", end.isoformat()),
    ]
    if params:
        qp += [bigquery.ScalarQueryParameter(k, "STRING", v) for k, v in params.items()]
    job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=qp))
    return {r["hour"]: (int(r["tx_count"]), float(r["amount"])) for r in job.result()}


# ── postgres ──────────────────────────────────────────────────────────────────

def upsert_labels(conn, labels):
    """labels: list of (chain, address, exchange, kind, source)."""
    with conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO address_labels (chain, address, exchange, kind, source)
               VALUES %s
               ON CONFLICT (chain, address) DO UPDATE SET
                 exchange = EXCLUDED.exchange,
                 kind = EXCLUDED.kind,
                 source = EXCLUDED.source,
                 updated_at = now()""",
            labels,
        )
    conn.commit()


def upsert_flow(conn, rows):
    """rows: list of (asset, exchange, timestamp, inflow, outflow, netflow, tx_count, source)."""
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO flow_bars
                 (asset, exchange, timestamp, inflow, outflow, netflow, tx_count, source)
               VALUES %s
               ON CONFLICT (asset, exchange, timestamp) DO UPDATE SET
                 inflow = EXCLUDED.inflow,
                 outflow = EXCLUDED.outflow,
                 netflow = EXCLUDED.netflow,
                 tx_count = EXCLUDED.tx_count,
                 source = EXCLUDED.source""",
            rows,
        )
    conn.commit()
    return len(rows)


def watermark(conn, asset, exchange):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_ts FROM netflow_fetch_state WHERE asset = %s AND exchange = %s",
            (asset, exchange),
        )
        row = cur.fetchone()
    return row[0] if row else None


def set_watermark(conn, asset, exchange, ts):
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO netflow_fetch_state (asset, exchange, last_ts)
               VALUES (%s, %s, %s)
               ON CONFLICT (asset, exchange) DO UPDATE SET last_ts = EXCLUDED.last_ts""",
            (asset, exchange, ts),
        )
    conn.commit()


# ── labels ────────────────────────────────────────────────────────────────────

def load_labels(path: Path) -> list[tuple]:
    """Normalize data/netflow/labels.json into (chain, address, exchange, kind, source)."""
    data = json.loads(path.read_text())
    out = []
    for exchange, cfg in data.get("exchanges", {}).items():
        kind = cfg.get("kind", "hot")
        source = cfg.get("source", "manual")
        for chain, addrs in cfg.get("chains", {}).items():
            for a in addrs:
                a = a.strip()
                if not a or a.startswith("PASTE_"):
                    continue
                out.append((chain, a.lower() if chain == "ethereum" else a, exchange, kind, source))
    for org, chains in data.get("treasuries", {}).items():
        for chain, addrs in chains.items():
            for a in addrs:
                out.append((chain, a.strip(), org, "treasury", "public"))
    # dedupe, keep order
    seen, dedup = set(), []
    for row in out:
        if row[1] not in seen:
            seen.add(row[1])
            dedup.append(row)
    return dedup


# ── freshness / continuity ────────────────────────────────────────────────────

DATASETS = [  # (chain, dataset, table for MAX(block_timestamp))
    ("bitcoin", "bigquery-public-data.crypto_bitcoin", "outputs"),
    ("ethereum", "bigquery-public-data.crypto_ethereum", "transactions"),
]


def dataset_newest_hour(client, dataset, table):
    sql = f"SELECT MAX(block_timestamp) AS mx FROM `{dataset}.{table}`"
    for row in client.query(sql).result():
        if row["mx"] is None:
            return None
        return row["mx"].replace(minute=0, second=0, microsecond=0)
    return None


def report_freshness(client, stale_hours):
    now = datetime.now(timezone.utc)
    for chain, ds, table in DATASETS:
        mx = dataset_newest_hour(client, ds, table)
        age = (now - mx) if mx else None
        flag = " STALE" if age and age > timedelta(hours=stale_hours) else ""
        print(f"  {chain:10} newest dataset hour: {mx}  (age {age}){flag}")


def report_gaps(conn, gap_hours):
    with conn.cursor() as cur:
        cur.execute("""
            WITH g AS (
                SELECT asset, exchange, timestamp,
                       LEAD(timestamp) OVER (PARTITION BY asset, exchange ORDER BY timestamp) AS next_ts
                FROM flow_bars
            )
            SELECT asset, exchange,
                   COUNT(*) FILTER (WHERE next_ts IS NOT NULL AND next_ts - timestamp > make_interval(hours => %s)) AS gaps,
                   COALESCE(MAX(EXTRACT(EPOCH FROM (next_ts - timestamp)) / 3600.0), 0) AS max_gap_h,
                   MAX(timestamp) AS last_ts
            FROM g GROUP BY asset, exchange ORDER BY asset
        """, (gap_hours,))
        rows = cur.fetchall()
        if not rows:
            print("  no flow_bars rows yet")
            return
        print(f"  {'asset':6} {'exchange':8} {'gaps>%dh' % gap_hours:>10} {'max_gap_h':>10}  last_ts")
        for r in rows:
            print(f"  {r[0]:6} {r[1]:8} {r[2]:>10} {r[3]:>10.1f}  {r[4]}")


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--assets", default="", help="Comma-separated subset of " + ",".join(ASSETS) + " (default: all)")
    parser.add_argument("--backfill", default="", help="Rebuild from DATE (e.g. 2024-01-01)")
    parser.add_argument("--end", default="", help="Fetch up to DATE (default: now - lag hours). Use with --backfill to chunk")
    parser.add_argument("--labels-only", action="store_true", help="Only (re)load labels")
    parser.add_argument("--freshness-only", action="store_true", help="Dataset staleness + flow_bars gap report, no fetch")
    parser.add_argument("--lag-hours", type=int, default=DEFAULT_LAG_HOURS)
    parser.add_argument("--stale-hours", type=int, default=DEFAULT_STALE_HOURS)
    parser.add_argument("--gap-hours", type=int, default=DEFAULT_GAP_HOURS)
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--labels", default=str(LABELS_PATH), help="Path to labels JSON")
    parser.add_argument("--project", default=os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT", ""), help="GCP project for BigQuery billing/quota")
    args = parser.parse_args()

    labels = load_labels(Path(args.labels))
    print(f"Labels: {len(labels)} addresses from {args.labels}")

    conn = psycopg2.connect(PG_URL)
    try:
        upsert_labels(conn, labels)
        print("  address_labels updated")

        if args.labels_only:
            return

        client = bigquery.Client(project=args.project or None)
        now = datetime.now(timezone.utc)
        end = now - timedelta(hours=args.lag_hours)
        end = end.replace(minute=0, second=0, microsecond=0)
        if args.end:
            end = min(end, datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc))

        if args.freshness_only:
            report_freshness(client, args.stale_hours)
            report_gaps(conn, args.gap_hours)
            return

        report_freshness(client, args.stale_hours)

        binance_by_chain: dict[str, list[str]] = {}
        for chain, addr, exchange, _, _ in labels:
            if exchange == "binance":
                binance_by_chain.setdefault(chain, []).append(addr)

        assets = {a: v for a, v in ASSETS.items()}
        if args.assets:
            want = {a.strip() for a in args.assets.split(",") if a.strip()}
            unknown = want - set(ASSETS)
            if unknown:
                parser.error(f"unknown assets: {sorted(unknown)}")
            assets = {a: v for a, v in ASSETS.items() if a in want}

        for asset, (chain, dataset, token) in assets.items():
            addrs = binance_by_chain.get(chain, [])
            if not addrs:
                print(f"{asset}: no binance labels on chain '{chain}', skipping")
                continue

            wm = watermark(conn, asset, "binance")
            if args.backfill:
                start = datetime.strptime(args.backfill, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            elif wm:
                start = wm
            else:
                start = end - timedelta(days=args.lookback_days)

            # Clamp the fetch horizon to the dataset's newest hour: never fetch
            # (or watermark) past data the dataset doesn't have yet.  A stale
            # dataset then just yields no new rows and the next run retries.
            table = "outputs" if chain == "bitcoin" else "transactions"
            mx = dataset_newest_hour(client, dataset, table)
            end_asset = min(end, mx + timedelta(hours=1)) if mx else end
            if mx and end - mx > timedelta(hours=args.stale_hours):
                print(f"{asset}: WARN dataset stale (newest {mx}); fetching only to {end_asset}")

            if end_asset <= start:
                print(f"{asset}: nothing new (watermark {wm})")
                continue

            print(f"{asset}: [{start.isoformat()} → {end_asset.isoformat()}) ", end="", flush=True)

            if token:
                q_in = erc20_query(dataset, token, "to_address", start, end)
                q_out = erc20_query(dataset, token, "from_address", start, end)
            elif chain == "bitcoin":
                q_in = btc_query(dataset, "outputs", start, end)
                q_out = btc_query(dataset, "inputs", start, end)
            else:
                q_in = eth_query(dataset, "to_address", start, end)
                q_out = eth_query(dataset, "from_address", start, end)

            div = DIVISOR[asset]
            params = {"token": token} if token else None
            inflow = run_query(client, q_in, addrs, start, end_asset, params)
            outflow = run_query(client, q_out, addrs, start, end_asset, params)

            hours = sorted(set(inflow) | set(outflow))
            rows = []
            for h in hours:
                amt_in = inflow.get(h, (0, 0))[1] / div
                amt_out = outflow.get(h, (0, 0))[1] / div
                n_tx = inflow.get(h, (0, 0))[0] + outflow.get(h, (0, 0))[0]
                rows.append((asset, "binance", h, amt_in, amt_out, amt_in - amt_out, n_tx, "bigquery"))

            n = upsert_flow(conn, rows)
            if rows:
                set_watermark(conn, asset, "binance", max(hours))
            print(f"{n} hours upserted")

        print("Gap report (holes in flow_bars > %dh):" % args.gap_hours)
        report_gaps(conn, args.gap_hours)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
