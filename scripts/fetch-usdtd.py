#!/usr/bin/env -S uv run
# /// script
# dependencies = ["polars"]
# ///
"""Fetch USDT.D from CoinGecko. Per-coin caching for incremental backfill.

USDT.D = USDT Market Cap / Sum(top N coin market caps) * 100

Each coin's history is cached separately. Re-run to retry failed coins.
Lite mode uses top 5 coins (~80% accuracy, 5× faster).

Usage:
  just usdtd-fetch                    # incremental (fill gaps from cached)
  just usdtd-fetch -- --backfill 2025-07-01  # backfill to a specific start
  just usdtd-fetch -- --lite               # top 5 coins only
  just usdtd-fetch -- --no-fetch           # recompute from cache only
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

import polars as pl
import urllib.request
import urllib.error

COINGECKO_BASE = "https://api.coingecko.com/api/v3"
COINGECKO_KEY = os.environ.get("COINGECKO_API_KEY", "")
RATE_LIMIT_DELAY = 4.0
RETRY_BACKOFF = 5.0
MAX_RETRIES = 3
TOP_N = 25
LITE_N = 5

EXCLUDED = {
    "USDT", "USDC", "USDS", "USDE", "DAI", "USDG", "PYUSD", "USD1",
    "USDD", "USDF", "USD0", "USX", "CRVUSD", "USDAI", "USDY",
    "SUSDC", "SUSDE", "REUSD", "FDUSD", "TUSD", "BUSD", "FRAX",
    "LUSD", "GUSD", "EURC", "GHO", "USTB", "PAXG", "XAUT",
}
SAFE = "USDTD_USDT_DOMINANCE"


def _add_key(url: str) -> str:
    if not COINGECKO_KEY:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}x_cg_demo_api_key={COINGECKO_KEY}"


def fetch_json(url: str) -> dict:
    delay = RATE_LIMIT_DELAY
    for attempt in range(MAX_RETRIES):
        time.sleep(delay)
        req = urllib.request.Request(_add_key(url), headers={"User-Agent": "mdx/1.0"})
        if COINGECKO_KEY:
            req.add_header("x-cg-demo-api-key", COINGECKO_KEY)
        try:
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < MAX_RETRIES - 1:
                delay += RETRY_BACKOFF
                continue
            raise


def fetch_mcap(coin_id: str, days: str) -> list[tuple[int, float]]:
    url = f"{COINGECKO_BASE}/coins/{coin_id}/market_chart?vs_currency=usd&days={days}"
    try:
        data = fetch_json(url)
        return [(int(ts), m) for ts, m in data.get("market_caps", [])]
    except urllib.error.HTTPError as e:
        if e.code == 429:
            return []  # caller will retry next run
        print(f"  WARN {coin_id}: {e}", file=sys.stderr)
        return []
    except (json.JSONDecodeError, urllib.error.URLError) as e:
        print(f"  WARN {coin_id}: {e}", file=sys.stderr)
        return []


def load_coin_cache(path: Path) -> list[tuple[int, float]] | None:
    if not path.exists():
        return None
    with open(path) as f:
        raw = json.load(f)
    return [(int(ts), float(v)) for ts, v in raw]


def save_coin_cache(path: Path, data: list[tuple[int, float]]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f)


def compute(usdt: dict[int, float], others: dict[str, dict[int, float]]) -> list[tuple[int, float]]:
    n = len(others) + 1
    tss = sorted(set(usdt.keys()) | {ts for c in others.values() for ts in c})
    out = []
    for ts in tss:
        u = usdt.get(ts, 0)
        vals = {k: v.get(ts, 0) for k, v in others.items()}
        p = sum(1 for v in vals.values() if v > 0) + (1 if u > 0 else 0)
        total = sum(vals.values()) + u
        if total > 0 and u > 0 and p >= n * 0.8:
            v = (u / total) * 100.0
            if v < 25.0:
                out.append((ts, v))
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="data/usdtd")
    parser.add_argument("--timeframe", default="1d")
    parser.add_argument("--backfill", default="", help="Backfill to DATE (e.g. 2025-07-01)")
    parser.add_argument("--no-fetch", action="store_true")
    parser.add_argument("--lite", action="store_true")
    parser.add_argument("--cache-dir", default="")
    args = parser.parse_args()

    out_dir = Path(args.output) / args.timeframe
    out_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = Path(args.cache_dir or Path(args.output) / "cache")
    coins_dir = cache_dir / "coins"
    coins_dir.mkdir(parents=True, exist_ok=True)

    today = datetime.now(timezone.utc)
    top_n = LITE_N if args.lite else TOP_N
    tag = "lite" if args.lite else "full"

    # Determine target date range
    existing = None
    for p in out_dir.glob(f"{SAFE}_{args.timeframe}_*.parquet"):
        existing = pl.read_parquet(p)
        break

    if args.backfill:
        target_start = datetime.strptime(args.backfill, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        target_end = today
    elif existing is not None and len(existing) > 0:
        target_start = datetime.fromtimestamp(existing["timestamp"].min() / 1000, tz=timezone.utc)
        target_end = today
        print(f"Existing: {target_start.date()} → {existing['timestamp'].max() / 1000:.0f}")
    else:
        target_start = today - timedelta(days=365)
        target_end = today
        print("No existing data, targeting ~365 days")

    days_needed = (target_end - target_start).days + 1
    days_str = str(min(days_needed, 365))
    print(f"Target: {target_start.date()} → {target_end.date()} ({days_str}d)")

    # ── Coin list ──
    coins_cache = cache_dir / "coins.json"
    coin_list = None
    if coins_cache.exists() and (today.timestamp() - os.path.getmtime(coins_cache)) < 7 * 86400:
        with open(coins_cache) as f:
            coin_list = json.load(f)
    if coin_list is None:
        print("Fetching top coins...", end=" ", flush=True)
        coin_list = fetch_json(
            f"{COINGECKO_BASE}/coins/markets?vs_currency=usd&order=market_cap_desc&per_page={top_n}&page=1"
        )
        with open(coins_cache, "w") as f:
            json.dump(coin_list, f)
        print(f"{len(coin_list)} coins")

    usdt_entry = next((c for c in coin_list if c["symbol"].upper() == "USDT"), None)
    if not usdt_entry:
        print("ERROR: USDT not in top coins")
        sys.exit(1)

    coins_to_fetch = [
        c for c in coin_list
        if c["id"] == usdt_entry["id"] or c["symbol"].upper() not in EXCLUDED
    ]

    # ── Per-coin fetch (cached individually) ──
    all_data: dict[str, list[tuple[int, float]]] = {}
    fetched = 0
    skipped = 0
    failed = 0

    for coin in coins_to_fetch:
        cf = coins_dir / f"{coin['id']}.json"
        cached = load_coin_cache(cf)

        # Check if cached data covers our target range
        # Check cache: use it if it covers our target range
        if cached:
            min_ts = min(ts for ts, _ in cached)
            max_ts = max(ts for ts, _ in cached)
            min_dt = datetime.fromtimestamp(min_ts / 1000, tz=timezone.utc)
            max_dt = datetime.fromtimestamp(max_ts / 1000, tz=timezone.utc)
            # Cache is good if it reaches back 360+ days and ends recently
            if max_dt >= today - timedelta(days=2) and max_dt >= target_end - timedelta(days=7):
                all_data[coin["id"]] = cached
                skipped += 1
                continue

        if args.no_fetch:
            if cached:
                all_data[coin["id"]] = cached
            continue

        label = f"{coin['symbol']}"
        print(f"  {label}...", end=" ", flush=True)
        hist = fetch_mcap(coin["id"], days_str)
        if hist:
            # Merge with existing cache if we have it (for incremental extension)
            if cached:
                merged = {ts: v for ts, v in cached}
                merged.update({ts: v for ts, v in hist})
                hist = sorted(merged.items())
            save_coin_cache(cf, hist)
            all_data[coin["id"]] = hist
            fetched += 1
            print(f"{len(hist)} pts")
        else:
            # Keep stale cache if fetch failed
            if cached:
                all_data[coin["id"]] = cached
                print("using cached")
            else:
                failed += 1
                print("FAILED")

    print(f"Fetched: {fetched}, cached: {skipped}, failed: {failed}")

    if not all_data:
        print("ERROR: no data")
        sys.exit(1)

    # ── Compute ──
    usdt_data = dict(all_data.get(usdt_entry["id"], []))
    others = {k: dict(v) for k, v in all_data.items() if k != usdt_entry["id"]}
    pts = compute(usdt_data, others)

    if not pts:
        print("ERROR: no data points computed (check coin coverage)")
        sys.exit(1)

    merged = pl.DataFrame(
        {"timestamp": [ts for ts, _ in pts], "dominance": [v for _, v in pts]},
        schema={"timestamp": pl.Int64, "dominance": pl.Float64},
    ).sort("timestamp")

    # Merge with existing parquet
    if existing is not None and len(existing) > 0:
        existing_max = existing["timestamp"].max()
        new_rows = merged.filter(pl.col("timestamp") > existing_max)
        if len(new_rows) > 0:
            merged = pl.concat([existing, new_rows]).unique(subset=["timestamp"]).sort("timestamp")
            print(f"Merged with existing: {len(merged)} rows")
        else:
            merged = existing

    first = datetime.fromtimestamp(merged["timestamp"][0] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    last = datetime.fromtimestamp(merged["timestamp"][-1] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

    for p in out_dir.glob(f"{SAFE}_{args.timeframe}_*.parquet"):
        p.unlink()
    out_path = out_dir / f"{SAFE}_{args.timeframe}_{first}_{last}.parquet"
    merged.write_parquet(out_path)

    print(f"\n{out_path}")
    print(f"  {len(merged)} rows, {merged['dominance'].min():.2f}% - {merged['dominance'].max():.2f}%, latest: {merged['dominance'].tail(1).item():.2f}%")

    if failed > 0:
        print(f"\n⚠ {failed} coin(s) still missing data. Re-run to retry.")
        sys.exit(1)


if __name__ == "__main__":
    main()
