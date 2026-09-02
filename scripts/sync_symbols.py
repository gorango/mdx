#!/usr/bin/env python3
"""Fetch top coins from CoinGecko, cross-check against Binance Futures USDT
perpetuals, and write config/symbols.yaml."""

import argparse
import json
import os
import sys
import urllib.request
from pathlib import Path

# Project root is two levels above exchanges/scripts (…/twain)
REPO = Path(__file__).resolve().parents[2]
SYMBOLS_PATH = REPO / "config" / "symbols.yaml"
COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=200&page=1"
BINANCE_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"


def fetch_json(url):
    resp = urllib.request.urlopen(url)
    return json.loads(resp.read())


def fmt_cap(n):
    if n >= 1e12:
        return f"${n / 1e12:.2f}T"
    elif n >= 1e9:
        return f"${n / 1e9:.2f}B"
    elif n >= 1e6:
        return f"${n / 1e6:.1f}M"
    return f"${n:.0f}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=SYMBOLS_PATH,
        help="Output YAML path (default: %(default)s)",
    )
    args = parser.parse_args()
    output_path = Path(args.output)

    print("Fetching CoinGecko top 200...")
    coins = fetch_json(COINGECKO_URL)

    print("Fetching Binance Futures exchangeInfo...")
    exchange_info = fetch_json(BINANCE_URL)

    binance_symbols = {
        s["symbol"]: s
        for s in exchange_info["symbols"]
        if s["status"] == "TRADING"
        and s["contractType"] == "PERPETUAL"
        and s["quoteAsset"] == "USDT"
    }
    print(f"  {len(binance_symbols)} USDT perpetuals on Binance Futures")

    print("Fetching cryptoHFT data symbols...")
    try:
        hft_data = fetch_json(
            "https://api.cryptohftdata.com/symbols?exchange=binance_futures"
        )
        hft_symbols = set(hft_data.get("symbols", hft_data))
        print(f"  {len(hft_symbols)} symbols available on cryptoHFT")
    except Exception as e:
        print(f"  WARNING: could not fetch cryptoHFT symbols ({e}) — skipping check")
        hft_symbols = None

    exclude = {
        "usdt",
        "usdc",
        "usds",
        "usde",
        "dai",
        "usdg",
        "pyusd",
        "usd1",
        "usdd",
        "usdf",
        "usd0",
        "usx",
        "crvusd",
        "usdai",
        "usdy",
        "susdc",
        "susde",
        "reusd",
        "satusd",
        "ausd",
        "usat",
        "fdusd",
        "tusd",
        "busd",
        "frax",
        "lusd",
        "gusd",
        "husd",
        "musd",
        "ustb",
        "eutbl",
        "ousg",
        "usdglo",
        "apxusd",
        "usdgo",
        "usyc",
        "gho",
        "eurc",
        "payb",
        "stable",
        "bfusd",
        "figr_heloc",
    }

    matched = []
    for c in coins:
        sym = c["symbol"].lower()
        name = c["name"]
        rank = c.get("market_cap_rank") or "?"
        cap = c.get("market_cap") or 0

        if sym in exclude:
            continue
        if not sym.isascii():
            continue

        candidates = [sym.upper() + "USDT"]
        if len(sym) <= 6:
            candidates.append("1000" + sym.upper() + "USDT")

        for bsym in candidates:
            if bsym in binance_symbols:
                canon = bsym.replace("USDT", "/USDT:PERP")
                matched.append((rank, canon, name, cap))
                break

    matched.sort(key=lambda x: int(x[0]) if str(x[0]).isdigit() else 999)

    if hft_symbols is not None:
        before = len(matched)
        matched = [(r, c, n, cap) for r, c, n, cap in matched if c.replace("/USDT:PERP", "USDT") in hft_symbols]
        if before - len(matched):
            print(f"  Filtered {before - len(matched)} symbols not on cryptoHFT")

    lines = [
        "# Symbols that trade on Binance Futures USDT perpetuals",
        "# and have historical data available on cryptoHFT",
        f"# Generated {__import__('datetime').date.today()} from CoinGecko top-200 × Binance exchangeInfo",
    ]
    for rank, canon, name, cap in matched:
        lines.append(f'- "{canon}"  # {rank}. {name} ({fmt_cap(cap)})')

    out = "\n".join(lines) + "\n"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        f.write(out)

    print(f"\nWrote {len(matched)} symbols to {output_path}")


if __name__ == "__main__":
    main()
