#!/usr/bin/env python3
"""Listen for PostgreSQL NOTIFY events."""

import os
import sys
import argparse
import signal
import time

import psycopg2


def main():
    parser = argparse.ArgumentParser(description="Listen for PostgreSQL notifications")
    parser.add_argument(
        "channel",
        nargs="?",
        default="orderbook_bar_insert",
        help="Channel to listen on (default: orderbook_bar_insert)",
    )
    parser.add_argument(
        "--db",
        default=os.environ.get(
            "PG_URL", "postgres://postgres:postgres@localhost:5432/mdx"
        ),
        help="Database URL",
    )
    args = parser.parse_args()

    conn = psycopg2.connect(args.db)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"LISTEN {args.channel}")
    sys.stdout.flush()

    print(f"Listening for '{args.channel}' notifications...", flush=True)
    print("Press Ctrl+C to exit", flush=True)
    print("", flush=True)

    def signal_handler(sig, frame):
        print("\nShutting down...")
        conn.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    try:
        while True:
            conn.poll()
            while conn.notifies:
                notify = conn.notifies.pop(0)
                print(f"[{notify.channel}] {notify.payload}", flush=True)
    except Exception as e:
        print(f"Error: {e}")
        conn.close()
        sys.exit(1)


if __name__ == "__main__":
    main()
