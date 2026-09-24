#!/usr/bin/env python3
"""List Arcus markets (testnet, staging, or mainnet).

  showmarkets.py                       # aligned table of the common columns
  showmarkets.py --condensed           # CSV of the common columns (no header, for piping)
  showmarkets.py --condensed --header  # ... with a CSV header row first
  showmarkets.py --verbose             # aligned table of ALL fields
  showmarkets.py --condensed --verbose # CSV of ALL fields (for piping)
"""

import argparse
import csv
import sys
from arcus_common_public import add_network_args, run_pipe_safe, NETWORKS, cell, get_json, market_id_key, markets_cache_path, require_dict, write_markets_cache   # shared public helpers (formerly local copies)

MARKETS_URL = None   # set in main() from the required --testnet/--staging selector

# --createjson writes the raw /v1/markets response here for the launcher's other tools (wsorderbook /
# showorderbook) to read, collapsing a whole launcher run to ONE server call. This tool runs FIRST in
# the launcher, so it is the natural creator. The launcher exports ARCUS_MARKETS_CACHE with a PER-RUN
# path so a foreign/stale file at the predictable path can never be trusted (a unique-path write
# failure just leaves no file -> readers fail-open to a live fetch, never to stale data). Falls back to
# a NETWORK-scoped predictable path for manual use (testnet/staging/mainnet marketId maps differ).


# (key, alignment) for the default/condensed common columns. minOrderSize/maxOrderSize (v1.3.8, re-added)
# sit with tick/step as the order-placement constraints; the rest of the fields (high24h/low24h/
# openInterestCapNotional/...) are surfaced by --verbose / --verbosecondensed (all_keys).
COMMON_COLS = [
    ("marketId", ">"), ("marketDisplayName", "<"), ("tickSize", ">"),
    ("stepSize", ">"), ("minOrderSize", ">"), ("maxOrderSize", ">"),
    ("oraclePrice", ">"), ("status", "<"),
]


def fetch_markets(cache_path=None):
    """Fetch and sort markets, turning network/parse failures into clean CLI errors. When cache_path
    is set, the RAW response is also written there (best-effort) to warm the launcher's shared cache
    -- we always fetch fresh (never read that cache) so displayed output is never stale."""
    data = get_json(MARKETS_URL, what="markets", prog="showmarkets")   # retries transient failures + honors Retry-After (like the sibling tools)

    markets = require_dict(data, "markets", "showmarkets").get("markets")
    if not isinstance(markets, list):
        raise SystemExit("showmarkets: unexpected response shape (no 'markets' list).")
    if cache_path is not None:
        write_markets_cache(cache_path, data)   # raw shape {"markets": [...]} for sibling tools
    return sorted((m for m in markets if isinstance(m, dict)), key=market_id_key)


def all_keys(markets):
    """Every field key seen across markets, in first-seen order."""
    return list(dict.fromkeys(k for m in markets for k in m.keys()))


def print_table(markets, cols):
    """Print an aligned table for the given (key, align) columns, widths sized to data."""
    widths = {k: len(k) for k, _ in cols}
    for m in markets:
        for k, _ in cols:
            widths[k] = max(widths[k], len(cell(m.get(k))))
    header = "  ".join(f"{k:{align}{widths[k]}}" for k, align in cols)
    separator = "-" * len(header)
    print(f"\n  Markets: {MARKETS_URL}\n")
    print(header)
    print(separator)
    for m in markets:
        print("  ".join(f"{cell(m.get(k)):{align}{widths[k]}}" for k, align in cols))
    print(separator)
    print(f"  {len(markets)} markets\n")


def write_delimited(markets, keys, delimiter, header=False):
    """Write rows (optional header) via csv.writer so commas/quotes are escaped safely."""
    w = csv.writer(sys.stdout, delimiter=delimiter, lineterminator="\n")
    if header:                       # opt-in header row (--header), for parity with the display tools' --condensed
        w.writerow(keys)
    for m in markets:
        w.writerow([m.get(k, "") for k in keys])


def main():
    global MARKETS_URL
    parser = argparse.ArgumentParser(description="List markets.")
    parser.add_argument("--condensed", action="store_true",
                        help="CSV output, comma-delimited, safe for piping. Alone: the common columns; "
                             "with --verbose: ALL fields")
    parser.add_argument("--verbose", action="store_true",
                        help="show ALL fields, not just the common columns. Alone: an aligned table; "
                             "with --condensed: CSV")
    parser.add_argument("--header", action="store_true",
                        help="with --condensed, emit a CSV header row first (default: no header, "
                             "for parity with the display tools)")
    add_network_args(parser)
    parser.add_argument("--createjson", action="store_true",
                        help="also write the raw /v1/markets response to "
                             "/tmp/arcus_markets_<network>.json so sibling launcher tools resolve "
                             "markets from that file instead of re-hitting the server")
    parser.add_argument("--market",
                        help="show only this market (display name or marketId; the venue added a server-side "
                             "market filter in v1.4.5, but we filter the already-fetched list so --createjson "
                             "still caches the FULL set)")
    args = parser.parse_args()
    if args.header and not args.condensed:
        raise SystemExit("showmarkets: --header requires --condensed.")
    MARKETS_URL = NETWORKS[args.network] + "/v1/markets"

    cache_path = markets_cache_path(args.network) if args.createjson else None
    markets = fetch_markets(cache_path)

    if args.market:   # filter the DISPLAY only (the cache written by fetch_markets stays the full set)
        want = str(args.market).upper()
        markets = [m for m in markets if str(m.get("marketDisplayName", "")).upper() == want
                   or str(m.get("marketId")) == str(args.market)]
        if not markets:
            raise SystemExit(f"showmarkets: unknown market {args.market!r} (not found in /v1/markets).")

    if args.condensed and args.verbose:
        write_delimited(markets, all_keys(markets), delimiter=",", header=args.header)     # ALL fields, CSV
    elif args.condensed:
        write_delimited(markets, [k for k, _ in COMMON_COLS], delimiter=",", header=args.header)  # common columns
    elif args.verbose:
        print_table(markets, [(k, ">") for k in all_keys(markets)])          # ALL fields, aligned table
    else:
        print_table(markets, COMMON_COLS)                                    # common columns, aligned table


if __name__ == "__main__":
    run_pipe_safe(main)
