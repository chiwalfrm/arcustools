"""
Display open positions for an account.

  python3 display_positions.py <eth_address>
  python3 display_positions.py <eth_address> --market BTC-USD   # only that market's position
  python3 display_positions.py <eth_address> --condensed   # CSV, one row/position, all fields (pipe/file/spreadsheet)
  python3 display_positions.py <eth_address> --condensed --header   # ... with a CSV header row first
  python3 display_positions.py <eth_address> --verbose              # EVERY field per position (key:value block; nested cumulativeFunding as a blob)
  python3 display_positions.py <eth_address> --condensed --verbose  # EVERY field, one CSV row per position (header once)

Uses GET /v1/positions, a public account-scoped read that takes only the
`address` query parameter and needs NO signature -- so this display tool needs
just the address, not the creds file (same as display_orders.py).

The endpoint returns positions keyed by stringified marketId; an account with
no open positions returns an empty object `{}`. Mark price comes from `markPx`
(falling back to the legacy `oraclePx` name); shown as "-" when absent/0, since
the venue then falls back to the entry price for its notional / PnL math.
"""

import argparse
import csv
import sys
import urllib.parse
from decimal import Decimal
from arcus_common_public import add_network_args, print_verbose_records, write_verbose_csv, require_eth_address, run_pipe_safe, NETWORKS, cell, dec, get_json_dict, market_id_key, num   # shared public helpers (formerly local copies)

BASE = None   # set in main() from the required --testnet/--staging/--mainnet selector


def funding_of(p):
    cf = p.get("cumulativeFunding")
    return cf.get("sinceOpen") if isinstance(cf, dict) else None


def mark_p(p):
    """The position's mark price: `markPx` (the current API field), falling back to the legacy `oraclePx`
    name for older builds. (The endpoint was returning markPx while this tool still read oraclePx -> the MARK
    column showed '-' for every position; this reads the right field.)"""
    raw = p.get("markPx")
    return p.get("oraclePx") if raw is None else raw


def mark_str(p):
    """MARK column: the mark price, or '-' when absent/0 (venue then falls back to the entry price for its
    notional / PnL math)."""
    raw = mark_p(p)
    d = dec(raw)
    if d is None or d == 0:
        return "-"
    return num(raw)


# (header, alignment, value-getter -> display string). Widths are sized to data.
COLS = [
    ("MARKET", "<", lambda p: cell(p.get("marketDisplayName"))),
    ("SIDE", "<", lambda p: cell(p.get("side"))),
    ("SIZE", ">", lambda p: num(p.get("size"), 4)),
    ("ENTRY", ">", lambda p: num(p.get("averageEntryPrice"))),
    ("MARK", ">", mark_str),
    ("LEV", ">", lambda p: cell(p.get("leverage"))),
    ("MARGIN", "<", lambda p: cell(p.get("marginMode"))),
    ("NOTIONAL", ">", lambda p: num(p.get("positionValueNotional"))),
    ("uPnL", ">", lambda p: num(p.get("unrealizedPnl"))),
    ("FUNDING", ">", lambda p: num(funding_of(p))),
]

# --condensed CSV columns (one row per position, RAW values -- no commas/padding/totals), symmetric with the other
# display tools' --condensed. A SUPERSET of the human table: adds marginUsed / borrowedCapital / accountIndex that the
# table omits. `markPx` is the real mark field (see mark_p); funding = cumulativeFunding.sinceOpen (flattened).
CONDENSED_COLS = [
    ("marketDisplayName", lambda p: p.get("marketDisplayName")),
    ("side", lambda p: p.get("side")),
    ("size", lambda p: p.get("size")),
    ("averageEntryPrice", lambda p: p.get("averageEntryPrice")),
    ("markPx", mark_p),
    ("leverage", lambda p: p.get("leverage")),
    ("marginMode", lambda p: p.get("marginMode")),
    ("marginUsed", lambda p: p.get("marginUsed")),
    ("borrowedCapital", lambda p: p.get("borrowedCapital")),
    ("positionValueNotional", lambda p: p.get("positionValueNotional")),
    ("unrealizedPnl", lambda p: p.get("unrealizedPnl")),
    ("cumulativeFundingSinceOpen", funding_of),
    ("accountIndex", lambda p: p.get("accountIndex")),
]


def fetch_positions(address, market=None):
    """GET /v1/positions -> dict keyed by marketId; clean CLI errors on failure. `market` (display name or
    numeric id) applies the SERVER-SIDE market filter (v0.1.10.0); None = all markets (an unknown market -> 400)."""
    q = {"address": address}
    if market is not None:
        q["market"] = market
    query = urllib.parse.urlencode(q)
    # Shared retrying reader: Retry-After/backoff on 429 (incl. Cloudflare 1015) + 5xx, and require_dict
    # (so a non-object 2xx body is a clean error, not a .get AttributeError) -- robust for cron/ops.
    data = get_json_dict(f"{BASE}/v1/positions?{query}", "positions", "display_positions")
    positions = data.get("positions")
    if positions is None:
        # UNKNOWN != flat: a flat account returns an empty OBJECT {}, so a MISSING/null 'positions' is a
        # malformed/unreadable response, NOT "no open positions". Treating it as flat is fail-OPEN -- it would
        # print "0 open position(s)" and mislead ops. Fail closed (matches close_position's unknown!=flat).
        raise SystemExit("display_positions: 'positions' missing/null in /v1/positions response -- account state "
                         "UNKNOWN (NOT treating as flat). Retry.")
    if not isinstance(positions, dict):
        raise SystemExit("display_positions: unexpected 'positions' shape (expected object).")
    return {mid: p for mid, p in positions.items() if isinstance(p, dict)}   # drop non-dict values so downstream .get() can't crash


def main():
    global BASE
    parser = argparse.ArgumentParser(description="Display open positions for an account.")
    parser.add_argument("address", help="Ethereum address of the account to display")
    parser.add_argument("--market",
                        help="show only this market's position (display name or numeric marketId; "
                             "server-side filter, v0.1.10.0; default: all)")
    parser.add_argument("--condensed", action="store_true",
                        help="machine-readable CSV: one row per position with all fields "
                             "(market,side,size,entry,markPx,leverage,marginMode,marginUsed,borrowedCapital,"
                             "notional,uPnL,fundingSinceOpen,accountIndex), raw values, no totals")
    parser.add_argument("--header", action="store_true",
                        help="with --condensed, emit a CSV header row first "
                             "(error if used without --condensed)")
    parser.add_argument("--verbose", action="store_true",
                        help="show EVERY field of each position (incl. address/marginUsed/borrowedCapital + the nested "
                             "cumulativeFunding, which the default columns omit/flatten): an aligned key:value block "
                             "per position, or -- with --condensed -- one all-fields CSV row per position. Header "
                             "emitted ONCE (implied by --verbose; single fetch, so no #UPDATED_HEADER).")
    add_network_args(parser)
    args = parser.parse_args()
    BASE = NETWORKS[args.network]
    require_eth_address(args.address, "display_positions")
    if args.header and not args.condensed:
        raise SystemExit("display_positions: --header requires --condensed.")

    positions = sorted((p for p in fetch_positions(args.address, args.market).values() if isinstance(p, dict)),
                       key=market_id_key)   # drop any null/non-dict position value defensively
    if args.market:   # defensive local match (the server already filtered + 400s a typo); accepts name OR id
        want = str(args.market).upper()
        positions = [p for p in positions
                     if str(p.get("marketDisplayName", "")).upper() == want or str(p.get("marketId")) == str(args.market)]

    if args.condensed:
        if args.verbose:
            write_verbose_csv(positions)   # --verbose: EVERY field, header ONCE (nested cumulativeFunding as a blob)
            return
        # Raw values straight from the API, CSV-escaped, one row per position -- for pipes / files / spreadsheets.
        writer = csv.writer(sys.stdout, lineterminator="\n")
        if args.header:
            writer.writerow([h for h, _ in CONDENSED_COLS])
        for p in positions:
            writer.writerow(["" if get(p) is None else get(p) for _, get in CONDENSED_COLS])
        return

    scope = f" in {args.market}" if args.market else ""
    if args.verbose:   # block dump: EVERY field of each position (nested cumulativeFunding shown as a blob)
        print_verbose_records(positions, f"{len(positions)} open position(s){scope} for {args.address}", time_keys=frozenset())
        return
    print(f"{len(positions)} open position(s){scope} for {args.address}\n")
    if not positions:
        return

    widths = [max(len(h), max((len(get(p)) for p in positions), default=0))
              for h, _, get in COLS]
    header = "  ".join(f"{h:{a}{w}}" for (h, a, _), w in zip(COLS, widths))
    print(header)
    print("-" * len(header))
    for p in positions:
        print("  ".join(f"{get(p):{a}{w}}" for (_, a, get), w in zip(COLS, widths)))

    total_pnl = sum((dec(p.get("unrealizedPnl")) or Decimal(0) for p in positions), Decimal(0))
    total_funding = sum((dec(funding_of(p)) or Decimal(0) for p in positions), Decimal(0))
    print("-" * len(header))
    # Right-align both totals in a shared field so their cents line up vertically.
    NUMW = 20
    print(f"{'Total unrealized PnL:':21} {total_pnl:>{NUMW},.2f}")
    print(f"{'Total funding:':21} {total_funding:>{NUMW},.2f}")


if __name__ == "__main__":
    run_pipe_safe(main)
