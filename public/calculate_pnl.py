#!/usr/bin/env python3
"""Compute FIFO realized/unrealized PnL for ONE arcus market from a fills file.

Input is the --condensed output of display_fills.py (CSV), e.g.:
    python3 display_fills.py 0xADDR --market BTC-USD --limit unlimited --condensed --mainnet > fills.txt
    python3 calculate_pnl.py fills.txt --mainnet
The file (or stdin: pass '-' or pipe) has one fill per line in display_fills CONDENSED_KEYS order:
    createdAt,marketDisplayName,side,size,price,fee,role,closedPnl,positionEffect,tradeId,orderId,liquidationMethod,liquidatedUser
Only createdAt(0) marketDisplayName(1) side(2) size(3) price(4) fee(5) are used; a --header row
is skipped. Rows are ordered OLDEST-first (by createdAt, epoch microseconds) for FIFO matching.
(arcus fills also carry the API's own per-fill closedPnl, but this tool computes FIFO independently
so the result doesn't depend on the venue's accounting -- useful as a cross-check.)

Output: a human-readable summary by default. With --condensed, one comma-delimited (CSV) line
intended for other programs to parse (arcus market names never contain commas, unlike dydxv4
tickers which force pipe) --
    openRemaining,market,avgOpenPrice,realizedPnL,latestPrice,unrealizedPnL,totalFees,totalVolume
      openRemaining  net remaining position after FIFO (signed: + long, - short), in coin
      avgOpenPrice   size-weighted avg price of the REMAINING open lots only (0 if flat)
      realizedPnL    FIFO realized PnL, gross/price-only (fees are NOT deducted; see totalFees)
      latestPrice    oraclePrice from /v1/markets (or the --price override)
      unrealizedPnL  (latestPrice - avgOpenPrice) * openRemaining
      totalFees      sum of every fill's fee
      totalVolume    sum of size*price over every fill -- notional in USD, NOT coin quantity

IMPORTANT -- the fills file must be COMPLETE from the position's inception for FIFO to be correct.
display_fills defaults to the newest 1000; use `--limit unlimited` (and/or --from/--to) so the file
holds the whole history, otherwise FIFO treats the oldest fill in the file as the position's start.

All arithmetic is exact Decimal (no float rounding noise). Exactly one of --mainnet/--testnet/
--staging/--price is required (mutually exclusive): a network to fetch the live oraclePrice (an
unsigned public read), or --price to supply the latest price directly (a fully offline run).
Stdlib only.
"""

import argparse
import csv
import os
import sys
from collections import deque
from decimal import Decimal
from functools import partial
from arcus_common_public import add_network_args, run_pipe_safe, NETWORKS, dec, get_json_dict   # shared public helpers (formerly local copies)

get_json = partial(get_json_dict, prog="calculate_pnl")   # get_json + require_dict, this tool's prog


# display_fills --condensed CONDENSED_KEYS positions.
C_CREATED, C_MARKET, C_SIDE, C_SIZE, C_PRICE, C_FEE = 0, 1, 2, 3, 4, 5
MIN_FIELDS = C_FEE + 1     # need at least through the fee column


def price_arg(s):
    d = dec(s)
    if d is None or d <= 0:
        raise argparse.ArgumentTypeError("must be a positive number")
    return d


def fmt_num(d, sign=False):
    """Human-readable Decimal: comma-grouped, up to 8 dp, trailing zeros stripped. Exact zero
    renders as '0' (no +/- sign). Used for PRICES and position size (sub-penny prices kept)."""
    if d == 0:
        return "0"
    s = f"{d:+,.8f}" if sign else f"{d:,.8f}"
    return s.rstrip("0").rstrip(".") if "." in s else s


def fmt_cents(d, sign=False):
    """Dollar amount rounded to cents (2 dp), comma-grouped; avoids -0.00/+0.00. Used for PnL,
    fees, and volume -- NOT for prices (which can be fractions of a cent)."""
    s = f"{d:+,.2f}" if sign else f"{d:,.2f}"
    return "0.00" if s.lstrip("+-") == "0.00" else s


def print_human(market, remaining, avg, realized, latest, unrealized, fees, volume, price_src):
    """Readable summary (default output). --condensed emits the machine line instead."""
    base = market.split("-")[0] if "-" in market else market
    print()
    print(f"  FIFO PnL - {market}   (latest price {fmt_num(latest)} via {price_src})")
    print()
    if remaining == 0:
        print("  Open position remaining   flat")
        print("  Average open price        -")
    else:
        side = "long" if remaining > 0 else "short"
        print(f"  Open position remaining   {fmt_num(remaining)} {base} ({side})")
        print(f"  Average open price        {fmt_num(avg)}")
    print(f"  Realized PnL (FIFO)       {fmt_cents(realized, sign=True)}")
    print(f"  Unrealized PnL (FIFO)     {fmt_cents(unrealized, sign=True)}")
    print(f"  Total fees                {fmt_cents(fees)}")
    print(f"  Total volume (USD)        {fmt_cents(volume)}")
    print()


def _parse_fill(parts, lineno, want_market):
    """Parse ONE CSV row -> a fill dict, or None to skip (empty / --header / malformed / off-market /
    non-positive size or price). Warns to stderr on a genuinely malformed row (never on a header or an
    off-market filter). createdAt is kept as an int (epoch microseconds)."""
    if not parts or not any(c.strip() for c in parts):
        return None
    if len(parts) < MIN_FIELDS:
        print(f"calculate_pnl: line {lineno}: too few fields ({len(parts)} < {MIN_FIELDS}), skipping",
              file=sys.stderr)
        return None
    side = parts[C_SIDE].strip().upper()
    size, price, fee = dec(parts[C_SIZE]), dec(parts[C_PRICE]), dec(parts[C_FEE])
    market, created_s = parts[C_MARKET].strip(), parts[C_CREATED].strip()
    if side not in ("BUY", "SELL") or size is None or price is None:
        if parts[C_CREATED].strip().lower() == "createdat":     # a --header row -> skip quietly
            return None
        print(f"calculate_pnl: line {lineno}: unparseable side/size/price, skipping", file=sys.stderr)
        return None
    if want_market and market != want_market:
        return None
    # A real trade has size > 0 AND price > 0. dec() accepts "0"/"-5" (finite but <= 0), which would
    # corrupt FIFO: a negative size FLIPS the fill's side (q = size for a BUY), and a 0/negative price
    # gives garbage realized PnL + volume. Skip non-positive rows (size/price are both non-None here).
    if size <= 0 or price <= 0:
        print(f"calculate_pnl: line {lineno}: non-positive size/price ({size}/{price}), skipping",
              file=sys.stderr)
        return None
    try:
        created = int(created_s)
    except (TypeError, ValueError):
        created = -1                                            # unparseable time -> sort oldest
    return {"created": created, "market": market, "side": side,
            "size": size, "price": price, "fee": fee if fee is not None else Decimal(0)}


def read_fills(fh, want_market):
    """BUFFERED parse (stdin / the non-monotonic fallback): CSV rows -> (fills list, markets_seen).
    O(rows) memory -- the streaming path (reversed_lines + compute_pnl) avoids this for a seekable file."""
    fills, markets = [], set()
    for lineno, parts in enumerate(csv.reader(fh), 1):
        f = _parse_fill(parts, lineno, want_market)
        if f is not None:
            fills.append(f)
            markets.add(f["market"])
    return fills, markets


def parsed_fills(line_iter, want_market):
    """GENERATOR: parse a stream of CSV lines -> fill dicts (skipping bad rows). Feeds compute_pnl so a
    huge file is processed one row at a time instead of being buffered into a list."""
    for lineno, parts in enumerate(csv.reader(line_iter), 1):
        f = _parse_fill(parts, lineno, want_market)
        if f is not None:
            yield f


def scan_markets(path, want_market):
    """Cheap FORWARD pass -> the set of distinct markets among trade rows (O(#markets) memory, no dicts,
    no warnings). Only needed to auto-detect the market when --market is not given."""
    markets = set()
    with open(path) as fh:
        for parts in csv.reader(fh):
            if len(parts) < MIN_FIELDS:
                continue
            if parts[C_SIDE].strip().upper() not in ("BUY", "SELL"):   # skips header + malformed quietly
                continue
            m = parts[C_MARKET].strip()
            if not want_market or m == want_market:
                markets.add(m)
    return markets


def reversed_lines(path, chunk=1 << 20):
    """Yield the lines of `path` from LAST to FIRST without loading the whole file (bounded memory: one
    ~chunk block at a time). A newest-first display_fills file read last-line-first is OLDEST-first --
    exactly the order FIFO needs -- so PnL streams in O(open position) memory, not O(file)."""
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        pos = f.tell()
        tail = b""
        while pos > 0:
            step = min(chunk, pos)
            pos -= step
            f.seek(pos)
            block = f.read(step) + tail
            parts = block.split(b"\n")
            tail = parts[0]                 # parts[0] may be a partial line -> hold it for the next (earlier) block
            for ln in reversed(parts[1:]):
                yield ln.decode("utf-8", "replace")
        if tail:
            yield tail.decode("utf-8", "replace")


def resolve_market(markets):
    """Pick the single market from the detected set, or a clean error (mirrors the original main)."""
    if len(markets) == 1:
        return next(iter(markets))
    if not markets:
        raise SystemExit("calculate_pnl: no fills parsed from input.")
    raise SystemExit("calculate_pnl: input mixes multiple markets "
                     f"({', '.join(sorted(markets))}); pass --market to pick one.")


def fifo_step(lots, realized, q, p):
    """Apply one signed fill (qty q at price p) to the FIFO `lots` deque; return the updated realized PnL.
    Closes opposing lots oldest-first; a leftover that extends the position COLLAPSES into the last lot when
    it shares that lot's price -- a run of same-price fills is ONE entry, not many, so the deque stays bounded
    by the number of open price levels, not the fill count. Output is identical to per-fill lots (adjacent
    same-price lots are indistinguishable to FIFO)."""
    while q != 0 and lots and (lots[0][0] > 0) != (q > 0):
        lot = lots[0]
        close = min(abs(lot[0]), abs(q))
        realized += (p - lot[1]) * close if lot[0] > 0 else (lot[1] - p) * close
        lot[0] -= close if lot[0] > 0 else -close
        q -= close if q > 0 else -close
        if lot[0] == 0:
            lots.popleft()
    if q != 0:                                   # leftover opens/extends the position (same sign as any lots)
        if lots and lots[-1][1] == p:            # same-price run -> merge into the last lot instead of appending
            lots[-1][0] += q
        else:
            lots.append([q, p])
    return realized


class NotMonotonic(Exception):
    """compute_pnl raises this when createdAt is not non-decreasing -- the input is NOT in display_fills
    (newest-first) order, so a backward stream can't assume oldest-first. The caller falls back to a
    buffered sort (correct for any input)."""


def compute_pnl(fills, guard_monotonic):
    """ONE pass over `fills` (an iterable of fill dicts, OLDEST-first) computing FIFO realized PnL +
    fee/volume totals with O(1) memory aside from the open-lot deque. Returns
    (realized, remaining, avg, total_fees, total_volume, market, n). Raises NotMonotonic when
    guard_monotonic and a createdAt decreases (input not newest-first)."""
    lots = deque()                              # each: [signed_qty, price]; all entries share the position's sign
    realized = total_fees = total_volume = Decimal(0)
    market, n, prev = None, 0, None
    for f in fills:
        c = f["created"]
        if guard_monotonic and prev is not None and c < prev:
            raise NotMonotonic
        prev, market, n = c, f["market"], n + 1
        q = f["size"] if f["side"] == "BUY" else -f["size"]
        realized = fifo_step(lots, realized, q, f["price"])
        total_fees += f["fee"]
        total_volume += f["size"] * f["price"]
    remaining = sum((lot[0] for lot in lots), Decimal(0))
    avg = sum((lot[0] * lot[1] for lot in lots), Decimal(0)) / remaining if remaining != 0 else Decimal(0)
    return realized, remaining, avg, total_fees, total_volume, market, n


def fetch_oracle_price(base, market):
    """oraclePrice for `market` from /v1/markets (a list keyed by marketDisplayName), or clean exit."""
    data = get_json(f"{base}/v1/markets", "markets")
    markets = data.get("markets") if isinstance(data, dict) else None
    if not isinstance(markets, list):
        raise SystemExit("calculate_pnl: unexpected /v1/markets response (no 'markets' list).")
    m = next((x for x in markets if isinstance(x, dict)
              and str(x.get("marketDisplayName")) == market), None)
    if m is None:
        raise SystemExit(f"calculate_pnl: market {market!r} not found in /v1/markets.")
    op = dec(m.get("oraclePrice"))
    if op is None or op <= 0:      # dec() rejects non-finite; also reject <= 0 (a 0/negative oracle -> garbage unrealized PnL)
        raise SystemExit(f"calculate_pnl: no usable oraclePrice for {market!r} (got {m.get('oraclePrice')!r}).")
    return op


def main():
    p = argparse.ArgumentParser(description="FIFO PnL for one arcus market from a fills file.")
    p.add_argument("file", nargs="?", default="-",
                   help="fills file (display_fills --condensed output); '-' or omitted = stdin")
    p.add_argument("--market",
                   help="market to compute (marketDisplayName; required only if the file mixes markets)")
    p.add_argument("--condensed", action="store_true",
                   help="emit one machine-readable comma-delimited (CSV) line (for other programs) "
                        "instead of the human-readable summary")
    p.add_argument("--header", action="store_true",
                   help="with --condensed, emit a CSV header row first "
                        "(error if used without --condensed)")
    # Exactly one price SOURCE (mutually exclusive, required): a network to fetch the live
    # oraclePrice, or --price to supply it directly (a fully offline run). Everything else in the
    # output is computed from the file alone -- only latestPrice/unrealizedPnL need a price.
    src = add_network_args(p, verb="fetch the latest price from")
    src.add_argument("--price", type=price_arg, metavar="PRICE",
                     help="use this exact latest price for unrealized PnL (no network fetch)")
    args = p.parse_args()
    if args.header and not args.condensed:
        raise SystemExit("calculate_pnl: --header requires --condensed.")

    want = args.market
    result = None                                # (realized, remaining, avg, total_fees, total_volume, market)

    if args.file != "-":
        # FILE: STREAM it BACKWARD -- a newest-first display_fills file read last-line-first is OLDEST-first,
        # so FIFO runs in O(open position) memory instead of loading + sorting ~GBs for a 5M+ fill file. Only
        # scan for the market when --market isn't given (cheap forward pass). If the input turns out NOT to be
        # newest-first, fall back to the buffered sort below (correct for any input).
        try:
            market = want or resolve_market(scan_markets(args.file, want))
            r = compute_pnl(parsed_fills(reversed_lines(args.file), market), guard_monotonic=True)
            if r[6] == 0:                        # n == 0 -> nothing matched
                raise SystemExit("calculate_pnl: no fills parsed from input"
                                 + (f" for market {market}" if want else "") + ".")
            realized, remaining, avg, total_fees, total_volume = r[0], r[1], r[2], r[3], r[4]
            result = (realized, remaining, avg, total_fees, total_volume, market)
        except NotMonotonic:
            print("calculate_pnl: WARNING input is not in display_fills (newest-first) order; falling back "
                  "to a buffered sort (memory grows with the file).", file=sys.stderr)
        except OSError as e:
            raise SystemExit(f"calculate_pnl: cannot read {args.file}: {e}")

    if result is None:
        # STDIN (not seekable) or the non-monotonic fallback: buffered read + sort (original behaviour).
        if args.file == "-":
            if sys.stdin.isatty():
                raise SystemExit("calculate_pnl: no input file given and stdin is a tty (see --help).")
            fills, markets = read_fills(sys.stdin, want)
        else:
            with open(args.file) as fh:
                fills, markets = read_fills(fh, want)
        if not fills:
            raise SystemExit("calculate_pnl: no fills parsed from input"
                             + (f" for market {want}" if want else "") + ".")
        market = want or resolve_market(markets)
        # Oldest-first for FIFO: reverse (display_fills emits newest-first) then a stable sort by createdAt,
        # so ties keep the reversed (oldest-first) order and non-sorted input is still fixed.
        fills.reverse()
        fills.sort(key=lambda f: f["created"])
        r = compute_pnl(iter(fills), guard_monotonic=False)
        realized, remaining, avg, total_fees, total_volume = r[0], r[1], r[2], r[3], r[4]
        result = (realized, remaining, avg, total_fees, total_volume, market)

    realized, remaining, avg, total_fees, total_volume, market = result
    latest = args.price if args.price is not None else fetch_oracle_price(NETWORKS[args.network], market)
    unrealized = (latest - avg) * remaining          # signed remaining handles long/short

    if args.condensed:
        # comma-CSV via csv.writer (quotes defensively). Matches display_fills --condensed. (arcus marketDisplayNames
        # never contain commas; dydxv4 switched pipe->comma too 2026-09-03, so both toolkits are comma-CSV now.)
        w = csv.writer(sys.stdout, lineterminator="\n")
        if args.header:
            w.writerow(["remaining", "market", "avgOpenPrice", "realizedPnl",
                        "latestPrice", "unrealizedPnl", "totalFees", "totalVolume"])
        w.writerow([remaining, market, avg, realized, latest, unrealized, total_fees, total_volume])
    else:
        price_src = "--price override" if args.price is not None else f"{args.network} oracle"
        print_human(market, remaining, avg, realized, latest, unrealized, total_fees, total_volume, price_src)


if __name__ == "__main__":
    run_pipe_safe(main)
