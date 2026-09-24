#!/usr/bin/env python3
import sys
import math
import time
import json
import argparse
import urllib.error
import urllib.request
from decimal import Decimal, ROUND_DOWN, ROUND_UP, InvalidOperation
from arcus_common_public import add_network_args, run_pipe_safe, NETWORKS, markets_cache_path, positive_int, read_markets_cache, write_markets_cache, get_json_dict   # shared public helpers (formerly local copies)

# ── Constants ────────────────────────────────────────────────────────────────
MARKETS_URL = None   # set in main() from the required --testnet/--staging/--mainnet selector
MARKETS_CACHE = None
# Must match wsorderbook.py: HTTP port = PORT_BASE[network] + marketId.
PORT_BASE = {"mainnet": 10000, "testnet": 11000, "staging": 12000}
MAX_MARKET_ID = 999   # PORT_BASE entries are 1000 apart, so a marketId >= 1000 would push the port into
                      # the NEXT network's band (mainnet 10xxx / testnet 11xxx / staging 12xxx) -> wrong
                      # server. No wsorderbook port exists for such an id (mirrors dydxv4 MAX_CLOB_PAIR_ID).
# Launcher handoff cache written by showmarkets.py --createjson; read fail-open so this tool still
# works standalone when the file is absent. The launcher exports ARCUS_MARKETS_CACHE with a per-run
# path (so a foreign/stale file can't be trusted); we fall back to this NETWORK-scoped predictable
# path for manual use (testnet/staging/mainnet marketId maps differ). See wsorderbook.py for details.


# ── Market ID lookup ──────────────────────────────────────────────────────────
def fetch_market_id(market: str) -> int:
    """Resolve a market to its numeric marketId.

    Accepts a numeric marketId or a case-insensitively-matched display name, but
    EITHER WAY the market must exist in /v1/markets AND its id must fit the port
    band (0..MAX_MARKET_ID). A digit-only id that isn't a real market is a clean
    "not found" -- previously it was returned as-is without checking, yielding a
    wrong port (PORT_BASE + id) and a confusing "server not responding". An id
    beyond the band is a clean "out of range" (its port would collide with the
    next network's server). Network/parse/not-found failures become clean CLI
    errors rather than tracebacks.
    """
    numeric = market.isdigit() and market.isascii()   # isascii guard: exotic Unicode digits (², ⑤) pass
    if numeric:                                        # isdigit() but int() would raise -> treat as a name
        want_id = int(market)
        if not (0 <= want_id <= MAX_MARKET_ID):   # no valid port (incl. a negative id) -> reject before fetching markets
            raise SystemExit(f"showorderbook: marketId {want_id} out of range (0-{MAX_MARKET_ID}); "
                             "no wsorderbook port exists for it.")
    else:
        want_id = None
    data = read_markets_cache(MARKETS_CACHE)
    if data is None:                        # cache miss -> live fetch, then warm the cache. get_json_dict adds
        # what raw urlopen lacked: retries with Retry-After/backoff on 429 (incl. Cloudflare 1015) and 5xx, plus
        # require_dict (a non-object 2xx body -> clean error, not an AttributeError on data.get() below).
        data = get_json_dict(MARKETS_URL, "markets", "showorderbook")
        if not isinstance(data.get("markets"), list):
            raise SystemExit("showorderbook: unexpected /v1/markets response ('markets' missing or not a list).")
        write_markets_cache(MARKETS_CACHE, data)
    target = market.upper()
    for m in data.get("markets", []):
        try:
            mid = int(m["marketId"])
        except (KeyError, ValueError, TypeError):
            continue                        # skip a malformed entry rather than crash on int()
        if (numeric and mid == want_id) or (not numeric and str(m.get("marketDisplayName", "")).upper() == target):
            if not (0 <= mid <= MAX_MARKET_ID):   # a REAL market whose id is out of band (incl. NEGATIVE -> PORT_BASE+neg
                raise SystemExit(f"showorderbook: market '{market}' has marketId {mid} out of range "   # = wrong/foreign port)
                                 f"(0-{MAX_MARKET_ID}); no wsorderbook port exists for it.")
            return mid
    raise SystemExit(f"showorderbook: market '{market}' not found.")

# ── Crossed-price resolution ──────────────────────────────────────────────────
def _seq(entry):
    """Sequence id of a level (entry[2]); 0.0 if missing/non-numeric -- a malformed
    metadata field must not crash the display tool."""
    try:
        return float(entry[2])
    except (IndexError, ValueError, TypeError):
        return 0.0


def clean_levels(levels):
    """Drop rows that would traceback in the float() sort/crossed/display below: a usable level must be
    subscriptable with a numeric, FINITE price (lv[0]) and size (lv[1]). Short arrays, non-subscriptable rows,
    and non-numeric/NaN/Inf price|size are removed -- so a mangled feed or a foreign --server can't crash the
    tool, and a NaN can't poison the sort order or the spread%. Returns (clean_rows, dropped_count)."""
    clean, dropped = [], 0
    for lv in levels:
        try:
            p, s = float(lv[0]), float(lv[1])
        except (IndexError, KeyError, TypeError, ValueError):
            dropped += 1
            continue
        if not (math.isfinite(p) and math.isfinite(s)):
            dropped += 1
            continue
        if p <= 0 or s < 0:                      # a non-positive price or a negative size is not a real level ->
            dropped += 1                         # drop (mirrors the wsorderbook book-apply guard) so a mangled feed
            continue                             # can't render a nonsensical level in the displayed book
        # Normalize price/size to STRINGS: a NUMERIC price/size (a foreign --server / non-string feed) survives
        # the float() check above but crashes display's split_num (`"." in <float>` -> TypeError). clean_levels'
        # contract is "surviving rows won't traceback in display", so coerce here (str of a str is a no-op).
        clean.append([str(lv[0]), str(lv[1]), *lv[2:]])
    return clean, dropped


def _round_sig(price, sig, rounding):
    """Round a Decimal `price` to `sig` significant figures with the given rounding mode (client-side book
    aggregation mirroring the venue's REST l2 sigFigs). <=0 passes through."""
    if price <= 0:
        return price
    exp = price.adjusted() - (sig - 1)          # target exponent: 118466.1 (adjusted 5) at sig 2 -> exp 4 (nearest 10^4)
    return price.quantize(Decimal(1).scaleb(exp), rounding=rounding)


def aggregate_levels(levels, sig, round_down):
    """Merge levels whose price rounds to the same `sig` significant figures, summing sizes -- a client-side
    aggregated book view (like the venue's REST l2 sigFigs, which this tool can't use because it reads the
    locally-maintained wsorderbook book, not /v1/l2OrderBook). Bids round DOWN, asks round UP, so a bucket
    never prices better than the levels it holds (can't manufacture a cross). Input is already price-sorted
    and rounding is monotonic, so buckets keep that order. Output levels are [price, size] (the per-level
    seq is dropped -- a merged bucket has no single seq; display renders it as '?')."""
    rounding = ROUND_DOWN if round_down else ROUND_UP
    buckets, order = {}, []
    for lv in levels:
        try:
            price, size = Decimal(str(lv[0])), Decimal(str(lv[1]))
        except (InvalidOperation, IndexError, TypeError):
            continue
        key = f"{_round_sig(price, sig, rounding):f}"
        if key not in buckets:
            buckets[key] = Decimal(0)
            order.append(key)
        buckets[key] += size
    return [[k, f"{buckets[k]:f}"] for k in order]


def resolve_crosses(bids: list, asks: list):
    removed = []
    bi, ai = 0, 0
    while bi < len(bids) and ai < len(asks):
        if float(bids[bi][0]) < float(asks[ai][0]):
            break
        if _seq(bids[bi]) < _seq(asks[ai]):   # numeric compare, tolerant of bad ids
            removed.append(("bid", bids.pop(bi)))
        else:
            removed.append(("ask", asks.pop(ai)))
    return removed

# ── Display ───────────────────────────────────────────────────────────────────
def display(market: str, bids: list, asks: list, crossed=None, usd: bool = False):
    crossed = crossed or []   # avoid mutable-default-arg footgun

    def split_num(s):
        s = str(s)                     # defensive: a numeric price/size would TypeError on `"." in s` (clean_levels
        if "." in s:                   # normalizes, but any other caller stays safe too)
            i, d = s.split(".", 1)
            return i, "." + d
        return s, ""

    def col_widths_for(str_list):
        max_i = max_d = 0
        for s in str_list:
            i, d = split_num(s)
            max_i = max(max_i, len(i))
            max_d = max(max_d, len(d))
        return max_i, max_d

    def fmt(s, int_w, dec_w):
        i, d = split_num(s)
        return f"{i:>{int_w}}{d:<{dec_w}}"

    def fmt_cum(v):
        if usd:
            return f"${v:,.2f}"
        return f"{v:.8f}".rstrip('0').rstrip('.')

    def to_size_str(entry):
        v = float(entry[1])
        if usd:
            v *= float(entry[0])
            return f"${v:,.2f}"
        return f"{v:.8f}".rstrip('0').rstrip('.')

    # ── Pre-compute display strings ───────────────────────────────────────────
    bid_size_strs = [to_size_str(b) for b in bids]
    ask_size_strs = [to_size_str(a) for a in asks]

    cum_bid_strs, cum = [], 0.0
    for b in bids:
        v = float(b[1]) * (float(b[0]) if usd else 1.0)
        cum += v
        cum_bid_strs.append(fmt_cum(cum))

    cum_ask_strs, cum = [], 0.0
    for a in asks:
        v = float(a[1]) * (float(a[0]) if usd else 1.0)
        cum += v
        cum_ask_strs.append(fmt_cum(cum))

    # ── Column widths ─────────────────────────────────────────────────────────
    all_entries = bids + asks
    if not all_entries:
        pi, pd = 1, 0
    else:
        price_strs = [e[0] for e in all_entries]
        pi, pd = col_widths_for(price_strs)

    si, sd = col_widths_for(bid_size_strs + ask_size_strs) if (bid_size_strs or ask_size_strs) else (1, 0)
    cbi, cbd = col_widths_for(cum_bid_strs + cum_ask_strs) if (cum_bid_strs or cum_ask_strs) else (1, 0)

    col_w = pi + pd
    siz_w = si + sd
    cum_w = cbi + cbd

    def fmt_c(s):
        i, d = split_num(s)
        return f"{i:>{cbi}}{d:<{cbd}}"

    # ── Spread ────────────────────────────────────────────────────────────────
    if bids and asks:
        best_bid   = float(bids[0][0])
        best_ask   = float(asks[0][0])
        spread     = best_ask - best_bid
        midpoint   = (best_bid + best_ask) / 2
        if midpoint:                            # a 0 (or -0) midpoint from a 0-price book would ZeroDivisionError
            spread_str = f"  Spread: {spread:.1f} ({(spread / midpoint) * 100:.2f}%)"
        else:
            spread_str = f"  Spread: {spread:.1f} (pct n/a: midpoint 0)"
    else:
        spread_str = ""

    # ── Print ─────────────────────────────────────────────────────────────────
    header = (
        f"{'CBidS':>{cum_w}}  {'BidS':>{siz_w}}  {'BidP':>{col_w}}  "
        f"{'AskP':<{col_w}}  {'AskS':<{siz_w}}  {'CAskS':<{cum_w}}"
    )
    separator = "-" * len(header)

    print(f"\n  Orderbook: {market}{spread_str}\n")
    print(header)
    print(separator)

    rows = max(len(bids), len(asks))
    for i in range(rows):
        if i < len(bids):
            b_cum   = fmt_c(cum_bid_strs[i])
            b_size  = fmt(bid_size_strs[i], si, sd)
            b_price = fmt(bids[i][0], pi, pd)
        else:
            b_cum   = " " * cum_w
            b_size  = " " * siz_w
            b_price = " " * col_w

        if i < len(asks):
            a_price = fmt(asks[i][0], pi, pd)
            a_size  = fmt(ask_size_strs[i], si, sd)
            a_cum   = fmt_c(cum_ask_strs[i])
        else:
            a_price = " " * col_w
            a_size  = " " * siz_w
            a_cum   = " " * cum_w

        print(f"{b_cum}  {b_size}  {b_price}  {a_price}  {a_size}  {a_cum}")

    print(separator)
    print(f"  {len(bids)} bids   {len(asks)} asks")
    for side, entry in crossed:
        seq = entry[2] if len(entry) > 2 else "?"
        print(f"  Crossed {side} removed: {entry[0]} {entry[1]} {seq}")
    print()

# ── Entry point ───────────────────────────────────────────────────────────────
def check_server(url):
    """Verify the orderbook HTTP server is responding, then exit.

    Any HTTP response -- including an error status like 503 'not ready' -- means
    the server is up and answering, so we exit 0. Only a failure to get an HTTP
    response at all (connection refused, DNS failure, timeout) is treated as the
    server being down -> exit 1. The orderbook payload itself is not inspected.
    """
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            print(f"server responding: HTTP {r.status} at {url}")
            sys.exit(0)
    except urllib.error.HTTPError as e:
        print(f"server responding: HTTP {e.code} at {url}")
        sys.exit(0)
    except urllib.error.URLError as e:
        print(f"server not responding at {url}: {e.reason}")
        sys.exit(1)
    except (TimeoutError, OSError) as e:
        print(f"server not responding at {url}: {e}")
        sys.exit(1)


def fetch_book(url):
    """Fetch the orderbook, or return None (after printing why) on error / not-ready.

    The wsorderbook server returns HTTP 503 {"ready": false} while warming up or
    resyncing (and may, depending on proxies, send 200 with ready=false). Either
    way we report 'not ready' rather than crash or show a stale/empty book.
    """
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            data = json.loads(r.read())
    except urllib.error.HTTPError as e:
        if e.code == 503:
            print(f"orderbook not ready yet (HTTP 503, warming up/resyncing) at {url}")
        else:
            print(f"orderbook query failed: HTTP {e.code} at {url}")
        return None
    except urllib.error.URLError as e:
        print(f"could not reach {url}: {e.reason}")
        return None
    except (TimeoutError, OSError) as e:
        print(f"network error querying {url}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"invalid JSON from {url}: {e}")
        return None
    if not isinstance(data, dict):             # 200 but body isn't a JSON object -> don't crash on .get
        print(f"unexpected orderbook response shape (not a JSON object) at {url}")
        return None
    if data.get("ready") is False:             # 200 but explicitly flagged not-ready
        print(f"orderbook not ready yet (ready=false) at {url}")
        return None
    # bids/asks must be LISTS before main() does .sort() on them. data.get("bids", []) does NOT protect
    # against "bids": null (present-but-None returns None, not the default) or a non-list -> .sort() would
    # AttributeError. Treat a malformed shape as not-usable (clean message + retry/exit), like the checks above.
    if not isinstance(data.get("bids"), list) or not isinstance(data.get("asks"), list):
        print(f"unexpected orderbook response shape (bids/asks missing or not lists) at {url}")
        return None
    return data


def main():
    global MARKETS_URL, MARKETS_CACHE
    parser = argparse.ArgumentParser(description="Display orderbook for a market.")
    parser.add_argument("market",                                     help="Market symbol (e.g. BTC-USD) or numeric marketId")
    parser.add_argument("--server",  default="localhost",             help="Orderbook server host (default: localhost)")
    parser.add_argument("--nlevels", type=positive_int, default=None,  help="Number of price levels to display (default: all)")
    parser.add_argument("--sig-figs", type=positive_int, default=None, metavar="N",
                        help="aggregate the book for display: merge levels whose price rounds to N significant "
                             "figures (bids round down / asks round up), summing sizes -- like the venue's l2 sigFigs")
    parser.add_argument("--looping", action="store_true",             help="Continuously refresh the orderbook")
    parser.add_argument("--usd",     action="store_true",             help="Show size columns in USD value")
    parser.add_argument("--checkserver", action="store_true",         help="Only verify the orderbook HTTP server responds, then exit (0=up, 1=down)")
    add_network_args(parser, verb="resolve the market against")
    args = parser.parse_args()
    MARKETS_URL = NETWORKS[args.network] + "/v1/markets"
    MARKETS_CACHE = markets_cache_path(args.network)

    market = args.market
    host   = args.server

    market_id = fetch_market_id(market)
    port      = PORT_BASE[args.network] + market_id
    url       = f"http://{host}:{port}/orderbook"

    if args.checkserver:
        check_server(url)   # connects, prints status, and exits

    while True:
        data = fetch_book(url)
        if data is None:                       # not ready / transient error
            if not args.looping:
                sys.exit(1)
            time.sleep(1)
            continue

        bids = data.get("bids", [])
        asks = data.get("asks", [])

        # Row-level hardening: drop any malformed level (short array, non-subscriptable, non-numeric/NaN/Inf
        # price or size) BEFORE the float() sort/crossed/display below, so a mangled feed or a foreign --server
        # can't traceback the tool. (fetch_book already guaranteed bids/asks are LISTS; this validates ROWS.)
        bids, bdrop = clean_levels(bids)
        asks, adrop = clean_levels(asks)
        if bdrop or adrop:
            print(f"  note: dropped {bdrop} malformed bid + {adrop} malformed ask level(s)")

        bids.sort(key=lambda x: float(x[0]), reverse=True)
        asks.sort(key=lambda x: float(x[0]))

        crossed = resolve_crosses(bids, asks)

        if args.sig_figs is not None:                     # client-side sigFigs aggregation (crosses resolved first)
            bids = aggregate_levels(bids, args.sig_figs, round_down=True)
            asks = aggregate_levels(asks, args.sig_figs, round_down=False)

        if args.nlevels is not None:
            bids = bids[:args.nlevels]
            asks = asks[:args.nlevels]

        if args.looping and sys.stdout.isatty():
            print("\033[2J\033[H", end="")

        display(market, bids, asks, crossed, args.usd)

        if not args.looping:
            break

        time.sleep(1)

if __name__ == "__main__":
    run_pipe_safe(main)
