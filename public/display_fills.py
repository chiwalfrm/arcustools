"""
Display fills (trade history) for an account, newest-first.

  python3 display_fills.py <eth_address>                     # latest fills (up to 1000)
  python3 display_fills.py <eth_address> --market BTC-USD     # only that market
  python3 display_fills.py <eth_address> --from 1782000000000000 --to 1782600000000000
  python3 display_fills.py <eth_address> --limit 50 --condensed
  python3 display_fills.py <eth_address> --limit unlimited     # page the full history
  python3 display_fills.py <eth_address> --verbose             # EVERY field per fill (key:value block)
  python3 display_fills.py <eth_address> --condensed --verbose # EVERY field, one CSV row per fill (streams)

Uses GET /v1/fills (fill/trade history). This is a public, account-scoped read -- it
takes only the `address` query parameter and needs NO signature, so this display tool
needs just the address, not the creds file. --market is resolved to a canonical display
name and pushed to the SERVER-SIDE market filter (v1.2.1), so only that market's fills are
fetched/paged (with a defensive local match too); use --from/--to (epoch MICROseconds -- the
createdAt unit, NOT ms) to walk older history.
Output is sorted newest-first locally (not trusting API ordering).
"""

import argparse
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal
from functools import partial
from arcus_common_public import stream_condensed_pages, stream_verbose_csv, print_verbose_records, TABLE_MAX_ROWS, add_network_args, require_eth_address, run_pipe_safe, NETWORKS, UNLIMITED, cell, created_key, dec, dedup_vs_previous_page, id_or_json_key, epoch_us_arg, get_json_dict, limit_arg, page_pace_delay, resolve_market_name, when   # shared public helpers (formerly local copies)

_gjd = partial(get_json_dict, prog="display_fills")   # get_json + require_dict, this tool's prog
INFINITE_RETRY = False    # set by --infinite-retry: retry TRANSIENT fetch failures forever (never give up on a huge export)


def _get_json(url, what, **kw):
    """Wrap _gjd to inject this tool's --infinite-retry flag (read at call time, so main can flip it after arg
    parsing). Every _get_json(...) in this tool then honours --infinite-retry."""
    return _gjd(url, what, infinite=INFINITE_RETRY, **kw)


BASE = None   # set in main() from the required --testnet/--staging/--mainnet selector
PAGE_SIZE = 1000          # API max per request; page size used by --limit unlimited

# Fields emitted by --condensed (raw, one CSV row per fill; no header, per the display_* convention).
# liquidationMethod/liquidatedUser (the v1.3.5 forced-fill marker) are APPENDED at the END so positional
# consumers of the original 11 columns are unaffected (e.g. calculate_pnl reads only cols 0-5).
CONDENSED_KEYS = [
    "createdAt", "marketDisplayName", "side", "size", "price", "fee",
    "role", "closedPnl", "positionEffect", "tradeId", "orderId",
    "liquidationMethod", "liquidatedUser",
]


def _annotate_liq(f):
    """Flatten the v1.3.5 forced-fill marker -- a nested `liquidation` object {method: LIQUIDATION|ADL,
    liquidatedUser} present on the liquidated account's leg of a liquidation and the deleveraged
    counterparty's leg of an ADL, absent on voluntary fills -- into two top-level fields so the condensed
    CSV (via row.get(k)) and the table can surface it. '' on a voluntary fill. Mutates and returns f."""
    liq = f.get("liquidation")
    liq = liq if isinstance(liq, dict) else {}
    f["liquidationMethod"] = liq.get("method", "")
    f["liquidatedUser"] = liq.get("liquidatedUser", "")
    return f


def fetch_fills(address, limit, from_us=None, to_us=None, market=None, role=None, side=None):
    """GET /v1/fills (newest-first). Validates the response shape and returns the fills list.
    from_us/to_us are epoch MICROseconds (the /v1/fills from/to unit, same as createdAt). `market` is
    the canonical marketDisplayName for the SERVER-SIDE market filter (v1.2.1); None = all markets."""
    q = {"address": address, "limit": limit}
    if from_us is not None:
        q["from"] = from_us
    if to_us is not None:
        q["to"] = to_us
    if market is not None:
        q["market"] = market
    if role is not None:
        q["role"] = role
    if side is not None:
        q["side"] = side
    data = _get_json(f"{BASE}/v1/fills?{urllib.parse.urlencode(q)}", "fills")
    fills = data.get("fills")
    if fills is None:
        return []
    if not isinstance(fills, list):
        raise SystemExit("display_fills: unexpected /v1/fills response ('fills' is not a list).")
    return [_annotate_liq(f) for f in fills if isinstance(f, dict)]   # drop non-dicts; flatten the v1.3.5 liquidation marker


def iter_fills_pages(address, from_us=None, to_us=None, market=None, role=None, side=None):
    """--limit unlimited, as a GENERATOR: yield each page's FRESH (deduped) fills LIST, newest-first, as it
    pages BACKWARD via the `to` cursor -- so a caller can STREAM output instead of buffering the whole history
    before the first line. /v1/fills has only from/to bounds; `from` (if set) is a SERVER-SIDE lower bound so
    paging terminates at it. from/to/createdAt are all epoch MICROseconds (same unit), so the cursor is the
    oldest createdAt directly -- NO conversion. `to` is INCLUSIVE (closes over the microsecond it names), so the
    boundary fill re-reads next page; dedup by tradeId drops it. `market` (canonical marketDisplayName) applies
    the SERVER-SIDE market filter (v1.2.1) so only that market's fills page -- far cheaper than fetching all and
    filtering locally. fetch_fills_all() is just this, flattened."""
    # Dedup keys from the PREVIOUS page ONLY -- bounded memory. `to` is inclusive so page N+1 re-reads only the
    # boundary microsecond, and the cursor STRICTLY decreases (nc >= cursor breaks), so page N+1 can overlap only
    # page N. A global seen-set would grow to O(total fills) (~6M ids ≈ 600MB, swap-thrash on a small host).
    prev_keys, cursor = set(), to_us
    first = True
    while True:
        q = {"address": address, "limit": PAGE_SIZE}
        if from_us is not None:
            q["from"] = from_us
        if cursor is not None:
            q["to"] = cursor
        if market is not None:
            q["market"] = market
        if role is not None:
            q["role"] = role
        if side is not None:
            q["side"] = side
        # Pace pages AFTER the first: a full 1000-row /v1/fills page costs ~70 IP-weight and the per-IP
        # bucket refills 25/s, so unpaced back-to-back paging drives the bucket negative -> 429. The first
        # page rides the already-full bucket (no pause).
        data = _get_json(f"{BASE}/v1/fills?{urllib.parse.urlencode(q)}", "fills",
                         delay=(0.0 if first else page_pace_delay()))
        first = False
        fills = data.get("fills")
        if fills is None:
            break
        if not isinstance(fills, list):
            raise SystemExit("display_fills: unexpected /v1/fills response ('fills' is not a list).")
        if not fills:
            break
        # Dedup by tradeId (id-less fill -> full-row JSON) against the PREVIOUS page only: the inclusive `to`
        # cursor re-reads just the boundary microsecond, which strictly decreases, so page N+1 overlaps ONLY
        # page N -- bounded to ~one page, not O(total). (See dedup_vs_previous_page.)
        fresh, prev_keys = dedup_vs_previous_page(fills, prev_keys, lambda f: id_or_json_key(f, "tradeId"))
        if fresh:
            yield [_annotate_liq(f) for f in fresh]   # STREAM this page's fresh fills (liquidation marker flattened)
        if len(fills) < PAGE_SIZE:      # fewer than a full page -> reached the oldest fill / `from`
            break
        if not fresh:                    # no new tradeIds -> stop, never loop
            break
        positives = [c for c in (created_key(f) for f in fills if isinstance(f, dict)) if c > 0]
        if not positives:                # nothing with a usable timestamp to advance the cursor
            break
        nc = min(positives)              # oldest createdAt (epoch MICROseconds) -> `to` for the next page, inclusive
        if cursor is not None and nc >= cursor:    # cursor didn't decrease -> avoid an infinite loop
            break
        cursor = nc


def fetch_fills_all(address, from_us=None, to_us=None, market=None, role=None, side=None):
    """--limit unlimited: page /v1/fills to completeness, COLLECTED into one list (for the table path, which
    needs the full set for column widths + the totals footer). Streaming callers use iter_fills_pages()."""
    return [f for page in iter_fills_pages(address, from_us, to_us, market, role, side) for f in page]


def print_table(fills, address, label, note):
    """Aligned table with column widths sized to the data, plus a fee/PnL totals footer."""
    # (header, alignment, value-getter). closedPnl/positionEffect are optional on REST -> "".
    cols = [
        ("CREATED (UTC)", "<", lambda f: when(f.get("createdAt"))),
        ("MARKET", "<", lambda f: cell(f.get("marketDisplayName"))),
        ("SIDE", "<", lambda f: cell(f.get("side"))),
        ("SIZE", ">", lambda f: cell(f.get("size"))),
        ("PRICE", ">", lambda f: cell(f.get("price"))),
        ("FEE", ">", lambda f: cell(f.get("fee"))),
        ("ROLE", "<", lambda f: cell(f.get("role"))),
        ("CLOSEDPNL", ">", lambda f: cell(f.get("closedPnl"))),
        ("EFFECT", "<", lambda f: cell(f.get("positionEffect"))),
        ("LIQ/ADL", "<", lambda f: cell(f.get("liquidationMethod"))),   # v1.3.5 forced-fill marker ('' = voluntary)
        ("TRADE ID", "<", lambda f: cell(f.get("tradeId"))),
        ("ORDER ID", "<", lambda f: cell(f.get("orderId"))),
    ]
    widths = [max(len(h), max((len(get(f)) for f in fills), default=0)) for h, _, get in cols]

    print(f"{len(fills)} fill(s) [{label}] for {address}{note}\n")
    head = "  ".join(f"{h:{a}{w}}" for (h, a, _), w in zip(cols, widths))
    print(head)
    print("-" * len(head))
    for f in fills:
        print("  ".join(f"{get(f):{a}{w}}" for (_, a, get), w in zip(cols, widths)))

    total_fee = sum((dec(f.get("fee")) or Decimal(0)) for f in fills)
    total_pnl = sum((dec(f.get("closedPnl")) or Decimal(0)) for f in fills)
    print("-" * len(head))
    print(f"  TOTAL  fees {total_fee:,.6f}   realized PnL {total_pnl:,.6f}   over {len(fills)} fill(s)")


def main():
    global BASE, INFINITE_RETRY
    parser = argparse.ArgumentParser(description="Display account fills (trade history).")
    parser.add_argument("address", help="Ethereum address of the account to display")
    parser.add_argument("--market",
                        help="show only fills in this market (display name or marketId; default: all)")
    parser.add_argument("--role", choices=["MAKER", "TAKER"],
                        help="show only fills with this liquidity role (server-side filter, v1.4.5)")
    parser.add_argument("--side", choices=["BUY", "SELL"],
                        help="show only fills on this side (server-side filter, v1.6.0)")
    parser.add_argument("--limit", type=limit_arg, default=1000, metavar="N",
                        help="max fills to fetch: 1-1000 (default/max 1000), or 'unlimited' to page "
                             "the FULL history backward (honors --from/--to as server-side bounds)")
    parser.add_argument("--from", dest="from_us", type=epoch_us_arg, metavar="EPOCH_US",
                        help="only fills at/after this start time (epoch MICROseconds, inclusive -- the "
                             "createdAt unit; e.g. a ms value x 1000)")
    parser.add_argument("--to", dest="to_us", type=epoch_us_arg, metavar="EPOCH_US",
                        help="only fills at/before this end time (epoch MICROseconds, inclusive)")
    parser.add_argument("--condensed", action="store_true",
                        help="machine-readable: one CSV row per fill "
                             "(createdAt,market,side,size,price,fee,role,closedPnl,effect,tradeId,orderId,liquidationMethod,liquidatedUser), "
                             "raw values, no header/padding/totals")
    parser.add_argument("--header", action="store_true",
                        help="with --condensed, emit a CSV header row first "
                             "(error if used without --condensed)")
    parser.add_argument("--verbose", action="store_true",
                        help="show EVERY field of each fill (incl. clientId/originalSize/remainingSize/accountIndex, "
                             "which the default columns omit): an aligned key:value block per fill, or -- with "
                             "--condensed -- one all-fields CSV row per fill (STREAMS; the header is ALWAYS emitted "
                             "since the columns are dynamic, so --header is implied).")
    parser.add_argument("--infinite-retry", action="store_true",
                        help="never give up on TRANSIENT fetch failures (network timeout / 429 / 5xx): retry "
                             "forever with capped backoff instead of failing after 5 attempts -- for very large "
                             "multi-hour exports. A terminal 4xx (403/404) still fails fast.")
    add_network_args(parser)
    args = parser.parse_args()
    INFINITE_RETRY = args.infinite_retry
    BASE = NETWORKS[args.network]
    require_eth_address(args.address, "display_fills")
    if args.from_us is not None and args.to_us is not None and args.from_us > args.to_us:
        raise SystemExit("display_fills: --from must be <= --to.")
    if args.header and not args.condensed:
        raise SystemExit("display_fills: --header requires --condensed.")

    # Validate/resolve --market up front (a typo must FAIL, not silently return 0 fills) to its canonical
    # display name, which /v1/fills takes as a SERVER-SIDE filter (v1.2.1) -- so ONLY that market's fills are
    # fetched/paged (far cheaper than fetching everything and filtering locally, esp. for --limit unlimited on
    # a huge account). A defensive local marketDisplayName match stays below in case the server filter regresses.
    target_name = resolve_market_name(BASE, args.market, "display_fills") if args.market else None

    # --condensed is a machine-readable firehose (usually piped): STREAM each page as it pages in -- the
    # backward walk yields pages newest-first -- so a HUGE account starts printing immediately and shows steady
    # progress instead of buffering the whole history before the first line (and `| head` can stop it early).
    # No global re-sort in stream mode: the pagination order IS newest-first; a consumer needing a strict order
    # sorts its own copy.
    if args.condensed:
        pages = (iter_fills_pages(args.address, args.from_us, args.to_us, market=target_name, role=args.role, side=args.side)
                 if args.limit == UNLIMITED
                 else [fetch_fills(args.address, args.limit, args.from_us, args.to_us, market=target_name, role=args.role, side=args.side)])
        keep = None if target_name is None else (lambda f: f.get("marketDisplayName") == target_name)
        if args.verbose:
            stream_verbose_csv(pages, keep=keep)   # --verbose: EVERY field, streaming CSV, header always emitted
        else:
            stream_condensed_pages(pages, CONDENSED_KEYS, header=args.header, keep=keep)
        return

    # Human table: needs the FULL set (column widths + totals footer), so it collects then sorts -- can't stream.
    if args.limit == UNLIMITED:
        raw = fetch_fills_all(args.address, args.from_us, args.to_us, market=target_name, role=args.role, side=args.side)
        truncated = False                         # paginated to completeness (within --from/--to + --market)
    else:
        raw = fetch_fills(args.address, args.limit, args.from_us, args.to_us, market=target_name, role=args.role, side=args.side)
        truncated = len(raw) >= args.limit        # a full page back -> older fills may exist
    if len(raw) > TABLE_MAX_ROWS:
        raise SystemExit(
            f"display_fills: too many fills for a table (buffered > {TABLE_MAX_ROWS:,}). The table mode holds the "
            f"ENTIRE result in memory (column widths + sort + totals); it would exhaust RAM on a large account. "
            f"Use --condensed (streams at ~constant memory), narrow with --from/--to, or a smaller --limit.")
    fills = [f for f in raw if target_name is None or f.get("marketDisplayName") == target_name]
    fills.sort(key=created_key, reverse=True)      # enforce newest-first locally

    label = target_name if target_name else "ALL"
    # With the server-side --market filter, a bounded --limit N now returns the latest N fills OF THAT MARKET
    # (not N account-wide then filtered), so the truncation note is simply "older <market> fills may exist".
    note = ""
    if truncated:
        if target_name is not None:
            note = (f"  (latest {args.limit} {label} fills shown; older {label} fills may exist -- "
                    f"use --from/--to or a larger --limit)")
        else:
            note = f"  (latest {args.limit} shown; older fills exist -- use --from/--to or a larger --limit)"
    if not fills:
        print(f"0 fill(s) [{label}] for {args.address}{note}\n")
        return
    if args.verbose:   # block dump: EVERY field of each fill (buffered set, like the table)
        print_verbose_records(fills, f"{len(fills)} fill(s) [{label}] for {args.address}{note}", time_keys={"createdAt"})
    else:
        print_table(fills, args.address, label, note)


if __name__ == "__main__":
    run_pipe_safe(main)
