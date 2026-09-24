"""
Display orders for an account, newest-first.

  python3 display_orders.py <eth_address>                 # every order, any status (up to 1000)
  python3 display_orders.py <eth_address> --status OPEN    # ALL live/open orders (via /v1/openOrders)
  python3 display_orders.py <eth_address> --status CANCELED --limit 50
  python3 display_orders.py <eth_address> --limit unlimited     # page the FULL history
  python3 display_orders.py <eth_address> --from 1782000000000000 --to 1782600000000000
  python3 display_orders.py <eth_address> --verbose             # EVERY field per order (key:value block)
  python3 display_orders.py <eth_address> --condensed --verbose # EVERY field, one CSV row per order (pipe/spreadsheet)

The default table/--condensed show a curated column set; --verbose adds the rest (triggerPrice/tpslType/parentOrderId/
reduceOnly on conditional orders, goodTilTime on all) -- as a key:value block, or one all-fields CSV row with --condensed.

This is a public, account-scoped read -- it takes only the `address` query parameter and needs NO
signature, so this display tool needs just the address, not the creds file.

TWO endpoints, chosen by --status:
  * --status OPEN  -> GET /v1/openOrders  (the LIVE open-orders set). This is the complete + cheap way
    to answer "what's resting right now": it returns EVERY live order (semantic (ii) -- OPEN *and* the
    other live statuses PARTIALLY_FILLED / UNTRIGGERED / TPSL_PLACED, which are all still open orders),
    with NO status re-filter. Avoids the history path's failure mode where an old-but-still-resting
    order sits beyond a bounded window and gets missed. Newest-first by placement time; from/to bound
    `createdAt` (which does NOT move on partial fills, so paging stays stable).
  * any other --status (or none) -> GET /v1/orders  (order history: open, filled, canceled, rejected).
    Server-side market/side/status filters (v1.4.5/v1.6.0/v1.7.1); non-server statuses filter locally; from/to bound `updatedAt`.

Both endpoints are newest-first, paginate the same way (limit 1-1000; from/to epoch MICROseconds -- NOT
ms; inclusive `to` re-reads the boundary row, deduped by orderId), and return the same unified order
shape. --limit unlimited pages BACKWARD to completeness. An account can hold up to 10,000 live orders,
so the old 1000-row cap could silently drop orders -- use --limit unlimited (or --from/--to) on busy
accounts. Output is sorted newest-first locally (not trusting API ordering).
"""

import argparse
import urllib.parse
from decimal import Decimal, InvalidOperation
from arcus_common_public import (resolve_market_name, stream_condensed_pages, stream_verbose_csv, print_verbose_records, TABLE_MAX_ROWS, add_network_args, require_eth_address, run_pipe_safe, NETWORKS, UNLIMITED, cell, created_key, dedup_vs_previous_page, id_or_json_key,
                                 epoch_us_arg, get_json_dict, limit_arg, page_pace_delay, updated_key, when)   # shared public helpers

BASE = None   # set in main() from the required --testnet/--staging/--mainnet selector
PAGE_SIZE = 1000          # API max per request; page size used by --limit unlimited
INFINITE_RETRY = False    # set by --infinite-retry: retry TRANSIENT fetch failures forever (never give up on a huge export)

# Status values the venue can report, per the order schema.
STATUSES = [
    "PENDING", "OPEN", "PARTIALLY_FILLED", "FILLED", "CANCELED",
    "MARGIN_CANCELED", "REJECTED", "UNTRIGGERED", "TPSL_PLACED",
    "TPSL_TRIGGERED", "TPSL_CANCELED", "LIQUIDATED", "ADL", "ACK",
    "CANCEL_ACKNOWLEDGED", "CANCEL_ALL_ACKNOWLEDGED", "CANCEL_PENDING",
    "ERROR",
]

# The venue's SERVER-SIDE status filter (v1.7.1) accepts only these 7 (case-insensitive); other STATUSES
# values are filtered CLIENT-SIDE (the server 400s on them).
SERVER_STATUSES = frozenset({"OPEN", "UNTRIGGERED", "FILLED", "CANCELED", "REJECTED", "LIQUIDATED", "ADL"})


def status_list(s):
    """--status type: a single status or comma-separated list, each a valid STATUSES value (case-insensitive).
    Returns the normalized UPPER comma-string; pushed server-side when every value is in SERVER_STATUSES."""
    vals = [v.strip().upper() for v in s.split(",") if v.strip()]
    if not vals:
        raise argparse.ArgumentTypeError("--status is empty")
    bad = [v for v in vals if v not in STATUSES]
    if bad:
        raise argparse.ArgumentTypeError(f"invalid status {bad}; valid: {', '.join(STATUSES)}")
    return ",".join(vals)

# Fields emitted by --condensed (raw, one CSV row per order). `tif` (normalized GTC->GTT, see _annotate_tif)
# is APPENDED at the END so positional consumers of the original 9 columns are unaffected.
CONDENSED_KEYS = [
    "marketDisplayName", "side", "status", "type",
    "price", "originalSize", "remainingSize", "orderId", "clientId", "tif",
]


def partially_filled(order):
    """True when some -- but not all -- of the order has filled.

    i.e. 0 < remainingSize < originalSize. A fully filled order has
    remainingSize 0 (not partial); an untouched order has remaining == size.
    Uses Decimal (sizes are decimal strings) to avoid float precision issues.
    """
    try:
        remaining = Decimal(str(order.get("remainingSize")))
        size = Decimal(str(order.get("originalSize")))
        if not (remaining.is_finite() and size.is_finite()):   # Decimal(str("NaN")) constructs fine, but the ordered
            return False                                        # compare below RAISES InvalidOperation on a NaN -> crash
        return 0 < remaining < size
    except (InvalidOperation, TypeError, ValueError):
        return False


def _annotate_tif(o):
    """Add a normalized top-level `tif`: the venue reads a toolkit GTT order (submitted as TIF_GTC) back as
    timeInForce "GTC", so map GTC->GTT to match how it was placed (the toolkit exposes no separate GTC);
    ALO/IOC/FOK pass through. Lets the condensed CSV (row.get) and the table show a consistent TIF. Mutates o."""
    raw = o.get("timeInForce")
    o["tif"] = "GTT" if raw == "GTC" else (raw if raw is not None else "")
    return o


def _orders_from(body):
    """Validate the shape (mirrors display_fills/funding): a MISSING 'orders' key is "no orders" -> [],
    but a PRESENT-but-non-list value (dict/str/number) must be a clean error, not fall through `or []`
    (truthy non-lists slip past that) and crash the downstream sort/comprehension with AttributeError.
    Both /v1/orders and /v1/openOrders return the list under `orders`. Also flattens the venue's TIF
    (GTC->GTT) into a normalized `tif` field on every order (see _annotate_tif)."""
    orders = body.get("orders")
    if orders is None:
        return []
    if not isinstance(orders, list):
        raise SystemExit("display_orders: unexpected response ('orders' is not a list).")
    return [_annotate_tif(o) for o in orders if isinstance(o, dict)]   # drop non-dicts; normalize TIF for display


def fetch_page(path_seg, address, limit, from_us=None, to_us=None, delay=0.0, market=None, side=None, status=None):
    """GET /v1/<path_seg> (one page, newest-first), turning network/HTTP/JSON failures into clean CLI
    errors. path_seg is 'orders' (history) or 'openOrders' (live set); from_us/to_us are epoch
    MICROseconds bounding the endpoint's window field (updatedAt for orders, createdAt for openOrders).
    market/side/status are SERVER-SIDE filters (v1.4.5/v1.6.0/v1.7.1); openOrders ignores `side` (the caller
    filters it locally) and is the whole live set so it is passed no `status`."""
    q = {"address": address, "limit": limit}
    if from_us is not None:
        q["from"] = from_us
    if to_us is not None:
        q["to"] = to_us
    if market is not None:
        q["market"] = market
    if side is not None:
        q["side"] = side
    if status is not None:
        q["status"] = status
    # Shared retrying reader: Retry-After/backoff on 429 (incl. Cloudflare 1015) + 5xx, clean CLI errors,
    # require_dict included -- so this tool is no longer fragile under a rate-limit burst in cron/ops.
    body = get_json_dict(f"{BASE}/v1/{path_seg}?{urllib.parse.urlencode(q)}", "orders", "display_orders",
                         infinite=INFINITE_RETRY, delay=delay)
    return _orders_from(body)


def iter_pages(path_seg, cursor_key, address, from_us=None, to_us=None, market=None, side=None, status=None):
    """--limit unlimited, as a GENERATOR: yield each page's FRESH (deduped) orders LIST, newest-first, as
    it pages BACKWARD via the `to` cursor -- so a caller can STREAM output instead of buffering. The
    endpoint has only from/to bounds; `from` (if set) is a SERVER-SIDE lower bound so paging terminates
    at it. `cursor_key` extracts the endpoint's window field (updated_key for /v1/orders, created_key for
    /v1/openOrders) -- window field / from / to are all epoch MICROseconds (same unit), so the cursor is
    the oldest value directly, NO conversion. `to` is INCLUSIVE (closes over the microsecond it names), so
    the boundary order re-reads next page; dedup by orderId drops it. fetch_all() is this, flattened."""
    # Dedup keys from the PREVIOUS page ONLY -- bounded memory. `to` is inclusive so page N+1 re-reads only the
    # boundary microsecond, and the cursor STRICTLY decreases (nc >= cursor breaks), so page N+1 can overlap only
    # page N. A global seen-set would grow to O(total orders) (an account can hold up to 10k live orders, and the
    # /v1/orders history is unbounded).
    prev_keys, cursor = set(), to_us
    first = True
    while True:
        page = fetch_page(path_seg, address, PAGE_SIZE, from_us, cursor,
                          delay=(0.0 if first else page_pace_delay()),
                          market=market, side=side, status=status)
        # Pace pages AFTER the first: a full 1000-row page costs ~70 IP-weight and the per-IP bucket
        # refills 25/s, so unpaced back-to-back paging drives the bucket negative -> 429. The first page
        # rides the already-full bucket (no pause -- delay=0.0 above).
        first = False
        if not page:
            break
        # Dedup by orderId (id-less order -> full-row JSON) against the PREVIOUS page only: the inclusive `to`
        # cursor re-reads just the boundary microsecond, which strictly decreases, so page N+1 overlaps ONLY
        # page N -- bounded to ~one page, not O(total). (See dedup_vs_previous_page.)
        fresh, prev_keys = dedup_vs_previous_page(page, prev_keys, lambda o: id_or_json_key(o, "orderId"))
        if fresh:
            yield fresh                 # STREAM this page's fresh orders to the caller before the next
        if len(page) < PAGE_SIZE:        # fewer than a full page -> reached the oldest order / `from`
            break
        if not fresh:                    # no new orderIds -> stop, never loop
            break
        positives = [c for c in (cursor_key(o) for o in page) if c > 0]
        if not positives:                # nothing with a usable timestamp to advance the cursor
            break
        nc = min(positives)              # oldest window-field value (epoch us) -> `to` for next page, inclusive
        if cursor is not None and nc >= cursor:    # cursor didn't decrease -> avoid an infinite loop
            break
        cursor = nc


def fetch_all(path_seg, cursor_key, address, from_us=None, to_us=None, market=None, side=None, status=None):
    """--limit unlimited: page to completeness, COLLECTED into one list (for the table path, which needs
    the full set for column widths). Streaming callers use iter_pages()."""
    return [o for page in iter_pages(path_seg, cursor_key, address, from_us, to_us, market, side, status) for o in page]


def print_table(orders, address, label, note):
    """Aligned table with column widths sized to the data (handles long values)."""
    has_partial = any(partially_filled(o) for o in orders)

    # (header, alignment, value-getter). A 1-char '*' flag column is inserted
    # after REMAINING only when something is partially filled.
    cols = [
        ("CREATED (UTC)", "<", lambda o: when(o.get("createdAt"))),
        ("MARKET", "<", lambda o: cell(o.get("marketDisplayName"))),
        ("SIDE", "<", lambda o: cell(o.get("side"))),
        ("STATUS", "<", lambda o: cell(o.get("status"))),
        ("TYPE", "<", lambda o: cell(o.get("type"))),
        ("TIF", "<", lambda o: cell(o.get("tif"))),   # normalized GTC->GTT (see _annotate_tif)
        ("PRICE", ">", lambda o: cell(o.get("price"))),
        ("SIZE", ">", lambda o: cell(o.get("originalSize"))),
        ("REMAINING", ">", lambda o: cell(o.get("remainingSize"))),
    ]
    if has_partial:
        cols.append(("", "<", lambda o: "*" if partially_filled(o) else ""))
    cols += [
        ("ORDER ID", "<", lambda o: cell(o.get("orderId"))),
        ("CLIENTID", "<", lambda o: cell(o.get("clientId"))),
    ]

    widths = []
    for header, _, get in cols:
        widths.append(max(len(header), max((len(get(o)) for o in orders), default=0)))

    legend = "    (* = partially filled)" if has_partial else ""
    print(f"{len(orders)} order(s) [{label}] for {address}{note}{legend}\n")

    head = "  ".join(f"{h:{a}{w}}" for (h, a, _), w in zip(cols, widths))
    print(head)
    print("-" * len(head))
    for o in orders:
        print("  ".join(f"{get(o):{a}{w}}" for (_, a, get), w in zip(cols, widths)))


def main():
    global BASE, INFINITE_RETRY
    parser = argparse.ArgumentParser(description="Display account orders.")
    parser.add_argument("address", help="Ethereum address of the account to display")
    parser.add_argument("--status", type=status_list, metavar="STATUS[,STATUS...]",
                        help="show only orders in these statuses (comma-separated; default: all). --status OPEN "
                             "reads the live /v1/openOrders set and returns EVERY live order (incl. "
                             "PARTIALLY_FILLED/UNTRIGGERED/TPSL_PLACED). OPEN/UNTRIGGERED/FILLED/CANCELED/REJECTED/"
                             "LIQUIDATED/ADL filter server-side; other statuses filter client-side.")
    parser.add_argument("--market",
                        help="show only orders in this market (display name or marketId; server-side, v1.4.5)")
    parser.add_argument("--side", choices=["BUY", "SELL"],
                        help="show only orders on this side (server-side on /v1/orders, v1.6.0)")
    parser.add_argument("--limit", type=limit_arg, default=1000, metavar="N",
                        help="max orders to fetch: 1-1000 (default/max 1000), or 'unlimited' to page "
                             "the FULL set backward (honors --from/--to as server-side bounds)")
    parser.add_argument("--from", dest="from_us", type=epoch_us_arg, metavar="EPOCH_US",
                        help="lower time bound (epoch MICROseconds, inclusive -- e.g. a ms value x 1000). "
                             "Bounds updatedAt for history; createdAt for --status OPEN (/v1/openOrders)")
    parser.add_argument("--to", dest="to_us", type=epoch_us_arg, metavar="EPOCH_US",
                        help="upper time bound (epoch MICROseconds, inclusive)")
    parser.add_argument("--condensed", action="store_true",
                        help="machine-readable: one CSV row per order "
                             "(market,side,status,type,price,size,remaining,orderid,clientid,tif), "
                             "raw values, no header/padding/'*' marker")
    parser.add_argument("--header", action="store_true",
                        help="with --condensed, emit a CSV header row first "
                             "(error if used without --condensed)")
    parser.add_argument("--verbose", action="store_true",
                        help="show EVERY field of each order (incl. triggerPrice/tpslType/parentOrderId/reduceOnly/"
                             "goodTilTime, which the default columns omit): an aligned key:value block per order, or -- "
                             "with --condensed -- one CSV row per order carrying all fields. STREAMS (no buffering); "
                             "columns are discovered from the data and GROW as new fields appear (later rows can have "
                             "more columns). The header (+ a '#UPDATED_HEADER' line whenever columns grow) is ALWAYS "
                             "emitted for --condensed --verbose so the dynamic columns stay interpretable -- --header "
                             "is implied (redundant but harmless).")
    parser.add_argument("--infinite-retry", action="store_true",
                        help="never give up on TRANSIENT fetch failures (network timeout / 429 / 5xx): retry "
                             "forever with capped backoff instead of failing after 5 attempts -- for very large "
                             "multi-hour exports. A terminal 4xx (403/404) still fails fast.")
    add_network_args(parser)
    args = parser.parse_args()
    INFINITE_RETRY = args.infinite_retry
    BASE = NETWORKS[args.network]
    require_eth_address(args.address, "display_orders")
    if args.from_us is not None and args.to_us is not None and args.from_us > args.to_us:
        raise SystemExit("display_orders: --from must be <= --to.")
    if args.header and not args.condensed:
        raise SystemExit("display_orders: --header requires --condensed.")

    # --status OPEN -> the live open-orders endpoint (paged by createdAt), returning the WHOLE live set
    # with NO status re-filter (semantic (ii): PARTIALLY_FILLED/UNTRIGGERED/TPSL_PLACED are open too).
    # Every other status (or none) -> order history (paged by updatedAt), status applied locally.
    requested = args.status.split(",") if args.status else None    # normalized UPPER list by status_list
    use_open = (requested == ["OPEN"])
    target_name = resolve_market_name(BASE, args.market, "display_orders") if args.market else None
    # Push status server-side (v1.7.1) only when EVERY requested value is server-filterable (the venue's 7) and
    # we're on the history path; else filter status client-side (the server 400s on the other STATUSES values).
    server_status = (",".join(requested) if requested and not use_open and set(requested) <= SERVER_STATUSES
                     else None)
    path_seg = "openOrders" if use_open else "orders"
    cursor_key = created_key if use_open else updated_key
    label = args.status or "ALL"

    def keep(o):
        if target_name is not None and o.get("marketDisplayName") != target_name:
            return False                         # defensive local market match (server already filtered)
        if args.side is not None and o.get("side") != args.side:
            return False                         # /v1/openOrders ignores `side` -> enforce locally
        if requested is not None and not use_open and server_status is None:
            return o.get("status") in requested  # client-side status (values the server won't filter)
        return True

    # --condensed is machine-readable (usually piped). With --limit unlimited, STREAM each page as it pages
    # in (backward walk yields newest-first) so a HUGE account starts printing immediately; no global
    # re-sort in stream mode (the pagination order IS newest-first). Bounded --limit keeps the historical
    # behavior: fetch, filter, sort by createdAt, write.
    if args.condensed and args.verbose:
        # --condensed --verbose = EVERY field, FUTURE-PROOF, and STREAMING (NO buffering -- these tools run under tight
        # memory). Columns are DISCOVERED from the data (blank slate): any new key a later row carries is appended to
        # the END, so later rows can have more columns than earlier ones (a ragged CSV). Same paging as plain --condensed
        # -- unlimited streams via iter_pages; --limit N is one already-sorted page. See stream_verbose_csv (shared).
        if args.limit == UNLIMITED:
            stream_verbose_csv(iter_pages(path_seg, cursor_key, args.address, args.from_us, args.to_us, market=target_name, side=args.side, status=server_status),
                               keep=keep)
        else:
            orders = [o for o in fetch_page(path_seg, args.address, args.limit, args.from_us, args.to_us, market=target_name, side=args.side, status=server_status)
                      if keep(o)]
            orders.sort(key=created_key, reverse=True)
            stream_verbose_csv([orders])   # already filtered+sorted
        return

    if args.condensed:
        if args.limit == UNLIMITED:
            stream_condensed_pages(iter_pages(path_seg, cursor_key, args.address, args.from_us, args.to_us, market=target_name, side=args.side, status=server_status),
                                   CONDENSED_KEYS, header=args.header, keep=keep)
        else:
            orders = [o for o in fetch_page(path_seg, args.address, args.limit, args.from_us, args.to_us, market=target_name, side=args.side, status=server_status)
                      if keep(o)]
            orders.sort(key=created_key, reverse=True)
            stream_condensed_pages([orders], CONDENSED_KEYS, header=args.header)   # already filtered+sorted
        return

    # Human table: needs the FULL set (column widths), so it collects then sorts -- can't stream.
    if args.limit == UNLIMITED:
        raw = fetch_all(path_seg, cursor_key, args.address, args.from_us, args.to_us, market=target_name, side=args.side, status=server_status)
        truncated = False                         # paginated to completeness (within --from/--to)
    else:
        raw = fetch_page(path_seg, args.address, args.limit, args.from_us, args.to_us, market=target_name, side=args.side, status=server_status)
        truncated = len(raw) >= args.limit        # a full page back -> older orders may exist
    if len(raw) > TABLE_MAX_ROWS:
        raise SystemExit(
            f"display_orders: too many orders for a table (buffered > {TABLE_MAX_ROWS:,}). The table mode holds the "
            f"ENTIRE result in memory (column widths + sort); it would exhaust RAM on a large account. Use "
            f"--condensed (streams at ~constant memory), narrow with --from/--to, or a smaller --limit.")
    orders = [o for o in raw if keep(o)]
    orders.sort(key=created_key, reverse=True)     # enforce newest-first locally

    # Be honest about scope: a bounded fetch may hide older orders (an account can hold up to 10,000 live).
    note = ""
    if truncated:
        note = f"  (latest {args.limit} shown; older orders may exist -- use --limit unlimited or --from/--to)"
    if not orders:
        print(f"0 order(s) [{label}] for {args.address}{note}\n")
        return
    if args.verbose:
        print_verbose_records(orders, f"{len(orders)} order(s) [{label}] for {args.address}{note}", time_keys={"createdAt", "updatedAt", "goodTilTime"})   # full key:value block per order
    else:
        print_table(orders, args.address, label, note)


if __name__ == "__main__":
    run_pipe_safe(main)
