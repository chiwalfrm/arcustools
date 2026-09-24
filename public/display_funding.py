"""
Display funding payments for an account, newest-first.

  python3 display_funding.py <eth_address>                      # last 30 days (API default)
  python3 display_funding.py <eth_address> --market BTC-USD      # only that market
  python3 display_funding.py <eth_address> --from 1782000000000000  # walk older history (epoch µs)
  python3 display_funding.py <eth_address> --limit 50 --condensed
  python3 display_funding.py <eth_address> --limit unlimited      # full history (bypasses 30-day default)
  python3 display_funding.py <eth_address> --verbose              # EVERY field per payment (key:value block)
  python3 display_funding.py <eth_address> --condensed --verbose  # EVERY field, one CSV row per payment (streams)

Uses GET /v1/funding (per-account funding payment history). This is a public, account-scoped
read -- it takes only the `address` query parameter and needs NO signature, so this display
tool needs just the address, not the creds file. Sign convention: payment positive = RECEIVED,
negative = PAID. --market is resolved to a canonical display name and pushed to the SERVER-SIDE
market filter (v1.4.8), so only that market's payments are fetched/paged (with a defensive local
match too). The API DEFAULTS to the last 30 days when --from is omitted; use --from/--to (epoch µs)
to widen/walk history.
Output is sorted newest-first locally (not trusting API ordering).

--from/--to are epoch MICROseconds -- the arcus dev team unified /v1/funding to µs (matching
display_fills/display_transfers) across ALL networks (testnet/staging/mainnet).
"""

import argparse
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal
from functools import partial
from arcus_common_public import stream_condensed_pages, stream_verbose_csv, print_verbose_records, TABLE_MAX_ROWS, add_network_args, require_eth_address, run_pipe_safe, NETWORKS, UNLIMITED, cell, dec, dedup_vs_previous_page, epoch_us_arg, get_json_dict, limit_arg, page_pace_delay, resolve_market_name, when   # shared public helpers (formerly local copies)

_gjd = partial(get_json_dict, prog="display_funding")   # get_json + require_dict, this tool's prog
INFINITE_RETRY = False    # set by --infinite-retry: retry TRANSIENT fetch failures forever (never give up on a huge export)


def _get_json(url, what, **kw):
    """Wrap _gjd to inject this tool's --infinite-retry flag (read at call time, so main can flip it after arg
    parsing). Every _get_json(...) in this tool then honours --infinite-retry."""
    return _gjd(url, what, infinite=INFINITE_RETRY, **kw)


BASE = None   # set in main() from the required --testnet/--staging/--mainnet selector
PAGE_SIZE = 1000          # API max per request; page size used by --limit unlimited

# Fields emitted by --condensed (raw, one CSV row per payment; no header, per the display_* convention).
CONDENSED_KEYS = ["time", "marketDisplayName", "fundingRate", "size", "payment"]


def time_key(payment):
    """Sort key by time (desc via reverse=True); missing/bad sorts oldest."""
    try:
        return int(payment.get("time"))
    except (TypeError, ValueError):
        return -1


def fetch_funding(address, limit, from_us=None, to_us=None, market=None):
    """GET /v1/funding (newest-first). Validates the response shape and returns the payments list. `market`
    is the canonical marketDisplayName for the SERVER-SIDE market filter (v1.4.8); None = all markets."""
    q = {"address": address, "limit": limit}
    if from_us is not None:
        q["from"] = from_us
    if to_us is not None:
        q["to"] = to_us
    if market is not None:
        q["market"] = market
    data = _get_json(f"{BASE}/v1/funding?{urllib.parse.urlencode(q)}", "funding")
    payments = data.get("fundingPayments")
    if payments is None:
        return []
    if not isinstance(payments, list):
        raise SystemExit("display_funding: unexpected /v1/funding response ('fundingPayments' is not a list).")
    return [p for p in payments if isinstance(p, dict)]   # drop any non-dict element so downstream .get() can't crash


def iter_funding_pages(address, from_us=None, to_us=None, market=None):
    """--limit unlimited, as a GENERATOR: yield each page's FRESH (deduped) payments LIST, newest-first, as it
    pages BACKWARD via the `to` cursor -- so a caller can STREAM output instead of buffering the whole history
    first. /v1/funding has only from/to bounds and DEFAULTS to the last 30 days when `from` is omitted, so we
    send from=0 (unless --from) to reach genuine full history. `time`, `from` and `to` are all MICROseconds
    (the arcus dev team unified funding to µs like fills/transfers, on all networks), so the cursor is the
    oldest `time` DIRECTLY -- inclusive of the boundary row (no conversion), and dedup by (marketDisplayName,
    time) drops the re-read. `market` (canonical marketDisplayName) applies the SERVER-SIDE market filter
    (v1.4.8) so only that market's payments page. fetch_funding_all() flattens."""
    # Dedup keys from the PREVIOUS page ONLY -- bounded memory (a global set would grow to O(total payments)).
    # `to` is inclusive so page N+1 re-reads only the boundary microsecond, and the cursor STRICTLY decreases
    # (nc >= cursor breaks), so page N+1 can overlap only page N.
    prev_keys = set()
    eff_from = from_us if from_us is not None else 0
    cursor = to_us
    first = True
    while True:
        q = {"address": address, "limit": PAGE_SIZE, "from": eff_from}
        if market is not None:
            q["market"] = market
        if cursor is not None:
            q["to"] = cursor
        # Pace pages AFTER the first: a full 1000-row /v1/funding page costs ~70 IP-weight and the per-IP
        # bucket refills 25/s, so unpaced back-to-back paging drives the bucket negative -> 429. The first
        # page rides the already-full bucket (no pause).
        data = _get_json(f"{BASE}/v1/funding?{urllib.parse.urlencode(q)}", "funding",
                         delay=(0.0 if first else page_pace_delay()))
        first = False
        payments = data.get("fundingPayments")
        if payments is None:
            break
        if not isinstance(payments, list):
            raise SystemExit("display_funding: unexpected /v1/funding response ('fundingPayments' is not a list).")
        if not payments:
            break
        # Dedup by (marketDisplayName, time) -- payments carry no id -- against the PREVIOUS page only (the
        # inclusive `to` cursor re-reads just the boundary row). (See dedup_vs_previous_page.)
        fresh, prev_keys = dedup_vs_previous_page(
            payments, prev_keys, lambda p: (p.get("marketDisplayName"), p.get("time")) if isinstance(p, dict) else None)
        if fresh:
            yield fresh                     # STREAM this page's fresh payments before fetching the next
        if len(payments) < PAGE_SIZE:      # fewer than a full page -> reached the oldest / `from`
            break
        if not fresh:                       # no new rows -> stop, never loop
            break
        positives = [c for c in (time_key(p) for p in payments if isinstance(p, dict)) if c > 0]
        if not positives:                   # nothing with a usable timestamp to advance the cursor
            break
        nc = min(positives)                        # to is MICROseconds now (same as time) -> oldest time directly, inclusive
        if cursor is not None and nc >= cursor:    # cursor didn't decrease -> avoid an infinite loop
            break
        cursor = nc


def fetch_funding_all(address, from_us=None, to_us=None, market=None):
    """--limit unlimited: page /v1/funding to completeness, COLLECTED into one list (for the table path, which
    needs the full set for column widths + the totals footer). Streaming callers use iter_funding_pages()."""
    return [p for page in iter_funding_pages(address, from_us, to_us, market) for p in page]


def print_table(payments, address, label, note):
    """Aligned table with column widths sized to the data, plus a received/paid/net footer."""
    cols = [
        ("TIME (UTC)", "<", lambda p: when(p.get("time"))),
        ("MARKET", "<", lambda p: cell(p.get("marketDisplayName"))),
        ("FUNDING RATE", ">", lambda p: cell(p.get("fundingRate"))),
        ("SIZE", ">", lambda p: cell(p.get("size"))),
        ("PAYMENT", ">", lambda p: cell(p.get("payment"))),
    ]
    widths = [max(len(h), max((len(get(p)) for p in payments), default=0)) for h, _, get in cols]

    print(f"{len(payments)} funding payment(s) [{label}] for {address}{note}\n")
    head = "  ".join(f"{h:{a}{w}}" for (h, a, _), w in zip(cols, widths))
    print(head)
    print("-" * len(head))
    for p in payments:
        print("  ".join(f"{get(p):{a}{w}}" for (_, a, get), w in zip(cols, widths)))

    # payment: positive = received, negative = paid. Net = received - paid = sum of all.
    amounts = [dec(p.get("payment")) or Decimal(0) for p in payments]
    received = sum((a for a in amounts if a > 0), Decimal(0))
    paid = sum((-a for a in amounts if a < 0), Decimal(0))
    net = received - paid
    print("-" * len(head))
    print(f"  TOTAL  net {net:+,.6f}   (received {received:,.6f}, paid {paid:,.6f})   "
          f"over {len(payments)} payment(s)")


def main():
    global BASE, INFINITE_RETRY
    parser = argparse.ArgumentParser(description="Display account funding payments.")
    parser.add_argument("address", help="Ethereum address of the account to display")
    parser.add_argument("--market",
                        help="show only payments in this market (display name or marketId; default: all)")
    parser.add_argument("--limit", type=limit_arg, default=1000, metavar="N",
                        help="max payments to fetch: 1-1000 (default/max 1000), or 'unlimited' to page "
                             "the FULL history backward (sends from=0 to bypass the 30-day default; "
                             "honors --from/--to)")
    # --from/--to are epoch MICROseconds -- SAME unit as the response `time` and as display_fills/
    # display_transfers (the arcus dev team unified funding to µs across all networks).
    parser.add_argument("--from", dest="from_us", type=epoch_us_arg, metavar="EPOCH_US",
                        help="only payments at/after this start time (epoch MICROseconds, inclusive -- the "
                             "`time` unit; e.g. a ms value x 1000); omit and the API defaults to the last 30 days")
    parser.add_argument("--to", dest="to_us", type=epoch_us_arg, metavar="EPOCH_US",
                        help="only payments at/before this end time (epoch MICROseconds, inclusive; default: now)")
    parser.add_argument("--condensed", action="store_true",
                        help="machine-readable: one CSV row per payment "
                             "(time,market,fundingRate,size,payment), raw values, no header/padding/totals")
    parser.add_argument("--header", action="store_true",
                        help="with --condensed, emit a CSV header row first "
                             "(error if used without --condensed)")
    parser.add_argument("--verbose", action="store_true",
                        help="show EVERY field of each payment (the default columns omit marketId): an aligned "
                             "key:value block per payment, or -- with --condensed -- one all-fields CSV row per "
                             "payment (STREAMS; the header is ALWAYS emitted since the columns are dynamic, so "
                             "--header is implied).")
    parser.add_argument("--infinite-retry", action="store_true",
                        help="never give up on TRANSIENT fetch failures (network timeout / 429 / 5xx): retry "
                             "forever with capped backoff instead of failing after 5 attempts -- for very large "
                             "multi-hour exports. A terminal 4xx (403/404) still fails fast.")
    add_network_args(parser)
    args = parser.parse_args()
    INFINITE_RETRY = args.infinite_retry
    BASE = NETWORKS[args.network]
    require_eth_address(args.address, "display_funding")
    if args.from_us is not None and args.to_us is not None and args.from_us > args.to_us:
        raise SystemExit("display_funding: --from must be <= --to.")
    if args.header and not args.condensed:
        raise SystemExit("display_funding: --header requires --condensed.")

    # Validate/resolve --market up front (a typo must FAIL, not silently return 0 rows) to its canonical
    # display name, which /v1/funding takes as a SERVER-SIDE filter (v1.4.8) -- so ONLY that market's payments
    # are fetched/paged (a defensive local marketDisplayName match stays below in case the server filter regresses).
    target_name = resolve_market_name(BASE, args.market, "display_funding") if args.market else None

    # --condensed is a machine-readable firehose (usually piped): STREAM each page as it pages in (newest-first
    # via the backward walk) so a huge account starts printing immediately + shows steady progress instead of
    # buffering the whole history first (and `| head` can stop it early). No global re-sort in stream mode --
    # the pagination order IS newest-first; a consumer needing a strict order sorts its own copy.
    if args.condensed:
        pages = (iter_funding_pages(args.address, args.from_us, args.to_us, market=target_name)
                 if args.limit == UNLIMITED
                 else [fetch_funding(args.address, args.limit, args.from_us, args.to_us, market=target_name)])
        keep = None if target_name is None else (lambda p: p.get("marketDisplayName") == target_name)
        if args.verbose:
            stream_verbose_csv(pages, keep=keep)   # --verbose: EVERY field, streaming CSV, header always emitted
        else:
            stream_condensed_pages(pages, CONDENSED_KEYS, header=args.header, keep=keep)
        return

    # Human table: needs the FULL set (column widths + totals footer), so it collects then sorts -- can't stream.
    if args.limit == UNLIMITED:
        raw = fetch_funding_all(args.address, args.from_us, args.to_us, market=target_name)
        truncated = False                         # paginated to completeness (within --from/--to + --market)
    else:
        raw = fetch_funding(args.address, args.limit, args.from_us, args.to_us, market=target_name)
        truncated = len(raw) >= args.limit        # a full page back -> older payments may exist
    if len(raw) > TABLE_MAX_ROWS:
        raise SystemExit(
            f"display_funding: too many payments for a table (buffered > {TABLE_MAX_ROWS:,}). The table mode holds "
            f"the ENTIRE result in memory (column widths + sort + totals); it would exhaust RAM on a large account. "
            f"Use --condensed (streams at ~constant memory), narrow with --from/--to, or a smaller --limit.")
    payments = [p for p in raw if target_name is None or p.get("marketDisplayName") == target_name]
    payments.sort(key=time_key, reverse=True)      # enforce newest-first locally

    label = target_name if target_name else "ALL"
    # Be honest about scope: the default window is only the last 30 days. With the server-side --market filter,
    # a bounded --limit N returns the latest N payments OF THAT MARKET.
    notes = []
    if args.from_us is None and args.limit != UNLIMITED:
        notes.append("default window: last 30 days -- pass --from (epoch µs) or --limit unlimited for older history")
    if truncated:
        if target_name is not None:
            notes.append(f"latest {args.limit} {label} payments shown; older {label} payments may exist")
        else:
            notes.append(f"latest {args.limit} shown; older payments may exist")
    note = ("  (" + "; ".join(notes) + ")") if notes else ""
    if not payments:
        print(f"0 funding payment(s) [{label}] for {args.address}{note}\n")
        return
    if args.verbose:   # block dump: EVERY field of each payment (buffered set, like the table)
        print_verbose_records(payments, f"{len(payments)} funding payment(s) [{label}] for {args.address}{note}", time_keys={"time"})
    else:
        print_table(payments, args.address, label, note)


if __name__ == "__main__":
    run_pipe_safe(main)
