"""
Display an account's deposits/withdrawals (and transfers), newest-first.

  python3 display_transfers.py <eth_address> --mainnet                     # latest 1000 (default)
  python3 display_transfers.py <eth_address> --mainnet --limit unlimited   # FULL history (complete NET)
  python3 display_transfers.py <eth_address> --mainnet --all          # include same-user sub-account moves
  python3 display_transfers.py <eth_address> --from 1782000000000000 --to 1782600000000000 --mainnet
  python3 display_transfers.py <eth_address> --mainnet --condensed --header
  python3 display_transfers.py <eth_address> --mainnet --verbose             # EVERY field per transfer (key:value block)
  python3 display_transfers.py <eth_address> --mainnet --condensed --verbose # EVERY field, one CSV row per transfer (header once)

Uses GET /v1/accountTransferUpdates (per-account transfer history). Public, account-scoped read
(address query param, no signature). One call covers ALL of the address's accountIndexes -- the
endpoint is address-scoped, not index-scoped. `createdAt` AND the --from/--to request filters are
all epoch MICROseconds (same unit -- a createdAt is a valid from/to with no conversion). `amount` is a positive
quote-currency (USDC) decimal; DIRECTION comes from `type`.

Types (per the arcus API spec -- note the counterintuitive names): DEPOSIT / WITHDRAWAL (external
chain movements); INTERNAL_TRANSFER = a move between sub-accounts of the SAME user (net-neutral for
the wallet); SELF_ACCOUNT_TRANSFER = a transfer between DISTINCT users (a real balance change);
REFERRAL_CLAIM (a credit). BY DEFAULT INTERNAL_TRANSFER rows are HIDDEN (a same-user sub-account
move nets to zero across the wallet, so it would distort a deposits-minus-withdrawals
reconciliation); pass --all to include them. Sign: DEPOSIT / REFERRAL_CLAIM / SELF_ACCOUNT_TRANSFER-in
= +, WITHDRAWAL / SELF_ACCOUNT_TRANSFER-out = -; INTERNAL_TRANSFER is net-neutral (0).
SELF_ACCOUNT_TRANSFER direction is inferred from the account's own wire ids (the destinationAccountId
of its deposits / sourceAccountId of its withdrawals) -- best-effort. Only APPLIED rows (status
APPLIED or absent) count toward totals; REJECTED_* rows are shown but excluded from the net.

Defaults to the latest 1000 rows (--limit N, 1-1000); `--limit unlimited` walks the FULL history via
the `to` cursor (createdAt µs, inclusive, dedup by id) -- which the NET reconciliation needs to be COMPLETE.
A bounded fetch that fills its page WARNS that the NET is partial (a truncated transfer list would
mislead a balance reconciliation). `--condensed` = one CSV row per row (comma; arcus never puts commas
in these fields). Output is newest-first.
"""

import argparse
import csv
import sys
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal
from functools import partial
from arcus_common_public import print_verbose_records, write_verbose_csv, TABLE_MAX_ROWS, add_network_args, require_eth_address, run_pipe_safe, NETWORKS, UNLIMITED, dec, id_or_json_key, epoch_us_arg, get_json_dict, limit_arg, page_pace_delay, when   # shared public helpers (formerly local copies)

_gjd = partial(get_json_dict, prog="display_transfers")   # get_json + require_dict, this tool's prog
INFINITE_RETRY = False    # set by --infinite-retry: retry TRANSIENT fetch failures forever (never give up on a huge export)


def _get_json(url, what, **kw):
    """Wrap _gjd to inject this tool's --infinite-retry flag (read at call time, so main can flip it after arg
    parsing). Every _get_json(...) in this tool then honours --infinite-retry."""
    return _gjd(url, what, infinite=INFINITE_RETRY, **kw)


BASE = None
PAGE_SIZE = 1000          # API max rows per request (honored ONLY when `from` is a valid timestamp -- see below).
# ★ /v1/accountTransferUpdates applies a DEFAULT RECENT WINDOW when `from` is omitted OR 0 (0 is treated as
# "unset"): you get a small recent slice, NOT full history (observed: 23 rows on an account with 38), and `total`
# just echoes the returned count. A valid far-past `from` escapes the window and makes `limit` honored (one request
# returned all 38), so every request sends `from` = --from or HISTORY_FROM_US when unset. (from=1 is a 400.)
HISTORY_FROM_US = 1600000000000000   # 2020-09-13 UTC, in epoch MICROseconds
INFLOW_TYPES = {"DEPOSIT", "REFERRAL_CLAIM"}
CONDENSED_KEYS = ["id", "createdAt", "type", "status", "accountIndex", "amount"]  # amount = SIGNED


def created_us(row):
    try:
        return int(row.get("createdAt"))
    except (TypeError, ValueError):
        return -1


def iter_transfers_pages(address, from_us=None, to_us=None):
    """ALL transfer updates for the address, as a GENERATOR: yield each page's NEW rows LIST, paging BACKWARD via
    the `to` cursor. from/to/createdAt are epoch MICROseconds.

    The PRIMARY fix (shared with the siblings): send a valid far-past `from` floor (HISTORY_FROM_US when --from is
    unset) -- `from` omitted/0 gives a default recent window, so this is what makes `limit` honored and full history
    reachable. With that, one request returns everything for a normal account (e.g. all 38), and the `<PAGE_SIZE`
    break ends paging correctly.

    TRANSFERS-ONLY extra guard: should an account ever exceed one page (>1000 transfers) and need the `to` cursor,
    note this endpoint's `to` is ERRATIC -- pages OVERLAP NON-ADJACENTLY (a row seen on page 1 can reappear on page 3),
    so the previous-page-only `dedup_vs_previous_page` used by fills/funding/orders is NOT safe here. This pager keeps
    a GLOBAL `seen` set (bounded -- transfers are few per account, not the millions of fills) and advances the cursor
    by the whole page's min createdAt, forced strictly older if it stalls, stopping when a page yields ZERO new ids.
    NB the TOOL (main) already BUFFERS the whole set (own_wire_ids for signed amounts). fetch_transfers_all() flattens."""
    seen, cursor = set(), to_us
    eff_from = from_us if from_us else HISTORY_FROM_US   # 0/unset -> default window, so force a real floor
    first = True
    while True:
        q = {"address": address, "limit": PAGE_SIZE, "from": eff_from}
        if cursor is not None:
            q["to"] = cursor
        # Pace pages AFTER the first: a full page costs ~70 IP-weight and the per-IP bucket refills 25/s, so
        # unpaced back-to-back paging drives the bucket negative -> 429. First page rides the full bucket (no pause).
        data = _get_json(f"{BASE}/v1/accountTransferUpdates?{urllib.parse.urlencode(q)}", "transfers",
                         delay=(0.0 if first else page_pace_delay()))
        first = False
        rows = data.get("accountTransferUpdates")
        if rows is None:
            break
        if not isinstance(rows, list):
            raise SystemExit("display_transfers: unexpected response ('accountTransferUpdates' is not a list).")
        if not rows:
            break
        # GLOBAL dedup by id (id-less row -> full-row JSON): the `to` cursor overlaps non-adjacently, so a
        # previous-page-only set would re-emit rows. Track min createdAt over the WHOLE page (not just new rows)
        # so the cursor still advances even on an all-overlap page.
        fresh, page_min = [], None
        for r in rows:
            if not isinstance(r, dict):
                continue
            ca = created_us(r)
            if ca > 0:
                page_min = ca if page_min is None else min(page_min, ca)
            k = id_or_json_key(r, "id")
            if k in seen:
                continue
            seen.add(k)
            fresh.append(r)
        if fresh:
            yield fresh
        if len(rows) < PAGE_SIZE:        # with a real `from`, the API honors the limit -> a short page is the end
            break
        if not fresh:                    # a full page with NO new ids -> every reachable row has been seen
            break
        if page_min is None:
            break
        nc = page_min                    # oldest createdAt this page -> `to` for the next page (inclusive)
        if cursor is not None and nc >= cursor:
            nc = cursor - 1              # cursor stuck -> force it strictly older (µs) so paging can't loop
        if nc < 0:
            break
        cursor = nc


def fetch_transfers_all(address, from_us=None, to_us=None):
    """ALL transfer updates for the address, COLLECTED into one list -- the tool needs the full set (own_wire_ids
    for signed amounts, plus the table's widths/sort/totals). Streaming callers use iter_transfers_pages()."""
    return [r for page in iter_transfers_pages(address, from_us, to_us) for r in page]


def fetch_transfers(address, limit, from_us=None, to_us=None):
    """Bounded --limit N: ONE request. With a real `from` floor (HISTORY_FROM_US when --from is unset) the API
    HONORS `limit` and returns the newest N across ALL history (not the default recent window). Returns the rows
    list (non-dicts dropped)."""
    q = {"address": address, "limit": limit, "from": from_us if from_us else HISTORY_FROM_US}
    if to_us is not None:
        q["to"] = to_us
    data = _get_json(f"{BASE}/v1/accountTransferUpdates?{urllib.parse.urlencode(q)}", "transfers")
    rows = data.get("accountTransferUpdates")
    if rows is None:
        return []
    if not isinstance(rows, list):
        raise SystemExit("display_transfers: unexpected response ('accountTransferUpdates' is not a list).")
    return [r for r in rows if isinstance(r, dict)]


def is_applied(row):
    """APPLIED (or absent status -- successful rows carry no status) counts; REJECTED_* does not."""
    st = row.get("status")
    return st is None or st == "APPLIED"


def own_wire_ids(rows):
    """The account's own wire-account ids, learned from its deposits (destinationAccountId) and
    withdrawals (sourceAccountId). Used to infer SELF_ACCOUNT_TRANSFER direction."""
    ids = set()
    for r in rows:
        if r.get("type") == "DEPOSIT" and r.get("destinationAccountId"):
            ids.add(r["destinationAccountId"])
        if r.get("type") == "WITHDRAWAL" and r.get("sourceAccountId"):
            ids.add(r["sourceAccountId"])
    return ids


def signed_amount(row, own_ids):
    """Signed USDC effect on the wallet. INTERNAL_TRANSFER (same-user sub-account move) is
    net-neutral (0). SELF_ACCOUNT_TRANSFER (transfer to/from a DISTINCT user) changes the balance;
    direction is inferred from own wire ids (best-effort; 0 if it can't be determined)."""
    amt = dec(row.get("amount")) or Decimal(0)
    ty = row.get("type")
    if ty in INFLOW_TYPES:
        return amt
    if ty == "WITHDRAWAL":
        return -amt
    if ty == "INTERNAL_TRANSFER":
        return Decimal(0)
    if ty == "SELF_ACCOUNT_TRANSFER":
        if row.get("sourceAccountId") in own_ids:
            return -amt
        if row.get("destinationAccountId") in own_ids:
            return amt
        return Decimal(0)
    return Decimal(0)


def fmt_amount(d):
    s = f"{d:+,.6f}"
    return "0.000000" if s.lstrip("+-") in ("0", "0.000000") else s


COLS = [
    ("createdAt", "CREATED (UTC)", "<"), ("type", "TYPE", "<"), ("status", "STATUS", "<"),
    ("amount", "AMOUNT (USDC)", ">"), ("accountIndex", "IDX", ">"), ("id", "ID", "<"),
]


def main():
    global BASE, INFINITE_RETRY
    parser = argparse.ArgumentParser(description="Display account deposits/withdrawals/transfers.")
    parser.add_argument("address", help="Ethereum address of the account to display")
    parser.add_argument("--all", action="store_true",
                        help="ALSO show INTERNAL_TRANSFER rows (moves between your own sub-accounts "
                             "-- hidden by default; they net to zero for the wallet)")
    parser.add_argument("--limit", type=limit_arg, default=1000, metavar="N",
                        help="max rows to fetch: 1-1000 (default/max 1000), or 'unlimited' to page the "
                             "FULL history backward. NOTE the NET reconciliation is only COMPLETE with "
                             "'unlimited' (or a --from/--to window that covers the account); a bounded "
                             "fetch that fills the page warns that the NET is partial")
    parser.add_argument("--from", dest="from_us", type=epoch_us_arg, metavar="EPOCH_US",
                        help="only rows at/after this start time (epoch MICROseconds, inclusive -- the "
                             "createdAt unit, NOT ms)")
    parser.add_argument("--to", dest="to_us", type=epoch_us_arg, metavar="EPOCH_US",
                        help="only rows at/before this end time (epoch MICROseconds, inclusive)")
    parser.add_argument("--condensed", action="store_true",
                        help="machine-readable: one CSV row per transfer "
                             "(id,createdAt,type,status,accountIndex,signedAmount), no header/padding/totals")
    parser.add_argument("--header", action="store_true",
                        help="with --condensed, emit a CSV header row first (error without --condensed)")
    parser.add_argument("--verbose", action="store_true",
                        help="show EVERY field of each transfer (incl. netQuoteBalance/globalSequenceId/address/"
                             "sourceAccountId/destinationAccountId + the RAW `amount`, not the signed one): an aligned "
                             "key:value block per transfer, or -- with --condensed -- one all-fields CSV row per "
                             "transfer. Header emitted ONCE (implied by --verbose; buffered, so no #UPDATED_HEADER).")
    parser.add_argument("--infinite-retry", action="store_true",
                        help="never give up on TRANSIENT fetch failures (network timeout / 429 / 5xx): retry "
                             "forever with capped backoff instead of failing after 5 attempts -- for very large "
                             "multi-hour exports. A terminal 4xx (403/404) still fails fast.")
    add_network_args(parser)
    args = parser.parse_args()
    INFINITE_RETRY = args.infinite_retry
    BASE = NETWORKS[args.network]
    require_eth_address(args.address, "display_transfers")
    if args.from_us is not None and args.to_us is not None and args.from_us > args.to_us:
        raise SystemExit("display_transfers: --from must be <= --to.")
    if args.header and not args.condensed:
        raise SystemExit("display_transfers: --header requires --condensed.")

    if args.limit == UNLIMITED:
        raw = fetch_transfers_all(args.address, args.from_us, args.to_us)
        truncated = False                          # paginated to completeness (within --from/--to)
    else:
        raw = fetch_transfers(args.address, args.limit, args.from_us, args.to_us)
        truncated = len(raw) >= args.limit         # a full page (limit honored via the `from` floor) -> older rows exist -> NET partial
    if len(raw) > TABLE_MAX_ROWS:
        raise SystemExit(
            f"display_transfers: too many transfers to buffer (> {TABLE_MAX_ROWS:,}). This tool holds the full set "
            f"in memory in BOTH modes (own_wire_ids for signed amounts, plus the table's sort/totals). Narrow with "
            f"--from/--to or a smaller --limit.")
    hidden = sum(1 for r in raw if r.get("type") == "INTERNAL_TRANSFER")
    kept = raw if args.all else [r for r in raw if r.get("type") != "INTERNAL_TRANSFER"]
    kept.sort(key=created_us, reverse=True)      # newest-first
    own_ids = own_wire_ids(raw)

    if args.condensed:
        if args.verbose:
            write_verbose_csv(kept)   # --verbose: EVERY field, header ONCE (buffered; RAW amount, no signed transform)
            return
        writer = csv.writer(sys.stdout, lineterminator="\n")
        if args.header:
            writer.writerow(CONDENSED_KEYS)
        for r in kept:
            row = {**r, "amount": signed_amount(r, own_ids)}
            writer.writerow(["" if row.get(k) is None else row.get(k) for k in CONDENSED_KEYS])
        return

    if not kept:
        note = "" if args.all else " (same-user INTERNAL_TRANSFER rows hidden; use --all to show)"
        print(f"0 transfer(s) for {args.address} [{args.network}]{note}\n")
        return

    if args.verbose:   # block dump: EVERY field of each transfer (raw records -- RAW amount; sign derivable from type/direction)
        print_verbose_records(kept, f"{len(kept)} transfer(s) for {args.address} [{args.network}]", time_keys={"createdAt"})
        return

    def cell(k, r):
        if k == "amount":
            return fmt_amount(signed_amount(r, own_ids))
        if k == "createdAt":
            return when(r.get("createdAt"))
        v = r.get(k, "")
        return "" if v is None else str(v)

    widths = {k: len(h) for k, h, _ in COLS}
    for r in kept:
        for k, _, _ in COLS:
            widths[k] = max(widths[k], len(cell(k, r)))
    header = "  ".join(f"{h:{al}{widths[k]}}" for k, h, al in COLS)
    print(f"\n  Transfers: {args.address}  [{args.network}]\n")
    print(header)
    print("-" * len(header))
    for r in kept:
        print("  ".join(f"{cell(k, r):{al}{widths[k]}}" for k, _, al in COLS))
    print("-" * len(header))

    applied = [r for r in kept if is_applied(r)]
    def tot(ty):
        return sum((dec(r.get("amount")) or Decimal(0)) for r in applied if r.get("type") == ty)
    dep, wd, ref = tot("DEPOSIT"), tot("WITHDRAWAL"), tot("REFERRAL_CLAIM")
    xfernet = sum(signed_amount(r, own_ids) for r in applied if r.get("type") == "SELF_ACCOUNT_TRANSFER")
    net_flow = dep - wd + ref + xfernet
    extra = ""
    if ref:
        extra += f", referral {ref:,.6f}"
    if xfernet:
        extra += f", transfers {xfernet:+,.6f}"
    hid = f"   ({hidden} internal same-user transfer(s) hidden)" if hidden and not args.all else ""
    rej = len(kept) - len(applied)
    rejnote = f", {rej} rejected (excluded)" if rej else ""
    print(f"  {len(kept)} transfer(s){rejnote}   deposits {dep:,.6f}, withdrawals {wd:,.6f}{extra}   "
          f"NET {net_flow:+,.6f} USDC{hid}\n")
    if truncated:
        print(f"  WARNING: only the latest {args.limit} rows were fetched -- older rows may exist, so the NET "
              f"above is INCOMPLETE. Use --limit unlimited (optionally with --from/--to) for a full "
              f"deposits/withdrawals reconciliation.\n")


if __name__ == "__main__":
    run_pipe_safe(main)
