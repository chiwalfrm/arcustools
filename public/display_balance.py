"""
Display account balance / collateral for an account.

  python3 display_balance.py <eth_address>
  python3 display_balance.py <eth_address> --condensed   # CSV, one row of account fields (pipe/file/spreadsheet)
  python3 display_balance.py <eth_address> --condensed --header   # ... with a CSV header row first
  python3 display_balance.py <eth_address> --verbose              # EVERY account field (key:value block; nested positions as a blob)
  python3 display_balance.py <eth_address> --condensed --verbose  # EVERY account field, one CSV row (header once)

Uses GET /v1/account, a public account-scoped read that takes only the
`address` query parameter and needs NO signature -- so this display tool needs
just the address, not the creds file (same as display_orders/display_positions).

All monetary values are full quote-currency (USDC) decimal strings:
  equity          = netQuoteBalance + Σ(size × oracle)   -- total account value
  freeCollateral  = equity − Σ initial margin            -- available to trade
  netQuoteBalance = aggregate cash as of the last event  -- moves only on cash flows
"""

import argparse
import csv
import sys
import urllib.parse
from arcus_common_public import add_network_args, print_verbose_records, write_verbose_csv, require_eth_address, run_pipe_safe, NETWORKS, get_json, num, require_dict   # shared public helpers (formerly local copies)

BASE = None   # set in main() from the required --testnet/--staging/--mainnet selector


def count_positions(positions):
    """Open-position count, tolerant of shape (dict keyed by marketId, or list)."""
    return len(positions) if isinstance(positions, (dict, list)) else 0


# --condensed CSV columns (ONE row: the account's scalar balance fields), symmetric with the other display tools'
# --condensed. `openPositions` is the count of the nested positions object (the positions themselves are
# display_positions' domain, not repeated here). Raw values, no commas.
CONDENSED_COLS = [
    ("address", lambda a: a.get("address")),
    ("accountIndex", lambda a: a.get("accountIndex")),
    ("equity", lambda a: a.get("equity")),
    ("freeCollateral", lambda a: a.get("freeCollateral")),
    ("netQuoteBalance", lambda a: a.get("netQuoteBalance")),
    ("netDeposits", lambda a: a.get("netDeposits")),
    ("pendingDeposits", lambda a: a.get("pendingDeposits")),
    ("pendingWithdrawals", lambda a: a.get("pendingWithdrawals")),
    ("openPositions", lambda a: count_positions(a.get("positions"))),
    ("sequenceNumber", lambda a: a.get("sequenceNumber")),
]


def fetch_account(address):
    """GET /v1/account, turning network/HTTP/JSON failures into clean CLI errors."""
    query = urllib.parse.urlencode({"address": address})
    # Shared retrying reader (Retry-After/backoff on 429 incl. Cloudflare 1015 + 5xx). none_on_404: a 404 means
    # the address is valid but never traded/deposited -> friendly message rather than a raw HTTP error. A bad
    # address (400) and other non-429 4xx still raise SystemExit with the API's own error body, inside get_json.
    data = get_json(f"{BASE}/v1/account?{query}", what="account", prog="display_balance", none_on_404=True)
    if data is None:
        raise SystemExit(f"No activity yet for {address} (account has never been touched).")
    return require_dict(data, "account", "display_balance")


def main():
    global BASE
    parser = argparse.ArgumentParser(description="Display account balance / collateral.")
    parser.add_argument("address", help="Ethereum address of the account to display")
    parser.add_argument("--condensed", action="store_true",
                        help="machine-readable CSV: one row of the account fields "
                             "(address,accountIndex,equity,freeCollateral,netQuoteBalance,netDeposits,"
                             "pendingDeposits,pendingWithdrawals,openPositions,sequenceNumber), raw values")
    parser.add_argument("--header", action="store_true",
                        help="with --condensed, emit a CSV header row first "
                             "(error if used without --condensed)")
    parser.add_argument("--verbose", action="store_true",
                        help="show EVERY account field (incl. sequenceNumber + the nested positions object, which the "
                             "default view omits/summarizes): an aligned key:value block, or -- with --condensed -- one "
                             "all-fields CSV row. Header emitted ONCE (implied by --verbose; single fetch, no #UPDATED_HEADER).")
    add_network_args(parser)
    args = parser.parse_args()
    BASE = NETWORKS[args.network]
    address = args.address

    # Cheap local check -> a clear error before any network round-trip.
    require_eth_address(address, "display_balance")
    if args.header and not args.condensed:
        raise SystemExit("display_balance: --header requires --condensed.")

    acct = fetch_account(address)

    if args.condensed:
        if args.verbose:
            write_verbose_csv([acct])   # --verbose: EVERY account field, header ONCE (nested positions as a blob)
            return
        # One CSV row of the account's raw fields (no commas), for pipes / files / spreadsheets.
        writer = csv.writer(sys.stdout, lineterminator="\n")
        if args.header:
            writer.writerow([h for h, _ in CONDENSED_COLS])
        writer.writerow(["" if get(acct) is None else get(acct) for _, get in CONDENSED_COLS])
        return

    if args.verbose:   # block dump: EVERY account field (nested positions shown as a blob)
        print_verbose_records([acct], f"Account {acct.get('address', address)} (index {acct.get('accountIndex', '?')})", time_keys=frozenset())
        return

    # Label / value rows; values right-aligned in a shared field so cents line up.
    rows = [
        ("Equity",              num(acct.get("equity"))),
        ("Free collateral",     num(acct.get("freeCollateral"))),
        ("Net quote balance",   num(acct.get("netQuoteBalance"))),
        ("Net deposits",        num(acct.get("netDeposits"))),
        ("Pending deposits",    num(acct.get("pendingDeposits"))),
        ("Pending withdrawals", num(acct.get("pendingWithdrawals"))),
        ("Open positions",      str(count_positions(acct.get("positions")))),
    ]

    labelw = max(len(label) for label, _ in rows)
    valuew = max(len(value) for _, value in rows)

    print(f"Account {acct.get('address', address)}  (index {acct.get('accountIndex', '?')})\n")
    for label, value in rows:
        print(f"  {label:<{labelw}} : {value:>{valuew}}")


if __name__ == "__main__":
    run_pipe_safe(main)
