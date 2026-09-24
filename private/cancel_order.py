"""
Cancel orders on Arcus (v5 signing).

  python3 cancel_order.py --all                  # cancel every open order (all markets)
  python3 cancel_order.py --orderid 0xabc123      # cancel one order by its server order ID
  python3 cancel_order.py --clientid mybid        # cancel one order by its clientId
  python3 cancel_order.py --clientid mybid --market BTC-USD   # disambiguate a reused clientId

Exactly one of --all / --orderid / --clientid is required.

Signing (see ordersign.py):
  * cancelOrder uses the TYPED canonical payload (op=2): keys ad,ai,[c,]ct,[id,]m,op,v.
    It needs the order's marketId, resolved from GET /v1/openOrders by matching the
    orderId or clientId.
  * cancelAllOrders uses the LEGACY scheme: ts_ns + "cancelAllOrders" + canonicalJSON(body).
  * Both also require `address` as a QUERY parameter (not part of the signature).

These are asynchronous: a 202 means accepted; the terminal CANCELED arrives over the
orders WS. Resolves ordersign.py / arcus_creds_<network>.json relative to this script.
"""

import argparse
import json
import os
import sys
import urllib.parse

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

from ordersign import Signer
import arcus_common_private
from arcus_common_private import (add_network_args, call, check_order_response, fetch_open_orders, load_creds,
                          select_network, server_clock_shim, validate_client_id)


def parse_args():
    parser = argparse.ArgumentParser(description="Cancel orders on Arcus.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--all", action="store_true",
                       help="cancel every open order on the account (all markets)")
    group.add_argument("--orderid", metavar="ID", help="cancel one order by its server order ID")
    group.add_argument("--clientid", "--client-id", dest="clientid", metavar="CID",
                       help="cancel one order by its clientId")
    parser.add_argument("--market", help="restrict an --orderid/--clientid match to this market "
                                          "(disambiguate a reused clientId)")
    add_network_args(parser)
    args = parser.parse_args()
    # --market only disambiguates a single-order cancel. With --all it would be
    # silently ignored (the cancel hits EVERY market), so reject the combo rather
    # than appear to scope. (Market-scoped cancel-all is API-supported but not
    # implemented here.)
    if args.all and args.market:
        parser.error("--market cannot be combined with --all; --all cancels every market. "
                     "Use --orderid/--clientid to target a single order.")
    return args


def cancel_all(address, account_index, signer, query):
    print(f"Cancelling ALL open orders for {address} at {arcus_common_private.BASE}.")
    body = {"address": address, "accountIndex": account_index}
    with server_clock_shim():                                   # server-align the X-Timestamp so a drifted clock can't 401 the panic cancel
        headers = signer.sign_legacy("/v1/cancelAllOrders", body)   # legacy scheme
    resp = call("POST", f"/v1/cancelAllOrders?{query}", body, headers)
    print("Cancel-all response:", json.dumps(resp, indent=2))
    check_order_response(resp, "cancelAllOrders", cancel=True)   # order-not-found on a cancel = already gone (idempotent success)


def cancel_one(args, address, account_index, signer, query):
    # FAIL CLOSED on an unreadable openOrders body (non-dict / missing / non-list 'orders'): order state is
    # UNKNOWN, so we must not read it as "no matching order" (a resting order could be missed) or traceback.
    orders = fetch_open_orders(query, "cancel_order")
    if args.orderid:
        cands = [o for o in orders if o.get("orderId") == args.orderid]
        ident_desc = f"orderId {args.orderid}"
    else:
        cands = [o for o in orders if o.get("clientId") == args.clientid]
        ident_desc = f"clientId {args.clientid}"
    if args.market:
        # --market may be a numeric marketId or a case-insensitive display name;
        # open orders carry both, so filter directly (no /v1/markets lookup needed).
        if args.market.isdigit():
            mid = str(int(args.market))
            cands = [o for o in cands if str(o.get("marketId")) == mid]
        else:
            up = args.market.upper()
            cands = [o for o in cands if str(o.get("marketDisplayName", "")).upper() == up]

    if not cands:
        where = f" in {args.market}" if args.market else ""
        raise SystemExit(f"No open order with {ident_desc}{where} (already filled/canceled, or wrong id).")
    if len(cands) > 1:
        markets = ", ".join(sorted(str(o.get("marketDisplayName")) for o in cands))
        raise SystemExit(f"{len(cands)} open orders match {ident_desc} (markets: {markets}); "
                         f"pass --market to disambiguate.")
    match = cands[0]
    try:
        market_id = int(match["marketId"])
    except (KeyError, ValueError, TypeError):
        raise SystemExit(f"matched order has no valid marketId: {match.get('marketId')!r}.")

    # Identify the cancel by whichever was given (typed payload op=2). Wrap the sign in the clock shim so a
    # drifted local clock can't push the X-Timestamp outside the server's +/-30 s auth window -> 401.
    if args.orderid:
        with server_clock_shim():
            headers = signer.sign_cancel_order(address=address, account_index=account_index,
                                               market_id=market_id, order_id=args.orderid)
        body = {"address": address, "accountIndex": account_index,
                "marketId": market_id, "kind": "orderId", "orderId": args.orderid}
    else:
        with server_clock_shim():
            headers = signer.sign_cancel_order(address=address, account_index=account_index,
                                               market_id=market_id, client_id=args.clientid)
        body = {"address": address, "accountIndex": account_index,
                "marketId": market_id, "kind": "clientId", "clientId": args.clientid}

    resp = call("POST", f"/v1/cancelOrder?{query}", body, headers)
    print(f"Cancel response ({match.get('marketDisplayName', '')}):", json.dumps(resp, indent=2))
    check_order_response(resp, "cancelOrder", cancel=True)   # order-not-found on a cancel = already gone (idempotent success)


def main():
    args = parse_args()
    select_network(args.network)
    if args.clientid is not None:
        validate_client_id(args.clientid)

    creds = load_creds()
    address = creds["eth_address"]
    account_index = creds["account_index"]
    signer = Signer.from_private_key_hex(creds["api_private_key"])
    query = urllib.parse.urlencode({"address": address})

    if args.all:
        cancel_all(address, account_index, signer, query)
    else:
        cancel_one(args, address, account_index, signer, query)


if __name__ == "__main__":
    main()
