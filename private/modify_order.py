"""
Modify (cancel + replace) an open order on Arcus, identified by orderId (the API
requires it) and/or clientId.

  # convenience: identify by clientId; ONE openOrders lookup resolves the orderId,
  # the immutable fields, and any omitted price/quantity.
  python3 modify_order.py --clientid mmbid1 --price 41800 --quantity 0.002 --testnet
  python3 modify_order.py --clientid mmbid1 --price 41800 --testnet     # reprice only
  python3 modify_order.py --orderid 0xabc123 --quantity 0.002 --testnet # resize only

  # FAST PATH (no server call, for tight loops): pass --orderid + all immutables
  # (--market --side --tif) + BOTH --price and --quantity, plus --reduce-only if it's
  # reduce-only, and DO NOT pass --clientid (passing it forces a lookup to validate it,
  # since it is used as a cross-check and never sent to the venue).
  python3 modify_order.py --orderid 0xabc123 \
      --price 41800 --quantity 0.002 --market BTC-USD --side BUY --tif GTT --testnet

Identify with --orderid and/or --clientid (at least one). The venue ALWAYS identifies a
modify by orderId AND requires EXACTLY ONE of orderId/clientId (sending both -> HTTP 400),
so clientId is used only to LOOK UP the order (resolved to its orderId) and is never echoed.
The orderId is preserved across modifies, so it's a stable handle to cache; the replacement
also keeps the order's original clientId (verified live), so clientId-based tracking survives.

What modifyOrder is: the server does an atomic cancel + replace (orderId preserved).
Only `price`/`quantity`/`goodTilTime` change; `side`, `timeInForce`, `marketId`, and
`reduceOnly` are IMMUTABLE and verified against the resting order (mismatch -> rejected).
When we look the order up, supplied overrides are checked first.

Signing (see ordersign.py): modifyOrder is the TYPED payload op=3
  {ad,ai,[c,]ct,g,[id,]m,op,p,q,r,s,t,v} -- orderId (`id`) required and is the SOLE
  identifier we send; we omit `c` (clientId) so the venue's exactly-one rule is satisfied.
  goodTilTime/reduceOnly/side/timeInForce are all part of the signature.
goodTilTime: a same-price size-reduce REUSES the resting order's goodTilTime so the venue keeps the order
IN PLACE (preserving queue priority); every other modify uses a fresh goodTilTime >= 1 month out (365 days).

Resolves ordersign.py / arcus_redis.py / arcus_creds_<network>.json relative to this script.
"""

import argparse
import json
import os
import sys
import time
import urllib.parse

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

import arcus_redis as marketcache
import ordersign
from ordersign import Signer
from arcus_common_private import (add_network_args, call, check_order_response, clock_delta, dec, fetch_open_orders, load_creds, positive_decimal,
                          select_network, server_clock_shim, to_quantums, to_ticks, validate_client_id)

TIFS = ("GTT", "IOC", "FOK", "ALO")
SIDES = {"BUY": ordersign.SIDE_BUY, "SELL": ordersign.SIDE_SELL}
TIF_INT = {"GTT": ordersign.TIF_GTC, "IOC": ordersign.TIF_IOC,
           "FOK": ordersign.TIF_FOK, "ALO": ordersign.TIF_ALO}
# The replacement order needs a fresh goodTilTime >= 1 month out (server rule).
GOOD_TIL_DAYS = 365


def parse_args():
    parser = argparse.ArgumentParser(description="Modify an open order by clientId/orderId (cancel + replace).")
    parser.add_argument("--clientid", "--client-id", dest="client_id",
                        help="the order's clientId — used to look up the order (the modify itself is "
                             "always identified to the venue by orderId; clientId is not echoed)")
    parser.add_argument("--orderid", metavar="ID",
                        help="server order ID (the API identifies a modify by this); pass it "
                             "with --market/--side/--tif/--price/--quantity to skip the lookup")
    parser.add_argument("--price", help="new limit price (decimal > 0); omit to keep current")
    parser.add_argument("--quantity", help="new size in base-asset units (decimal > 0); omit to keep current")
    parser.add_argument("--market", help="market display name (e.g. BTC-USD)")
    parser.add_argument("--side", choices=["BUY", "SELL"])
    parser.add_argument("--tif", choices=list(TIFS))
    parser.add_argument("--reduce-only", action="store_true",
                        help="the order is reduce-only (must match the original)")
    parser.add_argument("--refresh-cache", action="store_true",
                        help="force re-fetch of market metadata (bypass the Redis cache)")
    add_network_args(parser)
    args = parser.parse_args()
    if not args.client_id and not args.orderid:
        parser.error("specify --orderid and/or --clientid")
    if args.price is None and args.quantity is None:
        parser.error("specify --price and/or --quantity")
    return args


def main():
    args = parse_args()
    select_network(args.network)

    # --- Validate inputs locally before any lookup/signing --------------------
    if args.client_id is not None:
        validate_client_id(args.client_id)
    if args.price is not None:
        positive_decimal(args.price, "--price")
    if args.quantity is not None:
        positive_decimal(args.quantity, "--quantity")

    creds = load_creds()
    address = creds["eth_address"]
    account_index = creds["account_index"]
    signer = Signer.from_private_key_hex(creds["api_private_key"])

    query = urllib.parse.urlencode({"address": address})
    # orderId-first, to MATCH the lookup below (which locates by orderId when --orderid is present and only
    # falls back to clientId otherwise). If this were clientId-first, a "not found" error on a bad --orderid
    # passed alongside a valid --clientid would wrongly blame the clientId.
    ident_desc = f"orderId {args.orderid}" if args.orderid else f"clientId {args.client_id}"

    # The API REQUIRES orderId to identify a modify (and rejects sending both orderId+clientId), so a
    # --clientid is only used to LOOK UP + validate the order, never echoed. FAST PATH (no lookup):
    # --orderid present + all immutables (--market/--side/--tif) + BOTH --price/--quantity + NO --clientid.
    # Otherwise ONE openOrders lookup resolves the orderId, recovers the immutable fields (and validates
    # a supplied --clientid against the order's actual clientId), and supplies any omitted price/quantity.
    # A passed --clientid ALWAYS forces the lookup: since we no longer echo it, the only way it can act as a
    # cross-check is to validate it against the resting order -- otherwise a wrong --clientid on the fast path
    # would be silently ignored (the assertion "this order is clientId X" would be a no-op).
    have_all_immutable = bool(args.market and args.side and args.tif)
    have_both_pq = args.price is not None and args.quantity is not None
    need_lookup = bool(args.client_id) or not (args.orderid and have_all_immutable and have_both_pq)

    match = None
    if need_lookup:
        # FAIL CLOSED on an unreadable openOrders body (non-dict / missing / non-list 'orders'): order state is
        # UNKNOWN, so we must not read it as "no matching order" (line below) or traceback on `.get`/iteration.
        orders = fetch_open_orders(query, "modify_order")
        if args.orderid:
            match = next((o for o in orders if o.get("orderId") == args.orderid), None)
        else:
            matches = [o for o in orders if o.get("clientId") == args.client_id]
            if len(matches) > 1:
                raise SystemExit(f"clientId {args.client_id!r} matches {len(matches)} open orders; "
                                 f"pass --orderid to disambiguate.")
            match = matches[0] if matches else None
        if match is None:
            raise SystemExit(f"No open order with {ident_desc} (already filled/canceled, or wrong id).")

    # Resolve the immutable fields + identity. openOrders echoes timeInForce as "GTC"
    # for t=0; normalize to GTT.
    if match is not None:
        order_id = match.get("orderId")
        client_echo = match.get("clientId")            # the order's ACTUAL clientId (may be None)
        actual_market = match.get("marketDisplayName")
        actual_side = match.get("side")
        actual_tif = {"GTC": "GTT"}.get(match.get("timeInForce"), match.get("timeInForce"))
        actual_reduce = bool(match.get("reduceOnly"))
        # Supplied overrides MUST agree with the live order, else we'd sign/send the
        # wrong (immutable) values and get a server rejection. A supplied --clientid must match
        # the order's ACTUAL clientId -- including the case where the order has NONE (client_echo
        # is None): a mismatched/extraneous --clientid must be rejected, not silently ignored.
        if args.client_id is not None and args.client_id != client_echo:
            if client_echo is None:
                raise SystemExit(f"--clientid {args.client_id!r} was given, but the order has NO clientId.")
            raise SystemExit(f"--clientid {args.client_id!r} does not match the order's {client_echo!r}.")
        if args.market is not None:
            ok = (str(int(args.market)) == str(match.get("marketId")) if args.market.isdigit()
                  else args.market.upper() == str(actual_market).upper())   # int() normalizes "01"==1 (matches cancel_order)
            if not ok:
                raise SystemExit(f"--market {args.market!r} does not match the order's "
                                 f"{actual_market!r} (id {match.get('marketId')}).")
        for name, supplied, actual in (("side", args.side, actual_side),
                                       ("tif", args.tif, actual_tif)):
            if supplied is not None and supplied != actual:
                raise SystemExit(f"--{name} {supplied!r} does not match the order's {actual!r}.")
        if args.reduce_only and not actual_reduce:
            raise SystemExit("--reduce-only given, but the order is not reduce-only.")
        market_name, side, tif, reduce_only = actual_market, actual_side, actual_tif, actual_reduce
    else:
        order_id = args.orderid
        client_echo = args.client_id                   # may be None -> order must have no clientId
        market_name, side, tif, reduce_only = args.market, args.side, args.tif, args.reduce_only

    # orderId is REQUIRED to sign a modify (it identifies the resting order). The lookup path takes it from the
    # matched /v1/openOrders row, which can be missing/null on a malformed body -> fail CLEAN here rather than a
    # confusing downstream signing error. (The fast path's order_id is the user-supplied --orderid, always present.)
    if not order_id:
        raise SystemExit("modify_order: the matched order has no usable orderId; cannot modify "
                         "(the /v1/openOrders row is missing it).")

    # In the LOOKUP path side/tif come straight from the resting order (openOrders) and are otherwise
    # unvalidated: unless the user passed --side/--tif, the mismatch check above never runs, so an unexpected
    # value (or None if the field is missing) would KeyError in SIDES[side]/TIF_INT[tif] at signing. Fail clean
    # instead. (The fast path's side/tif come from argparse choices, so this is a no-op there.)
    if side not in SIDES:
        raise SystemExit(f"modify_order: order has an unrecognized side {side!r} (expected one of {sorted(SIDES)}).")
    if tif not in TIF_INT:
        raise SystemExit(f"modify_order: order has an unrecognized timeInForce {tif!r} "
                         f"(expected one of {sorted(TIF_INT)}).")

    # Price/quantity: user value if given, else keep the order's current value (lookup only).
    # For quantity, preserve REMAINING (still-open) size, NOT originalSize -- modify is an atomic
    # cancel+replace and `quantity` is the replacement order's full resting size, so re-using
    # originalSize on a PARTIALLY-FILLED order would re-expand it (e.g. a remaining 0.4 back to 1.0,
    # increasing exposure). remainingSize == originalSize for an unfilled order, so no change there.
    price = str(args.price) if args.price is not None else str(match.get("price"))
    quantity = str(args.quantity) if args.quantity is not None else str(match.get("remainingSize"))
    # The order's price/remainingSize come straight from /v1/openOrders; a non-numeric or non-finite
    # value (Infinity/NaN) would else reach ordersign's int(rounded) as an uncaught OverflowError while
    # SIGNING. Validate here (dec() rejects non-finite) -- the signing lib stays untouched.
    dp, dq = dec(price), dec(quantity)
    if dp is None or dq is None or dp <= 0 or dq <= 0:   # finite AND > 0: a non-positive remainingSize means the order
        raise SystemExit(f"modify_order: order price/size must be a finite POSITIVE number "   # is fully filled/gone; a
                         f"(price={price!r}, size={quantity!r}).")                              # 0/neg price is invalid

    # Market metadata (Redis-cached) -> tick/step; exact-multiple conversion.
    try:
        mkt = marketcache.get_market(market_name, args.network, refresh=args.refresh_cache)
    except marketcache.MarketCacheError as e:
        raise SystemExit(f"marketcache: {e}")
    # Cross-check cached metadata against the live order (in-memory; no extra
    # network/Redis call) -- a stale/corrupt cache must never make us sign a
    # modify for the wrong marketId.
    if match is not None and str(mkt["marketId"]) != str(match.get("marketId")):
        raise SystemExit(f"market-cache mismatch: cache says marketId {mkt['marketId']} for "
                         f"{market_name!r}, but the open order is marketId {match.get('marketId')} "
                         f"(try --refresh-cache).")
    market_id = mkt["marketId"]
    market_name = mkt.get("marketDisplayName", market_name)   # canonical for display
    price_ticks = to_ticks(price, mkt["tickSize"])
    quantity_quantums = to_quantums(quantity, mkt["stepSize"])

    # QUEUE PRIORITY (venue contract): a modify is applied IN PLACE -- preserving queue priority -- only when
    # it keeps the SAME price, REDUCES size, AND leaves goodTilTime UNCHANGED. Any price move, size increase,
    # or differing goodTilTime is an atomic cancel+replace that LOSES priority. This is a manual tool for real
    # users, so we PRESERVE priority when we can: on a same-price size-reduce (lookup path only -- we need the
    # resting order's goodTilTime to echo it), reuse the resting goodTilTime instead of stamping a fresh one.
    # Otherwise (price change, size increase/keep, the fast path with no lookup, or a resting expiry too close
    # to the venue's ~1-month floor to safely echo) use a fresh 365d expiry. Echoing is harmless when it does
    # NOT enable in-place (those cases cancel+replace regardless of goodTilTime). NB: in-place vs cancel+replace
    # is not directly observable (no queue-position field; createdAt resets on every modify), so this follows
    # the documented contract. (Changelog review #3, 2026-08-20.)
    # Compute the GTT off the SERVER-corrected clock, and reuse the SAME offset for the sign shim below so
    # /v1/time is fetched ONCE, not twice. clock_delta() is fail-soft (0 if /v1/time is down -> local clock,
    # the prior behavior). The 365-day expiry makes drift immaterial to the order, but this keeps the GTT and
    # the signature's X-Timestamp on one clock (parity with place_order / market_maker).
    delta_ns = clock_delta()
    now_us = (time.time_ns() + delta_ns) // 1000
    fresh_gtt = str(now_us + GOOD_TIL_DAYS * 86_400 * 1_000_000)
    resting_gtt = dec(str(match.get("goodTilTime"))) if match is not None else None
    resting_price = dec(str(match.get("price"))) if match is not None else None
    resting_remaining = dec(str(match.get("remainingSize"))) if match is not None else None
    one_month_floor_us = now_us + 35 * 86_400 * 1_000_000   # safe margin over the venue's ~1-month minimum
    preserve_priority = (
        match is not None
        and resting_price is not None and dp == resting_price               # SAME price (dp is the resolved price)
        and resting_remaining is not None and dq < resting_remaining        # REDUCED size
        and resting_gtt is not None and resting_gtt >= one_month_floor_us   # resting expiry still safely >= 1 month out
    )
    good_til_us = str(int(resting_gtt)) if preserve_priority else fresh_gtt

    kept = [n for n, v in (("price", args.price), ("quantity", args.quantity)) if v is None]
    src = "overrides (no lookup)" if not need_lookup else f"openOrders ({ident_desc})"
    echo_note = f" (order's clientId {client_echo}, not echoed)" if client_echo else ""
    print(f"MODIFY orderId {order_id}{echo_note}  {side} {quantity} {market_name} @ {price}  "
          f"tif={tif} reduceOnly={reduce_only}")
    print(f"  fields from: {src}" + (f"; kept current {', '.join(kept)}" if kept else ""))
    if preserve_priority:
        print("  same price + reduced size: keeping the resting goodTilTime to preserve queue priority "
              "(in-place modify per the venue contract)")

    # --- Sign the typed modify payload (op=3), then POST ----------------------
    # server_clock_shim(delta_ns): sign_modify_order mints its X-Timestamp from an internal time.time_ns(); the
    # shim server-aligns it so a drifted local clock can't 401 the modify (parity with place_order/close_position).
    # Reuses the delta measured above for the GTT, so no second /v1/time round-trip.
    with server_clock_shim(delta_ns):
        headers = signer.sign_modify_order(
            address=address, account_index=account_index, market_id=market_id,
            price_ticks=price_ticks, quantity_quantums=quantity_quantums,
            good_til_time_ns_=ordersign.good_til_time_ns(good_til_us),
            reduce_only=reduce_only, side=SIDES[side], time_in_force=TIF_INT[tif],
            order_id=order_id, client_id=None,     # identify by orderId ONLY -- see below
        )
    # Identify the modify by orderId ONLY: the venue requires EXACTLY ONE of orderId/clientId in BOTH the
    # signed payload AND the body -- sending both is rejected (HTTP 400 "provide exactly one of orderId or
    # clientId (not both)"; verified live 2026-08-20). ordersign requires order_id anyway, so we omit the
    # clientId echo (client_id=None above -> no `c` in the canonical payload) and DON'T put clientId in the
    # body. A --clientid arg is still used to LOOK UP + validate the order above; it is just not echoed. The
    # replacement keeps the order's original clientId regardless (verified live), so nothing downstream that
    # tracks orders by clientId (e.g. the market maker) is affected.
    body = {
        "address": address, "accountIndex": account_index, "marketId": market_id,
        "orderId": order_id, "side": side, "timeInForce": tif,
        "price": price, "quantity": quantity, "reduceOnly": reduce_only,
        "goodTilTime": good_til_us,
    }

    resp = call("POST", f"/v1/modifyOrder?{query}", body, headers)
    print("Modify response:", json.dumps(resp, indent=2))
    check_order_response(resp, "modifyOrder")


if __name__ == "__main__":
    main()
