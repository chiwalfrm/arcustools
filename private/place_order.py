"""
Place an order on Arcus (v5 typed-payload signing).

LIMIT (a --price is given):
  python3 place_order.py --market BTC-USD --quantity 0.1 --price 50000
  python3 place_order.py --market ETH-USD --side SELL --quantity 1 --price 4000 --tif GTT

MARKET (no --price): orderType=MARKET, --tif defaults to IOC (override with --tif):
  python3 place_order.py --market BTC-USD --quantity 0.5
  python3 place_order.py --market BTC-USD --side SELL --quantity 0.5 --force

TRIGGER / stop-loss / take-profit (--trigger is the trigger/stop price; --tpsl sl|tp, default sl):
  Standalone conditional order via POST /v1/placeOrder with tpslType + stopPrice, signed op=4 (untriggered).
  # stop-loss MARKET: SELL 10.99 ZEC-USD when mark drops to/through 901.2 (no --price => market on trigger):
  python3 place_order.py --market ZEC-USD --side SELL --quantity 10.99 --trigger 901.2 --reduce-only
  # stop-loss LIMIT: same trigger, rest a GTT limit at 895 when it fires (give --price):
  python3 place_order.py --market ZEC-USD --side SELL --quantity 10.99 --trigger 901.2 --price 895 --reduce-only
  # --tpsl tp = take-profit (forced reduce-only). A stop-loss is almost always --reduce-only too.
  # Fires on MARK price; a trigger already on the wrong side of mark is rejected (InvalidRequest).

Sizing -- give exactly one of --quantity / --quantityusd:
  --quantity      order size in base-asset units (e.g. 0.5 BTC).
  --quantityusd   spend this many USD; the coin quantity is derived -- from --price
                  for a limit order, or by walking the book to the budget for a
                  market order -- then rounded DOWN to the market step size.

Market-order slippage guard:
  Pulls GET /v1/l2OrderBook/<market>, sorts the book, computes mid (best bid/ask),
  and walks it (asks for BUY, bids for SELL) to estimate the average fill price for
  --quantity. If that average deviates more than MAX_SLIPPAGE (3%) from mid, the
  order is NOT placed unless --force (market only). The protective price bound is
  the worst consumed level +/-PRICE_BUFFER (normal) or mark +/-FORCE_MARK_BOUND
  (--force), tick-aligned.

Signing (see ordersign.py): placeOrder is signed over the TYPED canonical payload.
This is the only step that uses the Ed25519 API PRIVATE key.

Resolves ordersign.py and arcus_creds_<network>.json relative to this script (in private/), so it works from any working directory.
"""

import argparse
import json
import os
import sys
import time
import urllib.parse
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

# Resolve ordersign / arcus_common_private / arcus_creds_<network>.json relative to THIS script
# (in private/), so it works from any cwd and can't import a stray module.
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

import ordersign
from ordersign import Signer
from arcus_common_private import (
    add_network_args, call, check_order_response, clock_delta, dec, load_creds, place_market_ioc, positive_decimal, resolve_market, round_to_increment, select_network, to_quantums, to_ticks, validate_client_id)

# The MARKET-order path (book-walk + slippage guard + protective bound + sign/POST, and its MAX_SLIPPAGE /
# PRICE_BUFFER / FORCE_MARK_BOUND constants) now lives in arcus_common_private.place_market_ioc, shared with
# pivot_trader.py. This module keeps only the LIMIT path + USD sizing + the CLI.
SIDES = {"BUY": ordersign.SIDE_BUY, "SELL": ordersign.SIDE_SELL}
TIFS = {"GTT": ordersign.TIF_GTC, "FOK": ordersign.TIF_FOK,
        "IOC": ordersign.TIF_IOC, "ALO": ordersign.TIF_ALO}

# The venue now REQUIRES a goodTilTime >= 1 month in the future on EVERY order
# (incl. IOC/FOK/market -- IOC/market won't actually rest, but the field is
# validated). 365 days clears the minimum with a wide margin.
GOOD_TIL_DAYS = 365

# For a TPSL MARKET leg the venue still requires a price, validated to within 10% of stopPrice (NOT live mark).
# Push the execution price THROUGH the trigger by this fraction so it fills like a market order when it fires.
# Must stay < 0.10 to clear the server's 10%-of-stopPrice band.
TRIGGER_MARKET_SLIP = Decimal("0.05")


# ── place_order-specific helpers ──────────────────────────────────────────────
def to_step(qty, step_size):
    """Round a base-asset quantity DOWN to a valid step multiple (never overspends)."""
    q = round_to_increment(qty, step_size, ROUND_FLOOR)
    if q <= 0:
        raise SystemExit(f"computed quantity rounds to 0 at step {step_size}; increase --quantityusd.")
    return q


# ── Order-book walking (USD sizing only; the base-size walk lives in arcus_common_private.walk_book) ──────
def walk_book_usd(levels, budget):
    """Walk pre-sorted levels by USD budget -> (filled_qty, avg, worst, enough)."""
    spent, filled, worst = Decimal(0), Decimal(0), None
    for price_s, size_s in levels:
        remaining = budget - spent
        if remaining <= 0:
            break
        price, size = Decimal(price_s), Decimal(size_s)
        level_cost = price * size
        if level_cost <= remaining:
            spent += level_cost
            filled += size
        else:
            filled += remaining / price
            spent = budget
        worst = price_s
    avg = (spent / filled) if filled > 0 else None
    return filled, avg, worst, spent >= budget


# ── CLI ────────────────────────────────────────────────────────────────────────
def parse_args():
    parser = argparse.ArgumentParser(description="Place an order on Arcus.")
    parser.add_argument("--market", default="BTC-USD", help="market display name, e.g. BTC-USD")
    parser.add_argument("--side", default="BUY", choices=list(SIDES))
    qty_group = parser.add_mutually_exclusive_group(required=True)
    qty_group.add_argument("--quantity", help="order size in base-asset units (decimal > 0)")
    qty_group.add_argument("--quantityusd", help="order size in USD (decimal > 0); rounded DOWN to step")
    parser.add_argument("--price", default=None, help="limit price (decimal > 0). Omit for a MARKET order. "
                        "With --trigger: given => stop-LIMIT at this price; omitted => stop-MARKET on trigger.")
    parser.add_argument("--trigger", default=None, help="trigger (stop) price (decimal > 0): makes this a TPSL / "
                        "conditional order. Fires when mark price reaches it. See --tpsl.")
    parser.add_argument("--tpsl", default="sl", choices=["sl", "tp"],
                        help="for --trigger orders: 'sl' stop-loss (default) or 'tp' take-profit (forced reduce-only).")
    parser.add_argument("--tif", default=None, choices=list(TIFS),
                        help="time-in-force (default GTT for limit, IOC for market; forced for trigger orders)")
    parser.add_argument("--reduce-only", action="store_true", help="reduce-only order")
    parser.add_argument("--clientid", "--client-id", dest="client_id",
                        help="client-assigned order id (1-36 chars, [A-Za-z0-9_-])")
    parser.add_argument("--force", action="store_true",
                        help="market orders only: place even if slippage exceeds 3%%")
    add_network_args(parser)
    return parser.parse_args()


def main():
    args = parse_args()
    select_network(args.network)
    is_trigger = args.trigger is not None
    is_market = args.price is None and not is_trigger   # a trigger order with no --price is a stop-MARKET, not a plain MARKET
    tif = args.tif or ("IOC" if is_market else "GTT")
    # Server rule: MARKET orders must be IOC (no resting/FOK market orders).
    if is_market and tif != "IOC":
        raise SystemExit(f"--tif {tif}: a MARKET order (no --price) must be IOC.")
    # (Historical note: reduce-only orders once had to be IOC or FOK. testnet-v1.1.98, 2026-07-08,
    # dropped that rule -- a resting reduce-only order is now valid with any TIF. Confirmed live on
    # testnet 2026-08-20: a reduce-only GTT rested (status OPEN) instead of the old validation reject.
    # So no reduce-only/TIF guard here; only the MARKET-must-be-IOC rule above still applies.)

    # --- Validate all inputs locally, before any signing/sending --------------
    if args.price is not None:
        positive_decimal(args.price, "--price")
    if args.trigger is not None:
        positive_decimal(args.trigger, "--trigger")
    if args.quantity is not None:
        positive_decimal(args.quantity, "--quantity")
    if args.quantityusd is not None:
        positive_decimal(args.quantityusd, "--quantityusd")
    if args.client_id is not None:
        validate_client_id(args.client_id)
    if args.force and not is_market:
        where = "trigger orders" if is_trigger else "limit orders"
        print(f"note: --force is ignored for {where} (slippage guard is plain-market-only).")

    creds = load_creds()
    address = creds["eth_address"]
    account_index = creds["account_index"]
    signer = Signer.from_private_key_hex(creds["api_private_key"])

    # --- Resolve market (by id or case-insensitive name) -> canonical name ----
    resp = call("GET", "/v1/markets")
    if not isinstance(resp, dict) or not isinstance(resp.get("markets"), list):
        raise SystemExit("place_order: unexpected /v1/markets response (expected an object with a 'markets' list).")
    markets = resp["markets"]
    mkt = resolve_market(markets, args.market)
    if mkt is None:
        raise SystemExit(f"Unknown market {args.market!r}.")
    try:
        market_id = int(mkt["marketId"])
        tick_size, step_size = mkt["tickSize"], mkt["stepSize"]
        dt, ds = dec(tick_size), dec(step_size)
        if dt is None or ds is None or dt <= 0 or ds <= 0:   # require FINITE and > 0: a 0 increment -> DivisionByZero in
            raise ValueError                                  # round_to_increment; a NEGATIVE one -> negative signed ticks/
                                                              # quantums (ordersign rejects only a ZERO divisor). Matches the
                                                              # cache/market-maker finite-positive tick/step validation.
        args.market = mkt["marketDisplayName"]   # canonicalize (l2OrderBook path needs it)
    except (KeyError, ValueError, TypeError):
        raise SystemExit(f"place_order: market {args.market!r} has incomplete/malformed metadata "
                         f"(marketId / tickSize>0 / stepSize>0 / marketDisplayName).")
    usd_mode = args.quantityusd is not None
    # Optional order-size bounds (minOrderSize/maxOrderSize v1.3.8, minOrderNotional): validated against the
    # FINAL qty below for a clear client-side error instead of a venue OrderSizeTooLarge / min-size / min-notional
    # reject. Each bound is skipped when absent/null/non-positive.
    min_order_size = dec(mkt.get("minOrderSize"))
    max_order_size = dec(mkt.get("maxOrderSize"))
    min_order_notional = dec(mkt.get("minOrderNotional"))

    # --- Determine the order's quantity and price -----------------------------
    if is_market:
        # Size the order in base units, then hand off to the shared MARKET-IOC path: the book-walk + slippage
        # guard + protective bound + size/notional check + sign/POST live in place_market_ioc (arcus_common_private),
        # the SAME code pivot_trader.py uses. Only USD sizing (which walks the book by budget) stays here.
        if usd_mode:
            ob = call("GET", f"/v1/l2OrderBook/{urllib.parse.quote(args.market)}")
            if not isinstance(ob, dict):
                raise SystemExit(f"{args.market}: unexpected /v1/l2OrderBook response (not a JSON object).")
            bids, asks = ob.get("bids", []), ob.get("asks", [])
            if not bids or not asks:
                raise SystemExit(f"{args.market}: order book has no two-sided liquidity.")
            try:
                for lv in (*bids, *asks):
                    if not isinstance(lv, (list, tuple)) or len(lv) != 2:
                        raise ValueError("level is not a 2-element list")
                    p, s = Decimal(lv[0]), Decimal(lv[1])
                    if not (p.is_finite() and p > 0 and s.is_finite() and s > 0):
                        raise ValueError("level price/size must be finite and positive")
            except (ArithmeticError, TypeError, ValueError):
                raise SystemExit(f"{args.market}: malformed order book from /v1/l2OrderBook (level not a finite-positive [price, size] pair).")
            levels = sorted(asks, key=lambda lv: Decimal(lv[0])) if args.side == "BUY" \
                else sorted(bids, key=lambda lv: Decimal(lv[0]), reverse=True)
            budget = Decimal(args.quantityusd)
            raw_qty, _, _, budget_ok = walk_book_usd(levels, budget)
            qty = to_step(raw_qty, step_size)
            print(f"USD {budget} -> quantity {qty:f} {args.market} (rounded to step {step_size})")
            if not budget_ok:
                print(f"  WARNING: book can't absorb the full ${budget}; sized to available liquidity.")
        else:
            qty = Decimal(args.quantity)

        result = place_market_ioc(signer, address, account_index, mkt, args.side, qty,
                                  force=args.force, client_id=args.client_id)
        if not result["placed"]:                 # slippage guard blocked it (est > 3% and not --force)
            raise SystemExit(f"  {result['reason']} -- not placing. Re-run with --force to override.")
        print(f"MARKET {args.side} {qty:f} {args.market}: mid={result['mid']:.6f}  "
              f"est avg fill={result['avg_fill']:.6f}  slippage={result['slippage'] * 100:.2f}%  "
              f"worst level={result['worst']}  bound={result['bound']:f}" + ("  (--force)" if args.force else ""))
        if not result["enough"]:
            print(f"  WARNING: book only partly covered {qty:f}; the IOC partially filled.")
        print("Order response:", json.dumps(result["order"], indent=2))   # helper already check_order_response'd it
        return
    else:
        stop_price = None
        tpsl_type = None
        if is_trigger:
            # Conditional (stop-loss / take-profit) order -> POST /v1/placeOrder with tpslType + stopPrice,
            # signed with op=4 (OP_PLACE_UNTRIGGERED). stopPrice/tpslType are body-only (NOT signed); only the
            # signed `op` differs from a plain order. Fires on MARK price reaching stopPrice.
            tpsl_type = "STOP_LOSS" if args.tpsl == "sl" else "TAKE_PROFIT"
            stop_price = str(args.trigger)
            if tpsl_type == "TAKE_PROFIT" and not args.reduce_only:
                args.reduce_only = True        # venue requires TAKE_PROFIT to be reduce-only
                print("note: TAKE_PROFIT orders must be reduce-only; enabling --reduce-only.")
            forced_tif = "GTT" if args.price is not None else "IOC"   # venue: TPSL LIMIT=GTT, MARKET leg=IOC
            if args.tif is not None and args.tif != forced_tif:
                print(f"note: --tif {args.tif} ignored for a trigger {'LIMIT' if args.price is not None else 'MARKET'} "
                      f"order; using {forced_tif} (venue requirement).")
            tif = forced_tif
            if args.price is not None:
                order_type = "LIMIT"
                price = str(args.price)
            else:
                order_type = "MARKET"
                # A TPSL MARKET leg still carries a price (server-validated within 10% of stopPrice, not live mark).
                # Push it THROUGH the stop by TRIGGER_MARKET_SLIP so it fills like market; align to a tick multiple.
                st = Decimal(stop_price)
                raw = st * (Decimal(1) + TRIGGER_MARKET_SLIP) if args.side == "BUY" else st * (Decimal(1) - TRIGGER_MARKET_SLIP)
                rounding = ROUND_FLOOR if args.side == "BUY" else ROUND_CEILING
                price = f"{round_to_increment(raw, tick_size, rounding):f}"
        else:
            order_type = "LIMIT"
            price = str(args.price)
        if usd_mode:
            qty = to_step(Decimal(args.quantityusd) / Decimal(price), step_size)
            print(f"USD {args.quantityusd} @ {price} -> quantity {qty:f} (rounded to step {step_size})")
        else:
            qty = Decimal(args.quantity)
        if is_trigger:
            print(f"{args.market} (marketId {market_id})  {tpsl_type} {order_type} {args.side} {qty:f}  "
                  f"trigger={stop_price} price={price} tif={tif}"
                  + (" reduce-only" if args.reduce_only else "") + f"  tick={tick_size} step={step_size}")
        else:
            print(f"{args.market} (marketId {market_id})  LIMIT {args.side} {qty:f} @ {price}"
                  f"  tick={tick_size} step={step_size}")

    # Pre-validate the final size/notional against the market's bounds -> a clear error BEFORE signing (else the
    # venue rejects with OrderSizeTooLarge / a min-size / min-notional reason). Each bound is skipped when absent.
    if min_order_size is not None and min_order_size > 0 and qty < min_order_size:
        raise SystemExit(f"{args.market}: size {qty:f} is below the market minimum order size {min_order_size:f} "
                         f"(minOrderSize) -- increase --quantity/--quantityusd.")
    if max_order_size is not None and max_order_size > 0 and qty > max_order_size:
        raise SystemExit(f"{args.market}: size {qty:f} exceeds the market maximum order size {max_order_size:f} "
                         f"(maxOrderSize) -- reduce --quantity/--quantityusd.")
    _price_dec = dec(str(price))
    if (min_order_notional is not None and min_order_notional > 0 and _price_dec is not None
            and not (is_trigger and args.reduce_only)   # venue exempts reduce-only (incl. TPSL) from min-notional
            and qty * _price_dec < min_order_notional):
        raise SystemExit(f"{args.market}: order notional {qty * _price_dec:f} is below the market minimum "
                         f"{min_order_notional:f} (minOrderNotional) -- increase the size.")

    qty_str = f"{qty:f}"
    # Exact-multiple conversion (rejects mis-aligned price/qty with a clear error
    # rather than silently submitting something different from what was shown).
    price_ticks = to_ticks(price, tick_size)
    quantity_quantums = to_quantums(qty_str, step_size)

    # Every order needs a goodTilTime >= 1 month out, in SERVER time -- correct the
    # local clock by the /v1/time delta so the expiry (and the signed timestamp) are
    # server-aligned even if this box's clock drifts.
    delta_ns = clock_delta()
    server_us = (time.time_ns() + delta_ns) // 1000
    good_til_us = str(server_us + GOOD_TIL_DAYS * 86_400 * 1_000_000)

    # --- Sign the typed payload, then POST the REST body ----------------------
    ct = time.time_ns() + delta_ns           # server-aligned; also the X-Timestamp
    if is_trigger:
        # A TPSL / conditional order must be signed with op=4 (OP_PLACE_UNTRIGGERED). The signed canonical payload
        # is IDENTICAL to a plain op=1 placeOrder except the op value (stopPrice/tpslType are body-only, unsigned).
        # We produce it WITHOUT modifying the vendored ordersign.py: build the normal (op=1) payload with its public
        # builder, swap the single op field, then sign with the same Ed25519 key the Signer holds.
        payload = ordersign.place_order_payload(
            address=address, account_index=account_index, client_id=args.client_id,
            client_timestamp_ns=ct, good_til_time_ns_=ordersign.good_til_time_ns(good_til_us),
            market_id=market_id, price_ticks=price_ticks, quantity_quantums=quantity_quantums,
            reduce_only=args.reduce_only, side=SIDES[args.side], time_in_force=TIFS[tif],
        )
        marker = f'"op":{ordersign.OP_PLACE},'
        if marker not in payload:   # fail CLOSED: if the canonical format ever changes, don't send a bad signature
            raise SystemExit(f"{PROG}: cannot build a TPSL signature -- ordersign canonical payload format changed "
                             f"(expected {marker!r} in the op=1 payload). Not sending.")
        payload = payload.replace(marker, f'"op":{ordersign.OP_PLACE_UNTRIGGERED},', 1)
        sig = signer._priv.sign(payload.encode())   # same key/primitive ordersign.sign_place_order uses internally
        headers = {"X-API-Key": signer.pub_hex, "X-Timestamp": str(ct), "X-Signature": sig.hex()}
    else:
        headers = signer.sign_place_order(
            address=address, account_index=account_index, client_id=args.client_id,
            client_timestamp_ns=ct, good_til_time_ns_=ordersign.good_til_time_ns(good_til_us),
            market_id=market_id, price_ticks=price_ticks, quantity_quantums=quantity_quantums,
            reduce_only=args.reduce_only, side=SIDES[args.side], time_in_force=TIFS[tif],
        )
    body = {
        "address": address, "accountIndex": account_index, "marketId": market_id,
        "orderSide": args.side, "orderType": order_type, "quantity": qty_str,
        "price": price, "timeInForce": tif, "timestamp": ct,
        "goodTilTime": good_til_us,           # REQUIRED on every order now
    }
    if args.reduce_only:
        body["reduceOnly"] = True
    if is_trigger:
        body["tpslType"] = tpsl_type          # STOP_LOSS | TAKE_PROFIT (body-only; not part of the signed payload)
        body["stopPrice"] = stop_price        # trigger price in human USD (body-only)
    if args.client_id:
        body["clientId"] = args.client_id

    path = "/v1/placeOrder?" + urllib.parse.urlencode({"address": address})
    order = call("POST", path, body, headers)
    print("Order response:", json.dumps(order, indent=2))
    check_order_response(order, "placeOrder")   # a 2xx can still carry status REJECTED/ERROR -> fail closed


if __name__ == "__main__":
    main()
