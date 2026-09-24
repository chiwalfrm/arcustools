#!/usr/bin/env python3
"""Flatten arcus position(s) with reduce-only IOC MARKET orders -- a get-out button.

  close_position.py --testnet                         # close ALL open positions
  close_position.py --market BTC-USD --staging         # close just that market
  close_position.py --mainnet --max-slippage 0.03

Reads open positions (GET /v1/positions) and, for each, fires a REDUCE-ONLY IOC MARKET order on the
OPPOSITE side with a protective price bound = mark price +/- --max-slippage (default 0.05 = 5%).
Reduce-only guarantees it can only shrink/flatten, never flip (the venue rejects a reduce-only that
would grow a position: REDUCE_ONLY_WOULD_INCREASE). A thin book may leave a remainder (IOC) -- after
submitting, it RE-QUERIES positions and EXITS NONZERO if any close failed OR a position remains open
(including sub-step dust that can't be traded). Re-run to finish.

--testnet/--staging/--mainnet REQUIRED. Signs via ordersign; creds in
arcus_creds_<network>.json beside this script. (--max-slippage must stay < the venue's 10%-of-mark
market-order cap.)
"""
import argparse
import json
import os
import sys
import time
import urllib.parse
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))   # resolve ordersign/arcus_common_private beside this file
import ordersign
from ordersign import Signer
from arcus_common_private import (
    add_network_args, call, check_order_response, clamp_to_mark_cap, clock_delta, dec, describe_error, load_creds, positive_decimal, resolve_market, round_to_increment, select_network, to_quantums, to_ticks)

PROG = "close_position"
GOOD_TIL_DAYS = 365         # venue requires goodTilTime >= 1 month even on IOC; 365d clears it
SETTLE_SECONDS = 2          # let IOC fills settle before the residual re-query
SIDES = {"BUY": ordersign.SIDE_BUY, "SELL": ordersign.SIDE_SELL}


def fetch_positions(address):
    """GET /v1/positions -> {marketIdStr: posdict}. FAIL CLOSED (SystemExit) on an unreadable body: a non-dict
    response, or a MISSING/null/non-dict `positions` field, means exposure is UNKNOWN -- a PANIC-close must NOT
    read that as flat (exit 0), nor `.get` on a non-dict body (traceback). The venue returns an explicit `{}`
    for genuinely no positions, so ONLY a dict (incl empty {}) is trusted."""
    data = call("GET", "/v1/positions?" + urllib.parse.urlencode({"address": address}))
    positions = data.get("positions") if isinstance(data, dict) else None
    if not isinstance(positions, dict):
        raise SystemExit(f"{PROG}: positions unreadable -- /v1/positions did not return an object with a "
                         f"'positions' object (exposure UNKNOWN; NOT treating as flat). Retry.")
    return positions


def open_positions(address, target_mid):
    """Return (open, unknown). `open` = parseable NON-ZERO positions [(marketIdStr, posdict)]. `unknown` =
    descriptions of IN-SCOPE positions whose exposure CANNOT be determined -- a non-dict value, or a
    missing/non-finite/unparseable `size` (dec() rejects "NaN"/"Infinity"/junk). This is a PANIC-CLOSE
    (get-out button): an UNKNOWN position is NOT flat, so the caller must report it and exit nonzero rather
    than claim success on unreadable state -- treating malformed position state as flat would falsely tell
    the operator they have no exposure. Only a genuinely ZERO size is flat (correctly skipped).

    Scope FIRST on the dict KEY (the authoritative marketId, per fetch_positions' contract and how
    out.append/downstream key off `mid`), NOT the body's marketId field: a body that omitted marketId would
    make str(None) != target_mid silently skip a real position, AND scoping first means an out-of-scope
    malformed position can't block a --market close of a DIFFERENT market."""
    out, unknown = [], []
    for mid, p in fetch_positions(address).items():
        if target_mid is not None and str(mid) != target_mid:   # out of scope -> ignore
            continue
        if not isinstance(p, dict):
            unknown.append(f"marketId {mid} (non-dict position value: {type(p).__name__})")
            continue
        raw = p.get("size")
        size = dec(raw)
        if size is None:                       # missing / NaN / Inf / non-numeric -> exposure UNKNOWN, NOT flat
            unknown.append(f"marketId {mid} (unparseable size {raw!r})")
            continue
        if size == 0:                          # genuinely flat -> not open (correctly skipped)
            continue
        out.append((mid, p))
    return out, unknown


def main():
    p = argparse.ArgumentParser(description="Flatten arcus position(s) with reduce-only IOC market orders.")
    p.add_argument("--market", help="only close this market (display name or marketId); default: ALL open positions")
    p.add_argument("--max-slippage", default="0.05",
                   help="market-order price bound as a fraction off the mark (default 0.05 = 5%%; must be < 0.10)")
    add_network_args(p)
    args = p.parse_args()
    select_network(args.network)
    slip = positive_decimal(args.max_slippage, "--max-slippage")
    if slip >= Decimal("0.10"):
        raise SystemExit(f"{PROG}: --max-slippage must be < 0.10 (the venue's 10%-of-mark market-order cap).")

    creds = load_creds()
    address = creds["eth_address"]
    account_index = creds["account_index"]
    signer = Signer.from_private_key_hex(creds["api_private_key"])

    # Fetch market metadata up front: it supplies mark/tick/step for the plan AND lets us VALIDATE
    # --market -- a typo (e.g. BTX-USD) must FAIL, not silently match no positions and "succeed".
    resp = call("GET", "/v1/markets")
    if not isinstance(resp, dict) or not isinstance(resp.get("markets"), list):
        raise SystemExit(f"{PROG}: unexpected /v1/markets response (expected an object with a 'markets' list).")
    markets = resp["markets"]
    by_id = {str(m["marketId"]): m for m in markets if isinstance(m, dict) and m.get("marketId") is not None}
    target_mid, scope = None, ""
    if args.market:
        mkt = resolve_market(markets, args.market)
        if mkt is None:
            raise SystemExit(f"{PROG}: unknown market {args.market!r} (not found in /v1/markets).")
        if mkt.get("marketId") is None:
            raise SystemExit(f"{PROG}: market {args.market!r} has malformed metadata (no marketId).")
        target_mid = str(mkt["marketId"])
        scope = f" market {mkt.get('marketDisplayName', args.market)}"

    positions, unknown = open_positions(address, target_mid)
    if not positions and not unknown:          # truly nothing in scope (all sizes genuinely 0) -> flat, exit 0
        print(f"\n  No open positions for {address}{scope} [{args.network}]\n")
        return

    # Build the close plan. Sub-step dust (|size| < stepSize) CANNOT be traded -> record it (never a
    # silent skip + exit 0); the final re-query below reports it as still-open and exits nonzero. Seed
    # `skipped` with UNKNOWN-exposure positions (malformed/unparseable size): NOT flat, NOT closeable, so
    # they must flow into the "not flat -> exit 1" accounting rather than vanish into a false success.
    plan, dust, skipped = [], [], list(unknown)
    for mid, pos in positions:
        m = by_id.get(str(mid))
        if m is None:
            # One market's metadata gap must NOT abort flattening the OTHERS -- a panic-close of N positions
            # shouldn't leave all N open because one is odd. Skip+record; the residual re-query below finds
            # it still open -> NOT flat -> exit 1.
            skipped.append(f"marketId {mid} (not in /v1/markets)")
            print(f"  skip marketId {mid}: not found in /v1/markets -- NOT closeable", flush=True)
            continue
        try:                                    # a malformed /v1/markets entry (missing id/name/tick/step, or a
            disp = str(m["marketDisplayName"])  # non-numeric id) can't be closed -> skip+record (don't abort the
            mkt_id_int = int(m["marketId"])     # whole flatten; the residual re-query below catches it -> exit 1)
            tick, step = m["tickSize"], m["stepSize"]
            dt, ds = dec(tick), dec(step)
            if dt is None or ds is None or dt <= 0 or ds <= 0:   # require FINITE and > 0: a 0 step -> round_to_increment
                raise ValueError                                 # DivisionByZero (would abort the WHOLE flatten); a negative
                                                                 # tick/step -> negative signed quantums (ordersign rejects only 0)
        except (KeyError, ValueError, TypeError):
            skipped.append(f"marketId {mid} (incomplete/invalid market metadata)")
            print(f"  skip marketId {mid}: incomplete/invalid market metadata (tickSize/stepSize must be > 0) "
                  f"-- NOT closeable", flush=True)
            continue
        size = dec(pos.get("size"))
        # A MARKET order's protective bound is validated against markPrice (within 10% of mark, per docs).
        # markPrice "0" = no mark received yet; the docs are explicit that callers must NOT substitute
        # oraclePrice -- mark is an independent EWMA feed (verified live: mark != oracle on most markets),
        # so a bound built off oracle would be checked against the absent mark -> reject/inconsistent. Fail
        # clearly instead (operator can retry once a mark is available, or close via a limit order).
        mark = dec(m.get("markPrice"))
        if mark is None or mark <= 0:
            # No mark yet ('0' = none received) -> can't bound a MARKET close for THIS market. Skip+record
            # instead of aborting the whole flatten; the residual re-query catches it -> exit 1. (Re-run
            # when a mark is available, or close it via a limit order. Per docs, must NOT substitute oracle.)
            skipped.append(f"{m.get('marketDisplayName')} (no markPrice)")
            print(f"  skip {m.get('marketDisplayName')}: no markPrice ('0' = none received yet) -- can't "
                  f"bound a reduce-only MARKET close; NOT closeable", flush=True)
            continue
        close_side = "SELL" if size > 0 else "BUY"           # reduce a long by selling, a short by buying
        qty = round_to_increment(abs(size), step, ROUND_FLOOR)
        if qty <= 0:
            dust.append(f"{m.get('marketDisplayName')}={pos.get('size')}")
            print(f"  skip {m.get('marketDisplayName')}: |size| {abs(size)} below stepSize {step} "
                  f"(can't trade sub-step) -- NOT closeable", flush=True)
            continue
        # Protective bound = mark +/- slippage, rounded AWAY from mid so tick-rounding can't tighten it
        # (BUY-to-close UP, SELL-to-close DOWN) -- same direction as place_order's market bound.
        target = mark * (1 + slip) if close_side == "BUY" else mark * (1 - slip)
        bound = round_to_increment(target, tick, ROUND_CEILING if close_side == "BUY" else ROUND_FLOOR)
        # Keep the bound within the venue's 10%-of-mark cap: tick-rounding AWAY (above) can push an aggressive
        # --max-slippage / coarse tick past 10% -> the venue REJECTS the whole close. Clamp inward so it lands.
        bound = clamp_to_mark_cap(bound, mark, tick, close_side == "BUY")
        # A SELL-to-close floor can tick-round DOWN to 0 when the price is within ~1 tick of tickSize
        # (target < tick), leaving the reduce-only MARKET close with NO protective floor. Skip+record
        # (don't abort the whole flatten) -- the residual re-query below then reports it -> exit 1.
        if bound <= 0:
            skipped.append(f"{disp} (protective bound rounded to 0)")
            print(f"  skip {disp}: protective bound rounded to {bound} (price within ~1 tick of tickSize "
                  f"{tick}) -- a MARKET close would carry no protective floor; NOT closeable this way", flush=True)
            continue
        plan.append({"market": disp, "market_id": mkt_id_int,
                     "side": close_side, "qty": qty, "bound": bound, "mark": mark, "tick": tick, "step": step})

    if not plan:
        # Positions exist but none are closeable (sub-step dust, and/or markets skipped above) -> NOT flat.
        notclose = dust + skipped
        print(f"\n  {PROG}: nothing closeable for {address}{scope} [{args.network}]; "
              f"{len(notclose)} position(s) NOT flat: {', '.join(notclose)}\n")
        raise SystemExit(1)

    print(f"\n  {PROG}: flatten {len(plan)} position(s) for {address} [{args.network}]{scope}")
    for q in plan:
        print(f"    {q['market']:<16} close {q['side']} {q['qty']:f}  reduce-only IOC market "
              f"(mark {q['mark']:f}, bound {q['bound']:f})")
    if dust:
        print(f"    ({len(dust)} sub-step position(s) NOT closeable: {', '.join(dust)})")
    if skipped:
        print(f"    ({len(skipped)} position(s) SKIPPED, NOT closeable: {', '.join(skipped)})")
    print()

    ok = fail = 0
    delta_ns = clock_delta()                                 # server-clock offset: fetch ONCE, not per order
    for q in plan:
        try:
            price, qty_str = f"{q['bound']:f}", f"{q['qty']:f}"
            ct = time.time_ns() + delta_ns                   # server-aligned; also the X-Timestamp
            good_til_us = str((ct // 1000) + GOOD_TIL_DAYS * 86_400 * 1_000_000)
            headers = signer.sign_place_order(
                address=address, account_index=account_index, client_id=None,
                client_timestamp_ns=ct, good_til_time_ns_=ordersign.good_til_time_ns(good_til_us),
                market_id=q["market_id"], price_ticks=to_ticks(price, q["tick"]),
                quantity_quantums=to_quantums(qty_str, q["step"]),
                reduce_only=True, side=SIDES[q["side"]], time_in_force=ordersign.TIF_IOC)
            body = {"address": address, "accountIndex": account_index, "marketId": q["market_id"],
                    "orderSide": q["side"], "orderType": "MARKET", "quantity": qty_str,
                    "price": price, "timeInForce": "IOC", "timestamp": ct,
                    "goodTilTime": good_til_us, "reduceOnly": True}
            resp = call("POST", "/v1/placeOrder?" + urllib.parse.urlencode({"address": address}), body, headers)
            check_order_response(resp, f"close {q['market']}")   # 2xx body: raises on a non-dict body OR a REJECTED/
                                                                 # ERROR status -> caught below as a FAIL (so resp is a
                                                                 # dict here; no separate isinstance check needed)
            ok += 1
            print(f"  closing {q['market']} -> {q['side']} {qty_str} reduce-only  (orderId {resp.get('orderId', '')})")
        except (Exception, SystemExit) as e:                  # SystemExit = check_order_response reject; never fatal here
            fail += 1
            print(f"  FAILED {q['market']}: {describe_error(e)}")
    print(f"\n  submitted {ok} close order(s), {fail} failed.")

    # Confirm FLAT by re-querying ALL in-scope positions (catches partial fills AND the sub-step dust
    # above) -- the account is flat only if NOTHING remains open in scope.
    time.sleep(SETTLE_SECONDS)
    try:
        res_open, res_unknown = open_positions(address, target_mid)
        residual = [f"{p.get('marketDisplayName')}={p.get('size')}" for _, p in res_open] + res_unknown
    except (OSError, json.JSONDecodeError, SystemExit) as e:   # call() wraps transport/JSON errors as SystemExit
        print(f"  WARNING could not re-query positions to confirm flat ({describe_error(e)}); "
              f"treating as INCOMPLETE.", file=sys.stderr)
        raise SystemExit(1)

    if residual:
        print("  NOT flat -- still open: " + ", ".join(residual)
              + "  (re-run to finish; sub-step dust can't be closed).")
    print(f"  Verify: display_positions.py {address} --{args.network}\n")
    # Panic/automation semantics: a failed submit OR any remaining position is NOT success.
    if fail or residual:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
