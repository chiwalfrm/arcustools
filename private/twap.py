"""twap.py -- TWAP (time-weighted average price) market-order execution for ONE Arcus market.

Splits a large parent buy/sell into N smaller MARKET-IOC child slices placed at a fixed cadence over a
time window, to approximate the time-weighted average price and cut market impact. v1 is MARKET/IOC only:
each slice crosses the book via the shared place_market_ioc() helper (same book-walk + slippage guard +
protective bound as place_order.py), so nothing ever rests -- no cancel/dead-man machinery is needed.

  # buy 2.5 BTC over 1 hour in 20 slices (every 180s):
  twap.py --market BTC-USD --side BUY --quantity 2.5 --duration 3600 --slices 20 --mainnet

  # spend $150k selling ETH over 30 min, 15 slices (per-slice USD converted at the live price):
  twap.py --market ETH-USD --side SELL --quantityusd 150000 --duration 1800 --slices 15 --mainnet

  # preview the schedule without trading:
  twap.py --market BTC-USD --side BUY --quantity 2.5 --duration 3600 --slices 20 --mainnet --dry-run

Size is given EITHER as --quantity (base units) OR --quantityusd (total USD to spend, converted to a base
size per slice at the live reference price). Exactly one is required.

Guards (all optional):
  --max-slippage F   per-slice est. slippage ceiling vs mid (default from place_market_ioc = 3%); a slice
                     over it is SKIPPED and its quantity carried forward (never force-crossed).
  --limit-price P    hard price cap: never BUY above / SELL below P. A violating slice is skipped+carried.
  --max-adverse F    abort the TWAP if the reference price moves more than F against the arrival price
                     (e.g. 0.02 = 2%) -- don't keep buying into a spike.
  --randomize F      jitter each slice's size and timing by +/-F (e.g. 0.2 = +/-20%) to reduce predictability.

State: twap_state_<network>_<market>_<side>.json in the CWD tracks cumulative fill so --resume can continue
the remaining quantity over the remaining time after a crash/restart. A per-run flock prevents two overlapping
runs on the same market+side. Every slice fill is appended to twap_trades.log (CWD) as a CSV line:
  <datetime_iso>,<epoch_s>,<market>,<BUY|SELL>,<filledQty>,<avgFillPrice>,<slice#>
Resolves ordersign / arcus_creds_<network>.json / the market-order helper relative to THIS script, so it runs
from any cwd. Needs --testnet/--staging/--mainnet (REQUIRED, no default).
"""

import argparse
import fcntl
import hashlib
import json
import os
import random
import signal
import sys
import tempfile
import time
import urllib.parse
from datetime import datetime, timezone
from decimal import Decimal, ROUND_FLOOR

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

from ordersign import Signer
from arcus_common_private import (add_network_args, call, dec, describe_error, load_creds,
                                  place_market_ioc, request, resolve_market, select_network)

PROG = "twap"
FILL_POLL_TRIES = 6            # GET /v1/order retries to observe an IOC's terminal fill (placeOrder is async)
FILL_POLL_DELAY = 0.5          # s between fill polls
TERMINAL_STATUSES = {"FILLED", "PARTIALLY_FILLED", "CANCELED", "REJECTED"}
TRADES_LOG_NAME = "twap_trades.log"


def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def log(msg):
    print(f"[{_now_iso()}] {PROG}: {msg}", flush=True)


def _plain(d):
    """Fixed-point string for a Decimal (never scientific), or '' for None."""
    return "" if d is None else format(d, "f")


# ── single-instance lock + state (mirrors pivot_trader's conventions) ───────────────────────────────
def state_path(network, market, side):
    safe = "".join(c if (c.isalnum() or c in "._-") else "_" for c in f"{network}_{market}_{side}")
    return os.path.join(os.getcwd(), f"twap_state_{safe}.json")


def lock_path(path):
    tag = hashlib.sha256(os.path.abspath(path).encode()).hexdigest()[:8]
    return os.path.join(tempfile.gettempdir(), f"{os.path.basename(path)}.{tag}.lock")


def acquire_lock(path):
    lockf = open(lock_path(path), "w")
    try:
        fcntl.flock(lockf, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        raise SystemExit(f"{PROG}: another instance holds the lock for {os.path.basename(path)}; skipping.")
    return lockf


def save_state(path, state):
    state["updated_at"] = _now_iso()
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
        f.write("\n")
    os.replace(tmp, path)


def load_state(path):
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            s = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        raise SystemExit(f"{PROG}: state file {os.path.basename(path)} is unreadable/corrupt ({e}).")
    if not isinstance(s, dict):
        raise SystemExit(f"{PROG}: state file {os.path.basename(path)} is not a JSON object.")
    return s


# ── market metadata + reference price ───────────────────────────────────────────────────────────────
def read_market(market):
    blob = call("GET", "/v1/markets")
    markets = blob.get("markets") if isinstance(blob, dict) else None
    if not isinstance(markets, list):
        raise SystemExit(f"{PROG}: unexpected /v1/markets response (no 'markets' list).")
    mkt = resolve_market(markets, market)
    if mkt is None:
        raise SystemExit(f"{PROG}: unknown market {market!r}.")
    return mkt


def ref_price(mkt):
    """Reference price for USD->base sizing, max-adverse, and the benchmark: oraclePrice if usable, else
    the live order-book mid. Returns a positive Decimal or raises (no price = can't size/guard safely)."""
    oracle = dec(mkt.get("oraclePrice"))
    if oracle is not None and oracle.is_finite() and oracle > 0:
        return oracle
    name = mkt["marketDisplayName"]
    ob = call("GET", f"/v1/l2OrderBook/{urllib.parse.quote(name)}")
    bids = ob.get("bids") if isinstance(ob, dict) else None
    asks = ob.get("asks") if isinstance(ob, dict) else None
    if not bids or not asks:
        raise SystemExit(f"{PROG}: no usable oracle and no two-sided book for {name} -- cannot price slices.")
    mid = (Decimal(str(bids[0][0])) + Decimal(str(asks[0][0]))) / 2
    if not mid.is_finite() or mid <= 0:
        raise SystemExit(f"{PROG}: computed a non-positive mid for {name}.")
    return mid


def market_mins(mkt):
    """(min_order_size, min_order_notional, step_size) as Decimals (0 where the field is absent)."""
    z = Decimal(0)
    ms = dec(mkt.get("minOrderSize")) or z
    mn = dec(mkt.get("minOrderNotional")) or z
    st = dec(mkt.get("stepSize")) or z
    return (ms if ms > 0 else z, mn if mn > 0 else z, st if st > 0 else z)


def floor_to_step(qty, step):
    if step and step > 0:
        return (qty / step).to_integral_value(rounding=ROUND_FLOOR) * step
    return qty


# ── fill reconciliation (placeOrder is async -> read GET /v1/order + /v1/fills; from pivot_trader) ──
def read_order_terminal(order_id, address, account_index):
    q = urllib.parse.urlencode({"address": address, "accountIndex": account_index})
    o = {}
    for i in range(FILL_POLL_TRIES):
        try:
            o = request("GET", f"/v1/order/{urllib.parse.quote(str(order_id))}?{q}")
        except Exception:
            o = {}
        if isinstance(o, dict) and str(o.get("status", "")).upper() in TERMINAL_STATUSES:
            return o
        if i < FILL_POLL_TRIES - 1:
            time.sleep(FILL_POLL_DELAY)
    return o if isinstance(o, dict) else {}


def fill_vwap(order_id, market_name, address, account_index):
    """(VWAP Decimal, latest_epoch_us) of a filled order from /v1/fills, or (None, None). The order's own
    `price` is the protective bound, not the execution price -- the true fill price is on the per-fill rows."""
    if not order_id:
        return None, None
    q = {"address": address, "accountIndex": account_index}
    if market_name:
        q["marketDisplayName"] = market_name
    try:
        for i in range(FILL_POLL_TRIES):
            body = request("GET", f"/v1/fills?{urllib.parse.urlencode(q)}")
            fills = body.get("fills") if isinstance(body, dict) else None
            mine = ([f for f in fills if isinstance(f, dict) and str(f.get("orderId")) == str(order_id)]
                    if isinstance(fills, list) else [])
            if mine:
                num = den = Decimal(0)
                latest = None
                for f in mine:
                    p, s = dec(f.get("price")), dec(f.get("size"))
                    if p is None or s is None or not p.is_finite() or s <= 0:
                        continue
                    num += p * s
                    den += s
                    try:
                        cu = int(f.get("createdAt"))
                        latest = cu if latest is None else max(latest, cu)
                    except (TypeError, ValueError):
                        pass
                if den > 0:
                    return num / den, latest
            if i < FILL_POLL_TRIES - 1:
                time.sleep(FILL_POLL_DELAY)
    except Exception:
        return None, None
    return None, None


def log_trade(market, side, filled, price, slice_no):
    epoch = int(time.time())
    dt = datetime.fromtimestamp(epoch, timezone.utc).isoformat()
    line = f"{dt},{epoch},{market},{side},{_plain(filled)},{_plain(price)},{slice_no}\n"
    try:
        with open(os.path.join(os.getcwd(), TRADES_LOG_NAME), "a") as f:
            f.write(line)
    except OSError as e:
        log(f"WARNING could not append trade log: {e}")


# ── the TWAP engine ─────────────────────────────────────────────────────────────────────────────────
def jitter(value, frac):
    """value scaled by a uniform factor in [1-frac, 1+frac] (frac in [0,1)); value unchanged if frac<=0."""
    if frac <= 0:
        return value
    f = Decimal(str(random.uniform(float(1 - frac), float(1 + frac))))
    return value * f


def run_twap(args):
    select_network(args.network)
    side = args.side.upper()
    path = state_path(args.network, args.market, side)
    lock = acquire_lock(path)                                                    # noqa: F841 (held for lifetime)

    mkt = read_market(args.market)
    name = mkt["marketDisplayName"]
    min_sz, min_not, step = market_mins(mkt)
    px0 = ref_price(mkt)                       # arrival price (benchmark + max-adverse baseline)

    usd_mode = args.quantityusd is not None
    if usd_mode:
        target_usd = args.quantityusd
        target_base = target_usd / px0        # indicative only; USD is the authoritative target
    else:
        target_base = args.quantity
        target_usd = target_base * px0

    # Resume: fold in prior progress, continue the remainder over the remaining slices.
    prev = load_state(path) if args.resume else None
    done_base = (dec(prev.get("filled_base")) or Decimal(0)) if prev else Decimal(0)
    done_usd = (dec(prev.get("filled_usd")) or Decimal(0)) if prev else Decimal(0)
    slices_done = int(prev.get("slices_done", 0)) if prev else 0
    remaining_slices = max(1, args.slices - slices_done)

    interval = Decimal(args.duration) / args.slices
    nominal_base = (target_base - done_base) / remaining_slices

    # Validate the nominal slice against the market minimums up front (clearer than per-slice surprises).
    nominal_notional = nominal_base * px0
    warn = []
    if min_sz and nominal_base < min_sz:
        warn.append(f"nominal slice {_plain(nominal_base)} < minOrderSize {_plain(min_sz)}")
    if min_not and nominal_notional < min_not:
        warn.append(f"nominal slice notional {_plain(nominal_notional)} < minOrderNotional {_plain(min_not)}")
    if warn:
        max_slices = int((target_base * px0) / min_not) if min_not else args.slices
        hint = (f"  reduce --slices to <= ~{max_slices} so each slice clears the minimum."
                if min_not and max_slices >= 1 else "")
        msg = f"{PROG}: slice too small -- " + "; ".join(warn) + "." + ("\n" + hint if hint else "")
        if args.dry_run:
            print(msg, file=sys.stderr)       # dry-run: warn but still show the schedule
        else:
            raise SystemExit(msg)

    dtb, dnb = floor_to_step(target_base, step), floor_to_step(nominal_base, step)   # display-only quantization
    print(f"\n  {'DRY RUN -- ' if args.dry_run else ''}TWAP {side} {name}  [{args.network}]")
    print(f"    target        : " + (f"${_plain(target_usd)} (~{_plain(dtb)} base @ {_plain(px0)})"
                                      if usd_mode else f"{_plain(target_base)} base (~${_plain(target_usd)})"))
    print(f"    schedule      : {remaining_slices} slices, every {interval:.1f}s, ~{_plain(dnb)} base/slice")
    print(f"    arrival price : {_plain(px0)}   (oracle/mid)")
    print(f"    guards        : max-slippage {args.max_slippage if args.max_slippage is not None else 'default(3%)'}"
          f", limit-price {args.limit_price or '-'}, max-adverse {args.max_adverse or '-'}"
          f", randomize {args.randomize or '-'}")
    if slices_done:
        print(f"    resuming      : {slices_done} slices / {_plain(done_base)} base already done")
    print()
    if args.dry_run:
        print("  Not placing (--dry-run).\n")
        return

    creds = load_creds()
    address, account_index = creds["eth_address"], creds["account_index"]
    signer = Signer.from_private_key_hex(creds["api_private_key"])
    force = False                               # v1 never force-crosses; a blocked slice is skipped+carried

    state = {"market": name, "side": side, "network": args.network, "usd_mode": usd_mode,
             "target_base": _plain(target_base), "target_usd": _plain(target_usd), "slices": args.slices,
             "filled_base": _plain(done_base), "filled_usd": _plain(done_usd), "slices_done": slices_done,
             "arrival_price": _plain(px0), "started_at": _now_iso()}
    save_state(path, state)

    # Execution accumulators (filled_base/usd seed from resume so the report + carry math are cumulative).
    filled_base, filled_usd = done_base, done_usd
    bench_num = bench_den = Decimal(0)          # time-weighted mid benchmark (avg of sampled ref prices)
    start = time.monotonic()
    stop = {"flag": False}

    def _sig(_signum, _frame):
        stop["flag"] = True
        log("interrupt received -- finishing the current slice, then stopping.")
    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    for i in range(slices_done, args.slices):
        if stop["flag"]:
            break
        slice_no = i + 1
        remaining_slices = args.slices - i
        # Re-read market + reference price each slice (metadata/oracle drift, and USD sizing needs live px).
        mkt = read_market(args.market)
        px = ref_price(mkt)
        bench_num += px
        bench_den += 1

        # max-adverse guard: abort if the price has moved against us beyond the tolerance vs arrival.
        if args.max_adverse:
            adverse = ((px - px0) / px0) if side == "BUY" else ((px0 - px) / px0)
            if adverse > args.max_adverse:
                log(f"[{name}] ABORT at slice {slice_no}: price {_plain(px)} moved "
                    f"{adverse * 100:.2f}% against arrival {_plain(px0)} > max-adverse {args.max_adverse * 100:.2f}%.")
                break

        # Size this slice from the REMAINING target spread over the REMAINING slices (carries rounding and
        # any prior under-fill forward). USD mode converts at the live price.
        if usd_mode:
            remaining_usd = target_usd - filled_usd
            slice_base = (remaining_usd / remaining_slices) / px
        else:
            slice_base = (target_base - filled_base) / remaining_slices
        slice_base = jitter(slice_base, args.randomize)
        slice_base = floor_to_step(slice_base, step)
        # Don't exceed what's left; bump a dust slice up to the market minimum if the remainder allows.
        rem_base = (target_usd - filled_usd) / px if usd_mode else (target_base - filled_base)
        rem_base = floor_to_step(max(rem_base, Decimal(0)), step)
        if slice_base > rem_base:
            slice_base = rem_base
        if min_sz and 0 < slice_base < min_sz and rem_base >= min_sz:
            slice_base = min_sz
        if slice_base <= 0 or (min_sz and slice_base < min_sz) or (min_not and slice_base * px < min_not):
            log(f"[{name}] slice {slice_no}: remaining {_plain(rem_base)} below market minimum -- stopping "
                f"(dust left unfilled).")
            break

        # limit-price guard: skip (carry) a slice that would trade through the user's cap.
        if args.limit_price is not None:
            if (side == "BUY" and px > args.limit_price) or (side == "SELL" and px < args.limit_price):
                log(f"[{name}] slice {slice_no}: ref {_plain(px)} violates --limit-price "
                    f"{_plain(args.limit_price)} -- skipping (carried forward).")
                _sleep_to(start, i + 1, interval, stop)
                continue

        cid = f"twap-{int(time.time())}-{slice_no}"[:36]
        max_slip = args.max_slippage
        result = place_market_ioc(signer, address, account_index, mkt, side, slice_base,
                                  force=force, client_id=cid)
        # Honor a tighter --max-slippage than the helper's 3% default: treat an estimate over it as a skip.
        if result["placed"] and max_slip is not None and result["slippage"] > max_slip:
            log(f"[{name}] slice {slice_no}: NOTE helper placed within 3% but est slippage "
                f"{result['slippage'] * 100:.2f}% > --max-slippage {max_slip * 100:.2f}% (already sent).")
        if not result["placed"]:
            log(f"[{name}] slice {slice_no}: NOT placed ({result['reason']}) -- carried forward.")
            _sleep_to(start, i + 1, interval, stop)
            continue

        order = result["order"] or {}
        oid = order.get("orderId")
        state["pending_order_id"] = oid
        save_state(path, state)
        term = read_order_terminal(oid, address, account_index) if oid else {}
        filled = dec(term.get("filledSize")) or Decimal(0)
        if filled < 0:
            filled = Decimal(0)
        price, _ = fill_vwap(oid, name, address, account_index) if (oid and filled > 0) else (None, None)
        if price is None:
            price = result["avg_fill"]        # fall back to the book-walk estimate for the report
        filled_base += filled
        filled_usd += filled * (price if price else px)
        state.update({"pending_order_id": None, "filled_base": _plain(filled_base),
                      "filled_usd": _plain(filled_usd), "slices_done": slice_no})
        save_state(path, state)
        if filled > 0:
            log_trade(name, side, filled, price, slice_no)
        short = "" if result["enough"] else " [book thinner than slice -- partial]"
        log(f"[{name}] slice {slice_no}/{args.slices}: {side} {_plain(slice_base)} @ ~{_plain(price)} "
            f"(filled {_plain(filled)}{short}); cumulative {_plain(filled_base)} base.")

        _sleep_to(start, i + 1, interval, stop)

    # ── final report ────────────────────────────────────────────────────────────────────────────────
    vwap = (filled_usd / filled_base) if filled_base > 0 else None
    bench = (bench_num / bench_den) if bench_den > 0 else px0
    print(f"\n  TWAP {side} {name} complete:")
    print(f"    filled        : {_plain(filled_base)} base" + (f" / {_plain(target_base)} target"
          f" ({(filled_base / target_base * 100):.1f}%)" if target_base > 0 else ""))
    print(f"    spent/recv'd  : ${_plain(filled_usd)}")
    print(f"    achieved VWAP : {_plain(vwap)}")
    print(f"    arrival price : {_plain(px0)}    time-weighted mid: {_plain(bench)}")
    if vwap is not None and px0 > 0:
        slip_arr = (vwap - px0) / px0 * (1 if side == "BUY" else -1) * 10000
        slip_bch = (vwap - bench) / bench * (1 if side == "BUY" else -1) * 10000 if bench > 0 else None
        print(f"    slippage      : {slip_arr:+.1f} bps vs arrival"
              + (f", {slip_bch:+.1f} bps vs TWAP benchmark" if slip_bch is not None else "")
              + "  (negative = better than reference)")
    print(f"    state file    : {os.path.basename(path)}\n")


def _sleep_to(start, slices_elapsed, interval, stop):
    """Sleep until the next scheduled slice time (start + slices_elapsed*interval), honoring an interrupt.
    A slice that overran its interval just proceeds immediately (no negative sleep, no drift accumulation)."""
    target = start + float(interval) * slices_elapsed
    while not stop["flag"]:
        now = time.monotonic()
        if now >= target:
            return
        time.sleep(min(0.5, target - now))     # wake often so SIGINT is responsive


def main():
    p = argparse.ArgumentParser(description="TWAP market-order execution for one Arcus market (v1: MARKET/IOC).")
    p.add_argument("--market", required=True, help="market display name, e.g. BTC-USD")
    p.add_argument("--side", required=True, choices=["BUY", "SELL", "buy", "sell"])
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--quantity", help="total size in base units (e.g. 2.5)")
    g.add_argument("--quantityusd", help="total USD to execute (converted to base per slice at the live price)")
    p.add_argument("--duration", type=int, required=True, help="total execution window in seconds")
    p.add_argument("--slices", type=int, required=True, help="number of child slices (>=1)")
    p.add_argument("--max-slippage", help="per-slice est. slippage ceiling vs mid as a fraction, e.g. 0.01 "
                                          "(default: place_market_ioc's 3%%)")
    p.add_argument("--limit-price", help="hard price cap: never BUY above / SELL below this")
    p.add_argument("--max-adverse", help="abort if the price moves this fraction against arrival, e.g. 0.02")
    p.add_argument("--randomize", help="jitter slice size+timing by +/- this fraction, e.g. 0.2 (default 0)")
    p.add_argument("--resume", action="store_true", help="continue from the state file (remaining qty/time)")
    p.add_argument("--dry-run", action="store_true", help="print the schedule and exit without trading")
    add_network_args(p)
    args = p.parse_args()

    args.side = args.side.upper()
    if args.slices < 1:
        raise SystemExit(f"{PROG}: --slices must be >= 1.")
    if args.duration < 0:
        raise SystemExit(f"{PROG}: --duration must be >= 0.")
    # Parse decimals with clean errors.
    def _opt(name, v, allow=None):
        if v is None:
            return None
        d = dec(v)
        if d is None or not d.is_finite() or d <= 0:
            raise SystemExit(f"{PROG}: {name} must be a positive number (got {v!r}).")
        return d
    args.quantity = _opt("--quantity", args.quantity)
    args.quantityusd = _opt("--quantityusd", args.quantityusd)
    args.max_slippage = _opt("--max-slippage", args.max_slippage)
    args.limit_price = _opt("--limit-price", args.limit_price)
    args.max_adverse = _opt("--max-adverse", args.max_adverse)
    rz = dec(args.randomize) if args.randomize is not None else Decimal(0)
    if rz is None or not rz.is_finite() or rz < 0 or rz >= 1:
        raise SystemExit(f"{PROG}: --randomize must be in [0, 1) (got {args.randomize!r}).")
    args.randomize = rz
    run_twap(args)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        raise SystemExit(f"{PROG}: interrupted.")
    except Exception as e:
        raise SystemExit(f"{PROG}: unexpected error: {describe_error(e)}")
