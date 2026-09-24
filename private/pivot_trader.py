"""
pivot_trader.py -- pivot-based stop-and-reverse bot for ONE Arcus market, run from cron.

Premise: a fixed pivot price P. Oracle ABOVE P => bullish (hold +q); BELOW P => bearish (hold -q).
The bot trades only when the oracle crosses P, always holding its OWN +q or -q on top of whatever else
is in the account (it never reads the account position -- manual/human trades are ignored entirely).

  # arm it once (sets P and q, records the starting side, stays flat, waits for a flip):
  pivot_trader.py --init --market BTC-USD --pivot 64000 --quantity 0.5 --mainnet
  #   optional directional bias (locked at --init, mutually exclusive): --long-only | --short-only

  # cron runs it (no P/q needed -- read from the state file):
  pivot_trader.py --market BTC-USD --mainnet

  # MANUAL repair ONLY (never cron): fold an unconfirmed pending fill into `position` and clear it --
  # NO oracle read, NO trade, NO bias change. Fixes an out-of-sync position without an off-cycle sample:
  pivot_trader.py --market BTC-USD --mainnet --reconcile-only

Directional mode (locked at --init, stored in state; default when neither flag is given):
  both        -> +q bullish / -q bearish   -- the default stop-and-reverse (flips into the opposite quantity).
  --long-only -> +q bullish / 0 bearish    -- only ever long; a bearish flip FLATTENS to 0, then waits to re-buy.
  --short-only-> -q bearish / 0 bullish     -- only ever short; a bullish flip FLATTENS to 0, then waits to re-sell.

Per-run logic (normal mode):
  1. flock (one instance at a time).  2. Load state (error if none -- run --init first).
  3. Read the oracle (Redis 'markets' cache if present, else REST /v1/markets; must be finite > 0, else SKIP).
  4. side = bullish if oracle >= P else bearish.
  5. If position == 0 and side == starting_side -> WAIT (still on the entry side; first trade fires on a flip).
     Else target = target_position(side, q, direction); delta = target - position; trade the delta at market (IOC).
  This one `delta = target - position` gives the 1x first trade, the 2x flip (both mode) or the flatten (long/
  short-only), and self-correcting top-ups (a rare short fill is picked up next run) -- no partial fill can
  compound because `position` only moves by the ACTUAL fill (read from GET /v1/order/{orderId}, async placeOrder).

State file: pivot_state_<network>_<market>.json in the CWD (locked convention, not overridable). One bot per
directory. `position` is the bot's OWN net (sum of its own fills); deleting the file = "roll to the next pivot"
(the profitable position rides on as untracked baseline). Resolves ordersign / arcus_creds_<network>.json /
the market-order helper relative to THIS script, so it runs from any cwd.

Trade history: every fill is appended to pivot_trader_trades.log in the CWD (ONE shared file for all markets
run from that directory), as a plain CSV line
  <datetime_iso>,<epoch_s>,<market>,<BUY|SELL>,<filledQuantity>,<avgFillPrice>
The state JSON only tracks the current stance/position; this log is the durable per-fill record. The fill
QUANTITY is the order's definitive filledSize; the fill PRICE is the volume-weighted average of the order's
fills read from GET /v1/fills (the order object's own `price` is the protective bound, not the execution
price) -- left empty if not yet retrievable. Late-reconciled fills log their ACTUAL fill time.
"""

import argparse
import fcntl
import hashlib
import json
import os
import sys
import tempfile
import time
import urllib.parse
from datetime import datetime, timezone
from decimal import Decimal

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

from ordersign import Signer
import arcus_redis as account_cache
from arcus_common_private import (add_network_args, call, dec, describe_error, load_creds,
                                  place_market_ioc, request, resolve_market, select_network)

PROG = "pivot_trader"
MARKETS_CACHE_TTL = 5          # s; cache-aside TTL for the shared /v1/markets blob (oracle + metadata)
FILL_POLL_TRIES = 6            # GET /v1/order retries to observe an IOC's terminal fill (placeOrder is async)
FILL_POLL_DELAY = 0.5         # s between fill polls
# An IOC never rests, so it reaches one of these the instant the engine processes it; the poll only rides out
# the brief REST propagation delay. filledSize is the source of truth regardless of which terminal status it is.
TERMINAL_STATUSES = {"FILLED", "PARTIALLY_FILLED", "CANCELED", "REJECTED"}

# Trade-history log: ONE shared file in the CWD (same convention as the state file -- one bot per directory),
# appended, holding every market's fills. One plain CSV line per fill (no header):
#   <datetime_iso>,<epoch_s>,<market>,<BUY|SELL>,<filledQuantity>,<avgFillPrice>
TRADES_LOG_NAME = "pivot_trader_trades.log"


def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def log(msg):
    print(f"[{_now_iso()}] {PROG}: {msg}", flush=True)


# ── State file (the bot's entire memory) ─────────────────────────────────────────
def state_path(network, market):
    """pivot_state_<network>_<market>.json in the CWD. Convention is FIXED (not overridable). A market name
    is sanitized defensively (arcus names like BTC-USD are already filename-safe)."""
    safe = "".join(c if (c.isalnum() or c in "._-") else "_" for c in f"{network}_{market}")
    return os.path.join(os.getcwd(), f"pivot_state_{safe}.json")


def load_state(path):
    """Return the parsed state dict, or None if the file doesn't exist. A corrupt/unreadable file RAISES
    (SystemExit) rather than being silently treated as a fresh start -- that could abandon a live +/-q."""
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            state = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        raise SystemExit(f"{PROG}: state file {os.path.basename(path)} is unreadable/corrupt ({e}). "
                         f"Fix or remove it (rm = roll to a new pivot) -- NOT auto-recovering to avoid "
                         f"abandoning a live position.")
    if not isinstance(state, dict):
        raise SystemExit(f"{PROG}: state file {os.path.basename(path)} is not a JSON object.")
    return state


def save_state(path, state):
    """Atomic write (temp + os.replace) so a crash mid-write can never corrupt the baseline/bias."""
    state["updated_at"] = _now_iso()
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
        f.write("\n")
    os.replace(tmp, path)


def lock_path(path):
    """Lock file lives in the temp dir (tempfile.gettempdir(), honours TMPDIR, else /tmp) so it never clutters
    the working dir. The name keeps the state file's basename for legibility AND appends a short hash of the
    state file's ABSOLUTE path -- that preserves the current per-state-file (per-directory) lock scope, so two
    bots on the same market run from different dirs get distinct locks instead of colliding on a shared name."""
    abspath = os.path.abspath(path)
    tag = hashlib.sha256(abspath.encode()).hexdigest()[:8]
    return os.path.join(tempfile.gettempdir(), f"{os.path.basename(path)}.{tag}.lock")


def acquire_lock(path):
    """Single-instance lock (flock). A slow order must not let two cron runs overlap and double-trade.
    Returns the open file handle -- the CALLER must keep it referenced for the process lifetime."""
    lp = lock_path(path)
    lockf = open(lp, "w")
    try:
        fcntl.flock(lockf, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        raise SystemExit(f"{PROG}: another instance holds {os.path.basename(lp)}; skipping this run.")
    return lockf


# ── Oracle + market metadata (one cached /v1/markets read gives both) ────────────
def read_market_and_oracle(network, market):
    """Resolve `market` and read its oracle price from the shared 'markets' cache (Redis if present, else REST
    /v1/markets -- cache-aside, redis-optional). Returns (mkt_dict, oracle_Decimal_or_None). Mirrors
    market_maker.oracle_price(): the field is `oraclePrice`, and only a FINITE POSITIVE value is usable (arcus
    emits "0" when there is no oracle -- the caller treats None/<=0 as 'no usable oracle' and skips the run)."""
    blob = account_cache.cached_get(network, None, "markets",
                                    lambda: call("GET", "/v1/markets"), MARKETS_CACHE_TTL)
    markets = blob.get("markets") if isinstance(blob, dict) else None
    if not isinstance(markets, list):
        raise SystemExit(f"{PROG}: unexpected /v1/markets response (no 'markets' list).")
    mkt = resolve_market(markets, market)
    if mkt is None:
        raise SystemExit(f"{PROG}: unknown market {market!r}.")
    oracle = dec(mkt.get("oraclePrice"))
    if oracle is not None and (not oracle.is_finite() or oracle <= 0):
        oracle = None                                # 0 / non-finite = no usable oracle
    return mkt, oracle


def side_of(oracle, pivot):
    """Bullish at or above the pivot, bearish below. (Exact equality is a formality with a real oracle.)"""
    return "bullish" if oracle >= pivot else "bearish"


def target_position(side, q, direction):
    """The bot's desired SIGNED position for the current side, given the directional mode locked at --init.
      both  -> +q bullish / -q bearish   (default stop-and-reverse: flips into the opposite quantity).
      long  -> +q bullish / 0 bearish    (long-only: a bearish flip just FLATTENS to 0, then waits to re-buy).
      short -> -q bearish / 0 bullish     (short-only: a bullish flip just FLATTENS to 0, then waits to re-sell).
    delta = target - position then yields the flatten order for free (e.g. long +q, flip bearish -> target 0 ->
    delta -q -> SELL q)."""
    if direction == "long":
        return q if side == "bullish" else Decimal(0)
    if direction == "short":
        return -q if side == "bearish" else Decimal(0)
    return q if side == "bullish" else -q


def min_trade_size(mkt):
    """Smallest size the bot will place: the market's minOrderSize, or the step size if that's absent."""
    ms = dec(mkt.get("minOrderSize"))
    if ms is not None and ms > 0:
        return ms
    step = dec(mkt.get("stepSize"))
    return step if (step is not None and step > 0) else Decimal(0)


# ── Reading the bot's OWN fill (placeOrder is async -> read GET /v1/order/{orderId}) ──
def read_order_terminal(order_id, address, account_index):
    """Poll GET /v1/order/{orderId} until the order is terminal; return the order dict, or {} if it never
    became readable/terminal.

    A JUST-placed async order routinely 404s ('Order not found') for a beat before the Indexer can serve it on
    /v1/order (an order placed a cycle ago reads fine) -- and a transient network/5xx can fail any read. NEITHER
    is fatal here: we use request() (raises on HTTP>=400) and SWALLOW the error as 'not readable this attempt',
    keep polling, and if it never resolves return {} so the CALLER leaves the order PENDING to reconcile next
    run -- rather than call(), whose SystemExit on the first 404 would CRASH the whole run after the order was
    already placed (leaving the position out of sync until a human notices). Never applies an unconfirmed fill."""
    q = urllib.parse.urlencode({"address": address, "accountIndex": account_index})
    o = {}
    for i in range(FILL_POLL_TRIES):
        try:
            o = request("GET", f"/v1/order/{urllib.parse.quote(str(order_id))}?{q}")
        except Exception as e:                    # 404 (order not visible yet) / timeout / 5xx -> not terminal this try
            o = {}
            if i == FILL_POLL_TRIES - 1:
                log(f"order {order_id} still unreadable after {FILL_POLL_TRIES} tries ({describe_error(e)}); "
                    f"leaving it to reconcile next run")
        if isinstance(o, dict) and str(o.get("status", "")).upper() in TERMINAL_STATUSES:
            return o
        if i < FILL_POLL_TRIES - 1:
            time.sleep(FILL_POLL_DELAY)
    return o if isinstance(o, dict) else {}


def apply_fill(order, state):
    """Add an order's DEFINITIVE fill to the bot's own `position` (signed by the order's side). Returns
    (signed_fill, side, filled, status)."""
    side = str(order.get("side", "")).upper()
    filled = dec(order.get("filledSize")) or Decimal(0)
    if filled < 0:
        filled = Decimal(0)
    signed = filled if side == "BUY" else -filled
    state["position"] = str(dec(state["position"]) + signed)
    return signed, side, filled, str(order.get("status"))


# ── Trade-history logging (fill price + time come from /v1/fills, not the order) ──
def _plain(d):
    """Fixed-point string for a Decimal (never scientific notation), or '' for None."""
    return "" if d is None else format(d, "f")


def fill_price_and_time(order_id, market_name, address, account_index):
    """Look up the ACTUAL average execution price (VWAP) and time of a filled order from /v1/fills.

    The order object's own `price` is the submitted protective bound, NOT the execution price -- the true
    fill price only exists on the per-fill records. Reads GET /v1/fills (newest-first; `marketDisplayName` is
    a server-side filter), keeps the fills whose orderId matches, and returns
    (Decimal VWAP = Σ(price·size)/Σ size, epoch_us of the last matching fill) or (None, None) if none are
    found yet. Best-effort with the same short poll as the order read (fills can lag the order by a beat);
    ANY failure returns (None, None) so trade-history logging never blocks or breaks the bot."""
    if not order_id:
        return None, None
    q = {"address": address, "accountIndex": account_index}
    if market_name:
        q["marketDisplayName"] = market_name         # server-side narrow to this market's fills
    try:
        for i in range(FILL_POLL_TRIES):
            body = request("GET", f"/v1/fills?{urllib.parse.urlencode(q)}")   # request() raises normal exceptions (caught below), not call()'s SystemExit
            fills = body.get("fills") if isinstance(body, dict) else None
            mine = ([f for f in fills if isinstance(f, dict) and str(f.get("orderId")) == str(order_id)]
                    if isinstance(fills, list) else [])
            if mine:
                num = den = Decimal(0)
                latest_us = None
                for f in mine:
                    p, s = dec(f.get("price")), dec(f.get("size"))
                    if p is None or s is None or not p.is_finite() or s <= 0:
                        continue
                    num += p * s
                    den += s
                    try:
                        cu = int(f.get("createdAt"))
                        latest_us = cu if latest_us is None else max(latest_us, cu)
                    except (TypeError, ValueError):
                        pass
                if den > 0:
                    return num / den, latest_us
            if i < FILL_POLL_TRIES - 1:
                time.sleep(FILL_POLL_DELAY)
    except Exception:
        return None, None
    return None, None


def log_trade(market, side, filled, order_id, address, account_index):
    """Append one fill to the shared trade-history log (pivot_trader_trades.log in the CWD):
      <datetime_iso>,<epoch_s>,<market>,<BUY|SELL>,<filledQuantity>,<avgFillPrice>
    `filled` (Decimal > 0) is the authoritative fill size from the order; the price + time are looked up from
    /v1/fills (avgFillPrice left EMPTY if not yet retrievable -- the trade still happened, never drop it). The
    timestamp is the ACTUAL fill time when known (so a reconciled prior-run fill logs its real time, not now),
    else now. Best-effort: a lookup/I-O error is warned to stdout but never raises."""
    price, epoch_us = fill_price_and_time(order_id, market, address, account_index)
    epoch = int(epoch_us) // 1_000_000 if epoch_us else int(time.time())
    dt = datetime.fromtimestamp(epoch, timezone.utc).isoformat()
    line = f"{dt},{epoch},{market},{side},{_plain(filled)},{_plain(price)}\n"
    path = os.path.join(os.getcwd(), TRADES_LOG_NAME)
    try:
        with open(path, "a") as f:
            f.write(line)
    except OSError as e:
        log(f"WARNING could not append trade log {path}: {e}")


def settle_pending(address, account_index, state, path, market_name, tag=""):
    """If a `pending_order_id` is recorded (a prior run placed an order but crashed before accounting for it),
    read its definitive fill and fold it into `position`, then clear it. If the order still isn't terminal,
    LEAVE it pending (retried next run) rather than applying an unconfirmed fill -- never double-counts."""
    oid = state.get("pending_order_id")
    if not oid:
        return
    order = read_order_terminal(oid, address, account_index)
    if str(order.get("status", "")).upper() not in TERMINAL_STATUSES:
        log(f"{tag}pending order {oid} not yet terminal (status={order.get('status')}); leaving it to reconcile next run")
        return
    signed, side, filled, status = apply_fill(order, state)
    state["pending_order_id"] = None
    save_state(path, state)
    if filled > 0:                                       # a real fill reconciled late -> record it in the trade history
        log_trade(market_name, side, filled, oid, address, account_index)
    log(f"{tag}reconciled pending order {oid}: {side} filled {filled} (status {status}); position -> {state['position']}")


def execute_trade(signer, address, account_index, mkt, trade_side, size, state, path):
    """Place a MARKET IOC for `size` (Decimal > 0) on `trade_side`, then read the DEFINITIVE fill and fold it
    into `position`. Records `pending_order_id` BEFORE reading the fill so a crash mid-read is reconciled next
    run (not double-traded). A slippage-guard block just skips this run (position unchanged; retried next)."""
    market_name = mkt.get("marketDisplayName", "?")
    result = place_market_ioc(signer, address, account_index, mkt, trade_side, size)
    if not result["placed"]:
        log(f"[{market_name}] NOT PLACED ({result['reason']}; mid={result['mid']}, est fill={result['avg_fill']}) "
            f"-- position unchanged, will retry next run")
        return
    order = result["order"]
    order_id = order.get("orderId")
    if not order_id:
        raise SystemExit(f"{PROG}: placeOrder response carries no orderId -- cannot confirm the fill: {order}")
    state["pending_order_id"] = order_id                 # persist BEFORE reading the fill (crash-safe)
    save_state(path, state)
    order_status = read_order_terminal(order_id, address, account_index)
    if str(order_status.get("status", "")).upper() not in TERMINAL_STATUSES:
        log(f"[{market_name}] order {order_id} not terminal after polling (status={order_status.get('status')}) "
            f"-- leaving it PENDING to reconcile next run (position NOT advanced)")
        return
    signed, side, filled, status = apply_fill(order_status, state)
    state["pending_order_id"] = None
    save_state(path, state)
    if filled > 0:                                       # record the actual fill (qty + looked-up VWAP price) in the trade history
        log_trade(market_name, side, filled, order_id, address, account_index)
    short = "" if result["enough"] else " [book thinner than size -- partial]"
    log(f"[{market_name}] {trade_side} {size} @ bound {result['bound']} (est slippage {result['slippage'] * 100:.3f}%): "
        f"orderId {order_id} status {status} filled {filled}{short}; position -> {state['position']}")


# ── --init: arm / re-arm the state file ──────────────────────────────────────────
def do_init(args):
    select_network(args.network)
    path = state_path(args.network, args.market)
    lock = acquire_lock(path)                                                     # noqa: F841 (held for lifetime)
    existing = load_state(path)
    if existing is not None:
        pos = dec(existing.get("position", "0")) or Decimal(0)
        if pos != 0:
            raise SystemExit(f"{PROG}: {os.path.basename(path)} exists with position {existing.get('position')} "
                             f"(the bot is IN A TRADE) -- refusing to overwrite. To roll to a new pivot, bank the "
                             f"position and `rm {os.path.basename(path)}` first, then re-run --init.")
    pivot = dec(args.pivot)
    if pivot is None or not pivot.is_finite() or pivot <= 0:
        raise SystemExit(f"{PROG}: --pivot must be a positive number (got {args.pivot!r}).")
    q = dec(args.quantity)
    if q is None or not q.is_finite() or q <= 0:
        raise SystemExit(f"{PROG}: --quantity must be a positive number (got {args.quantity!r}).")

    mkt, oracle = read_market_and_oracle(args.network, args.market)
    market_name = mkt.get("marketDisplayName", args.market)
    if oracle is None:
        raise SystemExit(f"{PROG}: [{market_name}] oracle is unusable right now (oraclePrice="
                         f"{mkt.get('oraclePrice')!r}); can't record the starting side. Retry when the oracle is live.")
    step = dec(mkt.get("stepSize"))
    if step is not None and step > 0 and (q % step) != 0:
        clamped = (q // step) * step             # largest multiple of stepSize <= q -> round DOWN to a valid quantity
        hint = f"  Use: {_plain(clamped)}" if clamped > 0 else ""
        raise SystemExit(f"{PROG}: --quantity {_plain(q)} is not a multiple of the market step size "
                         f"{_plain(step)}.{hint}")
    mn = min_trade_size(mkt)
    if mn > 0 and q < mn:
        raise SystemExit(f"{PROG}: --quantity {_plain(q)} is below the market minimum order size {_plain(mn)}.")

    direction = "long" if args.long_only else "short" if args.short_only else "both"
    starting_side = side_of(oracle, pivot)
    now = _now_iso()
    state = {"pivot": str(pivot), "quantity": str(q), "starting_side": starting_side, "direction": direction,
             "position": "0", "pending_order_id": None, "created_at": now, "updated_at": now}
    save_state(path, state)
    mode_note = {"both": "flip +q<->-q", "long": "LONG-ONLY (flatten on bearish)",
                 "short": "SHORT-ONLY (flatten on bullish)"}[direction]
    log(f"[INIT {market_name}] pivot={pivot} q={q} [{mode_note}]  oracle={oracle} -> starting side {starting_side}; "
        f"position 0. Idle until the oracle crosses the pivot. State: {os.path.basename(path)}")


# ── normal mode: one cron tick ───────────────────────────────────────────────────
def do_run(args):
    select_network(args.network)
    path = state_path(args.network, args.market)
    lock = acquire_lock(path)                                                     # noqa: F841 (held for lifetime)
    state = load_state(path)
    if state is None:
        raise SystemExit(f"{PROG}: no state file {os.path.basename(path)} -- run with --init first to arm the bot.")
    for k in ("pivot", "quantity", "starting_side", "position"):
        if k not in state:
            raise SystemExit(f"{PROG}: state file {os.path.basename(path)} is missing '{k}'.")
    pivot = dec(state["pivot"]); q = dec(state["quantity"])
    starting_side = state["starting_side"]
    direction = state.get("direction", "both")   # older state files predate this field -> default flip strategy
    if direction not in ("both", "long", "short"):
        raise SystemExit(f"{PROG}: state file has an invalid direction {direction!r} (expected both/long/short).")
    if pivot is None or q is None or dec(state["position"]) is None:
        raise SystemExit(f"{PROG}: state file has a non-numeric pivot/quantity/position.")

    creds = load_creds()
    address, account_index = creds["eth_address"], creds["account_index"]
    signer = Signer.from_private_key_hex(creds["api_private_key"])

    mkt, oracle = read_market_and_oracle(args.network, args.market)
    market_name = mkt.get("marketDisplayName", args.market)
    if oracle is None:
        log(f"[{market_name}] no usable oracle (oraclePrice={mkt.get('oraclePrice')!r}); skipping this run")
        return
    side = side_of(oracle, pivot)

    # Fold in any unaccounted prior order first, then act on the current side.
    settle_pending(address, account_index, state, path, market_name, tag=f"[{market_name}] ")
    if state.get("pending_order_id"):            # a prior order we placed still isn't confirmed (unreadable/not
        # terminal): its fill may or may not have landed, so `position` is UNTRUSTWORTHY. Do NOT trade on it --
        # a further delta on a stale position could double the intended exposure. Skip; reconcile it next run.
        log(f"[{market_name}] pending order {state['pending_order_id']} not yet reconciled -- position may be "
            f"stale; NOT trading this run (will reconcile next run)")
        return
    position = dec(state["position"])

    if position == 0 and side == starting_side:
        log(f"[{market_name}] oracle={oracle} pivot={pivot} -> {side} (== starting side); waiting for a flip, no trade")
        return
    target = target_position(side, q, direction)
    delta = target - position
    ms = min_trade_size(mkt)
    if delta == 0 or abs(delta) < ms:
        log(f"[{market_name}] oracle={oracle} -> {side}; position {position} at/near target {target} "
            f"({direction}); delta {delta} < min {ms}; no trade")
        return
    execute_trade(signer, address, account_index, mkt, "BUY" if delta > 0 else "SELL", abs(delta), state, path)


# ── --reconcile-only: manual repair (fold in a pending fill; no oracle, no trade, no bias change) ──
def do_reconcile(args):
    """Fold any unconfirmed `pending_order_id`'s ACTUAL fill into `position` and clear it, then STOP.

    Runs ONLY the account-reconciliation half of a normal tick: it NEVER reads the oracle, NEVER trades, and
    NEVER touches the bias (pivot / quantity / starting_side / direction, all set only at --init). For MANUAL
    use to re-sync an out-of-sync position after a placed order couldn't be confirmed inline (e.g. a fresh-order
    404 left the fill pending) -- WITHOUT risking an off-cycle oracle sample flipping the position. Safe no-op
    when nothing is pending; if the pending order still isn't readable it's left pending (retry later)."""
    select_network(args.network)
    path = state_path(args.network, args.market)
    lock = acquire_lock(path)                                                     # noqa: F841 (held for lifetime)
    state = load_state(path)
    if state is None:
        raise SystemExit(f"{PROG}: no state file {os.path.basename(path)} -- nothing to reconcile (run --init first).")
    if "position" not in state or dec(state["position"]) is None:
        raise SystemExit(f"{PROG}: state file {os.path.basename(path)} has a missing/non-numeric 'position'.")

    pending = state.get("pending_order_id")
    if not pending:
        log(f"[{args.market}] reconcile-only: no pending order; position {state['position']} unchanged (nothing to do)")
        return

    creds = load_creds()                                     # address+accountIndex to READ the order/fills (no signer -- we never place)
    address, account_index = creds["eth_address"], creds["account_index"]
    before = state["position"]
    settle_pending(address, account_index, state, path, args.market, tag=f"[{args.market}] reconcile-only: ")
    if state.get("pending_order_id"):
        log(f"[{args.market}] reconcile-only: pending order {pending} still not confirmed; position "
            f"{state['position']} unchanged -- retry later (NO oracle read, NO trade)")
    else:
        log(f"[{args.market}] reconcile-only: done; position {before} -> {state['position']} (NO oracle read, NO trade)")


def main():
    p = argparse.ArgumentParser(description="Pivot stop-and-reverse bot for one Arcus market (cron-driven).")
    p.add_argument("--market", required=True, help="market display name, e.g. BTC-USD")
    p.add_argument("--init", action="store_true",
                   help="create/re-arm the state file: locks --pivot and --quantity and records the starting "
                        "side. Refuses if the state file exists and the bot is in a position (rm it to roll).")
    p.add_argument("--reconcile-only", action="store_true",
                   help="MANUAL repair (NEVER use in cron): fold any unconfirmed pending order's ACTUAL fill "
                        "into the tracked position and clear it, then STOP -- NO oracle read, NO trade, NO bias "
                        "change. For re-syncing after a placed order couldn't be confirmed inline (e.g. a "
                        "fresh-order 404). No-op if nothing is pending.")
    p.add_argument("--pivot", help="pivot price (required with --init; ignored otherwise)")
    p.add_argument("--quantity", help="position size q in base-asset units (required with --init; ignored otherwise)")
    bias = p.add_mutually_exclusive_group()
    bias.add_argument("--long-only", action="store_true",
                      help="directional bias, locked at --init: only ever hold +q. A bearish flip FLATTENS to 0 "
                           "(never goes short), then waits to re-buy on the next bullish crossing.")
    bias.add_argument("--short-only", action="store_true",
                      help="directional bias, locked at --init: only ever hold -q. A bullish flip FLATTENS to 0 "
                           "(never goes long), then waits to re-sell on the next bearish crossing.")
    add_network_args(p)
    args = p.parse_args()

    if args.init and args.reconcile_only:
        raise SystemExit(f"{PROG}: --init and --reconcile-only are mutually exclusive.")

    if args.init:
        if not args.pivot or not args.quantity:
            raise SystemExit(f"{PROG}: --init requires --pivot and --quantity.")
        do_init(args)
    elif args.reconcile_only:
        if args.pivot or args.quantity or args.long_only or args.short_only:
            print(f"{PROG}: note -- --pivot/--quantity/--long-only/--short-only are ignored in --reconcile-only "
                  f"mode (it only folds in a pending fill; the strategy/bias are untouched).", file=sys.stderr)
        do_reconcile(args)
    else:
        if args.pivot or args.quantity or args.long_only or args.short_only:
            print(f"{PROG}: note -- --pivot/--quantity/--long-only/--short-only are ignored in normal mode "
                  f"(the strategy is read from the state file locked at --init).", file=sys.stderr)
        do_run(args)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:                       # a cron bot must exit CLEANLY (nonzero) with a readable line,
        raise SystemExit(f"{PROG}: unexpected error: {describe_error(e)}")   # not a raw traceback, so logs stay legible
