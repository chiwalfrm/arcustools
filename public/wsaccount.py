#!/usr/bin/env python3
"""Subscribe to an account's WebSocket channels and log each to a rotating file.

  wsaccount.py 0xADDRESS
  wsaccount.py 0xADDRESS --log-dir /tmp/ws --timestamp

One rotating log file per channel. Unrecognized channels / unparseable frames
are printed to stdout (capture stdout to keep them). Reconnects with backoff.
"""
import argparse
import asyncio
import json
import os
import sys
import time

import websockets
from arcus_common_public import SUB_ERR_LIMIT, CACHE_TTL, REDIS_URL, dec, describe_error, emit as _emit, log_ts, make_publisher, now_iso, plan_reconnect_sleep, positive_int, require_eth_address, SubscriptionError, setup_logger, write_pidfile, ws_url   # shared public helpers (formerly local copies)


# ── Constants ────────────────────────────────────────────────────────────────

# Which subscribed channels warm which account-cache key (the rest are logged only). The `account`
# channel warms BOTH account (freeCollateral) AND positions -- its periodic full snapshot carries the
# whole positions map (verified live), so no separate positions subscription / delta tracking is needed.
CACHE_CHANNELS = {"account", "orders"}
# Force a fresh `orders` snapshot this often so the delta-maintained openOrders set can't silently drift
# (the one stateful cache; account/positions/markets self-correct via periodic full snapshots). The
# resubscribe REPLACES the set only when the new snapshot arrives -- the old set stays published in the
# meantime -- so there's no window where openOrders looks empty. 0 disables.
ORDERS_RESUBSCRIBE_S = 60
# Streaming order `state` (engine-authoritative per the docs) -> membership of the resting set.
ORDER_STATE_RESTING  = {"OPEN", "PARTIALLY_FILLED"}
ORDER_STATE_TERMINAL = {"FILLED", "CANCELED", "REJECTED"}
LOG_BASE        = "/mnt/arcuslogs"   # logs go under LOG_BASE/<network> (subdir auto-created)
LOG_MAX_BYTES   = 2097152
LOG_BACKUP      = 4
RECONNECT_BASE  = 1          # seconds; doubles per failure
INFINITE_RETRY  = False      # --infinite-retry: never give up after SUB_ERR_LIMIT subscription rejections (retry forever)
RECONNECT_MAX   = 60         # exponential-backoff cap for the DEFAULT path ONLY (unused under --reconnect-interval).
                            # Was briefly 120; reverted to 60 to match dydxv4 -- MAX-tuning doesn't fix the mass-drop
                            # trigger, --reconnect-interval does; a large single-IP fleet should set it. (see wsorderbook.py)
STABLE_AFTER    = 30         # s a connection must STAY UP before backoff resets (else an accept-then-close flap busy-loops)
OPEN_TIMEOUT    = 10
PING_INTERVAL   = 20
PING_TIMEOUT    = 20

CHANNELS = [
    "accountAttributeUpdates",   # incl. type=leverage/feeTier/leverageReject entries (leverageReject is NOT a separate channel)
    "account",
    "accountTransferUpdates",     # v1.3.3: deposits/withdrawals/transfers/referral-claims audit trail (log-only, not cache-warmed)
    "funding",
    "orders",
    "positions",
    "userFills",
]


# ── Logger setup ─────────────────────────────────────────────────────────────
# ── Optional Redis cache publisher (self-healing publisher shared from arcus_common_public) ──────
# Warms the market_maker fleet's account-cache keys straight from this WS feed -- the push-driven,
# rate-limit-free replacement for account_poller.py's REST warming. Mirrors wsorderbook.py's bbo
# publisher ; the CachePublisher itself is SHARED from arcus_common_public (make_publisher imported). redis.asyncio is OPTIONAL
# (imported above; absent => publishing is off and the tool logs to files exactly as before). A
# publish failure NEVER propagates into the WS loop; bounded socket/connect timeouts make a wedged
# connection RAISE (feeding the self-heal) rather than hang; after PUB_RECREATE_AFTER consecutive
# failures the client is rebuilt outright.
HEARTBEAT = 5          # s; republish the last-known blob at least this often while the socket is up


def acct_key(network, address, name):
    """Account-scoped key -- MUST equal arcus_redis._acct_key(network, address, name), the reader the bots use.
    The address is LOWERCASED (as arcus_redis._acct_key does) so the key is casing-independent: the bots read
    with their checksummed creds address and still land on the SAME key. Keep the two in lockstep."""
    return f"arcus:{network}:acct:{address.lower()}:{name}"


# ── channel → account-cache transforms / reconciler ───────────────────────────
def account_cache_blobs(contents):
    """From an `account` channel FULL SNAPSHOT (contents.isSnapshot true), build the two REST-shaped
    blobs the bots read: ({"freeCollateral": ...}, {"positions": <map>}). Returns (None, None) for a
    non-snapshot frame (the account channel interleaves order-lifecycle EVENTS -- type PLACED/etc.,
    no freeCollateral -- which must NOT be written into the account/positions caches)."""
    if not (isinstance(contents, dict) and contents.get("isSnapshot") and "freeCollateral" in contents):
        return None, None
    # freeCollateral must be a FINITE number before warming the account key -- mirror arcus_redis is_cacheable's
    # 'account' rule (_finite_number). The top guard only checks the KEY is present; a non-finite/null/non-numeric
    # VALUE (NaN/Inf/null/"abc") would otherwise be written VERBATIM (this warmer bypasses is_cacheable). The
    # reader's is_cacheable re-check largely rejects a bad cached value, but this warmer is the SOLE gate on the
    # write path, so don't fabricate a bad account blob. (dec() accepts a finite 0/negative -- a real underwater
    # account -- so a legitimate low freeCollateral is still warmed.)
    fc = contents.get("freeCollateral")
    account_blob = {"freeCollateral": fc} if dec(fc) is not None else None
    # positions is a map keyed by marketId, each value a position object with a string `size`.
    # SECURITY: a missing/malformed positions field must NOT be fabricated into {} -- an EMPTY map is read
    # as FLAT by the reader (market_maker.position() returns 0 for an absent market), so a partial snapshot
    # could make a bot believe it's flat on a REAL position and bypass --max-position. Only warm a REAL
    # positions dict whose every entry has a finite `size` (mirrors arcus_redis is_cacheable's 'positions'
    # check -- keep the two in lockstep); otherwise return None so the caller SKIPS the positions write
    # (the cache keeps its last-good value / falls through to a live REST fetch -- never a fabricated flat).
    positions = contents.get("positions")
    if isinstance(positions, dict) and all(
            isinstance(p, dict) and "size" in p and dec(p.get("size")) is not None
            for p in positions.values()):
        positions_blob = {"positions": positions}
    else:
        positions_blob = None
    return account_blob, positions_blob


def seed_orders(contents):
    """Seed the resting-order set from an `orders` SUBSCRIBED snapshot: {orderId: order}. Every entry in
    contents.openOrders is resting by definition. SECURITY: returns None for a MALFORMED snapshot -- contents
    not a dict, openOrders MISSING / not a list, OR ANY element that isn't a dict with a truthy `orderId`. A
    malformed element must NOT be silently DROPPED: dropping it (like a whole missing/non-list openOrders)
    yields an EMPTY/INCOMPLETE set that the caller would publish, and the reader treats a set MISSING a real
    resting order as 'no such quote' -> the market maker places DUPLICATE GTT orders instead of modify/cancel.
    On None the caller KEEPS the last-good set (periodic resubscribe self-heals). An EXPLICIT empty list is valid
    (genuinely no open orders) and returns {}. An order WITHOUT a clientId is SKIPPED (not one of the MM's, which
    are keyed by clientId) -- consistent with the delta path -- WITHOUT rejecting the whole snapshot (clientId-less
    frontend orders are legit, unlike a missing orderId). recentClosedOrders is ignored."""
    if not isinstance(contents, dict):
        return None
    oo = contents.get("openOrders")
    if not isinstance(oo, list):
        return None
    out = {}
    for o in oo:
        if not (isinstance(o, dict) and o.get("orderId")):
            return None                       # malformed element (non-dict / missing orderId) -> unusable snapshot
        if not o.get("clientId"):
            # No clientId -> this order can't be one of the READER's own (the MM identifies its quotes BY clientId,
            # mm-<market>-b/-a), so SKIP it rather than seed it -- CONSISTENT with the delta path (apply_order_update
            # skips a clientId-less upsert). Seeding it is data the reader ignores anyway, and excluding it can't
            # overwrite a clientId'd entry. A legit account CAN hold clientId-less (e.g. frontend) orders, so this
            # must NOT reject the whole snapshot -- unlike a missing orderId, which IS malformed.
            continue
        out[o["orderId"]] = o
    return out


def apply_order_update(orders, contents):
    """Apply one `orders` channel_data delta to the resting set, keyed by orderId. Membership is driven
    off the engine-authoritative `state` field (docs: clients can drive the state machine off it alone):
    OPEN/PARTIALLY_FILLED -> upsert; FILLED/CANCELED/REJECTED -> remove -- EXCEPT a CANCELED whose
    cancelReason is MODIFY_CANCELED, the transient cancel-leg of a modify, which is KEPT (the same orderId
    is re-placed on the next update). An update with an unrecognized/missing state leaves the set UNCHANGED
    (conservative -- the periodic resubscribe self-heals any drift).
    Returns True if the set changed (so the caller only republishes on a real change)."""
    if not isinstance(contents, dict):
        return False
    oid = contents.get("orderId")
    if not oid:
        return False
    state = contents.get("state")
    if state in ORDER_STATE_RESTING:
        if not contents.get("clientId"):     # our-order identity is by clientId; upserting a resting order WITHOUT
            return False                      # one would read as not-ours -> DUPLICATE. Skip -> keep the last-good
        orders[oid] = contents               # (clientId-bearing) version of this order; the resubscribe self-heals.
        return True
    if state in ORDER_STATE_TERMINAL:
        # MODIFY_CANCELED = the FIRST leg of a modify: the SAME orderId is re-placed by the very next update,
        # so it is NOT terminally gone (docs). Removing here would flicker the order OUT of the shared cache on
        # every modify -- the MM (which reads this cache) would then see its order briefly missing and pay a
        # fresh re-read via its stale-cache safeguard. Keep the existing cached order; the imminent re-place
        # upserts its fresh state, and a dropped follow-up self-heals via the 60s resubscribe. Only a GENUINE
        # terminal cancel (no marker) / FILLED / REJECTED removes it.
        if contents.get("cancelReason") == "MODIFY_CANCELED":
            return False
        return orders.pop(oid, None) is not None
    return False


# ── Message handling ──────────────────────────────────────────────────────────
async def handle_message(raw, loggers, add_ts, pub, ctx, state):
    """Route a frame to its channel log; unknown/malformed frames go to STDOUT (the operator captures
    stdout, so they aren't lost). When Redis publishing is on, ALSO warm the account-cache keys from the
    relevant channels -- file logging is unchanged, this only adds the Redis writes. `ctx` is
    (network, address); `state` is the per-connection cache state (resting-order set + heartbeat blobs)."""
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"[parse error] {e} | {raw}")
        return
    if not isinstance(msg, dict):          # valid JSON but not an object (bare array/number) -> STDOUT, keep reading
        print(raw)
        return
    # Server subscription/request error (channel-less frame with an `error` object or status >= 400): raise so
    # ws_loop reconnects instead of sitting on a dead subscription. Channel-less gate => real frames never match.
    if msg.get("channel") is None and (isinstance(msg.get("error"), dict)
                                       or (isinstance(msg.get("status"), int) and msg["status"] >= 400)):
        err = msg.get("error") if isinstance(msg.get("error"), dict) else {}
        raise SubscriptionError(err.get("message") or f"status {msg.get('status')}: {err or msg}")

    channel = msg.get("channel")
    logger = loggers.get(channel)
    if logger is None:                        # not a subscribed channel -> stdout
        print(raw)
        return
    line = json.dumps({"receivedAt": now_iso(), "msg": msg}, separators=(",", ":")) \
        if add_ts else raw
    _emit(logger, line)

    if pub is None or channel not in CACHE_CHANNELS:
        return
    network, address = ctx
    contents = msg.get("contents")
    if channel == "account":
        account_blob, positions_blob = account_cache_blobs(contents)
        # account + positions are INDEPENDENT cache keys (mirror is_cacheable's per-key checks): warm each only
        # when its OWN blob validated. positions_blob is non-None ONLY for a full snapshot, so un-nesting is safe
        # -- and it means a snapshot whose freeCollateral is non-finite (account_blob None) still warms VALID
        # positions instead of starving them.
        if account_blob is not None:          # freeCollateral present AND finite (full snapshot)
            await _warm(pub, state, acct_key(network, address, "account"), "account", account_blob)
        if positions_blob is not None:        # skip a missing/malformed positions map -- never warm a fabricated flat
            await _warm(pub, state, acct_key(network, address, "positions"), "positions", positions_blob)
    elif channel == "orders":
        typ = msg.get("type")
        changed = False
        if typ == "subscribed":
            seeded = seed_orders(contents)
            if seeded is None:                        # malformed snapshot (bad shape OR any bad element) -> unusable:
                print(f"[{log_ts()}] [wsaccount] orders snapshot malformed (openOrders missing/non-list, or an element "   # KEEP
                      "with no orderId); keeping last-good resting set", file=sys.stderr)                      # last-good
            else:                                                                                             # (dup-quote
                state["orders"] = seeded              # atomic replace (explicit list -- empty or populated)  # risk);
                state["orders_seeded"] = True         # a VALID snapshot grounds the set -- publish from here  # resub heals
                changed = True
        elif typ == "channel_data":
            changed = apply_order_update(state["orders"], contents)
        # Publish ONLY a set grounded in a valid snapshot: a delta applied before the first good snapshot this
        # connection would publish an INCOMPLETE set (same fail-open as a fabricated-empty one -> duplicate quotes).
        if changed and state.get("orders_seeded"):
            blob = {"orders": list(state["orders"].values())}
            await _warm(pub, state, acct_key(network, address, "openOrders"), "openOrders", blob)


async def _warm(pub, state, key, name, blob):
    """Publish a warmed key and record it for the heartbeat republish."""
    state["blobs"][name] = (key, blob)
    state["last_pub"][name] = time.monotonic()
    await pub.publish(key, blob)


# ── WebSocket loop ───────────────────────────────────────────────────────────
async def ws_loop(url, subscriptions, loggers, add_ts, pub, ctx, reconnect_interval=None):
    delay = RECONNECT_BASE
    sub_errs = 0                               # consecutive subscription rejections; reset once a connection is stable
    while True:
        conn_start = None
        # Per-connection cache state: resting-order set + last-good blobs for the heartbeat republish.
        # RESET each connection so a reconnect rebuilds openOrders from the fresh snapshot (no stale carryover).
        state = {"orders": {}, "orders_seeded": False, "blobs": {}, "last_pub": {}}
        try:
            async with websockets.connect(
                url, open_timeout=OPEN_TIMEOUT,
                ping_interval=PING_INTERVAL, ping_timeout=PING_TIMEOUT,
            ) as ws:
                conn_start = time.monotonic()
                for sub in subscriptions:
                    await ws.send(json.dumps(sub))
                if pub is not None:
                    _, address = ctx
                    orders_subbed = any(s.get("channel") == "orders" for s in subscriptions)
                    next_resub = (time.monotonic() + ORDERS_RESUBSCRIBE_S
                                  if orders_subbed and ORDERS_RESUBSCRIBE_S > 0 else None)
                    # Redis present: on-change writes happen in handle_message; here we add (a) the HEARTBEAT
                    # republish so short-TTL keys never expire between snapshots, and (b) the periodic forced
                    # `orders` re-snapshot so the delta-maintained openOrders set can't silently drift.
                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=HEARTBEAT)
                            await handle_message(raw, loggers, add_ts, pub, ctx, state)
                        except asyncio.TimeoutError:
                            pass
                        now = time.monotonic()
                        for name, (key, blob) in list(state["blobs"].items()):
                            if now - state["last_pub"].get(name, 0.0) >= HEARTBEAT:
                                state["last_pub"][name] = now
                                await pub.publish(key, blob)
                        if next_resub is not None and now >= next_resub:
                            next_resub = now + ORDERS_RESUBSCRIBE_S
                            # Re-request the snapshot; handle_message atomically REPLACES state["orders"] when
                            # the new `subscribed` frame lands, so the previously-published set stays valid in
                            # the gap (never an empty openOrders). A send failure just retries next interval.
                            try:
                                await ws.send(json.dumps({"type": "unsubscribe", "channel": "orders", "id": address}))
                                await ws.send(json.dumps({"type": "subscribe", "channel": "orders", "id": address, "snapshot": True}))
                            except Exception:
                                pass
                else:
                    # No redis: the original loop, untouched.
                    async for raw in ws:
                        await handle_message(raw, loggers, add_ts, None, ctx, state)
        except SubscriptionError as e:
            # Reconnectable, not fatal: resubscribe on a fresh socket with the normal backoff. Give up only
            # after SUB_ERR_LIMIT rejections in a row with no stable connection between (a genuinely bad sub).
            sub_errs += 1
            if sub_errs >= SUB_ERR_LIMIT and not INFINITE_RETRY:
                raise SystemExit(f"wsaccount: subscription rejected {sub_errs}x in a row, giving up: {e}")
            print(f"[{log_ts()}] [sub error] {e} — resubscribing (attempt {sub_errs}, with backoff)", file=sys.stderr)
        except websockets.ConnectionClosedOK:
            print(f"[{log_ts()}] [ws] connection closed — reconnecting", file=sys.stderr)   # CLEAN close (redis path: ws.recv() raises this) -> NOT an error
        except Exception as e:
            print(f"[{log_ts()}] [ws error] {describe_error(e)} — reconnecting", file=sys.stderr)   # actual delay set after the stability reset below
        # Sleep/backoff ALWAYS (outside the except) so a CLEAN server close (async-for ended, no exception)
        # also backs off -- previously that path skipped the except entirely and busy-looped at 0 delay.
        # Reset backoff ONLY if the connection proved STABLE (>= STABLE_AFTER s); resetting on connect
        # defeated backoff on an accept-then-close flap.
        # Default: exponential backoff (full jitter, doubling to RECONNECT_MAX, reset on a stable drop).
        # With --reconnect-interval: immediate on a genuine drop, else a flat ~interval wait -- so a
        # synchronized mass-disconnect of a large fleet stays under the per-IP new-conns/min cap (see
        # plan_reconnect_sleep).
        if conn_start is not None and time.monotonic() - conn_start >= STABLE_AFTER:
            sub_errs = 0                        # the connection proved stable => subscriptions are fine
        sleep_s, delay = plan_reconnect_sleep(
            conn_start, time.monotonic(), delay, RECONNECT_BASE, RECONNECT_MAX, STABLE_AFTER,
            reconnect_interval)
        await asyncio.sleep(sleep_s)


# ── Entry point ──────────────────────────────────────────────────────────────
async def amain(args):
    address = args.address
    require_eth_address(address, "wsaccount")

    # Always subscribe to the full fixed channel set (like wsexchange.py / wsorderbook.py). Each channel
    # logs to its own file, so there's nothing to gain from a subset -- and letting the operator pick one
    # could silently drop `account`/`orders` and disable the Redis cache-warming that depends on them.
    channels = CHANNELS

    try:
        os.makedirs(args.log_dir, exist_ok=True)
    except OSError as e:
        raise SystemExit(f"wsaccount: cannot create log dir {args.log_dir!r}: {e}")

    # A logger per channel. Logger names are namespaced by address so multiple
    # runs don't collide. Unknown channels / unparseable frames go to stdout.
    loggers = {
        ch: setup_logger(f"wsaccount.{ch}.{address}", f"{args.log_dir}/{ch}{address}.log",
                         args.max_bytes, args.log_backups)
        for ch in channels
    }

    subscriptions = [
        {"type": "subscribe", "channel": ch, "id": address, "snapshot": True}
        for ch in channels
    ]

    print(f"[{address}] Subscribing to {channels}")
    for ch in channels:
        print(f"  {ch} → {args.log_dir}/{ch}{address}.log")

    # Redis cache-warming is automatic: make_publisher returns None if redis-py is absent (then nothing
    # is written -- no error), and a down/slow Redis self-heals in publish(). No opt-out flag needed
    # (matches wsorderbook.py's bbo publisher).
    pub = make_publisher(REDIS_URL, f"[wsaccount {address[:8]} redis]")
    if pub is not None:
        warmed = [c for c in channels if c in CACHE_CHANNELS]
        if warmed:
            # The key lowercases the address (both here and in arcus_redis._acct_key), so this warms the
            # bots' keys regardless of the casing this tool is launched with -- as long as it's the
            # bots' account. Keys are printed so the operator can confirm the right ACCOUNT.
            names = ["account", "positions"] if "account" in warmed else []
            names += ["openOrders"] if "orders" in warmed else []
            print(f"  warming Redis keys (TTL {CACHE_TTL}s, heartbeat {HEARTBEAT}s,"
                  f" orders re-snapshot {ORDERS_RESUBSCRIBE_S}s):")
            for n in names:
                print(f"    {acct_key(args.network, address, n)}")
        else:
            print("  [note] no cache-warming channels selected (need 'account' and/or 'orders')")
    else:
        print("  [note] redis-py not installed -> account-cache Redis warming OFF (file logging only)")

    try:
        await ws_loop(args.url, subscriptions, loggers, args.timestamp, pub, (args.network, address), args.reconnect_interval)
    finally:
        if pub is not None:
            await pub.close()


def main():
    parser = argparse.ArgumentParser(description="Log an account's WebSocket channels to rotating files.")
    parser.add_argument("address", help="Ethereum address (0x + 40 hex)")
    parser.add_argument("--log-dir", default=None,
                        help="log directory (default: /mnt/arcuslogs/<network>)")
    parser.add_argument("--url", default=None,
                        help="override the WebSocket URL (default: derived from the network)")
    parser.add_argument("--max-bytes", type=positive_int, default=LOG_MAX_BYTES,
                        help=f"rotating-file size cap in bytes, > 0 (default {LOG_MAX_BYTES})")
    parser.add_argument("--log-backups", type=positive_int, default=LOG_BACKUP,
                        help=f"rotating-file backup count, >= 1 (default {LOG_BACKUP})")
    parser.add_argument("--timestamp", action="store_true",
                        help="wrap each logged line as JSONL with a local receivedAt "
                             "(default: log the raw server frame)")
    parser.add_argument("--reconnect-interval", type=positive_int, default=None,
                        help="seconds between FAILED reconnect attempts (a genuine drop still reconnects "
                             "immediately). Switches off exponential backoff. Use for a large single-IP fleet "
                             "whose connections all drop at once (e.g. an unstable VPN) to stay under the "
                             "per-IP new-conns/min cap. Default: exponential backoff.")
    parser.add_argument("--infinite-retry", action="store_true",
                        help="never give up after SUB_ERR_LIMIT consecutive subscription rejections -- "
                             "keep resubscribing forever (same backoff / --reconnect-interval). "
                             "Default: exit after SUB_ERR_LIMIT (10).")
    net = parser.add_mutually_exclusive_group(required=True)
    net.add_argument("--testnet", dest="network", action="store_const", const="testnet",
                     help="use testnet")
    net.add_argument("--staging", dest="network", action="store_const", const="staging",
                     help="use staging")
    net.add_argument("--mainnet", dest="network", action="store_const", const="mainnet",
                     help="use mainnet")
    args = parser.parse_args()
    global INFINITE_RETRY
    INFINITE_RETRY = args.infinite_retry
    if args.url is None:
        args.url = ws_url(args.network)
    if args.log_dir is None:
        args.log_dir = os.path.join(LOG_BASE, args.network)
    write_pidfile(args.log_dir, f"wsaccount-{args.address}")   # liveness marker: monitor checks the PID is alive
    try:
        asyncio.run(amain(args))
    except KeyboardInterrupt:
        print("\n[wsaccount] stopped")


if __name__ == "__main__":
    main()
