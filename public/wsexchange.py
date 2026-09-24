#!/usr/bin/env python3
"""Subscribe to exchange-wide WebSocket channels and log each to a rotating file.

  wsexchange.py
  wsexchange.py --log-dir /tmp/ws --max-bytes 5000000 --timestamp

Logs `exchangeAttributeUpdates` and `markets` to one rotating file each.
Unrecognized channels / unparseable frames are printed to stdout (capture the
program's stdout to keep them). Reconnects with exponential backoff.
"""
import argparse
import asyncio
import json
import os
import sys
import time

import websockets
from arcus_common_public import SUB_ERR_LIMIT, CACHE_TTL, REDIS_URL, dec, describe_error, emit as _emit, log_ts, make_publisher, now_iso, plan_reconnect_sleep, positive_int, SubscriptionError, setup_logger, write_pidfile, ws_url   # shared public helpers (formerly local copies)


# ── Constants ────────────────────────────────────────────────────────────────
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

CHANNELS = ["exchangeAttributeUpdates", "markets"]


# ── Logger setup ─────────────────────────────────────────────────────────────
# ── Optional Redis cache publisher (self-healing publisher shared from arcus_common_public) ──────
# Warms the market_maker fleet's exchange-wide markets cache key straight from this WS feed -- the
# push-driven, rate-limit-free replacement for account_poller.py's REST warming. Mirrors
# wsorderbook.py's bbo publisher ; the CachePublisher itself is SHARED from arcus_common_public (make_publisher imported).
# redis.asyncio is OPTIONAL (imported above; absent => publishing is off, file logging as before). A
# publish failure NEVER propagates into the WS loop; bounded socket/connect timeouts make a wedged
# connection RAISE (feeding the self-heal) rather than hang; after PUB_RECREATE_AFTER consecutive
# failures the client is rebuilt outright.
HEARTBEAT = 5          # s; republish the last-known blob at least this often while the socket is up


def market_key(network, name):
    """Exchange-wide key (address None) -- MUST equal arcus_redis._acct_key(network, None, name)."""
    return f"arcus:{network}:{name}"


# ── markets → account-cache transform ─────────────────────────────────────────
def _finite_positive(v):
    """True iff v parses as a finite Decimal > 0 (a usable tick/step increment). Mirrors
    arcus_redis._finite_positive across the public/private boundary -- keep the two in lockstep."""
    d = dec(v)
    return d is not None and d > 0


def markets_cache_blob(contents):
    """Transform a `markets` channel frame into the REST-shaped body the bots read from Redis:
    {"markets": [<market dict>, ...]}. The channel is snapshot-only -- `contents.markets` is a
    FULL MAP keyed by marketId on every frame (subscribe and each ~5s re-emission), so we just
    take its values. Returns None if the frame isn't a usable full snapshot, so a malformed/partial
    body is never written (the reader's is_cacheable would reject it anyway; we skip proactively)."""
    if not isinstance(contents, dict):
        return None
    mk = contents.get("markets")
    if not isinstance(mk, dict):
        return None
    # SECURITY: a market with a zero/negative/non-finite tickSize or stepSize would reach the market maker
    # (DivisionByZero, or negative ticks/quantums into signing). This WS warm path bypasses is_cacheable, so
    # validate here: DROP any market with a bad increment (keep the rest, so one bad market can't wipe the
    # SHARED markets cache). MarketMaker.__init__ also rejects a bad increment for any that reaches it via REST.
    markets = [m for m in mk.values()
               if isinstance(m, dict) and _finite_positive(m.get("tickSize")) and _finite_positive(m.get("stepSize"))]
    if not markets:
        return None
    return {"markets": markets}


# ── Message handling ──────────────────────────────────────────────────────────
async def handle_message(raw, loggers, add_ts, pub, network, state):
    """Route a frame to its channel log; unknown/malformed frames go to STDOUT (the operator
    captures stdout, so they aren't lost). When Redis publishing is on, ALSO warm the exchange-wide
    `markets` account-cache key -- file logging is unchanged, this only adds the Redis write."""
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

    logger = loggers.get(msg.get("channel"))
    if logger is None:                        # not a subscribed channel -> stdout
        print(raw)
        return
    line = json.dumps({"receivedAt": now_iso(), "msg": msg}, separators=(",", ":")) \
        if add_ts else raw
    _emit(logger, line)

    if pub is not None and msg.get("channel") == "markets":
        blob = markets_cache_blob(msg.get("contents"))
        if blob is not None:
            key = market_key(network, "markets")
            await pub.publish(key, blob)
            state["markets"] = (key, blob)        # remember the last-good blob for the heartbeat republish
            state["last_pub"] = time.monotonic()


# ── WebSocket loop ───────────────────────────────────────────────────────────
async def ws_loop(url, subscriptions, loggers, add_ts, pub, network, reconnect_interval=None):
    delay = RECONNECT_BASE
    sub_errs = 0                               # consecutive subscription rejections; reset once a connection is stable
    while True:
        conn_start = None
        state = {"markets": None, "last_pub": 0.0}   # last-good (key, blob) + monotonic publish time, per-connection
        try:
            async with websockets.connect(
                url, open_timeout=OPEN_TIMEOUT,
                ping_interval=PING_INTERVAL, ping_timeout=PING_TIMEOUT,
            ) as ws:
                conn_start = time.monotonic()
                for sub in subscriptions:
                    await ws.send(json.dumps(sub))
                if pub is not None:
                    # Redis present: on-change writes happen in handle_message; here we add the HEARTBEAT
                    # republish so the short-TTL key never expires between the channel's ~5s snapshots.
                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=HEARTBEAT)
                            await handle_message(raw, loggers, add_ts, pub, network, state)
                        except asyncio.TimeoutError:
                            pass
                        now = time.monotonic()
                        if state["markets"] is not None and now - state["last_pub"] >= HEARTBEAT:
                            key, blob = state["markets"]
                            await pub.publish(key, blob)
                            state["last_pub"] = now
                else:
                    # No redis: the original loop, untouched.
                    async for raw in ws:
                        await handle_message(raw, loggers, add_ts, None, network, state)
        except SubscriptionError as e:
            # Reconnectable, not fatal: resubscribe on a fresh socket with the normal backoff. Give up only
            # after SUB_ERR_LIMIT rejections in a row with no stable connection between (a genuinely bad sub).
            sub_errs += 1
            if sub_errs >= SUB_ERR_LIMIT and not INFINITE_RETRY:
                raise SystemExit(f"wsexchange: subscription rejected {sub_errs}x in a row, giving up: {e}")
            print(f"[{log_ts()}] [sub error] {e} — resubscribing (attempt {sub_errs}, with backoff)", file=sys.stderr)
        except websockets.ConnectionClosedOK:
            print(f"[{log_ts()}] [ws] connection closed — reconnecting", file=sys.stderr)   # CLEAN close (redis path: ws.recv() raises this) -> NOT an error
        except Exception as e:
            print(f"[{log_ts()}] [ws error] {describe_error(e)} — reconnecting", file=sys.stderr)   # actual delay set after the stability reset below
        # Sleep/backoff ALWAYS (outside the except) so a CLEAN server close (async-for ended, no exception)
        # also backs off -- previously that path skipped the except and busy-looped at 0 delay. Reset backoff
        # ONLY if the connection proved STABLE (>= STABLE_AFTER s); resetting on connect defeated backoff.
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
    try:
        os.makedirs(args.log_dir, exist_ok=True)
    except OSError as e:
        raise SystemExit(f"wsexchange: cannot create log dir {args.log_dir!r}: {e}")

    loggers = {
        ch: setup_logger(f"wsexchange.{ch}", f"{args.log_dir}/{ch}.log", args.max_bytes, args.log_backups)
        for ch in CHANNELS
    }

    # Always subscribe WITH the snapshot (like wsaccount.py / wsorderbook.py) -- the markets
    # cache-warming needs the full markets map, so there's no reason to skip it.
    subscriptions = [
        {"type": "subscribe", "channel": ch, "snapshot": True} for ch in CHANNELS
    ]

    print(f"[wsexchange] Subscribing to {CHANNELS}")
    for ch in CHANNELS:
        print(f"  {ch} → {args.log_dir}/{ch}.log")

    # Redis cache-warming is automatic: make_publisher returns None if redis-py is absent (then nothing
    # is written -- no error), and a down/slow Redis self-heals in publish(). No opt-out flag needed
    # (matches wsorderbook.py's bbo publisher).
    pub = make_publisher(REDIS_URL, "[wsexchange redis]")
    if pub is not None:
        print(f"  markets → Redis {market_key(args.network, 'markets')} "
              f"(TTL {CACHE_TTL}s, heartbeat {HEARTBEAT}s)")
    else:
        print("  [note] redis-py not installed -> markets Redis warming OFF (file logging only)")

    try:
        await ws_loop(args.url, subscriptions, loggers, args.timestamp, pub, args.network, args.reconnect_interval)
    finally:
        if pub is not None:
            await pub.close()


def main():
    parser = argparse.ArgumentParser(description="Log exchange-wide WebSocket channels to rotating files.")
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
    write_pidfile(args.log_dir, "wsexchange")   # liveness marker: monitor checks the PID is alive
    try:
        asyncio.run(amain(args))
    except KeyboardInterrupt:
        print("\n[wsexchange] stopped")


if __name__ == "__main__":
    main()
