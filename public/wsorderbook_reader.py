#!/usr/bin/env python3
"""wsorderbook_reader.py -- FAN-IN order-book reader: subscribe to MANY markets over ONE WebSocket
connection, maintain each market's L2 book, and publish the full book + BBO to Redis. NO webserver.

Pairs with wsorderbook_reader_companion.py, which serves ONE market's book on the usual HTTP port by
reading it back from Redis. Splitting the two lets a whole fleet share ONE websocket connection (N markets
-> 1 connection) instead of one connection per market -- the fix for the per-IP CONCURRENT-connection cap
that locks out a large single-IP fleet (see the connection-limit incident).

  wsorderbook_reader.py BTC-USD,ETH-USD,SOL-USD --testnet
  wsorderbook_reader.py BTC-USD ETH-USD SOL-USD --mainnet --reconnect-interval 120   # space-separated ok too

Markets are a comma- and/or space-separated list (a bare positional -- it's required, so no flag). Run ONE
reader for everything, or shard the market list across 2-3 readers for resilience (keys are per-market, so
overlap is harmless last-writer-wins); each reader is exactly ONE connection.

Reuses wsorderbook.py's engine verbatim (OrderBook / BboPublisher / exceptions / resolve_market / constants)
so the book logic stays one source of truth. Requires the `redis` package (publishing to Redis is the point).
"""
import argparse
import asyncio
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))   # find sibling wsorderbook.py (even via symlink)

import websockets

import wsorderbook as wsob
from wsorderbook import OrderBook, BboPublisher, SequenceGap, _bbo_ok, _new_bbo_client
from arcus_common_public import (NETWORKS, PUB_RECREATE_AFTER, REDIS_URL, SUB_ERR_LIMIT, describe_error,
                                 emit as _emit, log_ts, markets_cache_path, now_iso, plan_reconnect_sleep,
                                 positive_int, setup_logger, SubscriptionError, write_pidfile, ws_url)

try:
    import redis.asyncio as aioredis      # REQUIRED here (unlike wsorderbook where it's optional): the book IS the redis payload
except ImportError:
    aioredis = None

PROG = "wsorderbook_reader"
INFINITE_RETRY = False   # --infinite-retry: never give up after SUB_ERR_LIMIT subscription rejections (retry forever)
BOOK_KEY_FMT = "arcus:{network}:book:{market}"   # value = {ready, bids, asks, ts, seq}; the companion reads this
BOOK_TTL     = 5        # s; liveness/staleness guard on the book key -- companion serves 503 once it expires (reader dead)
FLUSH_INTERVAL = 1.0    # s; snapshot every market's current book to Redis this often (also refreshes ts/TTL on quiet markets)
RESYNC_MIN_INTERVAL = 5.0   # s; per-market cap on l2 re-subscribes so a persistently-malformed market (e.g. a `{}` snapshot)
                            # can't spam the SHARED socket with subscribe frames (it just stays not-ready and retries slowly)
RESUB_ACK_TIMEOUT = 3.0     # s; if the 'unsubscribed' ack for a resync never arrives, the flusher re-sends the unsubscribe
                            # (a dropped ack must not strand a market not-ready until the next full reconnect)


class BookPublisher:
    """Resilient full-book -> Redis writer. Mirrors BboPublisher's contract: a write failure NEVER propagates
    (book ingestion/flush must not stall), the client has bounded timeouts, and after PUB_RECREATE_AFTER
    consecutive failures the client is recreated to clear a wedged pool. Separate from BboPublisher because
    its payload/validation differ; it deliberately does NOT touch wsorderbook.py."""

    def __init__(self, url):
        self._url = url
        self._r = _new_bbo_client(url)
        self._fails = 0
        self._err_last = 0.0

    async def write(self, key, blob, ttl):
        try:
            await self._r.set(key, json.dumps(blob, separators=(",", ":")), ex=ttl)
            self._fails = 0
        except Exception as e:                    # redis down / slow / wedged -- log (throttled), never propagate
            self._fails += 1
            now = time.monotonic()
            if now - self._err_last >= 30:
                print(f"[{log_ts()}] [book redis] {describe_error(e)}", file=sys.stderr, flush=True)
                self._err_last = now
            if self._fails >= PUB_RECREATE_AFTER:
                try:
                    self._r = _new_bbo_client(self._url)   # from_url is lazy; near-impossible to raise
                except Exception:
                    pass
                self._fails = 0


def market_subscriptions(market, with_bbo):
    """The per-market subscribe frames, keyed by `id` so the reader can route each returned frame back."""
    subs = [
        {"type": "subscribe", "channel": "l2OrderbookUpdates", "id": market, "nLevels": 100, "snapshot": True},
        {"type": "subscribe", "channel": "trades", "id": market, "snapshot": True},
        {"type": "subscribe", "channel": "oraclePrices", "id": market, "snapshot": True},
    ]
    if with_bbo:
        subs.append({"type": "subscribe", "channel": "bbo", "id": market})
    return subs


async def resync_market(ws, ctx, market, reason):
    """A per-market book error (seq gap / malformed snapshot|delta): reset THAT book and get a FRESH snapshot on
    the SAME socket -- never a reconnect, so one bad market can't drop the other books. Arcus REJECTS a duplicate
    subscribe ('Already subscribed to l2OrderbookUpdates:<m>'), so a plain re-subscribe never delivers a snapshot
    and the book stays not-ready forever. Instead UNSUBSCRIBE here; the re-subscribe is sent when the
    'unsubscribed' ack arrives (route_frame) -- back-to-back unsub+sub races the server ('Already/Not subscribed').
    Throttled per market so a persistently-bad market can't spam the shared socket."""
    now = time.monotonic()
    st = ctx["resync"][market]
    if st["await_since"]:                          # already unsubscribed, awaiting the ack -> the flusher drives any retry
        return False
    if now - st["last"] < RESYNC_MIN_INTERVAL:
        return False                              # tried recently -> stay not-ready, retry later
    st["last"] = now
    st["await_since"] = now
    print(f"[{log_ts()}] [{market}] book resync ({reason}) -- unsubscribing l2 (re-subscribe on the ack)", file=sys.stderr, flush=True)
    await ws.send(json.dumps({"type": "unsubscribe", "channel": "l2OrderbookUpdates",
                              "id": market}))       # a send failure = dead socket -> propagates -> reconnect
    return True                                    # the unsubscribe was initiated (re-subscribe follows on the ack)


async def route_frame(ws, raw, ctx):
    """Dispatch ONE frame to the right market's book/logger by its `id`. Per-market book issues are handled
    locally (resync that market); only a SOCKET-level subscription error propagates (whole-socket reconnect)."""
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"[parse error] {e} | {str(raw)[:200]}")
        return
    if not isinstance(msg, dict):
        print(str(raw)[:200])
        return
    # Channel-less error / status>=400 = a SOCKET-level subscription/request failure -> raise so ws_loop reconnects.
    if msg.get("channel") is None and (isinstance(msg.get("error"), dict)
                                       or (isinstance(msg.get("status"), int) and msg["status"] >= 400)):
        err = msg.get("error") if isinstance(msg.get("error"), dict) else {}
        raise SubscriptionError(err.get("message") or f"status {msg.get('status')}: {err or msg}")
    channel = msg.get("channel")
    market = msg.get("id")
    if market not in ctx["books"]:                # a frame for a market we didn't subscribe -> stdout, keep reading
        print(str(raw)[:200])
        return
    if channel == "bbo":
        if ctx["bbo_pub"] is not None and msg.get("type") in ("subscribed", "channel_data"):
            c = msg.get("contents")
            if isinstance(c, dict) and _bbo_ok(c):   # well-formed (empty market, or finite-positive bestBid/bestAsk)
                st = ctx["bbo_states"][market]
                st["bbo"] = c
                await ctx["bbo_pub"].publish(ctx["bbo_keys"][market], c, time.time())
                st["last_pub"] = time.monotonic()
            # else: keep last-good bbo (the flusher heartbeat republishes it); never store/write junk
        return
    logger = ctx["loggers"].get(market, {}).get(channel)
    if logger is None:                            # not a subscribed channel for this market -> stdout
        print(str(raw)[:200])
        return
    line = (json.dumps({"receivedAt": now_iso(), "msg": msg}, separators=(",", ":")) if ctx["add_ts"] else raw)
    _emit(logger, line)
    if channel == "l2OrderbookUpdates":
        book = ctx["books"][market]
        mtype = msg.get("type")
        if mtype == "unsubscribed":               # our resync unsubscribe was ack'd -> NOW re-subscribe for a fresh snapshot
            st = ctx["resync"][market]
            if st["await_since"]:
                st["await_since"] = 0.0
                st["degraded_at"] = 0.0           # the incoming fresh snapshot also clears any pending degraded resync
                await ws.send(json.dumps({"type": "subscribe", "channel": "l2OrderbookUpdates",
                                          "id": market, "nLevels": 100, "snapshot": True}))
            return
        if mtype == "degraded":                   # server says this subscription is STALE (e.g. reason=snapshot_stale):
            book.reset()                          # reset -> not-ready, then honor retryAfterMs before resyncing (flusher fires it),
            retry_s = (msg.get("retryAfterMs") or 0) / 1000.0   # so we re-subscribe when the server's snapshot is fresh, not into a stale one
            ctx["resync"][market]["degraded_at"] = time.monotonic() + retry_s
            print(f"[{log_ts()}] [{market}] DEGRADED ({msg.get('reason')}) -- resync in {retry_s:.1f}s (server retryAfterMs)",
                  file=sys.stderr, flush=True)
            return
        contents = msg.get("contents") or {}
        try:
            if mtype == "subscribed":
                book.on_snapshot(contents)        # MalformedFrame on a `{}`/bad snapshot -> caught below
            elif mtype == "channel_data":
                book.on_delta(contents)           # SequenceGap (true gap) or MalformedFrame (bad shape) -> caught below
        except SequenceGap as e:                  # MalformedFrame subclasses SequenceGap -> both handled here, per-market
            book.reset()
            await resync_market(ws, ctx, market, e)


async def ws_loop_multi(url, markets, ctx, reconnect_interval):
    """ONE websocket for ALL `markets`: subscribe each, then route every frame by `id`. Socket-level failures
    reset every book and reconnect with backoff (mirrors wsorderbook.ws_loop); per-market book issues are
    resync'd inside route_frame without dropping the socket."""
    delay = wsob.RECONNECT_BASE
    sub_errs = 0
    with_bbo = ctx["bbo_pub"] is not None
    while True:
        conn_start = None
        ctx["ws"] = None
        try:
            async with websockets.connect(url, open_timeout=wsob.OPEN_TIMEOUT,
                                          ping_interval=wsob.PING_INTERVAL, ping_timeout=wsob.PING_TIMEOUT) as ws:
                conn_start = time.monotonic()
                ctx["ws"] = ws                    # published for the flusher's lost-ack resync retry
                for m in markets:
                    ctx["books"][m].reset()       # a fresh connection -> every book is stale until its snapshot re-arrives
                    ctx["resync"][m]["await_since"] = 0.0   # fresh subscribe below -> no pending unsubscribe carries over
                    ctx["resync"][m]["degraded_at"] = 0.0   # ditto: the reconnect re-snapshots, so drop any pending degraded resync
                    for sub in market_subscriptions(m, with_bbo):
                        await ws.send(json.dumps(sub))
                async for raw in ws:
                    await route_frame(ws, raw, ctx)
            for m in markets:
                ctx["books"][m].reset()
            print(f"[{log_ts()}] [ws] connection closed -- resubscribing all markets", file=sys.stderr, flush=True)
        except SubscriptionError as e:
            for m in markets:
                ctx["books"][m].reset()
            sub_errs += 1
            if sub_errs >= SUB_ERR_LIMIT and not INFINITE_RETRY:
                raise SystemExit(f"{PROG}: subscription rejected {sub_errs}x in a row, giving up: {e}")
            print(f"[{log_ts()}] [sub error] {e} -- resubscribing (attempt {sub_errs}, with backoff)", file=sys.stderr, flush=True)
        except websockets.ConnectionClosedOK:
            for m in markets:
                ctx["books"][m].reset()
            print(f"[{log_ts()}] [ws] connection closed -- resubscribing all markets", file=sys.stderr, flush=True)
        except Exception as e:
            for m in markets:
                ctx["books"][m].reset()
            print(f"[{log_ts()}] [ws error] {describe_error(e)} -- reconnecting", file=sys.stderr, flush=True)
        if conn_start is not None and time.monotonic() - conn_start >= wsob.STABLE_AFTER:
            sub_errs = 0                          # the connection proved stable => the subscriptions are fine
        sleep_s, delay = plan_reconnect_sleep(conn_start, time.monotonic(), delay, wsob.RECONNECT_BASE,
                                              wsob.RECONNECT_MAX, wsob.STABLE_AFTER, reconnect_interval)
        await asyncio.sleep(sleep_s)


async def flusher(markets, ctx):
    """Every FLUSH_INTERVAL, snapshot each market's current book to Redis (refreshing ts/TTL so a quiet
    market's key never expires) and heartbeat any quiet BBO. Writing not-ready books too keeps the key
    PRESENT while the reader is alive, so the companion can tell 'reader up, not ready' (503) from 'reader
    down' (key expired)."""
    while True:
        await asyncio.sleep(FLUSH_INTERVAL)
        now_wall = time.time()
        now_mono = time.monotonic()
        ws = ctx.get("ws")
        for m in markets:
            book = ctx["books"][m]
            payload = book.payload()              # {ready, bids, asks}
            payload["ts"] = now_wall
            payload["seq"] = book.last_seq
            await ctx["book_pub"].write(ctx["book_keys"][m], payload, BOOK_TTL)
            if ctx["bbo_pub"] is not None:        # BBO heartbeat: keep a quiet market's bbo key warm (matches wsorderbook)
                st = ctx["bbo_states"][m]
                if st["bbo"] is not None and now_mono - st["last_pub"] >= wsob.HEARTBEAT:
                    await ctx["bbo_pub"].publish(ctx["bbo_keys"][m], st["bbo"], now_wall)
                    st["last_pub"] = now_mono
            rst = ctx["resync"][m]                 # lost-'unsubscribed'-ack fallback: a dropped ack must not strand a
            if rst["await_since"] and ws is not None and now_mono - rst["await_since"] >= RESUB_ACK_TIMEOUT:
                rst["await_since"] = now_mono      # market not-ready forever -> re-send the unsubscribe on the live socket
                try:
                    await ws.send(json.dumps({"type": "unsubscribe", "channel": "l2OrderbookUpdates", "id": m}))
                except Exception:
                    pass                           # socket dying -> ws_loop_multi reconnects + resubscribes all
            if rst["degraded_at"] and ws is not None and now_mono >= rst["degraded_at"]:
                # server-signalled DEGRADED (snapshot_stale): retryAfterMs has elapsed -> resync for a fresh snapshot.
                if await resync_market(ws, ctx, m, "degraded snapshot_stale"):
                    rst["degraded_at"] = 0.0       # cleared once the resync actually fires; else retry next tick (throttled)


async def amain(args):
    wsob.MARKETS_CACHE = markets_cache_path(args.network)   # resolve_market reads these module globals
    wsob.MARKETS_URL = f"{NETWORKS[args.network]}/v1/markets"
    os.makedirs(args.log_dir, exist_ok=True)

    canon = []
    seen = set()
    for raw_m in args.markets:
        if not wsob.MARKET_RE.match(raw_m):
            raise SystemExit(f"{PROG}: invalid market {raw_m!r} (allowed: letters, digits, . _ -).")
        _, name = await wsob.resolve_market(raw_m)   # canonical display name (case-normalized, validated)
        if name not in seen:
            seen.add(name)
            canon.append(name)
    markets = canon
    print(f"[{PROG}] {args.network}: {len(markets)} market(s) over ONE connection -> {', '.join(markets)}")

    books = {m: OrderBook() for m in markets}
    loggers = {m: {
        "l2OrderbookUpdates": setup_logger(f"wsob.l2.{m}", f"{args.log_dir}/wsorderbook{m}.log", args.max_bytes, args.log_backups),
        "trades":             setup_logger(f"wsob.trades.{m}", f"{args.log_dir}/wstrades{m}.log", args.max_bytes, args.log_backups),
        "oraclePrices":       setup_logger(f"wsob.oracle.{m}", f"{args.log_dir}/oraclePrices{m}.log", args.max_bytes, args.log_backups),
    } for m in markets}
    ctx = {
        "books": books,
        "loggers": loggers,
        "add_ts": args.timestamp,
        "book_keys": {m: BOOK_KEY_FMT.format(network=args.network, market=m) for m in markets},
        "bbo_keys": {m: wsob.BBO_KEY_FMT.format(network=args.network, market=m) for m in markets},
        "bbo_pub": BboPublisher(args.redis_url),
        "book_pub": BookPublisher(args.redis_url),
        "bbo_states": {m: {"bbo": None, "last_pub": 0.0} for m in markets},
        "resync": {m: {"last": 0.0, "await_since": 0.0, "degraded_at": 0.0} for m in markets},
        "ws": None,                               # current live socket; the flusher uses it for the lost-ack resync retry
    }
    print(f"[{PROG}] publishing full book -> 'arcus:{args.network}:book:<market>' (TTL {BOOK_TTL}s) "
          f"+ BBO -> 'arcus:{args.network}:bbo:<market>'  [redis {args.redis_url}]")

    asyncio.create_task(flusher(markets, ctx))
    await ws_loop_multi(args.url, markets, ctx, args.reconnect_interval)


def main():
    p = argparse.ArgumentParser(description="Fan-in: read MANY markets' L2 books over ONE WebSocket and "
                                            "publish them to Redis (no webserver). Companion: wsorderbook_reader_companion.py.")
    p.add_argument("markets", nargs="+",
                   help="markets to read: comma- and/or space-separated, e.g. BTC-USD,ETH-USD SOL-USD")
    p.add_argument("--url", default=None, help="override the WebSocket URL (default: derived from the network)")
    p.add_argument("--redis-url", default=None, help=f"Redis URL to publish to (default: {REDIS_URL})")
    p.add_argument("--log-dir", default=None, help="log directory (default: /mnt/arcuslogs/<network>)")
    p.add_argument("--max-bytes", type=positive_int, default=wsob.LOG_MAX_BYTES,
                   help=f"rotating channel-log size cap in bytes, > 0 (default {wsob.LOG_MAX_BYTES})")
    p.add_argument("--log-backups", type=positive_int, default=wsob.LOG_BACKUP,
                   help=f"rotating channel-log backup count, >= 1 (default {wsob.LOG_BACKUP})")
    p.add_argument("--timestamp", action="store_true",
                   help="wrap each logged channel line as JSONL with a local receivedAt (default: raw server frame)")
    p.add_argument("--reconnect-interval", type=positive_int, default=None,
                   help="seconds between FAILED reconnect attempts (a genuine drop still reconnects immediately). "
                        "Switches off exponential backoff. Default: exponential backoff.")
    p.add_argument("--infinite-retry", action="store_true",
                   help="never give up after SUB_ERR_LIMIT consecutive subscription rejections -- "
                        "keep resubscribing forever (same backoff / --reconnect-interval). "
                        "Default: exit after SUB_ERR_LIMIT (10).")
    net = p.add_mutually_exclusive_group(required=True)
    net.add_argument("--testnet", dest="network", action="store_const", const="testnet", help="use testnet")
    net.add_argument("--staging", dest="network", action="store_const", const="staging", help="use staging")
    net.add_argument("--mainnet", dest="network", action="store_const", const="mainnet", help="use mainnet")
    args = p.parse_args()
    global INFINITE_RETRY
    INFINITE_RETRY = args.infinite_retry

    flat = []                                     # accept comma- AND space-separated; strip/dedupe, preserve order
    for tok in args.markets:
        flat.extend(x.strip() for x in tok.split(",") if x.strip())
    args.markets = flat
    if not args.markets:
        raise SystemExit(f"{PROG}: no markets given.")
    if aioredis is None:
        raise SystemExit(f"{PROG}: requires the 'redis' package -- publishing books to Redis is this tool's whole purpose.")

    args.url = args.url or ws_url(args.network)
    args.redis_url = args.redis_url or REDIS_URL
    args.log_dir = args.log_dir or os.path.join(wsob.LOG_BASE, args.network)
    write_pidfile(args.log_dir, f"{PROG}-{args.markets[0]}-{len(args.markets)}")   # liveness marker (first market + count)
    try:
        asyncio.run(amain(args))
    except KeyboardInterrupt:
        print(f"\n[{PROG}] stopped")


if __name__ == "__main__":
    main()
