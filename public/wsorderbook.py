#!/usr/bin/env python3
"""Maintain a market's L2 order book from the WebSocket stream and serve it over HTTP.

  wsorderbook.py BTC-USD
  wsorderbook.py BTC-USD --host 0.0.0.0 --log-dir /tmp/ob

Subscribes to l2OrderbookUpdates/trades/oraclePrices, applies the snapshot +
deltas into a local book, and serves it at http://<host>:<PORT_BASE[net]+marketId>/orderbook
(PORT_BASE: mainnet 10000 / testnet 11000 / staging 12000)
(the endpoint showorderbook.py reads). The book is only served once the first
snapshot has arrived (503 {"ready": false} until then), and the delta stream's
per-market lastSequenceId is checked for gaps -- on a gap it re-subscribes
(fresh snapshot) so the local book can't drift silently.
"""
import argparse
import asyncio
import json
import os
import re
import sys
import time
from decimal import Decimal, InvalidOperation

import aiohttp
import websockets
from aiohttp import web
from arcus_common_public import SUB_ERR_LIMIT, NETWORKS, PUB_RECREATE_AFTER, PUB_SOCKET_TIMEOUT, REDIS_URL, _PUB_ERR_THROTTLE, describe_error, emit as _emit, log_ts, markets_cache_path, now_iso, plan_reconnect_sleep, positive_int, read_markets_cache, require_dict, SubscriptionError, setup_logger, write_markets_cache, write_pidfile, ws_url   # shared public helpers (formerly local copies)

try:
    import redis.asyncio as aioredis      # OPTIONAL dependency: absent => the BBO→Redis feature is simply off
except ImportError:
    aioredis = None

# ── Constants ────────────────────────────────────────────────────────────────
BASE_URL     = None   # all four set in main() from the required --testnet/--staging/--mainnet selector
MARKETS_URL  = None
WS_URL       = None
MARKETS_CACHE = None

# Launcher handoff cache. showmarkets.py --createjson writes the raw /v1/markets response here so the
# many per-market tools the launcher starts sequentially resolve their market from ONE file instead of
# each re-hitting the server. The launcher exports ARCUS_MARKETS_CACHE with a PER-RUN path so a
# foreign/stale file at the predictable path can never be trusted; we fall back to this NETWORK-scoped
# predictable path only for manual/standalone use (testnet/staging/mainnet marketId maps differ, so
# they must never be crossed). Read is fail-open -- a missing/corrupt file just falls back to a live
# fetch. The launcher removes its per-run file when it finishes.

# HTTP port = PORT_BASE[network] + marketId. Per-network base keeps the 3 networks'
# orderbook servers on disjoint ranges (1000 markets each): mainnet 10xxx, testnet
# 11xxx, staging 12xxx.
PORT_BASE = {"mainnet": 10000, "testnet": 11000, "staging": 12000}
MAX_MARKET_ID   = 999   # PORT_BASE entries are 1000 apart, so a marketId >= 1000 would push the port into the
                        # NEXT network's band (mainnet 1000 -> 11000 = testnet's base) or past 65535. Mirrors
                        # showorderbook.MAX_MARKET_ID -- the reader already rejects out-of-band ids; the server must too.
LOG_BASE        = "/mnt/arcuslogs"   # logs go under LOG_BASE/<network> (subdir auto-created)
LOG_MAX_BYTES   = 2097152
LOG_BACKUP      = 4
RECONNECT_BASE  = 1
RECONNECT_MAX   = 60         # exponential-backoff cap for the DEFAULT path ONLY -- UNUSED under --reconnect-interval.
                            # Was briefly 120 to halve a 39-proc fleet's max-backoff attempt rate (78->39/min), but
                            # MAX-tuning does NOT fix the real trigger (a VPN IP-change drops ALL conns at once);
                            # --reconnect-interval is the fix. Reverted to 60 to match dydxv4 -- a large single-IP
                            # fleet should set --reconnect-interval (which ignores this).
STABLE_AFTER    = 30         # s a connection must STAY UP before backoff resets (else an accept-then-close flap busy-loops)
OPEN_TIMEOUT    = 10
PING_INTERVAL   = 20
PING_TIMEOUT    = 20
MARKET_RE       = re.compile(r"^[A-Za-z0-9._-]+$")   # safe for filenames + ids
INFINITE_RETRY  = False   # --infinite-retry: never give up after SUB_ERR_LIMIT subscription rejections (retry forever)

# Optional BBO → Redis publisher. redis-py is an OPTIONAL dependency; if it's absent (aioredis is None)
# the bbo subscription is skipped and this tool behaves exactly as before (HTTP orderbook only).
BBO_KEY_FMT = "arcus:{network}:bbo:{market}"   # one key per market; value = native bbo `contents` + our `ts`
BBO_TTL     = 3        # s; ≈ the reader's age-guard AND > HEARTBEAT so a live-but-quiet key never expires between beats
HEARTBEAT   = 1.0      # s; refresh `ts` at least this often while the socket is up (idle markets stay "alive")


class SequenceGap(Exception):
    """Raised when the L2 delta stream skips a sequence id (local book may be stale)."""


class MalformedFrame(SequenceGap):
    """Raised when a frame is UNUSABLE due to bad shape (a snapshot missing bids/asks lists / all-malformed
    rows; a delta that isn't an object, has no numeric lastSequenceId, or carries a non-list side). A subclass
    of SequenceGap so it still means "book not trustworthy", but caught SEPARATELY in ws_loop and takes the
    NORMAL BACKOFF path. A GENUINE sequence gap (seq != last+1) resyncs IMMEDIATELY (a fresh snapshot fixes it,
    no backoff); a persistently MALFORMED frame can't be fixed by resyncing, so a no-backoff resync would spin
    a tight reconnect storm (self-DoS, worst exactly during a venue incident) -- hence it backs off instead."""


# ── Order book state ──────────────────────────────────────────────────────────
class OrderBook:
    """Local L2 book keyed by Decimal price (so "1.0" and "1.00" collapse)."""

    def __init__(self):
        self.reset()

    def reset(self):
        self.bids = {}
        self.asks = {}
        self.ready = False
        self.last_seq = None
        self._awaiting_first_delta = False

    def _apply(self, book, levels, seq):
        if not isinstance(levels, list):        # a present-but-non-list bids/asks (bad shape) -> MALFORMED -> backoff
            raise MalformedFrame(f"levels is not a list: {type(levels).__name__}")   # (not a genuine seq gap; don't storm)
        for lv in levels:
            try:
                price, size = lv                # a scalar / wrong-arity row -> skip it (matches the bad-value skips
            except (TypeError, ValueError):     # below); never let one malformed row crash to the [ws error]+backoff path
                continue
            try:
                key = Decimal(price)
            except (InvalidOperation, TypeError, ValueError):
                continue
            if not key.is_finite():                 # Decimal() accepts "NaN"/"Infinity" -- a non-finite PRICE can't be
                continue                            # a real level; it leaks into the served /orderbook and NaN-keys the
                                                    # payload() Decimal sort (-> HTTP 500). Drop the row.
            if key <= 0:                            # a non-positive PRICE is not a real level -> drop (would otherwise be
                continue                            # stored and served/rendered in the local L2 HTTP book as a bad level)
            try:
                dsize = Decimal(size)
            except (InvalidOperation, TypeError, ValueError):
                continue                            # unparseable SIZE (JSON null / "" / non-numeric) -> malformed ->
                                                    # drop, exactly like the non-finite case below. (The #13 guard set
                                                    # dsize=None and fell through, STORING the bad size to be served.)
            if not dsize.is_finite():               # non-finite SIZE ("NaN"/"Infinity") -> malformed -> drop
                continue
            if dsize < 0:                           # a NEGATIVE size is malformed -> drop (0 is the valid "remove level"
                continue                            # signal handled just below; only strictly-positive sizes are levels)
            if dsize == 0:                          # explicit removal
                book.pop(key, None)
            else:
                book[key] = [price, size, seq]

    @staticmethod
    def _seq(contents):
        """lastSequenceId as int (accepts int or numeric string), else None."""
        try:
            return int(contents.get("lastSequenceId"))
        except (AttributeError, TypeError, ValueError):   # AttributeError: contents isn't a dict (bad shape)
            return None

    def on_snapshot(self, contents):
        # A snapshot is a FULL replacement, so it MUST explicitly carry BOTH sides as lists. If bids/asks are
        # absent (or contents isn't a dict), .get(...,[]) would fabricate an EMPTY book and still set ready=True
        # -- a reader would then read "ready, no liquidity" off a malformed frame (fail-OPEN). A genuinely empty
        # market still sends bids:[] and asks:[], so requiring them loses nothing. Malformed -> resync (reset +
        # immediate resubscribe) rather than serve a fabricated-empty ready book.
        if not (isinstance(contents, dict)
                and isinstance(contents.get("bids"), list)
                and isinstance(contents.get("asks"), list)):
            raise MalformedFrame(f"malformed snapshot (bids/asks not present as lists): {type(contents).__name__}")
        seq = self._seq(contents)             # normalized to int (or None)
        self.bids.clear()
        self.asks.clear()
        self._apply(self.bids, contents["bids"], seq)
        self._apply(self.asks, contents["asks"], seq)
        # Fabricate-empty guard: _apply skips malformed rows, so a side whose rows are ALL malformed would drop
        # to empty and we'd serve an empty book as READY (fail-OPEN). A genuinely empty market sends an EMPTY
        # list, so "rows in, none survived" means malformed -> resync rather than mark ready off a fabricated-
        # empty side. (An all-zero-size snapshot side, which shouldn't occur, also resyncs -- benign, recovers.)
        if (contents["bids"] and not self.bids) or (contents["asks"] and not self.asks):
            raise MalformedFrame("snapshot rows all malformed/empty (side dropped to empty)")
        self.last_seq = seq
        # The first delta after a snapshot is NOT snapshot_seq+1 (the snapshot's
        # ln lags the live stream), so adopt its seq as baseline without a gap check.
        self._awaiting_first_delta = True
        self.ready = True

    def on_delta(self, contents):
        if not isinstance(contents, dict):    # bad SHAPE (not a genuine seq gap) -> MalformedFrame -> backoff, not
            raise MalformedFrame(f"delta contents is not an object: {type(contents).__name__}")   # a no-backoff storm
        seq = self._seq(contents)
        if seq is None:                       # missing/non-numeric lastSequenceId = malformed frame -> backoff
            raise MalformedFrame(f"missing/non-numeric lastSequenceId: {contents.get('lastSequenceId')!r}")
        if self.last_seq is not None and not self._awaiting_first_delta:
            if seq != self.last_seq + 1:
                raise SequenceGap(f"expected {self.last_seq + 1}, got {seq}")
        self._apply(self.bids, contents.get("bids", []), seq)
        self._apply(self.asks, contents.get("asks", []), seq)
        self.last_seq = seq
        self._awaiting_first_delta = False

    def payload(self):
        bids = sorted(self.bids.values(), key=lambda x: Decimal(x[0]), reverse=True)
        asks = sorted(self.asks.values(), key=lambda x: Decimal(x[0]))
        return {"ready": self.ready, "bids": bids, "asks": asks}


# ── Logger setup ─────────────────────────────────────────────────────────────
# ── HTTP handler ─────────────────────────────────────────────────────────────
async def handle_orderbook(request):
    book = request.app["book"]
    payload = book.payload()
    # 503 until the first snapshot, so a caller never mistakes "not ready yet"
    # for "no liquidity".
    return web.json_response(payload, status=200 if book.ready else 503)


# ── BBO → Redis publisher (optional) ─────────────────────────────────────────
_pub_err_last = 0.0                 # a busy market would otherwise print after EVERY frame + heartbeat and
_pub_err_suppressed = 0            # flood the redirected stdout/err log.
_pub_recreate_last = 0.0           # separate throttle for the "recreated client" notice (same flood concern)
                                   # instead of hanging the WS ingestion loop -- that raise feeds _fails ->
                                   # _recreate (the real analog to dydxv4's per-call deadline). Kept at the
                                   # HEARTBEAT cadence: publish() is awaited INSIDE the recv loop, so a larger
                                   # value would stall order-book ingestion that long per cycle during a wedge
                                   # (and localhost Redis answers sub-ms, so 1s is already hugely generous).


def _pub_log(msg):
    """stderr log that NEVER raises. publish()/_recreate() run inside the WS recv loop, so a logging
    failure (e.g. OSError writing a disk-full redirected stderr) must not propagate and disturb the book."""
    try:
        print(f"[{log_ts()}] {msg}", file=sys.stderr, flush=True)
    except Exception:
        pass


def _new_bbo_client(url):
    """A redis.asyncio client with bounded socket + connect timeouts, so a hung/wedged connection RAISES
    (feeding the self-heal counter) rather than stalling the WS ingestion loop indefinitely."""
    return aioredis.from_url(url, socket_timeout=PUB_SOCKET_TIMEOUT, socket_connect_timeout=PUB_SOCKET_TIMEOUT)


def _finite_pos(v):
    """True iff `v` parses as a finite Decimal > 0."""
    try:
        d = Decimal(str(v))
    except (InvalidOperation, TypeError, ValueError):
        return False
    return d.is_finite() and d > 0


def _bbo_ok(contents):
    """VALIDATE a bbo `contents` (a dict) BEFORE writing it to Redis: each of bestBid/bestAsk must be null/absent
    (an empty side -> the reader falls back) OR a dict carrying a finite-POSITIVE price AND size. A present-but-
    garbage side (missing / non-finite / non-positive price|size, e.g. {"bestBid":{"price":"NaN"}}) means the
    frame is untrustworthy -- don't publish a top-of-book the MM would read off it. (The reader validates too;
    the publisher simply must not write junk.) An empty {} (empty market) is valid -> published as null sides."""
    for s in ("bestBid", "bestAsk"):
        v = contents.get(s)
        if v is None:
            continue
        if not (isinstance(v, dict) and _finite_pos(v.get("price")) and _finite_pos(v.get("size"))):
            return False
    return True


class BboPublisher:
    """Owns the (optional) Redis client for BBO publishing and SELF-HEALS. A publish failure NEVER
    propagates -- the WS/book/HTTP path must never stall or crash on Redis (the existing contract). The
    client has bounded socket/connect timeouts, so a wedged connection RAISES (it can't hang the WS loop);
    redis.asyncio's pool auto-reconnects a fast-failing client on the next command, and after
    PUB_RECREATE_AFTER CONSECUTIVE failures we also recreate the client outright to clear a stuck pool
    (the deadline+rebuild pairing mirrors dydxv4's channel-rebuild self-heal).
    `ts` is our write time (unix seconds); the reader ages off it to decide the feed is alive."""

    def __init__(self, url):
        self._url = url
        self._r = _new_bbo_client(url)   # from_url is lazy (no I/O) -- connects on the first command
        self._fails = 0

    async def publish(self, key, contents, ts):
        # VALIDATE BEFORE WRITING. (1) A truthy NON-dict (venue glitch: list/str/number) would raise in dict()
        # below, OUTSIDE the try -> bubble to ws_loop -> RESET the core L2 book (BBO is auxiliary, must never do
        # that). (2) A dict with a GARBAGE bestBid/bestAsk (non-finite/non-positive price|size) must not be written
        # verbatim for the MM to read a bad top-of-book off. Skip either -> key ages out / heartbeat keeps last-good.
        if not (isinstance(contents, dict) and _bbo_ok(contents)):
            return
        blob = dict(contents)      # bestBid/bestAsk (price+size), lastSequenceId, globalSequenceId, timestamp -- verbatim
        blob["ts"] = ts
        global _pub_err_last, _pub_err_suppressed
        try:
            await self._r.set(key, json.dumps(blob, separators=(",", ":")), ex=BBO_TTL)
            self._fails = 0
            _pub_err_suppressed = 0     # recovered; _pub_err_last is intentionally NOT reset, so a flapping
                                        # Redis (fail/succeed/fail...) still can't flood -- the 30s window persists
        except Exception as e:         # redis down / slow / wedged -- log (throttled), never propagate
            self._fails += 1
            now = time.monotonic()
            if now - _pub_err_last >= _PUB_ERR_THROTTLE:
                extra = f" (+{_pub_err_suppressed} suppressed)" if _pub_err_suppressed else ""
                _pub_log(f"[bbo redis] {describe_error(e)}{extra}")
                _pub_err_last, _pub_err_suppressed = now, 0
            else:
                _pub_err_suppressed += 1
            if self._fails >= PUB_RECREATE_AFTER:
                await self._recreate()

    async def _recreate(self):
        """Discard the current client (best-effort close) and build a fresh one -- resets the connection
        pool; the next publish reconnects. Fully guarded: MUST NOT raise (it runs inside publish()'s
        except, and a raise here would propagate to the WS loop and force a needless reconnect)."""
        global _pub_recreate_last
        old = self._r
        try:
            self._r = _new_bbo_client(self._url)
        except Exception:
            pass                        # from_url is lazy (near-impossible to raise); keep the old client if so
        self._fails = 0                 # reset regardless, so a failed rebuild doesn't re-enter recreate every cycle
        now = time.monotonic()
        if now - _pub_recreate_last >= _PUB_ERR_THROTTLE:   # throttle like the error line (avoid outage flood)
            _pub_log(f"[bbo redis] recreated client after {PUB_RECREATE_AFTER} consecutive failures")
            _pub_recreate_last = now
        try:
            closer = getattr(old, "aclose", None) or getattr(old, "close", None)   # aclose() (redis-py >=5) or close()
            if closer is not None:
                await asyncio.wait_for(closer(), PUB_SOCKET_TIMEOUT)   # bound it too, so recreate adds no unbounded stall
        except Exception:
            pass


# ── Frame handling / WebSocket loop ──────────────────────────────────────────
async def handle_frame(raw, book, loggers, add_ts, pub, key, state):
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"[parse error] {e} | {raw}")     # unparseable -> stdout (captured)
        return
    if not isinstance(msg, dict):               # valid JSON but not an object -> stdout, keep reading
        print(raw)
        return
    # A server subscription/request error: a channel-less frame with an `error` object or an HTTP-like
    # status >= 400 (WS 'Errors' docs). Raise -> ws_loop reconnects (resubscribe) instead of sitting on a
    # dead subscription. Gated on channel-less so a real channel frame (which carries `channel`) never matches.
    if msg.get("channel") is None and (isinstance(msg.get("error"), dict)
                                       or (isinstance(msg.get("status"), int) and msg["status"] >= 400)):
        err = msg.get("error") if isinstance(msg.get("error"), dict) else {}
        raise SubscriptionError(err.get("message") or f"status {msg.get('status')}: {err or msg}")
    channel = msg.get("channel")
    if channel == "bbo":
        # Native best-bid/offer -> Redis (no file log: high-frequency, Redis is its sink). ON-CHANGE: write
        # immediately on every bbo msg (sub-second freshness), and stamp last_pub so the ws_loop time-gate
        # heartbeat won't double-write right after. contents may be {} for an empty market -> null sides.
        # (The heartbeat covers the SPARSE-bbo case: this socket is often flooded by oraclePrices while bbo
        # rarely arrives -- measured AAPL mainnet ~69 oracle vs 1 bbo per 20s -- so the key stays warm.)
        if pub is not None and msg.get("type") in ("subscribed", "channel_data"):
            c = msg.get("contents")
            if isinstance(c, dict) and _bbo_ok(c):        # well-formed (empty market, or finite-positive bestBid/bestAsk)
                state["bbo"] = c
                await pub.publish(key, state["bbo"], time.time())
                state["last_pub"] = time.monotonic()
            # else: non-dict / garbage-field bbo -> KEEP the last-good state["bbo"] (the heartbeat republishes it),
            # never storing or writing junk. (publish() re-validates too, so a bad frame can't reach Redis.)
        return
    logger = loggers.get(channel)
    if logger is None:                          # not a subscribed channel -> stdout
        print(raw)
        return
    line = (json.dumps({"receivedAt": now_iso(), "msg": msg}, separators=(",", ":"))
            if add_ts else raw)
    _emit(logger, line)
    if channel == "l2OrderbookUpdates":
        contents = msg.get("contents") or {}
        if msg.get("type") == "subscribed":
            try:
                book.on_snapshot(contents)
            except SequenceGap as e:
                # Safety net: on_snapshot (and _apply beneath it) now raise MalformedFrame directly for every
                # snapshot-application failure; force the BACKOFF path for any residual SequenceGap too -- a
                # malformed snapshot resyncs to another malformed one, so it must NOT take the no-backoff path.
                raise MalformedFrame(str(e)) from e
        elif msg.get("type") == "channel_data":
            book.on_delta(contents)            # MalformedFrame (bad shape) -> backoff; SequenceGap (true gap) -> resync


async def ws_loop(url, subscriptions, book, loggers, add_ts, pub, key, reconnect_interval=None):
    delay = RECONNECT_BASE
    sub_errs = 0                               # consecutive subscription rejections; reset once a connection is stable
    while True:
        conn_start = None
        backoff = True                         # sleep+backoff before reconnecting, UNLESS a seq-gap (resync now)
        state = {"bbo": None, "last_pub": 0.0}  # last bbo contents + monotonic time of last publish (on-change + heartbeat gate), per-connection
        try:
            async with websockets.connect(
                url, open_timeout=OPEN_TIMEOUT,
                ping_interval=PING_INTERVAL, ping_timeout=PING_TIMEOUT,
            ) as ws:
                conn_start = time.monotonic()
                for sub in subscriptions:
                    await ws.send(json.dumps(sub))
                if pub is not None:
                    # Redis present: ON-CHANGE writes happen in handle_frame (every bbo msg). Here we add the
                    # HEARTBEAT: republish the latest bbo whenever it's been quiet >= HEARTBEAT (driven by any
                    # socket activity OR the recv timeout). Needed because the bbo channel is often sparse while
                    # this shared socket is flooded by oraclePrices, so a recv-timeout-only heartbeat starves
                    # and the key expires between bbo msgs. state["last_pub"] (reset by each on-change write)
                    # keeps this from double-writing right after one. A clean close raises ConnectionClosedOK.
                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=HEARTBEAT)
                            await handle_frame(raw, book, loggers, add_ts, pub, key, state)
                        except asyncio.TimeoutError:
                            pass
                        now = time.monotonic()
                        if state["bbo"] is not None and now - state["last_pub"] >= HEARTBEAT:
                            await pub.publish(key, state["bbo"], time.time())
                            state["last_pub"] = now
                else:
                    # No redis: the original loop, untouched (no heartbeat needed).
                    async for raw in ws:
                        await handle_frame(raw, book, loggers, add_ts, None, None, state)
            # Clean close (server ended the stream without an exception): the book
            # is now stale, so stop serving it as ready until the next snapshot.
            book.reset()
            print(f"[{log_ts()}] [ws] connection closed — resubscribing for a fresh snapshot", file=sys.stderr)
        except MalformedFrame as e:
            # Unrecoverable-by-resync: a persistently malformed snapshot OR delta (bad shape / non-list side /
            # missing seq). Reset and take the NORMAL backoff path (leave backoff=True) so we don't spin a tight
            # zero-sleep reconnect storm. MUST precede the SequenceGap handler below (MalformedFrame subclasses it).
            print(f"[{log_ts()}] [bad frame] {e} — resubscribing (with backoff)", file=sys.stderr)
            book.reset()
        except SequenceGap as e:
            print(f"[{log_ts()}] [seq gap] {e} — resubscribing for a fresh snapshot", file=sys.stderr)
            book.reset()
            backoff = False                    # immediate resync, no backoff (only a GENUINE seq gap reaches here now)
        except SubscriptionError as e:
            # Reconnectable, not fatal: resubscribe on a fresh socket with the normal backoff. Give up only
            # after SUB_ERR_LIMIT rejections in a row with no stable connection between (a genuinely bad sub).
            book.reset()
            sub_errs += 1
            if sub_errs >= SUB_ERR_LIMIT and not INFINITE_RETRY:
                raise SystemExit(f"wsorderbook: subscription rejected {sub_errs}x in a row, giving up: {e}")
            print(f"[{log_ts()}] [sub error] {e} — resubscribing (attempt {sub_errs}, with backoff)", file=sys.stderr)
        except websockets.ConnectionClosedOK:
            # Redis branch only: a clean server close surfaces as an exception (unlike async-for). Treat
            # it exactly like the clean-close path above (reset + normal backoff), NOT as an error.
            book.reset()
            print(f"[{log_ts()}] [ws] connection closed — resubscribing for a fresh snapshot", file=sys.stderr)
        except Exception as e:
            print(f"[{log_ts()}] [ws error] {describe_error(e)} — reconnecting", file=sys.stderr)   # actual delay set after the stability reset below
            book.reset()
        if conn_start is not None and time.monotonic() - conn_start >= STABLE_AFTER:
            sub_errs = 0                        # the connection proved stable => subscriptions are fine
        # Sleep/backoff for a clean close OR an error (NOT a seq-gap). Previously a CLEAN close skipped
        # every except and busy-looped at 0 delay; and resetting delay on connect defeated backoff on a
        # flap. Now: reset delay only if the connection proved STABLE (>= STABLE_AFTER s), then sleep.
        if backoff:
            # Default: exponential backoff (full jitter, doubling to RECONNECT_MAX, reset on a stable drop).
            # With --reconnect-interval: immediate on a genuine drop, else a flat ~interval wait -- so a
            # synchronized mass-disconnect of a large fleet stays under the per-IP new-conns/min cap instead
            # of storming (see plan_reconnect_sleep). Seq-gap resync above still skips this (backoff=False).
            sleep_s, delay = plan_reconnect_sleep(
                conn_start, time.monotonic(), delay, RECONNECT_BASE, RECONNECT_MAX, STABLE_AFTER,
                reconnect_interval)
            await asyncio.sleep(sleep_s)


# ── Market lookup ────────────────────────────────────────────────────────────
async def resolve_market(market: str):
    """Resolve a market (numeric id OR case-insensitive name) -> (marketId, displayName).

    Returns the CANONICAL display name -- it's reused as the WebSocket subscription
    `id` and in log filenames, where the server expects the name, not the id.
    """
    data = read_markets_cache(MARKETS_CACHE)
    if data is None:                        # cache miss -> live fetch, then warm the cache
        timeout = aiohttp.ClientTimeout(total=10)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(MARKETS_URL) as resp:
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
        except aiohttp.ClientError as e:
            raise SystemExit(f"wsorderbook: error fetching markets: {e}")
        except asyncio.TimeoutError:
            raise SystemExit(f"wsorderbook: timed out fetching {MARKETS_URL}")
        except json.JSONDecodeError as e:
            raise SystemExit(f"wsorderbook: invalid JSON from markets API: {e}")
        # Validate BEFORE caching/using (order: parse -> validate -> cache -> use): never write a
        # malformed body into the shared markets cache, and don't let a non-dict reach .get below.
        # (Cache hits are already safe -- read_markets_cache returns only a dict with a list 'markets'.)
        data = require_dict(data, "markets", "wsorderbook")
        if not isinstance(data.get("markets"), list):
            raise SystemExit("wsorderbook: unexpected markets response shape (no 'markets' list)")
        write_markets_cache(MARKETS_CACHE, data)
    numeric = market.isdigit() and market.isascii()   # isascii guard: exotic Unicode digits (², ⑤) pass isdigit() but int() raises (matches showorderbook; MARKET_RE already blocks them, so this is defense-in-depth)
    up = market.upper()
    for m in data.get("markets", []):
        if not isinstance(m, dict):
            continue
        try:
            mid = int(m["marketId"])
        except (KeyError, ValueError, TypeError):
            continue                        # skip a malformed entry rather than crash on int() (matches showorderbook)
        if (numeric and mid == int(market)) or \
           (not numeric and str(m.get("marketDisplayName", "")).upper() == up):
            return mid, str(m.get("marketDisplayName", ""))
    raise SystemExit(f"wsorderbook: market '{market}' not found.")


# ── Entry point ──────────────────────────────────────────────────────────────
async def amain(args):
    if not MARKET_RE.match(args.market):
        raise SystemExit(f"wsorderbook: invalid market {args.market!r} "
                         f"(allowed: letters, digits, . _ -).")

    try:
        os.makedirs(args.log_dir, exist_ok=True)
    except OSError as e:
        raise SystemExit(f"wsorderbook: cannot create log dir {args.log_dir!r}: {e}")

    print(f"[{args.market}] Resolving market …")
    market_id, market = await resolve_market(args.market)   # canonical display name
    # A marketId outside the per-network port band would make PORT_BASE[net]+marketId land in ANOTHER network's
    # band (mainnet 1000 -> 11000 = testnet's base) -- a reader aimed at that port would silently read a DIFFERENT
    # network's book -- or, for a very large id, exceed 65535. Refuse rather than cross bands (mirrors the reader).
    if not (0 <= market_id <= MAX_MARKET_ID):
        raise SystemExit(f"wsorderbook: marketId {market_id} is outside the per-network port band "
                         f"[0, {MAX_MARKET_ID}] -- its HTTP port would collide with another network's range "
                         f"(PORT_BASE[{args.network}]={PORT_BASE[args.network]}).")
    port = PORT_BASE[args.network] + market_id
    print(f"[{market}] marketId={market_id} ({args.network})  →  HTTP port {port}")

    # One log per subscribed channel; unknown/malformed frames go to stdout.
    loggers = {
        "l2OrderbookUpdates": setup_logger(f"wsob.l2.{market}", f"{args.log_dir}/wsorderbook{market}.log", args.max_bytes, args.log_backups),
        "trades":             setup_logger(f"wsob.trades.{market}", f"{args.log_dir}/wstrades{market}.log", args.max_bytes, args.log_backups),
        "oraclePrices":       setup_logger(f"wsob.oracle.{market}", f"{args.log_dir}/oraclePrices{market}.log", args.max_bytes, args.log_backups),
    }

    book = OrderBook()
    subscriptions = [
        {"type": "subscribe", "channel": "l2OrderbookUpdates", "id": market, "nLevels": 100, "snapshot": True},
        {"type": "subscribe", "channel": "trades", "id": market, "snapshot": True},
        {"type": "subscribe", "channel": "oraclePrices", "id": market, "snapshot": True},
    ]
    # Optional BBO → Redis publisher. redis-py is an OPTIONAL dependency: if it's not installed we skip
    # the bbo subscription entirely and this tool behaves exactly as before (HTTP orderbook only).
    pub = BboPublisher(REDIS_URL) if aioredis is not None else None
    key = None
    if pub is not None:
        key = BBO_KEY_FMT.format(network=args.network, market=market)
        subscriptions.append({"type": "subscribe", "channel": "bbo", "id": market})
        print(f"[{market}] publishing BBO → redis '{key}' (TTL {BBO_TTL}s, heartbeat {HEARTBEAT}s)")
    asyncio.create_task(ws_loop(args.url, subscriptions, book, loggers, args.timestamp, pub, key, args.reconnect_interval))

    app = web.Application()
    app["book"] = book
    app.router.add_get("/orderbook", handle_orderbook)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, args.host, port)
    await site.start()
    print(f"[{market}] Serving orderbook at http://{args.host}:{port}/orderbook")

    await asyncio.Event().wait()


def main():
    global BASE_URL, MARKETS_URL, WS_URL, MARKETS_CACHE, INFINITE_RETRY
    parser = argparse.ArgumentParser(description="Serve a market's L2 order book over HTTP.")
    parser.add_argument("market", help="market display name, e.g. BTC-USD")
    parser.add_argument("--host", default="127.0.0.1",
                        help="HTTP bind host (default 127.0.0.1; use 0.0.0.0 to expose)")
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
    INFINITE_RETRY = args.infinite_retry
    BASE_URL = NETWORKS[args.network]
    MARKETS_URL = f"{BASE_URL}/v1/markets"
    WS_URL = ws_url(args.network)
    MARKETS_CACHE = markets_cache_path(args.network)
    if args.url is None:
        args.url = WS_URL
    if args.log_dir is None:
        args.log_dir = os.path.join(LOG_BASE, args.network)
    write_pidfile(args.log_dir, f"wsorderbook-{args.market}")   # liveness marker: monitor checks the PID is alive
    try:
        asyncio.run(amain(args))
    except KeyboardInterrupt:
        print(f"\n[{args.market}] stopped")


if __name__ == "__main__":
    main()
