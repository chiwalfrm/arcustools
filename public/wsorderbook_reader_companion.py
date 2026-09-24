#!/usr/bin/env python3
"""wsorderbook_reader_companion.py -- serve ONE market's full L2 book over HTTP by reading it from Redis
(where wsorderbook_reader.py publishes it). Holds NO WebSocket connection, so it does NOT count against the
per-IP concurrent-connection cap -- that's the point of the reader/companion split.

  wsorderbook_reader_companion.py BTC-USD --testnet
  wsorderbook_reader_companion.py BTC-USD --testnet --host 0.0.0.0

Serves the SAME port (PORT_BASE[net] + marketId) and the SAME payload/status as wsorderbook.py --
`{"ready", "bids", "asks"}`, 200 when ready else 503 -- so existing `curl` consumers and the market maker's
--use-ws-orderbook keep working unchanged. Run one per port, exactly like wsorderbook.py today. Reuses
wsorderbook.py for market resolution + the port scheme. Requires the `redis` package (its data source).
"""
import argparse
import asyncio
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))   # find sibling wsorderbook.py

from aiohttp import web

import wsorderbook as wsob
from arcus_common_public import (NETWORKS, PUB_SOCKET_TIMEOUT, REDIS_URL, describe_error,
                                 markets_cache_path, positive_int, write_pidfile)

try:
    import redis.asyncio as aioredis
except ImportError:
    aioredis = None

PROG = "wsorderbook_reader_companion"
BOOK_KEY_FMT   = "arcus:{network}:book:{market}"   # must match wsorderbook_reader.BOOK_KEY_FMT
DEFAULT_MAX_AGE = 5      # s; serve 503 if the book key is older than this (reader stalled). ~ the reader's BOOK_TTL


def _not_ready(reason, status=503):
    """The exact not-ready shape wsorderbook serves (ready:false, empty sides), plus a `reason` for humans."""
    return web.json_response({"ready": False, "bids": [], "asks": [], "reason": reason}, status=status)


async def handle_orderbook(request):
    app = request.app
    try:
        raw = await app["redis"].get(app["book_key"])
    except Exception as e:                         # redis down/slow -> 503, never a 500
        return _not_ready(f"redis error: {describe_error(e)}")
    if raw is None:                                # key absent/expired -> reader is down or this market isn't warm yet
        return _not_ready("no book in redis (reader down or market not warm)")
    try:
        blob = json.loads(raw)                     # json.loads accepts bytes
    except (json.JSONDecodeError, TypeError, ValueError):
        return _not_ready("unparseable book blob in redis")
    if not isinstance(blob, dict):
        return _not_ready("book blob is not an object")
    ts = blob.get("ts")
    fresh = isinstance(ts, (int, float)) and (time.time() - ts) <= app["max_age"]
    if not fresh:                                  # present but stale (flusher stalled) -> 503 with the last-known book
        return web.json_response({"ready": False, "bids": blob.get("bids", []), "asks": blob.get("asks", []),
                                  "reason": "book is stale (reader not flushing)"}, status=503)
    ready = bool(blob.get("ready"))
    payload = {"ready": ready, "bids": blob.get("bids", []), "asks": blob.get("asks", [])}
    return web.json_response(payload, status=200 if ready else 503)


async def amain(args):
    wsob.MARKETS_CACHE = markets_cache_path(args.network)   # resolve_market reads these module globals
    wsob.MARKETS_URL = f"{NETWORKS[args.network]}/v1/markets"
    if not wsob.MARKET_RE.match(args.market):
        raise SystemExit(f"{PROG}: invalid market {args.market!r} (allowed: letters, digits, . _ -).")

    print(f"[{args.market}] Resolving market …")
    market_id, market = await wsob.resolve_market(args.market)   # canonical display name
    if not (0 <= market_id <= wsob.MAX_MARKET_ID):
        raise SystemExit(f"{PROG}: marketId {market_id} is outside the per-network port band "
                         f"[0, {wsob.MAX_MARKET_ID}] (PORT_BASE[{args.network}]={wsob.PORT_BASE[args.network]}).")
    port = wsob.PORT_BASE[args.network] + market_id

    r = aioredis.from_url(args.redis_url, socket_timeout=PUB_SOCKET_TIMEOUT,
                          socket_connect_timeout=PUB_SOCKET_TIMEOUT)
    try:
        await r.ping()                             # fail fast if the data source is unreachable at startup
    except Exception as e:
        raise SystemExit(f"{PROG}: Redis unreachable at {args.redis_url}: {describe_error(e)}")

    key = BOOK_KEY_FMT.format(network=args.network, market=market)
    app = web.Application()
    app["redis"] = r
    app["book_key"] = key
    app["max_age"] = args.max_age
    app.router.add_get("/orderbook", handle_orderbook)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, args.host, port)
    await site.start()
    print(f"[{market}] marketId={market_id} ({args.network})  →  HTTP port {port}")
    print(f"[{market}] serving orderbook (from redis '{key}', max-age {args.max_age}s) "
          f"at http://{args.host}:{port}/orderbook")
    await asyncio.Event().wait()


def main():
    p = argparse.ArgumentParser(description="Serve one market's L2 order book over HTTP from Redis "
                                            "(the wsorderbook_reader companion -- no WebSocket).")
    p.add_argument("market", help="market display name, e.g. BTC-USD")
    p.add_argument("--host", default="127.0.0.1",
                   help="HTTP bind host (default 127.0.0.1; use 0.0.0.0 to expose)")
    p.add_argument("--redis-url", default=None, help=f"Redis URL to read from (default: {REDIS_URL})")
    p.add_argument("--max-age", type=positive_int, default=DEFAULT_MAX_AGE,
                   help=f"serve 503 if the book key is older than this many seconds (default {DEFAULT_MAX_AGE})")
    p.add_argument("--log-dir", default=None, help="dir for the liveness-marker pids/ subdir (default: /mnt/arcuslogs/<network>)")
    net = p.add_mutually_exclusive_group(required=True)
    net.add_argument("--testnet", dest="network", action="store_const", const="testnet", help="use testnet")
    net.add_argument("--staging", dest="network", action="store_const", const="staging", help="use staging")
    net.add_argument("--mainnet", dest="network", action="store_const", const="mainnet", help="use mainnet")
    args = p.parse_args()

    if aioredis is None:
        raise SystemExit(f"{PROG}: requires the 'redis' package -- its data source is the reader's Redis keys.")
    args.redis_url = args.redis_url or REDIS_URL
    args.log_dir = args.log_dir or os.path.join(wsob.LOG_BASE, args.network)
    try:
        os.makedirs(args.log_dir, exist_ok=True)
    except OSError as e:
        raise SystemExit(f"{PROG}: cannot create log dir {args.log_dir!r}: {e}")
    write_pidfile(args.log_dir, f"{PROG}-{args.market}")   # liveness marker
    try:
        asyncio.run(amain(args))
    except KeyboardInterrupt:
        print(f"\n[{args.market}] stopped")


if __name__ == "__main__":
    main()
