#!/usr/bin/env python3
"""All Redis support for the Arcus toolkit -- the shared self-healing client PLUS the two caches, in
ONE module so "Redis" is one obvious place (merged from the former account_cache + marketcache).

  python3 arcus_redis.py BTC-USD --testnet            # inspect a market-cache entry
  python3 arcus_redis.py BTC-USD --testnet --refresh  # force a re-fetch

Three sections below:
  - CLIENT       -- one self-healing redis client, memoized PER socket_timeout (the MM/poller
                    account reads tolerate a 2s stall before the self-heal; a market lookup fails
                    fast at 0.5s to its API fallback), retried after a cooldown when Redis is down.
  - ACCOUNT CACHE-- short-TTL cache for ACCOUNT-WIDE / exchange-wide REST reads, so a fleet of
                    per-market market_maker bots doesn't each re-fetch the same data every loop.
  - MARKET CACHE -- Redis-backed cache of STATIC market metadata (marketId/tickSize/stepSize).

stdlib + redis, plus the canonical NETWORKS map imported from arcus_common_private (its logical home
-- a light import: ordersign is just Ed25519 via `cryptography`, no heavy SDK).
"""
import json
import os
import re
import time
import urllib.error
import urllib.request
from decimal import Decimal, InvalidOperation

from arcus_common_private import NETWORKS   # the toolkit's canonical API-base map, in its logical home (the network module)

try:
    # ONLY connectivity failures invalidate the memoized client (see _invalidate); a server-side ResponseError
    # (WRONGTYPE / OOM) is bad state, not a dead client, so it must fall through to `except Exception` uninvalidated.
    from redis.exceptions import ConnectionError as _RedisConnError, TimeoutError as _RedisTimeoutError
    _CONN_ERRORS = (_RedisConnError, _RedisTimeoutError)
except Exception:                      # redis-py absent -> no client is ever built, so this clause never matches
    _CONN_ERRORS = ()


# ── CLIENT (shared, self-healing) ─────────────────────────────────────────────
REDIS_URL = os.environ.get("ARCUS_REDIS_URL", "redis://127.0.0.1:6379/0")
ACCOUNT_SOCKET_TIMEOUT = 2.0   # s; account reads -- the SYNC MM/poller loop tolerates a 2s stall on a wedged socket
MARKET_SOCKET_TIMEOUT = 0.5    # s; market lookups fail fast to the direct-API fallback
_RETRY_COOLDOWN = 5.0          # s; retry a DOWN Redis at most this often
_CLIENTS = {}     # socket_timeout -> client (on success) | None (last connect failed) | absent (never tried)
_LAST_FAIL = {}   # socket_timeout -> monotonic time of the last failed connect


def _redis(client=None, socket_timeout=ACCOUNT_SOCKET_TIMEOUT):
    """A redis client, or None if redis-py is absent / the server is unreachable. Memoized PER
    `socket_timeout` (so the 2s account client and the 0.5s market client coexist, each pooled). A
    SUCCESSFUL client is memoized (redis-py auto-reconnects it through a blip); a FAILED connect is
    NOT memoized permanently -- it is retried every _RETRY_COOLDOWN s, so a process that started while
    Redis was briefly down (or whose Redis restarts mid-run) re-establishes the cache instead of
    falling back to live fetches for its whole lifetime."""
    if client is not None:
        return client
    memo = _CLIENTS.get(socket_timeout, "<unset>")
    if memo != "<unset>" and memo is not None:
        return memo
    if memo is None and (time.monotonic() - _LAST_FAIL.get(socket_timeout, 0.0)) < _RETRY_COOLDOWN:
        return None                                    # recently failed -> back off before retrying
    try:
        import redis
        c = redis.Redis.from_url(REDIS_URL, socket_timeout=socket_timeout, decode_responses=True)
        c.ping()
        _CLIENTS[socket_timeout] = c
    except Exception:
        _CLIENTS[socket_timeout] = None
        _LAST_FAIL[socket_timeout] = time.monotonic()
    return _CLIENTS.get(socket_timeout)


def _invalidate(passed_client, socket_timeout=ACCOUNT_SOCKET_TIMEOUT):
    """Drop the process-memoized client (for this socket_timeout) after a redis CONNECTIVITY failure
    (timeout / connection error) so the NEXT call re-connects through the _RETRY_COOLDOWN backoff
    instead of re-blocking up to socket_timeout on a wedged client on EVERY call (a hung Redis would
    otherwise stall the SYNC MM/poller loop ~socket_timeout per read for the whole outage). No-op when
    the caller passed its own client (we don't own it) or nothing is memoized. A PARSE / ResponseError
    must NOT come here -- that is bad data in a healthy Redis, not a dead client."""
    if passed_client is None and _CLIENTS.get(socket_timeout) not in (None, "<unset>"):
        _CLIENTS[socket_timeout] = None
        _LAST_FAIL[socket_timeout] = time.monotonic()


# ── ACCOUNT CACHE ─────────────────────────────────────────────────────────────
# Cache-aside, NO locks: a bot calls cached_get(...); on a hit it uses the cached blob, on a miss it
# fetches live and writes it back with a short TTL. Concurrent writes are harmless -- Redis is
# single-threaded so it serializes them, and the racing values are near-identical fresh snapshots
# (last-write-wins). An optional account_poller.py can keep the keys warm with a LONGER TTL than its
# poll interval, so the bots always hit and never fetch. If Redis is down, cached_get just calls the
# live fetch. Keys: arcus:<network>:acct:<address>:<name> (per-address) or arcus:<network>:<name>.
def _acct_key(network, address, name):
    # The address is LOWERCASED in the key so it's casing-independent: a bot reading with its
    # checksummed (EIP-55) creds address and a writer (account_poller / wsaccount) that may pass a
    # different casing still land on the SAME key. Ethereum addresses are case-insensitive, so this
    # is a safe canonical form. ONLY the key is normalized -- the address used for API calls and
    # signing is untouched. Any writer of this key family MUST lowercase identically (see wsaccount.py).
    return f"arcus:{network}:acct:{address.lower()}:{name}" if address else f"arcus:{network}:{name}"


def read(network, address, name, redis_client=None):
    """Cached blob for (network, address, name), or None on miss / Redis down / parse error."""
    client = _redis(redis_client)
    if not client:
        return None
    try:
        raw = client.get(_acct_key(network, address, name))
        return json.loads(raw) if raw else None
    except _CONN_ERRORS:
        _invalidate(redis_client)
        return None
    except Exception:
        return None


def read_bbo(network, market, redis_client=None):
    """Parsed BBO blob published by wsorderbook (key arcus:<network>:bbo:<market>), or None on
    miss / Redis down / parse error. A separate key family from the account cache (it's a WS-fed
    feed, not a REST-read cache) but shares this module's redis client + graceful fallthrough. The
    caller owns the freshness policy (age-guard on the blob's `ts`)."""
    client = _redis(redis_client)
    if not client:
        return None
    try:
        raw = client.get(f"arcus:{network}:bbo:{market}")
        return json.loads(raw) if raw else None
    except _CONN_ERRORS:
        _invalidate(redis_client)
        return None
    except Exception:
        return None


def ttl(network, address, name, redis_client=None):
    """Remaining TTL (seconds) for the key: >=0 live, -2 missing/expired, -1 set-without-expiry,
    or None if Redis is unavailable -- so a caller can tell 'key expired' (-2) apart from
    'Redis down' (None). Used by account_poller to measure expire-before-refresh gaps."""
    client = _redis(redis_client)
    if not client:
        return None
    try:
        return client.ttl(_acct_key(network, address, name))
    except _CONN_ERRORS:
        _invalidate(redis_client)
        return None
    except Exception:
        return None


def write(network, address, name, data, ttl, redis_client=None):
    """Cache `data` (a JSON-able blob) under the key with TTL `ttl` seconds. Best-effort:
    returns True if written, False if Redis is down / the write failed. Single-key SET, so the
    write is atomic -- a concurrent reader sees either the whole old or whole new value."""
    client = _redis(redis_client)
    if not client:
        return False
    try:
        client.set(_acct_key(network, address, name), json.dumps(data), ex=max(1, int(ttl)))
        return True
    except _CONN_ERRORS:
        _invalidate(redis_client)
        return False
    except Exception:
        return False


REQUIRED_KEYS = {"openOrders": "orders", "positions": "positions", "account": "freeCollateral", "markets": "markets"}
# The required key's VALUE must also have the right SHAPE, not merely EXIST: a cached {"markets": null}
# or {"markets": "bad"} satisfies "key present" yet crashes a reader that iterates it (resolve_market
# does `for m in markets`). Per-key value predicate (checked in addition to key-presence):
def _finite_number(v):
    """True iff v parses as a FINITE Decimal. Rejects null/bool/list/dict and unparseable strings, AND
    -- critically -- Infinity/NaN, which parse as valid Decimals but must never be cached as collateral
    (Decimal('inf')/'NaN' slip past a plain type check and break the reader's `fc < min_collateral` guard)."""
    if isinstance(v, bool):                 # bool is an int subclass; not a collateral figure
        return False
    try:
        return Decimal(str(v)).is_finite()
    except (InvalidOperation, ValueError, TypeError):
        return False


def _finite_positive(v):
    """True iff v parses as a finite Decimal > 0 -- a USABLE tick/step increment. A zero/negative/inf/NaN
    increment reaches the market maker as DivisionByZero or negative ticks/quantums into signing."""
    return _finite_number(v) and Decimal(str(v)) > 0


_CACHE_VALUE_OK = {
    "openOrders": lambda v: isinstance(v, list) and all(isinstance(x, dict) for x in v),  # list OF order dicts
    # each per-market value must be a POSITION OBJECT: a dict with a FINITE-number 'size'. A falsey non-dict
    # (e.g. []) or a dict without 'size' is read as FLAT by position() -> fail-OPEN on a real position; and a
    # present-but-unusable size (null/bool/"NaN"/"abc") is worth refusing too -- symmetry with 'account' below
    # (don't warm Redis with a body the risk path can only read as position-unknown). {} (no markets) stays
    # cacheable = a genuinely flat universe. The reader (position()) still fails closed on a bad size regardless.
    "positions":  lambda v: isinstance(v, dict) and all(
        isinstance(p, dict) and "size" in p and _finite_number(p.get("size")) for p in v.values()),
    "account":    _finite_number,                                               # freeCollateral: finite number only
    # A markets response is CACHEABLE when it's a non-empty LIST OF DICTS. We deliberately DON'T require every
    # row's tickSize/stepSize to be finite-positive here: one bad venue row (e.g. a new market shipped with a
    # zero increment) would otherwise make the WHOLE shared blob uncacheable AND fail fetch_startup_markets, so
    # EVERY bot in the fleet refuses to start. Per-market tick/step is rejected where it's actually USED -- at the
    # SELECTED market only: MarketMaker.__init__ and get_market/_valid_entry reject a bad increment for the market
    # a tool actually trades, and wsexchange's warmer drops bad rows from what it writes. So a bad row for market
    # X stops only the ONE bot trading X (with a clear error), never the whole fleet.
    "markets":    lambda v: isinstance(v, list) and bool(v) and all(isinstance(x, dict) for x in v),
}


def is_cacheable(name, data):
    """Whether `data` is worth caching under `name` -- the SINGLE source of truth shared by cached_get
    (bots' cache-aside writes) and account_poller (the warmer), so both writers apply identical rules.
    A KNOWN account name must be a dict that contains its required top-level key AND whose value has the
    right SHAPE (per _CACHE_VALUE_OK): a malformed 2xx body -- missing the key ({} / {"error":...}) OR
    the key present with a bad value ({"markets": null / "bad"}) -- must NOT be cached, or a reader
    mistakes it for a valid response (positions {} read as 'flat' on an OPEN position -> fail-OPEN; a
    non-list markets crashes startup resolution). An UNKNOWN name is cached as-is (generic blob cache)."""
    required = REQUIRED_KEYS.get(name)
    if required is None:
        return True                     # unknown name -> generic: cache whatever the caller fetched
    if not (isinstance(data, dict) and required in data):
        return False
    check = _CACHE_VALUE_OK.get(name)
    return check(data[required]) if check else True


def cached_get(network, address, name, fetch_fn, ttl, redis_client=None):
    """Cache-aside read: return the cached blob if present, else fetch_fn() and cache it with `ttl`.
    Redis down / any cache error -> fetch_fn() directly (no caching). No locking: a simultaneous miss
    in several callers just means a few redundant fetches + last-write-wins. The fetched body is
    ALWAYS returned to the caller, but only WRITTEN back if is_cacheable() -- so a bot never poisons
    the shared cache with a malformed body the poller would refuse (both writers share one bar). The
    READ side applies the same bar: a resident bad-shape body (e.g. written by a pre-shape-check
    writer, or set out-of-band) is treated as a MISS and re-fetched/re-validated, so the cache
    self-heals instead of serving junk on hits (the write/read bars stay symmetric)."""
    cached = read(network, address, name, redis_client)
    if cached is not None and is_cacheable(name, cached):
        return cached
    data = fetch_fn()
    if is_cacheable(name, data):
        write(network, address, name, data, ttl, redis_client)
    return data


# ── MARKET CACHE ──────────────────────────────────────────────────────────────
# Redis-backed cache of STATIC market metadata (marketId, tickSize, stepSize). These rarely change,
# so caching avoids a GET /v1/markets on every invocation (worthwhile for loops, e.g. a market-maker
# calling modify_order repeatedly). One API call populates the cache for ALL markets at once. Keys:
# arcus:<network>:market:<sanitized display name> AND arcus:<network>:market:<marketId> (dual-indexed).
# Live fields (markPrice/oraclePrice) are deliberately NOT cached -- read those fresh. Falls back to a
# direct API call whenever Redis is unavailable. NETWORKS is imported from arcus_common_private (its
# logical home); _markets_url uses it below.
MARKET_TTL = 3600          # 1 hour; tick/step are static but re-verify hourly
_KEY_UNSAFE = re.compile(r"[^A-Za-z0-9._:-]")


class MarketCacheError(Exception):
    """Network/API/parse failure while fetching market metadata."""


class MarketNotFound(MarketCacheError):
    """The requested market is not in the venue's universe."""


def _markets_url(network):
    if network not in NETWORKS:
        raise MarketCacheError(f"unknown network {network!r}; expected one of {sorted(NETWORKS)}.")
    return NETWORKS[network] + "/v1/markets"


def _market_key(display_name, network):
    """Redis key for a market, namespaced by network, LOWERCASED + sanitized so a lookup is
    case-insensitive (a caller passing 'btc-usd' hits the same key as 'BTC-USD') and odd characters
    can't make messy keys (any char outside [A-Za-z0-9._:-] becomes '_', so 'BTC-USD' -> the clean key
    arcus:testnet:market:btc-usd). Numeric-id keys are unaffected by .lower()."""
    return f"arcus:{network}:market:" + _KEY_UNSAFE.sub("_", str(display_name).lower())


def _valid_entry(e):
    """A cached/fetched entry needs a numeric id, a name, and FINITE POSITIVE tick/step. tick/step must
    parse as a positive finite Decimal -- consumers (e.g. modify_order via to_ticks/to_quantums) do
    Decimal(tickSize)/Decimal(stepSize), so a present-but-non-numeric or <=0/inf/NaN value would else
    crash them (InvalidOperation) AND, having passed here, never self-heal until TTL. Mirrors dydx
    _normalize_entry's tickSize validation."""
    return (isinstance(e, dict)
            and isinstance(e.get("marketId"), int)
            and isinstance(e.get("marketDisplayName"), str) and e["marketDisplayName"]
            and isinstance(e.get("tickSize"), str) and e["tickSize"]
            and isinstance(e.get("stepSize"), str) and e["stepSize"]
            and _finite_positive(e["tickSize"]) and _finite_positive(e["stepSize"]))


def _identity_match(entry, ident):
    """True iff a (shape-valid) cached entry's IDENTITY matches the requested ident: a numeric
    ident must equal marketId, otherwise (case-insensitive) the marketDisplayName. Guards against a
    cache hit on key X returning a well-shaped entry for a DIFFERENT market (corrupt/poisoned key) --
    which would otherwise feed the wrong tick/step/marketId into trading."""
    ident = str(ident)
    if ident.isdigit():
        return entry.get("marketId") == int(ident)
    return str(entry.get("marketDisplayName", "")).upper() == ident.upper()


def _fetch_all(network):
    """GET /v1/markets (for `network`) -> list of market dicts, or raise MarketCacheError."""
    markets_url = _markets_url(network)
    try:
        with urllib.request.urlopen(markets_url, timeout=10) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raise MarketCacheError(f"HTTP {e.code} fetching markets")
    except urllib.error.URLError as e:
        raise MarketCacheError(f"could not reach {markets_url}: {e.reason}")
    except (TimeoutError, OSError) as e:
        raise MarketCacheError(f"network error: {e}")
    except json.JSONDecodeError as e:
        raise MarketCacheError(f"invalid JSON from markets API: {e}")
    if not isinstance(data, dict):
        raise MarketCacheError("unexpected markets response shape (not a JSON object)")
    markets = data.get("markets")
    if not isinstance(markets, list):
        raise MarketCacheError("unexpected markets response shape (no 'markets' list)")
    return markets


def get_market(display_name, network, refresh=False, redis_client=None):
    """Return {marketId:int, marketDisplayName:str, tickSize:str, stepSize:str}.

    `network` (testnet/staging/mainnet) selects both the /v1/markets server and the Redis
    key namespace, so testnet and staging entries never collide in one Redis db.
    Checks Redis first (unless refresh=True) and only trusts a cache entry that
    passes shape validation; otherwise fetches GET /v1/markets, caches every
    market, and returns the requested one. Raises MarketNotFound for an unknown
    market, MarketCacheError for fetch/parse failures. Works without Redis.
    """
    r = redis_client if redis_client is not None else _redis(socket_timeout=MARKET_SOCKET_TIMEOUT)
    if r is not None and not refresh:
        try:
            cached = r.get(_market_key(display_name, network))
        except _CONN_ERRORS:                        # wedged/unresponsive client -> drop it (NEXT call backs off)
            _invalidate(redis_client, socket_timeout=MARKET_SOCKET_TIMEOUT)
            r = None                                # and stop using it THIS call too (skip the write-back below)
            cached = None
        except Exception:
            cached = None
        if cached:
            try:
                entry = json.loads(cached)
            except (ValueError, TypeError):
                entry = None
            if _valid_entry(entry) and _identity_match(entry, display_name):
                return entry
            # corrupt/stale/missing fields OR identity mismatch -> ignore and refetch

    # Match by numeric marketId or case-insensitive display name.
    numeric = str(display_name).isdigit()
    want_id = int(display_name) if numeric else None
    want_name = str(display_name).upper()

    found = None
    for m in _fetch_all(network):
        try:
            entry = {"marketId": int(m["marketId"]),
                     "marketDisplayName": str(m["marketDisplayName"]),
                     "tickSize": str(m["tickSize"]),
                     "stepSize": str(m["stepSize"])}
        except (KeyError, ValueError, TypeError):
            continue
        if not _valid_entry(entry):
            continue
        if r is not None:
            # Cache under BOTH the display name AND the numeric id, so a numeric
            # get_market("1") hits Redis instead of triggering a /v1/markets fetch.
            payload = json.dumps(entry)
            try:
                r.set(_market_key(m.get("marketDisplayName", ""), network), payload, ex=MARKET_TTL)
                r.set(_market_key(str(entry["marketId"]), network), payload, ex=MARKET_TTL)
            except _CONN_ERRORS:                    # drop the wedged client AND stop hammering it for the rest of this fetch
                _invalidate(redis_client, socket_timeout=MARKET_SOCKET_TIMEOUT)
                r = None
            except Exception:
                pass
        if (numeric and entry["marketId"] == want_id) or \
           (not numeric and str(m.get("marketDisplayName", "")).upper() == want_name):
            found = entry
    if found is None:
        raise MarketNotFound(f"market {display_name!r} not found")
    return found


# ── CLI (market-cache inspector) ──────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Inspect the Redis market-metadata cache.")
    p.add_argument("market", help="market display name, e.g. BTC-USD")
    p.add_argument("--refresh", action="store_true", help="force re-fetch from the API")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--testnet", dest="network", action="store_const", const="testnet",
                   help="use the testnet server + arcus:testnet:market: keys")
    g.add_argument("--staging", dest="network", action="store_const", const="staging",
                   help="use the staging server + arcus:staging:market: keys")
    g.add_argument("--mainnet", dest="network", action="store_const", const="mainnet",
                   help="use the mainnet server + arcus:mainnet:market: keys")
    a = p.parse_args()
    r = _redis(socket_timeout=MARKET_SOCKET_TIMEOUT)
    pre_cached = bool(r and r.exists(_market_key(a.market, a.network))) and not a.refresh
    try:
        entry = get_market(a.market, a.network, refresh=a.refresh)
    except MarketCacheError as e:
        raise SystemExit(f"arcus_redis: {e}")
    print(f"{a.market} [{a.network}]: {entry}")
    print(f"  redis: {'connected' if r else 'UNAVAILABLE (used API)'}; "
          f"source: {'cache hit' if pre_cached else 'API (now cached)'}")
