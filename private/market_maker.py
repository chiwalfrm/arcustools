"""
market_maker.py -- simple two-sided POST-ONLY quoter loop for Arcus.

  python3 market_maker.py 500 0.03                      # BTC-USD, $500/side, +/-3%
  python3 market_maker.py 500 0.03 --market ETH-USD --interval 15
  python3 market_maker.py 500 0.03 --max-position 0.05 --min-collateral 1000

Each cycle (every --interval seconds, default 15):
  sort book; mid = (best bid + best ask) / 2
  bid = mid*(1-spread) rounded DOWN to tick; ask = mid*(1+spread) rounded UP
  qty = usd / price rounded DOWN to step, per side
Quotes are placed POST-ONLY (ALO) so they can never take liquidity, with a local
passive check as a backstop. Stable clientIds (mm-<market>-b/-a); each cycle it
MODIFIES the live quote, PLACES it if missing, or CANCELS it if a guard disables
that side. Cancels its quotes on exit.

Risk guards (optional):
  --max-position N    stop quoting the side that would grow |position| past N (base units).
                      ALSO enables INVENTORY SKEW: once |position| >= 50% of N, the REDUCING side
                      quotes at 2x <usd> (the growing side stays <usd>) to mean-revert toward flat
                      faster -- so with --max-position set, ONE side can rest up to 2*<usd> notional
                      (still bounded by N).
  --min-collateral C  stop quoting (and pull quotes) when freeCollateral < C (USD)

Persistent in-process loop: creds, signer, and market metadata are loaded ONCE.
Resolves ordersign.py / arcus_creds_<network>.json relative to this script.
"""

import argparse
import json
import math
import os
import random
import signal
import sys
import time
import urllib.parse
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

import arcus_redis as account_cache
import ordersign
from ordersign import Signer
from arcus_common_private import (add_network_args, check_order_response, clock_delta_ns, describe_error,
                          load_creds, positive_decimal, rate_limit_details, request, resolve_market,
                          select_network, server_clock_shim, write_pidfile)

LOG_BASE = "/mnt/arcuslogs"   # /<network> appended for the liveness-marker pids/ dir (mirrors the WS tools);
                              # --log-dir overrides. Same place ws_liveness_check.py scans, so the MM is covered.


class RateLimited(Exception):
    """A read or write hit an HTTP 429 (the Arcus app limiter OR Cloudflare's edge 1015).

    This is deliberately NOT a pricing/data failure: the venue is throttling us, the book
    hasn't gone stale, and our resting quotes are POST-ONLY/ALO 365-day GTT orders that stay
    passive. So the fail-closed pull is the WRONG response here -- pulling fires an uncached
    openOrders re-read plus two cancels, i.e. 3 MORE throttled requests, which deepens the
    throttle (the observed 429 storm). Instead we LEAVE the resting quotes in place and back
    off for `retry_after` seconds (approved 2026-07-27). Carries the backoff so run() can
    honor Retry-After; `detail` is the describe_error() string for the log."""
    def __init__(self, retry_after, detail=""):
        super().__init__(detail)
        self.retry_after = retry_after
        self.detail = detail


def _raise_if_rate_limited(e):
    """If `e` is a 429, raise RateLimited (with a ms-precise backoff) so the caller aborts the cycle and
    backs off, LEAVING resting quotes in place. No-op otherwise, so the caller falls through to its normal
    fail-closed handling (pull quotes). Call this as the first line of a read/write `except` before
    describe_error()/pull_quotes(): rate_limit_details() reads the body ONCE (for the ms-precise retryAfterMs
    + reason), and since a 429 RAISES here the caller catches RateLimited and never calls describe_error(),
    so that body read stays the single consumer of e."""
    d = rate_limit_details(e)
    if d is not None:
        secs, _reason, detail = d
        raise RateLimited(secs, detail)

QUOTE_TIF = "ALO"                       # post-only: a quote can never take liquidity
# (goodTilTime is now computed SERVER-aligned in MarketMaker._far_future_us, not from the raw local clock)
# Inventory skew: once |position| exceeds SKEW_THRESHOLD * max_position, quote the REDUCING side at
# SKEW_MULT x size (the growing side stays normal) to mean-revert inventory toward flat faster.
# Active only when --max-position is set (the threshold is a fraction of it).
SKEW_THRESHOLD = Decimal("0.5")
SKEW_MULT = Decimal(2)

# Clock-skew guard. The signed X-Timestamp on every order must sit inside the venue's auth window
# (~±30s). ALL THREE ops now server-align via the cached self.clock_delta_ns: place_quote adds it to its
# client_timestamp directly; modify_quote/cancel_quote wrap their sign in server_clock_shim(self.clock_delta_ns),
# which shifts ordersign's internal time.time_ns() by the same offset (their signers don't accept an injected
# timestamp, and we deliberately don't fork the signing path -- the shim patches the clock, not the logic). We
# STILL measure the offset at startup + periodically and refuse to run when |offset| exceeds MAX_CLOCK_SKEW_S:
# it bounds the residual (a host drifting AFTER the last measurement, esp. while /v1/time is down) well inside
# the window, and aborts loudly on gross drift instead of silently failing place/modify/cancel (the last of
# which would also break the fail-closed quote pull). Fix is NTP on the host.
MAX_CLOCK_SKEW_S = 10.0                 # abort if |server-local clock offset| exceeds this
CLOCK_REFRESH_S = 300.0                 # re-measure the offset at least this often during the run
CLOCK_RETRY_S = 30.0                    # after a /v1/time FAILURE, retry this soon (not the full refresh)
CLOCK_RTT_MAX_S = 2.0                   # discard a clock sample whose /v1/time round-trip exceeds this: the
                                        # midpoint offset estimate is only good to +/- round_trip/2, so a slow/
                                        # rate-limited request would else fabricate a large apparent skew and
                                        # fatally abort the bot on a garbage reading (the GLD incident)

# --use-redis-bbo: reject a Redis BBO blob whose liveness `ts` is older than this (s). The wsorderbook
# publisher heartbeats ~1s, so >3s means the feed/publisher is dead -> fall back to the REST book.
REDIS_BBO_MAX_AGE = 3.0
# --disable-fallback: when the Redis BBO feed is stale/missing, do NOT hit the REST /v1/l2OrderBook -- a whole
# fleet falling back at once floods REST and trips the rate limit. Instead pull to flat and wait for the feed.
# A fleet-wide feed outage makes every bot go stale on the SAME cycle, so jitter the pull-to-flat over
# [0, this] seconds -- otherwise 37 bots fire their cancels in one synchronized burst (same herd-avoidance as
# the wsorderbook reconnect jitter). A blip shorter than a bot's random delay costs zero REST (re-check absorbs it).
DISABLE_FALLBACK_PULL_JITTER_MAX = 30.0
# --disable-fallback: an empty/absent Redis BBO gives no top-of-book, so a --disable-fallback bot would otherwise
# NEVER seed an empty market (a REST-fallback bot bootstraps off the oracle on an empty book; #3 can't reach that
# path). This sentinel is returned by _top_of_book() at most ONCE per process: it routes cycle() into its existing
# oracle branch so the bot places one oracle-priced quote to seed the book, then LATCHES (self._oracle_bootstrapped)
# so /v1/markets is NOT re-read every cycle (that would reintroduce the fleet-wide REST flood --disable-fallback exists
# to prevent). After the seed, the bot's own quotes appear in the bbo feed and normal redis-book quoting resumes.
ORACLE_BOOTSTRAP_SRC = "oracle-bootstrap"
# --oracle-when-stale: like the one-time bootstrap above, but returned EVERY cycle the Redis BBO is stale/empty
# (no latch) so the bot keeps quoting off the oracle instead of parking. For a SOLE liquidity provider on a thin
# market (common on testnet), once the bot pulls to flat the book -- and hence the bbo -- goes empty and STAYS
# empty, so the one-time seed can never re-fire and the market dies; this keeps it alive. Costs NO REST
# l2OrderBook and NO new WS conns: oracle_price() reads the exchange-wide markets cache (wsexchange-warmed,
# shared across the fleet, TTL cache_ttl), so it does not reintroduce the per-market REST flood --disable-fallback
# prevents. Post-only/ALO still stops us ever crossing a real book if liquidity reappears.
ORACLE_STALE_SRC = "oracle-stale"

# #5 proactive rate-limit pacing (see MarketMaker.call / _throttle_delay). Each write response carries
# rateLimit:{pool, remaining} where remaining = floor(cap - consumed) -- the ABSOLUTE tokens left in that
# per-subaccount pool (order: place/modify; cancel: cancelOrder). The cap GROWS with fill volume, so a fill
# raises it and the very next response's `remaining` reflects that -- we never track cap/volume ourselves.
# Once a pool is exhausted the venue drips 1 action / 10s; we watch `remaining` and, below --rate-limit-buffer,
# RAMP the pre-write delay 0 -> this so we glide into the drip rate instead of slamming into it (429 storm).
# remaining == -1 is the "account layer not enforced" sentinel (whitelisted/high-volume accounts) -> full speed.
RL_DRIP_PERIOD_S = 10.0    # the exhausted-pool drip cadence we ramp write spacing toward
RL_THROTTLE_LOG_S = 30.0   # throttle the "pacing" stderr notice to at most once per this many seconds

RUNNING = True


def _stop(_sig, _frame):
    global RUNNING
    RUNNING = False


def _ts():
    """Log-timestamp body "YYYY-MM-DD HH:MM:SS <epoch>". The trailing integer epoch (unix seconds) lets showlogs.sh
    window this bot's log to any time range exactly (no date/wrap guessing); the human date+time stays up front.
    Format matches public arcus_common_public.log_ts -- a SEPARATE copy on purpose (private/public stay independent,
    no cross-import); showlogs reads the epoch as the last bracket token so both shapes still window."""
    t = time.time()
    return f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))} {int(t)}'


def to_inc(value, increment, rounding):
    return (value / increment).to_integral_value(rounding=rounding) * increment


def bbo_top_of_book(blob, now, max_age):
    """(best_bid, best_ask) as Decimals (either may be None on a ONE-sided book) from a Redis BBO blob,
    or None if the blob is missing/non-dict, its liveness `ts` is absent/bad, it's STALE (now - ts >
    max_age), OR BOTH sides are absent. An empty "{ts}"-only blob (e.g. a bbo `subscribed` ack before
    any real bbo, or a market with no book) is NOT a usable book: returning None makes the caller fall
    back to the REST l2OrderBook instead of quoting oracle-only with no top-of-book/passive context.
    Pure (no Redis) so it's unit-testable. A FRESH ONE-sided blob IS authoritative -- the caller takes
    the oracle mid, exactly like a one-sided REST book.

    FRESHNESS SEMANTICS: `ts` is the PUBLISHER's write time (FEED liveness) -- refreshed on the publisher's
    heartbeat even when the top-of-book has NOT changed. So `0 <= age <= max_age` means "the wsorderbook feed
    is alive", NOT "the venue's top-of-book moved recently". The blob ALSO carries the venue's own `timestamp`
    (published verbatim) for any consumer that wants venue-side freshness, but we deliberately do NOT age off
    it: a genuinely QUIET market has an old venue timestamp while its BBO is still perfectly current, so ageing
    off it would false-reject exactly the thin/quiet markets this feed exists to serve. A wedged-but-connected
    venue feed (a frozen BBO on a MOVING market) is a rare separate failure not covered here."""
    if not isinstance(blob, dict):
        return None
    try:
        age = now - float(blob.get("ts"))
    except (TypeError, ValueError):
        return None
    # Require a FINITE, non-future, non-stale stamp: 0 <= age <= max_age. A bounded range (not just
    # `age > max_age`) is essential -- json.loads accepts NaN/Infinity tokens, and a NaN age makes every
    # comparison False (never "stale"), an Infinity ts gives a negative age, and a far-FUTURE ts also gives
    # a negative age; each would let a malformed/poisoned key read as fresh FOREVER under --use-redis-bbo.
    # NaN fails `0 <= age` (all NaN comparisons are False), so it's rejected here too -> REST fallback.
    if not (0 <= age <= max_age):
        return None
    _BAD = object()            # sentinel: a side PRESENT but corrupt (distinct from a genuinely absent side)
    def px(side):
        """Decimal top-of-book price, None if the side is genuinely ABSENT (missing key or explicit null ->
        a real one-sided book), or _BAD if the side is PRESENT but unusable (non-dict level, or an
        unparseable/non-finite price). A _BAD side means the blob is corrupt and is rejected wholesale --
        never silently downgraded to one-sided, which would quote off half a poisoned feed."""
        if blob.get(side) is None:                 # key missing OR explicit null -> genuinely absent side
            return None
        lvl = blob.get(side)
        if not isinstance(lvl, dict):
            return _BAD
        try:
            price = Decimal(str(lvl.get("price")))
        except (InvalidOperation, TypeError):
            return _BAD
        # present side must be a finite POSITIVE price. is_finite() FIRST short-circuits the `> 0` compare,
        # which would RAISE on a NaN. A zero/negative top-of-book is corrupt (a 0 bid would drag the mid down
        # and quote ~half price) -> reject the blob rather than quote off it.
        if not (price.is_finite() and price > 0):
            return _BAD
        # Defense-in-depth, mirroring the publisher's _bbo_ok (which requires BOTH price AND size finite-positive):
        # a present side with a valid price but a missing/NaN/<=0 SIZE is a poisoned/partial blob the publisher
        # would never have written -> reject wholesale. (size is unused for quoting today, so this only rejects a
        # malformed feed, never a valid one.)
        try:
            size = Decimal(str(lvl.get("size")))
        except (InvalidOperation, TypeError):
            return _BAD
        return price if (size.is_finite() and size > 0) else _BAD
    bid, ask = px("bestBid"), px("bestAsk")
    if bid is _BAD or ask is _BAD:
        return None            # a present-but-corrupt side -> reject the whole blob -> caller falls back to REST
    if bid is None and ask is None:
        return None            # both sides absent -> empty blob -> caller falls back to REST
    if bid is not None and ask is not None and bid >= ask:
        return None            # crossed/locked two-sided book (bid >= ask) -> corrupt/stale -> REST fallback
    return bid, ask


class MarketMaker:
    def __init__(self, args, creds, mkt):
        try:
            self.market = mkt["marketDisplayName"]   # canonical (used in l2OrderBook path + cids)
            self.market_id = int(mkt["marketId"])
            self.tick = Decimal(mkt["tickSize"])
            self.step = Decimal(mkt["stepSize"])
        except (KeyError, ValueError, TypeError, InvalidOperation):
            raise SystemExit("market_maker: market has incomplete/malformed metadata "
                             "(marketId/tickSize/stepSize/marketDisplayName).")
        # tickSize/stepSize MUST be finite and > 0: a zero increment -> DivisionByZero when rounding to it;
        # a negative one -> negative ticks/quantums reaching signing. is_finite() short-circuits the > 0
        # comparison so a NaN can't raise InvalidOperation here.
        if not (self.tick.is_finite() and self.tick > 0 and self.step.is_finite() and self.step > 0):
            raise SystemExit(f"market_maker: market {self.market} has a non-positive/non-finite increment "
                             f"(tickSize={self.tick}, stepSize={self.step}); both must be finite and > 0.")
        self.usd = args.usd            # already-parsed Decimals (see parse_args)
        self.spread = args.spread
        self.max_position = args.max_position
        self.min_collateral = args.min_collateral
        self.address = creds["eth_address"]
        self.account_index = creds["account_index"]
        self.signer = Signer.from_private_key_hex(creds["api_private_key"])
        self.query = urllib.parse.urlencode({"address": self.address})
        self.bid_cid = f"mm-{self.market}-b"
        self.ask_cid = f"mm-{self.market}-a"
        self.net = args.network                  # for the account_cache key namespace
        self.cache_ttl = args.cache_ttl
        self.cache_enabled = not args.no_cache
        self.use_redis_bbo = args.use_redis_bbo   # read top-of-book from the wsorderbook Redis BBO feed
        self.disable_fallback = args.disable_fallback   # with --use-redis-bbo: NO REST l2OrderBook fallback (pull-to-flat + wait)
        self.oracle_when_stale = args.oracle_when_stale  # with --use-redis-bbo: on a stale/empty bbo, quote off the cached oracle every cycle (no REST, no park)
        self.alternate_strategy = args.alternate_strategy  # size the REDUCING side to fully exit the position + one usd (flatten-and-flip) instead of the 2x skew; keeps the max-position cap
        # last (price, qty) we believe is RESTING per side -> skip a modify when nothing changed
        self.last_quote = {self.bid_cid: None, self.ask_cid: None}
        self._state_confirmed = False  # True once an AUTHORITATIVE (fresh) openOrders read has succeeded. Until then,
                                       # "last_quote all None" must NOT be read as "we are flat" -- a failed startup
                                       # seed can leave a prior run's resting 365-day GTT quote untracked (see
                                       # _fallback_disabled_pull, which else would park it WITHOUT canceling).
        # #5 proactive rate-limit pacing: ease off write cadence once a pool's remaining drops below this.
        self.rl_buffer = args.rate_limit_buffer
        self._pool_remaining = {"order": None, "cancel": None}   # latest write-response rateLimit.remaining (None/-1 => full speed)
        self._rl_log_last = 0.0                                  # monotonic; throttles the pacing notice
        self.clock_delta_ns = 0          # (server - local) offset in ns; measured by refresh_clock_delta()
        self._next_clock_check = 0.0      # time.monotonic() deadline for the next offset measurement
        self._clock_sampled = False       # True after the FIRST successful /v1/time sample -- run() won't quote until then
        self._feed_parked = False         # --disable-fallback: True while parked (no fresh Redis BBO); drives the one-time park/resume log
        self._oracle_bootstrapped = False # --disable-fallback: latched True after the one-time oracle seed of an empty market (no re-read /v1/markets per cycle)

    def _cached(self, name, address, fetch_fn):
        """Account-wide reads go through the short-TTL Redis cache (shared across the fleet) so
        N market bots don't each re-fetch the same data every loop. --no-cache fetches live."""
        if not self.cache_enabled:
            return fetch_fn()
        return account_cache.cached_get(self.net, address, name, fetch_fn, self.cache_ttl)

    # ── HTTP (raises on error; the loop classifies it and survives) ──────────
    @staticmethod
    def _write_pool(method, path):
        """Which per-subaccount rate-limit pool a WRITE charges, or None for reads / non-charging paths (those
        touch only the IP-weight layer, not the order/cancel pools). placeOrder/modifyOrder -> 'order';
        cancelOrder/cancelAllOrders -> 'cancel'."""
        if method != "POST":
            return None
        p = path.lower()
        if "cancel" in p:
            return "cancel"
        if "placeorder" in p or "modifyorder" in p:
            return "order"
        return None

    def _throttle_delay(self, pool, risk_off=False):
        """#5: seconds to wait BEFORE a write to `pool`, from its last-seen rateLimit.remaining. Full speed
        until --rate-limit-buffer of headroom, then RAMP 0 -> RL_DRIP_PERIOD_S as remaining -> 0 so we glide
        into the 1-action/10s drip instead of slamming into it. remaining == -1 (not-enforced sentinel) /
        unknown / buffer<=0 -> full speed; remaining <= 0 (exhausted) -> the full drip spacing.
        risk_off=True (shutdown / pull_quotes): a ONE-SHOT burst of a few cancels, not sustained quoting, so
        SKIP the gliding ramp when there's real headroom -- fire now rather than pay a glide meant to pace a
        continuous quote loop toward the limit. The exhausted (rem<=0) drip wait still applies: it's unavoidable
        (firing would just 429), and there sleeping lets a drip token replenish so the risk-off cancel succeeds."""
        rem = self._pool_remaining.get(pool)
        if rem is None or rem == -1 or self.rl_buffer <= 0:
            return 0.0
        if rem <= 0:                              # pool EXHAUSTED -> already on the drip (unavoidable even risk-off)
            return RL_DRIP_PERIOD_S
        if risk_off:                              # terminal risk-off burst WITH headroom -> fire now, no glide
            return 0.0
        if rem >= self.rl_buffer:                 # comfortable headroom
            return 0.0
        return RL_DRIP_PERIOD_S * (self.rl_buffer - rem) / self.rl_buffer   # ramp: remaining buffer->0 == delay 0->DRIP

    def _update_pool_remaining(self, default_pool, resp):
        """After a write, record rateLimit.remaining per pool from the response (its own `pool` is authoritative).
        An ABSENT rateLimit means rate limiting isn't configured -> full speed (clear to None)."""
        if not isinstance(resp, dict):
            return
        rl = resp.get("rateLimit")
        if not isinstance(rl, dict):
            self._pool_remaining[default_pool] = None
            return
        pool = rl.get("pool") if isinstance(rl.get("pool"), str) else default_pool
        rem = rl.get("remaining")
        if isinstance(rem, bool):                 # bool is an int subclass -- reject a stray True/False
            rem = None
        self._pool_remaining[pool] = rem if isinstance(rem, int) else None

    def _log_throttle(self, pool, delay):
        """Throttled (>=1/RL_THROTTLE_LOG_S) notice that #5 pacing is slowing writes, so an operator sees the
        bot easing off rather than silently stalling."""
        now = time.monotonic()
        if now - self._rl_log_last >= RL_THROTTLE_LOG_S:
            self._rl_log_last = now
            print(f"[{_ts()}] rate-limit pacing: {pool} pool remaining={self._pool_remaining.get(pool)} "
                  f"< buffer {self.rl_buffer} -> waiting {delay:.1f}s before the next {pool} write", flush=True)

    def call(self, method, path, body=None, headers=None, risk_off=False):
        pool = self._write_pool(method, path)
        if pool is not None:
            delay = self._throttle_delay(pool, risk_off=risk_off)
            if delay > 0:
                self._log_throttle(pool, delay)
                time.sleep(delay)
        resp = request(method, path, body, headers)
        if pool is not None:
            self._update_pool_remaining(pool, resp)
        return resp

    def refresh_clock_delta(self, startup=False):
        """Measure the (server - local) clock offset for server-aligned order timestamps, and ABORT
        (SystemExit) if it exceeds MAX_CLOCK_SKEW_S. All three ops apply this delta: place_quote adds it
        to its client_timestamp; modify_quote/cancel_quote wrap their sign in server_clock_shim(self.clock_delta_ns)
        so ordersign's internal X-Timestamp is shifted by the same offset. The guard bounds the residual (a host
        that drifts further AFTER the last measurement -- notably while /v1/time is down and the delta is stale)
        inside the venue's auth window, and refuses to run blind on gross drift. A transient /v1/time failure is
        NOT fatal: keep the last known delta and re-check next interval."""
        try:
            delta, rtt = clock_delta_ns()
        except Exception as e:
            # Transient /v1/time failure: keep the last known delta and retry SOON (CLOCK_RETRY_S), not
            # after the full CLOCK_REFRESH_S -- so a cold start with delta=0 self-corrects quickly once
            # /v1/time recovers (the skew guard then engages), instead of running uncorrected for 5 min.
            self._next_clock_check = time.monotonic() + CLOCK_RETRY_S
            print(f"[{_ts()}] clock-skew check skipped (/v1/time unavailable: "
                  f"{describe_error(e)}); keeping prior offset {self.clock_delta_ns / 1e9:+.3f}s; "
                  f"retry in {CLOCK_RETRY_S:g}s", flush=True)
            return
        # A high round-trip /v1/time (slow / rate-limited / Cloudflare-challenged) makes the MIDPOINT offset
        # estimate unreliable -- it's accurate only to +/- rtt/2, so a stalled request can fabricate a large
        # apparent skew. Treat it like a transient failure (keep the prior, low-RTT delta + retry soon) rather
        # than trust the garbage reading and FATALLY abort. The GLD incident: a ~20s stalled /v1/time read +10s
        # while the true offset was +0.17s, killing ONE bot while its 34 peers (fast checks) kept running.
        if rtt > CLOCK_RTT_MAX_S * 1_000_000_000:
            self._next_clock_check = time.monotonic() + CLOCK_RETRY_S
            print(f"[{_ts()}] clock sample DISCARDED (/v1/time round-trip {rtt / 1e9:.3f}s > {CLOCK_RTT_MAX_S:g}s "
                  f"-> apparent offset {delta / 1e9:+.1f}s is unreliable); keeping prior offset "
                  f"{self.clock_delta_ns / 1e9:+.3f}s; retry in {CLOCK_RETRY_S:g}s", flush=True)
            return
        if abs(delta) > MAX_CLOCK_SKEW_S * 1_000_000_000:
            raise SystemExit(
                f"market_maker: host clock is off from the Arcus server by {delta / 1e9:+.1f}s "
                f"(limit ±{MAX_CLOCK_SKEW_S:g}s). Signed order timestamps would fall outside the venue's "
                f"auth window, so place/modify/cancel (incl. quote pulls) would fail -- refusing to "
                f"{'start' if startup else 'continue'}. Sync the host clock (NTP) and restart.")
        self.clock_delta_ns = delta
        self._clock_sampled = True         # a successful in-window sample -> our signed timestamps are now server-aligned
        self._next_clock_check = time.monotonic() + CLOCK_REFRESH_S
        if startup:
            print(f"  clock offset vs server: {delta / 1e9:+.3f}s (within ±{MAX_CLOCK_SKEW_S:g}s); "
                  f"place timestamps server-aligned", flush=True)

    # ── Order ops (POST-ONLY / ALO) ──────────────────────────────────────────
    def _far_future_us(self):
        """goodTilTime 365 days out in SERVER time (host clock + clock_delta), aligned like the X-Timestamp --
        NOT the raw local wall clock. A 365-day GTT clears the venue's 1-month minimum regardless, and the
        clock-skew guard already bounds host drift to +/-MAX_CLOCK_SKEW_S, so this just keeps the expiry
        consistent with the signed timestamp rather than trusting the bare local clock."""
        return str((time.time_ns() + self.clock_delta_ns) // 1000 + 365 * 86_400 * 1_000_000)

    def place_quote(self, order_side, sside, cid, price, qty):
        ct = time.time_ns() + self.clock_delta_ns   # server-aligned X-Timestamp (host clock may drift)
        gtt = self._far_future_us()
        headers = self.signer.sign_place_order(
            address=self.address, account_index=self.account_index, client_id=cid,
            client_timestamp_ns=ct, good_til_time_ns_=ordersign.good_til_time_ns(gtt),
            market_id=self.market_id, price_ticks=ordersign.price_to_ticks(f"{price:f}", self.tick),
            quantity_quantums=ordersign.size_to_quantums(f"{qty:f}", self.step),
            side=sside, time_in_force=ordersign.TIF_ALO)
        body = {"address": self.address, "accountIndex": self.account_index, "marketId": self.market_id,
                "orderSide": order_side, "orderType": "LIMIT", "quantity": f"{qty:f}",
                "price": f"{price:f}", "timeInForce": QUOTE_TIF, "goodTilTime": gtt,
                "timestamp": ct, "clientId": cid}
        check_order_response(self.call("POST", f"/v1/placeOrder?{self.query}", body, headers), "placeOrder")

    def modify_quote(self, order_side, sside, cid, price, qty, order_id):
        # Identify the modify by orderId ONLY. The venue requires EXACTLY ONE of orderId/clientId in BOTH the
        # signed payload AND the body -- sending both is rejected (HTTP 400 "provide exactly one of orderId or
        # clientId (not both)", or 401 if the signature carries both; verified live 2026-08-20). ordersign
        # requires order_id anyway, so we pass client_id=None (no `c` in the canonical payload) and DON'T put
        # clientId in the body. `cid` stays a param for the caller/logging but is intentionally NOT sent; the
        # replacement keeps the order's original clientId regardless (verified live), so live_quotes' cid-based
        # tracking still finds it next cycle. (Changelog review, 2026-08-20 -- this modify path was previously
        # broken: it sent both identifiers, so every MM modify would have failed.)
        #
        # QUEUE PRIORITY (venue contract): a modify is IN PLACE (keeps priority) only when price is unchanged
        # AND size reduced AND goodTilTime unchanged; any price move / size increase / differing gtt is an
        # atomic cancel+replace that LOSES priority. Here that means every modify is a cancel+replace: the
        # common case is a price move (unavoidable -- and modify still beats cancel-then-place: atomic, no
        # resting gap, one request), and we always stamp a FRESH gtt below. The one priority-preserving case
        # (same-price size-REDUCE + echoed gtt) is deliberately NOT pursued: payoff is ~zero for a sole-LP bot
        # (size shrinks only via inventory skew, which moves only on FILLS = a counterparty) and rare
        # otherwise, AND we found no reliable observable to even verify priority was kept (orderId is preserved
        # across cancel-replace; createdAt moved even on a same-price+reduce+echoed-gtt modify). Not worth
        # touching the signing/gtt path for. (Changelog review #3.)
        gtt = self._far_future_us()
        # sign_modify_order mints its X-Timestamp from ordersign's internal time.time_ns(); the shim shifts it
        # by our cached self.clock_delta_ns so modify aligns EXACTLY like place_quote (no /v1/time fetch here).
        with server_clock_shim(self.clock_delta_ns):
            headers = self.signer.sign_modify_order(
                address=self.address, account_index=self.account_index, market_id=self.market_id,
                price_ticks=ordersign.price_to_ticks(f"{price:f}", self.tick),
                quantity_quantums=ordersign.size_to_quantums(f"{qty:f}", self.step),
                good_til_time_ns_=ordersign.good_til_time_ns(gtt),
                reduce_only=False, side=sside, time_in_force=ordersign.TIF_ALO,
                order_id=order_id, client_id=None)     # orderId-only identity (venue exactly-one rule)
        body = {"address": self.address, "accountIndex": self.account_index, "marketId": self.market_id,
                "orderId": order_id, "side": order_side, "timeInForce": QUOTE_TIF,
                "price": f"{price:f}", "quantity": f"{qty:f}", "reduceOnly": False, "goodTilTime": gtt}
        check_order_response(self.call("POST", f"/v1/modifyOrder?{self.query}", body, headers), "modifyOrder")

    def cancel_quote(self, cid, risk_off=False):
        # Same clock alignment as modify/place: shift ordersign's internal X-Timestamp by the cached offset so
        # the fail-closed quote pull can't 401 under host drift (within the ±MAX_CLOCK_SKEW_S the guard enforces).
        # risk_off=True (shutdown / pull_quotes): skip the #5 gliding ramp so a terminal cancel burst with pool
        # headroom fires immediately instead of pacing itself; the exhausted-pool drip wait still applies.
        with server_clock_shim(self.clock_delta_ns):
            headers = self.signer.sign_cancel_order(address=self.address, account_index=self.account_index,
                                                    market_id=self.market_id, client_id=cid)
        body = {"address": self.address, "accountIndex": self.account_index, "marketId": self.market_id,
                "kind": "clientId", "clientId": cid}
        check_order_response(self.call("POST", f"/v1/cancelOrder?{self.query}", body, headers, risk_off=risk_off),
                             "cancelOrder", cancel=True)   # order-already-gone is idempotent success, not a retry-forever failure

    def pull_quotes(self):
        """Cancel any of THIS bot's resting quotes (both sides). Called when fresh pricing
        can't be established for a cycle, so stale 365-day GTT orders don't keep resting at
        old prices -- ALO/POST-ONLY stops us TAKING, but a resting quote can still be picked
        off as the market moves away from it. Best-effort: a failed read/cancel is logged and
        last_quote is cleared so the side re-places cleanly once data returns. (Confirmed-clean
        cancellation on EXIT remains shutdown()'s job.)"""
        try:
            live = self.live_quotes(fresh=True)   # uncached: a stale cache must not hide a resting GTT quote
        except (OSError, json.JSONDecodeError, ValueError) as e:   # ValueError = malformed openOrders body
            # A 429 on the pull's openOrders READ: honor Retry-After NOW -- raise RateLimited so run() backs off,
            # instead of returning and letting the NEXT cycle fire its reads into the limiter first (the old gap).
            # The two best-effort cancels below would themselves 429 and deepen the storm, so we don't reach them.
            # Raising leaves the resting POST-ONLY/ALO quotes in place and last_quote untouched -> reconciliation
            # stays accurate (same as the cancel-side 429 path, and as run()'s own RateLimited handling).
            _raise_if_rate_limited(e)
            # We can't confirm WHICH side rests -- but cancel_quote identifies by clientId (kind:"clientId")
            # + marketId and needs NO orderId, so best-effort cancel BOTH known sides anyway. A spurious
            # cancel of a not-resting side just returns a harmless not-found (SystemExit, caught). Returning
            # WITHOUT trying would leave 365-day GTT quotes resting on any pricing/cache failure -- the exact
            # thing pull_quotes exists to prevent.
            print(f"  could not read open orders to pull quotes: {describe_error(e)}; canceling both sides best-effort")
            for cid in (self.bid_cid, self.ask_cid):
                try:
                    self.cancel_quote(cid, risk_off=True)
                    print(f"  pulled {cid} (unconfirmed read)")
                    self.last_quote[cid] = None                             # confirmed pulled -> forget it
                except (OSError, json.JSONDecodeError, SystemExit) as ce:   # not-found / transport -> best-effort
                    _raise_if_rate_limited(ce)   # a 429 DURING a cancel: STOP firing into the storm -> RateLimited so
                                                 # run() backs off (else we'd fire the other side's cancel too and deepen it)
                    # KEEP last_quote on failure: the quote MAY still rest, so next cycle retries the cancel (a
                    # re-cancel of an already-gone order is a harmless not-found) instead of a stale cache letting
                    # the reconciler skip it and leave a 365-day GTT quote resting.
                    print(f"  pull {cid} failed: {describe_error(ce)}; keeping last_quote to retry next cycle")
            return
        for cid in (self.bid_cid, self.ask_cid):
            if cid in live:
                try:
                    self.cancel_quote(cid, risk_off=True)
                    print(f"  pulled {cid}")
                    self.last_quote[cid] = None                            # confirmed pulled -> forget it
                except (OSError, json.JSONDecodeError, SystemExit) as e:   # SystemExit = 2xx REJECTED/ERROR body
                    _raise_if_rate_limited(e)    # a 429 DURING a cancel: STOP firing -> RateLimited so run() backs off
                    # KEEP last_quote on a FAILED risk-off cancel: the quote is still resting, so next cycle
                    # retries the cancel rather than clearing it and letting a stale cache skip the retry until
                    # the live read catches up (leaving a 365-day GTT quote resting at a stale price meanwhile).
                    print(f"  pull {cid} failed: {describe_error(e)}; keeping last_quote to retry next cycle")
            else:
                self.last_quote[cid] = None                               # not resting (fresh read) -> nothing to pull

    def _risk_off_on_429(self, e, what):
        """For a RISK read (position / free collateral / openOrders) that errored: if `e` is a 429/1015 we go
        RISK-OFF and back off. Leave-quotes-on-429 is right for PRICING reads (avoid a cancel storm; ALO can't
        take), but a RISK read we can't complete means inventory/collateral is UNKNOWN -- leaving 365-day GTT
        quotes resting there is fail-OPEN (a resting quote can still be picked off, breaching --max-position /
        --min-collateral, exactly during a Cloudflare 1015 storm). pull_quotes' openOrders GET would 429 too, so
        cancel BOTH sides BY CLIENTID (cache-independent, NO read; idempotent -- a not-resting cid returns a
        harmless CANCEL_ACKNOWLEDGED), THEN raise RateLimited so run() honors Retry-After. NO-OP (returns) when
        `e` is not a rate-limit error, so the caller falls through to its normal fail-closed pull_quotes().
        Bounded: a cancel that itself 429s KEEPS last_quote and is retried next cycle; once flat there's nothing
        to cancel, so it does not fire into the storm indefinitely."""
        d = rate_limit_details(e)          # reads the 429 body ONCE (the single consumer of `e`); None if not 429/1015
        if d is None:
            return                         # not throttled -> caller does its normal pull_quotes()
        secs, _reason, detail = d
        if not (self.max_position is not None or self.min_collateral is not None or self.alternate_strategy):
            # No inventory/collateral guard configured -> a resting quote can breach nothing, so PRESERVE the
            # deliberate leave-quotes-on-429 behavior (this openOrders read runs every cycle; cancelling here
            # would just churn quotes on a transient 429). Just back off. (position/collateral reads only run
            # when their guard IS set, so they never reach this early-out.)
            raise RateLimited(secs, detail)
        print(f"[{_ts()}] {what} read 429/1015 -> risk-off cancel (no read) then backing off ({detail})")
        for cid in (self.bid_cid, self.ask_cid):
            if self.last_quote.get(cid) is None:
                continue                   # not one we believe is resting -> skip (no needless write into the storm)
            try:
                self.cancel_quote(cid, risk_off=True)   # by clientId, no openOrders GET; risk_off skips the #5 glide
                self.last_quote[cid] = None             # confirmed pulled -> forget it
            except (OSError, json.JSONDecodeError, SystemExit) as ce:
                # the cancel itself may 429 in the storm -> best-effort: KEEP last_quote so next cycle retries it
                print(f"[{_ts()}]   risk-off cancel {cid} failed ({describe_error(ce)}); keeping last_quote to retry")
        raise RateLimited(secs, detail)

    # ── State reads ───────────────────────────────────────────────────────────
    def _order_is_ours(self, o):
        """True if openOrders entry `o` is one of THIS bot's resting quotes. The clientId (mm-<market>-b/-a)
        is MARKET-SCOPED -- it embeds this bot's market ticker -- so it alone uniquely identifies the order
        AND its market; no two markets can produce the same cid. marketId is therefore NOT required to match:
        a missing / null / malformed marketId must NOT disqualify our own clientId. Gating on marketId
        equality (the old `and str(o.get("marketId")) == str(self.market_id)`) failed OPEN -- any marketId
        corruption made live_quotes read a malformed resting quote as absent (-> double-quote) and made
        shutdown falsely confirm cancellation while an mm-<market>-b entry was still resting. Cancel is BY
        clientId (kind:"clientId", no marketId), so matching on clientId alone is also what actually acts on it."""
        return o.get("clientId") in (self.bid_cid, self.ask_cid)

    def live_quotes(self, fresh=False):
        """Map our resting clientId -> (orderId, remainingSize, restingPrice) IN THIS MARKET (scoped). modify needs
        the orderId (preserved across modifies; validated non-null here so cycle() never calls modify
        with a missing id); remainingSize is what is ACTUALLY resting right now (originalSize -
        filledSize), which cycle() compares against the intended qty so a partial fill that shrank the
        order forces a size-restoring modify instead of a stale keep.

        fresh=True bypasses the Redis account cache for an uncached /v1/openOrders read
        -- used by the fail-closed pull path (pull_quotes), where a stale or poller-warmed
        cache could hide a currently-resting quote and leave a 365-day GTT order live, the
        same reason shutdown() confirms cancellation against a fresh read."""
        fetch = lambda: self.call("GET", f"/v1/openOrders?{self.query}")
        src = fetch() if fresh else self._cached("openOrders", self.address, fetch)
        # A 2xx openOrders body can still be MALFORMED (not a dict, 'orders' not a list, or a non-dict order
        # entry) -- a poisoned/poller-missed cache or a bad server response. Validate the shape and RAISE on
        # violation so the caller (cycle / pull_quotes) fails CLOSED and pulls quotes, rather than letting a
        # bare `.get` AttributeError bubble to run()'s log-only handler and leave 365-day GTT quotes resting.
        # (Mirrors the l2OrderBook-malformed and position()/free_collateral() fail-closed paths.)
        if not isinstance(src, dict):
            raise ValueError(f"openOrders body is {type(src).__name__}, not an object")
        if "orders" not in src:
            raise ValueError("openOrders body has no 'orders' key")   # {} != "zero resting orders" -> fail CLOSED, not open
        orders = src.get("orders")
        if not isinstance(orders, list):
            raise ValueError(f"openOrders 'orders' is {type(orders).__name__}, not a list")
        if fresh:                       # an AUTHORITATIVE (uncached) read succeeded -> last_quote is now trustworthy:
            self._state_confirmed = True   # "empty last_quote" reliably means flat (see _fallback_disabled_pull)
        live = {}
        for o in orders:
            if not isinstance(o, dict):
                raise ValueError("openOrders contains a non-object order entry")
            cid = o.get("clientId")
            if self._order_is_ours(o):
                # remainingSize (= originalSize - filledSize) is the authoritative resting size. A
                # missing/unparseable value is a malformed body -> RAISE so the caller fails CLOSED and
                # pulls, mirroring the shape checks above (never silently treat it as full size).
                try:
                    remaining = Decimal(str(o.get("remainingSize")))
                except (InvalidOperation, TypeError):
                    raise ValueError(f"openOrders order {cid} has invalid remainingSize {o.get('remainingSize')!r}")
                if not remaining.is_finite():     # Decimal() accepts "NaN"/"Infinity" -> treat as malformed too
                    raise ValueError(f"openOrders order {cid} has non-finite remainingSize {o.get('remainingSize')!r}")
                if remaining <= 0:                # a RESTING order has POSITIVE remaining (0 = fully filled, gone);
                    raise ValueError(f"openOrders order {cid} has non-positive remainingSize {o.get('remainingSize')!r}")  # <=0 is malformed -> fail-closed pull, not an odd size-restoring modify
                # orderId is REQUIRED to modify a resting quote (modify_order_payload raises without it).
                # A missing/empty id is a malformed entry -> RAISE so the caller fails CLOSED and pulls
                # (cancel is by clientId, needs no orderId), rather than reaching modify_quote with a null
                # id whose ValueError the per-side handler doesn't catch -> bubbles + leaves state stale.
                oid = o.get("orderId")
                if not oid:
                    raise ValueError(f"openOrders order {cid} has no orderId")
                # resting PRICE (fail-closed like remainingSize): cycle()'s keep compares it against the
                # intended px, so a quote whose ACTUAL resting price drifted from local last_quote (stale
                # tickSize / external or partial modify) is re-priced rather than silently kept. A missing/
                # unparseable/non-finite price is a malformed body -> RAISE so the caller pulls (fail-closed).
                try:
                    price = Decimal(str(o.get("price")))
                except (InvalidOperation, TypeError):
                    raise ValueError(f"openOrders order {cid} has invalid price {o.get('price')!r}")
                if not price.is_finite():
                    raise ValueError(f"openOrders order {cid} has non-finite price {o.get('price')!r}")
                if price <= 0:                    # a resting order at price <= 0 is nonsensical -> malformed body;
                    raise ValueError(f"openOrders order {cid} has non-positive price {o.get('price')!r}")  # fail-closed pull rather than keep/modify against a bad price
                live[cid] = (oid, remaining, price)
        return live

    def position(self):
        """Signed position size for this market.

        Decimal(0) when genuinely flat, the signed size when parseable, or None when the position can't be
        determined -- an unparseable/non-finite size, a MALFORMED per-market value, OR a malformed body (not a
        dict, or no 'positions' object) -- so the risk guard fails CLOSED (treats it as unknown, never as flat).
        Only a market genuinely ABSENT from the 'positions' object is flat. This is the fetch-side backstop:
        the poller rejects malformed bodies before caching AND cached_get won't serve them, but a bot's OWN
        --no-cache / cache-miss fetch is validated HERE so a malformed value can never be misread as flat.
        """
        body = self._cached("positions", self.address,
                            lambda: self.call("GET", f"/v1/positions?{self.query}"))
        if not isinstance(body, dict) or "positions" not in body:
            return None                        # non-dict / no 'positions' key -> unknown, fail closed
        raw = body.get("positions")
        if raw is None:
            return None                        # 'positions' present but NULL -> exposure UNKNOWN, fail CLOSED. LIVE-VERIFIED
                                               # 2026-08-04: the API signals a FLAT account with `{}` (empty object), NOT null,
                                               # so a null here is an anomaly (glitch/error), not flat -> never read it as flat
                                               # (would fail-OPEN a real position). Genuine-flat `{}` is handled below (mid absent).
        if not isinstance(raw, dict):
            return None                        # []/false/"" etc -> NOT flat (never `or {}` it -> that fails OPEN); unknown, fail CLOSED
        positions = raw
        mid = str(self.market_id)
        if mid not in positions:
            return Decimal(0)                  # market genuinely ABSENT from the positions object -> flat
        p = positions[mid]                     # PRESENT key: null / non-dict is NOT absence -> must not read as flat
        if not isinstance(p, dict):
            return None                        # present-but-null/[]/etc -> unknown, fail CLOSED --
                                               # never misread a present malformed value as flat (fail-OPEN on a real position)
        try:
            size = Decimal(str(p.get("size")))
        except (InvalidOperation, TypeError):
            return None
        return size if size.is_finite() else None   # NaN/Infinity size -> unknown, fail closed

    def free_collateral(self):
        acct = self._cached("account", self.address, lambda: self.call("GET", f"/v1/account?{self.query}"))
        try:
            fc = Decimal(str(acct.get("freeCollateral")))
        except (InvalidOperation, TypeError, AttributeError):   # AttributeError = non-dict body -> unknown, fail closed
            return None
        return fc if fc.is_finite() else None   # NaN/Infinity -> unknown, fail closed (never bypass the collateral guard)

    def oracle_price(self):
        """Live oracle price for this market (Decimal > 0), or None if unavailable.
        The quoting reference when the book isn't two-sided. One extra /v1/markets
        read, only taken on the fallback path."""
        # A 2xx /v1/markets body can be MALFORMED (non-dict body, or 'markets' not a list) -- a poisoned/
        # poller-missed cache or a bad server response. `.get`/`m.get` on that raises AttributeError, which
        # the oracle caller in cycle() does NOT catch (only OSError/JSONDecodeError) -> it would bubble to
        # run()'s log-only handler and leave 365-day GTT quotes resting. Treat malformed as "unavailable":
        # return None (contract already allows it -> cycle's `if mid is None` pulls quotes). A single non-dict
        # sibling entry is skipped (it can't be our marketId anyway), not fatal.
        body = self._cached("markets", None, lambda: self.call("GET", "/v1/markets"))
        if not isinstance(body, dict):
            return None
        markets = body.get("markets", [])
        if not isinstance(markets, list):
            return None
        for m in markets:
            if not isinstance(m, dict):
                continue
            if str(m.get("marketId")) == str(self.market_id):
                try:
                    v = Decimal(str(m.get("oraclePrice")))
                except (InvalidOperation, TypeError):
                    return None
                # is_finite() FIRST: it short-circuits before `v > 0`, which itself RAISES InvalidOperation on
                # a NaN (Decimal("Infinity") would otherwise pass v>0 and return Infinity). That uncaught raise
                # would bubble out of preflight_max_position (catches only OSError/JSONDecodeError) and abort
                # startup before cleanup. Only a finite positive oracle is a usable quoting reference.
                return v if v.is_finite() and v > 0 else None
        return None

    def quote_prices(self, mid):
        bid = to_inc(mid * (1 - self.spread), self.tick, ROUND_FLOOR)
        ask = to_inc(mid * (1 + self.spread), self.tick, ROUND_CEILING)
        return bid, ask

    def preflight_max_position(self):
        """Warn at startup if --max-position is smaller than a single quote. The inventory guard
        is fail-closed and size-aware, so if one quote already exceeds the cap it pulls BOTH sides
        every cycle and NOTHING rests (a low-priced market like DYDX-USD makes this easy to hit,
        since max-position is in BASE UNITS, not USD). Estimates the size from the oracle price."""
        if self.max_position is None:
            return
        try:
            ref = self.oracle_price()
        except (OSError, json.JSONDecodeError):
            return                                       # best-effort: a transient markets read (e.g. --no-cache
                                                        # at boot) must not kill startup; the cycle re-checks anyway
        if ref is None or ref <= 0:
            return                                       # can't estimate; the cycle notes will show it
        bid_px = to_inc(ref * (1 - self.spread), self.tick, ROUND_FLOOR)
        ask_px = to_inc(ref * (1 + self.spread), self.tick, ROUND_CEILING)
        if bid_px <= 0 or ask_px <= 0:
            return
        worst = max(to_inc(self.usd / bid_px, self.step, ROUND_FLOOR),
                    to_inc(self.usd / ask_px, self.step, ROUND_FLOOR))
        if worst > self.max_position:
            print(f"WARNING: --max-position {self.max_position} (BASE UNITS, not USD) is smaller than a "
                  f"single ~{worst:f}-unit quote (${self.usd}/side at ~{ref:f}). The inventory guard will "
                  f"pull BOTH sides every cycle and NO orders will rest -- raise --max-position above "
                  f"{worst:f}, or lower --usd.")

    def redis_bbo(self):
        """Top-of-book from the local wsorderbook's Redis BBO feed (--use-redis-bbo), age-guarded on
        `ts`. (best_bid, best_ask) as Decimals (either may be None), or None when the key is missing/
        stale/unparseable so the caller falls back to the REST l2OrderBook."""
        return bbo_top_of_book(account_cache.read_bbo(self.net, self.market), time.time(), REDIS_BBO_MAX_AGE)

    def _top_of_book(self):
        """(best_bid, best_ask, source) as Decimals|None, or None if no fresh book could be read this
        cycle (caller pulls quotes). With --use-redis-bbo, prefer the Redis BBO feed and fall back to
        the REST l2OrderBook when it's stale/missing -- so enabling the flag is never worse than today."""
        if self.use_redis_bbo:
            bbo = self.redis_bbo()
            if bbo is not None:
                return bbo[0], bbo[1], "redis-book"
            if self.oracle_when_stale:
                # --oracle-when-stale: the bbo is stale/empty, so quote off the cached oracle EVERY cycle (no
                # latch). Route into cycle()'s oracle branch with an all-None tuple. Uses the wsexchange-warmed
                # markets cache -> no REST l2OrderBook and no new WS conn, so it keeps a sole-LP / thin market
                # alive without reintroducing the flood --disable-fallback guards against. Takes precedence over
                # the one-time bootstrap AND the REST fallback below (it is the intended stale-feed behaviour).
                return None, None, ORACLE_STALE_SRC
            if self.disable_fallback:
                # --disable-fallback: feed stale/missing, and we must NOT hit the REST l2OrderBook (a whole
                # fleet re-fetching per cycle trips the rate limit). ONE-TIME EXCEPTION: if we've never seeded
                # this market, hand cycle() an all-None tuple ONCE so its oracle branch places a single
                # oracle-priced quote to bootstrap an empty book (the flag latches there, on a successful oracle
                # read, so /v1/markets is not re-read every cycle). After the seed -- or if already latched --
                # signal "no price" WITHOUT touching REST; cycle() then pulls to flat (jittered) and waits.
                if not self._oracle_bootstrapped:
                    return None, None, ORACLE_BOOTSTRAP_SRC
                return None
            # stale / missing / down -> fall through to the REST book
        try:
            ob = self.call("GET", f"/v1/l2OrderBook/{urllib.parse.quote(self.market)}")
        except (OSError, json.JSONDecodeError) as e:
            _raise_if_rate_limited(e)   # 429 -> back off, LEAVE quotes resting (not a stale-pricing failure)
            # Can't establish fresh pricing this cycle -> don't leave stale 365-day GTT quotes
            # resting at old prices; pull them and re-quote once the book is readable again.
            print(f"[{_ts()}] l2OrderBook unavailable ({describe_error(e)}); pulling quotes")
            return None
        # Parse the book. A 2xx response can still carry MALFORMED data (non-numeric or short/wrong-shape
        # levels, or `ob` not even a dict) -> InvalidOperation/TypeError/IndexError/KeyError/ValueError/
        # AttributeError. Treat that exactly like an unreadable book: FAIL CLOSED (return None -> caller
        # pulls quotes) rather than let it bubble to run() (log-only) and leave stale GTT quotes resting.
        try:
            # Require bids AND asks to be PRESENT list fields. `[]` is a real empty side (kept), but a body
            # MISSING the fields ({} / {"error":...}) is NOT a valid empty book -- `.get(..., [])` would make
            # it look two-sided-empty and quote off oracle only. Reject -> same fail-closed pull as a bad book.
            if not isinstance(ob, dict) or not isinstance(ob.get("bids"), list) or not isinstance(ob.get("asks"), list):
                raise ValueError("l2OrderBook missing bids/asks list fields")
            bids, asks = ob["bids"], ob["asks"]
            # The API server returns each side top-of-book first (bids high→low, asks low→high) and
            # uncrossed, so bids[0]/asks[0] are already best -- no client-side sort needed (verified live
            # 2026-07-06 on testnet+mainnet). Same server trust as consuming its prices uncrossed.
            best_bid = Decimal(bids[0][0]) if bids else None
            best_ask = Decimal(asks[0][0]) if asks else None
            # Present prices must be finite AND POSITIVE. Decimal() accepts "Infinity"/"NaN" (non-finite), and a
            # zero/negative top-of-book is corrupt -- a 0 bid gave mid=50.5 on a $101 ask and quoted ~half price.
            # is_finite() FIRST short-circuits `> 0`, which would RAISE on a NaN. Reject -> same fail-closed pull.
            if (best_bid is not None and not (best_bid.is_finite() and best_bid > 0)) or \
               (best_ask is not None and not (best_ask.is_finite() and best_ask > 0)):
                raise ValueError("non-finite or non-positive top-of-book price")
            # A two-sided book must be uncrossed: bid < ask. A crossed/locked book (bid >= ask) is corrupt or
            # mid-transition -- quoting a spread around its mid would post through the market -> pull instead.
            if best_bid is not None and best_ask is not None and best_bid >= best_ask:
                raise ValueError(f"crossed/locked book: bid {best_bid} >= ask {best_ask}")
        except (InvalidOperation, TypeError, IndexError, KeyError, ValueError, AttributeError) as e:
            print(f"[{_ts()}] l2OrderBook malformed ({describe_error(e)}); pulling quotes")
            return None
        return best_bid, best_ask, "book"

    def _fallback_disabled_pull(self):
        """--disable-fallback: the Redis BBO feed is stale/missing. Do NOT touch the REST /v1/l2OrderBook (a
        whole fleet re-fetching per cycle trips the rate limit). If we still have resting quotes, JITTER first
        (a fleet-wide feed outage makes every bot go stale on the same cycle, so an un-jittered pull fires all
        cancels at once), re-check the feed (a blip shorter than the jitter costs zero REST), then cancel to
        flat BY CLIENTID -- no openOrders GET, because last_quote is our OWN authoritative record of what we
        placed (seeded from a fresh read at startup, kept accurate every cycle). A failed cancel keeps
        last_quote, so the next cycle retries; a 429 raises RateLimited so run() backs off (Retry-After).
        Once flat (last_quote all None) we do NOTHING -- zero REST -- and cycle() keeps re-reading Redis at the
        top each cycle, so we resume the instant the feed is fresh again."""
        resting = [cid for cid in (self.bid_cid, self.ask_cid) if self.last_quote[cid] is not None]
        if not resting:
            if self._state_confirmed:
                return   # genuinely flat (an authoritative fresh read confirmed it) -> stay silent (no REST); the
                         # top-of-cycle Redis read is what resumes us
            # State NEVER confirmed (startup seed failed + no fresh openOrders read since) -> we CANNOT assume flat:
            # a prior run's untracked 365-day GTT quote may still rest. Fall through and best-effort cancel BOTH cids
            # by clientId (idempotent -- a not-resting cid returns a harmless CANCEL_ACKNOWLEDGED) rather than park a
            # possibly-resting quote on a moving market.
            resting = [self.bid_cid, self.ask_cid]
        # Jitter so 37 bots don't fire their cancels in one synchronized burst; shutdown-responsive.
        self._sleep_responsive(random.uniform(0, DISABLE_FALLBACK_PULL_JITTER_MAX))
        if not RUNNING:
            return   # shutdown requested during the jitter -> let shutdown() do the confirmed pull, don't race it
        if self.redis_bbo() is not None:
            return   # feed recovered during the jitter -> DON'T cancel; cycle() resumes normal quoting next tick
        for cid in resting:
            try:
                self.cancel_quote(cid)                  # BY CLIENTID (no orderId / no openOrders GET needed)
                self.last_quote[cid] = None             # confirmed pulled -> forget it
                print(f"[{_ts()}] redis BBO stale + --disable-fallback: pulled {cid} to flat (no REST fallback; waiting for feed)")
            except (OSError, json.JSONDecodeError, SystemExit) as e:   # SystemExit = 2xx REJECTED/not-found body
                _raise_if_rate_limited(e)    # a 429 mid-pull -> RateLimited so run() backs off; STOP firing into the storm
                # KEEP last_quote on failure: the quote MAY still rest, so the next (re-jittered) cycle retries.
                print(f"[{_ts()}] --disable-fallback pull {cid} failed ({describe_error(e)}); keeping last_quote to retry")

    # ── One cycle ─────────────────────────────────────────────────────────────
    def cycle(self):
        # Acquire top-of-book (Redis BBO when --use-redis-bbo, else the REST l2OrderBook). None = no
        # fresh pricing this cycle -> pull quotes so stale 365-day GTT orders don't rest at old prices.
        top = self._top_of_book()
        if top is None:
            if self.disable_fallback:
                # One-time visibility: a --disable-fallback bot with no fresh Redis BBO goes quiet
                # (pull-to-flat, then wait), which looks like a hang -- notably at startup before any
                # feed exists. Log the transition ONCE so "parked, waiting for feed" is distinguishable
                # from a crash. _fallback_disabled_pull() still logs any actual quote pull it does.
                if not self._feed_parked:
                    self._feed_parked = True
                    print(f"[{_ts()}] no fresh Redis BBO for {self.market} "
                          f"(key arcus:{self.net}:bbo:{self.market}); --disable-fallback set -> not quoting, "
                          f"waiting for the feed (no REST fallback)", flush=True)
                self._fallback_disabled_pull()   # Redis feed stale: jittered pull-to-flat by clientId, NO REST flood; wait for the feed
            else:
                self.pull_quotes()               # normal: fail-closed pull so stale 365-day GTT quotes don't rest at old prices
            return
        # Usable top-of-book: if we were parked waiting for the feed, announce recovery ONCE.
        if self._feed_parked:
            self._feed_parked = False
            print(f"[{_ts()}] Redis BBO for {self.market} is fresh again -> resuming quoting", flush=True)
        best_bid, best_ask, book_src = top
        # Reference price: book mid when two-sided, else fall back to the oracle so we
        # can still quote (and bootstrap liquidity) on a one-sided / empty book.
        if best_bid is not None and best_ask is not None:
            mid, ref = (best_bid + best_ask) / 2, book_src
        else:
            try:
                mid = self.oracle_price()      # reads /v1/markets; can raise on transport/JSON error
            except (OSError, json.JSONDecodeError) as e:
                _raise_if_rate_limited(e)   # 429 -> back off, LEAVE quotes resting
                # Oracle fallback read failed -> no fresh reference; pull quotes rather than let the
                # raise bubble to run() (log-only) and leave stale 365-day GTT quotes resting.
                print(f"[{_ts()}] oracle read failed ({describe_error(e)}); pulling quotes")
                self.pull_quotes()
                return
            if mid is None:
                # No reference price -> pull quotes rather than leave them resting at stale prices.
                print(f"[{_ts()}] no two-sided book and no usable oracle; pulling quotes")
                self.pull_quotes()
                return
            # One-time --disable-fallback oracle bootstrap: we got a usable oracle mid, so this cycle seeds the
            # empty market. LATCH now (only on a successful oracle read) so _top_of_book() stops handing us the
            # bootstrap tuple and never re-reads /v1/markets every cycle -- a FAILED read above did NOT latch, so
            # the seed retries next cycle until it succeeds once. After the seed, our own quotes populate the bbo
            # feed and normal redis-book quoting resumes; if the feed stays empty next cycle we park (pull-to-flat).
            if book_src == ORACLE_BOOTSTRAP_SRC:
                self._oracle_bootstrapped = True
            # oracle-stale (--oracle-when-stale) does NOT latch: it re-quotes off the oracle every stale cycle.
            ref = book_src if book_src in (ORACLE_BOOTSTRAP_SRC, ORACLE_STALE_SRC) else "oracle"
        bid_px, ask_px = self.quote_prices(mid)

        want_bid, want_ask, notes = True, True, []
        # A price that rounds to <=0 (market at/near one tick) can't be quoted, and dividing usd/px below would
        # raise DivisionByZero BEFORE the per-side try -> bubble to run()'s log-only handler and leave stale
        # quotes resting. Treat a non-positive side as no-quote (mirrors preflight_max_position).
        if bid_px <= 0:
            want_bid = False; notes.append("bid-px<=0")
        if ask_px <= 0:
            want_ask = False; notes.append("ask-px<=0")
        if bid_px >= ask_px:                       # a 0 (or sub-tick) spread rounds our OWN bid >= ask: a locked/crossed
            want_bid = want_ask = False            # pair, not a valid two-sided quote -> quote NEITHER side (an ALO would
            notes.append(f"bid>=ask({bid_px:f}>={ask_px:f})")   # reject the crossing side anyway; risk-off then pulls any resting)

        # Read the position ONCE -- used for both inventory-skew sizing and the guard below. A transport/JSON
        # error here must PULL quotes (fail-closed), not bubble to run()'s log-only handler and leave stale
        # 365-day GTT quotes resting -- matching the pricing path above. (A parseable-but-unknown position
        # already returns None -> handled fail-closed by the guard below.)
        try:
            pos = self.position() if (self.max_position is not None or self.alternate_strategy) else None
        except (OSError, json.JSONDecodeError) as e:
            self._risk_off_on_429(e, "position")   # 429/1015 -> risk-off cancel (no read) + raise; else no-op below
            print(f"[{_ts()}] position read failed ({describe_error(e)}); pulling quotes")
            self.pull_quotes(); return

        # Sizing.
        if self.alternate_strategy:
            # --alternate-strategy: size the REDUCING side to FULLY exit the open position PLUS one usd-amount --
            # a full fill flattens the position and flips it to one usd-amount on the opposite side (the growing
            # side stays a plain usd-amount). So the bot works every position back toward +/- one usd-amount. This
            # is a more aggressive relative of the default 2x skew. --max-position is OPTIONAL here (recommended):
            # WITH it, the cap + collateral + passive guards below bound the growing side (safer); WITHOUT it, the
            # flatten-and-flip sizing is the only inventory control (fine when usd is small and/or the spread is
            # wide). Either way `pos` was read above (the sizing needs it). `pos` is in base units, added directly
            # to the base-unit quantity (not the usd notional).
            bid_qty = to_inc(self.usd / bid_px, self.step, ROUND_FLOOR) if bid_px > 0 else Decimal(0)
            ask_qty = to_inc(self.usd / ask_px, self.step, ROUND_FLOOR) if ask_px > 0 else Decimal(0)
            if pos is not None and pos > 0 and ask_px > 0:      # LONG -> SELL one usd-amount + the whole position (exit + flip short)
                ask_qty = to_inc(self.usd / ask_px + pos, self.step, ROUND_FLOOR)
                notes.append(f"flatten-ask(+{pos})")
            elif pos is not None and pos < 0 and bid_px > 0:    # SHORT -> BUY one usd-amount + the whole position (exit + flip long)
                bid_qty = to_inc(self.usd / bid_px - pos, self.step, ROUND_FLOOR)   # -pos = |pos|, added to the buy
                notes.append(f"flatten-bid(+{-pos})")
        else:
            # DEFAULT inventory skew: past SKEW_THRESHOLD * max_position, quote the REDUCING side SKEW_MULT x
            # larger (growing side stays normal) so inventory mean-reverts toward flat faster.
            bid_usd = ask_usd = self.usd
            if self.max_position is not None and pos is not None:
                skew_at = SKEW_THRESHOLD * self.max_position
                if pos >= skew_at:                         # long at/beyond threshold -> grow the SELL side
                    ask_usd = self.usd * SKEW_MULT; notes.append(f"skew-ask-{SKEW_MULT}x")
                elif pos <= -skew_at:                      # short at/beyond threshold -> grow the BUY side
                    bid_usd = self.usd * SKEW_MULT; notes.append(f"skew-bid-{SKEW_MULT}x")
            bid_qty = to_inc(bid_usd / bid_px, self.step, ROUND_FLOOR) if bid_px > 0 else Decimal(0)
            ask_qty = to_inc(ask_usd / ask_px, self.step, ROUND_FLOOR) if ask_px > 0 else Decimal(0)

        # Inventory guard (fail-closed): account for the PENDING quote size -- a
        # bid fill takes position to pos+bid_qty, an ask fill to pos-ask_qty -- so
        # don't quote a side that could breach +/-max. Unknown position pulls both.
        if self.max_position is not None:
            if pos is None:
                want_bid = want_ask = False; notes.append("position-unknown")
            else:
                # A side that would breach +/-max: DISABLE it only if it's the ADDING side (grows |pos| past the
                # limit -- intentional). If it's the REDUCING side (moves pos toward 0), do NOT pin inventory:
                # SHRINK it to a size that fits (prefer the un-skewed 1x if that fits, so the 2x inventory-skew
                # overshooting the FAR edge can't flip us to the opposite extreme; else the largest step that
                # fits). A reducing side always has room for >= 1 step, so it's never disabled here.
                if pos + bid_qty > self.max_position:              # BID (buy, +qty) would breach +max
                    if pos < 0:                                    # bid REDUCES a short -> shrink, don't disable
                        unskewed = to_inc(self.usd / bid_px, self.step, ROUND_FLOOR) if bid_px > 0 else Decimal(0)
                        fit = to_inc(self.max_position - pos, self.step, ROUND_FLOOR)
                        bid_qty = unskewed if (unskewed > 0 and pos + unskewed <= self.max_position) else fit
                        notes.append(f"reduce-fit-bid({bid_qty})")
                    else:                                          # bid ADDS to a long -> disable
                        want_bid = False; notes.append(f"max-long(pos={pos}+{bid_qty})")
                if pos - ask_qty < -self.max_position:             # ASK (sell, -qty) would breach -max
                    if pos > 0:                                    # ask REDUCES a long -> shrink, don't disable
                        unskewed = to_inc(self.usd / ask_px, self.step, ROUND_FLOOR) if ask_px > 0 else Decimal(0)
                        fit = to_inc(pos + self.max_position, self.step, ROUND_FLOOR)
                        ask_qty = unskewed if (unskewed > 0 and pos - unskewed >= -self.max_position) else fit
                        notes.append(f"reduce-fit-ask({ask_qty})")
                    else:                                          # ask ADDS to a short -> disable
                        want_ask = False; notes.append(f"max-short(pos={pos}-{ask_qty})")

        # Collateral guard (fail-closed): pull both quotes when free collateral is
        # low OR unknown (missing/unparseable).
        if self.min_collateral is not None:
            try:
                fc = self.free_collateral()
            except (OSError, json.JSONDecodeError) as e:
                self._risk_off_on_429(e, "collateral")   # 429/1015 -> risk-off cancel (no read) + raise; else no-op
                print(f"[{_ts()}] collateral read failed ({describe_error(e)}); pulling quotes")
                self.pull_quotes(); return
            if fc is None:
                want_bid = want_ask = False; notes.append("collateral-unknown")
            elif fc < self.min_collateral:
                want_bid = want_ask = False; notes.append(f"low-collateral({fc})")

        # Passive backstop (ALO already enforces this server-side): never quote
        # at/through the opposite top-of-book. Only check a side that EXISTS -- on
        # the oracle-fallback path one side of the book may be empty.
        if best_ask is not None and bid_px >= best_ask:
            want_bid = False; notes.append("bid-not-passive")
        if best_bid is not None and ask_px <= best_bid:
            want_ask = False; notes.append("ask-not-passive")
        if bid_qty <= 0:
            want_bid = False
        if ask_qty <= 0:
            want_ask = False

        try:
            live = self.live_quotes()
        except (OSError, json.JSONDecodeError, ValueError) as e:   # ValueError = malformed openOrders body
            self._risk_off_on_429(e, "openOrders")   # 429/1015 -> risk-off cancel (no read) + raise; else no-op below
            print(f"[{_ts()}] openOrders read failed ({describe_error(e)}); pulling quotes")
            self.pull_quotes(); return
        # Reconcile the CACHED openOrders against local truth before deciding place-vs-modify: if a side we
        # BELIEVE is placed (last_quote set) is MISSING from the cached set, the cache may just be STALE -- a
        # blind re-place would hit DUPLICATE_CLIENT_ID (reject storm) and leave the quote un-repriced. Re-read
        # ONCE, uncached, to get the authoritative set. Fires ONLY on a real discrepancy -> no extra read on the
        # normal path; and it turns a would-be re-place into a correct MODIFY when the order actually still rests.
        hold_place = set()   # cids whose place-vs-modify we could NOT confirm this cycle (the fresh re-read failed):
                             # HOLD their place (a blind place risks DUPLICATE), but still run the loop so the OTHER
                             # side's risk-off cancel is NOT skipped (a safety action that must not wait a whole cycle).
        if ((want_bid and self.bid_cid not in live and self.last_quote.get(self.bid_cid) is not None)
                or (want_ask and self.ask_cid not in live and self.last_quote.get(self.ask_cid) is not None)):
            try:
                live = self.live_quotes(fresh=True)
            except (OSError, json.JSONDecodeError, ValueError) as e:
                _raise_if_rate_limited(e)   # 429 -> back off, LEAVE quotes resting
                # Can't confirm place-vs-modify -> HOLD the place on the unconfirmed side(s), but do NOT skip the
                # WHOLE cycle: a risk-off cancel on the other side (want=False at max-position / low-collateral)
                # must still fire this cycle, else a resting quote could fill and breach the limit while we wait.
                print(f"[{_ts()}] openOrders reread failed ({describe_error(e)}); holding place on unconfirmed side(s), still running risk-off cancels")
                if want_bid and self.bid_cid not in live and self.last_quote.get(self.bid_cid) is not None:
                    hold_place.add(self.bid_cid)
                if want_ask and self.ask_cid not in live and self.last_quote.get(self.ask_cid) is not None:
                    hold_place.add(self.ask_cid)
        actions = []
        for cid, oside, sside, px, qty, want in (
            (self.bid_cid, "BUY", ordersign.SIDE_BUY, bid_px, bid_qty, want_bid),
            (self.ask_cid, "SELL", ordersign.SIDE_SELL, ask_px, ask_qty, want_ask),
        ):
            risk_off_cancel = False        # set True in the risk-off cancel branch: a failure THERE must KEEP last_quote (N7)
            try:
                if want and cid in live:
                    order_id, resting, resting_px = live[cid]
                    # keep ONLY when the ACTUAL resting price AND size match intended. We compare the exchange's
                    # real resting price (not local last_quote) so a quote whose resting price drifted -- stale
                    # tickSize, an external/partial modify, a future refactor -- is re-priced, not silently kept.
                    # And requiring resting == qty restores size filled away by a partial fill (comparing intended
                    # alone froze size until mid moved). qty is the SAME skew-adjusted size the max-position guard
                    # bounded above, so a restoring modify can never breach +/-max (a breaching side had want=False).
                    if resting_px == px and resting == qty:
                        self.last_quote[cid] = (px, qty)       # RECORD the adopted resting quote: keep otherwise never
                        actions.append(f"{oside}:keep")        # sets last_quote, so after a restart/cleared state the
                                                               # stale-cache reconcile had nothing to trigger on ->
                                                               # a stale cache -> blind re-place -> DUPLICATE_CLIENT_ID
                    else:
                        self.modify_quote(oside, sside, cid, px, qty, order_id)
                        self.last_quote[cid] = (px, qty)
                        actions.append(f"{oside}:modify {qty:f}@{px:f}")
                elif want:
                    if cid in hold_place:                       # place-vs-modify UNCONFIRMED (fresh re-read failed) ->
                        actions.append(f"{oside}:hold(openOrders unconfirmed)")   # HOLD, don't risk a DUPLICATE place;
                    else:                                       # leave last_quote (we believe it's placed) -> next cycle retries
                        self.place_quote(oside, sside, cid, px, qty)
                        self.last_quote[cid] = (px, qty)
                        actions.append(f"{oside}:place {qty:f}@{px:f}")
                elif cid in live or self.last_quote.get(cid) is not None:
                    # Pull a side we're NOT quoting (risk-off) that is resting OR that we BELIEVE is placed
                    # (last_quote set) even if a STALE cache doesn't show it -- cancel_quote is by clientId
                    # (cache-independent), and a spurious cancel of a not-resting side is a harmless not-found.
                    # Closes the risk-off/stale-cache gap: a resting bid at max-long must be pulled or it could
                    # fill and breach the limit. (No fresh read needed here, unlike the place-vs-modify path.)
                    risk_off_cancel = True                 # a failure below KEEPS last_quote (retry the cancel next cycle)
                    self.cancel_quote(cid)
                    self.last_quote[cid] = None            # confirmed pulled -> forget it
                    actions.append(f"{oside}:cancel(guard)")
                else:
                    self.last_quote[cid] = None
                    actions.append(f"{oside}:skip")
            except (OSError, json.JSONDecodeError, SystemExit) as e:
                # A 429 on a write means we're throttled: abort the cycle and back off rather than keep
                # firing order requests at the edge limiter. We raise BEFORE touching last_quote, so it
                # keeps reflecting the order ACTUALLY resting (the failed op did not apply) -- next cycle
                # re-derives the desired price and reconciles cleanly. (Approved 2026-07-27.)
                _raise_if_rate_limited(e)
                # Non-429 failure -- OSError/JSON = transport/HTTP-error (incl. 4xx rejects); SystemExit =
                # check_order_response flagged a 2xx REJECTED/ERROR body. What we do with last_quote depends
                # on WHICH op failed:
                if risk_off_cancel:
                    # A FAILED risk-off cancel: the quote may STILL rest, so KEEP last_quote -- next cycle's
                    # cancel(guard) fires again (cid in live OR last_quote set) instead of a stale empty cache
                    # letting it skip and leave a 365-day GTT quote resting past the max-position/collateral
                    # limit. (N7 -- same rule as pull_quotes: only clear on a CONFIRMED cancel.)
                    actions.append(f"{oside}:cancel(guard) FAILED {describe_error(e)}; keeping last_quote to retry")
                else:
                    # A failed PLACE/MODIFY: LEAVE last_quote as-is (do NOT clear). A modify targets a RESTING
                    # order, so a failed modify leaves the original resting; a failed PLACE we believed was placed
                    # (last_quote set -- e.g. a DUPLICATE_CLIENT_ID reject because the order actually still rests)
                    # also still rests. Clearing would drop the stale-cache reconcile protection -> next cycle
                    # a blind re-place -> DUPLICATE. Keeping SELF-HEALS: next cycle's reconcile does a fresh re-read
                    # and correctly modifies-or-places. Never fatal (a transient reject re-quotes next loop).
                    if self.last_quote.get(cid) is None:
                        # COLD-PLACE self-heal: a place we believed was NEW (last_quote None) but that REJECTED --
                        # most importantly DUPLICATE_CLIENT_ID, meaning the order ACTUALLY RESTS (a prior run's
                        # 365-day GTT quote under this clientId, or a place that applied at the venue but read back
                        # as failed). Leaving last_quote None left the reconcile guard (which fires ONLY when
                        # last_quote is set) UNARMED, so every cycle re-placed into DUPLICATE until the cache
                        # happened to show it. RECORD the intended quote so next cycle's fresh re-read reconciles:
                        # it ADOPTS/MODIFIES if the order rests, or (a fresh read that shows it does NOT rest)
                        # re-places cleanly. Safe for a genuine new-place failure too -- the fresh read is authoritative.
                        self.last_quote[cid] = (px, qty)
                    actions.append(f"{oside}:ERR {describe_error(e)} (last_quote kept)")
        tail = ("  [" + " ".join(notes) + "]") if notes else ""
        # Machine-readable dashboard suffix, appended LAST (after any notes): [bidsize,bidprice,askprice,asksize,maxpos].
        # Values are the CURRENT resting quote per side from last_quote (EMPTY if that side is pulled: skip/cancel ->
        # last_quote None); maxpos names the side capped by --max-position (bid = max-long, ask = max-short), else empty.
        # Consumed by dydxv4_mm_dashboard; the dashboard takes the LAST [...] on the line, so it's unambiguous vs `tail`.
        bq = self.last_quote.get(self.bid_cid); aq = self.last_quote.get(self.ask_cid)   # each: (px, qty) or None
        bsz, bpx = (f"{bq[1]:f}", f"{bq[0]:f}") if bq else ("", "")
        asz, apx = (f"{aq[1]:f}", f"{aq[0]:f}") if aq else ("", "")
        maxpos = ("bid" if any(n.startswith("max-long") for n in notes)
                  else "ask" if any(n.startswith("max-short") for n in notes) else "")
        print(f"[{_ts()}] mid={mid:.4f}({ref})  " + "  ".join(actions) + tail
              + f"  [{bsz},{bpx},{apx},{asz},{maxpos}]")

    def _pull_after_error(self):
        """Fail-closed pull for run()'s cycle-error handlers: an exception that escaped cycle() means
        reconciliation didn't finish, so quotes may be resting at stale prices (Arcus GTT orders are
        365-day -- they do NOT self-expire). Cancel them rather than log-and-leave. Wrapped so a pull
        failure during error handling can't itself crash the run loop (pull_quotes is best-effort but
        belt-and-suspenders)."""
        try:
            self.pull_quotes()
        except Exception as e:
            print(f"  pull after cycle error failed: {describe_error(e)}", flush=True)

    def _sleep_responsive(self, duration):
        """Sleep up to `duration` seconds in <=0.5s slices, waking immediately when RUNNING clears
        (SIGINT/SIGTERM), so a backoff or the inter-cycle wait never delays a clean shutdown."""
        slept = 0.0
        while RUNNING and slept < duration:
            time.sleep(min(0.5, duration - slept))
            slept += 0.5

    def _seed_last_quote(self):
        """STARTUP: seed last_quote from any of THIS bot's currently-resting orders (matched by clientId in
        live_quotes), so cycle 1's stale-cache reconcile is already ARMED. Without it a fresh process has
        last_quote None, so a STALE cache in the FIRST cycle could blind re-place a still-resting quote ->
        DUPLICATE_CLIENT_ID (finding-1's keep only arms it from a cycle that actually saw the order, i.e. cycle
        2+). Best-effort: a failed/blocked read just leaves last_quote empty (== prior behavior), never worse."""
        try:
            live = self.live_quotes(fresh=True)   # uncached, clientId-scoped, orderId/size/price validated
        except (OSError, json.JSONDecodeError, ValueError) as e:
            print(f"[{_ts()}] startup: could not read open orders to seed last_quote ({describe_error(e)}); "
                  f"cycle 1 reconcile falls back to the cache", flush=True)
            return
        for cid, (_oid, remaining, price) in live.items():
            self.last_quote[cid] = (price, remaining)
            print(f"[{_ts()}] startup: adopted resting {cid} @ {price:f} x {remaining:f} (last_quote seeded)", flush=True)

    def run(self, interval, cycles):
        self._seed_last_quote()   # arm cycle-1's reconcile from already-resting quotes (finding-1 first-cycle gap)
        n = 0
        while RUNNING:
            # Periodic clock-skew re-check: drift grows over a long run and NTP can step. On excessive
            # skew, STOP quoting -- main() then runs shutdown(), which pulls quotes fail-closed -- rather
            # than keep signing out-of-window place/modify/cancel. (A failed check reschedules sooner.)
            if time.monotonic() >= self._next_clock_check:
                try:
                    self.refresh_clock_delta()
                except SystemExit as e:
                    print(f"[{_ts()}] {e}", flush=True)
                    break
            # Don't quote until we've confirmed the clock at least once: if /v1/time was down at startup,
            # clock_delta_ns is still 0 and a skewed host would sign place/modify/cancel outside the venue's
            # auth window (all 401). Fail-CLOSED (place nothing) and keep retrying on the CLOCK_RETRY_S cadence
            # above, rather than quote blind or hard-exit on a transient outage. One-way latch, so this only
            # gates the STARTUP window -- once sampled it never re-engages.
            if not self._clock_sampled:
                wait = max(1.0, self._next_clock_check - time.monotonic())
                print(f"[{_ts()}] no server clock sample yet (/v1/time unavailable); not quoting, "
                      f"retrying in ~{wait:.0f}s", flush=True)
                self._sleep_responsive(wait)
                continue
            n += 1
            try:
                self.cycle()
            except RateLimited as e:
                # Throttled (Arcus app 429 or Cloudflare edge 1015): NOT a stale-pricing failure, so we do
                # NOT pull -- pulling would fire more throttled requests and deepen the storm. Leave the
                # POST-ONLY/ALO 365-day GTT quotes resting and honor Retry-After before the next cycle
                # (the backoff is ON TOP of the normal inter-cycle wait below). MUST precede the OSError/
                # Exception handlers: RateLimited is an Exception subclass they would otherwise swallow.
                print(f"[{_ts()}] rate limited ({e.detail}); backing off {e.retry_after:g}s, "
                      f"leaving quotes resting", flush=True)
                self._sleep_responsive(e.retry_after)
            except (OSError, json.JSONDecodeError) as e:
                # A transport error that escaped cycle()'s own fail-closed paths -> pull, then continue.
                print(f"[{_ts()}] cycle error: {describe_error(e)}; pulling quotes", flush=True)
                self._pull_after_error()
            except Exception as e:
                # Catch-all: an UNEXPECTED error means the cycle didn't finish reconciling, so quotes may
                # rest at stale prices. Fail CLOSED (pull), then continue so a transient bug doesn't leave
                # 365-day GTT quotes resting cycle after cycle.
                print(f"[{_ts()}] cycle error: {describe_error(e)}; pulling quotes", flush=True)
                self._pull_after_error()
            if cycles and n >= cycles:
                break
            self._sleep_responsive(interval)

    def shutdown(self, retries=3):
        """Cancel both quotes and CONFIRM they're gone via a FRESH (uncached) openOrders read,
        retrying any that still rest. Arcus quotes are GTT -- they do NOT self-expire -- so a
        silently-failed cancel can leave an order resting indefinitely; fail closed (nonzero exit)
        if cancellation can't be confirmed, so the operator knows to clean up manually."""
        print("shutting down -- canceling quotes...")
        remaining = None                                    # None = unknown; else list of resting cids
        for attempt in range(1, retries + 1):
            targets = remaining if remaining is not None else [self.bid_cid, self.ask_cid]
            for cid in targets:
                try:
                    self.cancel_quote(cid, risk_off=True)
                except (OSError, json.JSONDecodeError, SystemExit) as e:   # SystemExit = 2xx REJECTED/ERROR body
                    print(f"  {cid}: cancel error: {describe_error(e)}")    # the fresh openOrders read below is the source of truth
            # Confirm via a FRESH read (NOT the cache -- it could be stale and falsely show gone).
            try:
                body = self.call("GET", f"/v1/openOrders?{self.query}")
                if not isinstance(body, dict):
                    raise ValueError(f"openOrders body is {type(body).__name__}, not an object")
                if "orders" not in body:
                    raise ValueError("openOrders body has no 'orders' key")   # {} -> can't confirm cleanup -> retry/fail closed
                orders = body.get("orders")
                if not isinstance(orders, list) or not all(isinstance(o, dict) for o in orders):
                    raise ValueError("openOrders 'orders' is not a list of objects")
                remaining = [o.get("clientId") for o in orders if self._order_is_ours(o)]
            except (OSError, json.JSONDecodeError, ValueError) as e:   # ValueError = malformed openOrders body
                print(f"  could not confirm cancellation (openOrders read failed: {describe_error(e)})")
                remaining = None
                continue
            if not remaining:
                print("  all quotes canceled (confirmed gone).")
                return
            print(f"  still resting: {remaining}; retrying ({attempt}/{retries})")
        # Exhausted retries without confirming a clean cancel -> FAIL CLOSED.
        if remaining:
            raise SystemExit(f"market_maker: WARNING could not confirm cancellation of {remaining} -- these "
                             f"GTT orders may STILL BE RESTING. Cancel manually: "
                             f"cancel_order.py --clientid <id> --{self.net}")
        raise SystemExit("market_maker: could not confirm cancellation (openOrders unreadable); "
                         "verify open orders manually.")


# ── CLI ────────────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Two-sided post-only market-maker loop for Arcus.")
    p.add_argument("usd", help="USD to quote per side (> 0)")
    p.add_argument("spread", help="half-spread off mid, fraction in [0, 1) (e.g. 0.03 = 3%%)")
    p.add_argument("--market", default="BTC-USD", help="market display name (default BTC-USD)")
    p.add_argument("--interval", type=float, default=15, help="refresh seconds (> 0, default 15)")
    p.add_argument("--cycles", type=int, default=0, help="stop after N cycles (>= 0; 0 = forever)")
    p.add_argument("--rate-limit-buffer", type=int, default=100, metavar="N",
                   help="proactive rate-limit pacing: once a per-subaccount WRITE pool's remaining tokens drop "
                        "below N, ease off write cadence (ramp toward the venue's 1-action/10s drip) instead of "
                        "slamming into 429s; 0 disables. Default 100. A whitelisted/high-volume account reports "
                        "remaining=-1 (layer not enforced) and is never throttled.")
    p.add_argument("--max-position", help="cap |position| in base units; stop growing past it. ALSO enables "
                                          "inventory-skew: reducing side quotes 2x usd once |pos| >= 50%% of this")
    p.add_argument("--min-collateral", help="pull quotes when freeCollateral < this (USD)")
    p.add_argument("--cache-ttl", type=int, default=5,
                   help="Redis TTL (s) for shared account-wide reads (openOrders/positions/account/markets); "
                        "default 5. Must be < --interval. See arcus_redis.py / account_poller.py")
    p.add_argument("--no-cache", action="store_true",
                   help="bypass the Redis account cache; fetch every account-wide read live")
    p.add_argument("--log-dir", default=None,
                   help=f"dir for the liveness marker pids/ subdir (default: {LOG_BASE} with /<network> appended "
                        f"-- same place the WS tools and ws_liveness_check.py use)")
    p.add_argument("--use-redis-bbo", action="store_true",
                   help="derive best bid/ask from the local wsorderbook's Redis BBO feed "
                        "(arcus:<net>:bbo:<market>, age-guarded on ts) instead of a per-cycle REST "
                        "/v1/l2OrderBook; falls back to REST when the feed is stale/missing. Requires a "
                        "wsorderbook publishing BBO for this market.")
    p.add_argument("--disable-fallback", action="store_true",
                   help="with --use-redis-bbo: when the Redis BBO feed is stale/missing, do NOT fall back to "
                        "the REST /v1/l2OrderBook. Instead pull quotes to flat (jittered, by clientId) and wait "
                        "for the feed to refresh -- so a whole fleet on a network outage can't flood REST and "
                        "trip the rate limit. Requires --use-redis-bbo (no Redis feed = nothing to fall back FROM).")
    p.add_argument("--oracle-when-stale", action="store_true",
                   help="with --use-redis-bbo: when the Redis BBO feed is stale/empty, quote off the CACHED oracle "
                        "every cycle instead of parking (--disable-fallback) or hitting REST. Keeps a sole-LP / "
                        "thin market (common on testnet) alive: reads the wsexchange-warmed markets cache, so it "
                        "costs no REST l2OrderBook and no new WS connection. Requires --use-redis-bbo; takes "
                        "precedence over --disable-fallback's park and the REST fallback on a stale feed.")
    p.add_argument("--alternate-strategy", action="store_true",
                   help="aggressive flatten-and-flip sizing: instead of the default 2x inventory skew, size the "
                        "REDUCING side to fully exit the open position PLUS one usd-amount, so a full fill flattens "
                        "the position and flips it to one usd-amount on the opposite side. --max-position is optional "
                        "but RECOMMENDED: with it the cap still bounds the growing side; without it the flatten-and-flip "
                        "sizing is the only inventory control (reasonable when usd is small and/or the spread is wide).")
    add_network_args(p)
    a = p.parse_args()

    a.usd = positive_decimal(a.usd, "usd")
    a.spread = positive_decimal(a.spread, "spread", allow_zero=True)
    if a.spread >= 1:
        raise SystemExit("spread: must be < 1 (a fraction, e.g. 0.03 for 3%).")
    if not math.isfinite(a.interval) or a.interval <= 0:
        raise SystemExit("--interval: must be a finite value > 0.")
    if a.cycles < 0:
        raise SystemExit("--cycles: must be >= 0.")
    if a.cache_ttl < 1:
        raise SystemExit("--cache-ttl: must be >= 1.")
    if a.rate_limit_buffer < 0:
        raise SystemExit("--rate-limit-buffer: must be >= 0 (0 disables proactive pacing).")
    if a.cache_ttl >= a.interval:
        print(f"WARNING: --cache-ttl {a.cache_ttl}s >= --interval {a.interval}s; the cache may not refresh "
              f"each cycle (and a bot may not see its own just-placed orders). Use a TTL below the interval.")
    a.max_position = positive_decimal(a.max_position, "--max-position") if a.max_position is not None else None
    a.min_collateral = positive_decimal(a.min_collateral, "--min-collateral", allow_zero=True) if a.min_collateral is not None else None
    if a.disable_fallback and not a.use_redis_bbo:
        p.error("--disable-fallback requires --use-redis-bbo (it disables the Redis-BBO stale/missing REST "
                "fallback; with no Redis feed there is nothing to fall back FROM, so the flag is meaningless).")
    if a.oracle_when_stale and not a.use_redis_bbo:
        p.error("--oracle-when-stale requires --use-redis-bbo (it changes what happens when the Redis BBO feed "
                "is stale; with no Redis feed there is no stale-feed case, so the flag is meaningless).")
    return a


def fetch_startup_markets(args, attempts=10):
    """Startup /v1/markets with retry + JITTERED backoff. A cold-cache mass launch can make 35
    bots hit the heavy /v1/markets at once and some time out; rather than die on the spot (the
    old call()->SystemExit), retry so the bot rides out the burst -- and likely hits a now-warm
    cache on the next try. Uses request() (raises, so failures are catchable) and serves a warm
    cache instantly. Jitter is important: without it 35 bots would retry in lockstep and re-herd."""
    def fetch():
        return request("GET", "/v1/markets")
    for i in range(1, attempts + 1):
        try:
            data = fetch() if args.no_cache else account_cache.cached_get(args.network, None, "markets", fetch, args.cache_ttl)
        except (OSError, json.JSONDecodeError) as e:    # URLError/timeouts are OSError subclasses
            transport, reason = True, describe_error(e)
        else:
            # Shape backstop at the sole startup consumer of /v1/markets. is_cacheable is the shared write bar
            # (poller + cached_get), and cached_get also REVALIDATES hits -- so a bad resident Redis blob is now
            # treated as a miss and refetched, never served back as a hit. We still validate HERE because:
            #   (1) --no-cache bypasses cached_get entirely (live body, no cache bar), and
            #   (2) cached_get returns the live body even when it refuses to WRITE a non-cacheable one -- so a
            #       briefly-bad live 2xx must not reach resolve_market (`for m in markets` / m.get(...)).
            # Reuse is_cacheable("markets", ...) so this stays exactly as strict as the write bar (list OF DICTS),
            # and fail cleanly (SystemExit) rather than crash with AttributeError/TypeError inside resolve_market.
            # A bad shape is a SOFT, RETRYABLE error like a transport failure (see the loop) -- it can be transient
            # (API/proxy glitch, mid-deploy partial body, briefly-bad live refetch) -- but a permanently bad API
            # still exits after `attempts`.
            if account_cache.is_cacheable("markets", data):
                return data
            transport, reason = False, "unexpected response (markets missing or not a list of dicts)"

        if i == attempts:            # every attempt bad -> fail CLOSED (never quote without market metadata)
            if transport:
                raise SystemExit(f"startup /v1/markets failed after {attempts} attempts: {reason}")
            raise SystemExit(f"startup /v1/markets: {reason} after {attempts} attempts; "
                             f"clear the shared markets cache or check the API.")
        delay = min(2 ** (i - 1), 30) * random.uniform(0.5, 1.5)   # shape + transport share one budget/backoff
        print(f"startup /v1/markets {reason}; retry {i}/{attempts - 1} in {delay:.1f}s", flush=True)
        time.sleep(delay)


def main():
    args = parse_args()
    select_network(args.network)
    creds = load_creds()
    # Startup market resolution goes through the shared markets cache (poller-warmed, or the
    # first bot fetches and the rest hit it) with retry+backoff, so a mass launch neither fires
    # 35x /v1/markets nor dies when the burst times out.
    markets = fetch_startup_markets(args).get("markets", [])
    mkt = resolve_market(markets, args.market)
    if mkt is None:
        raise SystemExit(f"Unknown market {args.market!r}.")

    # Liveness marker so ws_liveness_check.py (via checkliveness.sh) notices if this MM dies. Best-effort (never
    # raises). Goes in the SAME <log_base>/<network>/pids the WS tools use, so the checker finds it with no extra
    # config. Uses the canonical marketDisplayName. NOTE: an intentionally-stopped MM leaves a dead-PID marker
    # that reads as DEAD -- remove its marker by hand when retiring a bot (a same-market restart sweeps its own).
    log_dir = args.log_dir if args.log_dir else os.path.join(LOG_BASE, args.network)
    write_pidfile(log_dir, f"market_maker-{mkt.get('marketDisplayName', args.market)}")

    mm = MarketMaker(args, creds, mkt)
    # Strategy descriptor: alternate-strategy prints with OR without a cap (--max-position optional there); the
    # default 2x skew only has anything to say when a cap is set (capless default = plain symmetric quoting).
    if mm.alternate_strategy:
        cap = (f"max-pos={mm.max_position}, growing side capped" if mm.max_position is not None
               else "no max-position -> uncapped (flatten-and-flip is the only inventory control)")
        strat_desc = f"  alternate-strategy: reducing side = position + one usd-amount (flatten-and-flip); {cap}"
    elif mm.max_position is not None:
        strat_desc = f"  max-pos={mm.max_position} (skew {SKEW_MULT}x reducing side at |pos|>={SKEW_THRESHOLD * mm.max_position})"
    else:
        strat_desc = ""
    print(f"market-maker: {mm.market} (id {mm.market_id})  ${mm.usd}/side  spread +/-{mm.spread * 100:.2f}%  "
          f"every {args.interval}s  tick={mm.tick} step={mm.step}  TIF={QUOTE_TIF}"
          + strat_desc
          + (f"  min-collat={mm.min_collateral}" if mm.min_collateral is not None else ""))
    if mm.use_redis_bbo:
        src = (f"  price source: Redis BBO 'arcus:{mm.net}:bbo:{mm.market}' "
               f"(age-guard {REDIS_BBO_MAX_AGE}s)")
        # Reflect the ACTUAL stale-feed behaviour (the old text claimed a REST fallback unconditionally, which
        # masked why a bot sits parked). --oracle-when-stale wins over --disable-fallback (checked first).
        if mm.oracle_when_stale:
            tail = " — on a stale/empty feed: quote off the cached oracle every cycle (--oracle-when-stale; no REST, no park)"
        elif mm.disable_fallback:
            tail = " — NO REST fallback (--disable-fallback): one-time oracle seed of an empty market, then pull-to-flat + wait when the feed is stale/missing"
        else:
            tail = " → REST /v1/l2OrderBook fallback"
        print(src + tail)

    mm.preflight_max_position()
    mm.refresh_clock_delta(startup=True)   # abort now if the host clock is too skewed to sign in-window

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    mm.run(args.interval, args.cycles)
    mm.shutdown()


if __name__ == "__main__":
    main()
