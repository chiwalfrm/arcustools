"""
Shared helpers for the Arcus private CLIs (order/signing, trading, and monitoring):
place_order.py, modify_order.py, cancel_order.py, close_position.py, market_maker.py, account_poller.py, marketdata_monitor.py.

Lives in private/ next to ordersign.py / arcus_redis.py / arcus_creds_<network>.json and
resolves them relative to ITS OWN location, so importers work from any cwd.
(The public/ read-only tools -- show*/ws*/display* -- share arcus_common_public instead;
they have no ordersign/creds coupling.)
"""

import contextlib
import io
import json
import math
import os
import re
import stat
import sys
import time
import urllib.error
import urllib.parse  # noqa: F401  (re-exported convenience for importers)
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR

import requests   # keep-alive HTTP (a process-wide Session) so repeated calls REUSE one TCP+TLS connection

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
import ordersign

CLIENT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,36}$")
ADDR_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")

# ── Network selection ─────────────────────────────────────────────────────────
# The Arcus REST API base per network -- the toolkit's canonical map, in its logical home (the network
# module). The private helpers/tools import it FROM here (arcus_redis, marketdata_monitor); the public
# tools keep their own copy in arcus_common_public (the public/private boundary forbids sharing one).
NETWORKS = {
    "testnet": "https://api.testnet.arcus.xyz",
    "staging": "https://api.staging.arcus.xyz",
    "mainnet": "https://api.arcus.xyz",
}
# One of these MUST be chosen per invocation (see add_network_args, which makes
# --testnet/--staging/--mainnet a required mutually-exclusive selector). The network
# flag is REQUIRED by design -- there is NO default, so a command can never hit the
# wrong network (e.g. trade on mainnet) by omission.
NETWORK = None        # set by select_network()
BASE = None           # set by select_network() -> NETWORKS[NETWORK]
CREDS_PATH = None     # set by select_network() -> arcus_creds_<network>.json


# ── liveness marker (pid-file) ───────────────────────────────────────────────────────────────────────────
# A long-running program drops a marker  <log_dir>/pids/<name>-<PID>  at startup so a monitor
# (ws_liveness_check.py) can tell whether it is still running. VERBATIM cross-tier copy of
# arcus_common_public.{pid_alive,write_pidfile} -- the public/private boundary forbids importing across it, so
# keep these two in lockstep with the public copy AND with the marker format the liveness checker parses
# (filename <name>-<PID>; body pid=/name=/started=/argv=). The private long-running tool that uses this is
# market_maker (its WS siblings live in public/ and use the public copy).
def pid_alive(pid):
    """True if a process with `pid` currently exists (best-effort, POSIX)."""
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return False
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True                              # exists, owned by another user
    except OSError:
        return False
    return True


def write_pidfile(log_dir, name):
    """Drop a liveness marker  <log_dir>/pids/<name>-<PID>  and return its path (None on failure).

    `name` must identify the program AND its instance (e.g. 'market_maker-BTC-USD') so two instances don't
    collide. Best-effort: any error is warned to stderr and swallowed -- a monitoring aid must never keep the
    program from starting. We deliberately DON'T delete the marker on exit (a dead-PID marker IS the "it died"
    signal); instead we sweep STALE same-name markers whose PID is already dead at startup.
    """
    try:
        pid = os.getpid()
        pid_dir = os.path.join(log_dir, "pids")   # keep markers out of the cluttered log dir
        os.makedirs(pid_dir, exist_ok=True)
        prefix = name + "-"
        keep = "%s%d" % (prefix, pid)
        for fn in os.listdir(pid_dir):           # sweep stale same-name markers left by prior DEAD instances
            if (fn.startswith(prefix) and fn != keep
                    and fn[len(prefix):].isdigit() and not pid_alive(fn[len(prefix):])):
                try:
                    os.remove(os.path.join(pid_dir, fn))
                except OSError:
                    pass
        path = os.path.join(pid_dir, keep)
        with open(path, "w") as fh:
            fh.write("pid=%d\nname=%s\nstarted=%s\nargv=%s\n" % (
                pid, name, datetime.now(timezone.utc).isoformat(timespec="seconds"), " ".join(sys.argv)))
        return path
    except Exception as e:                        # never let a monitoring aid break startup
        print("[pidfile] WARNING could not write pid-file for %r in %r: %s" % (name, log_dir, e),
              file=sys.stderr)
        return None


def add_network_args(parser):
    """Register the REQUIRED, mutually-exclusive --testnet/--staging/--mainnet selector.

    REQUIRED by design (no default): mainnet is live, so an omitted flag must never
    silently pick a network -- the caller states it explicitly. Sets args.network to
    the network string.
    """
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--testnet", dest="network", action="store_const", const="testnet",
                   help="use the testnet server + arcus_creds_testnet.json")
    g.add_argument("--staging", dest="network", action="store_const", const="staging",
                   help="use the staging server + arcus_creds_staging.json")
    g.add_argument("--mainnet", dest="network", action="store_const", const="mainnet",
                   help="use the mainnet server + arcus_creds_mainnet.json")
    return parser


def select_network(network):
    """Resolve `network` into the module globals BASE/CREDS_PATH used by
    request()/load_creds(). Call once, right after parse_args()."""
    global NETWORK, BASE, CREDS_PATH
    if network not in NETWORKS:
        raise SystemExit(f"unknown network {network!r}; expected one of {sorted(NETWORKS)}.")
    NETWORK = network
    BASE = NETWORKS[network]
    CREDS_PATH = os.path.join(_HERE, f"arcus_creds_{network}.json")
    return network


# ── Credentials ──────────────────────────────────────────────────────────────
def load_creds():
    """Load arcus_creds_<network>.json (next to this module) with clean errors."""
    if CREDS_PATH is None:
        raise SystemExit("no network selected; pass --testnet, --staging, or --mainnet.")
    try:
        # This file holds a fund-controlling Ed25519 api_private_key. A group/world-readable copy is
        # a real exposure -- FAIL CLOSED unless the operator explicitly opts out (e.g. a locked-down
        # shared box where perms can't be 600). generate_arcus_creds.sh writes 600; this enforces it.
        if os.stat(CREDS_PATH).st_mode & (stat.S_IRWXG | stat.S_IRWXO):
            if os.environ.get("ARCUS_ALLOW_INSECURE_CREDS") == "1":
                print(f"WARNING: {CREDS_PATH} is group/world-accessible "
                      "(allowed via ARCUS_ALLOW_INSECURE_CREDS=1).", file=sys.stderr)
            else:
                raise SystemExit(f"{CREDS_PATH} is group/world-accessible -- refusing to read a private "
                                 f"key from it.\n  Lock it:  chmod 600 {CREDS_PATH}\n"
                                 f"  Override: set ARCUS_ALLOW_INSECURE_CREDS=1 to read it anyway.")
        with open(CREDS_PATH) as f:
            creds = json.load(f)
    except FileNotFoundError:
        raise SystemExit(f"missing {CREDS_PATH} (create it with generate_arcus_creds.sh).")
    except json.JSONDecodeError as e:
        raise SystemExit(f"invalid JSON in {CREDS_PATH}: {e}")
    if not isinstance(creds, dict):   # a JSON list/str/number/null top level would else TypeError on the `k not in creds` /
        raise SystemExit(f"{CREDS_PATH}: expected a JSON object, got {type(creds).__name__}.")   # creds[...] access below
    for k in ("eth_address", "account_index", "api_private_key"):
        if k not in creds:
            raise SystemExit(f"{CREDS_PATH}: missing required field {k!r}.")
    # Validate shapes here -- this feeds signing, so catch bad creds early rather
    # than fail deep inside signing or send a malformed request body.
    if not ADDR_RE.match(str(creds["eth_address"])):
        raise SystemExit(f"{CREDS_PATH}: eth_address must be 0x + 40 hex chars.")
    ai = creds["account_index"]
    if isinstance(ai, bool) or not isinstance(ai, int) or ai < 0:
        raise SystemExit(f"{CREDS_PATH}: account_index must be a non-negative integer.")
    try:
        if len(bytes.fromhex(str(creds["api_private_key"]))) < 32:
            raise ValueError
    except ValueError:
        raise SystemExit(f"{CREDS_PATH}: api_private_key must be hex encoding >= 32 bytes (Ed25519).")
    return creds


# Definitive-failure order statuses on the synchronous 200 path. The place/cancel/modify endpoints
# are ASYNC: 202 -> status ACK / CANCEL_ACKNOWLEDGED (accepted; lifecycle on the orders WS), 200 ->
# the gateway already had definitive state, which CAN be REJECTED/ERROR (a 2xx HTTP code carrying a
# FAILURE body), 400 -> HTTPError (request() raises). The HTTP layer catches the 400s; this catches
# the 200-with-failure-body case so a rejected order/cancel isn't mistaken for success.
FAILED_ORDER_STATUSES = frozenset({"REJECTED", "ERROR"})

# For a CANCEL, "the order is not on the book" is the DESIRED end-state, not a failure. A cleanly-gone or
# never-placed clientId returns 202 CANCEL_ACKNOWLEDGED (already fine -- not a FAILED status), but a clientId
# whose order FILLED mid-modify-cycle (arcus modify = cancel-replace) comes back 200/REJECTED with one of these
# reasons instead of the clean ACK. On the CANCEL path treat them as IDEMPOTENT SUCCESS, so a caller (notably
# market_maker's cancel(guard) / pull_quotes) doesn't spin retrying a cancel of an order that is already gone
# -- the exact ORDER_NOT_FOUND_FOR_MODIFY retry-loop seen when the flatten-and-flip fill that hit --max-position
# is the very quote the inventory guard then tries to pull. ONLY the cancel path: the same reason on a
# modify/place is a real failure and still raises.
CANCEL_BENIGN_REASONS = frozenset({"ORDER_NOT_FOUND", "ORDER_NOT_FOUND_FOR_MODIFY"})


def check_order_response(resp, what="order", cancel=False):
    """Raise SystemExit if a place/cancel/modify response body reports a definitive failure
    (status REJECTED/ERROR on the 200 path). Returns resp otherwise (incl. the common 202 ACK).
    A NON-DICT body can't be status-checked, so treat it as a FAILURE (fail-closed) rather than
    silently returning it as success -- a mangled/unexpected 2xx must never read as a confirmed order.
    (request() normalizes an empty body to {}, so a legitimate empty ACK is still a dict and passes.)
    `cancel=True`: an order-not-found REJECT (CANCEL_BENIGN_REASONS) is treated as SUCCESS -- the cancel's
    goal (no such order resting) is already achieved, so it's idempotent like CANCEL_ACKNOWLEDGED."""
    if not isinstance(resp, dict):
        raise SystemExit(f"{what}: unexpected response -- expected a JSON object, got "
                         f"{type(resp).__name__}: {str(resp)[:120]}")
    status = str(resp.get("status", "")).upper()
    if status in FAILED_ORDER_STATUSES:
        if cancel and str(resp.get("rejectionReason", "")).upper() in CANCEL_BENIGN_REASONS:
            return resp   # order already gone (filled/canceled/never-placed) => the cancel's job is done
        reason = resp.get("rejectionReason") or resp.get("error") or status
        raise SystemExit(f"{what} {status}: {reason}")
    return resp


# ── HTTP ─────────────────────────────────────────────────────────────────────
def describe_error(e):
    """One-line, readable rendering of a request/JSON error."""
    if isinstance(e, urllib.error.HTTPError):
        try:
            return f"HTTP {e.code}: {e.read().decode()[:160]}"
        except Exception:
            return f"HTTP {e.code}"
    if isinstance(e, urllib.error.URLError):
        return f"unreachable: {e.reason}"
    if isinstance(e, json.JSONDecodeError):
        return f"bad JSON: {e}"
    return f"{type(e).__name__}: {e}"


def retry_after_seconds(e, default=1.0, cap=30.0):
    """Recommended backoff in SECONDS for a 429, or None if `e` is not a 429 HTTPError.

    Both rate-limit layers in front of Arcus surface as HTTP 429: the app limiter
    (body `{"error":"rate limited"}`) and Cloudflare's edge limiter (body
    `error code: 1015`) -- this classifies both by status code alone, so it catches
    the edge 1015 case too. The wait is read from the `Retry-After` response header
    (whole seconds; Arcus guarantees >= 1, rounding its precise `retryAfterMs` UP).

    Only the HEADER is consulted -- never the body -- so this composes with
    describe_error(), which consumes e.read(): call this FIRST, then describe_error()
    for the log, and the single body read still succeeds. Result clamped to
    [default, cap] so a missing/garbage header still backs off (>= default) and a
    hostile header can't park the loop for minutes (<= cap)."""
    if not (isinstance(e, urllib.error.HTTPError) and e.code == 429):
        return None
    secs = default
    hdrs = getattr(e, "headers", None)
    raw = hdrs.get("Retry-After") if hdrs else None
    if raw:
        try:
            secs = float(raw)
        except (TypeError, ValueError):
            secs = default                     # HTTP-date form (rare here) or junk -> fall back to default
        if not math.isfinite(secs):            # "nan"/"inf" parse fine; make finiteness EXPLICIT (don't rely on the
            secs = default                     # max/min arg-order to neutralize a non-finite header) -> clean default
    return max(default, min(secs, cap))


def rate_limit_details(e, default=1.0, cap=30.0):
    """For a 429 HTTPError, return (backoff_seconds, reason, detail); None if `e` is not a 429.

    Unlike retry_after_seconds() (header-only), this READS THE BODY ONCE to get the venue's
    ms-precise `retryAfterMs` -- the docs recommend preferring it over the `Retry-After` header,
    which is whole seconds ROUNDED UP (so it never under-waits but can overshoot by <1s) -- and the
    `reason` (ip|account_empty|account_partial|unknown), useful because order writes cost 0 IP weight,
    so a 429 while placing at a modest rate is usually shared-IP READ traffic, not the order pool.

    Backoff: body retryAfterMs (preferred) else the header else `default`, clamped to [default, cap].
    Because this CONSUMES e.read(), the caller must NOT also call describe_error(e) on the same error
    -- the 429 path raises RateLimited and never does; a non-JSON body (e.g. Cloudflare's 1015 edge
    limiter) just yields reason=None and the header/default backoff."""
    if not (isinstance(e, urllib.error.HTTPError) and e.code == 429):
        return None
    secs = None
    hdrs = getattr(e, "headers", None)
    raw = hdrs.get("Retry-After") if hdrs else None
    if raw:
        try:
            v = float(raw)
            if math.isfinite(v):
                secs = v                          # header fallback (whole seconds, rounded up)
        except (TypeError, ValueError):
            pass                                  # HTTP-date form / junk -> ignore, use body or default
    reason = None
    try:
        body_txt = e.read().decode("utf-8", "replace")
    except Exception:
        body_txt = ""
    if body_txt:
        try:
            obj = json.loads(body_txt)
        except (ValueError, TypeError):
            obj = None                            # non-JSON (Cloudflare 1015 text) -> header/default only
        if isinstance(obj, dict):
            ms = obj.get("retryAfterMs")
            if isinstance(ms, (int, float)) and not isinstance(ms, bool) and math.isfinite(ms) and ms >= 0:
                secs = ms / 1000.0                # PREFER the ms-precise body value over the rounded-up header
            r = obj.get("reason")
            if isinstance(r, str):
                reason = r
    if secs is None:
        secs = default
    secs = max(default, min(secs, cap))
    detail = "rate limited" + (f" (reason={reason})" if reason else "")
    snippet = body_txt.strip().replace("\n", " ")[:120]
    if snippet:
        detail += f": {snippet}"
    return secs, reason, detail


_SESSION = None


def _http_session():
    """Process-wide keep-alive HTTP session. WHY: the old per-call urllib.urlopen opened a FRESH TCP+TLS
    connection for EVERY request; across a fleet of bots doing several place/modify/cancel/read calls per
    cycle that made the per-IP CONNECTION rate explode (and trip Arcus's new-connections limit) even though
    the request COUNT is fine. A Session reuses one pooled connection, so N requests ride ONE connection
    instead of N. Lazy (a tool that never calls request() builds none). max_retries stays 0 (urllib3's
    default): OUR loops own retry/backoff -- a silent library retry of a non-idempotent POST could
    double-place an order."""
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
    return _SESSION


def request(method, path, body=None, headers=None):
    """Perform an HTTP request and return parsed JSON. RAISES on failure -- for callers that handle errors
    themselves (e.g. a long-running loop). Uses a keep-alive Session, but RAISES THE SAME urllib exceptions
    the old urlopen path did -- HTTPError (with .code / .headers / .read()) on an HTTP >=400, URLError on a
    transport failure -- so retry_after_seconds()/describe_error()/_raise_if_rate_limited() and every 429
    backoff site keep working byte-for-byte unchanged."""
    if BASE is None:
        raise SystemExit("no network selected; pass --testnet, --staging, or --mainnet.")
    hdrs = dict(headers or {})
    data = None
    if body is not None:
        data = ordersign.canonical_json(body)
        hdrs.setdefault("Content-Type", "application/json")  # only when there IS a body
    url = BASE + path
    try:
        r = _http_session().request(method, url, data=data, headers=hdrs, timeout=10)
    except requests.RequestException as e:
        # transport/connection failure (incl. a wedged/server-closed keep-alive socket) -> present it as the
        # urllib.error.URLError urlopen raised, so callers' `except (urllib.error.URLError, OSError, ...)` catch it.
        raise urllib.error.URLError(str(e)) from e
    if r.status_code >= 400:
        # >=400 -> raise a urllib.error.HTTPError with the SAME shape urlopen produced: .code (status),
        # .headers (case-insensitive, supports .get("Retry-After")), and a readable body via .read() (for
        # describe_error). This is what makes the 429/Retry-After classification work unchanged.
        raise urllib.error.HTTPError(url, r.status_code, r.reason, r.headers, io.BytesIO(r.content))
    return json.loads(r.content or b"{}")


def call(method, path, body=None, headers=None):
    """One-shot CLI variant of request(): any failure -> clean SystemExit."""
    try:
        return request(method, path, body, headers)
    except (urllib.error.URLError, OSError, json.JSONDecodeError) as e:
        raise SystemExit(describe_error(e))


def fetch_open_orders(query, prog):
    """GET /v1/openOrders -> the list of order DICTS. FAIL CLOSED on a malformed CONTAINER: a non-dict body, or a
    missing/non-list `orders`, means order state is UNKNOWN -- a caller (cancel/modify) must NOT read that as
    'no matching order' (a resting order could be missed), nor crash on `.get`/iteration of a non-dict/None.
    Non-dict ROWS inside a valid list are dropped with a warning (a malformed row may still be resting)."""
    body = call("GET", f"/v1/openOrders?{query}")
    if not isinstance(body, dict) or not isinstance(body.get("orders"), list):
        raise SystemExit(f"{prog}: open orders unreadable -- /v1/openOrders did not return an object with an "
                         f"'orders' list (order state UNKNOWN; NOT treating as empty). Retry.")
    raw = body["orders"]
    orders = [o for o in raw if isinstance(o, dict)]
    if len(orders) != len(raw):
        print(f"{prog}: WARNING skipped {len(raw) - len(orders)} malformed (non-dict) openOrders row(s); "
              f"a malformed order may still be resting.", file=sys.stderr)
    return orders


# ── Server clock (drift correction) ──────────────────────────────────────────
def server_time_ns():
    """API server clock in ns, from GET /v1/time ({"timeNs": <int>}). RAISES."""
    return int(request("GET", "/v1/time")["timeNs"])


def clock_delta_ns():
    """(offset_ns, round_trip_ns): the (server - local) clock offset to ADD to a local time.time_ns() to get
    server time, AND the /v1/time round-trip. The offset is measured at the request MIDPOINT to net out
    round-trip latency, but that estimate is only good to +/- round_trip/2 -- so a SLOW/asymmetric /v1/time
    (e.g. under Cloudflare rate-limiting, where urlopen's PER-OPERATION timeout lets a connect+read stall many
    seconds) can fabricate a large apparent offset. The round-trip lets a caller REJECT such an unreliable
    sample. RAISES on failure -- callers decide whether to fall back to local."""
    before = time.time_ns()
    api = server_time_ns()
    after = time.time_ns()
    return api - (before + after) // 2, after - before


# A /v1/time round-trip beyond this makes the MIDPOINT offset unreliable (good only to +/- rtt/2): a slow /
# rate-limited request can fabricate a multi-second apparent offset. clock_delta() discards such a sample and
# falls back to the local clock (0). Mirrors market_maker.CLOCK_RTT_MAX_S -- keep the two in step.
CLOCK_RTT_MAX_S = 2.0


def clock_delta():
    """Server-minus-local clock offset (ns) from /v1/time; 0 (use local clock) if it's unavailable OR the
    sample is unreliable -- a /v1/time hiccup shouldn't block an order, and the 365-day expiry clears the
    1-month minimum regardless of small drift. The fail-soft wrapper around clock_delta_ns() (which RAISES).

    Defense-in-depth: a SLOW/asymmetric /v1/time (Cloudflare rate-limiting; urlopen's per-operation timeout
    lets connect+read stall many seconds) fabricates a large apparent offset accurate only to +/- rtt/2. Rather
    than sign with garbage, DISCARD a sample whose round-trip exceeds CLOCK_RTT_MAX_S and use the local clock:
    a well-synced host signs MORE accurately from local than from a multi-second-off reading, and the venue's
    +/-30 s auth window covers any true small drift. (market_maker fetches clock_delta_ns() directly and applies
    its OWN rtt gate + skew abort; this guards the single-shot CLI tools that go through clock_delta.)"""
    try:
        offset, rtt = clock_delta_ns()
    except Exception as e:
        print(f"warning: /v1/time unavailable ({describe_error(e)}); using local clock.", file=sys.stderr)
        return 0
    if rtt > CLOCK_RTT_MAX_S * 1_000_000_000:
        print(f"warning: /v1/time round-trip {rtt / 1e9:.3f}s > {CLOCK_RTT_MAX_S:g}s "
              f"(offset unreliable); using local clock.", file=sys.stderr)
        return 0
    return offset


@contextlib.contextmanager
def server_clock_shim(delta_ns=None):
    """Shift time.time_ns() by the (server - local) offset for the duration of the block, so ordersign's
    signing functions that mint their OWN X-Timestamp internally -- sign_cancel_order / sign_modify_order /
    sign_legacy, which (unlike sign_place_order) take no client_timestamp param -- still produce a
    SERVER-ALIGNED timestamp. Without it, a local clock that drifts past the server's +/-30 s auth window makes
    those requests 401 -- fragile exactly for a PANIC cancel (place_order/close_position already align via
    clock_delta()+client_timestamp; market_maker.place_quote via time.time_ns()+self.clock_delta_ns).

    delta_ns: a PRECOMPUTED offset to use instead of fetching /v1/time. market_maker passes its cached
    self.clock_delta_ns so the HOT modify/cancel path adds NO network round-trip and aligns EXACTLY like
    place_quote. If None (the single-shot CLI tools), fetch once via clock_delta() (fail-soft to 0 if
    /v1/time is down -> no-op, prior local-clock behavior).

    WRAP ONLY THE SIGN CALL. This shifts the PROCESS-GLOBAL time.time_ns, so the wrapped block MUST be a
    purely synchronous sign call with NO await / no yield to other coroutines / nothing else that needs the
    true wall clock. That holds for cancel_order.py / modify_order.py (synchronous CLI) and market_maker.py
    (fully synchronous -- no asyncio). ordersign imports the SAME `time` module object, so patching the
    attribute here is what its internal time.time_ns() call resolves."""
    delta = clock_delta() if delta_ns is None else delta_ns   # server_ns - local_ns; MUST predate the patch
    if not delta:
        yield 0                           # 0 / unavailable -> no-op (local clock -- the prior behavior)
        return
    real = time.time_ns
    time.time_ns = lambda: real() + delta
    try:
        yield delta
    finally:
        time.time_ns = real               # ALWAYS restore, even on exception


# ── Validation / conversion ──────────────────────────────────────────────────
def round_to_increment(value, increment, rounding):
    """Round Decimal `value` to a multiple of `increment` (a decimal string like a tick/step size),
    using the given decimal `rounding` mode."""
    inc = Decimal(increment)
    return (value / inc).to_integral_value(rounding=rounding) * inc


def clamp_to_mark_cap(bound, mark, tick, is_buy, cap=Decimal("0.099")):
    """Clamp a protective MARKET-order price `bound` to within +/-`cap` of `mark`. The venue rejects a market
    order whose bound is more than 10% off mark, and tick-rounding a mark+/-slippage bound AWAY from mid can
    push it PAST that cap on a COARSE-tick market / an aggressive --max-slippage -> the whole order is rejected
    (a panic close then does nothing). Clamp INWARD to the nearest tick inside the band instead -- a tighter
    protective bound is safe (worst case a little more IOC remainder). `cap` defaults to 9.9%, a hair under the
    venue's 10% for tick-rounding margin. A missing/non-positive `mark` returns `bound` unchanged (the caller
    guards that case separately). The clamp only bites when the rounded bound would otherwise breach the cap."""
    if mark is None or mark <= 0:
        return bound
    if is_buy:                                                              # BUY bound sits ABOVE mark
        return min(bound, round_to_increment(mark * (1 + cap), tick, ROUND_FLOOR))
    return max(bound, round_to_increment(mark * (1 - cap), tick, ROUND_CEILING))   # SELL bound sits BELOW mark



def dec(value):
    """Decimal(value), or None if not numeric OR not finite (NaN/Infinity).

    Rejecting non-finite here protects every caller (e.g. dec(markPrice) for
    --force bounds), not just positive_decimal().
    """
    try:
        v = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return v if v.is_finite() else None


def positive_decimal(s, name, allow_zero=False):
    """Parse a CLI numeric arg as a finite Decimal (> 0, or >= 0 if allow_zero).

    Rejects NaN/Infinity -- `Decimal("NaN"/"Infinity")` parse fine and slip past
    `<= 0` comparisons (NaN comparisons are always False), so guard explicitly.
    """
    v = dec(s)
    if v is None or not v.is_finite():
        raise SystemExit(f"{name}: {s!r} is not a valid finite decimal.")
    if v < 0 or (v == 0 and not allow_zero):
        raise SystemExit(f"{name}: must be {'>= 0' if allow_zero else '> 0'} (got {s}).")
    return v


def validate_client_id(cid):
    if not isinstance(cid, str) or not CLIENT_ID_RE.match(cid):
        raise SystemExit("--clientid: must be 1-36 chars of [A-Za-z0-9_-].")


def to_ticks(price_str, tick):
    """price -> integer ticks; clean error (not a ValueError traceback) if off-tick."""
    try:
        return ordersign.price_to_ticks(price_str, tick)
    except ValueError:
        raise SystemExit(f"price {price_str} is not a multiple of the tick size {tick}.")


def to_quantums(qty_str, step):
    """size -> integer quantums; clean error if off-step."""
    try:
        return ordersign.size_to_quantums(qty_str, step)
    except ValueError:
        raise SystemExit(f"quantity {qty_str} is not a multiple of the step size {step}.")


# ── Market resolution ────────────────────────────────────────────────────────
def resolve_market(markets, ident):
    """Find a market by numeric marketId or case-insensitive display name (or None).

    Returns the market dict; its marketDisplayName is the CANONICAL name to use
    for the /v1/l2OrderBook path etc. (a numeric id 404s there).
    """
    ident = str(ident).strip()                # tolerate int input / stray whitespace
    if ident.isdigit():
        return next((m for m in markets if isinstance(m, dict) and str(m.get("marketId")) == str(int(ident))), None)
    up = ident.upper()
    return next((m for m in markets if isinstance(m, dict) and str(m.get("marketDisplayName", "")).upper() == up), None)


# ── Shared MARKET-order (IOC) placement ──────────────────────────────────────────
# The book-walk + slippage guard + protective-bound + sign/POST, shared by place_order.py AND pivot_trader.py
# so the market-order logic lives in ONE place. See place_order.py's module docstring for the guard rationale.
MARKET_MAX_SLIPPAGE     = Decimal("0.03")   # est. avg fill vs mid: refuse beyond this unless force=True
MARKET_PRICE_BUFFER     = Decimal("0.01")   # normal protective bound = worst consumed level +/- 1%
MARKET_FORCE_MARK_BOUND = Decimal("0.09")   # force bound = markPrice +/- 9% (stays under the API's 10%-of-mark cap)
MARKET_GOOD_TIL_DAYS    = 365               # every order needs a goodTilTime >= 1 month out; 365d clears it


def walk_book(levels, qty):
    """Walk pre-sorted [price, size] levels filling up to `qty` -> (avg, worst, filled, enough)."""
    remaining, cost, filled, worst = qty, Decimal(0), Decimal(0), None
    for price_s, size_s in levels:
        if remaining <= 0:
            break
        price, size = Decimal(price_s), Decimal(size_s)
        take = size if size < remaining else remaining
        cost += price * take
        filled += take
        remaining -= take
        worst = price_s
    avg = (cost / filled) if filled > 0 else None
    return avg, worst, filled, remaining <= 0


def place_market_ioc(signer, address, account_index, mkt, side, qty, *, force=False, client_id=None):
    """Place a MARKET IOC order for `qty` (Decimal, base units) on `side` ("BUY"/"SELL") of `mkt` (a
    /v1/markets entry). Walks the LIVE l2 book, applies the slippage guard + protective bound exactly like
    place_order.py, signs (Ed25519), POSTs /v1/placeOrder, and returns:

        {"placed": bool, "reason": str|None, "mid", "avg_fill", "slippage", "worst", "bound",
         "enough": bool, "order": <response>|None}

    When the slippage guard BLOCKS the order (est. slippage > MARKET_MAX_SLIPPAGE and not force) it returns
    placed=False / order=None with a human `reason` -- the CALLER decides whether that's fatal (CLI) or a
    skip-and-retry (bot). Raises SystemExit (fail-closed) on a malformed book, a non-positive/unsafe protective
    bound, a book dislocated >10% from mark, or a rejected order.

    NOTE: placeOrder is ASYNCHRONOUS -- `order` is the 200/202 body (orderId present; a definitive `filledSize`
    only on the best-effort 200 path). A caller needing the exact fill must read GET /v1/order/{orderId} after."""
    if side not in ("BUY", "SELL"):
        raise SystemExit(f"place_market_ioc: side must be BUY or SELL, got {side!r}.")
    if not isinstance(qty, Decimal) or not qty.is_finite() or qty <= 0:
        raise SystemExit(f"place_market_ioc: qty must be a finite positive Decimal, got {qty!r}.")
    try:
        market_id = int(mkt["marketId"])
        tick_size, step_size = mkt["tickSize"], mkt["stepSize"]
        market_name = mkt["marketDisplayName"]
        dt, ds = dec(tick_size), dec(step_size)
        if dt is None or ds is None or dt <= 0 or ds <= 0:
            raise ValueError
    except (KeyError, ValueError, TypeError):
        raise SystemExit("place_market_ioc: market has incomplete/malformed metadata "
                         "(marketId / tickSize>0 / stepSize>0 / marketDisplayName).")

    ob = call("GET", f"/v1/l2OrderBook/{urllib.parse.quote(market_name)}")
    if not isinstance(ob, dict):
        raise SystemExit(f"{market_name}: unexpected /v1/l2OrderBook response (not a JSON object).")
    bids, asks = ob.get("bids", []), ob.get("asks", [])
    if not bids or not asks:
        raise SystemExit(f"{market_name}: order book has no two-sided liquidity.")
    try:                                    # every level must be a [finite-positive price, finite-positive size]
        for lv in (*bids, *asks):           # PAIR -- bare Decimal() accepts "NaN"/"Infinity"/negatives, which would
            if not isinstance(lv, (list, tuple)) or len(lv) != 2:   # flow into mid/slippage and fail the guard OPEN.
                raise ValueError("level is not a 2-element list")
            p, s = Decimal(lv[0]), Decimal(lv[1])
            if not (p.is_finite() and p > 0 and s.is_finite() and s > 0):
                raise ValueError("level price/size must be finite and positive")
    except (ArithmeticError, TypeError, ValueError):
        raise SystemExit(f"{market_name}: malformed order book (level not a finite-positive [price, size] pair).")
    asks = sorted(asks, key=lambda lv: Decimal(lv[0]))               # defensive: don't trust server ordering
    bids = sorted(bids, key=lambda lv: Decimal(lv[0]), reverse=True)
    mid = (Decimal(bids[0][0]) + Decimal(asks[0][0])) / 2
    levels = asks if side == "BUY" else bids

    avg_fill, worst_price, filled, enough = walk_book(levels, qty)
    if avg_fill is None:
        raise SystemExit(f"{market_name}: empty book on the {side} side.")
    slippage = abs(avg_fill - mid) / mid
    if not slippage.is_finite():            # non-finite slippage = broken pricing; NEVER place (NaN>MAX is False)
        raise SystemExit(f"{market_name}: computed slippage is non-finite (mid={mid}, avg_fill={avg_fill}); book malformed.")
    result = {"placed": False, "reason": None, "mid": mid, "avg_fill": avg_fill, "slippage": slippage,
              "worst": worst_price, "bound": None, "enough": enough, "order": None}
    if slippage > MARKET_MAX_SLIPPAGE and not force:
        result["reason"] = f"est. slippage {slippage * 100:.2f}% exceeds {MARKET_MAX_SLIPPAGE * 100:.0f}% limit"
        return result

    # Protective bound: normal = worst level +/- 1% (rounded AWAY from mid so tick-rounding can't tighten it);
    # force = mark +/- 9%. The venue also checks the bound against a 10%-of-mark cap.
    mark = dec(mkt.get("markPrice"))
    if force:
        if mark is None or mark <= 0:
            mark = mid                      # markPrice missing/zero -> fall back to mid for the force bound
        target = mark * (1 + MARKET_FORCE_MARK_BOUND) if side == "BUY" else mark * (1 - MARKET_FORCE_MARK_BOUND)
    else:
        worst = Decimal(worst_price)
        target = worst * (1 + MARKET_PRICE_BUFFER) if side == "BUY" else worst * (1 - MARKET_PRICE_BUFFER)
    bound = round_to_increment(target, tick_size, ROUND_CEILING if side == "BUY" else ROUND_FLOOR)
    if force:
        bound = clamp_to_mark_cap(bound, mark, tick_size, side == "BUY")
    elif mark is not None and mark > 0 and abs(bound - mark) > mark * Decimal("0.10"):
        raise SystemExit(f"{market_name}: order-book bound {bound} is >10% off markPrice {mark} (the venue's "
                         "market-order cap); book dislocated -- use a mark-based (force) bound or a LIMIT order.")
    if bound <= 0:                          # a SELL floor can tick-round to 0 near tickSize -> no protective bound
        raise SystemExit(f"{market_name}: protective bound rounded to {bound} (<=0); price is within ~1 tick of "
                         f"tickSize {tick_size}, so a MARKET {side} would carry no protective bound. Refusing.")
    result["bound"] = bound

    # Pre-validate size/notional against the market's bounds -> a clear error BEFORE signing (else the venue
    # rejects with OrderSizeTooLarge / a min-size / min-notional reason). Each bound is skipped when absent.
    min_sz = dec(mkt.get("minOrderSize")); max_sz = dec(mkt.get("maxOrderSize")); min_not = dec(mkt.get("minOrderNotional"))
    if min_sz is not None and min_sz > 0 and qty < min_sz:
        raise SystemExit(f"{market_name}: size {qty:f} is below the market minimum order size {min_sz:f} (minOrderSize).")
    if max_sz is not None and max_sz > 0 and qty > max_sz:
        raise SystemExit(f"{market_name}: size {qty:f} exceeds the market maximum order size {max_sz:f} (maxOrderSize).")
    if min_not is not None and min_not > 0 and qty * bound < min_not:
        raise SystemExit(f"{market_name}: order notional {qty * bound:f} is below the market minimum {min_not:f} (minOrderNotional).")

    # Sign the typed payload + POST. goodTilTime >= 1 month out in SERVER time (correct the local clock).
    qty_str = f"{qty:f}"
    price_ticks = to_ticks(f"{bound:f}", tick_size)
    quantity_quantums = to_quantums(qty_str, step_size)
    delta_ns = clock_delta()
    server_us = (time.time_ns() + delta_ns) // 1000
    good_til_us = str(server_us + MARKET_GOOD_TIL_DAYS * 86_400 * 1_000_000)
    ct = time.time_ns() + delta_ns          # server-aligned; also the X-Timestamp
    headers = signer.sign_place_order(
        address=address, account_index=account_index, client_id=client_id,
        client_timestamp_ns=ct, good_til_time_ns_=ordersign.good_til_time_ns(good_til_us),
        market_id=market_id, price_ticks=price_ticks, quantity_quantums=quantity_quantums,
        reduce_only=False, side=ordersign.SIDE_BUY if side == "BUY" else ordersign.SIDE_SELL,
        time_in_force=ordersign.TIF_IOC,
    )
    body = {"address": address, "accountIndex": account_index, "marketId": market_id,
            "orderSide": side, "orderType": "MARKET", "quantity": qty_str, "price": f"{bound:f}",
            "timeInForce": "IOC", "timestamp": ct, "goodTilTime": good_til_us}
    if client_id:
        body["clientId"] = client_id
    path = "/v1/placeOrder?" + urllib.parse.urlencode({"address": address})
    order = call("POST", path, body, headers)
    check_order_response(order, "placeOrder")   # a 2xx can still carry status REJECTED/ERROR -> fail closed
    result["placed"] = True
    result["order"] = order
    return result
