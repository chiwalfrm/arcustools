"""
Schedule cancel-all (dead man's switch) on Arcus -- arm / refresh / disarm a per-subaccount deadline.

The deadline fires cancelAllOrders for the WHOLE subaccount (account-wide -- there is NO per-market
scope; a venue limitation). So the right use is ONE fleet-level heartbeat that answers "is this BOX
still alive?", NOT a per-bot switch:

  * Run ONE re-arm loop per box. PREFERRED = CRON single-shot (no long-running process to supervise):
        * * * * * <venv>/python3 .../schedule_cancel.py --arm 295 --mainnet >>/var/log/arcus_deadman.log 2>&1
    cron IS the always-on supervised daemon; each tick arms a fresh ~5-min deadline and exits. If the
    box dies, cron dies with it -> no arm -> the switch fires. NB the cadence is NOT a free choice: the
    deadline maxes at 5 MIN (venue cap), so you MUST re-arm within it or a healthy box false-fires --
    running e.g. every 15 min is impossible. 1 min is chosen for HEADROOM against a transient failed arm
    (network blip / slow /v1/time / 429): a 1-min cadence tolerates ~4 consecutive failures before a
    spurious cancel-all; every 2 min tolerates ~1; slower than ~3 min risks a single blip nuking the fleet.
    (Alternative: a long-running `--arm 295 --heartbeat` under systemd Restart=always -- Restart=always
    re-launches its OWN crash within the lead. Same effect; cron is simpler.)
  * A SINGLE bot dying does NOT trigger it -- the re-arm loop is independent of the individual bots, so
    the other markets keep quoting (and each bot already pulls its OWN quotes on its own clean exit).
  * The WHOLE BOX dying -- power loss, kernel panic, a network partition on day 1 of a 2-week vacation
    -- stops the refresh, and ~one lead later the venue cancels every resting order so 365-day GTT
    quotes don't sit unattended on a moving market. Account-wide cancel is exactly right here: the box
    is gone, so every bot is gone. The one auto-fire is far under the 10/UTC-day cap.

  MAINTENANCE: when you INTENTIONALLY stop the fleet, disable the cron line AND run `--disarm` once,
  else the last-armed deadline fires ~5 min later (harmless if the bots already pulled their quotes on
  shutdown, but disarming is the clean move).

  schedule_cancel.py --arm 60 --testnet              # arm: fire cancel-all in 60s unless refreshed
  schedule_cancel.py --at 1790000000000000 --mainnet  # arm at an ABSOLUTE epoch-microsecond deadline
  schedule_cancel.py --disarm --testnet               # disarm (cancel the pending deadline)
  schedule_cancel.py --arm 90 --heartbeat --testnet   # keep re-arming every ~lead/3 until killed

Lead time must be between 5 s and 5 min. Successful auto-fires are capped at 10 per UTC day per
subaccount (HTTP 429 past that). A 503 means the switch is NOT armed (cancel path disabled / store
unavailable) -- we retry a few times rather than assume coverage.

Signing (see ordersign.py): the deadline `time` is ABSOLUTE epoch MICROseconds in SERVER time, so we
compute it off the /v1/time-corrected clock (a drifted local clock would otherwise set the wrong
deadline). scheduleCancel has NO typed op; it uses the LEGACY scheme -- ts_ns + "scheduleCancel" +
canonicalJSON(body) -- exactly like cancelAllOrders (signer.sign_legacy). `address` is also a QUERY
param (not part of the signature). Resolves ordersign.py / arcus_creds_<network>.json relative to this
script, so it works from any cwd.

STATUS: a complete, general-purpose tool. The core (arm / refresh / disarm, single-shot) is LIVE-VERIFIED
on BOTH testnet and mainnet 2026-08-28 (arm -> scheduled, disarm -> disarmed, resting orders untouched);
the sign_legacy scheme (same as cancelAllOrders) is correct on both. The --heartbeat loop wraps the same
verified send() but hasn't been run long-term -- prefer the cron deployment above.

NOT USED by this toolkit's own MM fleet: with one bot per MARKET on a SHARED subaccount, the account-wide
auto-cancel (there is no per-market scope) plus the 5-min MAX lead make it the wrong fit -- a single bot
dying would be fine, but any refresh gap cancels EVERY market. It is kept in the (publicly shared) toolkit
because it IS the right safety net for other layouts (one subaccount per bot, or a single whole-box liveness
heartbeat where account-wide cancel on total-box death is exactly what you want).
"""

import argparse
import json
import os
import signal
import sys
import time
import urllib.error
import urllib.parse

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

from ordersign import Signer
from arcus_common_private import (add_network_args, request, check_order_response, clock_delta, clock_delta_ns,
                          CLOCK_RTT_MAX_S, describe_error, load_creds, retry_after_seconds, select_network,
                          server_clock_shim)

MIN_LEAD_S = 5           # venue minimum lead
MAX_LEAD_S = 295         # venue max is 300s (300000ms); cap 5s under so clock/latency headroom can't tip a
                         # freshly-computed deadline past the server's "at most 300000ms in the future" check
BUSY_503_RETRIES = 3     # a 503 = switch not armed (cancel path disabled / store unavailable) -> retry
RUNNING = True           # cleared by SIGINT/SIGTERM so --heartbeat exits between beats


def server_now_us(delta_ns):
    """Current SERVER epoch microseconds (local clock corrected by the /v1/time offset)."""
    return (time.time_ns() + delta_ns) // 1000


def lead_arg(s):
    """--arm lead in seconds: an int in [MIN_LEAD_S, MAX_LEAD_S]."""
    try:
        v = int(s)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError(f"lead must be an integer number of seconds ({MIN_LEAD_S}-{MAX_LEAD_S})")
    if not (MIN_LEAD_S <= v <= MAX_LEAD_S):
        raise argparse.ArgumentTypeError(f"lead must be {MIN_LEAD_S}-{MAX_LEAD_S} seconds (got {v})")
    return v


def epoch_us_arg(s):
    """--at absolute deadline in epoch MICROseconds (>= 1e14, matching the API's µs guard)."""
    try:
        v = int(s)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError("--at must be an integer epoch-microsecond timestamp")
    if v < 10 ** 14:
        raise argparse.ArgumentTypeError("--at looks too small to be epoch MICROseconds (expected >= 1e14)")
    return v


def parse_args():
    p = argparse.ArgumentParser(description="Arm/refresh/disarm the Arcus dead man's switch (scheduleCancel).")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--arm", type=lead_arg, metavar="SECONDS",
                   help=f"arm/refresh: fire cancel-all in SECONDS ({MIN_LEAD_S}-{MAX_LEAD_S}) unless refreshed")
    g.add_argument("--at", type=epoch_us_arg, metavar="EPOCH_US",
                   help="arm/refresh at an ABSOLUTE epoch-microsecond deadline (its lead must land in range)")
    g.add_argument("--disarm", action="store_true", help="disarm the pending deadline")
    p.add_argument("--heartbeat", action="store_true",
                   help="with --arm: keep re-arming every ~lead/3 seconds until this process is killed "
                        "(the actual dead-man's-switch mode -- the switch fires if THIS process dies)")
    p.add_argument("--interval", type=int, metavar="SECONDS", default=None,
                   help="heartbeat refresh cadence (default ~lead/3); must be >= 1 and <= half the lead so a "
                        "single slow/failed beat still refreshes before the deadline")
    add_network_args(p)
    args = p.parse_args()
    if args.heartbeat and args.arm is None:
        p.error("--heartbeat requires --arm SECONDS (an absolute --at or --disarm can't heartbeat)")
    if args.interval is not None and args.arm is not None:
        if args.interval < 1:
            p.error(f"--interval must be >= 1 second (got {args.interval})")
        # Require at least TWO beats inside the lead (interval <= arm/2), not merely interval < arm. A near-lead
        # interval leaves no room for send latency: one slow ~1-2s round-trip (or a single failed beat) would land
        # the next re-arm AFTER the prior deadline -> a spurious account-wide cancel on a healthy box. Halving the
        # lead guarantees a second beat still refreshes in time. (The default interval, arm//3, already satisfies this.)
        if args.interval > args.arm // 2:
            p.error(f"--interval ({args.interval}s) must be <= half the --arm lead ({args.arm}s -> {args.arm // 2}s) "
                    f"so a single slow or failed beat still refreshes before the deadline lapses")
    return args


def send(signer, address, account_index, query, time_us):
    """One scheduleCancel ping. time_us set = arm/refresh; time_us None = disarm. Returns the response dict.
    Retries a 503 (switch-not-armed) a few times; surfaces 429 (daily auto-fire cap) and 4xx clearly."""
    body = {"address": address, "accountIndex": account_index}
    if time_us is not None:
        body["time"] = time_us                     # omit entirely to disarm (the API also accepts null)
    for attempt in range(1, BUSY_503_RETRIES + 1):
        try:
            with server_clock_shim():              # server-align the X-Timestamp so a drifted clock can't 401
                headers = signer.sign_legacy("/v1/scheduleCancel", body)
            # request() (NOT call()) so an HTTPError propagates to the branch below: call() catches URLError --
            # of which HTTPError is a subclass -- and re-raises SystemExit, which would make the 503-retry loop
            # and the dedicated 429 auto-fire-cap message here DEAD CODE (every HTTP error became a generic exit).
            return request("POST", f"/v1/scheduleCancel?{query}", body, headers)
        except urllib.error.HTTPError as e:
            if e.code == 503 and attempt < BUSY_503_RETRIES:
                print(f"  scheduleCancel 503 (switch not armed yet); retry {attempt}/{BUSY_503_RETRIES}...",
                      file=sys.stderr)
                time.sleep(0.5 * attempt)
                continue
            if e.code == 429:
                ra = retry_after_seconds(e)
                raise SystemExit(f"scheduleCancel 429: daily auto-fire cap (10/UTC day/subaccount) reached"
                                 + (f"; retry after ~{ra:g}s" if ra else "") + ".")
            raise SystemExit(f"scheduleCancel failed: {describe_error(e)}")
        except (urllib.error.URLError, OSError, json.JSONDecodeError) as e:   # transport/JSON errors call() used to
            raise SystemExit(f"scheduleCancel failed: {describe_error(e)}")    # map to SystemExit (HTTPError handled above)


def report(resp, intent):
    """Validate + print a scheduleCancel response ({status: scheduled|disarmed, time, rateLimit}).

    For a DEAD-MAN SWITCH we fail CLOSED on any body that does not POSITIVELY confirm the intended state --
    check_order_response only catches non-dict / REJECTED/ERROR, so on its own an empty {}, an unknown status,
    or a 'disarmed' reply to an arm would print success while the switch is NOT actually armed. So we require,
    by intent: status=='scheduled' WITH a valid echoed integer `time` for arm/refresh, and status=='disarmed'
    for disarm. Anything else -> SystemExit (in --heartbeat that's caught and retried next beat; the previous
    good deadline still stands, so a single bad body can't strand the switch)."""
    check_order_response(resp, "scheduleCancel")       # fail-closed on a non-dict / REJECTED/ERROR body
    status = str(resp.get("status", "")).lower()
    when = resp.get("time")
    valid_time = isinstance(when, int) and not isinstance(when, bool)
    if intent == "disarm":
        if status != "disarmed":
            raise SystemExit(f"scheduleCancel disarm: expected status=disarmed, got status={resp.get('status')!r} "
                             f"(body {str(resp)[:120]}) -- treat the switch as NOT disarmed and retry.")
    else:                                              # arm / refresh
        if status != "scheduled" or not valid_time:
            raise SystemExit(f"scheduleCancel {intent}: expected status=scheduled with an echoed integer deadline, "
                             f"got status={resp.get('status')!r} time={when!r} (body {str(resp)[:120]}) -- the "
                             f"switch is NOT confirmed armed.")
    tail = f" deadline={when} (~{(when - server_now_us(0)) / 1e6:+.0f}s from local now)" if valid_time else ""
    print(f"scheduleCancel {intent}: status={status}{tail}", flush=True)
    return status


def main():
    args = parse_args()
    select_network(args.network)
    creds = load_creds()
    address, account_index = creds["eth_address"], creds["account_index"]
    signer = Signer.from_private_key_hex(creds["api_private_key"])
    query = urllib.parse.urlencode({"address": address})
    delta_ns = clock_delta()                          # measure the (server - local) offset once up front

    if args.disarm:
        report(send(signer, address, account_index, query, None), "disarm")
        return

    def deadline():
        if args.at is not None:
            return args.at
        return server_now_us(delta_ns) + args.arm * 1_000_000

    if not args.heartbeat:
        dl = deadline()
        if args.at is not None:                        # --at only had its magnitude shape-checked at parse time; enforce
            lead_s = (dl - server_now_us(delta_ns)) / 1e6   # the 5s-5min lead the help promises HERE (client-side) so a
            if not (MIN_LEAD_S <= lead_s <= MAX_LEAD_S):     # past or too-far --at fails clearly, not only as a venue 400.
                raise SystemExit(f"schedule_cancel: --at deadline is ~{lead_s:+.0f}s from now, outside the "
                                 f"{MIN_LEAD_S}-{MAX_LEAD_S}s lead window -- pass an --at nearer to now.")
        report(send(signer, address, account_index, query, dl), "arm")
        return

    # --- Heartbeat: keep the switch fresh while THIS process lives; if it dies, the switch fires. ---
    interval = args.interval if args.interval is not None else max(1, args.arm // 3)
    signal.signal(signal.SIGINT, lambda *_: globals().__setitem__("RUNNING", False))
    signal.signal(signal.SIGTERM, lambda *_: globals().__setitem__("RUNNING", False))
    print(f"heartbeat: re-arming a {args.arm}s deadline every {interval}s (Ctrl-C to stop). NOTE: on exit the "
          f"switch is LEFT ARMED, so it fires ~{args.arm}s after the last beat -- pass --disarm separately for a "
          f"clean stop that should NOT cancel.", flush=True)
    fails = 0
    while RUNNING:
        try:                                           # RE-MEASURE the offset each beat so a VM resume / NTP step self-
            offset, rtt = clock_delta_ns()             # corrects. Use the RAW *raising* measure, NOT clock_delta(): that
            if rtt <= CLOCK_RTT_MAX_S * 1_000_000_000: # wrapper fail-SOFTS to 0 (=local clock) on a /v1/time outage OR a
                delta_ns = offset                      # high-RTT sample, which for a long-lived heartbeat would THROW AWAY
            else:                                      # a previously-good server offset on a transient blip -- exactly when
                print(f"heartbeat: /v1/time round-trip {rtt / 1e9:.3f}s > {CLOCK_RTT_MAX_S:g}s (offset unreliable); "
                      f"keeping prior clock offset this beat", file=sys.stderr, flush=True)
        except Exception as e:                         # server validation is unavailable. So adopt ONLY a reliable (low-RTT)
            print(f"heartbeat: clock re-measure failed ({describe_error(e)}); keeping prior offset this beat",
                  file=sys.stderr, flush=True)          # fresh sample; otherwise keep the last good delta_ns (never revert to 0).
        try:
            report(send(signer, address, account_index, query, server_now_us(delta_ns) + args.arm * 1_000_000),
                   "refresh")
            fails = 0
        except (SystemExit, urllib.error.URLError, OSError) as e:
            # A dead man's switch MUST ride out a TRANSIENT failure (network blip, slow /v1/time, a 429/503,
            # a reset). send() turns those into SystemExit/URLError; letting them propagate out of this loop
            # would kill the heartbeat on a single hiccup and leave the last deadline armed -> the venue cancels
            # the whole fleet's orders ~lead later. So LOG and keep beating: the previous good arm's deadline is
            # still ~lead out, and the next good beat refreshes it in time. Only a failure that PERSISTS past the
            # lead lets the switch fire -- which is exactly right (the box/network is genuinely down). This is the
            # per-beat tolerance the module docstring describes; without it, heartbeat mode had none.
            fails += 1
            print(f"heartbeat: re-arm failed ({str(e) or type(e).__name__}); beat #{fails}, retrying next "
                  f"interval. Last good deadline stays armed; the switch fires only if failures persist ~{args.arm}s.",
                  file=sys.stderr, flush=True)
        slept = 0.0
        while RUNNING and slept < interval:            # responsive to SIGINT/SIGTERM
            time.sleep(min(0.5, interval - slept))
            slept += 0.5
    print("heartbeat stopped; the last deadline is still armed (will fire unless you --disarm).", flush=True)


if __name__ == "__main__":
    main()
