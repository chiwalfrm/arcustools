#!/usr/bin/env python3
"""Snapshot Arcus market metadata into Redis and report what changed since last run.

Purpose: catch venue-side changes to (mostly static) market definitions -- a relisted
market, a changed tickSize/stepSize/margin fraction, a new field added to the API, a
status flip -- by keeping a per-market baseline in Redis and diffing each fresh
/v1/markets response against it.

  marketdata_monitor.py --mainnet            # fetch, store new markets, diff existing ones
  marketdata_monitor.py --testnet
  marketdata_monitor.py --mainnet --update   # ALSO overwrite the baseline with the fresh
                                             # snapshot after reporting (acknowledge changes)
                                             # AND append per-field change history to disk
  marketdata_monitor.py --mainnet --compare-all  # ignore the exclusion list; diff every field

Delisting reaper (runs under --update):
  A market is delisted when its `status` flips ONLINE -> OFFLINE (arcus never removes a market from
  /v1/markets; it flips the status). On that edge, --update ALSO gracefully stops the market's dedicated
  per-market programs -- `market_maker` and whichever orderbook feeder is running (`wsorderbook` or
  `wsorderbook_reader_companion`, mutually exclusive on one port) -- by SIGTERM (SIGKILL only if a
  process outlasts --grace-seconds), then removes their liveness pid-markers so the WS liveness checker
  doesn't later flag the intentionally-dead PIDs. It finds the programs via those markers in
  <LOG_BASE>/<network>/pids (override with --pids-dir); a PID that was recycled onto an unrelated
  process is never killed. There is no separate flag -- reaping is part of "apply the changes" (--update),
  and a delisted/dead market's programs have no reason to keep running. Idempotent, and because --update
  advances the baseline to OFFLINE it fires exactly once on the transition. Without --update the tool only
  REPORTS (makes no changes), so nothing is reaped.

Field-history files (only written under --update):
  In addition to the Redis baseline, --update appends a persistent, greppable history of
  every field's changes (except fields in DONTSAVE_FIELDS) to:
      <output-dir>/<network>/marketdata/<marketDisplayName>/<fieldName>
  Each line is "<unix_epoch> <value>". A field file gets a new line only when the value
  differs from its own last line (nested values stored as key-sorted compact JSON so a
  reorder isn't a false change); all writes in one run share a single timestamp, so every
  change from a given run is greppable by that epoch. Without --update, no files are touched.

Behaviour, per the spec:
  1. GET /v1/markets (unsigned, no creds needed).
  2. For each market, look up Redis key "arcus:<network>:marketdata:<marketDisplayName>".
  3. If the key does NOT exist -> store the full market dict as JSON under that key.
  4. If the key DOES exist -> compare each field against the stored value and report
     what changed (added / removed / value-changed).

Some fields (oraclePrice, funding, 24h volume, ...) are EXPECTED to move every poll, so
they live in EXCLUDED_FIELDS below and are skipped in the diff (but still stored, so a
later --update baseline stays complete). Edit that set to tune what counts as "changed".

Key:   arcus:<network>:marketdata:<marketDisplayName>  (e.g. arcus:mainnet:marketdata:BTC-USD)
       -> JSON blob. Network-namespaced (matching the other arcus keys, e.g.
       arcus:<network>:market:) so testnet/staging/mainnet snapshots never collide in one db.
Store: no TTL -- the baseline must persist between runs to be a baseline.
Redis: ARCUS_REDIS_URL (default redis://127.0.0.1:6379/0). Redis is required here.
Stdlib + redis, plus NETWORKS/REDIS_URL from their canonical homes (arcus_common_private / arcus_redis).
"""

import argparse
import json
import os
import signal
import sys
import time
import urllib.error
import urllib.request
from arcus_common_private import NETWORKS   # canonical API-base map (its logical home)
from arcus_redis import REDIS_URL             # redis config (its logical home)

KEY_FMT = "arcus:{network}:marketdata:"   # actual key: KEY_FMT.format(network=...) + marketDisplayName

# Root for the on-disk per-field change history (only written under --update). Actual path is
# <output-dir>/<network>/marketdata/<marketDisplayName>/<fieldName>. Override with --output-dir.
FIELD_HISTORY_ROOT = "/mnt/arcuslogs"
MARKETDATA_SUBDIR = "marketdata"

# Fields expected to move on their own (live prices, funding, rolling 24h stats, market-hours
# flag). They are excluded from the change report but still saved in the snapshot, so a diff
# never lights up on them. Add/remove freely -- unknown fields NOT listed here are compared,
# which is deliberate: a brand-new API field shows up as an "added" change so you notice it.
EXCLUDED_FIELDS = {
    "oraclePrice",
    "markPrice",
    "lastTradePrice",
    "fundingRate",
    "nextFundingRate",
    "nextFundingAt",
    "priceChange24h",
    "volume24h",
    "volume24hNotional",
    "high24h",
    "low24h",
    "trades24h",
    "openInterest",
    "isOutsideRth",
    "currentSettlementPrice",
    "upperTradingBound",
    "lowerTradingBound",
    "nextUpperTradingBound",
    "nextLowerTradingBound",
    "isUpperInExpansionZone",
    "isLowerInExpansionZone",
    "upperZoneEnteredAt",
    "upperExpectedExpansionAt",
    "lowerZoneEnteredAt",
    "lowerExpectedExpansionAt",
}
#marketDisplayName
#fullAssetName
#marketId
#status
#baseAsset
#quoteAsset
#tickSize
#stepSize
#tickTiers
#minOrderNotional
#minOrderSize
#maxOrderSize
#(EXCLUDED)oraclePrice
#(EXCLUDED)markPrice
#(EXCLUDED)lastTradePrice
#(EXCLUDED)fundingRate
#(EXCLUDED)nextFundingRate
#(EXCLUDED)nextFundingAt
#(EXCLUDED)priceChange24h
#(EXCLUDED)volume24h
#(EXCLUDED)volume24hNotional
#(EXCLUDED)high24h
#(EXCLUDED)low24h
#(EXCLUDED)trades24h
#(EXCLUDED)openInterest
#openInterestCapNotional
#initialMarginFraction
#maintenanceMarginFraction
#offHoursInitialMarginFraction
#regularTradingHours
#(EXCLUDED)isOutsideRth
#(EXCLUDED)currentSettlementPrice
#(EXCLUDED)upperTradingBound
#(EXCLUDED)lowerTradingBound
#(EXCLUDED)nextUpperTradingBound
#(EXCLUDED)nextLowerTradingBound
#(EXCLUDED)isUpperInExpansionZone
#(EXCLUDED)isLowerInExpansionZone
#(EXCLUDED)upperZoneEnteredAt
#(EXCLUDED)upperExpectedExpansionAt
#(EXCLUDED)lowerZoneEnteredAt
#(EXCLUDED)lowerExpectedExpansionAt
#type
#category
#addedTimestamp
#assetResolution
#pythId
#openInterestCap


# Fields NOT saved to the on-disk field-history files (written under --update). Kept SEPARATE from
# EXCLUDED_FIELDS (which governs the Redis change DIFF) so saving and diffing can be tuned
# independently -- e.g. save a field's history while ignoring it in the diff, or vice versa. Currently
# EMPTY -- EVERY field is saved to history; uncomment the set(EXCLUDED_FIELDS) line below to instead skip
# saving the same live fields the diff ignores. Note: file-saving uses this collection unconditionally --
# --compare-all affects only the diff, never what gets saved.
#DONTSAVE_FIELDS = set(EXCLUDED_FIELDS)
DONTSAVE_FIELDS = {}


# Internal sentinel written INTO a baseline blob (not an API field) to mark "this market was reported MISSING
# from /v1/markets and we've already said so", so the absence scan doesn't repeat the line every run. Set only
# under --update; cleared (blob re-stored without it) when the market REAPPEARS. Leading underscore avoids any
# collision with a real API field, and it's stripped before every diff so it never leaks into the change report.
MISSING_FLAG = "_deleted_and_reported"


def fetch_markets(base_url):
    """GET <base_url>/v1/markets -> list of market dicts, or clean SystemExit on failure."""
    url = base_url + "/v1/markets"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            data = json.loads(r.read())
    except urllib.error.HTTPError as e:
        raise SystemExit(f"marketdata_monitor: HTTP {e.code} fetching markets: {e.reason}")
    except urllib.error.URLError as e:
        raise SystemExit(f"marketdata_monitor: could not reach {url}: {e.reason}")
    except (TimeoutError, OSError) as e:
        raise SystemExit(f"marketdata_monitor: network error: {e}")
    except json.JSONDecodeError as e:
        raise SystemExit(f"marketdata_monitor: invalid JSON from server: {e}")
    if not isinstance(data, dict):
        raise SystemExit("marketdata_monitor: unexpected markets response shape (not a JSON object)")
    markets = data.get("markets")
    if not isinstance(markets, list):
        raise SystemExit("marketdata_monitor: unexpected response shape (no 'markets' list)")
    return markets


def connect_redis():
    """A live Redis client, or SystemExit -- this tool's whole job is the Redis baseline."""
    try:
        import redis
    except ImportError:
        raise SystemExit("marketdata_monitor: the 'redis' package is not installed.")
    try:
        r = redis.Redis.from_url(REDIS_URL, socket_timeout=2.0, decode_responses=True)
        r.ping()
    except Exception as e:
        raise SystemExit(f"marketdata_monitor: Redis unavailable at {REDIS_URL}: {e}")
    return r


def _store(r, key, blob):
    """r.set with a clean CLI error on failure (mirrors the guarded reads), so a mid-run Redis write
    failure doesn't crash with a raw traceback (and leave partially-advanced baselines under --update)."""
    try:
        r.set(key, blob)
    except Exception as e:
        raise SystemExit(f"marketdata_monitor: Redis write failed for {key!r}: {e}")


def diff_fields(old, new, excluded):
    """Compare two market dicts, skipping `excluded` keys. Returns a list of
    (field, kind, old_value, new_value) where kind is 'added' | 'removed' | 'changed'.
    'added'/'removed' use None for the absent side. Ordering: added, then removed, then
    changed -- each in the field order of the relevant dict."""
    changes = []
    for k, v in new.items():
        if k in excluded:
            continue
        if k not in old:
            changes.append((k, "added", None, v))
        elif old[k] != v:
            changes.append((k, "changed", old[k], v))
    for k, v in old.items():
        if k in excluded or k in new:
            continue
        changes.append((k, "removed", v, None))
    # added first, then removed, then changed -- stable within each group
    order = {"added": 0, "removed": 1, "changed": 2}
    return sorted(changes, key=lambda c: order[c[1]])


def fmt_val(v):
    """Compact one-line rendering of a field value for the report (dicts/lists as JSON)."""
    if isinstance(v, (dict, list)):
        return json.dumps(v, separators=(",", ":"))
    return str(v)


def _safe_name(name):
    """Reject values unsafe as a path component (API names are normally simple like
    'BTC-USD' / 'tickSize'; this is belt-and-suspenders)."""
    return bool(name) and name not in (".", "..") and "/" not in name and "\x00" not in name


def _serialize_field(value):
    """Stable string form of a field value for the on-disk history. Nested values are
    key-sorted compact JSON so a mere reordering never registers as a change; the line
    format splits on the first space, so JSON commas/spaces are fine as the value."""
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    return str(value)


def _last_history_value(path):
    """Value from the last non-empty line of a field-history file, or None if the file is
    absent/empty/malformed (all of which mean 'no prior value' -> write the first line)."""
    try:
        with open(path, "r") as f:
            last = None
            for line in f:
                line = line.rstrip("\n")
                if line.strip():
                    last = line
    except FileNotFoundError:
        return None
    if last is None:
        return None
    _epoch, sep, value = last.partition(" ")
    return value if sep else None


def log_field_history(market, out_dir, now, dontsave):
    """Append '<now> <value>' to <out_dir>/<name>/<field> for each field NOT in `dontsave` whose
    value differs from that file's last line (creating files/dirs as needed). Best-effort:
    per-market/-field I/O errors warn to stderr and are skipped, never aborting the run."""
    name = market.get("marketDisplayName")
    if not _safe_name(name):
        return
    mkt_dir = os.path.join(out_dir, name)
    try:
        os.makedirs(mkt_dir, exist_ok=True)
    except OSError as e:
        print(f"  ! {name}: field-history dir error: {e}", file=sys.stderr)
        return
    for field, value in market.items():
        if field in dontsave or not _safe_name(field):
            continue
        current = _serialize_field(value)
        path = os.path.join(mkt_dir, field)
        prev = _last_history_value(path)
        if prev == current:
            continue                       # unchanged -> nothing to write
        try:
            with open(path, "a") as f:     # append: new file -> first line; else -> change line
                f.write(f"{now} {current}\n")
        except OSError as e:
            print(f"  ! {name}/{field}: field-history write failed: {e}", file=sys.stderr)


# --- Reaper: gracefully stop a delisted market's dedicated programs -----------------------------------------
# When a market flips ONLINE -> OFFLINE it is delisted, and its per-market long-running programs should be shut
# down. Each such program drops a liveness marker  <pids_dir>/<name>-<PID>  at startup (see arcus_common_public.
# write_pidfile), where <name> encodes program+market -- e.g. 'market_maker-BTC-USD', 'wsorderbook-BTC-USD',
# 'wsorderbook_reader_companion-BTC-USD'. On the delisting edge we match THIS market's markers, gracefully stop
# each live process (SIGTERM, escalating to SIGKILL only if it outlasts the grace period), and remove the marker
# so the WS liveness checker doesn't later flag the (intentionally) dead PID. The two orderbook feeders are
# mutually exclusive (one port), so whichever is running is simply the one with a marker -- we target all three
# program names and reap those that exist.
LOG_BASE = "/mnt/arcuslogs"           # <network>/pids appended -> the dir the WS/MM tools drop liveness markers in
REAP_PROGRAMS = {                      # marker-name prefix (before -<market>) -> script basename expected in the live cmdline (reuse guard)
    "market_maker": "market_maker.py",
    "wsorderbook": "wsorderbook.py",
    "wsorderbook_reader_companion": "wsorderbook_reader_companion.py",
}


def _pid_alive(pid):
    """True if a process with `pid` currently exists (best-effort, POSIX; mirrors the WS tools' check)."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True                              # exists, owned by another user
    except OSError:
        return False
    return True


def _proc_cmdline(pid):
    """The live process's argv joined by spaces (Linux /proc, NUL-separated), or None when it can't be read
    (non-Linux, no permission, a vanished PID, or a kernel thread with an empty cmdline)."""
    try:
        with open("/proc/%d/cmdline" % pid, "rb") as fh:
            raw = fh.read()
    except OSError:
        return None
    if not raw:
        return None
    return b" ".join(raw.split(b"\x00")).decode("utf-8", "replace").strip()


def _marker_argv(path):
    """The argv= line the program recorded in its marker at startup (None if absent)."""
    try:
        with open(path) as fh:
            for line in fh:
                if line.startswith("argv="):
                    return line.rstrip("\n")[len("argv="):]
    except OSError:
        pass
    return None


def _remove_marker(path):
    """Delete a liveness marker, tolerating a concurrent removal (FileNotFoundError) -- part of the reap."""
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
    except OSError as e:
        print(f"      ! reaper: could not remove marker {path}: {e}")


def _reap_one(name, pid, path, script, grace):
    """SIGTERM (then SIGKILL after `grace` seconds) one liveness-marker'd process, then remove the marker.

    A marker whose PID is already dead (the program exited on its own) is just cleaned up. A PID that is alive
    but is a DIFFERENT process (the original died and the OS recycled its PID) is NEVER killed -- proven via the
    same argv-vs-/proc-cmdline check the WS liveness checker uses -- but its stale marker is removed."""
    if not _pid_alive(pid):
        _remove_marker(path)
        print(f"      * reaper: {name} (pid {pid}) already stopped; marker removed")
        return
    # PID-reuse guard: when the live cmdline is readable, require a positive identity match -- the argv the
    # program recorded is a substring of its live /proc cmdline (interpreter prepended), or at minimum the
    # expected script name appears -- before signalling. A recycled PID now owned by something unrelated fails
    # this and is left alone. If /proc is unreadable we cannot prove reuse, so we trust our own marker.
    live = _proc_cmdline(pid)
    if live is not None:
        want = _marker_argv(path)
        if not ((want and want in live) or (script in live)):
            _remove_marker(path)
            print(f"      ! reaper: {name} pid {pid} is a DIFFERENT process now (PID reused) -- NOT killed; stale marker removed")
            return
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        _remove_marker(path)                     # raced us to exit between the alive-check and the signal
        print(f"      * reaper: {name} (pid {pid}) exited before SIGTERM; marker removed")
        return
    except OSError as e:
        print(f"      ! reaper: {name} pid {pid} SIGTERM failed: {e}")
        return
    deadline = time.time() + max(0, grace)       # wait for a graceful exit; escalate only if it outlasts grace
    while time.time() < deadline:
        if not _pid_alive(pid):
            break
        time.sleep(0.2)
    if _pid_alive(pid):
        try:
            os.kill(pid, signal.SIGKILL)
            print(f"      * reaper: {name} (pid {pid}) ignored SIGTERM for {grace}s -> SIGKILL")
        except OSError as e:
            print(f"      ! reaper: {name} pid {pid} SIGKILL failed: {e}")
    else:
        print(f"      * reaper: {name} (pid {pid}) stopped gracefully (SIGTERM)")
    _remove_marker(path)


def reap_market(market, pids_dir, grace):
    """Gracefully stop the delisted `market`'s dedicated programs and remove their liveness markers.

    Scans `pids_dir` for markers named  <program>-<market>-<PID>  for each program in REAP_PROGRAMS and reaps
    each via _reap_one. Best-effort: filesystem/OS errors are reported, never raised -- reaping one market must
    never abort the monitor run. Idempotent: once the markers are gone a later call finds nothing to do."""
    targets = {f"{prog}-{market}": script for prog, script in REAP_PROGRAMS.items()}
    try:
        entries = os.listdir(pids_dir)
    except FileNotFoundError:
        print(f"      * reaper: no pids dir {pids_dir} -- nothing to reap for {market}")
        return
    except OSError as e:
        print(f"      ! reaper: cannot read {pids_dir}: {e}")
        return
    reaped = 0
    for fn in sorted(entries):
        head, sep, tail = fn.rpartition("-")     # marker is <name>-<PID>; the PID is the trailing all-digit segment
        if not (sep and head and tail.isdigit()):
            continue                             # not a marker (e.g. a rotating .log with a dotted extension)
        script = targets.get(head)               # exact-name match -> this is one of THIS market's reap targets
        if script is None:
            continue
        _reap_one(head, int(tail), os.path.join(pids_dir, fn), script, grace)
        reaped += 1
    if reaped == 0:
        print(f"      * reaper: no running programs found for {market} (already stopped or never started)")


def main():
    p = argparse.ArgumentParser(
        description="Snapshot Arcus market metadata into Redis and report field changes.")
    net = p.add_mutually_exclusive_group(required=True)
    net.add_argument("--testnet", dest="network", action="store_const", const="testnet",
                     help="query the testnet server")
    net.add_argument("--staging", dest="network", action="store_const", const="staging",
                     help="query the staging server")
    net.add_argument("--mainnet", dest="network", action="store_const", const="mainnet",
                     help="query the mainnet server")
    p.add_argument("--update", action="store_true",
                   help="apply changes (default is report-only): overwrite each existing baseline with the "
                        "fresh snapshot (acknowledge the changes so the next run starts clean); ALSO append "
                        "per-field change history under --output-dir; ALSO reap a just-delisted market -- on a "
                        "status ONLINE->OFFLINE flip, gracefully stop its per-market programs (market_maker + "
                        "the running orderbook feeder: wsorderbook or wsorderbook_reader_companion) via "
                        "SIGTERM/SIGKILL and remove their liveness pid-markers (see --pids-dir/--grace-seconds)")
    p.add_argument("--compare-all", action="store_true",
                   help="ignore EXCLUDED_FIELDS and diff every field (incl. live prices/funding)")
    p.add_argument("--pids-dir", default=None,
                   help=f"dir holding the liveness pid-markers used by the --update delisting reaper "
                        f"(default: {LOG_BASE}/<network>/pids).")
    p.add_argument("--grace-seconds", type=int, default=10,
                   help="delisting reaper: seconds to wait after SIGTERM for a program to exit before "
                        "escalating to SIGKILL (default 10).")
    p.add_argument("--output-dir", default=FIELD_HISTORY_ROOT,
                   help="root for the on-disk per-field change history written under --update "
                        f"(actual path <output-dir>/<network>/marketdata/; default {FIELD_HISTORY_ROOT})")
    p.add_argument("--quiet", "-q", action="store_true",
                   help="print ONLY the per-market change blocks (new/reset/changed/skipped); "
                        "suppress the header and summary. Prints nothing at all when there are no "
                        "changes (cron/alert-friendly)")
    args = p.parse_args()

    base_url = NETWORKS[args.network]
    key_prefix = KEY_FMT.format(network=args.network)    # e.g. arcus:mainnet:marketdata:
    excluded = set() if args.compare_all else EXCLUDED_FIELDS
    # Where the delisting reaper looks for per-market liveness markers (the WS/MM tools' <LOG_BASE>/<network>/pids).
    # Only meaningful under --update (the reaper is part of "apply the changes"); harmless to compute otherwise.
    pids_dir = (os.path.expanduser(args.pids_dir) if args.pids_dir
                else os.path.join(LOG_BASE, args.network, "pids"))
    r = connect_redis()
    markets = fetch_markets(base_url)

    # On-disk per-field history is written ONLY under --update. One timestamp for the whole
    # run, so every change from this run is greppable by that epoch. If the output root can't
    # be created, warn once and carry on with the Redis-only monitoring (never abort on it).
    now = int(time.time())
    file_logging = args.update
    out_dir = None
    if file_logging:
        out_dir = os.path.join(os.path.expanduser(args.output_dir), args.network, MARKETDATA_SUBDIR)
        try:
            os.makedirs(out_dir, exist_ok=True)
        except OSError as e:
            print(f"  ! field-history logging disabled: cannot create {out_dir}: {e}", file=sys.stderr)
            file_logging = False

    if not args.quiet:
        print(f"\n  marketdata_monitor [{args.network}]: {len(markets)} markets from {base_url}/v1/markets")
        print(f"  key prefix '{key_prefix}' | excluded from diff: "
              f"{', '.join(sorted(excluded)) if excluded else '(none -- --compare-all)'}\n")

    n_new = n_changed = n_unchanged = n_skipped = n_missing = n_reappeared = n_still_missing = 0
    for m in markets:
        # Skip lines are gated by --quiet: a PERSISTENTLY malformed market (non-dict / no marketDisplayName)
        # is steady state, not a change, and would otherwise print on EVERY quiet run -> a recurring false
        # alert that defeats the "silent when no changes" contract. Still counted in n_skipped for the summary.
        if not isinstance(m, dict):
            n_skipped += 1
            if not args.quiet:
                print(f"  ! skipping a non-dict market entry: {fmt_val(m)[:120]}")
            continue
        name = m.get("marketDisplayName")
        if not isinstance(name, str) or not name:
            n_skipped += 1
            if not args.quiet:
                print(f"  ! skipping a market with missing/invalid marketDisplayName: {fmt_val(m)[:120]}")
            continue
        key = key_prefix + name

        # Persistent on-disk field history (independent of the Redis diff): runs for every valid
        # market under --update, so a brand-new market's first values are captured too. Saved fields
        # are filtered by DONTSAVE_FIELDS (separate from the diff's `excluded`).
        if file_logging:
            log_field_history(m, out_dir, now, DONTSAVE_FIELDS)

        try:
            cached = r.get(key)
        except Exception as e:
            raise SystemExit(f"marketdata_monitor: Redis read failed for {key!r}: {e}")

        if cached is None:
            # New market -> store the full response blob as the baseline.
            _store(r, key, json.dumps(m))
            n_new += 1
            print(f"  + NEW  {name}: baseline stored ({len(m)} fields)")
            continue

        try:
            old = json.loads(cached)
        except (ValueError, TypeError):
            old = None
        if not isinstance(old, dict):
            # Corrupt baseline -> treat as new and overwrite.
            _store(r, key, json.dumps(m))
            n_new += 1
            print(f"  + RESET {name}: stored baseline was corrupt; re-stored ({len(m)} fields)")
            continue

        # A baseline carrying MISSING_FLAG was previously reported gone; its presence NOW means it REAPPEARED.
        # pop() strips the sentinel BEFORE the diff so (a) our own field never enters the comparison and (b) we
        # STILL run the normal diff against the old baseline -- a market that vanished on a glitch and came back
        # with real metadata changes (e.g. tickSize/status) must NOT have those changes suppressed.
        reappeared = old.pop(MISSING_FLAG, None) is True
        changes = diff_fields(old, m, excluded)

        if reappeared:
            n_reappeared += 1
            note = f": {len(changes)} field change(s) while gone" if changes else " (no metadata change)"
            print(f"  ^ REAPPEARED {name}: back in /v1/markets after being reported missing{note}")
        elif not changes:
            n_unchanged += 1
            continue
        else:
            n_changed += 1
            print(f"  ~ CHANGED {name}: {len(changes)} field(s)")

        for field, kind, ov, nv in changes:            # shared detail block (empty when a reappear has no changes)
            if kind == "added":
                print(f"      + {field}: (absent) -> {fmt_val(nv)}")
            elif kind == "removed":
                print(f"      - {field}: {fmt_val(ov)} -> (absent)")
            else:
                print(f"      * {field}: {fmt_val(ov)} -> {fmt_val(nv)}")
        if args.update:                                 # store on CHANGED or REAPPEARED (the latter also clears the sentinel)
            _store(r, key, json.dumps(m))

        # Reaper: an ONLINE -> OFFLINE flip is a delisting -> gracefully stop this market's dedicated programs and
        # clear their liveness markers. This is part of "apply the changes", so it runs under --update (no separate
        # flag): without --update the tool only REPORTS. Keyed on the baseline->fresh status EDGE, and since --update
        # advances the baseline to OFFLINE it fires exactly once on the transition, not every run it then sits OFFLINE.
        if args.update and old.get("status") == "ONLINE" and m.get("status") == "OFFLINE":
            print(f"  = DELISTED {name}: status ONLINE -> OFFLINE; reaping its per-market programs")
            reap_market(name, pids_dir, args.grace_seconds)

    # Report any baseline whose market is ABSENT from the fresh /v1/markets response. IMPORTANT: arcus never
    # REMOVES a market from /v1/markets -- a delisted market flips `status` (ONLINE -> OFFLINE; verified live
    # the only two values), which the field-diff above ALREADY reports as a change. So a baseline going missing
    # is an ANOMALY -- almost always a partial/transient response -- NOT a real delisting. We therefore ONLY
    # REPORT it and NEVER delete the baseline, not even under --update: a flaky/partial fetch must never erase
    # monitoring history (that would also hide later real definition changes). --update refreshes only the
    # baselines of markets that WERE returned (the loop above); it deletes nothing.
    fetched = {m.get("marketDisplayName") for m in markets
               if isinstance(m, dict) and isinstance(m.get("marketDisplayName"), str) and m.get("marketDisplayName")}
    if not fetched:
        # A totally empty ('{"markets": []}') or all-malformed response isn't worth diffing -- every baseline
        # would print as MISSING (pure noise from a blip). Fast-fail with a clear message. (Nothing is deleted
        # regardless; this just avoids a misleading all-missing report.)
        raise SystemExit("marketdata_monitor: no usable markets in /v1/markets response (empty or all-malformed) "
                         "-- refusing to proceed (transient/partial response)")
    try:
        missing = [k for k in r.scan_iter(match=key_prefix + "*", count=1000) if k[len(key_prefix):] not in fetched]
    except Exception as e:
        raise SystemExit(f"marketdata_monitor: Redis scan failed: {e}")
    for key in sorted(missing):
        name = key[len(key_prefix):]
        try:
            blob = r.get(key)
            prev = json.loads(blob) if blob else None
        except Exception:
            prev = None
        if isinstance(prev, dict) and prev.get(MISSING_FLAG) is True:
            n_still_missing += 1                 # already reported this disappearance -> stay quiet, don't repeat
            continue
        n_missing += 1
        print(f"  ! MISSING {name}: not in the /v1/markets response (baseline KEPT, never deleted). Markets "
              f"should flip to OFFLINE, not vanish -- likely a partial/transient response; investigate if it persists.")
        if args.update and isinstance(prev, dict):
            prev[MISSING_FLAG] = True             # mark reported so future runs stay quiet until it reappears
            _store(r, key, json.dumps(prev))

    if not args.quiet:
        updated_note = (" (baselines + field history updated)" if file_logging
                        else " (baselines updated)" if args.update else "")
        extra = (f", {n_reappeared} reappeared" if n_reappeared else "") + \
                (f", {n_still_missing} still-missing" if n_still_missing else "") + \
                (f", {n_skipped} skipped" if n_skipped else "")
        print(f"\n  summary: {n_new} new, {n_changed} changed, {n_unchanged} unchanged, "
              f"{n_missing} missing" + extra + updated_note + "\n")


if __name__ == "__main__":
    main()
