#!/usr/bin/env python3
"""
ws_liveness_check.py -- report Arcus WebSocket logger programs that have DIED.

The Arcus WS loggers (wsorderbook.py, wsaccount.py, wsexchange.py) each drop a liveness marker
<log_dir>/pids/<name>-<PID>  when they start. This scans those pids/ dirs and reports any marker whose PID is no
longer alive -- i.e. that program crashed or was killed and nothing restarted it. Run it by hand, from `watch`,
or from cron to catch the "it died and I didn't notice for hours" case.

    ws_liveness_check.py                          # scan the default arcus log dirs, print dead ones
    ws_liveness_check.py --all                    # also list the healthy (alive) markers
    ws_liveness_check.py /mnt/arcuslogs/mainnet   # scan specific dirs instead of the defaults
    watch -n 30 ws_liveness_check.py              # keep an eye on it live

Each dir you pass is resolved to its pids/ subdir (where the markers live); the bare dir is used as a fallback.
Exit status: 0 = every marker's process is alive (or none found); 1 = at least one DEAD marker (something died).
Handy for alerting:   ws_liveness_check.py || <notify however you like>

Self-contained (stdlib only) on purpose: a health check must still run even if the trading venv is broken.
"""
import argparse
import os
import sys

# Arcus WS loggers log under LOG_BASE=/mnt/arcuslogs + /<network> (markers go in the pids/ subdir there).
DEFAULT_DIRS = ["/mnt/arcuslogs/mainnet", "/mnt/arcuslogs/testnet", "/mnt/arcuslogs/staging"]


def pid_alive(pid):
    """True if a process with `pid` currently exists (best-effort, POSIX)."""
    if pid <= 0:
        return False                             # 0/-1/negative are os.kill process-GROUP / broadcast targets, not a
                                                 # real PID -- a crafted `name-0` marker must not read as ALIVE
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True                              # exists, owned by another user
    except OSError:
        return False
    return True


def scan_dir(d):
    """Where markers live for a given log dir: the pids/ subdir if present, else the dir itself."""
    sub = os.path.join(d, "pids")
    return sub if os.path.isdir(sub) else d


def markers(d):
    """Yield (name, pid, path) for each  <name>-<PID>  marker file in dir `d`.

    A marker's trailing segment (after the last '-') is all digits with no extension, so rotating
    .log/.log.N/.stdout files -- which end in a dotted extension -- are never mistaken for markers.
    """
    try:
        names = os.listdir(d)
    except OSError:
        return
    for fn in names:
        head, sep, tail = fn.rpartition("-")
        if sep and head and tail.isdigit():
            yield head, int(tail), os.path.join(d, fn)


def started_of(path):
    """The `started=` line written into the marker (or '?')."""
    try:
        with open(path) as fh:
            for line in fh:
                if line.startswith("started="):
                    return line.strip()[len("started="):]
    except OSError:
        pass
    return "?"


def argv_of(path):
    """The `argv=` command line the logger recorded in its marker at startup (None if absent --
    markers predating the argv= line have none)."""
    try:
        with open(path) as fh:
            for line in fh:
                if line.startswith("argv="):
                    return line.rstrip("\n")[len("argv="):]
    except OSError:
        pass
    return None


def proc_cmdline(pid):
    """The live process's command line, argv joined by spaces (Linux /proc, NUL-separated), or None
    when it can't be read -- non-Linux, no permission, a vanished PID, or a kernel thread (empty cmdline)."""
    try:
        with open("/proc/%d/cmdline" % pid, "rb") as fh:
            raw = fh.read()
    except OSError:
        return None
    if not raw:
        return None
    return b" ".join(raw.split(b"\x00")).decode("utf-8", "replace").strip()


def pid_reused(pid, path):
    """True when `pid` is alive but is a DIFFERENT process than the logger that wrote this marker --
    i.e. the logger died and the OS recycled its PID onto something unrelated, which the bare os.kill
    check would misreport as ALIVE (the exact dead-logger case this tool exists to catch).

    The logger records `argv=' '.join(sys.argv)`; a Python process's live /proc cmdline is that same
    argv with the interpreter prepended, so the recorded argv is a SUBSTRING of the live cmdline for the
    genuine process. Conservative: if either side is unavailable (pre-argv marker, non-Linux / no /proc,
    empty cmdline) we cannot prove reuse -> return False (fall back to the plain pid-alive result; never
    raise a false DEAD)."""
    want = argv_of(path)
    live = proc_cmdline(pid)
    if not want or not live:
        return False
    return want not in live


def main():
    ap = argparse.ArgumentParser(description="Report Arcus WS logger programs whose liveness-marker PID is dead.")
    ap.add_argument("dirs", nargs="*",
                    help=f"arcus log dirs to scan (default: {', '.join(DEFAULT_DIRS)}); each dir's pids/ subdir is used")
    ap.add_argument("--all", action="store_true", help="also list ALIVE markers, not just dead ones")
    args = ap.parse_args()
    dirs = args.dirs or DEFAULT_DIRS

    total = alive = dead = 0
    dead_rows = []
    scanned = []
    for d in dirs:
        if not os.path.isdir(d):
            continue
        s = scan_dir(d)
        scanned.append(s)
        for name, pid, path in sorted(markers(s)):
            total += 1
            live = pid_alive(pid)
            reused = live and pid_reused(pid, path)   # PID exists but now belongs to an unrelated process
            if live and not reused:
                alive += 1
                if args.all:
                    print(f"  ALIVE  pid={pid:<8} {name}   (started {started_of(path)})   [{s}]")
            else:
                dead += 1
                note = "  (PID recycled onto an unrelated process)" if reused else ""
                dead_rows.append(f"  DEAD   pid={pid:<8} {name}{note}   (started {started_of(path)})   [{path}]")

    for row in dead_rows:
        print(row)
    print(f"# {total} markers | alive={alive} dead={dead} | scanned: {', '.join(scanned) or '(none)'}")
    if dead:
        print(f"# {dead} program(s) DIED -- restart them (a same-program restart clears its own stale marker); "
              f"remove a marker by hand only if you are retiring that program.", file=sys.stderr)
    sys.exit(1 if dead else 0)


if __name__ == "__main__":
    main()
