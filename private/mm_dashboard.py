#!/usr/bin/env python3
"""mm_dashboard.py -- one-shot dashboard of the arcus / dYdX v4 market_maker fleet's live quotes.

The venues' APIs can't enumerate a fleet's resting short-term orders, so this reads the bots' OWN per-market
logs. Each market_maker appends a machine-readable suffix to the END of every cycle log line:

    [bidsize,bidprice,askprice,asksize,maxpos]

A PULLED side is EMPTY; `maxpos` names the side capped by --max-position (bid|ask), else empty. For the chosen
venue+network it scans the logs, reads each one's LAST line, takes that suffix, and prints an aligned dashboard.
A log whose last line is older than --stale-seconds (default 60) is NOT QUOTING. Prints once and EXITS -- no
loop. Stdlib only; run it where the bots log.

  mm_dashboard.py --arcus --mainnet
  mm_dashboard.py --dydx  --testnet
  mm_dashboard.py --dydx  --mainnet --log-dir /tmp --stale-seconds 90

Log files: arcus  /tmp/arcus_<MARKET>_<NETWORK>.log ,  dYdX  /tmp/dydxv4_<MARKET>_<NETWORK>.log
"""

import argparse
import glob
import os
import re
import time
from datetime import datetime, timezone

DEFAULT_LOG_DIR = "/tmp"
DEFAULT_STALE = 60
PREFIX = {"arcus": "arcus_", "dydx": "dydxv4_"}      # /tmp log filename prefix per venue
TITLE = {"arcus": "arcus", "dydx": "dYdX v4"}


def last_line(path, chunk=8192):
    """The last non-blank line of `path` (tail the final `chunk` bytes), or None if empty/unreadable."""
    try:
        with open(path, "rb") as fh:
            fh.seek(0, os.SEEK_END)
            start = max(0, fh.tell() - chunk)
            fh.seek(start)
            data = fh.read()
    except OSError:
        return None
    lines = [ln for ln in data.decode("utf-8", "replace").splitlines() if ln.strip()]
    return lines[-1] if lines else None


def epoch_of(line):
    """The trailing integer in the leading [<datetime> <epoch>] bracket, or None."""
    m = re.match(r"\[([^\]]*)\]", line)
    if m:
        toks = m.group(1).split()
        if toks and toks[-1].isdigit():
            return int(toks[-1])
    return None


def machine_suffix(line):
    """The bot's machine suffix = the LAST [...] on the line, IF it splits into exactly 5 comma fields
    (bidsize,bidprice,askprice,asksize,maxpos). Returns that 5-tuple, or None. The leading timestamp bracket
    and any [notes] bracket never have 5 comma-fields, so the last-5-field bracket is unambiguously the suffix."""
    last = None
    for m in re.finditer(r"\[([^\]]*)\]", line):
        last = m.group(1)
    if last is None:
        return None
    parts = last.split(",")
    return tuple(p.strip() for p in parts) if len(parts) == 5 else None


def market_of(fname, prefix, network):
    """'<prefix><MARKET>_<network>.log' -> MARKET (only the known prefix + '_<network>.log' suffix are stripped,
    so a market that itself contains '-'/',' survives)."""
    return fname[len(prefix):-len("_%s.log" % network)]


def fmt_age(sec):
    sec = int(sec)
    if sec < 60:
        return "%ds" % sec
    m, s = divmod(sec, 60)
    if m < 60:
        return "%dm%02ds" % (m, s)
    h, m = divmod(m, 60)
    return "%dh%02dm" % (h, m)


def main():
    p = argparse.ArgumentParser(description="One-shot dashboard of the arcus / dYdX v4 market_maker fleet's live quotes.")
    ven = p.add_mutually_exclusive_group(required=True)
    ven.add_argument("--arcus", dest="venue", action="store_const", const="arcus", help="arcus logs (arcus_*.log)")
    ven.add_argument("--dydx", dest="venue", action="store_const", const="dydx", help="dYdX v4 logs (dydxv4_*.log)")
    net = p.add_mutually_exclusive_group(required=True)
    net.add_argument("--testnet", dest="network", action="store_const", const="testnet", help="testnet logs")
    net.add_argument("--mainnet", dest="network", action="store_const", const="mainnet", help="mainnet logs")
    p.add_argument("--log-dir", default=DEFAULT_LOG_DIR, help="dir holding the logs (default: %s)" % DEFAULT_LOG_DIR)
    p.add_argument("--stale-seconds", type=int, default=DEFAULT_STALE,
                   help="a last line older than this = NOT QUOTING (default: %d)" % DEFAULT_STALE)
    args = p.parse_args()

    prefix = PREFIX[args.venue]
    # Require "-USD" in the market segment so non-market logs (e.g. arcus_runall_mainnet.log,
    # arcus_start_mm_mainnet.log) are excluded -- every market ticker ends in -USD (incl. comma tickers).
    pattern = os.path.join(args.log_dir, "%s*-USD_%s.log" % (prefix, args.network))
    files = sorted(glob.glob(pattern))
    now = time.time()

    rows = []                                        # (quoting_bool, [market, bidsize, bidprice, askprice, asksize, note])
    n_quoting = 0
    stale = []
    for path in files:
        market = market_of(os.path.basename(path), prefix, args.network)
        line = last_line(path)
        ep = epoch_of(line) if line else None
        if ep is None:
            rows.append((False, [market, "", "", "", "", "NOT QUOTING (no data)"]))
            stale.append(market)
            continue
        age = now - ep
        if age >= args.stale_seconds:
            rows.append((False, [market, "", "", "", "", "NOT QUOTING (%s stale)" % fmt_age(age)]))
            stale.append(market)
            continue
        # live: the bot's machine suffix (both venues emit it every cycle)
        q = machine_suffix(line)
        n_quoting += 1
        if q is None:
            rows.append((True, [market, "", "", "", "", "(no quote data in last line)"]))
        else:
            bsz, bpx, apx, asz, maxpos = q
            note = "max-position (%s)" % maxpos if maxpos else ""
            rows.append((True, [market, bsz, bpx, apx, asz, note]))

    # QUOTING first, then NOT QUOTING at the BOTTOM; each section alphabetical by market.
    rows.sort(key=lambda r: (not r[0], r[1][0].lower()))
    rows = [cells for _q, cells in rows]

    heads = ["MARKET", "BIDSIZE", "BIDPRICE", "ASKPRICE", "ASKSIZE", "NOTES"]
    aligns = ["<", ">", ">", ">", ">", "<"]
    widths = [len(h) for h in heads]
    for r in rows:
        for i, c in enumerate(r):
            widths[i] = max(widths[i], len(c))

    def render(cells):
        return "  ".join("%*s" % ((-widths[i] if aligns[i] == "<" else widths[i]), c)
                         for i, c in enumerate(cells)).rstrip()

    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print("\n  %s market_maker quotes [%s] -- %d quoting, %d NOT quoting  (%d logs in %s, %s)\n"
          % (TITLE[args.venue], args.network, n_quoting, len(stale), len(files), args.log_dir, stamp))
    if not files:
        print("  (no logs matching %s)\n" % pattern)
        return
    print("  " + render(heads))
    print("  " + "-" * len(render(heads)))
    for r in rows:
        print("  " + render(r))
    if stale:
        print("\n  NOT QUOTING: " + ", ".join(sorted(stale, key=str.lower)))
    print()


if __name__ == "__main__":
    main()
