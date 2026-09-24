#!/usr/bin/env bash
# showlogs.sh - print a recent time window of an arcus poller OR market-maker log.
#
# Every log line is stamped "[HH:MM:SS <epoch>]" (poller/MM) or "[YYYY-MM-DD HH:MM:SS <epoch>]" (WS tools),
# where the trailing integer is unix-epoch seconds (read as the LAST token inside the first bracket).
# This prints the lines whose epoch is within --seconds of the real current time (default 86400 = last
# 24h). Because the epoch is ABSOLUTE, the window is EXACT: no date guessing, no midnight-wrap
# reconstruction, no clock-skew edge cases. A line WITHOUT that prefix (a continuation / traceback
# line) is attached to the preceding stamped line, so it is kept or dropped together with it.
#
# Usage:
#   ./showlogs.sh <logfile>                  # last 24h (default)
#   ./showlogs.sh <logfile> --seconds 3600   # last hour
#   ./showlogs.sh --seconds 600 <logfile>    # last 10 min (flag order-independent)
#
# Works on any epoch-stamped arcus log, e.g.:
#   /tmp/arcus_account_poller_mainnet.log
#   /tmp/arcus_AAPL-USD_mainnet.log          (per-market market-maker log)
# With no logfile it defaults to the mainnet account-poller log. Requires poller/MM code new enough to
# emit the epoch stamp; a legacy "[HH:MM:SS]"-only log produces no output. POSIX awk (mawk/BSD/busybox).

set -euo pipefail

seconds=86400
log=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --seconds)
      [ "$#" -ge 2 ] || { echo "showlogs.sh: --seconds requires a value" >&2; exit 1; }
      seconds="$2"; shift 2 ;;
    --seconds=*) seconds="${1#*=}"; shift ;;
    -h|--help)
      echo "usage: showlogs.sh [<logfile>] [--seconds N]   (default logfile = mainnet poller; N = 86400 = 24h)"
      exit 0 ;;
    -*) echo "showlogs.sh: unknown option: $1" >&2; exit 1 ;;
    *)  log="$1"; shift ;;
  esac
done

case "$seconds" in
  ''|*[!0-9]*) echo "showlogs.sh: --seconds must be a positive integer (got '$seconds')" >&2; exit 1 ;;
esac
[ "$seconds" -gt 0 ] || { echo "showlogs.sh: --seconds must be > 0" >&2; exit 1; }

log="${log:-/tmp/arcus_account_poller_mainnet.log}"

if [ ! -r "$log" ]; then
  echo "showlogs.sh: cannot read log file: $log" >&2
  exit 1
fi

now_epoch=$(date +%s)

awk -v now_epoch="$now_epoch" -v seconds="$seconds" '
/^\[[0-9]/ {
  # First bracket is the timestamp: "[HH:MM:SS <epoch>]" (poller/MM) OR "[YYYY-MM-DD HH:MM:SS <epoch>]"
  # (WS tools). The epoch is the LAST space-delimited token inside the first "]" -- read it
  # POSITION-INDEPENDENTLY so BOTH stamp shapes window correctly. A "[...]" whose last inner token is not
  # all digits is not a timestamp line -> fall through to the continuation rule below.
  cb = index($0, "]")
  nf = split(substr($0, 2, cb - 2), f, " ")
  if (f[nf] ~ /^[0-9]+$/) { epoch[++n] = f[nf] + 0; rec[n] = $0; next }
}
{ if (n) rec[n] = rec[n] ORS $0 }      # untimestamped continuation -> attach to the preceding record
END {
  if (n == 0) exit
  cutoff = now_epoch - seconds
  for (i = 1; i <= n; i++) if (epoch[i] >= cutoff) print rec[i]
}
' "$log"
