#!/usr/bin/env python3
"""Pre-deploy check: scan the orderbook-server port range and report anything
already bound, so a fresh toolkit deploy doesn't collide with a running service.

The orderbook servers (wsorderbook.py) bind PORT_BASE[network] + marketId
(marketIds start at 1): mainnet 10001-10999, testnet 11001-11999, staging
12001-12999. This scans 10000-12999 inclusive by attempting to bind AND listen
on each TCP port on --host -- mirroring wsorderbook's aiohttp socket
(SO_REUSEADDR + listen), so a port reported free is one the server can actually
take. A port that can't be taken is IN USE.

IMPORTANT for use as a deploy gate:
  * --host MUST match the host wsorderbook.py binds (both default 127.0.0.1). If
    you deploy with --host 0.0.0.0, scan with --host 0.0.0.0 -- a loopback-only
    scan can miss a process on another interface that would still block the
    public bind.
  * The base ports (10000/11000/12000) are reserved but never bound by a server
    (no marketId 0), so they're reported as a note, NOT a deploy blocker.

  checkports.py                              # scan the whole range 10000-12999 on 127.0.0.1
  checkports.py --testnet                    # scan only that network's range (11000-11999)
  checkports.py --host 0.0.0.0               # check the public-bind interface
  checkports.py --start 11000 --end 11999    # explicit custom range
  checkports.py --quiet                      # summary + conflicts only

--testnet/--staging/--mainnet are a convenience that set the range to that
network's 1000-port band (mainnet 10000-10999, testnet 11000-11999, staging
12000-12999); they're optional and are REJECTED if combined with --start/--end.

Exit status (so it can gate a deploy script):
  0  every port free            -> clear to deploy
  1  one or more ports in use   -> resolve before deploying
  2  a port could not be tested (and none were in use)
"""
import argparse
import errno
import socket
import sys

# Must match wsorderbook.py / showorderbook.py.
PORT_BASE = {"mainnet": 10000, "testnet": 11000, "staging": 12000}
RANGE_START, RANGE_END = 10000, 12999   # inclusive; the reserved orderbook range (staging tops at 12999)


def describe_port(port):
    """Map a port back to its network + marketId, for a friendly report."""
    for net, base in PORT_BASE.items():
        if base <= port < base + 1000:
            offset = port - base
            return (f"{net} marketId {offset}" if offset
                    else f"{net} base port -- reserved, never bound by a server (marketIds start at 1)")
    return "unassigned (outside the testnet/staging/mainnet ranges)"


def check_port(host, port):
    """Try to bind AND listen on host:port. Returns 'free', 'inuse', or ('error', detail).

    Mirrors how wsorderbook.py actually binds -- aiohttp web.TCPSite -> asyncio
    create_server, which on Unix sets SO_REUSEADDR and calls listen(). Matching the
    server's options exactly means a port reported free is one the server can really
    take: SO_REUSEADDR makes a lingering TIME_WAIT socket bindable for BOTH (so it's
    not a false positive), while an ACTIVE listener still fails bind -> correctly
    in-use. listen() also catches the rare case where bind succeeds but the socket
    can't transition to listening.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind((host, port))
        s.listen(1)
        return "free"
    except OSError as e:
        if e.errno == errno.EADDRINUSE:
            return "inuse"
        return ("error", e.strerror or str(e))
    finally:
        s.close()


def port_arg(s):
    v = int(s)
    if not (1 <= v <= 65535):
        raise argparse.ArgumentTypeError("must be a port in 1-65535")
    return v


def main():
    p = argparse.ArgumentParser(
        description="Scan the orderbook-server port range for conflicts before deploying.")
    p.add_argument("--host", default="127.0.0.1",
                   help="interface to test-bind -- MUST match the host wsorderbook.py binds "
                        "(both default 127.0.0.1). If you deploy with --host 0.0.0.0, scan with "
                        "--host 0.0.0.0, else a loopback-only scan misses a process on another "
                        "interface that would still block the public bind.")
    p.add_argument("--start", type=port_arg, default=None,
                   help=f"first port of a custom range (use WITH --end; default {RANGE_START}); "
                        "not allowed with a network flag")
    p.add_argument("--end", type=port_arg, default=None,
                   help=f"last port, inclusive (use WITH --start; default {RANGE_END}); "
                        "not allowed with a network flag")
    net = p.add_mutually_exclusive_group()
    net.add_argument("--mainnet", dest="network", action="store_const", const="mainnet",
                     help="scan only the mainnet range (10000-10999)")
    net.add_argument("--testnet", dest="network", action="store_const", const="testnet",
                     help="scan only the testnet range (11000-11999)")
    net.add_argument("--staging", dest="network", action="store_const", const="staging",
                     help="scan only the staging range (12000-12999)")
    p.add_argument("--quiet", action="store_true", help="print only the summary and any conflicts")
    args = p.parse_args()

    # A network flag sets the range to that network's band; --start/--end are an
    # alternative explicit range. The two are mutually exclusive.
    if args.network:
        if args.start is not None or args.end is not None:
            p.error("--testnet/--staging/--mainnet cannot be combined with --start/--end")
        base = PORT_BASE[args.network]
        start, end = base, base + 999
    else:
        if (args.start is None) != (args.end is None):
            p.error("--start and --end must be given together")
        if args.start is not None:
            start, end = args.start, args.end
        else:
            start, end = RANGE_START, RANGE_END
    if end < start:
        raise SystemExit("checkports: --end must be >= --start")

    # Validate the host is bindable at all before scanning the whole range,
    # otherwise an unusable --host would print one error per port.
    probe = check_port(args.host, 0)        # port 0 = OS-assigned ephemeral
    if isinstance(probe, tuple):
        raise SystemExit(f"checkports: cannot bind on host {args.host!r}: {probe[1]}")

    total = end - start + 1
    if not args.quiet:
        scope = f" [{args.network}]" if args.network else ""
        print(f"Scanning ports {start}-{end} ({total}){scope} on {args.host} ...")

    # Base ports (10000/11000/12000) are reserved but never bound by a server, since
    # marketIds start at 1 -- so report them but DON'T let them block a deploy.
    base_ports = set(PORT_BASE.values())
    inuse, errors, base_busy = [], [], []
    for port in range(start, end + 1):
        r = check_port(args.host, port)
        if r == "inuse":
            if port in base_ports:
                base_busy.append(port)
                print(f"  note   : {port}  ({describe_port(port)}) in use -- not a server port, won't block")
            else:
                inuse.append(port)
                print(f"  IN USE : {port}  ({describe_port(port)})")
        elif isinstance(r, tuple):
            errors.append(port)
            print(f"  ERROR  : {port}  ({describe_port(port)}) -- {r[1]}")

    print()
    if base_busy:
        print(f"note: {len(base_busy)} reserved base port(s) {base_busy} in use -- not used by any "
              f"orderbook server, so NOT a deploy blocker.")
    if not inuse and not errors:
        print(f"All clear on {args.host}: {total} ports scanned, no orderbook-server-port conflicts.")
        return 0
    if inuse:
        print(f"{len(inuse)} orderbook-server port(s) IN USE on {args.host} -- resolve before deploying.")
    if errors:
        print(f"{len(errors)} port(s) could not be tested (see ERROR lines).")
    return 1 if inuse else 2


if __name__ == "__main__":
    sys.exit(main())
