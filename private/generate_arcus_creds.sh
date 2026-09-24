#!/bin/sh
# Build arcus_creds_<network>.json from frontend-issued API keys. Shows the
# entries and asks for confirmation before writing. The Ethereum private key is
# deliberately NOT collected -- it isn't needed for trading (only at one-time
# registration).
#
# Usage: generate_arcus_creds.sh (--testnet | --staging) [--verifyonline]
#   --testnet|--staging  REQUIRED: which network this credential set is for. Sets
#                        the output file (arcus_creds_<network>.json) and the
#                        server used by --verifyonline.
#   --verifyonline       After the local key check, also confirm with the server
#                        that the public key is registered and ACTIVE (GET /v1/apiKeys).

NETWORK=""
VERIFY_ONLINE=0

usage() {
  echo "usage: $0 (--testnet | --staging | --mainnet) [--verifyonline]"
}

for arg in "$@"; do
  case "$arg" in
    --testnet|--staging|--mainnet)
      net=${arg#--}
      if [ -n "$NETWORK" ] && [ "$NETWORK" != "$net" ]; then
        echo "specify only one of --testnet/--staging/--mainnet"; exit 2
      fi
      NETWORK="$net" ;;
    --verifyonline) VERIFY_ONLINE=1 ;;
    -h|--help)
      usage
      echo "  --testnet|--staging|--mainnet  REQUIRED: which network's creds to build"
      echo "  --verifyonline       after local checks, confirm the key is registered ACTIVE"
      exit 0 ;;
    *)
      echo "unknown option: $arg"; usage; exit 2 ;;
  esac
done

if [ -z "$NETWORK" ]; then
  echo "error: one of --testnet, --staging, or --mainnet is required."; usage; exit 2
fi

# Write BESIDE this script (where arcus_common_private resolves arcus_creds_<network>.json), NOT in the
# caller's cwd -- so it lands in the right place regardless of where the script is invoked from.
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd) || SCRIPT_DIR="."
OUT="$SCRIPT_DIR/arcus_creds_${NETWORK}.json"
case "$NETWORK" in
  testnet) BASE="https://api.testnet.arcus.xyz" ;;
  staging) BASE="https://api.staging.arcus.xyz" ;;
  mainnet) BASE="https://api.arcus.xyz" ;;
esac

# Bail early if a creds file is already there, before making the user type
# anything -- saves the entry work if they don't actually want to overwrite.
if [ -e "$OUT" ]; then
  printf '%s already exists. Overwrite? [y/N]: ' "$OUT"
  read overwrite
  case "$overwrite" in
    [Yy]|[Yy][Ee][Ss]) ;;                       # fall through and continue
    *) echo "Aborted -- existing $OUT left unchanged."; exit 1 ;;
  esac
fi

printf "Enter eth_address: ";     read eth_address
# Validate the address format locally (0x + 40 hex), independent of --verifyonline, so a
# mis-paste is caught before the file is written -- and so the value is guaranteed safe to
# embed in JSON below. (The API keys are already validated as hex during key verification.)
if ! printf '%s' "$eth_address" | grep -Eq '^0x[0-9a-fA-F]{40}$'; then
  echo "  FAIL: eth_address must be '0x' followed by 40 hex characters."
  exit 1
fi
printf "Enter api_public_key: ";  read api_public_key
# Read the private key WITHOUT echoing it to the terminal (restore the tty even on Ctrl-C).
printf "Enter api_private_key: "
_stty_saved=$(stty -g 2>/dev/null)
[ -n "$_stty_saved" ] && trap 'stty "$_stty_saved" 2>/dev/null' INT TERM EXIT
stty -echo 2>/dev/null
read api_private_key
[ -n "$_stty_saved" ] && { stty "$_stty_saved" 2>/dev/null; trap - INT TERM EXIT; }
printf '\n'

# Verify the API private key locally before writing: it must be a valid Ed25519
# key, able to sign, and the public key it derives must match the api_public_key
# entered above. Catches the common mis-paste before it lands in the creds file.
echo
echo "Verifying API private key..."
# Pass the PRIVATE KEY on STDIN (a pipe), NOT an env var: an env var is readable via /proc/<pid>/environ for
# the subprocess's whole lifetime and is inherited by any child it spawns; a pipe is neither, and nothing
# touches disk. The public key is not secret, so it stays in the env. (`python3 -c` so stdin is free for the key.)
printf '%s' "$api_private_key" | API_PUB="$api_public_key" python3 -c '
import os, sys
try:
    from cryptography.hazmat.primitives.asymmetric import ed25519
except ImportError:
    sys.exit("  FAIL: python cryptography package not installed; cannot verify.")

priv_hex = sys.stdin.read().strip()
pub_hex  = (os.environ.get("API_PUB")  or "").strip()

try:
    raw = bytes.fromhex(priv_hex)
except ValueError:
    sys.exit("  FAIL: api_private_key is not valid hex.")
if len(raw) != 32:
    sys.exit(f"  FAIL: api_private_key must be 32 bytes (64 hex chars); got {len(raw)}.")

try:
    priv = ed25519.Ed25519PrivateKey.from_private_bytes(raw)
except Exception as e:
    sys.exit(f"  FAIL: not a valid Ed25519 private key: {e}")

# Sign/verify round trip proves the key can actually sign.
msg = b"arcus-cred-selftest"
priv.public_key().verify(priv.sign(msg), msg)

derived = priv.public_key().public_bytes_raw().hex()
if pub_hex.lower() != derived.lower():
    sys.exit("  FAIL: api_private_key does NOT match the api_public_key entered.\n"
             f"         entered public: {pub_hex}\n"
             f"         derived public: {derived}")

print("  OK: valid Ed25519 key, signs correctly, and matches the public key.")
'
if [ $? -ne 0 ]; then
  echo "Key verification failed -- nothing written."
  exit 1
fi

# Optional: confirm with the server that this public key is actually registered
# and ACTIVE for the given address (same unsigned read step3_verify.py uses).
if [ "$VERIFY_ONLINE" -eq 1 ]; then
  echo
  echo "Checking the key is registered and ACTIVE on $BASE ..."
  BASE="$BASE" ADDR="$eth_address" API_PUB="$api_public_key" python3 - <<'PY'
import os, sys, json, urllib.request, urllib.error
base = os.environ["BASE"]; addr = os.environ["ADDR"]
pub = (os.environ.get("API_PUB") or "").strip().lower()
try:
    req = urllib.request.Request(f"{base}/v1/apiKeys?address={addr}", method="GET",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        keys = json.loads(r.read() or b"{}").get("apiKeys", [])
except urllib.error.HTTPError as e:
    sys.exit(f"  FAIL: server returned HTTP {e.code}: {e.read().decode()[:200]}")
except Exception as e:
    sys.exit(f"  FAIL: could not reach {base}: {e}")

mine = next((k for k in keys if (k.get("apiKey") or "").lower() == pub), None)
if mine is None:
    sys.exit(f"  FAIL: this public key is not registered for {addr} "
             f"({len(keys)} key(s) found for that address).")
if mine.get("status") != "ACTIVE":
    sys.exit(f"  FAIL: key found but status is {mine.get('status')!r}, not ACTIVE.")
print(f"  OK: key is registered and ACTIVE (name: {mine.get('apiWalletName','?')}).")
PY
  if [ $? -ne 0 ]; then
    echo "Online verification failed -- nothing written."
    exit 1
  fi
fi

# Assemble once so what we show is exactly what we'd write. Build the JSON with
# json.dumps (not shell interpolation) so no character in any field can corrupt the
# file -- a quote/backslash/newline is escaped, not injected.
# Private key on STDIN again (never in env / never on disk); eth_address + public key are not secret -> env is fine.
creds=$(printf '%s' "$api_private_key" | ETH="$eth_address" PUB="$api_public_key" python3 -c '
import os, json, sys
print(json.dumps({
    "eth_address": os.environ["ETH"],
    "api_public_key": os.environ["PUB"],
    "api_private_key": sys.stdin.read().strip(),
    "account_index": 0,
}, indent=2))
')
if [ $? -ne 0 ] || [ -z "$creds" ]; then
  echo "Failed to assemble creds JSON -- nothing written."
  exit 1
fi

echo
echo "About to write the following to $OUT:"
# Show the entries for review but MASK the private key (the real value still gets written below).
cat <<EOF
{
  "eth_address": "$eth_address",
  "api_public_key": "$api_public_key",
  "api_private_key": "<hidden, ${#api_private_key} chars>",
  "account_index": 0
}
EOF
echo
printf "Everything look OK? [y/N]: "
read answer

case "$answer" in
  [Yy]|[Yy][Ee][Ss])
    # A plain '> "$OUT"' TRUNCATES an existing file but KEEPS its old mode -- so overwriting a
    # group/world-readable creds file would write the new private key into an insecure file
    # (umask only sets the mode of NEWLY created files). Write to a fresh temp (mktemp creates
    # it mode 0600) in the SAME dir, then atomically mv it into place: the secret never lands in
    # a readable file, and $OUT ends up 0600 regardless of any pre-existing mode.
    TMP=$(mktemp "$OUT.XXXXXX") || { echo "Could not create temp file -- nothing written."; exit 1; }
    if ! printf '%s\n' "$creds" > "$TMP"; then
      rm -f "$TMP"; echo "Failed to write creds -- nothing written."; exit 1
    fi
    chmod 600 "$TMP" 2>/dev/null            # re-assert 0600 (mktemp already does this)
    if ! mv -f "$TMP" "$OUT"; then
      rm -f "$TMP"; echo "Failed to move creds into place -- nothing written."; exit 1
    fi
    chmod 600 "$OUT" 2>/dev/null            # belt-and-suspenders for a cross-filesystem mv
    echo "Wrote $OUT (mode 600)."
    ;;
  *)
    echo "Aborted -- nothing written."
    exit 1
    ;;
esac
