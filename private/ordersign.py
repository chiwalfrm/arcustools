"""
Arcus v5 — Order Signing Reference
====================================
Self-contained implementation of the two-layer authentication scheme.
No internal SDK imports — drop this file into any project and adapt.

Requirements
------------
    pip install cryptography eth-account

Two-layer scheme overview
-------------------------

Layer 1 — Wallet ownership (one-time, POST /v1/createApiKey)
    secp256k1 EIP-191 personal_sign over canonical JSON:
        {"apiWalletName":"…","apiWalletPublicKey":"<ed25519-pub-hex>","validUntil":<ms>}
    The gateway runs ecrecover → verifies the recovered address matches `address`.

Layer 2 — Per-request auth (every mutating POST)
    Ed25519 signature over a canonical message. Two message formats exist:

    a) Typed payload (placeOrder / cancelOrder / modifyOrder / batch variants)
       The JSON object IS the signing message — no prefix.
       Keys are in fixed alphabetical order; values are engine integers, not decimal strings.

       placeOrder:
           {"ad":"0x…","ai":N,[,"c":"…"],"ct":N,"g":N,"m":N,"op":1,"p":N,"q":N,"r":0|1,"s":N,"t":N,"v":1}

       cancelOrder:
           {"ad":"0x…","ai":N,[,"c":"…"],"ct":N,[,"id":"…"],"m":N,"op":2,"v":1}

       modifyOrder:
           {"ad":"0x…","ai":N,[,"c":"…"],"ct":N,"g":N,[,"id":"…"],"m":N,"op":3,"p":N,"q":N,"r":0|1,"s":N,"t":N,"v":1}
           r/s/t echo the resting order's immutable attributes for block-validator verification.

    b) Legacy (cancelAllOrders, setLeverage, createApiKey, WebSocket auth)
       Message = timestamp_ns + action + canonicalJSON(body)
       where action = camelCase path segment (e.g. "cancelAllOrders").

    Headers on every signed request:
        X-API-Key:   <64-hex ed25519 public key>       ← IS the api key
        X-Timestamp: <unix nanoseconds as string>       ← within ±30 s of server clock
        X-Signature: <128-hex ed25519 signature>

Reference test vectors
-----------------------
    placeOrder:
        payload  = '{"ad":"0x1111111111111111111111111111111111111111","ai":0,"c":"12345",
                     "ct":1718644999000000000,"g":1718645000000000000,"m":1,"op":1,
                     "p":60000000000,"q":1000000000,"r":0,"s":0,"t":0,"v":1}'
        market   : id=1, tick_size="0.000001", quantum_size="0.000000001"
        price    : "60000"   → 60_000_000_000 ticks
        quantity : "1.0"     → 1_000_000_000 quantums

    modifyOrder:
        payload  = '{"ad":"0x1111111111111111111111111111111111111111","ai":0,
                     "ct":1718644999000000000,"g":4102444800000000000,"id":"ord-abc","m":1,"op":3,
                     "p":65000000000,"q":500000000,"r":0,"s":0,"t":0,"v":1}'
"""

from __future__ import annotations

import json
import time
from decimal import Decimal, InvalidOperation
from typing import Any

from cryptography.hazmat.primitives.asymmetric import ed25519

# ── Constants ─────────────────────────────────────────────────────────────────

# op field values
OP_PLACE             = 1  # placeOrder
OP_CANCEL            = 2  # cancelOrder
OP_MODIFY            = 3  # modifyOrder
OP_PLACE_UNTRIGGERED = 4  # placeOrder for TPSL / conditional orders (op=4 prevents cross-replay with op=1)

# side field values
SIDE_BUY  = 0
SIDE_SELL = 1

# time-in-force field values
TIF_GTC = 0   # Good-til-cancelled / Good-til-time
TIF_FOK = 1   # Fill-or-kill
TIF_IOC = 2   # Immediate-or-cancel
TIF_ALO = 3   # Add-liquidity-only (post-only)

PAYLOAD_VERSION = 1


# ── Unit conversion helpers ───────────────────────────────────────────────────

def price_to_ticks(price: str, tick_size: str) -> int:
    """Convert a human-readable decimal price to integer ticks.

    Uses exact arithmetic — raises ValueError if price is not evenly
    divisible by tick_size.

    Example:
        price_to_ticks("60000", "0.000001")  →  60_000_000_000
    """
    return _exact_divide(price, tick_size)


def size_to_quantums(size: str, quantum_size: str) -> int:
    """Convert a human-readable decimal size to integer quantums.

    Example:
        size_to_quantums("1.0", "0.000000001")  →  1_000_000_000
    """
    return _exact_divide(size, quantum_size)


def good_til_time_ns(good_til_time_us: str) -> int:
    """Convert a goodTilTime microsecond epoch string to nanoseconds.

    Returns 0 for "" or "0" (GTC orders have no expiry).
    """
    if good_til_time_us in ("", "0"):
        return 0
    return int(good_til_time_us) * 1000


def _exact_divide(value: str, divisor: str) -> int:
    if value in ("0", ""):
        return 0
    try:
        v, d = Decimal(value), Decimal(divisor)
    except InvalidOperation as exc:
        raise ValueError(f"invalid decimal: {exc}") from exc
    if d == 0:
        raise ValueError("divisor must not be zero")
    result = v / d
    rounded = result.to_integral_value()
    if result != rounded:
        raise ValueError(f"{value!r} is not evenly divisible by {divisor!r}")
    return int(rounded)


# ── Typed canonical payload builders (ordersign v1) ───────────────────────────
#
# The JSON string returned by each function IS the signing message.
# There is no timestamp prefix — the client_timestamp_ns is the "ct" field
# inside the object, and its string form is also the X-Timestamp header value.

def place_order_payload(
    *,
    address: str,
    account_index: int,
    client_id: str | None,
    client_timestamp_ns: int,
    good_til_time_ns_: int,
    market_id: int,
    price_ticks: int,
    quantity_quantums: int,
    reduce_only: bool,
    side: int,
    time_in_force: int,
) -> str:
    """Build the canonical placeOrder signing payload (ordersign v1).

    Key order: ad, ai, [c,] ct, g, m, op, p, q, r, s, t, v — fixed
    alphabetical, no whitespace.  `ad` is always lowercased; `r` is 0 or 1.
    """
    addr = address.lower()
    parts = [f'{{"ad":"{addr}","ai":{account_index}']
    if client_id:
        parts.append(f',"c":"{client_id}"')
    parts.append(f',"ct":{client_timestamp_ns}')
    parts.append(f',"g":{good_til_time_ns_}')
    parts.append(f',"m":{market_id}')
    parts.append(f',"op":{OP_PLACE}')
    parts.append(f',"p":{price_ticks}')
    parts.append(f',"q":{quantity_quantums}')
    parts.append(',"r":1' if reduce_only else ',"r":0')
    parts.append(f',"s":{side}')
    parts.append(f',"t":{time_in_force}')
    parts.append(f',"v":{PAYLOAD_VERSION}')
    parts.append("}")
    return "".join(parts)


def cancel_order_payload(
    *,
    address: str,
    account_index: int,
    client_timestamp_ns: int,
    market_id: int,
    order_id: str | None = None,
    client_id: str | None = None,
) -> str:
    """Build the canonical cancelOrder signing payload (ordersign v1).

    Exactly one of order_id or client_id must be provided.
    Key order: ad, ai, [c,] ct, [id,] m, op, v.
    """
    if not order_id and not client_id:
        raise ValueError("cancelOrder requires order_id or client_id")
    addr = address.lower()
    parts = [f'{{"ad":"{addr}","ai":{account_index}']
    if client_id:
        parts.append(f',"c":"{client_id}"')
    parts.append(f',"ct":{client_timestamp_ns}')
    if order_id:
        parts.append(f',"id":"{order_id}"')
    parts.append(f',"m":{market_id}')
    parts.append(f',"op":{OP_CANCEL}')
    parts.append(f',"v":{PAYLOAD_VERSION}')
    parts.append("}")
    return "".join(parts)


def modify_order_payload(
    *,
    address: str,
    account_index: int,
    client_timestamp_ns: int,
    good_til_time_ns_: int,
    market_id: int,
    price_ticks: int,
    quantity_quantums: int,
    reduce_only: bool,
    side: int,
    time_in_force: int,
    order_id: str | None = None,
    client_id: str | None = None,
) -> str:
    """Build the canonical modifyOrder signing payload (ordersign v1).

    ``order_id`` is always required (modify always identifies by server order ID).
    ``client_id`` is an echo of the resting order's original clientId — supply it
    if and only if the original placement included a clientId; the engine enforces
    the bidirectional invariant and rejects on mismatch.

    ``reduce_only``, ``side``, and ``time_in_force`` are immutable attributes of
    the resting order echoed into the payload so the block-validator can verify the
    signature without fetching the original order.

    ``good_til_time_ns_`` echoes the resting order's expiry; passing a new value
    triggers a cancel-replace with a fresh expiry.

    Key order: ad, ai, [c,] ct, g, [id,] m, op, p, q, r, s, t, v.
    """
    if not order_id:
        raise ValueError("modifyOrder requires order_id")
    addr = address.lower()
    parts = [f'{{"ad":"{addr}","ai":{account_index}']
    if client_id:
        parts.append(f',"c":"{client_id}"')
    parts.append(f',"ct":{client_timestamp_ns}')
    parts.append(f',"g":{good_til_time_ns_}')
    parts.append(f',"id":"{order_id}"')
    parts.append(f',"m":{market_id}')
    parts.append(f',"op":{OP_MODIFY}')
    parts.append(f',"p":{price_ticks}')
    parts.append(f',"q":{quantity_quantums}')
    parts.append(',"r":1' if reduce_only else ',"r":0')
    parts.append(f',"s":{side}')
    parts.append(f',"t":{time_in_force}')
    parts.append(f',"v":{PAYLOAD_VERSION}')
    parts.append("}")
    return "".join(parts)


# ── Legacy signing message (non-order endpoints) ──────────────────────────────
#
# Used by: cancelAllOrders, setLeverage, createApiKey (REST),
#          and WebSocket authentication / per-request signing.
#
# Message = timestamp_ns_str + action + canonicalJSON(body)
# where action = camelCase path segment (e.g. "cancelAllOrders").

def canonical_json(body: Any) -> bytes:
    """Serialize a dict to JSON with sorted keys and no whitespace."""
    return json.dumps(body, separators=(",", ":"), sort_keys=True).encode()


def legacy_signing_message(timestamp_ns: str, action: str, body: Any) -> bytes:
    """Build the legacy signing message: timestamp + action + canonicalJSON(body).

    Used for all endpoints that do NOT use the typed payload scheme:
    cancelAllOrders, setLeverage, batch operations, WebSocket auth.
    The HTTP method is NOT part of the message.
    """
    return timestamp_ns.encode() + action.encode() + canonical_json(body)


# ── Signer ────────────────────────────────────────────────────────────────────

class Signer:
    """Wraps an Ed25519 private key and produces signed request headers.

    The public key (pub_hex) IS the API key sent as X-API-Key.
    All timestamps are nanosecond-epoch strings — the server rejects anything
    outside a ±30 s window and treats each (api_key, timestamp) as single-use,
    so always generate a fresh timestamp per request.
    """

    def __init__(self, priv: ed25519.Ed25519PrivateKey) -> None:
        self._priv = priv
        self.pub_hex: str = priv.public_key().public_bytes_raw().hex()

    @classmethod
    def generate(cls) -> "Signer":
        """Generate a new Ed25519 keypair."""
        return cls(ed25519.Ed25519PrivateKey.generate())

    @classmethod
    def from_private_key_hex(cls, priv_hex: str) -> "Signer":
        """Load from a 64-hex or 32-hex private key string."""
        raw = bytes.fromhex(priv_hex)
        return cls(ed25519.Ed25519PrivateKey.from_private_bytes(raw[:32]))

    # ── Typed payload (ordersign v1) ─────────────────────────────────────────

    def sign_place_order(
        self,
        *,
        address: str,
        account_index: int,
        client_id: str | None,
        client_timestamp_ns: int,
        good_til_time_ns_: int,
        market_id: int,
        price_ticks: int,
        quantity_quantums: int,
        reduce_only: bool = False,
        side: int,
        time_in_force: int = TIF_GTC,
    ) -> dict[str, str]:
        """Sign a placeOrder. Returns {X-API-Key, X-Timestamp, X-Signature}.

        X-Timestamp = str(client_timestamp_ns) — matches the `ct` field in the
        payload, so generate client_timestamp_ns with time.time_ns() and pass
        the same value here and in the request body.
        """
        payload = place_order_payload(
            address=address,
            account_index=account_index,
            client_id=client_id,
            client_timestamp_ns=client_timestamp_ns,
            good_til_time_ns_=good_til_time_ns_,
            market_id=market_id,
            price_ticks=price_ticks,
            quantity_quantums=quantity_quantums,
            reduce_only=reduce_only,
            side=side,
            time_in_force=time_in_force,
        )
        sig = self._priv.sign(payload.encode())
        return {
            "X-API-Key":   self.pub_hex,
            "X-Timestamp": str(client_timestamp_ns),
            "X-Signature": sig.hex(),
        }

    def sign_cancel_order(
        self,
        *,
        address: str,
        account_index: int,
        market_id: int,
        order_id: str | None = None,
        client_id: str | None = None,
    ) -> dict[str, str]:
        """Sign a cancelOrder. Returns {X-API-Key, X-Timestamp, X-Signature}."""
        ts = time.time_ns()
        payload = cancel_order_payload(
            address=address,
            account_index=account_index,
            client_timestamp_ns=ts,
            market_id=market_id,
            order_id=order_id,
            client_id=client_id,
        )
        sig = self._priv.sign(payload.encode())
        return {
            "X-API-Key":   self.pub_hex,
            "X-Timestamp": str(ts),
            "X-Signature": sig.hex(),
        }

    def sign_modify_order(
        self,
        *,
        address: str,
        account_index: int,
        market_id: int,
        price_ticks: int,
        quantity_quantums: int,
        good_til_time_ns_: int,
        reduce_only: bool,
        side: int,
        time_in_force: int,
        order_id: str,
        client_id: str | None = None,
    ) -> dict[str, str]:
        """Sign a modifyOrder. Returns {X-API-Key, X-Timestamp, X-Signature}.

        X-Timestamp = str(client_timestamp_ns) — matches the ``ct`` field in the
        payload.  The same timestamp is used for both the payload and the header.
        """
        ts = time.time_ns()
        payload = modify_order_payload(
            address=address,
            account_index=account_index,
            client_timestamp_ns=ts,
            good_til_time_ns_=good_til_time_ns_,
            market_id=market_id,
            price_ticks=price_ticks,
            quantity_quantums=quantity_quantums,
            reduce_only=reduce_only,
            side=side,
            time_in_force=time_in_force,
            order_id=order_id,
            client_id=client_id,
        )
        sig = self._priv.sign(payload.encode())
        return {
            "X-API-Key":   self.pub_hex,
            "X-Timestamp": str(ts),
            "X-Signature": sig.hex(),
        }

    # ── Batch (per-element signatures) ───────────────────────────────────────

    def sign_batch_place_orders(
        self,
        orders: list[dict[str, Any]],
    ) -> tuple[str, list[dict[str, Any]]]:
        """Sign a batchPlaceOrders request.

        Each order in the list must already have its price and quantity in
        the REST body format (decimal strings). Each element is signed
        individually as a standalone placeOrder so that element signatures
        are identical whether submitted alone or inside a batch.

        Returns (timestamp_ns_str, signed_orders) where each element in
        signed_orders has an embedded `signature` field. Send the batch with:
            X-API-Key: signer.pub_hex
            X-Timestamp: timestamp_ns_str
            (no X-Signature on the outer envelope)
        """
        ts = str(time.time_ns())
        signed: list[dict[str, Any]] = []
        for order in orders:
            body = canonical_json(order)
            msg = legacy_signing_message(ts, "placeOrder", order)
            sig = self._priv.sign(msg)
            signed.append({**order, "signature": sig.hex()})
        return ts, signed

    def sign_batch_cancel_orders(
        self,
        cancels: list[dict[str, Any]],
    ) -> tuple[str, list[dict[str, Any]]]:
        """Sign a batchCancelOrders request (per-element, same pattern)."""
        ts = str(time.time_ns())
        signed = []
        for cancel in cancels:
            msg = legacy_signing_message(ts, "cancelOrder", cancel)
            sig = self._priv.sign(msg)
            signed.append({**cancel, "signature": sig.hex()})
        return ts, signed

    # ── Legacy scheme (cancelAllOrders, setLeverage, WS auth, etc.) ──────────

    def sign_legacy(self, path: str, body: Any) -> dict[str, str]:
        """Sign any endpoint that uses the legacy scheme.

        Message = timestamp_ns + action + canonicalJSON(body)
        where action = camelCase final path segment.

        Example:
            headers = signer.sign_legacy("/v1/cancelAllOrders", {"address": "0x…"})
        """
        ts = str(time.time_ns())
        action = path.rstrip("/").rsplit("/", 1)[-1]
        msg = legacy_signing_message(ts, action, body)
        sig = self._priv.sign(msg)
        return {
            "X-API-Key":   self.pub_hex,
            "X-Timestamp": ts,
            "X-Signature": sig.hex(),
        }

    def sign_ws(self, method: str, payload: Any) -> dict[str, str]:
        """Produce the auth triple for a mutating WebSocket request.

        Returns {"apiKey": …, "timestamp": …, "signature": …}.
        Embed on the WS request object alongside `type` and `payload`.
        """
        ts = str(time.time_ns())
        msg = legacy_signing_message(ts, method, payload)
        sig = self._priv.sign(msg)
        return {
            "apiKey":    self.pub_hex,
            "timestamp": ts,
            "signature": sig.hex(),
        }


# ── createApiKey bootstrap (secp256k1 EIP-191) ────────────────────────────────

def sign_create_api_key(
    eth_private_key_hex: str,
    api_wallet_name: str,
    api_wallet_public_key_hex: str,
    valid_until_ms: int,
) -> dict[str, str]:
    """Produce the EIP-191 (r, s, v) signature for POST /v1/createApiKey.

    The gateway computes:
        keccak256("\\x19Ethereum Signed Message:\\n" + len(msg) + msg)
    where msg = canonicalJSON({"apiWalletName":"…","apiWalletPublicKey":"…","validUntil":N})
    then runs ecrecover and rejects if the recovered address != request.address.

    valid_until_ms must be in [now + 1 day, now + 180 days] (milliseconds).

    Returns {"r": "0x…", "s": "0x…", "v": "0x…"}.
    Requires: pip install eth-account
    """
    from eth_account import Account  # noqa: PLC0415
    from eth_account.messages import encode_defunct  # noqa: PLC0415

    raw = eth_private_key_hex.lstrip("0x")
    account = Account.from_key(bytes.fromhex(raw))
    message = canonical_json({
        "apiWalletName":      api_wallet_name,
        "apiWalletPublicKey": api_wallet_public_key_hex,
        "validUntil":         valid_until_ms,
    })
    signed = account.sign_message(encode_defunct(primitive=message))
    return {"r": hex(signed.r), "s": hex(signed.s), "v": hex(signed.v)}


# ── Self-test (run: python ordersign.py) ──────────────────────────────────────

if __name__ == "__main__":
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

    def _verify(pub_hex: str, sig_hex: str, msg: bytes) -> None:
        pub = Ed25519PublicKey.from_public_bytes(bytes.fromhex(pub_hex))
        pub.verify(bytes.fromhex(sig_hex), msg)
        print("  ✓ signature verified")

    print("=== Arcus v5 Order Signing Self-Test ===\n")

    signer = Signer.generate()
    print(f"Generated keypair\n  pub (X-API-Key): {signer.pub_hex}\n")

    # ── 1. Reference vectors ─────────────────────────────────────────────────
    print("1. Reference payload vectors")

    # placeOrder
    PLACE_EXPECTED = (
        '{"ad":"0x1111111111111111111111111111111111111111","ai":0,"c":"12345",'
        '"ct":1718644999000000000,"g":1718645000000000000,"m":1,"op":1,'
        '"p":60000000000,"q":1000000000,"r":0,"s":0,"t":0,"v":1}'
    )
    got = place_order_payload(
        address="0x1111111111111111111111111111111111111111",
        account_index=0,
        client_id="12345",
        client_timestamp_ns=1718644999000000000,
        good_til_time_ns_=1718645000000000000,
        market_id=1,
        price_ticks=60_000_000_000,
        quantity_quantums=1_000_000_000,
        reduce_only=False,
        side=SIDE_BUY,
        time_in_force=TIF_GTC,
    )
    assert got == PLACE_EXPECTED, f"\ngot: {got}\nwant: {PLACE_EXPECTED}"
    print(f"  placeOrder:  {got}")
    print("  ✓ matches reference vector")

    # modifyOrder
    MODIFY_EXPECTED = (
        '{"ad":"0x1111111111111111111111111111111111111111","ai":0,'
        '"ct":1718644999000000000,"g":4102444800000000000,"id":"ord-abc","m":1,"op":3,'
        '"p":65000000000,"q":500000000,"r":0,"s":0,"t":0,"v":1}'
    )
    got_mod = modify_order_payload(
        address="0x1111111111111111111111111111111111111111",
        account_index=0,
        client_timestamp_ns=1718644999000000000,
        good_til_time_ns_=4102444800000000000,  # ~2100-01-01
        market_id=1,
        price_ticks=65_000_000_000,
        quantity_quantums=500_000_000,
        reduce_only=False,
        side=SIDE_BUY,
        time_in_force=TIF_GTC,
        order_id="ord-abc",
    )
    assert got_mod == MODIFY_EXPECTED, f"\ngot: {got_mod}\nwant: {MODIFY_EXPECTED}"
    print(f"  modifyOrder: {got_mod}")
    print("  ✓ matches reference vector\n")

    # ── 2. Unit conversions ──────────────────────────────────────────────────
    print("2. Unit conversions")
    assert price_to_ticks("60000", "0.000001") == 60_000_000_000
    assert size_to_quantums("1.0", "0.000000001") == 1_000_000_000
    assert good_til_time_ns("1718645000000000") == 1718645000000000000
    assert good_til_time_ns("") == 0
    print("  price_to_ticks('60000', '0.000001') →", price_to_ticks("60000", "0.000001"))
    print("  size_to_quantums('1.0', '0.000000001') →", size_to_quantums("1.0", "0.000000001"))
    print("  ✓ all conversions correct\n")

    # ── 3. placeOrder sign + verify ──────────────────────────────────────────
    print("3. placeOrder typed payload sign + verify")
    ts_ns = time.time_ns()
    headers = signer.sign_place_order(
        address="0xDeAdBeEf00000000000000000000000000000001",
        account_index=0,
        client_id="mm-order-1",
        client_timestamp_ns=ts_ns,
        good_til_time_ns_=ts_ns + 3_600_000_000_000,  # +1h
        market_id=0,
        price_ticks=price_to_ticks("60000", "0.000001"),
        quantity_quantums=size_to_quantums("0.001", "0.000000001"),
        side=SIDE_BUY,
        time_in_force=TIF_GTC,
    )
    print(f"  X-API-Key:   {headers['X-API-Key'][:16]}…")
    print(f"  X-Timestamp: {headers['X-Timestamp']}")
    print(f"  X-Signature: {headers['X-Signature'][:16]}…")
    payload_msg = place_order_payload(
        address="0xDeAdBeEf00000000000000000000000000000001",
        account_index=0,
        client_id="mm-order-1",
        client_timestamp_ns=ts_ns,
        good_til_time_ns_=ts_ns + 3_600_000_000_000,
        market_id=0,
        price_ticks=price_to_ticks("60000", "0.000001"),
        quantity_quantums=size_to_quantums("0.001", "0.000000001"),
        reduce_only=False,
        side=SIDE_BUY,
        time_in_force=TIF_GTC,
    ).encode()
    _verify(headers["X-API-Key"], headers["X-Signature"], payload_msg)

    # ── 4. cancelOrder sign + verify ─────────────────────────────────────────
    print("\n4. cancelOrder typed payload sign + verify")
    cancel_headers = signer.sign_cancel_order(
        address="0xDeAdBeEf00000000000000000000000000000001",
        account_index=0,
        market_id=0,
        order_id="ord-abc-123",
    )
    print(f"  X-Signature: {cancel_headers['X-Signature'][:16]}…")
    cancel_msg = cancel_order_payload(
        address="0xDeAdBeEf00000000000000000000000000000001",
        account_index=0,
        client_timestamp_ns=int(cancel_headers["X-Timestamp"]),
        market_id=0,
        order_id="ord-abc-123",
    ).encode()
    _verify(cancel_headers["X-API-Key"], cancel_headers["X-Signature"], cancel_msg)

    # ── 5. modifyOrder sign + verify ─────────────────────────────────────────
    print("\n5. modifyOrder typed payload sign + verify")
    mod_ts = time.time_ns()
    mod_headers = signer.sign_modify_order(
        address="0xDeAdBeEf00000000000000000000000000000001",
        account_index=0,
        market_id=0,
        good_til_time_ns_=mod_ts + 60 * 24 * 3600 * 1_000_000_000,  # +60 days
        price_ticks=price_to_ticks("61000", "0.000001"),
        quantity_quantums=size_to_quantums("0.001", "0.000000001"),
        reduce_only=False,
        side=SIDE_BUY,
        time_in_force=TIF_GTC,
        order_id="ord-abc-123",
    )
    print(f"  X-Signature: {mod_headers['X-Signature'][:16]}…")
    mod_msg = modify_order_payload(
        address="0xDeAdBeEf00000000000000000000000000000001",
        account_index=0,
        client_timestamp_ns=int(mod_headers["X-Timestamp"]),
        good_til_time_ns_=mod_ts + 60 * 24 * 3600 * 1_000_000_000,
        market_id=0,
        price_ticks=price_to_ticks("61000", "0.000001"),
        quantity_quantums=size_to_quantums("0.001", "0.000000001"),
        reduce_only=False,
        side=SIDE_BUY,
        time_in_force=TIF_GTC,
        order_id="ord-abc-123",
    ).encode()
    _verify(mod_headers["X-API-Key"], mod_headers["X-Signature"], mod_msg)

    # ── 6. Legacy scheme (cancelAllOrders) ───────────────────────────────────
    print("\n6. Legacy scheme (cancelAllOrders) sign + verify")
    body = {"address": "0xDeAdBeEf00000000000000000000000000000001", "marketId": 0}
    leg_headers = signer.sign_legacy("/v1/cancelAllOrders", body)
    leg_msg = legacy_signing_message(leg_headers["X-Timestamp"], "cancelAllOrders", body)
    _verify(leg_headers["X-API-Key"], leg_headers["X-Signature"], leg_msg)

    print("\n=== All tests passed ===")
