# arcustools

Command-line tools for the [Arcus](https://arcus.xyz) perpetuals exchange: account and
market reads, live WebSocket streams, and credentialed trading (order placement, market
making, TWAP execution, monitoring).

The tools are split into two directories by what they need to run:

## `public/`

Read-only tools. They query account-scoped and market data over Arcus' public REST and
WebSocket endpoints and **need no credentials** — nothing here signs or submits an order.

- Displays: `display_balance.py`, `display_positions.py`, `display_orders.py`,
  `display_fills.py`, `display_funding.py`, `display_transfers.py`, `showmarkets.py`,
  `showorderbook.py`, `calculate_pnl.py`, `funding_dashboard.py`
- WebSocket streams: `wsaccount.py`, `wsexchange.py`, `wsorderbook.py`,
  `wsorderbook_reader.py`, `wsorderbook_reader_companion.py`
- Utilities: `checkports.py`, `ws_liveness_check.py`, `arcus_common_public.py` (shared helpers)

## `private/`

Credentialed trading tools. These **sign and submit** transactions, so each one needs an
API key.

- Trading: `place_order.py`, `cancel_order.py`, `modify_order.py`, `close_position.py`,
  `schedule_cancel.py`
- Bots / automation: `market_maker.py`, `pivot_trader.py`, `twap.py` (TWAP order execution:
  slices a parent buy/sell into timed child orders), `account_poller.py`
- Monitoring: `marketdata_monitor.py`, `mm_dashboard.py`, `showlogs.sh`
- Internals: `ordersign.py` (order signing), `arcus_common_private.py`, `arcus_redis.py`

## Credentials

The trading tools read an API key from a per-network file **`arcus_creds_<network>.json`**
(e.g. `arcus_creds_mainnet.json`). **These files are intentionally not in this repo** —
they hold a private signing key and must never be committed. Generate your own:

```bash
private/generate_arcus_creds.sh
```

Keep the resulting `arcus_creds_*.json` outside version control.

## Requirements

Python 3. The public read/display tools are standard-library only. Some private tools use
Redis for shared state.

## Safety

`public/` tools cannot move funds. `private/` tools place real orders — read each tool's
`--help` and test against a testnet key before trading on mainnet.
