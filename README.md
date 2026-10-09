# NWWT Signal Desk — Pocket Option scanner

Two processes:

- **`bridge/po_bridge.py`** (Python) owns the Pocket Option connection through
  [chema-creator/PocketOptionApi](https://github.com/chema-creator/PocketOptionApi),
  pinned to commit `5b7418a`. It reads `POCKET_OPTION_SSID`, proves the
  connection, and serves sanitized results on `127.0.0.1` only.
- **`server.js`** (Node) is the existing scanner. It polls the bridge and feeds
  real candles into its existing candle pipeline. The strategy code is unchanged.

No frame, method name or auth format in this project is invented: the bridge
calls only the library's own `PocketOption` interface.

## Run

```
python3 -m venv .venv && . .venv/bin/activate
pip install -r bridge/requirements.txt
cp .env.example .env        # then put your SSID in .env (single-quoted)
python bridge/po_bridge.py  # terminal 1
npm install && npm start    # terminal 2, then open http://localhost:3000
```

The SSID must be the **complete** `42["auth",{"session":...,"isDemo":...,"uid":...}]`
string. The library does not validate it (a malformed one silently becomes an
empty demo session), so the bridge refuses to connect unless `session`,
`isDemo` and `uid` are present.

## When it counts as connected

The scanner stays stopped, and `/api/state` reports `connected: false`, until
the bridge proves all six:

| Proof | Source |
|---|---|
| authenticated | `check_connect()`, true only after the server's `successauth` |
| websocket connected | the library's underlying websocket reports OPEN |
| time synchronized | `is_time_synced()` |
| real asset count > 0 | `get_assets()` |
| real tick received | `get_realtime_ticks()` on a subscribed asset, newer than 60s |
| real 1 minute candle received | `get_historical_candles(asset, 60)`, spacing verified as 60s |

Check it directly: `curl http://127.0.0.1:8765/state`. It also reports
authentication status, websocket status, time sync, real asset count, active
subscriptions, last tick and candle timestamps, last received asset,
reconnect count and the last authentication error. If the SSID is invalid or
expired, the library's own message is shown and the bridge stops; it does not
retry or try another protocol. Restart the bridge after fixing the SSID.

## Assets

The bridge reads the real catalog from `get_assets()`, keeps the symbols ending
in `_otc` that are `is_available`, and subscribes to each at a 60 second
period, a few at a time. It re-reads the catalog every 30 seconds and drops
assets that stop being available. Symbols are never invented and there is no
fallback list.

The existing scanner reads one flat candle stream, so it is fed **one**
asset: `SCANNER_ASSET` if set, otherwise the available OTC asset with the
highest payout. Other assets are subscribed and tick-tracked but not scanned.

## Notes

- Candle and tick timestamps are the library's "server-native" times, passed
  through unconverted (the library documents these as typically UTC+2).
- The bridge never returns the SSID from `/state`, `/candles` or `/ticks`. It is
  removed from the process environment after it is read, library INFO/DEBUG
  logging is silenced (the library logs the start of its auth message at INFO),
  and all logs and responses are redacted. Do not set DEBUG logging.
- Not verified from the build environment, so confirm on your machine: live
  behaviour with the full OTC list subscribed, and the library with
  `websockets` 17 (it was written for 12/13). If a live run errors in the
  connect layer, pin an older `websockets`.
- The library's tick handler stores only the first entry of each `updateStream`
  message; the per-asset tick proof shows which assets actually stream.