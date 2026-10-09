# NWWT Pocket Stream

Pocket Option market watch frontend plus a separate Python bridge and Node scanner.

## How the pieces fit together

- `src/` is the React frontend built by Vite
- `server.js` runs the scanner API on port 3000 and serves the built frontend from `dist/`
- `bridge/po_bridge.py` owns the Pocket Option connection and exposes sanitized data only on `127.0.0.1:8765`
- The scanner remains stopped until the bridge proves authentication, websocket connection, time sync, real assets, a fresh real tick, and a verified one minute candle

## Setup

Use three terminals from the repository root.

### 1. Install dependencies and configure the frontend

```bash
npm install
cp .env.example .env
```

Put the complete Pocket Option SSID in `.env` as the value of `POCKET_OPTION_SSID`. Do not commit `.env`.

### 2. Start the Python bridge

```python
python3 -m venv .venv
```

Activate the environment, then install the pinned library and start the bridge:

```bash
pip install -r bridge/requirements.txt
python bridge/po_bridge.py
```

### 3. Start the Node API

```bash
npm start
```

This starts the API and scanner server at `http://localhost:3000`. Build the frontend first for production:

```bash
npm run build
npm start
```

### Development mode

Keep the Node API and Python bridge running, then run `npm run dev` in another terminal. Vite serves the frontend on port 5173 and proxies `/api` requests to the Node server on port 3000.

## Connection verification

The scanner stays stopped and `/api/state` reports `connected: false` until all six checks pass:

| Check | Evidence |
|---|---|
| Authenticated | The library confirms the server's `successauth` |
| Websocket connected | The underlying websocket reports OPEN |
| Time synchronized | The library reports synchronized time |
| Real asset catalog | The live catalog contains assets |
| Real tick | A subscribed asset has a fresh tick |
| Real one minute candle | Historical candles have verified 60 second spacing |

Inspect bridge status locally at `http://127.0.0.1:8765/state`.

## Assets

The bridge reads the live catalog from the library, keeps available symbols ending in `_otc`, and subscribes in small batches. It does not invent symbols or use a fallback list. The existing scanner evaluates one asset at a time: set `SCANNER_ASSET` to an available OTC symbol or leave it unset to select the available asset with the highest reported payout.

## Security and limitations

- The bridge binds to loopback only; do not expose its port publicly
- The SSID is read by the bridge, removed from its process environment, and redacted from logs and responses
- This needs a persistent host that can run both Python and Node processes. A static frontend host alone, including a standard static Vercel deployment, cannot keep this bridge alive
- Live Pocket Option connectivity has not been certified by this repository update; verify it with an authorized session before relying on the feed
