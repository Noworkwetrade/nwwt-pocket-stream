#!/usr/bin/env python3
"""NWWT Pocket Option bridge.

Owns the Pocket Option connection through chema-creator/PocketOptionApi
(pinned in requirements.txt) and nothing else: no strategy, no signals, no
synthetic data. It authenticates with the SSID from the POCKET_OPTION_SSID
secret, proves six conditions from what the library itself reports, and
serves the sanitized result on 127.0.0.1 for the Node scanner.

Interface used (all verified against the library source at the pinned commit):
  PocketOption(ssid), connect(), check_connect(), is_time_synced(),
  get_assets(), subscribe(asset, period), get_realtime_ticks(asset, limit),
  get_historical_candles(active, period, start_time, offset, count_request),
  get_server_timestamp()
plus the documented module state in pocketoptionapi.global_value
(websocket_error_reason, check_websocket_if_error).

The SSID is never logged, never returned by any endpoint, and is removed from
the process environment after it is read. The library itself logs the start
of its auth message at INFO level, so library logging is silenced below
WARNING and every log line and HTTP response is passed through a redactor.
"""

import json
import logging
import math
import os
import re
import signal
import sys
import threading
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

CANDLE_PERIOD = 60            # 1 minute candles, fixed
HISTORY_OFFSET = 9000         # large offset, as the library README recommends for loadHistoryPeriod
CATALOG_REFRESH_SECONDS = 30  # how often the real asset catalog is re-read
SUBSCRIBE_BATCH = 5           # subscriptions per loop pass, to pace the server
SUBSCRIBE_RETRY_SECONDS = 30
CANDLE_RETRY_SECONDS = 60
TIME_SYNC_GRACE_SECONDS = 20  # proceed to data steps after this even if time sync is still pending
TICK_FRESH_SECONDS = 60       # a tick feed older than this no longer counts as proven
TICK_CACHE = 50
LOOP_SECONDS = 1.0

TIME_BASIS = "server-native (as returned by the library, not converted to UTC)"

log = logging.getLogger("po_bridge")


# ---------------------------------------------------------------------------
# Secret handling
# ---------------------------------------------------------------------------
class Redactor:
    """Removes the SSID (and parts of it) from any text before it can leave the process."""

    def __init__(self):
        self._literals = []
        self._session_id = re.compile(r'session_id(?:\\)?";s:\d+:(?:\\)?"[^"\\]+')

    def register(self, ssid):
        if not ssid:
            return
        literals = {ssid, json.dumps(ssid)[1:-1]}
        match = re.search(r'"session"\s*:\s*"(.*)",\s*"isDemo"', ssid)  # same shape the library parses
        if match:
            raw = match.group(1)
            literals.add(raw)
            try:
                decoded = json.loads('"' + raw + '"')
                literals.add(decoded)
                literals.add(json.dumps(decoded)[1:-1])
            except ValueError:
                pass
        self._literals = sorted((x for x in literals if len(x) >= 6), key=len, reverse=True)

    def __call__(self, text):
        if not isinstance(text, str):
            return text
        for literal in self._literals:
            text = text.replace(literal, "[REDACTED]")
        return self._session_id.sub('session_id":[REDACTED]', text)


class RedactingFormatter(logging.Formatter):
    def __init__(self, fmt, redactor):
        super().__init__(fmt)
        self._redactor = redactor

    def format(self, record):
        return self._redactor(super().format(record))


def configure_logging(redactor):
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(RedactingFormatter("%(asctime)s %(levelname)s %(name)s: %(message)s", redactor))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(logging.INFO)
    for name in ("pocketoptionapi", "websockets", "asyncio", "urllib3"):
        logging.getLogger(name).setLevel(logging.WARNING)


def load_dotenv(path):
    """Minimal .env loader. Real environment variables always win over the file."""
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except OSError:
        return
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        if key and key not in os.environ:
            os.environ[key] = value


def validate_ssid(ssid):
    """Return an error message (never containing the SSID) or None.

    The library does not validate: a malformed SSID silently becomes demo mode
    with an empty session. The README documents the full 42["auth",{...}]
    payload, whose session / isDemo / uid fields are exactly what the
    library extracts, so those must be present.
    """
    if not ssid:
        return "POCKET_OPTION_SSID is not set"
    if not ssid.startswith('42["auth"'):
        return 'SSID must be the complete Socket.IO auth string starting with 42["auth",'
    missing = [k for k in ("session", "isDemo", "uid") if '"%s"' % k not in ssid]
    if missing:
        return "SSID is missing required field(s): " + ", ".join(missing)
    return None


# ---------------------------------------------------------------------------
# The only code that touches the library
# ---------------------------------------------------------------------------
class LibraryAdapter:
    def __init__(self, ssid):
        from pocketoptionapi import PocketOption
        import pocketoptionapi.global_value as global_value

        self._gv = global_value
        self._po = PocketOption(ssid)

    def connect(self):
        return self._po.connect()

    def check_connect(self):
        return bool(self._po.check_connect())

    def is_time_synced(self):
        return bool(self._po.is_time_synced())

    def get_assets(self):
        return self._po.get_assets()

    def subscribe(self, asset, period):
        return bool(self._po.subscribe(asset, period))

    def get_realtime_ticks(self, asset, limit):
        return self._po.get_realtime_ticks(asset, limit)

    def get_historical_candles(self, asset, period, offset, count_request):
        return self._po.get_historical_candles(asset, period, offset=offset, count_request=count_request)

    def get_server_timestamp(self):
        return self._po.get_server_timestamp()

    def socket_open(self):
        """True only if the library's underlying websocket object reports OPEN."""
        try:
            ws = self._po.api.websocket.websocket
            if ws is None:
                return False
            state = getattr(ws, "state", None)
            if state is not None:
                from websockets.protocol import State
                return state == State.OPEN
            return bool(getattr(ws, "open", False))
        except Exception:
            return False

    def error_state(self):
        return bool(self._gv.check_websocket_if_error), self._gv.websocket_error_reason

    def reconnect_attempts(self):
        try:
            return int(self._po.api.websocket.reconnect_attempts)
        except Exception:
            return 0

    def thread_alive(self):
        thread = getattr(self._po, "websocket_thread", None)
        return thread is None or thread.is_alive()

    def clear_send_lock(self):
        # The library's send helper busy-waits on this flag and leaves it set if
        # a send raises. This bridge is the only sender and runs every library
        # call on one thread, so clearing it before each call is safe.
        self._gv.ssl_Mutual_exclusion = False
        self._gv.ssl_Mutual_exclusion_write = False


# ---------------------------------------------------------------------------
# Candle validation (real data only: anything malformed is discarded, never repaired)
# ---------------------------------------------------------------------------
def _is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def normalize_candles(raw):
    candles, discarded = {}, 0
    for item in raw or []:
        if not isinstance(item, dict):
            discarded += 1
            continue
        parts = [item.get(k) for k in ("time", "open", "high", "low", "close")]
        if not all(_is_number(v) for v in parts):
            discarded += 1
            continue
        candle = dict(zip(("time", "open", "high", "low", "close"), parts))
        if _is_number(item.get("volume")):
            candle["volume"] = item["volume"]
        candles[candle["time"]] = candle
    return [candles[t] for t in sorted(candles)], discarded


def verify_period(candles):
    if len(candles) < 2:
        return False, "fewer than 2 candles returned, cannot verify the 1 minute spacing"
    recent = candles[-21:]
    deltas = Counter(round(b["time"] - a["time"]) for a, b in zip(recent, recent[1:]))
    spacing = deltas.most_common(1)[0][0]
    if spacing != CANDLE_PERIOD:
        return False, "candle spacing is %ss, expected %ss" % (spacing, CANDLE_PERIOD)
    return True, None


# ---------------------------------------------------------------------------
# Bridge state machine
# ---------------------------------------------------------------------------
def is_otc(symbol):
    return isinstance(symbol, str) and symbol.lower().endswith("_otc")


class Bridge:
    def __init__(self, adapter_factory, redactor, scanner_asset=None, clock=time.time, loop_seconds=LOOP_SECONDS):
        self._factory = adapter_factory
        self._redact = redactor
        self._scanner_env = (scanner_asset or "").strip() or None
        self._clock = clock
        self._loop_seconds = loop_seconds
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread = None

        self._s = {
            "bridge_phase": "starting",
            "authentication_status": "not_started",
            "authenticated": False,
            "websocket_connected": False,
            "time_synced": False,
            "real_asset_count": 0,
            "otc_asset_count": 0,
            "otc_available_count": 0,
            "real_tick_received": False,
            "real_candle_received": False,
            "last_tick_timestamp": None,
            "last_candle_timestamp": None,
            "last_received_asset": None,
            "reconnect_count": 0,
            "last_authentication_error": None,
            "last_connection_error": None,
            "last_error": None,
            "scanner_asset": None,
        }
        self._subscribed = set()
        self._eligible = {}
        self._tick_cache = {}
        self._tick_last_ts = {}
        self._last_tick_local = None
        self._candles = []
        self._last_catalog = 0.0
        self._retry_after = {}
        self._candle_retry = {}
        self._candle_retry_at = 0.0
        self._last_candle_minute = None

    # -- state helpers ------------------------------------------------------
    def _set(self, **fields):
        with self._lock:
            for key, value in fields.items():
                self._s[key] = self._redact(value) if isinstance(value, str) else value

    def fail_config(self, message):
        self._set(bridge_phase="failed", authentication_status="failed", last_authentication_error=message)

    def start(self):
        self._thread = threading.Thread(target=self._run, name="po-bridge-worker", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    # -- worker -------------------------------------------------------------
    def _run(self):
        try:
            self._loop()
        except Exception as exc:  # a bridge bug must be visible, never silent
            self._set(bridge_phase="failed", last_error="bridge internal error: %s: %s" % (type(exc).__name__, exc),
                      authenticated=False, websocket_connected=False)
            log.exception("bridge worker crashed")

    def _loop(self):
        adapter = self._factory()
        self._set(bridge_phase="connecting", authentication_status="pending")
        ok, err = adapter.connect()  # blocks up to ~30s; the library keeps retrying in its own thread afterwards
        if not ok and err:
            self._set(last_connection_error=str(err))

        previously_connected = False
        connected_since = None
        while not self._stop.is_set():
            now = self._clock()
            connected = adapter.check_connect()  # library: true only after the server's successauth
            socket_open = adapter.socket_open()
            error_flag, reason = adapter.error_state()
            self._set(websocket_connected=socket_open, authenticated=connected,
                      reconnect_count=adapter.reconnect_attempts())

            if error_flag:
                self._terminal_failure(reason)
                return
            if not connected and not adapter.thread_alive():
                self._terminal_failure(reason or "library websocket thread exited")
                return

            if connected and not previously_connected:
                # new authenticated session: the server has no subscriptions for it yet
                with self._lock:
                    self._subscribed.clear()
                    self._tick_cache.clear()
                    self._tick_last_ts.clear()
                    self._last_tick_local = None
                    self._last_catalog = 0.0
                    self._s.update(authentication_status="authenticated", bridge_phase="running",
                                   real_tick_received=False, last_connection_error=None,
                                   last_authentication_error=None)
                connected_since = now
            previously_connected = connected

            if connected:
                synced = adapter.is_time_synced()
                self._set(time_synced=synced)
                if synced or (now - connected_since) > TIME_SYNC_GRACE_SECONDS:
                    self._refresh_catalog(adapter, now)
                    self._subscribe_pending(adapter, now)
                self._poll_ticks(adapter, now)
                self._candle_step(adapter, now, synced)
            else:
                self._set(time_synced=adapter.is_time_synced())

            self._stop.wait(self._loop_seconds)

    def _terminal_failure(self, reason):
        reason = self._redact(str(reason or "unknown failure"))
        fields = dict(bridge_phase="failed", authenticated=False, websocket_connected=False)
        if reason.startswith("Unauthorized"):
            # exact library wording, e.g. SSID invalid or expired. Never retried automatically.
            fields.update(authentication_status="failed", last_authentication_error=reason)
        else:
            fields.update(authentication_status="not_authenticated", last_connection_error=reason)
        self._set(**fields)
        log.error("bridge stopped: %s", reason)

    # -- catalog and subscriptions -----------------------------------------
    def _refresh_catalog(self, adapter, now):
        interval = CATALOG_REFRESH_SECONDS if self._s["real_asset_count"] else 2
        if now - self._last_catalog < interval:
            return
        self._last_catalog = now
        assets = adapter.get_assets() or {}
        eligible = {}
        otc_total = 0
        for symbol, info in assets.items():
            if not is_otc(symbol):
                continue
            otc_total += 1
            if isinstance(info, dict) and info.get("is_available"):
                payout = info.get("payout")
                eligible[symbol] = payout if _is_number(payout) else 0
        with self._lock:
            self._eligible = eligible
            gone = self._subscribed - set(eligible)
            self._subscribed -= gone
            for symbol in gone:
                self._tick_cache.pop(symbol, None)
                self._tick_last_ts.pop(symbol, None)
            if self._s["scanner_asset"] in gone:
                self._s["scanner_asset"] = None
                self._candles = []
                self._s["real_candle_received"] = False
                self._s["last_candle_timestamp"] = None
            self._s.update(real_asset_count=len(assets), otc_asset_count=otc_total,
                           otc_available_count=len(eligible))

    def _subscribe_pending(self, adapter, now):
        with self._lock:
            pending = [s for s in sorted(self._eligible)
                       if s not in self._subscribed and self._retry_after.get(s, 0) <= now]
        for symbol in pending[:SUBSCRIBE_BATCH]:
            adapter.clear_send_lock()
            if adapter.subscribe(symbol, CANDLE_PERIOD):
                with self._lock:
                    self._subscribed.add(symbol)
            else:
                self._retry_after[symbol] = now + SUBSCRIBE_RETRY_SECONDS
                self._set(last_error="subscribe failed for %s" % symbol)

    # -- ticks --------------------------------------------------------------
    def _poll_ticks(self, adapter, now):
        with self._lock:
            symbols = sorted(self._subscribed)
        newest_ts, newest_asset = None, None
        for symbol in symbols:
            ticks = adapter.get_realtime_ticks(symbol, TICK_CACHE)
            if not ticks:
                continue
            last_ts = ticks[-1][0]
            with self._lock:
                self._tick_cache[symbol] = [[t, p] for t, p in ticks]
                if self._tick_last_ts.get(symbol) != last_ts:
                    self._tick_last_ts[symbol] = last_ts
                    if newest_ts is None or last_ts > newest_ts:
                        newest_ts, newest_asset = last_ts, symbol
        if newest_asset is not None:
            with self._lock:
                self._last_tick_local = now
                self._s.update(real_tick_received=True, last_tick_timestamp=newest_ts,
                               last_received_asset=newest_asset)

    # -- candles ------------------------------------------------------------
    def _candle_step(self, adapter, now, synced):
        if not synced:
            return
        with self._lock:
            asset = self._s["scanner_asset"]
            subscribed = set(self._subscribed)
            eligible = dict(self._eligible)
        if asset is None:
            if self._scanner_env:
                if self._scanner_env not in subscribed:
                    self._set(last_error="SCANNER_ASSET %s is not an available subscribed OTC asset" % self._scanner_env)
                    return
                asset = self._scanner_env
            else:
                ranked = sorted((s for s in subscribed if self._candle_retry.get(s, 0) <= now),
                                key=lambda s: (-eligible.get(s, 0), s))
                if not ranked:
                    return
                asset = ranked[0]
            if self._fetch_candles(adapter, asset, now):
                self._set(scanner_asset=asset)
            else:
                self._candle_retry[asset] = now + CANDLE_RETRY_SECONDS
            return
        if self._needs_refresh(adapter, now):
            self._fetch_candles(adapter, asset, now)

    def _needs_refresh(self, adapter, now):
        if now < self._candle_retry_at:
            return False
        server_now = adapter.get_server_timestamp()
        minute, second = int(server_now // 60), server_now % 60
        if minute != self._last_candle_minute and second >= 3:  # let the just-closed candle finalize
            return True
        return False

    def _fetch_candles(self, adapter, asset, now):
        server_now = adapter.get_server_timestamp()
        self._last_candle_minute = int(server_now // 60)
        adapter.clear_send_lock()
        raw = adapter.get_historical_candles(asset, CANDLE_PERIOD, HISTORY_OFFSET, 1)
        if not raw:
            self._candle_retry_at = now + 20
            self._set(last_error="no candles returned for %s (timeout, time not synced, or empty history)" % asset)
            return False
        candles, discarded = normalize_candles(raw)
        ok, why = verify_period(candles)
        if not ok:
            self._candle_retry_at = now + 20
            self._set(last_error="candles for %s rejected: %s" % (asset, why))
            return False
        with self._lock:
            self._candles = candles
            self._s.update(real_candle_received=True, last_candle_timestamp=candles[-1]["time"])
        if discarded:
            log.warning("discarded %d malformed candle(s) for %s", discarded, asset)
        return True

    # -- read side (HTTP threads never touch the library) -------------------
    def snapshot(self):
        now = self._clock()
        with self._lock:
            s = dict(self._s)
            subscribed = sorted(self._subscribed)
            tick_age = None if self._last_tick_local is None else round(now - self._last_tick_local, 1)
        tick_fresh = tick_age is not None and tick_age <= TICK_FRESH_SECONDS
        proofs = {
            "authenticated": s["authenticated"],
            "websocket_connected": s["websocket_connected"],
            "time_synced": s["time_synced"],
            "real_asset_count_gt_zero": s["real_asset_count"] > 0,
            "real_tick_received": bool(s["real_tick_received"] and tick_fresh),
            "real_candle_received": bool(s["real_candle_received"] and s["scanner_asset"]),
        }
        s.update(
            active_subscriptions=subscribed,
            active_subscription_count=len(subscribed),
            last_tick_age_seconds=tick_age,
            candle_period_seconds=CANDLE_PERIOD,
            time_basis=TIME_BASIS,
            proofs=proofs,
            connection_proven=all(proofs.values()),
        )
        return s

    def candles_for(self, asset, limit):
        with self._lock:
            scanner = self._s["scanner_asset"]
            if not scanner or (asset or scanner) != scanner or not self._candles:
                return None
            return scanner, list(self._candles[-limit:])

    def ticks_for(self, asset, limit):
        with self._lock:
            asset = asset or self._s["last_received_asset"]
            ticks = self._tick_cache.get(asset)
            if not ticks:
                return None
            return asset, list(ticks[-limit:])


# ---------------------------------------------------------------------------
# HTTP (127.0.0.1 only). Every body is redacted before it is written.
# ---------------------------------------------------------------------------
def make_handler(bridge, redactor):
    class Handler(BaseHTTPRequestHandler):
        def _send(self, status, payload):
            body = redactor(json.dumps(payload, allow_nan=False)).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            url = urlparse(self.path)
            query = parse_qs(url.query)
            asset = (query.get("asset") or [None])[0]
            try:
                limit = max(1, min(1000, int((query.get("limit") or ["200"])[0])))
            except ValueError:
                limit = 200
            if url.path == "/state":
                return self._send(200, bridge.snapshot())
            if url.path == "/candles":
                found = bridge.candles_for(asset, limit)
                if not found:
                    return self._send(404, {"error": "no real candles loaded for this asset"})
                name, candles = found
                return self._send(200, {"asset": name, "period_seconds": CANDLE_PERIOD, "time_basis": TIME_BASIS,
                                        "source": "PocketOption.get_historical_candles",
                                        "count": len(candles), "candles": candles})
            if url.path == "/ticks":
                found = bridge.ticks_for(asset, limit)
                if not found:
                    return self._send(404, {"error": "no real ticks received for this asset"})
                name, ticks = found
                return self._send(200, {"asset": name, "time_basis": TIME_BASIS,
                                        "source": "PocketOption.get_realtime_ticks",
                                        "count": len(ticks), "ticks": ticks})
            return self._send(404, {"error": "not found"})

        def log_message(self, fmt, *args):
            log.debug("http: " + fmt, *args)

    return Handler


def main():
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    ssid = os.environ.pop("POCKET_OPTION_SSID", "").strip()  # removed from the environment once read
    redactor = Redactor()
    redactor.register(ssid)
    configure_logging(redactor)

    bridge = Bridge(lambda: LibraryAdapter(ssid), redactor, scanner_asset=os.environ.get("SCANNER_ASSET"))
    problem = validate_ssid(ssid)
    if problem:
        bridge.fail_config(problem)
        log.error("not connecting: %s", problem)
    else:
        bridge.start()

    port = int(os.environ.get("PO_BRIDGE_PORT", "8765"))
    server = ThreadingHTTPServer(("127.0.0.1", port), make_handler(bridge, redactor))
    signal.signal(signal.SIGTERM, lambda *_: threading.Thread(target=server.shutdown, daemon=True).start())
    log.info("bridge listening on 127.0.0.1:%d", port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        bridge.stop()
        server.server_close()


if __name__ == "__main__":
    main()