// NWWT Signal Desk — Node scanner
//
// This process never holds the Pocket Option SSID. The Pocket Option
// connection lives entirely in bridge/po_bridge.py (the chema-creator
// PocketOptionApi client, which reads POCKET_OPTION_SSID itself). This file
// only polls that bridge on 127.0.0.1 and feeds the real 1 minute candles it
// reports into the existing candle pipeline (addCandle).
//
// The scanner stays stopped until the bridge proves all six conditions:
// authenticated, websocket connected, time synchronized, real asset count
// above zero, real tick received, real 1 minute candle received.

'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');

const BRIDGE_URL = process.env.PO_BRIDGE_URL || `http://127.0.0.1:${process.env.PO_BRIDGE_PORT || 8765}`;
const POLL_MS = 2000;
const CANDLES_PER_POLL = 200;

// ---------------------------------------------------------------------------
// Bridge client
// ---------------------------------------------------------------------------
let bridge = { reachable: false, error: 'bridge not polled yet' };
let lastFedTime = 0;      // newest candle time (seconds, server-native) already fed to addCandle
let feedAsset = null;

function getJson(url) {
  return new Promise((resolve, reject) => {
    const req = http.get(url, { timeout: 5000 }, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(data) }); }
        catch (e) { reject(new Error('bridge returned invalid JSON')); }
      });
    });
    req.on('timeout', () => req.destroy(new Error('request timed out')));
    req.on('error', reject);
  });
}

// "Connected" means all six conditions are proven by the bridge right now.
// Recomputed here from the raw fields instead of trusting a single flag.
function isProven(b) {
  return !!b && b.reachable === true &&
    b.authenticated === true &&
    b.websocket_connected === true &&
    b.time_synced === true &&
    b.real_asset_count > 0 &&
    b.real_tick_received === true &&
    b.real_candle_received === true &&
    typeof b.scanner_asset === 'string' && b.scanner_asset.length > 0 &&
    b.connection_proven === true;
}

async function feedCandles(asset) {
  if (feedAsset !== asset) {
    // the pipeline holds one flat stream, so a different asset must not mix with the old one
    candles = [];
    lastSignalTime = 0;
    lastResult = { direction: 'WAITING', reason: 'Screen the market to begin', score: 0, time: null, patterns: [] };
    lastFedTime = 0;
    feedAsset = asset;
  }
  const res = await getJson(`${BRIDGE_URL}/candles?asset=${encodeURIComponent(asset)}&limit=${CANDLES_PER_POLL}`);
  if (res.status !== 200 || !Array.isArray(res.body.candles)) return;

  const fresh = res.body.candles.filter((c) => c.time > lastFedTime);
  if (!fresh.length) return;

  // Backfill without triggering analysis for every historical candle, then analyze once if running.
  const wasRunning = running;
  running = false;
  for (const c of fresh) {
    addCandle({ asset, timestamp: c.time * 1000, open: c.open, close: c.close, high: c.high, low: c.low });
    lastFedTime = Math.max(lastFedTime, c.time);
  }
  running = wasRunning;
  if (running) analyzeMarket();
}

async function pollBridge() {
  try {
    const res = await getJson(`${BRIDGE_URL}/state`);
    if (res.status !== 200) throw new Error(`bridge returned HTTP ${res.status}`);
    bridge = Object.assign({ reachable: true }, res.body);
  } catch (e) {
    bridge = { reachable: false, error: `bridge unreachable: ${e.message}` };
  }

  if (!isProven(bridge)) {
    running = false;      // the scanner is stopped whenever the proof does not hold
    return;
  }
  try { await feedCandles(bridge.scanner_asset); }
  catch (e) { /* a failed candle poll is retried on the next cycle */ }
}


// ---------------------------------------------------------------------------
// Candle buffer + the exact same analysis engine used in the extension
// popup and the website edition — single flat stream, matching Pocket
// Option's one-active-chart-asset behavior.
// ---------------------------------------------------------------------------
let candles = [];
let running = false;
let lastSignalTime = 0;
let scoreThreshold = 3;
let lastResult = { direction: 'WAITING', reason: 'Screen the market to begin', score: 0, time: null, patterns: [] };

function addCandle(candle) {
  if (!candle || !Number.isFinite(candle.timestamp)) return;
  const normalized = {
    asset: candle.asset || 'UNKNOWN',
    timestamp: Number(candle.timestamp),
    open: Number(candle.open), close: Number(candle.close),
    high: Number(candle.high), low: Number(candle.low)
  };
  if (!Number.isFinite(normalized.open) || !Number.isFinite(normalized.close) || !Number.isFinite(normalized.high) || !Number.isFinite(normalized.low)) return;

  const existing = candles.findIndex(c => c.timestamp === normalized.timestamp && c.asset === normalized.asset);
  if (existing >= 0) candles[existing] = normalized; else candles.push(normalized);
  candles.sort((a, b) => a.timestamp - b.timestamp);
  if (candles.length > 200) candles = candles.slice(-200);

  if (running) analyzeMarket();
}

const candleDirection = c => c.close > c.open ? 'bullish' : c.close < c.open ? 'bearish' : 'neutral';
const body = c => Math.abs(c.close - c.open);
const range = c => c.high - c.low;
const upperWick = c => c.high - Math.max(c.open, c.close);
const lowerWick = c => Math.min(c.open, c.close) - c.low;

function isBullishEngulfing(prev, curr) {
  return candleDirection(prev) === 'bearish' && candleDirection(curr) === 'bullish' &&
    curr.open <= prev.close && curr.close >= prev.open && body(curr) > body(prev);
}
function isBearishEngulfing(prev, curr) {
  return candleDirection(prev) === 'bullish' && candleDirection(curr) === 'bearish' &&
    curr.open >= prev.close && curr.close <= prev.open && body(curr) > body(prev);
}
function isHammer(c) {
  const r = range(c);
  if (r <= 0) return false;
  return lowerWick(c) >= body(c) * 2 && upperWick(c) <= body(c) && body(c) / r <= 0.45;
}
function isPinBarAtResistance(c) {
  const r = range(c);
  if (r <= 0) return false;
  return upperWick(c) >= body(c) * 2 && upperWick(c) > lowerWick(c) && body(c) / r <= 0.45;
}
function isMorningStar(a, b, c) {
  return candleDirection(a) === 'bearish' && body(b) < body(a) * 0.5 && candleDirection(c) === 'bullish' && c.close > (a.open + a.close) / 2;
}
function isEveningStar(a, b, c) {
  return candleDirection(a) === 'bullish' && body(b) < body(a) * 0.5 && candleDirection(c) === 'bearish' && c.close < (a.open + a.close) / 2;
}
function recentLevels() {
  const recent = candles.slice(-12);
  if (recent.length < 4) return { support: null, resistance: null };
  return { support: Math.min(...recent.map(c => c.low)), resistance: Math.max(...recent.map(c => c.high)) };
}
function nearLevel(price, level) {
  if (level === null) return false;
  const recent = candles.slice(-12);
  if (!recent.length) return false;
  const ranges = recent.map(c => range(c)).filter(r => r > 0);
  if (!ranges.length) return false;
  const averageRange = ranges.reduce((a, b) => a + b, 0) / ranges.length;
  return Math.abs(price - level) <= averageRange * 0.5;
}

function analyzeMarket() {
  if (candles.length < 3) {
    lastResult = { direction: 'WAITING', reason: `Waiting for candles ${candles.length}/3`, score: 0, time: lastResult.time, patterns: [] };
    return;
  }
  const a = candles[candles.length - 3], b = candles[candles.length - 2], c = candles[candles.length - 1];
  const levels = recentLevels();
  let buyScore = 0, sellScore = 0;
  const patterns = [];

  if (nearLevel(c.low, levels.support)) buyScore += 1;
  if (nearLevel(c.high, levels.resistance)) sellScore += 1;
  if (isBullishEngulfing(b, c)) { buyScore += 2; patterns.push('Bullish Engulfing'); }
  if (isBearishEngulfing(b, c)) { sellScore += 2; patterns.push('Bearish Engulfing'); }
  if (isHammer(c)) { buyScore += 2; patterns.push('Hammer'); }
  if (isPinBarAtResistance(c)) { sellScore += 2; patterns.push('Pin Bar'); }
  if (isMorningStar(a, b, c)) { buyScore += 2; patterns.push('Morning Star'); }
  if (isEveningStar(a, b, c)) { sellScore += 2; patterns.push('Evening Star'); }

  let direction = 'WAITING', score = 0;
  if (buyScore >= scoreThreshold && buyScore > sellScore) { direction = 'BUY'; score = buyScore; }
  if (sellScore >= scoreThreshold && sellScore > buyScore) { direction = 'SELL'; score = sellScore; }

  if (direction === 'WAITING') {
    lastResult = { direction: 'WAITING', reason: patterns.length ? patterns.join(' + ') : 'No setup found', score: Math.max(buyScore, sellScore), time: lastResult.time, patterns };
    return;
  }
  if (c.timestamp === lastSignalTime) return;
  lastSignalTime = c.timestamp;
  lastResult = { direction, reason: patterns.length ? patterns.join(' + ') : 'Pattern and level confirmation', score, time: c.timestamp, patterns };
}

// ---------------------------------------------------------------------------
// HTTP: static frontend + sanitized state + simple controls. This process
// has no SSID, so none can appear in any response; the bridge diagnostics
// below are passed through exactly as the bridge reports them.
// ---------------------------------------------------------------------------
function publicState() {
  const proven = isProven(bridge);
  return {
    connected: proven,
    running,
    market: proven ? bridge.scanner_asset : null,
    period: proven ? bridge.candle_period_seconds : null,
    scoreThreshold,
    signal: lastResult,
    bridge
  };
}

const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css' };

const server = http.createServer((req, res) => {
  if (req.url === '/api/state') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    res.end(JSON.stringify(publicState()));
    return;
  }

  if (req.url === '/api/control' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      let gated = false;
      try {
        const msg = JSON.parse(body || '{}');
        if (msg.action === 'start' || msg.action === 'screen') {
          if (isProven(bridge)) { running = true; analyzeMarket(); }
          else gated = true;    // locked until the bridge proves all six conditions
        }
        if (msg.action === 'stop') { running = false; }
        if (typeof msg.scoreThreshold === 'number') scoreThreshold = msg.scoreThreshold;
      } catch (e) {}
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(Object.assign(publicState(), gated
        ? { gated: true, message: 'Scanner is locked until the bridge proves authentication, websocket, time sync, assets, a real tick and a real 1 minute candle.' }
        : {})));
    });
    return;
  }

  const filePath = path.join(__dirname, 'public', req.url === '/' ? '/index.html' : req.url);
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not found'); return; }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(filePath)] || 'application/octet-stream' });
    res.end(data);
  });
});

const PORT = process.env.PORT || 3000;
if (require.main === module) {
  server.listen(PORT, () => console.log(`[nwwt] listening on :${PORT}, bridge at ${BRIDGE_URL}`));
  pollBridge();
  setInterval(pollBridge, POLL_MS);
}

module.exports = {
  server, pollBridge, isProven, publicState, getBridge: () => bridge,
  addCandle, analyzeMarket, isHammer, isBullishEngulfing, isBearishEngulfing, isPinBarAtResistance,
  isMorningStar, isEveningStar, recentLevels, nearLevel,
  getLastResult: () => lastResult, getCandles: () => candles, isRunning: () => running,
  setRunning: (v) => { running = v; }, setScoreThreshold: (v) => { scoreThreshold = v; }
};