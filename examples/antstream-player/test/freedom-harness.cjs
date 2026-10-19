// Loads the player the way Freedom does: from a privileged `bzz:` scheme,
// with Freedom's Swarm provider (`window.swarm`) in the page. Mirrors
// Freedom where it matters to the player:
//
//  - the same scheme privileges (standard, secure, supportFetchAPI,
//    corsEnabled, stream);
//  - `bzz://<player>/<path>` serves this checkout's player files, and
//    `bzz://<ref>` any other reference's raw bytes (`<gateway>/bytes/<ref>`),
//    as Freedom's node answers a non-manifest reference, and an image
//    request (the poster, a catalog thumbnail) `<gateway>/bzz/<ref>/`, the
//    one-file manifest's file, as Freedom answers a manifest;
//  - `window.swarm` (test/swarm-stub-preload.cjs) answers
//    `swarm_readFeedEntry` and `swarm_readSingleOwnerChunk` from the
//    gateway's `/feeds` and `/soc` (`swarm-only-root-chunk`), with
//    Freedom's result shapes and error reasons (src/main/swarm/
//    feed-service.js, docs/swarm-provider-api.md), and Freedom's
//    permission-free read budget: 120 requests / 512 KiB per fixed 60 s
//    window (SWARM_MAX_REQUESTS to lower it), refused with
//    `-32602 rate_limited`. NO_SWARM=1 leaves it out. PREFILL_BYTES=<n>
//    starts the first window with n bytes already spent, as another tab
//    on the origin (or the page before a reload) would leave it.
//
// Run with Electron (Freedom ships 44.x), against test/fake-gateway.mjs or
// a real gateway (antd):
//
//   GATEWAY=http://127.0.0.1:1733 OUT=$TMPDIR/player.png WAIT_S=40 \
//     xvfb-run -a electron test/freedom-harness.cjs
//
// SWITCH_WAITING=<owner>/<topic> fills the player's read budget, switches
// to that channel while feed reads wait for it, then frees the budget and
// reports (`switchWaiting.ok`) that none of the old channel's waiting
// reads was sent.
//
// SAMPLE_MS=<n> reads, every n ms while it waits, the picture's reported
// size and the stage's box (`state.samples`), for a stream whose frame
// changes shape mid-stream (test/turn-test.mjs, #228); SAMPLE_SHOTS=<dir>
// also captures the page 1.5 s after each change of reported size.
//
// CHANNEL=<owner>/<topic> picks the channel (the fake gateway's by
// default), URL= any other page (e.g. the fake gateway's own HTTP page).
// Prints the player's state as JSON and writes a screenshot to OUT.

const { app, BrowserWindow, protocol, ipcMain } = require('electron');
const fs = require('fs');
const K = require('../keccak.js');
const path = require('path');

const GATEWAY = process.env.GATEWAY || 'http://127.0.0.1:1733';
const PLAYER = 'a'.repeat(64);
const PLAYER_DIR = path.join(__dirname, '..');
const CHANNEL = process.env.CHANNEL || '0xd5572300e441b77b72bdd318bbfa7b97a1f03096/antstream';
const WAIT_S = Number(process.env.WAIT_S || 40);
const TYPES = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css' };

const FORWARDED = new Set([
  'accept', 'accept-encoding', 'accept-language', 'if-match', 'if-modified-since',
  'if-none-match', 'if-range', 'if-unmodified-since', 'range', 'user-agent',
]);

protocol.registerSchemesAsPrivileged([
  {
    scheme: 'bzz',
    privileges: {
      standard: true, secure: true, supportFetchAPI: true, corsEnabled: true, stream: true,
      allowServiceWorkers: true,
    },
  },
]);

// -- window.swarm, main side ---------------------------------------------------

const WINDOW_MS = 60000;
const MAX_REQUESTS = Number(process.env.SWARM_MAX_REQUESTS || 120);
const MAX_BYTES = 512 * 1024;
const PREFILL_BYTES = Number(process.env.PREFILL_BYTES || 0);
const provider = {
  calls: {}, refused: 0, windows: [], current: null,
  // Every read's owner and time, for SWITCH_WAITING.
  log: [],
};

const fail = (code, message, data) => ({ error: { code, message, data } });
const hexIndex = (h) => (h && /^[0-9a-f]+$/i.test(h) ? parseInt(h, 16) : null);

async function swarmRead(method, params) {
  provider.calls[method] = (provider.calls[method] || 0) + 1;
  provider.log.push({ owner: String(params.owner || '').replace(/^0x/i, '').toLowerCase(), at: Date.now() });
  // A fixed window from the first read, as Freedom keeps it.
  const now = Date.now();
  if (!provider.current || now - provider.current.start >= WINDOW_MS) {
    provider.current = { start: now, requests: 0, bytes: provider.windows.length ? 0 : PREFILL_BYTES };
    provider.windows.push(provider.current);
  }
  const w = provider.current;
  const limited = () => {
    provider.refused += 1;
    return fail(-32602, 'Rate limited', { reason: 'rate_limited', windowMs: WINDOW_MS, maxRequests: MAX_REQUESTS, maxBytes: MAX_BYTES });
  };
  if (w.requests >= MAX_REQUESTS || w.bytes >= MAX_BYTES) return limited();
  w.requests += 1;
  const owner = String(params.owner || '').replace(/^0x/i, '');
  let res;
  if (method === 'swarm_readFeedEntry') {
    const exact = params.index !== undefined && params.index !== null;
    // By index: the slot's whole content, joined by the node (bee-js
    // `downloadExactFeedPayload` in Freedom).
    res = exact
      ? await fetch(`${GATEWAY}/soc/${owner}/${K.feedIdentifier(params.topic, params.index)}`)
      : await fetch(`${GATEWAY}/feeds/${owner}/${params.topic}`);
    if (res.status === 404) {
      return exact
        ? fail(-32602, `Feed entry not found at index ${params.index}`, { reason: 'entry_not_found' })
        : fail(-32602, 'Feed is empty', { reason: 'feed_empty' });
    }
    if (!res.ok) return fail(-32603, `HTTP ${res.status}`);
    const body = Buffer.from(await res.arrayBuffer());
    w.bytes += body.length;
    if (w.bytes > MAX_BYTES) return limited();
    const index = exact ? params.index : hexIndex(res.headers.get('swarm-feed-index'));
    const next = exact ? params.index + 1 : hexIndex(res.headers.get('swarm-feed-index-next'));
    return { result: { data: body.toString('base64'), encoding: 'base64', index, nextIndex: next } };
  }
  if (method === 'swarm_readSingleOwnerChunk') {
    res = await fetch(`${GATEWAY}/soc/${owner}/${params.identifier}`, { headers: { 'swarm-only-root-chunk': 'true' } });
    if (res.status === 404) return fail(-32602, 'Chunk not found', { reason: 'chunk_not_found' });
    if (!res.ok) return fail(-32603, `HTTP ${res.status}`);
    const wire = Buffer.from(await res.arrayBuffer());
    w.bytes += wire.length;
    if (w.bytes > MAX_BYTES) return limited();
    // A number, or a BigInt past the safe integers (a span carrying an
    // erasure-coding level in its top byte), as Freedom returns it.
    const big = wire.readBigUInt64LE(0);
    const span = big > BigInt(Number.MAX_SAFE_INTEGER) ? big : Number(big);
    return {
      result: {
        data: wire.subarray(8).toString('base64'), encoding: 'base64', span,
        owner: `0x${owner}`, identifier: params.identifier,
      },
    };
  }
  return fail(4200, `Unknown method ${method}`);
}

const seen = [];

// MEASURE=1: where the picture actually lands on screen, for the aspect
// test (test/portrait-test.mjs). The fixtures draw one bright disc on a
// dark frame; its bounding box in a capture of the video element says
// whether the picture is upright-and-round or squeezed, whatever the
// element's own box claims.
async function measure(win) {
  // Hide the native controls and the badge/overlay text so they can't be
  // mistaken for the disc, and bring the whole element on screen.
  await win.webContents.executeJavaScript(`(() => {
    const v = document.getElementById('video');
    v.controls = false;
    for (const id of ['badge', 'overlay']) document.getElementById(id).style.visibility = 'hidden';
    v.scrollIntoView({ block: 'start' });
  })()`);
  await new Promise((r) => setTimeout(r, 400));
  const box = await win.webContents.executeJavaScript(`(() => {
    const v = document.getElementById('video');
    const r = v.getBoundingClientRect();
    const w = document.querySelector('.video-wrap').getBoundingClientRect();
    return {
      video: { x: r.x, y: r.y, width: r.width, height: r.height },
      wrap: { width: w.width, height: w.height },
      videoWidth: v.videoWidth, videoHeight: v.videoHeight,
      viewport: { width: innerWidth, height: innerHeight },
      dpr: devicePixelRatio,
    };
  })()`);
  const rect = {
    x: Math.round(box.video.x), y: Math.round(box.video.y),
    width: Math.round(box.video.width), height: Math.round(box.video.height),
  };
  const img = await win.webContents.capturePage(rect);
  const { width, height } = img.getSize();
  const px = img.toBitmap(); // BGRA
  let minX = width, minY = height, maxX = -1, maxY = -1;
  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width; x++) {
      const i = (y * width + x) * 4;
      if (px[i] > 215 && px[i + 1] > 215 && px[i + 2] > 215) {
        if (x < minX) minX = x;
        if (x > maxX) maxX = x;
        if (y < minY) minY = y;
        if (y > maxY) maxY = y;
      }
    }
  }
  box.capture = { width, height };
  box.disc = maxX < 0 ? null : {
    width: maxX - minX + 1, height: maxY - minY + 1,
    cx: (minX + maxX) / 2 / width, cy: (minY + maxY) / 2 / height,
  };
  return box;
}

app.whenReady().then(async () => {
  protocol.handle('bzz', async (request) => {
    const u = new URL(request.url);
    seen.push(`${request.method} ${request.url}`);
    if (u.hostname === PLAYER) {
      const file = path.join(PLAYER_DIR, decodeURIComponent(u.pathname) === '/' ? 'index.html' : decodeURIComponent(u.pathname));
      if (!file.startsWith(PLAYER_DIR) || !fs.existsSync(file)) return new Response('not found', { status: 404 });
      return new Response(fs.readFileSync(file), { headers: { 'content-type': TYPES[path.extname(file)] || 'application/octet-stream' } });
    }
    const headers = new Headers();
    for (const [k, v] of request.headers.entries()) if (FORWARDED.has(k.toLowerCase())) headers.set(k, v);
    // `bzz://<ref>` and `bzz://<ref>/` are one URL here (a standard
    // scheme): an image is the thumbnail's manifest, anything else raw bytes.
    const image = /^image\//.test(request.headers.get('accept') || '');
    const res = await fetch(`${GATEWAY}/${image ? 'bzz' : 'bytes'}/${u.hostname}${image ? '/' : ''}`, { method: request.method, headers });
    return new Response(res.body, { status: res.status, headers: res.headers });
  });
  ipcMain.handle('swarm', (_e, method, params) => swarmRead(method, params || {}).catch((err) => fail(-32603, err.message)));

  const win = new BrowserWindow({
    width: Number(process.env.WIDTH || 1280),
    height: Number(process.env.HEIGHT || 900),
    useContentSize: true,
    show: true,
    webPreferences: {
      sandbox: true,
      preload: process.env.NO_SWARM ? undefined : path.join(__dirname, 'swarm-stub-preload.cjs'),
    },
  });
  win.webContents.on('console-message', (e) => {
    const { level, message } = e;
    if (level === 'error' || level === 'warning' || /error/i.test(message)) console.error(`[page] ${message}`);
  });
  await win.loadURL(process.env.URL || `bzz://${PLAYER}/#${CHANNEL}`);
  // Count stalls from the outside, the same way for any player version: a
  // `waiting` after playback first started, not caused by a seek.
  await win.webContents.executeJavaScript(`(() => {
    const v = document.getElementById('video');
    const s = window.__stalls = { firstPlayingMs: null, count: 0, totalMs: 0, since: null, t0: Date.now() };
    v.addEventListener('playing', () => {
      if (s.firstPlayingMs === null) s.firstPlayingMs = Date.now() - s.t0;
      if (s.since !== null) { s.totalMs += Date.now() - s.since; s.since = null; }
    });
    v.addEventListener('waiting', () => {
      if (s.firstPlayingMs === null || v.seeking || s.since !== null) return;
      s.count += 1;
      s.since = Date.now();
    });
  })()`);
  const samples = [];
  if (process.env.SAMPLE_MS) {
    const every = Number(process.env.SAMPLE_MS);
    const end = Date.now() + WAIT_S * 1000;
    let lastSize = null;
    let shot = null;
    let shots = 0;
    while (Date.now() < end) {
      const sample = await win.webContents.executeJavaScript(`(() => {
        const v = document.getElementById('video');
        const w = document.querySelector('.video-wrap').getBoundingClientRect();
        const r = v.getBoundingClientRect();
        return {
          t: Date.now(), currentTime: v.currentTime,
          videoWidth: v.videoWidth, videoHeight: v.videoHeight,
          wrap: { width: w.width, height: w.height },
          video: { width: r.width, height: r.height },
          viewport: { width: innerWidth, height: innerHeight },
        };
      })()`);
      samples.push(sample);
      const size = sample.videoWidth ? `${sample.videoWidth}x${sample.videoHeight}` : null;
      if (size && size !== lastSize) {
        lastSize = size;
        shots += 1;
        if (process.env.SAMPLE_SHOTS) shot = { at: Date.now() + 1500, name: `shape-${shots}-${size}.png` };
      }
      if (shot && Date.now() >= shot.at) {
        const img = await win.webContents.capturePage();
        fs.writeFileSync(path.join(process.env.SAMPLE_SHOTS, shot.name), img.toPNG());
        shot = null;
      }
      await new Promise((r) => setTimeout(r, every));
    }
  } else {
    await new Promise((r) => setTimeout(r, WAIT_S * 1000));
  }

  const state = await win.webContents.executeJavaScript(`(() => {
    const v = document.getElementById('video');
    const hls = window.antstreamPlayer && window.antstreamPlayer.hls;
    return {
      badge: document.getElementById('badge').textContent,
      status: document.getElementById('status').textContent,
      overlay: document.getElementById('overlay').hidden ? '' : document.getElementById('overlay').textContent,
      currentTime: v.currentTime,
      paused: v.paused,
      readyState: v.readyState,
      size: v.videoWidth + 'x' + v.videoHeight,
      latency: hls ? hls.latency : null,
      decodedFrames: v.getVideoPlaybackQuality ? v.getVideoPlaybackQuality().totalVideoFrames : null,
      droppedFrames: v.getVideoPlaybackQuality ? v.getVideoPlaybackQuality().droppedVideoFrames : null,
      stats: [...document.querySelectorAll('.stats div')].map(d => d.innerText.replace('\\n', ': ')),
      info: {
        title: document.getElementById('info-title').hidden ? '' : document.getElementById('info-title').textContent,
        when: document.getElementById('info-when').hidden ? '' : document.getElementById('info-when').textContent,
        description: document.getElementById('info-desc').hidden ? '' : document.getElementById('info-desc').textContent,
        poster: v.getAttribute('poster') || '',
        documentTitle: document.title,
      },
      markers: window.antstreamPlayer && window.antstreamPlayer.log
        ? window.antstreamPlayer.log.entries.filter((e) => e.kind === 'marker').map((e) => ((e.at - window.__stalls.t0) / 1000).toFixed(1) + ' ' + e.text)
        : null,
      stalls: Object.assign({}, window.__stalls, {
        totalMs: window.__stalls.totalMs + (window.__stalls.since !== null ? Date.now() - window.__stalls.since : 0),
      }),
    };
  })()`);
  if (samples.length) state.samples = samples;
  if (process.env.PROBE_SLOT) {
    // One slot read by hand through the player's own reader (in Freedom:
    // `swarm_readSingleOwnerChunk`, joining a body longer than one chunk).
    state.probe = await win.webContents.executeJavaScript(`(async () => {
      const p = window.antstreamPlayer;
      const a = await p.read.slot(p.channel, ${Number(process.env.PROBE_SLOT)});
      return { status: a.status, index: a.index, note: a.note, bytes: a.text ? new TextEncoder().encode(a.text).length : null,
        firstLines: a.text ? a.text.split('\\n').slice(0, 3) : null, endlist: a.text ? a.text.trimEnd().endsWith('#EXT-X-ENDLIST') : null };
    })()`);
  }
  if (process.env.MEASURE) state.geometry = await measure(win);
  if (process.env.SWITCH_TO) {
    // Open another channel (one that never loads) and report the stage's
    // shape a moment later: it must not keep the last channel's aspect.
    await win.webContents.executeJavaScript(`location.hash = ${JSON.stringify(process.env.SWITCH_TO)}`);
    await new Promise((r) => setTimeout(r, 3000));
    state.afterSwitch = await win.webContents.executeJavaScript(`(() => {
      const w = document.querySelector('.video-wrap');
      const r = w.getBoundingClientRect();
      return { width: r.width, height: r.height, aspect: w.style.getPropertyValue('--aspect'), size: document.getElementById('video').videoWidth + 'x' + document.getElementById('video').videoHeight };
    })()`);
  }
  if (process.env.SWITCH_WAITING) {
    // Switch channel while feed reads wait for the player's read budget
    // (the budget filled by hand): those reads are the old channel's and
    // must be dropped, never sent to window.swarm, even once room returns.
    const from = await win.webContents.executeJavaScript(`window.antstreamPlayer.channel.owner`);
    const atFill = await win.webContents.executeJavaScript(`(() => {
      const b = window.antstreamPlayer.budget;
      b.reads.push({ at: Date.now(), bytes: 1 << 30, waitedMs: 0, fill: true });
      return Date.now();
    })()`);
    // Long enough for the next polls to start and queue behind the fill.
    await new Promise((r) => setTimeout(r, 4000));
    const waiting = await win.webContents.executeJavaScript(`window.antstreamPlayer.log.entries.filter((e) => e.kind === 'playlist' && e.status === 'loading').length`);
    const switchedAt = Date.now();
    await win.webContents.executeJavaScript(`location.hash = ${JSON.stringify(process.env.SWITCH_WAITING)}`);
    await new Promise((r) => setTimeout(r, 1000));
    await win.webContents.executeJavaScript(`(() => {
      const b = window.antstreamPlayer.budget;
      b.reads = b.reads.filter((r) => !r.fill);
    })()`);
    await new Promise((r) => setTimeout(r, 3000));
    const entries = await win.webContents.executeJavaScript(`JSON.stringify(window.antstreamPlayer.log.entries.filter((e) => e.kind === 'playlist').map((e) => ({ status: e.status, note: e.note })))`);
    const sentAfterSwitch = provider.log.filter((c) => c.owner === from && c.at >= switchedAt).length;
    state.switchWaiting = {
      waitingAtSwitch: waiting,
      sentDuringFill: provider.log.filter((c) => c.at > atFill + 500 && c.at < switchedAt).length,
      oldChannelReadsSentAfterSwitch: sentAfterSwitch,
      newChannelReads: provider.log.filter((c) => c.owner !== from && c.at >= switchedAt).length,
      cancelled: JSON.parse(entries).filter((e) => e.status === 'cancelled').length,
      ok: waiting > 0 && sentAfterSwitch === 0,
    };
  }
  if (process.env.DUMP_LOG) {
    const entries = await win.webContents.executeJavaScript(`JSON.stringify(window.antstreamPlayer.log.entries)`);
    require('fs').writeFileSync(process.env.DUMP_LOG, entries);
  }
  state.feed = await win.webContents.executeJavaScript(`(() => {
    const f = window.antstreamPlayer && window.antstreamPlayer.feed;
    const p = window.antstreamPlayer;
    return f ? { index: f.index, cursor: f.cursor, joinedVia: f.joinedVia, headHeld: p.headHeld, headTooLarge: p.headTooLarge } : null;
  })()`);
  state.requests = {
    bzz: seen.filter((s) => !s.includes(PLAYER)).length,
    provider: {
      calls: provider.calls,
      refused: provider.refused,
      perWindow: provider.windows.map((w) => ({ requests: w.requests, kib: Math.round(w.bytes / 1024) })),
    },
    sample: seen.filter((s) => !s.includes(PLAYER)).slice(0, 3),
  };
  console.log(JSON.stringify(state, null, 2));
  if (process.env.OUT) {
    // With the debug panel open, show it rather than the video.
    await win.webContents.executeJavaScript(`(() => { const d = document.getElementById('debug'); if (d && !d.hidden) d.scrollIntoView(); })()`);
    await new Promise((r) => setTimeout(r, 600));
    const img = await win.webContents.capturePage();
    require('fs').writeFileSync(process.env.OUT, img.toPNG());
  }
  app.exit(0);
});
