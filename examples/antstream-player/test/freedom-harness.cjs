// Loads the player the way Freedom does: from a privileged `bzz:` scheme
// whose handler proxies to a local gateway. Mirrors Freedom's
// src/main/swarm/bzz-protocol.js where it matters to the player:
//
//  - the same scheme privileges (standard, secure, supportFetchAPI,
//    corsEnabled, stream);
//  - bzz://<ref>/<path>?<query> -> <gateway>/bzz/<ref>/<path>?<query>;
//  - only the same allowlisted request headers reach the gateway (so a
//    page-side Cache-Control is dropped, as in Freedom).
//
// Run with Electron (Freedom ships 44.x), against test/fake-gateway.mjs:
//
//   GATEWAY=http://127.0.0.1:1733 OUT=/tmp/player.png WAIT_S=40 \
//     xvfb-run -a electron test/freedom-harness.cjs
//
// Prints the player's state as JSON and writes a screenshot to OUT.

const { app, BrowserWindow, protocol } = require('electron');

const GATEWAY = process.env.GATEWAY || 'http://127.0.0.1:1733';
const PLAYER = 'a'.repeat(64);
const CHANNEL = 'c'.repeat(64);
const WAIT_S = Number(process.env.WAIT_S || 40);

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

const seen = [];

app.whenReady().then(async () => {
  protocol.handle('bzz', async (request) => {
    const u = new URL(request.url);
    const target = `${GATEWAY}/bzz/${u.hostname}${u.pathname}${u.search}`;
    const headers = new Headers();
    for (const [k, v] of request.headers.entries()) if (FORWARDED.has(k.toLowerCase())) headers.set(k, v);
    seen.push(`${request.method} ${request.url}`);
    const res = await fetch(target, { method: request.method, headers });
    return new Response(res.body, { status: res.status, headers: res.headers });
  });

  const win = new BrowserWindow({ width: 1280, height: 900, show: true, webPreferences: { sandbox: true } });
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
  await new Promise((r) => setTimeout(r, WAIT_S * 1000));

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
      markers: window.antstreamPlayer && window.antstreamPlayer.log
        ? window.antstreamPlayer.log.entries.filter((e) => e.kind === 'marker').map((e) => ((e.at - window.__stalls.t0) / 1000).toFixed(1) + ' ' + e.text)
        : null,
      stalls: Object.assign({}, window.__stalls, {
        totalMs: window.__stalls.totalMs + (window.__stalls.since !== null ? Date.now() - window.__stalls.since : 0),
      }),
    };
  })()`);
  if (process.env.DUMP_LOG) {
    const entries = await win.webContents.executeJavaScript(`JSON.stringify(window.antstreamPlayer.log.entries)`);
    require('fs').writeFileSync(process.env.DUMP_LOG, entries);
  }
  const segRequests = seen.filter((s) => s.includes('.m4s'));
  state.requests = {
    total: seen.length,
    playlists: seen.filter((s) => s.includes('stream.m3u8')).length,
    segments: segRequests.length,
    sample: seen.filter((s) => !s.includes(PLAYER)).slice(0, 4),
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
