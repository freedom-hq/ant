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
    const target = `${GATEWAY}/bzz/${u.hostname}${u.pathname}${u.search}`;
    const headers = new Headers();
    for (const [k, v] of request.headers.entries()) if (FORWARDED.has(k.toLowerCase())) headers.set(k, v);
    seen.push(`${request.method} ${request.url}`);
    const res = await fetch(target, { method: request.method, headers });
    return new Response(res.body, { status: res.status, headers: res.headers });
  });

  const win = new BrowserWindow({
    width: Number(process.env.WIDTH || 1280), height: Number(process.env.HEIGHT || 900), useContentSize: true, show: true, webPreferences: { sandbox: true } });
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
