#!/usr/bin/env node
// A stand-in for a bee/ant gateway carrying one live AntStream channel, for
// exercising the player without a funded publisher.
//
//   node test/fake-gateway.mjs <segments-dir> [port]
//
// <segments-dir> holds what the Android app's self-test mode writes
// (`init-1.mp4`, `seg-N.m4s`, `durations.txt`; see
// examples/android-stream/README.md). The server replays those segments in
// real time as a rolling live playlist shaped exactly like the publisher's
// (crates/ant-ffi/src/publisher.rs `render_playlist`): gateway-root
// absolute URIs, a 6-entry window, `#EXT-X-MAP` per writer, and
// `#EXT-X-DISCONTINUITY` where the recording loops. ENDLIST=<n> ends the
// broadcast after n segments. SLOW_MS=<n> delays every segment response by a
// random 0..n ms, like a cold Swarm read. RESTART_MS=<n> starts a new
// broadcast on the same channel n ms after one ended (its segment
// numbering starts over, as the publisher's does). STALL=<n> stops after
// n segments *without* ENDLIST, like a broadcaster that dropped off.
// STALE_PCT=<p> answers p% of playlist reads with an older playlist of the
// same broadcast, like a feed lookup that missed the newest updates.
//
// Routes:
//   /bzz/<PLAYER_REF>/<file>       the player (examples/antstream-player)
//   /bzz/<CHANNEL_REF>/stream.m3u8 the live playlist (no-cache)
//   /bzz/<ref>/<name>              segments, by per-segment reference

import { createServer } from 'node:http';
import { createHash } from 'node:crypto';
import { readFileSync, existsSync } from 'node:fs';
import { join, dirname, extname } from 'node:path';
import { fileURLToPath } from 'node:url';

export const PLAYER_REF = 'a'.repeat(64);
export const CHANNEL_REF = 'c'.repeat(64);

const here = dirname(fileURLToPath(import.meta.url));
const playerDir = join(here, '..');
const dir = process.argv[2];
const port = Number(process.argv[3] || 1733);
const endAfter = Number(process.env.ENDLIST || 0);
const slowMs = Number(process.env.SLOW_MS || 0);
const restartMs = Number(process.env.RESTART_MS || 0);
const stallAfter = Number(process.env.STALL || 0);
const stalePct = Number(process.env.STALE_PCT || 0);
if (!dir) {
  console.error('usage: fake-gateway.mjs <segments-dir> [port]');
  process.exit(2);
}

const records = readFileSync(join(dir, 'durations.txt'), 'utf8')
  .trim()
  .split('\n')
  .map((l) => l.split(' '))
  .filter(([, init]) => init === 'false')
  .map(([n, , ms]) => ({ file: `seg-${n}.m4s`, ms: Number(ms) }));
const init = readFileSync(join(dir, 'init-1.mp4'));

const refOf = (name) => createHash('sha256').update(name).digest('hex');
const blobs = new Map(); // ref -> { name, bytes, type }
const initRef = refOf('init');
blobs.set(initRef, { name: 'init-1.mp4', bytes: init, type: 'video/mp4' });

let published = []; // { seq, ref, name, ms, discontinuity, map }
let seq = 0;
let ended = false;
let broadcast = 0;

function publishNext() {
  if (ended) return;
  const rec = records[seq % records.length];
  const loop = seq > 0 && seq % records.length === 0;
  const name = `seg-${seq + 2}.m4s`;
  const ref = refOf(`${broadcast}-${seq}-${name}`);
  blobs.set(ref, { name, bytes: readFileSync(join(dir, rec.file)), type: 'video/iso.segment' });
  published.push({ seq, ref, name, ms: rec.ms, discontinuity: loop });
  seq += 1;
  if (stallAfter && seq >= stallAfter) return;
  if (endAfter && seq >= endAfter) {
    ended = true;
    if (restartMs) {
      setTimeout(() => {
        broadcast += 1;
        published = [];
        seq = 0;
        ended = false;
        console.log(`broadcast ${broadcast} starts`);
        publishNext();
      }, restartMs);
    }
    return;
  }
  setTimeout(publishNext, rec.ms);
}

function playlist() {
  // A stale read: the playlist as it was a few updates ago.
  const lag = Math.random() * 100 < stalePct ? 1 + Math.floor(Math.random() * 5) : 0;
  const upto = Math.max(1, published.length - lag);
  const window = published.slice(0, upto).slice(-6);
  const target = Math.max(...published.slice(0, upto).map((p) => Math.ceil(p.ms / 1000)), 2);
  let out = `#EXTM3U\n#EXT-X-VERSION:7\n#ANTSTREAM-BROADCAST:b${broadcast}\n`;
  out += `#EXT-X-TARGETDURATION:${target}\n`;
  out += `#EXT-X-MEDIA-SEQUENCE:${window.length ? window[0].seq : 0}\n`;
  const discBefore = published.slice(0, upto - window.length).filter((p) => p.discontinuity).length;
  if (discBefore) out += `#EXT-X-DISCONTINUITY-SEQUENCE:${discBefore}\n`;
  window.forEach((p, i) => {
    if (p.discontinuity && i > 0) out += '#EXT-X-DISCONTINUITY\n';
    if (i === 0 || p.discontinuity) out += `#EXT-X-MAP:URI="/bzz/${initRef}/init-1.mp4"\n`;
    out += `#EXTINF:${(p.ms / 1000).toFixed(3)},\n/bzz/${p.ref}/${p.name}\n`;
  });
  if (ended && lag === 0) out += '#EXT-X-ENDLIST\n';
  return out;
}

function sendBlob(res, blob) {
  res.writeHead(200, {
    'Content-Type': blob.type,
    'Cache-Control': 'public, max-age=31536000, immutable',
  });
  res.end(blob.bytes);
}

const TYPES = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css' };

createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  const m = /^\/bzz\/([0-9a-f]{64})\/?(.*)$/.exec(url.pathname);
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (!m) {
    res.writeHead(404).end('not found');
    return;
  }
  const [, ref, path] = m;
  if (ref === PLAYER_REF) {
    const file = join(playerDir, path || 'index.html');
    if (!file.startsWith(playerDir) || !existsSync(file)) {
      res.writeHead(404).end('not found');
      return;
    }
    res.writeHead(200, { 'Content-Type': TYPES[extname(file)] || 'application/octet-stream' });
    res.end(readFileSync(file));
    return;
  }
  if (ref === CHANNEL_REF && path === 'stream.m3u8') {
    if (!published.length) {
      res.writeHead(404).end('feed has no updates yet');
      return;
    }
    res.writeHead(200, { 'Content-Type': 'application/vnd.apple.mpegurl', 'Cache-Control': 'no-cache' });
    res.end(playlist());
    return;
  }
  const blob = blobs.get(ref);
  if (blob && blob.name === path) {
    setTimeout(() => sendBlob(res, blob), Math.random() * slowMs);
    return;
  }
  res.writeHead(404).end('not found');
}).listen(port, '127.0.0.1', () => {
  console.log(`fake gateway on http://127.0.0.1:${port}`);
  console.log(`player:  http://127.0.0.1:${port}/bzz/${PLAYER_REF}/#${CHANNEL_REF}`);
  // Like a broadcast that starts a moment after the viewer opens the link.
  setTimeout(publishNext, Number(process.env.START_DELAY_MS || 3000));
});
