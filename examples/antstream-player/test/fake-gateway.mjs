#!/usr/bin/env node
// A stand-in for a bee/ant gateway carrying one live AntStream channel in
// the Solar Punk format, for exercising the player without a funded
// publisher.
//
//   node test/fake-gateway.mjs <segments-dir> [port]
//
// <segments-dir> holds what the Android app's self-test mode writes
// (`init-1.mp4`, `seg-N.m4s`, `durations.txt`; see
// examples/android-stream/README.md). A `durations.txt` that lists more
// than one initialization segment (`<n> true 0` lines, each followed by
// its own media) replays a writer restart there: the next segment is
// listed under `init-<n>.mp4` as a new `#EXT-X-MAP` behind an
// `#EXT-X-DISCONTINUITY`, as the iOS app's turned broadcast is (#228). The server replays those segments in
// real time as a channel shaped like the publisher's `"solar-punk"` format
// (crates/ant-ffi/src/publisher.rs): a sequence feed of owner OWNER and
// topic `keccak256(TOPIC)` whose every update is the live playlist itself,
// segments and `#EXT-X-MAP` listed as bare 64-hex references, a fresh
// `#EXT-X-MAP` behind an `#EXT-X-DISCONTINUITY` where the recording loops
// (a writer restart), and a closing window (`#EXT-X-ENDLIST`) followed by
// the recording (`#EXT-X-PLAYLIST-TYPE:VOD`) as the last two updates of a
// broadcast.
//
// Settings (environment):
//   OWNER=<40 hex>, TOPIC=<text>   the channel (default topic `antstream`)
//   START_DELAY_MS=<n>  the first segment comes n ms after start (3000)
//   ENDLIST=<n>         end the broadcast after n segments
//   RESTART_MS=<n>      a new broadcast starts n ms after one ended; it
//                       continues the same feed (issue #173), media
//                       numbering on behind one discontinuity
//   STALL=<n>           stop after n segments *without* ENDLIST, like a
//                       broadcaster that dropped off
//   GAP_EVERY=<n>       every n-th segment is lost and listed as #EXT-X-GAP
//   WINDOW=<n>          live playlist entries (8)
//   SLOW_MS=<n>         every segment read waits a random 0..n ms
//   PLAYLIST_MS=<a>-<b> every head lookup (`GET /feeds`) answers after a
//                       random a..b ms, with the head as it was when the
//                       lookup started
//   STALE_PCT=<p>       p% of head lookups answer with an older update
//   SLOT_MS=<a>-<b>     every slot read (`GET /soc`) waits a..b ms
//   LADDER=1            write a ladder time marker every 10 s period
//                       naming the newest index (issue #174)
//   RECORDING_PAD=<n>   pad each recording with n bytes of comment lines
//                       (a recording past Freedom's 512 KiB read budget,
//                       as hours of broadcast make one)
//   PRE_UPDATES=<n>     n older updates (an earlier, ended broadcast)
//                       before this run's first, so a join from index 0
//                       would be a long walk
//   UPSTREAM=<url>      no fake channel: proxy everything but the player
//                       to a real gateway (e.g. antd)
//
// The owner's catalog feed (topic `antstream-catalog`, issue #220), off
// unless one of these is set:
//   CATALOG=<file>      its head is this file, served as it is (read again
//                       on every request: edit it to change the entry, or
//                       put anything in it to try a broken catalog)
//   CATALOG_TITLE=<text>, CATALOG_DESCRIPTION=<text>, CATALOG_STATE=<s>,
//   CATALOG_START=<ISO 8601 | +<minutes>>
//                       without CATALOG: one entry for the channel, built
//                       like crates/ant-ffi/src/catalog.rs writes it
//                       (state `scheduled` when CATALOG_START is given,
//                       else `live`)
//   THUMBNAIL=<jpeg>    the thumbnail: a built entry names it, and it is
//                       served at /bzz/<ref>/ for every `thumbnail` the
//                       catalog names
//   CATALOG_MS=<a>-<b>  every catalog read answers after a..b ms
//
// Routes:
//   /bzz/<PLAYER_REF>/<file>        the player (examples/antstream-player)
//   GET /feeds/<owner>/<topic hex>  the head: the newest update's playlist,
//                                   `swarm-feed-index` naming its index
//   GET /soc/<owner>/<identifier>   one feed slot, or a ladder marker;
//                                   `swarm-only-root-chunk: true` answers
//                                   the wrapped chunk (span ‖ payload)
//   GET /bytes/<ref>                media, and the parts of a playlist
//                                   longer than one chunk
//   GET /feeds/<owner>/<catalog topic hex>
//                                   the catalog's head (CATALOG...)
//   GET /bzz/<thumbnail ref>/       the thumbnail (THUMBNAIL)

import { createServer } from 'node:http';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { readFileSync, existsSync } from 'node:fs';
import { join, dirname, extname } from 'node:path';
import { fileURLToPath } from 'node:url';

const require = createRequire(import.meta.url);
const K = require('../keccak.js');

export const PLAYER_REF = 'a'.repeat(64);

const here = dirname(fileURLToPath(import.meta.url));
const playerDir = join(here, '..');
const dir = process.argv[2];
const port = Number(process.argv[3] || 1733);
const env = process.env;
const OWNER = (env.OWNER || 'd5572300e441b77b72bdd318bbfa7b97a1f03096').replace(/^0x/, '').toLowerCase();
const TOPIC = env.TOPIC || 'antstream';
const TOPIC_HEX = K.topicFromString(TOPIC);
const GROUP_HEX = K.topicFromString(`antstream-ladder-${TOPIC_HEX}`);
const endAfter = Number(env.ENDLIST || 0);
const slowMs = Number(env.SLOW_MS || 0);
const restartMs = Number(env.RESTART_MS || 0);
const stallAfter = Number(env.STALL || 0);
const stalePct = Number(env.STALE_PCT || 0);
const gapEvery = Number(env.GAP_EVERY || 0);
const windowSize = Number(env.WINDOW || 8);
const ladder = env.LADDER === '1';
const preUpdates = Number(env.PRE_UPDATES || 0);
const recordingPad = Number(env.RECORDING_PAD || 0);
const upstream = env.UPSTREAM || '';
const range = (v) => {
  const [a, b] = (v || '0').split('-').map(Number);
  return () => a + Math.random() * ((b || a) - a);
};
const playlistDelay = range(env.PLAYLIST_MS);
const catalogDelay = range(env.CATALOG_MS);
const slotDelay = range(env.SLOT_MS);
if (!dir && !upstream) {
  console.error('usage: fake-gateway.mjs <segments-dir> [port]   (or UPSTREAM=<url> fake-gateway.mjs - [port])');
  process.exit(2);
}

const refOf = (name) => createHash('sha256').update(name).digest('hex');
const blobs = new Map(); // ref -> { bytes, type }

// -- the catalog -------------------------------------------------------------

const CATALOG_TOPIC_HEX = K.topicFromString('antstream-catalog');
const thumbnail = env.THUMBNAIL ? readFileSync(env.THUMBNAIL) : null;
const thumbnailRef = thumbnail ? refOf(thumbnail) : null;
const catalogBuilt = !env.CATALOG && ['CATALOG_TITLE', 'CATALOG_DESCRIPTION', 'CATALOG_STATE', 'CATALOG_START'].some((k) => env[k] !== undefined);
const catalogStart = (() => {
  const v = env.CATALOG_START;
  if (!v) return null;
  const at = v.startsWith('+') ? new Date(Date.now() + Number(v.slice(1)) * 60000) : new Date(v);
  return Number.isNaN(at.getTime()) ? v : at.toISOString();
})();
const catalogAt = Date.now();

/** The catalog feed's head as it is now, or null when there is none. */
function catalogBody() {
  if (env.CATALOG) return readFileSync(env.CATALOG);
  if (!catalogBuilt) return null;
  const entry = {
    owner: OWNER,
    topic: TOPIC,
    title: env.CATALOG_TITLE || TOPIC,
    ...(env.CATALOG_DESCRIPTION ? { description: env.CATALOG_DESCRIPTION } : {}),
    state: env.CATALOG_STATE || (catalogStart ? 'scheduled' : 'live'),
    mediatype: 'video',
    timestamp: catalogAt,
    ...(thumbnailRef ? { thumbnail: thumbnailRef } : {}),
    ...(catalogStart ? { scheduledStartTime: catalogStart } : {}),
  };
  return Buffer.from(JSON.stringify([entry]));
}

/** Whether `ref` is a thumbnail the catalog names (served from THUMBNAIL). */
function isThumbnail(ref) {
  if (!thumbnail) return false;
  if (ref === thumbnailRef) return true;
  try {
    const list = JSON.parse(String(catalogBody()));
    return Array.isArray(list) && list.some((e) => e && String(e.thumbnail).toLowerCase() === ref);
  } catch (_) {
    return false;
  }
}

// -- the feed ----------------------------------------------------------------

const updates = []; // feed index -> playlist text
const socs = new Map(); // identifier -> () => body
const counts = { head: 0, slot: 0, marker: 0, bytes: 0, catalog: 0, thumbnail: 0, notFound: 0 };

function writeUpdate(text) {
  const index = updates.length;
  updates.push(text);
  socs.set(K.feedIdentifier(TOPIC_HEX, index), () => updates[index]);
  return index;
}

let lastMarkerPeriod = -1;
function writeMarker() {
  if (!ladder || !updates.length || ended) return;
  const now = Date.now();
  const period = Math.floor(now / 10000);
  if (period === lastMarkerPeriod) return;
  lastMarkerPeriod = period;
  const text = JSON.stringify({ v: 2, period, writtenAt: now, rungs: { [TOPIC_HEX]: updates.length - 1 }, segmentMs: 2000 });
  socs.set(K.ladderMarkerIdentifier(GROUP_HEX, period), () => text);
}

// -- the broadcast -----------------------------------------------------------

let records = [];
if (dir && !upstream) {
  // Each media segment under the init segment listed last before it.
  let init = 'init-1.mp4';
  for (const [n, isInit, ms] of readFileSync(join(dir, 'durations.txt'), 'utf8')
    .trim()
    .split('\n')
    .map((l) => l.split(' '))) {
    if (isInit === 'true') init = `init-${n}.mp4`;
    else records.push({ file: `seg-${n}.m4s`, ms: Number(ms), init });
  }
}
const initBytes = dir && !upstream ? readFileSync(join(dir, 'init-1.mp4')) : null;

let published = []; // this broadcast: { seq, ref, ms, discontinuity, map, gap }
let seq = 0; // this broadcast's segments
let mediaSeq = 0; // the channel's media numbering, continued across broadcasts
let discSeq = 0; // discontinuities before this broadcast's window
let ended = false;
let broadcast = 0;
let mapRef = null;

function newInit(tag, file = 'init-1.mp4') {
  const ref = refOf(`init-${broadcast}-${tag}`);
  blobs.set(ref, { bytes: file === 'init-1.mp4' ? initBytes : readFileSync(join(dir, file)), type: 'video/mp4' });
  return ref;
}

function entryLines(p, first) {
  let out = '';
  if (p.discontinuity) out += '#EXT-X-DISCONTINUITY\n';
  if (first || p.discontinuity) out += `#EXT-X-MAP:URI="${p.map}"\n`;
  if (p.gap) out += '#EXT-X-GAP\n';
  out += `#EXTINF:${(p.ms / 1000).toFixed(3)},\n${p.gap ? `gap-${p.seq}` : p.ref}\n`;
  return out;
}

function head(target) {
  return `#EXTM3U\n#EXT-X-VERSION:7\n#ANTSTREAM-BROADCAST:b${broadcast}\n#EXT-X-TARGETDURATION:${target}\n`;
}

function livePlaylist(endlist) {
  const window = published.slice(-windowSize);
  const target = Math.max(...published.map((p) => Math.ceil(p.ms / 1000)), 2);
  let out = head(target);
  out += `#EXT-X-MEDIA-SEQUENCE:${window[0].seq}\n`;
  // Like the publisher: the tag stays on a segment heading the window, and
  // counts in the sequence only once that segment has slid out.
  const before = discSeq + published.slice(0, published.length - window.length).filter((p) => p.discontinuity).length;
  if (before) out += `#EXT-X-DISCONTINUITY-SEQUENCE:${before}\n`;
  window.forEach((p, i) => { out += entryLines(p, i === 0); });
  return endlist ? `${out}#EXT-X-ENDLIST\n` : out;
}

function recordingPlaylist() {
  const target = Math.max(...published.map((p) => Math.ceil(p.ms / 1000)), 2);
  let out = head(target).replace('#EXT-X-TARGETDURATION', '#EXT-X-PLAYLIST-TYPE:VOD\n#EXT-X-TARGETDURATION');
  out += `#EXT-X-MEDIA-SEQUENCE:${published[0].seq}\n`;
  if (discSeq) out += `#EXT-X-DISCONTINUITY-SEQUENCE:${discSeq}\n`;
  published.forEach((p, i) => { out += entryLines(p, i === 0); });
  for (let n = 0; n < recordingPad; n += 1024) out += `#ANTSTREAM-PAD:${'x'.repeat(1024 - 16)}\n`;
  return `${out}#EXT-X-ENDLIST\n`;
}

function publishNext() {
  if (ended) return;
  const rec = records[seq % records.length];
  // The recording loops: a writer restart, a new init segment behind a
  // discontinuity. A new broadcast opens with one too.
  const loop = seq > 0 && seq % records.length === 0;
  // A writer restart inside the recording (another init segment).
  const restart = seq > 0 && !loop && rec.init !== records[(seq - 1) % records.length].init;
  const fresh = seq === 0 && (broadcast > 0 || preUpdates > 0);
  if (seq === 0 || loop || restart) mapRef = newInit(seq, rec.init);
  const ref = refOf(`${broadcast}-${seq}`);
  const gap = gapEvery > 0 && seq > 0 && seq % gapEvery === 0;
  if (!gap) blobs.set(ref, { bytes: readFileSync(join(dir, rec.file)), type: 'video/iso.segment' });
  published.push({ seq: mediaSeq, ref, ms: rec.ms, discontinuity: loop || restart || fresh, map: mapRef, gap });
  seq += 1;
  mediaSeq += 1;
  writeUpdate(livePlaylist(false));
  writeMarker();
  if (stallAfter && seq >= stallAfter) return;
  if (endAfter && seq >= endAfter) {
    ended = true;
    writeUpdate(livePlaylist(true));
    const at = writeUpdate(recordingPlaylist());
    console.log(`broadcast b${broadcast} ended: recording at index ${at}`);
    if (restartMs) {
      setTimeout(() => {
        discSeq += published.filter((p) => p.discontinuity).length;
        broadcast += 1;
        published = [];
        seq = 0;
        ended = false;
        console.log(`broadcast b${broadcast} starts at index ${updates.length}`);
        publishNext();
      }, restartMs);
    }
    return;
  }
  setTimeout(publishNext, rec.ms);
}

/**
 * An earlier, ended broadcast's updates ahead of this run's: one segment
 * each, the last one closed, so the channel's head is an ended broadcast
 * until this run's first segment.
 */
function prefill() {
  if (!preUpdates) return;
  const init = refOf('init-old');
  blobs.set(init, { bytes: initBytes, type: 'video/mp4' });
  for (let i = 0; i < preUpdates; i++) {
    const ref = refOf(`old-${i}`);
    blobs.set(ref, { bytes: readFileSync(join(dir, records[i % records.length].file)), type: 'video/iso.segment' });
    let text = '#EXTM3U\n#EXT-X-VERSION:7\n#ANTSTREAM-BROADCAST:old\n#EXT-X-TARGETDURATION:3\n';
    text += `#EXT-X-MEDIA-SEQUENCE:${i}\n#EXT-X-MAP:URI="${init}"\n#EXTINF:${(records[i % records.length].ms / 1000).toFixed(3)},\n${ref}\n`;
    writeUpdate(i === preUpdates - 1 ? `${text}#EXT-X-ENDLIST\n` : text);
  }
  mediaSeq = preUpdates;
  discSeq = 0;
  broadcast = 1;
}

// -- serving -----------------------------------------------------------------

/**
 * A SOC body as `swarm-only-root-chunk` answers it: `span(8 LE) ‖
 * payload`, the payload being the body itself up to one chunk, else the
 * references of its parts (each served by `/bytes` as the bytes under
 * it), as a chunk tree carries them: 4096-byte leaves, 128 references
 * to an intermediate chunk.
 */
function rootChunk(body) {
  const bytes = Buffer.from(body);
  const span = Buffer.alloc(8);
  span.writeBigUInt64LE(BigInt(bytes.length));
  if (bytes.length <= 4096) return Buffer.concat([span, bytes]);
  let size = 4096;
  while (size * 128 < bytes.length) size *= 128;
  const refs = [];
  for (let at = 0; at < bytes.length; at += size) {
    const part = bytes.subarray(at, at + size);
    const ref = refOf(`part-${refOf(part.toString('binary'))}`);
    blobs.set(ref, { bytes: part, type: 'application/octet-stream' });
    refs.push(Buffer.from(ref, 'hex'));
  }
  return Buffer.concat([span, ...refs]);
}

const TYPES = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css' };

function notFound(res, message) {
  counts.notFound += 1;
  res.writeHead(404, { 'Content-Type': 'application/json' }).end(JSON.stringify({ code: 404, message }));
}

async function proxy(req, res) {
  const r = await fetch(`${upstream}${req.url}`, { headers: { 'swarm-only-root-chunk': req.headers['swarm-only-root-chunk'] || 'false' } });
  const headers = {};
  for (const [k, v] of r.headers) if (!['content-encoding', 'transfer-encoding', 'connection'].includes(k)) headers[k] = v;
  res.writeHead(r.status, headers);
  res.end(Buffer.from(await r.arrayBuffer()));
}

createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  const path = url.pathname;
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Expose-Headers', 'swarm-feed-index, swarm-feed-index-next');
  const player = /^\/bzz\/([0-9a-f]{64})\/?(.*)$/.exec(path);
  if (player && player[1] !== PLAYER_REF && !upstream && isThumbnail(player[1])) {
    counts.thumbnail += 1;
    res.writeHead(200, { 'Content-Type': 'image/jpeg', 'Cache-Control': 'public, max-age=31536000, immutable' });
    res.end(thumbnail);
    return;
  }
  if (player && player[1] === PLAYER_REF) {
    const file = join(playerDir, player[2] || 'index.html');
    if (!file.startsWith(playerDir) || !existsSync(file)) return notFound(res, 'not found');
    res.writeHead(200, { 'Content-Type': TYPES[extname(file)] || 'application/octet-stream' });
    res.end(readFileSync(file));
    return;
  }
  if (upstream) {
    proxy(req, res).catch((err) => res.writeHead(502).end(String(err)));
    return;
  }
  const feedRoute = /^\/feeds\/(?:0x)?([0-9a-f]{40})\/([0-9a-f]{64})$/i.exec(path);
  if (feedRoute && feedRoute[2].toLowerCase() === CATALOG_TOPIC_HEX) {
    counts.catalog += 1;
    const body = feedRoute[1].toLowerCase() === OWNER ? catalogBody() : null;
    setTimeout(() => {
      if (!body) return notFound(res, 'no update found');
      res.writeHead(200, {
        'Content-Type': 'application/octet-stream',
        'Cache-Control': 'no-cache',
        'swarm-feed-index': '0'.repeat(16),
        'swarm-feed-index-next': '1'.padStart(16, '0'),
        'swarm-feed-resolved-version': 'v2',
      });
      res.end(body);
    }, catalogDelay());
    return;
  }
  if (feedRoute) {
    counts.head += 1;
    if (feedRoute[1].toLowerCase() !== OWNER || feedRoute[2].toLowerCase() !== TOPIC_HEX || !updates.length) {
      return notFound(res, 'no update found');
    }
    // A stale lookup: the head as it was a few updates ago.
    const lag = Math.random() * 100 < stalePct ? 1 + Math.floor(Math.random() * 5) : 0;
    const index = Math.max(0, updates.length - 1 - lag);
    const text = updates[index];
    setTimeout(() => {
      res.writeHead(200, {
        'Content-Type': 'application/octet-stream',
        'Cache-Control': 'no-cache',
        'swarm-feed-index': index.toString(16).padStart(16, '0'),
        'swarm-feed-index-next': (index + 1).toString(16).padStart(16, '0'),
        'swarm-feed-resolved-version': 'v2',
      });
      res.end(text);
    }, playlistDelay());
    return;
  }
  const socRoute = /^\/soc\/(?:0x)?([0-9a-f]{40})\/([0-9a-f]{64})$/i.exec(path);
  if (socRoute) {
    const id = socRoute[2].toLowerCase();
    const body = socRoute[1].toLowerCase() === OWNER ? socs.get(id) : undefined;
    const isMarker = body && body().startsWith('{');
    counts[isMarker ? 'marker' : 'slot'] += 1;
    setTimeout(() => {
      if (!body) return notFound(res, 'requested chunk cannot be retrieved');
      const onlyRoot = String(req.headers['swarm-only-root-chunk']).toLowerCase() === 'true';
      res.writeHead(200, { 'Content-Type': 'application/octet-stream', 'Cache-Control': 'no-cache' });
      res.end(onlyRoot ? rootChunk(body()) : body());
    }, slotDelay());
    return;
  }
  const bytesRoute = /^\/bytes\/([0-9a-f]{64})\/?$/.exec(path);
  if (bytesRoute) {
    counts.bytes += 1;
    const blob = blobs.get(bytesRoute[1]);
    if (!blob) return notFound(res, 'not found');
    setTimeout(() => {
      res.writeHead(200, { 'Content-Type': blob.type, 'Cache-Control': 'public, max-age=31536000, immutable' });
      res.end(blob.bytes);
    }, Math.random() * slowMs);
    return;
  }
  if (path === '/_counts') {
    res.writeHead(200, { 'Content-Type': 'application/json' }).end(JSON.stringify({ ...counts, updates: updates.length }));
    return;
  }
  notFound(res, 'not found');
}).listen(port, '127.0.0.1', () => {
  const channel = `0x${OWNER}/${encodeURIComponent(TOPIC)}`;
  console.log(`fake gateway on http://127.0.0.1:${port}${upstream ? ` (proxying to ${upstream})` : ''}`);
  console.log(`player:  http://127.0.0.1:${port}/bzz/${PLAYER_REF}/#${channel}`);
  if (upstream) return;
  console.log(`channel: owner 0x${OWNER}, topic "${TOPIC}" (${TOPIC_HEX})`);
  if (env.CATALOG || catalogBuilt) console.log(`catalog: ${env.CATALOG || 'built from CATALOG_*'} at /feeds/${OWNER}/${CATALOG_TOPIC_HEX}${thumbnailRef ? `, thumbnail /bzz/${thumbnailRef}/` : ''}`);
  prefill();
  if (ladder) setInterval(writeMarker, 500);
  // Like a broadcast that starts a moment after the viewer opens the link.
  setTimeout(publishNext, Number(env.START_DELAY_MS || 3000));
});
