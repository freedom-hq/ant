/*
 * AntStream web player.
 *
 * A channel is a Swarm feed manifest: `<channel>/stream.m3u8` resolves the
 * feed to the newest HLS playlist the publisher wrote
 * (crates/ant-ffi/src/publisher.rs), and every playlist reload resolves it
 * again, so hls.js follows the live edge by itself.
 *
 * The page runs in two kinds of places:
 *
 *  - Freedom (and any browser with a native `bzz:` scheme): the page is
 *    `bzz://<player>/#<channel>`, the playlist is `bzz://<channel>/stream.m3u8`.
 *  - An HTTP gateway (bee, ant): the page is `<root>/bzz/<player>/` and the
 *    playlist `<root>/bzz/<channel>/stream.m3u8`, `<root>` possibly a
 *    sub-path such as `https://example.org/ant`.
 *
 * The publisher lists segments by gateway-root path (`/bzz/<ref>/seg-N.m4s`).
 * Resolved against a `bzz://` playlist that path lands inside the channel
 * (`bzz://<channel>/bzz/<ref>/…`), and on a sub-path gateway it drops the
 * prefix, so every request goes through `toContentUrl`, which maps any
 * `…/bzz/<ref>/<rest>` back onto this environment's form.
 *
 * Freedom forwards no `Cache-Control` request header to its node, so
 * playlist reloads carry a `t=` query instead. Feed-resolved responses are
 * `no-cache` on the node side; segments are immutable and cache normally.
 *
 * Playlist reads and segment fetches bypass hls.js's one-at-a-time loading
 * (see "playlist reads" and "segment fetches" below), and debug.js shows
 * what they did.
 */
(function () {
  'use strict';

  const REF = /^(?:0x)?([0-9a-f]{64}(?:[0-9a-f]{64})?)$/i;
  const NAME = /^[a-z0-9-]+(\.[a-z0-9-]+)+$/i;
  const PLAYLIST = 'stream.m3u8';
  const RECENT_KEY = 'antstream.recent';

  const inBzz = location.protocol === 'bzz:';
  const gatewayRoot = (() => {
    if (inBzz) return '';
    const at = location.pathname.indexOf('/bzz/');
    return location.origin + (at > 0 ? location.pathname.slice(0, at) : '');
  })();

  const $ = (id) => document.getElementById(id);
  const video = $('video');
  const els = {
    form: $('channel-form'),
    input: $('channel-input'),
    stage: $('stage'),
    intro: $('intro'),
    badge: $('badge'),
    overlay: $('overlay'),
    status: $('status'),
    goLive: $('go-live'),
    unmute: $('unmute'),
    copy: $('copy-link'),
    channelRef: $('channel-ref'),
    recent: $('recent'),
    latency: $('stat-latency'),
    buffer: $('stat-buffer'),
    fetch: $('stat-fetch'),
    bw: $('stat-bw'),
    segments: $('stat-segments'),
    res: $('stat-res'),
  };

  // -- addressing -----------------------------------------------------------

  function contentUrl(ref, path) {
    return inBzz ? `bzz://${ref}/${path}` : `${gatewayRoot}/bzz/${ref}/${path}`;
  }

  /** Map `…/bzz/<ref>/<rest>` onto this environment's content URL. */
  function toContentUrl(url) {
    let u;
    try {
      u = new URL(url);
    } catch (_) {
      return url;
    }
    const m = /\/bzz\/([^/]+)\/(.*)$/.exec(u.pathname);
    if (!m) return url;
    return contentUrl(m[1], m[2]) + u.search;
  }

  function withCacheBust(url) {
    const u = new URL(url);
    u.searchParams.set('t', Date.now().toString(36));
    return u.toString();
  }

  /** Accept a bare reference, `0x…`, a bzz:// or gateway link, or an ENS name. */
  function parseChannel(input) {
    let s = (input || '').trim();
    if (!s) return null;
    s = s.replace(/^#/, '').replace(/^channel=/, '');
    const link = /(?:bzz:\/\/|\/bzz\/)([^/?#]+)/i.exec(s);
    if (link) s = link[1];
    const ref = REF.exec(s);
    if (ref) return ref[1].toLowerCase();
    if (NAME.test(s)) return s.toLowerCase();
    return null;
  }

  // -- settings and debug log ------------------------------------------------
  //
  // The tuning knobs below can be changed from the debug panel (debug.js)
  // and are kept in localStorage. The log holds every playlist read and
  // segment fetch with its timings plus markers (stalls, errors, new
  // broadcasts). It runs whether or not the panel is open, so opening it
  // after a hiccup still shows what happened.

  const SETTINGS_KEY = 'antstream.settings';
  const DEFAULTS = {
    // Segments fetched ahead of the one hls.js loads. 0: hls.js alone.
    prefetch: 3,
    // Feed reads kept overlapping while live. 0: hls.js's own reloads only.
    polls: 3,
    // Seconds to buffer before playing after a start, seek or stall. 0: off.
    rebuffer: 4,
  };
  const settings = (() => {
    try {
      return { ...DEFAULTS, ...JSON.parse(localStorage.getItem(SETTINGS_KEY) || '{}') };
    } catch (_) {
      return { ...DEFAULTS };
    }
  })();

  function setSetting(key, value) {
    settings[key] = value;
    try {
      localStorage.setItem(SETTINGS_KEY, JSON.stringify(settings));
    } catch (_) {
      /* storage unavailable: the setting lasts for this page */
    }
  }

  const MAX_LOG = 400;
  const MAX_SAMPLES = 240; // two minutes at two a second

  const log = {
    version: 0,
    entries: [],
    samples: [],
    session: null,
    touch() {
      this.version += 1;
    },
    add(entry) {
      entry.at = Date.now();
      this.entries.push(entry);
      if (this.entries.length > MAX_LOG) this.entries.shift();
      this.touch();
      return entry;
    },
    marker(text) {
      return this.add({ kind: 'marker', text });
    },
    begin(ch) {
      if (this.session && this.session.channel === ch) {
        // Same channel (a new broadcast, or a retry): keep the history,
        // time the first frame again.
        this.session.startedAt = Date.now();
        this.session.firstPlayingAt = null;
        this.marker('player restarted');
        return;
      }
      this.entries = [];
      this.samples = [];
      this.session = {
        channel: ch,
        startedAt: Date.now(),
        firstPlayingAt: null,
        stalls: 0,
        stallMs: 0,
        stallSince: null,
        claims: { ready: 0, joined: 0, miss: 0 },
      };
      this.touch();
    },
  };

  // A fresh LoaderStats, as hls.js's own loaders keep one.
  const newStats = () => ({
    aborted: false,
    loaded: 0,
    retry: 0,
    total: 0,
    chunkCount: 0,
    bwEstimate: 0,
    loading: { start: 0, first: 0, end: 0 },
    parsing: { start: 0, end: 0 },
    buffering: { start: 0, first: 0, end: 0 },
  });

  // -- playlist guard -------------------------------------------------------
  //
  // A Swarm node resolves "the newest feed update" by probing indices with
  // a short per-probe deadline, so a reload can come back with an *older*
  // playlist than the last one (an update still propagating loses the
  // race). Within a broadcast the playlist only moves forward, so an older
  // one is replaced by the newest seen. The publisher names each broadcast
  // in its playlists (`#ANTSTREAM-BROADCAST:<id>`): an id never seen before
  // is a new broadcast on the channel, an id seen earlier is a stale read.

  const guard = { text: null, key: null, id: null, ids: new Set(), newBroadcast: false };

  function resetGuard() {
    guard.text = null;
    guard.key = null;
    guard.id = null;
    guard.ids = new Set();
    guard.newBroadcast = false;
  }

  function playlistInfo(text) {
    const seq = Number((/#EXT-X-MEDIA-SEQUENCE:(\d+)/.exec(text) || [])[1] || 0);
    const segments = (text.match(/#EXTINF/g) || []).length;
    const ended = text.includes('#EXT-X-ENDLIST');
    const id = (/^#ANTSTREAM-BROADCAST:(\S+)/m.exec(text) || [])[1] || null;
    // Order within one broadcast: later window start, then more segments,
    // then closed.
    return { key: [seq, segments, ended ? 1 : 0], id, ended };
  }

  const newer = (a, b) => {
    for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return a[i] > b[i];
    return false;
  };

  function accept(text, info) {
    guard.text = text;
    guard.key = info.key;
    guard.id = info.id;
    if (info.id) guard.ids.add(info.id);
    return text;
  }

  function guardPlaylist(text) {
    const info = playlistInfo(text);
    if (guard.text === null) return accept(text, info);
    if (info.id && info.id !== guard.id) {
      // Another broadcast's playlist: a new one starts the channel over,
      // an earlier one is a stale read.
      if (guard.ids.has(info.id)) return guard.text;
      guard.newBroadcast = true;
      return accept(text, info);
    }
    if (newer(info.key, guard.key) || info.key.join() === guard.key.join()) return accept(text, info);
    return guard.text;
  }

  // -- playlist reads -------------------------------------------------------
  //
  // Every read of `<channel>/stream.m3u8` makes the node resolve the feed on
  // Swarm, which takes seconds. hls.js reloads a live playlist one read at
  // a time, so it hears of a new segment a whole read plus a reload
  // interval late, and the buffer drains meanwhile. Here reads overlap
  // instead: while live, a new one starts every POLL_MS, at most
  // `settings.polls` at once, and hls.js's reload is answered at once with
  // the newest playlist any of them returned (or with the next read to
  // finish, if none is newer than what hls.js has).

  const POLL_MS = 1500;
  // A channel whose playlist stopped moving (broadcaster gone, or paused)
  // is read less often.
  const IDLE_AFTER_MS = 20000;
  const IDLE_POLL_MS = 5000;
  const READ_TIMEOUT_MS = 30000;
  const reads = { gen: 0, url: null, inflight: 0, lastStart: 0, lastNewer: 0, waiters: [], delivered: null };

  function resetReads(url) {
    reads.gen += 1;
    reads.url = url;
    reads.inflight = 0;
    reads.lastStart = 0;
    reads.lastNewer = Date.now();
    reads.waiters = [];
    reads.delivered = null;
  }

  async function readPlaylist() {
    const gen = reads.gen;
    reads.inflight += 1;
    reads.lastStart = Date.now();
    const row = log.add({ kind: 'playlist', start: performance.now(), ms: null, status: 'loading', seq: null, segments: null, note: '' });
    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), READ_TIMEOUT_MS);
    let text = null;
    let error = null;
    try {
      const res = await fetch(withCacheBust(reads.url), { cache: 'no-store', signal: ctl.signal });
      row.status = res.status;
      if (!res.ok) throw Object.assign(new Error(`HTTP ${res.status}`), { code: res.status });
      text = await res.text();
    } catch (err) {
      error = err;
      if (row.status === 'loading') row.status = err.name === 'AbortError' ? 'timeout' : 'error';
      row.note = err.message;
    } finally {
      clearTimeout(timer);
    }
    row.ms = performance.now() - row.start;
    log.touch();
    if (gen !== reads.gen) return;
    reads.inflight -= 1;
    let result = { error };
    if (text !== null) {
      const info = playlistInfo(text);
      row.seq = info.key[0];
      row.segments = info.key[1];
      const before = guard.text;
      const kept = guardPlaylist(text);
      row.note = kept !== text ? 'stale: kept the newer one' : kept === before ? 'unchanged' : 'newer';
      if (row.note === 'newer') reads.lastNewer = Date.now();
      result = { text: kept };
    }
    const waiters = reads.waiters;
    reads.waiters = [];
    for (const w of waiters) w(result);
  }

  function nextPlaylist() {
    if (guard.text !== null && guard.text !== reads.delivered) return Promise.resolve({ text: guard.text });
    const p = new Promise((resolve) => reads.waiters.push(resolve));
    if (reads.inflight === 0) readPlaylist();
    return p;
  }

  /** Called twice a second: keep `settings.polls` reads overlapping while live. */
  function pollPlaylist() {
    if (!hls || ended || guard.text === null) return;
    const now = Date.now();
    const every = now - reads.lastNewer > IDLE_AFTER_MS ? IDLE_POLL_MS : POLL_MS;
    if (reads.inflight < settings.polls && now - reads.lastStart >= every) readPlaylist();
  }

  /** hls.js's playlist loader (`pLoader`), served by the reads above. */
  class PlaylistLoader {
    constructor() {
      this.stats = newStats();
      this.context = null;
      this.callbacks = null;
    }

    load(context, _config, callbacks) {
      this.context = context;
      this.callbacks = callbacks;
      const stats = this.stats;
      stats.loading.start = performance.now();
      nextPlaylist().then((r) => {
        const cb = this.callbacks;
        if (stats.aborted || !cb) return;
        stats.loading.first = stats.loading.end = performance.now();
        if (r.error) {
          cb.onError({ code: r.error.code || 0, text: r.error.message }, context, null, stats);
          return;
        }
        reads.delivered = r.text;
        stats.loaded = stats.total = r.text.length;
        cb.onSuccess({ url: context.url, data: r.text, code: 200 }, stats, context, null);
      });
    }

    abort() {
      this.stats.aborted = true;
      const cb = this.callbacks;
      if (cb && cb.onAbort) cb.onAbort(this.stats, this.context, null);
    }

    destroy() {
      this.stats.aborted = true;
      this.callbacks = null;
    }

    getCacheAge() {
      return null;
    }

    getResponseHeader() {
      return null;
    }
  }

  // -- segment fetches ------------------------------------------------------
  //
  // hls.js fetches one segment at a time, and a Swarm read of a fresh
  // segment spends most of its time finding chunks, not moving bytes. So
  // when hls.js asks for segment N, N+1 … N+`settings.prefetch` start too,
  // side by side, and hls.js's later request for one of them is served from
  // memory or joins the fetch already running. Fetches outside that window
  // (after a seek, or once played past) are cancelled.

  const MAX_SEGMENTS = 12;
  const segs = new Map(); // content URL -> fetch
  let cursorSn = -1; // the segment hls.js asked for last

  function fetchSegment(url, sn, prefetched) {
    const ctl = new AbortController();
    const row = log.add({
      kind: 'segment',
      sn,
      name: url.split('?')[0].split('/').pop(),
      start: performance.now(),
      ttfb: null,
      ms: null,
      bytes: null,
      status: 'loading',
      via: prefetched ? 'prefetch' : 'hls.js',
      note: '',
    });
    const f = { url, sn, ctl, row, data: null, ttfb: 0, ms: 0 };
    f.promise = fetch(url, { signal: ctl.signal }).then(async (res) => {
      row.ttfb = f.ttfb = performance.now() - row.start;
      if (!res.ok) throw Object.assign(new Error(`HTTP ${res.status}`), { code: res.status });
      const data = await res.arrayBuffer();
      row.ms = f.ms = performance.now() - row.start;
      row.bytes = data.byteLength;
      row.status = 'ok';
      f.data = data;
      log.touch();
      return data;
    });
    f.promise.catch((err) => {
      if (row.status === 'loading') {
        row.status = 'error';
        row.note = err.message;
      }
      if (row.ms === null) row.ms = performance.now() - row.start;
      if (segs.get(url) === f) segs.delete(url);
      log.touch();
    });
    segs.set(url, f);
    return f;
  }

  function cancelSegment(f, why) {
    if (f.row.status === 'loading') {
      f.row.status = 'cancelled';
      f.row.note = why;
      f.ctl.abort();
    }
    segs.delete(f.url);
  }

  function clearSegments() {
    for (const f of [...segs.values()]) cancelSegment(f, 'player stopped');
    cursorSn = -1;
  }

  /** Keep only fetches for the segment hls.js wants and the ones after it. */
  function trimSegments() {
    for (const f of [...segs.values()]) {
      if (typeof f.sn !== 'number') continue;
      if (f.sn < cursorSn) cancelSegment(f, 'behind the playhead');
      else if (f.sn > cursorSn + Math.max(settings.prefetch, 0)) cancelSegment(f, 'outside the prefetch window');
    }
    while (segs.size > MAX_SEGMENTS) cancelSegment(segs.values().next().value, 'cache full');
  }

  function prefetchAfter(sn) {
    const depth = settings.prefetch;
    if (!hls || !depth || typeof sn !== 'number' || sn < 0) return;
    const level = hls.levels[Math.max(hls.currentLevel, 0)];
    const frags = (level && level.details && level.details.fragments) || [];
    for (const frag of frags) {
      if (frag.sn <= sn || frag.sn > sn + depth) continue;
      const url = toContentUrl(frag.url);
      if (!segs.has(url)) fetchSegment(url, frag.sn, true);
    }
  }

  /** hls.js's fragment loader (`fLoader`), served by the fetches above. */
  class SegmentLoader {
    constructor() {
      this.stats = newStats();
      this.context = null;
      this.callbacks = null;
      this.timer = null;
    }

    load(context, config, callbacks) {
      this.context = context;
      this.callbacks = callbacks;
      const stats = this.stats;
      stats.loading.start = performance.now();
      const url = toContentUrl(context.url);
      const sn = context.frag ? context.frag.sn : null;
      if (typeof sn === 'number') {
        cursorSn = sn;
        trimSegments();
      }
      let f = segs.get(url);
      if (f) {
        const ready = f.data !== null;
        f.row.via = ready ? 'prefetch, ready' : 'prefetch, joined';
        if (log.session) log.session.claims[ready ? 'ready' : 'joined'] += 1;
      } else {
        f = fetchSegment(url, sn, false);
        if (log.session && typeof sn === 'number') log.session.claims.miss += 1;
      }
      prefetchAfter(sn);
      if (config.timeout) {
        this.timer = setTimeout(() => {
          const cb = this.callbacks;
          if (stats.aborted || !cb) return;
          this.callbacks = null;
          f.row.status = 'timeout';
          cancelSegment(f, 'timed out');
          cb.onTimeout(stats, context, null);
        }, config.timeout);
      }
      f.promise.then(
        (data) => {
          clearTimeout(this.timer);
          const cb = this.callbacks;
          if (stats.aborted || !cb) return;
          // Handed over once: hls.js may transfer the buffer to its worker.
          if (segs.get(url) === f) segs.delete(url);
          // The fetch's own timings, ending now: a prefetch that finished
          // early must not read as an instant download.
          const now = performance.now();
          stats.loading.start = now - f.ms;
          stats.loading.first = stats.loading.start + f.ttfb;
          stats.loading.end = now;
          stats.loaded = stats.total = data.byteLength;
          cb.onSuccess({ url: context.url, data, code: 200 }, stats, context, null);
        },
        (err) => {
          clearTimeout(this.timer);
          const cb = this.callbacks;
          if (stats.aborted || !cb) return;
          stats.loading.end = performance.now();
          cb.onError({ code: err.code || 0, text: err.message }, context, null, stats);
        },
      );
    }

    abort() {
      // The shared fetch keeps running: hls.js often asks again right away.
      clearTimeout(this.timer);
      this.stats.aborted = true;
      const cb = this.callbacks;
      if (cb && cb.onAbort) cb.onAbort(this.stats, this.context, null);
    }

    destroy() {
      clearTimeout(this.timer);
      this.stats.aborted = true;
      this.callbacks = null;
    }

    getCacheAge() {
      return null;
    }

    getResponseHeader() {
      return null;
    }
  }

  // Anything else hls.js loads (keys, steering) only needs its URL mapped.
  const BaseLoader = Hls.DefaultConfig.loader;
  class SwarmLoader extends BaseLoader {
    load(context, config, callbacks) {
      context.url = toContentUrl(context.url);
      super.load(context, config, callbacks);
    }
  }

  // Swarm reads of a fresh segment can take seconds, and a feed that has not
  // propagated yet answers 404 for a while: be patient, never give up early.
  const patient = (ttfb, total, retries) => ({
    default: {
      maxTimeToFirstByteMs: ttfb,
      maxLoadTimeMs: total,
      timeoutRetry: { maxNumRetry: 3, retryDelayMs: 0, maxRetryDelayMs: 0 },
      errorRetry: { maxNumRetry: retries, retryDelayMs: 1000, maxRetryDelayMs: 8000 },
    },
  });

  const HLS_CONFIG = {
    loader: SwarmLoader,
    pLoader: PlaylistLoader,
    fLoader: SegmentLoader,
    enableWorker: true,
    lowLatencyMode: false,
    // The publisher keeps as many segments as fit in one 4096-byte chunk
    // in its live playlist (about 40 of 2 s): sit four behind the newest
    // one, which leaves room to buffer through a slow read. Only a playhead that falls out of the window jumps forward: a
    // jump throws away what is buffered and stalls again.
    liveSyncDurationCount: 4,
    liveMaxLatencyDurationCount: 10,
    maxBufferLength: 30,
    backBufferLength: 30,
    manifestLoadPolicy: patient(30000, 60000, 6),
    playlistLoadPolicy: patient(30000, 40000, 6),
    fragLoadPolicy: patient(30000, 90000, 6),
  };

  // -- rebuffer gate --------------------------------------------------------
  //
  // Playing the moment one segment lands only buys a second or two before
  // the next stall. After a start, seek or stall, playback is held until
  // `settings.rebuffer` seconds are buffered ahead (or the broadcast ended,
  // or HOLD_MAX_MS passed). It holds with playbackRate 0, not pause():
  // hls.js keeps loading and treats rate 0 as halted rather than stalled.

  const HOLD_MAX_MS = 15000;
  const MAX_GAP_S = 1;
  const gate = { holding: false, why: '', since: 0 };

  function hold(why) {
    if (!settings.rebuffer) return;
    if (!gate.holding) {
      gate.holding = true;
      gate.since = Date.now();
    }
    gate.why = why;
    if (video.playbackRate !== 0) video.playbackRate = 0;
  }

  function release(how) {
    if (!gate.holding) return;
    gate.holding = false;
    video.playbackRate = 1;
    log.marker(`${gate.why}: playing after ${((Date.now() - gate.since) / 1000).toFixed(1)} s held (${how})`);
    firstPlaying();
    endStall();
  }

  function firstPlaying() {
    const s = log.session;
    if (!s || s.firstPlayingAt !== null || video.paused) return;
    s.firstPlayingAt = Date.now();
    log.marker(`first frame after ${((s.firstPlayingAt - s.startedAt) / 1000).toFixed(1)} s`);
  }

  function checkGate() {
    if (!gate.holding) return;
    if (!settings.rebuffer) release('gate off');
    else if (bufferAheadAcrossGaps() >= settings.rebuffer) release(`${bufferAheadAcrossGaps().toFixed(1)} s buffered`);
    else if (ended && video.buffered.length) release('broadcast ended');
    else if (Date.now() - gate.since > HOLD_MAX_MS) release('waited long enough');
    else if (video.playbackRate !== 0) video.playbackRate = 0; // attaching media resets it
  }

  function endStall() {
    const s = log.session;
    if (s && s.stallSince !== null) {
      s.stallMs += Date.now() - s.stallSince;
      s.stallSince = null;
      log.touch();
    }
  }

  video.addEventListener('waiting', () => {
    const s = log.session;
    if (!hls || !s || s.firstPlayingAt === null || video.seeking || video.paused) return;
    if (s.stallSince === null) {
      s.stalls += 1;
      s.stallSince = Date.now();
      log.marker(`stall at ${video.currentTime.toFixed(1)} s, ${bufferAhead().toFixed(1)} s buffered`);
    }
    // Stuck at a hole with video beyond it: hls.js skips it, but only
    // while playback runs.
    if (bufferAheadAcrossGaps() < settings.rebuffer) hold('stall');
  });

  video.addEventListener('seeking', () => {
    const s = log.session;
    if (hls && s && s.firstPlayingAt !== null && bufferAheadAcrossGaps() < settings.rebuffer) hold('seek');
  });

  video.addEventListener('playing', () => {
    const s = log.session;
    if (!s || gate.holding || video.playbackRate === 0) return;
    firstPlaying();
    endStall();
  });

  // -- state ----------------------------------------------------------------

  let hls = null;
  let channel = null;
  let ended = false;
  let everPlayed = false;
  let segmentsLoaded = 0;
  let lastFetchMs = null;
  let retryTimer = null;
  let statsTimer = null;
  let tickTimer = null;
  let endedPoll = null;
  let lastFragAt = 0;

  function setStatus(text) {
    els.status.textContent = text;
  }

  function setOverlay(text) {
    els.overlay.hidden = !text;
    els.overlay.textContent = text || '';
  }

  function setBadge(kind, text) {
    els.badge.className = `badge ${kind}`;
    els.badge.textContent = text;
  }

  function stop() {
    clearTimeout(retryTimer);
    clearInterval(statsTimer);
    clearInterval(tickTimer);
    clearInterval(endedPoll);
    endedPoll = null;
    if (hls) hls.destroy();
    hls = null;
    resetReads(null);
    clearSegments();
    gate.holding = false;
    video.playbackRate = 1;
  }

  function play(ch) {
    stop();
    channel = ch;
    ended = false;
    everPlayed = false;
    segmentsLoaded = 0;
    lastFetchMs = null;
    resetGuard();
    log.begin(ch);
    lastFragAt = Date.now();
    els.intro.hidden = true;
    els.stage.hidden = false;
    els.channelRef.textContent = ch;
    els.input.value = ch;
    remember(ch);
    setBadge('', 'CONNECTING');
    setOverlay('Finding the channel on Swarm…');
    setStatus('Resolving the channel feed…');

    if (!Hls.isSupported()) {
      // Safari-style native HLS: no custom loader, so this only works where
      // the playlist's absolute paths already resolve (an HTTP gateway).
      if (video.canPlayType('application/vnd.apple.mpegurl')) {
        video.src = contentUrl(ch, PLAYLIST);
        video.play().catch(() => {});
        return;
      }
      setOverlay('This browser cannot play HLS video.');
      return;
    }

    resetReads(contentUrl(ch, PLAYLIST));
    hold('start');
    hls = new Hls(HLS_CONFIG);
    hls.attachMedia(video);
    hls.on(Hls.Events.MEDIA_ATTACHED, () => hls.loadSource(contentUrl(ch, PLAYLIST)));
    hls.on(Hls.Events.MANIFEST_PARSED, () => {
      setStatus('Buffering…');
      video.play().catch(() => {});
    });
    hls.on(Hls.Events.LEVEL_LOADED, (_e, data) => {
      const details = data.details;
      // A channel is permanent: each broadcast continues the same feed
      // under its own broadcast id. The guard flags a new one; start over
      // on it.
      if (guard.newBroadcast) {
        log.marker('a new broadcast started on the channel');
        play(channel);
        return;
      }
      const wasEnded = ended;
      ended = !details.live;
      // New segments may have come into the prefetch window.
      prefetchAfter(cursorSn);
      if (ended && !wasEnded) log.marker('the broadcast ended');
      if (ended) {
        setStatus('The broadcast has ended: playing its last minutes. You will switch to the next one when it starts.');
        watchForNextBroadcast();
      }
    });
    hls.on(Hls.Events.FRAG_LOADED, (_e, data) => {
      segmentsLoaded += 1;
      lastFragAt = Date.now();
      const s = data.frag.stats;
      lastFetchMs = s.loading.end - s.loading.start;
    });
    hls.on(Hls.Events.ERROR, (_e, data) => onError(data));
    statsTimer = setInterval(updateStats, 1000);
    tickTimer = setInterval(tick, 250);
  }

  /**
   * After a broadcast ended, keep an eye on the channel: when a playlist
   * without #EXT-X-ENDLIST shows up, the channel is live again.
   */
  function watchForNextBroadcast() {
    if (endedPoll) return;
    endedPoll = setInterval(async () => {
      try {
        const res = await fetch(withCacheBust(contentUrl(channel, PLAYLIST)), { cache: 'no-store' });
        if (!res.ok) return;
        const text = await res.text();
        // Only a playlist from a *new* broadcast counts: a stale feed read
        // can still return an older, not yet closed playlist of the
        // broadcast that just ended.
        const info = playlistInfo(text);
        if (info.id && !guard.ids.has(info.id) && !info.ended) play(channel);
      } catch (_) {
        /* offline for a moment: try again on the next tick */
      }
    }, 10000);
  }

  /** Four times a second: the gate, overlapping reads, buffer samples. */
  let ticks = 0;
  function tick() {
    checkGate();
    pollPlaylist();
    ticks += 1;
    if (ticks % 2) return;
    const s = log.session;
    log.samples.push({
      t: Date.now(),
      ahead: bufferAhead(),
      latency: hls && Number.isFinite(hls.latency) ? hls.latency : null,
      stalled: !!s && s.stallSince !== null,
      holding: gate.holding,
    });
    if (log.samples.length > MAX_SAMPLES) log.samples.shift();
    log.touch();
  }

  function onError(data) {
    if (data.details === Hls.ErrorDetails.BUFFER_STALLED_ERROR) return;
    if (data.fatal || data.type === Hls.ErrorTypes.NETWORK_ERROR) {
      log.marker(`hls.js ${data.details}${data.fatal ? ' (fatal)' : ''}${data.response && data.response.code ? ` ${data.response.code}` : ''}`);
    }
    if (!data.fatal) return;
    if (data.type === Hls.ErrorTypes.MEDIA_ERROR) {
      setStatus('Recovering from a decode error…');
      hls.recoverMediaError();
      return;
    }
    // Network: the feed may not exist yet (broadcast not started, or not
    // propagated), or Swarm is slow. Keep trying; a live channel shows up.
    const waiting = !everPlayed;
    setOverlay(waiting ? 'Waiting for the broadcast to start…' : 'Reconnecting…');
    setStatus(`${data.details}${data.response && data.response.code ? ` (${data.response.code})` : ''}, retrying`);
    clearTimeout(retryTimer);
    retryTimer = setTimeout(() => {
      if (!hls) return;
      if (waiting) {
        play(channel);
      } else {
        hls.startLoad();
      }
    }, 4000);
  }

  // -- live UI --------------------------------------------------------------

  function bufferAhead() {
    const t = video.currentTime;
    for (let i = 0; i < video.buffered.length; i++) {
      if (video.buffered.start(i) <= t + 0.1 && t <= video.buffered.end(i)) {
        return video.buffered.end(i) - t;
      }
    }
    return 0;
  }

  /**
   * Buffered ahead counting past small holes (a discontinuity often leaves
   * one), which hls.js jumps by itself once playback runs.
   */
  function bufferAheadAcrossGaps() {
    const t = video.currentTime;
    let end = t;
    let total = 0;
    for (let i = 0; i < video.buffered.length; i++) {
      const s = video.buffered.start(i);
      const e = video.buffered.end(i);
      if (e <= end || s > end + MAX_GAP_S) continue;
      total += e - Math.max(s, end);
      end = e;
    }
    return total;
  }

  function updateStats() {
    if (!hls) return;
    const playing = !video.paused && video.readyState >= 3;
    if (playing) {
      everPlayed = true;
      setOverlay('');
    }
    const latency = hls.latency;
    const target = hls.targetLatency || 6;
    const behind = !ended && Number.isFinite(latency) && latency > 0 ? latency : null;

    // A live playlist that stops growing: the broadcaster dropped off
    // without closing the stream. hls.js keeps reloading it, so a resumed
    // or new broadcast still comes through.
    const stalled = !ended && everPlayed && Date.now() - lastFragAt > 20000 && bufferAhead() < 0.5;
    if (stalled) {
      setBadge('', 'NO SIGNAL');
      setOverlay('No new video from this channel. Waiting for the broadcaster…');
    } else if (ended) {
      setBadge('ended', 'ENDED');
    } else if (!everPlayed) {
      setBadge('', 'CONNECTING');
    } else if (behind !== null && behind > target + 6) {
      setBadge('behind', `${behind.toFixed(0)} s BEHIND`);
    } else {
      setBadge('live', '● LIVE');
    }
    els.goLive.hidden = ended || behind === null || behind <= target + 6;

    if (gate.holding && everPlayed) setStatus(`Buffering… ${bufferAhead().toFixed(1)} of ${settings.rebuffer} s`);
    else if (everPlayed && playing && !ended) setStatus('Playing live');
    else if (everPlayed && !playing && !video.paused) setStatus('Buffering…');

    els.latency.textContent = behind !== null ? `${behind.toFixed(1)} s` : '–';
    els.buffer.textContent = `${bufferAhead().toFixed(1)} s`;
    els.fetch.textContent = lastFetchMs !== null ? `${(lastFetchMs / 1000).toFixed(2)} s` : '–';
    els.bw.textContent = hls.bandwidthEstimate ? `${(hls.bandwidthEstimate / 1e6).toFixed(2)} Mbit/s` : '–';
    els.segments.textContent = `${segmentsLoaded}`;
    els.res.textContent = video.videoWidth ? `${video.videoWidth}×${video.videoHeight}` : '–';
    els.unmute.textContent = video.muted ? 'Unmute' : 'Mute';
  }

  els.goLive.addEventListener('click', () => {
    if (hls && Number.isFinite(hls.liveSyncPosition)) video.currentTime = hls.liveSyncPosition;
  });

  els.unmute.addEventListener('click', () => {
    video.muted = !video.muted;
    updateStats();
  });

  els.copy.addEventListener('click', async () => {
    const link = shareLink(channel);
    try {
      await navigator.clipboard.writeText(link);
      els.copy.textContent = 'Copied';
    } catch (_) {
      window.prompt('Copy this link', link);
    }
    setTimeout(() => { els.copy.textContent = 'Copy link'; }, 1500);
  });

  function shareLink(ch) {
    const u = new URL(location.href);
    u.hash = ch;
    return u.toString();
  }

  // -- channel selection ----------------------------------------------------

  function recent() {
    try {
      return JSON.parse(localStorage.getItem(RECENT_KEY) || '[]');
    } catch (_) {
      return [];
    }
  }

  function remember(ch) {
    try {
      const list = [ch, ...recent().filter((c) => c !== ch)].slice(0, 8);
      localStorage.setItem(RECENT_KEY, JSON.stringify(list));
    } catch (_) {
      /* storage unavailable: not worth failing playback over */
    }
  }

  function renderRecent() {
    els.recent.textContent = '';
    for (const ch of recent()) {
      const li = document.createElement('li');
      const a = document.createElement('a');
      a.href = `#${ch}`;
      a.textContent = ch;
      li.appendChild(a);
      els.recent.appendChild(li);
    }
  }

  function fromHash() {
    const ch = parseChannel(decodeURIComponent(location.hash));
    if (ch && ch !== channel) play(ch);
  }

  els.form.addEventListener('submit', (e) => {
    e.preventDefault();
    const ch = parseChannel(els.input.value);
    if (!ch) {
      els.input.setCustomValidity('Enter a 64-hex channel reference, a bzz:// link or an ENS name');
      els.input.reportValidity();
      return;
    }
    els.input.setCustomValidity('');
    if (location.hash.slice(1) === ch) play(ch);
    else location.hash = ch;
  });
  els.input.addEventListener('input', () => els.input.setCustomValidity(''));
  window.addEventListener('hashchange', fromHash);

  renderRecent();
  fromHash();

  // Exposed for the test harness and for debugging from the console.
  window.antstreamPlayer = {
    toContentUrl,
    parseChannel,
    contentUrl,
    log,
    settings,
    setSetting,
    DEFAULTS,
    bufferAhead,
    gate,
    get hls() {
      return hls;
    },
  };
})();
