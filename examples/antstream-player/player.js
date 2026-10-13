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

  // -- hls.js wiring --------------------------------------------------------

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

  const BaseLoader = Hls.DefaultConfig.loader;
  class SwarmLoader extends BaseLoader {
    load(context, config, callbacks) {
      let url = toContentUrl(context.url);
      const playlist = context.type === 'manifest' || context.type === 'level';
      if (playlist) {
        url = withCacheBust(url);
        const onSuccess = callbacks.onSuccess;
        callbacks = {
          ...callbacks,
          onSuccess: (response, stats, ctx, network) => {
            if (typeof response.data === 'string') response.data = guardPlaylist(response.data);
            onSuccess(response, stats, ctx, network);
          },
        };
      }
      context.url = url;
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
    enableWorker: true,
    lowLatencyMode: false,
    // The publisher keeps ~6 segments of 2 s in its live playlist: sit three
    // behind the edge and resync if more than five behind.
    liveSyncDurationCount: 3,
    liveMaxLatencyDurationCount: 5,
    maxBufferLength: 30,
    backBufferLength: 30,
    manifestLoadPolicy: patient(30000, 60000, 6),
    playlistLoadPolicy: patient(20000, 40000, 6),
    fragLoadPolicy: patient(30000, 90000, 6),
  };

  // -- state ----------------------------------------------------------------

  let hls = null;
  let channel = null;
  let ended = false;
  let everPlayed = false;
  let segmentsLoaded = 0;
  let lastFetchMs = null;
  let retryTimer = null;
  let statsTimer = null;
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
    clearInterval(endedPoll);
    endedPoll = null;
    if (hls) hls.destroy();
    hls = null;
  }

  function play(ch) {
    stop();
    channel = ch;
    ended = false;
    everPlayed = false;
    segmentsLoaded = 0;
    lastFetchMs = null;
    resetGuard();
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
        play(channel);
        return;
      }
      ended = !details.live;
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

  function onError(data) {
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

    if (everPlayed && playing && !ended) setStatus('Playing live');
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
  window.antstreamPlayer = { toContentUrl, parseChannel, contentUrl, get hls() { return hls; } };
})();
