/*
 * AntStream web player.
 *
 * A channel is a Swarm sequence feed named by its **owner** (an Ethereum
 * address) and a **topic text**, the Solar Punk stream format the
 * publisher writes with `"format": "solar-punk"`
 * (crates/ant-ffi/src/publisher.rs): the topic is `keccak256(utf8(text))`
 * (`topic_from_string`, bee-js `Topic.fromString`), and **every feed
 * update is the HLS playlist itself** (a v2 update wrapping the
 * playlist's root chunk). Segments and the `#EXT-X-MAP` are listed as
 * bare 64-hex references to raw bytes. A channel's topic can also be given
 * as the raw 32 topic bytes (64 hex, `0x` optional), for a channel whose
 * random topic no text names (the Android app's, an iOS channel created
 * before text topics). The iOS and Android apps broadcast on the topic
 * text `antstream`, one channel per account, so an owner alone names that
 * account's `antstream` channel; any other topic (the publisher's own
 * default `antstream/<channel>`, an older app's) has to be in the link.
 *
 * The page runs in two kinds of places:
 *
 *  - Freedom (`bzz://<player>/#<owner>/<topic>`): the feed is read through
 *    Freedom's Swarm provider, `window.swarm` (`swarm_readFeedEntry` for the
 *    head, `swarm_readSingleOwnerChunk` for a slot or a ladder marker, and
 *    `swarm_readFeedEntry` by index for a slot longer than one chunk), and
 *    media as `bzz://<ref>`, which Freedom's node answers with the raw
 *    bytes of a non-manifest reference. Permission-free provider reads are
 *    budgeted (120 requests / 512 KiB a minute), so they are paced
 *    ("provider budget" below).
 *  - An HTTP gateway (bee, ant; the page is `<root>/bzz/<player>/`,
 *    `<root>` possibly a sub-path): `GET <root>/feeds/{owner}/{topic}` for
 *    the head, `GET <root>/soc/{owner}/{identifier}` for a slot or marker,
 *    `GET <root>/bytes/<ref>` for media.
 *
 * The head lookup resolves "the newest update" by probing, which is slow
 * and can come back stale. So it is made once, when joining; after that
 * the player reads explicit slots (`identifier = keccak256(topic ‖
 * index_be8)`), the next few side by side, as Solar Punk's `feedFollow.ts`
 * does. A channel published with ladder time markers is joined from the
 * newest marker too, without waiting for the head.
 *
 * Playlist reads and segment fetches bypass hls.js's one-at-a-time loading
 * (see "playlist reads" and "segment fetches" below), and debug.js shows
 * what they did.
 */
(function () {
  'use strict';

  const K = window.antstreamKeccak;
  const OWNER = /^(?:0x)?([0-9a-f]{40})$/i;
  const REF = /^([0-9a-f]{64}(?:[0-9a-f]{64})?)$/i;
  // A topic given as its raw 32 bytes rather than as text.
  const RAW_TOPIC = /^(?:0x)?([0-9a-f]{64})$/i;
  const RECENT_KEY = 'antstream.recent.v2';
  // The topic text every AntStream app broadcast goes out on (the apps'
  // `ChannelAddress` `antstreamTopic`/`TOPIC`): an owner alone names it.
  const ANTSTREAM_TOPIC = 'antstream';

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
    infoTitle: $('info-title'),
    infoWhen: $('info-when'),
    infoDesc: $('info-desc'),
  };

  // -- addressing -----------------------------------------------------------

  /** Where the raw bytes of `ref` are read in this environment. */
  function contentUrl(ref) {
    return inBzz ? `bzz://${ref}` : `${gatewayRoot}/bytes/${ref}`;
  }

  /**
   * hls.js resolves the playlist's bare references against the playlist's
   * own URL (`PLAYLIST_URL`), so they reach the loaders as
   * `…/<64 hex>`; map that onto this environment's content URL.
   */
  function toContentUrl(url) {
    let u;
    try {
      u = new URL(url);
    } catch (_) {
      return url;
    }
    const last = u.pathname.split('/').pop();
    return REF.test(last) ? contentUrl(last.toLowerCase()) : url;
  }

  // The URL hls.js is given for the playlist. Nothing is fetched from it
  // (the playlist loader below reads the feed); it only anchors the bare
  // references in the playlist for `toContentUrl`.
  const PLAYLIST_URL = new URL('channel.m3u8', location.href.split('#')[0]).toString();

  /**
   * A channel from what the viewer pasted or the page's hash:
   * `<owner>/<topic>` (the owner `0x` + 40 hex, the topic URL-encoded
   * text), `swarm://<owner>/<topic>` (Solar Punk's form), or a player link
   * carrying either after its `#`. A topic of 64 hex digits (`0x`
   * optional) is the raw topic itself, not text to hash. An owner alone
   * (`<owner>` or `<owner>/`, not `swarm://<owner>/`) is that account's
   * AntStream channel, topic `antstream`, as the apps'
   * `ChannelAddress.parse` reads it.
   *
   * Text with whitespace in it (the apps' Share text, a message it was
   * pasted into) is searched for the channel instead: the first
   * `swarm://<owner>/<topic>`, `#0x<owner>/<topic>` or `0x<owner>/<topic>`
   * in it, the topic ending at whitespace (and at `?`/`#`/`/` in a
   * `swarm://` URI). A text input drops the Share text's line breaks,
   * gluing each line's link to the next line's label
   * (`…/#0x<owner>/antstreamAny browser: …`): the same channel further on,
   * whose topic the first one's starts with, is the one meant (the Share
   * text's last line, `Channel: 0x<owner>/antstream`, is never glued),
   * unless the text after that one goes on with the first topic's rest:
   * then it is the same topic cut at a space. Sentence punctuation after a
   * channel in text (`… 0x<owner>/antstream.`) is not part of its topic,
   * unless another mention of the same channel ends in it too (the Share
   * text of topic `show.`).
   */
  function parseChannel(input) {
    const s = (input || '').trim();
    if (!s) return null;
    return /\s/.test(s) ? findChannel(s) : parseChannelToken(s);
  }

  /** One channel string, the whole input: the form a link's hash carries. */
  function parseChannelToken(input) {
    let s = input;
    const hash = s.indexOf('#');
    if (hash >= 0) s = s.slice(hash + 1);
    let owner;
    let rest;
    const uri = /^swarm:\/\/([^/?#]+)\/([^/?#]+)/i.exec(s);
    if (uri) {
      // parseSwarmUri: the topic is the piece up to the next `/`.
      [, owner, rest] = uri;
    } else if (/^swarm:/i.test(s)) {
      // A truncated `swarm://<owner>/` names no topic, so no channel.
      return null;
    } else {
      const slash = s.indexOf('/');
      owner = slash < 0 ? s : s.slice(0, slash);
      rest = slash < 0 ? '' : s.slice(slash + 1);
    }
    const o = OWNER.exec(owner);
    if (!o) return null;
    return channelOf(o[1], topicOf(rest));
  }

  // A channel inside text, by where it starts: a `swarm://` URI (its topic
  // ends at `/`, `?` or `#`), or `0x<owner>` not inside a longer word or
  // hex string, its topic after a `/` (none: the owner alone).
  const CHANNEL_IN_TEXT = /swarm:\/\/((?:0x)?[0-9a-f]{40})\/([^\s/?#]+)|(?<![0-9a-z])0x([0-9a-f]{40})(?![0-9a-z])(?:\/(\S*))?/gi;

  /** The channel in free text, see `parseChannel`. */
  function findChannel(text) {
    const matches = [...text.matchAll(CHANNEL_IN_TEXT)].map((m) => {
      const uri = Boolean(m[1]);
      return {
        m,
        uri,
        owner: (uri ? m[1].replace(/^0x/i, '') : m[3]).toLowerCase(),
        token: uri ? m[2] : m[4] || '',
      };
    });
    const found = [];
    for (const { m, uri, owner, token } of matches) {
      // Sentence punctuation ends one word of a message, not every
      // mention of a channel. The apps' Share text names its channel up
      // to four times (two links, the Solar Punk URI, the Channel line),
      // and `encodeURIComponent` leaves `.!'()*~` as they are, so a topic
      // like `show.` ends every one of them in its `.`: when another
      // mention of the same owner has this one's text whole (or glued to
      // the next line's label, `show.Any`), the end is the topic's own.
      const repeated = matches.some(
        (o) => o.m !== m && o.owner === owner && o.token.startsWith(token),
      );
      const kept = repeated ? token : trimSentenceEnd(token);
      found.push({
        owner,
        topic: topicOf(kept),
        uri,
        // Where the topic, as kept, ends in the text.
        end: m.index + m[0].length - (token.length - kept.length),
      });
    }
    if (!found.length) return null;
    const first = found[0];
    let topic = first.topic;
    // The first one glued to the label after it: the same channel further
    // on whose topic it starts with. Not a `swarm://` URI's, whose topic
    // stops at a `/` the topic text can have. A later one only stands for
    // a shorter topic if the text after it doesn't go on with the first
    // topic's rest: `0x<owner>/my show` (the Channel line of topic
    // `my show`) is cut at its space, not a different, shorter channel.
    for (const f of found.slice(1)) {
      if (f.owner !== first.owner || f.uri || !f.topic || !topic.startsWith(f.topic)) continue;
      const rest = first.topic.slice(f.topic.length);
      const after = text.slice(f.end);
      let n = 0;
      while (n < rest.length && n < after.length && rest[n] === after[n]) n++;
      const meant = f.topic + rest.slice(0, n);
      if (meant.length < topic.length) topic = meant;
    }
    return channelOf(first.owner, topic);
  }

  // Punctuation that ends a sentence or closes a quote rather than a
  // topic, when a channel is the last word of a message
  // (`… 0x<owner>/antstream.`). Brackets only when unbalanced in the topic.
  const SENTENCE_END = /[.,;:!?'"\u2019\u201d\u00bb\u2026>)\]}]$/;
  const OPENER = { ')': '(', ']': '[', '}': '{' };

  /** `token` (a topic as it stands in text) without the sentence's end. */
  function trimSentenceEnd(token) {
    let t = token;
    while (SENTENCE_END.test(t)) {
      const last = t[t.length - 1];
      const open = OPENER[last];
      if (open && t.split(open).length > t.split(last).length - 1) break;
      t = t.slice(0, -1);
    }
    return t;
  }

  /**
   * The topic text a link carries after the owner, URL-encoded or not.
   * Trailing slashes go, as in the apps: `0x<owner>/` is the owner alone.
   */
  function topicOf(rest) {
    const bare = rest.replace(/\/+$/, '');
    try {
      return decodeURIComponent(bare);
    } catch (_) {
      return bare; // not URL-encoded: the text as it is
    }
  }

  /** `topic`: decoded; empty is the owner alone. */
  function channelOf(owner, topic) {
    const text = topic || ANTSTREAM_TOPIC;
    const raw = RAW_TOPIC.exec(text);
    return makeChannel(owner.toLowerCase(), text, raw ? raw[1].toLowerCase() : null);
  }

  /** `rawHex`: the topic's raw bytes as hex, when `topic` is not text. */
  function makeChannel(owner, topic, rawHex) {
    const topicHex = rawHex || K.topicFromString(topic);
    return {
      owner,
      // What is shown: the text, or `0x<hex>` for a raw topic.
      topic: rawHex ? `0x${rawHex}` : topic,
      topicHex,
      // The ladder this channel's time markers are written for, as the
      // publisher names it (`ladder_group_text`).
      groupHex: K.topicFromString(`antstream-ladder-${topicHex}`),
      // The hash and the recent list's form, also what is shown.
      key: `0x${owner}/${rawHex ? `0x${rawHex}` : encodeURIComponent(topic)}`,
    };
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
      if (this.session && this.session.channel === ch.key) {
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
        channel: ch.key,
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
  // The head lookup resolves "the newest feed update" by probing indices
  // with a short per-probe deadline, so it can come back with an *older*
  // playlist than one already read (an update still propagating loses the
  // race), and side-by-side slot reads can finish out of order. Within a
  // broadcast the playlist only moves forward, so an older one is replaced
  // by the newest seen. The publisher names each broadcast in its
  // playlists (`#ANTSTREAM-BROADCAST:<id>`): an id never seen before is a
  // new broadcast on the channel, an id seen earlier is a stale read.

  const guard = { text: null, key: null, id: null, ids: new Set(), newBroadcast: null };

  function resetGuard(ids) {
    guard.text = null;
    guard.key = null;
    guard.id = null;
    guard.ids = new Set(ids || []);
    guard.newBroadcast = null;
  }

  function playlistInfo(text) {
    const seq = Number((/#EXT-X-MEDIA-SEQUENCE:(\d+)/.exec(text) || [])[1] || 0);
    const segments = (text.match(/#EXTINF/g) || []).length;
    const ended = text.includes('#EXT-X-ENDLIST');
    const recording = ended && text.includes('#EXT-X-PLAYLIST-TYPE:VOD');
    const id = (/^#ANTSTREAM-BROADCAST:(\S+)/m.exec(text) || [])[1] || null;
    // Order within one broadcast: the recording first (the publisher's
    // final update, the whole broadcast from its first segment, so its
    // window start is *behind* every live playlist's, yet nothing comes
    // after it; a viewer whose reload skipped the brief closing window
    // must still take it as the end), then later window start, then more
    // segments, then closed.
    return { key: [recording ? 1 : 0, seq, segments, ended ? 1 : 0], id, ended };
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

  /**
   * The playlist to keep after reading `text` (at feed index `index`):
   * `text` itself, the newer one already held, or null when nothing is
   * held yet and `text` belongs to a broadcast already played (a stale
   * read after switching to a new broadcast).
   */
  function guardPlaylist(text, index) {
    const info = playlistInfo(text);
    if (info.id && info.id !== guard.id && guard.ids.has(info.id)) return guard.text;
    if (guard.text === null) return accept(text, info);
    if (info.id && info.id !== guard.id) {
      // A broadcast never seen before: the channel starts over on it.
      guard.newBroadcast = { id: info.id, index };
      return accept(text, info);
    }
    if (newer(info.key, guard.key) || info.key.join() === guard.key.join()) return accept(text, info);
    return guard.text;
  }

  // -- reading the feed -----------------------------------------------------
  //
  // Three reads, each answered the same way (`{ status, text, index }`,
  // status 200, 404 for "not written yet", 429 for a provider rate limit,
  // else an error): the feed head, one feed slot, and one ladder time
  // marker. Over HTTP they are gateway requests; in Freedom they go
  // through `window.swarm`.

  const READ_TIMEOUT_MS = 30000;
  const VIA = inBzz ? 'window.swarm' : 'HTTP';
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  function withTimeout(promise, ms, what) {
    let timer;
    return Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(Object.assign(new Error(`${what} timed out`), { timeout: true })), ms);
      }),
    ]).finally(() => clearTimeout(timer));
  }

  function fromBase64(b64) {
    const bin = atob(b64);
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  }

  const textOf = (bytes) => new TextDecoder().decode(bytes);

  // -- provider budget --
  //
  // Freedom answers permission-free `window.swarm` reads from a budget of
  // 120 requests and 512 KiB per origin per 60 s (a fixed window from the
  // first read). The player never asks to connect, so it keeps under that
  // on its own, with room to spare (a rolling minute, which is stricter),
  // and a read that is refused anyway (`rate_limited`) pauses every read
  // for a while, longer each time. Media is read as `bzz://` URLs, which
  // the budget does not count.

  const BUDGET_WINDOW_MS = 60000;
  const BUDGET_REQUESTS = 100;
  const BUDGET_BYTES = 400 * 1024;
  const budget = { reads: [], until: 0, backoffMs: 0, refused: 0 };

  function budgetFree(now, expected) {
    budget.reads = budget.reads.filter((r) => now - r.at < BUDGET_WINDOW_MS);
    if (now < budget.until) return false;
    const bytes = budget.reads.reduce((n, r) => n + r.bytes, 0);
    if (budget.reads.length >= BUDGET_REQUESTS) return false;
    // A read of known size (a recording) needs room for all of it: Freedom
    // charges the bytes after the read, and refuses it if they don't fit.
    return expected > 0 ? bytes + expected <= BUDGET_BYTES : bytes < BUDGET_BYTES;
  }

  /**
   * Wait for room in the budget; the entry to charge the read's bytes to.
   * `expected`: the bytes the read will bring, when known, held from the
   * start so reads started meanwhile can't take that room.
   * `wanted`: whether the read is still wanted (the viewer hasn't moved
   * to another channel); null, with nothing charged, once it isn't, so a
   * stale read never reaches Freedom and spends the new channel's window.
   */
  async function takeBudget(expected = 0, wanted = () => true) {
    const asked = Date.now();
    for (;;) {
      if (!wanted()) return null;
      if (budgetFree(Date.now(), expected)) break;
      await sleep(250);
    }
    const entry = { at: Date.now(), bytes: expected, waitedMs: Date.now() - asked };
    budget.reads.push(entry);
    return entry;
  }

  function refusedForRate(err) {
    budget.refused += 1;
    // Reads already under way when the first refusal came are refused
    // too: one pause, not one longer pause each.
    if (Date.now() < budget.until) return;
    budget.backoffMs = Math.min(Math.max(budget.backoffMs * 2, 5000), BUDGET_WINDOW_MS);
    const wait = (err.data && err.data.retryAfterMs) || budget.backoffMs;
    budget.until = Math.max(budget.until, Date.now() + wait);
    log.marker(`window.swarm rate limited: feed reads paused for ${(wait / 1000).toFixed(0)} s`);
  }

  const NOT_FOUND = new Set(['feed_empty', 'entry_not_found', 'chunk_not_found', 'feed_not_found']);

  /** One `window.swarm` call, its refusal turned into an answer. */
  async function provider(method, params, decode, expected, wanted) {
    const swarm = window.swarm;
    if (!swarm || typeof swarm.request !== 'function') {
      return { status: 'error', note: 'window.swarm is not available (open the channel in Freedom)' };
    }
    const entry = await takeBudget(expected, wanted);
    if (!entry) return { status: 'cancelled', note: 'not sent: the channel changed while it waited for the read budget' };
    const waited = entry.waitedMs > 500 ? `waited ${(entry.waitedMs / 1000).toFixed(1)} s for the read budget` : '';
    try {
      const result = await withTimeout(swarm.request({ method, params }), READ_TIMEOUT_MS, method);
      const answer = await decode(result);
      entry.bytes = Math.max(answer.size || 0, entry.bytes);
      budget.backoffMs = 0;
      return { ...answer, note: waited };
    } catch (err) {
      const reason = err && err.data && err.data.reason;
      if (reason === 'rate_limited') {
        refusedForRate(err);
        return { status: 429, note: 'rate_limited' };
      }
      if (NOT_FOUND.has(reason)) return { status: 404, note: [reason, waited].filter(Boolean).join(' · ') };
      if (err && err.timeout) return { status: 'timeout', note: err.message };
      return { status: 'error', note: `${err && err.code !== undefined ? `${err.code} ` : ''}${(err && err.message) || err}` };
    }
  }

  /**
   * A single-owner chunk's content when the wrapped chunk is all of it (a
   * live playlist, a marker), else `{ length }`, the whole body's size.
   * The span's top byte can carry the erasure-coding level of a larger
   * body (Freedom returns such a span as a BigInt); the 7 bytes under it
   * are the body's length.
   */
  function socBody(result) {
    const payload = fromBase64(result.data);
    const raw = BigInt(result.span);
    const length = raw & ((1n << 56n) - 1n);
    if (raw >> 56n !== 0n || length > BigInt(payload.length)) return { length: Number(length) };
    return { bytes: payload.subarray(0, Number(length)) };
  }

  /** One gateway GET: 404 is "not written yet", a body shorter than declared an error. */
  async function gateway(path, withIndex) {
    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), READ_TIMEOUT_MS);
    try {
      const res = await fetch(`${gatewayRoot}/${path}`, { cache: 'no-store', signal: ctl.signal });
      if (!res.ok) {
        const body = (await res.text().catch(() => '')).slice(0, 160);
        return { status: res.status, note: body };
      }
      const buf = await res.arrayBuffer();
      const declared = res.headers.get('content-length');
      if (declared !== null && !res.headers.get('content-encoding') && Number(declared) !== buf.byteLength) {
        return { status: 'error', note: `short body: ${buf.byteLength} of ${declared} bytes` };
      }
      let index = null;
      if (withIndex) {
        // 8 bytes big-endian, as 16 hex digits: `0000000000000022` is 34.
        const raw = (res.headers.get('swarm-feed-index') || '').trim();
        if (/^[0-9a-f]+$/i.test(raw)) index = parseInt(raw, 16);
      }
      return { status: 200, text: textOf(new Uint8Array(buf)), index };
    } catch (err) {
      return { status: err.name === 'AbortError' ? 'timeout' : 'error', note: err.message };
    } finally {
      clearTimeout(timer);
    }
  }

  // `wanted` (below): see takeBudget.
  function readHead(ch, wanted) {
    if (!inBzz) return gateway(`feeds/${ch.owner}/${ch.topicHex}`, true);
    return provider('swarm_readFeedEntry', { topic: ch.topicHex, owner: `0x${ch.owner}` }, (r) => {
      const bytes = fromBase64(r.data);
      return { status: 200, text: textOf(bytes), index: typeof r.index === 'number' ? r.index : null, size: bytes.length };
    }, 0, wanted);
  }

  function readSoc(owner, identifier, wanted) {
    if (!inBzz) return gateway(`soc/${owner}/${identifier}`, false);
    return provider('swarm_readSingleOwnerChunk', { owner: `0x${owner}`, identifier }, (r) => {
      const { bytes, length } = socBody(r);
      return bytes ? { status: 200, text: textOf(bytes), size: bytes.length } : { status: 'long', size: 0, length };
    }, 0, wanted);
  }

  const kib = (n) => `${Math.ceil(n / 1024)} KiB`;

  async function readSlot(ch, index, wanted) {
    const a = await readSoc(ch.owner, K.feedIdentifier(ch.topicHex, index), wanted);
    if (a.status !== 'long') return { ...a, index };
    // Longer than one chunk (a recording): the node joins it, a read whose
    // bytes count against the budget. Only the broadcast's last update.
    if (a.length > BUDGET_BYTES) {
      // More than a minute's budget: Freedom would refuse it on every try
      // (and charge the refused bytes, so other reads stop too). The slot
      // is written, though: the feed goes on after it.
      return {
        status: 'too-long',
        index,
        length: a.length,
        note: `longer than one chunk (${kib(a.length)}): more than Freedom lets a page read without a connection grant, not read`,
      };
    }
    const whole = await provider('swarm_readFeedEntry', { topic: ch.topicHex, owner: `0x${ch.owner}`, index }, (r) => {
      const bytes = fromBase64(r.data);
      return { status: 200, text: textOf(bytes), size: bytes.length };
    }, a.length, wanted);
    return { ...whole, index, note: [`longer than one chunk (${kib(a.length)}): read whole`, whole.note].filter(Boolean).join(' · ') };
  }

  const MARKER_PERIOD_MS = 10000;

  /**
   * The feed index a ladder time marker names for this channel, or null:
   * Solar Punk's `parseLadderMarker` (v2), and the publisher's
   * `encode_ladder_marker`.
   */
  function markerIndex(text, ch, period) {
    let m;
    try {
      m = JSON.parse(text);
    } catch (_) {
      return null;
    }
    const whole = (v) => Number.isSafeInteger(v) && v >= 0;
    if (!m || m.v !== 2 || m.period !== period || !whole(m.writtenAt)) return null;
    if (Math.floor(m.writtenAt / MARKER_PERIOD_MS) !== period || !whole(m.segmentMs) || !m.segmentMs) return null;
    const index = m.rungs && m.rungs[ch.topicHex];
    return whole(index) ? index : null;
  }

  // -- the catalog ------------------------------------------------------------
  //
  // The channel owner's catalog feed (topic `antstream-catalog`, owner the
  // channel's owner; crates/ant-ffi/src/catalog.rs) says what the channel
  // is: a title, a description, a thumbnail and, for an announced
  // broadcast, when it starts. Every update is the whole catalog, a JSON
  // array with an entry per channel, so only the head is read: once when
  // the channel opens, then every CATALOG_EVERY_MS while it waits or is
  // live (the publisher rewrites the entry when its info changes). It is
  // extra, never in playback's way: it starts beside the first playlist
  // read and nothing waits for it, in Freedom it is only sent while the
  // read budget has CATALOG_SPARE_* left for the playlist reads (else it
  // is put off), and a read the channel changed under is dropped. A
  // missing or unreadable catalog shows nothing extra.

  const CATALOG_TOPIC_HEX = K.topicFromString('antstream-catalog');
  const CATALOG_EVERY_MS = 60000;
  const CATALOG_DEFER_MS = 10000;
  const CATALOG_SPARE_REQUESTS = 20;
  const CATALOG_SPARE_BYTES = 64 * 1024;
  // More than any one channel's catalog should be: not parsed.
  const CATALOG_MAX_BYTES = 256 * 1024;

  // `entry`: the channel's entry, cleaned (see catalogEntry), or null.
  const catalog = { gen: 0, key: null, entry: null, timer: null, reads: 0, lastAt: 0, last: '' };

  const str = (v) => (typeof v === 'string' ? v : '');

  /**
   * This channel's entry in a catalog body, every field checked (the
   * catalog is the owner's own JSON, and may be anything): the entry whose
   * owner is the channel's and whose topic text hashes to the channel's
   * topic (`Topic.fromString`, as Solar Punk's readers do; a 64-hex topic
   * is text too). Of several, the newest `timestamp` wins, the later one
   * on a tie: the same rule as Android's `Catalog.select` and iOS's
   * `CatalogEntry.find`. Null when there is none.
   */
  function catalogEntry(text, ch) {
    let list;
    try {
      list = JSON.parse(text);
    } catch (_) {
      return null;
    }
    if (!Array.isArray(list)) return null;
    let best = null;
    for (const e of list) {
      if (!e || typeof e !== 'object') continue;
      const owner = OWNER.exec(str(e.owner));
      if (!owner || owner[1].toLowerCase() !== ch.owner) continue;
      const topic = str(e.topic);
      if (!topic) continue;
      if (K.topicFromString(topic) !== ch.topicHex) continue;
      const at = Number.isFinite(e.timestamp) ? e.timestamp : 0;
      if (best && at < best.at) continue;
      const start = typeof e.scheduledStartTime === 'string' || Number.isFinite(e.scheduledStartTime)
        ? new Date(e.scheduledStartTime) : null;
      best = {
        at,
        title: str(e.title).trim(),
        description: str(e.description).trim(),
        state: str(e.state),
        thumbnail: /^[0-9a-f]{64}$/i.test(str(e.thumbnail)) ? e.thumbnail.toLowerCase() : null,
        start: start && !Number.isNaN(start.getTime()) ? start : null,
      };
    }
    return best;
  }

  /** Where a thumbnail (a one-file /bzz manifest) is shown from here. */
  function thumbnailUrl(ref) {
    return inBzz ? `bzz://${ref}/` : `${gatewayRoot}/bzz/${ref}/`;
  }

  /** Room in Freedom's budget for an extra read, playback's reads kept clear of it. */
  function catalogRoom(now) {
    if (!inBzz) return true;
    if (!budgetFree(now, 0)) return false;
    const bytes = budget.reads.reduce((n, r) => n + r.bytes, 0);
    return budget.reads.length <= BUDGET_REQUESTS - CATALOG_SPARE_REQUESTS && bytes <= BUDGET_BYTES - CATALOG_SPARE_BYTES;
  }

  function scheduleCatalog(ms) {
    clearTimeout(catalog.timer);
    catalog.timer = setTimeout(readCatalog, ms);
  }

  /**
   * Open the catalog for channel `ch`: a new channel drops what the last
   * one showed (and any of its reads still under way) and reads at once;
   * the same channel (a retry, a new broadcast) keeps it, and `fresh`
   * reads it again now.
   */
  function openCatalog(ch, fresh) {
    if (catalog.key !== ch.key) {
      catalog.gen += 1;
      catalog.key = ch.key;
      catalog.entry = null;
      catalog.reads = 0;
      catalog.lastAt = 0;
      catalog.last = '';
      renderCatalog();
      scheduleCatalog(0);
    } else if (fresh) {
      scheduleCatalog(0);
    }
  }

  async function readCatalog() {
    const ch = channel;
    if (!ch || ch.key !== catalog.key) return;
    // A broadcast that ended plays its recording: nothing left to announce.
    // A new one on the channel reads it again (openCatalog `fresh`).
    if (ended) return scheduleCatalog(CATALOG_EVERY_MS);
    if (!catalogRoom(Date.now())) return scheduleCatalog(CATALOG_DEFER_MS);
    const gen = catalog.gen;
    const wanted = () => gen === catalog.gen;
    catalog.reads += 1;
    const row = log.add({
      kind: 'playlist',
      read: 'catalog',
      index: null,
      period: null,
      via: VIA,
      start: performance.now(),
      ms: null,
      status: 'loading',
      seq: null,
      segments: null,
      note: '',
    });
    let a;
    try {
      a = await readHead({ owner: ch.owner, topicHex: CATALOG_TOPIC_HEX }, wanted);
    } catch (err) {
      a = { status: 'error', note: err.message };
    }
    row.ms = performance.now() - row.start;
    row.status = a.status;
    row.note = a.note || '';
    if (typeof a.index === 'number') row.index = a.index;
    log.touch();
    if (!wanted()) return;
    catalog.lastAt = Date.now();
    if (a.status === 200) {
      const entry = a.text.length > CATALOG_MAX_BYTES ? null : catalogEntry(a.text, ch);
      catalog.entry = entry;
      catalog.last = entry ? `${entry.state || 'no state'}${entry.title ? ` · ${entry.title}` : ''}` : 'no entry for this channel';
      note(row, entry ? `catalog: ${catalog.last}` : 'catalog: no readable entry for this channel');
      renderCatalog();
    } else {
      // Not written (404), or not read this time: keep what is shown.
      catalog.last = a.status === 404 ? 'no catalog' : `not read (${a.status})`;
      if (a.status === 404) note(row, 'no catalog');
    }
    scheduleCatalog(CATALOG_EVERY_MS);
  }

  /**
   * "Starts at …" for an announced broadcast while the channel waits for
   * it; empty otherwise.
   */
  function startsAt() {
    const e = catalog.entry;
    if (!e || e.state !== 'scheduled' || !e.start || everPlayed) return '';
    const when = e.start.toLocaleString(undefined, {
      weekday: 'short', day: 'numeric', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit', timeZoneName: 'short',
    });
    return e.start.getTime() > Date.now() ? `Starts at ${when}` : `Was scheduled to start at ${when}`;
  }

  /** Show the catalog entry (or nothing): heading, description, poster, start. */
  function renderCatalog() {
    const e = catalog.entry;
    const title = e && e.title ? e.title : '';
    els.infoTitle.textContent = title;
    els.infoTitle.hidden = !title;
    els.infoDesc.textContent = e ? e.description : '';
    els.infoDesc.hidden = !(e && e.description);
    const when = startsAt();
    els.infoWhen.textContent = when;
    els.infoWhen.hidden = !when;
    document.title = title ? `${title} · AntStream Player` : 'AntStream Player';
    const poster = e && e.thumbnail ? thumbnailUrl(e.thumbnail) : '';
    if (poster) {
      if (video.getAttribute('poster') !== poster) video.setAttribute('poster', poster);
    } else {
      video.removeAttribute('poster');
    }
  }

  // -- playlist reads -------------------------------------------------------
  //
  // **Joining.** The head lookup takes seconds (it probes the feed for its
  // newest index) and can come back stale. So the player asks it once, and
  // at the same time reads the ladder time markers of this ten-second
  // period and the last one: a channel published with markers names its
  // newest index there, and a slot read from it is a single chunk away.
  // Whichever answers first gives the feed index to follow from.
  //
  // **Following.** From there, every read names an explicit slot (the one
  // after the newest read, and the next ones side by side), which a node
  // answers at once, with the update or with "not written yet". While
  // live, a new read starts every POLL_MS, up to `settings.polls` at once
  // on distinct slots. While catching up (from the join, which can land a
  // few updates behind), each slot found starts the next read at once,
  // until one comes back "not written yet". hls.js's reload is answered with the newest playlist any
  // of them returned, or, once one finished, with the one it already has.
  // After the broadcast ended, the slot after its last update is read now
  // and then: the channel's next broadcast continues the same feed there.

  const POLL_MS = 1500;
  // A channel whose playlist stopped moving (broadcaster gone, or paused)
  // is read less often.
  const IDLE_AFTER_MS = 20000;
  const IDLE_POLL_MS = 5000;
  const ENDED_POLL_MS = 5000;
  // A slot found this long after the last newer playlist (reads paused, a
  // slow node, the broadcaster back from a break) may be one of several
  // waiting: read on at once. This much later, ask the ladder markers too,
  // which can skip straight to the newest.
  const CATCH_UP_AFTER_MS = 5000;
  const REJOIN_AFTER_MS = 30000;
  // In Freedom, a head read the budget refuses may be the head itself being
  // over it: an ended broadcast's recording, which the provider joins and
  // charges in full before refusing. It may as well be a head that fits,
  // refused only because the origin's window was already partly spent (by
  // another tab on the same origin, or this page before a reload: the
  // player can't see those reads). So the first refusal only waits out the
  // window and reads the head once more, in a fresh one; a second refusal
  // in a row means the head is over the budget, and re-reading it on every
  // retry would spend a whole window each time, so for HEAD_REFUSED_HOLD_MS
  // the join only asks the ladder markers, which a new broadcast on the
  // channel writes from its start.
  const HEAD_RETRY_AFTER_MS = BUDGET_WINDOW_MS + 5000;
  const HEAD_REFUSED_HOLD_MS = 5 * 60000;
  // "In a row" means the retry the first refusal scheduled: a second
  // refusal counts only if it comes within this long of the first, i.e.
  // the read made once HEAD_RETRY_AFTER_MS is up (give or take a poll). A
  // refusal any later (the channel reopened minutes after, or the read
  // after a 5 min hold ran out) starts over at one strike, with its own
  // retry in a fresh window.
  const HEAD_STRIKE_WITHIN_MS = HEAD_RETRY_AFTER_MS + BUDGET_WINDOW_MS;
  // { key, at, strikes } of the channel whose head read was refused
  // (`strikes` refusals in a row); kept across `play()` retries, which
  // start the join over, and cleared by a head read that is answered or a
  // live broadcast found on the channel.
  let headRefused = null;

  /**
   * The refusal state after the head read of channel `key` is refused at
   * `now`, given the one before (`prev`, any channel's, or null).
   */
  function headRefusal(prev, key, now) {
    if (!prev || prev.key !== key) return { key, at: now, strikes: 1 };
    const hold = prev.strikes >= 2 ? HEAD_REFUSED_HOLD_MS : HEAD_RETRY_AFTER_MS;
    // A refusal while already held (a read that started before the first
    // one came back) is the same strike.
    if (now - prev.at < hold) return { key, at: prev.at, strikes: prev.strikes };
    if (prev.strikes === 1 && now - prev.at < HEAD_STRIKE_WITHIN_MS) return { key, at: now, strikes: 2 };
    return { key, at: now, strikes: 1 };
  }

  /** The head read was refused twice in a row: it is over the budget. */
  const headTooLarge = (ch) => !!headRefused && headRefused.key === ch.key && headRefused.strikes >= 2;

  function headRefusedError(ch) {
    return headTooLarge(ch)
      ? { code: 429, message: "the last broadcast's recording is over Freedom's read budget; open the channel through an HTTP gateway to play it" }
      : { code: 429, message: "Freedom's read budget refused the channel's head (another tab or an earlier load may have used it); reading it again in the next budget window" };
  }

  const HEAD_REFUSED_OVERLAY = 'This channel’s last broadcast is too large to read here. Watching for a new one…';

  /** Skip the head read for now: refused once and waiting for a fresh window, or over the budget. */
  function headHeld(ch) {
    if (!headRefused || headRefused.key !== ch.key) return false;
    const hold = headRefused.strikes >= 2 ? HEAD_REFUSED_HOLD_MS : HEAD_RETRY_AFTER_MS;
    return Date.now() - headRefused.at < hold;
  }

  /** The channel's head is readable again (answered, or a live broadcast is on). */
  function clearHeadRefused(ch) {
    if (headRefused && ch && headRefused.key === ch.key) headRefused = null;
  }

  const feed = {
    gen: 0,
    ch: null,
    // Slots after `cursor` are the ones still to read.
    cursor: -1,
    // The newest index read with a playlist, for the debug panel.
    index: null,
    joinedVia: null,
    joinPending: 0,
    slots: new Set(),
    inflight: 0,
    lastStart: 0,
    lastNewer: 0,
    waiters: [],
    delivered: null,
    lastError: null,
  };

  /** Start reading `ch`, from the feed index `from` when it is known. */
  function resetReads(ch, from) {
    feed.gen += 1;
    feed.ch = ch;
    const known = typeof from === 'number';
    feed.cursor = known ? from - 1 : -1;
    feed.index = null;
    feed.joinedVia = known ? 'the new broadcast' : null;
    feed.joinPending = 0;
    feed.slots = new Set();
    feed.inflight = 0;
    feed.lastStart = 0;
    feed.lastNewer = Date.now();
    feed.waiters = [];
    feed.delivered = null;
    feed.lastError = ch && headHeld(ch) ? headRefusedError(ch) : null;
  }

  /**
   * Start the next read: the join, or the next slot not being read.
   * `chase`: catching up (joining, or right after a read that caught up),
   * so a slot found sends the next read straight away.
   */
  function readPlaylist(chase) {
    if (!feed.ch) return;
    if (feed.joinedVia === null) {
      if (feed.joinPending) return;
      const period = Math.floor(Date.now() / MARKER_PERIOD_MS);
      if (!headHeld(feed.ch)) readOne('head');
      readOne('marker', period);
      readOne('marker', period - 1);
      return;
    }
    const ahead = Math.max(settings.polls, 1);
    for (let i = feed.cursor + 1; i <= feed.cursor + ahead; i++) {
      if (!feed.slots.has(i)) {
        readOne('slot', i, chase);
        return;
      }
    }
  }

  async function readOne(kind, arg, chase) {
    const gen = feed.gen;
    const ch = feed.ch;
    feed.inflight += 1;
    feed.lastStart = Date.now();
    if (kind === 'slot') feed.slots.add(arg);
    else feed.joinPending += 1;
    const row = log.add({
      kind: 'playlist',
      read: kind,
      index: kind === 'slot' ? arg : null,
      period: kind === 'marker' ? arg : null,
      via: VIA,
      start: performance.now(),
      ms: null,
      status: 'loading',
      seq: null,
      segments: null,
      note: '',
    });
    // A read still waiting for the read budget when the channel changes is
    // dropped there, not sent: Freedom would charge it to the origin's
    // window, which the new channel's head read then may not fit in.
    const wanted = () => gen === feed.gen;
    let a;
    try {
      a = kind === 'head' ? await readHead(ch, wanted) : kind === 'slot' ? await readSlot(ch, arg, wanted) : await readSoc(ch.owner, K.ladderMarkerIdentifier(ch.groupHex, arg), wanted);
    } catch (err) {
      a = { status: 'error', note: err.message };
    }
    row.ms = performance.now() - row.start;
    row.status = a.status;
    row.note = a.note || '';
    log.touch();
    if (gen !== feed.gen) return;
    feed.inflight -= 1;
    if (kind === 'slot') feed.slots.delete(arg);
    else feed.joinPending -= 1;
    if (kind === 'marker') onMarker(a, arg, row);
    else onPlaylist(kind, a, row, chase);
    deliver(kind !== 'marker');
  }

  const note = (row, text) => {
    row.note = row.note ? `${text} · ${row.note}` : text;
  };

  function onMarker(a, period, row) {
    if (a.status === 404) return note(row, 'no marker this period');
    if (a.status !== 200) return;
    const index = markerIndex(a.text, feed.ch, period);
    if (index === null) return note(row, 'not a v2 marker for this channel');
    row.index = index;
    if (index - 1 > feed.cursor) {
      // Read from the slot it names on.
      feed.cursor = index - 1;
      if (feed.joinedVia === null) feed.joinedVia = 'marker';
      note(row, `names index ${index}: reading from there`);
      readPlaylist(true);
    } else {
      note(row, `names index ${index}: already past it`);
    }
  }

  function onPlaylist(kind, a, row, chase) {
    if (kind === 'head' && (a.status === 200 || a.status === 404 || a.status === 'too-long')) clearHeadRefused(feed.ch);
    if (a.status === 'too-long') {
      // A recording too large to read here. The next broadcast continues
      // the feed after it (#173), so step past it rather than read it
      // again and again.
      feed.lastError = {
        code: 0,
        message: `this broadcast's recording (${kib(a.length)}) is larger than Freedom lets a page read without a connection grant; open the channel through an HTTP gateway to play it`,
      };
      if (a.index > feed.cursor) {
        feed.cursor = a.index;
        if (feed.index === null || a.index > feed.index) feed.index = a.index;
        if (feed.joinedVia === null) feed.joinedVia = kind;
        log.marker(`slot ${a.index} is a ${kib(a.length)} recording, over the read budget: not read, following on from slot ${a.index + 1}`);
        if (chase) readPlaylist(true);
      }
      return;
    }
    if (a.status !== 200) {
      if (kind === 'slot' && a.status === 404) note(row, 'not written yet');
      else if (kind === 'head' && a.status === 404) note(row, 'the feed has no updates yet');
      if (kind === 'head' && a.status === 429 && inBzz) {
        const wasHeld = headHeld(feed.ch);
        headRefused = headRefusal(headRefused, feed.ch.key, Date.now());
        const strikes = headRefused.strikes;
        if (!wasHeld) {
          log.marker(strikes >= 2
            ? `head read refused by the read budget again: it is over the budget, joining through the ladder markers only for ${HEAD_REFUSED_HOLD_MS / 60000} min`
            : `head read refused by the read budget: reading it again in a fresh window in ${HEAD_RETRY_AFTER_MS / 1000} s, joining through the ladder markers meanwhile`);
        }
        feed.lastError = headRefusedError(feed.ch);
        note(row, strikes >= 2 ? 'over the read budget: not read again for a while' : 'refused by the read budget: reading again in a fresh window');
      } else if (a.status !== 404) feed.lastError = { code: typeof a.status === 'number' ? a.status : 0, message: a.note || String(a.status) };
      else if (kind === 'head') feed.lastError = { code: 404, message: 'the feed has no updates yet' };
      return;
    }
    const index = typeof a.index === 'number' ? a.index : null;
    if (index !== null) row.index = index;
    const info = playlistInfo(a.text);
    // key is [recording, media sequence, entries, ended].
    row.seq = info.key[1];
    row.segments = info.key[2];
    const before = guard.text;
    const kept = guardPlaylist(a.text, index);
    if (kept === null) note(row, 'stale: an earlier broadcast');
    else if (kept !== a.text) note(row, 'stale: kept the newer one');
    else note(row, kept === before ? 'unchanged' : 'newer');
    // A live playlist on the channel (found through a marker or the slot
    // after the recording): the head is that small playlist now, not the
    // refused recording.
    if (kept === a.text && !info.key[3]) clearHeadRefused(feed.ch);
    const quiet = Date.now() - feed.lastNewer;
    if (kept === a.text && kept !== before) feed.lastNewer = Date.now();
    if (index !== null && (feed.index === null || index > feed.index)) feed.index = index;
    if (index !== null && index > feed.cursor) {
      feed.cursor = index;
      if (feed.joinedVia === null) feed.joinedVia = kind;
      // Joining, or catching up: there may be more after it, read on
      // without waiting. (At the live edge the next slot is not written
      // yet, so a regular poll that found one waits its turn.)
      if (kind === 'head' || chase || quiet > CATCH_UP_AFTER_MS) {
        note(row, 'reading on');
        readPlaylist(true);
      }
      if (kind === 'slot' && !chase && quiet > REJOIN_AFTER_MS) {
        const period = Math.floor(Date.now() / MARKER_PERIOD_MS);
        readOne('marker', period);
        readOne('marker', period - 1);
      }
    }
    // After the end, the channel's next broadcast continues the feed. (While
    // live, hls.js's next reload brings it and LEVEL_LOADED starts over.)
    if (guard.newBroadcast && ended) {
      log.marker('a new broadcast started on the channel');
      restartOnNewBroadcast();
    }
  }

  /**
   * Answer hls.js's waiting reload, if there is something to answer: a
   * newer playlist at once; the one it has once a playlist read finished
   * without a newer one; an error once every read of a join came back
   * without a playlist.
   */
  function deliver(playlistRead) {
    if (!feed.waiters.length) return;
    let result;
    if (guard.text !== null && guard.text !== feed.delivered) result = { text: guard.text };
    else if (guard.text !== null && playlistRead) result = { text: guard.text };
    else if (guard.text !== null || feed.inflight > 0) return;
    else result = { error: feed.lastError || { code: 404, message: 'the feed has no updates yet' } };
    const waiters = feed.waiters;
    feed.waiters = [];
    for (const w of waiters) w(result);
  }

  function nextPlaylist() {
    if (guard.text !== null && guard.text !== feed.delivered) return Promise.resolve({ text: guard.text });
    const p = new Promise((resolve) => feed.waiters.push(resolve));
    if (feed.inflight === 0) readPlaylist(guard.text === null);
    return p;
  }

  /** Called four times a second: keep slot reads going while live, slowly once ended. */
  function pollPlaylist() {
    if (!hls || guard.text === null) return;
    const now = Date.now();
    const every = ended ? ENDED_POLL_MS : now - feed.lastNewer > IDLE_AFTER_MS ? IDLE_POLL_MS : POLL_MS;
    const most = ended ? 1 : settings.polls;
    if (feed.inflight < most && now - feed.lastStart >= every) readPlaylist();
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
        feed.delivered = r.text;
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
      name: url.split('?')[0].split('/').filter(Boolean).pop(),
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
      // An #EXT-X-GAP entry has no media behind its URI.
      if (frag.gap) continue;
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
    // in its live playlist (about 26 of 2 s): sit four behind the newest
    // one, which leaves room to buffer through a slow read. A playhead more
    // than ten target durations (~20 s) behind the live edge jumps forward,
    // long before it would fall out of that window, so the longer playlist
    // does not let a web viewer lag further behind. A jump throws away what
    // is buffered and stalls again.
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
  // A hole the player jumps instead of waiting at: up to a few lost
  // segments. A segment the broadcaster dropped is listed as #EXT-X-GAP,
  // which hls.js never loads, so the media has a hole of one segment's
  // length (~2 s) there. Holding at it would wait for media that never
  // comes (the gate's rate 0 also keeps hls.js from skipping it).
  const MAX_HOLE_S = 6;
  const gate = { holding: false, why: '', since: 0 };
  let jumping = false;

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

  /**
   * Stuck in front of a hole (a lost segment, a discontinuity) with media
   * beyond it: jump to that media instead of waiting. Returns true if it
   * jumped.
   */
  function jumpHole() {
    if (bufferAhead() > 0.3) return false;
    const t = video.currentTime;
    for (let i = 0; i < video.buffered.length; i++) {
      const s = video.buffered.start(i);
      if (s > t + 0.05 && s - t <= MAX_HOLE_S) {
        log.marker(`jumped a ${(s - t).toFixed(1)} s hole at ${t.toFixed(1)} s (lost segment)`);
        // Our own seek: it lands on buffered media, so it must not start a
        // rebuffer hold, and a hold this stall began has done its job.
        jumping = true;
        video.currentTime = s + 0.05;
        if (gate.holding) release('jumped the hole');
        return true;
      }
    }
    return false;
  }

  function checkGate() {
    if (!gate.holding) return;
    jumpHole();
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
    // Stuck at a hole with video beyond it: jump it rather than hold.
    if (jumpHole()) return;
    if (bufferAheadAcrossGaps() < settings.rebuffer) hold('stall');
  });

  video.addEventListener('seeking', () => {
    const s = log.session;
    if (jumping) return;
    if (hls && s && s.firstPlayingAt !== null && bufferAheadAcrossGaps() < settings.rebuffer) hold('seek');
  });

  video.addEventListener('seeked', () => {
    jumping = false;
  });

  // The stage takes the picture's own shape (#194): a portrait broadcast
  // gets a portrait box instead of a narrow strip inside a 16:9 one.
  // `videoWidth`/`videoHeight` are the display size: Chromium (Freedom's
  // Electron 44 included), Safari and Firefox report a rotated track
  // (portrait broadcasts made before #194 store the picture sideways with
  // a rotate-90 track matrix) at its turned size, so no correction is
  // needed here; test/portrait-test.mjs checks both kinds. `resize` fires
  // again when a new broadcast on the channel changes orientation.
  function fitStage() {
    const wrap = video.parentElement;
    if (video.videoWidth > 0 && video.videoHeight > 0) {
      wrap.style.setProperty('--aspect', String(video.videoWidth / video.videoHeight));
    }
  }
  video.addEventListener('loadedmetadata', fitStage);
  video.addEventListener('resize', fitStage);

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
  // Whether this channel's first loaded playlist was live: a viewer who
  // joined live plays on from where they were when the broadcast ends,
  // even when the reload lands on the recording; only one who opened an
  // already-finished channel starts the recording from its first segment.
  let joinedLive = null;
  let everPlayed = false;
  let segmentsLoaded = 0;
  let lastFetchMs = null;
  // What the status line says while an ended broadcast plays.
  let endedStatus = '';
  let retryTimer = null;
  let statsTimer = null;
  let tickTimer = null;
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
    if (hls) hls.destroy();
    hls = null;
    resetReads(null);
    clearSegments();
    gate.holding = false;
    video.playbackRate = 1;
  }

  /**
   * Play channel `ch`. `next` carries on to a new broadcast on the same
   * channel: the feed index its first playlist was read at, and the ids of
   * the broadcasts before it, whose playlists are stale reads from then on.
   */
  function play(ch, next) {
    stop();
    channel = ch;
    ended = false;
    joinedLive = null;
    // Back to the default 16:9 stage until this channel's own first frame
    // sets it (fitStage): the last channel's shape says nothing about this
    // one, which may also never load at all.
    video.parentElement.style.removeProperty('--aspect');
    everPlayed = false;
    segmentsLoaded = 0;
    lastFetchMs = null;
    resetGuard(next && next.ids);
    log.begin(ch);
    lastFragAt = Date.now();
    els.intro.hidden = true;
    els.stage.hidden = false;
    els.channelRef.textContent = `0x${ch.owner} / ${ch.topic}`;
    els.input.value = ch.key;
    remember(ch.key);
    setBadge('', 'CONNECTING');
    if (headHeld(ch) && headTooLarge(ch)) {
      // A retry after the head was refused as over the budget: say so
      // rather than start over with "Finding the channel" each time.
      setOverlay(HEAD_REFUSED_OVERLAY);
      setStatus(`The last broadcast's recording is over Freedom's read budget. Open the channel through an HTTP gateway to play it; a new broadcast shows up here.`);
    } else {
      setOverlay('Finding the channel on Swarm…');
      setStatus('Resolving the channel feed…');
    }

    if (!Hls.isSupported()) {
      // The feed is read by the player itself, so a native HLS player
      // (Safari without Media Source Extensions) has nothing to load.
      setOverlay('This browser cannot play HLS video through the player (no Media Source Extensions).');
      return;
    }
    if (inBzz && !(window.swarm && typeof window.swarm.request === 'function')) {
      setOverlay('This page reads channels through Freedom\'s Swarm provider (window.swarm), which this browser does not have.');
      return;
    }

    resetReads(ch, next && next.index);
    hold('start');
    hls = new Hls(HLS_CONFIG);
    hls.attachMedia(video);
    hls.on(Hls.Events.MEDIA_ATTACHED, () => hls.loadSource(PLAYLIST_URL));
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
        restartOnNewBroadcast();
        return;
      }
      const wasEnded = ended;
      ended = !details.live;
      if (joinedLive === null) joinedLive = details.live;
      // New segments may have come into the prefetch window.
      prefetchAfter(cursorSn);
      if (ended && !wasEnded) log.marker('the broadcast ended');
      if (ended) {
        // A viewer who was following the live edge plays on from where
        // they were, to the end of the closing window (its last minutes) or
        // of the recording if the reload went straight to it; one who opens
        // a finished channel gets the recording the publisher leaves at the
        // head, the whole broadcast as VOD, from its first segment. The
        // slot after it keeps being read (pollPlaylist) for the next one.
        const after = 'You will switch to the next one when it starts.';
        if (joinedLive) {
          endedStatus = `The broadcast has ended: playing on to its end. ${after}`;
        } else {
          endedStatus = details.type === 'VOD'
            ? `The broadcast has ended: playing its recording from the start. ${after}`
            : `The broadcast has ended: playing its last minutes. ${after}`;
        }
        setStatus(endedStatus);
      }
    });
    hls.on(Hls.Events.FRAG_LOADED, (_e, data) => {
      segmentsLoaded += 1;
      lastFragAt = Date.now();
      const s = data.frag.stats;
      lastFetchMs = s.loading.end - s.loading.start;
    });
    hls.on(Hls.Events.ERROR, (_e, data) => onError(data));
    // Beside the first playlist read, never before it: see "the catalog".
    openCatalog(ch, !!next);
    statsTimer = setInterval(updateStats, 1000);
    tickTimer = setInterval(tick, 250);
  }

  /** Start over on the new broadcast the guard found, from where it was read. */
  function restartOnNewBroadcast() {
    const found = guard.newBroadcast;
    const ids = [...guard.ids].filter((id) => id !== found.id);
    // The head is the new broadcast's playlist now: no "too large" copy.
    clearHeadRefused(channel);
    play(channel, { index: found.index === null ? undefined : found.index, ids });
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
    const tooLarge = waiting && channel && headHeld(channel) && headTooLarge(channel);
    setOverlay(tooLarge ? HEAD_REFUSED_OVERLAY : waiting ? 'Waiting for the broadcast to start…' : 'Reconnecting…');
    const why = feed.lastError ? `: ${feed.lastError.message}` : '';
    setStatus(`${data.details}${data.response && data.response.code ? ` (${data.response.code})` : ''}${why}, retrying`);
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
   * Buffered ahead counting past holes up to MAX_HOLE_S (a discontinuity or
   * a lost segment leaves one), which `jumpHole` skips.
   */
  function bufferAheadAcrossGaps() {
    const t = video.currentTime;
    let end = t;
    let total = 0;
    for (let i = 0; i < video.buffered.length; i++) {
      const s = video.buffered.start(i);
      const e = video.buffered.end(i);
      if (e <= end || s > end + MAX_HOLE_S) continue;
      total += e - Math.max(s, end);
      end = e;
    }
    return total;
  }

  function updateStats() {
    if (!hls) return;
    const playing = !video.paused && video.readyState >= 3;
    if (playing) {
      if (!everPlayed) {
        everPlayed = true;
        // On now: no more "Starts at".
        renderCatalog();
      }
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
    else if (everPlayed && playing) setStatus(ended ? endedStatus : 'Playing live');
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
    u.hash = ch.key;
    return u.toString();
  }

  // -- channel selection ----------------------------------------------------

  const ASK = 'Enter the broadcaster\'s address and the channel\'s topic: 0x<40 hex>/<topic text>, 0x<40 hex>/0x<64 hex topic>, 0x<40 hex> alone for its antstream channel, a swarm:// link, or the text an AntStream app shares';

  /** Recent channels, by key; entries of the older reference form are dropped. */
  function recent() {
    try {
      const list = JSON.parse(localStorage.getItem(RECENT_KEY) || '[]');
      return Array.isArray(list) ? list.filter((k) => parseChannel(k)) : [];
    } catch (_) {
      return [];
    }
  }

  function remember(key) {
    try {
      const list = [key, ...recent().filter((c) => c !== key)].slice(0, 8);
      localStorage.setItem(RECENT_KEY, JSON.stringify(list));
    } catch (_) {
      /* storage unavailable: not worth failing playback over */
    }
  }

  function renderRecent() {
    els.recent.textContent = '';
    for (const key of recent()) {
      const ch = parseChannel(key);
      const li = document.createElement('li');
      const a = document.createElement('a');
      a.href = `#${ch.key}`;
      a.textContent = `0x${ch.owner} / ${ch.topic}`;
      li.appendChild(a);
      els.recent.appendChild(li);
    }
  }

  function fromHash() {
    const raw = location.hash.slice(1);
    if (!raw) return;
    const ch = parseChannel(raw);
    if (!ch) {
      // A link of the older form (a feed manifest reference, #<64 hex>),
      // whose channels are not played any more.
      els.input.value = raw;
      els.input.setCustomValidity(`This link names no owner/topic channel. ${ASK}`);
      els.input.reportValidity();
      return;
    }
    if (!channel || ch.key !== channel.key) play(ch);
  }

  els.form.addEventListener('submit', (e) => {
    e.preventDefault();
    const ch = parseChannel(els.input.value);
    if (!ch) {
      els.input.setCustomValidity(ASK);
      els.input.reportValidity();
      return;
    }
    els.input.setCustomValidity('');
    if (location.hash.slice(1) === ch.key) play(ch);
    else location.hash = ch.key;
  });
  els.input.addEventListener('input', () => els.input.setCustomValidity(''));
  window.addEventListener('hashchange', fromHash);

  renderRecent();
  fromHash();

  // -- build ------------------------------------------------------------------
  //
  // deploy-feed.mjs writes version.js next to the page at deploy time:
  // the build number (the feed index the build is published at), the
  // commit and the build time. A page served straight from a checkout
  // has none and says so.

  const build = window.ANTSTREAM_PLAYER_BUILD || null;

  function buildLabel() {
    if (!build) return 'Player: development build';
    const at = new Date(build.builtAt);
    const when = Number.isNaN(at.getTime())
      ? build.builtAt
      : `${at.toLocaleString()} (${at.toISOString().replace('T', ' ').slice(0, 16)} UTC)`;
    return `Player build ${build.build} · ${build.commit} · ${when}`;
  }

  const versionEl = document.getElementById('player-version');
  if (versionEl) versionEl.textContent = buildLabel();

  // Exposed for the test harness and for debugging from the console.
  window.antstreamPlayer = {
    build,
    buildLabel,
    toContentUrl,
    parseChannel,
    contentUrl,
    log,
    settings,
    setSetting,
    DEFAULTS,
    bufferAhead,
    gate,
    feed,
    budget,
    catalog,
    via: VIA,
    // One feed read by hand, e.g. `antstreamPlayer.read.slot(antstreamPlayer.channel, 12)`.
    read: { head: readHead, slot: readSlot, catalog: (ch) => readHead({ owner: ch.owner, topicHex: CATALOG_TOPIC_HEX }) },
    get channel() {
      return channel;
    },
    // The channel's head read was refused as over the read budget: the join
    // asks only the markers for now.
    get headHeld() {
      return !!channel && headHeld(channel);
    },
    // ... and refused twice in a row: over the budget, held for minutes.
    get headTooLarge() {
      return !!channel && headHeld(channel) && headTooLarge(channel);
    },
    get hls() {
      return hls;
    },
  };
})();
