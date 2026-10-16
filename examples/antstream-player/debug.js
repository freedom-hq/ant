/*
 * AntStream player debug panel.
 *
 * Shows what player.js logs (window.antstreamPlayer.log): buffer ahead now
 * and over the last two minutes, every feed read and segment fetch with its
 * timings, stalls and errors, and the tuning knobs (prefetch depth,
 * overlapping feed reads, rebuffer target). Opened with the Debug button;
 * the choice is remembered. The log can be copied or downloaded as JSON to
 * attach to a bug report.
 */
(function () {
  'use strict';

  const player = window.antstreamPlayer;
  if (!player) return;
  const { log, settings, setSetting, DEFAULTS } = player;

  const OPEN_KEY = 'antstream.debug';
  const MAX_ROWS = 200;
  const SLOW_MS = 2000;
  const SPARK_W = 600;
  const SPARK_H = 60;

  const toggle = document.getElementById('debug-toggle');
  const panel = document.getElementById('debug');
  const video = document.getElementById('video');

  // -- formatting -----------------------------------------------------------

  const clock = (ms) => {
    const d = new Date(ms);
    return `${d.toTimeString().slice(0, 8)}.${String(d.getMilliseconds()).padStart(3, '0')}`;
  };
  const sec = (ms) => (ms === null || ms === undefined ? '–' : `${(ms / 1000).toFixed(2)} s`);
  const size = (b) => (!b ? '–' : b >= 1048576 ? `${(b / 1048576).toFixed(2)} MB` : `${Math.round(b / 1024)} kB`);
  const mbps = (bytes, ms) => (bytes && ms ? ((bytes * 8) / ms / 1000).toFixed(2) : '–');

  function median(xs) {
    if (!xs.length) return null;
    const s = [...xs].sort((a, b) => a - b);
    return s[Math.floor(s.length / 2)];
  }

  function p95(xs) {
    if (!xs.length) return null;
    const s = [...xs].sort((a, b) => a - b);
    return s[Math.min(s.length - 1, Math.floor(s.length * 0.95))];
  }

  function el(tag, attrs, ...children) {
    const e = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs || {})) {
      if (k === 'class') e.className = v;
      else if (k.startsWith('on')) e.addEventListener(k.slice(2), v);
      else e.setAttribute(k, v);
    }
    for (const c of children) e.append(c);
    return e;
  }

  // -- layout ---------------------------------------------------------------

  const bufNum = el('span', { class: 'buf-num' });
  const bufFill = el('div', { class: 'buf-fill' });
  const bufTarget = el('div', { class: 'buf-target' });
  const bufScale = el('span', { class: 'buf-scale' });
  const bufNote = el('span', { class: 'buf-note' });
  const spark = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  spark.setAttribute('viewBox', `0 0 ${SPARK_W} ${SPARK_H}`);
  spark.setAttribute('preserveAspectRatio', 'none');
  spark.setAttribute('class', 'spark');
  const summary = el('div', { class: 'summary' });
  const tbody = el('tbody');
  let paused = false;
  const pauseBtn = el('button', { type: 'button', onclick: () => { paused = !paused; pauseBtn.classList.toggle('on', paused); pauseBtn.textContent = paused ? 'Resume' : 'Pause'; render(true); } }, 'Pause');
  const copyBtn = el('button', { type: 'button', onclick: copyLog }, 'Copy log');

  function knob(key, label, max, unit, title) {
    const input = el('input', { type: 'number', min: '0', max: String(max), step: '1', value: String(settings[key]), title });
    input.addEventListener('change', () => {
      const v = Math.max(0, Math.min(max, Math.round(Number(input.value) || 0)));
      input.value = String(v);
      setSetting(key, v);
      log.marker(`setting ${key} = ${v}`);
    });
    knobs[key] = input;
    return el('label', { title }, `${label} `, input, unit ? ` ${unit}` : '');
  }
  const knobs = {};

  panel.append(
    el('div', { class: 'head' },
      el('b', {}, 'Debug'),
      el('span', { class: 'muted' }, `${player.buildLabel()} · hls.js ${window.Hls ? Hls.version : '?'}`),
      el('span', { class: 'spacer' }),
      pauseBtn,
      copyBtn,
      el('button', { type: 'button', onclick: downloadLog }, 'Download'),
      el('button', { type: 'button', onclick: () => { log.entries = []; log.samples = []; log.touch(); } }, 'Clear'),
    ),
    el('div', { class: 'buf' },
      bufNum,
      el('div', { class: 'buf-bar' }, bufFill, bufTarget, bufScale),
      bufNote,
    ),
    spark,
    summary,
    el('div', { class: 'controls' },
      knob('prefetch', 'Prefetch', 8, 'segments', 'Segments fetched in parallel ahead of the one hls.js loads. 0 = off (hls.js alone, one at a time).'),
      knob('polls', 'Feed reads', 6, 'at once', 'Feed slots read side by side while live, a new read every 1.5 s. 0 = off (one read per hls.js reload).'),
      knob('rebuffer', 'Rebuffer to', 20, 's', 'Hold playback until this much is buffered ahead after a start, seek or stall. 0 = off.'),
      el('button', { type: 'button', onclick: resetKnobs }, 'Defaults'),
    ),
    el('div', { class: 'table-wrap' },
      el('table', {},
        el('thead', {}, el('tr', {}, ...['Time', 'What', '#', 'First byte', 'Total', 'Size', 'Mbit/s', 'Via', 'Status'].map((h) => el('th', {}, h)))),
        tbody,
      ),
    ),
  );

  function resetKnobs() {
    for (const [k, v] of Object.entries(DEFAULTS)) {
      setSetting(k, v);
      knobs[k].value = String(v);
    }
    log.marker('settings reset to defaults');
  }

  // -- rendering ------------------------------------------------------------

  function renderBuffer() {
    const ahead = player.bufferAhead();
    const target = settings.rebuffer;
    const max = Math.max(10, Math.ceil(Math.max(ahead, target, ...log.samples.map((s) => s.ahead)) / 5) * 5);
    const lvl = ahead < 1 ? 'bad' : ahead < Math.max(target, 2) ? 'low' : 'ok';
    bufNum.textContent = `${ahead.toFixed(1)} s ahead`;
    bufNum.className = `buf-num ${lvl}`;
    bufFill.className = `buf-fill ${lvl}`;
    bufFill.style.width = `${Math.min(ahead / max, 1) * 100}%`;
    bufTarget.hidden = !target;
    bufTarget.style.left = `${(target / max) * 100}%`;
    bufScale.textContent = `${max} s`;
    const s = log.session;
    const hls = player.hls;
    const latency = hls && Number.isFinite(hls.latency) ? `${hls.latency.toFixed(1)} s behind the newest segment seen` : '';
    bufNote.textContent = player.gate.holding
      ? `holding (${player.gate.why}) until ${target} s`
      : s && s.stallSince !== null ? 'stalled' : latency;
  }

  function renderSpark() {
    const samples = log.samples;
    spark.textContent = '';
    if (samples.length < 2) return;
    const now = Date.now();
    const span = 120000;
    const max = Math.max(10, ...samples.map((s) => s.ahead), settings.rebuffer);
    const x = (t) => ((t - (now - span)) / span) * SPARK_W;
    const y = (v) => SPARK_H - (Math.min(v, max) / max) * (SPARK_H - 4) - 2;
    const step = Math.max(1, (500 / span) * SPARK_W);
    const ns = 'http://www.w3.org/2000/svg';
    for (const s of samples) {
      if (!s.stalled && !s.holding) continue;
      const r = document.createElementNS(ns, 'rect');
      r.setAttribute('x', x(s.t));
      r.setAttribute('y', 0);
      r.setAttribute('width', step);
      r.setAttribute('height', SPARK_H);
      r.setAttribute('class', s.stalled ? 'stall' : 'held');
      spark.append(r);
    }
    if (settings.rebuffer) {
      const l = document.createElementNS(ns, 'line');
      l.setAttribute('x1', 0);
      l.setAttribute('x2', SPARK_W);
      l.setAttribute('y1', y(settings.rebuffer));
      l.setAttribute('y2', y(settings.rebuffer));
      l.setAttribute('class', 'target');
      spark.append(l);
    }
    const path = document.createElementNS(ns, 'path');
    path.setAttribute('d', samples.map((s, i) => `${i ? 'L' : 'M'}${x(s.t).toFixed(1)},${y(s.ahead).toFixed(1)}`).join(''));
    path.setAttribute('class', 'line');
    spark.append(path);
  }

  function renderSummary() {
    const s = log.session;
    const segs = log.entries.filter((e) => e.kind === 'segment');
    const ok = segs.filter((e) => e.status === 'ok');
    const failed = segs.filter((e) => e.status === 'error' || e.status === 'timeout');
    const cancelled = segs.filter((e) => e.status === 'cancelled');
    // A read dropped before it was sent (a restart or channel change while it
    // waited for the read budget) was never answered: not a failure.
    const done = log.entries.filter((e) => e.kind === 'playlist' && e.ms !== null && e.status !== 'cancelled');
    const reads = done.filter((e) => e.read !== 'marker');
    const readsOk = reads.filter((e) => e.status === 200);
    const notYet = reads.filter((e) => e.status === 404);
    const markers = done.filter((e) => e.read === 'marker');
    const limited = done.filter((e) => e.status === 429);
    const newerAt = readsOk.filter((e) => e.note.includes('newer')).map((e) => e.at);
    const gaps = newerAt.slice(1).map((t, i) => t - newerAt[i]);
    const feed = player.feed;
    const budget = player.budget;
    const bytes = ok.reduce((a, e) => a + e.bytes, 0);
    const ms = ok.reduce((a, e) => a + e.ms, 0);
    const stallMs = s ? s.stallMs + (s.stallSince !== null ? Date.now() - s.stallSince : 0) : 0;
    const c = s ? s.claims : { ready: 0, joined: 0, miss: 0 };
    const lines = [
      ['Segments', `${ok.length} ok · ${failed.length} failed · ${cancelled.length} cancelled`, failed.length > 0],
      ['Segment first byte', `median ${sec(median(ok.map((e) => e.ttfb)))} · p95 ${sec(p95(ok.map((e) => e.ttfb)))}`],
      ['Segment total', `median ${sec(median(ok.map((e) => e.ms)))} · p95 ${sec(p95(ok.map((e) => e.ms)))} · ${mbps(bytes, ms)} Mbit/s per fetch`],
      ['Prefetch', `${c.ready} ready · ${c.joined} joined · ${c.miss} fetched by hls.js`],
      ['Feed index', feed.index === null ? (player.headTooLarge ? '– · head over the read budget, joining through markers only' : player.headHeld ? '– · head refused by the read budget, read again in a fresh window' : '–') : `${feed.index} · joined via ${feed.joinedVia || '–'} · reading from ${feed.cursor + 1} · ${feed.ch ? feed.ch.topicHex.slice(0, 12) : ''}… over ${player.via}`],
      ['Feed reads', `${readsOk.length} ok · ${notYet.length} not written yet · ${reads.length - readsOk.length - notYet.length} failed · median ${sec(median(readsOk.map((e) => e.ms)))} · p95 ${sec(p95(readsOk.map((e) => e.ms)))}`, reads.length - readsOk.length - notYet.length > 0],
      ['Feed reads', `${readsOk.filter((e) => e.note.includes('newer')).length} newer · ${readsOk.filter((e) => e.note.includes('unchanged')).length} unchanged · ${readsOk.filter((e) => e.note.includes('stale')).length} stale · markers ${markers.filter((e) => e.status === 200).length} of ${markers.length} found`],
      ...(player.via === 'window.swarm'
        ? [['Read budget', `${budget.reads.length} reads · ${size(budget.reads.reduce((n, r) => n + r.bytes, 0))} in the last minute · ${budget.refused} rate limited${Date.now() < budget.until ? ` · paused ${sec(budget.until - Date.now())}` : ''}`, limited.length > 0]]
        : []),
      ['Live edge moved', gaps.length ? `every ${sec(median(gaps))} (median) · longest ${sec(Math.max(...gaps))}` : '–'],
      ['Stalls', s ? `${s.stalls} · ${sec(stallMs)} in total` : '–', s && s.stalls > 0],
      ['First frame', s && s.firstPlayingAt ? sec(s.firstPlayingAt - s.startedAt) : '–'],
    ];
    summary.textContent = '';
    for (const [k, v, bad] of lines) summary.append(el('div', {}, el('span', { class: 'muted' }, k), ' ', el('b', { class: bad ? 'bad' : '' }, v)));
  }

  function row(e) {
    const t = el('td', { class: 'mono' }, clock(e.at));
    if (e.kind === 'marker') {
      const bad = /stall|error|fatal/i.test(e.text);
      return el('tr', { class: `marker${bad ? ' bad' : ''}` }, t, el('td', { colspan: '8' }, e.text));
    }
    if (e.kind === 'playlist') {
      // How the read was answered: 200 with a playlist (or a marker), 404
      // for a slot not written yet, 429 rate limited, else an error. A read
      // dropped before it was sent (channel changed) is dimmed, not an error.
      const bad = e.status !== 200 && e.status !== 404 && e.status !== 'loading' && e.status !== 'cancelled';
      const stale = e.note.includes('stale');
      const what = e.read === 'head' ? 'feed head' : e.read === 'marker' ? `marker ${e.period}` : 'feed slot';
      return el('tr', { class: `playlist${bad ? ' bad' : stale ? ' warn' : e.status === 404 || e.status === 'cancelled' ? ' dim' : ''}` },
        t,
        el('td', {}, what),
        el('td', { class: 'mono' }, e.index === null || e.index === undefined ? '–' : String(e.index)),
        el('td', {}, ''),
        el('td', { class: e.ms > 4000 ? 'slow' : '' }, sec(e.ms)),
        el('td', {}, ''),
        el('td', {}, ''),
        el('td', {}, e.via || ''),
        el('td', {}, `${e.status}${e.note ? ` · ${e.note}` : ''}${e.seq === null ? '' : ` · seq ${e.seq}+${e.segments}`}`),
      );
    }
    const slow = (e.ttfb || 0) > SLOW_MS;
    const bad = e.status === 'error' || e.status === 'timeout';
    return el('tr', { class: `segment${bad ? ' bad' : e.status === 'cancelled' ? ' dim' : ''}` },
      t,
      el('td', { class: 'mono', title: e.name }, e.name.length > 16 ? `${e.name.slice(0, 12)}…` : e.name),
      el('td', { class: 'mono' }, e.sn === null || e.sn === undefined ? '' : String(e.sn)),
      el('td', { class: slow ? 'slow' : '' }, sec(e.ttfb)),
      el('td', { class: (e.ms || 0) > 2000 ? 'slow' : '' }, sec(e.ms)),
      el('td', {}, size(e.bytes)),
      el('td', {}, mbps(e.bytes, e.ms)),
      el('td', {}, e.via),
      el('td', {}, `${e.status}${e.note ? ` · ${e.note}` : ''}`),
    );
  }

  let shown = -1;
  function render(force) {
    if (panel.hidden) return;
    renderBuffer();
    if (!force && (paused || shown === log.version)) return;
    shown = log.version;
    renderSpark();
    renderSummary();
    if (paused && !force) return;
    tbody.textContent = '';
    const rows = log.entries.slice(-MAX_ROWS).reverse();
    for (const e of rows) tbody.append(row(e));
  }

  // -- export ---------------------------------------------------------------

  function exportJson() {
    const hls = player.hls;
    return JSON.stringify({
      exportedAt: new Date().toISOString(),
      page: location.href,
      userAgent: navigator.userAgent,
      hlsVersion: window.Hls ? Hls.version : null,
      playerBuild: player.build,
      settings,
      session: log.session,
      latency: hls && Number.isFinite(hls.latency) ? hls.latency : null,
      bufferAhead: player.bufferAhead(),
      video: { currentTime: video.currentTime, readyState: video.readyState, size: `${video.videoWidth}x${video.videoHeight}` },
      samples: log.samples,
      entries: log.entries,
    }, null, 2);
  }

  async function copyLog() {
    const text = exportJson();
    try {
      await navigator.clipboard.writeText(text);
      copyBtn.textContent = 'Copied';
    } catch (_) {
      const area = el('textarea');
      area.value = text;
      document.body.append(area);
      area.select();
      copyBtn.textContent = document.execCommand('copy') ? 'Copied' : 'Copy failed';
      area.remove();
    }
    setTimeout(() => { copyBtn.textContent = 'Copy log'; }, 1500);
  }

  function downloadLog() {
    const a = el('a', {
      href: URL.createObjectURL(new Blob([exportJson()], { type: 'application/json' })),
      download: `antstream-debug-${new Date().toISOString().replace(/[:.]/g, '-')}.json`,
    });
    document.body.append(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(a.href), 1000);
  }

  // -- open / close ---------------------------------------------------------

  function setOpen(open) {
    panel.hidden = !open;
    toggle.classList.toggle('on', open);
    try {
      localStorage.setItem(OPEN_KEY, open ? '1' : '');
    } catch (_) {
      /* not remembered */
    }
    render(true);
  }

  toggle.addEventListener('click', () => setOpen(panel.hidden));
  let open = false;
  try {
    open = localStorage.getItem(OPEN_KEY) === '1';
  } catch (_) {
    /* closed */
  }
  if (/[?&]debug\b/.test(location.search)) open = true;
  setOpen(open);
  setInterval(render, 500);
})();
