#!/usr/bin/env node
// The debug panel, docked to the bottom of the playing view.
//
//   node test/debug-panel-test.mjs
//
// Needs ffmpeg, xvfb-run and Electron (ELECTRON=<path to the binary>, or
// `electron` on PATH), as test/portrait-test.mjs does. It encodes a short
// recording, replays it as a live channel through test/fake-gateway.mjs,
// loads the player in test/freedom-harness.cjs and sends real input events
// (the harness's STEPS) to check that:
//
//  - the playing view has no channel line and no Copy link button;
//  - closed, the panel is hidden and the page has no bottom padding;
//  - the Debug button opens it docked to the bottom of the window, the
//    channel (owner / topic and the feed topic hash) at its top, its body
//    scrolling on its own, and the page padded at the bottom by exactly
//    the panel's height, so the page's end (the footer) scrolls clear of it;
//  - dragging the top edge resizes it, between 120 px and 80% of the
//    window, and the height is stored (and the padding follows);
//  - after a reload the panel is open again at the stored height;
//  - the arrow keys on the edge resize it, Escape inside it closes it and
//    puts the focus back on the Debug button, and so does the ×.
//
// At a desktop window (1280×900) and a phone-sized one (390×844), where
// the page must not scroll sideways. Screenshots of the playing view
// without and with the panel go to $OUT_DIR (default: a temp dir).

import { spawn, spawnSync } from 'node:child_process';
import { createRequire } from 'node:module';
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const K = createRequire(import.meta.url)('../keccak.js');
const electron = process.env.ELECTRON || 'electron';
const work = mkdtempSync(join(process.env.TMPDIR || tmpdir(), 'antstream-debug-'));
const outDir = process.env.OUT_DIR || work;
mkdirSync(outDir, { recursive: true });

const OWNER = 'd5572300e441b77b72bdd318bbfa7b97a1f03096';
const TOPIC = 'antstream';
const PAGE = `bzz://${'a'.repeat(64)}/#0x${OWNER}/${TOPIC}`;
const MIN = 120;

const VIEWPORTS = [
  { name: 'desktop', width: 1280, height: 900 },
  { name: 'mobile', width: 390, height: 844 },
];

/** 20 s of a 640×360 test picture as fMP4 HLS, in the fake gateway's layout. */
function encode(dir) {
  mkdirSync(dir, { recursive: true });
  const r = spawnSync('ffmpeg', [
    '-loglevel', 'error', '-y',
    '-f', 'lavfi', '-i', 'testsrc2=size=640x360:rate=30',
    '-f', 'lavfi', '-i', 'sine', '-t', '20',
    '-c:v', 'libx264', '-g', '30', '-sc_threshold', '0', '-c:a', 'aac',
    '-f', 'hls', '-hls_time', '2', '-hls_segment_type', 'fmp4',
    '-hls_fmp4_init_filename', 'init-1.mp4', '-hls_segment_filename', join(dir, 'seg-%d.m4s'),
    '-start_number', '2', '-hls_playlist_type', 'vod', join(dir, 'p.m3u8'),
  ], { encoding: 'utf8' });
  if (r.status !== 0) throw new Error(`ffmpeg: ${r.stderr}`);
  const lines = ['1 true 0'];
  let d = 0;
  for (const l of readFileSync(join(dir, 'p.m3u8'), 'utf8').split('\n')) {
    if (l.startsWith('#EXTINF:')) d = parseFloat(l.slice(8));
    const s = /^seg-(\d+)\.m4s$/.exec(l.trim());
    if (s) lines.push(`${s[1]} false ${Math.round(d * 1000)}`);
  }
  writeFileSync(join(dir, 'durations.txt'), `${lines.join('\n')}\n`);
}

// What the steps read back, by name. Scrolls to the end of the page (to
// see whether the footer clears the panel) and back to the top.
const PROBE = `(() => {
  const d = document.getElementById('debug');
  const r = d.getBoundingClientRect();
  scrollTo(0, document.documentElement.scrollHeight);
  const foot = document.querySelector('footer').getBoundingClientRect();
  const out = {
    hidden: d.hidden,
    height: Math.round(r.height),
    bottom: Math.round(r.bottom),
    width: Math.round(r.width),
    padding: getComputedStyle(document.body).paddingBottom,
    footerClear: d.hidden ? foot.bottom <= innerHeight + 1 : foot.bottom <= r.top + 1,
    bodyScroll: d.hidden ? null : getComputedStyle(d.querySelector('.body')).overflowY,
    stored: localStorage.getItem('antstream.debug.height'),
    open: localStorage.getItem('antstream.debug'),
    toggleOn: document.getElementById('debug-toggle').classList.contains('on'),
    focus: document.activeElement && (document.activeElement.id || document.activeElement.className || document.activeElement.tagName),
    chan: d.querySelector('.chan') ? d.querySelector('.chan').innerText.replace(/\\s+/g, ' ').trim() : null,
    removed: ['#channel-ref', '#copy-link', 'p.channel'].filter((s) => document.querySelector(s)),
    innerHeight,
    viewport: innerWidth,
    clientWidth: document.documentElement.clientWidth,
    overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth,
  };
  scrollTo(0, 0);
  return out;
})()`;

function run(cmd, args, env) {
  return new Promise((resolve) => {
    const p = spawn(cmd, args, { env: { ...process.env, ...env } });
    let out = '';
    let err = '';
    p.stdout.on('data', (d) => { out += d; });
    p.stderr.on('data', (d) => { err += d; });
    p.on('close', (code) => resolve({ code, out, err }));
  });
}

const segments = join(work, 'segments');
encode(segments);

const failures = [];
let port = 18330 + Math.floor(Math.random() * 500);
for (const vp of VIEWPORTS) {
  const label = vp.name;
  const steps = [];
  const probes = [];
  const probe = (name) => { probes.push(name); steps.push({ js: PROBE, name }); };
  const shot = (name) => steps.push({ shot: `${name}-${vp.name}.png` });
  const GRIP = '#debug .grip';

  probe('closed');
  shot('playing');
  steps.push({ click: '#debug-toggle' }, { wait: 300 });
  probe('opened');
  steps.push({ drag: GRIP, by: [0, -150] });
  probe('dragged');
  shot('playing-debug');
  // Far up, then far down: held at the bounds.
  steps.push({ drag: GRIP, by: [0, -Math.round(vp.height * 0.42)] });
  probe('tall');
  steps.push({ drag: GRIP, by: [0, Math.round(vp.height * 0.75)] });
  probe('short');
  steps.push({ drag: GRIP, by: [0, -200] });
  probe('set');
  steps.push({ load: PAGE }, { wait: 2500 });
  probe('reloaded');
  steps.push({ js: `document.querySelector(${JSON.stringify(GRIP)}).focus()` }, { key: 'Up' });
  probe('keyed');
  steps.push({ key: 'Escape' }, { wait: 100 });
  probe('escaped');
  steps.push({ click: '#debug-toggle' }, { wait: 200 }, { click: '#debug .close' }, { wait: 100 });
  probe('crossed');

  port += 1;
  const gw = spawn(process.execPath, [join(here, 'fake-gateway.mjs'), segments, String(port)], {
    env: { ...process.env, START_DELAY_MS: '500' },
    stdio: 'ignore',
  });
  const stepsFile = join(work, `steps-${vp.name}.json`);
  writeFileSync(stepsFile, JSON.stringify(steps));
  // A fresh profile per run: no remembered panel or height.
  const r = await run('xvfb-run', ['-a', electron, '--no-sandbox', `--user-data-dir=${join(work, `profile-${vp.name}`)}`, join(here, 'freedom-harness.cjs')], {
    GATEWAY: `http://127.0.0.1:${port}`,
    URL: PAGE,
    WAIT_S: '10',
    STEPS: stepsFile,
    STEPS_OUT: outDir,
    WIDTH: String(vp.width),
    HEIGHT: String(vp.height),
  });
  gw.kill();
  let state;
  try {
    state = JSON.parse(r.out.slice(r.out.indexOf('{')));
  } catch (_) {
    failures.push(`${label}: harness gave no state (exit ${r.code}): ${r.err.slice(-400)}`);
    continue;
  }
  const errors = state.steps.filter((s) => s.error);
  if (errors.length) failures.push(`${label}: steps failed: ${JSON.stringify(errors)}`);
  const values = state.steps.filter((s) => s.js === PROBE).map((s) => s.value);
  const p = Object.fromEntries(probes.map((name, i) => [name, values[i]]));
  const check = (ok, what) => {
    if (!ok) failures.push(`${label}: ${what}`);
    return ok;
  };
  const max = Math.floor(p.closed.innerHeight * 0.8);
  // Open, docked, `h` tall, and the page padded by exactly that.
  const docked = (s, h) => s && !s.hidden && s.height === h && s.padding === `${h}px` && s.bottom === s.innerHeight
    && s.width === s.clientWidth && s.footerClear && s.overflow === 0;

  check(state.decodedFrames > 0, `no frames decoded (${state.status})`);
  check(p.closed.removed.length === 0, `still on the page: ${p.closed.removed.join(', ')}`);
  check(p.closed.hidden && p.closed.padding === '0px' && !p.closed.toggleOn && p.closed.footerClear && p.closed.overflow === 0,
    `closed: ${JSON.stringify(p.closed)}`);
  const first = Math.round(p.closed.innerHeight * 0.4);
  check(docked(p.opened, first) && p.opened.toggleOn && p.opened.bodyScroll === 'auto',
    `the Debug button did not open the panel docked and ${first} px tall: ${JSON.stringify(p.opened)}`);
  check(p.opened.chan && p.opened.chan.includes(`0x${OWNER} / ${TOPIC}`) && p.opened.chan.includes(K.topicFromString(TOPIC)),
    `the channel is not at the top of the panel: ${p.opened.chan}`);
  check(docked(p.dragged, first + 150) && p.dragged.stored === String(first + 150),
    `dragging up 150 px: ${JSON.stringify(p.dragged)}`);
  check(docked(p.tall, max) && p.tall.stored === String(max), `dragged past 80% of the window (${max} px): ${JSON.stringify(p.tall)}`);
  check(docked(p.short, MIN) && p.short.stored === String(MIN), `dragged under ${MIN} px: ${JSON.stringify(p.short)}`);
  check(docked(p.set, MIN + 200) && p.set.stored === String(MIN + 200), `dragged back up 200 px: ${JSON.stringify(p.set)}`);
  check(docked(p.reloaded, MIN + 200), `after a reload the panel is not open at its stored height: ${JSON.stringify(p.reloaded)}`);
  check(docked(p.keyed, MIN + 224) && p.keyed.stored === String(MIN + 224), `ArrowUp on the edge: ${JSON.stringify(p.keyed)}`);
  check(p.escaped.hidden && p.escaped.padding === '0px' && p.escaped.focus === 'debug-toggle' && p.escaped.open === '' && !p.escaped.toggleOn,
    `Escape in the panel: ${JSON.stringify(p.escaped)}`);
  check(p.crossed.hidden && p.crossed.padding === '0px' && p.crossed.focus === 'debug-toggle' && p.crossed.stored === String(MIN + 224),
    `the ×: ${JSON.stringify(p.crossed)}`);
  console.log(`${label.padEnd(8)} ${failures.length ? 'see failures' : 'ok'}`);
}

if (failures.length) {
  console.error(`\nFAIL\n${failures.map((f) => `  ${f}`).join('\n')}`);
  process.exit(1);
}
console.log(`\nPASS (${VIEWPORTS.length} runs; screenshots in ${outDir}: ${readdirSync(outDir).filter((f) => f.endsWith('.png')).join(', ')})`);
