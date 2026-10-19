#!/usr/bin/env node
// A broadcast that changes shape mid-stream must re-shape the stage
// (issue #228).
//
//   node test/turn-test.mjs
//
// Needs ffmpeg, xvfb-run and Electron (ELECTRON=<path to the binary>, or
// `electron` on PATH), as test/portrait-test.mjs does. The iOS app turns a
// live broadcast with the phone: the capture writer restarts at the new
// size, so the stream carries a second initialization segment behind an
// `#EXT-X-DISCONTINUITY`. This encodes such a stream with ffmpeg (6 s of
// a 480×640 portrait picture, then 8 s of a 640×360 landscape one, each
// with its own init segment), replays it as a live channel through
// test/fake-gateway.mjs (which loops it, so it turns back to portrait
// too), loads the player in test/freedom-harness.cjs and samples, every
// 250 ms, the size the video reports and the box the stage draws
// (SAMPLE_MS). It checks that:
//
//  - the picture played in both shapes, and turned both ways (portrait →
//    landscape and back);
//  - once a new size has been reported for a second, the stage's box and
//    the video element's box both have its aspect: the stage follows the
//    stream, it doesn't keep the shape it opened with;
//  - the stage never grows wider than the viewport.
//
// At a desktop window (1280×900) and a phone-sized one (390×844).
// Screenshots of each shape (SAMPLE_SHOTS) go to $OUT_DIR (default: a
// temp dir).

import { spawn, spawnSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, copyFileSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const electron = process.env.ELECTRON || 'electron';
const work = mkdtempSync(join(process.env.TMPDIR || tmpdir(), 'antstream-turn-'));
const outDir = process.env.OUT_DIR || work;
mkdirSync(outDir, { recursive: true });

/** Encode `seconds` of a W×H picture (a centred disc) as fMP4 HLS in `dir`. */
function encode(dir, width, height, seconds) {
  mkdirSync(dir, { recursive: true });
  const r = Math.min(width, height) / 4;
  const geq = `geq=lum='if(lt(hypot(X-${width / 2}\\,Y-${height / 2})\\,${r})\\,235\\,16)':cb=128:cr=128`;
  const out = spawnSync('ffmpeg', [
    '-loglevel', 'error', '-y',
    '-f', 'lavfi', '-i', `color=c=black:s=${width}x${height}:r=30`,
    '-f', 'lavfi', '-i', 'sine', '-t', String(seconds), '-vf', geq,
    '-c:v', 'libx264', '-g', '30', '-sc_threshold', '0', '-c:a', 'aac',
    '-f', 'hls', '-hls_time', '2', '-hls_segment_type', 'fmp4',
    '-hls_fmp4_init_filename', 'init.mp4', '-hls_segment_filename', join(dir, 'seg-%d.m4s'),
    '-start_number', '0', '-hls_playlist_type', 'vod', join(dir, 'p.m3u8'),
  ], { encoding: 'utf8' });
  if (out.status !== 0) throw new Error(`ffmpeg: ${out.stderr}`);
  const durations = [];
  let d = 0;
  for (const l of readFileSync(join(dir, 'p.m3u8'), 'utf8').split('\n')) {
    if (l.startsWith('#EXTINF:')) d = parseFloat(l.slice(8));
    const s = /^seg-(\d+)\.m4s$/.exec(l.trim());
    if (s) durations.push({ file: join(dir, `seg-${s[1]}.m4s`), ms: Math.round(d * 1000) });
  }
  return { init: join(dir, 'init.mp4'), durations };
}

/**
 * One recording of both writers, numbered as the app's segment ring is:
 * one counter for init and media, each writer's init before its media.
 */
function turnedRecording(dir) {
  mkdirSync(dir, { recursive: true });
  const parts = [encode(join(work, 'portrait'), 480, 640, 6), encode(join(work, 'landscape'), 640, 360, 8)];
  const lines = [];
  let n = 1;
  for (const part of parts) {
    copyFileSync(part.init, join(dir, `init-${n}.mp4`));
    lines.push(`${n} true 0`);
    n += 1;
    for (const seg of part.durations) {
      copyFileSync(seg.file, join(dir, `seg-${n}.m4s`));
      lines.push(`${n} false ${seg.ms}`);
      n += 1;
    }
  }
  writeFileSync(join(dir, 'durations.txt'), `${lines.join('\n')}\n`);
}

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

const VIEWPORTS = [
  { name: 'desktop', width: 1280, height: 900 },
  { name: 'mobile', width: 390, height: 844 },
];
const SETTLE_MS = 1000;
const dir = join(work, 'turned');
turnedRecording(dir);

const failures = [];
let port = 17830 + Math.floor(Math.random() * 500);
for (const vp of VIEWPORTS) {
  port += 1;
  const gw = spawn(process.execPath, [join(here, 'fake-gateway.mjs'), dir, String(port)], {
    env: { ...process.env, START_DELAY_MS: '500' },
    stdio: 'ignore',
  });
  const shots = join(outDir, `turn-${vp.name}`);
  mkdirSync(shots, { recursive: true });
  const profile = join(work, `profile-${vp.name}`);
  const r = await run('xvfb-run', ['-a', electron, '--no-sandbox', `--user-data-dir=${profile}`, join(here, 'freedom-harness.cjs')], {
    GATEWAY: `http://127.0.0.1:${port}`,
    WAIT_S: '40',
    SAMPLE_MS: '250',
    SAMPLE_SHOTS: shots,
    WIDTH: String(vp.width),
    HEIGHT: String(vp.height),
  });
  gw.kill();
  const label = vp.name;
  let state;
  try {
    state = JSON.parse(r.out.slice(r.out.indexOf('{')));
  } catch (_) {
    failures.push(`${label}: harness gave no state (exit ${r.code}): ${r.err.slice(-400)}`);
    continue;
  }
  const samples = (state.samples || []).filter((s) => s.videoWidth > 0);
  // Runs of one reported size, in order.
  const runs = [];
  for (const s of samples) {
    const size = `${s.videoWidth}x${s.videoHeight}`;
    const last = runs[runs.length - 1];
    if (last && last.size === size) last.samples.push(s);
    else runs.push({ size, portrait: s.videoWidth < s.videoHeight, samples: [s] });
  }
  console.log(`${label.padEnd(8)} ${runs.map((x) => `${x.size} ${((x.samples[x.samples.length - 1].t - x.samples[0].t) / 1000).toFixed(1)}s`).join(' → ')}  → ${shots}`);
  const turns = runs.slice(1).map((x, i) => `${runs[i].portrait ? 'P' : 'L'}${x.portrait ? 'P' : 'L'}`);
  if (!runs.some((x) => x.portrait) || !runs.some((x) => !x.portrait)) {
    failures.push(`${label}: played only ${runs.map((x) => x.size).join(', ') || 'nothing'}`);
  }
  if (!turns.includes('PL')) failures.push(`${label}: never turned portrait → landscape`);
  if (!turns.includes('LP')) failures.push(`${label}: never turned landscape → portrait`);
  let checked = 0;
  for (const x of runs) {
    const aspect = x.samples[0].videoWidth / x.samples[0].videoHeight;
    for (const s of x.samples) {
      if (s.t - x.samples[0].t < SETTLE_MS) continue;
      checked += 1;
      const stage = s.wrap.width / s.wrap.height;
      const box = s.video.width / s.video.height;
      if (Math.abs(stage - aspect) > 0.03) {
        failures.push(`${label}: playing ${x.size}, the stage is ${s.wrap.width.toFixed(0)}×${s.wrap.height.toFixed(0)} (${stage.toFixed(3)}, want ${aspect.toFixed(3)})`);
        break;
      }
      if (Math.abs(box - aspect) > 0.03) {
        failures.push(`${label}: playing ${x.size}, the video box is ${box.toFixed(3)}, want ${aspect.toFixed(3)}`);
        break;
      }
      if (s.wrap.width > s.viewport.width + 1) {
        failures.push(`${label}: stage ${s.wrap.width}px wider than the ${s.viewport.width}px viewport`);
        break;
      }
    }
  }
  if (!checked) failures.push(`${label}: no settled sample to check`);
}

if (failures.length) {
  console.error(`\nFAIL\n${failures.map((f) => `  ${f}`).join('\n')}`);
  process.exit(1);
}
console.log(`\nPASS (${VIEWPORTS.length} runs; screenshots in ${outDir}: ${VIEWPORTS.map((vp) => readdirSync(join(outDir, `turn-${vp.name}`)).map((f) => `turn-${vp.name}/${f}`).join(', ')).join(', ')})`);
