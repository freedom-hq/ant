#!/usr/bin/env node
// Portrait streams must play portrait and unstretched (issue #194).
//
//   node test/portrait-test.mjs
//
// Needs ffmpeg, xvfb-run and Electron (ELECTRON=<path to the binary>, or
// `electron` on PATH). For each case it encodes a short recording with
// ffmpeg, replays it as a live channel through test/fake-gateway.mjs, loads
// the player in test/freedom-harness.cjs (Freedom's `bzz:` scheme) and
// checks what lands on screen:
//
//  - the video element's box has the picture's display aspect (portrait
//    for a portrait stream), not the stage's old fixed 16:9;
//  - the picture is not stretched: every frame is one bright disc on a
//    dark ground, and the disc's bounding box in a capture of the element
//    must be round (width ≈ height), centred, and — for a portrait stream
//    — taller-than-wide frame around it.
//
// Cases:
//  portrait  480×640 coded, identity matrix: what the apps publish since
//            #194 (real portrait frames).
//  rotated   640×480 coded with the Android app's rotate-90 tkhd matrix
//            (the picture stored sideways): every portrait broadcast made
//            before #194. Chromium applies the matrix and reports rotated
//            dimensions; the stage must follow them.
//  landscape 640×360: the stage stays landscape.
//
// After the measurement, every run opens a second channel that never loads
// (SWITCH_TO): the stage must go back to 16:9 rather than keep the first
// channel's shape.
//
// Each case runs at a desktop window (1280×900) and a phone-sized one
// (390×844). Screenshots go to $OUT_DIR (default: a temp dir).

import { spawn, spawnSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const electron = process.env.ELECTRON || 'electron';
const work = mkdtempSync(join(process.env.TMPDIR || tmpdir(), 'antstream-portrait-'));
const outDir = process.env.OUT_DIR || work;
mkdirSync(outDir, { recursive: true });

const R = 140; // disc radius in picture pixels

/** Encode `seconds` of a W×H picture (a disc centred, dark ground) as fMP4 HLS. */
function encode(dir, { width, height, transpose }) {
  mkdirSync(dir, { recursive: true });
  const cx = width / 2;
  const cy = height / 2;
  const geq = `geq=lum='if(lt(hypot(X-${cx}\\,Y-${cy})\\,${R})\\,235\\,16)':cb=128:cr=128`;
  const vf = transpose ? `${geq},transpose=2` : geq;
  const r = spawnSync('ffmpeg', [
    '-loglevel', 'error', '-y',
    '-f', 'lavfi', '-i', `color=c=black:s=${width}x${height}:r=30`,
    '-f', 'lavfi', '-i', 'sine', '-t', '12', '-vf', vf,
    '-c:v', 'libx264', '-g', '30', '-sc_threshold', '0', '-c:a', 'aac',
    '-f', 'hls', '-hls_time', '2', '-hls_segment_type', 'fmp4',
    '-hls_fmp4_init_filename', 'init-1.mp4', '-hls_segment_filename', join(dir, 'seg-%d.m4s'),
    '-start_number', '2', '-hls_playlist_type', 'vod', join(dir, 'p.m3u8'),
  ], { encoding: 'utf8' });
  if (r.status !== 0) throw new Error(`ffmpeg: ${r.stderr}`);
  const m3u8 = readFileSync(join(dir, 'p.m3u8'), 'utf8');
  const lines = ['1 true 0'];
  let d = 0;
  for (const l of m3u8.split('\n')) {
    if (l.startsWith('#EXTINF:')) d = parseFloat(l.slice(8));
    const s = /^seg-(\d+)\.m4s$/.exec(l.trim());
    if (s) lines.push(`${s[1]} false ${Math.round(d * 1000)}`);
  }
  writeFileSync(join(dir, 'durations.txt'), `${lines.join('\n')}\n`);
}

/** Find a box path (e.g. moov/trak/tkhd) in an ISO BMFF buffer; first match. */
function findBox(buf, path, start = 0, end = buf.length) {
  const [head, ...rest] = path;
  for (let at = start; at + 8 <= end;) {
    const size = buf.readUInt32BE(at);
    const type = buf.toString('latin1', at + 4, at + 8);
    if (size < 8) break;
    if (type === head) return rest.length ? findBox(buf, rest, at + 8, at + size) : at;
    at += size;
  }
  return -1;
}

/**
 * Write the Android app's pre-#194 portrait track header into an init
 * segment: the matrix for a 90° clockwise turn ({0,1,-1,0}, as
 * Fmp4Muxer.matrix(90) wrote it), width/height left at the coded size.
 */
function setRotate90(initPath) {
  const buf = readFileSync(initPath);
  const tkhd = findBox(buf, ['moov', 'trak', 'tkhd']);
  if (tkhd < 0) throw new Error('no tkhd');
  const version = buf[tkhd + 8];
  const matrix = tkhd + 8 + 4 + (version === 1 ? 32 : 20) + 8 + 8;
  const one = 0x00010000;
  [0, one, 0, -one, 0, 0, 0, 0, 0x40000000].forEach((v, i) => buf.writeInt32BE(v, matrix + i * 4));
  writeFileSync(initPath, buf);
}

const CASES = [
  { name: 'portrait', width: 480, height: 640, display: 480 / 640 },
  { name: 'rotated', width: 480, height: 640, transpose: true, rotate: true, display: 480 / 640 },
  { name: 'landscape', width: 640, height: 360, display: 640 / 360 },
];
const VIEWPORTS = [
  { name: 'desktop', width: 1280, height: 900 },
  { name: 'mobile', width: 390, height: 844 },
];

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

const failures = [];
let port = 17330 + Math.floor(Math.random() * 500);
for (const c of CASES) {
  const dir = join(work, c.name);
  encode(dir, c);
  if (c.rotate) setRotate90(join(dir, 'init-1.mp4'));
  for (const vp of VIEWPORTS) {
    port += 1;
    const gw = spawn(process.execPath, [join(here, 'fake-gateway.mjs'), dir, String(port)], {
      env: { ...process.env, START_DELAY_MS: '500' },
      stdio: 'ignore',
    });
    const shot = join(outDir, `${c.name}-${vp.name}.png`);
    // A fresh profile per run: no remembered debug panel or settings.
    const profile = join(work, `profile-${c.name}-${vp.name}`);
    const r = await run('xvfb-run', ['-a', electron, '--no-sandbox', `--user-data-dir=${profile}`, join(here, 'freedom-harness.cjs')], {
      GATEWAY: `http://127.0.0.1:${port}`,
      WAIT_S: '12',
      MEASURE: '1',
      OUT: shot,
      WIDTH: String(vp.width),
      HEIGHT: String(vp.height),
      SWITCH_TO: 'd'.repeat(64),
    });
    gw.kill();
    const label = `${c.name}/${vp.name}`;
    let state;
    try {
      state = JSON.parse(r.out.slice(r.out.indexOf('{')));
    } catch (_) {
      failures.push(`${label}: harness gave no state (exit ${r.code}): ${r.err.slice(-400)}`);
      continue;
    }
    const g = state.geometry;
    const check = (ok, what) => {
      if (!ok) failures.push(`${label}: ${what}`);
      return ok;
    };
    const reported = g.videoWidth / g.videoHeight;
    const box = g.video.width / g.video.height;
    const disc = g.disc;
    console.log(
      `${label.padEnd(20)} video ${g.videoWidth}×${g.videoHeight}  box ${g.video.width.toFixed(0)}×${g.video.height.toFixed(0)}`
      + `  disc ${disc ? `${disc.width}×${disc.height} at ${disc.cx.toFixed(2)},${disc.cy.toFixed(2)}` : 'none'}`
      + `  overflow ${g.wrap.width > g.viewport.width + 1 ? 'yes' : 'no'}  → ${shot}`,
    );
    check(state.decodedFrames > 0, `no frames decoded (${state.status})`);
    check(Math.abs(reported - c.display) < 0.02, `reported size ${g.videoWidth}×${g.videoHeight} is not the display aspect ${c.display.toFixed(3)}`);
    check(Math.abs(box - c.display) < 0.03, `element box ${box.toFixed(3)} ≠ display aspect ${c.display.toFixed(3)}`);
    check((box < 1) === (c.display < 1), `element box is ${box < 1 ? 'portrait' : 'landscape'}`);
    check(g.video.height <= g.viewport.height * 0.8 + 2, `element ${g.video.height}px tall, over 80vh of ${g.viewport.height}`);
    check(g.wrap.width <= g.viewport.width + 1, `stage ${g.wrap.width}px wider than the ${g.viewport.width}px viewport`);
    const after = state.afterSwitch;
    if (check(after, 'no state after switching channel')) {
      check(Math.abs(after.width / after.height - 16 / 9) < 0.03,
        `stage ${after.width.toFixed(0)}×${after.height.toFixed(0)} after opening a channel that never loads, not back to 16:9`);
    }
    if (check(disc, 'disc not found in the picture')) {
      check(Math.abs(disc.width / disc.height - 1) < 0.08, `disc ${disc.width}×${disc.height}: picture is stretched`);
      check(Math.abs(disc.cx - 0.5) < 0.05 && Math.abs(disc.cy - 0.5) < 0.05, `disc off-centre (${disc.cx.toFixed(2)}, ${disc.cy.toFixed(2)})`);
      // The disc spans 2R of the picture's short side: if the element box
      // is the picture, the disc fills the matching share of the box.
      const short = Math.min(c.width, c.height);
      const share = disc.width / Math.min(g.capture.width, g.capture.height);
      check(Math.abs(share - (2 * R) / short) < 0.06, `disc fills ${share.toFixed(2)} of the box's short side, picture would give ${((2 * R) / short).toFixed(2)} (letterboxed?)`);
    }
  }
}

if (failures.length) {
  console.error(`\nFAIL\n${failures.map((f) => `  ${f}`).join('\n')}`);
  process.exit(1);
}
console.log(`\nPASS (${CASES.length * VIEWPORTS.length} runs; screenshots in ${outDir}: ${readdirSync(outDir).filter((f) => f.endsWith('.png')).join(', ')})`);
