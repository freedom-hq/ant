#!/usr/bin/env node
// The start page's recent-channel dropdown, menu and help, driven from the
// keyboard and the mouse.
//
//   node test/start-page-test.mjs
//
// Needs xvfb-run and Electron (ELECTRON=<path to the binary>, or
// `electron` on PATH), as test/portrait-test.mjs does. It loads the player
// in test/freedom-harness.cjs with no channel and sends real input events
// (the harness's STEPS) to check that:
//
//  - with no recent channels the field opens no list (aria-expanded stays
//    false, the listbox stays hidden);
//  - with some, focusing or clicking the empty field opens them, newest
//    first, a known title shown over the shortened owner and the topic;
//  - ArrowDown/ArrowUp move aria-activedescendant through the options and
//    wrap, Escape closes the list and leaves the focus in the field,
//    typing filters, Delete removes the highlighted entry (from the list
//    and from localStorage), the × does the same with the mouse, and
//    Enter watches the highlighted channel (the page's hash becomes its
//    key) and closes the list;
//  - Tab goes field → Watch → Menu;
//  - the Menu button opens the menu with the focus on Help, Escape closes
//    it back onto the button, a click outside closes it, so does the Menu
//    button itself (also when, as in Safari, the press blurs Help first), Help opens the
//    help dialog (also when, as in Safari, the press on Help itself
//    clears the focus before the click) and Escape closes it back onto
//    the Menu button.
//
// At a desktop window (1280×900) and a phone-sized one (390×844), where
// the list must also stay clear of the Watch button and inside the
// viewport. Screenshots go to $OUT_DIR (default: a temp dir).

import { spawn } from 'node:child_process';
import { mkdtempSync, mkdirSync, writeFileSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const electron = process.env.ELECTRON || 'electron';
const work = mkdtempSync(join(process.env.TMPDIR || tmpdir(), 'antstream-start-'));
const outDir = process.env.OUT_DIR || work;
mkdirSync(outDir, { recursive: true });

const PAGE = `bzz://${'a'.repeat(64)}/`;
const A = '0xd5572300e441b77b72bdd318bbfa7b97a1f03096/antstream';
const B = '0x1111111111111111111111111111111111111111/cooking%20show';
const C = `0x2222222222222222222222222222222222222222/0x${'ab'.repeat(32)}`;
// Newest first, as stored: the keys as bare strings (the shape older
// builds read too), A's catalog title beside them.
const SEED = [A, B, C];
const SEED_TITLES = { [A]: 'Morning walk' };

const VIEWPORTS = [
  { name: 'desktop', width: 1280, height: 900 },
  { name: 'mobile', width: 390, height: 844 },
];

// What the steps read back, by name.
const PROBE = `(() => {
  const i = document.getElementById('channel-input');
  const l = document.getElementById('recent-list');
  const opts = [...l.querySelectorAll('[role=option]')];
  const box = (e) => { const r = e.getBoundingClientRect(); return { left: r.left, right: r.right, top: r.top, bottom: r.bottom }; };
  return {
    expanded: i.getAttribute('aria-expanded'),
    listHidden: l.hidden,
    active: i.getAttribute('aria-activedescendant'),
    selected: opts.filter((o) => o.getAttribute('aria-selected') === 'true').map((o) => o.id),
    options: opts.map((o) => o.innerText.replace(/\\s+/g, ' ').trim()),
    focus: document.activeElement && (document.activeElement.id || document.activeElement.tagName),
    stored: JSON.parse(localStorage.getItem('antstream.recent.v2') || '[]'),
    titles: JSON.parse(localStorage.getItem('antstream.recent-titles.v1') || '{}'),
    hash: decodeURIComponent(location.hash.slice(1)),
    menuHidden: document.getElementById('menu').hidden,
    menuExpanded: document.getElementById('menu-button').getAttribute('aria-expanded'),
    helpOpen: document.getElementById('help').open,
    focusInHelp: document.getElementById('help').contains(document.activeElement),
    list: l.hidden ? null : box(l),
    watch: box(document.querySelector('#channel-form button[type=submit]')),
    viewport: innerWidth,
    overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth,
  };
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

const failures = [];
for (const vp of VIEWPORTS) {
  const label = vp.name;
  const steps = [];
  const probes = [];
  const probe = (name) => { probes.push(name); steps.push({ js: PROBE, name }); };
  const shot = (name) => steps.push({ shot: `${name}-${vp.name}.png` });

  // No recent channels.
  steps.push({ js: "localStorage.removeItem('antstream.recent.v2'); localStorage.removeItem('antstream.recent-titles.v1')" }, { load: PAGE }, { wait: 300 });
  steps.push({ click: '#channel-input' });
  probe('empty');
  shot('start-empty');
  // Seeded.
  steps.push({
    js: `localStorage.setItem('antstream.recent.v2', ${JSON.stringify(JSON.stringify(SEED))}); localStorage.setItem('antstream.recent-titles.v1', ${JSON.stringify(JSON.stringify(SEED_TITLES))})`,
  }, { load: PAGE }, { wait: 300 });
  steps.push({ click: '#channel-input' });
  probe('opened');
  shot('start-dropdown');
  steps.push({ key: 'Down' });
  probe('down1');
  steps.push({ key: 'Down' }, { key: 'Down' });
  probe('down3');
  steps.push({ key: 'Down' });
  probe('wrapDown');
  steps.push({ key: 'Up' });
  probe('wrapUp');
  steps.push({ key: 'Escape' });
  probe('escaped');
  steps.push({ key: 'Down' });
  probe('reopened');
  steps.push({ type: 'cooking' });
  probe('filtered');
  steps.push({ key: 'Down' }, { key: 'Delete' });
  probe('deleted');
  steps.push({ key: 'A', modifiers: ['control'] }, { key: 'Backspace' });
  probe('cleared');
  steps.push({ click: '#recent-opt-1 .opt-remove' });
  probe('crossed');
  steps.push({ key: 'Down' }, { key: 'Enter' }, { wait: 300 });
  probe('picked');
  // Tab order from the field (back on the start page, nothing typed).
  steps.push({ load: PAGE }, { wait: 300 }, { click: '#channel-input' }, { key: 'Tab' });
  probe('tab1');
  steps.push({ key: 'Tab' });
  probe('tab2');
  // Menu and help.
  steps.push({ key: 'Enter' });
  probe('menuOpen');
  shot('menu');
  steps.push({ key: 'Escape' });
  probe('menuEscaped');
  steps.push({ click: '#menu-button' }, { at: [10, Math.round(vp.height * 0.7)] });
  probe('menuOutside');
  // The Menu button toggles it shut again.
  steps.push({ click: '#menu-button' }, { click: '#menu-button' });
  probe('menuToggled');
  // As Safari does it: a press on a button focuses nothing, so Help blurs
  // with no relatedTarget before the click.
  steps.push({ click: '#menu-button' }, {
    js: `document.getElementById('menu-button').addEventListener('mousedown', (e) => {
      e.preventDefault();
      document.activeElement.blur();
    }, { capture: true, once: true })`,
  }, { click: '#menu-button' });
  probe('menuToggledWebKit');
  steps.push({ click: '#menu-button' }, { key: 'Enter' }, { wait: 200 });
  probe('helpOpen');
  shot('help');
  steps.push({ key: 'Escape' }, { wait: 200 });
  probe('helpEscaped');
  // Safari again: the press on Help clears the focus (a focusout with no
  // relatedTarget) before its click lands. The menu must still be there
  // for the click, and Help must open.
  steps.push({ click: '#menu-button' }, {
    js: `document.getElementById('help-open').addEventListener('mousedown', (e) => {
      e.preventDefault();
      document.activeElement.blur();
    }, { capture: true, once: true })`,
  }, { click: '#help-open' }, { wait: 200 });
  probe('helpOpenWebKit');
  steps.push({ key: 'Escape' }, { wait: 200 });

  const stepsFile = join(work, `steps-${vp.name}.json`);
  writeFileSync(stepsFile, JSON.stringify(steps));
  const r = await run('xvfb-run', ['-a', electron, '--no-sandbox', `--user-data-dir=${join(work, `profile-${vp.name}`)}`, join(here, 'freedom-harness.cjs')], {
    // Nothing listens there: a picked channel is only looked for.
    GATEWAY: 'http://127.0.0.1:9',
    URL: PAGE,
    WAIT_S: '1',
    STEPS: stepsFile,
    STEPS_OUT: outDir,
    WIDTH: String(vp.width),
    HEIGHT: String(vp.height),
  });
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
  const open = (s) => s && s.expanded === 'true' && !s.listHidden;
  const closed = (s) => s && s.expanded === 'false' && s.listHidden && !s.active;

  check(closed(p.empty) && p.empty.options.length === 0, `no recent channels, yet the list opened: ${JSON.stringify(p.empty)}`);
  if (check(open(p.opened) && p.opened.options.length === 3, `focusing the empty field did not open the 3 recent channels: ${JSON.stringify(p.opened)}`)) {
    check(p.opened.options[0].includes('Morning walk') && p.opened.options[0].includes('0xd557…3096 / antstream'),
      `first option is not the titled channel with its short owner: ${p.opened.options[0]}`);
    check(p.opened.options[1].includes('0x1111…1111 / cooking show'), `second option: ${p.opened.options[1]}`);
    check(p.opened.options[2].includes(`0x2222…2222 / 0x${'ab'.repeat(32)}`), `third option: ${p.opened.options[2]}`);
    check(!p.opened.active, 'an option is active before any arrow key');
    const l = p.opened.list;
    check(l.right <= p.opened.watch.left + 1 || l.top >= p.opened.watch.bottom - 1, `the list covers the Watch button: list ${JSON.stringify(l)}, Watch ${JSON.stringify(p.opened.watch)}`);
    check(l.left >= 0 && l.right <= p.opened.viewport + 1 && p.opened.overflow === 0, `the list leaves the viewport: ${JSON.stringify(l)}, overflow ${p.opened.overflow}`);
  }
  check(p.down1.active === 'recent-opt-0' && p.down1.selected.join() === 'recent-opt-0', `ArrowDown: ${JSON.stringify([p.down1.active, p.down1.selected])}`);
  check(p.down3.active === 'recent-opt-2', `ArrowDown ×3: ${p.down3.active}`);
  check(p.wrapDown.active === 'recent-opt-0', `ArrowDown past the last did not wrap: ${p.wrapDown.active}`);
  check(p.wrapUp.active === 'recent-opt-2', `ArrowUp past the first did not wrap: ${p.wrapUp.active}`);
  check(closed(p.escaped) && p.escaped.focus === 'channel-input', `Escape: ${JSON.stringify(p.escaped)}`);
  check(open(p.reopened) && p.reopened.active === 'recent-opt-0', `ArrowDown on the closed list: ${JSON.stringify([p.reopened.expanded, p.reopened.active])}`);
  check(open(p.filtered) && p.filtered.options.length === 1 && p.filtered.options[0].includes('cooking show') && !p.filtered.active,
    `typing "cooking" did not narrow the list to that channel: ${JSON.stringify(p.filtered.options)}`);
  const keys = (s) => (s.stored.every((e) => typeof e === 'string') ? s.stored : ['(not a string list)', ...s.stored]);
  check(closed(p.deleted) && keys(p.deleted).join() === [A, C].join(), `Delete did not remove the highlighted channel: ${JSON.stringify(p.deleted.stored)}`);
  check(p.deleted.titles[A] === 'Morning walk', `removing an entry lost another's title: ${JSON.stringify(p.deleted.titles)}`);
  check(open(p.cleared) && p.cleared.options.length === 2, `clearing the field did not show the rest: ${JSON.stringify(p.cleared.options)}`);
  check(open(p.crossed) && keys(p.crossed).join() === A && p.crossed.options.length === 1 && p.crossed.focus === 'channel-input',
    `× did not remove the channel and keep the list: ${JSON.stringify(p.crossed)}`);
  check(closed(p.picked) && p.picked.hash === A, `Enter on an option did not watch it: hash ${p.picked.hash}, ${JSON.stringify(p.picked)}`);
  check(p.tab1.focus === 'BUTTON' && closed(p.tab1), `Tab from the field went to ${p.tab1.focus}`);
  check(p.tab2.focus === 'menu-button', `second Tab went to ${p.tab2.focus}, not the Menu button`);
  check(!p.menuOpen.menuHidden && p.menuOpen.menuExpanded === 'true' && p.menuOpen.focus === 'help-open', `menu: ${JSON.stringify(p.menuOpen)}`);
  check(p.menuEscaped.menuHidden && p.menuEscaped.menuExpanded === 'false' && p.menuEscaped.focus === 'menu-button', `Escape on the menu: ${JSON.stringify(p.menuEscaped)}`);
  check(p.menuOutside.menuHidden, 'a click outside left the menu open');
  check(p.menuToggled.menuHidden && p.menuToggled.menuExpanded === 'false' && p.menuToggled.focus === 'menu-button',
    `the Menu button did not close its open menu: ${JSON.stringify(p.menuToggled)}`);
  check(p.menuToggledWebKit.menuHidden && p.menuToggledWebKit.menuExpanded === 'false',
    `with Safari's focus on a press, the Menu button reopened its menu: ${JSON.stringify(p.menuToggledWebKit)}`);
  check(p.helpOpen.helpOpen && p.helpOpen.focusInHelp && p.helpOpen.menuHidden, `Help: ${JSON.stringify(p.helpOpen)}`);
  check(!p.helpEscaped.helpOpen && p.helpEscaped.focus === 'menu-button', `Escape on the help: ${JSON.stringify(p.helpEscaped)}`);
  check(p.helpOpenWebKit.helpOpen && p.helpOpenWebKit.menuHidden,
    `with Safari's focus cleared on the press, Help did not open: ${JSON.stringify(p.helpOpenWebKit)}`);
  console.log(`${label.padEnd(8)} ${failures.length ? 'see failures' : 'ok'}`);
}

if (failures.length) {
  console.error(`\nFAIL\n${failures.map((f) => `  ${f}`).join('\n')}`);
  process.exit(1);
}
console.log(`\nPASS (${VIEWPORTS.length} runs; screenshots in ${outDir}: ${readdirSync(outDir).filter((f) => f.endsWith('.png')).join(', ')})`);
