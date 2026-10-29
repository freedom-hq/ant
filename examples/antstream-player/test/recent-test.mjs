#!/usr/bin/env node
// The recent-channel list the start page's dropdown shows, as kept in
// localStorage (`antstream.recent.v2`).
//
//   node test/recent-test.mjs
//
// No dependencies, no browser: as test/channel-parse-test.mjs does, it
// runs player.js's own code (the channel parser and the "recent channels"
// block, cut out of the file as they are) against a stand-in
// localStorage, and checks that:
//
//  - the bare key strings older builds stored still read, without a title,
//    and side by side with `{ key, title }` entries;
//  - unparseable, duplicate and malformed entries are dropped, and an
//    unreadable store reads as empty;
//  - playing a channel puts it first and keeps its known title, a
//    catalog title is stored next to its key (and only for a listed
//    channel), and the list stays at 8;
//  - forget() removes exactly the one entry;
//  - matchRecent() filters on title, owner and topic, any case.

import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import vm from 'node:vm';
import assert from 'node:assert/strict';

const here = dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);
const K = require(join(here, '../keccak.js'));
const src = readFileSync(join(here, '../player.js'), 'utf8');

/** The source from the line starting with `from` up to (not including) `to`. */
function cut(from, to) {
  const a = src.indexOf(from);
  const b = src.indexOf(to, a);
  if (a < 0 || b < 0) throw new Error(`player.js: could not find ${JSON.stringify(from)}…${JSON.stringify(to)}`);
  return src.slice(a, b);
}

const code = [
  cut('  const OWNER = ', '\n  const ANTSTREAM_TOPIC'),
  cut('  const ANTSTREAM_TOPIC = ', '\n') + '\n',
  cut('  /**\n   * A channel from what the viewer pasted', '  // -- settings and debug log'),
  cut('  // -- recent channels', '  // -- the recent dropdown'),
  'Object.assign(globalThis, { recent, remember, rememberTitle, forget, matchRecent, shortOwner, RECENT_KEY });',
].join('\n');

const store = new Map();
const localStorage = {
  getItem: (k) => (store.has(k) ? store.get(k) : null),
  setItem: (k, v) => store.set(k, String(v)),
  removeItem: (k) => store.delete(k),
};
const ctx = { window: { antstreamKeccak: K }, K, localStorage };
vm.createContext(ctx);
vm.runInContext(code, ctx);
const P = ctx;
const KEY = P.RECENT_KEY;
assert.equal(KEY, 'antstream.recent.v2');
const set = (v) => store.set(KEY, typeof v === 'string' ? v : JSON.stringify(v));
const stored = () => JSON.parse(store.get(KEY));
const plain = (v) => JSON.parse(JSON.stringify(v));

const A = '0xd5572300e441b77b72bdd318bbfa7b97a1f03096/antstream';
const B = '0x1111111111111111111111111111111111111111/cooking%20show';
const C = `0x2222222222222222222222222222222222222222/0x${'ab'.repeat(32)}`;
let n = 0;
const test = (name, fn) => {
  store.clear();
  fn();
  n += 1;
  console.log(`ok  ${name}`);
};

test('old string entries still read', () => {
  set([A, B]);
  assert.deepEqual(plain(P.recent()), [{ key: A, title: '' }, { key: B, title: '' }]);
});

test('string and titled entries side by side', () => {
  set([{ key: A, title: 'Morning walk' }, B, { key: C }]);
  assert.deepEqual(plain(P.recent()), [{ key: A, title: 'Morning walk' }, { key: B, title: '' }, { key: C, title: '' }]);
});

test('bad entries are dropped', () => {
  set([`#${'a'.repeat(64)}`, null, 7, { title: 'no key' }, { key: 'nonsense' }, A, { key: A, title: 'dup' }, B]);
  assert.deepEqual(plain(P.recent()), [{ key: A, title: '' }, { key: B, title: '' }]);
  set('{not json');
  assert.deepEqual(plain(P.recent()), []);
  set({ key: A });
  assert.deepEqual(plain(P.recent()), []);
  assert.deepEqual(plain(P.recent()), []);
});

test('remember puts a channel first and keeps its title', () => {
  set([{ key: A, title: 'Morning walk' }, B]);
  P.remember(B);
  assert.deepEqual(stored(), [{ key: B, title: '' }, { key: A, title: 'Morning walk' }]);
  P.remember(A);
  assert.deepEqual(stored(), [{ key: A, title: 'Morning walk' }, { key: B, title: '' }]);
});

test('an old list is rewritten with its entries kept', () => {
  set([A, B]);
  P.remember(C);
  assert.deepEqual(stored(), [{ key: C, title: '' }, { key: A, title: '' }, { key: B, title: '' }]);
});

test('the list keeps the newest 8', () => {
  for (let i = 0; i < 10; i++) P.remember(`0x${String(i).repeat(40)}/t`);
  const keys = stored().map((e) => e.key);
  assert.equal(keys.length, 8);
  assert.equal(keys[0], `0x${'9'.repeat(40)}/t`);
  assert.ok(!keys.includes(`0x${'0'.repeat(40)}/t`));
});

test('a catalog title is stored next to its key', () => {
  set([A, B]);
  P.rememberTitle(B, '  Cooking show  ');
  assert.deepEqual(stored(), [{ key: A, title: '' }, { key: B, title: 'Cooking show' }]);
  // A newer title replaces it; an empty one keeps it.
  P.rememberTitle(B, 'Cooking show, part 2');
  P.rememberTitle(B, '');
  assert.equal(stored()[1].title, 'Cooking show, part 2');
  // Only for a channel that is in the list, and the order stays.
  P.rememberTitle(C, 'Not listed');
  assert.deepEqual(stored().map((e) => e.key), [A, B]);
});

test('forget removes exactly the one entry', () => {
  set([{ key: A, title: 'Morning walk' }, B, C]);
  P.forget(B);
  assert.deepEqual(stored(), [{ key: A, title: 'Morning walk' }, { key: C, title: '' }]);
  P.forget(B);
  assert.equal(stored().length, 2);
  P.forget(A);
  P.forget(C);
  assert.deepEqual(stored(), []);
  assert.deepEqual(plain(P.recent()), []);
});

test('matchRecent filters on title, owner and topic', () => {
  const list = [{ key: A, title: 'Morning walk' }, { key: B, title: '' }, { key: C, title: '' }];
  const keys = (q) => plain(P.matchRecent(list, q).map((m) => m.key));
  assert.deepEqual(keys(''), [A, B, C]);
  assert.deepEqual(keys('  '), [A, B, C]);
  assert.deepEqual(keys('MORNING'), [A]);
  assert.deepEqual(keys('cooking show'), [B]);
  assert.deepEqual(keys('0x2222'), [C]);
  assert.deepEqual(keys('abab'), [C]);
  assert.deepEqual(keys('d5572300'), [A]);
  assert.deepEqual(keys('nothing like it'), []);
  const m = P.matchRecent(list, 'cooking')[0];
  assert.equal(m.ch.topic, 'cooking show');
  assert.equal(P.shortOwner(m.ch.owner), '0x1111…1111');
});

console.log(`\nPASS (${n} tests)`);
