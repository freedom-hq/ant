#!/usr/bin/env node
// The channel field and the page's hash read the channel the apps copy or
// share (issue #222).
//
//   node test/channel-parse-test.mjs
//
// No dependencies, no browser: the test runs player.js's own
// `parseChannel` (and the helpers and constants it uses, cut out of the
// file as they are) against the exact strings the iPhone app's
// `ChannelAddress` builds (examples/ios-stream/AntStream/ChannelAddress.swift,
// the same text as Android's `ChannelAddress`): `playerLink`, `swarmURI`,
// `key`/`display`, `watchLinks` (the Share text) with its line breaks and
// with them dropped, as a text input does on paste, and an owner alone.

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
  cut('  const OWNER = ', '\n  const RECENT_KEY'),
  cut('  const ANTSTREAM_TOPIC = ', '\n') + '\n',
  cut('  /**\n   * A channel from what the viewer pasted', '  // -- settings and debug log'),
  'globalThis.parseChannel = parseChannel;',
].join('\n');
const ctx = { K };
vm.createContext(ctx);
vm.runInContext(code, ctx, { filename: 'player.js (parseChannel)' });
const { parseChannel } = ctx;

// -- what the iPhone app builds (ChannelAddress.swift) ----------------------

const PLAYER_REF = 'b268072928bd99d637992eb54c2a826b83affa36f61cf495297541ac42857c35';
const PUBLIC_GATEWAY = 'https://api.gateway.ethswarm.org';
const OWNER = '9f2c1a7e5b3d4c6a8e0f1b2d3c4e5f6a7b8c9d0e';
const RAW = '0123456789abcdef'.repeat(4);
const OTHER = '1111111111111111111111111111111111111111';

/** `ChannelAddress(owner:text:)` / `(owner:rawTopic:)`'s strings. */
function app(owner, topic) {
  const raw = topic.raw;
  const key = raw ? `0x${owner}/0x${raw}` : `0x${owner}/${encodeURIComponent(topic.text)}`;
  const display = raw ? `0x${owner}/0x${raw}` : `0x${owner}/${topic.text}`;
  const playerLink = `bzz://${PLAYER_REF}/#${key}`;
  const gatewayPlayerLink = `${PUBLIC_GATEWAY}/bzz/${PLAYER_REF}/#${key}`;
  const swarmURI = raw ? null : `swarm://${owner}/${topic.text}`;
  const lines = [
    'Watch my live stream on Swarm:',
    `Freedom browser: ${playerLink}`,
    `Any browser: ${gatewayPlayerLink}`,
  ];
  if (swarmURI) lines.push(`Solar Punk players: ${swarmURI}`);
  lines.push(`Channel: ${display}`);
  return { key, display, playerLink, gatewayPlayerLink, swarmURI, watchLinks: lines.join('\n') };
}

/** What an `<input>` keeps of pasted text: the value with line breaks stripped. */
const inputValue = (text) => text.replace(/[\r\n]/g, '');

let failures = 0;
let passed = 0;

function expect(name, input, want) {
  let got;
  try {
    got = parseChannel(input);
    if (want === null) {
      assert.equal(got, null);
    } else {
      assert.ok(got, 'no channel');
      assert.equal(`0x${got.owner}/${got.topic}`, want.display);
      assert.equal(got.key, want.key);
      assert.equal(got.topicHex, want.topicHex);
    }
    passed++;
    console.log(`ok   ${name}`);
  } catch (err) {
    failures++;
    const shown = got ? `0x${got.owner}/${got.topic} (key ${got.key})` : String(got);
    console.log(`FAIL ${name}\n     input ${JSON.stringify(input)}\n     got   ${shown}\n     ${err.message.split('\n')[0]}`);
  }
}

const channel = (owner, topic) => {
  const a = app(owner, topic);
  return {
    display: a.display,
    key: a.key,
    topicHex: topic.raw || K.topicFromString(topic.text),
  };
};

// -- the issue's table: every form plays 0x<owner>/antstream --------------

const ant = app(OWNER, { text: 'antstream' });
const antWant = channel(OWNER, { text: 'antstream' });

expect('web player link (Freedom)', ant.playerLink, antWant);
expect('web player link (any browser)', ant.gatewayPlayerLink, antWant);
expect('Solar Punk URI', ant.swarmURI, antWant);
expect('0x<owner>/antstream', ant.display, antWant);
expect('Share text, with line breaks', ant.watchLinks, antWant);
expect('Share text, line breaks stripped by the input', inputValue(ant.watchLinks), antWant);
expect('Share text, line breaks as spaces', ant.watchLinks.replace(/\n/g, ' '), antWant);
expect('Share text, CRLF line breaks', ant.watchLinks.replace(/\n/g, '\r\n'), antWant);
expect('owner alone, 0x<owner>', `0x${OWNER}`, antWant);
expect('owner alone, <owner>', OWNER, antWant);
expect('owner alone, 0x<owner>/', `0x${OWNER}/`, antWant);
expect('owner alone, upper-case', `0X${OWNER.toUpperCase()}`, antWant);
expect('owner alone, as the hash of a link', `bzz://${PLAYER_REF}/#0x${OWNER}`, antWant);
expect('owner alone, in text', `my channel: 0x${OWNER} — come watch`, antWant);
expect('Share text pasted into a message', `hey!\n\n${ant.watchLinks}\n\nsee you there`, antWant);
expect('only the Any browser line', `Any browser: ${ant.gatewayPlayerLink}`, antWant);
expect('only the Solar Punk line', `Solar Punk players: ${ant.swarmURI}`, antWant);

// -- a truncated swarm:// URI stays invalid, as in the apps ---------------

expect('truncated swarm://<owner>/', `swarm://${OWNER}/`, null);
expect('truncated swarm://<owner>', `swarm://${OWNER}`, null);
expect('truncated swarm://<owner>/ in text', `Solar Punk players: swarm://${OWNER}/`, null);
expect('truncated swarm://0x<owner>/ in text', `swarm://0x${OWNER}/ more`, null);
expect('truncated swarm://0x<owner> in text', `Solar Punk players: swarm://0x${OWNER} more`, null);
expect('truncated swarm:// before a real channel', `swarm://0x${OWNER}/ or 0x${OTHER}/antstream`, channel(OTHER, { text: 'antstream' }));

// -- an owner alone before a channel with a topic is not the channel ------

expect('explorer link with a sub-path before the channel', `https://gnosisscan.io/address/0x${OTHER}/tokens - watch 0x${OWNER}/antstream`, antWant);
expect('explorer link with a sub-path alone', `see https://gnosisscan.io/address/0x${OTHER}/tokens`, null);
expect('tip-jar address before the channel', `Tip jar 0x${OTHER} - watch 0x${OWNER}/antstream`, antWant);
expect('owner alone before a named topic', `0x${OTHER} then 0x${OWNER}/news`, channel(OWNER, { text: 'news' }));
expect('owner alone before the Share text', `tips: 0x${OTHER}\n${ant.watchLinks}`, antWant);
expect('owner alone before the stripped Share text', `tips: 0x${OTHER} ${inputValue(ant.watchLinks)}`, antWant);
expect('two different owners alone are ambiguous', `0x${OTHER} or 0x${OWNER}`, null);
expect('the same owner alone twice', `0x${OWNER} (0x${OWNER})`, antWant);

// -- one quoted link alone ------------------------------------------------

expect('quoted link, straight quotes', `"0x${OWNER}/antstream"`, antWant);
expect('quoted link, curly quotes', `\u201c0x${OWNER}/antstream\u201d`, antWant);
expect('quoted link, single quotes', `'${ant.playerLink}'`, antWant);
expect('quoted owner alone', `"0x${OWNER}"`, antWant);
expect('quoted truncated swarm://', `"swarm://${OWNER}/"`, null);
expect('owner too short', `0x${OWNER.slice(1)}`, null);
expect('older manifest link (#<64 hex>)', `bzz://${PLAYER_REF}/#${RAW}`, null);
expect('text without a channel', 'Watch my live stream on Swarm: soon', null);
expect('empty', '   ', null);
expect('a 64-hex reference in text is no owner', `see 0x${RAW} please`, null);

// -- what worked before keeps working --------------------------------------

const textTopics = ['antstream', 'antstream/My Channel', 'Café ☕ live', '50% off', 'a#b?c'];
for (const text of textTopics) {
  const a = app(OWNER, { text });
  const want = channel(OWNER, { text });
  expect(`hash link, text topic ${JSON.stringify(text)}`, a.key, want);
  expect(`player link, text topic ${JSON.stringify(text)}`, a.playerLink, want);
}
// Every topic whose Share text the field can read: one with no whitespace.
for (const text of ['antstream-4f1c2b9e', 'Café☕', 'a/b']) {
  const a = app(OWNER, { text });
  const want = channel(OWNER, { text });
  expect(`Share text, text topic ${JSON.stringify(text)}`, a.watchLinks, want);
  expect(`Share text stripped, text topic ${JSON.stringify(text)}`, inputValue(a.watchLinks), want);
}

// A topic with a space: the links carry it URL-encoded, the Solar Punk and
// Channel lines as it is, cut at the space. The Channel line is the same
// channel cut short, not a shorter one the links were glued to.
for (const text of ['my show', 'antstream/my show']) {
  const a = app(OWNER, { text });
  const want = channel(OWNER, { text });
  expect(`Share text, text topic with a space ${JSON.stringify(text)}`, a.watchLinks, want);
  expect(`Share text stripped, text topic with a space ${JSON.stringify(text)}`, inputValue(a.watchLinks), want);
  expect(`Share text as spaces, text topic with a space ${JSON.stringify(text)}`, a.watchLinks.replace(/\n/g, ' '), want);
}

// The Channel line carries the topic as it is, not URL-encoded: with the
// line breaks dropped it must still stand for the topic a literal `%XX`
// escape or a `/ ` in it spells, not that text decoded or cut at its `/`
// (R5-M1).
for (const text of ['100%25', 'a%2Fb', '%41', 'news/ today', 'a%20b c']) {
  const a = app(OWNER, { text });
  const want = channel(OWNER, { text });
  expect(`Share text stripped, topic as escapes or a slash ${JSON.stringify(text)}`, inputValue(a.watchLinks), want);
  expect(`Share text as spaces, topic as escapes or a slash ${JSON.stringify(text)}`, a.watchLinks.replace(/\n/g, ' '), want);
}

// Only a link glued to the next Share-text line's label stands for a
// later, shorter mention of its channel: otherwise the first channel in the
// text is the one meant, even when a later one's topic starts it (R4-M1).
const music = channel(OWNER, { text: 'antstream-music' });
expect('first channel, a later one a prefix of it', `My channels: 0x${OWNER}/antstream-music and 0x${OWNER}/antstream`, music);
expect('player link, a later bare one a prefix of it', `watch https://gw/bzz/ref/#0x${OWNER}/music and also 0x${OWNER}/mus`, channel(OWNER, { text: 'music' }));
expect('two lines, nothing glued', `0x${OWNER}/antstream/live\n0x${OWNER}/antstream`, channel(OWNER, { text: 'antstream/live' }));
expect('a topic ending in a label word, not glued', `0x${OWNER}/antstreamAny and 0x${OWNER}/antstream`, channel(OWNER, { text: 'antstreamAny' }));
for (const text of ['antstream', 'Café☕', 'show.']) {
  const a = app(OWNER, { text });
  const want = channel(OWNER, { text });
  expect(`Share text stripped from the Any browser line ${JSON.stringify(text)}`, inputValue(a.watchLinks.split('\n').slice(2).join('\n')), want);
  expect(`Share text stripped from the Solar Punk line ${JSON.stringify(text)}`, inputValue(a.watchLinks.split('\n').slice(3).join('\n')), want);
}

// The same, for a topic that also ends in sentence punctuation: the
// Channel line and the URI, as written, still witness its end when the
// line breaks are kept or turned into spaces (R6-M1).
for (const text of ['%2Ay.', 'y%2A#)', '?/ 2!', '#:/ )-)', '100%25.']) {
  const a = app(OWNER, { text });
  const want = channel(OWNER, { text });
  expect(`Share text, escapes or a slash, ending in punctuation ${JSON.stringify(text)}`, a.watchLinks, want);
  expect(`Share text stripped, escapes or a slash, ending in punctuation ${JSON.stringify(text)}`, inputValue(a.watchLinks), want);
  expect(`Share text as spaces, escapes or a slash, ending in punctuation ${JSON.stringify(text)}`, a.watchLinks.replace(/\n/g, ' '), want);
}

// A channel ending a sentence keeps its topic, not the sentence's end.
expect('channel ending a sentence', `come watch 0x${OWNER}/antstream.`, antWant);
expect('channel in quotes', `link: "0x${OWNER}/antstream", see you`, antWant);
expect('channel in brackets', `tonight (0x${OWNER}/antstream)!`, antWant);
expect('channel ending a question', `seen 0x${OWNER}/antstream?`, antWant);
expect('Solar Punk URI ending a sentence', `or swarm://${OWNER}/antstream.`, antWant);
expect('owner alone ending a sentence', `my channel is 0x${OWNER}.`, antWant);
expect('balanced brackets stay in the topic', `watch 0x${OWNER}/show(1) now`, channel(OWNER, { text: 'show(1)' }));
expect('a link alone keeps a trailing dot', `0x${OWNER}/v1.`, channel(OWNER, { text: 'v1.' }));
expect('two mentions, each ending its own sentence', `watch 0x${OWNER}/antstream, or 0x${OWNER}/antstream.`, antWant);
// The same form twice is two sentences, not the Share text: neither keeps
// the other's sentence end (R3-M1).
expect('same channel twice, both ending a sentence', `see 0x${OWNER}/antstream. — yes 0x${OWNER}/antstream.`, antWant);
expect('same form twice, the later topic longer', `0x${OWNER}/antstream. My other: 0x${OWNER}/antstream.v2`, antWant);
expect('swarm:// twice, both ending a sentence', `swarm://${OWNER}/antstream. Or swarm://${OWNER}/antstream.`, antWant);

// A topic that itself ends in what `encodeURIComponent` leaves unencoded
// (`.!'()*~`): every mention in the Share text ends in it, so it is the
// topic's, however the line breaks came through.
for (const text of ['show.', 'my show!', 'a)', "it's'", 'v1.0.']) {
  const a = app(OWNER, { text });
  const want = channel(OWNER, { text });
  expect(`Share text, topic ending in punctuation ${JSON.stringify(text)}`, a.watchLinks, want);
  expect(`Share text stripped, topic ending in punctuation ${JSON.stringify(text)}`, inputValue(a.watchLinks), want);
  expect(`Share text as spaces, topic ending in punctuation ${JSON.stringify(text)}`, a.watchLinks.replace(/\n/g, ' '), want);
  expect(`Share text in a message, topic ending in punctuation ${JSON.stringify(text)}`, `hey!\n\n${a.watchLinks}\n\nsee you`, want);
}

const raw = app(OWNER, { raw: RAW });
const rawWant = channel(OWNER, { raw: RAW });
expect('raw topic, hash link', raw.key, rawWant);
expect('raw topic, player link', raw.playerLink, rawWant);
expect('raw topic without 0x', `0x${OWNER}/${RAW}`, rawWant);
expect('raw topic, upper-case', `0x${OWNER.toUpperCase()}/0X${RAW.toUpperCase()}`, rawWant);
expect('raw topic, Share text', raw.watchLinks, rawWant);
expect('raw topic, Share text stripped', inputValue(raw.watchLinks), rawWant);
expect('swarm:// with a 0x owner', `swarm://0x${OWNER}/antstream`, antWant);
expect('swarm:// stops at ?', `swarm://${OWNER}/antstream?x=1`, antWant);
expect('owner without 0x and a topic', `${OWNER}/antstream`, antWant);
expect('not URL-encoded text topic', `0x${OWNER}/50%`, channel(OWNER, { text: '50%' }));

// The recent-channels list stores `key`s and reads them back with parseChannel.
for (const t of [{ text: 'antstream' }, { text: 'Café ☕ live' }, { raw: RAW }]) {
  const first = parseChannel(app(OWNER, t).playerLink);
  expect(`recent list round trip ${JSON.stringify(t)}`, first.key, channel(OWNER, t));
}

console.log(`\n${passed} passed, ${failures} failed`);
process.exit(failures ? 1 : 0);
