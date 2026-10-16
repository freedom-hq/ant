/*
 * Keccak-256 (the original Keccak padding Ethereum and Swarm use, not
 * NIST SHA3-256), for the addresses a channel is read at:
 *
 *  - the feed topic, `keccak256(utf8(text))`, what bee-js
 *    `Topic.fromString` and `topic_from_string` in
 *    crates/ant-ffi/src/publisher.rs compute;
 *  - a feed slot's identifier, `keccak256(topic ‖ index_be8)`;
 *  - a ladder time marker's identifier,
 *    `keccak256("ladder-marker" ‖ group topic ‖ period_be8)`.
 *
 * A handful of short inputs per read, so BigInt lanes are fast enough.
 * Loaded by the page as `window.antstreamKeccak` and by the Node test
 * tools through `require`.
 */
(function (root) {
  'use strict';

  const MASK = (1n << 64n) - 1n;
  const RATE = 136; // bytes: 1600 - 2 * 256 bits

  const RC = [];
  {
    // Round constants from the LFSR in the Keccak reference.
    let r = 1;
    for (let round = 0; round < 24; round++) {
      let c = 0n;
      for (let j = 0; j < 7; j++) {
        if (r & 1) c |= 1n << ((1n << BigInt(j)) - 1n);
        r = r & 0x80 ? ((r << 1) ^ 0x71) & 0xff : (r << 1) & 0xff;
      }
      RC.push(c);
    }
  }

  // Rotation offsets and the pi permutation, by lane index x + 5y.
  const ROT = new Array(25).fill(0);
  const PI = new Array(25).fill(0);
  {
    let x = 1;
    let y = 0;
    for (let t = 0; t < 24; t++) {
      ROT[x + 5 * y] = (((t + 1) * (t + 2)) / 2) % 64;
      [x, y] = [y, (2 * x + 3 * y) % 5];
    }
    // Lane (x, y) moves to (y, 2x + 3y).
    for (let x5 = 0; x5 < 5; x5++) for (let y5 = 0; y5 < 5; y5++) PI[x5 + 5 * y5] = y5 + 5 * ((2 * x5 + 3 * y5) % 5);
  }

  const rotl = (v, n) => (n === 0 ? v : ((v << BigInt(n)) | (v >> BigInt(64 - n))) & MASK);

  function permute(s) {
    const b = new Array(25);
    const c = new Array(5);
    for (let round = 0; round < 24; round++) {
      for (let x = 0; x < 5; x++) c[x] = s[x] ^ s[x + 5] ^ s[x + 10] ^ s[x + 15] ^ s[x + 20];
      for (let x = 0; x < 5; x++) {
        const d = c[(x + 4) % 5] ^ rotl(c[(x + 1) % 5], 1);
        for (let y = 0; y < 25; y += 5) s[x + y] ^= d;
      }
      for (let i = 0; i < 25; i++) b[PI[i]] = rotl(s[i], ROT[i]);
      for (let y = 0; y < 25; y += 5) {
        for (let x = 0; x < 5; x++) s[x + y] = b[x + y] ^ (~b[((x + 1) % 5) + y] & MASK & b[((x + 2) % 5) + y]);
      }
      s[0] ^= RC[round];
    }
  }

  /** Keccak-256 of `bytes` (a Uint8Array), as a 32-byte Uint8Array. */
  function keccak256(bytes) {
    const len = bytes.length;
    const padded = new Uint8Array((Math.floor(len / RATE) + 1) * RATE);
    padded.set(bytes);
    padded[len] ^= 0x01;
    padded[padded.length - 1] ^= 0x80;
    const s = new Array(25).fill(0n);
    for (let off = 0; off < padded.length; off += RATE) {
      for (let i = 0; i < RATE / 8; i++) {
        let lane = 0n;
        for (let k = 7; k >= 0; k--) lane = (lane << 8n) | BigInt(padded[off + i * 8 + k]);
        s[i] ^= lane;
      }
      permute(s);
    }
    const out = new Uint8Array(32);
    for (let i = 0; i < 4; i++) {
      let lane = s[i];
      for (let k = 0; k < 8; k++) {
        out[i * 8 + k] = Number(lane & 0xffn);
        lane >>= 8n;
      }
    }
    return out;
  }

  const toHex = (bytes) => Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');

  function fromHex(hex) {
    const h = hex.replace(/^0x/i, '');
    const out = new Uint8Array(h.length / 2);
    for (let i = 0; i < out.length; i++) out[i] = parseInt(h.slice(i * 2, i * 2 + 2), 16);
    return out;
  }

  function concat(...parts) {
    const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
    let at = 0;
    for (const p of parts) {
      out.set(p, at);
      at += p.length;
    }
    return out;
  }

  /** A whole number as 8 big-endian bytes. */
  function be8(n) {
    const out = new Uint8Array(8);
    new DataView(out.buffer).setBigUint64(0, BigInt(n), false);
    return out;
  }

  const utf8 = (text) => new TextEncoder().encode(text);

  /** A feed topic (64 hex) from its text, `Topic.fromString`. */
  const topicFromString = (text) => toHex(keccak256(utf8(text)));

  /** A sequence feed's slot identifier (64 hex): `keccak256(topic ‖ index_be8)`. */
  const feedIdentifier = (topicHex, index) => toHex(keccak256(concat(fromHex(topicHex), be8(index))));

  /** A ladder time marker's identifier (64 hex), Solar Punk's `ladderMarkerIdentifier`. */
  const ladderMarkerIdentifier = (groupHex, period) =>
    toHex(keccak256(concat(utf8('ladder-marker'), fromHex(groupHex), be8(period))));

  const api = { keccak256, toHex, fromHex, topicFromString, feedIdentifier, ladderMarkerIdentifier };
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.antstreamKeccak = api;
})(typeof self !== 'undefined' ? self : this);
