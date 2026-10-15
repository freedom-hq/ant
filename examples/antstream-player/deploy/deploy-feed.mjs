#!/usr/bin/env node
// Publish the AntStream web player to Swarm behind a feed, so it keeps one
// address: the feed manifest. Every run uploads the player as a collection
// and writes it as the feed's next update; viewers and the apps' share
// links follow the feed to the newest build.
//
//   cd examples/antstream-player/deploy && npm install
//   node deploy-feed.mjs --gateway http://127.0.0.1:1633 --stamp <batch id> \
//     --key <file with the feed owner's private key, hex> [--topic antstream-player] [--dry-run]
//
// The key only signs feed updates: it needs no funds and has no rights on
// any chain. The stamp pays for the upload on the gateway's node.
//
// Each build gets a version.js next to the page with its build number (the
// feed index it is published at), the commit and the build time, which the
// page shows in its footer and the debug panel.

import { Bee, PrivateKey, Topic } from '@ethersphere/bee-js'
import { execSync } from 'node:child_process'
import { cpSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const PLAYER_FILES = [
  'index.html',
  'player.css',
  'player.js',
  'debug.js',
  'vendor/hls.light.min.js',
  'vendor/hls.js-LICENSE',
]

function args() {
  const out = { topic: 'antstream-player', dryRun: false }
  const argv = process.argv.slice(2)
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]
    if (a === '--dry-run') out.dryRun = true
    else if (a.startsWith('--')) out[a.slice(2)] = argv[++i]
  }
  for (const need of ['gateway', 'stamp', 'key']) {
    if (!out[need]) {
      console.error(`missing --${need}`)
      process.exit(2)
    }
  }
  return out
}

const opts = args()
const playerDir = join(dirname(fileURLToPath(import.meta.url)), '..')
const key = new PrivateKey(readFileSync(opts.key, 'utf8').trim())
const owner = key.publicKey().address()
const topic = Topic.fromString(opts.topic)
const bee = new Bee(opts.gateway, { signer: key })

// The next feed index: one past the newest update, or 0 for a new feed.
let next = 0
try {
  const latest = await bee.feed.makeReader(topic, owner).downloadReference()
  next = Number(latest.feedIndexNext ? latest.feedIndexNext.toBigInt() : latest.feedIndex.toBigInt() + 1n)
} catch {
  next = 0
}

const commit = (() => {
  try {
    const sha = execSync('git rev-parse --short HEAD', { cwd: playerDir }).toString().trim()
    const dirty = execSync('git status --porcelain -- .', { cwd: playerDir }).toString().trim()
    return dirty ? `${sha}+local` : sha
  } catch {
    return 'unknown'
  }
})()
const build = { build: next, commit, builtAt: new Date().toISOString(), topic: opts.topic }

const stage = mkdtempSync(join(tmpdir(), 'antstream-player-'))
try {
  for (const f of PLAYER_FILES) cpSync(join(playerDir, f), join(stage, f), { recursive: true })
  writeFileSync(join(stage, 'version.js'), `window.ANTSTREAM_PLAYER_BUILD = ${JSON.stringify(build)};\n`)
  console.log(`build ${build.build} · ${build.commit} · ${build.builtAt}`)
  if (opts.dryRun) {
    console.log(`dry run: staged in ${stage}, nothing uploaded`)
    process.exit(0)
  }

  const upload = await bee.collection.uploadFromDirectory(opts.stamp, stage, { indexDocument: 'index.html' })
  console.log(`collection ${upload.reference.toHex()}`)

  await bee.feed.makeWriter(topic, key).uploadReference(opts.stamp, upload.reference, { index: next })
  const manifest = await bee.feed.createManifest(opts.stamp, topic, owner)
  console.log(`feed owner ${owner.toHex()} topic ${topic.toHex()} index ${next}`)
  console.log(`feed manifest ${manifest.toHex()}`)
} finally {
  rmSync(stage, { recursive: true, force: true })
}
