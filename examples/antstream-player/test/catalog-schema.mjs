#!/usr/bin/env node
// Checks the catalog entries the publisher writes (issue #220) against the
// schema Solar Punk's viewer reads the catalog with
// (test/vendor/solar-punk-catalog.mjs): the Rust fixtures
// crates/ant-ffi/testdata/catalog/{live,vod,scheduled}.json, or the files
// given, must pass `viewerCatalogSchema.parse` whole, and keep the fields
// the schema does not name (`description`) through the parse.
//
//   ZOD=<path to zod 4's index.js> node test/catalog-schema.mjs [file.json ...]
//
// zod is not vendored: ZOD names its module (in a Solar Punk checkout,
// stream-event-client/node_modules/zod/index.js); without ZOD the script
// tries `import('zod')`.

import { readFileSync } from 'node:fs';
import { isDeepStrictEqual } from 'node:util';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { solarPunkCatalog } from './vendor/solar-punk-catalog.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const fixtures = join(here, '../../../crates/ant-ffi/testdata/catalog');
const files = process.argv.length > 2
  ? process.argv.slice(2)
  : ['live', 'vod', 'scheduled'].map((n) => join(fixtures, `${n}.json`));

let zod;
try {
  zod = await import(process.env.ZOD ? pathToFileURL(resolve(process.env.ZOD)).href : 'zod');
} catch (err) {
  console.error(`zod not found (${err.message}).\nRun it as: ZOD=<path to zod 4's index.js> node test/catalog-schema.mjs [file.json ...]`);
  process.exit(2);
}
const z = zod.z || zod.default || zod;
if (typeof z.looseObject !== 'function') {
  console.error('this zod has no z.looseObject: zod 4 is needed');
  process.exit(2);
}
const { viewerCatalogSchema } = solarPunkCatalog(z);

let failed = 0;
for (const file of files) {
  const name = file.split('/').pop();
  let raw;
  try {
    raw = JSON.parse(readFileSync(file, 'utf8'));
  } catch (err) {
    console.log(`FAIL ${name}: ${err.message}`);
    failed += 1;
    continue;
  }
  const r = viewerCatalogSchema.safeParse(raw);
  if (!r.success) {
    console.log(`FAIL ${name}: ${JSON.stringify(r.error.issues)}`);
    failed += 1;
    continue;
  }
  // looseObject keeps what the schema does not name.
  const lost = raw.flatMap((e, i) => Object.keys(e).filter((k) => !isDeepStrictEqual(e[k], r.data[i][k])).map((k) => `${i}.${k}`));
  if (lost.length) {
    console.log(`FAIL ${name}: changed by the parse: ${lost.join(', ')}`);
    failed += 1;
    continue;
  }
  const states = r.data.map((e) => `${e.state} "${e.title}"${e.description !== undefined ? ', description kept' : ''}`);
  console.log(`ok   ${name}: ${states.join('; ')}`);
}
process.exit(failed ? 1 : 0);
