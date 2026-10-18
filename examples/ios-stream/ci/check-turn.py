#!/usr/bin/env python3
"""Check a broadcast that was turned while live (#228).

    check-turn.py <segment-ring-dir> <playlist-dir>

<segment-ring-dir> is the app's on-disk segment ring
(`Library/Caches/antstream/segments`: `init-<n>.mp4`, `seg-<n>.m4s`, one
counter for both), the very bytes the publisher uploads.
<playlist-dir> holds the playlists the publisher wrote
(`ci/record-gateway.py`). The run went live in portrait and was turned to
landscape once (`-antstream-shot-portrait -antstream-shot-turn`), so:

* exactly two writers: two initialization segments, the first encoded
  upright portrait and the second upright landscape (`mp4-geometry.py
  --expect`), and so no second restart for the one turn;
* both writers produced media;
* a playlist lists both: two `#EXT-X-MAP`s naming different segments,
  with an `#EXT-X-DISCONTINUITY` between them.

Exits 1 on the first check that fails.
"""
import glob
import os
import re
import subprocess
import sys

ring, playlists = sys.argv[1], sys.argv[2]
here = os.path.dirname(os.path.abspath(__file__))


def number(path):
    return int(re.search(r"-(\d+)\.", os.path.basename(path)).group(1))


def fail(message):
    print(f"FAIL: {message}")
    sys.exit(1)


inits = sorted(glob.glob(os.path.join(ring, "init-*.mp4")), key=number)
media = sorted(glob.glob(os.path.join(ring, "seg-*.m4s")), key=number)
print("init segments:", [os.path.basename(p) for p in inits])
if len(inits) != 2:
    fail(f"expected two writers (portrait, then landscape), found {len(inits)} init segments")
for path, shape in zip(inits, ("portrait", "landscape")):
    print(f"--- {os.path.basename(path)}, expected {shape}")
    result = subprocess.run(
        [sys.executable, os.path.join(here, "mp4-geometry.py"), "--expect", shape, path]
    )
    if result.returncode != 0:
        fail(f"{os.path.basename(path)} is not an upright {shape} frame")
first, second = number(inits[0]), number(inits[1])
before = [p for p in media if first < number(p) < second]
after = [p for p in media if number(p) > second]
print(f"media segments: {len(before)} portrait, {len(after)} landscape")
if not before or not after:
    fail("both writers must have produced media")

found = None
for path in sorted(glob.glob(os.path.join(playlists, "playlist-*.m3u8")), key=number):
    text = open(path, encoding="utf-8").read()
    maps = [(m.start(), m.group(1)) for m in re.finditer(r'#EXT-X-MAP:URI="([^"]+)"', text)]
    if len(maps) < 2:
        continue
    (first_at, first_uri), (second_at, second_uri) = maps[0], maps[1]
    if first_uri == second_uri:
        fail(f"{os.path.basename(path)}: both maps name {first_uri}")
    breaks = [m.start() for m in re.finditer(r"^#EXT-X-DISCONTINUITY$", text, re.M)]
    if not any(first_at < b < second_at for b in breaks):
        fail(f"{os.path.basename(path)}: no #EXT-X-DISCONTINUITY between the two maps")
    found = (path, text)
if found is None:
    count = len(glob.glob(os.path.join(playlists, "playlist-*.m3u8")))
    fail(f"no playlist (of {count}) lists both writers' maps")
print(f"--- {os.path.basename(found[0])}, the newest playlist listing both writers:")
print(found[1])
print("turned broadcast: OK")
