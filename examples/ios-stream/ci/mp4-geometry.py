#!/usr/bin/env python3
"""Print the video track geometry of an MP4 / fMP4 init segment (#194).

    mp4-geometry.py [--expect portrait|landscape] <init.mp4>

Shows what decides how a player shapes the picture: the coded size (the
avc1/hvc1 sample entry), the track header's width/height and its matrix
(a rotation flag when not identity). With --expect, exits 1 unless the
picture is encoded upright in that orientation: coded size of that shape,
identity matrix, track header equal to the coded size. Used by the
antstream-visual workflow on the iOS app's own segments.
"""
import struct
import sys

CONTAINERS = {b"moov", b"trak", b"mdia", b"minf", b"stbl"}


def boxes(data, start, end):
    at = start
    while at + 8 <= end:
        size, kind = struct.unpack(">I4s", data[at:at + 8])
        header = 8
        if size == 1:
            size = struct.unpack(">Q", data[at + 8:at + 16])[0]
            header = 16
        elif size == 0:
            size = end - at
        if size < header:
            break
        yield kind, at + header, at + size
        at += size


def find(data, start, end, kind):
    return next(((s, e) for k, s, e in boxes(data, start, end) if k == kind), None)


def video_track(data):
    moov = find(data, 0, len(data), b"moov")
    if not moov:
        sys.exit("no moov box")
    for kind, s, e in boxes(data, *moov):
        if kind != b"trak":
            continue
        mdia = find(data, s, e, b"mdia")
        hdlr = mdia and find(data, *mdia, b"hdlr")
        if hdlr and data[hdlr[0] + 8:hdlr[0] + 12] == b"vide":
            return (s, e), mdia
    sys.exit("no video track")


def main():
    args = sys.argv[1:]
    expect = None
    if args[:1] == ["--expect"]:
        expect, args = args[1], args[2:]
    data = open(args[0], "rb").read()
    (ts, te), mdia = video_track(data)

    tkhd = find(data, ts, te, b"tkhd")
    version = data[tkhd[0]]
    m = tkhd[0] + 4 + (32 if version == 1 else 20) + 8 + 8
    matrix = struct.unpack(">9i", data[m:m + 36])
    tw, th = (v / 65536 for v in struct.unpack(">II", data[m + 36:m + 44]))

    minf = find(data, *mdia, b"minf")
    stbl = find(data, *minf, b"stbl")
    stsd = find(data, *stbl, b"stsd")
    entry = next(boxes(data, stsd[0] + 8, stsd[1]))
    cw, ch = struct.unpack(">HH", data[entry[1] + 24:entry[1] + 28])

    identity = matrix == (0x10000, 0, 0, 0, 0x10000, 0, 0, 0, 0x40000000)
    print(f"file:          {args[0]}")
    print(f"sample entry:  {entry[0].decode()} coded {cw}x{ch}")
    print(f"tkhd:          {tw:g}x{th:g}")
    print(f"tkhd matrix:   {matrix} ({'identity' if identity else 'NOT identity: a rotation/flip flag'})")
    shape = "portrait" if ch > cw else "landscape"
    print(f"displayed as:  {shape} {cw}x{ch}" if identity else "displayed as:  depends on the player")
    if expect:
        problems = []
        if shape != expect:
            problems.append(f"coded {cw}x{ch} is {shape}, expected {expect}")
        if not identity:
            problems.append("matrix is not identity")
        if (tw, th) != (cw, ch):
            problems.append(f"tkhd {tw:g}x{th:g} differs from the coded size")
        if problems:
            print("FAIL: " + "; ".join(problems))
            sys.exit(1)
        print(f"OK: upright {expect} frames, identity matrix")


main()
