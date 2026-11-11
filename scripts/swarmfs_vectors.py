#!/usr/bin/env python3
"""Generate cross-implementation test vectors for BMT, SOC/feeds and
Mantaray with swarmfs (https://github.com/petfold/swarmfs), a Python Swarm
implementation that is neither bee nor ant (issue #62).

bee <-> ant vectors (``crates/ant-crypto/tests/trojan_gen.jsonl``) prove
the two agree with each other; they cannot catch both drifting from the
written format, since bee *is* the de-facto spec. swarmfs re-derived these
formats independently, in another language, so a vector it produces that
ant reproduces byte-for-byte is a cheap check on that failure mode.

Output (one JSON object per line, the same shape family as
``trojan_gen.jsonl``; everything is deterministic, so regenerating is a
no-op unless a format or swarmfs changes):

- ``crates/ant-crypto/tests/swarmfs_bmt.jsonl``       chunk data -> BMT address
- ``crates/ant-retrieval/tests/swarmfs_files.jsonl``  content -> root reference
                                                      (multi-chunk splitter trees)
- ``crates/ant-retrieval/tests/swarmfs_soc.jsonl``    topic/index -> feed id ->
                                                      SOC address, signed SOC chunks
- ``crates/ant-retrieval/tests/swarmfs_mantaray.jsonl`` entries -> manifest
                                                      node chunks + root reference

Nothing Python-side is in ant's build: the Rust tests only read the JSONL.
To regenerate (swarmfs needs Python >= 3.11; its ``feeds`` extra pulls
coincurve, which had no CPython 3.14 wheel and failed to build from source
when these vectors were made, while 3.12 installed cleanly):

    uv venv -p 3.12 .venv-vectors
    VIRTUAL_ENV=.venv-vectors uv pip install \\
        "swarmfs[feeds] @ git+https://github.com/petfold/swarmfs@3dc57b561515d545ca960c51cba2ef351d13007d"
    .venv-vectors/bin/python scripts/swarmfs_vectors.py

``SWARMFS_COMMIT`` below must match the pin you installed; it is recorded
in every vector line.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sys
from pathlib import Path

from swarmfs.bmt import CHUNK_PAYLOAD_SIZE, cac_data, chunk_address, keccak256
from swarmfs.feeds import FeedSigner, feed_identifier, soc_address, topic_bytes, verify_soc
from swarmfs.mantaray.build import add, save
from swarmfs.mantaray.node import Node
from swarmfs.splitter import split

SWARMFS_COMMIT = "3dc57b561515d545ca960c51cba2ef351d13007d"
GENERATOR = f"swarmfs@{SWARMFS_COMMIT[:12]}"

ROOT = Path(__file__).resolve().parent.parent
OUT_BMT = ROOT / "crates/ant-crypto/tests/swarmfs_bmt.jsonl"
OUT_FILES = ROOT / "crates/ant-retrieval/tests/swarmfs_files.jsonl"
OUT_SOC = ROOT / "crates/ant-retrieval/tests/swarmfs_soc.jsonl"
OUT_MANTARAY = ROOT / "crates/ant-retrieval/tests/swarmfs_mantaray.jsonl"


def pattern(n: int) -> bytes:
    """Deterministic content of length ``n``: byte i is
    ``((i * 7 + k) % 251) ^ ((k // 251) & 0xff)`` with ``k = i // 4096``,
    the chunk index.

    The Rust tests rebuild the same bytes from the formula, so large file
    vectors carry only their size, not their content. 251 is prime, so the
    pattern never lines up with the 32-byte segment or 4096-byte chunk
    grid. The ``+ k`` term alone repeats every 251 chunks (chunk k is the
    row ``7j + 59k mod 251``), so the XOR with ``k // 251`` tells those
    periods apart: every chunk of a file up to 251 * 256 chunks (~251 MiB)
    is distinct, so a splitter that reorders or mixes leaves cannot land on
    the same root by accident. For the first 251 chunks (files below
    ~1 MiB) the XOR term is 0.
    """
    return bytes(
        ((i * 7 + i // CHUNK_PAYLOAD_SIZE) % 251) ^ ((i // CHUNK_PAYLOAD_SIZE // 251) & 0xFF)
        for i in range(n)
    )


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(r, sort_keys=True) + "\n" for r in rows))
    print(f"wrote {path.relative_to(ROOT)} ({len(rows)} vectors, {path.stat().st_size} bytes)")


# --- BMT: chunk data -> address --------------------------------------------


def bmt_vectors() -> list[dict]:
    rows = []

    def row(case: str, data: bytes) -> None:
        rows.append({
            "case": case,
            "generator": GENERATOR,
            "chunk_data_hex": data.hex(),
            "address_hex": chunk_address(data).hex(),
        })

    # content-addressed chunks: span = payload length
    for n in (0, 1, 31, 32, 33, 63, 64, 65, 100, 1000, 2048, 2049, 4064, 4095, 4096):
        row(f"cac pattern len={n}", cac_data(pattern(n)))
    row("cac zeros len=4096", cac_data(bytes(CHUNK_PAYLOAD_SIZE)))
    row("cac 0xff len=4096", cac_data(b"\xff" * CHUNK_PAYLOAD_SIZE))
    row("cac zeros len=32", cac_data(bytes(32)))
    # a payload whose trailing bytes are zero must still hash to a different
    # address than the shorter payload: the span carries the length
    row("cac pattern+zero tail len=40", cac_data(pattern(32) + bytes(8)))

    # intermediate chunks: the span is the content length beneath the chunk,
    # not the payload length — the address must bind it as given
    refs = b"".join(keccak256(bytes([i])) for i in range(128))
    for span, nrefs in ((8192, 2), (4097, 2), (128 * 4096, 128), (2**40 + 5, 3)):
        data = span.to_bytes(8, "little") + refs[: 32 * nrefs]
        row(f"intermediate span={span} refs={nrefs}", data)
    # bee marks an erasure-coded root by setting the span's top byte to
    # 0x80|level; the address is still over the raw 8 span bytes
    span = (0x81 << 56) | 8192
    row("intermediate redundancy-flagged span", span.to_bytes(8, "little") + refs[:64])
    return rows


# --- files: content -> root reference ---------------------------------------


def file_vectors() -> list[dict]:
    rows = []
    sizes = (
        0, 1, 4095, 4096, 4097, 8192, 8193,
        127 * 4096, 128 * 4096, 128 * 4096 + 1, 129 * 4096 + 17,
        # a level of 128 intermediates plus one promoted leftover chunk
        128 * 128 * 4096 + 4096 + 1,
    )
    for n in sizes:
        root, chunks = split(pattern(n))
        rows.append({
            "case": f"split pattern size={n}",
            "generator": GENERATOR,
            "size": n,
            "content": "pattern",
            # guards the Rust copy of `pattern`: a content mismatch must not
            # read as a splitter/BMT divergence
            "content_sha256": hashlib.sha256(pattern(n)).hexdigest(),
            "chunk_count": len(chunks),
            "root_hex": root.hex(),
        })
    return rows


# --- SOC / feeds -------------------------------------------------------------


# Throwaway, deterministic secp256k1 keys (no funds, never used on-chain).
SIGNER_KEYS = (
    "ad" * 32,
    "0000000000000000000000000000000000000000000000000000000000000001",
    "fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364140",  # n - 1
    "5c3f9a1d2e4b6c8d0f1a2b3c4d5e6f70819293a4b5c6d7e8f90a1b2c3d4e5f60",
)

TOPICS = (
    "swarmlite-fixture",
    "",
    "antstream/channel/🎥",
    # 64 hex chars pass through as a raw topic (bee-js Topic semantics)
    "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
)

INDICES = (0, 1, 255, 256, 2**32, 2**63, 2**64 - 1)


def soc_vectors() -> list[dict]:
    rows = []
    ref = keccak256(b"ant-interop-reference")
    for ki, key in enumerate(SIGNER_KEYS):
        signer = FeedSigner(key)
        owner = signer.owner
        for ti, topic_str in enumerate(TOPICS):
            topic = topic_bytes(topic_str)
            for index in INDICES:
                # walk every key on every topic at one index, and every index
                # on the first key/topic, without the full cross product
                if not (index == INDICES[1] or (ki == 0 and ti == 0)):
                    continue
                identifier = feed_identifier(topic, index)
                timestamp = 1753344000 + index % 1000
                variants = [("v1", cac_data(timestamp.to_bytes(8, "big") + ref))]
                if index == INDICES[1]:
                    # v2: the SOC wraps a content chunk itself (a bare
                    # reference payload and a full 4096-byte chunk)
                    variants.append(("v2-ref", cac_data(ref)))
                    variants.append(("v2-full", cac_data(pattern(CHUNK_PAYLOAD_SIZE))))
                for kind, wrapped in variants:
                    digest = keccak256(identifier + chunk_address(wrapped))
                    sig = signer.sign_digest(digest)
                    soc = identifier + sig + wrapped
                    addr = soc_address(identifier, owner)
                    verify_soc(soc, owner, addr)  # swarmfs's own check
                    rows.append({
                        "case": f"key{ki} topic{ti} index={index} {kind}",
                        "generator": GENERATOR,
                        "signer_priv_hex": key,
                        "owner_hex": owner.hex(),
                        "topic_string": topic_str,
                        "topic_hex": topic.hex(),
                        # true when the string is 64 hex chars taken verbatim
                        # rather than hashed (a swarmfs/bee-js input
                        # convention, not part of the wire format)
                        "topic_is_raw": topic != keccak256(topic_str.encode()),
                        "index": str(index),  # u64 does not fit a JSON double
                        "identifier_hex": identifier.hex(),
                        "soc_address_hex": addr.hex(),
                        "wrapped_address_hex": chunk_address(wrapped).hex(),
                        "signature_hex": sig.hex(),
                        "soc_data_hex": soc.hex(),
                    })
    return rows


# --- Mantaray ------------------------------------------------------------------


def ref_for(path: str) -> bytes:
    return keccak256(b"ref:" + path.encode())


MANTARAY_CASES: list[tuple[str, bytes, list[tuple[str, dict[str, str]]]]] = [
    ("single file", bytes(32), [("index.html", {"Content-Type": "text/html"})]),
    ("siblings", bytes(32), [
        ("a.txt", {"Content-Type": "text/plain"}),
        ("b.txt", {"Content-Type": "text/plain"}),
        ("c.json", {"Content-Type": "application/json"}),
    ]),
    ("shared prefixes", bytes(32), [
        ("a", {}),
        ("ab", {}),
        ("abc", {"Content-Type": "text/plain"}),
        ("abd", {}),
        ("b", {}),
    ]),
    ("nested directories", bytes(32), [
        ("index.html", {"Content-Type": "text/html", "Filename": "index.html"}),
        ("css/site.css", {"Content-Type": "text/css"}),
        ("css/print.css", {"Content-Type": "text/css"}),
        ("img/logo.png", {"Content-Type": "image/png"}),
        ("img/icons/x.svg", {"Content-Type": "image/svg+xml"}),
        ("13/4358/2645.png", {"Content-Type": "image/png"}),
    ]),
    ("prefix longer than 30 bytes", bytes(32), [
        ("a-path-segment-that-is-well-over-thirty-bytes-long.bin", {}),
        ("0123456789012345678901234567890123456789012345678901234567890123456789.txt",
         {"Content-Type": "text/plain"}),
        ("a-path-segment-that-is-well-over-thirty-bytes-long.bin.sig", {}),
    ]),
    ("metadata padding edges", bytes(32), [
        # JSON lengths around the 30/32-byte block boundary of the
        # '\n'-padded metadata field (2-byte size + JSON)
        ("m29", {"k": "x" * 21}),   # {"k":"..."} = 29 bytes
        ("m30", {"k": "x" * 22}),   # 30 bytes: size + JSON fills one block
        ("m31", {"k": "x" * 23}),
        ("m62", {"k": "x" * 54}),   # 62 bytes: fills two blocks
        ("m63", {"k": "x" * 55}),
        ("multi", {"Content-Type": "text/plain", "Filename": "multi", "a": "1", "z": "26"}),
    ]),
    ("utf-8 paths", bytes(32), [
        ("café/menü.txt", {"Content-Type": "text/plain"}),
        ("日本語.html", {"Content-Type": "text/html"}),
    ]),
    # two paths that diverge inside one UTF-8 sequence: é = c3 a9, è = c3 a8,
    # so the trie splits mid-codepoint and the shared fork prefix is one byte
    ("split mid-codepoint", bytes(32), [
        ("é.txt", {"Content-Type": "text/plain"}),
        ("è.txt", {"Content-Type": "text/plain"}),
    ]),
    ("many forks", bytes(32),
     [(f"f{i:03d}", {}) for i in range(40)] + [(f"{c}.txt", {}) for c in "abcdefghij"]),
    # decode-only: bee lets a manifest carry a random obfuscation key; every
    # node body after the key is XORed with it
    ("obfuscated nested", keccak256(b"obfuscation"), [
        ("index.html", {"Content-Type": "text/html"}),
        ("css/site.css", {"Content-Type": "text/css"}),
        ("a-path-segment-that-is-well-over-thirty-bytes-long.bin", {}),
    ]),
]


def mantaray_vectors() -> list[dict]:
    rows = []
    for case, key, entries in MANTARAY_CASES:
        chunks: dict[bytes, bytes] = {}

        async def saver(node_bytes: bytes) -> bytes:
            data = cac_data(node_bytes)
            addr = chunk_address(data)
            chunks[addr] = data
            return addr

        async def build() -> bytes:
            root = Node(ref_bytes_size=32, obfuscation_key=key)
            for path, meta in entries:
                await add(root, path.encode(), ref_for(path), meta or None)
            return await save(root, saver)

        root_ref = asyncio.run(build())
        # every proper prefix of an entry path that is not an entry itself
        # (intermediate trie nodes, half a fork prefix, half a codepoint)
        # must not resolve; hex-encoded because a mid-codepoint prefix is
        # not valid UTF-8
        paths = {p.encode() for p, _ in entries}
        missing = sorted({
            p[:i] for p in paths for i in range(1, len(p)) if p[:i] not in paths
        })
        missing_hex = [m.hex() for m in missing]
        rows.append({
            "case": case,
            "generator": GENERATOR,
            "obfuscation_key_hex": key.hex(),
            "entries": [
                {"path": p, "ref_hex": ref_for(p).hex(), "metadata": m} for p, m in entries
            ],
            "root_hex": root_ref.hex(),
            "missing_paths_hex": missing_hex,
            "chunks": {a.hex(): d.hex() for a, d in sorted(chunks.items())},
        })
    return rows


def main() -> int:
    write_jsonl(OUT_BMT, bmt_vectors())
    write_jsonl(OUT_FILES, file_vectors())
    write_jsonl(OUT_SOC, soc_vectors())
    write_jsonl(OUT_MANTARAY, mantaray_vectors())
    return 0


if __name__ == "__main__":
    sys.exit(main())
