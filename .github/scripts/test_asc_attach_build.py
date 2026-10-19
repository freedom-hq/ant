#!/usr/bin/env python3
"""Offline tests for asc-attach-build.py's pure helpers.

Run with plain python3 (the signing tests also need an `openssl` binary):
  python3 .github/scripts/test_asc_attach_build.py
"""

import importlib.util
import json
import pathlib
import shutil
import socket
import subprocess
import tempfile
import threading
import unittest
from unittest import mock

_path = pathlib.Path(__file__).with_name("asc-attach-build.py")
_spec = importlib.util.spec_from_file_location("asc_attach_build", _path)
asc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(asc)


def version(vid, state, created="2026-01-01T00:00:00Z", field="appVersionState", platform="IOS"):
    return {
        "id": vid,
        "attributes": {"platform": platform, field: state, "createdDate": created},
    }


class TokenClaims(unittest.TestCase):
    def test_claims(self):
        headers, payload = asc.token_claims("KEY123", "issuer-uuid", 1_700_000_000.7)
        self.assertEqual(headers, {"alg": "ES256", "kid": "KEY123", "typ": "JWT"})
        self.assertEqual(payload["iss"], "issuer-uuid")
        self.assertEqual(payload["aud"], "appstoreconnect-v1")
        self.assertEqual(payload["iat"], 1_700_000_000)
        self.assertEqual(payload["exp"], 1_700_000_000 + asc.TOKEN_TTL_S)

    def test_lifetime_within_apples_cap(self):
        # Apple rejects tokens living longer than 20 minutes, and the
        # client must mint a fresh one before the old one expires.
        self.assertLessEqual(asc.TOKEN_TTL_S, 20 * 60)
        self.assertLess(asc.TOKEN_REFRESH_S + 120, asc.TOKEN_TTL_S)


class EditableVersion(unittest.TestCase):
    def test_none(self):
        self.assertIsNone(asc.editable_version([]))

    def test_in_review_is_not_editable(self):
        vs = [version("a", "WAITING_FOR_REVIEW"), version("b", "READY_FOR_DISTRIBUTION")]
        self.assertIsNone(asc.editable_version(vs))

    def test_editable_states(self):
        for state in (
            "PREPARE_FOR_SUBMISSION",
            "DEVELOPER_REJECTED",
            "REJECTED",
            "METADATA_REJECTED",
            "INVALID_BINARY",
        ):
            vs = [version("live", "READY_FOR_DISTRIBUTION"), version("next", state)]
            self.assertEqual(asc.editable_version(vs)["id"], "next", state)

    def test_falls_back_to_deprecated_app_store_state(self):
        vs = [version("a", "PREPARE_FOR_SUBMISSION", field="appStoreState")]
        self.assertEqual(asc.editable_version(vs)["id"], "a")

    def test_app_version_state_wins_over_app_store_state(self):
        v = version("a", "IN_REVIEW")
        v["attributes"]["appStoreState"] = "PREPARE_FOR_SUBMISSION"
        self.assertIsNone(asc.editable_version([v]))

    def test_other_platforms_ignored(self):
        vs = [version("mac", "PREPARE_FOR_SUBMISSION", platform="MAC_OS")]
        self.assertIsNone(asc.editable_version(vs))

    def test_newest_editable_wins(self):
        vs = [
            version("old", "REJECTED", created="2026-01-01T00:00:00Z"),
            version("new", "PREPARE_FOR_SUBMISSION", created="2026-03-01T00:00:00Z"),
        ]
        self.assertEqual(asc.editable_version(vs)["id"], "new")


class BuildStatus(unittest.TestCase):
    def build(self, state):
        return [{"id": "b", "attributes": {"processingState": state}}]

    def test_missing(self):
        self.assertEqual(asc.build_status([]), "missing")

    def test_states(self):
        self.assertEqual(asc.build_status(self.build("PROCESSING")), "processing")
        self.assertEqual(asc.build_status(self.build("VALID")), "valid")
        self.assertEqual(asc.build_status(self.build("INVALID")), "failed")
        self.assertEqual(asc.build_status(self.build("FAILED")), "failed")



def raw_to_der(raw):
    """Inverse of der_to_raw_signature, for handing back to openssl."""

    def integer(b):
        b = b.lstrip(b"\0") or b"\0"
        if b[0] & 0x80:
            b = b"\0" + b
        return b"\x02" + bytes([len(b)]) + b

    body = integer(raw[:32]) + integer(raw[32:])
    return b"\x30" + bytes([len(body)]) + body


@unittest.skipUnless(shutil.which("openssl"), "needs openssl")
class NewerBuild(unittest.TestCase):
    def test_compares_numerically(self):
        self.assertTrue(asc.is_newer_build("43", "42"))
        self.assertTrue(asc.is_newer_build("100", "99"))
        self.assertFalse(asc.is_newer_build("42", "42"))
        self.assertFalse(asc.is_newer_build("41", "42"))
        self.assertFalse(asc.is_newer_build(None, "42"))
        self.assertFalse(asc.is_newer_build("1.0.3", "42"))


class MainFlow(unittest.TestCase):
    """main() against a scripted Client.request: which calls go out."""

    def run_main(self, attached):
        calls = []

        def request(self_, method, path, params=None, body=None):
            calls.append((method, path))
            if path == "/v1/apps":
                return {"data": [{"id": "A", "attributes": {"bundleId": "b", "name": "N"}}]}
            if path == "/v1/builds":
                return {"data": [{"id": "B", "attributes": {"processingState": "VALID"}}]}
            if path == "/v1/apps/A/appStoreVersions":
                return {"data": [version("V", "PREPARE_FOR_SUBMISSION")]}
            if path == "/v1/appStoreVersions/V/build":
                return {"data": {"id": "X", "attributes": {"version": attached}} if attached else None}
            if method == "PATCH":
                return None
            raise AssertionError(path)

        env = {"ASC_KEY_ID": "k", "ASC_ISSUER_ID": "i", "ASC_KEY_PATH": "/x"}
        with mock.patch.dict(asc.os.environ, env), mock.patch.object(asc.Client, "request", request):
            rc = asc.main(["--bundle-id", "b", "--version", "0.1.42", "--build", "42"])
        return rc, [c for c in calls if c[0] == "PATCH"]

    def test_attaches_over_nothing_or_an_older_build(self):
        for attached in (None, "41"):
            rc, patches = self.run_main(attached)
            self.assertEqual(rc, 0)
            self.assertEqual(patches, [("PATCH", "/v1/appStoreVersions/V/relationships/build")])

    def test_leaves_a_newer_build_alone(self):
        rc, patches = self.run_main("43")
        self.assertEqual(rc, 0)
        self.assertEqual(patches, [])


class Signing(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        d = pathlib.Path(self.dir.name)
        self.key = d / "AuthKey.p8"
        self.pub = d / "pub.pem"
        # The same shape as Apple's .p8: an unencrypted PKCS#8 P-256 key.
        subprocess.run(
            ["openssl", "genpkey", "-algorithm", "EC", "-pkeyopt", "ec_paramgen_curve:P-256",
             "-out", str(self.key)],
            check=True, capture_output=True,
        )
        subprocess.run(
            ["openssl", "pkey", "-in", str(self.key), "-pubout", "-out", str(self.pub)],
            check=True, capture_output=True,
        )

    def tearDown(self):
        self.dir.cleanup()

    def test_der_to_raw_pads_short_integers(self):
        der = bytes.fromhex("3006020101020200ff")
        self.assertEqual(asc.der_to_raw_signature(der), (1).to_bytes(32, "big") + (255).to_bytes(32, "big"))

    def test_token_verifies_with_the_public_key(self):
        client = asc.Client("KEY123", "issuer-uuid", str(self.key))
        for _ in range(20):  # r/s with a leading zero byte come up ~1 in 128
            client._token = None
            token = client.token()
            head, payload, sig = token.split(".")
            raw = asc.base64.urlsafe_b64decode(sig + "=" * (-len(sig) % 4))
            self.assertEqual(len(raw), 64)
            sig_path = pathlib.Path(self.dir.name) / "sig.der"
            sig_path.write_bytes(raw_to_der(raw))
            subprocess.run(
                ["openssl", "dgst", "-sha256", "-verify", str(self.pub), "-signature", str(sig_path)],
                input=f"{head}.{payload}".encode(), check=True, capture_output=True,
            )
        claims = json.loads(asc.base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        self.assertEqual(claims["aud"], "appstoreconnect-v1")
        self.assertEqual(claims["iss"], "issuer-uuid")

    def test_token_verifies_with_pyjwt_if_installed(self):
        try:
            import jwt
        except ImportError:
            self.skipTest("PyJWT not installed")
        token = asc.Client("KEY123", "issuer-uuid", str(self.key)).token()
        self.assertEqual(jwt.get_unverified_header(token), {"alg": "ES256", "kid": "KEY123", "typ": "JWT"})
        claims = jwt.decode(token, self.pub.read_text(), algorithms=["ES256"], audience="appstoreconnect-v1")
        self.assertEqual(claims["iss"], "issuer-uuid")

    def test_missing_key_is_an_api_error(self):
        client = asc.Client("KEY123", "issuer-uuid", str(pathlib.Path(self.dir.name) / "nope.p8"))
        with self.assertRaises(asc.ApiError):
            client.token()


class Retry(unittest.TestCase):
    """Client.request against a local server that misbehaves on the
    first `drops` connections, then answers 200."""

    def serve(self, drops, misbehave):
        srv = socket.socket()
        srv.bind(("127.0.0.1", 0))
        srv.listen(8)
        self.addCleanup(srv.close)
        self.connections = 0

        def run():
            while True:
                try:
                    conn, _ = srv.accept()
                except OSError:
                    return
                with conn:
                    conn.recv(65536)
                    self.connections += 1
                    if self.connections <= drops:
                        misbehave(conn)
                    else:
                        body = b'{"data": []}'
                        conn.sendall(
                            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                            + f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
                            + body
                        )

        threading.Thread(target=run, daemon=True).start()
        return f"http://127.0.0.1:{srv.getsockname()[1]}"

    def client(self, base):
        c = asc.Client("k", "i", "/nonexistent")
        c.token = lambda: "token"
        patches = [mock.patch.object(asc, "API", base), mock.patch.object(asc.time, "sleep")]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        return c

    def test_get_retries_a_dropped_connection(self):
        # Closing without a reply: http.client.RemoteDisconnected.
        c = self.client(self.serve(1, lambda conn: None))
        self.assertEqual(c.request("GET", "/v1/builds"), {"data": []})
        self.assertEqual(self.connections, 2)

    def test_get_retries_a_truncated_body(self):
        # A Content-Length the body never reaches: http.client.IncompleteRead.
        def short(conn):
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{")

        c = self.client(self.serve(1, short))
        self.assertEqual(c.request("GET", "/v1/builds"), {"data": []})
        self.assertEqual(self.connections, 2)

    def test_get_gives_up_with_an_api_error(self):
        c = self.client(self.serve(99, lambda conn: None))
        with self.assertRaises(asc.ApiError):
            c.request("GET", "/v1/builds")
        self.assertEqual(self.connections, asc.GET_RETRIES + 1)

    def test_get_retries_when_the_error_body_is_cut(self):
        # A 503 whose body drops mid-read: e.read() raises IncompleteRead
        # inside the HTTPError arm, which must still retry.
        def cut_503(conn):
            conn.sendall(
                b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 100\r\n"
                b"Connection: close\r\n\r\n{"
            )

        c = self.client(self.serve(1, cut_503))
        self.assertEqual(c.request("GET", "/v1/builds"), {"data": []})
        self.assertEqual(self.connections, 2)

    def test_cut_error_body_on_a_4xx_is_an_api_error(self):
        def cut_401(conn):
            conn.sendall(
                b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 100\r\n"
                b"Connection: close\r\n\r\n{"
            )

        c = self.client(self.serve(99, cut_401))
        with self.assertRaisesRegex(asc.ApiError, "HTTP 401"):
            c.request("GET", "/v1/builds")
        self.assertEqual(self.connections, 1)

    def test_get_retries_a_non_json_200(self):
        def html(conn):
            body = b"<html>proxy</html>"
            conn.sendall(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n"
                + f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
                + body
            )

        c = self.client(self.serve(1, html))
        self.assertEqual(c.request("GET", "/v1/builds"), {"data": []})
        self.assertEqual(self.connections, 2)

        c = self.client(self.serve(99, html))
        with self.assertRaisesRegex(asc.ApiError, "not JSON"):
            c.request("GET", "/v1/builds")

    def test_patch_is_not_retried_but_still_an_api_error(self):
        c = self.client(self.serve(99, lambda conn: None))
        with self.assertRaises(asc.ApiError):
            c.request("PATCH", "/v1/appStoreVersions/x/relationships/build", body={})
        self.assertEqual(self.connections, 1)


if __name__ == "__main__":
    unittest.main()
