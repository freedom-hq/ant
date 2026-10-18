#!/usr/bin/env python3
"""Attach a freshly uploaded build to the app's editable App Store version.

Called as the last step of the iOS TestFlight workflows
(antdrive-ios-testflight.yml, antstream-ios-testflight.yml), after
`xcodebuild -exportArchive` has uploaded the build:

  1. look up the app by bundle ID;
  2. wait (poll every 30 s, give up after 45 min) until App Store Connect
     lists the build and has processed it (`processingState` VALID);
  3. find the app's editable iOS App Store version (Prepare for
     Submission, rejected back to the developer, or Invalid Binary) and
     set its build, unless the version already carries a newer build
     (a later run that finished first).

It never submits anything for review. With no editable version (one is
waiting for or in review, or none exists) it prints a notice and exits 0.

Usage:
  asc-attach-build.py --bundle-id baby.freedom.antdrive \
      --version 0.1.42 --build 42

Credentials come from the environment the workflows already set:
ASC_KEY_ID, ASC_ISSUER_ID and ASC_KEY_PATH (the .p8 file). The ES256
token is signed by the system `openssl` binary, so the script needs
nothing beyond the standard library: no package is installed from PyPI
into a step that holds the Admin-role key.
"""

import argparse
import base64
import http.client
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

API = "https://api.appstoreconnect.apple.com"

# Apple caps a token's lifetime at 20 minutes; mint a new one well before.
TOKEN_TTL_S = 15 * 60
TOKEN_REFRESH_S = 10 * 60

POLL_INTERVAL_S = 30
POLL_TIMEOUT_S = 45 * 60

# Transient failures (network, dropped connections, timeouts, 429, 5xx) on a GET are retried this many
# times, so one hiccup doesn't throw away a 30-minute processing wait.
GET_RETRIES = 4

# Version states whose build can still be changed. `appVersionState` is
# the current field; `appStoreState` (deprecated) is read when it's absent.
EDITABLE_STATES = {
    "PREPARE_FOR_SUBMISSION",
    "DEVELOPER_REJECTED",
    "REJECTED",
    "METADATA_REJECTED",
    # Apple refused the uploaded binary; the fix is attaching a new build.
    "INVALID_BINARY",
}


class ApiError(Exception):
    """An App Store Connect request failed; the message carries the body."""


# --- pure helpers (unit-tested offline) ---------------------------------


def token_claims(key_id, issuer_id, now, ttl=TOKEN_TTL_S):
    """The (headers, payload) of an App Store Connect API JWT."""
    headers = {"alg": "ES256", "kid": key_id, "typ": "JWT"}
    payload = {
        "iss": issuer_id,
        "iat": int(now),
        "exp": int(now) + ttl,
        "aud": "appstoreconnect-v1",
    }
    return headers, payload


def b64url(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _der_len(der, i):
    """Read a DER length at `der[i]`; return (length, index after it)."""
    first = der[i]
    if first < 0x80:
        return first, i + 1
    n = first & 0x7F
    return int.from_bytes(der[i + 1 : i + 1 + n], "big"), i + 1 + n


def der_to_raw_signature(der, size=32):
    """An ECDSA signature as `openssl dgst -sign` writes it (DER
    SEQUENCE of two INTEGERs) to the JWS form: r || s, each `size`
    bytes big-endian."""
    if der[0] != 0x30:
        raise ValueError("not a DER sequence")
    _, i = _der_len(der, 1)
    out = b""
    for _ in range(2):
        if der[i] != 0x02:
            raise ValueError("not a DER integer")
        n, i = _der_len(der, i + 1)
        value = int.from_bytes(der[i : i + n], "big")
        i += n
        out += value.to_bytes(size, "big")
    return out


def signing_input(headers, payload):
    """The `<header>.<payload>` part of a JWS, the bytes that get signed."""
    enc = lambda d: b64url(json.dumps(d, separators=(",", ":")).encode())
    return f"{enc(headers)}.{enc(payload)}"


def sign_es256(message, key_path):
    """Sign `message` (str) with the P-256 key at `key_path` through the
    system openssl; return the JWS signature segment."""
    try:
        der = subprocess.run(
            ["openssl", "dgst", "-sha256", "-sign", key_path],
            input=message.encode(),
            capture_output=True,
            check=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as e:
        detail = getattr(e, "stderr", b"") or b""
        raise ApiError(f"signing the API token failed: {e} {detail.decode(errors='replace')}") from None
    return b64url(der_to_raw_signature(der))


def version_state(version):
    attrs = version.get("attributes") or {}
    return attrs.get("appVersionState") or attrs.get("appStoreState")


def editable_version(versions):
    """The editable iOS App Store version among `versions`, or None.

    App Store Connect allows one editable version per platform; if more
    ever come back, take the most recently created one.
    """
    editable = [
        v
        for v in versions
        if (v.get("attributes") or {}).get("platform", "IOS") == "IOS"
        and version_state(v) in EDITABLE_STATES
    ]
    if not editable:
        return None
    return max(editable, key=lambda v: (v.get("attributes") or {}).get("createdDate") or "")


def build_status(builds):
    """Classify a `GET /v1/builds` result: 'missing', 'processing',
    'valid' or 'failed' (FAILED / INVALID)."""
    if not builds:
        return "missing"
    state = (builds[0].get("attributes") or {}).get("processingState")
    if state == "VALID":
        return "valid"
    if state in ("FAILED", "INVALID"):
        return "failed"
    return "processing"


def is_newer_build(attached, build):
    """True when the build already on the version (`attached`, a build
    number string or None) is numerically newer than `build`, so an
    older run that finished processing last must leave it alone."""
    try:
        return attached is not None and int(attached) > int(build)
    except ValueError:
        return False


def notice(msg):
    print(f"::notice::{msg}", flush=True)


def error(msg):
    print(f"::error::{msg}", flush=True)


# --- API client ---------------------------------------------------------


class _Failure(Exception):
    """One failed attempt inside `Client.request`."""

    def __init__(self, transient, detail):
        super().__init__(detail)
        self.transient = transient
        self.detail = detail


class Client:
    def __init__(self, key_id, issuer_id, key_path):
        self.key_id = key_id
        self.issuer_id = issuer_id
        self.key_path = key_path
        self._token = None
        self._minted = 0.0

    def token(self):
        now = time.time()
        if self._token is None or now - self._minted > TOKEN_REFRESH_S:
            headers, payload = token_claims(self.key_id, self.issuer_id, now)
            message = signing_input(headers, payload)
            self._token = f"{message}.{sign_es256(message, self.key_path)}"
            self._minted = now
        return self._token

    def request(self, method, path, params=None, body=None):
        url = API + path
        if params:
            url += "?" + urllib.parse.urlencode(params)
        data = json.dumps(body).encode() if body is not None else None
        attempts = GET_RETRIES + 1 if method == "GET" else 1
        for attempt in range(attempts):
            req = urllib.request.Request(url, data=data, method=method)
            req.add_header("Authorization", f"Bearer {self.token()}")
            if data is not None:
                req.add_header("Content-Type", "application/json")
            try:
                return self._send(req)
            except _Failure as f:
                if f.transient and attempt + 1 < attempts:
                    print(f"{method} {path}: {f.detail}, retrying", flush=True)
                    time.sleep(10 * (attempt + 1))
                    continue
                raise ApiError(f"{method} {path}: {f.detail}") from None
        raise AssertionError("unreachable")

    @staticmethod
    def _send(req):
        """One attempt. Every way it can fail comes out as a `_Failure`,
        so `request` is the only place that decides retry vs. ApiError."""
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            # Reading the error body can itself drop mid-way; that must
            # not escape as a raw IncompleteRead from inside this arm.
            try:
                detail = e.read().decode(errors="replace")
            except (http.client.HTTPException, OSError) as read_err:
                detail = f"(error body unreadable: {read_err!r})"
            raise _Failure(e.code == 429 or e.code >= 500, f"HTTP {e.code}: {detail}") from None
        except urllib.error.URLError as e:
            raise _Failure(True, str(e.reason)) from None
        except (http.client.HTTPException, OSError) as e:
            # urllib wraps only errors raised while *sending*; a
            # connection dropped or timed out while waiting for or
            # reading the response (RemoteDisconnected,
            # ConnectionResetError, TimeoutError, IncompleteRead)
            # comes through raw.
            raise _Failure(True, repr(e)) from None
        if not raw:
            return None
        try:
            return json.loads(raw)
        except ValueError:
            # A 200 that isn't JSON is a proxy/CDN page, not the API:
            # transient, like a 5xx.
            snippet = raw[:200].decode(errors="replace")
            raise _Failure(True, f"response is not JSON: {snippet!r}") from None


# --- steps --------------------------------------------------------------


def find_app(client, bundle_id):
    resp = client.request(
        "GET", "/v1/apps", {"filter[bundleId]": bundle_id, "fields[apps]": "bundleId,name"}
    )
    # filter[bundleId] also matches IDs that merely start with it.
    apps = [a for a in resp.get("data", []) if a["attributes"].get("bundleId") == bundle_id]
    if not apps:
        raise ApiError(f"no App Store Connect app with bundle ID {bundle_id}")
    return apps[0]


def wait_for_build(client, app_id, version, build):
    deadline = time.monotonic() + POLL_TIMEOUT_S
    params = {
        "filter[app]": app_id,
        "filter[version]": build,
        "filter[preReleaseVersion.version]": version,
        "fields[builds]": "version,processingState,uploadedDate",
        "limit": "1",
    }
    last = None
    while True:
        builds = client.request("GET", "/v1/builds", params).get("data", [])
        status = build_status(builds)
        if status == "valid":
            return builds[0]
        if status == "failed":
            state = builds[0]["attributes"].get("processingState")
            raise ApiError(f"build {version} ({build}) is {state} in App Store Connect")
        if status != last:
            print(
                "build not listed yet" if status == "missing" else "build is processing",
                "- polling every",
                POLL_INTERVAL_S,
                "s",
                flush=True,
            )
            last = status
        if time.monotonic() + POLL_INTERVAL_S > deadline:
            raise ApiError(
                f"build {version} ({build}) still {status} after "
                f"{POLL_TIMEOUT_S // 60} min; attach it by hand once it's processed"
            )
        time.sleep(POLL_INTERVAL_S)


def app_store_versions(client, app_id):
    resp = client.request(
        "GET",
        f"/v1/apps/{app_id}/appStoreVersions",
        {
            "filter[platform]": "IOS",
            "fields[appStoreVersions]": "platform,versionString,appVersionState,appStoreState,createdDate",
            "limit": "200",
        },
    )
    return resp.get("data", [])


def attached_build_number(client, version_id):
    """Build number currently set on the version, or None."""
    resp = client.request(
        "GET",
        f"/v1/appStoreVersions/{version_id}/build",
        {"fields[builds]": "version"},
    )
    data = (resp or {}).get("data")
    return (data.get("attributes") or {}).get("version") if data else None


def attach(client, version_id, build_id):
    client.request(
        "PATCH",
        f"/v1/appStoreVersions/{version_id}/relationships/build",
        body={"data": {"type": "builds", "id": build_id}},
    )


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--bundle-id", required=True)
    p.add_argument("--version", required=True, help="marketing version, e.g. 0.1.42")
    p.add_argument("--build", required=True, help="build number, e.g. 42")
    args = p.parse_args(argv)

    missing = [n for n in ("ASC_KEY_ID", "ASC_ISSUER_ID", "ASC_KEY_PATH") if not os.environ.get(n)]
    if missing:
        error(f"missing environment: {', '.join(missing)}")
        return 1

    try:
        client = Client(
            os.environ["ASC_KEY_ID"], os.environ["ASC_ISSUER_ID"], os.environ["ASC_KEY_PATH"]
        )
        app = find_app(client, args.bundle_id)
        name = app["attributes"].get("name") or args.bundle_id
        print(f"{name}: app {app['id']}; waiting for build {args.version} ({args.build})", flush=True)

        build = wait_for_build(client, app["id"], args.version, args.build)
        print(f"build {build['id']} is processed", flush=True)

        version = editable_version(app_store_versions(client, app["id"]))
        if version is None:
            notice(
                f"{name} has no editable iOS App Store version (one may be in review); "
                f"build {args.version} ({args.build}) was left unattached."
            )
            return 0

        vstring = version["attributes"].get("versionString")
        current = attached_build_number(client, version["id"])
        if is_newer_build(current, args.build):
            notice(
                f"{name}: App Store version {vstring} already has newer build {current}; "
                f"build {args.version} ({args.build}) was left unattached."
            )
            return 0
        attach(client, version["id"], build["id"])
        notice(
            f"{name}: build {args.version} ({args.build}) attached to App Store version "
            f"{vstring} ({version_state(version)}). Nothing was submitted for review."
        )
        return 0
    except ApiError as e:
        error(str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
