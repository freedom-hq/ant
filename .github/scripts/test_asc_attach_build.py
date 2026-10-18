#!/usr/bin/env python3
"""Offline tests for asc-attach-build.py's pure helpers.

Run with plain python3 (no PyJWT needed):
  python3 .github/scripts/test_asc_attach_build.py
"""

import importlib.util
import pathlib
import unittest

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
        for state in ("PREPARE_FOR_SUBMISSION", "DEVELOPER_REJECTED", "REJECTED", "METADATA_REJECTED"):
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


if __name__ == "__main__":
    unittest.main()
