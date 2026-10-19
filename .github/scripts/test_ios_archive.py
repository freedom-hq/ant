#!/usr/bin/env python3
"""Offline tests for ios-archive.py's pure helpers.

Run with plain python3:
  python3 .github/scripts/test_ios_archive.py
"""

import importlib.util
import pathlib
import unittest

_path = pathlib.Path(__file__).with_name("ios-archive.py")
_spec = importlib.util.spec_from_file_location("ios_archive", _path)
ios = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ios)


SETTINGS = {
    "WRAPPER_EXTENSION": "app",
    "FULL_PRODUCT_NAME": "AntStream.app",
    "PRODUCT_BUNDLE_IDENTIFIER": "baby.freedom.antstream",
    "DEVELOPMENT_TEAM": "46XPHRBLH5",
}


class AppTargetSettings(unittest.TestCase):
    def test_picks_the_app_target(self):
        entries = [
            {"target": "Lib", "buildSettings": {"WRAPPER_EXTENSION": "framework"}},
            {"target": "AntStream", "buildSettings": SETTINGS},
        ]
        self.assertIs(ios.app_target_settings(entries), SETTINGS)

    def test_no_app_or_several_is_an_error(self):
        with self.assertRaises(ValueError):
            ios.app_target_settings([{"buildSettings": {"WRAPPER_EXTENSION": "framework"}}])
        with self.assertRaises(ValueError):
            ios.app_target_settings([{"buildSettings": SETTINGS}, {"buildSettings": SETTINGS}])


class Expansion(unittest.TestCase):
    def test_prefixes_come_from_the_team(self):
        v = ios.expansion_vars(SETTINGS)
        self.assertEqual(v["AppIdentifierPrefix"], "46XPHRBLH5.")
        self.assertEqual(v["TeamIdentifierPrefix"], "46XPHRBLH5.")

    def test_no_team_no_prefixes(self):
        self.assertNotIn("AppIdentifierPrefix", ios.expansion_vars({"WRAPPER_EXTENSION": "app"}))

    def test_expands_both_forms_in_nested_values(self):
        ents = {
            "keychain-access-groups": ["$(AppIdentifierPrefix)${PRODUCT_BUNDLE_IDENTIFIER}"],
            "com.apple.developer.associated-domains": ["applinks:example.com"],
            "aps-environment": "production",
            "com.apple.developer.icloud-container-environment": {"x": "$(TeamIdentifierPrefix)y"},
            "flag": True,
        }
        out = ios.expand_entitlements(ents, ios.expansion_vars(SETTINGS))
        self.assertEqual(
            out["keychain-access-groups"], ["46XPHRBLH5.baby.freedom.antstream"]
        )
        self.assertEqual(out["com.apple.developer.associated-domains"], ["applinks:example.com"])
        self.assertEqual(out["com.apple.developer.icloud-container-environment"], {"x": "46XPHRBLH5.y"})
        self.assertIs(out["flag"], True)
        self.assertEqual(ios.unresolved(out), [])

    def test_unknown_or_modified_references_are_reported(self):
        out = ios.expand_entitlements(
            {"a": ["$(NOPE)"], "b": "$(PRODUCT_BUNDLE_IDENTIFIER:lower)"},
            ios.expansion_vars(SETTINGS),
        )
        self.assertEqual(
            sorted(ios.unresolved(out)), ["$(NOPE)", "$(PRODUCT_BUNDLE_IDENTIFIER:lower)"]
        )


if __name__ == "__main__":
    unittest.main()
