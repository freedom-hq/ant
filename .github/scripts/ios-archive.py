#!/usr/bin/env python3
"""Archive an iOS app *unsigned*, for `xcodebuild -exportArchive` to sign.

Run by the `testflight` job of the iOS TestFlight workflows
(antdrive-ios-testflight.yml, antstream-ios-testflight.yml) on macOS.

Why unsigned: archiving with the project's Automatic signing and
`-allowProvisioningUpdates` makes Xcode's cloud signing mint a new
"Apple Development: Created via API" certificate on every fresh runner
(its private key dies with the runner), and the team's development
certificate cap filled up after a few releases (issue #230). Archiving
with CODE_SIGNING_ALLOWED=NO needs no certificate at all. The export
step then signs the app with the cloud-managed Apple Distribution
certificate, which is not created per run and doesn't count against the
development cap.

What that costs, and how it's kept: an unsigned archive carries no
entitlements, and the export signs only with the base ones it derives
from the provisioning profile (application-identifier, team ID,
get-task-allow, beta-reports-active), dropping anything the target's
CODE_SIGN_ENTITLEMENTS file declares. Xcode refuses ad-hoc signing for
the iOS SDK, so when the target has an entitlements file this script
expands its $(VAR) references from the target's build settings and signs
the archived app ad-hoc (`codesign --sign -`) with the result; the
export keeps the entitlements of the signature it replaces. App
extensions (a PlugIns/ or Extensions/ folder) are not handled and fail
the step rather than ship without their entitlements.

Usage:
  ios-archive.py --project AntStream.xcodeproj --scheme AntStream \
      --archive-path "$RUNNER_TEMP/AntStream.xcarchive" \
      --version 0.1.42 --build 42

Run from the directory holding the project. Needs no credentials.
"""

import argparse
import json
import os
import plistlib
import re
import subprocess
import sys
import tempfile

CONFIGURATION = "Release"
DESTINATION = "generic/platform=iOS"

# The $(NAME) and ${NAME} references Xcode expands in an entitlements
# file. Modifiers ($(NAME:lower)) aren't supported and are left in place,
# which `unresolved` then reports.
_VAR = re.compile(r"\$\(([A-Za-z_][A-Za-z0-9_]*)\)|\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def error(msg):
    print(f"::error::{msg}", flush=True)


# --- pure helpers (unit-tested offline) ---------------------------------


def app_target_settings(show_build_settings):
    """The build settings of the app target among the entries
    `xcodebuild -showBuildSettings -json` printed (one per target the
    scheme builds)."""
    apps = [
        e["buildSettings"]
        for e in show_build_settings
        if e.get("buildSettings", {}).get("WRAPPER_EXTENSION") == "app"
    ]
    if len(apps) != 1:
        raise ValueError(f"expected one app target in the scheme, found {len(apps)}")
    return apps[0]


def expansion_vars(settings):
    """The variables available to an entitlements file: the target's
    build settings plus the two prefixes signing normally supplies."""
    team = settings.get("DEVELOPMENT_TEAM", "")
    out = dict(settings)
    if team:
        out.setdefault("AppIdentifierPrefix", f"{team}.")
        out.setdefault("TeamIdentifierPrefix", f"{team}.")
    return out


def expand(text, variables):
    """Replace every $(NAME) / ${NAME} whose NAME is in `variables`."""

    def sub(m):
        name = m.group(1) or m.group(2)
        return variables.get(name, m.group(0))

    return _VAR.sub(sub, text)


def expand_entitlements(value, variables):
    """`expand` applied to every string in a parsed entitlements plist."""
    if isinstance(value, str):
        return expand(value, variables)
    if isinstance(value, list):
        return [expand_entitlements(v, variables) for v in value]
    if isinstance(value, dict):
        return {k: expand_entitlements(v, variables) for k, v in value.items()}
    return value


def unresolved(value):
    """Every string in `value` that still holds a $( or ${ reference."""
    if isinstance(value, str):
        return [value] if ("$(" in value or "${" in value) else []
    if isinstance(value, list):
        return [s for v in value for s in unresolved(v)]
    if isinstance(value, dict):
        return [s for v in value.values() for s in unresolved(v)]
    return []


# --- steps --------------------------------------------------------------


def xcodebuild(*args, capture=False):
    cmd = ["xcodebuild", *args]
    print("+", " ".join(cmd), flush=True)
    return subprocess.run(cmd, check=True, capture_output=capture, text=capture).stdout


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--project", required=True)
    p.add_argument("--scheme", required=True)
    p.add_argument("--archive-path", required=True)
    p.add_argument("--version", required=True, help="marketing version, e.g. 0.1.42")
    p.add_argument("--build", required=True, help="build number, e.g. 42")
    args = p.parse_args(argv)

    common = [
        "-project", args.project,
        "-scheme", args.scheme,
        "-configuration", CONFIGURATION,
        "-destination", DESTINATION,
        # Nothing signs here, so nothing asks Apple for a certificate.
        "CODE_SIGNING_ALLOWED=NO",
        f"MARKETING_VERSION={args.version}",
        f"CURRENT_PROJECT_VERSION={args.build}",
    ]
    try:
        xcodebuild("archive", "-archivePath", args.archive_path, *common)
        settings = app_target_settings(
            json.loads(xcodebuild("-showBuildSettings", "-json", *common, capture=True))
        )
    except subprocess.CalledProcessError as e:
        error(f"xcodebuild failed (exit {e.returncode})")
        return 1
    except ValueError as e:
        error(str(e))
        return 1

    app = os.path.join(
        args.archive_path, "Products", "Applications", settings["FULL_PRODUCT_NAME"]
    )
    if not os.path.isdir(app):
        error(f"archive has no {app}")
        return 1
    for nested in ("PlugIns", "Extensions"):
        if os.path.isdir(os.path.join(app, nested)):
            error(
                f"{settings['FULL_PRODUCT_NAME']} embeds {nested}/: app extensions aren't "
                "handled by .github/scripts/ios-archive.py and would lose their "
                "entitlements; extend it before shipping one"
            )
            return 1

    ent_setting = settings.get("CODE_SIGN_ENTITLEMENTS", "").strip()
    if not ent_setting:
        print("no CODE_SIGN_ENTITLEMENTS: archive left unsigned", flush=True)
        return 0

    ent_path = os.path.join(settings.get("PROJECT_DIR", "."), ent_setting)
    with open(ent_path, "rb") as f:
        entitlements = expand_entitlements(plistlib.load(f), expansion_vars(settings))
    left = unresolved(entitlements)
    if left:
        error(f"{ent_setting}: can't expand {', '.join(sorted(set(left)))}")
        return 1

    with tempfile.NamedTemporaryFile("wb", suffix=".entitlements", delete=False) as f:
        plistlib.dump(entitlements, f)
        expanded = f.name
    try:
        print(f"carrying {ent_setting} into the archive: {sorted(entitlements)}", flush=True)
        subprocess.run(
            ["codesign", "--force", "--sign", "-", "--entitlements", expanded,
             "--generate-entitlement-der", app],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        error(f"ad-hoc signing {app} with {ent_setting} failed (exit {e.returncode})")
        return 1
    finally:
        os.unlink(expanded)
    return 0


if __name__ == "__main__":
    sys.exit(main())
