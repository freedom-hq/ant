//! Developer task runner. Cross-compiles `ant-ffi` for either an
//! iOS slice (`build-ios-sim` / `build-ios-device`), an Android slice
//! (`build-android-arm64` / `-armv7` / `-x86_64` / `-all`), or all
//! Android slices in series — the smoke-test apps in
//! `examples/ios-download/` and `examples/android-download/` link
//! one of the resulting libraries depending on which destination
//! their host build system is targeting.
//!
//! Usage:
//!
//!     cargo xtask build-ios-sim                    # aarch64-apple-ios-sim
//!     cargo xtask build-ios-sim --arch x86_64      # x86_64-apple-ios (Intel simulator)
//!     cargo xtask build-ios-sim --profile debug
//!     cargo xtask build-ios-device                 # aarch64-apple-ios (real iPhone / iPad)
//!     cargo xtask build-ios-device --profile debug
//!     cargo xtask build-ios-xcframework            # releasable AntFFI.xcframework (macOS only)
//!
//!     cargo xtask build-android-arm64              # aarch64-linux-android
//!     cargo xtask build-android-armv7              # armv7-linux-androideabi
//!     cargo xtask build-android-x86_64             # x86_64-linux-android (emulator)
//!     cargo xtask build-android-all                # all three Android ABIs
//!     cargo xtask build-android-arm64 --profile debug
//!
//! On success prints the absolute path of `libant_ffi.a` (iOS) or
//! `libant_ffi.so` (Android). For Android builds, the resulting `.so`
//! is also copied into
//! `examples/android-download/app/src/main/jniLibs/<abi>/` so a plain
//! `./gradlew assembleRelease` from the example directory finds it
//! without further wiring (mirroring the iOS xtask, which is invoked
//! from the Xcode "Build Rust" run-script phase).
//!
//! This stays in the xtask crate (no `cargo-*` sub-commands, no
//! `just`) so the repo is buildable with nothing but a stable Rust
//! toolchain plus, on Android, `cargo-ndk` and the Android NDK.

use std::env;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use anyhow::{anyhow, bail, Context, Result};

const DEFAULT_SIM_TARGET: &str = "aarch64-apple-ios-sim";
const X86_SIM_TARGET: &str = "x86_64-apple-ios";
const DEVICE_TARGET: &str = "aarch64-apple-ios";

/// Minimum iOS the released `AntFFI.xcframework` supports. Pinned
/// explicitly (instead of inheriting rustc's per-target default) so
/// the artifact has a documented, predictable floor. iOS 15 covers
/// every device back to the iPhone 6s; the in-repo example apps use a
/// much newer target, but they are demos, not the shipped artifact.
const IOS_DEPLOYMENT_TARGET: &str = "15.0";

const ANDROID_ARM64_TARGET: &str = "aarch64-linux-android";
const ANDROID_ARMV7_TARGET: &str = "armv7-linux-androideabi";
const ANDROID_X86_64_TARGET: &str = "x86_64-linux-android";

/// Android NDK API level (`-P` flag to `cargo ndk`). Matches the
/// `minSdk = 26` that `examples/android-download/app/build.gradle.kts`
/// declares — bumping this requires updating both sides in lock-step.
const ANDROID_NDK_API_LEVEL: &str = "26";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("xtask: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    let mut args = env::args_os().skip(1);
    let sub = args.next().ok_or_else(|| {
        anyhow!(
            "missing subcommand (try `build-ios-sim`, `build-ios-device`, \
             `build-ios-xcframework`, `build-android-arm64`, `build-android-armv7`, \
             `build-android-x86_64`, or `build-android-all`)"
        )
    })?;
    let rest: Vec<OsString> = args.collect();
    match sub.to_string_lossy().as_ref() {
        "build-ios-sim" => build_ios(rest, IosFlavor::Simulator),
        "build-ios-device" => build_ios(rest, IosFlavor::Device),
        "build-ios-xcframework" => build_ios_xcframework(&rest),
        "build-android-arm64" => build_android_one(&rest, ANDROID_ARM64_TARGET),
        "build-android-armv7" => build_android_one(&rest, ANDROID_ARMV7_TARGET),
        "build-android-x86_64" => build_android_one(&rest, ANDROID_X86_64_TARGET),
        "build-android-all" => build_android_all(&rest),
        "help" | "-h" | "--help" => {
            print_help();
            Ok(())
        }
        other => bail!("unknown subcommand: {other}"),
    }
}

fn print_help() {
    println!(
        "xtask — developer task runner\n\
         \n\
         SUBCOMMANDS:\n\
             build-ios-sim [--arch aarch64|x86_64] [--profile debug|release]\n\
                 Cross-compile crates/ant-ffi for the iOS simulator.\n\
                 Default: --arch aarch64 --profile release.\n\
             build-ios-device [--profile debug|release]\n\
                 Cross-compile crates/ant-ffi for a real iPhone / iPad\n\
                 (aarch64-apple-ios). Default: --profile release.\n\
             build-ios-xcframework\n\
                 Build the releasable AntFFI.xcframework (device +\n\
                 fat-simulator slices, header + module map). Always\n\
                 release + `chain`; requires macOS (lipo, xcodebuild).\n\
             build-android-arm64 [--profile debug|release]\n\
                 Cross-compile crates/ant-ffi for arm64 Android phones\n\
                 (aarch64-linux-android). Default: --profile release.\n\
             build-android-armv7 [--profile debug|release]\n\
                 Cross-compile for older armv7 Android phones\n\
                 (armv7-linux-androideabi).\n\
             build-android-x86_64 [--profile debug|release]\n\
                 Cross-compile for the Android emulator on x86_64 hosts\n\
                 (x86_64-linux-android).\n\
             build-android-all [--profile debug|release]\n\
                 Run build-android-arm64 + -armv7 + -x86_64 in series.\n\
                 Every build-android-* also takes --features <list> (extra\n\
                 features on top of jni, e.g. chain) and --app <name> (the\n\
                 examples/<name> app receiving jniLibs; default\n\
                 android-download).\n\
                 Requires `cargo-ndk` on PATH and the Android NDK installed.\n"
    );
}

#[derive(Clone, Copy)]
enum IosFlavor {
    Simulator,
    Device,
}

struct BuildOpts {
    target: &'static str,
    profile: Profile,
    /// Extra cargo features to enable (`--features a,b`). The `AntDrive`
    /// example app passes `chain` so its Storage tab can read the
    /// account's postage batch from Gnosis; the download-only
    /// `ios-download` app passes nothing.
    features: Vec<String>,
    /// Android only: the example app under `examples/` whose
    /// `app/src/main/jniLibs/<abi>/` receives the built `.so`
    /// (`--app`, default `android-download`).
    android_app: String,
}

#[derive(Clone, Copy)]
enum Profile {
    Debug,
    Release,
}

impl Profile {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Release => "release",
        }
    }
}

fn build_ios(args: Vec<OsString>, flavor: IosFlavor) -> Result<()> {
    let opts = parse_build_opts(args, flavor)?;

    ensure_rust_target(opts.target)?;

    let workspace_root = workspace_root()?;
    let mut cmd = Command::new(cargo_bin());
    cmd.arg("build")
        .arg("-p")
        .arg("ant-ffi")
        .arg("--target")
        .arg(opts.target)
        .current_dir(&workspace_root);
    if matches!(opts.profile, Profile::Release) {
        cmd.arg("--release");
    }
    if !opts.features.is_empty() {
        cmd.arg("--features").arg(opts.features.join(","));
    }
    // Xcode sets DEVELOPER_DIR / SDKROOT / IPHONEOS_DEPLOYMENT_TARGET
    // when driving xtask from a build phase. Propagating them by
    // default (no env scrubbing here) is what makes the script-phase
    // cross-compile work out of the box.

    let status = cmd
        .status()
        .with_context(|| format!("spawn `cargo build --target {}`", opts.target))?;
    if !status.success() {
        bail!("cargo build --target {} failed", opts.target);
    }

    let lib = workspace_root
        .join("target")
        .join(opts.target)
        .join(opts.profile.as_str())
        .join("libant_ffi.a");
    if !lib.exists() {
        bail!("expected {} after build, but it is missing", lib.display());
    }
    println!("{}", lib.display());
    Ok(())
}

fn parse_build_opts(args: Vec<OsString>, flavor: IosFlavor) -> Result<BuildOpts> {
    let mut target: &'static str = match flavor {
        IosFlavor::Simulator => DEFAULT_SIM_TARGET,
        IosFlavor::Device => DEVICE_TARGET,
    };
    let mut profile = Profile::Release;
    let mut features: Vec<String> = Vec::new();
    let mut it = args.into_iter();
    while let Some(arg) = it.next() {
        let arg = arg.to_string_lossy().into_owned();
        match arg.as_str() {
            "--features" => {
                let v = it
                    .next()
                    .ok_or_else(|| anyhow!("--features requires a value"))?
                    .to_string_lossy()
                    .into_owned();
                features.extend(
                    v.split([',', ' '])
                        .filter(|s| !s.is_empty())
                        .map(ToString::to_string),
                );
            }
            "--arch" => {
                if matches!(flavor, IosFlavor::Device) {
                    bail!("--arch is only meaningful for build-ios-sim (device is always aarch64)");
                }
                let v = it
                    .next()
                    .ok_or_else(|| anyhow!("--arch requires a value"))?
                    .to_string_lossy()
                    .into_owned();
                target = match v.as_str() {
                    "aarch64" => DEFAULT_SIM_TARGET,
                    "x86_64" => X86_SIM_TARGET,
                    other => bail!("unsupported --arch {other} (try aarch64 or x86_64)"),
                };
            }
            "--profile" => {
                let v = it
                    .next()
                    .ok_or_else(|| anyhow!("--profile requires a value"))?
                    .to_string_lossy()
                    .into_owned();
                profile = match v.as_str() {
                    "debug" => Profile::Debug,
                    "release" => Profile::Release,
                    other => bail!("unsupported --profile {other} (try debug or release)"),
                };
            }
            other => bail!("unknown argument: {other}"),
        }
    }
    Ok(BuildOpts {
        target,
        profile,
        features,
        android_app: String::new(),
    })
}

// ---------------------------------------------------------------------------
// AntFFI.xcframework (the interim pre-UniFFI release artifact;
// PLAN.md § "Interim releasable iOS artifact")
// ---------------------------------------------------------------------------

/// Build the releasable `AntFFI.xcframework`: device + fat-simulator
/// static libraries, the hand-written C header, and a module map so
/// Swift consumers write `import AntFFI` with no bridging header.
///
/// Always `--release --features chain` — features are baked into a
/// staticlib, and one variant beats a 2× artifact matrix (non-chain
/// callers pay only size). Must run on macOS (`lipo` + `xcodebuild`).
fn build_ios_xcframework(args: &[OsString]) -> Result<()> {
    if !args.is_empty() {
        bail!("build-ios-xcframework takes no arguments (always release + chain)");
    }
    let workspace_root = workspace_root()?;

    // 1. Cross-compile the three slices, pinning the deployment
    //    target so the artifact has a documented min-iOS floor.
    for target in [DEVICE_TARGET, DEFAULT_SIM_TARGET, X86_SIM_TARGET] {
        ensure_rust_target(target)?;
        let mut cmd = Command::new(cargo_bin());
        cmd.arg("build")
            .arg("-p")
            .arg("ant-ffi")
            .arg("--release")
            .arg("--features")
            .arg("chain")
            .arg("--target")
            .arg(target)
            .env("IPHONEOS_DEPLOYMENT_TARGET", IOS_DEPLOYMENT_TARGET)
            .current_dir(&workspace_root);
        let status = cmd
            .status()
            .with_context(|| format!("spawn `cargo build --target {target}`"))?;
        if !status.success() {
            bail!("cargo build --target {target} failed");
        }
    }

    let slice = |target: &str| {
        workspace_root
            .join("target")
            .join(target)
            .join("release")
            .join("libant_ffi.a")
    };
    let device_lib = slice(DEVICE_TARGET);
    let sim_arm = slice(DEFAULT_SIM_TARGET);
    let sim_x86 = slice(X86_SIM_TARGET);
    for lib in [&device_lib, &sim_arm, &sim_x86] {
        if !lib.exists() {
            bail!("expected {} after build, but it is missing", lib.display());
        }
    }

    // 2. Staging area. Recreated from scratch every run so a stale
    //    header or library can't leak into the artifact.
    let stage = workspace_root.join("target").join("ios-xcframework");
    if stage.exists() {
        std::fs::remove_dir_all(&stage)
            .with_context(|| format!("clear staging dir {}", stage.display()))?;
    }
    std::fs::create_dir_all(&stage)?;

    // 3. `lipo` the two simulator slices into one fat library:
    //    `-create-xcframework` accepts only one library per
    //    platform-variant (here: ios-simulator).
    let sim_fat_dir = stage.join("sim-fat");
    std::fs::create_dir_all(&sim_fat_dir)?;
    let sim_fat = sim_fat_dir.join("libant_ffi.a");
    let status = Command::new("lipo")
        .arg("-create")
        .arg(&sim_arm)
        .arg(&sim_x86)
        .arg("-output")
        .arg(&sim_fat)
        .status()
        .context("spawn `lipo` (this subcommand must run on macOS)")?;
    if !status.success() {
        bail!("lipo -create failed");
    }

    // 4. Stage the header + module map. The `link` directives carry
    //    the mandatory system-framework dependencies into consumers
    //    via autolinking, so an SPM `binaryTarget` user doesn't
    //    rediscover the linker errors by hand.
    let headers = stage.join("Headers");
    std::fs::create_dir_all(&headers)?;
    std::fs::copy(
        workspace_root
            .join("crates")
            .join("ant-ffi")
            .join("include")
            .join("ant.h"),
        headers.join("ant.h"),
    )
    .context("stage ant.h")?;
    std::fs::write(
        headers.join("module.modulemap"),
        "module AntFFI {\n    header \"ant.h\"\n    link framework \"Security\"\n    link framework \"SystemConfiguration\"\n    link framework \"CoreFoundation\"\n    export *\n}\n",
    )
    .context("write module.modulemap")?;

    // 5. Assemble the xcframework.
    let out = stage.join("AntFFI.xcframework");
    let status = Command::new("xcodebuild")
        .arg("-create-xcframework")
        .arg("-library")
        .arg(&device_lib)
        .arg("-headers")
        .arg(&headers)
        .arg("-library")
        .arg(&sim_fat)
        .arg("-headers")
        .arg(&headers)
        .arg("-output")
        .arg(&out)
        .status()
        .context("spawn `xcodebuild -create-xcframework`")?;
    if !status.success() {
        bail!("xcodebuild -create-xcframework failed");
    }

    println!("{}", out.display());
    Ok(())
}

// ---------------------------------------------------------------------------
// Android
//
// `cargo ndk` is the de-facto standard front-end for cross-compiling
// Rust to Android: it sets `CC_<triple>` / `AR_<triple>` /
// `CARGO_TARGET_<TRIPLE>_LINKER` to the prebuilt clang/lld inside
// `$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/<host>/bin/`, which is
// the only configuration where libp2p's transitive C deps build
// reliably across NDK versions. We always pass `--features jni` and
// `--crate-type cdylib` so a single `cargo ndk` invocation produces
// `libant_ffi.so` directly (skipping the rlib + staticlib emission
// that the `[lib] crate-type` declaration in `crates/ant-ffi/Cargo.toml`
// otherwise drags along).
// ---------------------------------------------------------------------------

fn build_android_one(args: &[OsString], target: &'static str) -> Result<()> {
    let opts = parse_android_opts(args, target)?;
    run_android_build(&opts)?;
    let lib = android_artifact_path(&opts)?;
    println!("{}", lib.display());
    Ok(())
}

fn build_android_all(args: &[OsString]) -> Result<()> {
    for target in [
        ANDROID_ARM64_TARGET,
        ANDROID_ARMV7_TARGET,
        ANDROID_X86_64_TARGET,
    ] {
        let opts = parse_android_opts(args, target)?;
        run_android_build(&opts)?;
        let lib = android_artifact_path(&opts)?;
        println!("{}", lib.display());
    }
    Ok(())
}

fn parse_android_opts(args: &[OsString], target: &'static str) -> Result<BuildOpts> {
    let mut profile = Profile::Release;
    let mut features: Vec<String> = Vec::new();
    let mut android_app = "android-download".to_string();
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        let arg = arg.to_string_lossy().into_owned();
        match arg.as_str() {
            "--profile" => {
                let v = it
                    .next()
                    .ok_or_else(|| anyhow!("--profile requires a value"))?
                    .to_string_lossy()
                    .into_owned();
                profile = match v.as_str() {
                    "debug" => Profile::Debug,
                    "release" => Profile::Release,
                    other => bail!("unsupported --profile {other} (try debug or release)"),
                };
            }
            "--features" => {
                let v = it
                    .next()
                    .ok_or_else(|| anyhow!("--features requires a value"))?
                    .to_string_lossy()
                    .into_owned();
                features.extend(
                    v.split([',', ' '])
                        .filter(|s| !s.is_empty() && *s != "jni")
                        .map(ToString::to_string),
                );
            }
            "--app" => {
                android_app = it
                    .next()
                    .ok_or_else(|| anyhow!("--app requires a value"))?
                    .to_string_lossy()
                    .into_owned();
            }
            other => bail!("unknown argument: {other}"),
        }
    }
    Ok(BuildOpts {
        target,
        profile,
        // `jni` itself is always added in `run_android_build`; these are
        // the extras on top (AntStream passes `chain` for its storage
        // plan and settlement calls).
        features,
        android_app,
    })
}

fn run_android_build(opts: &BuildOpts) -> Result<()> {
    ensure_cargo_ndk()?;
    ensure_rust_target(opts.target)?;

    let workspace_root = workspace_root()?;

    // cargo-ndk 4.x flag changes vs the 3.x flow we'd otherwise reach
    // for: `-P` (capital) is the API level, because lowercase `-p` now
    // passes through to cargo's `--package`. We deliberately skip
    // cargo-ndk's `-o jniLibs` mode because that recursively scans
    // `target/<triple>/<profile>/` and copies *every* `.so` it finds,
    // including transitive Rust deps like `if-watch` (which emits a
    // cdylib for use cases the smoke test doesn't hit). Instead, we
    // copy `libant_ffi.so` into the Gradle-shaped jniLibs tree
    // ourselves (see `copy_into_jni_libs` below). The inner
    // `cargo rustc` overrides `--crate-type cdylib` so only the `.so`
    // is emitted — the [lib] crate-type list in
    // `crates/ant-ffi/Cargo.toml` keeps staticlib + rlib for the
    // iOS / desktop slices.
    let mut cmd = Command::new("cargo");
    cmd.arg("ndk")
        .arg("-t")
        .arg(opts.target)
        .arg("-P")
        .arg(ANDROID_NDK_API_LEVEL)
        .arg("--")
        .arg("rustc")
        .arg("-p")
        .arg("ant-ffi")
        .arg("--features")
        .arg(
            std::iter::once("jni")
                .chain(opts.features.iter().map(String::as_str))
                .collect::<Vec<_>>()
                .join(","),
        )
        .arg("--crate-type")
        .arg("cdylib")
        .current_dir(&workspace_root);
    if matches!(opts.profile, Profile::Release) {
        cmd.arg("--release");
    }

    let status = cmd
        .status()
        .with_context(|| format!("spawn `cargo ndk -t {} -- rustc -p ant-ffi`", opts.target))?;
    if !status.success() {
        bail!("cargo ndk -t {} failed", opts.target);
    }

    let lib = android_artifact_path(opts)?;
    copy_into_jni_libs(opts, &lib)?;
    Ok(())
}

/// Copy `libant_ffi.so` into
/// `examples/<app>/app/src/main/jniLibs/<abi>/` (`--app`, default
/// `android-download`). Tolerates
/// the example app not existing yet — phase A of the Android plan
/// ships the Rust slice before the Gradle project, so the first
/// `build-android-arm64` after step 1 lands has nothing to copy into.
fn copy_into_jni_libs(opts: &BuildOpts, lib: &Path) -> Result<()> {
    let workspace_root = workspace_root()?;
    let abi = match opts.target {
        ANDROID_ARM64_TARGET => "arm64-v8a",
        ANDROID_ARMV7_TARGET => "armeabi-v7a",
        ANDROID_X86_64_TARGET => "x86_64",
        other => bail!("unknown Android ABI for target `{other}`"),
    };
    let main_src = workspace_root
        .join("examples")
        .join(&opts.android_app)
        .join("app")
        .join("src")
        .join("main");
    if !main_src.exists() {
        eprintln!(
            "xtask: {} does not exist yet; skipping jniLibs copy. \
             Build still emitted the .so at {}.",
            main_src.display(),
            lib.display()
        );
        return Ok(());
    }
    let dest_dir = main_src.join("jniLibs").join(abi);
    std::fs::create_dir_all(&dest_dir)
        .with_context(|| format!("create jniLibs destination {}", dest_dir.display()))?;
    let dest = dest_dir.join("libant_ffi.so");
    std::fs::copy(lib, &dest)
        .with_context(|| format!("copy {} -> {}", lib.display(), dest.display()))?;
    eprintln!("xtask: copied {} -> {}", lib.display(), dest.display());
    Ok(())
}

fn android_artifact_path(opts: &BuildOpts) -> Result<PathBuf> {
    let workspace_root = workspace_root()?;
    let lib = workspace_root
        .join("target")
        .join(opts.target)
        .join(opts.profile.as_str())
        .join("libant_ffi.so");
    if !lib.exists() {
        bail!("expected {} after build, but it is missing", lib.display());
    }
    Ok(lib)
}

/// Hint at the one-shot fix when `cargo-ndk` is missing. We don't try
/// to install it automatically because that would surprise operators
/// on restricted networks (same logic as `ensure_rust_target` below).
fn ensure_cargo_ndk() -> Result<()> {
    let out = Command::new("cargo").args(["ndk", "--version"]).output();
    let Ok(out) = out else {
        bail!("`cargo` is not on PATH; install Rust via rustup so `cargo ndk` can be invoked");
    };
    if out.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&out.stderr);
    let stdout = String::from_utf8_lossy(&out.stdout);
    bail!(
        "`cargo ndk --version` failed. Install cargo-ndk:\n\
         \n\
             cargo install cargo-ndk\n\
         \n\
         You also need the Android NDK (r26+) installed and either\n\
         `ANDROID_NDK_HOME` or `ANDROID_HOME` exported. cargo-ndk's stderr was:\n\
         {stderr}{stdout}"
    )
}

/// Check that `rustup target list --installed` mentions the target we
/// need, and if not, hint at the one-shot fix. We don't auto-add the
/// target because that would surprise operators on restricted networks
/// where `rustup` can't hit `static.rust-lang.org`.
fn ensure_rust_target(target: &str) -> Result<()> {
    let out = Command::new("rustup")
        .args(["target", "list", "--installed"])
        .output();
    let Ok(out) = out else {
        // rustup missing — assume the operator managed their toolchain
        // some other way (distro package, manual build). Let the
        // subsequent `cargo build` fail loudly if the target is
        // actually unavailable.
        return Ok(());
    };
    if !out.status.success() {
        return Ok(());
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    if stdout.lines().any(|l| l.trim() == target) {
        return Ok(());
    }
    bail!(
        "rust target `{target}` is not installed. Install it with:\n\
         \n\
             rustup target add {target}\n"
    );
}

fn cargo_bin() -> OsString {
    env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"))
}

fn workspace_root() -> Result<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let root = manifest_dir
        .parent()
        .ok_or_else(|| anyhow!("xtask crate has no parent directory"))?;
    let root_manifest = root.join("Cargo.toml");
    if !root_manifest.exists() {
        bail!(
            "expected workspace root at {} but no Cargo.toml there",
            root.display()
        );
    }
    Ok(absolutise(root))
}

fn absolutise(p: &Path) -> PathBuf {
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(p)
    }
}
