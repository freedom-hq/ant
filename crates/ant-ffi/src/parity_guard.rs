//! Guard for the "one orchestration, two sequencers" rule in AGENTS.md.
//!
//! `antd` and `ant-ffi` are two entry points over the same node, and each
//! sequences the same startup and chain-init steps. The #49 phantom-batch
//! fix landed in `antd` only because `ant-ffi` had its own copy of the
//! step (see `docs/ffi-parity-audit.md`). The rule: those decisions live
//! as `pub` helpers in the shared orchestration modules below, and both
//! entry points call them.
//!
//! This test lists every free `pub fn` in those modules and checks that
//! each one is referenced by both `antd` (its `main.rs`, and the
//! gateway's chain writer its HTTP routes run through) and
//! `crates/ant-ffi/src/*.rs`. Comments and `#[cfg(test)]` modules don't
//! count. A helper referenced by neither is fine: it's a building block
//! for another helper. A helper referenced by exactly one entry point
//! fails the test unless [`ONE_SIDED`] lists it with a reason, and an
//! entry there that no longer applies fails it too, so the list can't
//! rot.
//!
//! It checks references, not call order. It can't tell a startup call
//! from one behind a host FFI call, so it doesn't replace reading the
//! audit when an orchestration step changes.

/// The shared orchestration modules: `(path for messages, source)`.
const ORCHESTRATION_MODULES: &[(&str, &str)] = &[
    (
        "crates/ant-chain/src/discover.rs",
        include_str!("../../ant-chain/src/discover.rs"),
    ),
    (
        "crates/ant-chain/src/chequebook_store.rs",
        include_str!("../../ant-chain/src/chequebook_store.rs"),
    ),
    (
        "crates/ant-chain/src/funding.rs",
        include_str!("../../ant-chain/src/funding.rs"),
    ),
];

/// `antd`'s orchestration lives in `main.rs`, plus the gateway's chain
/// writer, which its storage and chequebook HTTP routes run through. If
/// it moves to another module, add that module here.
///
/// `ant-ffi`'s gateway (`ant_start_gateway`) runs through the same
/// writer, so a helper only the writer calls counts as `antd`-only here:
/// `ant-ffi`'s C API should call it too.
const ANTD_SOURCES: &[&str] = &[
    include_str!("../../antd/src/main.rs"),
    include_str!("../../ant-gateway/src/chainreader.rs"),
];

/// Every `ant-ffi` source file except this one.
const FFI_SOURCES: &[&str] = &[
    include_str!("lib.rs"),
    include_str!("drive.rs"),
    include_str!("gateway.rs"),
    include_str!("chain_transport.rs"),
    include_str!("stream.rs"),
    include_str!("bench.rs"),
    include_str!("manifest.rs"),
    include_str!("jni.rs"),
];

/// Orchestration helpers deliberately used by only one entry point:
/// `(helper, the side that uses it, why the other side doesn't)`.
const ONE_SIDED: &[(&str, Side, &str)] = &[
    (
        "top_up_batch",
        Side::Antd,
        "bee's `PATCH /stamps/topup` pays from the wallet's xBZZ; the C API only extends with \
         xDAI, through `extend_with_xdai`, which calls it",
    ),
    (
        "read_retrieval_funds",
        Side::Ffi,
        "the C API's deposit top-up re-reads the funds once and starts the funds watch, since \
         `ant_init` reloads a chequebook without an RPC and starts none; `antd` starts its watch \
         (`watch_retrieval_funds`, which reads through this) with settlement at startup",
    ),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Antd,
    Ffi,
}

/// `src` without `//` comments and without the `#[cfg(test)]` module
/// that ends the file (this repo keeps test modules last).
fn code_only(src: &str) -> String {
    let lines: Vec<&str> = src.lines().collect();
    let mut end = lines.len();
    for (i, line) in lines.iter().enumerate() {
        let next = lines.get(i + 1).map_or("", |l| l.trim_end());
        if line.starts_with("#[cfg(")
            && line.contains("test")
            && next.starts_with("mod ")
            && next.ends_with('{')
        {
            end = i;
            break;
        }
    }
    lines[..end]
        .iter()
        .map(|l| l.find("//").map_or(*l, |i| &l[..i]))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Names of the free `pub fn` / `pub async fn` items in `src`.
fn pub_fns(src: &str) -> Vec<String> {
    code_only(src)
        .lines()
        .filter_map(|l| {
            l.strip_prefix("pub fn ")
                .or_else(|| l.strip_prefix("pub async fn "))
        })
        .map(|rest| {
            rest.chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect()
        })
        .collect()
}

/// Whether `name` appears in `code` as a whole identifier.
fn mentions(code: &str, name: &str) -> bool {
    let ident = |c: char| c.is_ascii_alphanumeric() || c == '_';
    code.match_indices(name).any(|(i, _)| {
        let before = code[..i].chars().next_back();
        let after = code[i + name.len()..].chars().next();
        !before.is_some_and(ident) && !after.is_some_and(ident)
    })
}

fn used_by(sources: &[&str], name: &str) -> bool {
    sources.iter().any(|src| mentions(&code_only(src), name))
}

#[test]
fn shared_orchestration_helpers_are_used_by_both_entry_points() {
    let mut problems = Vec::new();
    let mut helpers = Vec::new();
    for (path, src) in ORCHESTRATION_MODULES {
        for name in pub_fns(src) {
            let side = match (used_by(ANTD_SOURCES, &name), used_by(FFI_SOURCES, &name)) {
                (true, false) => Some(Side::Antd),
                (false, true) => Some(Side::Ffi),
                _ => None,
            };
            let allowed = ONE_SIDED.iter().find(|(n, _, _)| *n == name);
            match (side, allowed) {
                (Some(side), None) => problems.push(format!(
                    "`{name}` ({path}) is used by {side:?} only. Call it from the other entry \
                     point too, or add it to ONE_SIDED with the reason"
                )),
                (Some(side), Some((_, listed, _))) if side != *listed => problems.push(format!(
                    "`{name}` is listed in ONE_SIDED as {listed:?}-only but is used by {side:?} only"
                )),
                (None, Some(_)) => problems.push(format!(
                    "`{name}` is listed in ONE_SIDED but isn't one-sided any more; remove the entry"
                )),
                _ => {}
            }
            helpers.push(name);
        }
    }
    for (name, _, _) in ONE_SIDED {
        if !helpers.iter().any(|h| h == name) {
            problems.push(format!(
                "ONE_SIDED lists `{name}`, which isn't a pub fn in the orchestration modules"
            ));
        }
    }
    assert!(
        !helpers.is_empty(),
        "found no helpers; the module sources moved?"
    );
    assert!(
        problems.is_empty(),
        "antd / ant-ffi orchestration drift (AGENTS.md, docs/ffi-parity-audit.md):\n  {}",
        problems.join("\n  "),
    );
}

#[test]
fn guard_parsing_sees_what_it_should() {
    let src = "\
pub fn shared_step() {}
pub async fn other_step(x: u8) {}
    pub fn method_is_not_free() {}
// pub fn commented_out() {}
#[cfg(test)]
mod tests {
    pub fn test_only() {}
}
";
    assert_eq!(pub_fns(src), vec!["shared_step", "other_step"]);
    let code = code_only("let a = shared_step(); // other_step\nlet b = shared_step_for();");
    assert!(mentions(&code, "shared_step"));
    assert!(!mentions(&code, "other_step"), "comments don't count");
    assert!(
        !mentions("shared_step_for()", "shared_step"),
        "whole identifiers only"
    );
}
