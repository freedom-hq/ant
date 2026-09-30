//! Bee-compatible YAML config (`antd --config <file>`, PLAN.md
//! J.5.E1/E2).
//!
//! Freedom configures the node by writing a bee YAML config and
//! launching the binary with `--config <file>`. For a drop-in we parse
//! the subset of bee's config keys Freedom actually writes and map them
//! onto `antd`'s native settings. Keys we don't model are ignored (and
//! logged once) rather than rejected, so a fuller bee config still
//! starts the daemon.
//!
//! Precedence is bee's: an explicit CLI flag overrides the config file,
//! which overrides the built-in default. The merge itself lives in
//! `main.rs` (it needs clap's `ArgMatches` to know which flags were
//! given on the command line); this module only parses and normalises.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

/// `cors-allowed-origins` accepts either a single scalar (`"null"`,
/// `"*"`) or a YAML list. Untagged so both shapes deserialize. Entries
/// may also be wildcard subdomains `scheme://*.host` (host with at
/// least two labels); `antd` refuses to start on a malformed one.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum CorsOrigins {
    One(String),
    Many(Vec<String>),
}

impl CorsOrigins {
    pub fn into_vec(self) -> Vec<String> {
        match self {
            Self::One(s) => vec![s],
            Self::Many(v) => v,
        }
    }
}

/// Parsed bee config. Every modelled key is optional; unmodelled keys
/// land in `extra` so we can log them without failing.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct BeeConfig {
    #[serde(rename = "api-addr")]
    pub api_addr: Option<String>,
    #[serde(rename = "data-dir")]
    pub data_dir: Option<String>,
    pub password: Option<String>,
    #[serde(rename = "password-file")]
    pub password_file: Option<String>,
    pub mainnet: Option<bool>,
    #[serde(rename = "network-id")]
    pub network_id: Option<u64>,
    #[serde(rename = "full-node")]
    pub full_node: Option<bool>,
    #[serde(rename = "swap-enable")]
    pub swap_enable: Option<bool>,
    #[serde(rename = "blockchain-rpc-endpoint")]
    pub blockchain_rpc_endpoint: Option<String>,
    #[serde(rename = "cors-allowed-origins")]
    pub cors_allowed_origins: Option<CorsOrigins>,
    pub verbosity: Option<String>,
    #[serde(rename = "nat-addr")]
    pub nat_addr: Option<String>,
    /// Any other bee keys (e.g. `resolver-options`,
    /// `skip-postage-snapshot`, `storage-incentives-enable`). Captured
    /// so we can log "ignoring N unmodelled keys" rather than erroring.
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_yaml::Value>,
}

impl BeeConfig {
    /// Parse a bee YAML config from `path`.
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("read config {}", path.display()))?;
        let cfg: Self = serde_yaml::from_str(&raw)
            .with_context(|| format!("parse YAML config {}", path.display()))?;
        Ok(cfg)
    }

    /// Normalise bee's `api-addr` to a [`SocketAddr`]. bee accepts
    /// `:1633` (all interfaces) and `localhost:1633`; we bind those to
    /// loopback because `antd` is a local light node and a wildcard bind
    /// would expose the unauthenticated API to the LAN.
    pub fn api_socket_addr(&self) -> Result<Option<SocketAddr>> {
        let Some(raw) = self.api_addr.as_deref().map(str::trim) else {
            return Ok(None);
        };
        if raw.is_empty() {
            return Ok(None);
        }
        let normalized = if let Some(port) = raw.strip_prefix(':') {
            format!("127.0.0.1:{port}")
        } else if let Some(port) = raw.strip_prefix("localhost:") {
            format!("127.0.0.1:{port}")
        } else {
            raw.to_string()
        };
        let addr = normalized
            .parse::<SocketAddr>()
            .with_context(|| format!("invalid api-addr {raw:?}"))?;
        Ok(Some(addr))
    }

    /// Network id: explicit `network-id` wins; else `mainnet: true` → 1.
    /// (`mainnet: false` with no `network-id` leaves it unset so the CLI
    /// default / flag decides.)
    pub fn network_id(&self) -> Option<u64> {
        self.network_id.or(match self.mainnet {
            Some(true) => Some(1),
            _ => None,
        })
    }

    /// Resolve the keystore password: inline `password` wins, else the
    /// trimmed contents of `password-file`.
    pub fn resolve_password(&self) -> Result<Option<String>> {
        if let Some(p) = self.password.as_ref() {
            return Ok(Some(p.clone()));
        }
        if let Some(file) = self.password_file.as_deref() {
            let raw = std::fs::read_to_string(file)
                .with_context(|| format!("read password-file {file}"))?;
            // bee trims a single trailing newline; trim_end is safe since
            // a password never legitimately ends in whitespace.
            return Ok(Some(raw.trim_end_matches(['\n', '\r']).to_string()));
        }
        Ok(None)
    }

    /// Map bee's `verbosity` level onto an `antd`/`RUST_LOG` level.
    pub fn log_level(&self) -> Option<String> {
        self.verbosity.as_deref().map(|v| {
            match v.trim().to_ascii_lowercase().as_str() {
                "0" | "silent" | "none" => "off",
                "1" | "error" => "error",
                "2" | "warn" | "warning" => "warn",
                "3" | "info" => "info",
                "4" | "debug" => "debug",
                "5" | "trace" => "trace",
                other => other,
            }
            .to_string()
        })
    }

    pub fn cors_origins_vec(&self) -> Vec<String> {
        self.cors_allowed_origins
            .clone()
            .map(CorsOrigins::into_vec)
            .unwrap_or_default()
    }
}

/// Parse a bee config from a YAML string (test helper).
#[cfg(test)]
pub fn parse_str(yaml: &str) -> Result<BeeConfig> {
    serde_yaml::from_str(yaml).map_err(|e| anyhow::anyhow!("parse YAML config: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const FREEDOM_CONFIG: &str = r#"
api-addr: 127.0.0.1:1633
data-dir: /home/user/.freedom/swarm
password: hunter2
mainnet: true
full-node: false
swap-enable: true
blockchain-rpc-endpoint: https://rpc.gnosischain.com
cors-allowed-origins: "null"
verbosity: info
resolver-options:
  - "https://cloudflare-eth.com"
skip-postage-snapshot: true
storage-incentives-enable: false
"#;

    #[test]
    fn parses_freedom_shaped_config() {
        let cfg = parse_str(FREEDOM_CONFIG).unwrap();
        assert_eq!(
            cfg.api_socket_addr().unwrap().unwrap().to_string(),
            "127.0.0.1:1633",
        );
        assert_eq!(cfg.data_dir.as_deref(), Some("/home/user/.freedom/swarm"));
        assert_eq!(cfg.resolve_password().unwrap().as_deref(), Some("hunter2"));
        assert_eq!(cfg.network_id(), Some(1));
        assert_eq!(cfg.swap_enable, Some(true));
        assert_eq!(
            cfg.blockchain_rpc_endpoint.as_deref(),
            Some("https://rpc.gnosischain.com"),
        );
        assert_eq!(cfg.cors_origins_vec(), vec!["null".to_string()]);
        assert_eq!(cfg.log_level().as_deref(), Some("info"));
        // Unmodelled keys are captured, not rejected.
        assert!(cfg.extra.contains_key("resolver-options"));
        assert!(cfg.extra.contains_key("skip-postage-snapshot"));
    }

    #[test]
    fn api_addr_colon_port_binds_loopback() {
        let cfg = parse_str("api-addr: \":1633\"").unwrap();
        assert_eq!(
            cfg.api_socket_addr().unwrap().unwrap().to_string(),
            "127.0.0.1:1633",
        );
    }

    #[test]
    fn api_addr_localhost_binds_loopback() {
        let cfg = parse_str("api-addr: localhost:8080").unwrap();
        assert_eq!(
            cfg.api_socket_addr().unwrap().unwrap().to_string(),
            "127.0.0.1:8080",
        );
    }

    #[test]
    fn cors_accepts_list() {
        let cfg = parse_str("cors-allowed-origins:\n  - https://a.example\n  - https://b.example")
            .unwrap();
        assert_eq!(
            cfg.cors_origins_vec(),
            vec![
                "https://a.example".to_string(),
                "https://b.example".to_string()
            ],
        );
    }

    #[test]
    fn empty_config_is_all_none() {
        let cfg = parse_str("{}").unwrap();
        assert!(cfg.api_socket_addr().unwrap().is_none());
        assert!(cfg.network_id().is_none());
        assert!(cfg.resolve_password().unwrap().is_none());
    }
}
