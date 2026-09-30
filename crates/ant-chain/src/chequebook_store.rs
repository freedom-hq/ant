//! Shared chequebook bootstrap: the on-disk association record plus the
//! "resolve / deploy a node-owned chequebook" mechanics that both
//! `antd` (at startup) and `ant-ffi` (on the first storage buy) need
//! for outbound SWAP settlement.
//!
//! The cheque *protocol* lives in [`crate::chequebook`] (EIP-712
//! signing, calldata) and the *wire* lives in `ant-p2p`. This module is
//! the thin orchestration layer in between: where a chequebook comes
//! from and how its address is persisted across restarts, so a node
//! deploys once and reuses forever. Building the actual
//! `ant_p2p::PushsyncSwapConfig` from the resolved address stays with
//! each caller, since that type belongs to a higher layer.
//!
//! `load_persisted_chequebook` / `persist_chequebook` / [`ChequebookFile`]
//! are pure file I/O and compile everywhere; the deploy / factory-check
//! helpers drive a JSON-RPC node and are gated on `chain-rpc`.

use std::path::Path;

/// On-disk record of the chequebook a node auto-deployed (or
/// rediscovered) for outbound settlement, persisted at
/// `<data-dir>/chequebook.json`. Written once on first deploy and
/// reloaded on every subsequent start (deploy-once, reuse-forever —
/// the equivalent of the chequebook record bee keeps in its
/// statestore, which we can't read). The issuer baked into the
/// chequebook is the node EOA, so the node signing key is the cheque
/// signer; the remaining fields are pure provenance for operator
/// debugging / on-chain cross-referencing.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChequebookFile {
    /// Deployed `SimpleSwap` contract address (`0x` + 40 hex).
    pub chequebook: String,
    /// Issuer EOA baked into the chequebook (the node's own address).
    pub issuer: String,
    /// CREATE2 salt used for the deploy (hex). Empty for a
    /// rediscovered chequebook whose salt we never learned.
    #[serde(default)]
    pub salt: String,
    /// Deploy tx hash (hex). Empty for a rediscovered chequebook.
    #[serde(default)]
    pub deploy_tx: String,
}

impl ChequebookFile {
    /// Build a record for `chequebook` issued by `issuer`, with no
    /// known salt / deploy tx (the rediscovered-chequebook case).
    #[must_use]
    pub fn rediscovered(chequebook: &[u8; 20], issuer: &[u8; 20]) -> Self {
        Self {
            chequebook: format!("0x{}", hex::encode(chequebook)),
            issuer: format!("0x{}", hex::encode(issuer)),
            salt: String::new(),
            deploy_tx: String::new(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ChequebookError {
    #[error("read chequebook association {0}: {1}")]
    Read(String, std::io::Error),
    #[error("parse chequebook association {0}: {1}")]
    Parse(String, serde_json::Error),
    #[error("decode persisted chequebook address '{0}'")]
    Decode(String),
    #[error("write {0}: {1}")]
    Write(String, std::io::Error),
    #[error("serialize chequebook association: {0}")]
    Serialize(serde_json::Error),
    /// Any JSON-RPC / transaction failure during deploy or factory
    /// verification, flattened to a string so the error type stays
    /// feature-agnostic for callers.
    #[cfg(feature = "chain-rpc")]
    #[error("{0}")]
    Chain(String),
    /// The node wallet can't afford the one-time deploy. Callers that
    /// treat auto-deploy as best-effort (e.g. `ant-ffi`) match this to
    /// skip softly; `antd` logs it and starts without settlement.
    #[cfg(feature = "chain-rpc")]
    #[error(
        "node wallet 0x{wallet} has {have} wei xDAI, needs ~{need} wei to deploy a chequebook"
    )]
    InsufficientGas {
        wallet: String,
        have: String,
        need: String,
    },
    #[cfg(feature = "chain-rpc")]
    #[error("deploy tx 0x{0} confirmed but emitted no SimpleSwapDeployed log")]
    NoDeployLog(String),
}

fn strip_0x(s: &str) -> &str {
    s.strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s)
}

/// Load the persisted chequebook address from `path`, if the file
/// exists. A malformed file is a hard error — silently ignoring it
/// would re-trigger a deploy and waste gas on every restart, so callers
/// decide whether to treat that as fatal (`antd`) or self-heal
/// (`ant-ffi` re-deploys on the next buy).
pub fn load_persisted_chequebook(path: &Path) -> Result<Option<[u8; 20]>, ChequebookError> {
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(path)
        .map_err(|e| ChequebookError::Read(path.display().to_string(), e))?;
    let file: ChequebookFile = serde_json::from_str(&raw)
        .map_err(|e| ChequebookError::Parse(path.display().to_string(), e))?;
    let mut cb = [0u8; 20];
    hex::decode_to_slice(strip_0x(file.chequebook.trim()), &mut cb)
        .map_err(|_| ChequebookError::Decode(file.chequebook.clone()))?;
    Ok(Some(cb))
}

/// Like [`load_persisted_chequebook`], but only adopts the record when
/// the `issuer` it names is `owner`.
///
/// A chequebook's issuer is baked into the contract on-chain: bee only
/// accepts a cheque whose signature recovers to `chequebook.issuer()`,
/// so a node that signs with a different key emits cheques every peer
/// silently drops while its own settlement status reads "ready". A
/// record can outlive the account that wrote it whenever the node key
/// changes under a fixed data dir (a restore-from-backup-key flow), and
/// the file already carries the owner, so callers that know their own
/// EOA should use this rather than trusting the path. A foreign record
/// reads as `Ok(None)` — "no chequebook for this account" — which is
/// exactly what a fresh account is, so callers rediscover or deploy
/// their own instead of failing.
pub fn load_persisted_chequebook_for(
    path: &Path,
    owner: &[u8; 20],
) -> Result<Option<[u8; 20]>, ChequebookError> {
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(path)
        .map_err(|e| ChequebookError::Read(path.display().to_string(), e))?;
    let file: ChequebookFile = serde_json::from_str(&raw)
        .map_err(|e| ChequebookError::Parse(path.display().to_string(), e))?;
    let mut issuer = [0u8; 20];
    hex::decode_to_slice(strip_0x(file.issuer.trim()), &mut issuer)
        .map_err(|_| ChequebookError::Decode(file.issuer.clone()))?;
    if issuer != *owner {
        tracing::warn!(
            path = %path.display(),
            issuer = %format!("0x{}", hex::encode(issuer)),
            owner = %format!("0x{}", hex::encode(owner)),
            "ignoring chequebook association issued by a different account",
        );
        return Ok(None);
    }
    let mut cb = [0u8; 20];
    hex::decode_to_slice(strip_0x(file.chequebook.trim()), &mut cb)
        .map_err(|_| ChequebookError::Decode(file.chequebook.clone()))?;
    Ok(Some(cb))
}

/// Atomically persist the chequebook association (write-tmp + rename),
/// so a crash mid-write can't leave a half-written file that a later
/// start would reject.
pub fn persist_chequebook(path: &Path, file: &ChequebookFile) -> Result<(), ChequebookError> {
    let json = serde_json::to_string_pretty(file).map_err(ChequebookError::Serialize)?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, json.as_bytes())
        .map_err(|e| ChequebookError::Write(tmp.display().to_string(), e))?;
    std::fs::rename(&tmp, path)
        .map_err(|e| ChequebookError::Write(path.display().to_string(), e))?;
    Ok(())
}

/// One `eth_call` to the Swarm chequebook factory's
/// `deployedContracts(address)` view. Returns `Ok(true)` iff the
/// factory has a record of having deployed `chequebook` itself — which
/// is exactly the predicate bee's `chequeStore.ReceiveCheque` uses to
/// decide whether to accept a cheque drawn on it. Cheques drawn on a
/// chequebook the factory doesn't know about are silently dropped by
/// bee, so emitting them just wastes bandwidth.
#[cfg(feature = "chain-rpc")]
pub async fn verify_chequebook_with_factory(
    client: &crate::ChainClient,
    chequebook: &[u8; 20],
) -> Result<bool, ChequebookError> {
    use crate::chequebook::{factory_deployed_contracts_calldata, GNOSIS_CHEQUEBOOK_FACTORY};

    let calldata = factory_deployed_contracts_calldata(chequebook);
    let v = client
        .eth_call(
            &format!("0x{}", hex::encode(GNOSIS_CHEQUEBOOK_FACTORY)),
            &format!("0x{}", hex::encode(&calldata)),
        )
        .await
        .map_err(|e| ChequebookError::Chain(format!("factory.deployedContracts eth_call: {e}")))?;
    if v.len() < 32 {
        return Err(ChequebookError::Chain(format!(
            "factory.deployedContracts returned <32 bytes (got {} bytes)",
            v.len()
        )));
    }
    let last_word = &v[v.len() - 32..];
    Ok(last_word.iter().any(|&b| b != 0))
}

/// One `eth_call` to `chequebook.issuer()` returning the 20-byte EOA
/// the contract recognises as its issuer. Cheques are only accepted by
/// bee peers when signed by this exact key (bee recovers the signer in
/// `cashChequeBeneficiary` and the chequebook's `issuer()` must match),
/// so a node whose cheque-signing key differs from `issuer()` emits
/// cheques every peer silently drops. Callers compare the result to the
/// EOA derived from their signing key before enabling outbound SWAP.
#[cfg(feature = "chain-rpc")]
pub async fn read_chequebook_issuer(
    client: &crate::ChainClient,
    chequebook: &[u8; 20],
) -> Result<[u8; 20], ChequebookError> {
    use crate::chequebook::chequebook_issuer_selector;

    let v = client
        .eth_call(
            &format!("0x{}", hex::encode(chequebook)),
            &format!("0x{}", hex::encode(chequebook_issuer_selector())),
        )
        .await
        .map_err(|e| ChequebookError::Chain(format!("chequebook.issuer eth_call: {e}")))?;
    if v.len() < 32 {
        return Err(ChequebookError::Chain(format!(
            "chequebook.issuer returned <32 bytes (got {} bytes)",
            v.len()
        )));
    }
    // Address is the low 20 bytes of the right-aligned 32-byte word.
    let word = &v[v.len() - 32..];
    let mut out = [0u8; 20];
    out.copy_from_slice(&word[12..32]);
    Ok(out)
}

/// Deploy a fresh factory-registered chequebook (issuer = `node_eth`,
/// gas paid by `wallet`), persist the association at `persist_path`,
/// then optionally fund it with up to `deposit_plur` xBZZ (capped to
/// the wallet's balance; `0` deploys it unfunded — bee still accepts
/// the cheques, cashing waits for a later deposit). Returns the
/// deployed chequebook address.
///
/// A gas pre-flight runs first: a deploy we can't pay for just burns a
/// failed-tx wait, so an underfunded wallet surfaces as
/// [`ChequebookError::InsufficientGas`] before anything is signed. The
/// reserve covers the funding transfer too when `deposit_plur > 0`.
#[cfg(feature = "chain-rpc")]
pub async fn auto_deploy_chequebook(
    client: &crate::ChainClient,
    wallet: &crate::tx::Wallet,
    node_eth: &[u8; 20],
    deposit_plur: u128,
    persist_path: &Path,
) -> Result<[u8; 20], ChequebookError> {
    use crate::chequebook::{
        extract_deployed_chequebook, random_chequebook_salt, DEFAULT_HARD_DEPOSIT_TIMEOUT_SECS,
        GNOSIS_BZZ_TOKEN_BYTES, GNOSIS_CHEQUEBOOK_FACTORY,
    };
    use crate::tx::{DEFAULT_GAS_PRICE_WEI, ERC20_TRANSFER_GAS, FACTORY_DEPLOY_CHEQUEBOOK_GAS};
    use primitive_types::U256;

    let gas_units = if deposit_plur > 0 {
        FACTORY_DEPLOY_CHEQUEBOOK_GAS + ERC20_TRANSFER_GAS
    } else {
        FACTORY_DEPLOY_CHEQUEBOOK_GAS
    };
    let need_gas_wei = U256::from(DEFAULT_GAS_PRICE_WEI).saturating_mul(U256::from(gas_units));
    let native = client
        .eth_get_balance_lower128(node_eth)
        .await
        .map_err(|e| ChequebookError::Chain(format!("read node xDAI balance: {e}")))?;
    if U256::from(native) < need_gas_wei {
        return Err(ChequebookError::InsufficientGas {
            wallet: hex::encode(node_eth),
            have: native.to_string(),
            need: need_gas_wei.to_string(),
        });
    }

    let salt = random_chequebook_salt();
    tracing::info!(
        target: "ant_chain::chequebook",
        factory = %format!("0x{}", hex::encode(GNOSIS_CHEQUEBOOK_FACTORY)),
        issuer = %format!("0x{}", hex::encode(node_eth)),
        "auto-deploying a factory-registered chequebook (issuer = node EOA)",
    );
    let receipt = wallet
        .deploy_chequebook(
            client,
            &GNOSIS_CHEQUEBOOK_FACTORY,
            node_eth,
            U256::from(DEFAULT_HARD_DEPOSIT_TIMEOUT_SECS),
            &salt,
        )
        .await
        .map_err(|e| ChequebookError::Chain(format!("factory.deploySimpleSwap: {e}")))?;
    let cb = extract_deployed_chequebook(&receipt)
        .ok_or_else(|| ChequebookError::NoDeployLog(hex::encode(receipt.tx_hash)))?;
    tracing::info!(
        target: "ant_chain::chequebook",
        chequebook = %format!("0x{}", hex::encode(cb)),
        tx = %format!("0x{}", hex::encode(receipt.tx_hash)),
        block = receipt.block_number,
        "auto-deployed chequebook",
    );

    // Persist before funding so a crash between the deploy and the
    // transfer still reuses this chequebook on the next start rather
    // than deploying a second one.
    persist_chequebook(
        persist_path,
        &ChequebookFile {
            chequebook: format!("0x{}", hex::encode(cb)),
            issuer: format!("0x{}", hex::encode(node_eth)),
            salt: format!("0x{}", hex::encode(salt)),
            deploy_tx: format!("0x{}", hex::encode(receipt.tx_hash)),
        },
    )?;

    if deposit_plur > 0 {
        match client
            .erc20_balance_of_lower128(crate::GNOSIS_BZZ_TOKEN, node_eth)
            .await
        {
            Ok(bzz_bal) => {
                let deposit = deposit_plur.min(bzz_bal);
                if deposit == 0 {
                    tracing::warn!(
                        target: "ant_chain::chequebook",
                        chequebook = %format!("0x{}", hex::encode(cb)),
                        "node wallet holds no xBZZ; deployed an unfunded chequebook (cheques are still accepted; deposit BZZ later)",
                    );
                } else {
                    match wallet
                        .erc20_transfer(client, &GNOSIS_BZZ_TOKEN_BYTES, &cb, U256::from(deposit))
                        .await
                    {
                        Ok(r) => tracing::info!(
                            target: "ant_chain::chequebook",
                            chequebook = %format!("0x{}", hex::encode(cb)),
                            deposit_plur = deposit,
                            tx = %format!("0x{}", hex::encode(r.tx_hash)),
                            "funded chequebook with xBZZ",
                        ),
                        Err(e) => tracing::warn!(
                            target: "ant_chain::chequebook",
                            chequebook = %format!("0x{}", hex::encode(cb)),
                            "chequebook funding transfer failed; chequebook is deployed and usable but unfunded: {e}",
                        ),
                    }
                }
            }
            Err(e) => tracing::warn!(
                target: "ant_chain::chequebook",
                "could not read node xBZZ balance; deployed chequebook left unfunded: {e}",
            ),
        }
    }

    Ok(cb)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persist_then_load_round_trips() {
        let dir = std::env::temp_dir().join(format!("ant-cb-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("chequebook.json");
        let cb = [0x11u8; 20];
        persist_chequebook(&path, &ChequebookFile::rediscovered(&cb, &[0x22u8; 20])).unwrap();
        assert_eq!(load_persisted_chequebook(&path).unwrap(), Some(cb));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn owner_checked_load_skips_another_accounts_chequebook() {
        let dir = std::env::temp_dir().join(format!("ant-cb-owner-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("chequebook.json");
        let cb = [0x11u8; 20];
        let issuer = [0x22u8; 20];
        persist_chequebook(&path, &ChequebookFile::rediscovered(&cb, &issuer)).unwrap();

        // Its own issuer adopts it; anyone else sees "no chequebook" and
        // deploys their own rather than signing cheques bee will drop.
        assert_eq!(
            load_persisted_chequebook_for(&path, &issuer).unwrap(),
            Some(cb)
        );
        assert_eq!(
            load_persisted_chequebook_for(&path, &[0x33u8; 20]).unwrap(),
            None
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn missing_file_is_none() {
        let path = std::env::temp_dir().join("definitely-not-a-chequebook-file-xyz.json");
        let _ = std::fs::remove_file(&path);
        assert_eq!(load_persisted_chequebook(&path).unwrap(), None);
    }

    #[test]
    fn malformed_file_is_error() {
        let dir = std::env::temp_dir().join(format!("ant-cb-bad-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("chequebook.json");
        std::fs::write(&path, b"not json").unwrap();
        assert!(load_persisted_chequebook(&path).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }
}
