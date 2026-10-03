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
//! `load_persisted_chequebook_for` / `persist_chequebook` / [`ChequebookFile`]
//! are pure file I/O and compile everywhere; the deploy / factory-check
//! helpers drive a JSON-RPC node and are gated on `chain-rpc`.

use std::path::Path;

/// Target xBZZ deposit behind a node's chequebook, in PLUR (1 xBZZ =
/// 1e16 PLUR): **0.001 xBZZ**, the one default for `antd` and `ant-ffi`.
/// It is grounded in the #67 benchmark, where that deposit backed 65 K+
/// cheques with a wide margin (~300× one soak's measured settlement
/// demand). That is small enough not to compete with the postage the
/// user came to buy, and it isn't spent money: an unspent deposit stays
/// withdrawable by the issuer. A fresh deploy is funded up to it, and
/// an adopted chequebook is topped back up to it
/// ([`top_up_chequebook`]).
pub const DEFAULT_CHEQUEBOOK_DEPOSIT_PLUR: u128 = 10_000_000_000_000;

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
/// exists **and** the `issuer` it names is `owner`. This is the only
/// loader: both `antd` and `ant-ffi` know their own EOA, and trusting
/// the path alone is exactly the drift the parity audit found
/// (`docs/ffi-parity-audit.md`).
///
/// A malformed file is an error rather than "none": silently ignoring
/// it could re-trigger a deploy on every start, so callers decide.
/// `antd` treats it as fatal; `ant-ffi` rediscovers on-chain first and
/// only deploys (overwriting the file) when the chain has none.
///
/// A chequebook's issuer is baked into the contract on-chain: bee only
/// accepts a cheque whose signature recovers to `chequebook.issuer()`,
/// so a node that signs with a different key emits cheques every peer
/// silently drops while its own settlement status reads "ready". A
/// record can outlive the account that wrote it whenever the node key
/// changes under a fixed data dir (a restore-from-backup-key flow), and
/// the file already carries the owner, so the path alone is never
/// trusted. A foreign record
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

/// The two on-chain checks a chequebook must pass before a node signs
/// cheques on it (see [`check_chequebook`]). Each field keeps its own
/// read result so a caller can report them separately, as `antd` does.
#[cfg(feature = "chain-rpc")]
#[derive(Debug)]
pub struct ChequebookChecks {
    /// [`verify_chequebook_with_factory`]: `Ok(false)` means bee drops
    /// every cheque drawn on it.
    pub registered: Result<bool, ChequebookError>,
    /// [`read_chequebook_issuer`]: bee only accepts cheques signed by
    /// this EOA. `None` when [`check_chequebook`]'s [`IssuerRead`]
    /// policy skipped the read (nothing to compare against, or the
    /// factory check already disqualified the chequebook).
    pub issuer: Option<Result<[u8; 20], ChequebookError>>,
}

/// When [`check_chequebook`] reads `issuer()` — each read is an RPC
/// round-trip, so skip it when its answer can't change the outcome.
#[cfg(feature = "chain-rpc")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IssuerRead {
    /// Don't read it (e.g. no signing EOA to compare it with).
    Never,
    /// Read it unless the factory already answered "not registered",
    /// which disqualifies the chequebook on its own.
    UnlessUnregistered,
    /// Always read it (a caller that reports every check even when one
    /// has already failed, like `antd` under
    /// `--chequebook-allow-unverified`).
    Always,
}

/// Whether a node signing with a given key may use a chequebook, per
/// [`ChequebookChecks::verdict`].
#[cfg(feature = "chain-rpc")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChequebookVerdict {
    /// Every check that could be read passed.
    Usable,
    /// The Swarm chequebook factory doesn't know it.
    NotRegistered,
    /// Its on-chain `issuer()` is this other EOA, not the signer.
    IssuerMismatch([u8; 20]),
}

#[cfg(feature = "chain-rpc")]
impl ChequebookChecks {
    /// The shared rule for `antd` and `ant-ffi`: a check that answered
    /// "no" disqualifies the chequebook, while a check whose read
    /// *failed* is skipped rather than failed. Unverified is not bad, and
    /// dropping settlement over an RPC hiccup stalls uploads. `antd`
    /// lets `--chequebook-allow-unverified` override a disqualification.
    #[must_use]
    pub fn verdict(&self, signer: &[u8; 20]) -> ChequebookVerdict {
        if matches!(self.registered, Ok(false)) {
            return ChequebookVerdict::NotRegistered;
        }
        match self.issuer {
            Some(Ok(issuer)) if issuer != *signer => ChequebookVerdict::IssuerMismatch(issuer),
            _ => ChequebookVerdict::Usable,
        }
    }
}

/// How long after our own deploy (the record's write time) a factory
/// "not registered" may still be a lagging backend. A load-balanced RPC
/// trails by seconds to a few minutes, not for good.
#[cfg(feature = "chain-rpc")]
pub const DEPLOY_LAG_GRACE: std::time::Duration = std::time::Duration::from_mins(10);

/// Whether a factory "not registered" answer for the persisted `cb` may
/// just be a backend that hasn't seen our deploy yet (a load-balanced
/// RPC trails by a few blocks; e.g. `ant-ffi`'s launch-time
/// `ant_deploy_chequebook` followed within seconds by the gateway
/// start's check, or `antd` topping up the chequebook it deployed at
/// startup on the first stamp buy). Only for a chequebook we deployed ourselves — the
/// record carries its deploy tx — and only within [`DEPLOY_LAG_GRACE`]
/// of the record being written: `true` then when that tx's receipt isn't
/// visible or can't be read (unconfirmed, not "no"), or shows the
/// factory deploying `cb` (registered by construction). A visible
/// receipt without that deploy lets the "not registered" stand, as does
/// a record without a deploy tx (a rediscovered chequebook) or one older
/// than the grace (a backend doesn't lag for that long; a record whose
/// deploy tx isn't on this chain would otherwise pass as "lag" forever).
#[cfg(feature = "chain-rpc")]
pub async fn not_registered_may_be_lag(
    client: &crate::ChainClient,
    persist_path: &std::path::Path,
    cb: &[u8; 20],
) -> bool {
    use crate::chequebook::{GNOSIS_CHEQUEBOOK_FACTORY, SIMPLE_SWAP_DEPLOYED_TOPIC};

    // An unreadable mtime, or one in the future (clock change), counts as
    // outside the grace: the factory's "no" is a real answer.
    let recent = std::fs::metadata(persist_path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.elapsed().ok())
        .is_some_and(|age| age < DEPLOY_LAG_GRACE);
    if !recent {
        return false;
    }
    let Some(deploy_tx) = std::fs::read(persist_path)
        .ok()
        .and_then(|b| serde_json::from_slice::<ChequebookFile>(&b).ok())
        .and_then(|f| {
            let mut tx = [0u8; 32];
            hex::decode_to_slice(f.deploy_tx.trim_start_matches("0x"), &mut tx).ok()?;
            Some(tx)
        })
    else {
        return false;
    };
    match client.eth_get_transaction_receipt(&deploy_tx).await {
        Ok(Some(receipt)) => receipt.logs.iter().any(|l| {
            l.address == GNOSIS_CHEQUEBOOK_FACTORY
                && l.topics.first() == Some(&SIMPLE_SWAP_DEPLOYED_TOPIC)
                && l.data.get(12..32) == Some(cb.as_slice())
        }),
        Ok(None) | Err(_) => true,
    }
}

/// What [`top_up_chequebook`] did.
#[cfg(feature = "chain-rpc")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopUp {
    /// The chequebook already holds at least the target.
    NotNeeded,
    /// Moved `amount` PLUR from the wallet into the chequebook.
    Funded { amount: u128, tx: [u8; 32] },
    /// The chequebook is `shortfall` PLUR short, and the wallet holds no
    /// xBZZ to give.
    WalletEmpty { shortfall: u128 },
    /// The chain says no to this chequebook for `node_eth` (not
    /// factory-registered, or issued by someone else): nothing was
    /// sent. Peers drop its cheques, so a deposit would only strand
    /// xBZZ in it; the caller should switch settlement off for it.
    Refused(ChequebookVerdict),
}

/// Top `chequebook`'s xBZZ deposit up to `target_plur` from the node
/// wallet. The transfer is capped to what the wallet holds: a thin
/// wallet gives a partial deposit rather than a failed transfer (some
/// backing beats none). It never withdraws, so a chequebook at or above
/// the target is left alone.
///
/// Right before the transfer it runs [`check_chequebook`] and sends
/// only on a positive answer to both checks (registered with the
/// factory, `issuer()` is `node_eth`). Stricter than
/// [`ChequebookChecks::verdict`], which lets a failed read pass so an
/// RPC hiccup doesn't switch settlement off: a deposit can't be taken
/// back, so a failed read is an error here and a "no" is
/// [`TopUp::Refused`]. The check runs whatever the caller already
/// verified, so no path can fund a chequebook the chain rejects.
///
/// A chequebook that backs nothing only publishes until the peers'
/// payment tolerance runs out, then stalls (#73). Adopted chequebooks
/// (persisted, or rediscovered on-chain) can be sitting at zero, so both
/// `antd` and `ant-ffi` top them up with this before relying on them.
#[cfg(feature = "chain-rpc")]
pub async fn top_up_chequebook(
    client: &crate::ChainClient,
    wallet: &crate::tx::Wallet,
    node_eth: &[u8; 20],
    chequebook: &[u8; 20],
    target_plur: u128,
) -> Result<TopUp, ChequebookError> {
    use crate::chequebook::GNOSIS_BZZ_TOKEN_BYTES;
    use primitive_types::U256;

    let have = client
        .erc20_balance_of_lower128(crate::GNOSIS_BZZ_TOKEN, chequebook)
        .await
        .map_err(|e| ChequebookError::Chain(format!("read chequebook deposit: {e}")))?;
    let shortfall = target_plur.saturating_sub(have);
    if shortfall == 0 {
        return Ok(TopUp::NotNeeded);
    }
    let wallet_bzz = client
        .erc20_balance_of_lower128(crate::GNOSIS_BZZ_TOKEN, node_eth)
        .await
        .map_err(|e| ChequebookError::Chain(format!("read wallet xBZZ: {e}")))?;
    let amount = shortfall.min(wallet_bzz);
    if amount == 0 {
        return Ok(TopUp::WalletEmpty { shortfall });
    }
    let checks = check_chequebook(client, chequebook, IssuerRead::UnlessUnregistered).await;
    match checks.verdict(node_eth) {
        ChequebookVerdict::Usable => {}
        refused => return Ok(TopUp::Refused(refused)),
    }
    // Usable with a read that failed is unverified, not a "yes".
    let unread = [
        checks.registered.err(),
        checks.issuer.map_or_else(
            || Some(ChequebookError::Chain("issuer() not read".into())),
            Result::err,
        ),
    ];
    if let Some(e) = unread.into_iter().flatten().next() {
        return Err(ChequebookError::Chain(format!(
            "could not verify the chequebook before depositing into it: {e}"
        )));
    }
    let receipt = wallet
        .erc20_transfer(
            client,
            &GNOSIS_BZZ_TOKEN_BYTES,
            chequebook,
            U256::from(amount),
        )
        .await
        .map_err(|e| ChequebookError::Chain(format!("deposit transfer: {e}")))?;
    Ok(TopUp::Funded {
        amount,
        tx: receipt.tx_hash,
    })
}

/// What a node's chequebook can put into SWAP cheques, and at which
/// rates: for downloads (issue #121) and uploads (issue #127) alike,
/// since both pay through the one shared payer. (The `Retrieval` in the
/// name predates #127.) Read with [`read_retrieval_funds`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetrievalFunds {
    /// xBZZ ever deposited and not withdrawn, in PLUR: the chequebook's
    /// balance plus what beneficiaries have already cashed
    /// (`totalPaidOut`). Bee's `AvailableBalance` is this minus every
    /// cheque issued so far, which only the issuing node knows.
    pub deposited_plur: u128,
    /// Bee's oracle exchange rate, PLUR per accounting unit.
    pub exchange_rate_plur: u128,
    /// Bee's oracle deduction, PLUR, added to the first cheque to a peer.
    pub deduction_plur: u128,
}

/// How often the entry points re-read [`RetrievalFunds`] while
/// settlement runs ([`watch_retrieval_funds`]), so a deposit made by any
/// path (a buy, a top-up, `POST /chequebook/deposit`, someone else's
/// transfer) starts paying for downloads and uploads (pushsync) within
/// this long.
#[cfg(feature = "chain-rpc")]
pub const RETRIEVAL_FUNDS_REFRESH: std::time::Duration = std::time::Duration::from_secs(60);

/// Read [`RetrievalFunds`] for `chequebook`: its xBZZ balance, its
/// `totalPaidOut()`, and bee's price oracle (`getPrice()`). Any failed
/// read is an error, never a zero: the caller keeps what it knew.
#[cfg(feature = "chain-rpc")]
pub async fn read_retrieval_funds(
    client: &crate::ChainClient,
    chequebook: &[u8; 20],
) -> Result<RetrievalFunds, ChequebookError> {
    use crate::chequebook::{
        chequebook_total_paid_out_selector, price_oracle_get_price_selector,
        GNOSIS_SWAP_PRICE_ORACLE,
    };
    let word = |v: &[u8], at: usize, what: &str| -> Result<u128, ChequebookError> {
        let w = v
            .get(at..at + 32)
            .ok_or_else(|| ChequebookError::Chain(format!("{what} returned {} bytes", v.len())))?;
        if w[..16].iter().any(|&b| b != 0) {
            return Err(ChequebookError::Chain(format!("{what} overflows u128")));
        }
        Ok(u128::from_be_bytes(w[16..].try_into().expect("16 bytes")))
    };
    let balance = client
        .erc20_balance_of_lower128(crate::GNOSIS_BZZ_TOKEN, chequebook)
        .await
        .map_err(|e| ChequebookError::Chain(format!("read chequebook balance: {e}")))?;
    let paid_out = client
        .eth_call(
            &format!("0x{}", hex::encode(chequebook)),
            &format!("0x{}", hex::encode(chequebook_total_paid_out_selector())),
        )
        .await
        .map_err(|e| ChequebookError::Chain(format!("chequebook.totalPaidOut eth_call: {e}")))?;
    let paid_out = word(&paid_out, 0, "chequebook.totalPaidOut")?;
    let price = client
        .eth_call(
            &format!("0x{}", hex::encode(GNOSIS_SWAP_PRICE_ORACLE)),
            &format!("0x{}", hex::encode(price_oracle_get_price_selector())),
        )
        .await
        .map_err(|e| ChequebookError::Chain(format!("priceOracle.getPrice eth_call: {e}")))?;
    Ok(RetrievalFunds {
        deposited_plur: balance.saturating_add(paid_out),
        exchange_rate_plur: word(&price, 0, "priceOracle.getPrice")?,
        deduction_plur: word(&price, 32, "priceOracle.getPrice")?,
    })
}

/// Keep the node's view of `chequebook`'s [`RetrievalFunds`] current:
/// read them now and every [`RETRIEVAL_FUNDS_REFRESH`], handing each
/// successful read to `publish`. A failed read publishes nothing, so the
/// node keeps its last good value (a hiccup doesn't read as "empty").
/// Returns when `publish` returns `false` (the node is gone).
///
/// Both `antd` and `ant-ffi` run this for the chequebook settlement runs
/// on, publishing into `ControlCommand::SetRetrievalFunds`, so SWAP
/// payments for downloads (issue #121) and uploads (issue #127) spend at
/// most what the chequebook holds, on either entry point. Each keeps one
/// watch per node and aborts it when settlement moves to another
/// chequebook (or, in `antd`, is switched off): the node accepts funds
/// for any chequebook, so a stale watch would otherwise keep overwriting
/// the current one's.
#[cfg(feature = "chain-rpc")]
pub async fn watch_retrieval_funds<F, Fut>(
    client: crate::ChainClient,
    chequebook: [u8; 20],
    mut publish: F,
) where
    F: FnMut(RetrievalFunds) -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    loop {
        match read_retrieval_funds(&client, &chequebook).await {
            Ok(funds) => {
                if !publish(funds).await {
                    return;
                }
            }
            Err(e) => tracing::debug!(
                target: "ant_chain::chequebook_store",
                chequebook = %hex::encode(chequebook),
                "retrieval funds not refreshed: {e}",
            ),
        }
        tokio::time::sleep(RETRIEVAL_FUNDS_REFRESH).await;
    }
}

/// Run the chequebook checks against the chain: factory registration,
/// then `issuer()` as `issuer_read` allows. Shared by `antd` (every
/// chequebook it adopts at startup) and `ant-ffi` (a persisted
/// chequebook before enabling settlement), so the two can't disagree on
/// what makes a chequebook unusable.
#[cfg(feature = "chain-rpc")]
pub async fn check_chequebook(
    client: &crate::ChainClient,
    chequebook: &[u8; 20],
    issuer_read: IssuerRead,
) -> ChequebookChecks {
    let registered = verify_chequebook_with_factory(client, chequebook).await;
    let read_issuer = match issuer_read {
        IssuerRead::Never => false,
        IssuerRead::UnlessUnregistered => !matches!(registered, Ok(false)),
        IssuerRead::Always => true,
    };
    let issuer = if read_issuer {
        Some(read_chequebook_issuer(client, chequebook).await)
    } else {
        None
    };
    ChequebookChecks { registered, issuer }
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
        assert_eq!(
            load_persisted_chequebook_for(&path, &[0x22u8; 20]).unwrap(),
            Some(cb)
        );
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
        assert_eq!(
            load_persisted_chequebook_for(&path, &[0x22u8; 20]).unwrap(),
            None
        );
    }

    #[test]
    fn malformed_file_is_error() {
        let dir = std::env::temp_dir().join(format!("ant-cb-bad-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("chequebook.json");
        std::fs::write(&path, b"not json").unwrap();
        assert!(load_persisted_chequebook_for(&path, &[0x22u8; 20]).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(feature = "chain-rpc")]
    #[test]
    fn chequebook_verdict_disqualifies_only_on_a_read_no() {
        let signer = [0x0au8; 20];
        let failed = || ChequebookError::Chain("backend unavailable".into());
        let checks = |registered, issuer| ChequebookChecks {
            registered,
            issuer: Some(issuer),
        };

        assert_eq!(
            checks(Ok(true), Ok(signer)).verdict(&signer),
            ChequebookVerdict::Usable
        );
        assert_eq!(
            checks(Ok(false), Ok(signer)).verdict(&signer),
            ChequebookVerdict::NotRegistered
        );
        assert_eq!(
            checks(Ok(true), Ok([0x5e; 20])).verdict(&signer),
            ChequebookVerdict::IssuerMismatch([0x5e; 20])
        );
        // Not registered wins over a mismatch: it's the first thing bee checks.
        assert_eq!(
            checks(Ok(false), Ok([0x5e; 20])).verdict(&signer),
            ChequebookVerdict::NotRegistered
        );
        // A read that failed is skipped, never a disqualification.
        assert_eq!(
            checks(Err(failed()), Ok(signer)).verdict(&signer),
            ChequebookVerdict::Usable
        );
        assert_eq!(
            checks(Ok(true), Err(failed())).verdict(&signer),
            ChequebookVerdict::Usable
        );
        assert_eq!(
            checks(Err(failed()), Err(failed())).verdict(&signer),
            ChequebookVerdict::Usable
        );
        // An issuer read that was skipped is not a mismatch.
        let skipped = |registered| ChequebookChecks {
            registered,
            issuer: None,
        };
        assert_eq!(
            skipped(Ok(true)).verdict(&signer),
            ChequebookVerdict::Usable
        );
        assert_eq!(
            skipped(Ok(false)).verdict(&signer),
            ChequebookVerdict::NotRegistered
        );
    }

    /// A scripted backend for the top-up decisions: xBZZ `balanceOf`,
    /// plus the two chequebook checks (`None` answers an RPC error).
    /// Anything else (a transfer) answers an error and is counted.
    #[cfg(feature = "chain-rpc")]
    struct Balances {
        of: std::collections::HashMap<[u8; 20], u128>,
        registered: Option<bool>,
        issuer: Option<[u8; 20]>,
        other_calls: std::sync::Mutex<usize>,
    }

    #[cfg(feature = "chain-rpc")]
    impl crate::transport::ChainTransport for Balances {
        fn serve(&self, request_json: &str) -> Option<String> {
            let req: serde_json::Value = serde_json::from_str(request_json).unwrap();
            let data = req["params"][0]["data"].as_str().unwrap_or_default();
            let error = || {
                Some(
                    serde_json::json!({"jsonrpc": "2.0", "id": req["id"],
                        "error": {"code": -32603, "message": "backend unavailable"}})
                    .to_string(),
                )
            };
            let word = |w: String| {
                Some(
                    serde_json::json!({"jsonrpc": "2.0", "id": req["id"], "result": w}).to_string(),
                )
            };
            if req["method"] == "eth_call" {
                let deployed_sel = hex::encode(
                    &crate::chequebook::factory_deployed_contracts_calldata(&CHEQUEBOOK)[..4],
                );
                let issuer_sel = hex::encode(crate::chequebook::chequebook_issuer_selector());
                if data.starts_with("0x70a08231") {
                    let mut owner = [0u8; 20];
                    hex::decode_to_slice(&data[34..74], &mut owner).unwrap();
                    return match self.of.get(&owner) {
                        Some(bal) => word(format!("0x{bal:064x}")),
                        None => error(),
                    };
                }
                if data[2..].starts_with(&deployed_sel) {
                    return match self.registered {
                        Some(r) => word(format!("0x{:064x}", u8::from(r))),
                        None => error(),
                    };
                }
                if data[2..].starts_with(&issuer_sel) {
                    return match self.issuer {
                        Some(i) => word(format!("0x{}{}", "00".repeat(12), hex::encode(i))),
                        None => error(),
                    };
                }
            }
            *self.other_calls.lock().unwrap() += 1;
            Some(
                serde_json::json!({"jsonrpc": "2.0", "id": req["id"],
                    "error": {"code": -32603, "message": "scripted: no transfer here"}})
                .to_string(),
            )
        }
    }

    #[cfg(feature = "chain-rpc")]
    async fn top_up_with(of: &[([u8; 20], u128)]) -> (Result<TopUp, ChequebookError>, usize) {
        top_up_checked(of, Some(true), Some(WALLET)).await
    }

    #[cfg(feature = "chain-rpc")]
    async fn top_up_checked(
        of: &[([u8; 20], u128)],
        registered: Option<bool>,
        issuer: Option<[u8; 20]>,
    ) -> (Result<TopUp, ChequebookError>, usize) {
        let script = std::sync::Arc::new(Balances {
            of: of.iter().copied().collect(),
            registered,
            issuer,
            other_calls: std::sync::Mutex::new(0),
        });
        let client =
            crate::ChainClient::new("http://127.0.0.1:1").with_transport(Some(script.clone()));
        let wallet = crate::tx::Wallet::new([7u8; 32], crate::tx::GNOSIS_CHAIN_ID).unwrap();
        let result = top_up_chequebook(
            &client,
            &wallet,
            &WALLET,
            &CHEQUEBOOK,
            DEFAULT_CHEQUEBOOK_DEPOSIT_PLUR,
        )
        .await;
        let others = *script.other_calls.lock().unwrap();
        (result, others)
    }

    /// A scripted chain for [`read_retrieval_funds`]: the chequebook's
    /// xBZZ balance, its `totalPaidOut()` and the price oracle. `None`
    /// answers an RPC error.
    #[cfg(feature = "chain-rpc")]
    struct FundsChain {
        balance: Option<u128>,
        paid_out: Option<u128>,
        price: Option<(u128, u128)>,
    }

    #[cfg(feature = "chain-rpc")]
    impl crate::transport::ChainTransport for FundsChain {
        fn serve(&self, request_json: &str) -> Option<String> {
            let req: serde_json::Value = serde_json::from_str(request_json).unwrap();
            let to = req["params"][0]["to"]
                .as_str()
                .unwrap_or_default()
                .to_lowercase();
            let data = req["params"][0]["data"].as_str().unwrap_or_default();
            let answer = if data.starts_with("0x70a08231") {
                self.balance.map(|b| format!("0x{b:064x}"))
            } else if to == format!("0x{}", hex::encode(CHEQUEBOOK)) {
                assert_eq!(
                    &data[2..10],
                    hex::encode(crate::chequebook::chequebook_total_paid_out_selector())
                );
                self.paid_out.map(|p| format!("0x{p:064x}"))
            } else {
                assert_eq!(
                    to,
                    format!(
                        "0x{}",
                        hex::encode(crate::chequebook::GNOSIS_SWAP_PRICE_ORACLE)
                    )
                );
                self.price.map(|(r, d)| format!("0x{r:064x}{d:064x}"))
            };
            Some(
                match answer {
                    Some(w) => serde_json::json!({"jsonrpc": "2.0", "id": req["id"], "result": w}),
                    None => serde_json::json!({"jsonrpc": "2.0", "id": req["id"],
                        "error": {"code": -32603, "message": "backend unavailable"}}),
                }
                .to_string(),
            )
        }
    }

    #[cfg(feature = "chain-rpc")]
    async fn funds_from(chain: FundsChain) -> Result<RetrievalFunds, ChequebookError> {
        let client = crate::ChainClient::new("http://127.0.0.1:1")
            .with_transport(Some(std::sync::Arc::new(chain)));
        read_retrieval_funds(&client, &CHEQUEBOOK).await
    }

    /// The funds are the deposit ever made (balance + cashed out), at the
    /// oracle's rates; any read that fails is an error, never a zero
    /// (issue #121).
    #[cfg(feature = "chain-rpc")]
    #[tokio::test]
    async fn retrieval_funds_add_cashed_out_to_the_balance() {
        let full = || FundsChain {
            balance: Some(15_000_000_000_000),
            paid_out: Some(5_000_000_000_000),
            price: Some((100_000, 100)),
        };
        assert_eq!(
            funds_from(full()).await.unwrap(),
            RetrievalFunds {
                deposited_plur: 20_000_000_000_000,
                exchange_rate_plur: 100_000,
                deduction_plur: 100,
            }
        );
        for broken in [
            FundsChain {
                balance: None,
                ..full()
            },
            FundsChain {
                paid_out: None,
                ..full()
            },
            FundsChain {
                price: None,
                ..full()
            },
        ] {
            assert!(funds_from(broken).await.is_err());
        }
    }

    #[cfg(feature = "chain-rpc")]
    const WALLET: [u8; 20] = [0x0a; 20];
    #[cfg(feature = "chain-rpc")]
    const CHEQUEBOOK: [u8; 20] = [0xcb; 20];

    /// At or above the target: nothing moves, never a withdrawal.
    #[cfg(feature = "chain-rpc")]
    #[tokio::test]
    async fn top_up_leaves_a_funded_chequebook_alone() {
        for have in [
            DEFAULT_CHEQUEBOOK_DEPOSIT_PLUR,
            100 * DEFAULT_CHEQUEBOOK_DEPOSIT_PLUR,
        ] {
            let (result, others) = top_up_with(&[(CHEQUEBOOK, have), (WALLET, u128::MAX)]).await;
            assert_eq!(result.unwrap(), TopUp::NotNeeded);
            assert_eq!(others, 0, "no transfer");
        }
    }

    /// Short, but the wallet has no xBZZ: report the shortfall, no transfer.
    #[cfg(feature = "chain-rpc")]
    #[tokio::test]
    async fn top_up_reports_an_empty_wallet() {
        let have = DEFAULT_CHEQUEBOOK_DEPOSIT_PLUR / 4;
        let (result, others) = top_up_with(&[(CHEQUEBOOK, have), (WALLET, 0)]).await;
        assert_eq!(
            result.unwrap(),
            TopUp::WalletEmpty {
                shortfall: DEFAULT_CHEQUEBOOK_DEPOSIT_PLUR - have
            }
        );
        assert_eq!(others, 0, "no transfer");
    }

    /// A deposit that can't be read is an error, not "empty": the caller
    /// must not treat it as a shortfall and send money.
    #[cfg(feature = "chain-rpc")]
    #[tokio::test]
    async fn top_up_fails_on_an_unreadable_deposit() {
        let (result, others) = top_up_with(&[(WALLET, u128::MAX)]).await;
        assert!(result.is_err());
        assert_eq!(others, 0, "no transfer");
    }

    /// Short and the wallet can pay, but the chain says no: nothing is
    /// sent, whichever check refused.
    #[cfg(feature = "chain-rpc")]
    #[tokio::test]
    async fn top_up_refuses_a_chequebook_the_chain_rejects() {
        let balances = [(CHEQUEBOOK, 0), (WALLET, u128::MAX)];
        let (result, others) = top_up_checked(&balances, Some(false), Some(WALLET)).await;
        assert_eq!(
            result.unwrap(),
            TopUp::Refused(ChequebookVerdict::NotRegistered)
        );
        assert_eq!(others, 0, "no transfer");

        let stranger = [0x5e; 20];
        let (result, others) = top_up_checked(&balances, Some(true), Some(stranger)).await;
        assert_eq!(
            result.unwrap(),
            TopUp::Refused(ChequebookVerdict::IssuerMismatch(stranger))
        );
        assert_eq!(others, 0, "no transfer");
    }

    /// A check that can't be read is an error before any transfer: the
    /// verdict's "failed read passes" rule doesn't apply to spending.
    #[cfg(feature = "chain-rpc")]
    #[tokio::test]
    async fn top_up_fails_when_the_checks_cannot_be_read() {
        let balances = [(CHEQUEBOOK, 0), (WALLET, u128::MAX)];
        for (registered, issuer) in [(None, Some(WALLET)), (Some(true), None)] {
            let (result, others) = top_up_checked(&balances, registered, issuer).await;
            assert!(result.is_err(), "{registered:?}/{issuer:?}");
            assert_eq!(others, 0, "no transfer");
        }
    }

    /// Both checks positive: the transfer is attempted (and here fails,
    /// since the script has no transaction backend).
    #[cfg(feature = "chain-rpc")]
    #[tokio::test]
    async fn top_up_transfers_after_both_checks_pass() {
        let (result, others) = top_up_with(&[(CHEQUEBOOK, 0), (WALLET, u128::MAX)]).await;
        assert!(result.is_err(), "scripted transfer failure");
        assert!(others > 0, "a transfer was attempted");
    }

    #[test]
    fn default_deposit_is_one_thousandth_of_a_bzz() {
        // 1 xBZZ = 1e16 PLUR (16 decimals, not 18).
        assert_eq!(
            DEFAULT_CHEQUEBOOK_DEPOSIT_PLUR * 1_000,
            10_000_000_000_000_000
        );
    }
}
