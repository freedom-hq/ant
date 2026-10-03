//! Bee-compatible `/swarm/swap/1.0.0/swap` wire protocol — Phase 7
//! of M3.
//!
//! # What this is
//!
//! Bee's payments-above-pseudosettle layer settles per-peer debt by
//! exchanging **off-chain EIP-712-signed cheques**. The cheque is a
//! cumulative promise: cheque N supersedes cheque N-1 because both
//! are signed against the same `(chequebook, beneficiary)` pair and
//! the on-chain `cashChequeBeneficiary` only ever pays out
//! `cumulative_payout - already_paid`. Lost cheques are harmless;
//! the next cheque catches the beneficiary up.
//!
//! The cheque object itself is built and signed by
//! [`ant_chain::chequebook`]. This module owns the **wire layer**:
//! how cheques traverse the libp2p stream between peers.
//!
//! # The protocol (bee 2.7.x and 2.8.0)
//!
//! Bee 2.8.0 reshapes the BZZ handshake (new signed preimage; nonce /
//! timestamp / chequebook moved into `BzzAddress`) but does not touch
//! the swap stream itself. Cheque issue / receive remains the same
//! wire format and is unaffected by the cutover.
//!
//! `/swarm/swap/1.0.0/swap` is a single-direction stream:
//!
//! ```text
//! dialer → listener : varint + Headers pb (empty)            // bee headers preamble
//! listener → dialer : varint + Headers pb { exchange, deduction }
//! dialer → listener : varint + EmitCheque{ Cheque: bytes }   // JSON-encoded SignedCheque
//! listener          : FullClose when it accepted the cheque, Reset when not
//! ```
//!
//! The listener's response headers carry its price oracle's `exchange`
//! rate (PLUR per accounting unit) and `deduction` (PLUR). Bee pays
//! `debt × exchange + deduction` and credits `(paid − deduction) /
//! exchange` units ([`SettlementRates`], used by retrieval payments).
//!
//! `EmitCheque.Cheque` is **bee's `chequebook.SignedCheque`
//! JSON-marshalled** (NOT the binary cheque encoding); see
//! `pkg/settlement/swap/swapprotocol/swapprotocol.go::EmitCheque`. The
//! JSON shape go-ethereum produces by default is what the wire
//! carries:
//!
//! ```json
//! {
//!   "Chequebook": "0x...",        // 0x-hex 20 bytes (geth common.Address)
//!   "Beneficiary": "0x...",
//!   "CumulativePayout": 12345,    // bare number (big.Int's MarshalJSON)
//!   "Signature": "..."            // base64 65-byte r ‖ s ‖ v (default []byte JSON)
//! }
//! ```
//!
//! The receiving side validates the signature, recovers the issuer,
//! and credits the chequebook's beneficiary balance. Bee then asks
//! the chain whether `chequebook.issuer()` matches the recovered
//! signer; we expose [`recover_cheque_signer`](ant_chain::chequebook::recover_cheque_signer)
//! so callers can do the same lookup once they have a
//! [`ant_chain::ChainClient`].
//!
//! # What this module does
//!
//! - [`PROTOCOL_SWAP`] constant for protocol negotiation.
//! - [`EmitChequePb`]: the protobuf framing.
//! - [`encode_signed_cheque_json`] / [`decode_signed_cheque_json`]:
//!   bee-shape JSON for the wire's `Cheque` payload.
//! - [`CreditLedger`]: per-`chequebook` cumulative-paid-in tracking
//!   with crash-safe JSON persistence (atomic write + fsync), so a
//!   restart can't accept a replayed older cheque.
//! - [`run_inbound`]: listener task that drains accepted streams,
//!   verifies cheques, and updates the ledger.
//! - [`emit_cheque`]: outbound dialer that signs and ships a cheque.
//! - [`issue_and_emit`]: one-shot helper that mints a fresh cheque
//!   with `cumulative_payout = previous + amount` and emits it.
//!
//! # What this module does **not** do (yet)
//!
//! - On-chain `chequebook(Cheque.Chequebook).issuer()` validation.
//!   The ledger pins the `(chequebook → expected_signer)` binding on
//!   first sight; subsequent cheques must match. A future hardening
//!   step is a one-shot RPC at first contact to confirm the signer
//!   really controls the chequebook.
//! - Auto-cashout. Cashing the highest-cumulative cheque on-chain
//!   requires a wallet that can pay xDAI gas; today we just store
//!   the cheque. The operator can pull a snapshot from the ledger
//!   and call [`ant_chain::chequebook::cash_cheque_beneficiary_calldata`]
//!   manually.
//!
//! Settlement triggering lives with the callers: pushsync debt in
//! [`crate::pushsync_swap`], retrieval debt in
//! [`ant_retrieval::accounting`] paying through it (issue #121), with
//! [`open_settlement`], [`write_cheque`] and [`await_processed`].
//!
//! Everything here is wire-compatible with bee: a cheque we emit
//! reaches bee's `s.swap.ReceiveCheque` and would be accepted modulo
//! the on-chain issuer check; a cheque bee emits to us is parsed by
//! [`run_inbound`].

use crate::sinks::{HEADERS_MAX, STREAM_TIMEOUT};
use ant_chain::chequebook::{recover_cheque_signer, sign_cheque, Cheque, SignedCheque};
use ant_crypto::{CryptoError, SECP256K1_SECRET_LEN};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use futures::io::{AsyncReadExt, AsyncWriteExt};
use futures::StreamExt;
use libp2p::{PeerId, StreamProtocol};
use libp2p_stream::{Control, IncomingStreams};
use libp2p_swarm::Stream;
use primitive_types::U256;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{debug, info, trace, warn};

/// Bee's swap stream path. Built as `/swarm/<name>/<version>/<stream>`
/// where `name="swap"`, `version="1.0.0"`, and `stream="swap"` (see
/// `pkg/settlement/swap/swapprotocol/swapprotocol.go`).
pub const PROTOCOL_SWAP: &str = "/swarm/swap/1.0.0/swap";

/// Hard cap on the protobuf body of an `EmitCheque`. The JSON cheque
/// inside is ~250 bytes (3 × 0x-hex 20-byte addresses + decimal int +
/// base64 65-byte sig + JSON quoting). 4 KiB gives a generous
/// headroom for any future schema growth without risking a malicious
/// peer feeding us megabyte cheques.
const EMIT_CHEQUE_MAX: usize = 4 * 1024;

#[derive(Debug, Error)]
pub enum SwapError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("protobuf decode: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("protobuf encode: {0}")]
    Encode(#[from] prost::EncodeError),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("hex: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("base64: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("decimal: {0}")]
    Decimal(String),
    #[error("crypto: {0}")]
    Crypto(#[from] CryptoError),
    #[error("cheque rejected: {0}")]
    Rejected(String),
}

/// Bee `swapprotocol/pb/swap.proto::EmitCheque`.
#[derive(Clone, PartialEq, Eq, Message)]
pub struct EmitChequePb {
    /// JSON-encoded `chequebook.SignedCheque`.
    #[prost(bytes = "vec", tag = "1")]
    pub cheque: Vec<u8>,
}

/// Bee-shaped JSON for `chequebook.SignedCheque`. Must be a
/// byte-for-byte match of what go's `encoding/json` produces over
/// `chequebook.SignedCheque`:
///
/// - `Chequebook` / `Beneficiary`: 0x-prefixed hex (geth's
///   `common.Address.MarshalJSON`).
/// - `CumulativePayout`: a bare JSON number. Go's `*big.Int` implements
///   `json.Marshaler` (`MarshalJSON` writes the decimal digits
///   unquoted), and its `UnmarshalJSON` refuses a quoted string
///   (`math/big: cannot unmarshal "\"123\"" into a *big.Int`), so bee's
///   swap handler resets the stream on a quoted payout and never
///   credits the cheque (issue #121). Decoding also accepts the quoted
///   form, which earlier Ant versions sent.
/// - `Signature`: base64 standard-padding (geth `[]byte` default).
#[derive(Debug, Serialize, Deserialize)]
struct SignedChequeJson {
    #[serde(rename = "Chequebook")]
    chequebook: String,
    #[serde(rename = "Beneficiary")]
    beneficiary: String,
    #[serde(rename = "CumulativePayout")]
    cumulative_payout: Box<serde_json::value::RawValue>,
    #[serde(rename = "Signature")]
    signature: String,
}

/// JSON-encode a [`SignedCheque`] into the wire bytes carried inside
/// an `EmitCheque.Cheque` field. Lossless round-trip with
/// [`decode_signed_cheque_json`].
#[must_use]
pub fn encode_signed_cheque_json(signed: &SignedCheque) -> Vec<u8> {
    let body = SignedChequeJson {
        chequebook: format!("0x{}", hex::encode(signed.cheque.chequebook)),
        beneficiary: format!("0x{}", hex::encode(signed.cheque.beneficiary)),
        cumulative_payout: serde_json::value::RawValue::from_string(
            signed.cheque.cumulative_payout.to_string(),
        )
        .expect("decimal digits are a JSON number"),
        signature: BASE64.encode(signed.signature),
    };
    serde_json::to_vec(&body).expect("SignedChequeJson serialises")
}

/// Decode bee-shape JSON back into a [`SignedCheque`].
pub fn decode_signed_cheque_json(bytes: &[u8]) -> Result<SignedCheque, SwapError> {
    let body: SignedChequeJson = serde_json::from_slice(bytes)?;
    let chequebook = parse_addr_0x(&body.chequebook)?;
    let beneficiary = parse_addr_0x(&body.beneficiary)?;
    let payout = body.cumulative_payout.get().trim();
    let payout = payout
        .strip_prefix('"')
        .and_then(|p| p.strip_suffix('"'))
        .unwrap_or(payout);
    if payout.is_empty() || !payout.bytes().all(|b| b.is_ascii_digit()) {
        return Err(SwapError::Decimal(format!(
            "cumulative_payout: not a decimal integer: {payout}"
        )));
    }
    let cumulative_payout = U256::from_dec_str(payout)
        .map_err(|e| SwapError::Decimal(format!("cumulative_payout: {e}")))?;
    let sig_bytes = BASE64.decode(body.signature.trim())?;
    if sig_bytes.len() != 65 {
        return Err(SwapError::Rejected(format!(
            "signature length {} (expected 65)",
            sig_bytes.len(),
        )));
    }
    let mut signature = [0u8; 65];
    signature.copy_from_slice(&sig_bytes);
    Ok(SignedCheque {
        cheque: Cheque {
            chequebook,
            beneficiary,
            cumulative_payout,
        },
        signature,
    })
}

fn parse_addr_0x(s: &str) -> Result<[u8; 20], SwapError> {
    let s = s.trim();
    let s = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    let bytes = hex::decode(s)?;
    if bytes.len() != 20 {
        return Err(SwapError::Rejected(format!(
            "address length {} (expected 20)",
            bytes.len(),
        )));
    }
    let mut out = [0u8; 20];
    out.copy_from_slice(&bytes);
    Ok(out)
}

/// Per-chequebook record stored in the [`CreditLedger`]. Bee's swap
/// layer reasons in cumulative terms, so all we need to remember is
/// the highest cumulative payout we've accepted from this chequebook
/// plus the signer we pinned the chequebook to on first sight.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChequebookCredit {
    /// Lower-case hex (no 0x) of the issuer EOA we recovered from the
    /// first cheque this chequebook produced. Subsequent cheques whose
    /// signature recovers to a different EOA are rejected; protects
    /// against a peer swapping its issuer key out from under us mid-
    /// session.
    pub issuer_eoa_hex: String,
    /// Highest accepted `cumulative_payout` for this chequebook,
    /// stored as a decimal string so it round-trips through serde
    /// without losing precision (U256 doesn't impl serde itself).
    pub cumulative_payout_dec: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct LedgerSnapshot {
    /// Map keyed on chequebook address (lower-case hex, no `0x`).
    cheques: HashMap<String, ChequebookCredit>,
}

/// In-memory + on-disk per-chequebook credit ledger. The ledger is
/// the **trust anchor**: every accepted cheque must
///
/// 1. carry a `cumulative_payout` strictly greater than what we have
///    on file for that chequebook,
/// 2. recover to the same issuer EOA that signed the first cheque
///    we ever accepted from that chequebook (sticky binding), and
/// 3. signature-verify against the cheque's own `Chequebook` /
///    `Beneficiary` / `CumulativePayout` triple at the configured
///    chain id.
///
/// Persistence is atomic: every `record_accepted` flushes a fresh
/// JSON snapshot to a temp file, fsyncs, and renames over the live
/// path. A crash mid-write leaves the previous snapshot intact and
/// the new cheque unrecorded — bee's cumulative model means the
/// peer just re-emits the same (or higher) cheque on the next
/// connection and we accept it then.
pub struct CreditLedger {
    inner: Mutex<LedgerSnapshot>,
    persist_path: Option<PathBuf>,
    chain_id: u64,
    /// Beneficiary address all accepted cheques must target — i.e.
    /// our own node's EOA. A cheque with a different beneficiary is
    /// rejected (the issuer is paying someone else and the wire
    /// is presumably misrouted).
    our_beneficiary: [u8; 20],
}

impl CreditLedger {
    /// Build an empty ledger backed by `persist_path` (or memory-only
    /// if `None`). Loads any existing snapshot if the file exists; an
    /// unreadable / unparseable snapshot is treated as empty after
    /// logging — better to lose history than refuse to start.
    pub fn open(persist_path: Option<PathBuf>, chain_id: u64, our_beneficiary: [u8; 20]) -> Self {
        let inner = match &persist_path {
            Some(p) if p.exists() => match std::fs::read(p) {
                Ok(bytes) => match serde_json::from_slice::<LedgerSnapshot>(&bytes) {
                    Ok(snap) => {
                        info!(
                            target: "ant_p2p::swap",
                            path = %p.display(),
                            chequebooks = snap.cheques.len(),
                            "loaded credit ledger snapshot",
                        );
                        snap
                    }
                    Err(e) => {
                        warn!(
                            target: "ant_p2p::swap",
                            path = %p.display(),
                            "ledger snapshot unparseable: {e}; starting empty",
                        );
                        LedgerSnapshot::default()
                    }
                },
                Err(e) => {
                    warn!(
                        target: "ant_p2p::swap",
                        path = %p.display(),
                        "ledger snapshot unreadable: {e}; starting empty",
                    );
                    LedgerSnapshot::default()
                }
            },
            _ => LedgerSnapshot::default(),
        };
        Self {
            inner: Mutex::new(inner),
            persist_path,
            chain_id,
            our_beneficiary,
        }
    }

    /// Apply `signed` to the ledger, returning the previous
    /// cumulative payout for that chequebook (zero on first sight).
    /// Validates: signature, beneficiary, monotonicity, sticky-issuer
    /// binding. A pure read-only check (no ledger mutation) is
    /// available via [`Self::would_accept`].
    pub fn record_accepted(&self, signed: &SignedCheque) -> Result<U256, SwapError> {
        if signed.cheque.beneficiary != self.our_beneficiary {
            return Err(SwapError::Rejected(format!(
                "cheque beneficiary 0x{} != our 0x{}",
                hex::encode(signed.cheque.beneficiary),
                hex::encode(self.our_beneficiary),
            )));
        }
        let recovered = recover_cheque_signer(signed, self.chain_id)?;
        let chequebook_key = hex::encode(signed.cheque.chequebook);
        let recovered_hex = hex::encode(recovered);
        let mut guard = self.inner.lock().expect("ledger mutex poisoned");
        let prev_cum = match guard.cheques.get(&chequebook_key) {
            Some(c) => {
                if c.issuer_eoa_hex != recovered_hex {
                    return Err(SwapError::Rejected(format!(
                        "issuer for chequebook 0x{chequebook_key} changed: \
                         saw 0x{recovered_hex}, pinned to 0x{}",
                        c.issuer_eoa_hex,
                    )));
                }
                U256::from_dec_str(&c.cumulative_payout_dec)
                    .map_err(|e| SwapError::Decimal(format!("stored cumulative: {e}")))?
            }
            None => U256::zero(),
        };
        if signed.cheque.cumulative_payout <= prev_cum {
            return Err(SwapError::Rejected(format!(
                "non-monotonic cheque for 0x{chequebook_key}: cum={} <= stored={}",
                signed.cheque.cumulative_payout, prev_cum,
            )));
        }
        guard.cheques.insert(
            chequebook_key,
            ChequebookCredit {
                issuer_eoa_hex: recovered_hex,
                cumulative_payout_dec: signed.cheque.cumulative_payout.to_string(),
            },
        );
        if let Some(path) = &self.persist_path {
            if let Err(e) = persist_snapshot(path, &guard) {
                warn!(
                    target: "ant_p2p::swap",
                    path = %path.display(),
                    "ledger persist failed: {e}; in-memory state still updated",
                );
            }
        }
        Ok(prev_cum)
    }

    /// Read-only validation. Returns the would-be previous cumulative
    /// payout on success, the rejection reason on failure. Pure — no
    /// ledger mutation.
    pub fn would_accept(&self, signed: &SignedCheque) -> Result<U256, SwapError> {
        if signed.cheque.beneficiary != self.our_beneficiary {
            return Err(SwapError::Rejected(format!(
                "cheque beneficiary 0x{} != our 0x{}",
                hex::encode(signed.cheque.beneficiary),
                hex::encode(self.our_beneficiary),
            )));
        }
        let recovered = recover_cheque_signer(signed, self.chain_id)?;
        let chequebook_key = hex::encode(signed.cheque.chequebook);
        let recovered_hex = hex::encode(recovered);
        let guard = self.inner.lock().expect("ledger mutex poisoned");
        let prev_cum = match guard.cheques.get(&chequebook_key) {
            Some(c) => {
                if c.issuer_eoa_hex != recovered_hex {
                    return Err(SwapError::Rejected(format!(
                        "issuer for chequebook 0x{chequebook_key} changed",
                    )));
                }
                U256::from_dec_str(&c.cumulative_payout_dec)
                    .map_err(|e| SwapError::Decimal(format!("stored cumulative: {e}")))?
            }
            None => U256::zero(),
        };
        if signed.cheque.cumulative_payout <= prev_cum {
            return Err(SwapError::Rejected(format!(
                "non-monotonic cumulative payout {} <= stored {}",
                signed.cheque.cumulative_payout, prev_cum,
            )));
        }
        Ok(prev_cum)
    }

    /// Latest accepted cheque amount for `chequebook` (zero if none).
    /// Used by [`issue_and_emit`] callers that want to mint
    /// `cumulative_payout = stored + amount` for an outbound cheque.
    pub fn cumulative_for(&self, chequebook: &[u8; 20]) -> U256 {
        let key = hex::encode(chequebook);
        let guard = self.inner.lock().expect("ledger mutex poisoned");
        match guard.cheques.get(&key) {
            Some(c) => U256::from_dec_str(&c.cumulative_payout_dec).unwrap_or(U256::zero()),
            None => U256::zero(),
        }
    }

    /// The beneficiary address accepted cheques must target (our own
    /// node's EOA). Exposed so the accounting snapshot can render the
    /// `lastreceived.beneficiary` field of `GET /chequebook/cheque`.
    #[must_use]
    pub fn beneficiary(&self) -> [u8; 20] {
        self.our_beneficiary
    }

    /// Snapshot copy of every chequebook record we know about.
    /// Useful for `antctl swap list` (future) and for the cashout
    /// helper that wants to know the highest cheque per peer.
    pub fn snapshot(&self) -> Vec<(String, ChequebookCredit)> {
        let guard = self.inner.lock().expect("ledger mutex poisoned");
        guard
            .cheques
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

fn persist_snapshot(path: &Path, snap: &LedgerSnapshot) -> std::io::Result<()> {
    use std::io::Write;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(snap).map_err(std::io::Error::other)?;
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Sign a fresh cheque with `cumulative_payout = previous + amount`
/// (where `previous` comes from the ledger's outbound mirror — see
/// [`OutboundLedger`]) and return the signed object ready to feed
/// into [`emit_cheque`].
pub fn issue_cheque(
    secret: &[u8; SECP256K1_SECRET_LEN],
    chequebook: [u8; 20],
    beneficiary: [u8; 20],
    cumulative_payout: U256,
    chain_id: u64,
) -> Result<SignedCheque, SwapError> {
    let cheque = Cheque {
        chequebook,
        beneficiary,
        cumulative_payout,
    };
    Ok(sign_cheque(secret, &cheque, chain_id)?)
}

/// Companion to [`CreditLedger`] for OUTBOUND cheques: tracks the
/// last cumulative amount one chequebook issued to each peer's
/// beneficiary so the next cheque is monotonically larger.
///
/// Cumulatives belong to a `(chequebook, beneficiary)` pair: bee keeps
/// its `lastReceived` per chequebook, and a chequebook's liability
/// (what [`Self::total_issued`] reports) is only its own cheques. So the
/// file holds one section per chequebook and a ledger opened for one
/// chequebook reads and rewrites only its own section. Switching the
/// data dir to another chequebook (`--chequebook`, a fresh deploy after
/// a rejected one) starts that chequebook from zero, and switching back
/// finds the first one's cumulatives where they were.
///
/// A file written before the sections existed (a bare `beneficiary →
/// cumulative` map) is adopted by the first chequebook that opens it and
/// rewritten under it at once, so a second chequebook can't adopt it
/// too. Such files only ever held pushsync cheques, which were a few
/// million PLUR at most.
pub struct OutboundLedger {
    chequebook: [u8; 20],
    inner: Mutex<HashMap<String, U256>>,
    persist_path: Option<PathBuf>,
}

/// On-disk shape of the outbound ledger: `chequebook hex → (beneficiary
/// hex → cumulative as a decimal string)`.
#[derive(Default, Serialize, Deserialize)]
struct OutboundFile {
    chequebooks: HashMap<String, HashMap<String, String>>,
}

/// Serialises read-modify-write of outbound ledger files across every
/// ledger in the process: two services (an old chequebook's still
/// finishing a cheque, the new one's) can share one file.
static OUTBOUND_FILE_LOCK: Mutex<()> = Mutex::new(());

fn parse_cumulatives(map: HashMap<String, String>) -> HashMap<String, U256> {
    map.into_iter()
        .filter_map(|(k, v)| U256::from_dec_str(&v).ok().map(|u| (k, u)))
        .collect()
}

/// Read `path`. `Ok(None)` when it doesn't exist; a file that is neither
/// shape is an error.
fn read_outbound_file(path: &Path) -> std::io::Result<Option<OutboundFile>> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    if let Ok(file) = serde_json::from_slice::<OutboundFile>(&bytes) {
        return Ok(Some(file));
    }
    let legacy =
        serde_json::from_slice::<HashMap<String, String>>(&bytes).map_err(std::io::Error::other)?;
    // Marked with an empty key until a chequebook adopts it.
    Ok(Some(OutboundFile {
        chequebooks: HashMap::from([(String::new(), legacy)]),
    }))
}

impl OutboundLedger {
    /// Open `chequebook`'s cumulatives from `persist_path` (or start
    /// empty without one). See the type docs for the file layout.
    pub fn open(persist_path: Option<PathBuf>, chequebook: [u8; 20]) -> Self {
        let key = hex::encode(chequebook);
        let inner = match &persist_path {
            None => HashMap::new(),
            Some(p) => {
                let _file = OUTBOUND_FILE_LOCK
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                match read_outbound_file(p) {
                    Ok(None) => HashMap::new(),
                    Ok(Some(mut file)) => {
                        if let Some(legacy) = file.chequebooks.remove("") {
                            info!(
                                target: "ant_p2p::swap",
                                chequebook = %key,
                                "adopting an outbound ledger written before per-chequebook sections",
                            );
                            let mine = file.chequebooks.entry(key.clone()).or_default();
                            for (k, v) in legacy {
                                mine.entry(k).or_insert(v);
                            }
                            if let Err(e) = write_outbound_file(p, &file) {
                                warn!(target: "ant_p2p::swap", "outbound ledger migrate: {e}");
                            }
                        }
                        file.chequebooks
                            .remove(&key)
                            .map(parse_cumulatives)
                            .unwrap_or_default()
                    }
                    Err(e) => {
                        warn!(
                            target: "ant_p2p::swap",
                            "outbound ledger unreadable: {e}; starting empty",
                        );
                        HashMap::new()
                    }
                }
            }
        };
        Self {
            chequebook,
            inner: Mutex::new(inner),
            persist_path,
        }
    }

    /// Chequebook whose cheques this ledger tracks.
    pub fn chequebook(&self) -> [u8; 20] {
        self.chequebook
    }

    /// Last cumulative we issued to `beneficiary` (zero if none).
    pub fn cumulative_for(&self, beneficiary: &[u8; 20]) -> U256 {
        self.recorded_for(beneficiary).unwrap_or(U256::zero())
    }

    /// Sum of every beneficiary's cumulative: all PLUR this chequebook
    /// has promised in cheques from this node (bee's `totalIssued`).
    pub fn total_issued(&self) -> U256 {
        let guard = self.inner.lock().expect("outbound ledger poisoned");
        guard
            .values()
            .fold(U256::zero(), |acc, v| acc.saturating_add(*v))
    }

    /// Like [`Self::cumulative_for`] but distinguishes "no cheque ever
    /// issued" (`None`) from a recorded cumulative. The accounting
    /// snapshot needs the difference: bee's `/settlements/{peer}`
    /// 404s on a peer with no settlement record rather than reporting
    /// zero.
    pub fn recorded_for(&self, beneficiary: &[u8; 20]) -> Option<U256> {
        let key = hex::encode(beneficiary);
        let guard = self.inner.lock().expect("outbound ledger poisoned");
        guard.get(&key).copied()
    }

    /// Set `beneficiary` → `new_cumulative` (typically called after
    /// a successful [`emit_cheque`]). Persists atomically, rewriting
    /// only this chequebook's section of the file.
    pub fn record_issued(
        &self,
        beneficiary: &[u8; 20],
        new_cumulative: U256,
    ) -> std::io::Result<()> {
        let key = hex::encode(beneficiary);
        let mut guard = self.inner.lock().expect("outbound ledger poisoned");
        guard.insert(key, new_cumulative);
        if let Some(path) = &self.persist_path {
            let _file = OUTBOUND_FILE_LOCK
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // Keep other chequebooks' sections; a file we can't read is
            // replaced (the warning at open already said so).
            let mut file = read_outbound_file(path).ok().flatten().unwrap_or_default();
            file.chequebooks.remove("");
            file.chequebooks.insert(
                hex::encode(self.chequebook),
                guard
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_string()))
                    .collect(),
            );
            write_outbound_file(path, &file)?;
        }
        Ok(())
    }
}

fn write_outbound_file(path: &Path, file: &OutboundFile) -> std::io::Result<()> {
    use std::io::Write;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(file).map_err(std::io::Error::other)?;
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

// --- I/O helpers (length-delimited frames + bee headers preamble) ---

async fn write_empty_headers<W: AsyncWriteExt + Unpin>(w: &mut W) -> std::io::Result<()> {
    w.write_all(&[0u8]).await?;
    w.flush().await?;
    Ok(())
}

async fn read_delimited<R: AsyncReadExt + Unpin>(
    r: &mut R,
    max: usize,
) -> std::io::Result<Vec<u8>> {
    let len = read_varint_len(r).await?;
    if len > max {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("message too large: {len} bytes (cap {max})"),
        ));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn read_varint_len<R: AsyncReadExt + Unpin>(r: &mut R) -> std::io::Result<usize> {
    let mut byte = [0u8; 1];
    let mut acc: Vec<u8> = Vec::with_capacity(10);
    loop {
        r.read_exact(&mut byte).await?;
        acc.push(byte[0]);
        match unsigned_varint::decode::u64(&acc) {
            Ok((v, [])) => {
                return usize::try_from(v).map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "varint overflow")
                });
            }
            Ok(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid varint framing",
                ));
            }
            Err(unsigned_varint::decode::Error::Insufficient) => {
                if acc.len() > 10 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "varint too long",
                    ));
                }
            }
            Err(e) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("varint: {e}"),
                ));
            }
        }
    }
}

async fn write_delimited<W, M>(w: &mut W, msg: &M) -> std::io::Result<()>
where
    W: AsyncWriteExt + Unpin,
    M: Message,
{
    let mut buf = Vec::with_capacity(msg.encoded_len() + 10);
    msg.encode_length_delimited(&mut buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    w.write_all(&buf).await?;
    w.flush().await?;
    Ok(())
}

/// Drain the inbound stream without touching any ledger. Used when
/// the operator hasn't configured a chequebook beneficiary yet — we
/// still want to accept the protocol negotiation so bee doesn't
/// disconnect us, but we have nothing meaningful to do with the
/// cheque. Logs at trace.
pub async fn drain_inbound_unconfigured(mut incoming: IncomingStreams) {
    while let Some((peer, mut stream)) = incoming.next().await {
        tokio::spawn(async move {
            let _ = tokio::time::timeout(STREAM_TIMEOUT, async move {
                let _ = read_delimited(&mut stream, HEADERS_MAX).await;
                let _ = write_empty_headers(&mut stream).await;
                let _ = read_delimited(&mut stream, EMIT_CHEQUE_MAX).await;
                let _ = stream.close().await;
            })
            .await;
            trace!(
                target: "ant_p2p::swap",
                %peer,
                "drained inbound cheque (no chequebook configured; ignored)",
            );
        });
    }
}

// --- listener ---

/// Outcome surfaced by the inbound listener for each accepted cheque.
///
/// Currently advisory — operators can subscribe to log a one-line
/// "credited X PLUR from peer Y" event without re-parsing the
/// ledger snapshot.
#[derive(Debug, Clone)]
pub struct InboundCheque {
    pub peer: PeerId,
    pub chequebook: [u8; 20],
    pub issuer_eoa: [u8; 20],
    pub cumulative_payout: U256,
    pub previous_cumulative: U256,
}

/// Spawn the long-lived inbound listener. For each accepted stream
/// it: drains the bee headers preamble, reads the `EmitCheque` frame,
/// parses the JSON cheque, and applies it to the ledger. Successful
/// accepts publish to `events_tx` (best-effort: drops on backpressure).
///
/// Stays compatible with bee's `s.handler`: bee's listener reads the
/// dialer's headers FIRST, then writes back its own. We're the
/// listener here, so we mirror that — read first, then ack with
/// empty headers, then read the payload.
pub async fn run_inbound(
    mut incoming: IncomingStreams,
    ledger: Arc<CreditLedger>,
    events_tx: Option<mpsc::Sender<InboundCheque>>,
) {
    while let Some((peer, stream)) = incoming.next().await {
        let l = ledger.clone();
        let tx = events_tx.clone();
        tokio::spawn(async move {
            match tokio::time::timeout(STREAM_TIMEOUT, handle_inbound_stream(peer, stream, l)).await
            {
                Ok(Ok(ev)) => {
                    info!(
                        target: "ant_p2p::swap",
                        %peer,
                        chequebook = %hex::encode(ev.chequebook),
                        cumulative = %ev.cumulative_payout,
                        delta = %(ev.cumulative_payout.saturating_sub(ev.previous_cumulative)),
                        "accepted cheque",
                    );
                    if let Some(tx) = tx {
                        let _ = tx.try_send(ev);
                    }
                }
                Ok(Err(e)) => debug!(
                    target: "ant_p2p::swap",
                    %peer,
                    "inbound cheque rejected: {e}",
                ),
                Err(_) => warn!(
                    target: "ant_p2p::swap",
                    %peer,
                    "inbound stream timed out after {}s",
                    STREAM_TIMEOUT.as_secs(),
                ),
            }
        });
    }
}

async fn handle_inbound_stream(
    peer: PeerId,
    mut stream: Stream,
    ledger: Arc<CreditLedger>,
) -> Result<InboundCheque, SwapError> {
    // Headers preamble: bee's dialer writes first, so we read first.
    let _their_headers = read_delimited(&mut stream, HEADERS_MAX).await?;
    write_empty_headers(&mut stream).await?;

    let body = read_delimited(&mut stream, EMIT_CHEQUE_MAX).await?;
    let msg = EmitChequePb::decode(body.as_slice())?;
    let signed = decode_signed_cheque_json(&msg.cheque)?;
    let prev = ledger.record_accepted(&signed)?;
    let issuer = recover_cheque_signer(&signed, ledger.chain_id)?;
    // Half-close so bee's `stream.FullClose()` returns promptly.
    let _ = stream.close().await;
    trace!(
        target: "ant_p2p::swap",
        %peer,
        "stream closed after accepted cheque",
    );
    Ok(InboundCheque {
        peer,
        chequebook: signed.cheque.chequebook,
        issuer_eoa: issuer,
        cumulative_payout: signed.cheque.cumulative_payout,
        previous_cumulative: prev,
    })
}

// --- dialer ---

/// Open a swap stream to `peer` and emit `signed`. Returns once bee
/// has read the message (we close our half; bee's `s.handler` does
/// the rest). On error the caller should NOT update the outbound
/// ledger — the cheque hasn't actually been delivered.
pub async fn emit_cheque(
    control: &mut Control,
    peer: PeerId,
    signed: &SignedCheque,
) -> Result<(), SwapError> {
    let proto = StreamProtocol::new(PROTOCOL_SWAP);
    let mut stream = control
        .open_stream(peer, proto)
        .await
        .map_err(|e| std::io::Error::other(format!("open swap stream: {e}")))?;

    // Headers preamble — we're the dialer, so write first then read.
    write_empty_headers(&mut stream).await?;
    let _their_headers = read_delimited(&mut stream, HEADERS_MAX).await?;

    let body = encode_signed_cheque_json(signed);
    let msg = EmitChequePb { cheque: body };
    write_delimited(&mut stream, &msg).await?;
    let _ = stream.close().await;
    Ok(())
}

/// One-shot helper: look up the last cumulative we issued to
/// `beneficiary`, sign a fresh cheque for `previous + amount`, send
/// it, and on success record the new cumulative in the outbound
/// ledger. Returns the new cumulative amount on success.
#[allow(clippy::too_many_arguments)]
pub async fn issue_and_emit(
    control: &mut Control,
    peer: PeerId,
    secret: &[u8; SECP256K1_SECRET_LEN],
    chequebook: [u8; 20],
    beneficiary: [u8; 20],
    amount: U256,
    chain_id: u64,
    outbound: &OutboundLedger,
) -> Result<U256, SwapError> {
    let prev = outbound.cumulative_for(&beneficiary);
    let new_cum = prev
        .checked_add(amount)
        .ok_or_else(|| SwapError::Rejected("cumulative overflow".into()))?;
    let signed = issue_cheque(secret, chequebook, beneficiary, new_cum, chain_id)?;
    emit_cheque(control, peer, &signed).await?;
    if let Err(e) = outbound.record_issued(&beneficiary, new_cum) {
        warn!(
            target: "ant_p2p::swap",
            beneficiary = %hex::encode(beneficiary),
            "outbound ledger persist failed after successful emit: {e}",
        );
    }
    Ok(new_cum)
}

/// Bee's settlement response headers (`pkg/settlement/swap/headers`):
/// the cheque recipient's `exchange` rate (PLUR per accounting unit, from
/// the swap price oracle) and the one-off `deduction` (PLUR), both
/// big-endian unsigned integers. Bee's `swapprotocol.EmitCheque` reads
/// them before it signs and pays `amount × exchange + deduction`; the
/// recipient credits `(paid − deduction) / exchange` units.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SettlementRates {
    pub exchange_rate: U256,
    pub deduction: U256,
}

impl SettlementRates {
    /// PLUR a cheque must carry to clear `units` of accounting debt at
    /// these rates.
    #[must_use]
    pub fn cheque_amount(&self, units: u64) -> Option<U256> {
        U256::from(units)
            .checked_mul(self.exchange_rate)?
            .checked_add(self.deduction)
    }
}

/// Bee `internal/headers/pb::Header`.
#[derive(Clone, PartialEq, Eq, Message)]
struct HeaderPb {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(bytes = "vec", tag = "2")]
    value: Vec<u8>,
}

/// Bee `internal/headers/pb::Headers`.
#[derive(Clone, PartialEq, Eq, Message)]
struct HeadersPb {
    #[prost(message, repeated, tag = "1")]
    headers: Vec<HeaderPb>,
}

/// Decode the swap listener's response headers into [`SettlementRates`],
/// like bee's `ParseSettlementResponseHeaders`: `exchange` is required,
/// a missing `deduction` is zero (bee's `ErrNoDeductionHeader` path).
pub fn parse_settlement_headers(bytes: &[u8]) -> Result<SettlementRates, SwapError> {
    let headers = HeadersPb::decode(bytes)?;
    let field = |name: &str| {
        headers
            .headers
            .iter()
            .find(|h| h.key == name)
            .map(|h| h.value.as_slice())
    };
    let big = |v: &[u8]| -> Result<U256, SwapError> {
        if v.len() > 32 {
            return Err(SwapError::Rejected(format!(
                "settlement header of {} bytes",
                v.len()
            )));
        }
        Ok(U256::from_big_endian(v))
    };
    let exchange_rate =
        big(field("exchange").ok_or_else(|| SwapError::Rejected("no exchange header".into()))?)?;
    let deduction = match field("deduction") {
        Some(v) => big(v)?,
        None => U256::zero(),
    };
    Ok(SettlementRates {
        exchange_rate,
        deduction,
    })
}

/// Open a swap stream to `peer` and read the recipient's
/// [`SettlementRates`] from its response headers: the first half of bee's
/// `swapprotocol.EmitCheque`. Finish with [`write_cheque`] once the
/// cheque for those rates is signed, or drop the stream to abandon it.
pub async fn open_settlement(
    control: &mut Control,
    peer: PeerId,
) -> Result<(Stream, SettlementRates), SwapError> {
    let proto = StreamProtocol::new(PROTOCOL_SWAP);
    let mut stream = control
        .open_stream(peer, proto)
        .await
        .map_err(|e| std::io::Error::other(format!("open swap stream: {e}")))?;
    write_empty_headers(&mut stream).await?;
    let headers = read_delimited(&mut stream, HEADERS_MAX).await?;
    let rates = parse_settlement_headers(&headers)?;
    Ok((stream, rates))
}

/// Write `signed` on a stream from [`open_settlement`] and close our
/// side. The recipient may hold the cheque from here on (and even if
/// this fails halfway), so the caller counts it as issued before calling
/// this; [`await_processed`] tells when the recipient is done with it.
pub async fn write_cheque(stream: &mut Stream, signed: &SignedCheque) -> Result<(), SwapError> {
    let body = encode_signed_cheque_json(signed);
    write_delimited(stream, &EmitChequePb { cheque: body }).await?;
    stream.close().await?;
    Ok(())
}

/// Wait for the recipient of a cheque from [`write_cheque`] to finish
/// with the stream.
///
/// Bee's swap handler runs `ReceiveCheque` (signature, chequebook and
/// balance checks, then `NotifyPaymentReceived`) before it closes the
/// stream, so once the stream has ended bee has credited an accepted
/// cheque. Only then may the payer lower its own view of the debt:
/// lowering it as soon as the cheque is written lets new requests
/// through while the recipient still counts the old debt, which takes the
/// recipient past its disconnect limit, and bee blocklists the payer
/// (`debitAction.Apply`).
///
/// Bee ends the stream with `FullClose` on acceptance and `Reset` on a
/// refusal, but rust-yamux reports a remote reset as end of stream too,
/// so this can't tell the two apart; `Err` means only that the stream
/// failed some other way.
pub async fn await_processed(mut stream: Stream) -> Result<(), SwapError> {
    let mut rest = Vec::new();
    stream
        .read_to_end(&mut rest)
        .await
        .map_err(|e| SwapError::Rejected(format!("cheque stream failed: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ant_chain::chequebook::sign_cheque;
    use ant_crypto::{
        ethereum_address_from_public_key, random_secp256k1_secret, SECP256K1_SECRET_LEN,
    };
    use k256::ecdsa::SigningKey;

    fn make_secret() -> [u8; SECP256K1_SECRET_LEN] {
        random_secp256k1_secret()
    }

    fn eoa(secret: &[u8; SECP256K1_SECRET_LEN]) -> [u8; 20] {
        let sk = SigningKey::from_bytes(secret.into()).unwrap();
        ethereum_address_from_public_key(sk.verifying_key())
    }

    /// Bee's settlement response headers decode to the rates the
    /// cheque amount is computed from; a missing deduction is zero, a
    /// missing exchange rate is refused.
    #[test]
    fn settlement_headers_decode_like_bee() {
        let enc = |h: Vec<(&str, Vec<u8>)>| {
            HeadersPb {
                headers: h
                    .into_iter()
                    .map(|(k, v)| HeaderPb {
                        key: k.into(),
                        value: v,
                    })
                    .collect(),
            }
            .encode_to_vec()
        };
        // Mainnet oracle at the time of writing: 100 000 PLUR/unit, 100 PLUR.
        let rates = parse_settlement_headers(&enc(vec![
            ("exchange", vec![0x01, 0x86, 0xa0]),
            ("deduction", vec![0x64]),
        ]))
        .unwrap();
        assert_eq!(rates.exchange_rate, U256::from(100_000u64));
        assert_eq!(rates.deduction, U256::from(100u64));
        assert_eq!(
            rates.cheque_amount(675_000),
            Some(U256::from(67_500_000_100u64))
        );
        let no_deduction =
            parse_settlement_headers(&enc(vec![("exchange", vec![0x01, 0x86, 0xa0])])).unwrap();
        assert_eq!(no_deduction.deduction, U256::zero());
        assert!(parse_settlement_headers(&enc(vec![])).is_err());
        assert!(parse_settlement_headers(&[0u8; 0]).is_err());
    }

    /// JSON encoder / decoder round-trip.
    #[test]
    fn json_round_trip() {
        let secret = make_secret();
        let cheque = Cheque {
            chequebook: [0x11u8; 20],
            beneficiary: [0x22u8; 20],
            cumulative_payout: U256::from(123_456_789u64),
        };
        let signed = sign_cheque(&secret, &cheque, 100).unwrap();
        let bytes = encode_signed_cheque_json(&signed);
        let back = decode_signed_cheque_json(&bytes).unwrap();
        assert_eq!(back, signed);
    }

    /// Bee-shape JSON sanity: hex prefix, decimal cumulative, base64 sig.
    #[test]
    fn json_layout_matches_bee_shape() {
        let signed = SignedCheque {
            cheque: Cheque {
                chequebook: hex::decode("11111111111111111111111111111111111111aa")
                    .unwrap()
                    .try_into()
                    .unwrap(),
                beneficiary: hex::decode("22222222222222222222222222222222222222bb")
                    .unwrap()
                    .try_into()
                    .unwrap(),
                cumulative_payout: U256::from(987_654u64),
            },
            signature: [0xcdu8; 65],
        };
        let bytes = encode_signed_cheque_json(&signed);
        let s = std::str::from_utf8(&bytes).unwrap();
        assert!(s.contains(r#""Chequebook":"0x11111111111111111111111111111111111111aa""#));
        assert!(s.contains(r#""Beneficiary":"0x22222222222222222222222222222222222222bb""#));
        // A bare number, as go's `json.Marshal(*big.Int)` writes it: bee's
        // `UnmarshalJSON` refuses the quoted form (issue #121).
        assert!(s.contains(r#""CumulativePayout":987654,"#));
        // base64 of 65 bytes 0xcd = "zc3N…" repeated.
        assert!(s.contains(r#""Signature":"#));
        // Round-trip back, ensuring even the high-bit byte parses.
        let back = decode_signed_cheque_json(&bytes).unwrap();
        assert_eq!(back, signed);
    }

    /// Decoding takes the bare number bee sends, a payout past `u64`
    /// without losing digits, and the quoted form older Ant versions
    /// sent; it refuses a payout that isn't a decimal integer.
    #[test]
    fn cumulative_payout_decodes_bare_and_quoted() {
        let body = |payout: &str| {
            format!(
                r#"{{"Chequebook":"0x11111111111111111111111111111111111111aa","Beneficiary":"0x22222222222222222222222222222222222222bb","CumulativePayout":{payout},"Signature":"{}"}}"#,
                BASE64.encode([0xcdu8; 65])
            )
        };
        let big = "123456789012345678901234567890";
        for payout in [big.to_string(), format!("\"{big}\"")] {
            let decoded = decode_signed_cheque_json(body(&payout).as_bytes()).unwrap();
            assert_eq!(
                decoded.cheque.cumulative_payout,
                U256::from_dec_str(big).unwrap()
            );
        }
        for bad in ["-1", "1.5", "1e3", "\"0x10\"", "null"] {
            assert!(
                decode_signed_cheque_json(body(bad).as_bytes()).is_err(),
                "{bad} must be refused"
            );
        }
    }

    /// Ledger accepts a valid first cheque, then a strictly-larger
    /// follow-up. Rejects: smaller / equal cumulative, wrong
    /// beneficiary, swapped-issuer attack.
    #[test]
    fn ledger_monotonicity_and_pinning() {
        let issuer_secret = make_secret();
        let issuer_eoa = eoa(&issuer_secret);
        let our_eoa = [0xaau8; 20];
        let cheque1 = Cheque {
            chequebook: [0x77u8; 20],
            beneficiary: our_eoa,
            cumulative_payout: U256::from(100u64),
        };
        let signed1 = sign_cheque(&issuer_secret, &cheque1, 100).unwrap();
        let ledger = CreditLedger::open(None, 100, our_eoa);
        let prev = ledger.record_accepted(&signed1).unwrap();
        assert_eq!(prev, U256::zero());
        assert_eq!(ledger.cumulative_for(&[0x77u8; 20]), U256::from(100u64));

        // Strictly-larger follow-up accepted.
        let mut cheque2 = cheque1;
        cheque2.cumulative_payout = U256::from(150u64);
        let signed2 = sign_cheque(&issuer_secret, &cheque2, 100).unwrap();
        let prev2 = ledger.record_accepted(&signed2).unwrap();
        assert_eq!(prev2, U256::from(100u64));
        assert_eq!(ledger.cumulative_for(&[0x77u8; 20]), U256::from(150u64));

        // Equal cumulative rejected.
        let mut cheque_dup = cheque2.clone();
        cheque_dup.cumulative_payout = U256::from(150u64);
        let signed_dup = sign_cheque(&issuer_secret, &cheque_dup, 100).unwrap();
        assert!(matches!(
            ledger.record_accepted(&signed_dup).unwrap_err(),
            SwapError::Rejected(_),
        ));

        // Smaller cumulative rejected.
        let mut cheque_back = cheque2.clone();
        cheque_back.cumulative_payout = U256::from(120u64);
        let signed_back = sign_cheque(&issuer_secret, &cheque_back, 100).unwrap();
        assert!(matches!(
            ledger.record_accepted(&signed_back).unwrap_err(),
            SwapError::Rejected(_),
        ));

        // Wrong beneficiary rejected.
        let mut cheque_wrong_ben = cheque2.clone();
        cheque_wrong_ben.beneficiary = [0xffu8; 20];
        cheque_wrong_ben.cumulative_payout = U256::from(160u64);
        let signed_wrong_ben = sign_cheque(&issuer_secret, &cheque_wrong_ben, 100).unwrap();
        assert!(matches!(
            ledger.record_accepted(&signed_wrong_ben).unwrap_err(),
            SwapError::Rejected(_),
        ));

        // Swapped issuer (attacker mints a cheque under same chequebook):
        // signature recovers to a different EOA → rejected by sticky binding.
        let attacker_secret = make_secret();
        let mut cheque_attack = cheque2.clone();
        cheque_attack.cumulative_payout = U256::from(200u64);
        let signed_attack = sign_cheque(&attacker_secret, &cheque_attack, 100).unwrap();
        let err = ledger.record_accepted(&signed_attack).unwrap_err();
        match err {
            SwapError::Rejected(msg) => assert!(
                msg.contains("issuer for chequebook"),
                "expected pinning rejection, got: {msg}",
            ),
            other => panic!("expected Rejected, got {other:?}"),
        }

        // The legit issuer can still issue a strictly-larger cheque.
        let mut cheque3 = cheque2;
        cheque3.cumulative_payout = U256::from(300u64);
        let signed3 = sign_cheque(&issuer_secret, &cheque3, 100).unwrap();
        let prev3 = ledger.record_accepted(&signed3).unwrap();
        assert_eq!(prev3, U256::from(150u64));
        assert_eq!(ledger.cumulative_for(&[0x77u8; 20]), U256::from(300u64));

        let _ = issuer_eoa; // silence unused — recovery already validates it
    }

    /// Persist + reload across "process restarts" preserves
    /// monotonicity. A replay of the highest cheque after restart
    /// must still be rejected.
    #[test]
    fn ledger_persistence_replay_safe() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credits.json");
        let issuer_secret = make_secret();
        let our_eoa = [0xaau8; 20];

        let cheque = Cheque {
            chequebook: [0x77u8; 20],
            beneficiary: our_eoa,
            cumulative_payout: U256::from(500u64),
        };
        let signed = sign_cheque(&issuer_secret, &cheque, 100).unwrap();

        {
            let ledger = CreditLedger::open(Some(path.clone()), 100, our_eoa);
            ledger.record_accepted(&signed).unwrap();
        }

        // "Restart" — new ledger reads the snapshot.
        let ledger2 = CreditLedger::open(Some(path), 100, our_eoa);
        assert_eq!(
            ledger2.cumulative_for(&[0x77u8; 20]),
            U256::from(500u64),
            "snapshot must round-trip across restarts",
        );

        // Replay of the same cheque must be rejected as non-monotonic.
        let err = ledger2.record_accepted(&signed).unwrap_err();
        assert!(matches!(err, SwapError::Rejected(_)));
    }

    /// Outbound ledger persists across restarts and rejects
    /// going-backwards mistakes by surfacing the previous value.
    #[test]
    fn outbound_ledger_persists_cumulative() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("outbound.json");
        let beneficiary = [0xbbu8; 20];

        {
            let ob = OutboundLedger::open(Some(path.clone()), [0xc1; 20]);
            assert_eq!(ob.cumulative_for(&beneficiary), U256::zero());
            ob.record_issued(&beneficiary, U256::from(42u64)).unwrap();
        }
        let ob2 = OutboundLedger::open(Some(path), [0xc1; 20]);
        assert_eq!(ob2.cumulative_for(&beneficiary), U256::from(42u64));
    }

    /// Cumulatives belong to the chequebook that issued them (PR #126
    /// R1-F1): another chequebook on the same file starts from zero and
    /// owes nothing, the first one's survive its writes, and a file from
    /// before the sections is adopted by exactly one chequebook.
    #[test]
    fn outbound_ledger_is_scoped_to_its_chequebook() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pushsync_outbound.json");
        let (a, b) = ([0xa0u8; 20], [0xb0u8; 20]);
        let peer = [0x11u8; 20];
        std::fs::write(
            &path,
            br#"{"1111111111111111111111111111111111111111":"1500"}"#,
        )
        .unwrap();

        let ledger_a = OutboundLedger::open(Some(path.clone()), a);
        assert_eq!(ledger_a.cumulative_for(&peer), U256::from(1_500u64));
        let ledger_b = OutboundLedger::open(Some(path.clone()), b);
        assert_eq!(ledger_b.cumulative_for(&peer), U256::zero());
        assert_eq!(ledger_b.total_issued(), U256::zero());

        ledger_b.record_issued(&peer, U256::from(200u64)).unwrap();
        ledger_a.record_issued(&peer, U256::from(1_600u64)).unwrap();
        let reopened_b = OutboundLedger::open(Some(path.clone()), b);
        assert_eq!(reopened_b.cumulative_for(&peer), U256::from(200u64));
        let reopened_a = OutboundLedger::open(Some(path), a);
        assert_eq!(reopened_a.cumulative_for(&peer), U256::from(1_600u64));
        assert_eq!(reopened_a.total_issued(), U256::from(1_600u64));
    }

    /// `EmitChequePb` round-trips through prost, so any future change
    /// to the protobuf shape is caught by tests instead of by bee.
    #[test]
    fn emit_cheque_pb_round_trip() {
        let signed = SignedCheque {
            cheque: Cheque {
                chequebook: [0x11u8; 20],
                beneficiary: [0x22u8; 20],
                cumulative_payout: U256::from(7u64),
            },
            signature: [0xeeu8; 65],
        };
        let json = encode_signed_cheque_json(&signed);
        let msg = EmitChequePb { cheque: json };
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();
        let back = EmitChequePb::decode(buf.as_slice()).unwrap();
        let back_signed = decode_signed_cheque_json(&back.cheque).unwrap();
        assert_eq!(back_signed, signed);
    }

    /// `issue_cheque` binds the new cumulative correctly and the
    /// signature recovers to the issuer's own EOA.
    #[test]
    fn issue_cheque_round_trip() {
        let secret = make_secret();
        let want_eoa = eoa(&secret);
        let signed =
            issue_cheque(&secret, [0x99u8; 20], [0x88u8; 20], U256::from(999u64), 100).unwrap();
        assert_eq!(signed.cheque.cumulative_payout, U256::from(999u64));
        let recovered = recover_cheque_signer(&signed, 100).unwrap();
        assert_eq!(recovered, want_eoa);
    }
}
