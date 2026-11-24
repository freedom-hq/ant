//! Secp256k1 keys, Keccak-256, Ethereum and Swarm overlay addresses, and Bee-compatible
//! BZZ handshake signing (EIP-191 prefixed digest).
//!
//! Also exposes the chunk-level primitives: BMT hashing and content-addressed chunk
//! validation, see [`bmt`].

pub mod act;
pub mod bmt;
pub mod encryption;
mod error;
pub mod gsoc;
pub mod pss;
pub mod soc;

pub use bmt::{
    bmt_hash_with_span, bmt_root, cac_new, cac_valid, first_segment_address,
    first_segment_siblings, BMT_DEPTH, CHUNK_SIZE, SPAN_SIZE,
};
pub use encryption::{
    decrypt_chunk, decrypt_chunk_parts, encrypt_chunk_unpadded, random_encryption_key,
    stream_xor_in_place, DecryptError, EncryptError, KEY_LENGTH, REFERENCE_SIZE,
};
pub use error::CryptoError;
pub use soc::{soc_valid, SOC_HEADER_SIZE, SOC_ID_SIZE, SOC_MIN_CHUNK_SIZE, SOC_SIG_SIZE};

use k256::ecdsa::{RecoveryId, Signature, SigningKey, VerifyingKey};
use sha3::{Digest, Keccak256};

pub const OVERLAY_NONCE_LEN: usize = 32;
pub const SECP256K1_SECRET_LEN: usize = 32;

/// Keccak-256 as used by Ethereum / Swarm (`LegacyKeccak256` in Bee).
#[must_use]
pub fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut h = Keccak256::new();
    h.update(data);
    h.finalize().into()
}

/// Uncompressed SEC1 public key bytes without the `0x04` prefix (64 bytes), for hashing.
#[must_use]
pub fn uncompressed_pubkey_xy(pk: &VerifyingKey) -> [u8; 64] {
    let ep = pk.to_encoded_point(false);
    let s = ep.as_bytes();
    debug_assert_eq!(s[0], 0x04);
    let mut out = [0u8; 64];
    out.copy_from_slice(&s[1..65]);
    out
}

/// Ethereum address (20 bytes) from a secp256k1 public key (matches Bee `NewEthereumAddress`).
#[must_use]
pub fn ethereum_address_from_public_key(pk: &VerifyingKey) -> [u8; 20] {
    let xy = uncompressed_pubkey_xy(pk);
    let hash = keccak256(&xy);
    let mut addr = [0u8; 20];
    addr.copy_from_slice(&hash[12..]);
    addr
}

/// Swarm overlay address: `keccak256(eth_address ‖ network_id_le ‖ nonce)` (Bee `NewOverlayFromEthereumAddress`).
#[must_use]
pub fn overlay_from_ethereum_address(
    eth_address: &[u8; 20],
    network_id: u64,
    nonce: &[u8; OVERLAY_NONCE_LEN],
) -> [u8; 32] {
    let mut buf = [0u8; 20 + 8 + OVERLAY_NONCE_LEN];
    buf[..20].copy_from_slice(eth_address);
    buf[20..28].copy_from_slice(&network_id.to_le_bytes());
    buf[28..].copy_from_slice(nonce);
    keccak256(&buf)
}

/// Length of the EVM address embedded in the BZZ handshake (the peer's
/// chequebook contract). An empty chequebook is represented on the wire
/// as zero bytes (the field is optional), but it gets zero-padded to 20
/// bytes inside the signed preimage so that the recoverable signature
/// is over a fixed-length tail.
pub const HANDSHAKE_CHEQUEBOOK_LEN: usize = 20;

/// Wire version of the BZZ handshake protocol.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HandshakeWireVersion {
    /// bee 2.7.x — preimage: `bee-handshake- ‖ underlay ‖ overlay ‖ network_id_be`.
    /// Nonce sits next to the signature in the `Ack` rather than inside
    /// `BzzAddress`.
    V14,
    /// bee 2.8.0 — preimage extended with `‖ nonce ‖ timestamp_be ‖ chequebook(20B)`.
    /// Nonce, timestamp, and chequebook are now fields of `BzzAddress`.
    V15,
}

/// Payload signed in the BZZ handshake. The preimage depends on the wire
/// version we're talking to — V14 is the bee 2.7.x layout, V15 is bee
/// 2.8.0 with the extra `nonce ‖ timestamp_be ‖ chequebook(20B)` tail
/// (see [pkg/bzz/address.go](../bee/pkg/bzz/address.go)
/// `generateSignData`). Both layouts get wrapped with the EIP-191
/// prefix downstream by [`sign_handshake_data`] / [`recover_public_key`].
///
/// `chequebook` must be either zero-length (peer advertises no
/// chequebook) or exactly [`HANDSHAKE_CHEQUEBOOK_LEN`] bytes — empty
/// chequebooks are zero-padded into the preimage for V15. Any other
/// length is [`CryptoError::BadChequebookLen`] (checked for both wire
/// versions, although V14 doesn't sign the chequebook).
#[allow(clippy::too_many_arguments)]
pub fn handshake_sign_data(
    version: HandshakeWireVersion,
    underlay: &[u8],
    overlay: &[u8; 32],
    network_id: u64,
    nonce: &[u8; OVERLAY_NONCE_LEN],
    timestamp: i64,
    chequebook: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    if !chequebook.is_empty() && chequebook.len() != HANDSHAKE_CHEQUEBOOK_LEN {
        return Err(CryptoError::BadChequebookLen(chequebook.len()));
    }
    Ok(match version {
        HandshakeWireVersion::V14 => {
            let mut v = Vec::with_capacity(14 + underlay.len() + 32 + 8);
            v.extend_from_slice(b"bee-handshake-");
            v.extend_from_slice(underlay);
            v.extend_from_slice(overlay);
            v.extend_from_slice(&network_id.to_be_bytes());
            v
        }
        HandshakeWireVersion::V15 => {
            let cb_padded: [u8; HANDSHAKE_CHEQUEBOOK_LEN] = if chequebook.is_empty() {
                [0u8; HANDSHAKE_CHEQUEBOOK_LEN]
            } else {
                let mut a = [0u8; HANDSHAKE_CHEQUEBOOK_LEN];
                a.copy_from_slice(chequebook);
                a
            };
            let mut v = Vec::with_capacity(
                14 + underlay.len() + 32 + 8 + OVERLAY_NONCE_LEN + 8 + HANDSHAKE_CHEQUEBOOK_LEN,
            );
            v.extend_from_slice(b"bee-handshake-");
            v.extend_from_slice(underlay);
            v.extend_from_slice(overlay);
            v.extend_from_slice(&network_id.to_be_bytes());
            v.extend_from_slice(nonce);
            v.extend_from_slice(&timestamp.to_be_bytes());
            v.extend_from_slice(&cb_padded);
            v
        }
    })
}

fn ethereum_prefixed_sign_data(sign_data: &[u8]) -> Vec<u8> {
    let len_str = sign_data.len().to_string();
    let mut p = Vec::with_capacity(2 + 26 + len_str.len() + sign_data.len());
    p.extend_from_slice(b"\x19Ethereum Signed Message:\n");
    p.extend_from_slice(len_str.as_bytes());
    p.extend_from_slice(sign_data);
    p
}

/// Keccak256 hash of the EIP-191–wrapped payload (Bee `hashWithEthereumPrefix`).
#[must_use]
pub fn ethereum_signed_message_hash(sign_data: &[u8]) -> [u8; 32] {
    keccak256(&ethereum_prefixed_sign_data(sign_data))
}

/// Sign handshake data the same way Bee's `crypto.Signer.Sign` does (EIP-191 + compact recovery id).
pub fn sign_handshake_data(
    secret: &[u8; SECP256K1_SECRET_LEN],
    sign_data: &[u8],
) -> Result<[u8; 65], CryptoError> {
    let sk = SigningKey::from_bytes(secret.into())?;
    let mut hasher = Keccak256::new();
    hasher.update(ethereum_prefixed_sign_data(sign_data));
    let (sig, recid) = sk.sign_digest_recoverable(hasher)?;
    let v = recid.to_byte().saturating_add(27);
    let mut out = [0u8; 65];
    out[..32].copy_from_slice(sig.r().to_bytes().as_slice());
    out[32..64].copy_from_slice(sig.s().to_bytes().as_slice());
    out[64] = v;
    Ok(out)
}

/// Sign a 32-byte prehash directly, **without** the EIP-191 wrapping
/// `sign_handshake_data` adds. Used for Swarm SOC signing where the
/// digest is the raw `keccak256(id ‖ cac_address)` — see
/// `bee/pkg/soc/soc.go::Sign`.
///
/// Output format matches `sign_handshake_data`: `r ‖ s ‖ v` with `v`
/// in `27..=28`, i.e. compatible with [`recover_public_key_from_prehash`].
pub fn sign_prehash(
    secret: &[u8; SECP256K1_SECRET_LEN],
    digest: &[u8; 32],
) -> Result<[u8; 65], CryptoError> {
    let sk = SigningKey::from_bytes(secret.into())?;
    let (sig, recid) = sk.sign_prehash_recoverable(digest)?;
    let v = recid.to_byte().saturating_add(27);
    let mut out = [0u8; 65];
    out[..32].copy_from_slice(sig.r().to_bytes().as_slice());
    out[32..64].copy_from_slice(sig.s().to_bytes().as_slice());
    out[64] = v;
    Ok(out)
}

/// Recover signer public key from a Bee / Ethereum 65-byte signature (`r‖s‖v` with `v` in 27..=28).
pub fn recover_public_key(
    signature: &[u8; 65],
    sign_data: &[u8],
) -> Result<VerifyingKey, CryptoError> {
    let digest = ethereum_signed_message_hash(sign_data);
    recover_public_key_from_prehash(signature, &digest)
}

/// Recover signer public key from a 65-byte signature over an
/// already-computed 32-byte digest (no EIP-191 prefix wrapping).
///
/// Used by Swarm's single-owner-chunk verification, which signs
/// `keccak256(id ‖ cac_address)` directly — see Bee
/// `pkg/soc/soc.go::FromChunk`.
///
/// Only `v ∈ {27, 28}` is accepted: those are the only values Bee (and
/// go-ethereum) signers emit. The "x-reduced" ids 29/30 are rejected,
/// and so is a high-S signature (k256's verify step refuses it).
pub fn recover_public_key_from_prehash(
    signature: &[u8; 65],
    digest: &[u8; 32],
) -> Result<VerifyingKey, CryptoError> {
    let recid = match signature[64] {
        27 => RecoveryId::new(false, false),
        28 => RecoveryId::new(true, false),
        _ => return Err(CryptoError::BadSignature),
    };
    let sig = Signature::from_slice(&signature[..64])?;
    Ok(VerifyingKey::recover_from_prehash(digest, &sig, recid)?)
}

/// Verify a BZZ handshake signature and that overlay matches `nonce` +
/// `network_id`. Bee 2.8 always validates the overlay; we follow suit
/// for both wire versions to keep the code path uniform.
///
/// For [`HandshakeWireVersion::V14`] `timestamp` and `chequebook` are
/// not signed. For both versions `chequebook` must be either empty or
/// exactly [`HANDSHAKE_CHEQUEBOOK_LEN`] bytes (any other length is
/// [`CryptoError::BadChequebookLen`]); on V15 an empty chequebook is
/// zero-padded into the preimage.
#[allow(clippy::too_many_arguments)]
pub fn verify_bzz_address_signature(
    version: HandshakeWireVersion,
    underlay: &[u8],
    overlay: &[u8; 32],
    signature: &[u8; 65],
    nonce: &[u8; OVERLAY_NONCE_LEN],
    network_id: u64,
    timestamp: i64,
    chequebook: &[u8],
) -> Result<[u8; 20], CryptoError> {
    let sign_data = handshake_sign_data(
        version, underlay, overlay, network_id, nonce, timestamp, chequebook,
    )?;
    let vk = recover_public_key(signature, &sign_data)?;
    let eth = ethereum_address_from_public_key(&vk);
    let expected = overlay_from_ethereum_address(&eth, network_id, nonce);
    if expected != *overlay {
        return Err(CryptoError::OverlayMismatch);
    }
    Ok(eth)
}

/// Random 32-byte overlay nonce.
#[must_use]
pub fn random_overlay_nonce() -> [u8; OVERLAY_NONCE_LEN] {
    let mut n = [0u8; OVERLAY_NONCE_LEN];
    getrandom::fill(&mut n).expect("OS RNG");
    n
}

/// Random secp256k1 signing key bytes (32 bytes).
#[must_use]
pub fn random_secp256k1_secret() -> [u8; SECP256K1_SECRET_LEN] {
    let mut s = [0u8; SECP256K1_SECRET_LEN];
    getrandom::fill(&mut s).expect("OS RNG");
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_verify_roundtrip_v15() {
        let secret = random_secp256k1_secret();
        let sk = SigningKey::from_bytes(&secret.into()).unwrap();
        let vk = VerifyingKey::from(&sk);
        let eth = ethereum_address_from_public_key(&vk);
        let network_id = 1u64;
        let nonce = random_overlay_nonce();
        let overlay = overlay_from_ethereum_address(&eth, network_id, &nonce);
        let underlay = b"/ip4/127.0.0.1/tcp/1634/p2p/12D3KooW";
        let timestamp = 1_715_000_000_i64;
        let chequebook: [u8; 20] = [0u8; 20];
        let sign_data = handshake_sign_data(
            HandshakeWireVersion::V15,
            underlay,
            &overlay,
            network_id,
            &nonce,
            timestamp,
            &chequebook,
        )
        .unwrap();
        let sig = sign_handshake_data(&secret, &sign_data).unwrap();
        let recovered_eth = verify_bzz_address_signature(
            HandshakeWireVersion::V15,
            underlay,
            &overlay,
            &sig,
            &nonce,
            network_id,
            timestamp,
            &chequebook,
        )
        .unwrap();
        assert_eq!(recovered_eth, eth);
    }

    /// V14 backward-compat: a 2.7 peer signs the shorter preimage,
    /// recovers fine, and the overlay validates against the same nonce
    /// that bee 2.7 ships next to the signature in the `Ack`.
    #[test]
    fn sign_verify_roundtrip_v14() {
        let secret = random_secp256k1_secret();
        let sk = SigningKey::from_bytes(&secret.into()).unwrap();
        let vk = VerifyingKey::from(&sk);
        let eth = ethereum_address_from_public_key(&vk);
        let network_id = 1u64;
        let nonce = random_overlay_nonce();
        let overlay = overlay_from_ethereum_address(&eth, network_id, &nonce);
        let underlay = b"/ip4/127.0.0.1/tcp/1634/p2p/12D3KooW";
        let sign_data = handshake_sign_data(
            HandshakeWireVersion::V14,
            underlay,
            &overlay,
            network_id,
            &nonce,
            0,
            &[],
        )
        .unwrap();
        let sig = sign_handshake_data(&secret, &sign_data).unwrap();
        let recovered_eth = verify_bzz_address_signature(
            HandshakeWireVersion::V14,
            underlay,
            &overlay,
            &sig,
            &nonce,
            network_id,
            0,
            &[],
        )
        .unwrap();
        assert_eq!(recovered_eth, eth);
    }

    /// Golden vector: the V15 preimage layout must end with `nonce ‖
    /// timestamp_be ‖ chequebook(20B)` and the V14 preimage must NOT
    /// include those tail bytes. Catches accidental field reorders.
    #[test]
    fn v15_preimage_layout() {
        let underlay = b"u";
        let overlay = [0u8; 32];
        let network_id = 1u64;
        let nonce = [0xcd_u8; OVERLAY_NONCE_LEN];
        let timestamp = 1_715_000_000_i64;
        let chequebook: [u8; HANDSHAKE_CHEQUEBOOK_LEN] = [0xab; HANDSHAKE_CHEQUEBOOK_LEN];

        let v14 = handshake_sign_data(
            HandshakeWireVersion::V14,
            underlay,
            &overlay,
            network_id,
            &nonce,
            timestamp,
            &chequebook,
        )
        .unwrap();
        let v15 = handshake_sign_data(
            HandshakeWireVersion::V15,
            underlay,
            &overlay,
            network_id,
            &nonce,
            timestamp,
            &chequebook,
        )
        .unwrap();
        assert_eq!(
            v14.len() + OVERLAY_NONCE_LEN + 8 + HANDSHAKE_CHEQUEBOOK_LEN,
            v15.len()
        );
        assert!(
            v15.starts_with(&v14),
            "V15 preimage must be V14 extended in-place"
        );
        assert!(v15.ends_with(&chequebook));

        // Empty chequebook on V15 zero-pads to 20 bytes inside the preimage.
        let v15_empty = handshake_sign_data(
            HandshakeWireVersion::V15,
            underlay,
            &overlay,
            network_id,
            &nonce,
            timestamp,
            &[],
        )
        .unwrap();
        assert_eq!(v15_empty.len(), v15.len());
        assert!(v15_empty.ends_with(&[0u8; HANDSHAKE_CHEQUEBOOK_LEN]));
    }

    /// Cross-check overlay formula with fixed vectors (endianness + layout).
    #[test]
    fn overlay_vector_endianness() {
        let mut eth = [0u8; 20];
        hex::decode_to_slice("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", &mut eth).unwrap();
        let network_id = 1u64;
        let mut nonce = [0u8; 32];
        nonce[0] = 0x01;
        let o = overlay_from_ethereum_address(&eth, network_id, &nonce);
        let h = hex::encode(o);
        assert_eq!(h.len(), 64);
        let sign_data = handshake_sign_data(
            HandshakeWireVersion::V14,
            b"bee-handshake-",
            &o,
            network_id,
            &nonce,
            0,
            &[],
        )
        .unwrap();
        assert!(sign_data.ends_with(&1u64.to_be_bytes()));
    }

    /// A signature the real signer produced, plus the digest it signed.
    fn signed_prehash() -> ([u8; 65], [u8; 32], VerifyingKey) {
        let secret = [0x11u8; SECP256K1_SECRET_LEN];
        let vk = VerifyingKey::from(&SigningKey::from_bytes(&secret.into()).unwrap());
        let digest = keccak256(b"recovery-id test");
        let sig = sign_prehash(&secret, &digest).unwrap();
        assert!(sig[64] == 27 || sig[64] == 28);
        assert_eq!(recover_public_key_from_prehash(&sig, &digest).unwrap(), vk);
        (sig, digest, vk)
    }

    /// Bee signers only emit v = 27/28; the x-reduced ids 29/30 (and
    /// any other value) must not be accepted.
    #[test]
    fn recover_rejects_v29_v30() {
        let (sig, digest, _) = signed_prehash();
        for v in [0u8, 1, 2, 3, 26, 29, 30, 31, 255] {
            let mut bad = sig;
            bad[64] = v;
            assert!(
                matches!(
                    recover_public_key_from_prehash(&bad, &digest),
                    Err(CryptoError::BadSignature)
                ),
                "v = {v} must be rejected"
            );
        }
        // Also a forged signature with `r < p − n`, for which an
        // x-reduced id (29/30) is mathematically meaningful and k256
        // recovers *some* key from it.
        let mut forged = [0u8; 65];
        let mut reduced_ok = false;
        for r in 1u8..=32 {
            forged[31] = r;
            forged[63] = 1;
            let Ok(s) = Signature::from_slice(&forged[..64]) else {
                continue;
            };
            for (v, y_odd) in [(29u8, false), (30, true)] {
                if VerifyingKey::recover_from_prehash(&digest, &s, RecoveryId::new(y_odd, true))
                    .is_ok()
                {
                    reduced_ok = true;
                    forged[64] = v;
                    assert!(
                        recover_public_key_from_prehash(&forged, &digest).is_err(),
                        "x-reduced v = {v} (r = {r}) must be rejected"
                    );
                }
            }
        }
        assert!(
            reduced_ok,
            "expected some small r to recover under an x-reduced id"
        );
        // The EIP-191 entry point goes through the same check.
        let mut bad = sign_handshake_data(&[0x22u8; SECP256K1_SECRET_LEN], b"x").unwrap();
        assert!(recover_public_key(&bad, b"x").is_ok());
        bad[64] += 2;
        assert!(recover_public_key(&bad, b"x").is_err());
    }

    /// Lock in k256's low-S rule: the malleable twin `(r, n − s, v ^ 1)`
    /// of a valid signature recovers the same key mathematically, but
    /// must be refused.
    #[test]
    fn recover_rejects_high_s() {
        let (sig, digest, _) = signed_prehash();
        let low = Signature::from_slice(&sig[..64]).unwrap();
        assert!(low.normalize_s().is_none(), "signer must emit low-S");
        let high_s = -*low.s();
        let high = Signature::from_scalars(low.r().to_bytes(), high_s.to_bytes()).unwrap();
        assert!(high.normalize_s().is_some(), "twin must be high-S");
        let mut twin = [0u8; 65];
        twin[..64].copy_from_slice(&high.to_bytes());
        for v in [27u8, 28] {
            twin[64] = v;
            assert!(
                recover_public_key_from_prehash(&twin, &digest).is_err(),
                "high-S with v = {v} must be rejected"
            );
        }
    }

    #[test]
    fn handshake_sign_data_bad_chequebook_len_errors() {
        let nonce = [0u8; OVERLAY_NONCE_LEN];
        for version in [HandshakeWireVersion::V14, HandshakeWireVersion::V15] {
            for len in [1usize, 19, 21, 32] {
                let cb = vec![0xab; len];
                assert!(
                    matches!(
                        handshake_sign_data(version, b"u", &[0u8; 32], 1, &nonce, 1, &cb),
                        Err(CryptoError::BadChequebookLen(l)) if l == len
                    ),
                    "{version:?} len {len}"
                );
            }
        }
        // ... and verification surfaces it rather than panicking.
        let r = verify_bzz_address_signature(
            HandshakeWireVersion::V15,
            b"u",
            &[0u8; 32],
            &[0u8; 65],
            &nonce,
            1,
            1,
            &[0xab; 7],
        );
        assert!(matches!(r, Err(CryptoError::BadChequebookLen(7))));
    }
}
