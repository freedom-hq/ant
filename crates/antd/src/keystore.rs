//! Web3 v3 keystore decryption for Freedom's injected identity
//! (PLAN.md J.5.E3).
//!
//! Freedom writes the node's swarm key as a Web3 Secret Storage v3
//! keystore at `<data-dir>/keys/swarm.key` (the same JSON format
//! `go-ethereum` / bee use) and launches the daemon with the decryption
//! `password` in its config. For `antd` to be a true drop-in we must
//! detect that file, decrypt it with the password, and adopt the
//! resulting 32-byte secp256k1 secret as our node identity — rather than
//! generating our own `identity.json`.
//!
//! The v3 scheme (<https://github.com/ethereum/wiki>, "Web3 Secret
//! Storage Definition"):
//!
//! 1. Derive a 32-byte key from the password with the named KDF
//!    (`scrypt` or `pbkdf2` with `hmac-sha256`).
//! 2. Verify `keccak256(derived[16..32] ‖ ciphertext) == mac`; a
//!    mismatch means the wrong password.
//! 3. AES-128-CTR-decrypt the ciphertext with `derived[0..16]` as key
//!    and the stored `iv` as the initial counter. The plaintext is the
//!    32-byte private key.
//!
//! Pure `RustCrypto` building blocks; no network, fully unit-tested
//! against the canonical Web3 Secret Storage test vectors.

use aes::Aes128;
use anyhow::{anyhow, bail, Context, Result};
use ctr::cipher::{KeyIvInit, StreamCipher};
use ctr::Ctr128BE;
use serde::Deserialize;
use sha2::Sha256;

type Aes128Ctr = Ctr128BE<Aes128>;

#[derive(Deserialize)]
struct Keystore {
    // go-ethereum lowercases this; some older tools use `Crypto`.
    #[serde(alias = "Crypto")]
    crypto: Crypto,
    version: u32,
}

#[derive(Deserialize)]
struct Crypto {
    cipher: String,
    cipherparams: CipherParams,
    ciphertext: String,
    kdf: String,
    kdfparams: serde_json::Value,
    mac: String,
}

#[derive(Deserialize)]
struct CipherParams {
    iv: String,
}

/// Decrypt a Web3 v3 keystore JSON blob, returning the 32-byte private
/// key. Errors distinguish a wrong password (MAC mismatch) from a
/// malformed / unsupported keystore so the operator gets an actionable
/// message.
pub fn decrypt_v3(json: &str, password: &str) -> Result<[u8; 32]> {
    let ks: Keystore = serde_json::from_str(json).context("parse v3 keystore json")?;
    if ks.version != 3 {
        bail!(
            "unsupported keystore version {} (only v3 is supported)",
            ks.version
        );
    }
    let c = &ks.crypto;
    if !c.cipher.eq_ignore_ascii_case("aes-128-ctr") {
        bail!(
            "unsupported keystore cipher {:?} (only aes-128-ctr)",
            c.cipher
        );
    }

    let ciphertext = decode_hex(&c.ciphertext).context("ciphertext")?;
    let iv = decode_hex(&c.cipherparams.iv).context("cipher iv")?;
    if iv.len() != 16 {
        bail!("cipher iv must be 16 bytes, got {}", iv.len());
    }

    let derived = derive_key(&c.kdf, &c.kdfparams, password.as_bytes())?;
    if derived.len() < 32 {
        bail!("derived key too short ({} bytes)", derived.len());
    }

    // MAC = keccak256(derived[16..32] ‖ ciphertext).
    let mut mac_input = Vec::with_capacity(16 + ciphertext.len());
    mac_input.extend_from_slice(&derived[16..32]);
    mac_input.extend_from_slice(&ciphertext);
    let mac = ant_crypto::keccak256(&mac_input);
    let want_mac = decode_hex(&c.mac).context("mac")?;
    if mac.as_slice() != want_mac.as_slice() {
        bail!("keystore MAC mismatch — wrong password or corrupt keystore");
    }

    // AES-128-CTR decrypt in place.
    let mut buf = ciphertext;
    let mut cipher = Aes128Ctr::new_from_slices(&derived[..16], &iv)
        .map_err(|e| anyhow!("init aes-128-ctr: {e}"))?;
    cipher.apply_keystream(&mut buf);

    if buf.len() != 32 {
        bail!("decrypted secret is {} bytes, expected 32", buf.len());
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&buf);
    Ok(out)
}

/// Derive the symmetric key from the password per the `kdf` field.
/// Supports the two KDFs the v3 spec defines: `scrypt` and `pbkdf2`
/// (with `hmac-sha256`).
fn derive_key(kdf: &str, params: &serde_json::Value, password: &[u8]) -> Result<Vec<u8>> {
    let dklen = params
        .get("dklen")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(32) as usize;
    if dklen < 32 {
        bail!("kdf dklen {dklen} too small (need >= 32)");
    }
    let salt = decode_hex(
        params
            .get("salt")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| anyhow!("kdfparams.salt missing"))?,
    )
    .context("kdf salt")?;

    let mut out = vec![0u8; dklen];
    match kdf.to_ascii_lowercase().as_str() {
        "scrypt" => {
            let n = u64_param(params, "n")?;
            let r = u64_param(params, "r")? as u32;
            let p = u64_param(params, "p")? as u32;
            if !n.is_power_of_two() {
                bail!("scrypt n={n} is not a power of two");
            }
            let log_n = n.trailing_zeros() as u8;
            let sparams = scrypt::Params::new(log_n, r, p, dklen)
                .map_err(|e| anyhow!("invalid scrypt params: {e}"))?;
            scrypt::scrypt(password, &salt, &sparams, &mut out)
                .map_err(|e| anyhow!("scrypt: {e}"))?;
        }
        "pbkdf2" => {
            let prf = params
                .get("prf")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("hmac-sha256");
            if !prf.eq_ignore_ascii_case("hmac-sha256") {
                bail!("unsupported pbkdf2 prf {prf:?} (only hmac-sha256)");
            }
            let c = u64_param(params, "c")? as u32;
            pbkdf2::pbkdf2_hmac::<Sha256>(password, &salt, c, &mut out);
        }
        other => bail!("unsupported kdf {other:?}"),
    }
    Ok(out)
}

fn u64_param(params: &serde_json::Value, key: &str) -> Result<u64> {
    params
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| anyhow!("kdfparams.{key} missing or not an integer"))
}

fn decode_hex(s: &str) -> Result<Vec<u8>> {
    let s = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    hex::decode(s).map_err(|e| anyhow!("invalid hex: {e}"))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    // Canonical Web3 Secret Storage pbkdf2 test vector (password
    // "testpassword" → the private key below). The matching scrypt
    // canonical vector uses `r=1`, which violates RustCrypto's strict
    // scrypt spec bound (`N < 2^(128·r/8)`); real bee keystores use
    // `r=8`, so scrypt is covered by the spec-valid round-trip instead.
    pub(crate) const EXPECTED_KEY: &str =
        "7a28b5ba57c53603b0b07b56bba752f7784bf506fa95edc395f5cf6c7514fe9d";

    pub(crate) const PBKDF2_KEYSTORE: &str = r#"{
        "crypto": {
            "cipher": "aes-128-ctr",
            "cipherparams": { "iv": "6087dab2f9fdbbfaddc31a909735c1e6" },
            "ciphertext": "5318b4d5bcd28de64ee5559e671353e16f075ecae9f99c7a79a38af5f869aa46",
            "kdf": "pbkdf2",
            "kdfparams": {
                "c": 262144,
                "dklen": 32,
                "prf": "hmac-sha256",
                "salt": "ae3cd4e7013836a3df6bd7241b12db061dbe2c6785853cce422d148a624ce0bd"
            },
            "mac": "517ead924a9d0dc3124507e3393d175ce3ff7c1e96529c6c555ce9e51205e9b2"
        },
        "id": "3198bc9c-6672-5ab3-d995-4942343ae5b6",
        "version": 3
    }"#;

    /// Independent sanity check that our pbkdf2 wiring matches the
    /// RFC-style PBKDF2-HMAC-SHA256 reference (`password`/`salt`/`c=1`).
    #[test]
    fn pbkdf2_hmac_sha256_rfc_vector() {
        let mut out = [0u8; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(b"password", b"salt", 1, &mut out);
        assert_eq!(
            hex::encode(out),
            "120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b",
        );
    }

    /// Full decrypt of the canonical pbkdf2 keystore → known private key.
    #[test]
    fn decrypts_pbkdf2_vector() {
        let key = decrypt_v3(PBKDF2_KEYSTORE, "testpassword").expect("decrypt pbkdf2");
        assert_eq!(hex::encode(key), EXPECTED_KEY);
    }

    /// Build a v3 keystore for `key`/`password` with the given KDF, then
    /// decrypt it. Exercises the scrypt branch with spec-valid params
    /// (`r=8`) end-to-end without an external scrypt vector.
    fn roundtrip(kdf: &str, key: &[u8; 32], password: &str) -> [u8; 32] {
        let salt = [0x11u8; 32];
        let iv = [0x22u8; 16];
        let mut derived = [0u8; 32];
        let kdfparams = match kdf {
            "scrypt" => {
                let params = scrypt::Params::new(12, 8, 1, 32).unwrap();
                scrypt::scrypt(password.as_bytes(), &salt, &params, &mut derived).unwrap();
                serde_json::json!({
                    "dklen": 32, "n": 4096, "r": 8, "p": 1, "salt": hex::encode(salt),
                })
            }
            "pbkdf2" => {
                pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, 4096, &mut derived);
                serde_json::json!({
                    "c": 4096, "dklen": 32, "prf": "hmac-sha256", "salt": hex::encode(salt),
                })
            }
            _ => unreachable!(),
        };
        let mut ciphertext = *key;
        Aes128Ctr::new_from_slices(&derived[..16], &iv)
            .unwrap()
            .apply_keystream(&mut ciphertext);
        let mut mac_in = Vec::new();
        mac_in.extend_from_slice(&derived[16..32]);
        mac_in.extend_from_slice(&ciphertext);
        let mac = ant_crypto::keccak256(&mac_in);
        let json = serde_json::json!({
            "version": 3,
            "crypto": {
                "cipher": "aes-128-ctr",
                "cipherparams": { "iv": hex::encode(iv) },
                "ciphertext": hex::encode(ciphertext),
                "kdf": kdf,
                "kdfparams": kdfparams,
                "mac": hex::encode(mac),
            }
        });
        decrypt_v3(&json.to_string(), password).expect("roundtrip decrypt")
    }

    #[test]
    fn scrypt_roundtrip() {
        let key = [0xABu8; 32];
        assert_eq!(roundtrip("scrypt", &key, "s3cret"), key);
    }

    #[test]
    fn pbkdf2_roundtrip() {
        let key = [0xCDu8; 32];
        assert_eq!(roundtrip("pbkdf2", &key, "s3cret"), key);
    }

    #[test]
    fn wrong_password_is_mac_mismatch() {
        let err = decrypt_v3(PBKDF2_KEYSTORE, "not-the-password").unwrap_err();
        assert!(
            err.to_string().contains("MAC mismatch"),
            "expected MAC mismatch, got: {err}",
        );
    }

    #[test]
    fn rejects_non_v3() {
        let bad = PBKDF2_KEYSTORE.replace("\"version\": 3", "\"version\": 1");
        let err = decrypt_v3(&bad, "testpassword").unwrap_err();
        assert!(err.to_string().contains("version"), "got: {err}");
    }
}
