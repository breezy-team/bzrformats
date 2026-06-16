//! OpenPGP signing and verification of commits, gated behind the `gpg`
//! feature.
//!
//! brz signs a revision by clearsigning the revision's testament short text
//! and storing the result in the repository's signature store. This module
//! produces that clearsigned text in-process with Sequoia, so a commit can
//! be signed without shelling out to `gpg`, and verifies a stored clearsigned
//! signature back to its plaintext.

use sequoia_openpgp::parse::stream::{
    MessageLayer, MessageStructure, VerificationHelper, VerifierBuilder,
};
use sequoia_openpgp::parse::Parse;
use sequoia_openpgp::policy::StandardPolicy;
use sequoia_openpgp::serialize::stream::{Message, Signer};
use sequoia_openpgp::{Cert, KeyHandle};
use std::io::{Read, Write};

/// An error from signing.
#[derive(Debug)]
pub enum SignError {
    /// The signing key could not be parsed.
    BadKey(String),
    /// The key has no usable signing-capable secret subkey.
    NoSigningKey,
    /// The OpenPGP layer failed to produce the signature.
    Sign(String),
}

impl std::fmt::Display for SignError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SignError::BadKey(e) => write!(f, "bad signing key: {e}"),
            SignError::NoSigningKey => write!(f, "no signing-capable secret key"),
            SignError::Sign(e) => write!(f, "signing failed: {e}"),
        }
    }
}

impl std::error::Error for SignError {}

/// Clearsign `plaintext` with the secret key in `cert_bytes` (a Transferable
/// Secret Key, ASCII-armored or binary), returning the armored clearsigned
/// text — the form brz stores in the signature store.
pub fn clearsign(plaintext: &[u8], cert_bytes: &[u8]) -> Result<Vec<u8>, SignError> {
    let policy = StandardPolicy::new();
    let cert = Cert::from_bytes(cert_bytes).map_err(|e| SignError::BadKey(e.to_string()))?;

    // Find a signing-capable secret key and turn it into a keypair.
    let keypair = cert
        .keys()
        .with_policy(&policy, None)
        .secret()
        .for_signing()
        .next()
        .ok_or(SignError::NoSigningKey)?
        .key()
        .clone()
        .into_keypair()
        .map_err(|e| SignError::Sign(e.to_string()))?;

    let mut sink: Vec<u8> = Vec::new();
    {
        let message = Message::new(&mut sink);
        // The cleartext signature framework produces its own armor framing.
        let mut signer = Signer::new(message, keypair)
            .map_err(|e| SignError::Sign(e.to_string()))?
            .cleartext()
            .build()
            .map_err(|e| SignError::Sign(e.to_string()))?;
        signer
            .write_all(plaintext)
            .map_err(|e| SignError::Sign(e.to_string()))?;
        signer
            .finalize()
            .map_err(|e| SignError::Sign(e.to_string()))?;
    }
    Ok(sink)
}

/// The outcome of verifying a signature, mirroring breezy's `gpg` status
/// constants (`SIGNATURE_VALID` etc.) so the values map straight onto the
/// Python ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationResult {
    /// The signature is valid and from an acceptable key.
    Valid = 0,
    /// The signing key is not in the supplied keyring.
    KeyMissing = 1,
    /// A signature is present but does not validate.
    NotValid = 2,
    /// The content is not signed at all.
    NotSigned = 3,
    /// The signature is from an expired key.
    Expired = 4,
}

/// A successful verification: the status plus the plaintext that was signed.
///
/// `plaintext` is `Some` whenever the clearsigned framing could be parsed
/// (even if the cryptographic check failed), so a caller can compare it to an
/// expected testament; it is `None` only when the input was not a clearsigned
/// message.
#[derive(Debug, Clone)]
pub struct Verification {
    /// The verification status.
    pub result: VerificationResult,
    /// The signed plaintext, if the clearsigned framing parsed.
    pub plaintext: Option<Vec<u8>>,
}

/// Collects the verification outcome while Sequoia walks the message.
struct Helper<'a> {
    certs: &'a [Cert],
    /// Set to the strongest outcome seen across signature layers.
    result: VerificationResult,
    /// Whether the message contained any signature group at all.
    saw_signature: bool,
}

impl VerificationHelper for &mut Helper<'_> {
    fn get_certs(&mut self, ids: &[KeyHandle]) -> sequoia_openpgp::Result<Vec<Cert>> {
        // Hand back the supplied keyring; absent keys surface as a missing-key
        // verification rather than an error.
        let _ = ids;
        Ok(self.certs.to_vec())
    }

    fn check(&mut self, structure: MessageStructure) -> sequoia_openpgp::Result<()> {
        for layer in structure {
            if let MessageLayer::SignatureGroup { results } = layer {
                self.saw_signature = true;
                self.result = summarize(&results);
            }
        }
        Ok(())
    }
}

/// Reduce a layer's per-signature results to a single status: a good signature
/// wins; otherwise an expired-key or missing-key result is reported; otherwise
/// the signature is not valid.
fn summarize(
    results: &[Result<
        sequoia_openpgp::parse::stream::GoodChecksum<'_>,
        sequoia_openpgp::parse::stream::VerificationError<'_>,
    >],
) -> VerificationResult {
    use sequoia_openpgp::parse::stream::VerificationError;
    let mut best = VerificationResult::NotValid;
    for r in results {
        match r {
            Ok(_) => return VerificationResult::Valid,
            Err(VerificationError::MissingKey { .. }) => {
                best = VerificationResult::KeyMissing;
            }
            Err(VerificationError::UnboundKey { .. }) | Err(VerificationError::BadKey { .. }) => {
                // An expired/revoked binding presents as a bad/unbound key.
                if best == VerificationResult::NotValid {
                    best = VerificationResult::Expired;
                }
            }
            Err(_) => {}
        }
    }
    best
}

/// Parse a keyring given as raw public-key blobs (each ASCII-armored or
/// binary) into [`Cert`]s. Used by callers that pass keys as bytes rather than
/// depending on the OpenPGP crate directly.
pub fn parse_keyring(keyring: &[Vec<u8>]) -> Result<Vec<Cert>, String> {
    keyring
        .iter()
        .map(|b| Cert::from_bytes(b).map_err(|e| e.to_string()))
        .collect()
}

/// Verify a clearsigned message against a keyring.
///
/// Returns the verification status and the extracted plaintext. `certs` is the
/// set of trusted public keys; an empty keyring yields
/// [`VerificationResult::KeyMissing`]. The plaintext is returned even when the
/// cryptographic check fails, so a caller can still compare it to an expected
/// testament (as breezy's `verify_revision_signature` does).
pub fn verify_clearsigned(signed: &[u8], certs: &[Cert]) -> Verification {
    let policy = StandardPolicy::new();
    let mut helper = Helper {
        certs,
        result: VerificationResult::NotValid,
        saw_signature: false,
    };
    let builder = match VerifierBuilder::from_bytes(signed) {
        Ok(b) => b,
        // Not a parseable OpenPGP message: treat as unsigned content.
        Err(_) => {
            return Verification {
                result: VerificationResult::NotSigned,
                plaintext: None,
            }
        }
    };
    let mut verifier = match builder.with_policy(&policy, None, &mut helper) {
        Ok(v) => v,
        Err(_) => {
            return Verification {
                result: VerificationResult::NotSigned,
                plaintext: None,
            }
        }
    };
    let mut plaintext = Vec::new();
    // Reading drives `Helper::check`, which records the result.
    let read = verifier.read_to_end(&mut plaintext);
    let result = if !helper.saw_signature {
        // Parsed as OpenPGP but carried no signature: not signed.
        VerificationResult::NotSigned
    } else {
        match read {
            Ok(_) => helper.result,
            // A failed read with no key is the missing-key case; else invalid.
            Err(_) if certs.is_empty() => VerificationResult::KeyMissing,
            Err(_) => VerificationResult::NotValid,
        }
    };
    Verification {
        result,
        plaintext: Some(plaintext),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sequoia_openpgp::cert::CertBuilder;
    use sequoia_openpgp::serialize::Serialize;

    #[test]
    fn clearsign_produces_a_signed_message() {
        let (cert, _) = CertBuilder::new().add_signing_subkey().generate().unwrap();
        let mut tsk = Vec::new();
        cert.as_tsk().serialize(&mut tsk).unwrap();

        let signed = clearsign(b"bazaar testament short form 3 strict\n", &tsk).unwrap();
        let text = String::from_utf8(signed).unwrap();
        assert!(text.starts_with("-----BEGIN PGP SIGNED MESSAGE-----"));
        assert!(text.contains("bazaar testament short form 3 strict"));
        assert!(text.contains("-----BEGIN PGP SIGNATURE-----"));
    }

    #[test]
    fn bad_key_is_rejected() {
        assert!(matches!(
            clearsign(b"x", b"not a key"),
            Err(SignError::BadKey(_))
        ));
    }

    /// Sign with a fresh key and verify against its public cert: valid, and the
    /// extracted plaintext matches.
    #[test]
    fn verify_round_trips_a_signed_message() {
        let (cert, _) = CertBuilder::new().add_signing_subkey().generate().unwrap();
        let mut tsk = Vec::new();
        cert.as_tsk().serialize(&mut tsk).unwrap();
        let plaintext = b"bazaar-ng testament short form 1\nrevision-id: r1\n";
        let signed = clearsign(plaintext, &tsk).unwrap();

        let v = verify_clearsigned(&signed, std::slice::from_ref(&cert));
        assert_eq!(v.result, VerificationResult::Valid);
        // The clearsigned framework dash-escapes and re-wraps, but the body
        // round-trips to the original plaintext.
        assert_eq!(v.plaintext.as_deref(), Some(&plaintext[..]));
    }

    /// Verifying against a keyring that lacks the signing key reports the key
    /// as missing.
    #[test]
    fn verify_reports_missing_key() {
        let (signer, _) = CertBuilder::new().add_signing_subkey().generate().unwrap();
        let mut tsk = Vec::new();
        signer.as_tsk().serialize(&mut tsk).unwrap();
        let signed = clearsign(b"hello\n", &tsk).unwrap();

        // Verify with an empty keyring.
        let v = verify_clearsigned(&signed, &[]);
        assert_eq!(v.result, VerificationResult::KeyMissing);
    }

    /// Non-OpenPGP input is reported as not signed.
    #[test]
    fn verify_unsigned_content() {
        let v = verify_clearsigned(b"just some text, not a signature\n", &[]);
        assert_eq!(v.result, VerificationResult::NotSigned);
        assert_eq!(v.plaintext, None);
    }
}
