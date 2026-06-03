//! OpenPGP signing of commits, gated behind the `gpg` feature.
//!
//! brz signs a revision by clearsigning the revision's testament short text
//! and storing the result in the repository's signature store. This module
//! produces that clearsigned text in-process with Sequoia, so a commit can
//! be signed without shelling out to `gpg`.

use sequoia_openpgp::parse::Parse;
use sequoia_openpgp::policy::StandardPolicy;
use sequoia_openpgp::serialize::stream::{Message, Signer};
use sequoia_openpgp::Cert;
use std::io::Write;

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
}
