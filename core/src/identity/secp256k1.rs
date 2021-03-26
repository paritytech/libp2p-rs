// Copyright 2019 Parity Technologies (UK) Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

//! Secp256k1 keys.

use asn1_der::typed::{DerDecodable, Sequence};
use rand::RngCore;
use sha2::{Digest as ShaDigestTrait, Sha256};
use secp256k1::{Message, Signature};
use super::error::{DecodingError, SigningError};
use zeroize::Zeroize;
use core::fmt;

/// A Secp256k1 keypair.
#[derive(Clone)]
pub struct Keypair {
    secret: SecretKey,
    public: PublicKey
}

impl Keypair {
    /// Generate a new sec256k1 `Keypair`.
    pub fn generate() -> Keypair {
        Keypair::from(SecretKey::generate())
    }

    /// Get the public key of this keypair.
    pub fn public(&self) -> &PublicKey {
        &self.public
    }

    /// Get the secret key of this keypair.
    pub fn secret(&self) -> &SecretKey {
        &self.secret
    }
}

impl fmt::Debug for Keypair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Keypair").field("public", &self.public).finish()
    }
}

/// Promote a Secp256k1 secret key into a keypair.
impl From<SecretKey> for Keypair {
    fn from(secret: SecretKey) -> Keypair {
        let public = PublicKey(secp256k1::PublicKey::from_secret_key(&secret.0));
        Keypair { secret, public }
    }
}

/// Demote a Secp256k1 keypair into a secret key.
impl From<Keypair> for SecretKey {
    fn from(kp: Keypair) -> SecretKey {
        kp.secret
    }
}

/// A Secp256k1 secret key.
#[derive(Clone)]
pub struct SecretKey(secp256k1::SecretKey);

impl fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SecretKey")
    }
}

impl SecretKey {
    /// Generate a new Secp256k1 secret key.
    pub fn generate() -> SecretKey {
        let mut r = rand::thread_rng();
        let mut b = [0; secp256k1::util::SECRET_KEY_SIZE];
        // This is how it is done in `secp256k1::SecretKey::random` which
        // we do not use here because it uses `rand::Rng` from rand-0.4.
        loop {
            r.fill_bytes(&mut b);
            if let Ok(k) = secp256k1::SecretKey::parse(&b) {
                return SecretKey(k)
            }
        }
    }

    /// Create a secret key from a byte slice, zeroing the slice on success.
    /// If the bytes do not constitute a valid Secp256k1 secret key, an
    /// error is returned.
    pub fn from_bytes(mut sk: impl AsMut<[u8]>) -> Result<SecretKey, DecodingError> {
        let sk_bytes = sk.as_mut();
        let secret = secp256k1::SecretKey::parse_slice(&*sk_bytes)
            .map_err(|_| DecodingError::new("failed to parse secp256k1 secret key"))?;
        sk_bytes.zeroize();
        Ok(SecretKey(secret))
    }

    /// Decode a DER-encoded Secp256k1 secret key in an ECPrivateKey
    /// structure as defined in [RFC5915], zeroing the input slice on success.
    ///
    /// [RFC5915]: https://tools.ietf.org/html/rfc5915
    pub fn from_der(mut der: impl AsMut<[u8]>) -> Result<SecretKey, DecodingError> {
        // TODO: Stricter parsing.
        let der_obj = der.as_mut();
        let obj: Sequence = DerDecodable::decode(der_obj)
            .map_err(|e| DecodingError::new("Secp256k1 DER ECPrivateKey").source(e))?;
        let sk_obj = obj.get(1)
            .map_err(|e| DecodingError::new("Not enough elements in DER").source(e))?;
        let mut sk_bytes: Vec<u8> = asn1_der::typed::DerDecodable::load(sk_obj)
            .map_err(DecodingError::new)?;
        let sk = SecretKey::from_bytes(&mut sk_bytes)?;
        sk_bytes.zeroize();
        der_obj.zeroize();
        Ok(sk)
    }

    /// Sign a message with this secret key, producing a DER-encoded
    /// ECDSA signature, as defined in [RFC3278].
    ///
    /// [RFC3278]: https://tools.ietf.org/html/rfc3278#section-8.2
    pub fn sign(&self, msg: &[u8]) -> Result<Vec<u8>, SigningError> {
        self.sign_hash(Sha256::digest(msg).as_ref())
    }

    /// Returns the raw bytes of the secret key.
    pub fn to_bytes(&self) -> [u8; 32] {
        self.0.serialize()
    }

    /// Sign a raw message of length 256 bits with this secret key, produces a DER-encoded
    /// ECDSA signature.
    pub fn sign_hash(&self, msg: &[u8]) -> Result<Vec<u8>, SigningError> {
        let m = Message::parse_slice(msg)
            .map_err(|_| SigningError::new("failed to parse secp256k1 digest"))?;
        Ok(secp256k1::sign(&m, &self.0).0.serialize_der().as_ref().into())
    }
}

/// A Secp256k1 public key.
#[derive(PartialEq, Eq, Clone)]
pub struct PublicKey(secp256k1::PublicKey);

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PublicKey(compressed): ")?;
        for byte in &self.encode() {
            write!(f, "{:x}", byte)?;
        }
        Ok(())
    }
}

impl PublicKey {
    /// Verify the Secp256k1 signature on a message using the public key.
    pub fn verify(&self, msg: &[u8], sig: &[u8]) -> bool {
        self.verify_hash(Sha256::digest(msg).as_ref(), sig)
    }

    /// Verify the Secp256k1 DER-encoded signature on a raw 256-bit message using the public key.
    pub fn verify_hash(&self, msg: &[u8], sig: &[u8]) -> bool {
        Message::parse_slice(msg)
            .and_then(|m| Signature::parse_der(sig).map(|s| secp256k1::verify(&m, &s, &self.0)))
            .unwrap_or(false)
    }

    /// Encode the public key in compressed form, i.e. with one coordinate
    /// represented by a single bit.
    pub fn encode(&self) -> [u8; 33] {
        self.0.serialize_compressed()
    }

    /// Encode the public key in uncompressed form.
    pub fn encode_uncompressed(&self) -> [u8; 65] {
        self.0.serialize()
    }

    /// Decode a public key from a byte slice in the the format produced
    /// by `encode`.
    pub fn decode(k: &[u8]) -> Result<PublicKey, DecodingError> {
        secp256k1::PublicKey::parse_slice(k, Some(secp256k1::PublicKeyFormat::Compressed))
            .map_err(|_| DecodingError::new("failed to parse secp256k1 public key"))
            .map(PublicKey)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secp256k1_secret_from_bytes() {
        let sk1 = SecretKey::generate();
        let mut sk_bytes = [0; 32];
        sk_bytes.copy_from_slice(&sk1.0.serialize()[..]);
        let sk2 = SecretKey::from_bytes(&mut sk_bytes).unwrap();
        assert_eq!(sk1.0.serialize(), sk2.0.serialize());
        assert_eq!(sk_bytes, [0; 32]);
    }
}
