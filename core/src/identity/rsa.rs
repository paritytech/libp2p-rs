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

//! RSA keys.

use asn1_der::typed::{DerEncodable, DerDecodable, DerTypeView, Sequence};
use asn1_der::{DerObject, Asn1DerError, Asn1DerErrorVariant, Sink, VecBacking};
use super::error::*;
use ring::rand::SystemRandom;
use ring::signature::{self, RsaKeyPair, RSA_PKCS1_SHA256, RSA_PKCS1_2048_8192_SHA256};
use ring::signature::KeyPair;
use std::{fmt, sync::Arc};
use zeroize::Zeroize;

/// An RSA keypair.
#[derive(Clone)]
pub struct Keypair(Arc<RsaKeyPair>);

impl Keypair {
    /// Decode an RSA keypair from a DER-encoded private key in PKCS#8 PrivateKeyInfo
    /// format (i.e. unencrypted) as defined in [RFC5208].
    ///
    /// [RFC5208]: https://tools.ietf.org/html/rfc5208#section-5
    pub fn from_pkcs8(der: &mut [u8]) -> Result<Keypair, DecodingError> {
        let kp = RsaKeyPair::from_pkcs8(&der)
            .map_err(|e| DecodingError::new("RSA PKCS#8 PrivateKeyInfo").source(e))?;
        der.zeroize();
        Ok(Keypair(Arc::new(kp)))
    }

    /// Get the public key from the keypair.
    pub fn public(&self) -> PublicKey {
        PublicKey(self.0.public_key().as_ref().to_vec())
    }

    /// Sign a message with this keypair.
    pub fn sign(&self, data: &[u8]) -> Result<Vec<u8>, SigningError> {
        let mut signature = vec![0; self.0.public_modulus_len()];
        let rng = SystemRandom::new();
        match self.0.sign(&RSA_PKCS1_SHA256, &rng, &data, &mut signature) {
            Ok(()) => Ok(signature),
            Err(e) => Err(SigningError::new("RSA").source(e))
        }
    }
}

/// An RSA public key.
#[derive(Clone, PartialEq, Eq)]
pub struct PublicKey(Vec<u8>);

impl PublicKey {
    /// Verify an RSA signature on a message using the public key.
    pub fn verify(&self, msg: &[u8], sig: &[u8]) -> bool {
        let key = signature::UnparsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, &self.0);
        key.verify(msg, sig).is_ok()
    }

    /// Encode the RSA public key in DER as a PKCS#1 RSAPublicKey structure,
    /// as defined in [RFC3447].
    ///
    /// [RFC3447]: https://tools.ietf.org/html/rfc3447#appendix-A.1.1
    pub fn encode_pkcs1(&self) -> Vec<u8> {
        // This is the encoding currently used in-memory, so it is trivial.
        self.0.clone()
    }

    /// Encode the RSA public key in DER as a X.509 SubjectPublicKeyInfo structure,
    /// as defined in [RFC5280].
    ///
    /// [RFC5280]: https://tools.ietf.org/html/rfc5280#section-4.1
    pub fn encode_x509(&self) -> Vec<u8> {
        let spki = Asn1SubjectPublicKeyInfo {
            algorithmIdentifier: Asn1RsaEncryption {
                algorithm: Asn1OidRsaEncryption,
                parameters: ()
            },
            subjectPublicKey: Asn1SubjectPublicKey(self.clone())
        };
        let mut buf = Vec::new();
        let buf = spki.encode(&mut buf).map(|_| buf)
            .expect("RSA X.509 public key encoding failed.");
        buf
    }

    /// Decode an RSA public key from a DER-encoded X.509 SubjectPublicKeyInfo
    /// structure. See also `encode_x509`.
    pub fn decode_x509(pk: &[u8]) -> Result<PublicKey, DecodingError> {
        Asn1SubjectPublicKeyInfo::decode(pk)
            .map_err(|e| DecodingError::new("RSA X.509").source(e))
            .map(|spki| spki.subjectPublicKey.0)
    }
}

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PublicKey(PKCS1): ")?;
        for byte in &self.0 {
            write!(f, "{:x}", byte)?;
        }
        Ok(())
    }
}

//////////////////////////////////////////////////////////////////////////////
// DER encoding / decoding of public keys
//
// Primer: http://luca.ntop.org/Teaching/Appunti/asn1.html
// Playground: https://lapo.it/asn1js/

/// A raw ASN1 OID.
#[derive(Copy, Clone)]
struct Asn1RawOid<'a> {
    object: DerObject<'a>
}

impl<'a> Asn1RawOid<'a> {
    /// The underlying OID as byte literal.
    pub fn oid(&self) -> &[u8] {
        self.object.value()
    }

    /// Writes an OID raw `value` as DER-object to `sink`.
    pub fn write<S: Sink>(value: &[u8], sink: &mut S) -> Result<(), Asn1DerError> {
        DerObject::write(Self::TAG, value.len(), &mut value.iter(), sink)
    }
}

impl<'a> DerTypeView<'a> for Asn1RawOid<'a> {
    const TAG: u8 = 6;

    fn object(&self) -> DerObject<'a> {
        self.object
    }
}

impl<'a> DerEncodable for Asn1RawOid<'a> {
    fn encode<S: Sink>(&self, sink: &mut S) -> Result<(), Asn1DerError> {
        self.object.encode(sink)
    }
}

impl<'a> DerDecodable<'a> for Asn1RawOid<'a> {
    fn load(object: DerObject<'a>) -> Result<Self, Asn1DerError> {
        if object.tag() != Self::TAG {
            return Err(Asn1DerError::new(Asn1DerErrorVariant::InvalidData(
                "DER object tag is not the object identifier tag.",
            )));
        }

        Ok(Self { object })
    }
}

/// The ASN.1 OID for "rsaEncryption".
#[derive(Clone)]
struct Asn1OidRsaEncryption;

impl Asn1OidRsaEncryption {
    /// The DER encoding of the object identifier (OID) 'rsaEncryption' for
    /// RSA public keys defined for X.509 in [RFC-3279] and used in
    /// SubjectPublicKeyInfo structures defined in [RFC-5280].
    ///
    /// [RFC-3279]: https://tools.ietf.org/html/rfc3279#section-2.3.1
    /// [RFC-5280]: https://tools.ietf.org/html/rfc5280#section-4.1
    const OID: [u8;9] = [ 0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x01 ];
}

impl DerEncodable for Asn1OidRsaEncryption {
    fn encode<S: Sink>(&self, sink: &mut S) -> Result<(), Asn1DerError> {
        Asn1RawOid::write(&Self::OID, sink)
    }
}

impl DerDecodable<'_> for Asn1OidRsaEncryption {
    fn load(object: DerObject<'_>) -> Result<Self, Asn1DerError> {
        match Asn1RawOid::load(object)?.oid() {
            oid if oid == Self::OID => Ok(Self),
            _ => Err(Asn1DerError::new(Asn1DerErrorVariant::InvalidData(
                "DER object is not the 'rsaEncryption' identifier.",
            )))
        }
    }
}

/// The ASN.1 AlgorithmIdentifier for "rsaEncryption".
struct Asn1RsaEncryption {
    algorithm: Asn1OidRsaEncryption,
    parameters: ()
}

impl DerEncodable for Asn1RsaEncryption {
    fn encode<S: Sink>(&self, sink: &mut S) -> Result<(), Asn1DerError> {
        let mut algorithm_buf = Vec::new();
        let algorithm = self.algorithm.der_object(VecBacking(&mut algorithm_buf))?;

        let mut parameters_buf = Vec::new();
        let parameters = self.parameters.der_object(VecBacking(&mut parameters_buf))?;

        Sequence::write(&[algorithm, parameters], sink)
    }
}

impl DerDecodable<'_> for Asn1RsaEncryption {
    fn load(object: DerObject<'_>) -> Result<Self, Asn1DerError> {
        let seq: Sequence = Sequence::load(object)?;

        Ok(Self{
            algorithm: seq.get_as(0)?,
            parameters: seq.get_as(1)?,
        })
    }
}

/// The ASN.1 SubjectPublicKey inside a SubjectPublicKeyInfo,
/// i.e. encoded as a DER BIT STRING.
struct Asn1SubjectPublicKey(PublicKey);

impl DerEncodable for Asn1SubjectPublicKey {
    fn encode<S: Sink>(&self, sink: &mut S) -> Result<(), Asn1DerError> {
        let pk_der = &(self.0).0;
        let mut bit_string = Vec::with_capacity(pk_der.len() + 1);
        // The number of bits in pk_der is trivially always a multiple of 8,
        // so there are always 0 "unused bits" signaled by the first byte.
        bit_string.push(0u8);
        bit_string.extend(pk_der);
        DerObject::write(3, bit_string.len(), &mut bit_string.iter(), sink)?;
        Ok(())
    }
}

impl DerDecodable<'_> for Asn1SubjectPublicKey {
    fn load(object: DerObject<'_>) -> Result<Self, Asn1DerError> {
        if object.tag() != 3 {
            return Err(Asn1DerError::new(
                Asn1DerErrorVariant::InvalidData("DER object tag is not the bit string tag."),
            ));
        }

        let pk_der: Vec<u8> = object.value().into_iter().skip(1).cloned().collect();
        // We don't parse pk_der further as an ASN.1 RsaPublicKey, since
        // we only need the DER encoding for `verify`.
        Ok(Self(PublicKey(pk_der)))
    }
}

/// ASN.1 SubjectPublicKeyInfo
#[allow(non_snake_case)]
struct Asn1SubjectPublicKeyInfo {
    algorithmIdentifier: Asn1RsaEncryption,
    subjectPublicKey: Asn1SubjectPublicKey
}

impl DerEncodable for Asn1SubjectPublicKeyInfo {
    fn encode<S: Sink>(&self, sink: &mut S) -> Result<(), Asn1DerError> {
        let mut identifier_buf = Vec::new();
        let identifier = self.algorithmIdentifier.der_object(VecBacking(&mut identifier_buf))?;

        let mut key_buf = Vec::new();
        let key = self.subjectPublicKey.der_object(VecBacking(&mut key_buf))?;

        Sequence::write(&[identifier, key], sink)
    }
}

impl DerDecodable<'_> for Asn1SubjectPublicKeyInfo {
    fn load(object: DerObject<'_>) -> Result<Self, Asn1DerError> {
        let seq: Sequence = Sequence::load(object)?;

        Ok(Self {
            algorithmIdentifier: seq.get_as(0)?,
            subjectPublicKey: seq.get_as(1)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::*;
    use rand::seq::SliceRandom;
    use std::fmt;

    const KEY1: &'static [u8] = include_bytes!("test/rsa-2048.pk8");
    const KEY2: &'static [u8] = include_bytes!("test/rsa-3072.pk8");
    const KEY3: &'static [u8] = include_bytes!("test/rsa-4096.pk8");

    #[derive(Clone)]
    struct SomeKeypair(Keypair);

    impl fmt::Debug for SomeKeypair {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "SomeKeypair")
        }
    }

    impl Arbitrary for SomeKeypair {
        fn arbitrary<G: Gen>(g: &mut G) -> SomeKeypair {
            let mut key = [KEY1, KEY2, KEY3].choose(g).unwrap().to_vec();
            SomeKeypair(Keypair::from_pkcs8(&mut key).unwrap())
        }
    }

    #[test]
    fn rsa_from_pkcs8() {
        assert!(Keypair::from_pkcs8(&mut KEY1.to_vec()).is_ok());
        assert!(Keypair::from_pkcs8(&mut KEY2.to_vec()).is_ok());
        assert!(Keypair::from_pkcs8(&mut KEY3.to_vec()).is_ok());
    }

    #[test]
    fn rsa_x509_encode_decode() {
        fn prop(SomeKeypair(kp): SomeKeypair) -> Result<bool, String> {
            let pk = kp.public();
            PublicKey::decode_x509(&pk.encode_x509())
                .map_err(|e| e.to_string())
                .map(|pk2| pk2 == pk)
        }
        QuickCheck::new().tests(10).quickcheck(prop as fn(_) -> _);
    }

    #[test]
    fn rsa_sign_verify() {
        fn prop(SomeKeypair(kp): SomeKeypair, msg: Vec<u8>) -> Result<bool, SigningError> {
            kp.sign(&msg).map(|s| kp.public().verify(&msg, &s))
        }
        QuickCheck::new().tests(10).quickcheck(prop as fn(_,_) -> _);
    }
}
