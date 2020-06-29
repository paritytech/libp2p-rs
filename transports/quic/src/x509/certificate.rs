// Copyright 2020 Parity Technologies (UK) Ltd.
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

//! Certificate handling for libp2p
//!
//! This handles generation, signing, and verification.
//!
//! This crate uses the `log` crate to emit log output.  Events that will occur
//! normally are output at `trace` level, while “expected” error conditions
//! (ones that can result during correct use of the library) are logged at
//! `debug` level.

use super::LIBP2P_SIGNING_PREFIX_LENGTH;
use libp2p_core::identity;

const LIBP2P_OID: &[u64] = &[1, 3, 6, 1, 4, 1, 53594, 1, 1];
const LIBP2P_SIGNATURE_ALGORITHM_PUBLIC_KEY_LENGTH: usize = 65;
static LIBP2P_SIGNATURE_ALGORITHM: &rcgen::SignatureAlgorithm = &rcgen::PKCS_ECDSA_P256_SHA256;
// preferred, but not supported by rustls yet
//const LIBP2P_SIGNATURE_ALGORITHM_PUBLIC_KEY_LENGTH: usize = 32;
//static LIBP2P_SIGNATURE_ALGORITHM: &rcgen::SignatureAlgorithm =
// &rcgen::PKCS_ED25519

/// Generates a self-signed TLS certificate that includes a libp2p-specific
/// certificate extension containing the public key of the given keypair.
pub(crate) fn make_cert(
    keypair: &identity::Keypair,
) -> Result<rcgen::Certificate, super::ConfigError> {
    // Keypair used to sign the certificate.
    let certif_keypair = rcgen::KeyPair::generate(&LIBP2P_SIGNATURE_ALGORITHM)?;

    // The libp2p-specific extension to the certificate contains a signature of the public key
    // of the certificate using the libp2p private key.
    let libp2p_ext_signature = {
        let certif_pubkey = certif_keypair.public_key_raw();
        assert_eq!(
            certif_pubkey.len(),
            LIBP2P_SIGNATURE_ALGORITHM_PUBLIC_KEY_LENGTH,
            "ed25519 public keys are {} bytes",
            LIBP2P_SIGNATURE_ALGORITHM_PUBLIC_KEY_LENGTH
        );

        let mut buf =
            [0u8; LIBP2P_SIGNING_PREFIX_LENGTH + LIBP2P_SIGNATURE_ALGORITHM_PUBLIC_KEY_LENGTH];
        buf[..LIBP2P_SIGNING_PREFIX_LENGTH].copy_from_slice(&super::LIBP2P_SIGNING_PREFIX[..]);
        buf[LIBP2P_SIGNING_PREFIX_LENGTH..].copy_from_slice(certif_pubkey);
        keypair.sign(&buf)?
    };

    // Generate the libp2p-specific extension.
    let libp2p_extension: rcgen::CustomExtension = {
        let extension_content = {
            let serialized_pubkey = keypair.public().into_protobuf_encoding();
            yasna::construct_der(|writer| {
                writer.write_sequence(|writer| {
                    writer
                        .next()
                        .write_bitvec_bytes(&serialized_pubkey, serialized_pubkey.len() * 8);
                    writer
                        .next()
                        .write_bitvec_bytes(&libp2p_ext_signature, libp2p_ext_signature.len() * 8);
                })
            })
        };

        let mut ext = rcgen::CustomExtension::from_oid_content(LIBP2P_OID, extension_content);
        ext.set_criticality(true);
        ext
    };

    let certificate = {
        let mut params = rcgen::CertificateParams::new(vec![]);
        params.distinguished_name = rcgen::DistinguishedName::new();
        params.custom_extensions.push(libp2p_extension);
        params.alg = &LIBP2P_SIGNATURE_ALGORITHM;
        params.key_pair = Some(certif_keypair);
        rcgen::Certificate::from_params(params)?
    };

    Ok(certificate)
}
