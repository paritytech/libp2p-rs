use crate::identity::error::SigningError;
use crate::identity::Keypair;
use crate::{identity, PublicKey};
use std::convert::TryInto;
use std::fmt;
use unsigned_varint::encode::usize_buffer;

/// A signed envelope contains an arbitrary byte string payload, a signature of the payload, and the public key that can be used to verify the signature.
///
/// For more details see libp2p RFC0002: <https://github.com/libp2p/specs/blob/master/RFC/0002-signed-envelopes.md>
#[derive(Debug, Clone, PartialEq)]
pub struct SignedEnvelope {
    key: PublicKey,
    payload_type: Vec<u8>,
    payload: Vec<u8>,
    signature: Vec<u8>,
}

impl SignedEnvelope {
    /// Constructs a new [`SignedEnvelope`].
    pub fn new(
        key: Keypair,
        domain_separation: String,
        payload_type: Vec<u8>,
        payload: Vec<u8>,
    ) -> Result<Self, SigningError> {
        let buffer = signature_payload(domain_separation, &payload_type, &payload);

        let signature = key.sign(&buffer)?;

        Ok(Self {
            key: key.public(),
            payload_type,
            payload,
            signature,
        })
    }

    /// Verify this [`SignedEnvelope`] against the provided domain-separation string.
    #[must_use]
    pub fn verify(&self, domain_separation: String) -> bool {
        let buffer = signature_payload(domain_separation, &self.payload_type, &self.payload);

        self.key.verify(&buffer, &self.signature)
    }

    /// Extract the payload of this [`SignedEnvelope`].
    ///
    /// You must provide the correct domain-separation string and expected payload type in order to get the payload.
    /// This guards against accidental mis-use of the payload where the signature was created for a different purpose or payload type.
    pub fn payload(
        &self,
        domain_separation: String,
        expected_payload_type: &[u8],
    ) -> Result<&[u8], ReadPayloadError> {
        if &self.payload_type != expected_payload_type {
            return Err(ReadPayloadError::UnexpectedPayloadType {
                expected: expected_payload_type.to_vec(),
                got: self.payload_type.clone(),
            });
        }

        if !self.verify(domain_separation) {
            return Err(ReadPayloadError::InvalidSignature);
        }

        Ok(&self.payload)
    }

    /// Encode this [`SignedEnvelope`] using the protobuf encoding specified in the RFC.
    pub fn into_protobuf_encoding(self) -> Vec<u8> {
        use prost::Message;

        let envelope = crate::envelope_proto::Envelope {
            public_key: Some(self.key.into()),
            payload_type: self.payload_type,
            payload: self.payload,
            signature: self.signature,
        };

        let mut buf = Vec::with_capacity(envelope.encoded_len());
        envelope
            .encode(&mut buf)
            .expect("Vec<u8> provides capacity as needed");

        buf
    }

    /// Decode a [`SignedEnvelope`] using the protobuf encoding specified in the RFC.
    pub fn from_protobuf_encoding(bytes: &[u8]) -> Result<Self, DecodingError> {
        use prost::Message;

        let envelope = crate::envelope_proto::Envelope::decode(bytes)?;

        Ok(Self {
            key: envelope.public_key.ok_or(DecodingError::MissingPublicKey)?.try_into()?,
            payload_type: envelope.payload_type,
            payload: envelope.payload,
            signature: envelope.signature,
        })
    }
}

fn signature_payload(domain_separation: String, payload_type: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut domain_sep_length_buffer = usize_buffer();
    let domain_sep_length =
        unsigned_varint::encode::usize(domain_separation.len(), &mut domain_sep_length_buffer);

    let mut payload_type_length_buffer = usize_buffer();
    let payload_type_length =
        unsigned_varint::encode::usize(payload_type.len(), &mut payload_type_length_buffer);

    let mut payload_length_buffer = usize_buffer();
    let payload_length = unsigned_varint::encode::usize(payload.len(), &mut payload_length_buffer);

    let mut buffer = Vec::with_capacity(
        domain_sep_length.len()
            + domain_separation.len()
            + payload_type_length.len()
            + payload_type.len()
            + payload_length.len()
            + payload.len(),
    );

    buffer.extend_from_slice(domain_sep_length);
    buffer.extend_from_slice(domain_separation.as_bytes());
    buffer.extend_from_slice(payload_type_length);
    buffer.extend_from_slice(payload_type);
    buffer.extend_from_slice(payload_length);
    buffer.extend_from_slice(payload);

    buffer
}

/// Errors that occur whilst decoding a [`SignedEnvelope`] from its byte representation.
#[derive(Debug)]
pub enum DecodingError {
    /// Decoding the provided bytes as a signed envelope failed.
    InvalidEnvelope(prost::DecodeError),
    /// The public key in the envelope could not be converted to our internal public key type.
    InvalidPublicKey(identity::error::DecodingError),
    /// The public key in the envelope could not be converted to our internal public key type.
    MissingPublicKey,
}

impl From<prost::DecodeError> for DecodingError {
    fn from(e: prost::DecodeError) -> Self {
        Self::InvalidEnvelope(e)
    }
}

impl From<identity::error::DecodingError> for DecodingError {
    fn from(e: identity::error::DecodingError) -> Self {
        Self::InvalidPublicKey(e)
    }
}

impl fmt::Display for DecodingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEnvelope(_) => write!(f, "Failed to decode envelope"),
            Self::InvalidPublicKey(_) => write!(f, "Failed to convert public key"),
            Self::MissingPublicKey => write!(f, "Public key is missing from protobuf struct")
        }
    }
}

impl std::error::Error for DecodingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidEnvelope(inner) => Some(inner),
            Self::InvalidPublicKey(inner) => Some(inner),
            Self::MissingPublicKey => None
        }
    }
}

/// Errors that occur whilst extracting the payload of a [`SignedEnvelope`].
#[derive(Debug)]
pub enum ReadPayloadError {
    /// The signature on the signed envelope does not verify with the provided domain separation string.
    InvalidSignature,
    /// The payload contained in the envelope is not of the expected type.
    UnexpectedPayloadType { expected: Vec<u8>, got: Vec<u8> },
}

impl fmt::Display for ReadPayloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSignature => write!(f, "Invalid signature"),
            Self::UnexpectedPayloadType { expected, got } => write!(
                f,
                "Unexpected payload type, expected {:?} but got {:?}",
                expected, got
            ),
        }
    }
}

impl std::error::Error for ReadPayloadError {}
