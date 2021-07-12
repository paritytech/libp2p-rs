// Copyright 2021 COMIT Network.
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

use crate::DEFAULT_TTL;
use asynchronous_codec::{Bytes, BytesMut, Decoder, Encoder};
use libp2p_core::{peer_record, signed_envelope, PeerRecord, SignedEnvelope};
use std::convert::{TryFrom, TryInto};
use std::fmt;
use unsigned_varint::codec::UviBytes;
use uuid::Uuid;

pub type Ttl = u64;

#[derive(Debug, Clone)]
pub enum Message {
    Register(NewRegistration),
    RegisterResponse(Result<Ttl, ErrorCode>),
    Unregister(Namespace),
    Discover {
        namespace: Option<Namespace>,
        cookie: Option<Cookie>,
        limit: Option<Ttl>,
    },
    DiscoverResponse(Result<(Vec<Registration>, Cookie), ErrorCode>),
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Namespace(String);

impl Namespace {
    /// Creates a new [`Namespace`] from a static string.
    ///
    /// This will panic if the namespace is too long. We accepting panicking in this case because we are enforcing a `static lifetime which means this value can only be a constant in the program and hence we hope the developer checked that it is of an acceptable length.
    pub fn from_static(value: &'static str) -> Self {
        if value.len() > 255 {
            panic!("Namespace '{}' is too long!", value)
        }

        Namespace(value.to_owned())
    }

    pub fn new(value: String) -> Result<Self, NamespaceTooLong> {
        if value.len() > 255 {
            return Err(NamespaceTooLong);
        }

        Ok(Namespace(value))
    }
}

impl From<Namespace> for String {
    fn from(namespace: Namespace) -> Self {
        namespace.0
    }
}

impl fmt::Display for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PartialEq<str> for Namespace {
    fn eq(&self, other: &str) -> bool {
        self.0.eq(other)
    }
}

impl PartialEq<Namespace> for str {
    fn eq(&self, other: &Namespace) -> bool {
        other.0.eq(self)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Namespace is too long")]
pub struct NamespaceTooLong;

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct Cookie {
    id: Uuid,
    namespace: Option<Namespace>,
}

impl Cookie {
    /// Construct a new [`Cookie`] for a given namespace.
    ///
    /// This cookie will only be valid for subsequent DISCOVER requests targeting the same namespace.
    pub fn for_namespace(namespace: Namespace) -> Self {
        Self {
            id: Uuid::new_v4(),
            namespace: Some(namespace),
        }
    }

    /// Construct a new [`Cookie`] for a DISCOVER request that inquires about all namespaces.
    pub fn for_all_namespaces() -> Self {
        Self {
            id: Uuid::new_v4(),
            namespace: None,
        }
    }

    pub fn into_wire_encoding(self) -> Vec<u8> {
        let namespace = self.namespace.map(|ns| ns.0).unwrap_or_default();

        let mut buffer = Vec::with_capacity(16 + namespace.len());
        buffer.extend_from_slice(self.id.as_bytes());
        buffer.extend_from_slice(namespace.as_bytes());

        buffer
    }

    pub fn from_wire_encoding(mut bytes: Vec<u8>) -> Result<Self, InvalidCookie> {
        // check length early to avoid panic during slicing
        if bytes.len() < 16 {
            return Err(InvalidCookie);
        }

        let namespace = bytes.split_off(16);
        let namespace = if namespace.is_empty() {
            None
        } else {
            Some(
                Namespace::new(String::from_utf8(namespace).map_err(|_| InvalidCookie)?)
                    .map_err(|_| InvalidCookie)?,
            )
        };

        let bytes = <[u8; 16]>::try_from(bytes).map_err(|_| InvalidCookie)?;
        let uuid = Uuid::from_bytes(bytes);

        Ok(Self {
            id: uuid,
            namespace,
        })
    }

    pub fn namespace(&self) -> Option<&Namespace> {
        self.namespace.as_ref()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("The cookie was malformed")]
pub struct InvalidCookie;

#[derive(Debug, Clone)]
pub struct NewRegistration {
    pub namespace: Namespace,
    pub record: PeerRecord,
    pub ttl: Option<u64>,
}

impl NewRegistration {
    pub fn new(namespace: Namespace, record: PeerRecord, ttl: Option<Ttl>) -> Self {
        Self {
            namespace,
            record,
            ttl,
        }
    }

    pub fn effective_ttl(&self) -> Ttl {
        self.ttl.unwrap_or(DEFAULT_TTL)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Registration {
    pub namespace: Namespace,
    pub record: PeerRecord,
    pub ttl: Ttl,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ErrorCode {
    InvalidNamespace,
    InvalidSignedPeerRecord,
    InvalidTtl,
    InvalidCookie,
    NotAuthorized,
    InternalError,
    Unavailable,
}

pub struct RendezvousCodec {
    /// Codec to encode/decode the Unsigned varint length prefix of the frames.
    length_codec: UviBytes,
}

impl Default for RendezvousCodec {
    fn default() -> Self {
        let mut length_codec = UviBytes::default();
        length_codec.set_max_len(1024 * 1024); // 1MB

        Self { length_codec }
    }
}

impl Encoder for RendezvousCodec {
    type Item = Message;
    type Error = Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        use prost::Message;

        let message = wire::Message::from(item);

        let mut buf = Vec::with_capacity(message.encoded_len());

        message
            .encode(&mut buf)
            .expect("Buffer has sufficient capacity");

        // length prefix the protobuf message, ensuring the max limit is not hit
        self.length_codec.encode(Bytes::from(buf), dst)?;

        Ok(())
    }
}

impl Decoder for RendezvousCodec {
    type Item = Message;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        use prost::Message;

        let message = match self.length_codec.decode(src)? {
            Some(p) => p,
            None => return Ok(None),
        };

        let message = wire::Message::decode(message)?;

        Ok(Some(message.try_into()?))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to encode message as bytes")]
    Encode(#[from] prost::EncodeError),
    #[error("Failed to decode message from bytes")]
    Decode(#[from] prost::DecodeError),
    #[error("Failed to read/write")]
    Io(#[from] std::io::Error),
    #[error("Failed to convert wire message to internal data model")]
    ConversionError(#[from] ConversionError),
}

impl From<Message> for wire::Message {
    fn from(message: Message) -> Self {
        use wire::message::*;

        match message {
            Message::Register(NewRegistration {
                namespace,
                record,
                ttl,
            }) => wire::Message {
                r#type: Some(MessageType::Register.into()),
                register: Some(Register {
                    ns: Some(namespace.into()),
                    ttl,
                    signed_peer_record: Some(
                        record.into_signed_envelope().into_protobuf_encoding(),
                    ),
                }),
                register_response: None,
                unregister: None,
                discover: None,
                discover_response: None,
            },
            Message::RegisterResponse(Ok(ttl)) => wire::Message {
                r#type: Some(MessageType::RegisterResponse.into()),
                register_response: Some(RegisterResponse {
                    status: Some(ResponseStatus::Ok.into()),
                    status_text: None,
                    ttl: Some(ttl),
                }),
                register: None,
                discover: None,
                unregister: None,
                discover_response: None,
            },
            Message::RegisterResponse(Err(error)) => wire::Message {
                r#type: Some(MessageType::RegisterResponse.into()),
                register_response: Some(RegisterResponse {
                    status: Some(ResponseStatus::from(error).into()),
                    status_text: None,
                    ttl: None,
                }),
                register: None,
                discover: None,
                unregister: None,
                discover_response: None,
            },
            Message::Unregister(namespace) => wire::Message {
                r#type: Some(MessageType::Unregister.into()),
                unregister: Some(Unregister {
                    ns: Some(namespace.into()),
                    id: None,
                }),
                register: None,
                register_response: None,
                discover: None,
                discover_response: None,
            },
            Message::Discover {
                namespace,
                cookie,
                limit,
            } => wire::Message {
                r#type: Some(MessageType::Discover.into()),
                discover: Some(Discover {
                    ns: namespace.map(|ns| ns.into()),
                    cookie: cookie.map(|cookie| cookie.into_wire_encoding()),
                    limit,
                }),
                register: None,
                register_response: None,
                unregister: None,
                discover_response: None,
            },
            Message::DiscoverResponse(Ok((registrations, cookie))) => wire::Message {
                r#type: Some(MessageType::DiscoverResponse.into()),
                discover_response: Some(DiscoverResponse {
                    registrations: registrations
                        .into_iter()
                        .map(|reggo| Register {
                            ns: Some(reggo.namespace.into()),
                            ttl: Some(reggo.ttl),
                            signed_peer_record: Some(
                                reggo.record.into_signed_envelope().into_protobuf_encoding(),
                            ),
                        })
                        .collect(),
                    status: Some(ResponseStatus::Ok.into()),
                    status_text: None,
                    cookie: Some(cookie.into_wire_encoding()),
                }),
                register: None,
                discover: None,
                unregister: None,
                register_response: None,
            },
            Message::DiscoverResponse(Err(error)) => wire::Message {
                r#type: Some(MessageType::DiscoverResponse.into()),
                discover_response: Some(DiscoverResponse {
                    registrations: Vec::new(),
                    status: Some(ResponseStatus::from(error).into()),
                    status_text: None,
                    cookie: None,
                }),
                register: None,
                discover: None,
                unregister: None,
                register_response: None,
            },
        }
    }
}

impl TryFrom<wire::Message> for Message {
    type Error = ConversionError;

    fn try_from(message: wire::Message) -> Result<Self, Self::Error> {
        use wire::message::*;

        let message = match message {
            wire::Message {
                r#type: Some(0),
                register:
                    Some(Register {
                        ns,
                        ttl,
                        signed_peer_record: Some(signed_peer_record),
                    }),
                ..
            } => Message::Register(NewRegistration {
                namespace: ns
                    .map(Namespace::new)
                    .transpose()?
                    .ok_or(ConversionError::MissingNamespace)?,
                ttl,
                record: PeerRecord::from_signed_envelope(SignedEnvelope::from_protobuf_encoding(
                    &signed_peer_record,
                )?)?,
            }),
            wire::Message {
                r#type: Some(1),
                register_response:
                    Some(RegisterResponse {
                        status: Some(0),
                        ttl,
                        ..
                    }),
                ..
            } => Message::RegisterResponse(Ok(ttl.ok_or(ConversionError::MissingTtl)?)),
            wire::Message {
                r#type: Some(3),
                discover: Some(Discover { ns, limit, cookie }),
                ..
            } => Message::Discover {
                namespace: ns.map(Namespace::new).transpose()?,
                cookie: cookie.map(Cookie::from_wire_encoding).transpose()?,
                limit,
            },
            wire::Message {
                r#type: Some(4),
                discover_response:
                    Some(DiscoverResponse {
                        registrations,
                        status: Some(0),
                        cookie: Some(cookie),
                        ..
                    }),
                ..
            } => {
                let registrations = registrations
                    .into_iter()
                    .map(|reggo| {
                        Ok(Registration {
                            namespace: reggo
                                .ns
                                .map(Namespace::new)
                                .transpose()?
                                .ok_or(ConversionError::MissingNamespace)?,
                            record: PeerRecord::from_signed_envelope(
                                SignedEnvelope::from_protobuf_encoding(
                                    &reggo
                                        .signed_peer_record
                                        .ok_or(ConversionError::MissingSignedPeerRecord)?,
                                )?,
                            )?,
                            ttl: reggo.ttl.ok_or(ConversionError::MissingTtl)?,
                        })
                    })
                    .collect::<Result<Vec<_>, ConversionError>>()?;
                let cookie = Cookie::from_wire_encoding(cookie)?;

                Message::DiscoverResponse(Ok((registrations, cookie)))
            }
            wire::Message {
                r#type: Some(1),
                register_response:
                    Some(RegisterResponse {
                        status: Some(error_code),
                        ..
                    }),
                ..
            } => {
                let error_code = wire::message::ResponseStatus::from_i32(error_code)
                    .ok_or(ConversionError::BadStatusCode)?
                    .try_into()?;
                Message::RegisterResponse(Err(error_code))
            }
            wire::Message {
                r#type: Some(2),
                unregister: Some(Unregister { ns, .. }),
                ..
            } => Message::Unregister(
                ns.map(Namespace::new)
                    .transpose()?
                    .ok_or(ConversionError::MissingNamespace)?,
            ),
            wire::Message {
                r#type: Some(4),
                discover_response:
                    Some(DiscoverResponse {
                        status: Some(error_code),
                        ..
                    }),
                ..
            } => {
                let error = wire::message::ResponseStatus::from_i32(error_code)
                    .ok_or(ConversionError::BadStatusCode)?
                    .try_into()?;
                Message::DiscoverResponse(Err(error))
            }
            _ => return Err(ConversionError::InconsistentWireMessage),
        };

        Ok(message)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConversionError {
    #[error("The wire message is consistent")]
    InconsistentWireMessage,
    #[error("Missing namespace field")]
    MissingNamespace,
    #[error("Invalid namespace")]
    InvalidNamespace(#[from] NamespaceTooLong),
    #[error("Missing signed peer record field")]
    MissingSignedPeerRecord,
    #[error("Missing TTL field")]
    MissingTtl,
    #[error("Bad status code")]
    BadStatusCode,
    #[error("Failed to decode signed envelope")]
    BadSignedEnvelope(#[from] signed_envelope::DecodingError),
    #[error("Failed to decode envelope as signed peer record")]
    BadSignedPeerRecord(#[from] peer_record::FromEnvelopeError),
    #[error(transparent)]
    BadCookie(#[from] InvalidCookie),
    #[error("The requested PoW difficulty is out of range")]
    PoWDifficultyOutOfRange,
    #[error("The provided PoW hash is not 32 bytes long")]
    BadPoWHash,
}

impl ConversionError {
    pub fn to_error_code(&self) -> ErrorCode {
        match self {
            ConversionError::MissingNamespace => ErrorCode::InvalidNamespace,
            ConversionError::MissingSignedPeerRecord => ErrorCode::InvalidSignedPeerRecord,
            ConversionError::BadSignedEnvelope(_) => ErrorCode::InvalidSignedPeerRecord,
            ConversionError::BadSignedPeerRecord(_) => ErrorCode::InvalidSignedPeerRecord,
            ConversionError::BadCookie(_) => ErrorCode::InvalidCookie,
            ConversionError::MissingTtl => ErrorCode::InvalidTtl,
            ConversionError::InconsistentWireMessage => ErrorCode::InternalError,
            ConversionError::BadStatusCode => ErrorCode::InternalError,
            ConversionError::PoWDifficultyOutOfRange => ErrorCode::InternalError,
            ConversionError::BadPoWHash => ErrorCode::InternalError,
            ConversionError::InvalidNamespace(_) => ErrorCode::InvalidNamespace,
        }
    }
}

impl TryFrom<wire::message::ResponseStatus> for ErrorCode {
    type Error = UnmappableStatusCode;

    fn try_from(value: wire::message::ResponseStatus) -> Result<Self, Self::Error> {
        use wire::message::ResponseStatus::*;

        let code = match value {
            Ok => return Err(UnmappableStatusCode(value)),
            EInvalidNamespace => ErrorCode::InvalidNamespace,
            EInvalidSignedPeerRecord => ErrorCode::InvalidSignedPeerRecord,
            EInvalidTtl => ErrorCode::InvalidTtl,
            EInvalidCookie => ErrorCode::InvalidCookie,
            ENotAuthorized => ErrorCode::NotAuthorized,
            EInternalError => ErrorCode::InternalError,
            EUnavailable => ErrorCode::Unavailable,
        };

        Result::Ok(code)
    }
}

impl From<ErrorCode> for wire::message::ResponseStatus {
    fn from(error_code: ErrorCode) -> Self {
        use wire::message::ResponseStatus::*;

        match error_code {
            ErrorCode::InvalidNamespace => EInvalidNamespace,
            ErrorCode::InvalidSignedPeerRecord => EInvalidSignedPeerRecord,
            ErrorCode::InvalidTtl => EInvalidTtl,
            ErrorCode::InvalidCookie => EInvalidCookie,
            ErrorCode::NotAuthorized => ENotAuthorized,
            ErrorCode::InternalError => EInternalError,
            ErrorCode::Unavailable => EUnavailable,
        }
    }
}

impl From<UnmappableStatusCode> for ConversionError {
    fn from(_: UnmappableStatusCode) -> Self {
        ConversionError::InconsistentWireMessage
    }
}

#[derive(Debug, thiserror::Error)]
#[error("The response code ({0:?}) cannot be mapped to our ErrorCode enum")]
pub struct UnmappableStatusCode(wire::message::ResponseStatus);

mod wire {
    include!(concat!(env!("OUT_DIR"), "/rendezvous.pb.rs"));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cookie_wire_encoding_roundtrip() {
        let cookie = Cookie::for_namespace(Namespace::from_static("foo"));

        let bytes = cookie.clone().into_wire_encoding();
        let parsed = Cookie::from_wire_encoding(bytes).unwrap();

        assert_eq!(parsed, cookie);
    }

    #[test]
    fn cookie_wire_encoding_length() {
        let cookie = Cookie::for_namespace(Namespace::from_static("foo"));

        let bytes = cookie.into_wire_encoding();

        assert_eq!(bytes.len(), 16 + 3)
    }
}
