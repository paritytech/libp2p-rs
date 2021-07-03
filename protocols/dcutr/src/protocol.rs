// Copyright 2021 Protocol Labs.
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

use crate::message_proto::{hole_punch, HolePunch};
use asynchronous_codec::{Framed, FramedParts};
use bytes::{Bytes, BytesMut};
use futures::{future::BoxFuture, prelude::*};
use libp2p_core::{upgrade, Multiaddr};
use libp2p_swarm::NegotiatedSubstream;
use prost::Message;
use std::convert::{TryFrom, TryInto};
use std::error;
use std::fmt;
use std::io::Cursor;
use std::iter;
use unsigned_varint::codec::UviBytes;
use std::time::{Duration, Instant};
use futures_timer::Delay;

const PROTOCOL_NAME: &[u8; 15] = b"/libp2p/connect";

// TODO: Should this be split up in two files? Inbound and outbound?

pub struct OutboundUpgrade {
    obs_addrs: Vec<Multiaddr>,
}

impl upgrade::UpgradeInfo for OutboundUpgrade {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(PROTOCOL_NAME)
    }
}

impl OutboundUpgrade {
    pub fn new(obs_addrs: Vec<Multiaddr>) -> Self {
        Self { obs_addrs }
    }
}

impl upgrade::OutboundUpgrade<NegotiatedSubstream> for OutboundUpgrade {
    type Output = Connect;
    type Error = OutboundUpgradeError;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_outbound(self, substream: NegotiatedSubstream, _: Self::Info) -> Self::Future {
        let msg = HolePunch {
            r#type: Some(hole_punch::Type::Connect.into()),
            obs_addrs: self.obs_addrs.into_iter().map(|a| a.to_vec()).collect(),
        };

        let mut encoded_msg = BytesMut::new();
        msg.encode(&mut encoded_msg)
            // TODO: Double check. Safe to panic here?
            .expect("all the mandatory fields are always filled; QED");

        let codec = UviBytes::default();
        // TODO: Needed?
        // codec.set_max_len(MAX_MESSAGE_SIZE);
        let mut substream = Framed::new(substream, codec);

        async move {
            substream.send(encoded_msg.freeze()).await?;

            // TODO: Should we do this before or after flushing?
            let sent_time = Instant::now();

            let msg: bytes::BytesMut = substream
                .next()
                .await
                .ok_or(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, ""))??;

            let rtt = sent_time.elapsed();

            let HolePunch { r#type, obs_addrs } = HolePunch::decode(Cursor::new(msg))?;

            // TODO: Unwrap safe here?
            let r#type = hole_punch::Type::from_i32(r#type.unwrap())
                .ok_or(OutboundUpgradeError::ParseTypeField)?;
            match r#type {
                hole_punch::Type::Connect => {}
                hole_punch::Type::Sync => return Err(OutboundUpgradeError::UnexpectedTypeSync),
            }

            let obs_addrs = if obs_addrs.is_empty() {
                return Err(OutboundUpgradeError::NoAddresses);
            } else {
                obs_addrs
                    .into_iter()
                    .map(TryFrom::try_from)
                    .collect::<Result<Vec<Multiaddr>, _>>()
                    .map_err(|_| OutboundUpgradeError::InvalidAddrs)?
            };

            let msg = HolePunch {
                r#type: Some(hole_punch::Type::Sync.into()),
                obs_addrs: vec![],
            };

            let mut encoded_msg = BytesMut::new();
            msg.encode(&mut encoded_msg)
            // TODO: Double check. Safe to panic here?
                .expect("all the mandatory fields are always filled; QED");

            substream.send(encoded_msg.freeze()).await?;

            Delay::new(rtt / 2).await;

            Ok(Connect { obs_addrs })
        }
        .boxed()
    }
}

#[derive(Debug)]
pub enum OutboundUpgradeError {
    Decode(prost::DecodeError),
    Io(std::io::Error),
    MissingStatusField,
    MissingReservationField,
    NoAddresses,
    InvalidReservationExpiration,
    InvalidAddrs,
    ParseTypeField,
    UnexpectedTypeConnect,
    UnexpectedTypeSync,
    ParseStatusField,
}

impl From<std::io::Error> for OutboundUpgradeError {
    fn from(e: std::io::Error) -> Self {
        OutboundUpgradeError::Io(e)
    }
}

impl From<prost::DecodeError> for OutboundUpgradeError {
    fn from(e: prost::DecodeError) -> Self {
        OutboundUpgradeError::Decode(e)
    }
}

impl fmt::Display for OutboundUpgradeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutboundUpgradeError::Decode(e) => {
                write!(f, "Failed to decode response: {}.", e)
            }
            OutboundUpgradeError::Io(e) => {
                write!(f, "Io error {}", e)
            }
            OutboundUpgradeError::MissingStatusField => {
                write!(f, "Expected 'status' field to be set.")
            }
            OutboundUpgradeError::MissingReservationField => {
                write!(f, "Expected 'reservation' field to be set.")
            }
            OutboundUpgradeError::NoAddresses => {
                write!(f, "Expected at least one address in reservation.")
            }
            OutboundUpgradeError::InvalidReservationExpiration => {
                write!(f, "Invalid expiration timestamp in reservation.")
            }
            OutboundUpgradeError::InvalidAddrs => {
                write!(f, "Invalid addresses in reservation.")
            }
            OutboundUpgradeError::ParseTypeField => {
                write!(f, "Failed to parse response type field.")
            }
            OutboundUpgradeError::UnexpectedTypeConnect => {
                write!(f, "Unexpected message type 'connect'")
            }
            OutboundUpgradeError::UnexpectedTypeSync => {
                write!(f, "Unexpected message type 'sync'")
            }
            OutboundUpgradeError::ParseStatusField => {
                write!(f, "Failed to parse response type field.")
            }
        }
    }
}

impl error::Error for OutboundUpgradeError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            OutboundUpgradeError::Decode(e) => Some(e),
            OutboundUpgradeError::Io(e) => Some(e),
            OutboundUpgradeError::MissingStatusField => None,
            OutboundUpgradeError::MissingReservationField => None,
            OutboundUpgradeError::NoAddresses => None,
            OutboundUpgradeError::InvalidReservationExpiration => None,
            OutboundUpgradeError::InvalidAddrs => None,
            OutboundUpgradeError::ParseTypeField => None,
            OutboundUpgradeError::UnexpectedTypeConnect => None,
            OutboundUpgradeError::UnexpectedTypeSync => None,
            OutboundUpgradeError::ParseStatusField => None,
        }
    }
}

pub struct InboundUpgrade {}

impl upgrade::UpgradeInfo for InboundUpgrade {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(PROTOCOL_NAME)
    }
}

impl upgrade::InboundUpgrade<NegotiatedSubstream> for InboundUpgrade {
    type Output = InboundConnect;
    type Error = InboundUpgradeError;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(self, substream: NegotiatedSubstream, _: Self::Info) -> Self::Future {
        let codec = UviBytes::default();
        // TODO: Needed?
        // codec.set_max_len(MAX_MESSAGE_SIZE);
        let mut substream = Framed::new(substream, codec);

        async move {
            let msg: bytes::BytesMut = substream
                .next()
                .await
                .ok_or(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, ""))??;

            let HolePunch { r#type, obs_addrs } = HolePunch::decode(Cursor::new(msg))?;

            let obs_addrs = if obs_addrs.is_empty() {
                return Err(InboundUpgradeError::NoAddresses);
            } else {
                obs_addrs
                    .into_iter()
                    .map(TryFrom::try_from)
                    .collect::<Result<Vec<Multiaddr>, _>>()
                    .map_err(|_| InboundUpgradeError::InvalidAddrs)?
            };

            let r#type = hole_punch::Type::from_i32(r#type.unwrap())
                .ok_or(InboundUpgradeError::ParseTypeField)?;

            match r#type {
                hole_punch::Type::Connect => {}
                hole_punch::Type::Sync => return Err(InboundUpgradeError::UnexpectedTypeSync),
            }

            Ok(InboundConnect {
                substream,
                remote_obs_addrs: obs_addrs,
            })
        }
        .boxed()
    }
}

// TODO: Should we rename this to OutboundConnect?
pub struct Connect {
    pub obs_addrs: Vec<Multiaddr>,
}

pub struct InboundConnect {
    substream: Framed<NegotiatedSubstream, UviBytes>,
    remote_obs_addrs: Vec<Multiaddr>,
}

impl InboundConnect {
    // TODO: Should this really use InboundUpgradeError?
    pub async fn accept(
        mut self,
        local_obs_addrs: Vec<Multiaddr>,
    ) -> Result<Vec<Multiaddr>, InboundUpgradeError> {
        let msg = HolePunch {
            r#type: Some(hole_punch::Type::Connect.into()),
            obs_addrs: local_obs_addrs.into_iter().map(|a| a.to_vec()).collect(),
        };

        let mut encoded_msg = BytesMut::new();
        msg.encode(&mut encoded_msg)
            // TODO: Double check. Safe to panic here?
            .expect("all the mandatory fields are always filled; QED");

        self.substream.send(encoded_msg.freeze()).await?;
        let msg: bytes::BytesMut = self
            .substream
            .next()
            .await
            .ok_or(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, ""))??;

        let HolePunch { r#type, .. } = HolePunch::decode(Cursor::new(msg))?;

        // TODO: Unwrap safe here?
        let r#type = hole_punch::Type::from_i32(r#type.unwrap())
            .ok_or(InboundUpgradeError::ParseTypeField)?;
        match r#type {
            hole_punch::Type::Connect => return Err(InboundUpgradeError::UnexpectedTypeConnect),
            hole_punch::Type::Sync => {}
        }

        Ok(self.remote_obs_addrs)
    }
}

// TODO: Are all of these needed?
#[derive(Debug)]
pub enum InboundUpgradeError {
    Decode(prost::DecodeError),
    Io(std::io::Error),
    MissingStatusField,
    MissingReservationField,
    NoAddresses,
    InvalidReservationExpiration,
    InvalidAddrs,
    ParseTypeField,
    UnexpectedTypeConnect,
    UnexpectedTypeSync,
    ParseStatusField,
}

impl From<std::io::Error> for InboundUpgradeError {
    fn from(e: std::io::Error) -> Self {
        InboundUpgradeError::Io(e)
    }
}

impl From<prost::DecodeError> for InboundUpgradeError {
    fn from(e: prost::DecodeError) -> Self {
        InboundUpgradeError::Decode(e)
    }
}

impl fmt::Display for InboundUpgradeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InboundUpgradeError::Decode(e) => {
                write!(f, "Failed to decode response: {}.", e)
            }
            InboundUpgradeError::Io(e) => {
                write!(f, "Io error {}", e)
            }
            InboundUpgradeError::MissingStatusField => {
                write!(f, "Expected 'status' field to be set.")
            }
            InboundUpgradeError::MissingReservationField => {
                write!(f, "Expected 'reservation' field to be set.")
            }
            InboundUpgradeError::NoAddresses => {
                write!(f, "Expected at least one address in reservation.")
            }
            InboundUpgradeError::InvalidReservationExpiration => {
                write!(f, "Invalid expiration timestamp in reservation.")
            }
            InboundUpgradeError::InvalidAddrs => {
                write!(f, "Invalid addresses.")
            }
            InboundUpgradeError::ParseTypeField => {
                write!(f, "Failed to parse response type field.")
            }
            InboundUpgradeError::UnexpectedTypeConnect => {
                write!(f, "Unexpected message type 'connect'")
            }
            InboundUpgradeError::UnexpectedTypeSync => {
                write!(f, "Unexpected message type 'sync'")
            }
            InboundUpgradeError::ParseStatusField => {
                write!(f, "Failed to parse response type field.")
            }
        }
    }
}

impl error::Error for InboundUpgradeError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            InboundUpgradeError::Decode(e) => Some(e),
            InboundUpgradeError::Io(e) => Some(e),
            InboundUpgradeError::MissingStatusField => None,
            InboundUpgradeError::MissingReservationField => None,
            InboundUpgradeError::NoAddresses => None,
            InboundUpgradeError::InvalidReservationExpiration => None,
            InboundUpgradeError::InvalidAddrs => None,
            InboundUpgradeError::ParseTypeField => None,
            InboundUpgradeError::UnexpectedTypeConnect => None,
            InboundUpgradeError::UnexpectedTypeSync => None,
            InboundUpgradeError::ParseStatusField => None,
            InboundUpgradeError::InvalidAddrs => None,
        }
    }
}
