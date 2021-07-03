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

use crate::v2::message_proto::{hop_message, HopMessage, Limit, Reservation, Status};
use crate::v2::protocol::{HOP_PROTOCOL_NAME, MAX_MESSAGE_SIZE};
use asynchronous_codec::{Framed, FramedParts};
use bytes::{Bytes, BytesMut};
use futures::{future::BoxFuture, prelude::*};
use libp2p_core::{upgrade, Multiaddr, PeerId};
use libp2p_swarm::NegotiatedSubstream;
use prost::Message;
use std::convert::TryInto;
use std::io::Cursor;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{error, fmt, iter};
use unsigned_varint::codec::UviBytes;

pub struct Upgrade {
    pub reservation_duration: Duration,
    pub max_circuit_duration: Duration,
    pub max_circuit_bytes: u64,
}

impl upgrade::UpgradeInfo for Upgrade {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(HOP_PROTOCOL_NAME)
    }
}

impl upgrade::InboundUpgrade<NegotiatedSubstream> for Upgrade {
    type Output = Req;
    type Error = UpgradeError;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(self, substream: NegotiatedSubstream, _: Self::Info) -> Self::Future {
        let mut codec = UviBytes::<bytes::Bytes>::default();
        codec.set_max_len(MAX_MESSAGE_SIZE);
        let mut substream = Framed::new(substream, codec);

        async move {
            let msg: bytes::BytesMut = substream
                .next()
                .await
                .ok_or(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, ""))??;

            let HopMessage {
                r#type,
                peer,
                reservation: _,
                limit: _,
                status: _,
            } = HopMessage::decode(Cursor::new(msg))?;

            let r#type = hop_message::Type::from_i32(r#type).ok_or(UpgradeError::ParseTypeField)?;
            match r#type {
                hop_message::Type::Reserve => Ok(Req::Reserve(ReservationReq {
                    substream,
                    reservation_duration: self.reservation_duration,
                    max_circuit_duration: self.max_circuit_duration,
                    max_circuit_bytes: self.max_circuit_bytes,
                })),
                hop_message::Type::Connect => {
                    let dst = PeerId::from_bytes(&peer.ok_or(UpgradeError::MissingPeer)?.id)
                        .map_err(|_| UpgradeError::ParsePeerId)?;
                    Ok(Req::Connect(CircuitReq { substream, dst }))
                }
                hop_message::Type::Status => Err(UpgradeError::UnexpectedTypeStatus),
            }
        }
        .boxed()
    }
}

#[derive(Debug)]
pub enum UpgradeError {
    Decode(prost::DecodeError),
    Io(std::io::Error),
    ParseTypeField,
    ParsePeerId,
    MissingPeer,
    UnexpectedTypeStatus,
}

impl From<std::io::Error> for UpgradeError {
    fn from(e: std::io::Error) -> Self {
        UpgradeError::Io(e)
    }
}

impl From<prost::DecodeError> for UpgradeError {
    fn from(e: prost::DecodeError) -> Self {
        UpgradeError::Decode(e)
    }
}

impl fmt::Display for UpgradeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UpgradeError::Decode(e) => {
                write!(f, "Failed to decode response: {}.", e)
            }
            UpgradeError::Io(e) => {
                write!(f, "Io error {}", e)
            }
            UpgradeError::ParseTypeField => {
                write!(f, "Failed to parse response type field.")
            }
            UpgradeError::ParsePeerId => {
                write!(f, "Failed to parse peer id.")
            }
            UpgradeError::MissingPeer => {
                write!(f, "Expected 'peer' field to be set.")
            }
            UpgradeError::UnexpectedTypeStatus => {
                write!(f, "Unexpected message type 'status'")
            }
        }
    }
}

impl error::Error for UpgradeError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            UpgradeError::Decode(e) => Some(e),
            UpgradeError::Io(e) => Some(e),
            UpgradeError::ParseTypeField => None,
            UpgradeError::ParsePeerId => None,
            UpgradeError::MissingPeer => None,
            UpgradeError::UnexpectedTypeStatus => None,
        }
    }
}

pub enum Req {
    Reserve(ReservationReq),
    Connect(CircuitReq),
}

pub struct ReservationReq {
    substream: Framed<NegotiatedSubstream, UviBytes>,
    reservation_duration: Duration,
    max_circuit_duration: Duration,
    max_circuit_bytes: u64,
}

impl ReservationReq {
    pub async fn accept(self, addrs: Vec<Multiaddr>) -> Result<(), std::io::Error> {
        let msg = HopMessage {
            r#type: hop_message::Type::Status.into(),
            peer: None,
            reservation: Some(Reservation {
                addrs: addrs.into_iter().map(|a| a.to_vec()).collect(),
                expire: Some(
                    (SystemTime::now() + self.reservation_duration)
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                ),
                // TODO: Does this need to be set?
                voucher: None,
            }),
            limit: Some(Limit {
                // TODO: Handle unwrap
                duration: Some(self.max_circuit_duration.as_secs().try_into().unwrap()),
                data: Some(self.max_circuit_bytes),
            }),
            status: Some(Status::Ok.into()),
        };

        self.send(msg).await
    }

    pub async fn deny(self, status: Status) -> Result<(), std::io::Error> {
        let msg = HopMessage {
            r#type: hop_message::Type::Status.into(),
            peer: None,
            reservation: None,
            limit: None,
            status: Some(status.into()),
        };

        self.send(msg).await
    }

    async fn send(mut self, msg: HopMessage) -> Result<(), std::io::Error> {
        let mut msg_bytes = BytesMut::new();
        msg.encode(&mut msg_bytes)
            // TODO: Sure panicing is safe here?
            .expect("all the mandatory fields are always filled; QED");
        self.substream.send(msg_bytes.freeze()).await?;
        self.substream.flush().await?;
        self.substream.close().await?;

        Ok(())
    }
}

pub struct CircuitReq {
    dst: PeerId,
    substream: Framed<NegotiatedSubstream, UviBytes>,
}

impl CircuitReq {
    pub fn dst(&self) -> PeerId {
        self.dst
    }

    pub async fn accept(mut self) -> Result<(NegotiatedSubstream, Bytes), std::io::Error> {
        let msg = HopMessage {
            r#type: hop_message::Type::Status.into(),
            peer: None,
            reservation: None,
            limit: None,
            status: Some(Status::Ok.into()),
        };

        self.send(msg).await?;

        let FramedParts {
            io,
            read_buffer,
            write_buffer,
            ..
        } = self.substream.into_parts();
        assert!(
            write_buffer.is_empty(),
            "Expect a flushed Framed to have an empty write buffer."
        );

        Ok((io, read_buffer.freeze()))
    }

    pub async fn deny(mut self, status: Status) -> Result<(), std::io::Error> {
        let msg = HopMessage {
            r#type: hop_message::Type::Status.into(),
            peer: None,
            reservation: None,
            limit: None,
            status: Some(status.into()),
        };
        self.send(msg).await?;
        self.substream.close().await
    }

    async fn send(&mut self, msg: HopMessage) -> Result<(), std::io::Error> {
        let mut msg_bytes = BytesMut::new();
        msg.encode(&mut msg_bytes)
            // TODO: Sure panicing is safe here?
            .expect("all the mandatory fields are always filled; QED");
        self.substream.send(msg_bytes.freeze()).await?;
        self.substream.flush().await?;

        Ok(())
    }
}
