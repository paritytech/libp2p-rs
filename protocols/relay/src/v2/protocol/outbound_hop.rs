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

use crate::v2::message_proto::{hop_message, HopMessage, Peer, Status};
use crate::v2::protocol::{HOP_PROTOCOL_NAME, MAX_MESSAGE_SIZE};
use asynchronous_codec::{Framed, FramedParts};
use bytes::Bytes;
use futures::{future::BoxFuture, prelude::*};
use futures_timer::Delay;
use libp2p_core::{upgrade, Multiaddr, PeerId};
use libp2p_swarm::NegotiatedSubstream;
use prost::Message;
use std::convert::{TryFrom, TryInto};
use std::io::Cursor;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{error, fmt, iter};
use unsigned_varint::codec::UviBytes;

pub enum Upgrade {
    Reserve,
    Connect { dst_peer_id: PeerId },
}

impl upgrade::UpgradeInfo for Upgrade {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(HOP_PROTOCOL_NAME)
    }
}

impl upgrade::OutboundUpgrade<NegotiatedSubstream> for Upgrade {
    type Output = Output;
    type Error = UpgradeError;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_outbound(self, substream: NegotiatedSubstream, _: Self::Info) -> Self::Future {
        let msg = match self {
            Upgrade::Reserve => HopMessage {
                r#type: hop_message::Type::Reserve.into(),
                peer: None,
                reservation: None,
                limit: None,
                status: None,
            },
            Upgrade::Connect { dst_peer_id } => HopMessage {
                r#type: hop_message::Type::Connect.into(),
                peer: Some(Peer {
                    id: dst_peer_id.to_bytes(),
                    addrs: vec![],
                }),
                reservation: None,
                limit: None,
                status: None,
            },
        };

        let mut encoded_msg = Vec::new();
        msg.encode(&mut encoded_msg)
            .expect("all the mandatory fields are always filled; QED");

        let mut codec = UviBytes::default();
        codec.set_max_len(MAX_MESSAGE_SIZE);
        let mut substream = Framed::new(substream, codec);

        async move {
            substream.send(std::io::Cursor::new(encoded_msg)).await?;
            let msg: bytes::BytesMut = substream
                .next()
                .await
                .ok_or(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, ""))??;

            let HopMessage {
                r#type,
                peer: _,
                reservation,
                limit: _,
                status,
            } = HopMessage::decode(Cursor::new(msg))?;

            let r#type = hop_message::Type::from_i32(r#type).ok_or(UpgradeError::ParseTypeField)?;
            match r#type {
                hop_message::Type::Connect => return Err(UpgradeError::UnexpectedTypeConnect),
                hop_message::Type::Reserve => return Err(UpgradeError::UnexpectedTypeReserve),
                hop_message::Type::Status => {}
            }

            let status = Status::from_i32(status.ok_or(UpgradeError::MissingStatusField)?)
                .ok_or(UpgradeError::ParseStatusField)?;
            match status {
                Status::Ok => {}
                s => return Err(UpgradeError::UnexpectedStatus(s)),
            }

            let output = match self {
                Upgrade::Reserve => {
                    let reservation = reservation.ok_or(UpgradeError::MissingReservationField)?;

                    let addrs = if reservation.addrs.is_empty() {
                        return Err(UpgradeError::NoAddressesinReservation);
                    } else {
                        reservation
                            .addrs
                            .into_iter()
                            .map(TryFrom::try_from)
                            .collect::<Result<Vec<Multiaddr>, _>>()
                            .map_err(|_| UpgradeError::InvalidReservationAddrs)?
                    };

                    let renewal_timeout = if let Some(expires) = reservation.expire {
                        Some(
                            unix_timestamp_to_instant(expires)
                                .and_then(|instant| instant.checked_duration_since(Instant::now()))
                                .map(|duration| Delay::new(duration))
                                .ok_or(UpgradeError::InvalidReservationExpiration)?,
                        )
                    } else {
                        None
                    };

                    substream.close().await?;

                    Output::Reservation {
                        renewal_timeout,
                        addrs,
                    }
                }
                Upgrade::Connect { .. } => {
                    let FramedParts {
                        io,
                        read_buffer,
                        write_buffer,
                        ..
                    } = substream.into_parts();
                    assert!(
                        write_buffer.is_empty(),
                        "Expect a flushed Framed to have empty write buffer."
                    );

                    Output::Circuit {
                        substream: io,
                        read_buffer: read_buffer.freeze(),
                    }
                }
            };

            Ok(output)
        }
        .boxed()
    }
}

#[derive(Debug)]
pub enum UpgradeError {
    Decode(prost::DecodeError),
    Io(std::io::Error),
    MissingStatusField,
    MissingReservationField,
    NoAddressesinReservation,
    InvalidReservationExpiration,
    InvalidReservationAddrs,
    ParseTypeField,
    UnexpectedTypeConnect,
    UnexpectedTypeReserve,
    ParseStatusField,
    UnexpectedStatus(Status),
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
            UpgradeError::MissingStatusField => {
                write!(f, "Expected 'status' field to be set.")
            }
            UpgradeError::MissingReservationField => {
                write!(f, "Expected 'reservation' field to be set.")
            }
            UpgradeError::NoAddressesinReservation => {
                write!(f, "Expected at least one address in reservation.")
            }
            UpgradeError::InvalidReservationExpiration => {
                write!(f, "Invalid expiration timestamp in reservation.")
            }
            UpgradeError::InvalidReservationAddrs => {
                write!(f, "Invalid addresses in reservation.")
            }
            UpgradeError::ParseTypeField => {
                write!(f, "Failed to parse response type field.")
            }
            UpgradeError::UnexpectedTypeConnect => {
                write!(f, "Unexpected message type 'connect'")
            }
            UpgradeError::UnexpectedTypeReserve => {
                write!(f, "Unexpected message type 'reserve'")
            }
            UpgradeError::ParseStatusField => {
                write!(f, "Failed to parse response type field.")
            }
            UpgradeError::UnexpectedStatus(status) => {
                write!(f, "Unexpected message status '{:?}'", status)
            }
        }
    }
}

impl error::Error for UpgradeError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            UpgradeError::Decode(e) => Some(e),
            UpgradeError::Io(e) => Some(e),
            UpgradeError::MissingStatusField => None,
            UpgradeError::MissingReservationField => None,
            UpgradeError::NoAddressesinReservation => None,
            UpgradeError::InvalidReservationExpiration => None,
            UpgradeError::InvalidReservationAddrs => None,
            UpgradeError::ParseTypeField => None,
            UpgradeError::UnexpectedTypeConnect => None,
            UpgradeError::UnexpectedTypeReserve => None,
            UpgradeError::ParseStatusField => None,
            UpgradeError::UnexpectedStatus(_) => None,
        }
    }
}

fn unix_timestamp_to_instant(secs: u64) -> Option<Instant> {
    Instant::now().checked_add(Duration::from_secs(
        secs.checked_sub(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        )?
        .try_into()
        .ok()?,
    ))
}

pub enum Output {
    Reservation {
        renewal_timeout: Option<Delay>,
        addrs: Vec<Multiaddr>,
    },
    Circuit {
        substream: NegotiatedSubstream,
        read_buffer: Bytes,
    },
}
