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

use crate::message_proto::{circuit_relay, CircuitRelay};
use crate::protocol::{MAX_ACCEPTED_MESSAGE_LEN, PROTOCOL_NAME};
use asynchronous_codec::{Framed, FramedParts};
use futures::channel::oneshot;
use futures::future::BoxFuture;
use futures::prelude::*;
use libp2p_core::{upgrade, Multiaddr, PeerId};
use libp2p_swarm::NegotiatedSubstream;
use prost::Message;
use std::{error, fmt, iter};
use unsigned_varint::codec::UviBytes;

/// Ask a remote to act as a relay.
///
/// If we take a situation where a *source* wants to talk to a *destination* through a *relay*,
/// this struct is the message that the *source* sends to the *relay* at initialization. The
/// parameters passed to `OutgoingRelayReq::new()` are the information of the *destination*
/// (not the information of the *relay*).
///
/// The upgrade should be performed on a substream to the *relay*.
///
/// If the upgrade succeeds, the substream is returned and is now a brand new connection pointing
/// to the *destination*.
pub struct OutgoingRelayReq {
    src_id: PeerId,
    dst_id: PeerId,
    dst_address: Option<Multiaddr>,
}

impl OutgoingRelayReq {
    /// Builds a request for the target to act as a relay to a third party.
    pub fn new(src_id: PeerId, dst_id: PeerId, dst_address: Option<Multiaddr>) -> Self {
        OutgoingRelayReq {
            src_id,
            dst_id,
            dst_address,
        }
    }
}

impl upgrade::UpgradeInfo for OutgoingRelayReq {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(PROTOCOL_NAME)
    }
}

impl upgrade::OutboundUpgrade<NegotiatedSubstream> for OutgoingRelayReq {
    type Output = (super::Connection, oneshot::Receiver<()>);
    type Error = OutgoingRelayReqError;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_outbound(self, substream: NegotiatedSubstream, _: Self::Info) -> Self::Future {
        let OutgoingRelayReq {
            src_id,
            dst_id,
            dst_address,
        } = self;

        let message = CircuitRelay {
            r#type: Some(circuit_relay::Type::Hop.into()),
            src_peer: Some(circuit_relay::Peer {
                id: src_id.to_bytes(),
                addrs: vec![],
            }),
            dst_peer: Some(circuit_relay::Peer {
                id: dst_id.to_bytes(),
                addrs: vec![dst_address.unwrap_or(Multiaddr::empty()).to_vec()],
            }),
            code: None,
        };
        let mut encoded = Vec::new();
        message
            .encode(&mut encoded)
            .expect("all the mandatory fields are always filled; QED");

        let mut codec = UviBytes::default();
        codec.set_max_len(MAX_ACCEPTED_MESSAGE_LEN);

        let mut substream = Framed::new(substream, codec);

        async move {
            substream.send(std::io::Cursor::new(encoded)).await?;
            let msg =
                substream
                    .next()
                    .await
                    .ok_or(OutgoingRelayReqError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "",
                    )))??;

            let msg = std::io::Cursor::new(msg);
            let CircuitRelay {
                r#type,
                src_peer,
                dst_peer,
                code,
            } = CircuitRelay::decode(msg)?;

            match r#type
                .map(circuit_relay::Type::from_i32)
                .flatten()
                .ok_or(OutgoingRelayReqError::ParseTypeField)?
            {
                circuit_relay::Type::Status => {}
                s => return Err(OutgoingRelayReqError::ExpectedStatusType(s)),
            }

            match code
                .map(circuit_relay::Status::from_i32)
                .flatten()
                .ok_or(OutgoingRelayReqError::ParseStatusField)?
            {
                circuit_relay::Status::Success => {}
                e => return Err(OutgoingRelayReqError::ExpectedSuccessStatus(e)),
            }

            if src_peer.is_some() {
                return Err(OutgoingRelayReqError::UnexpectedSrcPeerWithStatusType);
            }
            if dst_peer.is_some() {
                return Err(OutgoingRelayReqError::UnexpectedDstPeerWithStatusType);
            }

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

            let (tx, rx) = oneshot::channel();

            Ok((super::Connection::new(read_buffer.freeze(), io, tx), rx))
        }
        .boxed()
    }
}

#[derive(Debug)]
pub enum OutgoingRelayReqError {
    Decode(prost::DecodeError),
    Io(std::io::Error),
    ParseTypeField,
    ParseStatusField,
    ExpectedStatusType(circuit_relay::Type),
    UnexpectedSrcPeerWithStatusType,
    UnexpectedDstPeerWithStatusType,
    ExpectedSuccessStatus(circuit_relay::Status),
}

impl From<std::io::Error> for OutgoingRelayReqError {
    fn from(e: std::io::Error) -> Self {
        OutgoingRelayReqError::Io(e)
    }
}

impl From<prost::DecodeError> for OutgoingRelayReqError {
    fn from(e: prost::DecodeError) -> Self {
        OutgoingRelayReqError::Decode(e)
    }
}

impl fmt::Display for OutgoingRelayReqError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutgoingRelayReqError::Decode(e) => {
                write!(f, "Failed to decode response: {}.", e)
            }
            OutgoingRelayReqError::Io(e) => {
                write!(f, "Io error {}", e)
            }
            OutgoingRelayReqError::ParseTypeField => {
                write!(f, "Failed to parse response type field.")
            }
            OutgoingRelayReqError::ParseStatusField => {
                write!(f, "Failed to parse response status field.")
            }
            OutgoingRelayReqError::ExpectedStatusType(t) => {
                write!(f, "Expected status message type, but got {:?}", t)
            }
            OutgoingRelayReqError::UnexpectedSrcPeerWithStatusType => {
                write!(f, "Unexpected source peer with status type.")
            }
            OutgoingRelayReqError::UnexpectedDstPeerWithStatusType => {
                write!(f, "Unexpected destination peer with status type.")
            }
            OutgoingRelayReqError::ExpectedSuccessStatus(s) => {
                write!(f, "Expected success status but got {:?}", s)
            }
        }
    }
}

impl error::Error for OutgoingRelayReqError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            OutgoingRelayReqError::Decode(e) => Some(e),
            OutgoingRelayReqError::Io(e) => Some(e),
            OutgoingRelayReqError::ParseTypeField => None,
            OutgoingRelayReqError::ParseStatusField => None,
            OutgoingRelayReqError::ExpectedStatusType(_) => None,
            OutgoingRelayReqError::UnexpectedSrcPeerWithStatusType => None,
            OutgoingRelayReqError::UnexpectedDstPeerWithStatusType => None,
            OutgoingRelayReqError::ExpectedSuccessStatus(_) => None,
        }
    }
}
