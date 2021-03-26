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
use crate::protocol::{Peer, MAX_ACCEPTED_MESSAGE_LEN, PROTOCOL_NAME};
use asynchronous_codec::{Framed, FramedParts};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::prelude::*;
use libp2p_core::{upgrade, Multiaddr, PeerId};
use libp2p_swarm::NegotiatedSubstream;
use prost::Message;
use std::{fmt, error, iter};
use unsigned_varint::codec::UviBytes;

/// Ask the remote to become a destination. The upgrade succeeds if the remote accepts, and fails
/// if the remote refuses.
///
/// If we take a situation where a *source* wants to talk to a *destination* through a *relay*,
/// this struct is the message that the *relay* sends to the *destination* at initialization. The
/// parameters passed to `OutgoingDstReq::new()` are the information of the *source* and the
/// *destination* (not the information of the *relay*).
///
/// The upgrade should be performed on a substream to the *destination*.
///
/// If the upgrade succeeds, the substream is returned and we must link it with the data sent from
/// the source.
#[derive(Debug, Clone)]
pub struct OutgoingDstReq {
    /// The message to send to the destination. Pre-computed.
    message: Vec<u8>,
}

impl OutgoingDstReq {
    /// Creates a `OutgoingDstReq`. Must pass the parameters of the message.
    pub(crate) fn new(src_id: PeerId, src_addr: Multiaddr, dst_peer: Peer) -> Self {
        let message = CircuitRelay {
            r#type: Some(circuit_relay::Type::Stop.into()),
            src_peer: Some(circuit_relay::Peer {
                id: src_id.to_bytes(),
                addrs: vec![src_addr.to_vec()],
            }),
            dst_peer: Some(circuit_relay::Peer {
                id: dst_peer.peer_id.to_bytes(),
                addrs: dst_peer.addrs.into_iter().map(|a| a.to_vec()).collect(),
            }),
            code: None,
        };
        let mut encoded_msg = Vec::new();
        message
            .encode(&mut encoded_msg)
            .expect("all the mandatory fields are always filled; QED");

        OutgoingDstReq {
            message: encoded_msg,
        }
    }
}

impl upgrade::UpgradeInfo for OutgoingDstReq {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(PROTOCOL_NAME)
    }
}

impl upgrade::OutboundUpgrade<NegotiatedSubstream> for OutgoingDstReq {
    type Output = (NegotiatedSubstream, Bytes);
    type Error = OutgoingDstReqError;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_outbound(self, substream: NegotiatedSubstream, _: Self::Info) -> Self::Future {
        let mut codec = UviBytes::default();
        codec.set_max_len(MAX_ACCEPTED_MESSAGE_LEN);

        let mut substream = Framed::new(substream, codec);

        async move {
            substream.send(std::io::Cursor::new(self.message)).await?;
            let msg =
                substream
                    .next()
                    .await
                    .ok_or(OutgoingDstReqError::Io(std::io::Error::new(
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
                .ok_or(OutgoingDstReqError::ParseTypeField)?
            {
                circuit_relay::Type::Status => {}
                s => return Err(OutgoingDstReqError::ExpectedStatusType(s)),
            }

            if src_peer.is_some() {
                return Err(OutgoingDstReqError::UnexpectedSrcPeerWithStatusType);
            }
            if dst_peer.is_some() {
                return Err(OutgoingDstReqError::UnexpectedDstPeerWithStatusType);
            }

            match code
                .map(circuit_relay::Status::from_i32)
                .flatten()
                .ok_or(OutgoingDstReqError::ParseStatusField)?
            {
                circuit_relay::Status::Success => {}
                s => return Err(OutgoingDstReqError::ExpectedSuccessStatus(s)),
            }

            let FramedParts {
                io,
                read_buffer,
                write_buffer,
                ..
            } = substream.into_parts();
            assert!(
                write_buffer.is_empty(),
                "Expect a flushed Framed to have an empty write buffer."
            );

            Ok((io, read_buffer.freeze()))
        }
        .boxed()
    }
}

#[derive(Debug)]
pub enum OutgoingDstReqError {
    Decode(prost::DecodeError),
    Io(std::io::Error),
    ParseTypeField,
    ParseStatusField,
    ExpectedStatusType(circuit_relay::Type),
    ExpectedSuccessStatus(circuit_relay::Status),
    UnexpectedSrcPeerWithStatusType,
    UnexpectedDstPeerWithStatusType,
}

impl From<std::io::Error> for OutgoingDstReqError {
    fn from(e: std::io::Error) -> Self {
        OutgoingDstReqError::Io(e)
    }
}

impl From<prost::DecodeError> for OutgoingDstReqError {
    fn from(e: prost::DecodeError) -> Self {
        OutgoingDstReqError::Decode(e)
    }
}

impl fmt::Display for OutgoingDstReqError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutgoingDstReqError::Decode(e) => {
                write!(f, "Failed to decode response: {}.", e)
            }
            OutgoingDstReqError::Io(e) => {
                write!(f, "Io error {}", e)
            }
            OutgoingDstReqError::ParseTypeField => {
                write!(f, "Failed to parse response type field.")
            }
            OutgoingDstReqError::ParseStatusField => {
                write!(f, "Failed to parse response status field.")
            }
            OutgoingDstReqError::ExpectedStatusType(t) => {
                write!(f, "Expected status message type, but got {:?}", t)
            }
            OutgoingDstReqError::UnexpectedSrcPeerWithStatusType => {
                write!(f, "Unexpected source peer with status type.")
            }
            OutgoingDstReqError::UnexpectedDstPeerWithStatusType => {
                write!(f, "Unexpected destination peer with status type.")
            }
            OutgoingDstReqError::ExpectedSuccessStatus(s) => {
                write!(f, "Expected success status but got {:?}", s)
            }
        }
    }
}

impl error::Error for OutgoingDstReqError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            OutgoingDstReqError::Decode(e) => Some(e),
            OutgoingDstReqError::Io(e) => Some(e),
            OutgoingDstReqError::ParseTypeField => None,
            OutgoingDstReqError::ParseStatusField => None,
            OutgoingDstReqError::ExpectedStatusType(_) => None,
            OutgoingDstReqError::UnexpectedSrcPeerWithStatusType => None,
            OutgoingDstReqError::UnexpectedDstPeerWithStatusType => None,
            OutgoingDstReqError::ExpectedSuccessStatus(_) => None,
        }
    }
}
