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
use crate::protocol::incoming_dst_req::IncomingDstReq;
use crate::protocol::incoming_relay_req::IncomingRelayReq;
use crate::protocol::{Peer, PeerParseError, MAX_ACCEPTED_MESSAGE_LEN, PROTOCOL_NAME};
use asynchronous_codec::Framed;
use futures::channel::oneshot;
use futures::{future::BoxFuture, prelude::*};
use libp2p_core::upgrade;
use libp2p_swarm::NegotiatedSubstream;
use prost::Message;

use std::io::Cursor;
use std::{convert::TryFrom, error, fmt, iter};
use unsigned_varint::codec::UviBytes;

/// Configuration for an inbound upgrade that handles requests from the remote for the relay
/// protocol.
#[derive(Debug, Clone)]
pub struct RelayListen {}

/// Outcome of the listening.
pub enum RelayRemoteReq {
    /// We have been asked to become a destination.
    DstReq(IncomingDstReq),
    /// We have been asked to relay communications to another node.
    RelayReq((IncomingRelayReq, oneshot::Receiver<()>)),
}

impl RelayListen {
    /// Builds a new `RelayListen` with default options.
    pub fn new() -> RelayListen {
        RelayListen {}
    }
}

impl upgrade::UpgradeInfo for RelayListen {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(PROTOCOL_NAME)
    }
}

impl upgrade::InboundUpgrade<NegotiatedSubstream> for RelayListen {
    type Output = RelayRemoteReq;
    type Error = RelayListenError;
    type Future = BoxFuture<'static, Result<RelayRemoteReq, RelayListenError>>;

    fn upgrade_inbound(self, substream: NegotiatedSubstream, _: Self::Info) -> Self::Future {
        async move {
            let mut codec = UviBytes::<bytes::Bytes>::default();
            codec.set_max_len(MAX_ACCEPTED_MESSAGE_LEN);
            let mut substream = Framed::new(substream, codec);

            let msg: bytes::BytesMut = substream
                .next()
                .await
                .ok_or(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, ""))??;
            let CircuitRelay {
                r#type,
                src_peer,
                dst_peer,
                code: _,
            } = CircuitRelay::decode(Cursor::new(msg))?;

            match circuit_relay::Type::from_i32(r#type.ok_or(RelayListenError::NoMessageType)?)
                .ok_or(RelayListenError::InvalidMessageTy)?
            {
                circuit_relay::Type::Hop => {
                    let peer = Peer::try_from(dst_peer.ok_or(RelayListenError::NoDstPeer)?)?;
                    let (rq, notifyee) = IncomingRelayReq::new(substream, peer);
                    Ok(RelayRemoteReq::RelayReq((rq, notifyee)))
                }
                circuit_relay::Type::Stop => {
                    let peer = Peer::try_from(src_peer.ok_or(RelayListenError::NoSrcPeer)?)?;
                    let rq = IncomingDstReq::new(substream, peer);
                    Ok(RelayRemoteReq::DstReq(rq))
                }
                _ => Err(RelayListenError::InvalidMessageTy),
            }
        }
        .boxed()
    }
}

/// Error while upgrading with a [`RelayListen`].
#[derive(Debug)]
pub enum RelayListenError {
    Decode(prost::DecodeError),
    Io(std::io::Error),
    NoSrcPeer,
    NoDstPeer,
    ParsePeer(PeerParseError),
    NoMessageType,
    InvalidMessageTy,
}

impl From<prost::DecodeError> for RelayListenError {
    fn from(err: prost::DecodeError) -> Self {
        RelayListenError::Decode(err)
    }
}

impl From<std::io::Error> for RelayListenError {
    fn from(e: std::io::Error) -> Self {
        RelayListenError::Io(e)
    }
}

impl From<PeerParseError> for RelayListenError {
    fn from(err: PeerParseError) -> Self {
        RelayListenError::ParsePeer(err)
    }
}

impl fmt::Display for RelayListenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RelayListenError::Decode(e) => {
                write!(f, "Failed to decode response: {}.", e)
            }
            RelayListenError::Io(e) => {
                write!(f, "Io error {}", e)
            }
            RelayListenError::NoSrcPeer => {
                write!(f, "Expected source peer id")
            }
            RelayListenError::NoDstPeer => {
                write!(f, "Expected destination peer id")
            }
            RelayListenError::ParsePeer(e) => {
                write!(f, "Failed to parse peer field: {}", e)
            }
            RelayListenError::NoMessageType => {
                write!(f, "Expected message type to be set.")
            }
            RelayListenError::InvalidMessageTy => {
                write!(f, "Invalid message type")
            }
        }
    }
}

impl error::Error for RelayListenError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            RelayListenError::Decode(e) => Some(e),
            RelayListenError::Io(e) => Some(e),
            RelayListenError::NoSrcPeer => None,
            RelayListenError::NoDstPeer => None,
            RelayListenError::ParsePeer(_) => None,
            RelayListenError::NoMessageType => None,
            RelayListenError::InvalidMessageTy => None,
        }
    }
}
