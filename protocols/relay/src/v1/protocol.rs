// Copyright 2018 Parity Technologies (UK) Ltd.
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

use crate::v1::message_proto::circuit_relay;

use libp2p_core::{multiaddr::Error as MultiaddrError, Multiaddr, PeerId};
use smallvec::SmallVec;
use std::{convert::TryFrom, error, fmt};

// Source -> Relay
mod incoming_relay_req;
mod outgoing_relay_req;
pub use self::incoming_relay_req::{IncomingRelayReq, IncomingRelayReqError};
pub use self::outgoing_relay_req::{OutgoingRelayReq, OutgoingRelayReqError};

// Relay -> Destination
mod incoming_dst_req;
mod outgoing_dst_req;
pub use self::incoming_dst_req::{IncomingDstReq, IncomingDstReqError};
pub use self::outgoing_dst_req::{OutgoingDstReq, OutgoingDstReqError};

mod listen;
pub use self::listen::{RelayListen, RelayListenError, RelayRemoteReq};

/// Any message received on the wire whose length exceeds this value is refused.
//
// The circuit relay specification sets a maximum of 1024 bytes per multiaddr. A single message can
// contain multiple addresses for both the source and destination node. Setting the maximum message
// length to 10 times that limit is an unproven estimate. Feel free to refine this in the future.
const MAX_ACCEPTED_MESSAGE_LEN: usize = 10 * 1024;

const PROTOCOL_NAME: &[u8; 27] = b"/libp2p/circuit/relay/0.1.0";


/// Representation of a `CircuitRelay_Peer` protobuf message with refined field types.
///
/// Can be parsed from a `CircuitRelay_Peer` using the `TryFrom` trait.
#[derive(Clone)]
pub(crate) struct Peer {
    pub(crate) peer_id: PeerId,
    pub(crate) addrs: SmallVec<[Multiaddr; 4]>,
}

impl TryFrom<circuit_relay::Peer> for Peer {
    type Error = PeerParseError;

    fn try_from(peer: circuit_relay::Peer) -> Result<Peer, Self::Error> {
        let circuit_relay::Peer { id, addrs } = peer;
        let peer_id = PeerId::from_bytes(&id).map_err(|_| PeerParseError::PeerIdParseError)?;
        let mut parsed_addrs = SmallVec::with_capacity(addrs.len());
        for addr in addrs.into_iter() {
            let addr = Multiaddr::try_from(addr).map_err(PeerParseError::MultiaddrParseError)?;
            parsed_addrs.push(addr);
        }
        Ok(Peer {
            peer_id,
            addrs: parsed_addrs,
        })
    }
}

/// Error while parsing information about a peer from a network message.
#[derive(Debug)]
pub enum PeerParseError {
    /// Failed to parse the identity of the peer.
    PeerIdParseError,
    /// Failed to parse one of the multiaddresses for the peer.
    MultiaddrParseError(MultiaddrError),
}

impl fmt::Display for PeerParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PeerParseError::PeerIdParseError => write!(f, "Error while parsing the peer id"),
            PeerParseError::MultiaddrParseError(ref err) => {
                write!(f, "Error while parsing a multiaddress: {}", err)
            }
        }
    }
}

impl error::Error for PeerParseError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            PeerParseError::PeerIdParseError => None,
            PeerParseError::MultiaddrParseError(ref err) => Some(err),
        }
    }
}
