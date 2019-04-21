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

//! Provides the `BrahmsRequest` upgrade that sends a request on the network and waits for a
//! potential response, and `BrahmsListen` upgrade that accepts a request from the remote.

use crate::codec::RawMessage;
use crate::pow::Pow;
use futures::{future, prelude::*};
use libp2p_core::upgrade::{self, InboundUpgrade, OutboundUpgrade, UpgradeInfo};
use libp2p_core::{Negotiated, Multiaddr, PeerId};
use std::{error, fmt, io, iter};
use tokio_io::{AsyncRead, AsyncWrite};

/// Request that can be sent to a peer.
#[derive(Debug, Clone)]
pub struct BrahmsPushRequest {
    /// Id of the local peer.
    pub local_peer_id: PeerId,
    /// Id of the peer we're going to send this message to. The message is only valid for this
    /// specific peer.
    pub remote_peer_id: PeerId,
    /// Addresses we're listening on.
    pub addresses: Vec<Multiaddr>,
    /// Difficulty of the proof of work.
    pub pow_difficulty: u8,
}

impl UpgradeInfo for BrahmsPushRequest {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(b"/paritytech/brahms/0.1.0")
    }
}

impl<TSocket> OutboundUpgrade<TSocket> for BrahmsPushRequest
where
    TSocket: AsyncRead + AsyncWrite,
{
    type Output = ();
    type Error = BrahmsPushRequestError;
    type Future = future::Either<future::FromErr<upgrade::WriteOne<Negotiated<TSocket>>, BrahmsPushRequestError>, future::FutureResult<(), BrahmsPushRequestError>>;

    #[inline]
    fn upgrade_outbound(self, socket: Negotiated<TSocket>, _: Self::Info) -> Self::Future {
        let addrs = self
            .addresses
            .into_iter()
            .map(Multiaddr::into_bytes)
            .collect();
        // TODO: what if lots of addrs? https://github.com/libp2p/rust-libp2p/issues/760

        match Pow::generate(&self.local_peer_id, &self.remote_peer_id, self.pow_difficulty) {
            Ok(pow) => future::Either::A(upgrade::write_one(socket, RawMessage::Push(addrs, pow.nonce()).into_bytes()).from_err()),
            Err(()) => future::Either::B(future::err(BrahmsPushRequestError::PowGenerationFailed)),
        }
    }
}

/// Error while sending a push request.
#[derive(Debug)]
pub enum BrahmsPushRequestError {
    /// I/O error.
    Io(io::Error),
    /// Failed to generate a proof of work for the given request with the given difficulty.
    PowGenerationFailed,
}

impl From<io::Error> for BrahmsPushRequestError {
    fn from(err: io::Error) -> Self {
        BrahmsPushRequestError::Io(err)
    }
}

impl fmt::Display for BrahmsPushRequestError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            BrahmsPushRequestError::Io(ref err) => write!(f, "{}", err),
            BrahmsPushRequestError::PowGenerationFailed =>
                write!(f, "Failed to generate a proof of work for the push request."),
        }
    }
}

impl error::Error for BrahmsPushRequestError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            BrahmsPushRequestError::Io(ref err) => Some(err),
            BrahmsPushRequestError::PowGenerationFailed => None,
        }
    }
}

/// Request that can be sent to a peer.
#[derive(Debug, Clone)]
pub struct BrahmsPullRequestRequest {}

impl UpgradeInfo for BrahmsPullRequestRequest {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(b"/paritytech/brahms/0.1.0")
    }
}

impl<TSocket> OutboundUpgrade<TSocket> for BrahmsPullRequestRequest
where
    TSocket: AsyncRead + AsyncWrite,
{
    type Output = Vec<(PeerId, Vec<Multiaddr>)>;
    type Error = Box<error::Error + Send + Sync>;
    type Future = upgrade::RequestResponse<Negotiated<TSocket>, (), fn(Vec<u8>, ()) -> Result<Self::Output, Self::Error>>;

    #[inline]
    fn upgrade_outbound(self, socket: Negotiated<TSocket>, _: Self::Info) -> Self::Future {
        let message = RawMessage::PullRequest.into_bytes();
        upgrade::request_response(socket, message, 2048, (), |message, ()| {
            match RawMessage::from_bytes(&message) {
                RawMessage::PullResponse(response) => {
                    let mut out = Vec::new();
                    for (peer_id, addrs) in response {
                        let peer_id = if let Ok(id) = PeerId::from_bytes(peer_id) {
                            id
                        } else {
                            return Err("Invalid peer ID in pull response".to_string().into());
                        };

                        let mut peer = Vec::new();
                        for addr in addrs {
                            if let Ok(a) = Multiaddr::from_bytes(addr) {
                                peer.push(a);
                            } else {
                                return Err("Invalid multiaddr in pull response".to_string().into());
                            }
                        }
                        out.push((peer_id, peer));
                    }
                    Ok(out)
                }
                RawMessage::Push(_, _) | RawMessage::PullRequest => {
                    Err("Invalid remote request".to_string().into())
                }
            }
        })
    }
}

/// Upgrade that listens for a request from the remote, and allows answering it.
#[derive(Debug, Clone)]
pub struct BrahmsListen {
    /// Id of the local peer. Messages not received by this id can be invalid.
    pub local_peer_id: PeerId,
    /// Id of the peer that sends us messages. Messages not sent by this id can be invalid.
    pub remote_peer_id: PeerId,
    /// Required difficulty of the proof of work.
    pub pow_difficulty: u8,
}

impl UpgradeInfo for BrahmsListen {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(b"/paritytech/brahms/0.1.0")
    }
}

impl<TSocket> InboundUpgrade<TSocket> for BrahmsListen
where
    TSocket: AsyncRead + AsyncWrite,
{
    type Output = BrahmsListenOut<TSocket>;
    type Error = Box<error::Error + Send + Sync>;   // TODO: better error
    type Future = upgrade::ReadRespond<Negotiated<TSocket>, Self, fn(Negotiated<TSocket>, Vec<u8>, Self) -> Result<Self::Output, Self::Error>>;

    #[inline]
    fn upgrade_inbound(self, socket: Negotiated<TSocket>, _: Self::Info) -> Self::Future {
        upgrade::read_respond(socket, 2048, self, |socket, message_bytes, me| {
            let message = RawMessage::from_bytes(&message_bytes);
            match message {
                RawMessage::Push(addrs, nonce) => {
                    // We swap the remote and local peer IDs, as the parameters are from the point
                    // of view of the remote.
                    if Pow::verify(&me.remote_peer_id, &me.local_peer_id, nonce, me.pow_difficulty).is_err() {
                        return Err(io::Error::new(io::ErrorKind::Other, "invalid PoW").into());
                    }
                    let mut addrs_parsed = Vec::with_capacity(addrs.len());
                    for addr in addrs {
                        addrs_parsed.push(Multiaddr::from_bytes(addr)?);
                    }
                    Ok(BrahmsListenOut::Push(addrs_parsed))
                }
                RawMessage::PullRequest => Ok(BrahmsListenOut::PullRequest(
                    BrahmsListenPullRequest {
                        inner: socket,
                    },
                )),
                RawMessage::PullResponse(_) => {
                    Err("Invalid remote request".to_string().into())
                }
            }
        })
    }
}

/// Request received from a remote.
// TODO: #[derive(Debug)]
pub enum BrahmsListenOut<TSocket> {
    /// The sender pushes itself to us. Contains the addresses it's listening on.
    Push(Vec<Multiaddr>),

    /// Sender requests us to send back our view of the network.
    PullRequest(BrahmsListenPullRequest<TSocket>),
}

/// Sender requests us to send back our view of the network.
// TODO: #[derive(Debug)]
pub struct BrahmsListenPullRequest<TSocket> {
    inner: Negotiated<TSocket>,
}

impl<TSocket> BrahmsListenPullRequest<TSocket> {
    /// Respond to the request.
    pub fn respond(
        self,
        view: impl IntoIterator<Item = (PeerId, impl IntoIterator<Item = Multiaddr>)>,
    ) -> upgrade::WriteOne<Negotiated<TSocket>>
    where
        TSocket: AsyncWrite
    {
        let view = view
            .into_iter()
            .map(|(peer_id, addrs)| {
                (
                    peer_id.into_bytes(),
                    addrs.into_iter().map(|addr| addr.into_bytes()).collect(),
                )
            })
            .collect();

        let msg_bytes = RawMessage::PullResponse(view).into_bytes();
        upgrade::write_one(self.inner, msg_bytes)
    }
}
