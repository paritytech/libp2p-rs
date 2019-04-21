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

use crate::protocol::{
    BrahmsListen, BrahmsListenOut, BrahmsListenPullRequest,
    BrahmsPullRequestRequest, BrahmsPushRequest, BrahmsPushRequestError,
};
use futures::prelude::*;
use libp2p_core::{
    either::{EitherError, EitherOutput},
    protocols_handler::KeepAlive,
    protocols_handler::IntoProtocolsHandler,
    protocols_handler::ProtocolsHandlerUpgrErr,
    upgrade::{EitherUpgrade, InboundUpgrade, OutboundUpgrade, WriteOne},
    Negotiated, Multiaddr, PeerId, ProtocolsHandler, ProtocolsHandlerEvent,
};
use smallvec::SmallVec;
use std::{error, fmt, io, marker::PhantomData, time::Duration, time::Instant};
use tokio_io::{AsyncRead, AsyncWrite};

/// Duration after which an inactive connection will be shut down.
const INACTIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// Per-connection handler for the Brahms protocol.
pub struct BrahmsHandler<TSubstream> {
    /// PeerId of the local node.
    local_peer_id: PeerId,
    /// Required difficulty of the proof of work.
    pow_difficulty: u8,
    /// Holds the generics in place.
    marker: PhantomData<TSubstream>,
}

pub struct BrahmsHandlerInner<TSubstream> {
    /// PeerId of the local node.
    local_peer_id: PeerId,

    /// PeerId of the remote node.
    remote_peer_id: PeerId,

    /// Required difficulty of the proof of work.
    pow_difficulty: u8,

    /// Queue of values that `poll` should produces.
    send_queue: SmallVec<[OutEvent; 16]>,

    /// Whether or not we should force the connection alive.
    connection_keep_alive: KeepAlive,

    /// The latest pull request received by the remote.
    ongoing_pull_request: Option<BrahmsListenPullRequest<TSubstream>>,

    /// Futures that flush pull responses.
    pull_response_flushes: SmallVec<[WriteOne<Negotiated<TSubstream>>; 4]>,
}

type OutEvent = ProtocolsHandlerEvent<
    EitherUpgrade<BrahmsPushRequest, BrahmsPullRequestRequest>,
    (),
    BrahmsHandlerEvent,
>;

/// Event that can be sent to the handler.
#[derive(Debug, Clone)]
pub enum BrahmsHandlerIn {
    /// Marks this connection as "keep alive". We will not disconnect.
    EnableKeepAlive,
    /// Unmarks this connection from the "keep alive" state. We can disconnect.
    DisableKeepAlive,
    /// Send something to the remote.
    Event(BrahmsHandlerEvent),
}

// TODO: document
#[derive(Debug, Clone)]
pub enum BrahmsHandlerEvent {
    Push {
        /// Id of the local peer.
        local_peer_id: PeerId,
        /// Id of the peer we're going to send this message to. The message is only valid for this
        /// specific peer.
        remote_peer_id: PeerId,
        /// Addresses we're listening on.
        addresses: Vec<Multiaddr>,
        /// Difficulty of the proof of work.
        pow_difficulty: u8,
    },
    PullRequest,
    PullResult { list: Vec<(PeerId, Vec<Multiaddr>)> },
}

impl<TSubstream> BrahmsHandler<TSubstream> {
    /// Builds a new `BrahmsHandler`.
    pub fn new(local_peer_id: PeerId, pow_difficulty: u8) -> Self {
        BrahmsHandler {
            local_peer_id,
            pow_difficulty,
            marker: PhantomData,
        }
    }
}

impl<TSubstream> IntoProtocolsHandler for BrahmsHandler<TSubstream>
where
    TSubstream: AsyncRead + AsyncWrite,
{
    type Handler = BrahmsHandlerInner<TSubstream>;

    fn into_handler(self, remote_peer_id: &PeerId) -> Self::Handler {
        BrahmsHandlerInner {
            local_peer_id: self.local_peer_id,
            remote_peer_id: remote_peer_id.clone(),
            pow_difficulty: self.pow_difficulty,
            send_queue: SmallVec::new(),
            connection_keep_alive: KeepAlive::Until(Instant::now() + INACTIVE_TIMEOUT),
            ongoing_pull_request: None,
            pull_response_flushes: SmallVec::new(),
        }
    }
}

impl<TSubstream> ProtocolsHandler for BrahmsHandlerInner<TSubstream>
where
    TSubstream: AsyncRead + AsyncWrite,
{
    type InEvent = BrahmsHandlerIn;
    type OutEvent = BrahmsHandlerEvent;
    type Substream = TSubstream;
    type Error = io::Error;
    type InboundProtocol = BrahmsListen;
    type OutboundProtocol = EitherUpgrade<BrahmsPushRequest, BrahmsPullRequestRequest>;
    type OutboundOpenInfo = ();

    #[inline]
    fn listen_protocol(&self) -> Self::InboundProtocol {
        BrahmsListen {
            local_peer_id: self.local_peer_id.clone(),
            remote_peer_id: self.remote_peer_id.clone(),
            pow_difficulty: self.pow_difficulty,
        }
    }

    fn inject_fully_negotiated_inbound(
        &mut self,
        request: <Self::InboundProtocol as InboundUpgrade<TSubstream>>::Output,
    ) {
        if let KeepAlive::Until(_) = self.connection_keep_alive {
            self.connection_keep_alive = KeepAlive::Until(Instant::now() + INACTIVE_TIMEOUT);
        }

        match request {
            BrahmsListenOut::Push(addresses) => {
                self.send_queue
                    .push(ProtocolsHandlerEvent::Custom(BrahmsHandlerEvent::Push {
                        addresses,
                        local_peer_id: self.local_peer_id.clone(),
                        remote_peer_id: self.remote_peer_id.clone(),
                        pow_difficulty: self.pow_difficulty,
                    }));
            }
            BrahmsListenOut::PullRequest(request) => {
                self.ongoing_pull_request = Some(request);
                self.send_queue.push(ProtocolsHandlerEvent::Custom(
                    BrahmsHandlerEvent::PullRequest,
                ));
            }
        }
    }

    fn inject_fully_negotiated_outbound(
        &mut self,
        response: <Self::OutboundProtocol as OutboundUpgrade<TSubstream>>::Output,
        _: Self::OutboundOpenInfo,
    ) {
        if let KeepAlive::Until(_) = self.connection_keep_alive {
            self.connection_keep_alive = KeepAlive::Until(Instant::now() + INACTIVE_TIMEOUT);
        }

        let list = match response {
            EitherOutput::First(()) => return,
            EitherOutput::Second(list) => list,
        };

        self.send_queue.push(ProtocolsHandlerEvent::Custom(
            BrahmsHandlerEvent::PullResult { list },
        ));
    }

    #[inline]
    fn inject_event(&mut self, message: BrahmsHandlerIn) {
        if let KeepAlive::Until(_) = self.connection_keep_alive {
            self.connection_keep_alive = KeepAlive::Until(Instant::now() + INACTIVE_TIMEOUT);
        }

        match message {
            BrahmsHandlerIn::Event(BrahmsHandlerEvent::Push { addresses, local_peer_id, remote_peer_id, pow_difficulty }) => {
                self.send_queue
                    .push(ProtocolsHandlerEvent::OutboundSubstreamRequest {
                        upgrade: EitherUpgrade::A(BrahmsPushRequest {
                            addresses,
                            local_peer_id,
                            remote_peer_id,
                            pow_difficulty,
                        }),
                        info: (),
                    });
            }
            BrahmsHandlerIn::Event(BrahmsHandlerEvent::PullRequest) => {
                self.send_queue
                    .push(ProtocolsHandlerEvent::OutboundSubstreamRequest {
                        upgrade: EitherUpgrade::B(BrahmsPullRequestRequest {}),
                        info: (),
                    });
            }
            BrahmsHandlerIn::Event(BrahmsHandlerEvent::PullResult { list }) => {
                if let Some(request) = self.ongoing_pull_request.take() {
                    let flush = request.respond(list);
                    self.pull_response_flushes.push(flush);
                }
            }
            BrahmsHandlerIn::EnableKeepAlive => {
                self.connection_keep_alive = KeepAlive::Forever;
            }
            BrahmsHandlerIn::DisableKeepAlive => {
                if let KeepAlive::Forever = self.connection_keep_alive {
                    self.connection_keep_alive = KeepAlive::Until(Instant::now() + INACTIVE_TIMEOUT);
                }
            }
        }
    }

    #[inline]
    fn inject_dial_upgrade_error(&mut self, _: Self::OutboundOpenInfo, _: ProtocolsHandlerUpgrErr<EitherError<BrahmsPushRequestError, Box<error::Error + Send + Sync>>>) {
    }

    #[inline]
    fn connection_keep_alive(&self) -> KeepAlive {
        self.connection_keep_alive
    }

    fn poll(
        &mut self,
    ) -> Poll<
        OutEvent,
        Self::Error,
    > {
        if !self.send_queue.is_empty() {
            let event = self.send_queue.remove(0);
            return Ok(Async::Ready(event));
        }

        // We remove each element from `pull_response_flushes` one by one and add them back if
        // not ready.
        for n in (0..self.pull_response_flushes.len()).rev() {
            let mut fut = self.pull_response_flushes.swap_remove(n);
            match fut.poll() {
                Ok(Async::NotReady) => {
                    self.pull_response_flushes.push(fut);
                }
                Ok(Async::Ready(())) | Err(_) => {}
            }
        }

        Ok(Async::NotReady)
    }
}

impl<TSubstream> fmt::Debug for BrahmsHandlerInner<TSubstream>
where
    TSubstream: AsyncRead + AsyncWrite,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.debug_struct("BrahmsHandlerInner")
            .field("send_queue", &self.send_queue.len())
            .field("ongoing_pull_request", &self.ongoing_pull_request.is_some())
            .field("pull_response_flushes", &self.pull_response_flushes.len())
            .finish()
    }
}
