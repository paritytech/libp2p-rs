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

use crate::protocol;
use futures::future::{BoxFuture, FutureExt};
use futures::stream::{FuturesUnordered, StreamExt};
use libp2p_core::{upgrade, ConnectedPoint, Multiaddr, PeerId};
use libp2p_swarm::protocols_handler::{InboundUpgradeSend, OutboundUpgradeSend};
use libp2p_swarm::{
    IntoProtocolsHandler, KeepAlive, NegotiatedSubstream, ProtocolsHandler, ProtocolsHandlerEvent,
    ProtocolsHandlerUpgrErr, SubstreamProtocol,
};
use std::collections::VecDeque;
use std::task::{Context, Poll};

pub enum In {
    Connect {
        obs_addrs: Vec<Multiaddr>,
    },
    // TODO: Needs both the inbound stream and the observed addresses.
    AcceptInboundConnect {
        obs_addrs: Vec<Multiaddr>,
        inbound_connect: protocol::InboundConnect,
    },
}

pub enum Event {
    InboundConnectReq(protocol::InboundConnect),
    // TODO: Rename to InboundConnectNegotiated?
    InboundConnectNeg(Vec<Multiaddr>),
    OutboundConnectNeg(Vec<Multiaddr>),
}

// TODO: Detour through Prototype needed?
pub struct Prototype {}

impl Prototype {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl IntoProtocolsHandler for Prototype {
    type Handler = Handler;

    fn into_handler(self, _remote_peer_id: &PeerId, _endpoint: &ConnectedPoint) -> Self::Handler {
        Handler {
            queued_events: Default::default(),
            inbound_connects: Default::default(),
        }
    }

    fn inbound_protocol(&self) -> <Self::Handler as ProtocolsHandler>::InboundProtocol {
        protocol::InboundUpgrade {}
    }
}

pub struct Handler {
    /// Queue of events to return when polled.
    queued_events: VecDeque<
        ProtocolsHandlerEvent<
            <Self as ProtocolsHandler>::OutboundProtocol,
            <Self as ProtocolsHandler>::OutboundOpenInfo,
            <Self as ProtocolsHandler>::OutEvent,
            <Self as ProtocolsHandler>::Error,
        >,
    >,

    inbound_connects:
        FuturesUnordered<BoxFuture<'static, Result<Vec<Multiaddr>, protocol::InboundUpgradeError>>>,
}

impl ProtocolsHandler for Handler {
    type InEvent = In;
    type OutEvent = Event;
    type Error = ProtocolsHandlerUpgrErr<std::io::Error>;
    type InboundProtocol = protocol::InboundUpgrade;
    type OutboundProtocol = protocol::OutboundUpgrade;
    type OutboundOpenInfo = ();
    type InboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(protocol::InboundUpgrade {}, ())
    }

    fn inject_fully_negotiated_inbound(
        &mut self,
        inbound_connect: <Self::InboundProtocol as upgrade::InboundUpgrade<NegotiatedSubstream>>::Output,
        _: Self::InboundOpenInfo,
    ) {
        self.queued_events
            .push_back(ProtocolsHandlerEvent::Custom(Event::InboundConnectReq(
                inbound_connect,
            )));
    }

    fn inject_fully_negotiated_outbound(
        &mut self,
        protocol::Connect { obs_addrs }: <Self::OutboundProtocol as upgrade::OutboundUpgrade<
            NegotiatedSubstream,
        >>::Output,
        _info: Self::OutboundOpenInfo,
    ) {
        self.queued_events
            .push_back(ProtocolsHandlerEvent::Custom(Event::OutboundConnectNeg(
                obs_addrs,
            )));
    }

    fn inject_event(&mut self, event: Self::InEvent) {
        match event {
            In::Connect { obs_addrs } => {
                self.queued_events
                    .push_back(ProtocolsHandlerEvent::OutboundSubstreamRequest {
                        protocol: SubstreamProtocol::new(
                            protocol::OutboundUpgrade::new(obs_addrs),
                            (),
                        ),
                    });
            }
            In::AcceptInboundConnect {
                inbound_connect,
                obs_addrs,
            } => {
                self.inbound_connects
                    .push(inbound_connect.accept(obs_addrs).boxed());
            }
        }
    }

    fn inject_listen_upgrade_error(
        &mut self,
        _: Self::InboundOpenInfo,
        error: ProtocolsHandlerUpgrErr<<Self::InboundProtocol as InboundUpgradeSend>::Error>,
    ) {
        todo!("{:?}", error)
    }

    fn inject_dial_upgrade_error(
        &mut self,
        _open_info: Self::OutboundOpenInfo,
        _error: ProtocolsHandlerUpgrErr<<Self::OutboundProtocol as OutboundUpgradeSend>::Error>,
    ) {
        todo!()
    }

    // TODO: Why is this not a mut reference? If it were the case, we could do all keep alive handling in here.
    fn connection_keep_alive(&self) -> KeepAlive {
        // TODO
        KeepAlive::Yes
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        ProtocolsHandlerEvent<
            Self::OutboundProtocol,
            Self::OutboundOpenInfo,
            Self::OutEvent,
            Self::Error,
        >,
    > {
        // Return queued events.
        if let Some(event) = self.queued_events.pop_front() {
            return Poll::Ready(event);
        }

        while let Poll::Ready(Some(remote_addrs)) = self.inbound_connects.poll_next_unpin(cx) {
            return Poll::Ready(ProtocolsHandlerEvent::Custom(Event::InboundConnectNeg(
                remote_addrs.unwrap(),
            )));
        }

        Poll::Pending
    }
}
