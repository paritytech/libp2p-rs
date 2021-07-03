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

//! [`NetworkBehaviour`] to act as a direct connection upgrade through relay node.

use crate::handler;
use libp2p_core::connection::{ConnectedPoint, ConnectionId};
use libp2p_core::multiaddr::Protocol;
use libp2p_core::{Multiaddr, PeerId};
use libp2p_swarm::{
    DialPeerCondition, NetworkBehaviour, NetworkBehaviourAction, NotifyHandler, PollParameters,
};
use std::collections::VecDeque;
use std::task::{Context, Poll};

/// The events produced by the [`Behaviour`].
#[derive(Debug)]
pub enum Event {}

pub struct Behaviour {
    /// Queue of actions to return when polled.
    queued_actions: VecDeque<NetworkBehaviourAction<handler::In, Event>>,
}

impl Behaviour {
    pub fn new() -> Self {
        Behaviour {
            queued_actions: Default::default(),
        }
    }
}

impl NetworkBehaviour for Behaviour {
    type ProtocolsHandler = handler::Prototype;
    type OutEvent = Event;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        handler::Prototype::new()
    }

    fn addresses_of_peer(&mut self, _peer_id: &PeerId) -> Vec<Multiaddr> {
        vec![]
    }

    fn inject_connected(&mut self, _peer_id: &PeerId) {}

    fn inject_connection_established(
        &mut self,
        peer_id: &PeerId,
        connection_id: &ConnectionId,
        connected_point: &ConnectedPoint,
    ) {
        if connected_point
            .get_remote_address()
            .iter()
            .any(|p| p == Protocol::P2pCircuit)
        {
            self.queued_actions
                .push_back(NetworkBehaviourAction::NotifyHandler {
                    peer_id: *peer_id,
                    handler: NotifyHandler::One(*connection_id),
                    event: handler::In::Connect { obs_addrs: vec![] },
                });
        }
    }

    fn inject_dial_failure(&mut self, _peer_id: &PeerId) {
    }

    fn inject_disconnected(&mut self, _peer: &PeerId) {
        todo!();
    }

    fn inject_connection_closed(
        &mut self,
        _peer_id: &PeerId,
        _connection_id: &ConnectionId,
        _: &ConnectedPoint,
    ) {
        todo!();
    }

    fn inject_event(
        &mut self,
        event_source: PeerId,
        connection: ConnectionId,
        handler_event: handler::Event,
    ) {
        match handler_event {
            handler::Event::InboundConnectReq(inbound_connect) => {
                self.queued_actions
                    .push_back(NetworkBehaviourAction::NotifyHandler {
                        peer_id: event_source,
                        handler: NotifyHandler::One(connection),
                        event: handler::In::AcceptInboundConnect {
                            inbound_connect,
                            obs_addrs: vec![],
                        },
                    });
            }
            handler::Event::InboundConnectNeg(remote_addrs) => {
                println!("Listener: Dialing");
                self.queued_actions.push_back(NetworkBehaviourAction::DialAddress {
                    // TODO: Handle empty addresses.
                    // TODO: What about the other addresses?
                    address: remote_addrs.into_iter().next().unwrap(),
                });
            }
            handler::Event::OutboundConnectNeg(remote_addrs) => {
                println!("Dialer: Dialing");
                self.queued_actions.push_back(NetworkBehaviourAction::DialAddress {
                    // TODO: Handle empty addresses.
                    // TODO: What about the other addresses?
                    address: remote_addrs.into_iter().next().unwrap(),
                });
            }
        }
    }

    fn poll(
        &mut self,
        _cx: &mut Context<'_>,
        poll_parameters: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<handler::In, Self::OutEvent>> {
        if let Some(mut event) = self.queued_actions.pop_front() {
            // Set obs addresses.
            if let NetworkBehaviourAction::NotifyHandler {
                event:
                    handler::In::Connect {
                        ref mut obs_addrs, ..
                    },
                ..
            }
            | NetworkBehaviourAction::NotifyHandler {
                event:
                    handler::In::AcceptInboundConnect {
                        ref mut obs_addrs, ..
                    },
                ..
            } = &mut event
            {
                *obs_addrs = poll_parameters
                    .external_addresses()
                    .map(|a| {
                        a.addr
                            .with(Protocol::P2p((*poll_parameters.local_peer_id()).into()))
                    })
                    .collect();
            }

            return Poll::Ready(event);
        }

        Poll::Pending
    }
}
