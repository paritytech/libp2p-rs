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

use crate::handler::{RelayHandlerConfig, RelayHandlerEvent, RelayHandlerIn, RelayHandlerProto};
use crate::message_proto::circuit_relay;
use crate::protocol;
use crate::transport::TransportToBehaviourMsg;
use crate::RequestId;
use futures::channel::{mpsc, oneshot};
use futures::prelude::*;
use libp2p_core::connection::{ConnectedPoint, ConnectionId, ListenerId};
use libp2p_core::multiaddr::Multiaddr;
use libp2p_core::PeerId;
use libp2p_swarm::{
    DialPeerCondition, NetworkBehaviour, NetworkBehaviourAction, NotifyHandler, PollParameters,
};
use std::collections::{hash_map::Entry, HashMap, HashSet, VecDeque};
use std::task::{Context, Poll};
use std::time::Duration;

/// Network behaviour allowing the local node to act as a source, a relay and a destination.
pub struct Relay {
    config: RelayConfig,
    /// Channel receiver from [`crate::RelayTransport`].
    from_transport: mpsc::Receiver<TransportToBehaviourMsg>,

    /// Events that need to be send to a [`RelayListener`](crate::transport::RelayListener) via
    /// [`Self::listeners`] or [`Self::listener_any_relay`].
    outbox_to_listeners: VecDeque<(PeerId, BehaviourToListenerMsg)>,
    /// Events that need to be yielded to the outside when polling.
    outbox_to_swarm: VecDeque<NetworkBehaviourAction<RelayHandlerIn, ()>>,

    /// List of peers the network is connected to.
    connected_peers: HashMap<PeerId, HashSet<ConnectionId>>,

    /// Requests by the local node to a relay to relay a connection for the local node to a
    /// destination.
    outgoing_relay_reqs: OutgoingRelayReqs,

    /// Requests for the local node to act as a relay from a source to a destination indexed by
    /// destination [`PeerId`].
    incoming_relay_reqs: HashMap<PeerId, Vec<IncomingRelayReq>>,

    /// List of relay nodes via which the local node is explicitly listening for incoming relayed
    /// connections.
    ///
    /// Indexed by relay [`PeerId`]. Contains channel sender to listener.
    listeners: HashMap<PeerId, RelayListener>,

    /// Channel sender to listener listening for incoming relayed connections from relay nodes via
    /// which the local node is not explicitly listening.
    listener_any_relay: Option<mpsc::Sender<BehaviourToListenerMsg>>,
}

#[derive(Default)]
struct OutgoingRelayReqs {
    /// Indexed by relay peer id.
    dialing: HashMap<PeerId, Vec<OutgoingDialingRelayReq>>,
    upgrading: HashMap<RequestId, OutgoingUpgradingRelayReq>,
}

struct OutgoingDialingRelayReq {
    request_id: RequestId,
    src_peer_id: PeerId,
    relay_addr: Multiaddr,
    dst_addr: Option<Multiaddr>,
    dst_peer_id: PeerId,
    send_back: oneshot::Sender<Result<protocol::Connection, OutgoingRelayReqError>>,
}

struct OutgoingUpgradingRelayReq {
    send_back: oneshot::Sender<Result<protocol::Connection, OutgoingRelayReqError>>,
}

enum IncomingRelayReq {
    DialingDst {
        src_peer_id: PeerId,
        src_addr: Multiaddr,
        src_connection_id: ConnectionId,
        request_id: RequestId,
        incoming_relay_req: protocol::IncomingRelayReq,
    },
}

pub struct RelayConfig {
    /// How long to keep connections alive when they're idle.
    ///
    /// For a server, acting as a relay, allowing other nodes to listen for
    /// incoming connections via oneself, this should likely be increased in
    /// order not to force the peer to reconnect too regularly.
    pub connection_idle_timeout: Duration,
    /// Whether to actively establish an outgoing connection to a destination
    /// node, when being asked by a source node to relay a connection to said
    /// destination node.
    ///
    /// For security reasons this behaviour is disabled by default. Instead a
    /// destination node should establish a connection to a relay node before
    /// advertising their relayed address via that relay node to a source node.
    pub actively_connect_to_dst_nodes: bool,
}

impl Default for RelayConfig {
    fn default() -> Self {
        RelayConfig {
            connection_idle_timeout: Duration::from_secs(10),
            actively_connect_to_dst_nodes: false,
        }
    }
}

// TODO: For now one is only able to specify relay servers via
// `Swarm::listen_on(Multiaddress(<relay_server>/p2p-circuit/))`. In the future
// we might want to support adding them via the Relay behaviour? The latter
// would allow other behaviours to manage ones relay listeners.
impl Relay {
    /// Builds a new [`Relay`] [`NetworkBehaviour`].
    pub(crate) fn new(
        config: RelayConfig,
        from_transport: mpsc::Receiver<TransportToBehaviourMsg>,
    ) -> Self {
        Relay {
            config,
            from_transport,
            outbox_to_listeners: Default::default(),
            outbox_to_swarm: Default::default(),
            connected_peers: Default::default(),
            incoming_relay_reqs: Default::default(),
            outgoing_relay_reqs: Default::default(),
            listeners: Default::default(),
            listener_any_relay: Default::default(),
        }
    }
}

impl NetworkBehaviour for Relay {
    type ProtocolsHandler = RelayHandlerProto;
    type OutEvent = ();

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        RelayHandlerProto {
            config: RelayHandlerConfig {
                connection_idle_timeout: self.config.connection_idle_timeout,
            },
        }
    }

    fn addresses_of_peer(&mut self, remote_peer_id: &PeerId) -> Vec<Multiaddr> {
        self.listeners
            .iter()
            .filter_map(|(peer_id, r)| {
                if let RelayListener::Connecting { relay_addr, .. } = r {
                    if peer_id == remote_peer_id {
                        return Some(relay_addr.clone());
                    }
                }
                None
            })
            .chain(
                self.outgoing_relay_reqs
                    .dialing
                    .get(remote_peer_id)
                    .into_iter()
                    .flatten()
                    .map(|OutgoingDialingRelayReq { relay_addr, .. }| relay_addr.clone()),
            )
            .chain(
                self.incoming_relay_reqs
                    .get(remote_peer_id)
                    .into_iter()
                    .flatten()
                    .map(
                        |IncomingRelayReq::DialingDst {
                             incoming_relay_req, ..
                         }| incoming_relay_req.dst_peer().addrs.clone(),
                    )
                    .flatten(),
            )
            .collect()
    }

    fn inject_connection_established(
        &mut self,
        peer: &PeerId,
        connection_id: &ConnectionId,
        _: &ConnectedPoint,
    ) {
        let is_first = self
            .connected_peers
            .entry(*peer)
            .or_default()
            .insert(*connection_id);
        assert!(
            is_first,
            "`inject_connection_established` called with known connection id"
        );

        if let Some(RelayListener::Connecting { .. }) = self.listeners.get(peer) {
            self.outbox_to_swarm
                .push_back(NetworkBehaviourAction::NotifyHandler {
                    peer_id: *peer,
                    handler: NotifyHandler::One(*connection_id),
                    event: RelayHandlerIn::UsedForListening(true),
                });
            let mut to_listener = match self.listeners.remove(peer) {
                None | Some(RelayListener::Connected { .. }) => unreachable!("See outer match."),
                Some(RelayListener::Connecting { to_listener, .. }) => to_listener,
            };
            to_listener
                .start_send(BehaviourToListenerMsg::ConnectionToRelayEstablished)
                .expect("Channel to have at least capacity of 1.");
            self.listeners.insert(
                *peer,
                RelayListener::Connected {
                    connection_id: *connection_id,
                    to_listener,
                },
            );
        }
    }

    fn inject_connected(&mut self, peer_id: &PeerId) {
        assert!(
            self.connected_peers
                .get(peer_id)
                .map(|cs| !cs.is_empty())
                .unwrap_or(false),
            "Expect to be connected to peer with at least one connection."
        );

        if let Some(reqs) = self.outgoing_relay_reqs.dialing.remove(peer_id) {
            for req in reqs {
                let OutgoingDialingRelayReq {
                    request_id,
                    src_peer_id,
                    relay_addr: _,
                    dst_addr,
                    dst_peer_id,
                    send_back,
                } = req;
                self.outbox_to_swarm
                    .push_back(NetworkBehaviourAction::NotifyHandler {
                        peer_id: *peer_id,
                        handler: NotifyHandler::Any,
                        event: RelayHandlerIn::OutgoingRelayReq {
                            src_peer_id,
                            request_id,
                            dst_peer_id,
                            dst_addr: dst_addr.clone(),
                        },
                    });

                self.outgoing_relay_reqs
                    .upgrading
                    .insert(request_id, OutgoingUpgradingRelayReq { send_back });
            }
        }

        // Ask the newly-opened connection to be used as destination if relevant.
        if let Some(reqs) = self.incoming_relay_reqs.remove(peer_id) {
            for req in reqs {
                let IncomingRelayReq::DialingDst {
                    src_peer_id,
                    src_addr,
                    src_connection_id,
                    request_id,
                    incoming_relay_req,
                } = req;
                let event = RelayHandlerIn::OutgoingDstReq {
                    src_peer_id,
                    src_addr,
                    src_connection_id,
                    request_id,
                    incoming_relay_req,
                };

                self.outbox_to_swarm
                    .push_back(NetworkBehaviourAction::NotifyHandler {
                        peer_id: *peer_id,
                        handler: NotifyHandler::Any,
                        event: event,
                    });
            }
        }
    }

    fn inject_dial_failure(&mut self, peer_id: &PeerId) {
        if let Entry::Occupied(o) = self.listeners.entry(*peer_id) {
            if matches!(o.get(), RelayListener::Connecting{ .. }) {
                // By removing the entry, the channel to the listener is dropped and thus the
                // listener is notified that dialing the relay failed.
                o.remove_entry();
            }
        }

        if let Some(reqs) = self.outgoing_relay_reqs.dialing.remove(peer_id) {
            for req in reqs {
                let _ = req.send_back.send(Err(OutgoingRelayReqError::DialingRelay));
            }
        }

        if let Some(reqs) = self.incoming_relay_reqs.remove(peer_id) {
            for req in reqs {
                let IncomingRelayReq::DialingDst {
                    src_peer_id,
                    incoming_relay_req,
                    ..
                } = req;
                self.outbox_to_swarm
                    .push_back(NetworkBehaviourAction::NotifyHandler {
                        peer_id: src_peer_id,
                        handler: NotifyHandler::Any,
                        event: RelayHandlerIn::DenyIncomingRelayReq(
                            incoming_relay_req.deny(circuit_relay::Status::HopCantDialDst),
                        ),
                    })
            }
        }
    }

    fn inject_connection_closed(
        &mut self,
        peer: &PeerId,
        connection: &ConnectionId,
        _: &ConnectedPoint,
    ) {
        // Remove connection from the set of connections for the given peer. In case the set is
        // empty it will be removed in `inject_disconnected`.
        let was_present = self
            .connected_peers
            .get_mut(peer)
            .expect("`inject_connection_closed` called for connected peer.")
            .remove(connection);
        assert!(
            was_present,
            "`inject_connection_closed` called for known connection"
        );

        match self.listeners.get(peer) {
            None => {}
            Some(RelayListener::Connecting { .. }) => unreachable!(
                "State mismatch. Listener waiting for connection while \
                 connection previously established.",
            ),
            Some(RelayListener::Connected { connection_id, .. }) => {
                if connection_id == connection {
                    if let Some(new_primary) = self
                        .connected_peers
                        .get(peer)
                        .and_then(|cs| cs.iter().next())
                    {
                        let to_listener = match self.listeners.remove(peer) {
                            None | Some(RelayListener::Connecting { .. }) => {
                                unreachable!("Due to outer match.")
                            }
                            Some(RelayListener::Connected { to_listener, .. }) => to_listener,
                        };
                        self.listeners.insert(
                            *peer,
                            RelayListener::Connected {
                                connection_id: *new_primary,
                                to_listener,
                            },
                        );
                        self.outbox_to_swarm
                            .push_back(NetworkBehaviourAction::NotifyHandler {
                                peer_id: *peer,
                                handler: NotifyHandler::One(*new_primary),
                                event: RelayHandlerIn::UsedForListening(true),
                            });
                    } else {
                        // There are no more connections to the relay left that
                        // could be promoted as primary. Remove the listener,
                        // notifying the listener by dropping the channel to it.
                        self.listeners.remove(peer);
                    }
                }
            }
        }
    }

    fn inject_addr_reach_failure(
        &mut self,
        _peer_id: Option<&PeerId>,
        _addr: &Multiaddr,
        _error: &dyn std::error::Error,
    ) {
        // Handled in `inject_dial_failure`.
    }

    fn inject_listener_error(&mut self, _id: ListenerId, _err: &(dyn std::error::Error + 'static)) {
    }

    fn inject_listener_closed(&mut self, _id: ListenerId, _reason: Result<(), &std::io::Error>) {}

    fn inject_disconnected(&mut self, id: &PeerId) {
        self.connected_peers.remove(id);

        if let Some(reqs) = self.incoming_relay_reqs.remove(id) {
            for req in reqs {
                let IncomingRelayReq::DialingDst {
                    src_peer_id,
                    incoming_relay_req,
                    ..
                } = req;
                self.outbox_to_swarm
                    .push_back(NetworkBehaviourAction::NotifyHandler {
                        peer_id: src_peer_id,
                        handler: NotifyHandler::Any,
                        event: RelayHandlerIn::DenyIncomingRelayReq(
                            incoming_relay_req.deny(circuit_relay::Status::HopCantDialDst),
                        ),
                    })
            }
        }
    }

    fn inject_event(
        &mut self,
        event_source: PeerId,
        connection: ConnectionId,
        event: RelayHandlerEvent,
    ) {
        match event {
            // Remote wants us to become a relay.
            RelayHandlerEvent::IncomingRelayReq {
                request_id,
                src_addr,
                req,
            } => {
                if self.connected_peers.get(&req.dst_peer().peer_id).is_some() {
                    let dest_id = req.dst_peer().peer_id;
                    let event = RelayHandlerIn::OutgoingDstReq {
                        src_peer_id: event_source,
                        src_addr,
                        src_connection_id: connection,
                        request_id,
                        incoming_relay_req: req,
                    };
                    self.outbox_to_swarm
                        .push_back(NetworkBehaviourAction::NotifyHandler {
                            peer_id: dest_id,
                            handler: NotifyHandler::Any,
                            event,
                        });
                } else {
                    if self.config.actively_connect_to_dst_nodes {
                        let dest_id = req.dst_peer().peer_id;
                        self.incoming_relay_reqs.entry(dest_id).or_default().push(
                            IncomingRelayReq::DialingDst {
                                request_id,
                                incoming_relay_req: req,
                                src_peer_id: event_source,
                                src_addr,
                                src_connection_id: connection,
                            },
                        );
                        self.outbox_to_swarm
                            .push_back(NetworkBehaviourAction::DialPeer {
                                peer_id: dest_id,
                                condition: DialPeerCondition::NotDialing,
                            });
                    } else {
                        self.outbox_to_swarm
                            .push_back(NetworkBehaviourAction::NotifyHandler {
                                peer_id: event_source,
                                handler: NotifyHandler::One(connection),
                                event: RelayHandlerIn::DenyIncomingRelayReq(
                                    req.deny(circuit_relay::Status::HopNoConnToDst),
                                ),
                            });
                    }
                }
            }
            // Remote wants us to become a destination.
            RelayHandlerEvent::IncomingDstReq(request) => {
                let got_explicit_listener = self
                    .listeners
                    .get(&event_source)
                    .map(|l| !l.is_closed())
                    .unwrap_or(false);
                let got_listener_for_any_relay = self
                    .listener_any_relay
                    .as_mut()
                    .map(|l| !l.is_closed())
                    .unwrap_or(false);

                let send_back = if got_explicit_listener || got_listener_for_any_relay {
                    RelayHandlerIn::AcceptDstReq(request)
                } else {
                    RelayHandlerIn::DenyDstReq(request)
                };

                self.outbox_to_swarm
                    .push_back(NetworkBehaviourAction::NotifyHandler {
                        peer_id: event_source,
                        handler: NotifyHandler::One(connection),
                        event: send_back,
                    });
            }
            RelayHandlerEvent::OutgoingRelayReqError(_dst_peer_id, request_id) => {
                self.outgoing_relay_reqs
                    .upgrading
                    .remove(&request_id)
                    .expect("Outgoing relay request error for unknown request.");
            }
            RelayHandlerEvent::OutgoingRelayReqSuccess(_dst, request_id, stream) => {
                let send_back = self
                    .outgoing_relay_reqs
                    .upgrading
                    .remove(&request_id)
                    .map(|OutgoingUpgradingRelayReq { send_back, .. }| send_back)
                    .expect("Outgoing relay request success for unknown request.");
                let _ = send_back.send(Ok(stream));
            }
            RelayHandlerEvent::IncomingDstReqSuccess {
                stream,
                src_peer_id,
                relay_peer_id,
                relay_addr,
            } => self.outbox_to_listeners.push_back((
                relay_peer_id,
                BehaviourToListenerMsg::IncomingRelayedConnection {
                    stream,
                    src_peer_id,
                    relay_peer_id,
                    relay_addr,
                },
            )),
            RelayHandlerEvent::OutgoingDstReqError {
                src_connection_id,
                incoming_relay_req_deny_fut,
            } => {
                self.outbox_to_swarm
                    .push_back(NetworkBehaviourAction::NotifyHandler {
                        peer_id: event_source,
                        handler: NotifyHandler::One(src_connection_id),
                        event: RelayHandlerIn::DenyIncomingRelayReq(incoming_relay_req_deny_fut),
                    });
            }
        }
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
        poll_parameters: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<RelayHandlerIn, Self::OutEvent>> {
        if !self.outbox_to_listeners.is_empty() {
            let relay_peer_id = self.outbox_to_listeners[0].0;

            let listeners = &mut self.listeners;
            let listener_any_relay = self.listener_any_relay.as_mut();

            // Get channel sender to the listener that is explicitly listening
            // via this relay node, or, if registered, channel sender to
            // listener listening via any relay.
            let to_listener = listeners
                .get_mut(&relay_peer_id)
                .filter(|l| !l.is_closed())
                .and_then(|l| match l {
                    RelayListener::Connected { to_listener, .. } => Some(to_listener),
                    // State mismatch. Got relayed connection via relay, but
                    // local node is not connected to relay.
                    RelayListener::Connecting { .. } => None,
                })
                .or_else(|| listener_any_relay)
                .filter(|l| !l.is_closed());

            match to_listener {
                Some(to_listener) => match to_listener.poll_ready(cx) {
                    Poll::Ready(Ok(())) => {
                        if let Err(mpsc::SendError { .. }) = to_listener.start_send(
                            self.outbox_to_listeners
                                .pop_front()
                                .expect("Outbox is empty despite !is_empty().")
                                .1,
                        ) {
                            self.listeners.remove(&relay_peer_id);
                        }
                    }
                    Poll::Ready(Err(mpsc::SendError { .. })) => {
                        self.outbox_to_listeners.pop_front();
                        self.listeners.remove(&relay_peer_id);
                    }
                    Poll::Pending => {}
                },
                None => {
                    // No listener to send request to, thus dropping it. This
                    // case should be rare, as we check whether we have a
                    // listener before accepting an incoming destination
                    // request.
                    let event = self.outbox_to_listeners.pop_front();
                    log::trace!("Dropping event for unknown listener: {:?}", event);
                }
            }
        }

        loop {
            match self.from_transport.poll_next_unpin(cx) {
                Poll::Ready(Some(TransportToBehaviourMsg::DialReq {
                    request_id,
                    relay_addr,
                    relay_peer_id,
                    dst_addr,
                    dst_peer_id,
                    send_back,
                })) => {
                    if let Some(_) = self.connected_peers.get(&relay_peer_id) {
                        // In case we are already listening via the relay,
                        // prefer the primary connection.
                        let handler = self
                            .listeners
                            .get(&relay_peer_id)
                            .and_then(|s| {
                                if let RelayListener::Connected { connection_id, .. } = s {
                                    Some(NotifyHandler::One(*connection_id))
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(NotifyHandler::Any);
                        self.outbox_to_swarm
                            .push_back(NetworkBehaviourAction::NotifyHandler {
                                peer_id: relay_peer_id,
                                handler,
                                event: RelayHandlerIn::OutgoingRelayReq {
                                    request_id,
                                    src_peer_id: *poll_parameters.local_peer_id(),
                                    dst_peer_id,
                                    dst_addr: dst_addr.clone(),
                                },
                            });

                        self.outgoing_relay_reqs
                            .upgrading
                            .insert(request_id, OutgoingUpgradingRelayReq { send_back });
                    } else {
                        self.outgoing_relay_reqs
                            .dialing
                            .entry(relay_peer_id)
                            .or_default()
                            .push(OutgoingDialingRelayReq {
                                src_peer_id: *poll_parameters.local_peer_id(),
                                request_id,
                                relay_addr,
                                dst_addr,
                                dst_peer_id,
                                send_back,
                            });
                        return Poll::Ready(NetworkBehaviourAction::DialPeer {
                            peer_id: relay_peer_id,
                            condition: DialPeerCondition::Disconnected,
                        });
                    }
                }
                Poll::Ready(Some(TransportToBehaviourMsg::ListenReq {
                    relay_peer_id_and_addr,
                    mut to_listener,
                })) => {
                    match relay_peer_id_and_addr {
                        // Listener is listening for all incoming relayed
                        // connections from any relay
                        // node.
                        None => {
                            match self.listener_any_relay.as_mut() {
                                Some(sender) if !sender.is_closed() => {
                                    // Already got listener listening for all
                                    // incoming relayed connections. Signal to
                                    // listener by dropping the channel sender
                                    // to the listener.
                                }
                                _ => {
                                    to_listener
                                        .start_send(
                                            BehaviourToListenerMsg::ConnectionToRelayEstablished,
                                        )
                                        .expect("Channel to have at least capacity of 1.");
                                    self.listener_any_relay = Some(to_listener);
                                }
                            }
                        }
                        // Listener is listening for incoming relayed
                        // connections from this relay only.
                        Some((relay_peer_id, relay_addr)) => {
                            if let Some(connections) = self.connected_peers.get(&relay_peer_id) {
                                to_listener
                                    .start_send(
                                        BehaviourToListenerMsg::ConnectionToRelayEstablished,
                                    )
                                    .expect("Channel to have at least capacity of 1.");
                                let primary_connection =
                                    connections.iter().next().expect("At least one connection.");
                                self.listeners.insert(
                                    relay_peer_id,
                                    RelayListener::Connected {
                                        connection_id: *primary_connection,
                                        to_listener,
                                    },
                                );

                                self.outbox_to_swarm.push_back(
                                    NetworkBehaviourAction::NotifyHandler {
                                        peer_id: relay_peer_id,
                                        handler: NotifyHandler::One(*primary_connection),
                                        event: RelayHandlerIn::UsedForListening(true),
                                    },
                                );
                            } else {
                                self.listeners.insert(
                                    relay_peer_id,
                                    RelayListener::Connecting {
                                        relay_addr,
                                        to_listener,
                                    },
                                );
                                return Poll::Ready(NetworkBehaviourAction::DialPeer {
                                    peer_id: relay_peer_id,
                                    condition: DialPeerCondition::Disconnected,
                                });
                            }
                        }
                    }
                }
                Poll::Ready(None) => unreachable!(
                    "`Relay` `NetworkBehaviour` polled after channel from \
                     `RelayTransport` has been closed.",
                ),
                Poll::Pending => break,
            }
        }

        if let Some(event) = self.outbox_to_swarm.pop_front() {
            return Poll::Ready(event);
        }

        Poll::Pending
    }
}

#[derive(Debug)]
pub enum BehaviourToListenerMsg {
    ConnectionToRelayEstablished,
    IncomingRelayedConnection {
        stream: protocol::Connection,
        src_peer_id: PeerId,
        relay_peer_id: PeerId,
        relay_addr: Multiaddr,
    },
}

enum RelayListener {
    Connecting {
        relay_addr: Multiaddr,
        to_listener: mpsc::Sender<BehaviourToListenerMsg>,
    },
    Connected {
        connection_id: ConnectionId,
        to_listener: mpsc::Sender<BehaviourToListenerMsg>,
    },
}

impl RelayListener {
    /// Returns whether the channel to the
    /// [`RelayListener`](crate::transport::RelayListener) is closed.
    fn is_closed(&self) -> bool {
        match self {
            RelayListener::Connecting { to_listener, .. }
            | RelayListener::Connected { to_listener, .. } => to_listener.is_closed(),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum OutgoingRelayReqError {
    DialingRelay,
}
