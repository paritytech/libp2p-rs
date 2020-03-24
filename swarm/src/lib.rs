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

//! High level manager of the network.
//!
//! A [`Swarm`] contains the state of the network as a whole. The entire
//! behaviour of a libp2p network can be controlled through the `Swarm`.
//! The `Swarm` struct contains all active and pending connections to
//! remotes and manages the state of all the substreams that have been
//! opened, and all the upgrades that were built upon these substreams.
//!
//! # Initializing a Swarm
//!
//! Creating a `Swarm` requires three things:
//!
//!  1. A network identity of the local node in form of a [`PeerId`].
//!  2. An implementation of the [`Transport`] trait. This is the type that
//!     will be used in order to reach nodes on the network based on their
//!     address. See the `transport` module for more information.
//!  3. An implementation of the [`NetworkBehaviour`] trait. This is a state
//!     machine that defines how the swarm should behave once it is connected
//!     to a node.
//!
//! # Network Behaviour
//!
//! The [`NetworkBehaviour`] trait is implemented on types that indicate to
//! the swarm how it should behave. This includes which protocols are supported
//! and which nodes to try to connect to. It is the `NetworkBehaviour` that
//! controls what happens on the network. Multiple types that implement
//! `NetworkBehaviour` can be composed into a single behaviour.
//!
//! # Protocols Handler
//!
//! The [`ProtocolsHandler`] trait defines how each active connection to a
//! remote should behave: how to handle incoming substreams, which protocols
//! are supported, when to open a new outbound substream, etc.
//!

mod behaviour;
mod registry;
mod upgrade;

pub mod protocols_handler;
pub mod toggle;

pub use behaviour::{
    NetworkBehaviour,
    NetworkBehaviourAction,
    NetworkBehaviourEventProcess,
    PollParameters,
    NotifyHandler,
    DialPeerCondition
};
pub use protocols_handler::{
    IntoProtocolsHandler,
    IntoProtocolsHandlerSelect,
    KeepAlive,
    ProtocolsHandler,
    ProtocolsHandlerEvent,
    ProtocolsHandlerSelect,
    ProtocolsHandlerUpgrErr,
    OneShotHandler,
    SubstreamProtocol
};

use protocols_handler::NodeHandlerWrapperBuilder;
use futures::{
    prelude::*,
    executor::{ThreadPool, ThreadPoolBuilder},
    stream::FusedStream,
};
use libp2p_core::{
    Executor,
    Transport,
    Multiaddr,
    Negotiated,
    PeerId,
    connection::{
        ConnectionId,
        ConnectionInfo,
        ConnectedPoint,
        EstablishedConnection,
        IntoConnectionHandler,
        ListenerId,
        Substream
    },
    transport::{TransportError, boxed::Boxed as BoxTransport},
    muxing::{StreamMuxer, StreamMuxerBox},
    network::{
        DialError,
        Network,
        NetworkInfo,
        NetworkEvent,
        NetworkConfig,
        Peer,
        peer::ConnectedPeer,
    },
    upgrade::ProtocolName,
};
use registry::{Addresses, AddressIntoIter};
use smallvec::SmallVec;
use std::{error, fmt, hash::Hash, io, ops::{Deref, DerefMut}, pin::Pin, task::{Context, Poll}};
use std::collections::HashSet;
use upgrade::UpgradeInfoSend as _;

/// Contains the state of the network, plus the way it should behave.
pub type Swarm<TBehaviour, TConnInfo = PeerId> = ExpandedSwarm<
    TBehaviour,
    <<<TBehaviour as NetworkBehaviour>::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::InEvent,
    <<<TBehaviour as NetworkBehaviour>::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::OutEvent,
    <TBehaviour as NetworkBehaviour>::ProtocolsHandler,
    TConnInfo,
>;


// /// Contains the state of the network, plus the way it should behave.
// pub struct SwarmNetwork<TTransport, TBehaviour, TConnInfo = PeerId> {
//     behaviour: TBehaviour,
//     inner: Network<
//         TTransport,
//         <<<TBehaviour as NetworkBehaviour>::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::InEvent,
//         <<<TBehaviour as NetworkBehaviour>::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::OutEvent,
//         <TBehaviour as NetworkBehaviour>::ProtocolsHandler,
//         TConnInfo,
//     >
// }


/// Substream for which a protocol has been chosen.
///
/// Implements the [`AsyncRead`](futures::io::AsyncRead) and
/// [`AsyncWrite`](futures::io::AsyncWrite) traits.
pub type NegotiatedSubstream = Negotiated<Substream<StreamMuxerBox>>;

/// Event generated by the `Swarm`.
#[derive(Debug)]
pub enum SwarmEvent<TBvEv> {
    /// Event generated by the `NetworkBehaviour`.
    Behaviour(TBvEv),
    /// We are now connected to the given peer.
    ConnectionEstablished {
        peer: PeerId,
        connection: ConnectionId,
        endpoint: ConnectedPoint,
    },
    /// We are now disconnected from the given peer.
    ConnectionClosed {
        peer: PeerId,
        connection: ConnectionId,
        endpoint: ConnectedPoint,
    },
    /// One of our listeners has reported a new local listening address.
    NewListenAddr(Multiaddr),
    /// One of our listeners has reported the expiration of a listening address.
    ExpiredListenAddr(Multiaddr),
    /// Tried to dial an address but it ended up being unreachaable.
    UnreachableAddr {
        /// `PeerId` that we were trying to reach. `None` if we don't know in advance which peer
        /// we were trying to reach.
        peer_id: Option<PeerId>,
        /// Address that we failed to reach.
        address: Multiaddr,
        /// Error that has been encountered.
        error: Box<dyn error::Error + Send>,
    },
    /// A new dialing attempt has been initiated.
    Dialing(PeerId, ConnectionId),
}

/// Contains the state of the network, plus the way it should behave.
pub struct ExpandedSwarm<TBehaviour, TInEvent, TOutEvent, THandler, TConnInfo = PeerId>
where
    THandler: IntoProtocolsHandler,
    TConnInfo: ConnectionInfo<PeerId = PeerId>,
{
    network: Network<
        BoxTransport<(TConnInfo, StreamMuxerBox), io::Error>,
        TInEvent,
        TOutEvent,
        NodeHandlerWrapperBuilder<THandler>,
        TConnInfo,
        PeerId,
    >,

    /// Handles which nodes to connect to and how to handle the events sent back by the protocol
    /// handlers.
    behaviour: TBehaviour,

    /// List of protocols that the behaviour says it supports.
    supported_protocols: SmallVec<[Vec<u8>; 16]>,

    /// List of multiaddresses we're listening on.
    listened_addrs: SmallVec<[Multiaddr; 8]>,

    /// List of multiaddresses we're listening on, after account for external IP addresses and
    /// similar mechanisms.
    external_addrs: Addresses,

    /// List of nodes for which we deny any incoming connection.
    banned_peers: HashSet<PeerId>,

    /// Pending event to be delivered to connection handlers
    /// (or dropped if the peer disconnected) before the `behaviour`
    /// can be polled again.
    pending_event: Option<(PeerId, PendingNotifyHandler, TInEvent)>
}

impl<TBehaviour, TInEvent, TOutEvent, THandler, TConnInfo> Deref for
    ExpandedSwarm<TBehaviour, TInEvent, TOutEvent, THandler, TConnInfo>
where
    THandler: IntoProtocolsHandler,
    TConnInfo: ConnectionInfo<PeerId = PeerId>,
{
    type Target = TBehaviour;

    fn deref(&self) -> &Self::Target {
        &self.behaviour
    }
}

impl<TBehaviour, TInEvent, TOutEvent, THandler, TConnInfo> DerefMut for
    ExpandedSwarm<TBehaviour, TInEvent, TOutEvent, THandler, TConnInfo>
where
    THandler: IntoProtocolsHandler,
    TConnInfo: ConnectionInfo<PeerId = PeerId>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.behaviour
    }
}

impl<TBehaviour, TInEvent, TOutEvent, THandler, TConnInfo> Unpin for
    ExpandedSwarm<TBehaviour, TInEvent, TOutEvent, THandler, TConnInfo>
where
    THandler: IntoProtocolsHandler,
    TConnInfo: ConnectionInfo<PeerId = PeerId>,
{
}

impl<TBehaviour, TInEvent, TOutEvent, THandler, TConnInfo>
    ExpandedSwarm<TBehaviour, TInEvent, TOutEvent, THandler, TConnInfo>
where TBehaviour: NetworkBehaviour<ProtocolsHandler = THandler>,
      TInEvent: Clone + Send + 'static,
      TOutEvent: Send + 'static,
      TConnInfo: ConnectionInfo<PeerId = PeerId> + fmt::Debug + Clone + Send + 'static,
      THandler: IntoProtocolsHandler + Send + 'static,
      THandler::Handler: ProtocolsHandler<InEvent = TInEvent, OutEvent = TOutEvent>,
{
    /// Builds a new `Swarm`.
    pub fn new<TTransport, TMuxer>(transport: TTransport, behaviour: TBehaviour, local_peer_id: PeerId) -> Self
    where
        TMuxer: StreamMuxer + Send + Sync + 'static,
        TMuxer::OutboundSubstream: Send + 'static,
        <TMuxer as StreamMuxer>::OutboundSubstream: Send + 'static,
        <TMuxer as StreamMuxer>::Substream: Send + 'static,
        TTransport: Transport<Output = (TConnInfo, TMuxer)> + Clone + Send + Sync + 'static,
        TTransport::Error: Send + Sync + 'static,
        TTransport::Listener: Send + 'static,
        TTransport::ListenerUpgrade: Send + 'static,
        TTransport::Dial: Send + 'static,
    {
        SwarmBuilder::new(transport, behaviour, local_peer_id)
            .build()
    }

    /// Returns information about the [`Network`] underlying the `Swarm`.
    pub fn network_info(me: &Self) -> NetworkInfo {
        me.network.info()
    }

    /// Starts listening on the given address.
    ///
    /// Returns an error if the address is not supported.
    pub fn listen_on(me: &mut Self, addr: Multiaddr) -> Result<ListenerId, TransportError<io::Error>> {
        me.network.listen_on(addr)
    }

    /// Remove some listener.
    ///
    /// Returns `Ok(())` if there was a listener with this ID.
    pub fn remove_listener(me: &mut Self, id: ListenerId) -> Result<(), ()> {
        me.network.remove_listener(id)
    }

    /// Tries to dial the given address.
    ///
    /// Returns an error if the address is not supported.
    pub fn dial_addr(me: &mut Self, addr: Multiaddr) -> Result<(), DialError<io::Error>> {
        let handler = me.behaviour.new_handler();
        me.network.dial(&addr, handler.into_node_handler_builder()).map(|_id| ())
    }

    /// Tries to initiate a dialing attempt to the given peer, if its status
    /// is satisfied by the given condition.
    ///
    /// If there is an ongoing dialing attempt, the current addresses of the
    /// peer, as reported by [`NetworkBehaviour::addresses_of_peer`] are added
    /// to the ongoing dialing attempt, ignoring duplicates.
    ///
    /// If no addresses are reported for the peer by the behaviour, this is a noop.
    ///
    /// Returns `Some` with the connection ID of the first connection
    /// attempt if a new dialing attempt has been initiated.
    pub fn dial(me: &mut Self, peer_id: PeerId, condition: DialPeerCondition) -> Option<ConnectionId> {
        let addrs = me.behaviour.addresses_of_peer(&peer_id);
        match me.network.peer(peer_id.clone()) {
            Peer::Disconnected(peer) => {
                // A new dialing attempt to a disconnected peer is always started,
                // for any `condition`.
                let mut addrs = addrs.into_iter();
                if let Some(first) = addrs.next() {
                    let handler = me.behaviour.new_handler().into_node_handler_builder();
                    match peer.connect(first, addrs, handler) {
                        Ok(mut peer) => return Some(peer.connection().id()),
                        Err(error) => {
                            log::debug!(
                                "New dialing attempt to disconnected peer {:?} failed: {:?}.",
                                peer_id, error);
                            me.behaviour.inject_dial_failure(&peer_id);
                        }
                    }
                }
                None
            },
            Peer::Dialing(mut peer) => {
                peer.connection().add_addresses(addrs);
                None
            },
            Peer::Connected(peer) => {
                // A new dialing attempt to a connected peer is started only if
                // the condition permits it.
                match condition {
                    DialPeerCondition::NotDialing if !peer.is_dialing() => {
                        let mut addrs = addrs.into_iter();
                        if let Some(first) = addrs.next() {
                            let handler = me.behaviour.new_handler().into_node_handler_builder();
                            match peer.connect(first, addrs, handler) {
                                Ok(mut peer) => return Some(peer.connection().id()),
                                Err(error) => {
                                    log::debug!(
                                        "New dialing attempt to connected peer {:?} failed: {:?}.",
                                        peer_id, error);
                                    me.behaviour.inject_dial_failure(&peer_id);
                                }
                            }
                        }
                        None
                    }
                    _ => None
                }
            }
            Peer::Local => {
                log::debug!("Ignoring request to dial the local peer.");
                None
            }
        }
    }

    /// Returns an iterator that produces the list of addresses we're listening on.
    pub fn listeners(me: &Self) -> impl Iterator<Item = &Multiaddr> {
        me.network.listen_addrs()
    }

    /// Returns an iterator that produces the list of addresses that other nodes can use to reach
    /// us.
    pub fn external_addresses(me: &Self) -> impl Iterator<Item = &Multiaddr> {
        me.external_addrs.iter()
    }

    /// Returns the peer ID of the swarm passed as parameter.
    pub fn local_peer_id(me: &Self) -> &PeerId {
        &me.network.local_peer_id()
    }

    /// Adds an external address.
    ///
    /// An external address is an address we are listening on but that accounts for things such as
    /// NAT traversal.
    pub fn add_external_address(me: &mut Self, addr: Multiaddr) {
        me.external_addrs.add(addr)
    }

    /// Returns the connection info for an arbitrary connection with the peer, or `None`
    /// if there is no connection to that peer.
    // TODO: should take &self instead of &mut self, but the API in network requires &mut
    pub fn connection_info(me: &mut Self, peer_id: &PeerId) -> Option<TConnInfo> {
        if let Some(mut n) = me.network.peer(peer_id.clone()).into_connected() {
            Some(n.some_connection().info().clone())
        } else {
            None
        }
    }

    /// Bans a peer by its peer ID.
    ///
    /// Any incoming connection and any dialing attempt will immediately be rejected.
    /// This function has no effect is the peer is already banned.
    pub fn ban_peer_id(me: &mut Self, peer_id: PeerId) {
        me.banned_peers.insert(peer_id.clone());
        if let Some(c) = me.network.peer(peer_id).into_connected() {
            c.disconnect();
        }
    }

    /// Unbans a peer.
    pub fn unban_peer_id(me: &mut Self, peer_id: PeerId) {
        me.banned_peers.remove(&peer_id);
    }

    /// Returns the next event that happens in the `Swarm`.
    ///
    /// Includes events from the `NetworkBehaviour` but also events about the connections status.
    pub async fn next_event(&mut self) -> SwarmEvent<TBehaviour::OutEvent> {
        future::poll_fn(move |cx| ExpandedSwarm::poll_next_event(Pin::new(self), cx)).await
    }

    /// Returns the next event produced by the [`NetworkBehaviour`].
    pub async fn next(&mut self) -> TBehaviour::OutEvent {
        future::poll_fn(move |cx| {
            loop {
                let event = futures::ready!(ExpandedSwarm::poll_next_event(Pin::new(self), cx));
                if let SwarmEvent::Behaviour(event) = event {
                    return Poll::Ready(event);
                }
            }
        }).await
    }

    /// Internal function used by everything event-related.
    ///
    /// Polls the `Swarm` for the next event.
    fn poll_next_event(mut self: Pin<&mut Self>, cx: &mut Context)
        -> Poll<SwarmEvent<TBehaviour::OutEvent>>
    {
        // We use a `this` variable because the compiler can't mutably borrow multiple times
        // across a `Deref`.
        let this = &mut *self;

        loop {
            let mut network_not_ready = false;

            // First let the network make progress.
            match this.network.poll(cx) {
                Poll::Pending => network_not_ready = true,
                Poll::Ready(NetworkEvent::ConnectionEvent { connection, event }) => {
                    let peer = connection.peer_id().clone();
                    let connection = connection.id();
                    this.behaviour.inject_event(peer, connection, event);
                },
                Poll::Ready(NetworkEvent::ConnectionEstablished { connection, num_established }) => {
                    let peer = connection.peer_id().clone();
                    if this.banned_peers.contains(&peer) {
                        this.network.peer(peer)
                            .into_connected()
                            .expect("the Network just notified us that we were connected; QED")
                            .disconnect();
                    } else {
                        log::debug!("Connection established: {:?}; Total (peer): {}.",
                            connection.connected(), num_established);
                        let endpoint = connection.endpoint().clone();
                        this.behaviour.inject_connection_established(&peer, &connection.id(), &endpoint);
                        if num_established == 1 {
                            this.behaviour.inject_connected(&peer);
                        }
                        return Poll::Ready(SwarmEvent::ConnectionEstablished {
                            peer, connection: connection.id(), endpoint
                        });
                    }
                },
                Poll::Ready(NetworkEvent::ConnectionError { id, connected, error, num_established }) => {
                    log::debug!("Connection {:?} closed: {:?}", connected, error);
                    let info = connected.info;
                    let endpoint = connected.endpoint;
                    this.behaviour.inject_connection_closed(info.peer_id(), &id, &endpoint);
                    if num_established == 0 {
                        this.behaviour.inject_disconnected(info.peer_id());
                    }
                    return Poll::Ready(SwarmEvent::ConnectionClosed {
                        peer: info.peer_id().clone(),
                        connection: id,
                        endpoint,
                    });
                },
                Poll::Ready(NetworkEvent::IncomingConnection(incoming)) => {
                    let handler = this.behaviour.new_handler();
                    if let Err(e) = incoming.accept(handler.into_node_handler_builder()) {
                        log::warn!("Incoming connection rejected: {:?}", e);
                    }
                },
                Poll::Ready(NetworkEvent::NewListenerAddress { listener_id, listen_addr }) => {
                    log::debug!("Listener {:?}; New address: {:?}", listener_id, listen_addr);
                    if !this.listened_addrs.contains(&listen_addr) {
                        this.listened_addrs.push(listen_addr.clone())
                    }
                    this.behaviour.inject_new_listen_addr(&listen_addr);
                    return Poll::Ready(SwarmEvent::NewListenAddr(listen_addr));
                }
                Poll::Ready(NetworkEvent::ExpiredListenerAddress { listener_id, listen_addr }) => {
                    log::debug!("Listener {:?}; Expired address {:?}.", listener_id, listen_addr);
                    this.listened_addrs.retain(|a| a != &listen_addr);
                    this.behaviour.inject_expired_listen_addr(&listen_addr);
                    return Poll::Ready(SwarmEvent::ExpiredListenAddr(listen_addr));
                }
                Poll::Ready(NetworkEvent::ListenerClosed { listener_id, addresses, reason }) => {
                    log::debug!("Listener {:?}; Closed by {:?}.", listener_id, reason);
                    for addr in addresses.iter() {
                        this.behaviour.inject_expired_listen_addr(addr);
                    }
                    this.behaviour.inject_listener_closed(listener_id);
                }
                Poll::Ready(NetworkEvent::ListenerError { listener_id, error }) =>
                    this.behaviour.inject_listener_error(listener_id, &error),
                Poll::Ready(NetworkEvent::IncomingConnectionError { error, .. }) => {
                    log::debug!("Incoming connection failed: {:?}", error);
                },
                Poll::Ready(NetworkEvent::DialError { peer_id, multiaddr, error, attempts_remaining }) => {
                    log::debug!(
                        "Connection attempt to {:?} via {:?} failed with {:?}. Attempts remaining: {}.",
                        peer_id, multiaddr, error, attempts_remaining);
                    this.behaviour.inject_addr_reach_failure(Some(&peer_id), &multiaddr, &error);
                    if attempts_remaining == 0 {
                        this.behaviour.inject_dial_failure(&peer_id);
                    }
                    return Poll::Ready(SwarmEvent::UnreachableAddr {
                        peer_id: Some(peer_id.clone()),
                        address: multiaddr,
                        error: Box::new(error),
                    });
                },
                Poll::Ready(NetworkEvent::UnknownPeerDialError { multiaddr, error, .. }) => {
                    log::debug!("Connection attempt to address {:?} of unknown peer failed with {:?}",
                        multiaddr, error);
                    this.behaviour.inject_addr_reach_failure(None, &multiaddr, &error);
                    return Poll::Ready(SwarmEvent::UnreachableAddr {
                        peer_id: None,
                        address: multiaddr,
                        error: Box::new(error),
                    });
                },
            }

            // After the network had a chance to make progress, try to deliver
            // the pending event emitted by the behaviour in the previous iteration
            // to the connection handler(s). The pending event must be delivered
            // before polling the behaviour again. If the targeted peer
            // meanwhie disconnected, the event is discarded.
            if let Some((peer_id, handler, event)) = this.pending_event.take() {
                if let Some(mut peer) = this.network.peer(peer_id.clone()).into_connected() {
                    match handler {
                        PendingNotifyHandler::One(conn_id) =>
                            if let Some(mut conn) = peer.connection(conn_id) {
                                if let Some(event) = notify_one(&mut conn, event, cx) {
                                    this.pending_event = Some((peer_id, handler, event));
                                    return Poll::Pending
                                }
                            },
                        PendingNotifyHandler::Any(ids) => {
                            if let Some((event, ids)) = notify_any(ids, &mut peer, event, cx) {
                                let handler = PendingNotifyHandler::Any(ids);
                                this.pending_event = Some((peer_id, handler, event));
                                return Poll::Pending
                            }
                        }
                        PendingNotifyHandler::All(ids) => {
                            if let Some((event, ids)) = notify_all(ids, &mut peer, event, cx) {
                                let handler = PendingNotifyHandler::All(ids);
                                this.pending_event = Some((peer_id, handler, event));
                                return Poll::Pending
                            }
                        }
                    }
                }
            }

            debug_assert!(this.pending_event.is_none());

            let behaviour_poll = {
                let mut parameters = SwarmPollParameters {
                    local_peer_id: &mut this.network.local_peer_id(),
                    supported_protocols: &this.supported_protocols,
                    listened_addrs: &this.listened_addrs,
                    external_addrs: &this.external_addrs
                };
                this.behaviour.poll(cx, &mut parameters)
            };

            match behaviour_poll {
                Poll::Pending if network_not_ready => return Poll::Pending,
                Poll::Pending => (),
                Poll::Ready(NetworkBehaviourAction::GenerateEvent(event)) => {
                    return Poll::Ready(SwarmEvent::Behaviour(event))
                },
                Poll::Ready(NetworkBehaviourAction::DialAddress { address }) => {
                    let _ = ExpandedSwarm::dial_addr(&mut *this, address);
                },
                Poll::Ready(NetworkBehaviourAction::DialPeer { peer_id, condition }) => {
                    if this.banned_peers.contains(&peer_id) {
                        this.behaviour.inject_dial_failure(&peer_id);
                    } else {
                        if let Some(id) = ExpandedSwarm::dial(this, peer_id.clone(), condition) {
                            return Poll::Ready(SwarmEvent::Dialing(peer_id, id))
                        }
                    }
                },
                Poll::Ready(NetworkBehaviourAction::NotifyHandler { peer_id, handler, event }) => {
                    if let Some(mut peer) = this.network.peer(peer_id.clone()).into_connected() {
                        match handler {
                            NotifyHandler::One(connection) => {
                                if let Some(mut conn) = peer.connection(connection) {
                                    if let Some(event) = notify_one(&mut conn, event, cx) {
                                        let handler = PendingNotifyHandler::One(connection);
                                        this.pending_event = Some((peer_id, handler, event));
                                        return Poll::Pending
                                    }
                                }
                            }
                            NotifyHandler::Any => {
                                let ids = peer.connections().into_ids().collect();
                                if let Some((event, ids)) = notify_any(ids, &mut peer, event, cx) {
                                    let handler = PendingNotifyHandler::Any(ids);
                                    this.pending_event = Some((peer_id, handler, event));
                                    return Poll::Pending
                                }
                            }
                            NotifyHandler::All => {
                                let ids = peer.connections().into_ids().collect();
                                if let Some((event, ids)) = notify_all(ids, &mut peer, event, cx) {
                                    let handler = PendingNotifyHandler::All(ids);
                                    this.pending_event = Some((peer_id, handler, event));
                                    return Poll::Pending
                                }
                            }
                        }
                    }
                },
                Poll::Ready(NetworkBehaviourAction::ReportObservedAddr { address }) => {
                    for addr in this.network.address_translation(&address) {
                        if this.external_addrs.iter().all(|a| *a != addr) {
                            this.behaviour.inject_new_external_addr(&addr);
                        }
                        this.external_addrs.add(addr);
                    }
                },
            }
        }
    }
}

/// Connections to notify of a pending event.
///
/// The connection IDs to notify of an event are captured at the time
/// the behaviour emits the event, in order not to forward the event
/// to new connections which the behaviour may not have been aware of
/// at the time it issued the request for sending it.
enum PendingNotifyHandler {
    One(ConnectionId),
    Any(SmallVec<[ConnectionId; 10]>),
    All(SmallVec<[ConnectionId; 10]>),
}

/// Notify a single connection of an event.
///
/// Returns `Some` with the given event if the connection is not currently
/// ready to receive another event, in which case the current task is
/// scheduled to be woken up.
///
/// Returns `None` if the connection is closing or the event has been
/// successfully sent, in either case the event is consumed.
fn notify_one<'a, TInEvent, TConnInfo, TPeerId>(
    conn: &mut EstablishedConnection<'a, TInEvent, TConnInfo, TPeerId>,
    event: TInEvent,
    cx: &mut Context,
) -> Option<TInEvent>
where
    TPeerId: Eq + std::hash::Hash + Clone,
    TConnInfo: ConnectionInfo<PeerId = TPeerId>
{
    match conn.poll_ready_notify_handler(cx) {
        Poll::Pending => Some(event),
        Poll::Ready(Err(())) => None, // connection is closing
        Poll::Ready(Ok(())) => {
            // Can now only fail if connection is closing.
            let _ = conn.notify_handler(event);
            None
        }
    }
}

/// Notify any one of a given list of connections of a peer of an event.
///
/// Returns `Some` with the given event and a new list of connections if
/// none of the given connections was able to receive the event but at
/// least one of them is not closing, in which case the current task
/// is scheduled to be woken up. The returned connections are those which
/// may still become ready to receive another event.
///
/// Returns `None` if either all connections are closing or the event
/// was successfully sent to a handler, in either case the event is consumed.
fn notify_any<'a, TTrans, TInEvent, TOutEvent, THandler, TConnInfo, TPeerId>(
    ids: SmallVec<[ConnectionId; 10]>,
    peer: &mut ConnectedPeer<'a, TTrans, TInEvent, TOutEvent, THandler, TConnInfo, TPeerId>,
    event: TInEvent,
    cx: &mut Context,
) -> Option<(TInEvent, SmallVec<[ConnectionId; 10]>)>
where
    TTrans: Transport,
    THandler: IntoConnectionHandler<TConnInfo>,
    TPeerId: Eq + Hash + Clone,
    TConnInfo: ConnectionInfo<PeerId = TPeerId>
{
    let mut pending = SmallVec::new();
    let mut event = Some(event); // (1)
    for id in ids.into_iter() {
        if let Some(mut conn) = peer.connection(id) {
            match conn.poll_ready_notify_handler(cx) {
                Poll::Pending => pending.push(id),
                Poll::Ready(Err(())) => {} // connection is closing
                Poll::Ready(Ok(())) => {
                    let e = event.take().expect("by (1),(2)");
                    if let Err(e) = conn.notify_handler(e) {
                        event = Some(e) // (2)
                    } else {
                        break
                    }
                }
            }
        }
    }

    event.and_then(|e|
        if !pending.is_empty() {
            Some((e, pending))
        } else {
            None
        })
}

/// Notify all of the given connections of a peer of an event.
///
/// Returns `Some` with the given event and a new list of connections if
/// at least one of the given connections is currently not able to receive
/// the event, in which case the current task is scheduled to be woken up and
/// the returned connections are those which still need to receive the event.
///
/// Returns `None` if all connections are either closing or the event
/// was successfully sent to all handlers whose connections are not closing,
/// in either case the event is consumed.
fn notify_all<'a, TTrans, TInEvent, TOutEvent, THandler, TConnInfo, TPeerId>(
    ids: SmallVec<[ConnectionId; 10]>,
    peer: &mut ConnectedPeer<'a, TTrans, TInEvent, TOutEvent, THandler, TConnInfo, TPeerId>,
    event: TInEvent,
    cx: &mut Context,
) -> Option<(TInEvent, SmallVec<[ConnectionId; 10]>)>
where
    TTrans: Transport,
    TInEvent: Clone,
    THandler: IntoConnectionHandler<TConnInfo>,
    TPeerId: Eq + Hash + Clone,
    TConnInfo: ConnectionInfo<PeerId = TPeerId>
{
    if ids.len() == 1 {
        if let Some(mut conn) = peer.connection(ids[0]) {
            return notify_one(&mut conn, event, cx).map(|e| (e, ids))
        }
    }

    let mut pending = SmallVec::new();
    for id in ids.into_iter() {
        if let Some(mut conn) = peer.connection(id) {
            match conn.poll_ready_notify_handler(cx) {
                Poll::Pending => pending.push(id),
                Poll::Ready(Ok(())) => {
                    // Can now only fail due to the connection suddenly closing,
                    // which we ignore.
                    let _ = conn.notify_handler(event.clone());
                },
                Poll::Ready(Err(())) => {} // connection is closing
            }
        }
    }

    if !pending.is_empty() {
        return Some((event, pending))
    }

    None
}

impl<TBehaviour, TInEvent, TOutEvent, THandler, TConnInfo> Stream for
    ExpandedSwarm<TBehaviour, TInEvent, TOutEvent, THandler, TConnInfo>
where TBehaviour: NetworkBehaviour<ProtocolsHandler = THandler>,
      THandler: IntoProtocolsHandler + Send + 'static,
      TInEvent: Clone + Send + 'static,
      TOutEvent: Send + 'static,
      THandler::Handler: ProtocolsHandler<InEvent = TInEvent, OutEvent = TOutEvent>,
      TConnInfo: ConnectionInfo<PeerId = PeerId> + fmt::Debug + Clone + Send + 'static,
{
    type Item = TBehaviour::OutEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        loop {
            let event = futures::ready!(ExpandedSwarm::poll_next_event(self.as_mut(), cx));
            if let SwarmEvent::Behaviour(event) = event {
                return Poll::Ready(Some(event));
            }
        }
    }
}

/// the stream of behaviour events never terminates, so we can implement fused for it
impl<TBehaviour, TInEvent, TOutEvent, THandler, TConnInfo> FusedStream for
    ExpandedSwarm<TBehaviour, TInEvent, TOutEvent, THandler, TConnInfo>
where TBehaviour: NetworkBehaviour<ProtocolsHandler = THandler>,
      THandler: IntoProtocolsHandler + Send + 'static,
      TInEvent: Clone + Send + 'static,
      TOutEvent: Send + 'static,
      THandler::Handler: ProtocolsHandler<InEvent = TInEvent, OutEvent = TOutEvent>,
      TConnInfo: ConnectionInfo<PeerId = PeerId> + fmt::Debug + Clone + Send + 'static,
{
    fn is_terminated(&self) -> bool {
        false
    }
}

/// Parameters passed to `poll()`, that the `NetworkBehaviour` has access to.
// TODO: #[derive(Debug)]
pub struct SwarmPollParameters<'a> {
    local_peer_id: &'a PeerId,
    supported_protocols: &'a [Vec<u8>],
    listened_addrs: &'a [Multiaddr],
    external_addrs: &'a Addresses,
}

impl<'a> PollParameters for SwarmPollParameters<'a> {
    type SupportedProtocolsIter = std::vec::IntoIter<Vec<u8>>;
    type ListenedAddressesIter = std::vec::IntoIter<Multiaddr>;
    type ExternalAddressesIter = AddressIntoIter;

    fn supported_protocols(&self) -> Self::SupportedProtocolsIter {
        self.supported_protocols.to_vec().into_iter()
    }

    fn listened_addresses(&self) -> Self::ListenedAddressesIter {
        self.listened_addrs.to_vec().into_iter()
    }

    fn external_addresses(&self) -> Self::ExternalAddressesIter {
        self.external_addrs.clone().into_iter()
    }

    fn local_peer_id(&self) -> &PeerId {
        self.local_peer_id
    }
}

/// A `SwarmBuilder` provides an API for configuring and constructing a `Swarm`,
/// including the underlying [`Network`].
pub struct SwarmBuilder<TBehaviour, TConnInfo> {
    local_peer_id: PeerId,
    transport: BoxTransport<(TConnInfo, StreamMuxerBox), io::Error>,
    behaviour: TBehaviour,
    network_config: Option<NetworkConfig>,
}

impl<TBehaviour, TConnInfo> SwarmBuilder<TBehaviour, TConnInfo>
where TBehaviour: NetworkBehaviour,
      TConnInfo: ConnectionInfo<PeerId = PeerId> + fmt::Debug + Clone + Send + 'static,
{
    /// Creates a new `SwarmBuilder` from the given transport, behaviour and
    /// local peer ID. The `Swarm` with its underlying `Network` is obtained
    /// via [`SwarmBuilder::build`].
    pub fn new<TTrans, TMuxer>(transport: TTrans, behaviour: TBehaviour, local_peer_id: PeerId) -> Self
    where
        TMuxer: StreamMuxer + Send + Sync + 'static,
        TMuxer::OutboundSubstream: Send + 'static,
        <TMuxer as StreamMuxer>::OutboundSubstream: Send + 'static,
        <TMuxer as StreamMuxer>::Substream: Send + 'static,
        TTrans: Transport<Output = (TConnInfo, TMuxer)> + Clone + Send + Sync + 'static,
        TTrans::Error: Send + Sync + 'static,
        TTrans::Listener: Send + 'static,
        TTrans::ListenerUpgrade: Send + 'static,
        TTrans::Dial: Send + 'static,
    {
        let transport = transport
            .map(|(conn_info, muxer), _| (conn_info, StreamMuxerBox::new(muxer)))
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))
            .boxed();

        SwarmBuilder {
            local_peer_id,
            transport,
            behaviour,
            network_config: None,
        }
    }

    /// Configures the underlying network for the swarm.
    pub fn network_config(mut self, cfg: NetworkConfig) -> Self {
        self.network_config = Some(cfg);
        self
    }

    /// Builds a `Swarm` with the current configuration.
    pub fn build(mut self) -> Swarm<TBehaviour, TConnInfo> {
        let supported_protocols = self.behaviour
            .new_handler()
            .inbound_protocol()
            .protocol_info()
            .into_iter()
            .map(|info| info.protocol_name().to_vec())
            .collect();

        let mut network_cfg = self.network_config.unwrap_or_default();

        // If no executor has been explicitly configured, try to set up a thread pool.
        if network_cfg.executor().is_none() {
            struct PoolWrapper(ThreadPool);
            impl Executor for PoolWrapper {
                fn exec(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) {
                    self.0.spawn_ok(f)
                }
            }

            match ThreadPoolBuilder::new()
                .name_prefix("libp2p-swarm-task-")
                .create()
                .map(|tp| Box::new(PoolWrapper(tp)) as Box<_>)
            {
                Ok(executor) => { network_cfg.set_executor(Box::new(executor)); },
                Err(err) => log::warn!("Failed to create executor thread pool: {:?}", err)
            }
        }

        let network = Network::new(self.transport, self.local_peer_id, network_cfg);

        ExpandedSwarm {
            network,
            behaviour: self.behaviour,
            supported_protocols,
            listened_addrs: SmallVec::new(),
            external_addrs: Addresses::default(),
            banned_peers: HashSet::new(),
            pending_event: None
        }
    }
}

/// Dummy implementation of [`NetworkBehaviour`] that doesn't do anything.
#[derive(Clone, Default)]
pub struct DummyBehaviour {
}

impl NetworkBehaviour for DummyBehaviour {
    type ProtocolsHandler = protocols_handler::DummyProtocolsHandler;
    type OutEvent = void::Void;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        protocols_handler::DummyProtocolsHandler::default()
    }

    fn addresses_of_peer(&mut self, _: &PeerId) -> Vec<Multiaddr> {
        Vec::new()
    }

    fn inject_connected(&mut self, _: &PeerId) {}

    fn inject_connection_established(&mut self, _: &PeerId, _: &ConnectionId, _: &ConnectedPoint) {}

    fn inject_disconnected(&mut self, _: &PeerId) {}

    fn inject_connection_closed(&mut self, _: &PeerId, _: &ConnectionId, _: &ConnectedPoint) {}

    fn inject_event(&mut self, _: PeerId, _: ConnectionId,
        _: <Self::ProtocolsHandler as ProtocolsHandler>::OutEvent) {}

    fn poll(&mut self, _: &mut Context, _: &mut impl PollParameters) ->
        Poll<NetworkBehaviourAction<<Self::ProtocolsHandler as
        ProtocolsHandler>::InEvent, Self::OutEvent>>
    {
        Poll::Pending
    }

}

#[cfg(test)]
mod tests {
    use crate::{DummyBehaviour, SwarmBuilder};
    use libp2p_core::{
        PeerId,
        PublicKey,
        identity,
        network::NetworkConfig,
        transport::dummy::{DummyStream, DummyTransport}
    };
    use libp2p_mplex::Multiplex;

    fn get_random_id() -> PublicKey {
        identity::Keypair::generate_ed25519().public()
    }

    #[test]
    fn test_build_swarm() {
        let id = get_random_id();
        let transport = DummyTransport::<(PeerId, Multiplex<DummyStream>)>::new();
        let behaviour = DummyBehaviour {};
        let mut network_cfg = NetworkConfig::default();
        network_cfg.set_incoming_limit(4);
        let swarm = SwarmBuilder::new(transport, behaviour, id.into())
            .network_config(network_cfg)
            .build();
        assert_eq!(swarm.network.incoming_limit(), Some(4));
    }

    #[test]
    fn test_build_swarm_with_max_listeners_none() {
        let id = get_random_id();
        let transport = DummyTransport::<(PeerId, Multiplex<DummyStream>)>::new();
        let swarm = SwarmBuilder::new(transport, DummyBehaviour {}, id.into()).build();
        assert!(swarm.network.incoming_limit().is_none())
    }
}
