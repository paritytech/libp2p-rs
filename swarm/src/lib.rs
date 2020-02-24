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
    PollParameters
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
        peer::PeerState,
    },
    upgrade::ProtocolName,
};
use registry::{Addresses, AddressIntoIter};
use smallvec::SmallVec;
use std::{error, fmt, io, ops::{Deref, DerefMut}, pin::Pin, task::{Context, Poll}};
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
    Connected(PeerId),
    /// We are now disconnected from the given peer.
    Disconnected(PeerId),
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
    /// Startng to try to reach the given peer.
    StartConnect(PeerId),
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

    /// Pending event message to be delivered.
    send_event_to_complete: Option<(PeerId, Option<ConnectionId>, TInEvent)>
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
      TInEvent: Send + 'static,
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

    /// Tries to reach the given peer using the elements in the topology.
    ///
    /// Has no effect if we are already connected to that peer, or if no address is known for the
    /// peer.
    pub fn dial(me: &mut Self, peer_id: PeerId) {
        let addrs = me.behaviour.addresses_of_peer(&peer_id);
        match me.network.peer(peer_id.clone()) {
            Peer::Disconnected(peer) => {
                let mut addrs = addrs.into_iter();
                if let Some(first) = addrs.next() {
                    let handler = me.behaviour.new_handler().into_node_handler_builder();
                    if peer.connect(first, addrs, handler).is_err() {
                        me.behaviour.inject_dial_failure(&peer_id);
                    }
                }
            },
            Peer::Dialing(mut peer) => {
                peer.connection().add_addresses(addrs)
            },
            Peer::Connected(_) | Peer::Local => {}
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

        'poll: loop {
            let mut network_not_ready = false;

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
                    } else if num_established == 1 {
                        let endpoint = connection.endpoint().clone();
                        this.behaviour.inject_connected(peer.clone(), endpoint);
                        return Poll::Ready(SwarmEvent::Connected(peer));
                    }
                },
                Poll::Ready(NetworkEvent::ConnectionError { connected, error, num_established }) => {
                    log::error!("Connection {:?} closed by {:?}", connected, error);
                    if num_established == 0 {
                        let peer = connected.peer_id().clone();
                        let endpoint = connected.endpoint;
                        this.behaviour.inject_disconnected(&peer, endpoint);
                        return Poll::Ready(SwarmEvent::Disconnected(peer));
                    }
                },
                Poll::Ready(NetworkEvent::IncomingConnection(incoming)) => {
                    let handler = this.behaviour.new_handler();
                    if let Err(e) = incoming.accept(handler.into_node_handler_builder()) {
                        log::warn!("Incoming connection rejected: {:?}", e);
                    }
                },
                Poll::Ready(NetworkEvent::NewListenerAddress { listen_addr, .. }) => {
                    if !this.listened_addrs.contains(&listen_addr) {
                        this.listened_addrs.push(listen_addr.clone())
                    }
                    this.behaviour.inject_new_listen_addr(&listen_addr);
                    return Poll::Ready(SwarmEvent::NewListenAddr(listen_addr));
                }
                Poll::Ready(NetworkEvent::ExpiredListenerAddress { listen_addr, .. }) => {
                    this.listened_addrs.retain(|a| a != &listen_addr);
                    this.behaviour.inject_expired_listen_addr(&listen_addr);
                    return Poll::Ready(SwarmEvent::ExpiredListenAddr(listen_addr));
                }
                Poll::Ready(NetworkEvent::ListenerClosed { listener_id, .. }) =>
                    this.behaviour.inject_listener_closed(listener_id),
                Poll::Ready(NetworkEvent::ListenerError { listener_id, error }) =>
                    this.behaviour.inject_listener_error(listener_id, &error),
                Poll::Ready(NetworkEvent::IncomingConnectionError { .. }) => {},
                Poll::Ready(NetworkEvent::DialError { peer_id, multiaddr, error, new_state }) => {
                    this.behaviour.inject_addr_reach_failure(Some(&peer_id), &multiaddr, &error);
                    if let PeerState::Disconnected = new_state {
                        this.behaviour.inject_dial_failure(&peer_id);
                    }
                    return Poll::Ready(SwarmEvent::UnreachableAddr {
                        peer_id: Some(peer_id.clone()),
                        address: multiaddr,
                        error: Box::new(error),
                    });
                },
                Poll::Ready(NetworkEvent::UnknownPeerDialError { multiaddr, error, .. }) => {
                    this.behaviour.inject_addr_reach_failure(None, &multiaddr, &error);
                    return Poll::Ready(SwarmEvent::UnreachableAddr {
                        peer_id: None,
                        address: multiaddr,
                        error: Box::new(error),
                    });
                },
            }

            // Try to deliver pending event.
            if let Some((peer_id, connection_id, event)) = this.send_event_to_complete.take() {
                if let Some(mut peer) = this.network.peer(peer_id.clone()).into_connected() {
                    if let Some(conn_id) = connection_id {
                        if let Some(mut conn) = peer.connection(conn_id) {
                            if let Some(event) = conn.try_notify_handler(event, cx) {
                                this.send_event_to_complete = Some((peer_id, connection_id, event));
                                return Poll::Pending
                            }
                        }
                    } else {
                        let mut connections = peer.connections();
                        let mut event = Some(event); // (*)
                        while let Some(mut conn) = connections.next() {
                            if conn.poll_ready_notify_handler(cx).is_ready() {
                                conn.notify_handler(event.take().expect("by (*)"));
                                break
                            }
                        }
                        if let Some(event) = event.take() {
                            this.send_event_to_complete = Some((peer_id, None, event));
                            return Poll::Pending
                        }
                    }
                }
            }

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
                Poll::Ready(NetworkBehaviourAction::DialPeer { peer_id }) => {
                    if this.banned_peers.contains(&peer_id) {
                        this.behaviour.inject_dial_failure(&peer_id);
                    } else {
                        ExpandedSwarm::dial(&mut *this, peer_id.clone());
                        return Poll::Ready(SwarmEvent::StartConnect(peer_id))
                    }
                },
                Poll::Ready(NetworkBehaviourAction::NotifyHandler { peer_id, connection, event }) => {
                    if let Some(mut peer) = this.network.peer(peer_id.clone()).into_connected() {
                        if let Some(mut conn) = peer.connection(connection) {
                            if let Some(event) = conn.try_notify_handler(event, cx) {
                                debug_assert!(this.send_event_to_complete.is_none());
                                this.send_event_to_complete = Some((peer_id, Some(connection), event));
                                return Poll::Pending;
                            }
                        }
                    }
                },
                Poll::Ready(NetworkBehaviourAction::NotifyAnyHandler { peer_id, event }) => {
                    if let Some(mut peer) = this.network.peer(peer_id.clone()).into_connected() {
                        let mut connections = peer.connections();
                        while let Some(mut conn) = connections.next() {
                            if conn.poll_ready_notify_handler(cx).is_ready() {
                                conn.notify_handler(event);
                                continue 'poll
                            }
                        }

                        debug_assert!(this.send_event_to_complete.is_none());
                        this.send_event_to_complete = Some((peer_id, None, event));
                        return Poll::Pending;
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

impl<TBehaviour, TInEvent, TOutEvent, THandler, TConnInfo> Stream for
    ExpandedSwarm<TBehaviour, TInEvent, TOutEvent, THandler, TConnInfo>
where TBehaviour: NetworkBehaviour<ProtocolsHandler = THandler>,
      THandler: IntoProtocolsHandler + Send + 'static,
      TInEvent: Send + 'static,
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
      TInEvent: Send + 'static,
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

pub struct SwarmBuilder<TBehaviour, TConnInfo> {
    local_peer_id: PeerId,
    transport: BoxTransport<(TConnInfo, StreamMuxerBox), io::Error>,
    behaviour: TBehaviour,
    network: NetworkConfig,
}

impl<TBehaviour, TConnInfo> SwarmBuilder<TBehaviour, TConnInfo>
where TBehaviour: NetworkBehaviour,
      TConnInfo: ConnectionInfo<PeerId = PeerId> + fmt::Debug + Clone + Send + 'static,
{
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
        let transport = transport
            .map(|(conn_info, muxer), _| (conn_info, StreamMuxerBox::new(muxer)))
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))
            .boxed();

        SwarmBuilder {
            local_peer_id,
            transport,
            behaviour,
            network: NetworkConfig::default(),
        }
    }

    pub fn incoming_limit(mut self, incoming_limit: usize) -> Self {
        self.network.set_pending_incoming_limit(incoming_limit);
        self
    }

    /// Sets the executor to use to spawn background tasks.
    ///
    /// By default, uses a threads pool.
    pub fn executor(mut self, executor: impl Executor + Send + 'static) -> Self {
        self.network.set_executor(Box::new(executor));
        self
    }

    /// Shortcut for calling `executor` with an object that calls the given closure.
    pub fn executor_fn(mut self, executor: impl Fn(Pin<Box<dyn Future<Output = ()> + Send>>) + Send + 'static) -> Self {
        struct SpawnImpl<F>(F);
        impl<F: Fn(Pin<Box<dyn Future<Output = ()> + Send>>)> Executor for SpawnImpl<F> {
            fn exec(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) {
                (self.0)(f)
            }
        }
        self.network.set_executor(Box::new(SpawnImpl(executor)));
        self
    }

    pub fn build(mut self) -> Swarm<TBehaviour, TConnInfo> {
        let supported_protocols = self.behaviour
            .new_handler()
            .inbound_protocol()
            .protocol_info()
            .into_iter()
            .map(|info| info.protocol_name().to_vec())
            .collect();

        // If no executor has been explicitly configured, try to set up
        // a thread pool.
        if self.network.executor().is_none() {
            struct PoolWrapper(ThreadPool);
            impl Executor for PoolWrapper {
                fn exec(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) {
                    self.0.spawn_ok(f)
                }
            }

            if let Some(executor) = ThreadPoolBuilder::new()
                .name_prefix("libp2p-task-")
                .create()
                .ok()
                .map(|tp| Box::new(PoolWrapper(tp)) as Box<_>)
            {
                self.network.set_executor(Box::new(executor));
            }
        }

        let network = Network::new(
            self.transport,
            self.local_peer_id,
            self.network,
        );

        ExpandedSwarm {
            network,
            behaviour: self.behaviour,
            supported_protocols,
            listened_addrs: SmallVec::new(),
            external_addrs: Addresses::default(),
            banned_peers: HashSet::new(),
            send_event_to_complete: None
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

    fn inject_connected(&mut self, _: PeerId, _: libp2p_core::ConnectedPoint) {}

    fn inject_disconnected(&mut self, _: &PeerId, _: libp2p_core::ConnectedPoint) {}

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
        identity,
        PeerId,
        PublicKey,
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
        let swarm = SwarmBuilder::new(transport, behaviour, id.into())
            .incoming_limit(4).build();
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
