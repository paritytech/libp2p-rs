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
#[cfg(test)]
mod test;
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
    OneShotHandlerConfig,
    SubstreamProtocol
};
pub use registry::{AddressScore, AddressRecord, AddAddressResult};

use protocols_handler::{
    NodeHandlerWrapperBuilder,
    NodeHandlerWrapperError,
};
use futures::{
    prelude::*,
    executor::ThreadPoolBuilder,
    stream::FusedStream,
};
use libp2p_core::{
    Executor,
    Transport,
    Multiaddr,
    Negotiated,
    PeerId,
    connection::{
        ConnectionError,
        ConnectionId,
        ConnectionLimit,
        ConnectedPoint,
        EstablishedConnection,
        IntoConnectionHandler,
        ListenerId,
        PendingConnectionError,
        Substream
    },
    transport::{self, TransportError},
    muxing::StreamMuxerBox,
    network::{
        self,
        ConnectionLimits,
        Network,
        NetworkInfo,
        NetworkEvent,
        NetworkConfig,
        peer::ConnectedPeer,
    },
    upgrade::{ProtocolName},
};
use registry::{Addresses, AddressIntoIter};
use smallvec::SmallVec;
use std::{error, fmt, io, pin::Pin, task::{Context, Poll}};
use std::collections::HashSet;
use std::num::{NonZeroU32, NonZeroUsize};
use upgrade::UpgradeInfoSend as _;

/// Contains the state of the network, plus the way it should behave.
pub type Swarm<TBehaviour> = ExpandedSwarm<
    TBehaviour,
    <<<TBehaviour as NetworkBehaviour>::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::InEvent,
    <<<TBehaviour as NetworkBehaviour>::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::OutEvent,
    <TBehaviour as NetworkBehaviour>::ProtocolsHandler,
>;

/// Substream for which a protocol has been chosen.
///
/// Implements the [`AsyncRead`](futures::io::AsyncRead) and
/// [`AsyncWrite`](futures::io::AsyncWrite) traits.
pub type NegotiatedSubstream = Negotiated<Substream<StreamMuxerBox>>;

/// Event generated by the `Swarm`.
#[derive(Debug)]
pub enum SwarmEvent<TBvEv, THandleErr> {
    /// Event generated by the `NetworkBehaviour`.
    Behaviour(TBvEv),
    /// A connection to the given peer has been opened.
    ConnectionEstablished {
        /// Identity of the peer that we have connected to.
        peer_id: PeerId,
        /// Endpoint of the connection that has been opened.
        endpoint: ConnectedPoint,
        /// Number of established connections to this peer, including the one that has just been
        /// opened.
        num_established: NonZeroU32,
    },
    /// A connection with the given peer has been closed,
    /// possibly as a result of an error.
    ConnectionClosed {
        /// Identity of the peer that we have connected to.
        peer_id: PeerId,
        /// Endpoint of the connection that has been closed.
        endpoint: ConnectedPoint,
        /// Number of other remaining connections to this same peer.
        num_established: u32,
        /// Reason for the disconnection, if it was not a successful
        /// active close.
        cause: Option<ConnectionError<NodeHandlerWrapperError<THandleErr>>>,
    },
    /// A new connection arrived on a listener and is in the process of protocol negotiation.
    ///
    /// A corresponding [`ConnectionEstablished`](SwarmEvent::ConnectionEstablished),
    /// [`BannedPeer`](SwarmEvent::BannedPeer), or
    /// [`IncomingConnectionError`](SwarmEvent::IncomingConnectionError) event will later be
    /// generated for this connection.
    IncomingConnection {
        /// Local connection address.
        /// This address has been earlier reported with a [`NewListenAddr`](SwarmEvent::NewListenAddr)
        /// event.
        local_addr: Multiaddr,
        /// Address used to send back data to the remote.
        send_back_addr: Multiaddr,
    },
    /// An error happened on a connection during its initial handshake.
    ///
    /// This can include, for example, an error during the handshake of the encryption layer, or
    /// the connection unexpectedly closed.
    IncomingConnectionError {
        /// Local connection address.
        /// This address has been earlier reported with a [`NewListenAddr`](SwarmEvent::NewListenAddr)
        /// event.
        local_addr: Multiaddr,
        /// Address used to send back data to the remote.
        send_back_addr: Multiaddr,
        /// The error that happened.
        error: PendingConnectionError<io::Error>,
    },
    /// We connected to a peer, but we immediately closed the connection because that peer is banned.
    BannedPeer {
        /// Identity of the banned peer.
        peer_id: PeerId,
        /// Endpoint of the connection that has been closed.
        endpoint: ConnectedPoint,
    },
    /// Tried to dial an address but it ended up being unreachaable.
    UnreachableAddr {
        /// `PeerId` that we were trying to reach.
        peer_id: PeerId,
        /// Address that we failed to reach.
        address: Multiaddr,
        /// Error that has been encountered.
        error: PendingConnectionError<io::Error>,
        /// Number of remaining connection attempts that are being tried for this peer.
        attempts_remaining: u32,
    },
    /// Tried to dial an address but it ended up being unreachaable.
    /// Contrary to `UnreachableAddr`, we don't know the identity of the peer that we were trying
    /// to reach.
    UnknownPeerUnreachableAddr {
        /// Address that we failed to reach.
        address: Multiaddr,
        /// Error that has been encountered.
        error: PendingConnectionError<io::Error>,
    },
    /// One of our listeners has reported a new local listening address.
    NewListenAddr(Multiaddr),
    /// One of our listeners has reported the expiration of a listening address.
    ExpiredListenAddr(Multiaddr),
    /// One of the listeners gracefully closed.
    ListenerClosed {
        /// The addresses that the listener was listening on. These addresses are now considered
        /// expired, similar to if a [`ExpiredListenAddr`](SwarmEvent::ExpiredListenAddr) event
        /// has been generated for each of them.
        addresses: Vec<Multiaddr>,
        /// Reason for the closure. Contains `Ok(())` if the stream produced `None`, or `Err`
        /// if the stream produced an error.
        reason: Result<(), io::Error>,
    },
    /// One of the listeners reported a non-fatal error.
    ListenerError {
        /// The listener error.
        error: io::Error,
    },
    /// A new dialing attempt has been initiated.
    ///
    /// A [`ConnectionEstablished`](SwarmEvent::ConnectionEstablished)
    /// event is reported if the dialing attempt succeeds, otherwise a
    /// [`UnreachableAddr`](SwarmEvent::UnreachableAddr) event is reported
    /// with `attempts_remaining` equal to 0.
    Dialing(PeerId),
}

/// Contains the state of the network, plus the way it should behave.
pub struct ExpandedSwarm<TBehaviour, TInEvent, TOutEvent, THandler>
where
    THandler: IntoProtocolsHandler,
{
    network: Network<
        transport::Boxed<(PeerId, StreamMuxerBox)>,
        TInEvent,
        TOutEvent,
        NodeHandlerWrapperBuilder<THandler>,
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
    pending_event: Option<(PeerId, PendingNotifyHandler, TInEvent)>,

    /// The configured override for substream protocol upgrades, if any.
    substream_upgrade_protocol_override: Option<libp2p_core::upgrade::Version>,
}

impl<TBehaviour, TInEvent, TOutEvent, THandler> Unpin for
    ExpandedSwarm<TBehaviour, TInEvent, TOutEvent, THandler>
where
    THandler: IntoProtocolsHandler,
{
}

impl<TBehaviour, TInEvent, TOutEvent, THandler, THandleErr>
    ExpandedSwarm<TBehaviour, TInEvent, TOutEvent, THandler>
where TBehaviour: NetworkBehaviour<ProtocolsHandler = THandler>,
      TInEvent: Send + 'static,
      TOutEvent: Send + 'static,
      THandler: IntoProtocolsHandler + Send + 'static,
      THandler::Handler: ProtocolsHandler<InEvent = TInEvent, OutEvent = TOutEvent, Error = THandleErr>,
      THandleErr: error::Error + Send + 'static,
{
    /// Builds a new `Swarm`.
    pub fn new(
        transport: transport::Boxed<(PeerId, StreamMuxerBox)>,
        behaviour: TBehaviour,
        local_peer_id: PeerId
    ) -> Self {
        SwarmBuilder::new(transport, behaviour, local_peer_id).build()
    }

    /// Returns information about the [`Network`] underlying the `Swarm`.
    pub fn network_info(&self) -> NetworkInfo {
        self.network.info()
    }

    /// Starts listening on the given address.
    ///
    /// Returns an error if the address is not supported.
    pub fn listen_on(&mut self, addr: Multiaddr) -> Result<ListenerId, TransportError<io::Error>> {
        let id = self.network.listen_on(addr)?;
        self.behaviour.inject_new_listener(id);
        Ok(id)
    }

    /// Remove some listener.
    ///
    /// Returns `Ok(())` if there was a listener with this ID.
    pub fn remove_listener(&mut self, id: ListenerId) -> Result<(), ()> {
        self.network.remove_listener(id)
    }

    /// Initiates a new dialing attempt to the given address.
    pub fn dial_addr(&mut self, addr: Multiaddr) -> Result<(), DialError> {
        let handler = self.behaviour.new_handler()
            .into_node_handler_builder()
            .with_substream_upgrade_protocol_override(self.substream_upgrade_protocol_override);
        Ok(self.network.dial(&addr, handler).map(|_id| ())?)
    }

    /// Initiates a new dialing attempt to the given peer.
    pub fn dial(&mut self, peer_id: &PeerId) -> Result<(), DialError> {
        if self.banned_peers.contains(peer_id) {
            self.behaviour.inject_dial_failure(peer_id);
            return Err(DialError::Banned)
        }

        let self_listening = &self.listened_addrs;
        let mut addrs = self.behaviour.addresses_of_peer(peer_id)
            .into_iter()
            .filter(|a| !self_listening.contains(a));

        let result =
            if let Some(first) = addrs.next() {
                let handler = self.behaviour.new_handler()
                    .into_node_handler_builder()
                    .with_substream_upgrade_protocol_override(self.substream_upgrade_protocol_override);
                self.network.peer(*peer_id)
                    .dial(first, addrs, handler)
                    .map(|_| ())
                    .map_err(DialError::from)
            } else {
                Err(DialError::NoAddresses)
            };

        if let Err(error) = &result {
            log::debug!(
                "New dialing attempt to peer {:?} failed: {:?}.",
                peer_id, error);
            self.behaviour.inject_dial_failure(&peer_id);
        }

        result
    }

    /// Returns an iterator that produces the list of addresses we're listening on.
    pub fn listeners(&self) -> impl Iterator<Item = &Multiaddr> {
        self.network.listen_addrs()
    }

    /// Returns the peer ID of the swarm passed as parameter.
    pub fn local_peer_id(&self) -> &PeerId {
        self.network.local_peer_id()
    }

    /// Returns an iterator for [`AddressRecord`]s of external addresses
    /// of the local node, in decreasing order of their current
    /// [score](AddressScore).
    pub fn external_addresses(&self) -> impl Iterator<Item = &AddressRecord> {
        self.external_addrs.iter()
    }

    /// Adds an external address record for the local node.
    ///
    /// An external address is an address of the local node known to
    /// be (likely) reachable for other nodes, possibly taking into
    /// account NAT. The external addresses of the local node may be
    /// shared with other nodes by the `NetworkBehaviour`.
    ///
    /// The associated score determines both the position of the address
    /// in the list of external addresses (which can determine the
    /// order in which addresses are used to connect to) as well as
    /// how long the address is retained in the list, depending on
    /// how frequently it is reported by the `NetworkBehaviour` via
    /// [`NetworkBehaviourAction::ReportObservedAddr`] or explicitly
    /// through this method.
    pub fn add_external_address(&mut self, a: Multiaddr, s: AddressScore) -> AddAddressResult {
        let result = self.external_addrs.add(a.clone(), s);
        let expired = match &result {
            AddAddressResult::Inserted { expired } => {
                self.behaviour.inject_new_external_addr(&a);
                expired
            }
            AddAddressResult::Updated { expired } => expired,
        };
        for a in expired {
            self.behaviour.inject_expired_external_addr(&a.addr);
        }
        result
    }

    /// Removes an external address of the local node, regardless of
    /// its current score. See [`ExpandedSwarm::add_external_address`]
    /// for details.
    ///
    /// Returns `true` if the address existed and was removed, `false`
    /// otherwise.
    pub fn remove_external_address(&mut self, addr: &Multiaddr) -> bool {
        if self.external_addrs.remove(addr) {
            self.behaviour.inject_expired_external_addr(addr);
            true
        } else {
            false
        }
    }

    /// Bans a peer by its peer ID.
    ///
    /// Any incoming connection and any dialing attempt will immediately be rejected.
    /// This function has no effect if the peer is already banned.
    pub fn ban_peer_id(&mut self, peer_id: PeerId) {
        if self.banned_peers.insert(peer_id) {
            if let Some(peer) = self.network.peer(peer_id).into_connected() {
                peer.disconnect();
            }
        }
    }

    /// Unbans a peer.
    pub fn unban_peer_id(&mut self, peer_id: PeerId) {
        self.banned_peers.remove(&peer_id);
    }

    /// Checks whether the [`Network`] has an established connection to a peer.
    pub fn is_connected(&self, peer_id: &PeerId) -> bool {
        self.network.is_connected(peer_id)
    }

    /// Returns a reference to the provided [`NetworkBehaviour`].
    pub fn behaviour(&self) -> &TBehaviour {
        &self.behaviour
    }

    /// Returns a mutable reference to the provided [`NetworkBehaviour`].
    pub fn behaviour_mut(&mut self) -> &mut TBehaviour {
        &mut self.behaviour
    }

    /// Returns the next event that happens in the `Swarm`.
    ///
    /// Includes events from the `NetworkBehaviour` but also events about the connections status.
    pub async fn next_event(&mut self) -> SwarmEvent<TBehaviour::OutEvent, THandleErr> {
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
    fn poll_next_event(mut self: Pin<&mut Self>, cx: &mut Context<'_>)
        -> Poll<SwarmEvent<TBehaviour::OutEvent, THandleErr>>
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
                    let peer = connection.peer_id();
                    let connection = connection.id();
                    this.behaviour.inject_event(peer, connection, event);
                },
                Poll::Ready(NetworkEvent::AddressChange { connection, new_endpoint, old_endpoint }) => {
                    let peer = connection.peer_id();
                    let connection = connection.id();
                    this.behaviour.inject_address_change(&peer, &connection, &old_endpoint, &new_endpoint);
                },
                Poll::Ready(NetworkEvent::ConnectionEstablished { connection, num_established }) => {
                    let peer_id = connection.peer_id();
                    let endpoint = connection.endpoint().clone();
                    if this.banned_peers.contains(&peer_id) {
                        this.network.peer(peer_id)
                            .into_connected()
                            .expect("the Network just notified us that we were connected; QED")
                            .disconnect();
                        return Poll::Ready(SwarmEvent::BannedPeer {
                            peer_id,
                            endpoint,
                        });
                    } else {
                        log::debug!("Connection established: {:?}; Total (peer): {}.",
                            connection.connected(), num_established);
                        let endpoint = connection.endpoint().clone();
                        this.behaviour.inject_connection_established(&peer_id, &connection.id(), &endpoint);
                        if num_established.get() == 1 {
                            this.behaviour.inject_connected(&peer_id);
                        }
                        return Poll::Ready(SwarmEvent::ConnectionEstablished {
                            peer_id, num_established, endpoint
                        });
                    }
                },
                Poll::Ready(NetworkEvent::ConnectionClosed { id, connected, error, num_established }) => {
                    if let Some(error) = error.as_ref() {
                        log::debug!("Connection {:?} closed: {:?}", connected, error);
                    } else {
                        log::debug!("Connection {:?} closed (active close).", connected);
                    }
                    let peer_id = connected.peer_id;
                    let endpoint = connected.endpoint;
                    this.behaviour.inject_connection_closed(&peer_id, &id, &endpoint);
                    if num_established == 0 {
                        this.behaviour.inject_disconnected(&peer_id);
                    }
                    return Poll::Ready(SwarmEvent::ConnectionClosed {
                        peer_id,
                        endpoint,
                        cause: error,
                        num_established,
                    });
                },
                Poll::Ready(NetworkEvent::IncomingConnection { connection, .. }) => {
                    let handler = this.behaviour.new_handler()
                        .into_node_handler_builder()
                        .with_substream_upgrade_protocol_override(this.substream_upgrade_protocol_override);
                    let local_addr = connection.local_addr.clone();
                    let send_back_addr = connection.send_back_addr.clone();
                    if let Err(e) = this.network.accept(connection, handler) {
                        log::warn!("Incoming connection rejected: {:?}", e);
                    }
                    return Poll::Ready(SwarmEvent::IncomingConnection {
                        local_addr,
                        send_back_addr,
                    });
                },
                Poll::Ready(NetworkEvent::NewListenerAddress { listener_id, listen_addr }) => {
                    log::debug!("Listener {:?}; New address: {:?}", listener_id, listen_addr);
                    if !this.listened_addrs.contains(&listen_addr) {
                        this.listened_addrs.push(listen_addr.clone())
                    }
                    this.behaviour.inject_new_listen_addr(listener_id, &listen_addr);
                    return Poll::Ready(SwarmEvent::NewListenAddr(listen_addr));
                }
                Poll::Ready(NetworkEvent::ExpiredListenerAddress { listener_id, listen_addr }) => {
                    log::debug!("Listener {:?}; Expired address {:?}.", listener_id, listen_addr);
                    this.listened_addrs.retain(|a| a != &listen_addr);
                    this.behaviour.inject_expired_listen_addr(listener_id, &listen_addr);
                    return Poll::Ready(SwarmEvent::ExpiredListenAddr(listen_addr));
                }
                Poll::Ready(NetworkEvent::ListenerClosed { listener_id, addresses, reason }) => {
                    log::debug!("Listener {:?}; Closed by {:?}.", listener_id, reason);
                    for addr in addresses.iter() {
                        this.behaviour.inject_expired_listen_addr(listener_id, addr);
                    }
                    this.behaviour.inject_listener_closed(listener_id, match &reason {
                        Ok(()) => Ok(()),
                        Err(err) => Err(err),
                    });
                    return Poll::Ready(SwarmEvent::ListenerClosed {
                        addresses,
                        reason,
                    });
                }
                Poll::Ready(NetworkEvent::ListenerError { listener_id, error }) => {
                    this.behaviour.inject_listener_error(listener_id, &error);
                    return Poll::Ready(SwarmEvent::ListenerError {
                        error,
                    });
                },
                Poll::Ready(NetworkEvent::IncomingConnectionError { local_addr, send_back_addr, error }) => {
                    log::debug!("Incoming connection failed: {:?}", error);
                    return Poll::Ready(SwarmEvent::IncomingConnectionError {
                        local_addr,
                        send_back_addr,
                        error,
                    });
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
                        peer_id,
                        address: multiaddr,
                        error,
                        attempts_remaining,
                    });
                },
                Poll::Ready(NetworkEvent::UnknownPeerDialError { multiaddr, error, .. }) => {
                    log::debug!("Connection attempt to address {:?} of unknown peer failed with {:?}",
                        multiaddr, error);
                    this.behaviour.inject_addr_reach_failure(None, &multiaddr, &error);
                    return Poll::Ready(SwarmEvent::UnknownPeerUnreachableAddr {
                        address: multiaddr,
                        error,
                    });
                },
            }

            // After the network had a chance to make progress, try to deliver
            // the pending event emitted by the behaviour in the previous iteration
            // to the connection handler(s). The pending event must be delivered
            // before polling the behaviour again. If the targeted peer
            // meanwhie disconnected, the event is discarded.
            if let Some((peer_id, handler, event)) = this.pending_event.take() {
                if let Some(mut peer) = this.network.peer(peer_id).into_connected() {
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
                        let condition_matched = match condition {
                            DialPeerCondition::Disconnected => this.network.is_disconnected(&peer_id),
                            DialPeerCondition::NotDialing => !this.network.is_dialing(&peer_id),
                            DialPeerCondition::Always => true,
                        };
                        if condition_matched {
                            if ExpandedSwarm::dial(this, &peer_id).is_ok() {
                                return Poll::Ready(SwarmEvent::Dialing(peer_id))
                            }
                        } else {
                            // Even if the condition for a _new_ dialing attempt is not met,
                            // we always add any potentially new addresses of the peer to an
                            // ongoing dialing attempt, if there is one.
                            log::trace!("Condition for new dialing attempt to {:?} not met: {:?}",
                                peer_id, condition);
                            let self_listening = &this.listened_addrs;
                            if let Some(mut peer) = this.network.peer(peer_id).into_dialing() {
                                let addrs = this.behaviour.addresses_of_peer(peer.id());
                                let mut attempt = peer.some_attempt();
                                for a in addrs {
                                    if !self_listening.contains(&a) {
                                        attempt.add_address(a);
                                    }
                                }
                            }
                        }
                    }
                },
                Poll::Ready(NetworkBehaviourAction::NotifyHandler { peer_id, handler, event }) => {
                    if let Some(mut peer) = this.network.peer(peer_id).into_connected() {
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
                        }
                    }
                },
                Poll::Ready(NetworkBehaviourAction::ReportObservedAddr { address, score }) => {
                    for addr in this.network.address_translation(&address) {
                        this.add_external_address(addr, score);
                    }
                },
            }
        }
    }
}

/// Connection to notify of a pending event.
///
/// The connection IDs out of which to notify one of an event are captured at
/// the time the behaviour emits the event, in order not to forward the event to
/// a new connection which the behaviour may not have been aware of at the time
/// it issued the request for sending it.
enum PendingNotifyHandler {
    One(ConnectionId),
    Any(SmallVec<[ConnectionId; 10]>),
}

/// Notify a single connection of an event.
///
/// Returns `Some` with the given event if the connection is not currently
/// ready to receive another event, in which case the current task is
/// scheduled to be woken up.
///
/// Returns `None` if the connection is closing or the event has been
/// successfully sent, in either case the event is consumed.
fn notify_one<'a, TInEvent>(
    conn: &mut EstablishedConnection<'a, TInEvent>,
    event: TInEvent,
    cx: &mut Context<'_>,
) -> Option<TInEvent>
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
fn notify_any<'a, TTrans, TInEvent, TOutEvent, THandler>(
    ids: SmallVec<[ConnectionId; 10]>,
    peer: &mut ConnectedPeer<'a, TTrans, TInEvent, TOutEvent, THandler>,
    event: TInEvent,
    cx: &mut Context<'_>,
) -> Option<(TInEvent, SmallVec<[ConnectionId; 10]>)>
where
    TTrans: Transport,
    THandler: IntoConnectionHandler,
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

impl<TBehaviour, TInEvent, TOutEvent, THandler> Stream for
    ExpandedSwarm<TBehaviour, TInEvent, TOutEvent, THandler>
where TBehaviour: NetworkBehaviour<ProtocolsHandler = THandler>,
      THandler: IntoProtocolsHandler + Send + 'static,
      TInEvent: Send + 'static,
      TOutEvent: Send + 'static,
      THandler::Handler: ProtocolsHandler<InEvent = TInEvent, OutEvent = TOutEvent>,
{
    type Item = TBehaviour::OutEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let event = futures::ready!(ExpandedSwarm::poll_next_event(self.as_mut(), cx));
            if let SwarmEvent::Behaviour(event) = event {
                return Poll::Ready(Some(event));
            }
        }
    }
}

/// the stream of behaviour events never terminates, so we can implement fused for it
impl<TBehaviour, TInEvent, TOutEvent, THandler> FusedStream for
    ExpandedSwarm<TBehaviour, TInEvent, TOutEvent, THandler>
where TBehaviour: NetworkBehaviour<ProtocolsHandler = THandler>,
      THandler: IntoProtocolsHandler + Send + 'static,
      TInEvent: Send + 'static,
      TOutEvent: Send + 'static,
      THandler::Handler: ProtocolsHandler<InEvent = TInEvent, OutEvent = TOutEvent>,
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
        &self.local_peer_id
    }
}

/// A `SwarmBuilder` provides an API for configuring and constructing a `Swarm`,
/// including the underlying [`Network`].
pub struct SwarmBuilder<TBehaviour> {
    local_peer_id: PeerId,
    transport: transport::Boxed<(PeerId, StreamMuxerBox)>,
    behaviour: TBehaviour,
    network_config: NetworkConfig,
    substream_upgrade_protocol_override: Option<libp2p_core::upgrade::Version>,
}

impl<TBehaviour> SwarmBuilder<TBehaviour>
where TBehaviour: NetworkBehaviour,
{
    /// Creates a new `SwarmBuilder` from the given transport, behaviour and
    /// local peer ID. The `Swarm` with its underlying `Network` is obtained
    /// via [`SwarmBuilder::build`].
    pub fn new(
        transport: transport::Boxed<(PeerId, StreamMuxerBox)>,
        behaviour: TBehaviour,
        local_peer_id: PeerId
    ) -> Self {
        SwarmBuilder {
            local_peer_id,
            transport,
            behaviour,
            network_config: Default::default(),
            substream_upgrade_protocol_override: None,
        }
    }

    /// Configures the `Executor` to use for spawning background tasks.
    ///
    /// By default, unless another executor has been configured,
    /// [`SwarmBuilder::build`] will try to set up a `ThreadPool`.
    pub fn executor(mut self, e: Box<dyn Executor + Send>) -> Self {
        self.network_config = self.network_config.with_executor(e);
        self
    }

    /// Configures the number of events from the [`NetworkBehaviour`] in
    /// destination to the [`ProtocolsHandler`] that can be buffered before
    /// the [`Swarm`] has to wait. An individual buffer with this number of
    /// events exists for each individual connection.
    ///
    /// The ideal value depends on the executor used, the CPU speed, and the
    /// volume of events. If this value is too low, then the [`Swarm`] will
    /// be sleeping more often than necessary. Increasing this value increases
    /// the overall memory usage.
    pub fn notify_handler_buffer_size(mut self, n: NonZeroUsize) -> Self {
        self.network_config = self.network_config.with_notify_handler_buffer_size(n);
        self
    }

    /// Configures the number of extra events from the [`ProtocolsHandler`] in
    /// destination to the [`NetworkBehaviour`] that can be buffered before
    /// the [`ProtocolsHandler`] has to go to sleep.
    ///
    /// There exists a buffer of events received from [`ProtocolsHandler`]s
    /// that the [`NetworkBehaviour`] has yet to process. This buffer is
    /// shared between all instances of [`ProtocolsHandler`]. Each instance of
    /// [`ProtocolsHandler`] is guaranteed one slot in this buffer, meaning
    /// that delivering an event for the first time is guaranteed to be
    /// instantaneous. Any extra event delivery, however, must wait for that
    /// first event to be delivered or for an "extra slot" to be available.
    ///
    /// This option configures the number of such "extra slots" in this
    /// shared buffer. These extra slots are assigned in a first-come,
    /// first-served basis.
    ///
    /// The ideal value depends on the executor used, the CPU speed, the
    /// average number of connections, and the volume of events. If this value
    /// is too low, then the [`ProtocolsHandler`]s will be sleeping more often
    /// than necessary. Increasing this value increases the overall memory
    /// usage, and more importantly the latency between the moment when an
    /// event is emitted and the moment when it is received by the
    /// [`NetworkBehaviour`].
    pub fn connection_event_buffer_size(mut self, n: usize) -> Self {
        self.network_config = self.network_config.with_connection_event_buffer_size(n);
        self
    }

    /// Configures the connection limits.
    pub fn connection_limits(mut self, limits: ConnectionLimits) -> Self {
        self.network_config = self.network_config.with_connection_limits(limits);
        self
    }

    /// Configures an override for the substream upgrade protocol to use.
    ///
    /// The subtream upgrade protocol is the multistream-select protocol
    /// used for protocol negotiation on substreams. Since a listener
    /// supports all existing versions, the choice of upgrade protocol
    /// only effects the "dialer", i.e. the peer opening a substream.
    ///
    /// > **Note**: If configured, specific upgrade protocols for
    /// > individual [`SubstreamProtocol`]s emitted by the `NetworkBehaviour`
    /// > are ignored.
    pub fn substream_upgrade_protocol_override(mut self, v: libp2p_core::upgrade::Version) -> Self {
        self.substream_upgrade_protocol_override = Some(v);
        self
    }

    /// Builds a `Swarm` with the current configuration.
    pub fn build(mut self) -> Swarm<TBehaviour> {
        let supported_protocols = self.behaviour
            .new_handler()
            .inbound_protocol()
            .protocol_info()
            .into_iter()
            .map(|info| info.protocol_name().to_vec())
            .collect();

        // If no executor has been explicitly configured, try to set up a thread pool.
        let network_cfg = self.network_config.or_else_with_executor(|| {
            match ThreadPoolBuilder::new()
                .name_prefix("libp2p-swarm-task-")
                .create()
            {
                Ok(tp) => {
                    Some(Box::new(move |f| tp.spawn_ok(f)))
                },
                Err(err) => {
                    log::warn!("Failed to create executor thread pool: {:?}", err);
                    None
                }
            }
        });

        let network = Network::new(self.transport, self.local_peer_id, network_cfg);

        ExpandedSwarm {
            network,
            behaviour: self.behaviour,
            supported_protocols,
            listened_addrs: SmallVec::new(),
            external_addrs: Addresses::default(),
            banned_peers: HashSet::new(),
            pending_event: None,
            substream_upgrade_protocol_override: self.substream_upgrade_protocol_override,
        }
    }
}

/// The possible failures of [`ExpandedSwarm::dial`].
#[derive(Debug)]
pub enum DialError {
    /// The peer is currently banned.
    Banned,
    /// The configured limit for simultaneous outgoing connections
    /// has been reached.
    ConnectionLimit(ConnectionLimit),
    /// The address given for dialing is invalid.
    InvalidAddress(Multiaddr),
    /// [`NetworkBehaviour::addresses_of_peer`] returned no addresses
    /// for the peer to dial.
    NoAddresses
}

impl From<network::DialError> for DialError {
    fn from(err: network::DialError) -> DialError {
        match err {
            network::DialError::ConnectionLimit(l) => DialError::ConnectionLimit(l),
            network::DialError::InvalidAddress(a) => DialError::InvalidAddress(a),
        }
    }
}

impl fmt::Display for DialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DialError::ConnectionLimit(err) => write!(f, "Dial error: {}", err),
            DialError::NoAddresses => write!(f, "Dial error: no addresses for peer."),
            DialError::InvalidAddress(a) => write!(f, "Dial error: invalid address: {}", a),
            DialError::Banned => write!(f, "Dial error: peer is banned.")
        }
    }
}

impl error::Error for DialError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            DialError::ConnectionLimit(err) => Some(err),
            DialError::InvalidAddress(_) => None,
            DialError::NoAddresses => None,
            DialError::Banned => None
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

    fn poll(&mut self, _: &mut Context<'_>, _: &mut impl PollParameters) ->
        Poll<NetworkBehaviourAction<<Self::ProtocolsHandler as
        ProtocolsHandler>::InEvent, Self::OutEvent>>
    {
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use crate::protocols_handler::DummyProtocolsHandler;
    use crate::test::{MockBehaviour, CallTraceBehaviour};
    use futures::{future, executor};
    use libp2p_core::{
        identity,
        upgrade,
        multiaddr,
        transport
    };
    use libp2p_noise as noise;
    use super::*;

    fn new_test_swarm<T, O>(handler_proto: T) -> Swarm<CallTraceBehaviour<MockBehaviour<T, O>>>
    where
        T: ProtocolsHandler + Clone,
        T::OutEvent: Clone,
        O: Send + 'static
    {
        let id_keys = identity::Keypair::generate_ed25519();
        let pubkey = id_keys.public();
        let noise_keys = noise::Keypair::<noise::X25519Spec>::new().into_authentic(&id_keys).unwrap();
        let transport = transport::MemoryTransport::default()
            .upgrade(upgrade::Version::V1)
            .authenticate(noise::NoiseConfig::xx(noise_keys).into_authenticated())
            .multiplex(libp2p_mplex::MplexConfig::new())
            .boxed();
        let behaviour = CallTraceBehaviour::new(MockBehaviour::new(handler_proto));
        SwarmBuilder::new(transport, behaviour, pubkey.into()).build()
    }

    /// Establishes a number of connections between two peers,
    /// after which one peer bans the other.
    ///
    /// The test expects both behaviours to be notified via pairs of
    /// inject_connected / inject_disconnected as well as
    /// inject_connection_established / inject_connection_closed calls.
    #[test]
    fn test_connect_disconnect_ban() {
        // Since the test does not try to open any substreams, we can
        // use the dummy protocols handler.
        let mut handler_proto = DummyProtocolsHandler::default();
        handler_proto.keep_alive = KeepAlive::Yes;

        let mut swarm1 = new_test_swarm::<_, ()>(handler_proto.clone());
        let mut swarm2 = new_test_swarm::<_, ()>(handler_proto);

        let addr1: Multiaddr = multiaddr::Protocol::Memory(rand::random::<u64>()).into();
        let addr2: Multiaddr = multiaddr::Protocol::Memory(rand::random::<u64>()).into();

        swarm1.listen_on(addr1.clone().into()).unwrap();
        swarm2.listen_on(addr2.clone().into()).unwrap();

        // Test execution state. Connection => Disconnecting => Connecting.
        enum State {
            Connecting,
            Disconnecting,
        }

        let swarm1_id = *swarm1.local_peer_id();

        let mut banned = false;
        let mut unbanned = false;

        let num_connections = 10;

        for _ in 0 .. num_connections {
            swarm1.dial_addr(addr2.clone()).unwrap();
        }
        let mut state = State::Connecting;

        executor::block_on(future::poll_fn(move |cx| {
            loop {
                let poll1 = Swarm::poll_next_event(Pin::new(&mut swarm1), cx);
                let poll2 = Swarm::poll_next_event(Pin::new(&mut swarm2), cx);
                match state {
                    State::Connecting => {
                        for s in &[&swarm1, &swarm2] {
                            if s.behaviour.inject_connection_established.len() > 0 {
                                assert_eq!(s.behaviour.inject_connected.len(), 1);
                            } else {
                                assert_eq!(s.behaviour.inject_connected.len(), 0);
                            }
                            assert!(s.behaviour.inject_connection_closed.len() == 0);
                            assert!(s.behaviour.inject_disconnected.len() == 0);
                        }
                        if [&swarm1, &swarm2].iter().all(|s| {
                            s.behaviour.inject_connection_established.len() == num_connections
                        }) {
                            if banned {
                                return Poll::Ready(())
                            }
                            swarm2.ban_peer_id(swarm1_id.clone());
                            swarm1.behaviour.reset();
                            swarm2.behaviour.reset();
                            banned = true;
                            state = State::Disconnecting;
                        }
                    }
                    State::Disconnecting => {
                        for s in &[&swarm1, &swarm2] {
                            if s.behaviour.inject_connection_closed.len() < num_connections {
                                assert_eq!(s.behaviour.inject_disconnected.len(), 0);
                            } else {
                                assert_eq!(s.behaviour.inject_disconnected.len(), 1);
                            }
                            assert_eq!(s.behaviour.inject_connection_established.len(), 0);
                            assert_eq!(s.behaviour.inject_connected.len(), 0);
                        }
                        if [&swarm1, &swarm2].iter().all(|s| {
                            s.behaviour.inject_connection_closed.len() == num_connections
                        }) {
                            if unbanned {
                                return Poll::Ready(())
                            }
                            // Unban the first peer and reconnect.
                            swarm2.unban_peer_id(swarm1_id.clone());
                            swarm1.behaviour.reset();
                            swarm2.behaviour.reset();
                            unbanned = true;
                            for _ in 0 .. num_connections {
                                swarm2.dial_addr(addr1.clone()).unwrap();
                            }
                            state = State::Connecting;
                        }
                    }
                }

                if poll1.is_pending() && poll2.is_pending() {
                    return Poll::Pending
                }
            }
        }))
    }
}
