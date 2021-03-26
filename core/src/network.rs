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

mod event;
pub mod peer;

pub use crate::connection::{ConnectionLimits, ConnectionCounters};
pub use event::{NetworkEvent, IncomingConnection};
pub use peer::Peer;

use crate::{
    ConnectedPoint,
    Executor,
    Multiaddr,
    PeerId,
    connection::{
        ConnectionId,
        ConnectionLimit,
        ConnectionHandler,
        IntoConnectionHandler,
        IncomingInfo,
        OutgoingInfo,
        ListenersEvent,
        ListenerId,
        ListenersStream,
        PendingConnectionError,
        Substream,
        manager::ManagerConfig,
        pool::{Pool, PoolEvent},
    },
    muxing::StreamMuxer,
    transport::{Transport, TransportError},
};
use fnv::{FnvHashMap};
use futures::{prelude::*, future};
use smallvec::SmallVec;
use std::{
    collections::hash_map,
    convert::TryFrom as _,
    error,
    fmt,
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
};

/// Implementation of `Stream` that handles the nodes.
pub struct Network<TTrans, TInEvent, TOutEvent, THandler>
where
    TTrans: Transport,
    THandler: IntoConnectionHandler,
{
    /// The local peer ID.
    local_peer_id: PeerId,

    /// Listeners for incoming connections.
    listeners: ListenersStream<TTrans>,

    /// The nodes currently active.
    pool: Pool<TInEvent, TOutEvent, THandler, TTrans::Error,
        <THandler::Handler as ConnectionHandler>::Error>,

    /// The ongoing dialing attempts.
    ///
    /// There may be multiple ongoing dialing attempts to the same peer.
    /// Each dialing attempt is associated with a new connection and hence
    /// a new connection ID.
    ///
    /// > **Note**: `dialing` must be consistent with the pending outgoing
    /// > connections in `pool`. That is, for every entry in `dialing`
    /// > there must exist a pending outgoing connection in `pool` with
    /// > the same connection ID. This is ensured by the implementation of
    /// > `Network` (see `dial_peer_impl` and `on_connection_failed`)
    /// > together with the implementation of `DialingAttempt::abort`.
    dialing: FnvHashMap<PeerId, SmallVec<[peer::DialingState; 10]>>,
}

impl<TTrans, TInEvent, TOutEvent, THandler> fmt::Debug for
    Network<TTrans, TInEvent, TOutEvent, THandler>
where
    TTrans: fmt::Debug + Transport,
    THandler: fmt::Debug + ConnectionHandler,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_struct("ReachAttempts")
            .field("local_peer_id", &self.local_peer_id)
            .field("listeners", &self.listeners)
            .field("peers", &self.pool)
            .field("dialing", &self.dialing)
            .finish()
    }
}

impl<TTrans, TInEvent, TOutEvent, THandler> Unpin for
    Network<TTrans, TInEvent, TOutEvent, THandler>
where
    TTrans: Transport,
    THandler: IntoConnectionHandler,
{
}

impl<TTrans, TInEvent, TOutEvent, THandler>
    Network<TTrans, TInEvent, TOutEvent, THandler>
where
    TTrans: Transport,
    THandler: IntoConnectionHandler,
{
    fn disconnect(&mut self, peer: &PeerId) {
        self.pool.disconnect(peer);
        self.dialing.remove(peer);
    }
}

impl<TTrans, TInEvent, TOutEvent, TMuxer, THandler>
    Network<TTrans, TInEvent, TOutEvent, THandler>
where
    TTrans: Transport + Clone,
    TMuxer: StreamMuxer,
    THandler: IntoConnectionHandler + Send + 'static,
    THandler::Handler: ConnectionHandler<Substream = Substream<TMuxer>, InEvent = TInEvent, OutEvent = TOutEvent> + Send,
    <THandler::Handler as ConnectionHandler>::OutboundOpenInfo: Send,
    <THandler::Handler as ConnectionHandler>::Error: error::Error + Send,
{
    /// Creates a new node events stream.
    pub fn new(
        transport: TTrans,
        local_peer_id: PeerId,
        config: NetworkConfig,
    ) -> Self {
        Network {
            local_peer_id,
            listeners: ListenersStream::new(transport),
            pool: Pool::new(local_peer_id, config.manager_config, config.limits),
            dialing: Default::default(),
        }
    }

    /// Returns the transport passed when building this object.
    pub fn transport(&self) -> &TTrans {
        self.listeners.transport()
    }

    /// Start listening on the given multiaddress.
    pub fn listen_on(&mut self, addr: Multiaddr) -> Result<ListenerId, TransportError<TTrans::Error>> {
        self.listeners.listen_on(addr)
    }

    /// Remove a previously added listener.
    ///
    /// Returns `Ok(())` if a listener with this ID was in the list.
    pub fn remove_listener(&mut self, id: ListenerId) -> Result<(), ()> {
        self.listeners.remove_listener(id)
    }

    /// Returns an iterator that produces the list of addresses we are listening on.
    pub fn listen_addrs(&self) -> impl Iterator<Item = &Multiaddr> {
        self.listeners.listen_addrs()
    }

    /// Maps the given `observed_addr`, representing an address of the local
    /// node observed by a remote peer, onto the locally known listen addresses
    /// to yield one or more addresses of the local node that may be publicly
    /// reachable.
    ///
    /// I.e. this method incorporates the view of other peers into the listen
    /// addresses seen by the local node to account for possible IP and port
    /// mappings performed by intermediate network devices in an effort to
    /// obtain addresses for the local peer that are also reachable for peers
    /// other than the peer who reported the `observed_addr`.
    ///
    /// The translation is transport-specific. See [`Transport::address_translation`].
    pub fn address_translation<'a>(&'a self, observed_addr: &'a Multiaddr)
        -> Vec<Multiaddr>
    where
        TMuxer: 'a,
        THandler: 'a,
    {
        let transport = self.listeners.transport();
        let mut addrs: Vec<_> = self.listen_addrs()
            .filter_map(move |server| transport.address_translation(server, observed_addr))
            .collect();

        // remove duplicates
        addrs.sort_unstable();
        addrs.dedup();

        addrs
    }

    /// Returns the peer id of the local node.
    pub fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    /// Dials a [`Multiaddr`] that may or may not encapsulate a
    /// specific expected remote peer ID.
    ///
    /// The given `handler` will be used to create the
    /// [`Connection`](crate::connection::Connection) upon success and the
    /// connection ID is returned.
    pub fn dial(&mut self, address: &Multiaddr, handler: THandler)
        -> Result<ConnectionId, DialError>
    where
        TTrans: Transport<Output = (PeerId, TMuxer)>,
        TTrans::Error: Send + 'static,
        TTrans::Dial: Send + 'static,
        TMuxer: Send + Sync + 'static,
        TMuxer::OutboundSubstream: Send,
        TInEvent: Send + 'static,
        TOutEvent: Send + 'static,
    {
        // If the address ultimately encapsulates an expected peer ID, dial that peer
        // such that any mismatch is detected. We do not "pop off" the `P2p` protocol
        // from the address, because it may be used by the `Transport`, i.e. `P2p`
        // is a protocol component that can influence any transport, like `libp2p-dns`.
        if let Some(multiaddr::Protocol::P2p(ma)) = address.iter().last() {
            if let Ok(peer) = PeerId::try_from(ma) {
                return self.dial_peer(DialingOpts {
                    peer,
                    address: address.clone(),
                    handler,
                    remaining: Vec::new(),
                })
            }
        }

        // The address does not specify an expected peer, so just try to dial it as-is,
        // accepting any peer ID that the remote identifies as.
        let info = OutgoingInfo { address, peer_id: None };
        match self.transport().clone().dial(address.clone()) {
            Ok(f) => {
                let f = f.map_err(|err| PendingConnectionError::Transport(TransportError::Other(err)));
                self.pool.add_outgoing(f, handler, info).map_err(DialError::ConnectionLimit)
            }
            Err(err) => {
                let f = future::err(PendingConnectionError::Transport(err));
                self.pool.add_outgoing(f, handler, info).map_err(DialError::ConnectionLimit)
            }
        }
    }

    /// Returns information about the state of the `Network`.
    pub fn info(&self) -> NetworkInfo {
        let num_peers = self.pool.num_peers();
        let connection_counters = self.pool.counters().clone();
        NetworkInfo {
            num_peers,
            connection_counters,
        }
    }

    /// Returns an iterator for information on all pending incoming connections.
    pub fn incoming_info(&self) -> impl Iterator<Item = IncomingInfo<'_>> {
        self.pool.iter_pending_incoming()
    }

    /// Returns the list of addresses we're currently dialing without knowing the `PeerId` of.
    pub fn unknown_dials(&self) -> impl Iterator<Item = &Multiaddr> {
        self.pool.iter_pending_outgoing()
            .filter_map(|info| {
                if info.peer_id.is_none() {
                    Some(info.address)
                } else {
                    None
                }
            })
    }

    /// Returns a list of all connected peers, i.e. peers to whom the `Network`
    /// has at least one established connection.
    pub fn connected_peers(&self) -> impl Iterator<Item = &PeerId> {
        self.pool.iter_connected()
    }

    /// Checks whether the network has an established connection to a peer.
    pub fn is_connected(&self, peer: &PeerId) -> bool {
        self.pool.is_connected(peer)
    }

    /// Checks whether the network has an ongoing dialing attempt to a peer.
    pub fn is_dialing(&self, peer: &PeerId) -> bool {
        self.dialing.contains_key(peer)
    }

    /// Checks whether the network has neither an ongoing dialing attempt,
    /// nor an established connection to a peer.
    pub fn is_disconnected(&self, peer: &PeerId) -> bool {
        !self.is_connected(peer) && !self.is_dialing(peer)
    }

    /// Returns a list of all the peers to whom a new outgoing connection
    /// is currently being established.
    pub fn dialing_peers(&self) -> impl Iterator<Item = &PeerId> {
        self.dialing.keys()
    }

    /// Obtains a view of a [`Peer`] with the given ID in the network.
    pub fn peer(&mut self, peer_id: PeerId)
        -> Peer<'_, TTrans, TInEvent, TOutEvent, THandler>
    {
        Peer::new(self, peer_id)
    }

    /// Accepts a pending incoming connection obtained via [`NetworkEvent::IncomingConnection`],
    /// adding it to the `Network`s connection pool subject to the configured limits.
    ///
    /// Once the connection is established and all transport protocol upgrades
    /// completed, the connection is associated with the provided `handler`.
    pub fn accept(
        &mut self,
        connection: IncomingConnection<TTrans::ListenerUpgrade>,
        handler: THandler,
    ) -> Result<ConnectionId, ConnectionLimit>
    where
        TInEvent: Send + 'static,
        TOutEvent: Send + 'static,
        TMuxer: StreamMuxer + Send + Sync + 'static,
        TMuxer::OutboundSubstream: Send,
        TTrans: Transport<Output = (PeerId, TMuxer)>,
        TTrans::Error: Send + 'static,
        TTrans::ListenerUpgrade: Send + 'static,
    {
        let upgrade = connection.upgrade.map_err(|err|
            PendingConnectionError::Transport(TransportError::Other(err)));
        let info = IncomingInfo {
            local_addr: &connection.local_addr,
            send_back_addr: &connection.send_back_addr,
        };
        self.pool.add_incoming(upgrade, handler, info)
    }

    /// Provides an API similar to `Stream`, except that it cannot error.
    pub fn poll<'a>(&'a mut self, cx: &mut Context<'_>) -> Poll<NetworkEvent<'a, TTrans, TInEvent, TOutEvent, THandler>>
    where
        TTrans: Transport<Output = (PeerId, TMuxer)>,
        TTrans::Error: Send + 'static,
        TTrans::Dial: Send + 'static,
        TTrans::ListenerUpgrade: Send + 'static,
        TMuxer: Send + Sync + 'static,
        TMuxer::OutboundSubstream: Send,
        TInEvent: Send + 'static,
        TOutEvent: Send + 'static,
        THandler: IntoConnectionHandler + Send + 'static,
        THandler::Handler: ConnectionHandler<Substream = Substream<TMuxer>, InEvent = TInEvent, OutEvent = TOutEvent> + Send + 'static,
        <THandler::Handler as ConnectionHandler>::Error: error::Error + Send + 'static,
    {
        // Poll the listener(s) for new connections.
        match ListenersStream::poll(Pin::new(&mut self.listeners), cx) {
            Poll::Pending => (),
            Poll::Ready(ListenersEvent::Incoming {
                listener_id,
                upgrade,
                local_addr,
                send_back_addr
            }) => {
                return Poll::Ready(NetworkEvent::IncomingConnection {
                    listener_id,
                    connection: IncomingConnection {
                        upgrade,
                        local_addr,
                        send_back_addr,
                    }
                })
            }
            Poll::Ready(ListenersEvent::NewAddress { listener_id, listen_addr }) => {
                return Poll::Ready(NetworkEvent::NewListenerAddress { listener_id, listen_addr })
            }
            Poll::Ready(ListenersEvent::AddressExpired { listener_id, listen_addr }) => {
                return Poll::Ready(NetworkEvent::ExpiredListenerAddress { listener_id, listen_addr })
            }
            Poll::Ready(ListenersEvent::Closed { listener_id, addresses, reason }) => {
                return Poll::Ready(NetworkEvent::ListenerClosed { listener_id, addresses, reason })
            }
            Poll::Ready(ListenersEvent::Error { listener_id, error }) => {
                return Poll::Ready(NetworkEvent::ListenerError { listener_id, error })
            }
        }

        // Poll the known peers.
        let event = match self.pool.poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(PoolEvent::ConnectionEstablished { connection, num_established }) => {
                if let hash_map::Entry::Occupied(mut e) = self.dialing.entry(connection.peer_id()) {
                    e.get_mut().retain(|s| s.current.0 != connection.id());
                    if e.get().is_empty() {
                        e.remove();
                    }
                }

                NetworkEvent::ConnectionEstablished {
                    connection,
                    num_established,
                }
            }
            Poll::Ready(PoolEvent::PendingConnectionError { id, endpoint, error, handler, pool, .. }) => {
                let dialing = &mut self.dialing;
                let (next, event) = on_connection_failed(dialing, id, endpoint, error, handler);
                if let Some(dial) = next {
                    let transport = self.listeners.transport().clone();
                    if let Err(e) = dial_peer_impl(transport, pool, dialing, dial) {
                        log::warn!("Dialing aborted: {:?}", e);
                    }
                }
                event
            }
            Poll::Ready(PoolEvent::ConnectionClosed { id, connected, error, num_established, .. }) => {
                NetworkEvent::ConnectionClosed {
                    id,
                    connected,
                    num_established,
                    error,
                }
            }
            Poll::Ready(PoolEvent::ConnectionEvent { connection, event }) => {
                NetworkEvent::ConnectionEvent {
                    connection,
                    event,
                }
            }
            Poll::Ready(PoolEvent::AddressChange { connection, new_endpoint, old_endpoint }) => {
                NetworkEvent::AddressChange {
                    connection,
                    new_endpoint,
                    old_endpoint,
                }
            }
        };

        Poll::Ready(event)
    }

    /// Initiates a connection attempt to a known peer.
    fn dial_peer(&mut self, opts: DialingOpts<PeerId, THandler>)
        -> Result<ConnectionId, DialError>
    where
        TTrans: Transport<Output = (PeerId, TMuxer)>,
        TTrans::Dial: Send + 'static,
        TTrans::Error: Send + 'static,
        TMuxer: Send + Sync + 'static,
        TMuxer::OutboundSubstream: Send,
        TInEvent: Send + 'static,
        TOutEvent: Send + 'static,
    {
        dial_peer_impl(self.transport().clone(), &mut self.pool, &mut self.dialing, opts)
    }
}

/// Options for a dialing attempt (i.e. repeated connection attempt
/// via a list of address) to a peer.
struct DialingOpts<PeerId, THandler> {
    peer: PeerId,
    handler: THandler,
    address: Multiaddr,
    remaining: Vec<Multiaddr>,
}

/// Standalone implementation of `Network::dial_peer` for more granular borrowing.
fn dial_peer_impl<TMuxer, TInEvent, TOutEvent, THandler, TTrans>(
    transport: TTrans,
    pool: &mut Pool<TInEvent, TOutEvent, THandler, TTrans::Error,
        <THandler::Handler as ConnectionHandler>::Error>,
    dialing: &mut FnvHashMap<PeerId, SmallVec<[peer::DialingState; 10]>>,
    opts: DialingOpts<PeerId, THandler>
) -> Result<ConnectionId, DialError>
where
    THandler: IntoConnectionHandler + Send + 'static,
    <THandler::Handler as ConnectionHandler>::Error: error::Error + Send + 'static,
    <THandler::Handler as ConnectionHandler>::OutboundOpenInfo: Send + 'static,
    THandler::Handler: ConnectionHandler<
        Substream = Substream<TMuxer>,
        InEvent = TInEvent,
        OutEvent = TOutEvent,
    > + Send + 'static,
    TTrans: Transport<Output = (PeerId, TMuxer)>,
    TTrans::Dial: Send + 'static,
    TTrans::Error: error::Error + Send + 'static,
    TMuxer: StreamMuxer + Send + Sync + 'static,
    TMuxer::OutboundSubstream: Send + 'static,
    TInEvent: Send + 'static,
    TOutEvent: Send + 'static,
{
    // Ensure the address to dial encapsulates the `p2p` protocol for the
    // targeted peer, so that the transport has a "fully qualified" address
    // to work with.
    let addr = p2p_addr(opts.peer, opts.address).map_err(DialError::InvalidAddress)?;

    let result = match transport.dial(addr.clone()) {
        Ok(fut) => {
            let fut = fut.map_err(|e| PendingConnectionError::Transport(TransportError::Other(e)));
            let info = OutgoingInfo { address: &addr, peer_id: Some(&opts.peer) };
            pool.add_outgoing(fut, opts.handler, info).map_err(DialError::ConnectionLimit)
        },
        Err(err) => {
            let fut = future::err(PendingConnectionError::Transport(err));
            let info = OutgoingInfo { address: &addr, peer_id: Some(&opts.peer) };
            pool.add_outgoing(fut, opts.handler, info).map_err(DialError::ConnectionLimit)
        },
    };

    if let Ok(id) = &result {
        dialing.entry(opts.peer).or_default().push(
            peer::DialingState {
                current: (*id, addr),
                remaining: opts.remaining,
            },
        );
    }

    result
}

/// Callback for handling a failed connection attempt, returning an
/// event to emit from the `Network`.
///
/// If the failed connection attempt was a dialing attempt and there
/// are more addresses to try, new `DialingOpts` are returned.
fn on_connection_failed<'a, TTrans, TInEvent, TOutEvent, THandler>(
    dialing: &mut FnvHashMap<PeerId, SmallVec<[peer::DialingState; 10]>>,
    id: ConnectionId,
    endpoint: ConnectedPoint,
    error: PendingConnectionError<TTrans::Error>,
    handler: Option<THandler>,
) -> (Option<DialingOpts<PeerId, THandler>>, NetworkEvent<'a, TTrans, TInEvent, TOutEvent, THandler>)
where
    TTrans: Transport,
    THandler: IntoConnectionHandler,
{
    // Check if the failed connection is associated with a dialing attempt.
    let dialing_failed = dialing.iter_mut()
        .find_map(|(peer, attempts)| {
            if let Some(pos) = attempts.iter().position(|s| s.current.0 == id) {
                let attempt = attempts.remove(pos);
                let last = attempts.is_empty();
                Some((*peer, attempt, last))
            } else {
                None
            }
        });

    if let Some((peer_id, mut attempt, last)) = dialing_failed {
        if last {
            dialing.remove(&peer_id);
        }

        let num_remain = u32::try_from(attempt.remaining.len()).unwrap();
        let failed_addr = attempt.current.1.clone();

        let (opts, attempts_remaining) =
            if num_remain > 0 {
                if let Some(handler) = handler {
                    let next_attempt = attempt.remaining.remove(0);
                    let opts = DialingOpts {
                        peer: peer_id,
                        handler,
                        address: next_attempt,
                        remaining: attempt.remaining
                    };
                    (Some(opts), num_remain)
                } else {
                    // The error is "fatal" for the dialing attempt, since
                    // the handler was already consumed. All potential
                    // remaining connection attempts are thus void.
                    (None, 0)
                }
            } else {
                (None, 0)
            };

        (opts, NetworkEvent::DialError {
            attempts_remaining,
            peer_id,
            multiaddr: failed_addr,
            error,
        })
    } else {
        // A pending incoming connection or outgoing connection to an unknown peer failed.
        match endpoint {
            ConnectedPoint::Dialer { address } =>
                (None, NetworkEvent::UnknownPeerDialError {
                    multiaddr: address,
                    error,
                }),
            ConnectedPoint::Listener { local_addr, send_back_addr } =>
                (None, NetworkEvent::IncomingConnectionError {
                    local_addr,
                    send_back_addr,
                    error
                })
        }
    }
}

/// Information about the network obtained by [`Network::info()`].
#[derive(Clone, Debug)]
pub struct NetworkInfo {
    /// The total number of connected peers.
    num_peers: usize,
    /// Counters of ongoing network connections.
    connection_counters: ConnectionCounters,
}

impl NetworkInfo {
    /// The number of connected peers, i.e. peers with whom at least
    /// one established connection exists.
    pub fn num_peers(&self) -> usize {
        self.num_peers
    }

    /// Gets counters for ongoing network connections.
    pub fn connection_counters(&self) -> &ConnectionCounters {
        &self.connection_counters
    }
}

/// The (optional) configuration for a [`Network`].
///
/// The default configuration specifies no dedicated task executor, no
/// connection limits, a connection event buffer size of 32, and a
/// `notify_handler` buffer size of 8.
#[derive(Default)]
pub struct NetworkConfig {
    /// Note that the `ManagerConfig`s task command buffer always provides
    /// one "free" slot per task. Thus the given total `notify_handler_buffer_size`
    /// exposed for configuration on the `Network` is reduced by one.
    manager_config: ManagerConfig,
    /// The effective connection limits.
    limits: ConnectionLimits,
}

impl NetworkConfig {
    /// Configures the executor to use for spawning connection background tasks.
    pub fn with_executor(mut self, e: Box<dyn Executor + Send>) -> Self {
        self.manager_config.executor = Some(e);
        self
    }

    /// Configures the executor to use for spawning connection background tasks,
    /// only if no executor has already been configured.
    pub fn or_else_with_executor<F>(mut self, f: F) -> Self
    where
        F: FnOnce() -> Option<Box<dyn Executor + Send>>
    {
        self.manager_config.executor = self.manager_config.executor.or_else(f);
        self
    }

    /// Sets the maximum number of events sent to a connection's background task
    /// that may be buffered, if the task cannot keep up with their consumption and
    /// delivery to the connection handler.
    ///
    /// When the buffer for a particular connection is full, `notify_handler` will no
    /// longer be able to deliver events to the associated `ConnectionHandler`,
    /// thus exerting back-pressure on the connection and peer API.
    pub fn with_notify_handler_buffer_size(mut self, n: NonZeroUsize) -> Self {
        self.manager_config.task_command_buffer_size = n.get() - 1;
        self
    }

    /// Sets the maximum number of buffered connection events (beyond a guaranteed
    /// buffer of 1 event per connection).
    ///
    /// When the buffer is full, the background tasks of all connections will stall.
    /// In this way, the consumers of network events exert back-pressure on
    /// the network connection I/O.
    pub fn with_connection_event_buffer_size(mut self, n: usize) -> Self {
        self.manager_config.task_event_buffer_size = n;
        self
    }

    /// Sets the connection limits to enforce.
    pub fn with_connection_limits(mut self, limits: ConnectionLimits) -> Self {
        self.limits = limits;
        self
    }
}

/// Ensures a given `Multiaddr` is a `/p2p/...` address for the given peer.
///
/// If the given address is already a `p2p` address for the given peer,
/// i.e. the last encapsulated protocol is `/p2p/<peer-id>`, this is a no-op.
///
/// If the given address is already a `p2p` address for a different peer
/// than the one given, the given `Multiaddr` is returned as an `Err`.
///
/// If the given address is not yet a `p2p` address for the given peer,
/// the `/p2p/<peer-id>` protocol is appended to the returned address.
fn p2p_addr(peer: PeerId, addr: Multiaddr) -> Result<Multiaddr, Multiaddr> {
    if let Some(multiaddr::Protocol::P2p(hash)) = addr.iter().last() {
        if &hash != peer.as_ref() {
            return Err(addr)
        }
        Ok(addr)
    } else {
        Ok(addr.with(multiaddr::Protocol::P2p(peer.into())))
    }
}

/// Possible (synchronous) errors when dialing a peer.
#[derive(Clone, Debug)]
pub enum DialError {
    /// The dialing attempt is rejected because of a connection limit.
    ConnectionLimit(ConnectionLimit),
    /// The address being dialed is invalid, e.g. if it refers to a different
    /// remote peer than the one being dialed.
    InvalidAddress(Multiaddr),
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Dummy;

    impl Executor for Dummy {
        fn exec(&self, _: Pin<Box<dyn Future<Output=()> + Send>>) { }
    }

    #[test]
    fn set_executor() {
        NetworkConfig::default()
            .with_executor(Box::new(Dummy))
            .with_executor(Box::new(|f| {
                async_std::task::spawn(f);
            }));
    }
}
