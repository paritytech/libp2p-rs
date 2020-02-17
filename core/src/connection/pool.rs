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

use crate::{
    Executor,
    ConnectedPoint,
    PeerId,
    connection::{
        self,
        ConnectionId,
        ConnectionLimit,
        IncomingInfo,
        OutgoingInfo,
        Connection,
        Substream,
        Connected,
        ConnectionError,
        ConnectionHandler,
        IntoConnectionHandler,
        ConnectionInfo,
        manager::{self, Manager},
    },
    muxing::StreamMuxer,
};
use either::Either;
use fnv::FnvHashMap;
use futures::prelude::*;
use smallvec::SmallVec;
use std::{error, fmt, hash::Hash, task::Context, task::Poll};

/// A connection `Pool` manages a set of connections for each peer.
pub struct Pool<TInEvent, TOutEvent, THandler, TTransErr, THandlerErr, TConnInfo = PeerId, TPeerId = PeerId> {
    local_id: TPeerId,

    /// The configuration of the pool.
    limits: PoolLimits,

    /// The connection manager that handles the connection I/O for both
    /// established and pending connections.
    ///
    /// For every established connection there is a corresponding entry in `established`.
    manager: Manager<TInEvent, TOutEvent, THandler, TTransErr, THandlerErr, TConnInfo>, // tasks

    /// The managed connections of each peer that are currently considered
    /// established, as witnessed by the associated `ConnectedPoint`.
    established: FnvHashMap<TPeerId, FnvHashMap<ConnectionId, ConnectedPoint>>,

    /// The pending connections that are currently being negotiated.
    pending: FnvHashMap<ConnectionId, (ConnectedPoint, Option<TPeerId>)>,
}

impl<TInEvent, TOutEvent, THandler, TTransErr, THandlerErr, TConnInfo, TPeerId> fmt::Debug
for Pool<TInEvent, TOutEvent, THandler, TTransErr, THandlerErr, TConnInfo, TPeerId>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        // TODO: More useful debug impl?
        f.debug_struct("Pool")
            .field("limits", &self.limits)
            .finish()
    }
}

impl<TInEvent, TOutEvent, THandler, TTransErr, THandlerErr, TConnInfo, TPeerId> Unpin
for Pool<TInEvent, TOutEvent, THandler, TTransErr, THandlerErr, TConnInfo, TPeerId> {}

/// Event that can happen on the `Pool`.
pub enum PoolEvent<'a, TInEvent, TOutEvent, THandler, TTransErr, THandlerErr, TConnInfo, TPeerId> {
    /// A new connection has been established.
    ///
    /// In order for the connection to remain in the pool, it must be accepted.
    /// If the `NewConnection` is dropped without being accepted, the connection
    /// is closed and removed from the pool.
    ConnectionEstablished {
        connection: EstablishedConnection<'a, TInEvent, TConnInfo, TPeerId>,
        num_established: usize,
    },

    /// A newly established connection has been dropped because of the configured
    /// per-peer connection limit.
    ConnectionLimitReached {
        connected: Connected<TConnInfo>,
        info: ConnectionLimit,
    },

    /// An established connection has encountered an error.
    ///
    /// Can only happen after a node has been successfully reached.
    ConnectionError {
        id: ConnectionId,
        /// Information about the connection that errored.
        connected: Connected<TConnInfo>,
        /// The error that happened.
        error: ConnectionError<THandlerErr, TTransErr>,
        /// A reference to the pool that used to manage the connection.
        pool: &'a mut Pool<TInEvent, TOutEvent, THandler, TTransErr, THandlerErr, TConnInfo, TPeerId>,
        /// The remaining number of established connections to the same peer.
        num_established: usize,
    },

    /// A connection attempt failed.
    PendingConnectionError {
        /// The ID of the failed connection.
        id: ConnectionId,
        /// The local endpoint of the failed connection.
        endpoint: ConnectedPoint,
        /// The error that occurred.
        error: ConnectionError<THandlerErr, TTransErr>,
        /// The handler that was supposed to handle the connection.
        handler: THandler,
        /// The (expected) peer of the failed connection.
        peer: Option<TPeerId>,
        /// A reference to the pool that managed the connection.
        pool: &'a mut Pool<TInEvent, TOutEvent, THandler, TTransErr, THandlerErr, TConnInfo, TPeerId>,
    },

    /// A node has produced an event.
    ConnectionEvent {
        /// The connection that has generated the event.
        connection: EstablishedConnection<'a, TInEvent, TConnInfo, TPeerId>,
        /// The produced event.
        event: TOutEvent,
    },
}

impl<'a, TInEvent, TOutEvent, THandler, TTransErr, THandlerErr, TConnInfo, TPeerId> fmt::Debug
for PoolEvent<'a, TInEvent, TOutEvent, THandler, TTransErr, THandlerErr, TConnInfo, TPeerId>
where
    TOutEvent: fmt::Debug,
    TTransErr: fmt::Debug,
    THandlerErr: fmt::Debug,
    TConnInfo: fmt::Debug,
    TInEvent: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match *self {
            PoolEvent::ConnectionEstablished { ref connection, .. } => {
                f.debug_tuple("PoolEvent::ConnectionEstablished")
                    .field(connection)
                    .finish()
            },
            PoolEvent::ConnectionError { ref id, ref connected, ref error, .. } => {
                f.debug_struct("PoolEvent::ConnectionError")
                    .field("id", id)
                    .field("connected", connected)
                    .field("error", error)
                    .finish()
            },
            PoolEvent::PendingConnectionError { ref id, ref error, .. } => {
                f.debug_struct("PoolEvent::PendingConnectionError")
                    .field("id", id)
                    .field("error", error)
                    .finish()
            },
            PoolEvent::ConnectionEvent { ref connection, ref event } => {
                f.debug_struct("PoolEvent::ConnectionEvent")
                    .field("conn_info", connection.info())
                    .field("event", event)
                    .finish()
            },
            PoolEvent::ConnectionLimitReached { ref connected, ref info } => {
                f.debug_struct("PoolEvent::ConnectionLimitReached")
                    .field("connected", connected)
                    .field("info", info)
                    .finish()
            }
        }
    }
}

impl<TInEvent, TOutEvent, THandler, TTransErr, THandlerErr, TConnInfo, TPeerId>
    Pool<TInEvent, TOutEvent, THandler, TTransErr, THandlerErr, TConnInfo, TPeerId>
where
    TConnInfo: ConnectionInfo<PeerId = TPeerId>,
    TPeerId: Eq + Hash,
{
    /// Creates a new empty `Pool`.
    pub fn new(
        local_id: TPeerId,
        executor: Option<Box<dyn Executor + Send>>,
        limits: PoolLimits
    ) -> Self {
        Pool {
            local_id,
            limits,
            manager: Manager::new(executor),
            established: Default::default(),
            pending: Default::default(),
        }
    }

    /// Gets the configured connection limits of the pool.
    pub fn limits(&self) -> &PoolLimits {
        &self.limits
    }

    /// Adds a pending incoming connection to the pool in the form of a
    /// `Future` that establishes and negotiates the connection.
    ///
    /// Returns an error if the limit of pending incoming connections
    /// has been reached.
    pub fn add_incoming<TFut, TMuxer>(
        &mut self,
        future: TFut,
        handler: THandler,
        info: IncomingInfo,
    ) -> Result<ConnectionId, ConnectionLimit>
    where
        TConnInfo: ConnectionInfo<PeerId = TPeerId> + Send + 'static,
        TFut: Future<
            Output = Result<(TConnInfo, TMuxer), ConnectionError<THandlerErr, TTransErr>>
        > + Send + 'static,
        THandler: IntoConnectionHandler<TConnInfo> + Send + 'static,
        THandler::Handler: ConnectionHandler<
            Substream = Substream<TMuxer>,
            InEvent = TInEvent,
            OutEvent = TOutEvent,
            Error = THandlerErr
        > + Send + 'static,
        <THandler::Handler as ConnectionHandler>::OutboundOpenInfo: Send + 'static,
        TTransErr: error::Error + Send + 'static,
        THandlerErr: error::Error + Send + 'static,
        TInEvent: Send + 'static,
        TOutEvent: Send + 'static,
        TMuxer: StreamMuxer + Send + Sync + 'static,
        TMuxer::OutboundSubstream: Send + 'static,
    {
        let endpoint = info.to_connected_point();
        if let Some(limit) = self.limits.max_pending_incoming {
            let current = self.iter_pending_incoming().count();
            if current >= limit {
                return Err(ConnectionLimit { limit, current })
            }
        }
        Ok(self.add_pending(future, handler, endpoint, None))
    }

    /// Adds a pending outgoing connection to the pool in the form of a `Future`
    /// that establishes and negotiates the connection.
    ///
    /// Returns an error if the limit of pending outgoing connections
    /// has been reached.
    pub fn add_outgoing<TFut, TMuxer>(
        &mut self,
        future: TFut,
        handler: THandler,
        info: OutgoingInfo<TPeerId>,
    ) -> Result<ConnectionId, ConnectionLimit>
    where
        TConnInfo: ConnectionInfo<PeerId = TPeerId> + Send + 'static,
        TFut: Future<
            Output = Result<(TConnInfo, TMuxer), ConnectionError<THandlerErr, TTransErr>>
        > + Send + 'static,
        THandler: IntoConnectionHandler<TConnInfo> + Send + 'static,
        THandler::Handler: ConnectionHandler<
            Substream = Substream<TMuxer>,
            InEvent = TInEvent,
            OutEvent = TOutEvent,
            Error = THandlerErr
        > + Send + 'static,
        <THandler::Handler as ConnectionHandler>::OutboundOpenInfo: Send + 'static,
        TTransErr: error::Error + Send + 'static,
        THandlerErr: error::Error + Send + 'static,
        TInEvent: Send + 'static,
        TOutEvent: Send + 'static,
        TMuxer: StreamMuxer + Send + Sync + 'static,
        TMuxer::OutboundSubstream: Send + 'static,
        TPeerId: Clone,
    {
        let endpoint = info.to_connected_point();
        if let Some(limit) = self.limits.max_pending_outgoing {
            let current = self.iter_pending_outgoing().count();
            if current >= limit {
                return Err(ConnectionLimit { limit, current })
            }
        }
        Ok(self.add_pending(future, handler, endpoint, info.peer_id.cloned()))
    }

    /// Adds a pending connection to the pool in the form of a
    /// `Future` that establishes and negotiates the connection.
    fn add_pending<TFut, TMuxer>(
        &mut self,
        future: TFut,
        handler: THandler,
        endpoint: ConnectedPoint,
        peer: Option<TPeerId>,
    ) -> ConnectionId
    where
        TConnInfo: ConnectionInfo<PeerId = TPeerId> + Send + 'static,
        TFut: Future<
            Output = Result<(TConnInfo, TMuxer), ConnectionError<THandlerErr, TTransErr>>
        > + Send + 'static,
        THandler: IntoConnectionHandler<TConnInfo> + Send + 'static,
        THandler::Handler: ConnectionHandler<
            Substream = Substream<TMuxer>,
            InEvent = TInEvent,
            OutEvent = TOutEvent,
            Error = THandlerErr
        > + Send + 'static,
        <THandler::Handler as ConnectionHandler>::OutboundOpenInfo: Send + 'static,
        TTransErr: error::Error + Send + 'static,
        THandlerErr: error::Error + Send + 'static,
        TInEvent: Send + 'static,
        TOutEvent: Send + 'static,
        TMuxer: StreamMuxer + Send + Sync + 'static,
        TMuxer::OutboundSubstream: Send + 'static,
    {
        let future = future.and_then({
            let endpoint = endpoint.clone();
            move |(info, muxer)| {
                let connected = Connected { info, endpoint };
                future::ready(Ok((connected, muxer)))
            }
        });
        let id = self.manager.add_pending(future, handler);
        self.pending.insert(id, (endpoint, peer));
        id
    }

    /// Sends an event to all nodes.
    ///
    /// This function is "atomic", in the sense that if `Poll::Pending` is returned then no event
    /// has been sent to any node yet.
    #[must_use]
    pub fn poll_broadcast(&mut self, event: &TInEvent, cx: &mut Context) -> Poll<()>
    where
        TInEvent: Clone
    {
        self.manager.poll_broadcast(event, cx)
    }

    /// Adds an existing established connection to the pool.
    ///
    /// Returns the assigned connection ID on success. An error is returned
    /// if the configured maximum number of established connections for the
    /// connected peer has been reached.
    pub fn add<TMuxer>(&mut self, c: Connection<TMuxer, THandler::Handler>, i: Connected<TConnInfo>)
        -> Result<ConnectionId, ConnectionLimit>
    where
        THandler: IntoConnectionHandler<TConnInfo> + Send + 'static,
        THandler::Handler: ConnectionHandler<
            Substream = connection::Substream<TMuxer>,
            InEvent = TInEvent,
            OutEvent = TOutEvent,
            Error = THandlerErr
        > + Send + 'static,
        <THandler::Handler as ConnectionHandler>::OutboundOpenInfo: Send + 'static,
        TTransErr: error::Error + Send + 'static,
        THandlerErr: error::Error + Send + 'static,
        TInEvent: Send + 'static,
        TOutEvent: Send + 'static,
        TMuxer: StreamMuxer + Send + Sync + 'static,
        TMuxer::OutboundSubstream: Send + 'static,
        TConnInfo: Clone + Send + 'static,
        TPeerId: Clone,
    {
        if let Some(limit) = self.limits.max_established_per_peer {
            let current = self.num_peer_established(i.peer_id());
            if limit >= current {
                return Err(ConnectionLimit { limit, current })
            }
        }
        let id = self.manager.add(c, i.clone());
        self.established.entry(i.peer_id().clone()).or_default().insert(id, i.endpoint);
        Ok(id)
    }

    /// Gets an entry representing a connection in the pool.
    ///
    /// Returns `None` if the pool has no connection with the given ID.
    pub fn get(&mut self, id: ConnectionId)
        -> Option<PoolConnection<'_, TInEvent, TConnInfo, TPeerId>>
    {
        match self.manager.entry(id) {
            Some(manager::Entry::Established(entry)) =>
                Some(PoolConnection::Established(EstablishedConnection {
                    entry,
                    established: &mut self.established,
                })),
            Some(manager::Entry::Pending(entry)) =>
                Some(PoolConnection::Pending(PendingConnection {
                    entry,
                    pending: &mut self.pending,
                })),
            None => None
        }
    }

    /// Gets an established connection from the pool.
    ///
    /// If a connection ID is given and no established connection with that
    /// ID is known to the pool, `None` is returned.
    ///
    /// If no connection ID is given and the pool contains at least one
    /// established connection to the given peer, an unspecified selection
    /// is made among these connections.
    ///
    /// If no connection ID is given and no established connection exists
    /// for the given peer, `None` is returned.
    pub fn get_established(&mut self, peer: &TPeerId, id: Option<ConnectionId>)
        -> Option<EstablishedConnection<'_, TInEvent, TConnInfo, TPeerId>>
    {
        match id {
            Some(id) => match self.get(id) {
                Some(PoolConnection::Established(c)) => Some(c),
                _ => None
            }
            None => match self.established.get(peer) {
                None => None,
                Some(conns) => conns.keys().copied()
                    .find(|id| self.manager.connected(id).is_some())
                    .and_then(move |id| match self.manager.entry(id) {
                        Some(manager::Entry::Established(entry)) =>
                            Some(EstablishedConnection {
                                established: &mut self.established,
                                entry,
                            }),
                        _ => None
                    })
            }
        }
    }

    /// Gets an entry representing a pending, outgoing connection to a peer.
    ///
    /// If a connection ID is given and no pending outgoing connection with that
    /// ID is known to the pool, `None` is returned.
    ///
    /// If no connection ID is given and the pool contains at least one
    /// pending outgoing connection to the given peer, an unspecified selection
    /// is made among these connections.
    ///
    /// If no connection ID is given and no pending outgoing connection exists
    /// to the given peer, `None` is returned.
    pub fn get_outgoing(&mut self, peer: &TPeerId, id: Option<ConnectionId>)
        -> Option<PendingConnection<'_, TInEvent, TConnInfo, TPeerId>>
    {
        match id {
            Some(id) => match self.get(id) {
                Some(PoolConnection::Pending(c)) => Some(c),
                _ => None
            }
            None => self.pending.iter()
                .find_map(|(id, (_endpoint, peer2))|
                    if Some(peer) == peer2.as_ref() {
                        Some(*id)
                    } else {
                        None
                    })
                .and_then(move |id| match self.manager.entry(id) {
                    Some(manager::Entry::Pending(entry)) =>
                        Some(PendingConnection {
                            pending: &mut self.pending,
                            entry,
                        }),
                    _ => None
                })
        }
    }

    /// Returns true if we are connected to the given peer.
    ///
    /// This will return true only after a `NodeReached` event has been produced by `poll()`.
    pub fn is_connected(&self, id: &TPeerId) -> bool {
        self.established.contains_key(id)
    }

    /// Returns the number of connected peers, i.e. those with at least one
    /// established connection in the pool.
    pub fn num_connected(&self) -> usize {
        self.established.len()
    }

    /// Close all connections to the given peer.
    pub fn disconnect(&mut self, peer: &TPeerId) {
        if let Some(conns) = self.established.get(peer) {
            for id in conns.keys() {
                match self.manager.entry(*id) {
                    Some(manager::Entry::Established(e)) => { e.close(); },
                    _ => {}
                }
            }
        }

        for (id, (_endpoint, peer2)) in &self.pending {
            if Some(peer) == peer2.as_ref() {
                match self.manager.entry(*id) {
                    Some(manager::Entry::Pending(e)) => { e.abort(); },
                    _ => {}
                }
            }
        }
    }

    /// Asynchronously notifies a connection handler for the given peer of an event.
    ///
    /// If multiple connections exist to the peer, it is unspecified which handler
    /// receives the event.
    pub fn notify_handler(&mut self, peer: &TPeerId, event: TInEvent)
        -> Option<impl Future<Output = ()> + '_>
    {
        if let Some(conns) = self.established.get(peer) {
            let conns = conns.keys().copied().collect::<SmallVec<[ConnectionId; 10]>>();
            let mut event = Some(event);
            Some(future::poll_fn(move |cx| {
                for id in &conns {
                    match self.manager.entry(*id) {
                        Some(manager::Entry::Established(mut e)) => {
                            if e.poll_ready_notify_handler(cx).is_ready() {
                                e.notify_handler(event.take().expect("Polled after completion."));
                                return Poll::Ready(())
                            }
                        },
                        _ => {}
                    }
                }
                return Poll::Pending;
            }))
        } else {
            None
        }
    }

    /// Counts the number of established connections in the pool.
    pub fn num_established(&self) -> usize {
        self.established.iter().fold(0, |n, (_, conns)| n + conns.len())
    }

    /// Counts the number of pending connections in the pool.
    pub fn num_pending(&self) -> usize {
        self.iter_pending_info().count()
    }

    /// Counts the number of established connections to the given peer.
    pub fn num_peer_established(&self, peer: &TPeerId) -> usize {
        self.established.get(peer).map_or(0, |conns| conns.len())
    }

    /// Returns an iterator over all connection IDs of established
    /// connections of `peer` known to the pool.
    pub fn iter_peer_established<'a>(&'a mut self, peer: &TPeerId) -> EstablishedConnectionIter<
        'a, impl Iterator<Item = ConnectionId>, TInEvent, TOutEvent, THandler, TTransErr, THandlerErr,
        TConnInfo, TPeerId>
    {
        let ids = self.iter_peer_established_info(peer)
            .map(|(id, _endpoint)| *id)
            .collect::<SmallVec<[ConnectionId; 10]>>()
            .into_iter();

        EstablishedConnectionIter { pool: self, ids }
    }

    /// Returns an iterator for information on all pending incoming connections.
    pub fn iter_pending_incoming(&self) -> impl Iterator<Item = IncomingInfo<'_>> {
        self.iter_pending_info()
            .filter_map(|(_, ref endpoint, _)| {
                match endpoint {
                    ConnectedPoint::Listener { local_addr, send_back_addr } => {
                        Some(IncomingInfo { local_addr, send_back_addr })
                    },
                    ConnectedPoint::Dialer { .. } => None,
                }
            })
    }

    /// Returns an iterator for information on all pending incoming connections.
    pub fn iter_pending_outgoing(&self) -> impl Iterator<Item = OutgoingInfo<'_, TPeerId>> {
        self.iter_pending_info()
            .filter_map(|(_, ref endpoint, ref peer_id)| {
                match endpoint {
                    ConnectedPoint::Listener { .. } => None,
                    ConnectedPoint::Dialer { address } =>
                        Some(OutgoingInfo { address, peer_id: peer_id.as_ref() }),
                }
            })
    }

    /// Returns an iterator over all connection IDs and associated endpoints
    /// of established connections to `peer` known to the pool.
    pub fn iter_peer_established_info(&self, peer: &TPeerId)
        -> impl Iterator<Item = (&ConnectionId, &ConnectedPoint)> + '_
    {
        match self.established.get(peer) {
            Some(conns) => Either::Left(conns.iter()),
            None => Either::Right(std::iter::empty())
        }
    }

    /// Returns an iterator over all pending connection IDs together
    /// with associated endpoints and expected peer IDs in the pool.
    pub fn iter_pending_info(&self)
        -> impl Iterator<Item = (&ConnectionId, &ConnectedPoint, &Option<TPeerId>)> + '_
    {
        self.pending.iter().map(|(id, (endpoint, info))| (id, endpoint, info))
    }

    /// Returns an iterator over all connected peers, i.e. those that have
    /// at least one established connection in the pool.
    pub fn iter_connected<'a>(&'a self) -> impl Iterator<Item = &'a TPeerId> + 'a {
        self.established.keys()
    }

    /// Polls the connection pool for events.
    ///
    /// > **Note**: We use a regular `poll` method instead of implementing `Stream` in order to
    /// > remove the `Err` variant, but also because we want the `Pool` to stay
    /// > borrowed if necessary.
    pub fn poll<'a>(&'a mut self, cx: &mut Context) -> Poll<
        PoolEvent<'a, TInEvent, TOutEvent, THandler, TTransErr, THandlerErr, TConnInfo, TPeerId>
    > where
        TConnInfo: Clone,
        TPeerId: Clone,
    {
        loop {
            let item = match self.manager.poll(cx) {
                Poll::Ready(item) => item,
                Poll::Pending => return Poll::Pending,
            };

            match item {
                manager::Event::PendingConnectionError { id, error, handler } => {
                    if let Some((endpoint, peer)) = self.pending.remove(&id) {
                        return Poll::Ready(PoolEvent::PendingConnectionError {
                            id,
                            endpoint,
                            error,
                            handler,
                            peer,
                            pool: self
                        })
                    }
                },
                manager::Event::ConnectionError { id, connected, error } => {
                    let num_established =
                        if let Some(conns) = self.established.get_mut(connected.peer_id()) {
                            conns.remove(&id);
                            conns.len()
                        } else {
                            0
                        };
                    if num_established == 0 {
                        self.established.remove(connected.peer_id());
                    }
                    return Poll::Ready(PoolEvent::ConnectionError {
                        id, connected, error, num_established, pool: self
                    })
                },
                manager::Event::ConnectionEstablished { entry } => {
                    let id = entry.id();
                    if let Some((endpoint, peer)) = self.pending.remove(&id) {
                        if let Some(peer) = peer {
                            let current = self.established.get(&peer).map_or(0, |conns| conns.len());
                            if let Err(e) = self.limits.check_established(current) {
                                let connected = entry.close();
                                return Poll::Ready(PoolEvent::ConnectionLimitReached {
                                    connected,
                                    info: e,
                                })
                            }
                            if &peer != entry.connected().peer_id() {
                                let connected = entry.close();
                                let num_established = self.established.get(&peer)
                                    .map_or(0, |conns| conns.len());
                                return Poll::Ready(PoolEvent::ConnectionError {
                                    id,
                                    connected,
                                    error: ConnectionError::InvalidPeerId,
                                    pool: self,
                                    num_established,
                                })
                            }
                        }
                        if &self.local_id == entry.connected().peer_id() {
                            let connected = entry.close();
                            return Poll::Ready(PoolEvent::ConnectionError {
                                id,
                                connected,
                                error: ConnectionError::InvalidPeerId,
                                pool: self,
                                num_established: 0,
                            })
                        }
                        let peer = entry.connected().peer_id().clone();
                        let conns = self.established.entry(peer).or_default();
                        let num_established = conns.len() + 1;
                        conns.insert(id, endpoint);
                        match self.get(id) {
                            Some(PoolConnection::Established(connection)) =>
                                return Poll::Ready(PoolEvent::ConnectionEstablished {
                                    connection, num_established
                                }),
                            _ => unreachable!("since `entry` is an `EstablishedEntry`.")
                        }
                    }
                },
                manager::Event::ConnectionEvent { entry, event } => {
                    let id = entry.id();
                    match self.get(id) {
                        Some(PoolConnection::Established(connection)) =>
                            return Poll::Ready(PoolEvent::ConnectionEvent {
                                connection,
                                event,
                            }),
                        _ => unreachable!("since `entry` is an `EstablishedEntry`.")
                    }
                }
            }
        }
    }

}

/// A connection in a [`Pool`].
pub enum PoolConnection<'a, TInEvent, TConnInfo, TPeerId> {
    Pending(PendingConnection<'a, TInEvent, TConnInfo, TPeerId>),
    Established(EstablishedConnection<'a, TInEvent, TConnInfo, TPeerId>),
}

/// A pending connection in a [`Pool`].
pub struct PendingConnection<'a, TInEvent, TConnInfo, TPeerId> {
    entry: manager::PendingEntry<'a, TInEvent, TConnInfo>,
    pending: &'a mut FnvHashMap<ConnectionId, (ConnectedPoint, Option<TPeerId>)>,
}

impl<TInEvent, TConnInfo, TPeerId>
    PendingConnection<'_, TInEvent, TConnInfo, TPeerId>
{
    /// Returns the local connection ID.
    pub fn id(&self) -> ConnectionId {
        self.entry.id()
    }

    /// Returns the (expected) identity of the remote peer, if known.
    pub fn peer_id(&self) -> &Option<TPeerId> {
        &self.pending.get(&self.entry.id()).expect("`entry` is a pending entry").1
    }

    /// Returns information about this endpoint of the connection.
    pub fn endpoint(&self) -> &ConnectedPoint {
        &self.pending.get(&self.entry.id()).expect("`entry` is a pending entry").0
    }

    /// Aborts the connection attempt, closing the connection.
    pub fn abort(self)
    where
        TPeerId: Eq + Hash + Clone,
    {
        self.pending.remove(&self.entry.id());
        self.entry.abort();
    }
}

/// An established connection in a [`Pool`].
pub struct EstablishedConnection<'a, TInEvent, TConnInfo, TPeerId> {
    entry: manager::EstablishedEntry<'a, TInEvent, TConnInfo>,
    established: &'a mut FnvHashMap<TPeerId, FnvHashMap<ConnectionId, ConnectedPoint>>,
}

impl<TInEvent, TConnInfo, TPeerId> fmt::Debug
for EstablishedConnection<'_, TInEvent, TConnInfo, TPeerId>
where
    TInEvent: fmt::Debug,
    TConnInfo: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_struct("EstablishedConnection")
            .field("entry", &self.entry)
            .finish()
    }
}

impl<TInEvent, TConnInfo, TPeerId>
    EstablishedConnection<'_, TInEvent, TConnInfo, TPeerId>
{
    pub fn connected(&self) -> &Connected<TConnInfo> {
        self.entry.connected()
    }

    /// Returns information about the connected endpoint.
    pub fn endpoint(&self) -> &ConnectedPoint {
        &self.entry.connected().endpoint
    }

    /// Returns connection information obtained from the transport.
    pub fn info(&self) -> &TConnInfo {
        &self.entry.connected().info
    }
}

impl<TInEvent, TConnInfo, TPeerId>
    EstablishedConnection<'_, TInEvent, TConnInfo, TPeerId>
where
    TConnInfo: ConnectionInfo<PeerId = TPeerId>,
    TPeerId: Eq + Hash + Clone,
{
    /// Returns the local connection ID.
    pub fn id(&self) -> ConnectionId {
        self.entry.id()
    }

    /// Returns the identity of the connected peer.
    pub fn peer_id(&self) -> &TPeerId {
        self.info().peer_id()
    }

    /// (Asynchronously) notifies the connection handler of an event.
    ///
    /// Must be called only after a successful call to `poll_ready_notify_handler()`,
    /// without iterruption by another thread.
    pub fn notify_handler(&mut self, event: TInEvent) {
        self.entry.notify_handler(event)
    }

    /// Checks if the connection is ready to receive an event for the
    /// connection handler via `notify_handler`.
    pub fn poll_ready_notify_handler(&mut self, cx: &mut Context) -> Poll<()> {
        self.entry.poll_ready_notify_handler(cx)
    }

    /// Closes the connection, returning the connection information.
    pub fn close(self) -> Connected<TConnInfo> {
        let id = self.entry.id();
        let info = self.entry.close();

        let empty =
            if let Some(conns) = self.established.get_mut(info.peer_id()) {
                conns.remove(&id);
                conns.is_empty()
            } else {
                false
            };

        if empty {
            self.established.remove(info.peer_id());
        }

        info
    }
}

/// An iterator over established connections in a [`Pool`].
pub struct EstablishedConnectionIter<'a, I, TInEvent, TOutEvent, THandler, TTransErr, THandlerErr, TConnInfo, TPeerId> {
    pool: &'a mut Pool<TInEvent, TOutEvent, THandler, TTransErr, THandlerErr, TConnInfo, TPeerId>,
    ids: I
}

// Note: Ideally this would be an implementation of `Iterator`, but that
// requires GATs (cf. https://github.com/rust-lang/rust/issues/44265) and
// a different definition of `Iterator`.
impl<'a, I, TInEvent, TOutEvent, THandler, TTransErr, THandlerErr, TConnInfo, TPeerId>
    EstablishedConnectionIter<'a, I, TInEvent, TOutEvent, THandler, TTransErr, THandlerErr, TConnInfo, TPeerId>
where
    I: Iterator<Item = ConnectionId>
{
    /// Obtains the next connection, if any.
    pub fn next<'b>(&'b mut self) -> Option<EstablishedConnection<'b, TInEvent, TConnInfo, TPeerId>>
    {
        if let Some(id) = self.ids.next() {
            let established = &mut self.pool.established;
            if let Some(manager::Entry::Established(entry)) = self.pool.manager.entry(id) {
                return Some(EstablishedConnection { entry, established })
            }
        }
        None
    }

    /// Turns the iterator into an iterator over just the connection IDs.
    pub fn into_ids(self) -> impl Iterator<Item = ConnectionId> {
        self.ids
    }
}

/// The configurable limits of a connection [`Pool`].
#[derive(Debug, Clone, Default)]
pub struct PoolLimits {
    pub max_pending_outgoing: Option<usize>,
    pub max_pending_incoming: Option<usize>,
    pub max_established_per_peer: Option<usize>,
}

impl PoolLimits {
    fn check_established(&self, current: usize) -> Result<(), ConnectionLimit> {
        if let Some(limit) = self.max_established_per_peer {
            if limit >= current {
                return Err(ConnectionLimit { limit, current })
            }
        }
        Ok(())
    }
}