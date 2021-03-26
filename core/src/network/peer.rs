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
    Multiaddr,
    Transport,
    StreamMuxer,
    connection::{
        Connected,
        ConnectedPoint,
        ConnectionHandler,
        Connection,
        ConnectionId,
        ConnectionLimit,
        EstablishedConnection,
        EstablishedConnectionIter,
        IntoConnectionHandler,
        PendingConnection,
        Substream,
        pool::Pool,
    },
    PeerId
};
use fnv::FnvHashMap;
use smallvec::SmallVec;
use std::{
    collections::hash_map,
    error,
    fmt,
};
use super::{Network, DialingOpts, DialError};

/// The possible representations of a peer in a [`Network`], as
/// seen by the local node.
///
/// > **Note**: In any state there may always be a pending incoming
/// > connection attempt from the peer, however, the remote identity
/// > of a peer is only known once a connection is fully established.
pub enum Peer<'a, TTrans, TInEvent, TOutEvent, THandler>
where
    TTrans: Transport,
    THandler: IntoConnectionHandler
{
    /// At least one established connection exists to the peer.
    Connected(ConnectedPeer<'a, TTrans, TInEvent, TOutEvent, THandler>),

    /// There is an ongoing dialing (i.e. outgoing connection) attempt
    /// to the peer. There may already be other established connections
    /// to the peer.
    Dialing(DialingPeer<'a, TTrans, TInEvent, TOutEvent, THandler>),

    /// There exists no established connection to the peer and there is
    /// currently no ongoing dialing (i.e. outgoing connection) attempt
    /// in progress.
    Disconnected(DisconnectedPeer<'a, TTrans, TInEvent, TOutEvent, THandler>),

    /// The peer represents the local node.
    Local,
}

impl<'a, TTrans, TInEvent, TOutEvent, THandler> fmt::Debug for
    Peer<'a, TTrans, TInEvent, TOutEvent, THandler>
where
    TTrans: Transport,
    THandler: IntoConnectionHandler,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Peer::Connected(p) => {
                f.debug_struct("Connected")
                    .field("peer", &p)
                    .finish()
            }
            Peer::Dialing(p) => {
                f.debug_struct("Dialing")
                    .field("peer", &p)
                    .finish()
            }
            Peer::Disconnected(p) => {
                f.debug_struct("Disconnected")
                    .field("peer", &p)
                    .finish()
            }
            Peer::Local => {
                f.debug_struct("Local")
                    .finish()
            }
        }
    }
}

impl<'a, TTrans, TInEvent, TOutEvent, THandler>
    Peer<'a, TTrans, TInEvent, TOutEvent, THandler>
where
    TTrans: Transport,
    THandler: IntoConnectionHandler,
{
    pub(super) fn new(
        network: &'a mut Network<TTrans, TInEvent, TOutEvent, THandler>,
        peer_id: PeerId
    ) -> Self {
        if peer_id == network.local_peer_id {
            return Peer::Local;
        }

        if network.pool.is_connected(&peer_id) {
            return Self::connected(network, peer_id)
        }

        if network.dialing.get_mut(&peer_id).is_some() {
            return Self::dialing(network, peer_id);
        }

        Self::disconnected(network, peer_id)
    }


    fn disconnected(
        network: &'a mut Network<TTrans, TInEvent, TOutEvent, THandler>,
        peer_id: PeerId
    ) -> Self {
        Peer::Disconnected(DisconnectedPeer { network, peer_id })
    }

    fn connected(
        network: &'a mut Network<TTrans, TInEvent, TOutEvent, THandler>,
        peer_id: PeerId
    ) -> Self {
        Peer::Connected(ConnectedPeer { network, peer_id })
    }

    fn dialing(
        network: &'a mut Network<TTrans, TInEvent, TOutEvent, THandler>,
        peer_id: PeerId
    ) -> Self {
        Peer::Dialing(DialingPeer { network, peer_id })
    }
}

impl<'a, TTrans, TMuxer, TInEvent, TOutEvent, THandler>
    Peer<'a, TTrans, TInEvent, TOutEvent, THandler>
where
    TTrans: Transport<Output = (PeerId, TMuxer)> + Clone,
    TTrans::Error: Send + 'static,
    TTrans::Dial: Send + 'static,
    TMuxer: StreamMuxer + Send + Sync + 'static,
    TMuxer::OutboundSubstream: Send,
    TInEvent: Send + 'static,
    TOutEvent: Send + 'static,
    THandler: IntoConnectionHandler + Send + 'static,
    THandler::Handler: ConnectionHandler<Substream = Substream<TMuxer>, InEvent = TInEvent, OutEvent = TOutEvent> + Send,
    <THandler::Handler as ConnectionHandler>::OutboundOpenInfo: Send,
    <THandler::Handler as ConnectionHandler>::Error: error::Error + Send + 'static,
{
    /// Checks whether the peer is currently connected.
    ///
    /// Returns `true` iff [`Peer::into_connected`] returns `Some`.
    pub fn is_connected(&self) -> bool {
        match self {
            Peer::Connected(..) => true,
            Peer::Dialing(peer) => peer.is_connected(),
            Peer::Disconnected(..) => false,
            Peer::Local => false
        }
    }

    /// Checks whether the peer is currently being dialed.
    ///
    /// Returns `true` iff [`Peer::into_dialing`] returns `Some`.
    pub fn is_dialing(&self) -> bool {
        match self {
            Peer::Dialing(_) => true,
            Peer::Connected(peer) => peer.is_dialing(),
            Peer::Disconnected(..) => false,
            Peer::Local => false
        }
    }

    /// Checks whether the peer is currently disconnected.
    ///
    /// Returns `true` iff [`Peer::into_disconnected`] returns `Some`.
    pub fn is_disconnected(&self) -> bool {
        matches!(self, Peer::Disconnected(..))
    }

    /// Initiates a new dialing attempt to this peer using the given addresses.
    ///
    /// The connection ID of the first connection attempt, i.e. to `address`,
    /// is returned, together with a [`DialingPeer`] for further use. The
    /// `remaining` addresses are tried in order in subsequent connection
    /// attempts in the context of the same dialing attempt, if the connection
    /// attempt to the first address fails.
    pub fn dial<I>(self, address: Multiaddr, remaining: I, handler: THandler)
        -> Result<
            (ConnectionId, DialingPeer<'a, TTrans, TInEvent, TOutEvent, THandler>),
            DialError
        >
    where
        I: IntoIterator<Item = Multiaddr>,
    {
        let (peer_id, network) = match self {
            Peer::Connected(p) => (p.peer_id, p.network),
            Peer::Dialing(p) => (p.peer_id, p.network),
            Peer::Disconnected(p) => (p.peer_id, p.network),
            Peer::Local => return Err(DialError::ConnectionLimit(ConnectionLimit {
                current: 0, limit: 0
            }))
        };

        let id = network.dial_peer(DialingOpts {
            peer: peer_id,
            handler,
            address,
            remaining: remaining.into_iter().collect(),
        })?;

        Ok((id, DialingPeer { network, peer_id }))
    }

    /// Converts the peer into a `ConnectedPeer`, if an established connection exists.
    ///
    /// Succeeds if the there is at least one established connection to the peer.
    pub fn into_connected(self) -> Option<
        ConnectedPeer<'a, TTrans, TInEvent, TOutEvent, THandler>
    > {
        match self {
            Peer::Connected(peer) => Some(peer),
            Peer::Dialing(peer) => peer.into_connected(),
            Peer::Disconnected(..) => None,
            Peer::Local => None,
        }
    }

    /// Converts the peer into a `DialingPeer`, if a dialing attempt exists.
    ///
    /// Succeeds if the there is at least one pending outgoing connection to the peer.
    pub fn into_dialing(self) -> Option<
        DialingPeer<'a, TTrans, TInEvent, TOutEvent, THandler>
    > {
        match self {
            Peer::Dialing(peer) => Some(peer),
            Peer::Connected(peer) => peer.into_dialing(),
            Peer::Disconnected(..) => None,
            Peer::Local => None
        }
    }

    /// Converts the peer into a `DisconnectedPeer`, if neither an established connection
    /// nor a dialing attempt exists.
    pub fn into_disconnected(self) -> Option<
        DisconnectedPeer<'a, TTrans, TInEvent, TOutEvent, THandler>
    > {
        match self {
            Peer::Disconnected(peer) => Some(peer),
            _ => None,
        }
    }
}

/// The representation of a peer in a [`Network`] to whom at least
/// one established connection exists. There may also be additional ongoing
/// dialing attempts to the peer.
pub struct ConnectedPeer<'a, TTrans, TInEvent, TOutEvent, THandler>
where
    TTrans: Transport,
    THandler: IntoConnectionHandler,
{
    network: &'a mut Network<TTrans, TInEvent, TOutEvent, THandler>,
    peer_id: PeerId,
}

impl<'a, TTrans, TInEvent, TOutEvent, THandler>
    ConnectedPeer<'a, TTrans, TInEvent, TOutEvent, THandler>
where
    TTrans: Transport,
    THandler: IntoConnectionHandler,
{
    pub fn id(&self) -> &PeerId {
        &self.peer_id
    }

    /// Returns the `ConnectedPeer` into a `Peer`.
    pub fn into_peer(self) -> Peer<'a, TTrans, TInEvent, TOutEvent, THandler> {
        Peer::Connected(self)
    }

    /// Obtains an established connection to the peer by ID.
    pub fn connection(&mut self, id: ConnectionId)
        -> Option<EstablishedConnection<TInEvent>>
    {
        self.network.pool.get_established(id)
    }

    /// The number of established connections to the peer.
    pub fn num_connections(&self) -> u32 {
        self.network.pool.num_peer_established(&self.peer_id)
    }

    /// Checks whether there is an ongoing dialing attempt to the peer.
    ///
    /// Returns `true` iff [`ConnectedPeer::into_dialing`] returns `Some`.
    pub fn is_dialing(&self) -> bool {
        self.network.dialing.contains_key(&self.peer_id)
    }

    /// Converts this peer into a [`DialingPeer`], if there is an ongoing
    /// dialing attempt, `None` otherwise.
    pub fn into_dialing(self) -> Option<
        DialingPeer<'a, TTrans, TInEvent, TOutEvent, THandler>
    > {
        if self.network.dialing.contains_key(&self.peer_id) {
            Some(DialingPeer { network: self.network, peer_id: self.peer_id })
        } else {
            None
        }
    }

    /// Gets an iterator over all established connections to the peer.
    pub fn connections(&mut self) ->
        EstablishedConnectionIter<
            impl Iterator<Item = ConnectionId>,
            TInEvent,
            TOutEvent,
            THandler,
            TTrans::Error,
            <THandler::Handler as ConnectionHandler>::Error>
    {
        self.network.pool.iter_peer_established(&self.peer_id)
    }

    /// Obtains some established connection to the peer.
    pub fn some_connection(&mut self)
        -> EstablishedConnection<TInEvent>
    {
        self.connections()
            .into_first()
            .expect("By `Peer::new` and the definition of `ConnectedPeer`.")
    }

    /// Disconnects from the peer, closing all connections.
    pub fn disconnect(self)
        -> DisconnectedPeer<'a, TTrans, TInEvent, TOutEvent, THandler>
    {
        self.network.disconnect(&self.peer_id);
        DisconnectedPeer { network: self.network, peer_id: self.peer_id }
    }
}

impl<'a, TTrans, TInEvent, TOutEvent, THandler> fmt::Debug for
    ConnectedPeer<'a, TTrans, TInEvent, TOutEvent, THandler>
where
    TTrans: Transport,
    THandler: IntoConnectionHandler,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_struct("ConnectedPeer")
            .field("peer_id", &self.peer_id)
            .field("established", &self.network.pool.iter_peer_established_info(&self.peer_id))
            .field("attempts", &self.network.dialing.get(&self.peer_id))
            .finish()
    }
}

/// The representation of a peer in a [`Network`] to whom a dialing
/// attempt is ongoing. There may already exist other established
/// connections to this peer.
pub struct DialingPeer<'a, TTrans, TInEvent, TOutEvent, THandler>
where
    TTrans: Transport,
    THandler: IntoConnectionHandler,
{
    network: &'a mut Network<TTrans, TInEvent, TOutEvent, THandler>,
    peer_id: PeerId,
}

impl<'a, TTrans, TInEvent, TOutEvent, THandler>
    DialingPeer<'a, TTrans, TInEvent, TOutEvent, THandler>
where
    TTrans: Transport,
    THandler: IntoConnectionHandler,
{
    pub fn id(&self) -> &PeerId {
        &self.peer_id
    }

    /// Returns the `DialingPeer` into a `Peer`.
    pub fn into_peer(self) -> Peer<'a, TTrans, TInEvent, TOutEvent, THandler> {
        Peer::Dialing(self)
    }

    /// Disconnects from this peer, closing all established connections and
    /// aborting all dialing attempts.
    pub fn disconnect(self)
        -> DisconnectedPeer<'a, TTrans, TInEvent, TOutEvent, THandler>
    {
        self.network.disconnect(&self.peer_id);
        DisconnectedPeer { network: self.network, peer_id: self.peer_id }
    }

    /// Checks whether there is an established connection to the peer.
    ///
    /// Returns `true` iff [`DialingPeer::into_connected`] returns `Some`.
    pub fn is_connected(&self) -> bool {
        self.network.pool.is_connected(&self.peer_id)
    }

    /// Converts the peer into a `ConnectedPeer`, if an established connection exists.
    pub fn into_connected(self)
        -> Option<ConnectedPeer<'a, TTrans, TInEvent, TOutEvent, THandler>>
    {
        if self.is_connected() {
            Some(ConnectedPeer { peer_id: self.peer_id, network: self.network })
        } else {
            None
        }
    }

    /// Obtains a dialing attempt to the peer by connection ID of
    /// the current connection attempt.
    pub fn attempt(&mut self, id: ConnectionId)
        -> Option<DialingAttempt<'_, TInEvent>>
    {
        if let hash_map::Entry::Occupied(attempts) = self.network.dialing.entry(self.peer_id) {
            if let Some(pos) = attempts.get().iter().position(|s| s.current.0 == id) {
                if let Some(inner) = self.network.pool.get_outgoing(id) {
                    return Some(DialingAttempt { pos, inner, attempts })
                }
            }
        }
        None
    }

    /// Gets an iterator over all dialing (i.e. pending outgoing) connections to the peer.
    pub fn attempts(&mut self)
        -> DialingAttemptIter<'_,
            TInEvent,
            TOutEvent,
            THandler,
            TTrans::Error,
            <THandler::Handler as ConnectionHandler>::Error>
    {
        DialingAttemptIter::new(&self.peer_id, &mut self.network.pool, &mut self.network.dialing)
    }

    /// Obtains some dialing connection to the peer.
    ///
    /// At least one dialing connection is guaranteed to exist on a `DialingPeer`.
    pub fn some_attempt(&mut self)
        -> DialingAttempt<'_, TInEvent>
    {
        self.attempts()
            .into_first()
            .expect("By `Peer::new` and the definition of `DialingPeer`.")
    }
}

impl<'a, TTrans, TInEvent, TOutEvent, THandler> fmt::Debug for
    DialingPeer<'a, TTrans, TInEvent, TOutEvent, THandler>
where
    TTrans: Transport,
    THandler: IntoConnectionHandler,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_struct("DialingPeer")
            .field("peer_id", &self.peer_id)
            .field("established", &self.network.pool.iter_peer_established_info(&self.peer_id))
            .field("attempts", &self.network.dialing.get(&self.peer_id))
            .finish()
    }
}

/// The representation of a peer to whom the `Network` has currently
/// neither an established connection, nor an ongoing dialing attempt
/// initiated by the local peer.
pub struct DisconnectedPeer<'a, TTrans, TInEvent, TOutEvent, THandler>
where
    TTrans: Transport,
    THandler: IntoConnectionHandler,
{
    peer_id: PeerId,
    network: &'a mut Network<TTrans, TInEvent, TOutEvent, THandler>,
}

impl<'a, TTrans, TInEvent, TOutEvent, THandler> fmt::Debug for
    DisconnectedPeer<'a, TTrans, TInEvent, TOutEvent, THandler>
where
    TTrans: Transport,
    THandler: IntoConnectionHandler,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_struct("DisconnectedPeer")
            .field("peer_id", &self.peer_id)
            .finish()
    }
}

impl<'a, TTrans, TInEvent, TOutEvent, THandler>
    DisconnectedPeer<'a, TTrans, TInEvent, TOutEvent, THandler>
where
    TTrans: Transport,
    THandler: IntoConnectionHandler,
{
    pub fn id(&self) -> &PeerId {
        &self.peer_id
    }

    /// Returns the `DisconnectedPeer` into a `Peer`.
    pub fn into_peer(self) -> Peer<'a, TTrans, TInEvent, TOutEvent, THandler> {
        Peer::Disconnected(self)
    }

    /// Moves the peer into a connected state by supplying an existing
    /// established connection.
    ///
    /// No event is generated for this action.
    ///
    /// # Panics
    ///
    /// Panics if `connected.peer_id` does not identify the current peer.
    pub fn set_connected<TMuxer>(
        self,
        connected: Connected,
        connection: Connection<TMuxer, THandler::Handler>,
    ) -> Result<
        ConnectedPeer<'a, TTrans, TInEvent, TOutEvent, THandler>,
        ConnectionLimit
    > where
        TInEvent: Send + 'static,
        TOutEvent: Send + 'static,
        THandler: Send + 'static,
        TTrans::Error: Send + 'static,
        THandler::Handler: ConnectionHandler<Substream = Substream<TMuxer>, InEvent = TInEvent, OutEvent = TOutEvent> + Send,
        <THandler::Handler as ConnectionHandler>::OutboundOpenInfo: Send,
        <THandler::Handler as ConnectionHandler>::Error: error::Error + Send + 'static,
        TMuxer: StreamMuxer + Send + Sync + 'static,
        TMuxer::OutboundSubstream: Send,
    {
        if connected.peer_id != self.peer_id {
            panic!("Invalid peer ID given: {:?}. Expected: {:?}", connected.peer_id, self.peer_id)
        }

        self.network.pool.add(connection, connected)
            .map(move |_id| ConnectedPeer {
                network: self.network,
                peer_id: self.peer_id,
            })
    }
}

/// The (internal) state of a `DialingAttempt`, tracking the
/// current connection attempt as well as remaining addresses.
#[derive(Debug, Clone)]
pub(super) struct DialingState {
    /// The ID and (remote) address of the current connection attempt.
    pub(super) current: (ConnectionId, Multiaddr),
    /// Multiaddresses to attempt if the current one fails.
    pub(super) remaining: Vec<Multiaddr>,
}

/// A `DialingAttempt` is an ongoing outgoing connection attempt to
/// a known / expected remote peer ID and a list of alternative addresses
/// to connect to, if the current connection attempt fails.
pub struct DialingAttempt<'a, TInEvent> {
    /// The underlying pending connection in the `Pool`.
    inner: PendingConnection<'a, TInEvent>,
    /// All current dialing attempts of the peer.
    attempts: hash_map::OccupiedEntry<'a, PeerId, SmallVec<[DialingState; 10]>>,
    /// The position of the current `DialingState` of this connection in the `attempts`.
    pos: usize,
}

impl<'a, TInEvent>
    DialingAttempt<'a, TInEvent>
{
    /// Returns the ID of the current connection attempt.
    pub fn id(&self) -> ConnectionId {
        self.inner.id()
    }

    /// Returns the (expected) peer ID of the dialing attempt.
    pub fn peer_id(&self) -> &PeerId {
        self.attempts.key()
    }

    /// Returns the remote address of the current connection attempt.
    pub fn address(&self) -> &Multiaddr {
        match self.inner.endpoint() {
            ConnectedPoint::Dialer { address } => address,
            ConnectedPoint::Listener { .. } => unreachable!("by definition of a `DialingAttempt`.")
        }
    }

    /// Aborts the dialing attempt.
    ///
    /// Aborting a dialing attempt involves aborting the current connection
    /// attempt and dropping any remaining addresses given to [`Peer::dial()`]
    /// that have not yet been tried.
    pub fn abort(mut self) {
        self.attempts.get_mut().remove(self.pos);
        if self.attempts.get().is_empty() {
            self.attempts.remove();
        }
        self.inner.abort();
    }

    /// Adds an address to the end of the remaining addresses
    /// for this dialing attempt. Duplicates are ignored.
    pub fn add_address(&mut self, addr: Multiaddr) {
        let remaining = &mut self.attempts.get_mut()[self.pos].remaining;
        if remaining.iter().all(|a| a != &addr) {
            remaining.push(addr);
        }
    }
}

/// An iterator over the ongoing dialing attempts to a peer.
pub struct DialingAttemptIter<'a, TInEvent, TOutEvent, THandler, TTransErr, THandlerErr> {
    /// The peer whose dialing attempts are being iterated.
    peer_id: &'a PeerId,
    /// The underlying connection `Pool` of the `Network`.
    pool: &'a mut Pool<TInEvent, TOutEvent, THandler, TTransErr, THandlerErr>,
    /// The state of all current dialing attempts known to the `Network`.
    ///
    /// Ownership of the `OccupiedEntry` for `peer_id` containing all attempts must be
    /// borrowed to each `DialingAttempt` in order for it to remove the entry if the
    /// last dialing attempt is aborted.
    dialing: &'a mut FnvHashMap<PeerId, SmallVec<[DialingState; 10]>>,
    /// The current position of the iterator in `dialing[peer_id]`.
    pos: usize,
    /// The total number of elements in `dialing[peer_id]` to iterate over.
    end: usize,
}

// Note: Ideally this would be an implementation of `Iterator`, but that
// requires GATs (cf. https://github.com/rust-lang/rust/issues/44265) and
// a different definition of `Iterator`.
impl<'a, TInEvent, TOutEvent, THandler, TTransErr, THandlerErr>
    DialingAttemptIter<'a, TInEvent, TOutEvent, THandler, TTransErr, THandlerErr>
{
    fn new(
        peer_id: &'a PeerId,
        pool: &'a mut Pool<TInEvent, TOutEvent, THandler, TTransErr, THandlerErr>,
        dialing: &'a mut FnvHashMap<PeerId, SmallVec<[DialingState; 10]>>,
    ) -> Self {
        let end = dialing.get(peer_id).map_or(0, |conns| conns.len());
        Self { pos: 0, end, pool, dialing, peer_id }
    }

    /// Obtains the next dialing connection, if any.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<DialingAttempt<'_, TInEvent>> {
        // If the number of elements reduced, the current `DialingAttempt` has been
        // aborted and iteration needs to continue from the previous position to
        // account for the removed element.
        let end = self.dialing.get(self.peer_id).map_or(0, |conns| conns.len());
        if self.end > end {
            self.end = end;
            self.pos -= 1;
        }

        if self.pos == self.end {
            return None
        }

        if let hash_map::Entry::Occupied(attempts) = self.dialing.entry(*self.peer_id) {
            let id = attempts.get()[self.pos].current.0;
            if let Some(inner) = self.pool.get_outgoing(id) {
                let conn = DialingAttempt { pos: self.pos, inner, attempts };
                self.pos += 1;
                return Some(conn)
            }
        }

        None
    }

    /// Returns the first connection, if any, consuming the iterator.
    pub fn into_first<'b>(self)
        -> Option<DialingAttempt<'b, TInEvent>>
    where 'a: 'b
    {
        if self.pos == self.end {
            return None
        }

        if let hash_map::Entry::Occupied(attempts) = self.dialing.entry(*self.peer_id) {
            let id = attempts.get()[self.pos].current.0;
            if let Some(inner) = self.pool.get_outgoing(id) {
                return Some(DialingAttempt { pos: self.pos, inner, attempts })
            }
        }

        None
    }
}
