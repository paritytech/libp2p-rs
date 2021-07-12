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

//! Network events and associated information.

use crate::{
    Multiaddr,
    connection::{
        ConnectionId,
        ConnectedPoint,
        ConnectionError,
        ConnectionHandler,
        Connected,
        EstablishedConnection,
        IntoConnectionHandler,
        ListenerId,
        PendingConnectionError,
    },
    transport::Transport,
    PeerId
};
use std::{fmt, num::NonZeroU32};

/// Event that can happen on the `Network`.
pub enum NetworkEvent<'a, TTrans, TInEvent, TOutEvent, THandler>
where
    TTrans: Transport,
    THandler: IntoConnectionHandler,
{
    /// One of the listeners gracefully closed.
    ListenerClosed {
        /// The listener ID that closed.
        listener_id: ListenerId,
        /// The addresses that the listener was listening on.
        addresses: Vec<Multiaddr>,
        /// Reason for the closure. Contains `Ok(())` if the stream produced `None`, or `Err`
        /// if the stream produced an error.
        reason: Result<(), TTrans::Error>,
    },

    /// One of the listeners reported a non-fatal error.
    ListenerError {
        /// The listener that errored.
        listener_id: ListenerId,
        /// The listener error.
        error: TTrans::Error
    },

    /// One of the listeners is now listening on an additional address.
    NewListenerAddress {
        /// The listener that is listening on the new address.
        listener_id: ListenerId,
        /// The new address the listener is now also listening on.
        listen_addr: Multiaddr
    },

    /// One of the listeners is no longer listening on some address.
    ExpiredListenerAddress {
        /// The listener that is no longer listening on some address.
        listener_id: ListenerId,
        /// The expired address.
        listen_addr: Multiaddr
    },

    /// A new connection arrived on a listener.
    ///
    /// To accept the connection, see [`Network::accept`](crate::Network::accept).
    IncomingConnection {
        /// The listener who received the connection.
        listener_id: ListenerId,
        /// The pending incoming connection.
        connection: IncomingConnection<TTrans::ListenerUpgrade>,
    },

    /// An error happened on a connection during its initial handshake.
    ///
    /// This can include, for example, an error during the handshake of the encryption layer, or
    /// the connection unexpectedly closed.
    IncomingConnectionError {
        /// Local connection address.
        local_addr: Multiaddr,
        /// Address used to send back data to the remote.
        send_back_addr: Multiaddr,
        /// The error that happened.
        error: PendingConnectionError<TTrans::Error>,
    },

    /// A new connection to a peer has been established.
    ConnectionEstablished {
        /// The newly established connection.
        connection: EstablishedConnection<'a, TInEvent>,
        /// The total number of established connections to the same peer,
        /// including the one that has just been opened.
        num_established: NonZeroU32,
    },

    /// An established connection to a peer has been closed.
    ///
    /// A connection may close if
    ///
    ///   * it encounters an error, which includes the connection being
    ///     closed by the remote. In this case `error` is `ome`.
    ///   * it was actively closed by [`EstablishedConnection::start_close`],
    ///     i.e. a successful, orderly close. In this case `error` is `None`.
    ///   * it was actively closed by [`super::peer::ConnectedPeer::disconnect`] or
    ///     [`super::peer::DialingPeer::disconnect`], i.e. dropped without an
    ///     orderly close. In this case `error` is `None`.
    ///
    ConnectionClosed {
        /// The ID of the connection that encountered an error.
        id: ConnectionId,
        /// Information about the connection that encountered the error.
        connected: Connected,
        /// The error that occurred.
        error: Option<ConnectionError<<THandler::Handler as ConnectionHandler>::Error>>,
        /// The remaining number of established connections to the same peer.
        num_established: u32,
    },

    /// A dialing attempt to an address of a peer failed.
    DialError {
        /// The number of remaining dialing attempts.
        attempts_remaining: u32,

        /// Id of the peer we were trying to dial.
        peer_id: PeerId,

        /// The multiaddr we failed to reach.
        multiaddr: Multiaddr,

        /// The error that happened.
        error: PendingConnectionError<TTrans::Error>,
    },

    /// Failed to reach a peer that we were trying to dial.
    UnknownPeerDialError {
        /// The multiaddr we failed to reach.
        multiaddr: Multiaddr,

        /// The error that happened.
        error: PendingConnectionError<TTrans::Error>,
    },

    /// An established connection produced an event.
    ConnectionEvent {
        /// The connection on which the event occurred.
        connection: EstablishedConnection<'a, TInEvent>,
        /// Event that was produced by the node.
        event: TOutEvent,
    },

    /// An established connection has changed its address.
    AddressChange {
        /// The connection whose address has changed.
        connection: EstablishedConnection<'a, TInEvent>,
        /// New endpoint of this connection.
        new_endpoint: ConnectedPoint,
        /// Old endpoint of this connection.
        old_endpoint: ConnectedPoint,
    },
}

impl<TTrans, TInEvent, TOutEvent, THandler> fmt::Debug for
    NetworkEvent<'_, TTrans, TInEvent, TOutEvent, THandler>
where
    TInEvent: fmt::Debug,
    TOutEvent: fmt::Debug,
    TTrans: Transport,
    TTrans::Error: fmt::Debug,
    THandler: IntoConnectionHandler,
    <THandler::Handler as ConnectionHandler>::Error: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            NetworkEvent::NewListenerAddress { listener_id, listen_addr } => {
                f.debug_struct("NewListenerAddress")
                    .field("listener_id", listener_id)
                    .field("listen_addr", listen_addr)
                    .finish()
            }
            NetworkEvent::ExpiredListenerAddress { listener_id, listen_addr } => {
                f.debug_struct("ExpiredListenerAddress")
                    .field("listener_id", listener_id)
                    .field("listen_addr", listen_addr)
                    .finish()
            }
            NetworkEvent::ListenerClosed { listener_id, addresses, reason } => {
                f.debug_struct("ListenerClosed")
                    .field("listener_id", listener_id)
                    .field("addresses", addresses)
                    .field("reason", reason)
                    .finish()
            }
            NetworkEvent::ListenerError { listener_id, error } => {
                f.debug_struct("ListenerError")
                    .field("listener_id", listener_id)
                    .field("error", error)
                    .finish()
            }
            NetworkEvent::IncomingConnection { connection, .. } => {
                f.debug_struct("IncomingConnection")
                    .field("local_addr", &connection.local_addr)
                    .field("send_back_addr", &connection.send_back_addr)
                    .finish()
            }
            NetworkEvent::IncomingConnectionError { local_addr, send_back_addr, error } => {
                f.debug_struct("IncomingConnectionError")
                    .field("local_addr", local_addr)
                    .field("send_back_addr", send_back_addr)
                    .field("error", error)
                    .finish()
            }
            NetworkEvent::ConnectionEstablished { connection, .. } => {
                f.debug_struct("ConnectionEstablished")
                    .field("connection", connection)
                    .finish()
            }
            NetworkEvent::ConnectionClosed { id, connected, error, .. } => {
                f.debug_struct("ConnectionClosed")
                    .field("id", id)
                    .field("connected", connected)
                    .field("error", error)
                    .finish()
            }
            NetworkEvent::DialError { attempts_remaining, peer_id, multiaddr, error } => {
                f.debug_struct("DialError")
                    .field("attempts_remaining", attempts_remaining)
                    .field("peer_id", peer_id)
                    .field("multiaddr", multiaddr)
                    .field("error", error)
                    .finish()
            }
            NetworkEvent::UnknownPeerDialError { multiaddr, error, .. } => {
                f.debug_struct("UnknownPeerDialError")
                    .field("multiaddr", multiaddr)
                    .field("error", error)
                    .finish()
            }
            NetworkEvent::ConnectionEvent { connection, event } => {
                f.debug_struct("ConnectionEvent")
                    .field("connection", connection)
                    .field("event", event)
                    .finish()
            }
            NetworkEvent::AddressChange { connection, new_endpoint, old_endpoint } => {
                f.debug_struct("AddressChange")
                    .field("connection", connection)
                    .field("new_endpoint", new_endpoint)
                    .field("old_endpoint", old_endpoint)
                    .finish()
            }
        }
    }
}

/// A pending incoming connection produced by a listener.
pub struct IncomingConnection<TUpgrade> {
    /// The connection upgrade.
    pub(crate) upgrade: TUpgrade,
    /// Local connection address.
    pub local_addr: Multiaddr,
    /// Address used to send back data to the remote.
    pub send_back_addr: Multiaddr,
}
