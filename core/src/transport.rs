// Copyright 2017-2018 Parity Technologies (UK) Ltd.
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

//! Connection-oriented communication channels.
//!
//! The main entity of this module is the [`Transport`] trait, which provides an
//! interface for establishing connections with other nodes, thereby negotiating
//! any desired protocols. The rest of the module defines combinators for
//! modifying a transport through composition with other transports or protocol upgrades.

use crate::ConnectedPoint;
use futures::prelude::*;
use multiaddr::Multiaddr;
use std::{error::Error, fmt};

pub mod and_then;
pub mod choice;
pub mod dummy;
pub mod map;
pub mod map_err;
pub mod memory;
pub mod timeout;
pub mod upgrade;

mod boxed;
mod optional;

pub use self::boxed::Boxed;
pub use self::choice::OrTransport;
pub use self::memory::MemoryTransport;
pub use self::optional::OptionalTransport;
pub use self::upgrade::Upgrade;

/// A transport provides connection-oriented communication between two peers
/// through ordered streams of data (i.e. connections).
///
/// Connections are established either by [listening](Transport::listen_on)
/// or [dialing](Transport::dial) on a [`Transport`]. A peer that
/// obtains a connection by listening is often referred to as the *listener* and the
/// peer that initiated the connection through dialing as the *dialer*, in
/// contrast to the traditional roles of *server* and *client*.
///
/// Most transports also provide a form of reliable delivery on the established
/// connections but the precise semantics of these guarantees depend on the
/// specific transport.
///
/// This trait is implemented for concrete connection-oriented transport protocols
/// like TCP or Unix Domain Sockets, but also on wrappers that add additional
/// functionality to the dialing or listening process (e.g. name resolution via
/// the DNS).
///
/// Additional protocols can be layered on top of the connections established
/// by a [`Transport`] through an upgrade mechanism that is initiated via
/// [`upgrade`](Transport::upgrade).
///
/// > **Note**: The methods of this trait use `self` and not `&self` or `&mut self`. In other
/// >           words, listening or dialing consumes the transport object. This has been designed
/// >           so that you would implement this trait on `&Foo` or `&mut Foo` instead of directly
/// >           on `Foo`.
pub trait Transport {
    /// The result of a connection setup process, including protocol upgrades.
    ///
    /// Typically the output contains at least a handle to a data stream (i.e. a
    /// connection or a substream multiplexer on top of a connection) that
    /// provides APIs for sending and receiving data through the connection.
    type Output;

    /// An error that occurred during connection setup.
    type Error: Error;

    /// A stream of [`Output`](Transport::Output)s for inbound connections.
    ///
    /// An item should be produced whenever a connection is received at the lowest level of the
    /// transport stack. The item must be a [`ListenerUpgrade`](Transport::ListenerUpgrade) future
    /// that resolves to an [`Output`](Transport::Output) value once all protocol upgrades
    /// have been applied.
    ///
    /// If this stream produces an error, it is considered fatal and the listener is killed. It
    /// is possible to report non-fatal errors by producing a [`ListenerEvent::Error`].
    type Listener: Stream<Item = Result<ListenerEvent<Self::ListenerUpgrade, Self::Error>, Self::Error>>;

    /// A pending [`Output`](Transport::Output) for an inbound connection,
    /// obtained from the [`Listener`](Transport::Listener) stream.
    ///
    /// After a connection has been accepted by the transport, it may need to go through
    /// asynchronous post-processing (i.e. protocol upgrade negotiations). Such
    /// post-processing should not block the `Listener` from producing the next
    /// connection, hence further connection setup proceeds asynchronously.
    /// Once a `ListenerUpgrade` future resolves it yields the [`Output`](Transport::Output)
    /// of the connection setup process.
    type ListenerUpgrade: Future<Output = Result<Self::Output, Self::Error>>;

    /// A pending [`Output`](Transport::Output) for an outbound connection,
    /// obtained from [dialing](Transport::dial).
    type Dial: Future<Output = Result<Self::Output, Self::Error>>;

    /// Listens on the given [`Multiaddr`], producing a stream of pending, inbound connections
    /// and addresses this transport is listening on (cf. [`ListenerEvent`]).
    ///
    /// Returning an error from the stream is considered fatal. The listener can also report
    /// non-fatal errors by producing a [`ListenerEvent::Error`].
    fn listen_on(self, addr: Multiaddr) -> Result<Self::Listener, TransportError<Self::Error>>
    where
        Self: Sized;

    /// Dials the given [`Multiaddr`], returning a future for a pending outbound connection.
    ///
    /// If [`TransportError::MultiaddrNotSupported`] is returned, it may be desirable to
    /// try an alternative [`Transport`], if available.
    fn dial(self, addr: Multiaddr) -> Result<Self::Dial, TransportError<Self::Error>>
    where
        Self: Sized;

    /// Performs a transport-specific mapping of an address `observed` by
    /// a remote onto a local `listen` address to yield an address for
    /// the local node that may be reachable for other peers.
    fn address_translation(&self, listen: &Multiaddr, observed: &Multiaddr) -> Option<Multiaddr>;

    /// Boxes the transport, including custom transport errors.
    fn boxed(self) -> boxed::Boxed<Self::Output>
    where
        Self: Transport + Sized + Clone + Send + Sync + 'static,
        Self::Dial: Send + 'static,
        Self::Listener: Send + 'static,
        Self::ListenerUpgrade: Send + 'static,
        Self::Error: Send + Sync,
    {
        boxed::boxed(self)
    }

    /// Applies a function on the connections created by the transport.
    fn map<F, O>(self, f: F) -> map::Map<Self, F>
    where
        Self: Sized,
        F: FnOnce(Self::Output, ConnectedPoint) -> O + Clone
    {
        map::Map::new(self, f)
    }

    /// Applies a function on the errors generated by the futures of the transport.
    fn map_err<F, E>(self, f: F) -> map_err::MapErr<Self, F>
    where
        Self: Sized,
        F: FnOnce(Self::Error) -> E + Clone
    {
        map_err::MapErr::new(self, f)
    }

    /// Adds a fallback transport that is used when encountering errors
    /// while establishing inbound or outbound connections.
    ///
    /// The returned transport will act like `self`, except that if `listen_on` or `dial`
    /// return an error then `other` will be tried.
    fn or_transport<U>(self, other: U) -> OrTransport<Self, U>
    where
        Self: Sized,
        U: Transport,
        <U as Transport>::Error: 'static
    {
        OrTransport::new(self, other)
    }

    /// Applies a function producing an asynchronous result to every connection
    /// created by this transport.
    ///
    /// This function can be used for ad-hoc protocol upgrades or
    /// for processing or adapting the output for following configurations.
    ///
    /// For the high-level transport upgrade procedure, see [`Transport::upgrade`].
    fn and_then<C, F, O>(self, f: C) -> and_then::AndThen<Self, C>
    where
        Self: Sized,
        C: FnOnce(Self::Output, ConnectedPoint) -> F + Clone,
        F: TryFuture<Ok = O>,
        <F as TryFuture>::Error: Error + 'static
    {
        and_then::AndThen::new(self, f)
    }

    /// Begins a series of protocol upgrades via an
    /// [`upgrade::Builder`](upgrade::Builder).
    //
    // TODO: Method still needed now that `upgrade` takes `self` only?
    fn upgrade(self) -> upgrade::Builder<Self>
    where
        Self: Sized,
        Self::Error: 'static
    {
        upgrade::Builder::new(self)
    }
}

/// Event produced by [`Transport::Listener`]s.
///
/// Transports are expected to produce `Upgrade` events only for
/// listen addresses which have previously been announced via
/// a `NewAddress` event and which have not been invalidated by
/// an `AddressExpired` event yet.
#[derive(Clone, Debug, PartialEq)]
pub enum ListenerEvent<TUpgr, TErr> {
    /// The transport is listening on a new additional [`Multiaddr`].
    NewAddress(Multiaddr),
    /// An upgrade, consisting of the upgrade future, the listener address and the remote address.
    Upgrade {
        /// The upgrade.
        upgrade: TUpgr,
        /// The local address which produced this upgrade.
        local_addr: Multiaddr,
        /// The remote address which produced this upgrade.
        remote_addr: Multiaddr
    },
    /// A [`Multiaddr`] is no longer used for listening.
    AddressExpired(Multiaddr),
    /// A non-fatal error has happened on the listener.
    ///
    /// This event should be generated in order to notify the user that something wrong has
    /// happened. The listener, however, continues to run.
    Error(TErr),
}

impl<TUpgr, TErr> ListenerEvent<TUpgr, TErr> {
    /// In case this [`ListenerEvent`] is an upgrade, apply the given function
    /// to the upgrade and multiaddress and produce another listener event
    /// based the the function's result.
    pub fn map<U>(self, f: impl FnOnce(TUpgr) -> U) -> ListenerEvent<U, TErr> {
        match self {
            ListenerEvent::Upgrade { upgrade, local_addr, remote_addr } => {
                ListenerEvent::Upgrade { upgrade: f(upgrade), local_addr, remote_addr }
            }
            ListenerEvent::NewAddress(a) => ListenerEvent::NewAddress(a),
            ListenerEvent::AddressExpired(a) => ListenerEvent::AddressExpired(a),
            ListenerEvent::Error(e) => ListenerEvent::Error(e),
        }
    }

    /// In case this [`ListenerEvent`] is an [`Error`](ListenerEvent::Error),
    /// apply the given function to the error and produce another listener event based on the
    /// function's result.
    pub fn map_err<U>(self, f: impl FnOnce(TErr) -> U) -> ListenerEvent<TUpgr, U> {
        match self {
            ListenerEvent::Upgrade { upgrade, local_addr, remote_addr } =>
                ListenerEvent::Upgrade { upgrade, local_addr, remote_addr },
            ListenerEvent::NewAddress(a) => ListenerEvent::NewAddress(a),
            ListenerEvent::AddressExpired(a) => ListenerEvent::AddressExpired(a),
            ListenerEvent::Error(e) => ListenerEvent::Error(f(e)),
        }
    }

    /// Returns `true` if this is an `Upgrade` listener event.
    pub fn is_upgrade(&self) -> bool {
        matches!(self, ListenerEvent::Upgrade {..})
    }

    /// Try to turn this listener event into upgrade parts.
    ///
    /// Returns `None` if the event is not actually an upgrade,
    /// otherwise the upgrade and the remote address.
    pub fn into_upgrade(self) -> Option<(TUpgr, Multiaddr)> {
        if let ListenerEvent::Upgrade { upgrade, remote_addr, .. } = self {
            Some((upgrade, remote_addr))
        } else {
            None
        }
    }

    /// Returns `true` if this is a `NewAddress` listener event.
    pub fn is_new_address(&self) -> bool {
        matches!(self, ListenerEvent::NewAddress(_))
    }

    /// Try to turn this listener event into the `NewAddress` part.
    ///
    /// Returns `None` if the event is not actually a `NewAddress`,
    /// otherwise the address.
    pub fn into_new_address(self) -> Option<Multiaddr> {
        if let ListenerEvent::NewAddress(a) = self {
            Some(a)
        } else {
            None
        }
    }

    /// Returns `true` if this is an `AddressExpired` listener event.
    pub fn is_address_expired(&self) -> bool {
        matches!(self, ListenerEvent::AddressExpired(_))
    }

    /// Try to turn this listener event into the `AddressExpired` part.
    ///
    /// Returns `None` if the event is not actually a `AddressExpired`,
    /// otherwise the address.
    pub fn into_address_expired(self) -> Option<Multiaddr> {
        if let ListenerEvent::AddressExpired(a) = self {
            Some(a)
        } else {
            None
        }
    }

    /// Returns `true` if this is an `Error` listener event.
    pub fn is_error(&self) -> bool {
        matches!(self, ListenerEvent::Error(_))
    }

    /// Try to turn this listener event into the `Error` part.
    ///
    /// Returns `None` if the event is not actually a `Error`,
    /// otherwise the error.
    pub fn into_error(self) -> Option<TErr> {
        if let ListenerEvent::Error(err) = self {
            Some(err)
        } else {
            None
        }
    }
}

/// An error during [dialing][Transport::dial] or [listening][Transport::listen_on]
/// on a [`Transport`].
#[derive(Debug, Clone)]
pub enum TransportError<TErr> {
    /// The [`Multiaddr`] passed as parameter is not supported.
    ///
    /// Contains back the same address.
    MultiaddrNotSupported(Multiaddr),

    /// Any other error that a [`Transport`] may produce.
    Other(TErr),
}

impl<TErr> TransportError<TErr> {
    /// Applies a function to the the error in [`TransportError::Other`].
    pub fn map<TNewErr>(self, map: impl FnOnce(TErr) -> TNewErr) -> TransportError<TNewErr> {
        match self {
            TransportError::MultiaddrNotSupported(addr) => TransportError::MultiaddrNotSupported(addr),
            TransportError::Other(err) => TransportError::Other(map(err)),
        }
    }
}

impl<TErr> fmt::Display for TransportError<TErr>
where TErr: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransportError::MultiaddrNotSupported(addr) => write!(f, "Multiaddr is not supported: {}", addr),
            TransportError::Other(err) => write!(f, "{}", err),
        }
    }
}

impl<TErr> Error for TransportError<TErr>
where TErr: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            TransportError::MultiaddrNotSupported(_) => None,
            TransportError::Other(err) => Some(err),
        }
    }
}
