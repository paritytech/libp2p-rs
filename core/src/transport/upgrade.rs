// Copyright 2017-2019 Parity Technologies (UK) Ltd.
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

//! Configuration of transport protocol upgrades.


use crate::{
    ConnectedPoint,
    Negotiated,
    transport::{
        Transport,
        TransportError,
        and_then::AndThen,
        boxed::boxed,
        timeout::TransportTimeout,
    },
    muxing::{StreamMuxer, StreamMuxerBox},
    upgrade::{
        self,
        Role,
        Version,
        AuthenticationVersion,
        OutboundUpgrade,
        InboundUpgrade,
        UpgradeError,
        OutboundUpgradeApply,
        InboundUpgradeApply,
        AuthenticationUpgradeApply,
    },
    PeerId
};
use futures::{prelude::*, ready, future::Either};
use multiaddr::Multiaddr;
use std::{
    error::Error,
    pin::Pin,
    task::{Context, Poll},
    time::Duration
};

/// A `Builder` facilitates upgrading of a [`Transport`] for use with
/// a [`Network`].
///
/// The upgrade process is defined by the following stages:
///
///    [`authenticate`](Builder::authenticate)`{1}`
/// -> [`apply`](Authenticated::apply)`{*}`
/// -> [`multiplex`](Authenticated::multiplex)`{1}`
///
/// It thus enforces the following invariants on every transport
/// obtained from [`multiplex`](Authenticated::multiplex):
///
///   1. The transport must be [authenticated](Builder::authenticate)
///      and [multiplexed](Authenticated::multiplex).
///   2. Authentication must precede the negotiation of a multiplexer.
///   3. Applying a multiplexer is the last step in the upgrade process.
///   4. The [`Transport::Output`] conforms to the requirements of a [`Network`],
///      namely a tuple of a [`PeerId`] (from the authentication upgrade) and a
///      [`StreamMuxer`] (from the multiplexing upgrade).
///
/// [`Network`]: crate::Network
#[derive(Clone)]
pub struct Builder<T> {
    inner: T,
}

impl<T> Builder<T>
where
    T: Transport,
    T::Error: 'static,
{
    /// Creates a `Builder` over the given (base) `Transport`.
    pub fn new(inner: T) -> Builder<T> {
        Builder { inner }
    }

    /// Upgrades the transport to perform authentication of the remote.
    ///
    /// The supplied upgrade receives the I/O resource `C` and must
    /// produce a pair `(PeerId, D)`, where `D` is a new I/O resource.
    /// The upgrade must thus at a minimum identify the remote, which typically
    /// involves the use of a cryptographic authentication protocol in the
    /// context of establishing a secure channel.
    ///
    /// ## Transitions
    ///
    ///   * I/O upgrade: `C -> (PeerId, D)`.
    ///   * Transport output: `C -> (PeerId, D)`
    pub fn authenticate<C, D, U, E>(self, upgrade: U) -> Authenticated<
        AndThen<T, impl FnOnce(C, ConnectedPoint) -> AuthenticationUpgradeApply<C, U> + Clone>
    > where
        T: Transport<Output = C>,
        C: AsyncRead + AsyncWrite + Unpin,
        D: AsyncRead + AsyncWrite + Unpin,
        U: InboundUpgrade<Negotiated<C>, Output = (PeerId, D), Error = E>,
        U: OutboundUpgrade<Negotiated<C>, Output = (PeerId, D), Error = E> + Clone,
        E: Error + 'static,
    {
        self.authenticate_with_version(upgrade, AuthenticationVersion::default())
    }

    /// Same as [`Builder::authenticate`] with the option to choose the
    /// [`AuthenticationVersion`] used to upgrade the connection.
    pub fn authenticate_with_version<C, D, U, E>(self, upgrade: U, version: AuthenticationVersion) -> Authenticated<
        AndThen<T, impl FnOnce(C, ConnectedPoint) -> AuthenticationUpgradeApply<C, U> + Clone>
    > where
        T: Transport<Output = C>,
        C: AsyncRead + AsyncWrite + Unpin,
        D: AsyncRead + AsyncWrite + Unpin,
        U: InboundUpgrade<Negotiated<C>, Output = (PeerId, D), Error = E>,
        U: OutboundUpgrade<Negotiated<C>, Output = (PeerId, D), Error = E> + Clone,
        E: Error + 'static,
    {
        Authenticated(Builder::new(self.inner.and_then(move |conn, endpoint| {
                upgrade::apply_authentication(conn, upgrade, endpoint, version)
        })))
    }
}

/// An transport with peer authentication, obtained from [`Builder::authenticate`].
#[derive(Clone)]
pub struct Authenticated<T>(Builder<T>);

impl<T> Authenticated<T>
where
    T: Transport,
    T::Error: 'static
{
    /// Applies an arbitrary upgrade.
    ///
    /// The upgrade receives the I/O resource (i.e. connection) `C` and
    /// must produce a new I/O resource `D`. Any number of such upgrades
    /// can be performed.
    ///
    /// ## Transitions
    ///
    ///   * I/O upgrade: `C -> D`.
    ///   * Transport output: `(PeerId, C) -> (PeerId, D)`.
    pub fn apply<C, D, U, E>(self, upgrade: U) -> Authenticated<AndThen<T, impl FnOnce(((PeerId, Role), C), ConnectedPoint) -> UpgradeAuthenticated<C, U, (PeerId, Role)> + Clone>>
    where
        T: Transport<Output = ((PeerId, Role), C)>,
        C: AsyncRead + AsyncWrite + Unpin,
        D: AsyncRead + AsyncWrite + Unpin,
        U: InboundUpgrade<Negotiated<C>, Output = D, Error = E>,
        U: OutboundUpgrade<Negotiated<C>, Output = D, Error = E> + Clone,
        E: Error + 'static,
    {
        self.apply_with_version(upgrade, Version::default())
    }

    /// Same as [`Authenticated::apply`] with the option to choose the
    /// [`Version`] used to upgrade the connection.
    pub fn apply_with_version<C, D, U, E>(self, upgrade: U, version: Version) -> Authenticated<AndThen<T, impl FnOnce(((PeerId, Role), C), ConnectedPoint) -> UpgradeAuthenticated<C, U, (PeerId, Role)> + Clone>>
    where
        T: Transport<Output = ((PeerId, Role), C)>,
        C: AsyncRead + AsyncWrite + Unpin,
        D: AsyncRead + AsyncWrite + Unpin,
        U: InboundUpgrade<Negotiated<C>, Output = D, Error = E>,
        U: OutboundUpgrade<Negotiated<C>, Output = D, Error = E> + Clone,
        E: Error + 'static,
    {
        Authenticated(Builder::new(self.0.inner.and_then(move |((i, r), c), _endpoint| {
            let upgrade = match r {
                Role::Initiator => {
                    Either::Left(upgrade::apply_outbound(c, upgrade, version))
                },
                Role::Responder => {
                    Either::Right(upgrade::apply_inbound(c, upgrade))

                }
            };
            UpgradeAuthenticated { user_data: Some((i, r)), upgrade }
        })))
    }

    /// Upgrades the transport with a (sub)stream multiplexer.
    ///
    /// The supplied upgrade receives the I/O resource `C` and must
    /// produce a [`StreamMuxer`] `M`. The transport must already be authenticated.
    /// This ends the (regular) transport upgrade process.
    ///
    /// ## Transitions
    ///
    ///   * I/O upgrade: `C -> M`.
    ///   * Transport output: `(PeerId, C) -> (PeerId, M)`.
    pub fn multiplex<C, M, U, E>(self, upgrade: U) -> Multiplexed<
        AndThen<T, impl FnOnce(((PeerId, Role), C), ConnectedPoint) -> UpgradeAuthenticated<C, U, PeerId> + Clone>
    > where
        T: Transport<Output = ((PeerId, Role), C)>,
        C: AsyncRead + AsyncWrite + Unpin,
        M: StreamMuxer,
        U: InboundUpgrade<Negotiated<C>, Output = M, Error = E>,
        U: OutboundUpgrade<Negotiated<C>, Output = M, Error = E> + Clone,
        E: Error + 'static,
    {
        self.multiplex_with_version(upgrade, Version::default())
    }

    /// Same as [`Authenticated::multiplex`] with the option to choose the
    /// [`Version`] used to upgrade the connection.
    pub fn multiplex_with_version<C, M, U, E>(self, upgrade: U, version: Version) -> Multiplexed<
        AndThen<T, impl FnOnce(((PeerId, Role), C), ConnectedPoint) -> UpgradeAuthenticated<C, U, PeerId> + Clone>
    > where
        T: Transport<Output = ((PeerId, Role), C)>,
        C: AsyncRead + AsyncWrite + Unpin,
        M: StreamMuxer,
        U: InboundUpgrade<Negotiated<C>, Output = M, Error = E>,
        U: OutboundUpgrade<Negotiated<C>, Output = M, Error = E> + Clone,
        E: Error + 'static,
    {
        Multiplexed(self.0.inner.and_then(move |((i, r), c), _endpoint| {
            let upgrade = match r {
                Role::Initiator => {
                    Either::Left(upgrade::apply_outbound(c, upgrade, version))
                },
                Role::Responder => {
                    Either::Right(upgrade::apply_inbound(c, upgrade))

                }
            };
            UpgradeAuthenticated { user_data: Some(i), upgrade }
        }))
    }
}

/// An upgrade that negotiates a (sub)stream multiplexer on
/// top of an authenticated transport.
///
/// Configured through [`Authenticated::multiplex`].
#[pin_project::pin_project]
pub struct UpgradeAuthenticated<C, U, D>
where
    C: AsyncRead + AsyncWrite + Unpin,
    U: InboundUpgrade<Negotiated<C>> + OutboundUpgrade<Negotiated<C>>,
{
    user_data: Option<D>,
    #[pin]
    upgrade: EitherUpgrade<C, U>,
}

impl<C, U, M, E, D> Future for UpgradeAuthenticated<C, U, D>
where
    C: AsyncRead + AsyncWrite + Unpin,
    U: InboundUpgrade<Negotiated<C>, Output = M, Error = E>,
    U: OutboundUpgrade<Negotiated<C>, Output = M, Error = E>
{
    type Output = Result<(D, M), UpgradeError<E>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let m = match ready!(Future::poll(this.upgrade, cx)) {
            Ok(m) => m,
            Err(err) => return Poll::Ready(Err(err)),
        };
        let user_data = this.user_data.take().expect("UpgradeAuthenticated future polled after completion.");
        Poll::Ready(Ok((user_data, m)))
    }
}

/// A authenticated and multiplexed transport, obtained from
/// [`Authenticated::multiplex`].
#[derive(Clone)]
pub struct Multiplexed<T>(T);

impl<T> Multiplexed<T> {
    /// Boxes the authenticated, multiplexed transport, including
    /// the [`StreamMuxer`] and custom transport errors.
    pub fn boxed<M>(self) -> super::Boxed<(PeerId, StreamMuxerBox)>
    where
        T: Transport<Output = (PeerId, M)> + Sized + Clone + Send + Sync + 'static,
        T::Dial: Send + 'static,
        T::Listener: Send + 'static,
        T::ListenerUpgrade: Send + 'static,
        T::Error: Send + Sync,
        M: StreamMuxer + Send + Sync + 'static,
        M::Substream: Send + 'static,
        M::OutboundSubstream: Send + 'static
    {
        boxed(self.map(|(i, m), _| (i, StreamMuxerBox::new(m))))
    }

    /// Adds a timeout to the setup and protocol upgrade process for all
    /// inbound and outbound connections established through the transport.
    pub fn timeout(self, timeout: Duration) -> Multiplexed<TransportTimeout<T>> {
        Multiplexed(TransportTimeout::new(self.0, timeout))
    }

    /// Adds a timeout to the setup and protocol upgrade process for all
    /// outbound connections established through the transport.
    pub fn outbound_timeout(self, timeout: Duration) -> Multiplexed<TransportTimeout<T>> {
        Multiplexed(TransportTimeout::with_outgoing_timeout(self.0, timeout))
    }

    /// Adds a timeout to the setup and protocol upgrade process for all
    /// inbound connections established through the transport.
    pub fn inbound_timeout(self, timeout: Duration) -> Multiplexed<TransportTimeout<T>> {
        Multiplexed(TransportTimeout::with_ingoing_timeout(self.0, timeout))
    }
}

impl<T> Transport for Multiplexed<T>
where
    T: Transport,
{
    type Output = T::Output;
    type Error = T::Error;
    type Listener = T::Listener;
    type ListenerUpgrade = T::ListenerUpgrade;
    type Dial = T::Dial;

    fn dial(self, addr: Multiaddr) -> Result<Self::Dial, TransportError<Self::Error>> {
        self.0.dial(addr)
    }

    fn listen_on(self, addr: Multiaddr) -> Result<Self::Listener, TransportError<Self::Error>> {
        self.0.listen_on(addr)
    }

    fn address_translation(&self, server: &Multiaddr, observed: &Multiaddr) -> Option<Multiaddr> {
        self.0.address_translation(server, observed)
    }
}

/// An inbound or outbound upgrade.
type EitherUpgrade<C, U> = future::Either<OutboundUpgradeApply<C, U>, InboundUpgradeApply<C, U>>;
