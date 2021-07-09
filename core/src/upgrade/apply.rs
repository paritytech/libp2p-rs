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

use crate::{ConnectedPoint, Negotiated, PeerId};
use crate::upgrade::{InboundUpgrade, OutboundUpgrade, UpgradeError, ProtocolName};
use futures::{future::{Either, TryFutureExt, MapOk}, prelude::*};
use log::debug;
use multistream_select::{self, DialerSelectFuture, ListenerSelectFuture};
use std::{iter, mem, pin::Pin, task::Context, task::Poll};

pub use multistream_select::{Role, NegotiationError};

/// Wrapper around multistream-select `Version`.
///
/// See [`multistream_select::Version`] for details.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Version {
    /// See [`multistream_select::Version::V1`].
    V1,
    /// See [`multistream_select::Version::V1Lazy`].
    V1Lazy,
}

impl From<Version> for multistream_select::Version {
    fn from(v: Version) -> Self {
        match v {
            Version::V1 => multistream_select::Version::V1,
            Version::V1Lazy => multistream_select::Version::V1Lazy,
        }
    }
}

impl Default for Version {
    fn default() -> Self {
        match multistream_select::Version::default() {
            multistream_select::Version::V1 => Version::V1,
            multistream_select::Version::V1Lazy => Version::V1Lazy,
            multistream_select::Version::V1SimultaneousOpen => unreachable!("see `v1_sim_open_is_not_default`"),
        }
    }
}

/// Applies an upgrade to the inbound and outbound direction of a connection or substream.
///
/// Note: Use [`apply_authentication`] when negotiating an authentication protocol on top of a
/// transport allowing simultaneously opened connections.
pub fn apply<C, U>(conn: C, up: U, cp: ConnectedPoint, v: Version)
    -> Either<InboundUpgradeApply<C, U>, OutboundUpgradeApply<C, U>>
where
    C: AsyncRead + AsyncWrite + Unpin,
    U: InboundUpgrade<Negotiated<C>> + OutboundUpgrade<Negotiated<C>>,
{
    if cp.is_listener() {
        Either::Left(apply_inbound(conn, up))
    } else {
        Either::Right(apply_outbound(conn, up, v))
    }
}

/// Tries to perform an upgrade on an inbound connection or substream.
pub fn apply_inbound<C, U>(conn: C, up: U) -> InboundUpgradeApply<C, U>
where
    C: AsyncRead + AsyncWrite + Unpin,
    U: InboundUpgrade<Negotiated<C>>,
{
    let iter = up.protocol_info().into_iter().map(NameWrap as fn(_) -> NameWrap<_>);
    let future = multistream_select::listener_select_proto(conn, iter);
    InboundUpgradeApply {
        inner: InboundUpgradeApplyState::Init { future, upgrade: up }
    }
}

/// Tries to perform an upgrade on an outbound connection or substream.
pub fn apply_outbound<C, U>(conn: C, up: U, v: Version) -> OutboundUpgradeApply<C, U>
where
    C: AsyncRead + AsyncWrite + Unpin,
    U: OutboundUpgrade<Negotiated<C>>
{
    let iter = up.protocol_info().into_iter().map(NameWrap as fn(_) -> NameWrap<_>);
    let future = multistream_select::dialer_select_proto(conn, iter, v.into());
    OutboundUpgradeApply {
        inner: OutboundUpgradeApplyState::Init { future, upgrade: up }
    }
}

/// Future returned by `apply_inbound`. Drives the upgrade process.
pub struct InboundUpgradeApply<C, U>
where
    C: AsyncRead + AsyncWrite + Unpin,
    U: InboundUpgrade<Negotiated<C>>
{
    inner: InboundUpgradeApplyState<C, U>
}

enum InboundUpgradeApplyState<C, U>
where
    C: AsyncRead + AsyncWrite + Unpin,
    U: InboundUpgrade<Negotiated<C>>,
{
    Init {
        future: ListenerSelectFuture<C, NameWrap<U::Info>>,
        upgrade: U,
    },
    Upgrade {
        future: Pin<Box<U::Future>>
    },
    Undefined
}

impl<C, U> Unpin for InboundUpgradeApply<C, U>
where
    C: AsyncRead + AsyncWrite + Unpin,
    U: InboundUpgrade<Negotiated<C>>,
{
}

impl<C, U> Future for InboundUpgradeApply<C, U>
where
    C: AsyncRead + AsyncWrite + Unpin,
    U: InboundUpgrade<Negotiated<C>>,
{
    type Output = Result<U::Output, UpgradeError<U::Error>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match mem::replace(&mut self.inner, InboundUpgradeApplyState::Undefined) {
                InboundUpgradeApplyState::Init { mut future, upgrade } => {
                    let (info, io) = match Future::poll(Pin::new(&mut future), cx)? {
                        Poll::Ready(x) => x,
                        Poll::Pending => {
                            self.inner = InboundUpgradeApplyState::Init { future, upgrade };
                            return Poll::Pending
                        }
                    };
                    self.inner = InboundUpgradeApplyState::Upgrade {
                        future: Box::pin(upgrade.upgrade_inbound(io, info.0))
                    };
                }
                InboundUpgradeApplyState::Upgrade { mut future } => {
                    match Future::poll(Pin::new(&mut future), cx) {
                        Poll::Pending => {
                            self.inner = InboundUpgradeApplyState::Upgrade { future };
                            return Poll::Pending
                        }
                        Poll::Ready(Ok(x)) => {
                            debug!("Successfully applied negotiated protocol");
                            return Poll::Ready(Ok(x))
                        }
                        Poll::Ready(Err(e)) => {
                            debug!("Failed to apply negotiated protocol");
                            return Poll::Ready(Err(UpgradeError::Apply(e)))
                        }
                    }
                }
                InboundUpgradeApplyState::Undefined =>
                    panic!("InboundUpgradeApplyState::poll called after completion")
            }
        }
    }
}

/// Future returned by `apply_outbound`. Drives the upgrade process.
pub struct OutboundUpgradeApply<C, U>
where
    C: AsyncRead + AsyncWrite + Unpin,
    U: OutboundUpgrade<Negotiated<C>>
{
    inner: OutboundUpgradeApplyState<C, U>
}

enum OutboundUpgradeApplyState<C, U>
where
    C: AsyncRead + AsyncWrite + Unpin,
    U: OutboundUpgrade<Negotiated<C>>
{
    Init {
        future: DialerSelectFuture<C, NameWrapIter<<U::InfoIter as IntoIterator>::IntoIter>>,
        upgrade: U
    },
    Upgrade {
        future: Pin<Box<U::Future>>
    },
    Undefined
}

impl<C, U> Unpin for OutboundUpgradeApply<C, U>
where
    C: AsyncRead + AsyncWrite + Unpin,
    U: OutboundUpgrade<Negotiated<C>>,
{
}

impl<C, U> Future for OutboundUpgradeApply<C, U>
where
    C: AsyncRead + AsyncWrite + Unpin,
    U: OutboundUpgrade<Negotiated<C>>,
{
    type Output = Result<U::Output, UpgradeError<U::Error>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match mem::replace(&mut self.inner, OutboundUpgradeApplyState::Undefined) {
                OutboundUpgradeApplyState::Init { mut future, upgrade } => {
                    let (info, connection, role) = match Future::poll(Pin::new(&mut future), cx)? {
                        Poll::Ready(x) => x,
                        Poll::Pending => {
                            self.inner = OutboundUpgradeApplyState::Init { future, upgrade };
                            return Poll::Pending
                        }
                    };
                    assert_eq!(
                        role, Role::Initiator,
                        "Expect negotiation not using `Version::V1SimultaneousOpen` to either return \
                         as `Initiator` or fail.",
                    );
                    self.inner = OutboundUpgradeApplyState::Upgrade {
                        future: Box::pin(upgrade.upgrade_outbound(connection, info.0))
                    };
                }
                OutboundUpgradeApplyState::Upgrade { mut future } => {
                    match Future::poll(Pin::new(&mut future), cx) {
                        Poll::Pending => {
                            self.inner = OutboundUpgradeApplyState::Upgrade { future };
                            return Poll::Pending
                        }
                        Poll::Ready(Ok(x)) => {
                            debug!("Successfully applied negotiated protocol");
                            return Poll::Ready(Ok(x))
                        }
                        Poll::Ready(Err(e)) => {
                            debug!("Failed to apply negotiated protocol");
                            return Poll::Ready(Err(UpgradeError::Apply(e)));
                        }
                    }
                }
                OutboundUpgradeApplyState::Undefined =>
                    panic!("OutboundUpgradeApplyState::poll called after completion")
            }
        }
    }
}

/// Wrapper around multistream-select `Version`.
///
/// See [`multistream_select::Version`] for details.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthenticationVersion {
    /// See [`multistream_select::Version::V1`].
    V1,
    /// See [`multistream_select::Version::V1Lazy`].
    V1Lazy,
    /// See [`multistream_select::Version::V1SimultaneousOpen`].
    V1SimultaneousOpen
}

impl Default for AuthenticationVersion {
    fn default() -> Self {
        match multistream_select::Version::default() {
            multistream_select::Version::V1 => AuthenticationVersion::V1,
            multistream_select::Version::V1Lazy => AuthenticationVersion::V1Lazy,
            multistream_select::Version::V1SimultaneousOpen => AuthenticationVersion::V1SimultaneousOpen,
        }
    }
}

impl From<AuthenticationVersion> for multistream_select::Version {
    fn from(v: AuthenticationVersion) -> Self {
        match v {
            AuthenticationVersion::V1 => multistream_select::Version::V1,
            AuthenticationVersion::V1Lazy => multistream_select::Version::V1Lazy,
            AuthenticationVersion::V1SimultaneousOpen => multistream_select::Version::V1SimultaneousOpen,
        }
    }
}

/// Applies an authentication upgrade to the inbound or outbound direction of a connection.
///
/// Note: This is like [`apply`] with additional support for transports allowing simultaneously
/// opened connections. Unless run on such transport and used to negotiate the authentication
/// protocol you likely want to use [`apply`] instead of [`apply_authentication`].
pub fn apply_authentication<C, D, U>(conn: C, up: U, cp: ConnectedPoint, v: AuthenticationVersion)
    -> AuthenticationUpgradeApply<C, U>
where
    C: AsyncRead + AsyncWrite + Unpin,
    D: AsyncRead + AsyncWrite + Unpin,
    U: InboundUpgrade<Negotiated<C>, Output = (PeerId, D)>,
    U: OutboundUpgrade<Negotiated<C>, Output = (PeerId, D), Error = <U as InboundUpgrade<Negotiated<C>>>::Error> + Clone,

{
    fn add_responder<P, C>(input: (P, C)) -> (P, C, Role) {
        (input.0, input.1, Role::Responder)
    }

    let iter = up.protocol_info().into_iter().map(NameWrap as fn(_) -> NameWrap<_>);

    AuthenticationUpgradeApply {
        inner: AuthenticationUpgradeApplyState::Init{
            future: match cp {
                ConnectedPoint::Dialer { .. } => Either::Left(
                    multistream_select::dialer_select_proto(conn, iter, v.into()),
                ),
                ConnectedPoint::Listener { .. } => Either::Right(
                    multistream_select::listener_select_proto(conn, iter)
                        .map_ok(add_responder as fn (_) -> _),
                ),
            },
            upgrade: up,
        },
    }
}

pub struct AuthenticationUpgradeApply<C, U>
where
    U: InboundUpgrade<Negotiated<C>> + OutboundUpgrade<Negotiated<C>>,
{
    inner: AuthenticationUpgradeApplyState<C, U>
}

impl<C, U> Unpin for AuthenticationUpgradeApply<C, U>
where
    C: AsyncRead + AsyncWrite + Unpin,
    U: InboundUpgrade<Negotiated<C>> + OutboundUpgrade<Negotiated<C>>,
{
}

enum AuthenticationUpgradeApplyState<C, U>
where
    U: InboundUpgrade<Negotiated<C>> + OutboundUpgrade<Negotiated<C>>,
{
    Init {
        future: Either<
            multistream_select::DialerSelectFuture<C, NameWrapIter<<U::InfoIter as IntoIterator>::IntoIter>>,
            MapOk<
                ListenerSelectFuture<C, NameWrap<U::Info>>,
                fn((NameWrap<U::Info>, Negotiated<C>)) -> (NameWrap<U::Info>, Negotiated<C>, Role)
            >,
        >,
        upgrade: U,
    },
    Upgrade {
        role: Role,
        future: Either<
            Pin<Box<<U as OutboundUpgrade<Negotiated<C>>>::Future>>,
            Pin<Box<<U as InboundUpgrade<Negotiated<C>>>::Future>>,
        >,
    },
    Undefined
}

impl<C, D, U> Future for AuthenticationUpgradeApply<C, U>
where
    C: AsyncRead + AsyncWrite + Unpin,
    D: AsyncRead + AsyncWrite + Unpin,
    U: InboundUpgrade<Negotiated<C>, Output = (PeerId, D)>,
    U: OutboundUpgrade<Negotiated<C>, Output = (PeerId, D), Error = <U as InboundUpgrade<Negotiated<C>>>::Error> + Clone,
{
    type Output = Result<
        ((PeerId, Role), D),
        UpgradeError<<U as InboundUpgrade<Negotiated<C>>>::Error>,
    >;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match mem::replace(&mut self.inner, AuthenticationUpgradeApplyState::Undefined) {
                AuthenticationUpgradeApplyState::Init { mut future, upgrade } => {
                    let (info, io, role) = match Future::poll(Pin::new(&mut future), cx)? {
                        Poll::Ready(x) => x,
                        Poll::Pending => {
                            self.inner = AuthenticationUpgradeApplyState::Init { future, upgrade };
                            return Poll::Pending
                        }
                    };
                    let fut = match role {
                        Role::Initiator => Either::Left(Box::pin(upgrade.upgrade_outbound(io, info.0))),
                        Role::Responder => Either::Right(Box::pin(upgrade.upgrade_inbound(io, info.0))),
                    };
                    self.inner = AuthenticationUpgradeApplyState::Upgrade {
                        future: fut,
                        role,
                    };
                }
                AuthenticationUpgradeApplyState::Upgrade { mut future, role } => {
                    match Future::poll(Pin::new(&mut future), cx) {
                        Poll::Pending => {
                            self.inner = AuthenticationUpgradeApplyState::Upgrade { future, role };
                            return Poll::Pending
                        }
                        Poll::Ready(Ok((peer_id, d))) => {
                            debug!("Successfully applied negotiated protocol");
                            return Poll::Ready(Ok(((peer_id, role), d)))
                        }
                        Poll::Ready(Err(e)) => {
                            debug!("Failed to apply negotiated protocol");
                            return Poll::Ready(Err(UpgradeError::Apply(e)))
                        }
                    }
                }
                AuthenticationUpgradeApplyState::Undefined =>
                    panic!("AuthenticationUpgradeApplyState::poll called after completion")
            }
        }
    }
}

pub type NameWrapIter<I> = iter::Map<I, fn(<I as Iterator>::Item) -> NameWrap<<I as Iterator>::Item>>;

/// Wrapper type to expose an `AsRef<[u8]>` impl for all types implementing `ProtocolName`.
#[derive(Clone)]
pub struct NameWrap<N>(N);

impl<N: ProtocolName> AsRef<[u8]> for NameWrap<N> {
    fn as_ref(&self) -> &[u8] {
        self.0.protocol_name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v1_sim_open_is_not_default() {
        assert_ne!(
            multistream_select::Version::default(),
            multistream_select::Version::V1SimultaneousOpen,
        );
    }
}
