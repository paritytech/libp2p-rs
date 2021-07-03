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

use crate::{ConnectedPoint, Negotiated};
use crate::upgrade::{InboundUpgrade, OutboundUpgrade, UpgradeError, ProtocolName};
use futures::{future::{Either, TryFutureExt, MapOk}, prelude::*};
use log::debug;
use multistream_select::{self, DialerSelectFuture, ListenerSelectFuture};
use std::{iter, mem, pin::Pin, task::Context, task::Poll};

pub use multistream_select::{Version, SimOpenRole, NegotiationError};

/// Applies an upgrade to the inbound and outbound direction of a connection or substream.
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


/// Applies an authentication upgrade to the inbound or outbound direction of a connection or substream.
//
// TODO: This is specific to authentication upgrades, given that it can handle simultaneous open.
// Should this be moved to transport.rs?
pub fn apply_authentication<C, U>(conn: C, up: U, cp: ConnectedPoint, v: Version)
    -> AuthenticationUpgradeApply<C, U>
where
    C: AsyncRead + AsyncWrite + Unpin,
    U: InboundUpgrade<Negotiated<C>> + OutboundUpgrade<Negotiated<C>>,
{
    let iter = up.protocol_info().into_iter().map(NameWrap as fn(_) -> NameWrap<_>);

    AuthenticationUpgradeApply {
        inner: AuthenticationUpgradeApplyState::Init{
            future: match cp {
                ConnectedPoint::Dialer { .. } => Either::Left(
                    multistream_select::dialer_select_proto(conn, iter, v),
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

// TODO: This is a hack to get a fn pointer. Can we do better?
fn add_responder<P, C>(input: (P, C)) -> (P, C, SimOpenRole) {
    (input.0, input.1, SimOpenRole::Responder)
}

pub struct AuthenticationUpgradeApply<C, U>
where
    C: AsyncRead + AsyncWrite + Unpin,
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
    C: AsyncRead + AsyncWrite + Unpin,
    U: InboundUpgrade<Negotiated<C>> + OutboundUpgrade<Negotiated<C>>,
{
    Init {
        future: Either<
                multistream_select::DialerSelectFuture<C, NameWrapIter<<U::InfoIter as IntoIterator>::IntoIter>>,
            MapOk<
                    ListenerSelectFuture<C, NameWrap<U::Info>>,
                fn((NameWrap<U::Info>, Negotiated<C>)) -> (NameWrap<U::Info>, Negotiated<C>, SimOpenRole)
                    >,
            >,
        upgrade: U,
    },
    Upgrade {
        role: SimOpenRole,
        future: Either<
                Pin<Box<<U as OutboundUpgrade<Negotiated<C>>>::Future>>,
            Pin<Box<<U as InboundUpgrade<Negotiated<C>>>::Future>>,
            >,
    },
    Undefined
}

impl<C, U> Future for AuthenticationUpgradeApply<C, U>
where
    C: AsyncRead + AsyncWrite + Unpin,
    U: InboundUpgrade<Negotiated<C>> + OutboundUpgrade<Negotiated<C>,
        Output = <U as InboundUpgrade<Negotiated<C>>>::Output,
        Error = <U as InboundUpgrade<Negotiated<C>>>::Error
    >
{
    type Output = Result<
        (<U as InboundUpgrade<Negotiated<C>>>::Output, SimOpenRole),
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
                        SimOpenRole::Initiator => Either::Left(Box::pin(upgrade.upgrade_outbound(io, info.0))),
                        SimOpenRole::Responder => Either::Right(Box::pin(upgrade.upgrade_inbound(io, info.0))),
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
                        Poll::Ready(Ok(x)) => {
                            debug!("Successfully applied negotiated protocol");
                            return Poll::Ready(Ok((x, role)))
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
    let future = multistream_select::dialer_select_proto(conn, iter, v);
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
                    // TODO: Don't ignore the SimOpenRole here. Instead add assert!.
                    let (info, connection, _) = match Future::poll(Pin::new(&mut future), cx)? {
                        Poll::Ready(x) => x,
                        Poll::Pending => {
                            self.inner = OutboundUpgradeApplyState::Init { future, upgrade };
                            return Poll::Pending
                        }
                    };
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

type NameWrapIter<I> = iter::Map<I, fn(<I as Iterator>::Item) -> NameWrap<<I as Iterator>::Item>>;

/// Wrapper type to expose an `AsRef<[u8]>` impl for all types implementing `ProtocolName`.
#[derive(Clone)]
struct NameWrap<N>(N);

impl<N: ProtocolName> AsRef<[u8]> for NameWrap<N> {
    fn as_ref(&self) -> &[u8] {
        self.0.protocol_name()
    }
}
