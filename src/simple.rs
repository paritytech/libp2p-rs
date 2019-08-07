// Copyright 2017 Parity Technologies (UK) Ltd.
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

use crate::protocols::upgrade::{InboundUpgrade, OutboundUpgrade, UpgradeInfo, Negotiated};
use bytes::Bytes;
use futures::{future::FromErr, prelude::*};
use std::{iter, io::Error as IoError, sync::Arc};
use tokio_io::{AsyncRead, AsyncWrite};

/// Implementation of `ConnectionUpgrade`. Convenient to use with small protocols.
#[derive(Debug)]
pub struct SimpleProtocol<F> {
    info: Bytes,
    // Note: we put the closure `F` in an `Arc` because Rust closures aren't automatically clonable
    // yet.
    upgrade: Arc<F>,
}

impl<F> SimpleProtocol<F> {
    /// Builds a `SimpleProtocol`.
    #[inline]
    pub fn new<N>(info: N, upgrade: F) -> SimpleProtocol<F>
    where
        N: Into<Bytes>,
    {
        SimpleProtocol {
            info: info.into(),
            upgrade: Arc::new(upgrade),
        }
    }
}

impl<F> Clone for SimpleProtocol<F> {
    #[inline]
    fn clone(&self) -> Self {
        SimpleProtocol {
            info: self.info.clone(),
            upgrade: self.upgrade.clone(),
        }
    }
}

impl<F> UpgradeInfo for SimpleProtocol<F> {
    type Info = Bytes;
    type InfoIter = iter::Once<Self::Info>;

    #[inline]
    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(self.info.clone())
    }
}

impl<C, F, O> InboundUpgrade<C> for SimpleProtocol<F>
where
    C: AsyncRead + AsyncWrite,
    F: Fn(Negotiated<C>) -> O,
    O: IntoFuture<Error = IoError>
{
    type Output = O::Item;
    type Error = IoError;
    type Future = FromErr<O::Future, IoError>;

    #[inline]
    fn upgrade_inbound(self, socket: Negotiated<C>, _: Self::Info) -> Self::Future {
        let upgrade = &self.upgrade;
        upgrade(socket).into_future().from_err()
    }
}

impl<C, F, O> OutboundUpgrade<C> for SimpleProtocol<F>
where
    C: AsyncRead + AsyncWrite,
    F: Fn(Negotiated<C>) -> O,
    O: IntoFuture<Error = IoError>
{
    type Output = O::Item;
    type Error = IoError;
    type Future = FromErr<O::Future, IoError>;

    #[inline]
    fn upgrade_outbound(self, socket: Negotiated<C>, _: Self::Info) -> Self::Future {
        let upgrade = &self.upgrade;
        upgrade(socket).into_future().from_err()
    }
}
