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

use crate::transport::{Transport, TransportError};
use futures::prelude::*;
use multiaddr::Multiaddr;
use std::{error, fmt, sync::Arc};

/// See the `Transport::boxed` method.
#[inline]
pub fn boxed<T>(transport: T) -> Boxed<T::Output, T::Error>
where
    T: Transport + Clone + Send + Sync + 'static,
    T::Dial: Send + 'static,
    T::Listener: Send + 'static,
    T::ListenerUpgrade: Send + 'static,
{
    Boxed {
        inner: Arc::new(transport) as Arc<_>,
    }
}

pub type Dial<O, E> = Box<Future<Item = O, Error = E> + Send>;
pub type Listener<O, E> = Box<Stream<Item = (ListenerUpgrade<O, E>, Multiaddr), Error = E> + Send>;
pub type ListenerUpgrade<O, E> = Box<Future<Item = O, Error = E> + Send>;

trait Abstract<O, E> {
    fn listen_on(&self, addr: Multiaddr) -> Result<(Listener<O, E>, Multiaddr), TransportError<E>>;
    fn dial(&self, addr: Multiaddr) -> Result<Dial<O, E>, TransportError<E>>;
    fn nat_traversal(&self, server: &Multiaddr, observed: &Multiaddr) -> Option<Multiaddr>;
}

impl<T, O, E> Abstract<O, E> for T
where
    T: Transport<Output = O, Error = E> + Clone + 'static,
    E: error::Error,
    T::Dial: Send + 'static,
    T::Listener: Send + 'static,
    T::ListenerUpgrade: Send + 'static,
{
    fn listen_on(&self, addr: Multiaddr) -> Result<(Listener<O, E>, Multiaddr), TransportError<E>> {
        let (listener, new_addr) = Transport::listen_on(self.clone(), addr)?;
        let fut = listener.map(|(upgrade, addr)| {
            (Box::new(upgrade) as ListenerUpgrade<O, E>, addr)
        });
        Ok((Box::new(fut) as Box<_>, new_addr))
    }

    fn dial(&self, addr: Multiaddr) -> Result<Dial<O, E>, TransportError<E>> {
        let fut = Transport::dial(self.clone(), addr)?;
        Ok(Box::new(fut) as Box<_>)
    }

    #[inline]
    fn nat_traversal(&self, server: &Multiaddr, observed: &Multiaddr) -> Option<Multiaddr> {
        Transport::nat_traversal(self, server, observed)
    }
}

/// See the `Transport::boxed` method.
pub struct Boxed<O, E> {
    inner: Arc<Abstract<O, E> + Send + Sync>,
}

impl<O, E> fmt::Debug for Boxed<O, E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BoxedTransport")
    }
}

impl<O, E> Clone for Boxed<O, E> {
    #[inline]
    fn clone(&self) -> Self {
        Boxed {
            inner: self.inner.clone(),
        }
    }
}

impl<O, E> Transport for Boxed<O, E>
where E: error::Error,
{
    type Output = O;
    type Error = E;
    type Listener = Listener<O, E>;
    type ListenerUpgrade = ListenerUpgrade<O, E>;
    type Dial = Dial<O, E>;

    #[inline]
    fn listen_on(self, addr: Multiaddr) -> Result<(Self::Listener, Multiaddr), TransportError<Self::Error>> {
        self.inner.listen_on(addr)
    }

    #[inline]
    fn dial(self, addr: Multiaddr) -> Result<Self::Dial, TransportError<Self::Error>> {
        self.inner.dial(addr)
    }

    #[inline]
    fn nat_traversal(&self, server: &Multiaddr, observed: &Multiaddr) -> Option<Multiaddr> {
        self.inner.nat_traversal(server, observed)
    }
}
