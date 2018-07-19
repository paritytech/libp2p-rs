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

use bytes::IntoBuf;
use futures::{future::{self, FutureResult}, prelude::*, stream, sync::mpsc};
use multiaddr::{AddrComponent, Multiaddr};
use parking_lot::Mutex;
use rw_stream_sink::RwStreamSink;
use std::{io, sync::Arc};
use Transport;


pub fn connector<T>() -> (Dialer<T>, Listener<T>) {
    let (tx, rx) = mpsc::unbounded();
    (Dialer(tx), Listener(Arc::new(Mutex::new(rx))))
}


pub struct Dialer<T>(mpsc::UnboundedSender<Chan<T>>);

impl<T> Clone for Dialer<T> {
    fn clone(&self) -> Self {
        Dialer(self.0.clone())
    }
}

impl<T: IntoBuf + 'static> Transport for Dialer<T> {
    type Output = Channel<T>;
    type Listener = Box<Stream<Item=Self::ListenerUpgrade, Error=io::Error>>;
    type ListenerUpgrade = FutureResult<(Self::Output, Self::MultiaddrFuture), io::Error>;
    type MultiaddrFuture = FutureResult<Multiaddr, io::Error>;
    type Dial = Box<Future<Item=(Self::Output, Self::MultiaddrFuture), Error=io::Error>>;

    fn listen_on(self, addr: Multiaddr) -> Result<(Self::Listener, Multiaddr), (Self, Multiaddr)> {
        Err((self, addr))
    }

    fn dial(self, addr: Multiaddr) -> Result<Self::Dial, (Self, Multiaddr)> {
        if !is_memory_addr(&addr) {
            return Err((self, addr))
        }
        let (a_tx, a_rx) = mpsc::unbounded();
        let (b_tx, b_rx) = mpsc::unbounded();
        let a = Chan::new(a_rx, b_tx);
        let b = Chan::new(b_rx, a_tx);
        let future = self.0.send(b)
            .map(move |_| (a.into(), future::ok(addr)))
            .map_err(|_| io::ErrorKind::ConnectionAborted.into());
        Ok(Box::new(future))
    }

    fn nat_traversal(&self, server: &Multiaddr, observed: &Multiaddr) -> Option<Multiaddr> {
        if server == observed {
            Some(server.clone())
        } else {
            None
        }
    }
}


pub struct Listener<T>(Arc<Mutex<mpsc::UnboundedReceiver<Chan<T>>>>);

impl<T> Clone for Listener<T> {
    fn clone(&self) -> Self {
        Listener(self.0.clone())
    }
}

impl<T: IntoBuf + 'static> Transport for Listener<T> {
    type Output = Channel<T>;
    type Listener = Box<Stream<Item=Self::ListenerUpgrade, Error=io::Error>>;
    type ListenerUpgrade = FutureResult<(Self::Output, Self::MultiaddrFuture), io::Error>;
    type MultiaddrFuture = FutureResult<Multiaddr, io::Error>;
    type Dial = Box<Future<Item=(Self::Output, Self::MultiaddrFuture), Error=io::Error>>;

    fn listen_on(self, addr: Multiaddr) -> Result<(Self::Listener, Multiaddr), (Self, Multiaddr)> {
        if !is_memory_addr(&addr) {
            return Err((self, addr))
        }
        let addr2 = addr.clone();
        let receiver = self.0.clone();
        let stream = stream::poll_fn(move || receiver.lock().poll())
            .map(move |channel| {
                future::ok((channel.into(), future::ok(addr.clone())))
            })
            .map_err(|()| io::ErrorKind::ConnectionReset.into());
        Ok((Box::new(stream), addr2))
    }

    fn dial(self, addr: Multiaddr) -> Result<Self::Dial, (Self, Multiaddr)> {
        Err((self, addr))
    }

    fn nat_traversal(&self, server: &Multiaddr, observed: &Multiaddr) -> Option<Multiaddr> {
        if server == observed {
            Some(server.clone())
        } else {
            None
        }
    }
}


fn is_memory_addr(a: &Multiaddr) -> bool {
    Some(AddrComponent::Memory) == a.iter().next()
}

pub type Channel<T> = RwStreamSink<Chan<T>>;

/// A channel represents an established, in-memory, logical connection between two endoints.
pub struct Chan<T> {
    incoming: mpsc::UnboundedReceiver<T>,
    outgoing: mpsc::UnboundedSender<T>,
}

impl<T> Chan<T> {
    fn new(inc: mpsc::UnboundedReceiver<T>, out: mpsc::UnboundedSender<T>) -> Self {
        Chan { incoming: inc, outgoing: out }
    }
}

impl<T> Stream for Chan<T> {
    type Item = T;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.incoming.poll().map_err(|()| io::ErrorKind::ConnectionReset.into())
    }
}

impl<T> Sink for Chan<T> {
    type SinkItem = T;
    type SinkError = io::Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        self.outgoing.start_send(item).map_err(|_| io::ErrorKind::ConnectionReset.into())
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.outgoing.poll_complete().map_err(|_| io::ErrorKind::ConnectionReset.into())
    }
}

impl<T: IntoBuf> Into<RwStreamSink<Chan<T>>> for Chan<T> {
    fn into(self) -> RwStreamSink<Chan<T>> {
        RwStreamSink::new(self)
    }
}
