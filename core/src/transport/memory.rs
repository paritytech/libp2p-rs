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

use crate::{Transport, transport::{TransportError, ListenerEvent}};
use fnv::FnvHashMap;
use futures::{future::{self, Ready}, prelude::*, channel::mpsc, task::Context, task::Poll};
use lazy_static::lazy_static;
use multiaddr::{Protocol, Multiaddr};
use parking_lot::Mutex;
use rw_stream_sink::RwStreamSink;
use std::{collections::hash_map::Entry, error, fmt, io, num::NonZeroU64, pin::Pin};

lazy_static! {
    static ref HUB: Hub = Hub(Mutex::new(FnvHashMap::default()));
}

struct Hub(Mutex<FnvHashMap<NonZeroU64, ChannelSender>>);

/// A [`mpsc::Sender`] enabling a [`DialFuture`] to send a [`Channel`] and the
/// port of the dialer to a [`Listener`].
type ChannelSender = mpsc::Sender<(Channel<Vec<u8>>, NonZeroU64)>;

/// A [`mpsc::Receiver`] enabling a [`Listener`] to receive a [`Channel`] and
/// the port of the dialer from a [`DialFuture`].
type ChannelReceiver = mpsc::Receiver<(Channel<Vec<u8>>, NonZeroU64)>;

impl Hub {
    /// Registers the given port on the hub.
    ///
    /// Randomizes port when given port is `0`. Returns [`None`] when given port
    /// is already occupied.
    fn register_port(&self, port: u64) -> Option<(ChannelReceiver, NonZeroU64)> {
        let mut hub = self.0.lock();

        let port = if let Some(port) = NonZeroU64::new(port) {
            port
        } else {
            loop {
                let port = match NonZeroU64::new(rand::random()) {
                    Some(p) => p,
                    None => continue,
                };
                if !hub.contains_key(&port) {
                    break port;
                }
            }
        };

        let (tx, rx) = mpsc::channel(2);
        match hub.entry(port) {
            Entry::Occupied(_) => return None,
            Entry::Vacant(e) => e.insert(tx)
        };

        Some((rx, port))
    }

    fn unregister_port(&self, port: &NonZeroU64) -> Option<ChannelSender> {
        self.0.lock().remove(port)
    }

    fn get(&self, port: &NonZeroU64) -> Option<ChannelSender> {
        self.0.lock().get(port).cloned()
    }
}

/// Transport that supports `/memory/N` multiaddresses.
#[derive(Debug, Copy, Clone, Default)]
pub struct MemoryTransport;

/// Connection to a `MemoryTransport` currently being opened.
pub struct DialFuture {
    /// Ephemeral source port.
    ///
    /// These ports mimic TCP ephemeral source ports but are not actually used
    /// by the memory transport due to the direct use of channels. They merely
    /// ensure that every connection has a unique address for each dialer, which
    /// is not at the same time a listen address (analogous to TCP).
    dial_port: NonZeroU64,
    sender: ChannelSender,
    channel_to_send: Option<Channel<Vec<u8>>>,
    channel_to_return: Option<Channel<Vec<u8>>>,
}

impl DialFuture {
    fn new(port: NonZeroU64) -> Option<Self> {
        let sender = HUB.get(&port)?;

        let (_dial_port_channel, dial_port) = HUB.register_port(0)
            .expect("there to be some random unoccupied port.");

        let (a_tx, a_rx) = mpsc::channel(4096);
        let (b_tx, b_rx) = mpsc::channel(4096);
        Some(DialFuture {
            dial_port,
            sender,
            channel_to_send: Some(RwStreamSink::new(Chan {
                incoming: a_rx,
                outgoing: b_tx,
                dial_port: None,
            })),
            channel_to_return: Some(RwStreamSink::new(Chan {
                incoming: b_rx,
                outgoing: a_tx,
                dial_port: Some(dial_port),
            })),
        })
    }
}

impl Future for DialFuture {
    type Output = Result<Channel<Vec<u8>>, MemoryTransportError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {

        match self.sender.poll_ready(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Ok(())) => {},
            Poll::Ready(Err(_)) => return Poll::Ready(Err(MemoryTransportError::Unreachable)),
        }

        let channel_to_send = self.channel_to_send.take()
            .expect("Future should not be polled again once complete");
        let dial_port = self.dial_port;
        match self.sender.start_send((channel_to_send, dial_port)) {
            Err(_) => return Poll::Ready(Err(MemoryTransportError::Unreachable)),
            Ok(()) => {}
        }

        Poll::Ready(Ok(self.channel_to_return.take()
                .expect("Future should not be polled again once complete")))
    }
}

impl Transport for MemoryTransport {
    type Output = Channel<Vec<u8>>;
    type Error = MemoryTransportError;
    type Listener = Listener;
    type ListenerUpgrade = Ready<Result<Self::Output, Self::Error>>;
    type Dial = DialFuture;

    fn listen_on(self, addr: Multiaddr) -> Result<Self::Listener, TransportError<Self::Error>> {
        let port = if let Ok(port) = parse_memory_addr(&addr) {
            port
        } else {
            return Err(TransportError::MultiaddrNotSupported(addr));
        };

        let (rx, port) = match HUB.register_port(port) {
            Some((rx, port)) => (rx, port),
            None => return Err(TransportError::Other(MemoryTransportError::Unreachable)),
        };

        let listener = Listener {
            port,
            addr: Protocol::Memory(port.get()).into(),
            receiver: rx,
            tell_listen_addr: true
        };

        Ok(listener)
    }

    fn dial(self, addr: Multiaddr) -> Result<DialFuture, TransportError<Self::Error>> {
        let port = if let Ok(port) = parse_memory_addr(&addr) {
            if let Some(port) = NonZeroU64::new(port) {
                port
            } else {
                return Err(TransportError::Other(MemoryTransportError::Unreachable));
            }
        } else {
            return Err(TransportError::MultiaddrNotSupported(addr));
        };

        DialFuture::new(port).ok_or(TransportError::Other(MemoryTransportError::Unreachable))
    }

    fn address_translation(&self, _server: &Multiaddr, _observed: &Multiaddr) -> Option<Multiaddr> {
        None
    }
}

/// Error that can be produced from the `MemoryTransport`.
#[derive(Debug, Copy, Clone)]
pub enum MemoryTransportError {
    /// There's no listener on the given port.
    Unreachable,
    /// Tries to listen on a port that is already in use.
    AlreadyInUse,
}

impl fmt::Display for MemoryTransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            MemoryTransportError::Unreachable => write!(f, "No listener on the given port."),
            MemoryTransportError::AlreadyInUse => write!(f, "Port already occupied."),
        }
    }
}

impl error::Error for MemoryTransportError {}

/// Listener for memory connections.
pub struct Listener {
    /// Port we're listening on.
    port: NonZeroU64,
    /// The address we are listening on.
    addr: Multiaddr,
    /// Receives incoming connections.
    receiver: ChannelReceiver,
    /// Generate `ListenerEvent::NewAddress` to inform about our listen address.
    tell_listen_addr: bool
}

impl Stream for Listener {
    type Item = Result<ListenerEvent<Ready<Result<Channel<Vec<u8>>, MemoryTransportError>>, MemoryTransportError>, MemoryTransportError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.tell_listen_addr {
            self.tell_listen_addr = false;
            return Poll::Ready(Some(Ok(ListenerEvent::NewAddress(self.addr.clone()))))
        }

        let (channel, dial_port) = match Stream::poll_next(Pin::new(&mut self.receiver), cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => panic!("Alive listeners always have a sender."),
            Poll::Ready(Some(v)) => v,
        };

        let event = ListenerEvent::Upgrade {
            upgrade: future::ready(Ok(channel)),
            local_addr: self.addr.clone(),
            remote_addr: Protocol::Memory(dial_port.get()).into()
        };

        Poll::Ready(Some(Ok(event)))
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        let val_in = HUB.unregister_port(&self.port);
        debug_assert!(val_in.is_some());
    }
}

/// If the address is `/memory/n`, returns the value of `n`.
fn parse_memory_addr(a: &Multiaddr) -> Result<u64, ()> {
    let mut protocols = a.iter();
    match protocols.next() {
        Some(Protocol::Memory(port)) => match protocols.next() {
            None | Some(Protocol::P2p(_)) => Ok(port),
            _ => Err(())
        }
        _ => Err(())
    }
}

/// A channel represents an established, in-memory, logical connection between two endpoints.
///
/// Implements `AsyncRead` and `AsyncWrite`.
pub type Channel<T> = RwStreamSink<Chan<T>>;

/// A channel represents an established, in-memory, logical connection between two endpoints.
///
/// Implements `Sink` and `Stream`.
pub struct Chan<T = Vec<u8>> {
    incoming: mpsc::Receiver<T>,
    outgoing: mpsc::Sender<T>,

    // Needed in [`Drop`] implementation of [`Chan`] to unregister the dialing
    // port with the global [`HUB`]. Is [`Some`] when [`Chan`] of dialer and
    // [`None`] when [`Chan`] of listener.
    //
    // Note: Listening port is unregistered in [`Drop`] implementation of
    // [`Listener`].
    dial_port: Option<NonZeroU64>,
}

impl<T> Unpin for Chan<T> {
}

impl<T> Stream for Chan<T> {
    type Item = Result<T, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Stream::poll_next(Pin::new(&mut self.incoming), cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(v)) => Poll::Ready(Some(Ok(v))),
        }
    }
}

impl<T> Sink<T> for Chan<T> {
    type Error = io::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.outgoing.poll_ready(cx)
            .map(|v| v.map_err(|_| io::ErrorKind::BrokenPipe.into()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        self.outgoing.start_send(item).map_err(|_| io::ErrorKind::BrokenPipe.into())
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl<T: AsRef<[u8]>> Into<RwStreamSink<Chan<T>>> for Chan<T> {
    fn into(self) -> RwStreamSink<Chan<T>> {
        RwStreamSink::new(self)
    }
}

impl<T> Drop for Chan<T> {
    fn drop(&mut self) {
        if let Some(port) = self.dial_port {
            let channel_sender = HUB.unregister_port(&port);
            debug_assert!(channel_sender.is_some());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_memory_addr_works() {
        assert_eq!(parse_memory_addr(&"/memory/5".parse().unwrap()), Ok(5));
        assert_eq!(parse_memory_addr(&"/tcp/150".parse().unwrap()), Err(()));
        assert_eq!(parse_memory_addr(&"/memory/0".parse().unwrap()), Ok(0));
        assert_eq!(parse_memory_addr(&"/memory/5/tcp/150".parse().unwrap()), Err(()));
        assert_eq!(parse_memory_addr(&"/tcp/150/memory/5".parse().unwrap()), Err(()));
        assert_eq!(parse_memory_addr(&"/memory/1234567890".parse().unwrap()), Ok(1_234_567_890));
    }

    #[test]
    fn listening_twice() {
        let transport = MemoryTransport::default();
        assert!(transport.listen_on("/memory/1639174018481".parse().unwrap()).is_ok());
        assert!(transport.listen_on("/memory/1639174018481".parse().unwrap()).is_ok());
        let _listener = transport.listen_on("/memory/1639174018481".parse().unwrap()).unwrap();
        assert!(transport.listen_on("/memory/1639174018481".parse().unwrap()).is_err());
        assert!(transport.listen_on("/memory/1639174018481".parse().unwrap()).is_err());
        drop(_listener);
        assert!(transport.listen_on("/memory/1639174018481".parse().unwrap()).is_ok());
        assert!(transport.listen_on("/memory/1639174018481".parse().unwrap()).is_ok());
    }

    #[test]
    fn port_not_in_use() {
        let transport = MemoryTransport::default();
        assert!(transport.dial("/memory/810172461024613".parse().unwrap()).is_err());
        let _listener = transport.listen_on("/memory/810172461024613".parse().unwrap()).unwrap();
        assert!(transport.dial("/memory/810172461024613".parse().unwrap()).is_ok());
    }

    #[test]
    fn communicating_between_dialer_and_listener() {
        let msg = [1, 2, 3];

        // Setup listener.

        let rand_port = rand::random::<u64>().saturating_add(1);
        let t1_addr: Multiaddr = format!("/memory/{}", rand_port).parse().unwrap();
        let cloned_t1_addr = t1_addr.clone();

        let t1 = MemoryTransport::default();

        let listener = async move {
            let listener = t1.listen_on(t1_addr.clone()).unwrap();

            let upgrade = listener.filter_map(|ev| futures::future::ready(
                ListenerEvent::into_upgrade(ev.unwrap())
            )).next().await.unwrap();

            let mut socket = upgrade.0.await.unwrap();

            let mut buf = [0; 3];
            socket.read_exact(&mut buf).await.unwrap();

            assert_eq!(buf, msg);
        };

        // Setup dialer.

        let t2 = MemoryTransport::default();
        let dialer = async move {
            let mut socket = t2.dial(cloned_t1_addr).unwrap().await.unwrap();
            socket.write_all(&msg).await.unwrap();
        };

        // Wait for both to finish.

        futures::executor::block_on(futures::future::join(listener, dialer));
    }

    #[test]
    fn dialer_address_unequal_to_listener_address() {
        let listener_addr: Multiaddr = Protocol::Memory(
            rand::random::<u64>().saturating_add(1),
        ).into();
        let listener_addr_cloned = listener_addr.clone();

        let listener_transport = MemoryTransport::default();

        let listener = async move {
            let mut listener = listener_transport.listen_on(listener_addr.clone())
                .unwrap();
            while let Some(ev) = listener.next().await {
                if let ListenerEvent::Upgrade { remote_addr, .. } = ev.unwrap() {
                    assert!(
                        remote_addr != listener_addr,
                        "Expect dialer address not to equal listener address."
                    );
                    return;
                }
            }
        };

        let dialer = async move {
            MemoryTransport::default().dial(listener_addr_cloned)
                .unwrap()
                .await
                .unwrap();
        };

        futures::executor::block_on(futures::future::join(listener, dialer));
    }

    #[test]
    fn dialer_port_is_deregistered() {
        let (terminate, should_terminate) = futures::channel::oneshot::channel();
        let (terminated, is_terminated) = futures::channel::oneshot::channel();

        let listener_addr: Multiaddr = Protocol::Memory(
            rand::random::<u64>().saturating_add(1),
        ).into();
        let listener_addr_cloned = listener_addr.clone();

        let listener_transport = MemoryTransport::default();

        let listener = async move {
            let mut listener = listener_transport.listen_on(listener_addr.clone())
                .unwrap();
            while let Some(ev) = listener.next().await {
                if let ListenerEvent::Upgrade { remote_addr, .. } = ev.unwrap() {
                    let dialer_port = NonZeroU64::new(
                        parse_memory_addr(&remote_addr).unwrap(),
                    ).unwrap();

                    assert!(
                        HUB.get(&dialer_port).is_some(),
                        "Expect dialer port to stay registered while connection is in use.",
                    );

                    terminate.send(()).unwrap();
                    is_terminated.await.unwrap();

                    assert!(
                        HUB.get(&dialer_port).is_none(),
                        "Expect dialer port to be deregistered once connection is dropped.",
                    );

                    return;
                }
            }
        };

        let dialer = async move {
            let _chan = MemoryTransport::default().dial(listener_addr_cloned)
                .unwrap()
                .await
                .unwrap();

            should_terminate.await.unwrap();
            drop(_chan);
            terminated.send(()).unwrap();
        };

        futures::executor::block_on(futures::future::join(listener, dialer));
    }
}
