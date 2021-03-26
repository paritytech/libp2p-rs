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

//! Implementation of the libp2p `Transport` trait for TCP/IP.
//!
//! # Usage
//!
//! This crate provides a `TcpConfig` and `TokioTcpConfig`, depending on
//! the enabled features, which implement the `Transport` trait for use as a
//! transport with `libp2p-core` or `libp2p-swarm`.

mod provider;

#[cfg(feature = "async-io")]
pub use provider::async_io;

/// The type of a [`GenTcpConfig`] using the `async-io` implementation.
#[cfg(feature = "async-io")]
pub type TcpConfig = GenTcpConfig<async_io::Tcp>;

#[cfg(feature = "tokio")]
pub use provider::tokio;

/// The type of a [`GenTcpConfig`] using the `tokio` implementation.
#[cfg(feature = "tokio")]
pub type TokioTcpConfig = GenTcpConfig<tokio::Tcp>;

use futures::{
    future::{self, BoxFuture, Ready},
    prelude::*,
    ready,
};
use futures_timer::Delay;
use libp2p_core::{
    address_translation,
    multiaddr::{Multiaddr, Protocol},
    transport::{ListenerEvent, Transport, TransportError},
};
use socket2::{Domain, Socket, Type};
use std::{
    collections::HashSet,
    io,
    net::{SocketAddr, IpAddr, TcpListener},
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Context, Poll},
    time::Duration,
};

use provider::{Provider, IfEvent};

/// The configuration for a TCP/IP transport capability for libp2p.
///
/// A [`GenTcpConfig`] implements the [`Transport`] interface and thus
/// is consumed on [`Transport::listen_on`] and [`Transport::dial`].
/// However, the config can be cheaply cloned to perform multiple such
/// operations with the same config.
#[derive(Clone, Debug)]
pub struct GenTcpConfig<T> {
    /// The type of the I/O provider.
    _impl: std::marker::PhantomData<T>,
    /// TTL to set for opened sockets, or `None` to keep default.
    ttl: Option<u32>,
    /// `TCP_NODELAY` to set for opened sockets, or `None` to keep default.
    nodelay: Option<bool>,
    /// Size of the listen backlog for listen sockets.
    backlog: u32,
    /// The configuration of port reuse when dialing.
    port_reuse: PortReuse,
}

type Port = u16;

/// The configuration for port reuse of listening sockets.
#[derive(Debug, Clone)]
enum PortReuse {
    /// Port reuse is disabled, i.e. ephemeral local ports are
    /// used for outgoing TCP connections.
    Disabled,
    /// Port reuse when dialing is enabled, i.e. the local
    /// address and port that a new socket for an outgoing
    /// connection is bound to are chosen from an existing
    /// listening socket, if available.
    Enabled {
        /// The addresses and ports of the listening sockets
        /// registered as eligible for port reuse when dialing.
        listen_addrs: Arc<RwLock<HashSet<(IpAddr, Port)>>>
    },
}

impl PortReuse {
    /// Registers a socket address for port reuse.
    ///
    /// Has no effect if port reuse is disabled.
    fn register(&mut self, ip: IpAddr, port: Port) {
        if let PortReuse::Enabled { listen_addrs } = self {
            log::trace!("Registering for port reuse: {}:{}", ip, port);
            listen_addrs
                .write()
                .expect("`register()` and `unregister()` never panic while holding the lock")
                .insert((ip, port));
        }
    }

    /// Unregisters a socket address for port reuse.
    ///
    /// Has no effect if port reuse is disabled.
    fn unregister(&mut self, ip: IpAddr, port: Port) {
        if let PortReuse::Enabled { listen_addrs } = self {
            log::trace!("Unregistering for port reuse: {}:{}", ip, port);
            listen_addrs
                .write()
                .expect("`register()` and `unregister()` never panic while holding the lock")
                .remove(&(ip, port));
        }
    }

    /// Selects a listening socket address suitable for use
    /// as the local socket address when dialing.
    ///
    /// If multiple listening sockets are registered for port
    /// reuse, one is chosen whose IP protocol version and
    /// loopback status is the same as that of `remote_ip`.
    ///
    /// Returns `None` if port reuse is disabled or no suitable
    /// listening socket address is found.
    fn local_dial_addr(&self, remote_ip: &IpAddr) -> Option<SocketAddr> {
        if let PortReuse::Enabled { listen_addrs } = self {
            for (ip, port) in listen_addrs
                .read()
                .expect("`register()` and `unregister()` never panic while holding the lock")
                .iter()
            {
                if ip.is_ipv4() == remote_ip.is_ipv4()
                    && ip.is_loopback() == remote_ip.is_loopback()
                {
                     return Some(SocketAddr::new(*ip, *port))
                }
            }
        }

        None
    }
}

impl<T> GenTcpConfig<T>
where
    T: Provider + Send,
{
    /// Creates a new configuration for a TCP/IP transport:
    ///
    ///   * Nagle's algorithm, i.e. `TCP_NODELAY`, is _enabled_.
    ///     See [`GenTcpConfig::nodelay`].
    ///   * Reuse of listening ports is _disabled_.
    ///     See [`GenTcpConfig::port_reuse`].
    ///   * No custom `IP_TTL` is set. The default of the OS TCP stack applies.
    ///     See [`GenTcpConfig::ttl`].
    ///   * The size of the listen backlog for new listening sockets is `1024`.
    ///     See [`GenTcpConfig::listen_backlog`].
    pub fn new() -> Self {
        Self {
            ttl: None,
            nodelay: None,
            backlog: 1024,
            port_reuse: PortReuse::Disabled,
            _impl: std::marker::PhantomData,
        }
    }

    /// Configures the `IP_TTL` option for new sockets.
    pub fn ttl(mut self, value: u32) -> Self {
        self.ttl = Some(value);
        self
    }

    /// Configures the `TCP_NODELAY` option for new sockets.
    pub fn nodelay(mut self, value: bool) -> Self {
        self.nodelay = Some(value);
        self
    }

    /// Configures the listen backlog for new listen sockets.
    pub fn listen_backlog(mut self, backlog: u32) -> Self {
        self.backlog = backlog;
        self
    }

    /// Configures port reuse for local sockets, which implies
    /// reuse of listening ports for outgoing connections to
    /// enhance NAT traversal capabilities.
    ///
    /// Please refer to e.g. [RFC 4787](https://tools.ietf.org/html/rfc4787)
    /// section 4 and 5 for some of the NAT terminology used here.
    ///
    /// There are two main use-cases for port reuse among local
    /// sockets:
    ///
    ///   1. Creating multiple listening sockets for the same address
    ///      and port to allow accepting connections on multiple threads
    ///      without having to synchronise access to a single listen socket.
    ///
    ///   2. Creating outgoing connections whose local socket is bound to
    ///      the same address and port as a listening socket. In the rare
    ///      case of simple NATs with both endpoint-independent mapping and
    ///      endpoint-independent filtering, this can on its own already
    ///      permit NAT traversal by other nodes sharing the observed
    ///      external address of the local node. For the common case of
    ///      NATs with address-dependent or address and port-dependent
    ///      filtering, port reuse for outgoing connections can facilitate
    ///      further TCP hole punching techniques for NATs that perform
    ///      endpoint-independent mapping. Port reuse cannot facilitate
    ///      NAT traversal in the presence of "symmetric" NATs that employ
    ///      both address/port-dependent mapping and filtering, unless
    ///      there is some means of port prediction.
    ///
    /// Both use-cases are enabled when port reuse is enabled, with port reuse
    /// for outgoing connections (`2.` above) always being implied.
    ///
    /// > **Note**: Due to the identification of a TCP socket by a 4-tuple
    /// > of source IP address, source port, destination IP address and
    /// > destination port, with port reuse enabled there can be only
    /// > a single outgoing connection to a particular address and port
    /// > of a peer per local listening socket address.
    ///
    /// If enabled, the returned `GenTcpConfig` and all of its `Clone`s
    /// keep track of the listen socket addresses as they are reported
    /// by polling [`TcpListenStream`]s obtained from [`GenTcpConfig::listen_on()`].
    ///
    /// In contrast, two `GenTcpConfig`s constructed separately via [`GenTcpConfig::new()`]
    /// maintain these addresses independently. It is thus possible to listen on
    /// multiple addresses, enabling port reuse for each, knowing exactly which
    /// listen address is reused when dialing with a specific `GenTcpConfig`, as in
    /// the following example:
    ///
    /// ```no_run
    /// # use libp2p_core::transport::ListenerEvent;
    /// # use libp2p_core::{Multiaddr, Transport};
    /// # use futures::stream::StreamExt;
    /// #[cfg(feature = "async-io")]
    /// #[async_std::main]
    /// async fn main() -> std::io::Result<()> {
    /// use libp2p_tcp::TcpConfig;
    ///
    /// let listen_addr1: Multiaddr = "/ip4/127.0.0.1/tcp/9001".parse().unwrap();
    /// let listen_addr2: Multiaddr = "/ip4/127.0.0.1/tcp/9002".parse().unwrap();
    ///
    /// let tcp1 = TcpConfig::new().port_reuse(true);
    /// let mut listener1 = tcp1.clone().listen_on(listen_addr1.clone()).expect("listener");
    /// match listener1.next().await.expect("event")? {
    ///     ListenerEvent::NewAddress(listen_addr) => {
    ///         println!("Listening on {:?}", listen_addr);
    ///         let mut stream = tcp1.dial(listen_addr2.clone()).unwrap().await?;
    ///         // `stream` has `listen_addr1` as its local socket address.
    ///     }
    ///     _ => {}
    /// }
    ///
    /// let tcp2 = TcpConfig::new().port_reuse(true);
    /// let mut listener2 = tcp2.clone().listen_on(listen_addr2).expect("listener");
    /// match listener2.next().await.expect("event")? {
    ///     ListenerEvent::NewAddress(listen_addr) => {
    ///         println!("Listening on {:?}", listen_addr);
    ///         let mut socket = tcp2.dial(listen_addr1).unwrap().await?;
    ///         // `stream` has `listen_addr2` as its local socket address.
    ///     }
    ///     _ => {}
    /// }
    /// Ok(())
    /// }
    /// ```
    ///
    /// If a single `GenTcpConfig` is used and cloned for the creation of multiple
    /// listening sockets or a wildcard listen socket address is used to listen
    /// on any interface, there can be multiple such addresses registered for
    /// port reuse. In this case, one is chosen whose IP protocol version and
    /// loopback status is the same as that of the remote address. Consequently, for
    /// maximum control of the local listening addresses and ports that are used
    /// for outgoing connections, a new `GenTcpConfig` should be created for each
    /// listening socket, avoiding the use of wildcard addresses which bind a
    /// socket to all network interfaces.
    ///
    /// When this option is enabled on a unix system, the socket
    /// option `SO_REUSEPORT` is set, if available, to permit
    /// reuse of listening ports for multiple sockets.
    pub fn port_reuse(mut self, port_reuse: bool) -> Self {
        self.port_reuse = if port_reuse {
            PortReuse::Enabled {
                listen_addrs: Arc::new(RwLock::new(HashSet::new()))
            }
        } else {
            PortReuse::Disabled
        };

        self
    }

    fn create_socket(&self, socket_addr: &SocketAddr) -> io::Result<Socket> {
        let domain = if socket_addr.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        };
        let socket = Socket::new(domain, Type::STREAM, Some(socket2::Protocol::TCP))?;
        if socket_addr.is_ipv6() {
            socket.set_only_v6(true)?;
        }
        if let Some(ttl) = self.ttl {
            socket.set_ttl(ttl)?;
        }
        if let Some(nodelay) = self.nodelay {
            socket.set_nodelay(nodelay)?;
        }
        socket.set_reuse_address(true)?;
        #[cfg(unix)]
        if let PortReuse::Enabled { .. } = &self.port_reuse {
            socket.set_reuse_port(true)?;
        }
        Ok(socket)
    }

    fn do_listen(self, socket_addr: SocketAddr) -> io::Result<TcpListenStream<T>> {
        let socket = self.create_socket(&socket_addr)?;
        socket.bind(&socket_addr.into())?;
        socket.listen(self.backlog as _)?;
        socket.set_nonblocking(true)?;
        TcpListenStream::<T>::new(socket.into(), self.port_reuse)
    }

    async fn do_dial(self, socket_addr: SocketAddr) -> Result<T::Stream, io::Error> {
        let socket = self.create_socket(&socket_addr)?;

        if let Some(addr) = self.port_reuse.local_dial_addr(&socket_addr.ip()) {
            log::trace!("Binding dial socket to listen socket {}", addr);
            socket.bind(&addr.into())?;
        }

        socket.set_nonblocking(true)?;

        match socket.connect(&socket_addr.into()) {
            Ok(()) => {}
            Err(err) if err.raw_os_error() == Some(libc::EINPROGRESS) => {}
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
            Err(err) => return Err(err),
        };

        let stream = T::new_stream(socket.into()).await?;
        Ok(stream)
    }
}

impl<T> Transport for GenTcpConfig<T>
where
    T: Provider + Send + 'static,
    T::Listener: Unpin,
    T::IfWatcher: Unpin,
    T::Stream: Unpin,
{
    type Output = T::Stream;
    type Error = io::Error;
    type Dial = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send>>;
    type Listener = TcpListenStream<T>;
    type ListenerUpgrade = Ready<Result<Self::Output, Self::Error>>;

    fn listen_on(self, addr: Multiaddr) -> Result<Self::Listener, TransportError<Self::Error>> {
        let socket_addr = if let Ok(sa) = multiaddr_to_socketaddr(addr.clone()) {
            sa
        } else {
            return Err(TransportError::MultiaddrNotSupported(addr));
        };
        log::debug!("listening on {}", socket_addr);
        self.do_listen(socket_addr)
            .map_err(TransportError::Other)
    }

    fn dial(self, addr: Multiaddr) -> Result<Self::Dial, TransportError<Self::Error>> {
        let socket_addr = if let Ok(socket_addr) = multiaddr_to_socketaddr(addr.clone()) {
            if socket_addr.port() == 0 || socket_addr.ip().is_unspecified() {
                return Err(TransportError::MultiaddrNotSupported(addr));
            }
            socket_addr
        } else {
            return Err(TransportError::MultiaddrNotSupported(addr));
        };
        log::debug!("dialing {}", socket_addr);
        Ok(Box::pin(self.do_dial(socket_addr)))
    }

    /// When port reuse is disabled and hence ephemeral local ports are
    /// used for outgoing connections, the returned address is the
    /// `observed` address with the port replaced by the port of the
    /// `listen` address.
    ///
    /// If port reuse is enabled, `Some(observed)` is returned, as there
    /// is a chance that the `observed` address _and_ port are reachable
    /// for other peers if there is a NAT in the way that does endpoint-
    /// independent filtering. Furthermore, even if that is not the case
    /// and TCP hole punching techniques must be used for NAT traversal,
    /// the `observed` address is still the one that a remote should connect
    /// to for the purpose of the hole punching procedure, as it represents
    /// the mapped IP and port of the NAT device in front of the local
    /// node.
    ///
    /// `None` is returned if one of the given addresses is not a TCP/IP
    /// address.
    fn address_translation(&self, listen: &Multiaddr, observed: &Multiaddr) -> Option<Multiaddr> {
        match &self.port_reuse {
            PortReuse::Disabled => address_translation(listen, observed),
            PortReuse::Enabled { .. } => Some(observed.clone()),
        }
    }
}

type TcpListenerEvent<S> = ListenerEvent<Ready<Result<S, io::Error>>, io::Error>;

enum IfWatch<TIfWatcher> {
    Pending(BoxFuture<'static, io::Result<TIfWatcher>>),
    Ready(TIfWatcher),
}

/// The listening addresses of a [`TcpListenStream`].
enum InAddr<TIfWatcher> {
    /// The stream accepts connections on a single interface.
    One {
        addr: IpAddr,
        out: Option<Multiaddr>
    },
    /// The stream accepts connections on all interfaces.
    Any {
        addrs: HashSet<IpAddr>,
        if_watch: IfWatch<TIfWatcher>,
    }
}

/// A stream of incoming connections on one or more interfaces.
pub struct TcpListenStream<T>
where
    T: Provider
{
    /// The socket address that the listening socket is bound to,
    /// which may be a "wildcard address" like `INADDR_ANY` or `IN6ADDR_ANY`
    /// when listening on all interfaces for IPv4 respectively IPv6 connections.
    listen_addr: SocketAddr,
    /// The async listening socket for incoming connections.
    listener: T::Listener,
    /// The IP addresses of network interfaces on which the listening socket
    /// is accepting connections.
    ///
    /// If the listen socket listens on all interfaces, these may change over
    /// time as interfaces become available or unavailable.
    in_addr: InAddr<T::IfWatcher>,
    /// The port reuse configuration for outgoing connections.
    ///
    /// If enabled, all IP addresses on which this listening stream
    /// is accepting connections (`in_addr`) are registered for reuse
    /// as local addresses for the sockets of outgoing connections. They are
    /// unregistered when the stream encounters an error or is dropped.
    port_reuse: PortReuse,
    /// How long to sleep after a (non-fatal) error while trying
    /// to accept a new connection.
    sleep_on_error: Duration,
    /// The current pause, if any.
    pause: Option<Delay>,
}

impl<T> TcpListenStream<T>
where
    T: Provider
{
    /// Constructs a `TcpListenStream` for incoming connections around
    /// the given `TcpListener`.
    fn new(listener: TcpListener, port_reuse: PortReuse) -> io::Result<Self> {
        let listen_addr = listener.local_addr()?;

        let in_addr = if match &listen_addr {
            SocketAddr::V4(a) => a.ip().is_unspecified(),
            SocketAddr::V6(a) => a.ip().is_unspecified(),
        } {
            // The `addrs` are populated via `if_watch` when the
            // `TcpListenStream` is polled.
            InAddr::Any {
                addrs: HashSet::new(),
                if_watch: IfWatch::Pending(T::if_watcher()),
            }
        } else {
            InAddr::One {
                out: Some(ip_to_multiaddr(listen_addr.ip(), listen_addr.port())),
                addr: listen_addr.ip(),
            }
        };

        let listener = T::new_listener(listener)?;

        Ok(TcpListenStream {
            port_reuse,
            listener,
            listen_addr,
            in_addr,
            pause: None,
            sleep_on_error: Duration::from_millis(100),
        })
    }

    /// Disables port reuse for any listen address of this stream.
    ///
    /// This is done when the `TcpListenStream` encounters a fatal
    /// error (for the stream) or is dropped.
    ///
    /// Has no effect if port reuse is disabled.
    fn disable_port_reuse(&mut self) {
        match &self.in_addr {
            InAddr::One { addr, .. } => {
                self.port_reuse.unregister(*addr, self.listen_addr.port());
            },
            InAddr::Any { addrs, .. } => {
                for addr in addrs {
                    self.port_reuse.unregister(*addr, self.listen_addr.port());
                }
            }
        }
    }
}

impl<T> Drop for TcpListenStream<T>
where
    T: Provider
{
    fn drop(&mut self) {
        self.disable_port_reuse();
    }
}

impl<T> Stream for TcpListenStream<T>
where
    T: Provider,
    T::Listener: Unpin,
    T::Stream: Unpin,
    T::IfWatcher: Unpin,
{
    type Item = Result<TcpListenerEvent<T::Stream>, io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let me = Pin::into_inner(self);

        loop {
            match &mut me.in_addr {
                InAddr::Any { if_watch, addrs } => match if_watch {
                    // If we listen on all interfaces, wait for `if-watch` to be ready.
                    IfWatch::Pending(f) => match ready!(Pin::new(f).poll(cx)) {
                        Ok(w) => {
                            *if_watch = IfWatch::Ready(w);
                            continue
                        }
                        Err(err) => {
                            log::debug! {
                                "Failed to begin observing interfaces: {:?}. Scheduling retry.",
                                err
                            };
                            *if_watch = IfWatch::Pending(T::if_watcher());
                            me.pause = Some(Delay::new(me.sleep_on_error));
                            return Poll::Ready(Some(Ok(ListenerEvent::Error(err))));
                        }
                    },
                    // Consume all events for up/down interface changes.
                    IfWatch::Ready(watch) => while let Poll::Ready(ev) = T::poll_interfaces(watch, cx) {
                        match ev {
                            Ok(IfEvent::Up(inet)) => {
                                let ip = inet.addr();
                                if me.listen_addr.is_ipv4() == ip.is_ipv4() && addrs.insert(ip) {
                                    let ma = ip_to_multiaddr(ip, me.listen_addr.port());
                                    log::debug!("New listen address: {}", ma);
                                    me.port_reuse.register(ip, me.listen_addr.port());
                                    return Poll::Ready(Some(Ok(ListenerEvent::NewAddress(ma))));
                                }
                            }
                            Ok(IfEvent::Down(inet)) => {
                                let ip = inet.addr();
                                if me.listen_addr.is_ipv4() == ip.is_ipv4() && addrs.remove(&ip) {
                                    let ma = ip_to_multiaddr(ip, me.listen_addr.port());
                                    log::debug!("Expired listen address: {}", ma);
                                    me.port_reuse.unregister(ip, me.listen_addr.port());
                                    return Poll::Ready(Some(Ok(ListenerEvent::AddressExpired(ma))));
                                }
                            }
                            Err(err) => {
                                log::debug! {
                                    "Failure polling interfaces: {:?}. Scheduling retry.",
                                    err
                                };
                                me.pause = Some(Delay::new(me.sleep_on_error));
                                return Poll::Ready(Some(Ok(ListenerEvent::Error(err))));
                            }
                        }
                    },
                },
                // If the listener is bound to a single interface, make sure the
                // address is registered for port reuse and reported once.
                InAddr::One { addr, out } => if let Some(multiaddr) = out.take() {
                    me.port_reuse.register(*addr, me.listen_addr.port());
                    return Poll::Ready(Some(Ok(ListenerEvent::NewAddress(multiaddr))))
                }
            }

            if let Some(mut pause) = me.pause.take() {
                match Pin::new(&mut pause).poll(cx) {
                    Poll::Ready(_) => {}
                    Poll::Pending => {
                        me.pause = Some(pause);
                        return Poll::Pending;
                    }
                }
            }

            // Take the pending connection from the backlog.
            let incoming = match T::poll_accept(&mut me.listener, cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(incoming)) => incoming,
                Poll::Ready(Err(e)) => {
                    // These errors are non-fatal for the listener stream.
                    log::error!("error accepting incoming connection: {}", e);
                    me.pause = Some(Delay::new(me.sleep_on_error));
                    return Poll::Ready(Some(Ok(ListenerEvent::Error(e))));
                }
            };

            let local_addr = ip_to_multiaddr(incoming.local_addr.ip(), incoming.local_addr.port());
            let remote_addr = ip_to_multiaddr(incoming.remote_addr.ip(), incoming.remote_addr.port());

            log::debug!("Incoming connection from {} at {}", remote_addr, local_addr);

            return Poll::Ready(Some(Ok(ListenerEvent::Upgrade {
                upgrade: future::ok(incoming.stream),
                local_addr,
                remote_addr,
            })));
        }
    }
}

/// Extracts a `SocketAddr` from a given `Multiaddr`.
///
/// Fails if the given `Multiaddr` does not begin with an IP
/// protocol encapsulating a TCP port.
fn multiaddr_to_socketaddr(mut addr: Multiaddr) -> Result<SocketAddr, ()> {
    // "Pop" the IP address and TCP port from the end of the address,
    // ignoring a `/p2p/...` suffix as well as any prefix of possibly
    // outer protocols, if present.
    let mut port = None;
    while let Some(proto) = addr.pop() {
        match proto {
            Protocol::Ip4(ipv4) => match port {
                Some(port) => return Ok(SocketAddr::new(ipv4.into(), port)),
                None => return Err(())
            },
            Protocol::Ip6(ipv6) => match port {
                Some(port) => return Ok(SocketAddr::new(ipv6.into(), port)),
                None => return Err(())
            },
            Protocol::Tcp(portnum) => match port {
                Some(_) => return Err(()),
                None => { port = Some(portnum) }
            }
            Protocol::P2p(_) => {}
            _ => return Err(())
        }
    }
    Err(())
}

// Create a [`Multiaddr`] from the given IP address and port number.
fn ip_to_multiaddr(ip: IpAddr, port: u16) -> Multiaddr {
    Multiaddr::empty()
        .with(ip.into())
        .with(Protocol::Tcp(port))
}

#[cfg(test)]
mod tests {
    use futures::channel::mpsc;
    use super::*;

    #[test]
    fn multiaddr_to_tcp_conversion() {
        use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

        assert!(
            multiaddr_to_socketaddr("/ip4/127.0.0.1/udp/1234".parse::<Multiaddr>().unwrap())
                .is_err()
        );

        assert_eq!(
            multiaddr_to_socketaddr("/ip4/127.0.0.1/tcp/12345".parse::<Multiaddr>().unwrap()),
            Ok(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                12345,
            ))
        );
        assert_eq!(
            multiaddr_to_socketaddr(
                "/ip4/255.255.255.255/tcp/8080"
                    .parse::<Multiaddr>()
                    .unwrap()
            ),
            Ok(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255)),
                8080,
            ))
        );
        assert_eq!(
            multiaddr_to_socketaddr("/ip6/::1/tcp/12345".parse::<Multiaddr>().unwrap()),
            Ok(SocketAddr::new(
                IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)),
                12345,
            ))
        );
        assert_eq!(
            multiaddr_to_socketaddr(
                "/ip6/ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/tcp/8080"
                    .parse::<Multiaddr>()
                    .unwrap()
            ),
            Ok(SocketAddr::new(
                IpAddr::V6(Ipv6Addr::new(
                    65535, 65535, 65535, 65535, 65535, 65535, 65535, 65535,
                )),
                8080,
            ))
        );
    }

    #[test]
    fn communicating_between_dialer_and_listener() {
        env_logger::try_init().ok();

        async fn listener<T: Provider>(addr: Multiaddr, mut ready_tx: mpsc::Sender<Multiaddr>)  {
            let tcp = GenTcpConfig::<T>::new();
            let mut listener = tcp.listen_on(addr).unwrap();
            loop {
                match listener.next().await.unwrap().unwrap() {
                    ListenerEvent::NewAddress(listen_addr) => {
                        ready_tx.send(listen_addr).await.unwrap();
                    }
                    ListenerEvent::Upgrade { upgrade, .. } => {
                        let mut upgrade = upgrade.await.unwrap();
                        let mut buf = [0u8; 3];
                        upgrade.read_exact(&mut buf).await.unwrap();
                        assert_eq!(buf, [1, 2, 3]);
                        upgrade.write_all(&[4, 5, 6]).await.unwrap();
                        return
                    }
                    e => panic!("Unexpected listener event: {:?}", e),
                }
            }
        }

        async fn dialer<T: Provider>(mut ready_rx: mpsc::Receiver<Multiaddr>) {
            let addr = ready_rx.next().await.unwrap();
            let tcp = GenTcpConfig::<T>::new();

            // Obtain a future socket through dialing
            let mut socket = tcp.dial(addr.clone()).unwrap().await.unwrap();
            socket.write_all(&[0x1, 0x2, 0x3]).await.unwrap();

            let mut buf = [0u8; 3];
            socket.read_exact(&mut buf).await.unwrap();
            assert_eq!(buf, [4, 5, 6]);
        }

        fn test(addr: Multiaddr) {
            #[cfg(feature = "async-io")]
            {
                let (ready_tx, ready_rx) = mpsc::channel(1);
                let listener = listener::<async_io::Tcp>(addr.clone(), ready_tx);
                let dialer = dialer::<async_io::Tcp>(ready_rx);
                let listener = async_std::task::spawn(listener);
                async_std::task::block_on(dialer);
                async_std::task::block_on(listener);
            }

            #[cfg(feature = "tokio")]
            {
                let (ready_tx, ready_rx) = mpsc::channel(1);
                let listener = listener::<tokio::Tcp>(addr.clone(), ready_tx);
                let dialer = dialer::<tokio::Tcp>(ready_rx);
                let rt = tokio_crate::runtime::Builder::new_current_thread().enable_io().build().unwrap();
                let tasks = tokio_crate::task::LocalSet::new();
                let listener = tasks.spawn_local(listener);
                tasks.block_on(&rt, dialer);
                tasks.block_on(&rt, listener).unwrap();
            }
        }

        test("/ip4/127.0.0.1/tcp/0".parse().unwrap());
        test("/ip6/::1/tcp/0".parse().unwrap());
    }

    #[test]
    fn wildcard_expansion() {
        env_logger::try_init().ok();

        async fn listener<T: Provider>(addr: Multiaddr, mut ready_tx: mpsc::Sender<Multiaddr>) {
            let tcp = GenTcpConfig::<T>::new();
            let mut listener = tcp.listen_on(addr).unwrap();

            loop {
                match listener.next().await.unwrap().unwrap() {
                    ListenerEvent::NewAddress(a) => {
                        let mut iter = a.iter();
                        match iter.next().expect("ip address") {
                            Protocol::Ip4(ip) => assert!(!ip.is_unspecified()),
                            Protocol::Ip6(ip) => assert!(!ip.is_unspecified()),
                            other => panic!("Unexpected protocol: {}", other),
                        }
                        if let Protocol::Tcp(port) = iter.next().expect("port") {
                            assert_ne!(0, port)
                        } else {
                            panic!("No TCP port in address: {}", a)
                        }
                        ready_tx.send(a).await.ok();
                        return
                    }
                    _ => {}
                }
            }
        }

        async fn dialer<T: Provider>(mut ready_rx: mpsc::Receiver<Multiaddr>) {
            let dest_addr = ready_rx.next().await.unwrap();
            let tcp = GenTcpConfig::<T>::new();
            tcp.dial(dest_addr).unwrap().await.unwrap();
        }

        fn test(addr: Multiaddr) {
            #[cfg(feature = "async-io")]
            {
                let (ready_tx, ready_rx) = mpsc::channel(1);
                let listener = listener::<async_io::Tcp>(addr.clone(), ready_tx);
                let dialer = dialer::<async_io::Tcp>(ready_rx);
                let listener = async_std::task::spawn(listener);
                async_std::task::block_on(dialer);
                async_std::task::block_on(listener);
            }

            #[cfg(feature = "tokio")]
            {
                let (ready_tx, ready_rx) = mpsc::channel(1);
                let listener = listener::<tokio::Tcp>(addr.clone(), ready_tx);
                let dialer = dialer::<tokio::Tcp>(ready_rx);
                let rt = tokio_crate::runtime::Builder::new_current_thread().enable_io().build().unwrap();
                let tasks = tokio_crate::task::LocalSet::new();
                let listener = tasks.spawn_local(listener);
                tasks.block_on(&rt, dialer);
                tasks.block_on(&rt, listener).unwrap();
            }
        }

        test("/ip4/0.0.0.0/tcp/0".parse().unwrap());
        test("/ip6/::1/tcp/0".parse().unwrap());
    }

    #[test]
    fn port_reuse_dialing() {
        env_logger::try_init().ok();

        async fn listener<T: Provider>(addr: Multiaddr, mut ready_tx: mpsc::Sender<Multiaddr>) {
            let tcp = GenTcpConfig::<T>::new();
            let mut listener = tcp.listen_on(addr).unwrap();
            loop {
                match listener.next().await.unwrap().unwrap() {
                    ListenerEvent::NewAddress(listen_addr) => {
                        ready_tx.send(listen_addr).await.ok();
                    }
                    ListenerEvent::Upgrade { upgrade, .. } => {
                        let mut upgrade = upgrade.await.unwrap();
                        let mut buf = [0u8; 3];
                        upgrade.read_exact(&mut buf).await.unwrap();
                        assert_eq!(buf, [1, 2, 3]);
                        upgrade.write_all(&[4, 5, 6]).await.unwrap();
                        return
                    }
                    e => panic!("Unexpected event: {:?}", e),
                }
            }
        }

        async fn dialer<T: Provider>(addr: Multiaddr, mut ready_rx: mpsc::Receiver<Multiaddr>) {
            let dest_addr = ready_rx.next().await.unwrap();
            let tcp = GenTcpConfig::<T>::new().port_reuse(true);
            let mut listener = tcp.clone().listen_on(addr).unwrap();
            match listener.next().await.unwrap().unwrap() {
                ListenerEvent::NewAddress(_) => {
                    // Obtain a future socket through dialing
                    let mut socket = tcp.dial(dest_addr).unwrap().await.unwrap();
                    socket.write_all(&[0x1, 0x2, 0x3]).await.unwrap();
                    // socket.flush().await;
                    let mut buf = [0u8; 3];
                    socket.read_exact(&mut buf).await.unwrap();
                    assert_eq!(buf, [4, 5, 6]);
                }
                e => panic!("Unexpected listener event: {:?}", e)
            }
        }

        fn test(addr: Multiaddr) {
            #[cfg(feature = "async-io")]
            {
                let (ready_tx, ready_rx) = mpsc::channel(1);
                let listener = listener::<async_io::Tcp>(addr.clone(), ready_tx);
                let dialer = dialer::<async_io::Tcp>(addr.clone(), ready_rx);
                let listener = async_std::task::spawn(listener);
                async_std::task::block_on(dialer);
                async_std::task::block_on(listener);
            }

            #[cfg(feature = "tokio")]
            {
                let (ready_tx, ready_rx) = mpsc::channel(1);
                let listener = listener::<tokio::Tcp>(addr.clone(), ready_tx);
                let dialer = dialer::<tokio::Tcp>(addr.clone(), ready_rx);
                let rt = tokio_crate::runtime::Builder::new_current_thread().enable_io().build().unwrap();
                let tasks = tokio_crate::task::LocalSet::new();
                let listener = tasks.spawn_local(listener);
                tasks.block_on(&rt, dialer);
                tasks.block_on(&rt, listener).unwrap();
            }
        }

        test("/ip4/127.0.0.1/tcp/0".parse().unwrap());
        test("/ip6/::1/tcp/0".parse().unwrap());
    }

    #[test]
    fn port_reuse_listening() {
        env_logger::try_init().ok();

        async fn listen_twice<T: Provider>(addr: Multiaddr) {
            let tcp = GenTcpConfig::<T>::new().port_reuse(true);
            let mut listener1 = tcp.clone().listen_on(addr).unwrap();
            match listener1.next().await.unwrap().unwrap() {
                ListenerEvent::NewAddress(addr1) => {
                    // Listen on the same address a second time.
                    let mut listener2 = tcp.clone().listen_on(addr1.clone()).unwrap();
                    match listener2.next().await.unwrap().unwrap() {
                        ListenerEvent::NewAddress(addr2) => {
                            assert_eq!(addr1, addr2);
                            return
                        }
                        e => panic!("Unexpected listener event: {:?}", e),
                    }
                }
                e => panic!("Unexpected listener event: {:?}", e),
            }
        }

        fn test(addr: Multiaddr) {
            #[cfg(feature = "async-io")]
            {
                let listener = listen_twice::<async_io::Tcp>(addr.clone());
                async_std::task::block_on(listener);
            }

            #[cfg(feature = "tokio")]
            {
                let listener = listen_twice::<tokio::Tcp>(addr.clone());
                let rt = tokio_crate::runtime::Builder::new_current_thread().enable_io().build().unwrap();
                rt.block_on(listener);
            }
        }

        test("/ip4/127.0.0.1/tcp/0".parse().unwrap());
    }

    #[test]
    fn listen_port_0() {
        env_logger::try_init().ok();

        async fn listen<T: Provider>(addr: Multiaddr) -> Multiaddr {
            GenTcpConfig::<T>::new()
                .listen_on(addr)
                .unwrap()
                .next()
                .await
                .expect("some event")
                .expect("no error")
                .into_new_address()
                .expect("listen address")
        }

        fn test(addr: Multiaddr) {
            #[cfg(feature = "async-io")]
            {
                let new_addr = async_std::task::block_on(listen::<async_io::Tcp>(addr.clone()));
                assert!(!new_addr.to_string().contains("tcp/0"));
            }

            #[cfg(feature = "tokio")]
            {
                let rt = tokio_crate::runtime::Builder::new_current_thread().enable_io().build().unwrap();
                let new_addr = rt.block_on(listen::<tokio::Tcp>(addr.clone()));
                assert!(!new_addr.to_string().contains("tcp/0"));
            }
        }

        test("/ip6/::1/tcp/0".parse().unwrap());
        test("/ip4/127.0.0.1/tcp/0".parse().unwrap());
    }

    #[test]
    fn listen_invalid_addr() {
        env_logger::try_init().ok();

        fn test(addr: Multiaddr) {
            #[cfg(feature = "async-io")]
            {
                let tcp = TcpConfig::new();
                assert!(tcp.listen_on(addr.clone()).is_err());
            }

            #[cfg(feature = "tokio")]
            {
                let tcp = TokioTcpConfig::new();
                assert!(tcp.listen_on(addr.clone()).is_err());
            }
        }

        test("/ip4/127.0.0.1/tcp/12345/tcp/12345".parse().unwrap());
    }
}
