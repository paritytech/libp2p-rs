// Copyright 2017-2020 Parity Technologies (UK) Ltd.
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

//! Background task dedicated to manage the QUIC state machine.
//!
//! Considering that all QUIC communications happen over a single UDP socket, one needs to
//! maintain a unique synchronization point that holds the state of all the active connections.
//!
//! The [`Endpoint`] object represents this synchronization point. It maintains a background task
//! whose role is to interface with the UDP socket. Communication between the background task and
//! the rest of the code only happens through channels. See the documentation of the
//! [`background_task`] for a thorough description.

use crate::{connection::Connection, x509};

use async_std::net::SocketAddr;
use either::Either;
use futures::{
    channel::{mpsc, oneshot},
    lock::Mutex,
    prelude::*,
};
use libp2p_core::{
    multiaddr::{host_addresses, Multiaddr, Protocol},
    transport::TransportError,
};
use std::{
    collections::{HashMap, VecDeque},
    fmt, io,
    sync::{Arc, Weak},
    task::Poll,
    time::{Duration, Instant},
};
use tracing::{info, warn};

/// Represents the configuration for the [`Endpoint`].
#[derive(Debug, Clone)]
pub struct Config {
    /// The client configuration to pass to `quinn_proto`.
    client_config: quinn_proto::ClientConfig,
    /// The server configuration to pass to `quinn_proto`.
    server_config: Arc<quinn_proto::ServerConfig>,
    /// The endpoint configuration to pass to `quinn_proto`.
    endpoint_config: Arc<quinn_proto::EndpointConfig>,
    /// The [`Multiaddr`] to use to spawn the UDP socket.
    multiaddr: Multiaddr,
}

impl Config {
    /// Creates a new configuration object with default values.
    pub fn new(
        keypair: &libp2p_core::identity::Keypair,
        multiaddr: Multiaddr,
    ) -> Result<Self, x509::ConfigError> {
        let mut transport = quinn_proto::TransportConfig::default();
        transport.stream_window_uni(0);
        transport.datagram_receive_buffer_size(None);
        transport.keep_alive_interval(Some(Duration::from_millis(10)));
        let transport = Arc::new(transport);
        let (client_tls_config, server_tls_config) = x509::make_tls_config(keypair)?;
        let mut server_config = quinn_proto::ServerConfig::default();
        server_config.transport = transport.clone();
        server_config.crypto = Arc::new(server_tls_config);
        let mut client_config = quinn_proto::ClientConfig::default();
        client_config.transport = transport;
        client_config.crypto = Arc::new(client_tls_config);
        Ok(Self {
            client_config,
            server_config: Arc::new(server_config),
            endpoint_config: Default::default(),
            multiaddr: multiaddr,
        })
    }
}

/// Object containing all the QUIC resources shared between all connections.
// TODO: expand docs
// TODO: Debug trait
// TODO: remove useless fields
pub struct Endpoint {
    /// Channel to the background of the endpoint.
    /// See [`Endpoint::new_connections`] (just below) for a commentary about the mutex.
    to_endpoint: Mutex<mpsc::Sender<ToEndpoint>>,

    /// Channel where new connections are being sent.
    /// This is protected by a futures-friendly `Mutex`, meaning that receiving a connection is
    /// done in two steps: locking this mutex, and grabbing the next element on the `Receiver`.
    /// The only consequence of this `Mutex` is that multiple simultaneous calls to
    /// [`Endpoint::next_incoming`] are serialized.
    new_connections: Mutex<mpsc::Receiver<Either<Connection, Multiaddr>>>,

    /// Copy of [`Endpoint::to_endpoint`], except not behind a `Mutex`. Used if we want to be guaranteed a
    /// slot in the messages buffer.
    to_endpoint2: mpsc::Sender<ToEndpoint>,

    /// Configuration passed at initialization.
    // TODO: remove?
    config: Config,
    /// Multiaddr of the local UDP socket passed in the configuration at initialization after it
    /// has potentially been modified to handle port number `0`.
    local_multiaddr: Multiaddr,
}

impl Endpoint {
    /// Builds a new `Endpoint`.
    pub fn new(config: Config) -> Result<Arc<Endpoint>, TransportError<io::Error>> {
        let mut multiaddr = config.multiaddr.clone();
        let local_socket_addr = match crate::transport::multiaddr_to_socketaddr(&config.multiaddr) {
            Ok(a) => a,
            Err(()) => return Err(TransportError::MultiaddrNotSupported(multiaddr)),
        };

        // NOT blocking, as per man:bind(2), as we pass an IP address.
        let socket =
            std::net::UdpSocket::bind(&local_socket_addr).map_err(TransportError::Other)?;
        // TODO:
        let port_is_zero = local_socket_addr.port() == 0;
        let local_socket_addr = socket.local_addr().map_err(TransportError::Other)?;
        if port_is_zero {
            assert_ne!(local_socket_addr.port(), 0);
            assert_eq!(multiaddr.pop(), Some(Protocol::Quic));
            assert_eq!(multiaddr.pop(), Some(Protocol::Udp(0)));
            multiaddr.push(Protocol::Udp(local_socket_addr.port()));
            multiaddr.push(Protocol::Quic);
        }

        let (to_endpoint_tx, to_endpoint_rx) = mpsc::channel(32);
        let to_endpoint2 = to_endpoint_tx.clone();
        let (new_connections_tx, new_connections_rx) = mpsc::channel(500);

        let endpoint = Arc::new(Endpoint {
            to_endpoint: Mutex::new(to_endpoint_tx),
            to_endpoint2,
            new_connections: Mutex::new(new_connections_rx),
            config: config.clone(),
            local_multiaddr: config.multiaddr.clone(), // TODO: no
        });

        let send_addr = |e| {
            new_connections_tx
                .clone()
                .try_send(Either::Right(e))
                .expect("we just cloned this, so we have capacity; qed")
        };

        // TODO: IP address stuff
        if local_socket_addr.ip().is_unspecified() {
            tracing::info!("returning all local IPs for unspecified address");
            let suffixes = [Protocol::Udp(local_socket_addr.port()), Protocol::Quic];
            let local_addresses = host_addresses(&suffixes).map_err(TransportError::Other)?;
            for (_, _, address) in local_addresses {
                tracing::info!("sending address {:?}", address);
                send_addr(address)
            }
        } else {
            tracing::info!("sending address {:?}", multiaddr);
            send_addr(multiaddr)
        }

        // TODO: just for testing, do proper task spawning
        async_std::task::spawn(background_task(
            config.clone(),
            Arc::downgrade(&endpoint),
            async_std::net::UdpSocket::from(socket),
            new_connections_tx,
            to_endpoint_rx.fuse(),
        ));

        // let endpoint = EndpointRef { reference, channel };
        // let join_handle = spawn(endpoint.clone());
        // Ok((Self(endpoint), join_handle))

        Ok(endpoint)
    }

    /// Asks the endpoint to start dialing the given address.
    ///
    /// Note that this method only *starts* the dialing. `Ok` is returned as soon as possible, even
    /// when the remote might end up being unreachable.
    pub(crate) async fn dial(
        &self,
        addr: SocketAddr,
    ) -> Result<Connection, quinn_proto::ConnectError> {
        // The two `expect`s below can panic if the background task has stopped. The background
        // task can stop only if the `Endpoint` is destroyed or if the task itself panics. In other
        // words, we panic here iff a panic has already happened somewhere else, which is a
        // reasonable thing to do.
        let (tx, rx) = oneshot::channel();
        self.to_endpoint
            .lock()
            .await
            .send(ToEndpoint::Dial { addr, result: tx })
            .await
            .expect("background task has crashed");
        info!("Sent dial message, awaiting response");
        rx.await.expect("background task has crashed")
    }

    /// Tries to pop a new incoming connection from the queue.
    pub(crate) async fn next_incoming(&self) -> Either<Connection, Multiaddr> {
        // The `expect` below can panic if the background task has stopped. The background task
        // can stop only if the `Endpoint` is destroyed or if the task itself panics. In other
        // words, we panic here iff a panic has already happened somewhere else, which is a
        // reasonable thing to do.
        let mut new_connections = self.new_connections.lock().await;
        new_connections
            .next()
            .await
            .expect("background task has crashed")
    }

    /// Asks the endpoint to send a UDP packet.
    ///
    /// Note that this method only queues the packet and returns as soon as the packet is in queue.
    /// There is no guarantee that the packet will actually be sent, but considering that this is
    /// a UDP packet, you cannot rely on the packet being delivered anyway.
    pub(crate) async fn send_udp_packet(
        &self,
        destination: SocketAddr,
        data: impl Into<Box<[u8]>>,
    ) {
        let _ = self
            .to_endpoint
            .lock()
            .await
            .send(ToEndpoint::SendUdpPacket {
                destination,
                data: data.into(),
            })
            .await;
    }

    /// Report to the endpoint an event on a [`quinn_proto::Connection`].
    ///
    /// This is typically called by a [`Connection`].
    ///
    /// If `event.is_drained()` is true, the event indicates that the connection no longer exists.
    /// This must therefore be the last event sent using this [`quinn_proto::ConnectionHandle`].
    pub(crate) async fn report_quinn_event(
        &self,
        connection_id: quinn_proto::ConnectionHandle,
        event: quinn_proto::EndpointEvent,
    ) {
        self.to_endpoint
            .lock()
            .await
            .send(ToEndpoint::ProcessConnectionEvent {
                connection_id,
                event,
            })
            .await
            .expect("background task has crashed");
    }

    /// Similar to [`Endpoint::report_quinn_event`], except that the message sending is guaranteed
    /// to be instantaneous and to succeed.
    ///
    /// This method bypasses back-pressure mechanisms and is meant to be called only from
    /// destructors, where waiting is not advisable.
    pub(crate) fn report_quinn_event_non_block(
        &self,
        connection_id: quinn_proto::ConnectionHandle,
        event: quinn_proto::EndpointEvent,
    ) {
        // We implement this by cloning the `mpsc::Sender`. Since each sender is guaranteed a slot
        // in the buffer, cloning the sender reserves the slot and sending thus always succeeds.
        let result = self
            .to_endpoint2
            .clone()
            .try_send(ToEndpoint::ProcessConnectionEvent {
                connection_id,
                event,
            });
        assert!(result.is_ok());
    }
}

/// Message sent to the endpoint background task.
#[derive(Debug)]
enum ToEndpoint {
    /// Instruct the endpoint to start connecting to the given address.
    Dial {
        /// UDP address to connect to.
        addr: SocketAddr,
        /// Channel to return the result of the dialing to.
        result: oneshot::Sender<Result<Connection, quinn_proto::ConnectError>>,
    },
    /// Sent by a `quinn_proto` connection when the endpoint needs to process an event generated
    /// by a connection. The event itself is opaque to us. Only `quinn_proto` knows what is in
    /// there.
    ProcessConnectionEvent {
        connection_id: quinn_proto::ConnectionHandle,
        event: quinn_proto::EndpointEvent,
    },
    /// Instruct the endpoint to send a packet of data on its UDP socket.
    SendUdpPacket {
        /// Destination of the UDP packet.
        destination: SocketAddr,
        /// Packet of data to send.
        data: Box<[u8]>,
    },
}

/// Task that runs in the background for as long as the endpont is alive. Responsible for
/// processing messages and the UDP socket.
///
/// The `receiver` parameter must be the receiving side of the `Endpoint::to_endpoint` sender.
///
/// # Behaviour
///
/// This background task is responsible for the following:
///
/// - Sending packets on the UDP socket.
/// - Receiving packets from the UDP socket and feed them to the [`quinn_proto::Endpoint`] state
///   machine.
/// - Transmitting events generated by the [`quinn_proto::Endpoint`] to the corresponding
///   [`Connection`].
/// - Receiving messages from the `receiver` and processing the requested actions. This includes
///   UDP packets to send and events emitted by the [`Connection`] objects.
/// - Sending new connections on `new_connections`.
///
/// When it comes to channels, there exists three main multi-producer-single-consumer channels
/// in play:
///
/// - One channel, represented by `Endpoint::to_endpoint` and `receiver`, that communicates
///   messages from [`Endpoint`] to the background task and from the [`Connection`] to the
///   background task.
/// - One channel per each existing connection that communicates messages from the background
///   task to that [`Connection`].
/// - One channel for the background task to send newly-opened connections to. The receiving
///   side is normally processed by a "listener" as defined by the [`libp2p_core::Transport`]
///   trait.
///
/// In order to avoid an unbounded buffering of events, we prioritize sending data on the UDP
/// socket over everything else. If the network interface is too busy to process our packets,
/// everything comes to a freeze (including receiving UDP packets) until it is ready to accept
/// more.
///
/// Apart from freezing when the network interface is too busy, the background task should sleep
/// as little as possible. It is in particular important for the `receiver` to be drained as
/// quickly as possible in order to avoid unnecessary back-pressure on the [`Connection`] objects.
///
/// ## Back-pressure on `new_connections`
///
/// The [`quinn_proto::Endpoint`] object contains an accept buffer, in other words a buffer of the
/// incoming connections waiting to be accepted. When a new connection is signalled, we send this
/// new connection on the `new_connections` channel in an asynchronous way, and we only free a slot
/// in the accept buffer once the element has actually been enqueued on `new_connections`. There
/// are therefore in total three buffers in play: the `new_connections` channel itself, the queue
/// of elements being sent on `new_connections`, and the accept buffer of the
/// [`quinn_proto::Endpoint`].
///
/// Unfortunately, this design has the consequence that, on the network layer, we will accept a
/// certain number of incoming connections even if [`Endpoint::next_incoming`] is never even
/// called. The `quinn-proto` library doesn't provide any way to not accept incoming connections
/// apart from filling the accept buffer.
///
/// ## Back-pressure on connections
///
/// Because connections are processed by the user at a rate of their choice, we cannot properly
/// handle the situation where the channel from the background task to individual connections is
/// full. Sleeping the task while waiting for the connection to be processed by the user could
/// even lead to a deadlock if this processing is also sleeping waiting for some other action that
/// itself depends on the background task (e.g. if processing the connection is waiting for a
/// message arriving on a different connection).
///
/// In an ideal world, we would handle a background-task-to-connection channel being full by
/// dropping UDP packets destined to this connection, as a way to back-pressure the remote.
/// Unfortunately, the `quinn-proto` library doesn't provide any way for us to know which
/// connection a UDP packet is destined for before it has been turned into a
/// [`ConnectionEvent`](quinn_proto::ConnectionEvent) and because these
/// [`ConnectionEvent`](quinn_proto::ConnectionEvent)s are sometimes used to synchronize the states
/// of the endpoint and connection, it would be a logic error to silently drop them.
///
/// We handle this tricky situation by simply killing connections as soon as their associated
/// channel is full.
///
// TODO: actually implement the killing of connections if channel is full, at the moment we just
// wait
/// # Shutdown
///
/// The background task shuts down if `endpoint_weak`, `receiver` or `new_connections` become
/// disconnected/invalid. This corresponds to the lifetime of the associated [`Endpoint`].
///
/// Keep in mind that we pass an `Arc<Endpoint>` whenever we create a new connection, which
/// guarantees that the [`Endpoint`], and therefore the background task, is properly kept alive
/// for as long as any QUIC connection is open.

#[derive(Copy, Clone, Debug)]
enum Void {}
async fn background_task(
    config: Config,
    endpoint_weak: Weak<Endpoint>,
    udp_socket: async_std::net::UdpSocket,
    mut new_connections: mpsc::Sender<Either<Connection, Multiaddr>>,
    mut receiver: stream::Fuse<mpsc::Receiver<ToEndpoint>>,
) {
    // The actual QUIC state machine.
    let mut endpoint = quinn_proto::Endpoint::new(
        config.endpoint_config.clone(),
        Some(config.server_config.clone()),
    );

    // List of all active connections, with a sender to notify them of events.
    let mut alive_connections = HashMap::<quinn_proto::ConnectionHandle, mpsc::Sender<_>>::new();

    // Buffer where we write packets received from the UDP socket.
    let mut socket_recv_buffer = vec![0; 65536];

    // The quinn_proto endpoint can give us new connections for as long as its accept buffer
    // isn't full. This buffer is used to push these new connections while we are waiting to
    // send them on the `new_connections` channel. We only call `endpoint.accept()` when we remove
    // an element from this list, which guarantees that it doesn't grow unbounded.
    // TODO: with_capacity?
    let mut queued_new_connections = VecDeque::new();

    // Next packet waiting to be transmitted on the UDP socket, if any.
    // Note that this variable isn't strictly necessary, but it reduces code duplication in the
    // code below.
    let mut next_packet_out: Option<(SocketAddr, Box<[u8]>)> = None;

    // Main loop of the task.
    loop {
        // Start by flushing `next_packet_out`.
        if let Some((destination, data)) = next_packet_out.take() {
            tracing::trace!("sending {} bytes to {}", data.len(), destination);
            // We block the current task until the packet is sent. This way, if the
            // network interface is too busy, we back-pressure all of our internal
            // channels.
            // TODO: set ECN bits; there is no support for them in the ecosystem right now
            // TODO: use a circular buffer instead.
            match udp_socket.send_to(&data, destination).await {
                Ok(n) if n == data.len() => {}
                Ok(_) => tracing::error!(
                    "QUIC UDP socket violated expectation that packets are always fully \
                    transferred"
                ),

                // Errors on the socket are expected to never happen, and we handle them by simply
                // printing a log message. The packet gets discarded in case of error, but we are
                // robust to packet losses and it is consequently not a logic error to process with
                // normal operations.
                Err(err) => tracing::error!("Error while sending on QUIC UDP socket: {:?}", err),
            }
        }

        // The endpoint might request packets to be sent out. This is handled in priority to avoid
        // buffering up packets.
        if let Some(packet) = endpoint.poll_transmit() {
            tracing::trace!("Got a new packet to send");
            assert!(next_packet_out.is_none());
            next_packet_out = Some((packet.destination, packet.contents));
            continue;
        }

        futures::select! {
            message = receiver.next() => {
                // Received a message from a different part of the code requesting us to
                // do something.
                span!("message received");
                match message {
                    // Shut down if the endpoint has shut down.
                    None => return,

                    Some(ToEndpoint::Dial { addr, result }) => {
                        span!("dialing", addr = display(addr), side = debug(quinn_proto::Side::Client));
                        info!("received dialout request");
                        // This `"l"` seems necessary because an empty string is an invalid domain
                        // name. While we don't use domain names, the underlying rustls library
                        // is based upon the assumption that we do.
                        let (connection_id, connection) =
                            match endpoint.connect(config.client_config.clone(), addr, "l") {
                                Ok(c) => c,
                                Err(err) => {
                                    let _ = result.send(Err(err));
                                    warn!("QUIC connection failure");
                                    continue;
                                }
                            };

                        info!("received connection ID: {:?}", connection_id);
                        let endpoint_arc = match endpoint_weak.upgrade() {
                            Some(ep) => ep,
                            None => return, // Shut down the task if the endpoint is dead.
                        };

                        info!("endpoint is alive");

                        assert_eq!(connection.side(), quinn_proto::Side::Client);
                        let (tx, rx) = mpsc::channel(16);
                        let connection = Connection::from_quinn_connection(endpoint_arc, connection, connection_id, rx);
                        alive_connections.insert(connection_id, tx);
                        let _ = result.send(Ok(connection));
                        info!("sent reply to dialer");
                    }

                    // A connection wants to notify the endpoint of something.
                    Some(ToEndpoint::ProcessConnectionEvent { connection_id, event }) => {
                        if !alive_connections.contains_key(&connection_id) {
                            continue
                        }
                        // We "drained" event indicates that the connection no longer exists and
                        // its ID can be reclaimed.
                        let is_drained_event = event.is_drained();
                        if is_drained_event {
                            alive_connections.remove(&connection_id);
                        }
                        if let Some(event_back) = endpoint.handle_event(connection_id, event) {
                            assert!(!is_drained_event);
                            // TODO: don't await here /!\
                            let _ =  alive_connections.get_mut(&connection_id).unwrap().clone().try_send(event_back);
                        }
                    }

                    // Data needs to be sent on the UDP socket.
                    Some(ToEndpoint::SendUdpPacket { destination, data }) => {
                        assert!(next_packet_out.is_none());
                        next_packet_out = Some((destination, data));
                        continue;
                    }
                }
            }

            // The future we create here wakes up if two conditions are fulfilled:
            //
            // - The `new_connections` channel is ready to accept a new element.
            // - `queued_new_connections` is not empty.
            //
            // When this happens, we pop an element from `queued_new_connections`, put it on the
            // channel, and call `endpoint.accept()`, thereby allowing the QUIC state machine to
            // feed a new incoming connection to us.
            readiness = {
                let active = !queued_new_connections.is_empty();
                let new_connections = &mut new_connections;
                future::poll_fn(move |cx| {
                    if active { new_connections.poll_ready(cx) } else { Poll::Pending }
                }).fuse()
            } => {
                span!("time to accept connection");
                if readiness.is_err() {
                    // new_connections channel has been dropped, meaning that the endpoint has
                    // been destroyed.
                    return;
                }

                let elem = queued_new_connections.pop_front()
                    .expect("if queue is empty, the future above is always Pending; qed");
                new_connections.start_send(elem)
                    .expect("future is waken up only if poll_ready returned Ready; qed");
                endpoint.accept();
            }

            result = udp_socket.recv_from(&mut socket_recv_buffer).fuse() => {
                let (packet_len, packet_src) = match result {
                    Ok(v) => v,
                    // Errors on the socket are expected to never happen, and we handle them by
                    // simply printing a log message.
                    Err(err) => {
                        tracing::error!("Error while receive on QUIC UDP socket: {:?}", err);
                        continue;
                    },
                };
                span!("received packet", len = display(packet_len), src = display(packet_src));
                tracing::trace!("processing");

                // Received a UDP packet from the socket.
                assert!(packet_len <= socket_recv_buffer.len());
                let packet = From::from(&socket_recv_buffer[..packet_len]);
                // TODO: ECN bits aren't handled
                match endpoint.handle(Instant::now(), packet_src, None, packet) {
                    None => {},
                    Some((connec_id, quinn_proto::DatagramEvent::ConnectionEvent(event))) => {
                        // Event to send to an existing connection.
                        if let Some(sender) = alive_connections.get_mut(&connec_id) {
                            let _ = sender.clone().try_send(event);
                        } else {
                            tracing::error!("State mismatch: event for closed connection");
                        }
                    },
                    Some((connec_id, quinn_proto::DatagramEvent::NewConnection(connec))) => {
                        // A new connection has been received. `connec_id` is a newly-allocated
                        // identifier.
                        assert_eq!(connec.side(), quinn_proto::Side::Server);
                        let (tx, rx) = mpsc::channel(16);
                        alive_connections.insert(connec_id, tx);
                        let endpoint_arc = match endpoint_weak.upgrade() {
                            Some(ep) => ep,
                            None => {
                                tracing::trace!("endpoint is dead, exiting");
                                return // Shut down the task if the endpoint is dead.
                            }
                        };
                        let connection = Connection::from_quinn_connection(endpoint_arc, connec, connec_id, rx);

                        // As explained in the documentation, we put this new connection in an
                        // intermediary buffer. At the next loop iteration we will try to move it
                        // to the `new_connections` channel. We call `endpoint.accept()` only once
                        // the element has successfully been sent on `new_connections`.
                        queued_new_connections.push_back(Either::Left(connection));
                        tracing::trace!("connection queued");
                    },
                }
                tracing::trace!("receive processing complete");
            }
        }
    }
}

impl fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("Endpoint").finish()
    }
}
