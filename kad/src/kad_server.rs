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

//! Contains a `ConnectionUpgrade` that makes it possible to send requests and receive responses
//! from nodes after the upgrade.
//!
//! # Usage
//!
//! - Create a `KademliaServerConfig` object. This struct implements `ConnectionUpgrade`.
//!
//! - Update a connection through that `KademliaServerConfig`. The output yields you a
//!   `KademliaServerController` and a stream that must be driven to completion. The controller
//!   allows you to perform queries and receive responses. The stream produces incoming requests
//!   from the remote.
//!
//! This `KademliaServerController` is usually extracted and stored in some sort of hash map in an
//! `Arc` in order to be available whenever we need to request something from a node.

use bytes::Bytes;
use futures::sync::{mpsc, oneshot};
use futures::{future, Future, Sink, stream, Stream};
use libp2p_peerstore::PeerId;
use libp2p_core::ConnectionUpgrade;
use libp2p_core::Endpoint;
use multiaddr::Multiaddr;
use protocol::{self, KadMsg, KademliaProtocolConfig, Peer, ConnectionType};
use std::collections::VecDeque;
use std::io::{Error as IoError, ErrorKind as IoErrorKind};
use std::iter;
use std::sync::{Arc, atomic};
use tokio_io::{AsyncRead, AsyncWrite};

/// Configuration for a Kademlia server.
///
/// Implements `ConnectionUpgrade`. On a successful upgrade, produces a `KademliaServerController`
/// and a `Future`. The controller lets you send queries to the remote and receive answers, while
/// the `Future` must be driven to completion in order for things to work.
#[derive(Debug, Clone)]
pub struct KademliaServerConfig {
    raw_proto: KademliaProtocolConfig,
}

impl KademliaServerConfig {
    /// Builds a configuration object for an upcoming Kademlia server.
    #[inline]
    pub fn new() -> Self {
        KademliaServerConfig {
            raw_proto: KademliaProtocolConfig,
        }
    }
}

impl<C> ConnectionUpgrade<C> for KademliaServerConfig
where
    C: AsyncRead + AsyncWrite + 'static, // TODO: 'static :-/
{
    type Output = (
        KademliaServerController,
        Box<Stream<Item = KademliaIncomingRequest, Error = IoError>>,
    );
    type Future = Box<Future<Item = Self::Output, Error = IoError>>;
    type NamesIter = iter::Once<(Bytes, ())>;
    type UpgradeIdentifier = ();

    #[inline]
    fn protocol_names(&self) -> Self::NamesIter {
        ConnectionUpgrade::<C>::protocol_names(&self.raw_proto)
    }

    #[inline]
    fn upgrade(self, incoming: C, id: (), endpoint: Endpoint, addr: &Multiaddr) -> Self::Future {
        let future = self.raw_proto
            .upgrade(incoming, id, endpoint, addr)
            .map(move |connec| {
                let (tx, rx) = mpsc::unbounded();
                let future = kademlia_handler(connec, rx);
                let controller = KademliaServerController { inner: tx };
                (controller, future)
            });

        Box::new(future) as Box<_>
    }
}

/// Allows sending Kademlia requests and receiving responses.
#[derive(Debug, Clone)]
pub struct KademliaServerController {
    // In order to send a request, we use this sender to send a tuple. The first element of the
    // tuple is the message to send to the remote, and the second element is what is used to
    // receive the response. If the query doesn't expect a response (eg. `PUT_VALUE`), then the
    // one-shot sender will be dropped without being used.
    inner: mpsc::UnboundedSender<(KadMsg, oneshot::Sender<KadMsg>)>,
}

impl KademliaServerController {
    /// Sends a `FIND_NODE` query to the node and provides a future that will contain the response.
    // TODO: future item could be `impl Iterator` instead
    pub fn find_node(
        &self,
        searched_key: &PeerId,
    ) -> impl Future<Item = Vec<Peer>, Error = IoError> {
        let message = protocol::KadMsg::FindNodeReq {
            key: searched_key.clone().into_bytes(),
        };

        let (tx, rx) = oneshot::channel();

        match self.inner.unbounded_send((message, tx)) {
            Ok(()) => (),
            Err(_) => {
                let fut = future::err(IoError::new(
                    IoErrorKind::ConnectionAborted,
                    "connection to remote has aborted",
                ));

                return future::Either::B(fut);
            }
        };

        let future = rx.map_err(|_| {
            IoError::new(
                IoErrorKind::ConnectionAborted,
                "connection to remote has aborted",
            )
        }).and_then(|msg| match msg {
            KadMsg::FindNodeRes { closer_peers, .. } => Ok(closer_peers),
            _ => Err(IoError::new(
                IoErrorKind::InvalidData,
                "invalid response type received from the remote",
            )),
        });

        future::Either::A(future)
    }

    /// Sends a `PING` query to the node. Because of the way the protocol is designed, there is
    /// no way to differentiate between a ping and a pong. Therefore this function doesn't return a
    /// future, and the only way to be notified of the result is through the stream.
    pub fn ping(&self) -> Result<(), IoError> {
        // Dummy channel, as the `tx` is going to be dropped anyway.
        let (tx, _rx) = oneshot::channel();
        match self.inner.unbounded_send((protocol::KadMsg::Ping, tx)) {
            Ok(()) => Ok(()),
            Err(_) => Err(IoError::new(
                IoErrorKind::ConnectionAborted,
                "connection to remote has aborted",
            )),
        }
    }
}

/// Request received from the remote.
pub enum KademliaIncomingRequest {
    /// Find the nodes closest to `searched`.
    FindNode {
        /// The value being searched.
        searched: PeerId,
        /// Object to use to respond to the request.
        responder: KademliaFindNodeRespond,
    },

    // TODO: PutValue and FindValue

    /// Received either a ping or a pong.
    PingPong,
}

/// Object used to respond to `FindNode` queries from remotes.
pub struct KademliaFindNodeRespond {
    inner: oneshot::Sender<KadMsg>,
}

impl KademliaFindNodeRespond {
    /// Respond to the `FindNode` request.
    pub fn respond<I>(self, peers: I)
        where I: Iterator<Item = (PeerId, Vec<Multiaddr>, ConnectionType)>
    {
        let _ = self.inner.send(KadMsg::FindNodeRes {
            closer_peers: peers.map(|peer| {
                protocol::Peer {
                    node_id: peer.0,
                    multiaddrs: peer.1,
                    connection_ty: peer.2,
                }
            }).collect()
        });
    }
}

// Handles a newly-opened Kademlia stream with a remote peer.
//
// Takes a `Stream` and `Sink` of Kademlia messages representing the connection to the client,
// plus a `Receiver` that will receive messages to transmit to that connection.
//
// Returns a `Stream` that must be resolved in order for progress to work. The `Stream` will
// produce objects that represent the requests sent by the remote. These requests must be answered
// immediately before the stream continues to produce items.
fn kademlia_handler<'a, S>(
    kad_bistream: S,
    rq_rx: mpsc::UnboundedReceiver<(KadMsg, oneshot::Sender<KadMsg>)>,
) -> Box<Stream<Item = KademliaIncomingRequest, Error = IoError> + 'a>
where
    S: Stream<Item = KadMsg, Error = IoError> + Sink<SinkItem = KadMsg, SinkError = IoError> + 'a,
{
    let (kad_sink, kad_stream) = kad_bistream.split();

    // This is a stream of futures containing local responses.
    // Every time we receive a request from the remote, we create a `oneshot::channel()` and send
    // the receiving end to `responders_tx`.
    // This way, if a future is available on `responders_rx`, we block until it produces the
    // response.
    let (responders_tx, responders_rx) = mpsc::unbounded();

    // Will be set to true if either `kad_stream` or `rq_rx` is closed.
    let finished = Arc::new(atomic::AtomicBool::new(false));

    // We combine all the streams into one so that the loop wakes up whenever any generates
    // something.
    enum EventSource {
        Remote(KadMsg),
        LocalRequest(KadMsg, oneshot::Sender<KadMsg>),
        LocalResponse(oneshot::Receiver<KadMsg>),
        Finished,
    }

    let events = {
        let responders = responders_rx
            .map(|m| EventSource::LocalResponse(m))
            .map_err(|_| unreachable!());
        let rq_rx = rq_rx
            .map(|(m, o)| EventSource::LocalRequest(m, o))
            .map_err(|_| unreachable!())
            .chain({
                let finished = finished.clone();
                future::lazy(move || {
                    finished.store(true, atomic::Ordering::SeqCst);
                    Ok(EventSource::Finished)
                }).into_stream()
            });
        let kad_stream = kad_stream
            .map(|m| EventSource::Remote(m))
            .chain({
                let finished = finished.clone();
                future::lazy(move || {
                    finished.store(true, atomic::Ordering::SeqCst);
                    Ok(EventSource::Finished)
                }).into_stream()
            });
        responders.select(rq_rx).select(kad_stream)
    };

    let stream = stream::unfold((events, kad_sink, responders_tx, VecDeque::new(), 0u32),
        move |(events, kad_sink, responders_tx, mut send_back_queue, expected_pongs)| {
            if finished.load(atomic::Ordering::SeqCst) {
                return None;
            }

            Some(events
                .into_future()
                .map_err(|(err, _)| err)
                .and_then(move |(message, events)| -> Box<Future<Item = _, Error = _>> {
                    match message {
                        Some(EventSource::Finished) | None => {
                            // `finished` should have been set to true earlier, causing this
                            // function to return `None`.
                            unreachable!()
                        },
                        Some(EventSource::LocalResponse(message)) => {
                            let future = message
                                .map_err(|_| {
                                    // The user destroyed the responder without responding.
                                    warn!("Kad responder object destroyed without responding");
                                    panic!()        // TODO: what to do here? we have to close the connection
                                })
                                .and_then(move |message| {
                                    kad_sink
                                        .send(message)
                                        .map(move |kad_sink| {
                                            let state = (events, kad_sink, responders_tx, send_back_queue, expected_pongs);
                                            (None, state)
                                        })
                                });
                            Box::new(future)
                        },
                        Some(EventSource::LocalRequest(message @ KadMsg::PutValue { .. }, _)) => {
                            // A `PutValue` request. Contrary to other types of messages, this one
                            // doesn't expect any answer and therefore we ignore the sender.
                            let future = kad_sink
                                .send(message)
                                .map(move |kad_sink| {
                                    let state = (events, kad_sink, responders_tx, send_back_queue, expected_pongs);
                                    (None, state)
                                });
                            Box::new(future) as Box<_>
                        }
                        Some(EventSource::LocalRequest(message @ KadMsg::Ping { .. }, _)) => {
                            // A local `Ping` request.
                            let expected_pongs = expected_pongs.checked_add(1)
                                .expect("overflow in number of simultaneous pings");
                            let future = kad_sink
                                .send(message)
                                .map(move |kad_sink| {
                                    let state = (events, kad_sink, responders_tx, send_back_queue, expected_pongs);
                                    (None, state)
                                });
                            Box::new(future) as Box<_>
                        }
                        Some(EventSource::LocalRequest(message, send_back)) => {
                            // Any local request other than `PutValue` or `Ping`.
                            send_back_queue.push_back(send_back);
                            let future = kad_sink
                                .send(message)
                                .map(move |kad_sink| {
                                    let state = (events, kad_sink, responders_tx, send_back_queue, expected_pongs);
                                    (None, state)
                                });
                            Box::new(future) as Box<_>
                        }
                        Some(EventSource::Remote(KadMsg::Ping)) => {
                            // The way the protocol was designed, there is no way to differentiate
                            // between a ping and a pong.
                            if let Some(expected_pongs) = expected_pongs.checked_sub(1) {
                                // Maybe we received a PONG, or maybe we received a PONG, no way
                                // to tell. If it was a PING and we expected a PONG, then the
                                // remote will see its PING answered only when it PONGs us.
                                let future = future::ok({
                                    let state = (events, kad_sink, responders_tx, send_back_queue, expected_pongs);
                                    let rq = KademliaIncomingRequest::PingPong;
                                    (Some(rq), state)
                                });
                                Box::new(future) as Box<_>
                            } else {
                                let future = kad_sink
                                    .send(KadMsg::Ping)
                                    .map(move |kad_sink| {
                                        let state = (events, kad_sink, responders_tx, send_back_queue, expected_pongs);
                                        let rq = KademliaIncomingRequest::PingPong;
                                        (Some(rq), state)
                                    });
                                Box::new(future) as Box<_>
                            }
                        }
                        Some(EventSource::Remote(message @ KadMsg::FindNodeRes { .. }))
                        | Some(EventSource::Remote(message @ KadMsg::GetValueRes { .. })) => {
                            // `FindNodeRes` or `GetValueRes` received on the socket.
                            // Send it back through `send_back_queue`.
                            if let Some(send_back) = send_back_queue.pop_front() {
                                let _ = send_back.send(message);
                                let future = future::ok({
                                    let state = (events, kad_sink, responders_tx, send_back_queue, expected_pongs);
                                    (None, state)
                                });
                                Box::new(future)
                            } else {
                                debug!("Remote sent a Kad response but we didn't request anything");
                                let future = future::err(IoErrorKind::InvalidData.into());
                                Box::new(future)
                            }
                        }
                        Some(EventSource::Remote(KadMsg::FindNodeReq { key, .. })) => {
                            let peer_id = match PeerId::from_bytes(key) {
                                Ok(id) => id,
                                Err(key) => {
                                    debug!("Ignoring FIND_NODE request with invalid key: {:?}", key);
                                    let future = future::err(IoError::new(IoErrorKind::InvalidData, "invalid key in FIND_NODE"));
                                    return Box::new(future);
                                }
                            };

                            let (tx, rx) = oneshot::channel();
                            let _ = responders_tx.unbounded_send(rx);
                            let future = future::ok({
                                let state = (events, kad_sink, responders_tx, send_back_queue, expected_pongs);
                                let rq = KademliaIncomingRequest::FindNode {
                                    searched: peer_id,
                                    responder: KademliaFindNodeRespond {
                                        inner: tx
                                    }
                                };
                                (Some(rq), state)
                            });

                            Box::new(future)
                        }
                        Some(EventSource::Remote(KadMsg::GetValueReq { .. })) => {
                            unimplemented!()        // FIXME:
                        }
                        Some(EventSource::Remote(KadMsg::PutValue { .. })) => {
                            unimplemented!()        // FIXME:
                        }
                    }
                }))
    }).filter_map(|val| val);

    Box::new(stream) as Box<Stream<Item = _, Error = IoError>>
}
