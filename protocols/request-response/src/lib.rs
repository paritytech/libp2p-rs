// Copyright 2020 Parity Technologies (UK) Ltd.
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

//! Generic request/response protocols.
//!
//! ## General Usage
//!
//! [`RequestResponse`] is a `NetworkBehaviour` that implements a generic
//! request/response protocol or protocol family, whereby each request is
//! sent over a new substream on a connection. `RequestResponse` is generic
//! over the actual messages being sent, which are defined in terms of a
//! [`RequestResponseCodec`]. Creating a request/response protocol thus amounts
//! to providing an implementation of this trait which can then be
//! given to [`RequestResponse::new`]. Further configuration options are
//! available via the [`RequestResponseConfig`].
//!
//! Requests are sent using [`RequestResponse::send_request`] and the
//! responses received as [`RequestResponseMessage::Response`] via
//! [`RequestResponseEvent::Message`].
//!
//! Responses are sent using [`RequestResponse::send_response`] upon
//! receiving a [`RequestResponseMessage::Request`] via
//! [`RequestResponseEvent::Message`].
//!
//! ## Protocol Families
//!
//! A single [`RequestResponse`] instance can be used with an entire
//! protocol family that share the same request and response types.
//! For that purpose, [`RequestResponseCodec::Protocol`] is typically
//! instantiated with a sum type.
//!
//! ## Limited Protocol Support
//!
//! It is possible to only support inbound or outbound requests for
//! a particular protocol. This is achieved by instantiating `RequestResponse`
//! with protocols using [`ProtocolSupport::Inbound`] or
//! [`ProtocolSupport::Outbound`]. Any subset of protocols of a protocol
//! family can be configured in this way. Such protocols will not be
//! advertised during inbound respectively outbound protocol negotiation
//! on the substreams.

pub mod codec;
pub mod handler;
pub mod throttled;

pub use codec::{RequestResponseCodec, ProtocolName};
pub use handler::ProtocolSupport;
pub use throttled::Throttled;

use futures::{
    channel::oneshot,
};
use handler::{
    RequestProtocol,
    RequestResponseHandler,
    RequestResponseHandlerEvent,
};
use libp2p_core::{
    ConnectedPoint,
    Multiaddr,
    PeerId,
    connection::ConnectionId,
};
use libp2p_swarm::{
    DialPeerCondition,
    NetworkBehaviour,
    NetworkBehaviourAction,
    NotifyHandler,
    PollParameters,
};
use smallvec::SmallVec;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    time::Duration,
    sync::{atomic::AtomicU64, Arc},
    task::{Context, Poll}
};

/// An inbound request or response.
#[derive(Debug)]
pub enum RequestResponseMessage<TRequest, TResponse, TChannelResponse = TResponse> {
    /// A request message.
    Request {
        /// The ID of this request.
        request_id: RequestId,
        /// The request message.
        request: TRequest,
        /// The channel waiting for the response.
        ///
        /// If this channel is dropped instead of being used to send a response
        /// via [`RequestResponse::send_response`], a [`RequestResponseEvent::InboundFailure`]
        /// with [`InboundFailure::ResponseOmission`] is emitted.
        channel: ResponseChannel<TChannelResponse>,
    },
    /// A response message.
    Response {
        /// The ID of the request that produced this response.
        ///
        /// See [`RequestResponse::send_request`].
        request_id: RequestId,
        /// The response message.
        response: TResponse
    },
}

/// The events emitted by a [`RequestResponse`] protocol.
#[derive(Debug)]
pub enum RequestResponseEvent<TRequest, TResponse, TChannelResponse = TResponse> {
    /// An incoming message (request or response).
    Message {
        /// The peer who sent the message.
        peer: PeerId,
        /// The incoming message.
        message: RequestResponseMessage<TRequest, TResponse, TChannelResponse>
    },
    /// An outbound request failed.
    OutboundFailure {
        /// The peer to whom the request was sent.
        peer: PeerId,
        /// The (local) ID of the failed request.
        request_id: RequestId,
        /// The error that occurred.
        error: OutboundFailure,
    },
    /// An inbound request failed.
    InboundFailure {
        /// The peer from whom the request was received.
        peer: PeerId,
        /// The ID of the failed inbound request.
        request_id: RequestId,
        /// The error that occurred.
        error: InboundFailure,
    },
    /// A response to an inbound request has been sent.
    ///
    /// When this event is received, the response has been flushed on
    /// the underlying transport connection.
    ResponseSent {
        /// The peer to whom the response was sent.
        peer: PeerId,
        /// The ID of the inbound request whose response was sent.
        request_id: RequestId,
    },
}

/// Possible failures occurring in the context of sending
/// an outbound request and receiving the response.
#[derive(Debug, Clone, PartialEq)]
pub enum OutboundFailure {
    /// The request could not be sent because a dialing attempt failed.
    DialFailure,
    /// The request timed out before a response was received.
    ///
    /// It is not known whether the request may have been
    /// received (and processed) by the remote peer.
    Timeout,
    /// The connection closed before a response was received.
    ///
    /// It is not known whether the request may have been
    /// received (and processed) by the remote peer.
    ConnectionClosed,
    /// The remote supports none of the requested protocols.
    UnsupportedProtocols,
}

/// Possible failures occurring in the context of receiving an
/// inbound request and sending a response.
#[derive(Debug, Clone, PartialEq)]
pub enum InboundFailure {
    /// The inbound request timed out, either while reading the
    /// incoming request or before a response is sent, e.g. if
    /// [`RequestResponse::send_response`] is not called in a
    /// timely manner.
    Timeout,
    /// The connection closed before a response could be send.
    ConnectionClosed,
    /// The local peer supports none of the protocols requested
    /// by the remote.
    UnsupportedProtocols,
    /// The local peer failed to respond to an inbound request
    /// due to the [`ResponseChannel`] being dropped instead of
    /// being passed to [`RequestResponse::send_response`].
    ResponseOmission,
}

/// A channel for sending a response to an inbound request.
///
/// See [`RequestResponse::send_response`].
#[derive(Debug)]
pub struct ResponseChannel<TResponse> {
    request_id: RequestId,
    peer: PeerId,
    sender: oneshot::Sender<TResponse>,
}

impl<TResponse> ResponseChannel<TResponse> {
    /// Checks whether the response channel is still open, i.e.
    /// the `RequestResponse` behaviour is still waiting for a
    /// a response to be sent via [`RequestResponse::send_response`]
    /// and this response channel.
    ///
    /// If the response channel is no longer open then the inbound
    /// request timed out waiting for the response.
    pub fn is_open(&self) -> bool {
        !self.sender.is_canceled()
    }

    /// Get the ID of the inbound request waiting for a response.
    pub(crate) fn request_id(&self) -> RequestId {
        self.request_id
    }
}

/// The ID of an inbound or outbound request.
///
/// Note: [`RequestId`]'s uniqueness is only guaranteed between two
/// inbound and likewise between two outbound requests. There is no
/// uniqueness guarantee in a set of both inbound and outbound
/// [`RequestId`]s nor in a set of inbound or outbound requests
/// originating from different [`RequestResponse`] behaviours.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct RequestId(u64);

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The configuration for a `RequestResponse` protocol.
#[derive(Debug, Clone)]
pub struct RequestResponseConfig {
    request_timeout: Duration,
    connection_keep_alive: Duration,
}

impl Default for RequestResponseConfig {
    fn default() -> Self {
        Self {
            connection_keep_alive: Duration::from_secs(10),
            request_timeout: Duration::from_secs(10),
        }
    }
}

impl RequestResponseConfig {
    /// Sets the keep-alive timeout of idle connections.
    pub fn set_connection_keep_alive(&mut self, v: Duration) -> &mut Self {
        self.connection_keep_alive = v;
        self
    }

    /// Sets the timeout for inbound and outbound requests.
    pub fn set_request_timeout(&mut self, v: Duration) -> &mut Self {
        self.request_timeout = v;
        self
    }
}

/// A request/response protocol for some message codec.
pub struct RequestResponse<TCodec>
where
    TCodec: RequestResponseCodec,
{
    /// The supported inbound protocols.
    inbound_protocols: SmallVec<[TCodec::Protocol; 2]>,
    /// The supported outbound protocols.
    outbound_protocols: SmallVec<[TCodec::Protocol; 2]>,
    /// The next (local) request ID.
    next_request_id: RequestId,
    /// The next (inbound) request ID.
    next_inbound_id: Arc<AtomicU64>,
    /// The protocol configuration.
    config: RequestResponseConfig,
    /// The protocol codec for reading and writing requests and responses.
    codec: TCodec,
    /// Pending events to return from `poll`.
    pending_events: VecDeque<
        NetworkBehaviourAction<
            RequestProtocol<TCodec>,
            RequestResponseEvent<TCodec::Request, TCodec::Response>>>,
    /// The currently connected peers, their pending outbound and inbound responses and their known,
    /// reachable addresses, if any.
    connected: HashMap<PeerId, SmallVec<[Connection; 2]>>,
    /// Externally managed addresses via `add_address` and `remove_address`.
    addresses: HashMap<PeerId, SmallVec<[Multiaddr; 6]>>,
    /// Requests that have not yet been sent and are waiting for a connection
    /// to be established.
    pending_outbound_requests: HashMap<PeerId, SmallVec<[RequestProtocol<TCodec>; 10]>>,
}

impl<TCodec> RequestResponse<TCodec>
where
    TCodec: RequestResponseCodec + Clone,
{
    /// Creates a new `RequestResponse` behaviour for the given
    /// protocols, codec and configuration.
    pub fn new<I>(codec: TCodec, protocols: I, cfg: RequestResponseConfig) -> Self
    where
        I: IntoIterator<Item = (TCodec::Protocol, ProtocolSupport)>
    {
        let mut inbound_protocols = SmallVec::new();
        let mut outbound_protocols = SmallVec::new();
        for (p, s) in protocols {
            if s.inbound() {
                inbound_protocols.push(p.clone());
            }
            if s.outbound() {
                outbound_protocols.push(p.clone());
            }
        }
        RequestResponse {
            inbound_protocols,
            outbound_protocols,
            next_request_id: RequestId(1),
            next_inbound_id: Arc::new(AtomicU64::new(1)),
            config: cfg,
            codec,
            pending_events: VecDeque::new(),
            connected: HashMap::new(),
            pending_outbound_requests: HashMap::new(),
            addresses: HashMap::new(),
        }
    }

    /// Creates a `RequestResponse` which limits requests per peer.
    ///
    /// The behaviour is wrapped in [`Throttled`] and detects the limits
    /// per peer at runtime which are then enforced.
    pub fn throttled<I>(c: TCodec, protos: I, cfg: RequestResponseConfig) -> Throttled<TCodec>
    where
        I: IntoIterator<Item = (TCodec::Protocol, ProtocolSupport)>,
        TCodec: Send,
        TCodec::Protocol: Sync
    {
        Throttled::new(c, protos, cfg)
    }

    /// Initiates sending a request.
    ///
    /// If the targeted peer is currently not connected, a dialing
    /// attempt is initiated and the request is sent as soon as a
    /// connection is established.
    ///
    /// > **Note**: In order for such a dialing attempt to succeed,
    /// > the `RequestResonse` protocol must either be embedded
    /// > in another `NetworkBehaviour` that provides peer and
    /// > address discovery, or known addresses of peers must be
    /// > managed via [`RequestResponse::add_address`] and
    /// > [`RequestResponse::remove_address`].
    pub fn send_request(&mut self, peer: &PeerId, request: TCodec::Request) -> RequestId {
        let request_id = self.next_request_id();
        let request = RequestProtocol {
            request_id,
            codec: self.codec.clone(),
            protocols: self.outbound_protocols.clone(),
            request,
        };

        if let Some(request) = self.try_send_request(peer, request) {
            self.pending_events.push_back(NetworkBehaviourAction::DialPeer {
                peer_id: *peer,
                condition: DialPeerCondition::Disconnected,
            });
            self.pending_outbound_requests.entry(*peer).or_default().push(request);
        }

        request_id
    }

    /// Initiates sending a response to an inbound request.
    ///
    /// If the [`ResponseChannel`] is already closed due to a timeout or the
    /// connection being closed, the response is returned as an `Err` for
    /// further handling. Once the response has been successfully sent on the
    /// corresponding connection, [`RequestResponseEvent::ResponseSent`] is
    /// emitted. In all other cases [`RequestResponseEvent::InboundFailure`]
    /// will be or has been emitted.
    ///
    /// The provided `ResponseChannel` is obtained from an inbound
    /// [`RequestResponseMessage::Request`].
    pub fn send_response(&mut self, ch: ResponseChannel<TCodec::Response>, rs: TCodec::Response)
        -> Result<(), TCodec::Response>
    {
        ch.sender.send(rs)
    }

    /// Adds a known address for a peer that can be used for
    /// dialing attempts by the `Swarm`, i.e. is returned
    /// by [`NetworkBehaviour::addresses_of_peer`].
    ///
    /// Addresses added in this way are only removed by `remove_address`.
    pub fn add_address(&mut self, peer: &PeerId, address: Multiaddr) {
        self.addresses.entry(*peer).or_default().push(address);
    }

    /// Removes an address of a peer previously added via `add_address`.
    pub fn remove_address(&mut self, peer: &PeerId, address: &Multiaddr) {
        let mut last = false;
        if let Some(addresses) = self.addresses.get_mut(peer) {
            addresses.retain(|a| a != address);
            last = addresses.is_empty();
        }
        if last {
            self.addresses.remove(peer);
        }
    }

    /// Checks whether a peer is currently connected.
    pub fn is_connected(&self, peer: &PeerId) -> bool {
        if let Some(connections) = self.connected.get(peer) {
            !connections.is_empty()
        } else {
            false
        }
    }

    /// Checks whether an outbound request to the peer with the provided
    /// [`PeerId`] initiated by [`RequestResponse::send_request`] is still
    /// pending, i.e. waiting for a response.
    pub fn is_pending_outbound(&self, peer: &PeerId, request_id: &RequestId) -> bool {
        // Check if request is already sent on established connection.
        let est_conn = self.connected.get(peer)
            .map(|cs| cs.iter().any(|c| c.pending_inbound_responses.contains(request_id)))
            .unwrap_or(false);
        // Check if request is still pending to be sent.
        let pen_conn = self.pending_outbound_requests.get(peer)
            .map(|rps| rps.iter().any(|rp| {rp.request_id == *request_id}))
            .unwrap_or(false);

        est_conn || pen_conn
    }

    /// Checks whether an inbound request from the peer with the provided
    /// [`PeerId`] is still pending, i.e. waiting for a response by the local
    /// node through [`RequestResponse::send_response`].
    pub fn is_pending_inbound(&self, peer: &PeerId, request_id: &RequestId) -> bool {
        self.connected.get(peer)
            .map(|cs| cs.iter().any(|c| c.pending_outbound_responses.contains(request_id)))
            .unwrap_or(false)
    }

    /// Returns the next request ID.
    fn next_request_id(&mut self) -> RequestId {
        let request_id = self.next_request_id;
        self.next_request_id.0 += 1;
        request_id
    }

    /// Tries to send a request by queueing an appropriate event to be
    /// emitted to the `Swarm`. If the peer is not currently connected,
    /// the given request is return unchanged.
    fn try_send_request(&mut self, peer: &PeerId, request: RequestProtocol<TCodec>)
        -> Option<RequestProtocol<TCodec>>
    {
        if let Some(connections) = self.connected.get_mut(peer) {
            if connections.is_empty() {
                return Some(request)
            }
            let ix = (request.request_id.0 as usize) % connections.len();
            let conn = &mut connections[ix];
            conn.pending_inbound_responses.insert(request.request_id);
            self.pending_events.push_back(NetworkBehaviourAction::NotifyHandler {
                peer_id: *peer,
                handler: NotifyHandler::One(conn.id),
                event: request
            });
            None
        } else {
            Some(request)
        }
    }

    /// Remove pending outbound response for the given peer and connection.
    ///
    /// Returns `true` if the provided connection to the given peer is still
    /// alive and the [`RequestId`] was previously present and is now removed.
    /// Returns `false` otherwise.
    fn remove_pending_outbound_response(
        &mut self,
        peer: &PeerId,
        connection: ConnectionId,
        request: RequestId,
    ) -> bool {
        self.get_connection_mut(peer, connection)
            .map(|c| c.pending_outbound_responses.remove(&request))
            .unwrap_or(false)
    }

    /// Remove pending inbound response for the given peer and connection.
    ///
    /// Returns `true` if the provided connection to the given peer is still
    /// alive and the [`RequestId`] was previously present and is now removed.
    /// Returns `false` otherwise.
    fn remove_pending_inbound_response(
        &mut self,
        peer: &PeerId,
        connection: ConnectionId,
        request: &RequestId,
    ) -> bool {
        self.get_connection_mut(peer, connection)
            .map(|c| c.pending_inbound_responses.remove(request))
            .unwrap_or(false)
    }

    /// Returns a mutable reference to the connection in `self.connected`
    /// corresponding to the given [`PeerId`] and [`ConnectionId`].
    fn get_connection_mut(
        &mut self,
        peer: &PeerId,
        connection: ConnectionId,
    ) -> Option<&mut Connection> {
        self.connected.get_mut(peer).and_then(|connections| {
            connections.iter_mut().find(|c| c.id == connection)
        })
    }
}

impl<TCodec> NetworkBehaviour for RequestResponse<TCodec>
where
    TCodec: RequestResponseCodec + Send + Clone + 'static,
{
    type ProtocolsHandler = RequestResponseHandler<TCodec>;
    type OutEvent = RequestResponseEvent<TCodec::Request, TCodec::Response>;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        RequestResponseHandler::new(
            self.inbound_protocols.clone(),
            self.codec.clone(),
            self.config.connection_keep_alive,
            self.config.request_timeout,
            self.next_inbound_id.clone()
        )
    }

    fn addresses_of_peer(&mut self, peer: &PeerId) -> Vec<Multiaddr> {
        let mut addresses = Vec::new();
        if let Some(connections) = self.connected.get(peer) {
            addresses.extend(connections.iter().filter_map(|c| c.address.clone()))
        }
        if let Some(more) = self.addresses.get(peer) {
            addresses.extend(more.into_iter().cloned());
        }
        addresses
    }

    fn inject_connected(&mut self, peer: &PeerId) {
        if let Some(pending) = self.pending_outbound_requests.remove(peer) {
            for request in pending {
                let request = self.try_send_request(peer, request);
                assert!(request.is_none());
            }
        }
    }

    fn inject_connection_established(&mut self, peer: &PeerId, conn: &ConnectionId, endpoint: &ConnectedPoint) {
        let address = match endpoint {
            ConnectedPoint::Dialer { address } => Some(address.clone()),
            ConnectedPoint::Listener { .. } => None
        };
        self.connected.entry(*peer)
            .or_default()
            .push(Connection::new(*conn, address));
    }

    fn inject_connection_closed(&mut self, peer_id: &PeerId, conn: &ConnectionId, _: &ConnectedPoint) {
        let connections = self.connected.get_mut(peer_id)
            .expect("Expected some established connection to peer before closing.");

        let connection = connections.iter()
            .position(|c| &c.id == conn)
            .map(|p: usize| connections.remove(p))
            .expect("Expected connection to be established before closing.");

        if connections.is_empty() {
            self.connected.remove(peer_id);
        }

        for request_id in connection.pending_outbound_responses {
            self.pending_events.push_back(NetworkBehaviourAction::GenerateEvent(
                RequestResponseEvent::InboundFailure {
                    peer: *peer_id,
                    request_id,
                    error: InboundFailure::ConnectionClosed
                }
            ));

        }

        for request_id in connection.pending_inbound_responses {
            self.pending_events.push_back(NetworkBehaviourAction::GenerateEvent(
                RequestResponseEvent::OutboundFailure {
                    peer: *peer_id,
                    request_id,
                    error: OutboundFailure::ConnectionClosed
                }
            ));
        }
    }

    fn inject_disconnected(&mut self, peer: &PeerId) {
        self.connected.remove(peer);
    }

    fn inject_dial_failure(&mut self, peer: &PeerId) {
        // If there are pending outgoing requests when a dial failure occurs,
        // it is implied that we are not connected to the peer, since pending
        // outgoing requests are drained when a connection is established and
        // only created when a peer is not connected when a request is made.
        // Thus these requests must be considered failed, even if there is
        // another, concurrent dialing attempt ongoing.
        if let Some(pending) = self.pending_outbound_requests.remove(peer) {
            for request in pending {
                self.pending_events.push_back(NetworkBehaviourAction::GenerateEvent(
                    RequestResponseEvent::OutboundFailure {
                        peer: *peer,
                        request_id: request.request_id,
                        error: OutboundFailure::DialFailure
                    }
                ));
            }
        }
    }

    fn inject_event(
        &mut self,
        peer: PeerId,
        connection: ConnectionId,
        event: RequestResponseHandlerEvent<TCodec>,
    ) {
        match event {
            RequestResponseHandlerEvent::Response { request_id, response } => {
                let removed = self.remove_pending_inbound_response(&peer, connection, &request_id);
                debug_assert!(
                    removed,
                    "Expect request_id to be pending before receiving response.",
                );

                let message = RequestResponseMessage::Response { request_id, response };
                self.pending_events.push_back(
                    NetworkBehaviourAction::GenerateEvent(
                        RequestResponseEvent::Message { peer, message }));
            }
            RequestResponseHandlerEvent::Request { request_id, request, sender } => {
                let channel = ResponseChannel { request_id, peer, sender };
                let message = RequestResponseMessage::Request { request_id, request, channel };
                self.pending_events.push_back(NetworkBehaviourAction::GenerateEvent(
                    RequestResponseEvent::Message { peer, message }
                ));

                match self.get_connection_mut(&peer, connection) {
                    Some(connection) => {
                        let inserted = connection.pending_outbound_responses.insert(request_id);
                        debug_assert!(inserted, "Expect id of new request to be unknown.");
                    },
                    // Connection closed after `RequestResponseEvent::Request` has been emitted.
                    None => {
                        self.pending_events.push_back(NetworkBehaviourAction::GenerateEvent(
                            RequestResponseEvent::InboundFailure {
                                peer,
                                request_id,
                                error: InboundFailure::ConnectionClosed
                            }
                        ));
                    }
                }
            }
            RequestResponseHandlerEvent::ResponseSent(request_id) => {
                let removed = self.remove_pending_outbound_response(&peer, connection, request_id);
                debug_assert!(removed, "Expect request_id to be pending before response is sent.");

                self.pending_events.push_back(
                    NetworkBehaviourAction::GenerateEvent(
                        RequestResponseEvent::ResponseSent { peer, request_id }));
            }
            RequestResponseHandlerEvent::ResponseOmission(request_id) => {
                let removed = self.remove_pending_outbound_response(&peer, connection, request_id);
                debug_assert!(
                    removed,
                    "Expect request_id to be pending before response is omitted.",
                );

                self.pending_events.push_back(
                    NetworkBehaviourAction::GenerateEvent(
                        RequestResponseEvent::InboundFailure {
                            peer,
                            request_id,
                            error: InboundFailure::ResponseOmission
                        }));
            }
            RequestResponseHandlerEvent::OutboundTimeout(request_id) => {
                let removed = self.remove_pending_inbound_response(&peer, connection, &request_id);
                debug_assert!(removed, "Expect request_id to be pending before request times out.");

                self.pending_events.push_back(
                    NetworkBehaviourAction::GenerateEvent(
                        RequestResponseEvent::OutboundFailure {
                            peer,
                            request_id,
                            error: OutboundFailure::Timeout,
                        }));
            }
            RequestResponseHandlerEvent::InboundTimeout(request_id) => {
                // Note: `RequestResponseHandlerEvent::InboundTimeout` is emitted both for timing
                // out to receive the request and for timing out sending the response. In the former
                // case the request is never added to `pending_outbound_responses` and thus one can
                // not assert the request_id to be present before removing it.
                self.remove_pending_outbound_response(&peer, connection, request_id);

                self.pending_events.push_back(
                    NetworkBehaviourAction::GenerateEvent(
                        RequestResponseEvent::InboundFailure {
                            peer,
                            request_id,
                            error: InboundFailure::Timeout,
                        }));
            }
            RequestResponseHandlerEvent::OutboundUnsupportedProtocols(request_id) => {
                let removed = self.remove_pending_inbound_response(&peer, connection, &request_id);
                debug_assert!(
                    removed,
                    "Expect request_id to be pending before failing to connect.",
                );

                self.pending_events.push_back(
                    NetworkBehaviourAction::GenerateEvent(
                        RequestResponseEvent::OutboundFailure {
                            peer,
                            request_id,
                            error: OutboundFailure::UnsupportedProtocols,
                        }));
            }
            RequestResponseHandlerEvent::InboundUnsupportedProtocols(request_id) => {
                // Note: No need to call `self.remove_pending_outbound_response`,
                // `RequestResponseHandlerEvent::Request` was never emitted for this request and
                // thus request was never added to `pending_outbound_responses`.
                self.pending_events.push_back(
                    NetworkBehaviourAction::GenerateEvent(
                        RequestResponseEvent::InboundFailure {
                            peer,
                            request_id,
                            error: InboundFailure::UnsupportedProtocols,
                        }));
            }
        }
    }

    fn poll(&mut self, _: &mut Context<'_>, _: &mut impl PollParameters)
        -> Poll<NetworkBehaviourAction<
            RequestProtocol<TCodec>,
            RequestResponseEvent<TCodec::Request, TCodec::Response>
        >>
    {
        if let Some(ev) = self.pending_events.pop_front() {
            return Poll::Ready(ev);
        } else if self.pending_events.capacity() > EMPTY_QUEUE_SHRINK_THRESHOLD {
            self.pending_events.shrink_to_fit();
        }

        Poll::Pending
    }
}

/// Internal threshold for when to shrink the capacity
/// of empty queues. If the capacity of an empty queue
/// exceeds this threshold, the associated memory is
/// released.
const EMPTY_QUEUE_SHRINK_THRESHOLD: usize = 100;

/// Internal information tracked for an established connection.
struct Connection {
    id: ConnectionId,
    address: Option<Multiaddr>,
    /// Pending outbound responses where corresponding inbound requests have
    /// been received on this connection and emitted via `poll` but have not yet
    /// been answered.
    pending_outbound_responses: HashSet<RequestId>,
    /// Pending inbound responses for previously sent requests on this
    /// connection.
    pending_inbound_responses: HashSet<RequestId>
}

impl Connection {
    fn new(id: ConnectionId, address: Option<Multiaddr>) -> Self {
        Self {
            id,
            address,
            pending_outbound_responses: Default::default(),
            pending_inbound_responses: Default::default(),
        }
    }
}
