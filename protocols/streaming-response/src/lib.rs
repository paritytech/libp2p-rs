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

//! Generic streaming request/response protocol.
//!
//! ## General Usage
//!
//! [`StreamingResponse`] is a `NetworkBehaviour` that implements a generic
//! request/response protocol or protocol family, whereby each request is sent
//! over a new substream on a connection. `StreamingResponse` is generic over the
//! actual messages being sent, which are defined in terms of a [`Codec`].
//! Creating a request/response protocol thus amounts to providing an
//! implementation of this trait which can then be given to
//! [`StreamingResponseConfig::new`], and finally to [`StreamingResponse::new`].
//! Further configuration options are available said config. For convenience, a
//! default implementation for [`StreamingResponse`] is provided.
//!
//! Requests are sent using [`StreamingResponse::request`] and the
//! responses received as [`StreamingResponseEvent::ResponseReceived`].
//!
//! Individual responses are sent using [`StreamingResponse::respond`]
//! upon receiving a [`StreamingResponseEvent::Request`]. The response stream can
//! be finalized by calling [`StreamingResponse::finish_response`],
//! which will result in the emission of an
//! [`StreamingResponseEvent::ResponseFinished`] on the requester's side. After
//! that, the response channel can't be used anymore.
//!
//! An ongoing request is cancelled if either the peer disconnects, or a
//! [`StreamingResponseMessage::CancelRequest`] message is sent.
//!
//! ## Protocol Families
//!
//! A single [`StreamingResponse`] instance can be used with an entire
//! protocol family that share the same request and response types. For that
//! purpose, [`Codec`] is typically instantiated with a sum type.
//!
//! ## Differences to `libp2p::request_response`
//!
//! The ergonomics of this behaviour are inspired by the
//! `libp2p::request_response` implementation. However, it enables the exchange
//! of multiple response frames per request. Currently, it does neither support
//! timeouts nor signalling of successful commits of outbound messages to the
//! underlying transport mechanism. Sending requests and/or responses is a
//! fire-and-forget action. Only if the remote peer is disconnected, consumer
//! code will be notified through [`CancellationReason::PeerDisconnected`] via
//! [`StreamingResponseEvent::ResponseFinished`].
//! Another notable difference is that this behaviour won't initiate any dialing
//! attempts, thus this behaviour needs to be wrapped inside another behaviour
//! providing dialing functionality.
//!
//! ## Parallelism and response frame ordering
//!
//! Internally, this behaviour uses `libp2p::swarm::OneShotHandler`, where for
//! each request a new substream is created. Meaning, that for each request and
//! all subsequent responses, this substream is used. Given the asynchronous
//! nature of the internals of `libp2p`, smaller response frames might make it to
//! the recipient earlier than bigger ones. Each response frame includes a
//! monotonic sequence number, which can be used for ordering purposes. However,
//! users can also set [`StreamingResponseConfig::ordered_outgoing`] flag, which
//! will commit individual responses sequentially to the underlying transport
//! mechanism.

use libp2p_core::{connection::ConnectionId, ConnectedPoint, Multiaddr, PeerId};
use libp2p_swarm::{
    NetworkBehaviour, NetworkBehaviourAction, NotifyHandler, OneShotHandler, OneShotHandlerConfig,
    PollParameters, SubstreamProtocol,
};
use protocol::{RequestId, StreamingResponseMessage};
use std::task::{Context, Poll};
use std::{
    collections::{BTreeMap, VecDeque},
    convert::Into,
};
use thiserror::Error;

mod protocol;

pub use protocol::{Codec, SequenceNo, StreamingResponseConfig};

#[derive(Error, Debug)]
pub enum StreamingResponseError {
    #[error("Channel closed")]
    ChannelClosed,
}
pub(crate) type Result<T> = std::result::Result<T, StreamingResponseError>;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
/// Opaque struct identifying a response stream.
pub struct ChannelId {
    peer_id: PeerId,
    con: ConnectionId,
    peer_request_id: RequestId,
}

impl ChannelId {
    fn new(peer_id: PeerId, con: ConnectionId, peer_request_id: RequestId) -> Self {
        Self {
            peer_id,
            con,
            peer_request_id,
        }
    }
    pub fn peer(&self) -> PeerId {
        self.peer_id
    }
}

impl Into<(PeerId, ConnectionId)> for ChannelId {
    fn into(self) -> (PeerId, ConnectionId) {
        (self.peer_id, self.con)
    }
}

#[derive(Debug)]
pub enum CancellationReason {
    PeerDisconnected,
    PeerCancelled,
}

#[derive(Debug)]
pub enum StreamingResponseEvent<TCodec: Codec> {
    /// A new request has been received from a remote peer
    ReceivedRequest {
        /// Identifier for this response channel
        channel_id: ChannelId,
        /// Request payload
        payload: TCodec::Request,
    },
    /// An ongoing request has been cancelled, either because the peer
    /// disconnected or a `CancelRequest` message was received.
    CancelledRequest {
        /// Identifier for this response channel
        channel_id: ChannelId,
        /// Reason for the cancellation
        reason: CancellationReason,
    },
    /// A response frame for an ongoing request has been received.
    ResponseReceived {
        /// Local requestId identifying the response stream
        request_id: RequestId,
        /// Monotonically increasing sequence number
        sequence_no: SequenceNo,
        /// Response payload
        payload: TCodec::Response,
    },
    /// An ongoing response stream has been finalized, either because the peer
    /// disconnected or a `ResponseEnd` message was received.
    ResponseFinished {
        /// Local requestId identifying the response stream
        request_id: RequestId,
        /// Monotonically increasing sequence number
        sequence_no: SequenceNo,
    },
}

#[derive(Default)]
pub struct StreamingResponse<TCodec: Codec> {
    config: StreamingResponseConfig<TCodec>,
    /// Internal queue for events to be emitted to `libp2p::Swarm`
    events: VecDeque<
        NetworkBehaviourAction<StreamingResponseMessage<TCodec>, StreamingResponseEvent<TCodec>>,
    >,
    /// Request ID for the next outgoing request
    next_request_id: RequestId,
    /// Map from (PeerId, ConnectionId) tuple to a map from RequestId to the last
    /// sequence_no
    open_channels: BTreeMap<(PeerId, ConnectionId), BTreeMap<RequestId, SequenceNo>>,
}

impl<TCodec> StreamingResponse<TCodec>
where
    TCodec: Codec,
{
    pub fn new(config: StreamingResponseConfig<TCodec>) -> Self {
        Self {
            config,
            open_channels: Default::default(),
            events: Default::default(),
            next_request_id: RequestId(0),
        }
    }

    /// Initiates sending a request. The caller needs to make sure that the
    /// target `peer_id` is already connected. This behaviour won't initiate any
    /// dialing attempts.
    /// A `RequestId` is returned to identify future responses, or any failures.
    pub fn request(&mut self, peer_id: PeerId, request: TCodec::Request) -> RequestId {
        let id = self.next_request_id();
        let event = StreamingResponseMessage::Request {
            id,
            payload: request,
        };
        self.events
            .push_back(NetworkBehaviourAction::NotifyHandler {
                event,
                peer_id,
                // Can't name a specific peer connection here.
                handler: NotifyHandler::Any,
            });
        id
    }

    /// Initiates sending a response given a `ChannelIdentifier`. This function
    /// will return an error, if the channel is not intact any more.
    pub fn respond(&mut self, id: ChannelId, payload: TCodec::Response) -> Result<()> {
        let x = self
            .open_channels
            .get_mut(&(id.peer_id, id.con))
            .ok_or(StreamingResponseError::ChannelClosed)?;
        let seq_no = x
            .get_mut(&id.peer_request_id)
            .ok_or(StreamingResponseError::ChannelClosed)?;
        seq_no.increment();

        self.events
            .push_back(NetworkBehaviourAction::NotifyHandler {
                handler: NotifyHandler::One(id.con),
                peer_id: id.peer_id,
                event: StreamingResponseMessage::Response {
                    id: id.peer_request_id,
                    payload,
                    seq_no: *seq_no,
                },
            });
        Ok(())
    }

    /// Finalize a response stream.
    pub fn finish_response(&mut self, id: ChannelId) -> Result<()> {
        let x = self
            .open_channels
            .get_mut(&(id.peer_id, id.con))
            .ok_or(StreamingResponseError::ChannelClosed)?;
        let mut seq_no = x
            .remove(&id.peer_request_id)
            .ok_or(StreamingResponseError::ChannelClosed)?;
        // Clean map if there are no other ongoing requests
        if x.is_empty() {
            let _ = self.open_channels.remove(&(id.peer_id, id.con));
        }
        seq_no.increment();

        self.events
            .push_back(NetworkBehaviourAction::NotifyHandler {
                handler: NotifyHandler::One(id.con),
                peer_id: id.peer_id,
                event: StreamingResponseMessage::ResponseEnd {
                    id: id.peer_request_id,
                    seq_no,
                },
            });
        Ok(())
    }

    /// Send a response and finalize the stream. This is just a convenient
    /// method.
    pub fn respond_final(&mut self, id: ChannelId, payload: TCodec::Response) -> Result<()> {
        self.respond(id.clone(), payload)?;
        self.finish_response(id)
    }

    fn next_request_id(&mut self) -> RequestId {
        let r = self.next_request_id;
        self.next_request_id.0 += 1;
        r
    }
}

impl<TCodec> NetworkBehaviour for StreamingResponse<TCodec>
where
    TCodec: Codec + Send + Clone + 'static,
    TCodec::Request: Send + 'static,
    TCodec::Response: Send + 'static,
{
    type ProtocolsHandler = OneShotHandler<
        StreamingResponseConfig<TCodec>,
        StreamingResponseMessage<TCodec>,
        HandlerEvent<TCodec>,
    >;
    type OutEvent = StreamingResponseEvent<TCodec>;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        // This effectively serializes all requests per handler (thus
        // per response stream)
        let max_dial_negotiated = if self.config.ordered_outgoing { 1 } else { 8 };
        OneShotHandler::new(
            SubstreamProtocol::new(Default::default(), ()),
            OneShotHandlerConfig {
                max_dial_negotiated,
                ..Default::default()
            },
        )
    }

    fn addresses_of_peer(&mut self, _: &PeerId) -> Vec<Multiaddr> {
        Vec::new()
    }

    // No bookkeeping done here.
    fn inject_connection_established(&mut self, _: &PeerId, _: &ConnectionId, _: &ConnectedPoint) {}
    fn inject_connected(&mut self, _: &PeerId) {}

    fn inject_connection_closed(
        &mut self,
        peer_id: &PeerId,
        con_id: &ConnectionId,
        _: &ConnectedPoint,
    ) {
        // remove any pending requests from the just disconnected (PeerId,
        // ConnectionId)
        if let Some(c) = self.open_channels.remove(&(*peer_id, *con_id)) {
            for (id, _) in c {
                // No need to send `ResponseEnd` to the remote peer, as the
                // connection is already closed
                self.events.push_back(NetworkBehaviourAction::GenerateEvent(
                    StreamingResponseEvent::CancelledRequest {
                        channel_id: ChannelId::new(*peer_id, *con_id, id),
                        reason: CancellationReason::PeerDisconnected,
                    },
                ));
            }
        }
    }

    fn inject_disconnected(&mut self, _: &PeerId) {
        // Handled by `inject_connection_closed`
    }

    fn inject_event(&mut self, peer: PeerId, con_id: ConnectionId, msg: HandlerEvent<TCodec>) {
        use HandlerEvent::*;
        let ev = match msg {
            Rx(StreamingResponseMessage::Request { id, payload }) => {
                let channel_id = ChannelId::new(peer, con_id, id);

                self.open_channels
                    .entry((peer, con_id))
                    .or_insert_with(BTreeMap::new)
                    .insert(id, SequenceNo::default());

                StreamingResponseEvent::ReceivedRequest {
                    payload,
                    channel_id,
                }
            }
            Rx(StreamingResponseMessage::CancelRequest { id }) => {
                let channel_id = ChannelId::new(peer, con_id, id);
                if let Some(requests_per_peer) =
                    self.open_channels.get_mut(&channel_id.clone().into())
                {
                    if let Some(mut seq_no) = requests_per_peer.remove(&channel_id.peer_request_id)
                    {
                        // Acknowledge end of stream to peer
                        seq_no.increment();
                        self.events
                            .push_back(NetworkBehaviourAction::NotifyHandler {
                                peer_id: peer,
                                handler: NotifyHandler::One(con_id),
                                event: StreamingResponseMessage::ResponseEnd { id, seq_no },
                            });
                        // Cleanup if this was the only request from `peer`
                        if requests_per_peer.is_empty() {
                            self.open_channels.remove(&channel_id.clone().into());
                        }
                        StreamingResponseEvent::CancelledRequest {
                            channel_id,
                            reason: CancellationReason::PeerCancelled,
                        }
                    } else {
                        // Peer has other ongoing requests, but not with this
                        // request_id.
                        return;
                    }
                } else {
                    // No record of this request, discard.
                    return;
                }
            }
            Rx(StreamingResponseMessage::Response {
                id,
                payload,
                seq_no,
            }) => StreamingResponseEvent::ResponseReceived {
                request_id: id,
                sequence_no: seq_no,
                payload,
            },
            Rx(StreamingResponseMessage::ResponseEnd { seq_no, id }) => {
                StreamingResponseEvent::ResponseFinished {
                    sequence_no: seq_no,
                    request_id: id,
                }
            }
            Tx => {
                return;
            }
        };
        self.events
            .push_back(NetworkBehaviourAction::GenerateEvent(ev));
    }

    fn poll(
        &mut self,
        _: &mut Context,
        _: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<StreamingResponseMessage<TCodec>, Self::OutEvent>> {
        if let Some(event) = self.events.pop_front() {
            Poll::Ready(event)
        } else {
            Poll::Pending
        }
    }
}

/// Transmission between the `OneShotHandler` and `StreamingResponse`.
pub enum HandlerEvent<TCodec: Codec> {
    /// We received a `Message` from a remote.
    Rx(StreamingResponseMessage<TCodec>),
    /// We successfully sent a `Message`.
    Tx,
}

impl<TCodec> From<StreamingResponseMessage<TCodec>> for HandlerEvent<TCodec>
where
    TCodec: Codec,
{
    fn from(message: StreamingResponseMessage<TCodec>) -> Self {
        Self::Rx(message)
    }
}

impl<TCodec> From<()> for HandlerEvent<TCodec>
where
    TCodec: Codec,
{
    fn from(_: ()) -> Self {
        Self::Tx
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p_swarm::AddressRecord;
    use serde::{Deserialize, Serialize};
    use std::sync::{Arc, Mutex};
    struct DummySwarm {
        peer_id: PeerId,
        behaviour: Arc<Mutex<StreamingResponse<TestCodec>>>,
        connections: BTreeMap<PeerId, Arc<Mutex<StreamingResponse<TestCodec>>>>,
    }
    impl DummySwarm {
        fn new() -> Self {
            Self {
                peer_id: PeerId::random(),
                behaviour: Arc::new(Mutex::new(StreamingResponse::<TestCodec>::new(
                    Default::default(),
                ))),
                connections: Default::default(),
            }
        }
        fn peer_id(&self) -> &PeerId {
            &self.peer_id
        }
        fn dial(&mut self, other: &mut DummySwarm) {
            self.connections
                .insert(*other.peer_id(), other.behaviour.clone());
            other
                .connections
                .insert(*self.peer_id(), self.behaviour.clone());
        }
        fn request(&self, peer_id: PeerId, request: TestRequest) -> RequestId {
            self.behaviour.lock().unwrap().request(peer_id, request)
        }
        fn respond(&self, cid: ChannelId, response: TestResponse) -> Result<()> {
            self.behaviour.lock().unwrap().respond(cid, response)
        }
        fn finish_response(&self, cid: ChannelId) -> Result<()> {
            self.behaviour.lock().unwrap().finish_response(cid)
        }
        fn poll_until_pending(&self) -> Vec<StreamingResponseEvent<TestCodec>> {
            let waker = futures::task::noop_waker();
            let mut ctx = Context::from_waker(&waker);
            let mut me = self.behaviour.lock().unwrap();
            let mut events = vec![];
            while let Poll::Ready(e) = me.poll(&mut ctx, &mut DummyPollParameters) {
                match e {
                    NetworkBehaviourAction::NotifyHandler { peer_id, event, .. } => {
                        if let Some(other) = self.connections.get(&peer_id) {
                            let mut other = other.lock().unwrap();
                            other.inject_event(
                                *self.peer_id(),
                                ConnectionId::new(0),
                                HandlerEvent::Rx(event),
                            );
                        }
                    }
                    NetworkBehaviourAction::GenerateEvent(event) => events.push(event),
                    m => panic!(format!("Unexpected event {:?}", m)),
                }
            }
            events
        }
    }
    #[derive(Clone, Debug)]
    struct TestCodec;
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    struct TestRequest {
        initial_count: u64,
    }
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    struct TestResponse {
        counter: u64,
    }
    impl Codec for TestCodec {
        type Request = TestRequest;
        type Response = TestResponse;

        fn protocol_info() -> &'static [u8] {
            b"/test"
        }
    }
    struct DummyPollParameters;
    impl PollParameters for DummyPollParameters {
        type SupportedProtocolsIter = std::iter::Empty<Vec<u8>>;
        type ListenedAddressesIter = std::iter::Empty<Multiaddr>;
        type ExternalAddressesIter = std::iter::Empty<AddressRecord>;
        fn supported_protocols(&self) -> Self::SupportedProtocolsIter {
            unimplemented!()
        }
        fn listened_addresses(&self) -> Self::ListenedAddressesIter {
            unimplemented!()
        }
        fn external_addresses(&self) -> Self::ExternalAddressesIter {
            unimplemented!()
        }
        fn local_peer_id(&self) -> &PeerId {
            unimplemented!()
        }
    }
    //
    #[test]
    fn smoke() {
        let mut a = DummySwarm::new();
        let mut b = DummySwarm::new();
        // Setup connection
        a.dial(&mut b);

        // send request
        let request = TestRequest { initial_count: 42 };
        let test_request_id = a.request(*b.peer_id(), request.clone());
        assert!(a.poll_until_pending().is_empty());
        let channel_id = if let StreamingResponseEvent::ReceivedRequest {
            channel_id,
            payload,
        } = b.poll_until_pending().first().unwrap()
        {
            assert_eq!(*payload, request);
            assert_eq!(channel_id.peer_request_id, test_request_id);
            assert_eq!(channel_id.peer_id, *a.peer_id());
            channel_id
        } else {
            panic!()
        }
        .clone();

        // send response
        {
            let response = TestResponse { counter: 43 };
            b.respond(channel_id.clone(), response.clone()).unwrap();
            assert!(b.poll_until_pending().is_empty());
            if let StreamingResponseEvent::ResponseReceived {
                payload,
                request_id,
                sequence_no,
            } = a.poll_until_pending().first().unwrap()
            {
                assert_eq!(sequence_no.0, 1);
                assert_eq!(*request_id, test_request_id);
                assert_eq!(*payload, response);
            } else {
                panic!()
            }
        }

        // send another response
        {
            let response = TestResponse { counter: 44 };
            b.respond(channel_id, response.clone()).unwrap();
            assert!(b.poll_until_pending().is_empty());
            if let StreamingResponseEvent::ResponseReceived {
                payload,
                request_id,
                sequence_no,
            } = a.poll_until_pending().first().unwrap()
            {
                assert_eq!(sequence_no.0, 2);
                assert_eq!(*request_id, test_request_id);
                assert_eq!(*payload, response);
            } else {
                panic!()
            }
        }
    }

    #[test]
    fn two_parallel_requests_from_the_same_peer() {
        let mut a = DummySwarm::new();
        let mut b = DummySwarm::new();
        // Setup connection
        a.dial(&mut b);

        // send request 1
        let request = TestRequest { initial_count: 42 };
        let request_id_1 = a.request(*b.peer_id(), request.clone());
        assert!(a.poll_until_pending().is_empty());
        let channel_id_1 = if let StreamingResponseEvent::ReceivedRequest {
            channel_id,
            payload,
        } = b.poll_until_pending().first().unwrap()
        {
            assert_eq!(*payload, request);
            assert_eq!(channel_id.peer_request_id, request_id_1);
            assert_eq!(channel_id.peer_id, *a.peer_id());
            channel_id
        } else {
            panic!()
        }
        .clone();

        // send request 2
        let request_2 = TestRequest { initial_count: 84 };
        let request_id_2 = a.request(*b.peer_id(), request_2.clone());
        assert!(a.poll_until_pending().is_empty());
        let channel_id_2 = if let StreamingResponseEvent::ReceivedRequest {
            channel_id,
            payload,
        } = b.poll_until_pending().first().unwrap()
        {
            assert_eq!(*payload, request_2);
            assert_eq!(channel_id.peer_request_id, request_id_2);
            assert_eq!(channel_id.peer_id, *a.peer_id());
            channel_id
        } else {
            panic!()
        }
        .clone();

        // send response for request 1
        {
            let response = TestResponse { counter: 43 };
            b.respond(channel_id_1.clone(), response.clone()).unwrap();
            assert!(b.poll_until_pending().is_empty());
            if let StreamingResponseEvent::ResponseReceived {
                payload,
                request_id,
                sequence_no,
            } = a.poll_until_pending().first().unwrap()
            {
                assert_eq!(sequence_no.0, 1);
                assert_eq!(*request_id, request_id_1);
                assert_eq!(*payload, response);
            } else {
                panic!()
            }
        }

        // finish request 1
        {
            b.finish_response(channel_id_1.clone()).unwrap();
            assert!(b.poll_until_pending().is_empty());
            if let StreamingResponseEvent::ResponseFinished {
                request_id,
                sequence_no,
            } = a.poll_until_pending().first().unwrap()
            {
                assert_eq!(sequence_no.0, 2);
                assert_eq!(*request_id, request_id_1);
            } else {
                panic!()
            }

            // Try to send another response on the finished stream
            let response = TestResponse { counter: 43 };
            if let Err(StreamingResponseError::ChannelClosed) = b.respond(channel_id_1, response) {
            } else {
                panic!()
            }
        }

        // send response for request 2
        {
            let response = TestResponse { counter: 85 };
            b.respond(channel_id_2, response.clone()).unwrap();
            assert!(b.poll_until_pending().is_empty());
            if let StreamingResponseEvent::ResponseReceived {
                payload,
                request_id,
                sequence_no,
            } = a.poll_until_pending().first().unwrap()
            {
                assert_eq!(sequence_no.0, 1);
                assert_eq!(*request_id, request_id_2);
                assert_eq!(*payload, response);
            } else {
                panic!()
            }
        }
    }
}
