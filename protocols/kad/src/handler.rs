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

use crate::protocol::{
    KadInStreamSink, KadOutStreamSink, KadPeer, KadRequestMsg, KadResponseMsg,
    KademliaProtocolConfig,
};
use crate::record::{self, Record};
use futures::prelude::*;
use libp2p_swarm::{
    IntoProtocolsHandler,
    KeepAlive,
    NegotiatedSubstream,
    SubstreamProtocol,
    ProtocolsHandler,
    ProtocolsHandlerEvent,
    ProtocolsHandlerUpgrErr
};
use libp2p_core::{
    ConnectedPoint,
    PeerId,
    either::EitherOutput,
    upgrade::{self, InboundUpgrade, OutboundUpgrade}
};
use log::trace;
use std::{error, fmt, io, marker::PhantomData, pin::Pin, task::Context, task::Poll, time::Duration};
use wasm_timer::Instant;

/// A prototype from which [`KademliaHandler`]s can be constructed.
pub struct KademliaHandlerProto<T> {
    config: KademliaHandlerConfig,
    _type: PhantomData<T>,
}

impl<T> KademliaHandlerProto<T> {
    pub fn new(config: KademliaHandlerConfig) -> Self {
        KademliaHandlerProto { config, _type: PhantomData }
    }
}

impl<T: Clone + Send + 'static> IntoProtocolsHandler for KademliaHandlerProto<T> {
    type Handler = KademliaHandler<T>;

    fn into_handler(self, _: &PeerId, endpoint: &ConnectedPoint) -> Self::Handler {
        KademliaHandler::new(self.config, endpoint.clone())
    }

    fn inbound_protocol(&self) -> <Self::Handler as ProtocolsHandler>::InboundProtocol {
        if self.config.allow_listening {
            upgrade::EitherUpgrade::A(self.config.protocol_config.clone())
        } else {
            upgrade::EitherUpgrade::B(upgrade::DeniedUpgrade)
        }
    }
}

/// Protocol handler that manages substreams for the Kademlia protocol
/// on a single connection with a peer.
///
/// The handler will automatically open a Kademlia substream with the remote for each request we
/// make.
///
/// It also handles requests made by the remote.
pub struct KademliaHandler<TUserData> {
    /// Configuration for the Kademlia protocol.
    config: KademliaHandlerConfig,

    /// Next unique ID of a connection.
    next_connec_unique_id: UniqueConnecId,

    /// List of active substreams with the state they are in.
    substreams: Vec<SubstreamState<TUserData>>,

    /// Until when to keep the connection alive.
    keep_alive: KeepAlive,

    /// The connected endpoint of the connection that the handler
    /// is associated with.
    endpoint: ConnectedPoint,

    /// The current state of protocol confirmation.
    protocol_status: ProtocolStatus,
}

/// The states of protocol confirmation that a connection
/// handler transitions through.
enum ProtocolStatus {
    /// It is as yet unknown whether the remote supports the
    /// configured protocol name.
    Unconfirmed,
    /// The configured protocol name has been confirmed by the remote
    /// but has not yet been reported to the `Kademlia` behaviour.
    Confirmed,
    /// The configured protocol has been confirmed by the remote
    /// and the confirmation reported to the `Kademlia` behaviour.
    Reported,
}

/// Configuration of a [`KademliaHandler`].
#[derive(Debug, Clone)]
pub struct KademliaHandlerConfig {
    /// Configuration of the wire protocol.
    pub protocol_config: KademliaProtocolConfig,

    /// If false, we deny incoming requests.
    pub allow_listening: bool,

    /// Time after which we close an idle connection.
    pub idle_timeout: Duration,
}

/// State of an active substream, opened either by us or by the remote.
enum SubstreamState<TUserData> {
    /// We haven't started opening the outgoing substream yet.
    /// Contains the request we want to send, and the user data if we expect an answer.
    OutPendingOpen(KadRequestMsg, Option<TUserData>),
    /// Waiting to send a message to the remote.
    OutPendingSend(
        KadOutStreamSink<NegotiatedSubstream>,
        KadRequestMsg,
        Option<TUserData>,
    ),
    /// Waiting to flush the substream so that the data arrives to the remote.
    OutPendingFlush(KadOutStreamSink<NegotiatedSubstream>, Option<TUserData>),
    /// Waiting for an answer back from the remote.
    // TODO: add timeout
    OutWaitingAnswer(KadOutStreamSink<NegotiatedSubstream>, TUserData),
    /// An error happened on the substream and we should report the error to the user.
    OutReportError(KademliaHandlerQueryErr, TUserData),
    /// The substream is being closed.
    OutClosing(KadOutStreamSink<NegotiatedSubstream>),
    /// Waiting for a request from the remote.
    InWaitingMessage(UniqueConnecId, KadInStreamSink<NegotiatedSubstream>),
    /// Waiting for the user to send a `KademliaHandlerIn` event containing the response.
    InWaitingUser(UniqueConnecId, KadInStreamSink<NegotiatedSubstream>),
    /// Waiting to send an answer back to the remote.
    InPendingSend(UniqueConnecId, KadInStreamSink<NegotiatedSubstream>, KadResponseMsg),
    /// Waiting to flush an answer back to the remote.
    InPendingFlush(UniqueConnecId, KadInStreamSink<NegotiatedSubstream>),
    /// The substream is being closed.
    InClosing(KadInStreamSink<NegotiatedSubstream>),
}

impl<TUserData> SubstreamState<TUserData> {
    /// Tries to close the substream.
    ///
    /// If the substream is not ready to be closed, returns it back.
    fn try_close(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        match self {
            SubstreamState::OutPendingOpen(_, _)
            | SubstreamState::OutReportError(_, _) => Poll::Ready(()),
            SubstreamState::OutPendingSend(ref mut stream, _, _)
            | SubstreamState::OutPendingFlush(ref mut stream, _)
            | SubstreamState::OutWaitingAnswer(ref mut stream, _)
            | SubstreamState::OutClosing(ref mut stream) => match Sink::poll_close(Pin::new(stream), cx) {
                Poll::Ready(_) => Poll::Ready(()),
                Poll::Pending => Poll::Pending,
            },
            SubstreamState::InWaitingMessage(_, ref mut stream)
            | SubstreamState::InWaitingUser(_, ref mut stream)
            | SubstreamState::InPendingSend(_, ref mut stream, _)
            | SubstreamState::InPendingFlush(_, ref mut stream)
            | SubstreamState::InClosing(ref mut stream) => match Sink::poll_close(Pin::new(stream), cx) {
                Poll::Ready(_) => Poll::Ready(()),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

/// Event produced by the Kademlia handler.
#[derive(Debug)]
pub enum KademliaHandlerEvent<TUserData> {
    /// The configured protocol name has been confirmed by the peer through
    /// a successfully negotiated substream.
    ///
    /// This event is only emitted once by a handler upon the first
    /// successfully negotiated inbound or outbound substream and
    /// indicates that the connected peer participates in the Kademlia
    /// overlay network identified by the configured protocol name.
    ProtocolConfirmed { endpoint: ConnectedPoint },

    /// Request for the list of nodes whose IDs are the closest to `key`. The number of nodes
    /// returned is not specified, but should be around 20.
    FindNodeReq {
        /// The key for which to locate the closest nodes.
        key: Vec<u8>,
        /// Identifier of the request. Needs to be passed back when answering.
        request_id: KademliaRequestId,
    },

    /// Response to an `KademliaHandlerIn::FindNodeReq`.
    FindNodeRes {
        /// Results of the request.
        closer_peers: Vec<KadPeer>,
        /// The user data passed to the `FindNodeReq`.
        user_data: TUserData,
    },

    /// Same as `FindNodeReq`, but should also return the entries of the local providers list for
    /// this key.
    GetProvidersReq {
        /// The key for which providers are requested.
        key: record::Key,
        /// Identifier of the request. Needs to be passed back when answering.
        request_id: KademliaRequestId,
    },

    /// Response to an `KademliaHandlerIn::GetProvidersReq`.
    GetProvidersRes {
        /// Nodes closest to the key.
        closer_peers: Vec<KadPeer>,
        /// Known providers for this key.
        provider_peers: Vec<KadPeer>,
        /// The user data passed to the `GetProvidersReq`.
        user_data: TUserData,
    },

    /// An error happened when performing a query.
    QueryError {
        /// The error that happened.
        error: KademliaHandlerQueryErr,
        /// The user data passed to the query.
        user_data: TUserData,
    },

    /// The peer announced itself as a provider of a key.
    AddProvider {
        /// The key for which the peer is a provider of the associated value.
        key: record::Key,
        /// The peer that is the provider of the value for `key`.
        provider: KadPeer,
    },

    /// Request to get a value from the dht records
    GetRecord {
        /// Key for which we should look in the dht
        key: record::Key,
        /// Identifier of the request. Needs to be passed back when answering.
        request_id: KademliaRequestId,
    },

    /// Response to a `KademliaHandlerIn::GetRecord`.
    GetRecordRes {
        /// The result is present if the key has been found
        record: Option<Record>,
        /// Nodes closest to the key.
        closer_peers: Vec<KadPeer>,
        /// The user data passed to the `GetValue`.
        user_data: TUserData,
    },

    /// Request to put a value in the dht records
    PutRecord {
        record: Record,
        /// Identifier of the request. Needs to be passed back when answering.
        request_id: KademliaRequestId,
    },

    /// Response to a request to store a record.
    PutRecordRes {
        /// The key of the stored record.
        key: record::Key,
        /// The value of the stored record.
        value: Vec<u8>,
        /// The user data passed to the `PutValue`.
        user_data: TUserData,
    }
}

/// Error that can happen when requesting an RPC query.
#[derive(Debug)]
pub enum KademliaHandlerQueryErr {
    /// Error while trying to perform the query.
    Upgrade(ProtocolsHandlerUpgrErr<io::Error>),
    /// Received an answer that doesn't correspond to the request.
    UnexpectedMessage,
    /// I/O error in the substream.
    Io(io::Error),
}

impl fmt::Display for KademliaHandlerQueryErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KademliaHandlerQueryErr::Upgrade(err) => {
                write!(f, "Error while performing Kademlia query: {}", err)
            },
            KademliaHandlerQueryErr::UnexpectedMessage => {
                write!(f, "Remote answered our Kademlia RPC query with the wrong message type")
            },
            KademliaHandlerQueryErr::Io(err) => {
                write!(f, "I/O error during a Kademlia RPC query: {}", err)
            },
        }
    }
}

impl error::Error for KademliaHandlerQueryErr {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            KademliaHandlerQueryErr::Upgrade(err) => Some(err),
            KademliaHandlerQueryErr::UnexpectedMessage => None,
            KademliaHandlerQueryErr::Io(err) => Some(err),
        }
    }
}

impl From<ProtocolsHandlerUpgrErr<io::Error>> for KademliaHandlerQueryErr {
    fn from(err: ProtocolsHandlerUpgrErr<io::Error>) -> Self {
        KademliaHandlerQueryErr::Upgrade(err)
    }
}

/// Event to send to the handler.
#[derive(Debug)]
pub enum KademliaHandlerIn<TUserData> {
    /// Resets the (sub)stream associated with the given request ID,
    /// thus signaling an error to the remote.
    ///
    /// Explicitly resetting the (sub)stream associated with a request
    /// can be used as an alternative to letting requests simply time
    /// out on the remote peer, thus potentially avoiding some delay
    /// for the query on the remote.
    Reset(KademliaRequestId),

    /// Request for the list of nodes whose IDs are the closest to `key`. The number of nodes
    /// returned is not specified, but should be around 20.
    FindNodeReq {
        /// Identifier of the node.
        key: Vec<u8>,
        /// Custom user data. Passed back in the out event when the results arrive.
        user_data: TUserData,
    },

    /// Response to a `FindNodeReq`.
    FindNodeRes {
        /// Results of the request.
        closer_peers: Vec<KadPeer>,
        /// Identifier of the request that was made by the remote.
        ///
        /// It is a logic error to use an id of the handler of a different node.
        request_id: KademliaRequestId,
    },

    /// Same as `FindNodeReq`, but should also return the entries of the local providers list for
    /// this key.
    GetProvidersReq {
        /// Identifier being searched.
        key: record::Key,
        /// Custom user data. Passed back in the out event when the results arrive.
        user_data: TUserData,
    },

    /// Response to a `GetProvidersReq`.
    GetProvidersRes {
        /// Nodes closest to the key.
        closer_peers: Vec<KadPeer>,
        /// Known providers for this key.
        provider_peers: Vec<KadPeer>,
        /// Identifier of the request that was made by the remote.
        ///
        /// It is a logic error to use an id of the handler of a different node.
        request_id: KademliaRequestId,
    },

    /// Indicates that this provider is known for this key.
    ///
    /// The API of the handler doesn't expose any event that allows you to know whether this
    /// succeeded.
    AddProvider {
        /// Key for which we should add providers.
        key: record::Key,
        /// Known provider for this key.
        provider: KadPeer,
    },

    /// Request to retrieve a record from the DHT.
    GetRecord {
        /// The key of the record.
        key: record::Key,
        /// Custom data. Passed back in the out event when the results arrive.
        user_data: TUserData,
    },

    /// Response to a `GetRecord` request.
    GetRecordRes {
        /// The value that might have been found in our storage.
        record: Option<Record>,
        /// Nodes that are closer to the key we were searching for.
        closer_peers: Vec<KadPeer>,
        /// Identifier of the request that was made by the remote.
        request_id: KademliaRequestId,
    },

    /// Put a value into the dht records.
    PutRecord {
        record: Record,
        /// Custom data. Passed back in the out event when the results arrive.
        user_data: TUserData,
    },

    /// Response to a `PutRecord`.
    PutRecordRes {
        /// Key of the value that was put.
        key: record::Key,
        /// Value that was put.
        value: Vec<u8>,
        /// Identifier of the request that was made by the remote.
        request_id: KademliaRequestId,
    }
}

/// Unique identifier for a request. Must be passed back in order to answer a request from
/// the remote.
#[derive(Debug, PartialEq, Eq)]
pub struct KademliaRequestId {
    /// Unique identifier for an incoming connection.
    connec_unique_id: UniqueConnecId,
}

/// Unique identifier for a connection.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct UniqueConnecId(u64);

impl<TUserData> KademliaHandler<TUserData> {
    /// Create a [`KademliaHandler`] using the given configuration.
    pub fn new(config: KademliaHandlerConfig, endpoint: ConnectedPoint) -> Self {
        let keep_alive = KeepAlive::Until(Instant::now() + config.idle_timeout);

        KademliaHandler {
            config,
            endpoint,
            next_connec_unique_id: UniqueConnecId(0),
            substreams: Vec::new(),
            keep_alive,
            protocol_status: ProtocolStatus::Unconfirmed,
        }
    }
}

impl<TUserData> ProtocolsHandler for KademliaHandler<TUserData>
where
    TUserData: Clone + Send + 'static,
{
    type InEvent = KademliaHandlerIn<TUserData>;
    type OutEvent = KademliaHandlerEvent<TUserData>;
    type Error = io::Error; // TODO: better error type?
    type InboundProtocol = upgrade::EitherUpgrade<KademliaProtocolConfig, upgrade::DeniedUpgrade>;
    type OutboundProtocol = KademliaProtocolConfig;
    // Message of the request to send to the remote, and user data if we expect an answer.
    type OutboundOpenInfo = (KadRequestMsg, Option<TUserData>);
    type InboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        if self.config.allow_listening {
            SubstreamProtocol::new(self.config.protocol_config.clone(), ()).map_upgrade(upgrade::EitherUpgrade::A)
        } else {
            SubstreamProtocol::new(upgrade::EitherUpgrade::B(upgrade::DeniedUpgrade), ())
        }
    }

    fn inject_fully_negotiated_outbound(
        &mut self,
        protocol: <Self::OutboundProtocol as OutboundUpgrade<NegotiatedSubstream>>::Output,
        (msg, user_data): Self::OutboundOpenInfo,
    ) {
        self.substreams.push(SubstreamState::OutPendingSend(protocol, msg, user_data));
        if let ProtocolStatus::Unconfirmed = self.protocol_status {
            // Upon the first successfully negotiated substream, we know that the
            // remote is configured with the same protocol name and we want
            // the behaviour to add this peer to the routing table, if possible.
            self.protocol_status = ProtocolStatus::Confirmed;
        }
    }

    fn inject_fully_negotiated_inbound(
        &mut self,
        protocol: <Self::InboundProtocol as InboundUpgrade<NegotiatedSubstream>>::Output,
        (): Self::InboundOpenInfo
    ) {
        // If `self.allow_listening` is false, then we produced a `DeniedUpgrade` and `protocol`
        // is a `Void`.
        let protocol = match protocol {
            EitherOutput::First(p) => p,
            EitherOutput::Second(p) => void::unreachable(p),
        };

        debug_assert!(self.config.allow_listening);
        let connec_unique_id = self.next_connec_unique_id;
        self.next_connec_unique_id.0 += 1;
        self.substreams.push(SubstreamState::InWaitingMessage(connec_unique_id, protocol));
        if let ProtocolStatus::Unconfirmed = self.protocol_status {
            // Upon the first successfully negotiated substream, we know that the
            // remote is configured with the same protocol name and we want
            // the behaviour to add this peer to the routing table, if possible.
            self.protocol_status = ProtocolStatus::Confirmed;
        }
    }

    fn inject_event(&mut self, message: KademliaHandlerIn<TUserData>) {
        match message {
            KademliaHandlerIn::Reset(request_id) => {
                let pos = self.substreams.iter().position(|state| match state {
                        SubstreamState::InWaitingUser(conn_id, _) =>
                            conn_id == &request_id.connec_unique_id,
                    _ => false,
                });
                if let Some(pos) = pos {
                    // TODO: we don't properly close down the substream
                    let waker = futures::task::noop_waker();
                    let mut cx = Context::from_waker(&waker);
                    let _ = self.substreams.remove(pos).try_close(&mut cx);
                }
            }
            KademliaHandlerIn::FindNodeReq { key, user_data } => {
                let msg = KadRequestMsg::FindNode { key };
                self.substreams.push(SubstreamState::OutPendingOpen(msg, Some(user_data)));
            }
            KademliaHandlerIn::FindNodeRes {
                closer_peers,
                request_id,
            } => {
                let pos = self.substreams.iter().position(|state| match state {
                    SubstreamState::InWaitingUser(ref conn_id, _) =>
                        conn_id == &request_id.connec_unique_id,
                    _ => false,
                });

                if let Some(pos) = pos {
                    let (conn_id, substream) = match self.substreams.remove(pos) {
                        SubstreamState::InWaitingUser(conn_id, substream) => (conn_id, substream),
                        _ => unreachable!(),
                    };

                    let msg = KadResponseMsg::FindNode {
                        closer_peers,
                    };
                    self.substreams
                        .push(SubstreamState::InPendingSend(conn_id, substream, msg));
                }
            }
            KademliaHandlerIn::GetProvidersReq { key, user_data } => {
                let msg = KadRequestMsg::GetProviders { key };
                self.substreams
                    .push(SubstreamState::OutPendingOpen(msg, Some(user_data)));
            }
            KademliaHandlerIn::GetProvidersRes {
                closer_peers,
                provider_peers,
                request_id,
            } => {
                let pos = self.substreams.iter().position(|state| match state {
                    SubstreamState::InWaitingUser(ref conn_id, _)
                        if conn_id == &request_id.connec_unique_id =>
                    {
                        true
                    }
                    _ => false,
                });

                if let Some(pos) = pos {
                    let (conn_id, substream) = match self.substreams.remove(pos) {
                        SubstreamState::InWaitingUser(conn_id, substream) => (conn_id, substream),
                        _ => unreachable!(),
                    };

                    let msg = KadResponseMsg::GetProviders {
                        closer_peers,
                        provider_peers,
                    };
                    self.substreams
                        .push(SubstreamState::InPendingSend(conn_id, substream, msg));
                }
            }
            KademliaHandlerIn::AddProvider { key, provider } => {
                let msg = KadRequestMsg::AddProvider { key, provider };
                self.substreams.push(SubstreamState::OutPendingOpen(msg, None));
            }
            KademliaHandlerIn::GetRecord { key, user_data } => {
                let msg = KadRequestMsg::GetValue { key };
                self.substreams.push(SubstreamState::OutPendingOpen(msg, Some(user_data)));

            }
            KademliaHandlerIn::PutRecord { record, user_data } => {
                let msg = KadRequestMsg::PutValue { record };
                self.substreams
                    .push(SubstreamState::OutPendingOpen(msg, Some(user_data)));
            }
            KademliaHandlerIn::GetRecordRes {
                record,
                closer_peers,
                request_id,
            } => {
                let pos = self.substreams.iter().position(|state| match state {
                    SubstreamState::InWaitingUser(ref conn_id, _)
                        => conn_id == &request_id.connec_unique_id,
                    _ => false,
                });

                if let Some(pos) = pos {
                    let (conn_id, substream) = match self.substreams.remove(pos) {
                        SubstreamState::InWaitingUser(conn_id, substream) => (conn_id, substream),
                        _ => unreachable!(),
                    };

                    let msg = KadResponseMsg::GetValue {
                        record,
                        closer_peers,
                    };
                    self.substreams
                        .push(SubstreamState::InPendingSend(conn_id, substream, msg));
                }
            }
            KademliaHandlerIn::PutRecordRes {
                key,
                request_id,
                value,
            } => {
                let pos = self.substreams.iter().position(|state| match state {
                    SubstreamState::InWaitingUser(ref conn_id, _)
                        if conn_id == &request_id.connec_unique_id =>
                        {
                            true
                        }
                    _ => false,
                });

                if let Some(pos) = pos {
                    let (conn_id, substream) = match self.substreams.remove(pos) {
                        SubstreamState::InWaitingUser(conn_id, substream) => (conn_id, substream),
                        _ => unreachable!(),
                    };

                    let msg = KadResponseMsg::PutValue {
                        key,
                        value,
                    };
                    self.substreams
                        .push(SubstreamState::InPendingSend(conn_id, substream, msg));
                }
            }
        }
    }

    fn inject_dial_upgrade_error(
        &mut self,
        (_, user_data): Self::OutboundOpenInfo,
        error: ProtocolsHandlerUpgrErr<io::Error>,
    ) {
        // TODO: cache the fact that the remote doesn't support kademlia at all, so that we don't
        //       continue trying
        if let Some(user_data) = user_data {
            self.substreams
                .push(SubstreamState::OutReportError(error.into(), user_data));
        }
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        self.keep_alive
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        ProtocolsHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::OutEvent, Self::Error>,
    > {
        if self.substreams.is_empty() {
            return Poll::Pending;
        }

        if let ProtocolStatus::Confirmed = self.protocol_status {
            self.protocol_status = ProtocolStatus::Reported;
            return Poll::Ready(ProtocolsHandlerEvent::Custom(
                KademliaHandlerEvent::ProtocolConfirmed {
                    endpoint: self.endpoint.clone()
                }))
        }

        // We remove each element from `substreams` one by one and add them back.
        for n in (0..self.substreams.len()).rev() {
            let mut substream = self.substreams.swap_remove(n);

            loop {
                match advance_substream(substream, self.config.protocol_config.clone(), cx) {
                    (Some(new_state), Some(event), _) => {
                        self.substreams.push(new_state);
                        return Poll::Ready(event);
                    }
                    (None, Some(event), _) => {
                        if self.substreams.is_empty() {
                            self.keep_alive = KeepAlive::Until(Instant::now() + self.config.idle_timeout);
                        }
                        return Poll::Ready(event);
                    }
                    (Some(new_state), None, false) => {
                        self.substreams.push(new_state);
                        break;
                    }
                    (Some(new_state), None, true) => {
                        substream = new_state;
                        continue;
                    }
                    (None, None, _) => {
                        break;
                    }
                }
            }
        }

        if self.substreams.is_empty() {
            // We destroyed all substreams in this function.
            self.keep_alive = KeepAlive::Until(Instant::now() + self.config.idle_timeout);
        } else {
            self.keep_alive = KeepAlive::Yes;
        }

        Poll::Pending
    }
}

impl Default for KademliaHandlerConfig {
    fn default() -> Self {
        KademliaHandlerConfig {
            protocol_config: Default::default(),
            allow_listening: true,
            idle_timeout: Duration::from_secs(10),
        }
    }
}

/// Advances one substream.
///
/// Returns the new state for that substream, an event to generate, and whether the substream
/// should be polled again.
fn advance_substream<TUserData>(
    state: SubstreamState<TUserData>,
    upgrade: KademliaProtocolConfig,
    cx: &mut Context<'_>,
) -> (
    Option<SubstreamState<TUserData>>,
    Option<
        ProtocolsHandlerEvent<
            KademliaProtocolConfig,
            (KadRequestMsg, Option<TUserData>),
            KademliaHandlerEvent<TUserData>,
            io::Error,
        >,
    >,
    bool,
)
{
    match state {
        SubstreamState::OutPendingOpen(msg, user_data) => {
            let ev = ProtocolsHandlerEvent::OutboundSubstreamRequest {
                protocol: SubstreamProtocol::new(upgrade, (msg, user_data))
            };
            (None, Some(ev), false)
        }
        SubstreamState::OutPendingSend(mut substream, msg, user_data) => {
            match Sink::poll_ready(Pin::new(&mut substream), cx) {
                Poll::Ready(Ok(())) => {
                    match Sink::start_send(Pin::new(&mut substream), msg) {
                        Ok(()) => (
                            Some(SubstreamState::OutPendingFlush(substream, user_data)),
                            None,
                            true,
                        ),
                        Err(error) => {
                            let event = if let Some(user_data) = user_data {
                                Some(ProtocolsHandlerEvent::Custom(KademliaHandlerEvent::QueryError {
                                    error: KademliaHandlerQueryErr::Io(error),
                                    user_data
                                }))
                            } else {
                                None
                            };

                            (None, event, false)
                        }
                    }
                },
                Poll::Pending => (
                    Some(SubstreamState::OutPendingSend(substream, msg, user_data)),
                    None,
                    false,
                ),
                Poll::Ready(Err(error)) => {
                    let event = if let Some(user_data) = user_data {
                        Some(ProtocolsHandlerEvent::Custom(KademliaHandlerEvent::QueryError {
                            error: KademliaHandlerQueryErr::Io(error),
                            user_data
                        }))
                    } else {
                        None
                    };

                    (None, event, false)
                }
            }
        }
        SubstreamState::OutPendingFlush(mut substream, user_data) => {
            match Sink::poll_flush(Pin::new(&mut substream), cx) {
                Poll::Ready(Ok(())) => {
                    if let Some(user_data) = user_data {
                        (
                            Some(SubstreamState::OutWaitingAnswer(substream, user_data)),
                            None,
                            true,
                        )
                    } else {
                        (Some(SubstreamState::OutClosing(substream)), None, true)
                    }
                }
                Poll::Pending => (
                    Some(SubstreamState::OutPendingFlush(substream, user_data)),
                    None,
                    false,
                ),
                Poll::Ready(Err(error)) => {
                    let event = if let Some(user_data) = user_data {
                        Some(ProtocolsHandlerEvent::Custom(KademliaHandlerEvent::QueryError {
                            error: KademliaHandlerQueryErr::Io(error),
                            user_data,
                        }))
                    } else {
                        None
                    };

                    (None, event, false)
                }
            }
        }
        SubstreamState::OutWaitingAnswer(mut substream, user_data) => match Stream::poll_next(Pin::new(&mut substream), cx) {
            Poll::Ready(Some(Ok(msg))) => {
                let new_state = SubstreamState::OutClosing(substream);
                let event = process_kad_response(msg, user_data);
                (
                    Some(new_state),
                    Some(ProtocolsHandlerEvent::Custom(event)),
                    true,
                )
            }
            Poll::Pending => (
                Some(SubstreamState::OutWaitingAnswer(substream, user_data)),
                None,
                false,
            ),
            Poll::Ready(Some(Err(error))) => {
                let event = KademliaHandlerEvent::QueryError {
                    error: KademliaHandlerQueryErr::Io(error),
                    user_data,
                };
                (None, Some(ProtocolsHandlerEvent::Custom(event)), false)
            }
            Poll::Ready(None) => {
                let event = KademliaHandlerEvent::QueryError {
                    error: KademliaHandlerQueryErr::Io(io::ErrorKind::UnexpectedEof.into()),
                    user_data,
                };
                (None, Some(ProtocolsHandlerEvent::Custom(event)), false)
            }
        },
        SubstreamState::OutReportError(error, user_data) => {
            let event = KademliaHandlerEvent::QueryError { error, user_data };
            (None, Some(ProtocolsHandlerEvent::Custom(event)), false)
        }
        SubstreamState::OutClosing(mut stream) => match Sink::poll_close(Pin::new(&mut stream), cx) {
            Poll::Ready(Ok(())) => (None, None, false),
            Poll::Pending => (Some(SubstreamState::OutClosing(stream)), None, false),
            Poll::Ready(Err(_)) => (None, None, false),
        },
        SubstreamState::InWaitingMessage(id, mut substream) => match Stream::poll_next(Pin::new(&mut substream), cx) {
            Poll::Ready(Some(Ok(msg))) => {
                if let Ok(ev) = process_kad_request(msg, id) {
                    (
                        Some(SubstreamState::InWaitingUser(id, substream)),
                        Some(ProtocolsHandlerEvent::Custom(ev)),
                        false,
                    )
                } else {
                    (Some(SubstreamState::InClosing(substream)), None, true)
                }
            }
            Poll::Pending => (
                Some(SubstreamState::InWaitingMessage(id, substream)),
                None,
                false,
            ),
            Poll::Ready(None) => {
                trace!("Inbound substream: EOF");
                (None, None, false)
            }
            Poll::Ready(Some(Err(e))) => {
                trace!("Inbound substream error: {:?}", e);
                (None, None, false)
            },
        },
        SubstreamState::InWaitingUser(id, substream) => (
            Some(SubstreamState::InWaitingUser(id, substream)),
            None,
            false,
        ),
        SubstreamState::InPendingSend(id, mut substream, msg) => match Sink::poll_ready(Pin::new(&mut substream), cx) {
            Poll::Ready(Ok(())) => match Sink::start_send(Pin::new(&mut substream), msg) {
                Ok(()) => (
                    Some(SubstreamState::InPendingFlush(id, substream)),
                    None,
                    true,
                ),
                Err(_) => (None, None, false),
            },
            Poll::Pending => (
                Some(SubstreamState::InPendingSend(id, substream, msg)),
                None,
                false,
            ),
            Poll::Ready(Err(_)) => (None, None, false),
        }
        SubstreamState::InPendingFlush(id, mut substream) => match Sink::poll_flush(Pin::new(&mut substream), cx) {
            Poll::Ready(Ok(())) => (
                Some(SubstreamState::InWaitingMessage(id, substream)),
                None,
                true,
            ),
            Poll::Pending => (
                Some(SubstreamState::InPendingFlush(id, substream)),
                None,
                false,
            ),
            Poll::Ready(Err(_)) => (None, None, false),
        },
        SubstreamState::InClosing(mut stream) => match Sink::poll_close(Pin::new(&mut stream), cx) {
            Poll::Ready(Ok(())) => (None, None, false),
            Poll::Pending => (Some(SubstreamState::InClosing(stream)), None, false),
            Poll::Ready(Err(_)) => (None, None, false),
        },
    }
}

/// Processes a Kademlia message that's expected to be a request from a remote.
fn process_kad_request<TUserData>(
    event: KadRequestMsg,
    connec_unique_id: UniqueConnecId,
) -> Result<KademliaHandlerEvent<TUserData>, io::Error> {
    match event {
        KadRequestMsg::Ping => {
            // TODO: implement; although in practice the PING message is never
            //       used, so we may consider removing it altogether
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "the PING Kademlia message is not implemented",
            ))
        }
        KadRequestMsg::FindNode { key } => Ok(KademliaHandlerEvent::FindNodeReq {
            key,
            request_id: KademliaRequestId { connec_unique_id },
        }),
        KadRequestMsg::GetProviders { key } => Ok(KademliaHandlerEvent::GetProvidersReq {
            key,
            request_id: KademliaRequestId { connec_unique_id },
        }),
        KadRequestMsg::AddProvider { key, provider } => {
            Ok(KademliaHandlerEvent::AddProvider { key, provider })
        }
        KadRequestMsg::GetValue { key } => Ok(KademliaHandlerEvent::GetRecord {
            key,
            request_id: KademliaRequestId { connec_unique_id },
        }),
        KadRequestMsg::PutValue { record } => Ok(KademliaHandlerEvent::PutRecord {
            record,
            request_id: KademliaRequestId { connec_unique_id },
        })
    }
}

/// Process a Kademlia message that's supposed to be a response to one of our requests.
fn process_kad_response<TUserData>(
    event: KadResponseMsg,
    user_data: TUserData,
) -> KademliaHandlerEvent<TUserData> {
    // TODO: must check that the response corresponds to the request
    match event {
        KadResponseMsg::Pong => {
            // We never send out pings.
            KademliaHandlerEvent::QueryError {
                error: KademliaHandlerQueryErr::UnexpectedMessage,
                user_data,
            }
        }
        KadResponseMsg::FindNode { closer_peers } => {
            KademliaHandlerEvent::FindNodeRes {
                closer_peers,
                user_data,
            }
        },
        KadResponseMsg::GetProviders {
            closer_peers,
            provider_peers,
        } => KademliaHandlerEvent::GetProvidersRes {
            closer_peers,
            provider_peers,
            user_data,
        },
        KadResponseMsg::GetValue {
            record,
            closer_peers,
        } => KademliaHandlerEvent::GetRecordRes {
            record,
            closer_peers,
            user_data,
        },
        KadResponseMsg::PutValue { key, value, .. } => {
            KademliaHandlerEvent::PutRecordRes {
                key,
                value,
                user_data,
            }
        }
    }
}
