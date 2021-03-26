// Copyright 2019 Parity Technologies (UK) Ltd.
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

use crate::message_proto::circuit_relay;
use crate::protocol;
use crate::RequestId;
use futures::channel::oneshot::{self, Canceled};
use futures::future::BoxFuture;
use futures::prelude::*;
use futures::stream::FuturesUnordered;
use libp2p_core::connection::ConnectionId;
use libp2p_core::either::{EitherError, EitherOutput};
use libp2p_core::{upgrade, ConnectedPoint, Multiaddr, PeerId};
use libp2p_swarm::{
    IntoProtocolsHandler, KeepAlive, NegotiatedSubstream, ProtocolsHandler, ProtocolsHandlerEvent,
    ProtocolsHandlerUpgrErr, SubstreamProtocol,
};
use log::warn;
use std::task::{Context, Poll};
use std::time::Duration;
use wasm_timer::Instant;

pub struct RelayHandlerConfig {
    pub connection_idle_timeout: Duration,
}

pub struct RelayHandlerProto {
    pub config: RelayHandlerConfig,
}

impl IntoProtocolsHandler for RelayHandlerProto {
    type Handler = RelayHandler;

    fn into_handler(self, remote_peer_id: &PeerId, endpoint: &ConnectedPoint) -> Self::Handler {
        RelayHandler::new(
            self.config,
            *remote_peer_id,
            endpoint.get_remote_address().clone(),
        )
    }

    fn inbound_protocol(&self) -> <Self::Handler as ProtocolsHandler>::InboundProtocol {
        protocol::RelayListen::new()
    }
}

/// Protocol handler that handles the relay protocol.
///
/// There are four possible situations in play here:
///
/// - The handler emits [`RelayHandlerEvent::IncomingRelayReq`] if the node we handle asks us to act
///   as a relay. You must send a [`RelayHandlerIn::OutgoingDstReq`] to another handler, or send
///   back a [`RelayHandlerIn::DenyIncomingRelayReq`].
///
/// - The handler emits [`RelayHandlerEvent::IncomingDstReq`] if the node we handle asks us to act
///   as a destination. You must either send back a [`RelayHandlerIn::AcceptDstReq`]`, or send back
///   a [`RelayHandlerIn::DenyDstReq`].
///
/// - Send a [`RelayHandlerIn::OutgoingRelayReq`] if the node we handle must act as a relay to a
///   destination. The handler will either send back a
///   [`RelayHandlerEvent::OutgoingRelayReqSuccess`] containing the stream to the destination, or a
///   [`RelayHandlerEvent::OutgoingRelayReqError`].
///
/// - Send a [`RelayHandlerIn::OutgoingDstReq`] if the node we handle must act as a destination. The
///   handler will automatically notify the source whether the request was accepted or denied.
pub struct RelayHandler {
    config: RelayHandlerConfig,
    /// Specifies whether the handled connection is used to listen for incoming relayed connections.
    used_for_listening: bool,
    remote_address: Multiaddr,
    remote_peer_id: PeerId,
    /// Futures that send back negative responses.
    deny_futures: FuturesUnordered<BoxFuture<'static, Result<(), std::io::Error>>>,
    /// Futures that send back an accept response to a relay.
    accept_dst_futures: FuturesUnordered<
        BoxFuture<
            'static,
            Result<
                (PeerId, protocol::Connection, oneshot::Receiver<()>),
                protocol::IncomingDstReqError,
            >,
        >,
    >,
    /// Futures that copy from a source to a destination.
    copy_futures: FuturesUnordered<BoxFuture<'static, Result<(), protocol::IncomingRelayReqError>>>,
    /// Requests asking the remote to become a relay.
    outgoing_relay_reqs: Vec<OutgoingRelayReq>,
    /// Requests asking the remote to become a destination.
    outgoing_dst_reqs: Vec<OutgoingDstReq>,
    /// Queue of events to return when polled.
    queued_events: Vec<RelayHandlerEvent>,
    /// Tracks substreams lend out to other [`RelayHandler`]s or as
    /// [`Connection`](protocol::Connection) to the
    /// [`RelayTransport`](crate::RelayTransport).
    ///
    /// For each substream to the peer of this handler, there is a future in here that resolves once
    /// the given substream is dropped.
    ///
    /// Once all substreams are dropped and this handler has no other work, [`KeepAlive::Until`] can
    /// be set, allowing the connection to be closed eventually.
    alive_lend_out_substreams: FuturesUnordered<oneshot::Receiver<()>>,
    /// The current connection keep-alive.
    keep_alive: KeepAlive,
    /// A pending fatal error that results in the connection being closed.
    pending_error: Option<
        ProtocolsHandlerUpgrErr<
            EitherError<
                protocol::RelayListenError,
                EitherError<protocol::OutgoingRelayReqError, protocol::OutgoingDstReqError>,
            >,
        >,
    >,
}

struct OutgoingRelayReq {
    src_peer_id: PeerId,
    dst_peer_id: PeerId,
    request_id: RequestId,
    /// Addresses of the destination.
    dst_addr: Option<Multiaddr>,
}

struct OutgoingDstReq {
    src_peer_id: PeerId,
    src_addr: Multiaddr,
    src_connection_id: ConnectionId,
    request_id: RequestId,
    incoming_relay_req: protocol::IncomingRelayReq,
}

/// Event produced by the relay handler.
pub enum RelayHandlerEvent {
    /// The remote wants us to relay communications to a third party. You must either send back a
    /// [`RelayHandlerIn::DenyIncomingRelayReq`], or an [`RelayHandlerIn::OutgoingDstReq`] to any
    /// connection handler for the destination peer, providing the [`protocol::IncomingRelayReq`].
    IncomingRelayReq {
        request_id: RequestId,
        src_addr: Multiaddr,
        req: protocol::IncomingRelayReq,
    },

    /// The remote is a relay and is relaying a connection to us. In other words, we are used as
    /// a destination. The behaviour can accept or deny the request via
    /// [`AcceptDstReq`](RelayHandlerIn::AcceptDstReq) or
    /// [`DenyDstReq`](RelayHandlerIn::DenyDstReq).
    IncomingDstReq(protocol::IncomingDstReq),

    /// A `RelayReq` that has previously been sent has been accepted by the remote. Contains
    /// a substream that communicates with the requested destination.
    ///
    /// > **Note**: There is no proof that we are actually communicating with the destination. An
    /// >           encryption handshake has to be performed on top of this substream in order to
    /// >           avoid MITM attacks.
    OutgoingRelayReqSuccess(PeerId, RequestId, protocol::Connection),

    /// The local node has accepted an incoming destination request. Contains a substream that
    /// communicates with the source.
    ///
    /// > **Note**: There is no proof that we are actually communicating with the destination. An
    /// >           encryption handshake has to be performed on top of this substream in order to
    /// >           avoid MITM attacks.
    IncomingDstReqSuccess {
        stream: protocol::Connection,
        src_peer_id: PeerId,
        relay_peer_id: PeerId,
        relay_addr: Multiaddr,
    },

    /// A `RelayReq` that has previously been sent by the local node has failed.
    OutgoingRelayReqError(PeerId, RequestId),

    /// A destination request that has previously been sent by the local node has failed.
    ///
    /// Includes the incoming relay request, which is yet to be denied due to the failure.
    OutgoingDstReqError {
        src_connection_id: ConnectionId,
        incoming_relay_req_deny_fut: BoxFuture<'static, Result<(), std::io::Error>>,
    },
}

/// Event that can be sent to the relay handler.
pub enum RelayHandlerIn {
    /// Tell the handler whether it is handling a connection used to listen for incoming relayed
    /// connections.
    UsedForListening(bool),
    /// Denies a relay request sent by the node we talk to acting as a source.
    DenyIncomingRelayReq(BoxFuture<'static, Result<(), std::io::Error>>),

    /// Accepts a destination request sent by the node we talk to.
    AcceptDstReq(protocol::IncomingDstReq),

    /// Denies a destination request sent by the node we talk to.
    DenyDstReq(protocol::IncomingDstReq),

    /// Opens a new substream to the remote and asks it to relay communications to a third party.
    OutgoingRelayReq {
        src_peer_id: PeerId,
        dst_peer_id: PeerId,
        request_id: RequestId,
        /// Addresses known for this peer to transmit to the remote.
        dst_addr: Option<Multiaddr>,
    },

    /// Asks the node to be used as a destination for a relayed connection.
    ///
    /// The positive or negative response will be written to `substream`.
    OutgoingDstReq {
        /// Peer id of the node whose communications are being relayed.
        src_peer_id: PeerId,
        /// Address of the node whose communications are being relayed.
        src_addr: Multiaddr,
        src_connection_id: ConnectionId,
        request_id: RequestId,
        /// Incoming relay request from the source node.
        incoming_relay_req: protocol::IncomingRelayReq,
    },
}

impl RelayHandler {
    /// Builds a new `RelayHandler`.
    pub fn new(
        config: RelayHandlerConfig,
        remote_peer_id: PeerId,
        remote_address: Multiaddr,
    ) -> Self {
        RelayHandler {
            config,
            used_for_listening: false,
            remote_address,
            remote_peer_id,
            deny_futures: Default::default(),
            accept_dst_futures: Default::default(),
            copy_futures: Default::default(),
            outgoing_relay_reqs: Default::default(),
            outgoing_dst_reqs: Default::default(),
            queued_events: Default::default(),
            alive_lend_out_substreams: Default::default(),
            keep_alive: KeepAlive::Yes,
            pending_error: None,
        }
    }
}

impl ProtocolsHandler for RelayHandler {
    type InEvent = RelayHandlerIn;
    type OutEvent = RelayHandlerEvent;
    type Error = ProtocolsHandlerUpgrErr<
        EitherError<
            protocol::RelayListenError,
            EitherError<protocol::OutgoingRelayReqError, protocol::OutgoingDstReqError>,
        >,
    >;
    type InboundProtocol = protocol::RelayListen;
    type OutboundProtocol =
        upgrade::EitherUpgrade<protocol::OutgoingRelayReq, protocol::OutgoingDstReq>;
    type OutboundOpenInfo = RelayOutboundOpenInfo;
    type InboundOpenInfo = RequestId;

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(protocol::RelayListen::new(), RequestId::new())
    }

    fn inject_fully_negotiated_inbound(
        &mut self,
        protocol: <Self::InboundProtocol as upgrade::InboundUpgrade<NegotiatedSubstream>>::Output,
        request_id: Self::InboundOpenInfo,
    ) {
        match protocol {
            // We have been asked to act as a relay.
            protocol::RelayRemoteReq::RelayReq((incoming_relay_request, notifyee)) => {
                self.alive_lend_out_substreams.push(notifyee);
                self.queued_events
                    .push(RelayHandlerEvent::IncomingRelayReq {
                        request_id,
                        src_addr: self.remote_address.clone(),
                        req: incoming_relay_request,
                    });
            }
            // We have been asked to become a destination.
            protocol::RelayRemoteReq::DstReq(dst_request) => {
                self.queued_events
                    .push(RelayHandlerEvent::IncomingDstReq(dst_request));
            }
        }
    }

    fn inject_fully_negotiated_outbound(
        &mut self,
        protocol: <Self::OutboundProtocol as upgrade::OutboundUpgrade<NegotiatedSubstream>>::Output,
        open_info: Self::OutboundOpenInfo,
    ) {
        match protocol {
            // We have successfully negotiated a substream towards a relay.
            EitherOutput::First((substream_to_dest, notifyee)) => {
                let (dst_peer_id, request_id) = match open_info {
                    RelayOutboundOpenInfo::Relay {
                        dst_peer_id,
                        request_id,
                    } => (dst_peer_id, request_id),
                    RelayOutboundOpenInfo::Destination { .. } => unreachable!(
                        "Can not successfully dial a relay when actually dialing a destination."
                    ),
                };

                self.alive_lend_out_substreams.push(notifyee);
                self.queued_events
                    .push(RelayHandlerEvent::OutgoingRelayReqSuccess(
                        dst_peer_id,
                        request_id,
                        substream_to_dest,
                    ));
            }
            // We have successfully asked the node to be a destination.
            EitherOutput::Second((to_dest_substream, from_dst_read_buffer)) => {
                let incoming_relay_req = match open_info {
                    RelayOutboundOpenInfo::Destination {
                        incoming_relay_req, ..
                    } => incoming_relay_req,
                    RelayOutboundOpenInfo::Relay { .. } => unreachable!(
                        "Can not successfully dial a destination when actually dialing a relay."
                    ),
                };
                self.copy_futures
                    .push(incoming_relay_req.fulfill(to_dest_substream, from_dst_read_buffer));
            }
        }
    }

    fn inject_event(&mut self, event: Self::InEvent) {
        match event {
            RelayHandlerIn::UsedForListening(s) => self.used_for_listening = s,
            // Deny a relay request from the node we handle.
            RelayHandlerIn::DenyIncomingRelayReq(req) => {
                self.deny_futures.push(req);
            }
            RelayHandlerIn::AcceptDstReq(request) => self.accept_dst_futures.push(request.accept()),
            RelayHandlerIn::DenyDstReq(request) => self.deny_futures.push(request.deny()),
            // Ask the node we handle to act as a relay.
            RelayHandlerIn::OutgoingRelayReq {
                src_peer_id,
                dst_peer_id,
                request_id,
                dst_addr,
            } => {
                self.outgoing_relay_reqs.push(OutgoingRelayReq {
                    src_peer_id,
                    dst_peer_id,
                    request_id,
                    dst_addr,
                });
            }
            // Ask the node we handle to act as a destination.
            RelayHandlerIn::OutgoingDstReq {
                src_peer_id,
                src_addr,
                src_connection_id,
                request_id,
                incoming_relay_req,
            } => {
                self.outgoing_dst_reqs.push(OutgoingDstReq {
                    src_peer_id,
                    src_addr,
                    src_connection_id,
                    request_id,
                    incoming_relay_req,
                });
            }
        }
    }

    fn inject_listen_upgrade_error(
        &mut self,
        _: Self::InboundOpenInfo,
        error: ProtocolsHandlerUpgrErr<protocol::RelayListenError>,
    ) {
        match error {
            ProtocolsHandlerUpgrErr::Timeout | ProtocolsHandlerUpgrErr::Timer => {}
            ProtocolsHandlerUpgrErr::Upgrade(upgrade::UpgradeError::Select(
                upgrade::NegotiationError::Failed,
            )) => {}
            ProtocolsHandlerUpgrErr::Upgrade(upgrade::UpgradeError::Select(
                upgrade::NegotiationError::ProtocolError(e),
            )) => {
                self.pending_error = Some(ProtocolsHandlerUpgrErr::Upgrade(
                    upgrade::UpgradeError::Select(upgrade::NegotiationError::ProtocolError(e)),
                ));
            }
            ProtocolsHandlerUpgrErr::Upgrade(upgrade::UpgradeError::Apply(error)) => {
                self.pending_error = Some(ProtocolsHandlerUpgrErr::Upgrade(
                    upgrade::UpgradeError::Apply(EitherError::A(error)),
                ))
            }
        }
    }

    fn inject_dial_upgrade_error(
        &mut self,
        open_info: Self::OutboundOpenInfo,
        error: ProtocolsHandlerUpgrErr<
            EitherError<protocol::OutgoingRelayReqError, protocol::OutgoingDstReqError>,
        >,
    ) {
        match open_info {
            RelayOutboundOpenInfo::Relay {
                dst_peer_id,
                request_id,
            } => {
                match error {
                    ProtocolsHandlerUpgrErr::Timeout | ProtocolsHandlerUpgrErr::Timer => {}
                    ProtocolsHandlerUpgrErr::Upgrade(upgrade::UpgradeError::Select(
                        upgrade::NegotiationError::Failed,
                    )) => {}
                    ProtocolsHandlerUpgrErr::Upgrade(upgrade::UpgradeError::Select(
                        upgrade::NegotiationError::ProtocolError(e),
                    )) => {
                        self.pending_error = Some(ProtocolsHandlerUpgrErr::Upgrade(
                            upgrade::UpgradeError::Select(
                                upgrade::NegotiationError::ProtocolError(e),
                            ),
                        ));
                    }
                    ProtocolsHandlerUpgrErr::Upgrade(upgrade::UpgradeError::Apply(
                        EitherError::A(error),
                    )) => match error {
                        protocol::OutgoingRelayReqError::Decode(_)
                        | protocol::OutgoingRelayReqError::Io(_)
                        | protocol::OutgoingRelayReqError::ParseTypeField
                        | protocol::OutgoingRelayReqError::ParseStatusField
                        | protocol::OutgoingRelayReqError::UnexpectedSrcPeerWithStatusType
                        | protocol::OutgoingRelayReqError::UnexpectedDstPeerWithStatusType
                        | protocol::OutgoingRelayReqError::ExpectedStatusType(_) => {
                            self.pending_error = Some(ProtocolsHandlerUpgrErr::Upgrade(
                                upgrade::UpgradeError::Apply(EitherError::B(EitherError::A(error))),
                            ));
                        }
                        protocol::OutgoingRelayReqError::ExpectedSuccessStatus(status) => {
                            match status {
                                circuit_relay::Status::Success => {
                                    unreachable!("Status success was explicitly expected earlier.")
                                }
                                // With either status below there is no reason to stay connected.
                                // Thus terminate the connection.
                                circuit_relay::Status::HopSrcAddrTooLong
                                | circuit_relay::Status::HopSrcMultiaddrInvalid
                                | circuit_relay::Status::MalformedMessage => {
                                    self.pending_error = Some(ProtocolsHandlerUpgrErr::Upgrade(
                                        upgrade::UpgradeError::Apply(EitherError::B(
                                            EitherError::A(error),
                                        )),
                                    ));
                                }
                                // While useless for reaching this particular destination, the
                                // connection to the relay might still proof helpful for other
                                // destinations. Thus do not terminate the connection.
                                circuit_relay::Status::StopSrcAddrTooLong
                                | circuit_relay::Status::StopDstAddrTooLong
                                | circuit_relay::Status::StopSrcMultiaddrInvalid
                                | circuit_relay::Status::StopDstMultiaddrInvalid
                                | circuit_relay::Status::StopRelayRefused
                                | circuit_relay::Status::HopDstAddrTooLong
                                | circuit_relay::Status::HopDstMultiaddrInvalid
                                | circuit_relay::Status::HopNoConnToDst
                                | circuit_relay::Status::HopCantDialDst
                                | circuit_relay::Status::HopCantOpenDstStream
                                | circuit_relay::Status::HopCantSpeakRelay
                                | circuit_relay::Status::HopCantRelayToSelf => {}
                            }
                        }
                    },
                    ProtocolsHandlerUpgrErr::Upgrade(upgrade::UpgradeError::Apply(
                        EitherError::B(_),
                    )) => {
                        unreachable!("Can not receive an OutgoingDstReqError when dialing a relay.")
                    }
                }

                self.queued_events
                    .push(RelayHandlerEvent::OutgoingRelayReqError(
                        dst_peer_id,
                        request_id,
                    ));
            }
            RelayOutboundOpenInfo::Destination {
                src_connection_id,
                incoming_relay_req,
                ..
            } => {
                let err_code = match error {
                    ProtocolsHandlerUpgrErr::Timeout | ProtocolsHandlerUpgrErr::Timer => {
                        circuit_relay::Status::HopCantOpenDstStream
                    }
                    ProtocolsHandlerUpgrErr::Upgrade(upgrade::UpgradeError::Select(
                        upgrade::NegotiationError::Failed,
                    )) => circuit_relay::Status::HopCantSpeakRelay,
                    ProtocolsHandlerUpgrErr::Upgrade(upgrade::UpgradeError::Select(
                        upgrade::NegotiationError::ProtocolError(e),
                    )) => {
                        self.pending_error = Some(ProtocolsHandlerUpgrErr::Upgrade(
                            upgrade::UpgradeError::Select(
                                upgrade::NegotiationError::ProtocolError(e),
                            ),
                        ));
                        circuit_relay::Status::HopCantSpeakRelay
                    }
                    ProtocolsHandlerUpgrErr::Upgrade(upgrade::UpgradeError::Apply(
                        EitherError::A(_),
                    )) => unreachable!(
                        "Can not receive an OutgoingRelayReqError when dialing a destination."
                    ),
                    ProtocolsHandlerUpgrErr::Upgrade(upgrade::UpgradeError::Apply(
                        EitherError::B(error),
                    )) => {
                        match error {
                            protocol::OutgoingDstReqError::Decode(_)
                            | protocol::OutgoingDstReqError::Io(_)
                            | protocol::OutgoingDstReqError::ParseTypeField
                            | protocol::OutgoingDstReqError::ParseStatusField
                            | protocol::OutgoingDstReqError::UnexpectedSrcPeerWithStatusType
                            | protocol::OutgoingDstReqError::UnexpectedDstPeerWithStatusType
                            | protocol::OutgoingDstReqError::ExpectedStatusType(_) => {
                                self.pending_error = Some(ProtocolsHandlerUpgrErr::Upgrade(
                                    upgrade::UpgradeError::Apply(EitherError::B(EitherError::B(
                                        error,
                                    ))),
                                ));
                                circuit_relay::Status::HopCantOpenDstStream
                            }
                            protocol::OutgoingDstReqError::ExpectedSuccessStatus(status) => {
                                match status {
                                    circuit_relay::Status::Success => {
                                        unreachable!(
                                            "Status success was explicitly expected earlier."
                                        )
                                    }
                                    // A destination node returning `Hop.*` status is a protocol
                                    // violation. Thus terminate the connection.
                                    circuit_relay::Status::HopDstAddrTooLong
                                    | circuit_relay::Status::HopDstMultiaddrInvalid
                                    | circuit_relay::Status::HopNoConnToDst
                                    | circuit_relay::Status::HopCantDialDst
                                    | circuit_relay::Status::HopCantOpenDstStream
                                    | circuit_relay::Status::HopCantSpeakRelay
                                    | circuit_relay::Status::HopCantRelayToSelf
                                    | circuit_relay::Status::HopSrcAddrTooLong
                                    | circuit_relay::Status::HopSrcMultiaddrInvalid => {
                                        self.pending_error =
                                            Some(ProtocolsHandlerUpgrErr::Upgrade(
                                                upgrade::UpgradeError::Apply(EitherError::B(
                                                    EitherError::B(error),
                                                )),
                                            ));
                                    }
                                    // With either status below there is no reason to stay connected.
                                    // Thus terminate the connection.
                                    circuit_relay::Status::StopDstAddrTooLong
                                    | circuit_relay::Status::StopDstMultiaddrInvalid
                                    | circuit_relay::Status::MalformedMessage => {
                                        self.pending_error =
                                            Some(ProtocolsHandlerUpgrErr::Upgrade(
                                                upgrade::UpgradeError::Apply(EitherError::B(
                                                    EitherError::B(error),
                                                )),
                                            ));
                                    }
                                    // While useless for reaching this particular destination, the
                                    // connection to the relay might still proof helpful for other
                                    // destinations. Thus do not terminate the connection.
                                    circuit_relay::Status::StopSrcAddrTooLong
                                    | circuit_relay::Status::StopSrcMultiaddrInvalid
                                    | circuit_relay::Status::StopRelayRefused => {}
                                }
                                status
                            }
                        }
                    }
                };

                self.queued_events
                    .push(RelayHandlerEvent::OutgoingDstReqError {
                        src_connection_id,
                        incoming_relay_req_deny_fut: incoming_relay_req.deny(err_code),
                    });
            }
        }
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        self.keep_alive
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        ProtocolsHandlerEvent<
            Self::OutboundProtocol,
            Self::OutboundOpenInfo,
            Self::OutEvent,
            Self::Error,
        >,
    > {
        // Check for a pending (fatal) error.
        if let Some(err) = self.pending_error.take() {
            // The handler will not be polled again by the `Swarm`.
            return Poll::Ready(ProtocolsHandlerEvent::Close(err));
        }

        // Request the remote to act as a relay.
        if !self.outgoing_relay_reqs.is_empty() {
            let OutgoingRelayReq {
                src_peer_id,
                dst_peer_id,
                request_id,
                dst_addr,
            } = self.outgoing_relay_reqs.remove(0);
            self.outgoing_relay_reqs.shrink_to_fit();
            return Poll::Ready(ProtocolsHandlerEvent::OutboundSubstreamRequest {
                protocol: SubstreamProtocol::new(
                    upgrade::EitherUpgrade::A(protocol::OutgoingRelayReq::new(
                        src_peer_id,
                        dst_peer_id,
                        dst_addr,
                    )),
                    RelayOutboundOpenInfo::Relay {
                        dst_peer_id,
                        request_id,
                    },
                ),
            });
        }

        // Request the remote to act as destination.
        if !self.outgoing_dst_reqs.is_empty() {
            let OutgoingDstReq {
                src_peer_id,
                src_addr,
                src_connection_id,
                request_id,
                incoming_relay_req,
            } = self.outgoing_dst_reqs.remove(0);
            self.outgoing_dst_reqs.shrink_to_fit();
            return Poll::Ready(ProtocolsHandlerEvent::OutboundSubstreamRequest {
                protocol: SubstreamProtocol::new(
                    upgrade::EitherUpgrade::B(protocol::OutgoingDstReq::new(
                        src_peer_id,
                        src_addr,
                        incoming_relay_req.dst_peer().clone(),
                    )),
                    RelayOutboundOpenInfo::Destination {
                        src_peer_id,
                        request_id,
                        src_connection_id,
                        incoming_relay_req,
                    },
                ),
            });
        }

        match self.accept_dst_futures.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok((src_peer_id, substream, notifyee)))) => {
                self.alive_lend_out_substreams.push(notifyee);
                let event = RelayHandlerEvent::IncomingDstReqSuccess {
                    stream: substream,
                    src_peer_id,
                    relay_peer_id: self.remote_peer_id,
                    relay_addr: self.remote_address.clone(),
                };
                return Poll::Ready(ProtocolsHandlerEvent::Custom(event));
            }
            Poll::Ready(Some(Err(e))) => {
                log::debug!("Failed to accept destination future: {:?}", e);
            }
            Poll::Ready(None) => {}
            Poll::Pending => {}
        }

        while let Poll::Ready(Some(result)) = self.copy_futures.poll_next_unpin(cx) {
            if let Err(e) = result {
                warn!("Incoming relay request failed: {:?}", e);
            }
        }

        while let Poll::Ready(Some(result)) = self.deny_futures.poll_next_unpin(cx) {
            if let Err(e) = result {
                warn!("Denying request failed: {:?}", e);
            }
        }

        // Report the queued events.
        if !self.queued_events.is_empty() {
            let event = self.queued_events.remove(0);
            return Poll::Ready(ProtocolsHandlerEvent::Custom(event));
        }

        while let Poll::Ready(Some(Err(Canceled))) =
            self.alive_lend_out_substreams.poll_next_unpin(cx)
        {}

        if self.used_for_listening
            || !self.deny_futures.is_empty()
            || !self.accept_dst_futures.is_empty()
            || !self.copy_futures.is_empty()
            || !self.alive_lend_out_substreams.is_empty()
        {
            // Protocol handler is busy.
            self.keep_alive = KeepAlive::Yes;
        } else {
            // Protocol handler is idle.
            if matches!(self.keep_alive, KeepAlive::Yes) {
                self.keep_alive =
                    KeepAlive::Until(Instant::now() + self.config.connection_idle_timeout);
            }
        }

        Poll::Pending
    }
}

pub enum RelayOutboundOpenInfo {
    Relay {
        dst_peer_id: PeerId,
        request_id: RequestId,
    },
    Destination {
        src_peer_id: PeerId,
        src_connection_id: ConnectionId,
        request_id: RequestId,
        incoming_relay_req: protocol::IncomingRelayReq,
    },
}
