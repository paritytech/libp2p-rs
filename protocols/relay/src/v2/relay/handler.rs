// Copyright 2021 Protocol Labs.
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

use crate::v2::copy_future::CopyFuture;
use crate::v2::message_proto::Status;
use crate::v2::protocol::{inbound_hop, outbound_stop};
use crate::v2::relay::CircuitId;
use bytes::Bytes;
use futures::channel::oneshot::{self, Canceled};
use futures::future::{BoxFuture, FutureExt, TryFutureExt};
use futures::io::AsyncWriteExt;
use futures::stream::{FuturesUnordered, StreamExt};
use futures_timer::Delay;
use libp2p_core::connection::ConnectionId;
use libp2p_core::either::EitherError;
use libp2p_core::{upgrade, ConnectedPoint, Multiaddr, PeerId};
use libp2p_swarm::protocols_handler::{InboundUpgradeSend, OutboundUpgradeSend};
use libp2p_swarm::{
    IntoProtocolsHandler, KeepAlive, NegotiatedSubstream, ProtocolsHandler, ProtocolsHandlerEvent,
    ProtocolsHandlerUpgrErr, SubstreamProtocol,
};
use std::collections::VecDeque;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

pub struct Config {
    pub reservation_duration: Duration,
    pub max_circuit_duration: Duration,
    pub max_circuit_bytes: u64,
}

pub enum In {
    AcceptReservationReq {
        inbound_reservation_req: inbound_hop::ReservationReq,
        addrs: Vec<Multiaddr>,
    },
    DenyReservationReq {
        inbound_reservation_req: inbound_hop::ReservationReq,
        status: Status,
    },
    DenyCircuitReq {
        circuit_id: Option<CircuitId>,
        inbound_circuit_req: inbound_hop::CircuitReq,
        status: Status,
    },
    NegotiateOutboundConnect {
        circuit_id: CircuitId,
        inbound_circuit_req: inbound_hop::CircuitReq,
        relay_peer_id: PeerId,
        src_peer_id: PeerId,
        src_connection_id: ConnectionId,
    },
    AcceptAndDriveCircuit {
        circuit_id: CircuitId,
        dst_peer_id: PeerId,
        inbound_circuit_req: inbound_hop::CircuitReq,
        dst_handler_notifier: oneshot::Sender<()>,
        dst_stream: NegotiatedSubstream,
        dst_pending_data: Bytes,
    },
}

/// The events produced by the [`Handler`].
pub enum Event {
    /// An inbound reservation request has been received.
    ReservationReqReceived {
        inbound_reservation_req: inbound_hop::ReservationReq,
    },
    /// An inbound reservation request has been accepted.
    ReservationReqAccepted {
        /// Indicates whether the request replaces an existing reservation.
        renewed: bool,
    },
    /// Accepting an inbound reservation request failed.
    ReservationReqAcceptFailed { error: std::io::Error },
    /// An inbound reservation request has been denied.
    ReservationReqDenied {},
    /// Denying an inbound reservation request has failed.
    ReservationReqDenyFailed { error: std::io::Error },
    /// An inbound reservation has timed out.
    ReservationTimedOut {},
    /// An inbound circuit request has been received.
    CircuitReqReceived(inbound_hop::CircuitReq),
    /// An inbound circuit request has been denied.
    CircuitReqDenied {
        circuit_id: Option<CircuitId>,
        dst_peer_id: PeerId,
    },
    /// Denying an inbound circuit request failed.
    CircuitReqDenyFailed {
        circuit_id: Option<CircuitId>,
        dst_peer_id: PeerId,
        error: std::io::Error,
    },
    /// An inbound cirucit request has been accepted.
    CircuitReqAccepted {
        circuit_id: CircuitId,
        dst_peer_id: PeerId,
    },
    /// Accepting an inbound circuit request failed.
    CircuitReqAcceptFailed {
        circuit_id: CircuitId,
        dst_peer_id: PeerId,
        error: std::io::Error,
    },
    /// An outbound substream for an inbound circuit request has been
    /// negotiated.
    OutboundConnectNegotiated {
        circuit_id: CircuitId,
        src_peer_id: PeerId,
        src_connection_id: ConnectionId,
        inbound_circuit_req: inbound_hop::CircuitReq,
        dst_handler_notifier: oneshot::Sender<()>,
        dst_stream: NegotiatedSubstream,
        dst_pending_data: Bytes,
    },
    /// Negotiating an outbound substream for an inbound circuit request failed.
    OutboundConnectNegotiationFailed {
        circuit_id: CircuitId,
        src_peer_id: PeerId,
        src_connection_id: ConnectionId,
        inbound_circuit_req: inbound_hop::CircuitReq,
        status: Status,
    },
    /// An inbound circuit has closed.
    CircuitClosed {
        circuit_id: CircuitId,
        dst_peer_id: PeerId,
        error: Option<std::io::Error>,
    },
}

pub struct Prototype {
    pub config: Config,
}

impl IntoProtocolsHandler for Prototype {
    type Handler = Handler;

    fn into_handler(self, _remote_peer_id: &PeerId, _endpoint: &ConnectedPoint) -> Self::Handler {
        Handler {
            config: self.config,
            queued_events: Default::default(),
            pending_error: Default::default(),
            reservation_accept_futures: Default::default(),
            reservation_deny_futures: Default::default(),
            circuit_accept_futures: Default::default(),
            circuit_deny_futures: Default::default(),
            alive_lend_out_substreams: Default::default(),
            circuits: Default::default(),
            active_reservation: Default::default(),
            keep_alive: KeepAlive::Yes,
        }
    }

    fn inbound_protocol(&self) -> <Self::Handler as ProtocolsHandler>::InboundProtocol {
        inbound_hop::Upgrade {
            reservation_duration: self.config.reservation_duration,
            max_circuit_duration: self.config.max_circuit_duration,
            max_circuit_bytes: self.config.max_circuit_bytes,
        }
    }
}

/// [`ProtocolsHandler`] that manages substreams for a relay on a single
/// connection with a peer.
pub struct Handler {
    /// Static [`Handler`] [`Config`].
    config: Config,

    /// Queue of events to return when polled.
    queued_events: VecDeque<
        ProtocolsHandlerEvent<
            <Self as ProtocolsHandler>::OutboundProtocol,
            <Self as ProtocolsHandler>::OutboundOpenInfo,
            <Self as ProtocolsHandler>::OutEvent,
            <Self as ProtocolsHandler>::Error,
        >,
    >,

    /// A pending fatal error that results in the connection being closed.
    pending_error: Option<
        ProtocolsHandlerUpgrErr<
            EitherError<inbound_hop::UpgradeError, outbound_stop::UpgradeError>,
        >,
    >,

    /// Until when to keep the connection alive.
    keep_alive: KeepAlive,

    /// Futures accepting an inbound reservation request.
    reservation_accept_futures: Futures<Result<(), std::io::Error>>,
    /// Futures denying an inbound reservation request.
    reservation_deny_futures: Futures<Result<(), std::io::Error>>,
    /// Timeout for the currently active reservation.
    active_reservation: Option<Delay>,

    /// Futures accepting an inbound circuit request.
    circuit_accept_futures: Futures<Result<CircuitParts, (CircuitId, PeerId, std::io::Error)>>,
    /// Futures deying an inbound circuit request.
    circuit_deny_futures: Futures<(Option<CircuitId>, PeerId, Result<(), std::io::Error>)>,
    /// Tracks substreams lend out to other [`Handler`]s.
    ///
    /// Contains a [`futures::future::Future`] for each lend out substream that
    /// resolves once the substream is dropped.
    ///
    /// Once all substreams are dropped and this handler has no other work,
    /// [`KeepAlive::Until`] can be set, allowing the connection to be closed
    /// eventually.
    alive_lend_out_substreams: FuturesUnordered<oneshot::Receiver<()>>,
    /// Futures relaying data for circuit between two peers.
    circuits: Futures<(CircuitId, PeerId, Result<(), std::io::Error>)>,
}

type Futures<T> = FuturesUnordered<BoxFuture<'static, T>>;

impl ProtocolsHandler for Handler {
    type InEvent = In;
    type OutEvent = Event;
    type Error = ProtocolsHandlerUpgrErr<
        EitherError<inbound_hop::UpgradeError, outbound_stop::UpgradeError>,
    >;
    type InboundProtocol = inbound_hop::Upgrade;
    type OutboundProtocol = outbound_stop::Upgrade;
    type OutboundOpenInfo = OutboundOpenInfo;
    type InboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(
            inbound_hop::Upgrade {
                reservation_duration: self.config.reservation_duration,
                max_circuit_duration: self.config.max_circuit_duration,
                max_circuit_bytes: self.config.max_circuit_bytes,
            },
            (),
        )
    }

    fn inject_fully_negotiated_inbound(
        &mut self,
        request: <Self::InboundProtocol as upgrade::InboundUpgrade<NegotiatedSubstream>>::Output,
        _request_id: Self::InboundOpenInfo,
    ) {
        match request {
            inbound_hop::Req::Reserve(inbound_reservation_req) => {
                self.queued_events.push_back(ProtocolsHandlerEvent::Custom(
                    Event::ReservationReqReceived {
                        inbound_reservation_req,
                    },
                ));
            }
            inbound_hop::Req::Connect(circuit_req) => {
                self.queued_events.push_back(ProtocolsHandlerEvent::Custom(
                    Event::CircuitReqReceived(circuit_req),
                ));
            }
        }
    }

    fn inject_fully_negotiated_outbound(
        &mut self,
        (dst_stream, dst_pending_data): <Self::OutboundProtocol as upgrade::OutboundUpgrade<
            NegotiatedSubstream,
        >>::Output,
        outbound_open_info: Self::OutboundOpenInfo,
    ) {
        let OutboundOpenInfo {
            circuit_id,
            inbound_circuit_req,
            src_peer_id,
            src_connection_id,
        } = outbound_open_info;
        let (tx, rx) = oneshot::channel();
        self.alive_lend_out_substreams.push(rx);

        self.queued_events.push_back(ProtocolsHandlerEvent::Custom(
            Event::OutboundConnectNegotiated {
                circuit_id,
                src_peer_id,
                src_connection_id,
                inbound_circuit_req,
                dst_handler_notifier: tx,
                dst_stream,
                dst_pending_data,
            },
        ));
    }

    fn inject_event(&mut self, event: Self::InEvent) {
        match event {
            In::AcceptReservationReq {
                inbound_reservation_req,
                addrs,
            } => {
                self.reservation_accept_futures
                    .push(inbound_reservation_req.accept(addrs).boxed());
            }
            In::DenyReservationReq {
                inbound_reservation_req,
                status,
            } => {
                self.reservation_deny_futures
                    .push(inbound_reservation_req.deny(status).boxed());
            }
            In::NegotiateOutboundConnect {
                circuit_id,
                inbound_circuit_req,
                relay_peer_id,
                src_peer_id,
                src_connection_id,
            } => {
                self.queued_events
                    .push_back(ProtocolsHandlerEvent::OutboundSubstreamRequest {
                        protocol: SubstreamProtocol::new(
                            outbound_stop::Upgrade {
                                relay_peer_id,
                                max_circuit_duration: self.config.max_circuit_duration,
                                max_circuit_bytes: self.config.max_circuit_bytes,
                            },
                            OutboundOpenInfo {
                                circuit_id,
                                inbound_circuit_req,
                                src_peer_id,
                                src_connection_id,
                            },
                        ),
                    });
            }
            In::DenyCircuitReq {
                circuit_id,
                inbound_circuit_req,
                status,
            } => {
                let dst_peer_id = inbound_circuit_req.dst();
                self.circuit_deny_futures.push(
                    inbound_circuit_req
                        .deny(status)
                        .map(move |result| (circuit_id, dst_peer_id, result))
                        .boxed(),
                );
            }
            In::AcceptAndDriveCircuit {
                circuit_id,
                dst_peer_id,
                inbound_circuit_req,
                dst_handler_notifier,
                dst_stream,
                dst_pending_data,
            } => {
                self.circuit_accept_futures.push(
                    inbound_circuit_req
                        .accept()
                        .map_ok(move |(src_stream, src_pending_data)| CircuitParts {
                            circuit_id,
                            src_stream,
                            src_pending_data,
                            dst_peer_id,
                            dst_handler_notifier,
                            dst_stream,
                            dst_pending_data,
                        })
                        .map_err(move |e| (circuit_id, dst_peer_id, e))
                        .boxed(),
                );
            }
        }
    }

    fn inject_listen_upgrade_error(
        &mut self,
        _: Self::InboundOpenInfo,
        error: ProtocolsHandlerUpgrErr<<Self::InboundProtocol as InboundUpgradeSend>::Error>,
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
        error: ProtocolsHandlerUpgrErr<<Self::OutboundProtocol as OutboundUpgradeSend>::Error>,
    ) {
        let status = match error {
            ProtocolsHandlerUpgrErr::Timeout | ProtocolsHandlerUpgrErr::Timer => {
                Status::ConnectionFailed
            }
            ProtocolsHandlerUpgrErr::Upgrade(upgrade::UpgradeError::Select(
                upgrade::NegotiationError::Failed,
            )) => {
                // The remote has previously done a reservation. Doing a reservation but not
                // supporting the stop protocol is pointless, thus disconnecting.
                self.pending_error = Some(ProtocolsHandlerUpgrErr::Upgrade(
                    upgrade::UpgradeError::Select(upgrade::NegotiationError::Failed),
                ));
                Status::ConnectionFailed
            }
            ProtocolsHandlerUpgrErr::Upgrade(upgrade::UpgradeError::Select(
                upgrade::NegotiationError::ProtocolError(e),
            )) => {
                self.pending_error = Some(ProtocolsHandlerUpgrErr::Upgrade(
                    upgrade::UpgradeError::Select(upgrade::NegotiationError::ProtocolError(e)),
                ));
                Status::ConnectionFailed
            }
            ProtocolsHandlerUpgrErr::Upgrade(upgrade::UpgradeError::Apply(error)) => {
                match error {
                    outbound_stop::UpgradeError::Decode(_)
                    | outbound_stop::UpgradeError::Io(_)
                    | outbound_stop::UpgradeError::ParseTypeField
                    | outbound_stop::UpgradeError::MissingStatusField
                    | outbound_stop::UpgradeError::ParseStatusField
                    | outbound_stop::UpgradeError::UnexpectedTypeConnect => {
                        self.pending_error = Some(ProtocolsHandlerUpgrErr::Upgrade(
                            upgrade::UpgradeError::Apply(EitherError::B(error)),
                        ));
                        Status::ConnectionFailed
                    }
                    outbound_stop::UpgradeError::UnexpectedStatus(status) => {
                        match status {
                            Status::Ok => {
                                unreachable!("Status success is explicitly exempt.")
                            }
                            // A destination node returning nonsensical status is a protocol
                            // violation. Thus terminate the connection.
                            Status::ReservationRefused
                            | Status::NoReservation
                            | Status::ConnectionFailed => {
                                self.pending_error = Some(ProtocolsHandlerUpgrErr::Upgrade(
                                    upgrade::UpgradeError::Apply(EitherError::B(error)),
                                ));
                            }
                            // With either status below there is no reason to stay connected.
                            // Thus terminate the connection.
                            Status::MalformedMessage | Status::UnexpectedMessage => {
                                self.pending_error = Some(ProtocolsHandlerUpgrErr::Upgrade(
                                    upgrade::UpgradeError::Apply(EitherError::B(error)),
                                ))
                            }
                            // While useless for reaching this particular destination, the
                            // connection to the relay might still proof helpful for other
                            // destinations. Thus do not terminate the connection.
                            Status::ResourceLimitExceeded | Status::PermissionDenied => {}
                        }
                        status
                    }
                }
            }
        };

        let OutboundOpenInfo {
            circuit_id,
            inbound_circuit_req,
            src_peer_id,
            src_connection_id,
        } = open_info;

        self.queued_events.push_back(ProtocolsHandlerEvent::Custom(
            Event::OutboundConnectNegotiationFailed {
                circuit_id,
                src_peer_id,
                src_connection_id,
                inbound_circuit_req,
                status,
            },
        ));
    }

    // TODO: Why is this not a mut reference? If it were the case, we could do all keep alive handling in here.
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

        // Return queued events.
        if let Some(event) = self.queued_events.pop_front() {
            return Poll::Ready(event);
        }

        while let Poll::Ready(Some((circuit_id, dst_peer_id, result))) =
            self.circuits.poll_next_unpin(cx)
        {
            match result {
                Ok(()) => {
                    return Poll::Ready(ProtocolsHandlerEvent::Custom(Event::CircuitClosed {
                        circuit_id,
                        dst_peer_id,
                        error: None,
                    }))
                }
                Err(e) => {
                    return Poll::Ready(ProtocolsHandlerEvent::Custom(Event::CircuitClosed {
                        circuit_id,
                        dst_peer_id,
                        error: Some(e),
                    }))
                }
            }
        }

        while let Poll::Ready(Some(result)) = self.reservation_accept_futures.poll_next_unpin(cx) {
            match result {
                Ok(()) => {
                    let renewed = self
                        .active_reservation
                        .replace(Delay::new(self.config.reservation_duration))
                        .is_some();
                    return Poll::Ready(ProtocolsHandlerEvent::Custom(
                        Event::ReservationReqAccepted { renewed },
                    ));
                }
                Err(error) => {
                    return Poll::Ready(ProtocolsHandlerEvent::Custom(
                        Event::ReservationReqAcceptFailed { error },
                    ));
                }
            }
        }

        while let Poll::Ready(Some(result)) = self.reservation_deny_futures.poll_next_unpin(cx) {
            match result {
                Ok(()) => {
                    return Poll::Ready(ProtocolsHandlerEvent::Custom(
                        Event::ReservationReqDenied {},
                    ))
                }
                Err(error) => {
                    return Poll::Ready(ProtocolsHandlerEvent::Custom(
                        Event::ReservationReqDenyFailed { error },
                    ));
                }
            }
        }

        while let Poll::Ready(Some(result)) = self.circuit_accept_futures.poll_next_unpin(cx) {
            match result {
                Ok(parts) => {
                    let CircuitParts {
                        circuit_id,
                        mut src_stream,
                        src_pending_data,
                        dst_peer_id,
                        dst_handler_notifier,
                        mut dst_stream,
                        dst_pending_data,
                    } = parts;
                    let max_circuit_duration = self.config.max_circuit_duration;
                    let max_circuit_bytes = self.config.max_circuit_bytes;

                    let circuit = async move {
                        let (result_1, result_2) = futures::future::join(
                            src_stream.write_all(&dst_pending_data),
                            dst_stream.write_all(&src_pending_data),
                        )
                        .await;
                        result_1?;
                        result_2?;

                        CopyFuture::new(
                            src_stream,
                            dst_stream,
                            max_circuit_duration,
                            max_circuit_bytes,
                        )
                        .await?;

                        // Inform destination handler that the stream to the destination is dropped.
                        drop(dst_handler_notifier);
                        Ok(())
                    }
                    .map(move |r| (circuit_id, dst_peer_id, r))
                    .boxed();

                    self.circuits.push(circuit);

                    return Poll::Ready(ProtocolsHandlerEvent::Custom(Event::CircuitReqAccepted {
                        circuit_id,
                        dst_peer_id,
                    }));
                }
                Err((circuit_id, dst_peer_id, error)) => {
                    return Poll::Ready(ProtocolsHandlerEvent::Custom(
                        Event::CircuitReqAcceptFailed {
                            circuit_id,
                            dst_peer_id,
                            error,
                        },
                    ));
                }
            }
        }

        while let Poll::Ready(Some((circuit_id, dst_peer_id, result))) =
            self.circuit_deny_futures.poll_next_unpin(cx)
        {
            match result {
                Ok(()) => {
                    return Poll::Ready(ProtocolsHandlerEvent::Custom(Event::CircuitReqDenied {
                        circuit_id,
                        dst_peer_id,
                    }));
                }
                Err(error) => {
                    return Poll::Ready(ProtocolsHandlerEvent::Custom(
                        Event::CircuitReqDenyFailed {
                            circuit_id,
                            dst_peer_id,
                            error,
                        },
                    ));
                }
            }
        }

        while let Poll::Ready(Some(Err(Canceled))) =
            self.alive_lend_out_substreams.poll_next_unpin(cx)
        {}

        if let Some(Poll::Ready(())) = self
            .active_reservation
            .as_mut()
            .map(|fut| fut.poll_unpin(cx))
        {
            self.active_reservation = None;
            return Poll::Ready(ProtocolsHandlerEvent::Custom(Event::ReservationTimedOut {}));
        }

        if self.reservation_accept_futures.is_empty()
            && self.reservation_deny_futures.is_empty()
            && self.circuit_accept_futures.is_empty()
            && self.circuit_deny_futures.is_empty()
            && self.alive_lend_out_substreams.is_empty()
            && self.circuits.is_empty()
            && self.active_reservation.is_none()
        {
            match self.keep_alive {
                KeepAlive::Yes => {
                    self.keep_alive = KeepAlive::Until(Instant::now() + Duration::from_secs(10));
                }
                KeepAlive::Until(_) => {}
                KeepAlive::No => panic!("Handler never sets KeepAlive::No."),
            }
        } else {
            self.keep_alive = KeepAlive::Yes;
        }

        Poll::Pending
    }
}

pub struct OutboundOpenInfo {
    circuit_id: CircuitId,
    inbound_circuit_req: inbound_hop::CircuitReq,
    src_peer_id: PeerId,
    src_connection_id: ConnectionId,
}

pub struct CircuitParts {
    circuit_id: CircuitId,
    src_stream: NegotiatedSubstream,
    src_pending_data: Bytes,
    dst_peer_id: PeerId,
    dst_handler_notifier: oneshot::Sender<()>,
    dst_stream: NegotiatedSubstream,
    dst_pending_data: Bytes,
}
