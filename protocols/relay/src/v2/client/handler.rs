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

use crate::v2::client::transport;
use crate::v2::message_proto::Status;
use crate::v2::protocol::{inbound_stop, outbound_hop};
use futures::channel::mpsc::Sender;
use futures::channel::oneshot;
use futures::future::{BoxFuture, FutureExt};
use futures::stream::{FuturesUnordered, StreamExt};
use futures_timer::Delay;
use libp2p_core::either::EitherError;
use libp2p_core::multiaddr::Protocol;
use libp2p_core::{upgrade, ConnectedPoint, Multiaddr, PeerId};
use libp2p_swarm::protocols_handler::{InboundUpgradeSend, OutboundUpgradeSend};
use libp2p_swarm::{
    IntoProtocolsHandler, KeepAlive, NegotiatedSubstream, ProtocolsHandler, ProtocolsHandlerEvent,
    ProtocolsHandlerUpgrErr, SubstreamProtocol,
};
use log::debug;
use std::collections::VecDeque;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

pub enum In {
    Reserve {
        to_listener: Sender<transport::ToListenerMsg>,
    },
    EstablishCircuit {
        dst_peer_id: PeerId,
        send_back:
            oneshot::Sender<Result<super::RelayedConnection, transport::OutgoingRelayReqError>>,
    },
}

pub enum Event {
    ReservationReqAccepted {
        /// Indicates whether the request replaces an existing reservation.
        renewal: bool,
    },
    ReservationReqFailed {
        /// Indicates whether the request replaces an existing reservation.
        renewal: bool,
    },
    OutboundCircuitReqFailed {},
    /// An inbound circuit request has been denied.
    InboundCircuitReqDenied {
        src_peer_id: PeerId,
    },
    /// Denying an inbound circuit request failed.
    InboundCircuitReqDenyFailed {
        src_peer_id: PeerId,
        error: std::io::Error,
    },
}

pub struct Prototype {
    local_peer_id: PeerId,
}

impl Prototype {
    pub(crate) fn new(local_peer_id: PeerId) -> Self {
        Self { local_peer_id }
    }
}

impl IntoProtocolsHandler for Prototype {
    type Handler = Handler;

    fn into_handler(self, remote_peer_id: &PeerId, endpoint: &ConnectedPoint) -> Self::Handler {
        Handler {
            remote_peer_id: *remote_peer_id,
            remote_addr: endpoint.get_remote_address().clone(),
            local_peer_id: self.local_peer_id,
            queued_events: Default::default(),
            pending_error: Default::default(),
            reservation: None,
            alive_lend_out_substreams: Default::default(),
            circuit_deny_futs: Default::default(),
            keep_alive: KeepAlive::Yes,
        }
    }

    fn inbound_protocol(&self) -> <Self::Handler as ProtocolsHandler>::InboundProtocol {
        inbound_stop::Upgrade {}
    }
}

pub struct Handler {
    local_peer_id: PeerId,
    remote_peer_id: PeerId,
    remote_addr: Multiaddr,
    pending_error: Option<
        ProtocolsHandlerUpgrErr<
            EitherError<inbound_stop::UpgradeError, outbound_hop::UpgradeError>,
        >,
    >,
    /// Until when to keep the connection alive.
    keep_alive: KeepAlive,

    /// Queue of events to return when polled.
    queued_events: VecDeque<
        ProtocolsHandlerEvent<
            <Self as ProtocolsHandler>::OutboundProtocol,
            <Self as ProtocolsHandler>::OutboundOpenInfo,
            <Self as ProtocolsHandler>::OutEvent,
            <Self as ProtocolsHandler>::Error,
        >,
    >,

    reservation: Option<Reservation>,

    /// Tracks substreams lend out to the transport.
    ///
    /// Contains a [`futures::future::Future`] for each lend out substream that
    /// resolves once the substream is dropped.
    ///
    /// Once all substreams are dropped and this handler has no other work,
    /// [`KeepAlive::Until`] can be set, allowing the connection to be closed
    /// eventually.
    alive_lend_out_substreams: FuturesUnordered<oneshot::Receiver<()>>,

    circuit_deny_futs: FuturesUnordered<BoxFuture<'static, (PeerId, Result<(), std::io::Error>)>>,
}

impl ProtocolsHandler for Handler {
    type InEvent = In;
    type OutEvent = Event;
    type Error = ProtocolsHandlerUpgrErr<
        EitherError<inbound_stop::UpgradeError, outbound_hop::UpgradeError>,
    >;
    type InboundProtocol = inbound_stop::Upgrade;
    type OutboundProtocol = outbound_hop::Upgrade;
    type OutboundOpenInfo = OutboundOpenInfo;
    type InboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(inbound_stop::Upgrade {}, ())
    }

    fn inject_fully_negotiated_inbound(
        &mut self,
        inbound_circuit: <Self::InboundProtocol as upgrade::InboundUpgrade<NegotiatedSubstream>>::Output,
        _: Self::InboundOpenInfo,
    ) {
        match &mut self.reservation {
            Some(Reservation::Accepted { pending_msgs, .. })
            | Some(Reservation::Renewal { pending_msgs, .. }) => {
                let src_peer_id = inbound_circuit.src_peer_id();
                let (tx, rx) = oneshot::channel();
                self.alive_lend_out_substreams.push(rx);
                let connection = super::RelayedConnection::new_inbound(inbound_circuit, tx);
                pending_msgs.push_back(transport::ToListenerMsg::IncomingRelayedConnection {
                    stream: connection,
                    src_peer_id,
                    relay_peer_id: self.remote_peer_id,
                    relay_addr: self.remote_addr.clone(),
                })
            }
            _ => {
                let src_peer_id = inbound_circuit.src_peer_id();
                self.circuit_deny_futs.push(
                    inbound_circuit
                        .deny(Status::NoReservation)
                        .map(move |result| (src_peer_id, result))
                        .boxed(),
                )
            }
        }
    }

    fn inject_fully_negotiated_outbound(
        &mut self,
        output: <Self::OutboundProtocol as upgrade::OutboundUpgrade<NegotiatedSubstream>>::Output,
        info: Self::OutboundOpenInfo,
    ) {
        match (output, info) {
            (
                outbound_hop::Output::Reservation {
                    renewal_timeout,
                    addrs,
                },
                OutboundOpenInfo::Reserve { to_listener },
            ) => {
                let (renewal, mut pending_msgs) = match self.reservation.take() {
                    Some(Reservation::Accepted { pending_msgs, .. })
                    | Some(Reservation::Renewal { pending_msgs, .. }) => (true, pending_msgs),
                    None => (false, VecDeque::new()),
                };

                pending_msgs.push_back(transport::ToListenerMsg::Reservation {
                    addrs: addrs
                        .into_iter()
                        .map(|a| a.with(Protocol::P2p(self.local_peer_id.into())))
                        .collect(),
                });
                self.reservation = Some(Reservation::Accepted {
                    renewal_timeout,
                    pending_msgs,
                    to_listener,
                });

                self.queued_events.push_back(ProtocolsHandlerEvent::Custom(
                    Event::ReservationReqAccepted { renewal },
                ));
            }
            (
                outbound_hop::Output::Circuit {
                    substream,
                    read_buffer,
                },
                OutboundOpenInfo::Connect { send_back },
            ) => {
                let (tx, rx) = oneshot::channel();
                self.alive_lend_out_substreams.push(rx);
                let _ = send_back.send(Ok(super::RelayedConnection::new_outbound(
                    substream,
                    read_buffer,
                    tx,
                )));
            }
            _ => unreachable!(),
        }
    }

    fn inject_event(&mut self, event: Self::InEvent) {
        match event {
            In::Reserve { to_listener } => {
                self.queued_events
                    .push_back(ProtocolsHandlerEvent::OutboundSubstreamRequest {
                        protocol: SubstreamProtocol::new(
                            outbound_hop::Upgrade::Reserve,
                            OutboundOpenInfo::Reserve { to_listener },
                        ),
                    });
            }
            In::EstablishCircuit {
                send_back,
                dst_peer_id,
            } => {
                self.queued_events
                    .push_back(ProtocolsHandlerEvent::OutboundSubstreamRequest {
                        protocol: SubstreamProtocol::new(
                            outbound_hop::Upgrade::Connect { dst_peer_id },
                            OutboundOpenInfo::Connect { send_back },
                        ),
                    });
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
        match open_info {
            // TODO: Inform listener or just drop it?
            OutboundOpenInfo::Reserve { to_listener: _ } => {
                let renewal = self.reservation.take().is_some();

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
                    ProtocolsHandlerUpgrErr::Upgrade(upgrade::UpgradeError::Apply(error)) => {
                        match error {
                            outbound_hop::UpgradeError::Decode(_)
                            | outbound_hop::UpgradeError::Io(_)
                            | outbound_hop::UpgradeError::ParseTypeField
                            | outbound_hop::UpgradeError::ParseStatusField
                            | outbound_hop::UpgradeError::MissingStatusField
                            | outbound_hop::UpgradeError::MissingReservationField
                            | outbound_hop::UpgradeError::NoAddressesinReservation
                            | outbound_hop::UpgradeError::InvalidReservationExpiration
                            | outbound_hop::UpgradeError::InvalidReservationAddrs
                            | outbound_hop::UpgradeError::UnexpectedTypeConnect
                            | outbound_hop::UpgradeError::UnexpectedTypeReserve => {
                                self.pending_error = Some(ProtocolsHandlerUpgrErr::Upgrade(
                                    upgrade::UpgradeError::Apply(EitherError::B(error)),
                                ));
                            }
                            outbound_hop::UpgradeError::UnexpectedStatus(status) => {
                                match status {
                                    Status::Ok => {
                                        unreachable!(
                                            "Status success was explicitly expected earlier."
                                        )
                                    }
                                    // With either status below there is either no reason to stay
                                    // connected or it is a protocol violation.
                                    // Thus terminate the connection.
                                    Status::ConnectionFailed
                                    | Status::NoReservation
                                    | Status::PermissionDenied
                                    | Status::UnexpectedMessage
                                    | Status::MalformedMessage => {
                                        self.pending_error =
                                            Some(ProtocolsHandlerUpgrErr::Upgrade(
                                                upgrade::UpgradeError::Apply(EitherError::B(error)),
                                            ));
                                    }
                                    // The connection to the relay might still proof helpful CONNECT
                                    // requests. Thus do not terminate the connection.
                                    Status::ReservationRefused | Status::ResourceLimitExceeded => {}
                                }
                            }
                        }
                    }
                }

                self.queued_events.push_back(ProtocolsHandlerEvent::Custom(
                    Event::ReservationReqFailed { renewal },
                ));
            }
            // TODO: Should we inform listenerupgrade via send_back?
            OutboundOpenInfo::Connect { send_back: _ } => {
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
                    ProtocolsHandlerUpgrErr::Upgrade(upgrade::UpgradeError::Apply(error)) => {
                        match error {
                            outbound_hop::UpgradeError::Decode(_)
                            | outbound_hop::UpgradeError::Io(_)
                            | outbound_hop::UpgradeError::ParseTypeField
                            | outbound_hop::UpgradeError::ParseStatusField
                            | outbound_hop::UpgradeError::MissingStatusField
                            | outbound_hop::UpgradeError::MissingReservationField
                            | outbound_hop::UpgradeError::NoAddressesinReservation
                            | outbound_hop::UpgradeError::InvalidReservationExpiration
                            | outbound_hop::UpgradeError::InvalidReservationAddrs
                            | outbound_hop::UpgradeError::UnexpectedTypeConnect
                            | outbound_hop::UpgradeError::UnexpectedTypeReserve => {
                                self.pending_error = Some(ProtocolsHandlerUpgrErr::Upgrade(
                                    upgrade::UpgradeError::Apply(EitherError::B(error)),
                                ));
                            }
                            outbound_hop::UpgradeError::UnexpectedStatus(status) => {
                                match status {
                                    Status::Ok => {
                                        unreachable!(
                                            "Status success was explicitly expected earlier."
                                        )
                                    }
                                    // With either status below there is either no reason to stay
                                    // connected or it is a protocol violation.
                                    // Thus terminate the connection.
                                    Status::ReservationRefused
                                    | Status::UnexpectedMessage
                                    | Status::MalformedMessage => {
                                        self.pending_error =
                                            Some(ProtocolsHandlerUpgrErr::Upgrade(
                                                upgrade::UpgradeError::Apply(EitherError::B(error)),
                                            ));
                                    }
                                    // While useless for reaching this particular destination, the
                                    // connection to the relay might still proof helpful for other
                                    // destinations. Thus do not terminate the connection.
                                    Status::ResourceLimitExceeded
                                    | Status::ConnectionFailed
                                    | Status::NoReservation
                                    | Status::PermissionDenied => {}
                                }
                            }
                        }
                    }
                };

                self.queued_events.push_back(ProtocolsHandlerEvent::Custom(
                    Event::OutboundCircuitReqFailed {},
                ));
            }
        }
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

        // Check if reservation needs renewal.
        match self.reservation.take() {
            Some(Reservation::Accepted {
                mut renewal_timeout,
                pending_msgs,
                to_listener,
            }) => match renewal_timeout.as_mut().map(|t| t.poll_unpin(cx)) {
                Some(Poll::Ready(())) => {
                    self.reservation = Some(Reservation::Renewal { pending_msgs });
                    return Poll::Ready(ProtocolsHandlerEvent::OutboundSubstreamRequest {
                        protocol: SubstreamProtocol::new(
                            outbound_hop::Upgrade::Reserve,
                            OutboundOpenInfo::Reserve { to_listener },
                        ),
                    });
                }
                Some(Poll::Pending) | None => {
                    self.reservation = Some(Reservation::Accepted {
                        renewal_timeout,
                        pending_msgs,
                        to_listener,
                    });
                }
            },
            r => self.reservation = r,
        }

        // Forward messages to transport listener.
        if let Some(Reservation::Accepted {
            pending_msgs,
            to_listener,
            ..
        }) = &mut self.reservation
        {
            if !pending_msgs.is_empty() {
                match to_listener.poll_ready(cx) {
                    Poll::Ready(Ok(())) => {
                        if let Err(e) = to_listener
                            .start_send(pending_msgs.pop_front().expect("Called !is_empty()."))
                        {
                            debug!("Failed to sent pending message to listener: {:?}", e);
                            self.reservation.take();
                        }
                    }
                    Poll::Ready(Err(e)) => {
                        debug!("Channel to listener failed: {:?}", e);
                        self.reservation.take();
                    }
                    Poll::Pending => {}
                }
            }
        }

        // Deny incoming circuit requests.
        while let Poll::Ready(Some((src_peer_id, result))) =
            self.circuit_deny_futs.poll_next_unpin(cx)
        {
            match result {
                Ok(()) => {
                    return Poll::Ready(ProtocolsHandlerEvent::Custom(
                        Event::InboundCircuitReqDenied { src_peer_id },
                    ))
                }
                Err(error) => {
                    return Poll::Ready(ProtocolsHandlerEvent::Custom(
                        Event::InboundCircuitReqDenyFailed { src_peer_id, error },
                    ))
                }
            }
        }

        // Update keep-alive handling.
        if self.reservation.is_none()
            && self.alive_lend_out_substreams.is_empty()
            && self.circuit_deny_futs.is_empty()
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

enum Reservation {
    Accepted {
        /// [`None`] if reservation does not expire.
        renewal_timeout: Option<Delay>,
        pending_msgs: VecDeque<transport::ToListenerMsg>,
        to_listener: Sender<transport::ToListenerMsg>,
    },
    Renewal {
        pending_msgs: VecDeque<transport::ToListenerMsg>,
    },
}

pub enum OutboundOpenInfo {
    Reserve {
        to_listener: Sender<transport::ToListenerMsg>,
    },
    Connect {
        send_back:
            oneshot::Sender<Result<super::RelayedConnection, transport::OutgoingRelayReqError>>,
    },
}
