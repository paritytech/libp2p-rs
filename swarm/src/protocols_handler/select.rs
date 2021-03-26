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

use crate::upgrade::{SendWrapper, InboundUpgradeSend, OutboundUpgradeSend};
use crate::protocols_handler::{
    KeepAlive,
    SubstreamProtocol,
    IntoProtocolsHandler,
    ProtocolsHandler,
    ProtocolsHandlerEvent,
    ProtocolsHandlerUpgrErr,
};

use libp2p_core::{
    ConnectedPoint,
    Multiaddr,
    PeerId,
    either::{EitherError, EitherOutput},
    upgrade::{EitherUpgrade, SelectUpgrade, UpgradeError, NegotiationError, ProtocolError}
};
use std::{cmp, task::Context, task::Poll};

/// Implementation of `IntoProtocolsHandler` that combines two protocols into one.
#[derive(Debug, Clone)]
pub struct IntoProtocolsHandlerSelect<TProto1, TProto2> {
    /// The first protocol.
    proto1: TProto1,
    /// The second protocol.
    proto2: TProto2,
}

impl<TProto1, TProto2> IntoProtocolsHandlerSelect<TProto1, TProto2> {
    /// Builds a `IntoProtocolsHandlerSelect`.
    pub(crate) fn new(proto1: TProto1, proto2: TProto2) -> Self {
        IntoProtocolsHandlerSelect {
            proto1,
            proto2,
        }
    }
}

impl<TProto1, TProto2> IntoProtocolsHandler for IntoProtocolsHandlerSelect<TProto1, TProto2>
where
    TProto1: IntoProtocolsHandler,
    TProto2: IntoProtocolsHandler,
{
    type Handler = ProtocolsHandlerSelect<TProto1::Handler, TProto2::Handler>;

    fn into_handler(self, remote_peer_id: &PeerId, connected_point: &ConnectedPoint) -> Self::Handler {
        ProtocolsHandlerSelect {
            proto1: self.proto1.into_handler(remote_peer_id, connected_point),
            proto2: self.proto2.into_handler(remote_peer_id, connected_point),
        }
    }

    fn inbound_protocol(&self) -> <Self::Handler as ProtocolsHandler>::InboundProtocol {
        SelectUpgrade::new(SendWrapper(self.proto1.inbound_protocol()), SendWrapper(self.proto2.inbound_protocol()))
    }
}

/// Implementation of `ProtocolsHandler` that combines two protocols into one.
#[derive(Debug, Clone)]
pub struct ProtocolsHandlerSelect<TProto1, TProto2> {
    /// The first protocol.
    proto1: TProto1,
    /// The second protocol.
    proto2: TProto2,
}

impl<TProto1, TProto2> ProtocolsHandlerSelect<TProto1, TProto2> {
    /// Builds a `ProtocolsHandlerSelect`.
    pub(crate) fn new(proto1: TProto1, proto2: TProto2) -> Self {
        ProtocolsHandlerSelect {
            proto1,
            proto2,
        }
    }
}

impl<TProto1, TProto2> ProtocolsHandler for ProtocolsHandlerSelect<TProto1, TProto2>
where
    TProto1: ProtocolsHandler,
    TProto2: ProtocolsHandler,
{
    type InEvent = EitherOutput<TProto1::InEvent, TProto2::InEvent>;
    type OutEvent = EitherOutput<TProto1::OutEvent, TProto2::OutEvent>;
    type Error = EitherError<TProto1::Error, TProto2::Error>;
    type InboundProtocol = SelectUpgrade<SendWrapper<<TProto1 as ProtocolsHandler>::InboundProtocol>, SendWrapper<<TProto2 as ProtocolsHandler>::InboundProtocol>>;
    type OutboundProtocol = EitherUpgrade<SendWrapper<TProto1::OutboundProtocol>, SendWrapper<TProto2::OutboundProtocol>>;
    type OutboundOpenInfo = EitherOutput<TProto1::OutboundOpenInfo, TProto2::OutboundOpenInfo>;
    type InboundOpenInfo = (TProto1::InboundOpenInfo, TProto2::InboundOpenInfo);

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        let proto1 = self.proto1.listen_protocol();
        let proto2 = self.proto2.listen_protocol();
        let timeout = *std::cmp::max(proto1.timeout(), proto2.timeout());
        let (u1, i1) = proto1.into_upgrade();
        let (u2, i2) = proto2.into_upgrade();
        let choice = SelectUpgrade::new(SendWrapper(u1), SendWrapper(u2));
        SubstreamProtocol::new(choice, (i1, i2)).with_timeout(timeout)
    }

    fn inject_fully_negotiated_outbound(&mut self, protocol: <Self::OutboundProtocol as OutboundUpgradeSend>::Output, endpoint: Self::OutboundOpenInfo) {
        match (protocol, endpoint) {
            (EitherOutput::First(protocol), EitherOutput::First(info)) =>
                self.proto1.inject_fully_negotiated_outbound(protocol, info),
            (EitherOutput::Second(protocol), EitherOutput::Second(info)) =>
                self.proto2.inject_fully_negotiated_outbound(protocol, info),
            (EitherOutput::First(_), EitherOutput::Second(_)) =>
                panic!("wrong API usage: the protocol doesn't match the upgrade info"),
            (EitherOutput::Second(_), EitherOutput::First(_)) =>
                panic!("wrong API usage: the protocol doesn't match the upgrade info")
        }
    }

    fn inject_fully_negotiated_inbound(&mut self, protocol: <Self::InboundProtocol as InboundUpgradeSend>::Output, (i1, i2): Self::InboundOpenInfo) {
        match protocol {
            EitherOutput::First(protocol) =>
                self.proto1.inject_fully_negotiated_inbound(protocol, i1),
            EitherOutput::Second(protocol) =>
                self.proto2.inject_fully_negotiated_inbound(protocol, i2)
        }
    }

    fn inject_event(&mut self, event: Self::InEvent) {
        match event {
            EitherOutput::First(event) => self.proto1.inject_event(event),
            EitherOutput::Second(event) => self.proto2.inject_event(event),
        }
    }

    fn inject_address_change(&mut self, new_address: &Multiaddr) {
        self.proto1.inject_address_change(new_address);
        self.proto2.inject_address_change(new_address)
    }

    fn inject_dial_upgrade_error(&mut self, info: Self::OutboundOpenInfo, error: ProtocolsHandlerUpgrErr<<Self::OutboundProtocol as OutboundUpgradeSend>::Error>) {
        match (info, error) {
            (EitherOutput::First(info), ProtocolsHandlerUpgrErr::Timer) => {
                self.proto1.inject_dial_upgrade_error(info, ProtocolsHandlerUpgrErr::Timer)
            },
            (EitherOutput::First(info), ProtocolsHandlerUpgrErr::Timeout) => {
                self.proto1.inject_dial_upgrade_error(info, ProtocolsHandlerUpgrErr::Timeout)
            },
            (EitherOutput::First(info), ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Select(err))) => {
                self.proto1.inject_dial_upgrade_error(info, ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Select(err)))
            },
            (EitherOutput::First(info), ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Apply(EitherError::A(err)))) => {
                self.proto1.inject_dial_upgrade_error(info, ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Apply(err)))
            },
            (EitherOutput::First(_), ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Apply(EitherError::B(_)))) => {
                panic!("Wrong API usage; the upgrade error doesn't match the outbound open info");
            },
            (EitherOutput::Second(info), ProtocolsHandlerUpgrErr::Timeout) => {
                self.proto2.inject_dial_upgrade_error(info, ProtocolsHandlerUpgrErr::Timeout)
            },
            (EitherOutput::Second(info), ProtocolsHandlerUpgrErr::Timer) => {
                self.proto2.inject_dial_upgrade_error(info, ProtocolsHandlerUpgrErr::Timer)
            },
            (EitherOutput::Second(info), ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Select(err))) => {
                self.proto2.inject_dial_upgrade_error(info, ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Select(err)))
            },
            (EitherOutput::Second(info), ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Apply(EitherError::B(err)))) => {
                self.proto2.inject_dial_upgrade_error(info, ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Apply(err)))
            },
            (EitherOutput::Second(_), ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Apply(EitherError::A(_)))) => {
                panic!("Wrong API usage; the upgrade error doesn't match the outbound open info");
            },
        }
    }

    fn inject_listen_upgrade_error(&mut self, (i1, i2): Self::InboundOpenInfo, error: ProtocolsHandlerUpgrErr<<Self::InboundProtocol as InboundUpgradeSend>::Error>) {
        match error {
            ProtocolsHandlerUpgrErr::Timer => {
                self.proto1.inject_listen_upgrade_error(i1, ProtocolsHandlerUpgrErr::Timer);
                self.proto2.inject_listen_upgrade_error(i2, ProtocolsHandlerUpgrErr::Timer)
            }
            ProtocolsHandlerUpgrErr::Timeout => {
                self.proto1.inject_listen_upgrade_error(i1, ProtocolsHandlerUpgrErr::Timeout);
                self.proto2.inject_listen_upgrade_error(i2, ProtocolsHandlerUpgrErr::Timeout)
            }
            ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Select(NegotiationError::Failed)) => {
                self.proto1.inject_listen_upgrade_error(i1, ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Select(NegotiationError::Failed)));
                self.proto2.inject_listen_upgrade_error(i2, ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Select(NegotiationError::Failed)));
            }
            ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Select(NegotiationError::ProtocolError(e))) => {
                let (e1, e2);
                match e {
                    ProtocolError::IoError(e) => {
                        e1 = NegotiationError::ProtocolError(ProtocolError::IoError(e.kind().into()));
                        e2 = NegotiationError::ProtocolError(ProtocolError::IoError(e))
                    }
                    ProtocolError::InvalidMessage => {
                        e1 = NegotiationError::ProtocolError(ProtocolError::InvalidMessage);
                        e2 = NegotiationError::ProtocolError(ProtocolError::InvalidMessage)
                    }
                    ProtocolError::InvalidProtocol => {
                        e1 = NegotiationError::ProtocolError(ProtocolError::InvalidProtocol);
                        e2 = NegotiationError::ProtocolError(ProtocolError::InvalidProtocol)
                    }
                    ProtocolError::TooManyProtocols => {
                        e1 = NegotiationError::ProtocolError(ProtocolError::TooManyProtocols);
                        e2 = NegotiationError::ProtocolError(ProtocolError::TooManyProtocols)
                    }
                }
                self.proto1.inject_listen_upgrade_error(i1, ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Select(e1)));
                self.proto2.inject_listen_upgrade_error(i2, ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Select(e2)))
            }
            ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Apply(EitherError::A(e))) => {
                self.proto1.inject_listen_upgrade_error(i1, ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Apply(e)))
            }
            ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Apply(EitherError::B(e))) => {
                self.proto2.inject_listen_upgrade_error(i2, ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Apply(e)))
            }
        }
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        cmp::max(self.proto1.connection_keep_alive(), self.proto2.connection_keep_alive())
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<ProtocolsHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::OutEvent, Self::Error>> {
        match self.proto1.poll(cx) {
            Poll::Ready(ProtocolsHandlerEvent::Custom(event)) => {
                return Poll::Ready(ProtocolsHandlerEvent::Custom(EitherOutput::First(event)));
            },
            Poll::Ready(ProtocolsHandlerEvent::Close(event)) => {
                return Poll::Ready(ProtocolsHandlerEvent::Close(EitherError::A(event)));
            },
            Poll::Ready(ProtocolsHandlerEvent::OutboundSubstreamRequest { protocol }) => {
                return Poll::Ready(ProtocolsHandlerEvent::OutboundSubstreamRequest {
                    protocol: protocol
                        .map_upgrade(|u| EitherUpgrade::A(SendWrapper(u)))
                        .map_info(EitherOutput::First)
                });
            },
            Poll::Pending => ()
        };

        match self.proto2.poll(cx) {
            Poll::Ready(ProtocolsHandlerEvent::Custom(event)) => {
                return Poll::Ready(ProtocolsHandlerEvent::Custom(EitherOutput::Second(event)));
            },
            Poll::Ready(ProtocolsHandlerEvent::Close(event)) => {
                return Poll::Ready(ProtocolsHandlerEvent::Close(EitherError::B(event)));
            },
            Poll::Ready(ProtocolsHandlerEvent::OutboundSubstreamRequest { protocol }) => {
                return Poll::Ready(ProtocolsHandlerEvent::OutboundSubstreamRequest {
                    protocol: protocol
                        .map_upgrade(|u| EitherUpgrade::B(SendWrapper(u)))
                        .map_info(EitherOutput::Second)
                });
            },
            Poll::Pending => ()
        };

        Poll::Pending
    }
}
