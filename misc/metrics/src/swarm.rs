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

use open_metrics_client::encoding::text::Encode;
use open_metrics_client::metrics::counter::Counter;
use open_metrics_client::metrics::family::Family;
use open_metrics_client::registry::Registry;

pub struct Metrics {
    connections_incoming: Counter,
    connections_incoming_error: Family<IncomingConnectionErrorLabels, Counter>,

    connections_established: Family<ConnectionEstablishedLabeles, Counter>,
    connections_closed: Counter,

    new_listen_addr: Counter,
    expired_listen_addr: Counter,

    listener_closed: Counter,
    listener_error: Counter,

    dial_attempt: Counter,
    dial_unreachable_addr: Family<Vec<(String, String)>, Counter>,
    connected_to_banned_peer: Counter,
}

impl Metrics {
    pub fn new(registry: &mut Registry) -> Self {
        let sub_registry = registry.sub_registry("swarm");

        let connections_incoming = Counter::default();
        sub_registry.register(
            "connections_incoming",
            "Number of incoming connections",
            Box::new(connections_incoming.clone()),
        );

        let connections_incoming_error = Family::default();
        sub_registry.register(
            "connections_incoming_error",
            "Number of incoming connection errors",
            Box::new(connections_incoming_error.clone()),
        );

        let new_listen_addr = Counter::default();
        sub_registry.register(
            "new_listen_addr",
            "Number of new listen addresses",
            Box::new(new_listen_addr.clone()),
        );

        let expired_listen_addr = Counter::default();
        sub_registry.register(
            "expired_listen_addr",
            "Number of expired listen addresses",
            Box::new(expired_listen_addr.clone()),
        );

        let listener_closed = Counter::default();
        sub_registry.register(
            "listener_closed",
            "Number of listeners closed",
            Box::new(listener_closed.clone()),
        );

        let listener_error = Counter::default();
        sub_registry.register(
            "listener_error",
            "Number of listener errors",
            Box::new(listener_error.clone()),
        );

        let dial_attempt = Counter::default();
        sub_registry.register(
            "dial_attempt",
            "Number of dial attempts",
            Box::new(dial_attempt.clone()),
        );

        let dial_unreachable_addr = Family::default();
        sub_registry.register(
            "dial_unreachable_addr",
            "Number of unreachable addresses dialed",
            Box::new(dial_unreachable_addr.clone()),
        );

        let connected_to_banned_peer = Counter::default();
        sub_registry.register(
            "connected_to_banned_peer",
            "Number of connection attempts to banned peer",
            Box::new(connected_to_banned_peer.clone()),
        );

        let connections_established = Family::default();
        sub_registry.register(
            "connections_established",
            "Number of connections established",
            Box::new(connections_established.clone()),
        );

        let connections_closed = Counter::default();
        sub_registry.register(
            "connections_closed",
            "Number of connections closed",
            Box::new(connections_closed.clone()),
        );

        Self {
            connections_incoming,
            connections_incoming_error,
            connections_established,
            connections_closed,
            new_listen_addr,
            expired_listen_addr,
            listener_closed,
            listener_error,
            dial_attempt,
            dial_unreachable_addr,
            connected_to_banned_peer,
        }
    }
}

impl<TBvEv, THandleErr> super::Recorder<libp2p_swarm::SwarmEvent<TBvEv, THandleErr>>
    for super::Metrics
{
    fn record(&self, event: &libp2p_swarm::SwarmEvent<TBvEv, THandleErr>) {
        match event {
            libp2p_swarm::SwarmEvent::Behaviour(_) => {}
            libp2p_swarm::SwarmEvent::ConnectionEstablished { endpoint, .. } => {
                self.swarm
                    .connections_established
                    .get_or_create(&ConnectionEstablishedLabeles {
                        role: endpoint.into(),
                    })
                    .inc();
            }
            libp2p_swarm::SwarmEvent::ConnectionClosed { .. } => {
                self.swarm.connections_closed.inc();
            }
            libp2p_swarm::SwarmEvent::IncomingConnection { .. } => {
                self.swarm.connections_incoming.inc();
            }
            libp2p_swarm::SwarmEvent::IncomingConnectionError { error, .. } => {
                self.swarm
                    .connections_incoming_error
                    .get_or_create(&IncomingConnectionErrorLabels {
                        error: error.into(),
                    })
                    .inc();
            }
            libp2p_swarm::SwarmEvent::BannedPeer { .. } => {
                self.swarm.connected_to_banned_peer.inc();
            }
            libp2p_swarm::SwarmEvent::UnreachableAddr { .. } => {
                self.swarm
                    .dial_unreachable_addr
                    .get_or_create(&vec![("peer".into(), "known".into())])
                    .inc();
            }
            libp2p_swarm::SwarmEvent::UnknownPeerUnreachableAddr { .. } => {
                self.swarm
                    .dial_unreachable_addr
                    .get_or_create(&vec![("peer".into(), "unknown".into())])
                    .inc();
            }
            libp2p_swarm::SwarmEvent::NewListenAddr(_) => {
                self.swarm.new_listen_addr.inc();
            }
            libp2p_swarm::SwarmEvent::ExpiredListenAddr(_) => {
                self.swarm.expired_listen_addr.inc();
            }
            libp2p_swarm::SwarmEvent::ListenerClosed { .. } => {
                self.swarm.listener_closed.inc();
            }
            libp2p_swarm::SwarmEvent::ListenerError { .. } => {
                self.swarm.listener_error.inc();
            }
            libp2p_swarm::SwarmEvent::Dialing(_) => {
                self.swarm.dial_attempt.inc();
            }
        }
    }
}

#[derive(Encode, Hash, Clone, Eq, PartialEq)]
struct ConnectionEstablishedLabeles {
    role: Role,
}

#[derive(Encode, Hash, Clone, Eq, PartialEq)]
enum Role {
    Dialer,
    Listener,
}

impl From<&libp2p_core::ConnectedPoint> for Role {
    fn from(point: &libp2p_core::ConnectedPoint) -> Self {
        match point {
            libp2p_core::ConnectedPoint::Dialer { .. } => Role::Dialer,
            libp2p_core::ConnectedPoint::Listener { .. } => Role::Listener,
        }
    }
}

#[derive(Encode, Hash, Clone, Eq, PartialEq)]
struct IncomingConnectionErrorLabels {
    error: PendingConnectionError,
}

#[derive(Encode, Hash, Clone, Eq, PartialEq)]
enum PendingConnectionError {
    InvalidPeerId,
    TransportErrorMultiaddrNotSupported,
    TransportErrorOther,
    ConnectionLimit,
    Io,
}

impl<TTransErr> From<&libp2p_core::connection::PendingConnectionError<TTransErr>>
    for PendingConnectionError
{
    fn from(point: &libp2p_core::connection::PendingConnectionError<TTransErr>) -> Self {
        match point {
            libp2p_core::connection::PendingConnectionError::InvalidPeerId => {
                PendingConnectionError::InvalidPeerId
            }
            libp2p_core::connection::PendingConnectionError::Transport(
                libp2p_core::transport::TransportError::MultiaddrNotSupported(_),
            ) => PendingConnectionError::TransportErrorMultiaddrNotSupported,
            libp2p_core::connection::PendingConnectionError::Transport(
                libp2p_core::transport::TransportError::Other(_),
            ) => PendingConnectionError::TransportErrorOther,
            libp2p_core::connection::PendingConnectionError::ConnectionLimit(_) => {
                PendingConnectionError::ConnectionLimit
            }
            libp2p_core::connection::PendingConnectionError::IO(_) => PendingConnectionError::Io,
        }
    }
}
