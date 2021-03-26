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

use crate::behaviour::{BehaviourToListenerMsg, OutgoingRelayReqError};
use crate::protocol;
use crate::RequestId;
use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::future::{BoxFuture, Future, FutureExt};
use futures::sink::SinkExt;
use futures::stream::{Stream, StreamExt};
use libp2p_core::either::{EitherError, EitherFuture, EitherOutput};
use libp2p_core::multiaddr::{Multiaddr, Protocol};
use libp2p_core::transport::{ListenerEvent, TransportError};
use libp2p_core::{PeerId, Transport};
use pin_project::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A [`Transport`] wrapping another [`Transport`] enabling relay capabilities.
///
/// Allows the local node to:
///
/// 1. Use inner wrapped transport as before.
///
///    ```
///    # use libp2p_core::{Multiaddr, multiaddr::{Protocol}, Transport};
///    # use libp2p_core::transport::memory::MemoryTransport;
///    # use libp2p_relay::{RelayConfig, new_transport_and_behaviour};
///    # let inner_transport = MemoryTransport::default();
///    # let (relay_transport, relay_behaviour) = new_transport_and_behaviour(
///    #     RelayConfig::default(),
///    #     inner_transport,
///    # );
///    relay_transport.dial(Multiaddr::empty().with(Protocol::Memory(42)));
///    ```
///
/// 2. Establish relayed connections by dialing `/p2p-circuit` addresses.
///
///    ```
///    # use libp2p_core::{Multiaddr, multiaddr::{Protocol}, PeerId, Transport};
///    # use libp2p_core::transport::memory::MemoryTransport;
///    # use libp2p_relay::{RelayConfig, new_transport_and_behaviour};
///    # let inner_transport = MemoryTransport::default();
///    # let (relay_transport, relay_behaviour) = new_transport_and_behaviour(
///    #     RelayConfig::default(),
///    #     inner_transport,
///    # );
///    let dst_addr_via_relay = Multiaddr::empty()
///        .with(Protocol::Memory(40)) // Relay address.
///        .with(Protocol::P2p(PeerId::random().into())) // Relay peer id.
///        .with(Protocol::P2pCircuit) // Signal to connect via relay and not directly.
///        .with(Protocol::Memory(42)) // Destination address.
///        .with(Protocol::P2p(PeerId::random().into())); // Destination peer id.
///    relay_transport.dial(dst_addr_via_relay).unwrap();
///    ```
///
/// 3. Listen for incoming relayed connections via specific relay.
///
///    ```
///    # use libp2p_core::{Multiaddr, multiaddr::{Protocol}, PeerId, Transport};
///    # use libp2p_core::transport::memory::MemoryTransport;
///    # use libp2p_relay::{RelayConfig, new_transport_and_behaviour};
///    # let inner_transport = MemoryTransport::default();
///    # let (relay_transport, relay_behaviour) = new_transport_and_behaviour(
///    #     RelayConfig::default(),
///    #     inner_transport,
///    # );
///    let relay_addr = Multiaddr::empty()
///        .with(Protocol::Memory(40)) // Relay address.
///        .with(Protocol::P2p(PeerId::random().into())) // Relay peer id.
///        .with(Protocol::P2pCircuit); // Signal to listen via remote relay node.
///    relay_transport.listen_on(relay_addr).unwrap();
///    ```
///
/// 4. Listen for incoming relayed connections via any relay.
///
///    Note: Without this listener, incoming relayed connections from relays, that the local node is
///    not explicitly listening via, are dropped.
///
///    ```
///    # use libp2p_core::{Multiaddr, multiaddr::{Protocol}, PeerId, Transport};
///    # use libp2p_core::transport::memory::MemoryTransport;
///    # use libp2p_relay::{RelayConfig, new_transport_and_behaviour};
///    # let inner_transport = MemoryTransport::default();
///    # let (relay_transport, relay_behaviour) = new_transport_and_behaviour(
///    #     RelayConfig::default(),
///    #     inner_transport,
///    # );
///    let addr = Multiaddr::empty()
///        .with(Protocol::P2pCircuit); // Signal to listen via any relay.
///    relay_transport.listen_on(addr).unwrap();
///    ```
#[derive(Clone)]
pub struct RelayTransport<T: Clone> {
    to_behaviour: mpsc::Sender<TransportToBehaviourMsg>,

    inner_transport: T,
}

impl<T: Clone> RelayTransport<T> {
    /// Create a new [`RelayTransport`] by wrapping an existing [`Transport`] in a
    /// [`RelayTransport`].
    ///
    ///```
    /// # use libp2p_core::transport::dummy::DummyTransport;
    /// # use libp2p_relay::{RelayConfig, new_transport_and_behaviour};
    ///
    /// let inner_transport = DummyTransport::<()>::new();
    /// let (relay_transport, relay_behaviour) = new_transport_and_behaviour(
    ///     RelayConfig::default(),
    ///     inner_transport,
    /// );
    ///```
    pub(crate) fn new(t: T) -> (Self, mpsc::Receiver<TransportToBehaviourMsg>) {
        let (to_behaviour, from_transport) = mpsc::channel(0);

        let transport = RelayTransport {
            to_behaviour,

            inner_transport: t,
        };

        (transport, from_transport)
    }
}

impl<T: Transport + Clone> Transport for RelayTransport<T> {
    type Output = EitherOutput<<T as Transport>::Output, protocol::Connection>;
    type Error = EitherError<<T as Transport>::Error, RelayError>;
    type Listener = RelayListener<T>;
    type ListenerUpgrade = RelayedListenerUpgrade<T>;
    type Dial = EitherFuture<<T as Transport>::Dial, RelayedDial>;

    fn listen_on(self, addr: Multiaddr) -> Result<Self::Listener, TransportError<Self::Error>> {
        let orig_addr = addr.clone();

        match parse_relayed_multiaddr(addr)? {
            // Address does not contain circuit relay protocol. Use inner transport.
            Err(addr) => {
                let inner_listener = match self.inner_transport.listen_on(addr) {
                    Ok(listener) => listener,
                    Err(TransportError::MultiaddrNotSupported(addr)) => {
                        return Err(TransportError::MultiaddrNotSupported(addr))
                    }
                    Err(TransportError::Other(err)) => {
                        return Err(TransportError::Other(EitherError::A(err)))
                    }
                };
                Ok(RelayListener::Inner(inner_listener))
            }
            // Address does contain circuit relay protocol. Use relayed listener.
            Ok(relayed_addr) => {
                let relay_peer_id_and_addr = match relayed_addr {
                    // TODO: In the future we might want to support listening via a relay by its
                    // address only.
                    RelayedMultiaddr {
                        relay_peer_id: None,
                        relay_addr: Some(_),
                        ..
                    } => return Err(RelayError::MissingRelayPeerId.into()),
                    // TODO: In the future we might want to support listening via a relay by its
                    // peer_id only.
                    RelayedMultiaddr {
                        relay_peer_id: Some(_),
                        relay_addr: None,
                        ..
                    } => return Err(RelayError::MissingRelayAddr.into()),
                    // Listen for incoming relayed connections via specific relay.
                    RelayedMultiaddr {
                        relay_peer_id: Some(peer_id),
                        relay_addr: Some(addr),
                        ..
                    } => Some((peer_id, addr)),
                    // Listen for incoming relayed connections via any relay.
                    RelayedMultiaddr {
                        relay_peer_id: None,
                        relay_addr: None,
                        ..
                    } => None,
                };

                let (to_listener, from_behaviour) = mpsc::channel(0);
                let mut to_behaviour = self.to_behaviour.clone();
                let msg_to_behaviour = Some(
                    async move {
                        to_behaviour
                            .send(TransportToBehaviourMsg::ListenReq {
                                relay_peer_id_and_addr,
                                to_listener,
                            })
                            .await
                    }
                    .boxed(),
                );

                Ok(RelayListener::Relayed {
                    from_behaviour,
                    msg_to_behaviour,
                    report_listen_addr: Some(orig_addr),
                })
            }
        }
    }

    fn dial(self, addr: Multiaddr) -> Result<Self::Dial, TransportError<Self::Error>> {
        match parse_relayed_multiaddr(addr)? {
            // Address does not contain circuit relay protocol. Use inner transport.
            Err(addr) => match self.inner_transport.dial(addr) {
                Ok(dialer) => Ok(EitherFuture::First(dialer)),
                Err(TransportError::MultiaddrNotSupported(addr)) => {
                    Err(TransportError::MultiaddrNotSupported(addr))
                }
                Err(TransportError::Other(err)) => Err(TransportError::Other(EitherError::A(err))),
            },
            // Address does contain circuit relay protocol. Dial destination via relay.
            Ok(RelayedMultiaddr {
                relay_peer_id,
                relay_addr,
                dst_peer_id,
                dst_addr,
            }) => {
                // TODO: In the future we might want to support dialing a relay by its address only.
                let relay_peer_id = relay_peer_id.ok_or(RelayError::MissingRelayPeerId)?;
                let relay_addr = relay_addr.ok_or(RelayError::MissingRelayAddr)?;
                let dst_peer_id = dst_peer_id.ok_or(RelayError::MissingDstPeerId)?;

                let mut to_behaviour = self.to_behaviour.clone();
                Ok(EitherFuture::Second(
                    async move {
                        let (tx, rx) = oneshot::channel();
                        to_behaviour
                            .send(TransportToBehaviourMsg::DialReq {
                                request_id: RequestId::new(),
                                relay_addr,
                                relay_peer_id,
                                dst_addr,
                                dst_peer_id,
                                send_back: tx,
                            })
                            .await?;
                        let stream = rx.await??;
                        Ok(stream)
                    }
                    .boxed(),
                ))
            }
        }
    }

    fn address_translation(&self, server: &Multiaddr, observed: &Multiaddr) -> Option<Multiaddr> {
        self.inner_transport.address_translation(server, observed)
    }
}

#[derive(Default)]
struct RelayedMultiaddr {
    relay_peer_id: Option<PeerId>,
    relay_addr: Option<Multiaddr>,
    dst_peer_id: Option<PeerId>,
    dst_addr: Option<Multiaddr>,
}

/// Parse a [`Multiaddr`] containing a [`Protocol::P2pCircuit`].
///
/// Returns `Ok(Err(provided_addr))` when passed address contains no [`Protocol::P2pCircuit`].
///
/// Returns `Err(_)` when address is malformed.
fn parse_relayed_multiaddr(
    addr: Multiaddr,
) -> Result<Result<RelayedMultiaddr, Multiaddr>, RelayError> {
    if !addr.iter().any(|p| matches!(p, Protocol::P2pCircuit)) {
        return Ok(Err(addr));
    }

    let mut relayed_multiaddr = RelayedMultiaddr::default();

    let mut before_circuit = true;
    for protocol in addr.into_iter() {
        match protocol {
            Protocol::P2pCircuit => {
                if before_circuit {
                    before_circuit = false;
                } else {
                    return Err(RelayError::MultipleCircuitRelayProtocolsUnsupported);
                }
            }
            Protocol::P2p(hash) => {
                let peer_id = PeerId::from_multihash(hash).map_err(|_| RelayError::InvalidHash)?;

                if before_circuit {
                    if relayed_multiaddr.relay_peer_id.is_some() {
                        return Err(RelayError::MalformedMultiaddr);
                    }
                    relayed_multiaddr.relay_peer_id = Some(peer_id)
                } else {
                    if relayed_multiaddr.dst_peer_id.is_some() {
                        return Err(RelayError::MalformedMultiaddr);
                    }
                    relayed_multiaddr.dst_peer_id = Some(peer_id)
                }
            }
            p => {
                if before_circuit {
                    relayed_multiaddr
                        .relay_addr
                        .get_or_insert(Multiaddr::empty())
                        .push(p);
                } else {
                    relayed_multiaddr
                        .dst_addr
                        .get_or_insert(Multiaddr::empty())
                        .push(p);
                }
            }
        }
    }

    Ok(Ok(relayed_multiaddr))
}

#[pin_project(project = RelayListenerProj)]
pub enum RelayListener<T: Transport> {
    Inner(#[pin] <T as Transport>::Listener),
    Relayed {
        from_behaviour: mpsc::Receiver<BehaviourToListenerMsg>,

        msg_to_behaviour: Option<BoxFuture<'static, Result<(), mpsc::SendError>>>,
        report_listen_addr: Option<Multiaddr>,
    },
}

impl<T: Transport> Stream for RelayListener<T> {
    type Item = Result<
        ListenerEvent<RelayedListenerUpgrade<T>, EitherError<<T as Transport>::Error, RelayError>>,
        EitherError<<T as Transport>::Error, RelayError>,
    >;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match this {
            RelayListenerProj::Inner(listener) => match listener.poll_next(cx) {
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(EitherError::A(e)))),
                Poll::Ready(Some(Ok(ListenerEvent::Upgrade {
                    upgrade,
                    local_addr,
                    remote_addr,
                }))) => {
                    return Poll::Ready(Some(Ok(ListenerEvent::Upgrade {
                        upgrade: RelayedListenerUpgrade::Inner(upgrade),
                        local_addr,
                        remote_addr,
                    })))
                }
                Poll::Ready(Some(Ok(ListenerEvent::NewAddress(addr)))) => {
                    return Poll::Ready(Some(Ok(ListenerEvent::NewAddress(addr))))
                }
                Poll::Ready(Some(Ok(ListenerEvent::AddressExpired(addr)))) => {
                    return Poll::Ready(Some(Ok(ListenerEvent::AddressExpired(addr))))
                }
                Poll::Ready(Some(Ok(ListenerEvent::Error(err)))) => {
                    return Poll::Ready(Some(Ok(ListenerEvent::Error(EitherError::A(err)))))
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => {}
            },
            RelayListenerProj::Relayed {
                from_behaviour,
                msg_to_behaviour,
                report_listen_addr,
            } => {
                if let Some(msg) = msg_to_behaviour {
                    match Future::poll(msg.as_mut(), cx) {
                        Poll::Ready(Ok(())) => *msg_to_behaviour = None,
                        Poll::Ready(Err(e)) => {
                            return Poll::Ready(Some(Err(EitherError::B(e.into()))))
                        }
                        Poll::Pending => {}
                    }
                }

                match from_behaviour.poll_next_unpin(cx) {
                    Poll::Ready(Some(BehaviourToListenerMsg::IncomingRelayedConnection {
                        stream,
                        src_peer_id,
                        relay_addr,
                        relay_peer_id: _
                    })) => {
                        return Poll::Ready(Some(Ok(ListenerEvent::Upgrade {
                            upgrade: RelayedListenerUpgrade::Relayed(Some(stream)),
                            local_addr: relay_addr.with(Protocol::P2pCircuit),
                            remote_addr: Protocol::P2p(src_peer_id.into()).into(),
                        })));
                    }
                    Poll::Ready(Some(BehaviourToListenerMsg::ConnectionToRelayEstablished)) => {
                        return Poll::Ready(Some(Ok(ListenerEvent::NewAddress(
                            report_listen_addr
                                .take()
                                .expect("ConnectionToRelayEstablished to be send at most once"),
                        ))));
                    }
                    Poll::Ready(None) => return Poll::Ready(None),
                    Poll::Pending => {}
                }
            }
        }

        Poll::Pending
    }
}

pub type RelayedDial = BoxFuture<'static, Result<protocol::Connection, RelayError>>;

#[pin_project(project = RelayedListenerUpgradeProj)]
pub enum RelayedListenerUpgrade<T: Transport> {
    Inner(#[pin] <T as Transport>::ListenerUpgrade),
    Relayed(Option<protocol::Connection>),
}

impl<T: Transport> Future for RelayedListenerUpgrade<T> {
    type Output = Result<
        EitherOutput<<T as Transport>::Output, protocol::Connection>,
        EitherError<<T as Transport>::Error, RelayError>,
    >;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            RelayedListenerUpgradeProj::Inner(upgrade) => match upgrade.poll(cx) {
                Poll::Ready(Ok(out)) => return Poll::Ready(Ok(EitherOutput::First(out))),
                Poll::Ready(Err(err)) => return Poll::Ready(Err(EitherError::A(err))),
                Poll::Pending => {}
            },
            RelayedListenerUpgradeProj::Relayed(substream) => {
                return Poll::Ready(Ok(EitherOutput::Second(
                    substream.take().expect("Future polled after completion."),
                )))
            }
        }

        Poll::Pending
    }
}

/// Error that occurred during relay connection setup.
#[derive(Debug, Eq, PartialEq)]
pub enum RelayError {
    MissingRelayPeerId,
    MissingRelayAddr,
    MissingDstPeerId,
    InvalidHash,
    SendingMessageToBehaviour(mpsc::SendError),
    ResponseFromBehaviourCanceled,
    DialingRelay,
    MultipleCircuitRelayProtocolsUnsupported,
    MalformedMultiaddr,
}

impl<E> From<RelayError> for TransportError<EitherError<E, RelayError>> {
    fn from(error: RelayError) -> Self {
        TransportError::Other(EitherError::B(error))
    }
}

impl From<mpsc::SendError> for RelayError {
    fn from(error: mpsc::SendError) -> Self {
        RelayError::SendingMessageToBehaviour(error)
    }
}

impl From<oneshot::Canceled> for RelayError {
    fn from(_: oneshot::Canceled) -> Self {
        RelayError::ResponseFromBehaviourCanceled
    }
}

impl From<OutgoingRelayReqError> for RelayError {
    fn from(error: OutgoingRelayReqError) -> Self {
        match error {
            OutgoingRelayReqError::DialingRelay => RelayError::DialingRelay,
        }
    }
}

impl std::fmt::Display for RelayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RelayError::MissingRelayPeerId => {
                write!(f, "Missing relay peer id.")
            }
            RelayError::MissingRelayAddr => {
                write!(f, "Missing relay address.")
            }
            RelayError::MissingDstPeerId => {
                write!(f, "Missing destination peer id.")
            }
            RelayError::InvalidHash => {
                write!(f, "Invalid peer id hash.")
            }
            RelayError::SendingMessageToBehaviour(e) => {
                write!(f, "Failed to send message to relay behaviour: {:?}", e)
            }
            RelayError::ResponseFromBehaviourCanceled => {
                write!(f, "Response from behaviour was canceled")
            }
            RelayError::DialingRelay => {
                write!(f, "Dialing relay failed")
            }
            RelayError::MultipleCircuitRelayProtocolsUnsupported => {
                write!(f, "Address contains multiple circuit relay protocols (`p2p-circuit`) which is not supported.")
            }
            RelayError::MalformedMultiaddr => {
                write!(f, "One of the provided multiaddresses is malformed.")
            }
        }
    }
}

impl std::error::Error for RelayError {}

/// Message from the [`RelayTransport`] to the [`Relay`](crate::Relay)
/// [`NetworkBehaviour`](libp2p_swarm::NetworkBehaviour).
pub enum TransportToBehaviourMsg {
    /// Dial destination node via relay node.
    DialReq {
        request_id: RequestId,
        relay_addr: Multiaddr,
        relay_peer_id: PeerId,
        dst_addr: Option<Multiaddr>,
        dst_peer_id: PeerId,
        send_back: oneshot::Sender<Result<protocol::Connection, OutgoingRelayReqError>>,
    },
    /// Listen for incoming relayed connections via relay node.
    ListenReq {
        /// [`PeerId`] and [`Multiaddr`] of relay node.
        ///
        /// When [`None`] listen for connections from any relay node.
        relay_peer_id_and_addr: Option<(PeerId, Multiaddr)>,
        to_listener: mpsc::Sender<BehaviourToListenerMsg>,
    },
}
