use crate::codec::{ErrorCode, Message, NewRegistration, Registration};
use crate::handler;
use crate::handler::{InEvent, RendezvousHandler};
use libp2p_core::connection::ConnectionId;
use libp2p_core::identity::Keypair;
use libp2p_core::{AuthenticatedPeerRecord, Multiaddr, PeerId, PeerRecord};
use libp2p_swarm::{
    NetworkBehaviour, NetworkBehaviourAction, NotifyHandler, PollParameters, ProtocolsHandler,
};
use log::debug;
use std::collections::{HashMap, VecDeque};
use std::task::{Context, Poll};

pub struct Rendezvous {
    events: VecDeque<NetworkBehaviourAction<InEvent, Event>>,
    registrations: Registrations,
    key_pair: Keypair,
    external_addresses: Vec<Multiaddr>,
}

impl Rendezvous {
    pub fn new(key_pair: Keypair) -> Self {
        Self {
            events: Default::default(),
            registrations: Registrations::new(),
            key_pair,
            external_addresses: vec![],
        }
    }

    // TODO: Make it possible to filter for specific external-addresses (like onion addresses-only f.e.)
    pub fn register(&mut self, namespace: String, rendezvous_node: PeerId) {
        let authenticated_peer_record = AuthenticatedPeerRecord::from_record(
            self.key_pair.clone(),
            PeerRecord {
                peer_id: self.key_pair.public().into_peer_id(),
                seq: 0, // TODO: should be current unix timestamp
                addresses: self.external_addresses.clone(),
            },
        );

        self.events
            .push_back(NetworkBehaviourAction::NotifyHandler {
                peer_id: rendezvous_node,
                event: InEvent::RegisterRequest {
                    request: NewRegistration {
                        namespace,
                        record: authenticated_peer_record,
                        ttl: None,
                    },
                },
                handler: NotifyHandler::Any,
            });
    }

    pub fn unregister(&mut self, namespace: String, rendezvous_node: PeerId) {
        self.events
            .push_back(NetworkBehaviourAction::NotifyHandler {
                peer_id: rendezvous_node,
                event: InEvent::UnregisterRequest { namespace },
                handler: NotifyHandler::Any,
            });
    }

    pub fn discover(&mut self, ns: Option<String>, rendezvous_node: PeerId) {
        self.events
            .push_back(NetworkBehaviourAction::NotifyHandler {
                peer_id: rendezvous_node,
                event: InEvent::DiscoverRequest { namespace: ns },
                handler: NotifyHandler::Any,
            });
    }
}

#[derive(Debug)]
pub enum Event {
    Discovered {
        rendezvous_node: PeerId,
        registrations: Vec<Registration>,
    },
    AnsweredDiscoverRequest {
        enquirer: PeerId,
        registrations: Vec<Registration>,
    },
    FailedToDiscover {
        rendezvous_node: PeerId,
        err_code: ErrorCode,
    },
    RegisteredWithRendezvousNode {
        rendezvous_node: PeerId,
        ttl: i64,
        // TODO: get the namespace in as well, needs association between the registration request and the response
    },
    FailedToRegisterWithRendezvousNode {
        rendezvous_node: PeerId,
        err_code: ErrorCode,
        // TODO: get the namespace in as well, needs association between the registration request and the response
    },
    DeclinedRegisterRequest {
        peer: PeerId,
        // TODO: get the namespace in as well, needs association between the registration request and the response
    },
    PeerRegistered {
        peer: PeerId,
        namespace: String,
    },
    PeerUnregistered {
        peer: PeerId,
        namespace: String,
    },
}

impl NetworkBehaviour for Rendezvous {
    type ProtocolsHandler = RendezvousHandler;
    type OutEvent = Event;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        debug!("spawning protocol handler");
        RendezvousHandler::new()
    }

    fn addresses_of_peer(&mut self, _: &PeerId) -> Vec<Multiaddr> {
        Vec::new()
    }

    fn inject_connected(&mut self, peer_id: &PeerId) {
        debug!("New peer connected: {}", peer_id);
        // Dont need to do anything here?
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId) {
        debug!("Peer disconnected: {}", peer_id);
        // Don't need to do anything?
    }

    fn inject_event(
        &mut self,
        peer_id: PeerId,
        _connection: ConnectionId,
        message: handler::OutEvent,
    ) {
        match message {
            Message::Register(new_registration) => {
                let (namespace, ttl) = self.registrations.add(new_registration);

                // notify the handler that to send a response
                self.events
                    .push_back(NetworkBehaviourAction::NotifyHandler {
                        peer_id,
                        handler: NotifyHandler::Any,
                        event: InEvent::RegisterResponse { ttl },
                    });

                // emit behaviour event
                self.events.push_back(NetworkBehaviourAction::GenerateEvent(
                    Event::PeerRegistered {
                        peer: peer_id,
                        namespace,
                    },
                ));
            }
            Message::RegisterResponse { ttl } => self.events.push_back(
                NetworkBehaviourAction::GenerateEvent(Event::RegisteredWithRendezvousNode {
                    rendezvous_node: peer_id,
                    ttl,
                }),
            ),
            Message::FailedToRegister { error } => self.events.push_back(
                NetworkBehaviourAction::GenerateEvent(Event::FailedToRegisterWithRendezvousNode {
                    rendezvous_node: peer_id,
                    err_code: error,
                }),
            ),
            Message::Unregister { namespace } => {
                self.registrations.remove(namespace, peer_id);
                // TODO: Should send unregister response?
            }
            Message::Discover { namespace } => {
                let registrations = self.registrations.get(namespace).unwrap_or_default();

                self.events
                    .push_back(NetworkBehaviourAction::NotifyHandler {
                        peer_id,
                        handler: NotifyHandler::Any,
                        event: InEvent::DiscoverResponse {
                            discovered: registrations.clone(),
                        },
                    });
                self.events.push_back(NetworkBehaviourAction::GenerateEvent(
                    Event::AnsweredDiscoverRequest {
                        enquirer: peer_id,
                        registrations,
                    },
                ));
            }
            Message::DiscoverResponse { registrations } => {
                self.events
                    .push_back(NetworkBehaviourAction::GenerateEvent(Event::Discovered {
                        rendezvous_node: peer_id,
                        registrations,
                    }))
            }
            Message::FailedToDiscover { error } => self.events.push_back(
                NetworkBehaviourAction::GenerateEvent(Event::FailedToDiscover {
                    rendezvous_node: peer_id,
                    err_code: error,
                }),
            ),
        }
    }

    fn poll(
        &mut self,
        _cx: &mut Context<'_>,
        poll_params: &mut impl PollParameters,
    ) -> Poll<
        NetworkBehaviourAction<
            <Self::ProtocolsHandler as ProtocolsHandler>::InEvent,
            Self::OutEvent,
        >,
    > {
        // Update our external addresses based on the Swarm's current knowledge.
        // It doesn't make sense to register addresses on which we are not reachable, hence this should not be configurable from the outside.
        self.external_addresses = poll_params.external_addresses().map(|r| r.addr).collect();

        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }

        Poll::Pending
    }
}

// TODO: Unit Tests
pub struct Registrations {
    registrations_for_namespace: HashMap<String, HashMap<PeerId, Registration>>,
}

impl Registrations {
    pub fn new() -> Self {
        Self {
            registrations_for_namespace: Default::default(),
        }
    }

    pub fn add(&mut self, new_registration: NewRegistration) -> (String, i64) {
        let ttl = new_registration.effective_ttl();
        let namespace = new_registration.namespace;

        self.registrations_for_namespace
            .entry(namespace.clone())
            .or_insert_with(|| HashMap::new())
            .insert(
                new_registration.record.peer_id(),
                Registration {
                    namespace: namespace.clone(),
                    record: new_registration.record,
                    ttl,
                },
            );

        (namespace, ttl)
    }

    pub fn remove(&mut self, namespace: String, peer_id: PeerId) {
        if let Some(registrations) = self.registrations_for_namespace.get_mut(&namespace) {
            registrations.remove(&peer_id);
        }
    }

    pub fn get(&mut self, namespace: Option<String>) -> Option<Vec<Registration>> {
        if self.registrations_for_namespace.is_empty() {
            return None;
        }

        if let Some(namespace) = namespace {
            if let Some(registrations) = self.registrations_for_namespace.get(&namespace) {
                Some(
                    registrations
                        .values()
                        .cloned()
                        .collect::<Vec<Registration>>(),
                )
            } else {
                None
            }
        } else {
            let discovered = self
                .registrations_for_namespace
                .iter()
                .map(|(_, registrations)| {
                    registrations
                        .values()
                        .cloned()
                        .collect::<Vec<Registration>>()
                })
                .flatten()
                .collect::<Vec<Registration>>();

            Some(discovered)
        }
    }
}