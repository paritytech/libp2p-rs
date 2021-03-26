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

use crate::protocol::{FloodsubProtocol, FloodsubMessage, FloodsubRpc, FloodsubSubscription, FloodsubSubscriptionAction};
use crate::topic::Topic;
use crate::FloodsubConfig;
use cuckoofilter::{CuckooError, CuckooFilter};
use fnv::FnvHashSet;
use libp2p_core::{Multiaddr, PeerId, connection::ConnectionId};
use libp2p_swarm::{
    NetworkBehaviour,
    NetworkBehaviourAction,
    PollParameters,
    ProtocolsHandler,
    OneShotHandler,
    NotifyHandler,
    DialPeerCondition,
};
use log::warn;
use smallvec::SmallVec;
use std::{collections::VecDeque, iter};
use std::collections::hash_map::{DefaultHasher, HashMap};
use std::task::{Context, Poll};

/// Network behaviour that handles the floodsub protocol.
pub struct Floodsub {
    /// Events that need to be yielded to the outside when polling.
    events: VecDeque<NetworkBehaviourAction<FloodsubRpc, FloodsubEvent>>,

    config: FloodsubConfig,

    /// List of peers to send messages to.
    target_peers: FnvHashSet<PeerId>,

    /// List of peers the network is connected to, and the topics that they're subscribed to.
    // TODO: filter out peers that don't support floodsub, so that we avoid hammering them with
    //       opened substreams
    connected_peers: HashMap<PeerId, SmallVec<[Topic; 8]>>,

    // List of topics we're subscribed to. Necessary to filter out messages that we receive
    // erroneously.
    subscribed_topics: SmallVec<[Topic; 16]>,

    // We keep track of the messages we received (in the format `hash(source ID, seq_no)`) so that
    // we don't dispatch the same message twice if we receive it twice on the network.
    received: CuckooFilter<DefaultHasher>,
}

impl Floodsub {
    /// Creates a `Floodsub` with default configuration.
    pub fn new(local_peer_id: PeerId) -> Self {
        Self::from_config(FloodsubConfig::new(local_peer_id))
    }

    /// Creates a `Floodsub` with the given configuration.
    pub fn from_config(config: FloodsubConfig) -> Self {
        Floodsub {
            events: VecDeque::new(),
            config,
            target_peers: FnvHashSet::default(),
            connected_peers: HashMap::new(),
            subscribed_topics: SmallVec::new(),
            received: CuckooFilter::new(),
        }
    }

    /// Add a node to the list of nodes to propagate messages to.
    #[inline]
    pub fn add_node_to_partial_view(&mut self, peer_id: PeerId) {
        // Send our topics to this node if we're already connected to it.
        if self.connected_peers.contains_key(&peer_id) {
            for topic in self.subscribed_topics.iter().cloned() {
                self.events.push_back(NetworkBehaviourAction::NotifyHandler {
                    peer_id,
                    handler: NotifyHandler::Any,
                    event: FloodsubRpc {
                        messages: Vec::new(),
                        subscriptions: vec![FloodsubSubscription {
                            topic,
                            action: FloodsubSubscriptionAction::Subscribe,
                        }],
                    },
                });
            }
        }

        if self.target_peers.insert(peer_id) {
            self.events.push_back(NetworkBehaviourAction::DialPeer {
                peer_id, condition: DialPeerCondition::Disconnected
            });
        }
    }

    /// Remove a node from the list of nodes to propagate messages to.
    #[inline]
    pub fn remove_node_from_partial_view(&mut self, peer_id: &PeerId) {
        self.target_peers.remove(peer_id);
    }

    /// Subscribes to a topic.
    ///
    /// Returns true if the subscription worked. Returns false if we were already subscribed.
    pub fn subscribe(&mut self, topic: Topic) -> bool {
        if self.subscribed_topics.iter().any(|t| t.id() == topic.id()) {
            return false;
        }

        for peer in self.connected_peers.keys() {
            self.events.push_back(NetworkBehaviourAction::NotifyHandler {
                peer_id: *peer,
                handler: NotifyHandler::Any,
                event: FloodsubRpc {
                    messages: Vec::new(),
                    subscriptions: vec![FloodsubSubscription {
                        topic: topic.clone(),
                        action: FloodsubSubscriptionAction::Subscribe,
                    }],
                },
            });
        }

        self.subscribed_topics.push(topic);
        true
    }

    /// Unsubscribes from a topic.
    ///
    /// Note that this only requires the topic name.
    ///
    /// Returns true if we were subscribed to this topic.
    pub fn unsubscribe(&mut self, topic: Topic) -> bool {
        let pos = match self.subscribed_topics.iter().position(|t| *t == topic) {
            Some(pos) => pos,
            None => return false
        };

        self.subscribed_topics.remove(pos);

        for peer in self.connected_peers.keys() {
            self.events.push_back(NetworkBehaviourAction::NotifyHandler {
                peer_id: *peer,
                handler: NotifyHandler::Any,
                event: FloodsubRpc {
                    messages: Vec::new(),
                    subscriptions: vec![FloodsubSubscription {
                        topic: topic.clone(),
                        action: FloodsubSubscriptionAction::Unsubscribe,
                    }],
                },
            });
        }

        true
    }

    /// Publishes a message to the network, if we're subscribed to the topic only.
    pub fn publish(&mut self, topic: impl Into<Topic>, data: impl Into<Vec<u8>>) {
        self.publish_many(iter::once(topic), data)
    }

    /// Publishes a message to the network, even if we're not subscribed to the topic.
    pub fn publish_any(&mut self, topic: impl Into<Topic>, data: impl Into<Vec<u8>>) {
        self.publish_many_any(iter::once(topic), data)
    }

    /// Publishes a message with multiple topics to the network.
    ///
    ///
    /// > **Note**: Doesn't do anything if we're not subscribed to any of the topics.
    pub fn publish_many(&mut self, topic: impl IntoIterator<Item = impl Into<Topic>>, data: impl Into<Vec<u8>>) {
        self.publish_many_inner(topic, data, true)
    }

    /// Publishes a message with multiple topics to the network, even if we're not subscribed to any of the topics.
    pub fn publish_many_any(&mut self, topic: impl IntoIterator<Item = impl Into<Topic>>, data: impl Into<Vec<u8>>) {
        self.publish_many_inner(topic, data, false)
    }

    fn publish_many_inner(&mut self, topic: impl IntoIterator<Item = impl Into<Topic>>, data: impl Into<Vec<u8>>, check_self_subscriptions: bool) {
        let message = FloodsubMessage {
            source: self.config.local_peer_id,
            data: data.into(),
            // If the sequence numbers are predictable, then an attacker could flood the network
            // with packets with the predetermined sequence numbers and absorb our legitimate
            // messages. We therefore use a random number.
            sequence_number: rand::random::<[u8; 20]>().to_vec(),
            topics: topic.into_iter().map(Into::into).collect(),
        };

        let self_subscribed = self.subscribed_topics.iter().any(|t| message.topics.iter().any(|u| t == u));
        if self_subscribed {
            if let Err(e @ CuckooError::NotEnoughSpace) = self.received.add(&message) {
                warn!(
                    "Message was added to 'received' Cuckoofilter but some \
                     other message was removed as a consequence: {}", e,
                );
            }
            if self.config.subscribe_local_messages {
                self.events.push_back(
                    NetworkBehaviourAction::GenerateEvent(FloodsubEvent::Message(message.clone())));
            }
        }
        // Don't publish the message if we have to check subscriptions
        // and we're not subscribed ourselves to any of the topics.
        if check_self_subscriptions && !self_subscribed {
            return
        }

        // Send to peers we know are subscribed to the topic.
        for (peer_id, sub_topic) in self.connected_peers.iter() {
            if !sub_topic.iter().any(|t| message.topics.iter().any(|u| t == u)) {
                continue;
            }

            self.events.push_back(NetworkBehaviourAction::NotifyHandler {
                peer_id: *peer_id,
                handler: NotifyHandler::Any,
                event: FloodsubRpc {
                    subscriptions: Vec::new(),
                    messages: vec![message.clone()],
                }
            });
        }
    }
}

impl NetworkBehaviour for Floodsub {
    type ProtocolsHandler = OneShotHandler<FloodsubProtocol, FloodsubRpc, InnerMessage>;
    type OutEvent = FloodsubEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        Default::default()
    }

    fn addresses_of_peer(&mut self, _: &PeerId) -> Vec<Multiaddr> {
        Vec::new()
    }

    fn inject_connected(&mut self, id: &PeerId) {
        // We need to send our subscriptions to the newly-connected node.
        if self.target_peers.contains(id) {
            for topic in self.subscribed_topics.iter().cloned() {
                self.events.push_back(NetworkBehaviourAction::NotifyHandler {
                    peer_id: *id,
                    handler: NotifyHandler::Any,
                    event: FloodsubRpc {
                        messages: Vec::new(),
                        subscriptions: vec![FloodsubSubscription {
                            topic,
                            action: FloodsubSubscriptionAction::Subscribe,
                        }],
                    },
                });
            }
        }

        self.connected_peers.insert(*id, SmallVec::new());
    }

    fn inject_disconnected(&mut self, id: &PeerId) {
        let was_in = self.connected_peers.remove(id);
        debug_assert!(was_in.is_some());

        // We can be disconnected by the remote in case of inactivity for example, so we always
        // try to reconnect.
        if self.target_peers.contains(id) {
            self.events.push_back(NetworkBehaviourAction::DialPeer {
                peer_id: *id,
                condition: DialPeerCondition::Disconnected
            });
        }
    }

    fn inject_event(
        &mut self,
        propagation_source: PeerId,
        _connection: ConnectionId,
        event: InnerMessage,
    ) {
        // We ignore successful sends or timeouts.
        let event = match event {
            InnerMessage::Rx(event) => event,
            InnerMessage::Sent => return,
        };

        // Update connected peers topics
        for subscription in event.subscriptions {
            let remote_peer_topics = self.connected_peers
                .get_mut(&propagation_source)
                .expect("connected_peers is kept in sync with the peers we are connected to; we are guaranteed to only receive events from connected peers; QED");
            match subscription.action {
                FloodsubSubscriptionAction::Subscribe => {
                    if !remote_peer_topics.contains(&subscription.topic) {
                        remote_peer_topics.push(subscription.topic.clone());
                    }
                    self.events.push_back(NetworkBehaviourAction::GenerateEvent(FloodsubEvent::Subscribed {
                        peer_id: propagation_source,
                        topic: subscription.topic,
                    }));
                }
                FloodsubSubscriptionAction::Unsubscribe => {
                    if let Some(pos) = remote_peer_topics.iter().position(|t| t == &subscription.topic ) {
                        remote_peer_topics.remove(pos);
                    }
                    self.events.push_back(NetworkBehaviourAction::GenerateEvent(FloodsubEvent::Unsubscribed {
                        peer_id: propagation_source,
                        topic: subscription.topic,
                    }));
                }
            }
        }

        // List of messages we're going to propagate on the network.
        let mut rpcs_to_dispatch: Vec<(PeerId, FloodsubRpc)> = Vec::new();

        for message in event.messages {
            // Use `self.received` to skip the messages that we have already received in the past.
            // Note that this can result in false positives.
            match self.received.test_and_add(&message) {
                Ok(true) => {}, // Message  was added.
                Ok(false) => continue, // Message already existed.
                Err(e @ CuckooError::NotEnoughSpace) => { // Message added, but some other removed.
                    warn!(
                        "Message was added to 'received' Cuckoofilter but some \
                         other message was removed as a consequence: {}", e,
                    );
                }
            }

            // Add the message to be dispatched to the user.
            if self.subscribed_topics.iter().any(|t| message.topics.iter().any(|u| t == u)) {
                let event = FloodsubEvent::Message(message.clone());
                self.events.push_back(NetworkBehaviourAction::GenerateEvent(event));
            }

            // Propagate the message to everyone else who is subscribed to any of the topics.
            for (peer_id, subscr_topics) in self.connected_peers.iter() {
                if peer_id == &propagation_source {
                    continue;
                }

                if !subscr_topics.iter().any(|t| message.topics.iter().any(|u| t == u)) {
                    continue;
                }

                if let Some(pos) = rpcs_to_dispatch.iter().position(|(p, _)| p == peer_id) {
                    rpcs_to_dispatch[pos].1.messages.push(message.clone());
                } else {
                    rpcs_to_dispatch.push((*peer_id, FloodsubRpc {
                        subscriptions: Vec::new(),
                        messages: vec![message.clone()],
                    }));
                }
            }
        }

        for (peer_id, rpc) in rpcs_to_dispatch {
            self.events.push_back(NetworkBehaviourAction::NotifyHandler {
                peer_id,
                handler: NotifyHandler::Any,
                event: rpc,
            });
        }
    }

    fn poll(
        &mut self,
        _: &mut Context<'_>,
        _: &mut impl PollParameters,
    ) -> Poll<
        NetworkBehaviourAction<
            <Self::ProtocolsHandler as ProtocolsHandler>::InEvent,
            Self::OutEvent,
        >,
    > {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }

        Poll::Pending
    }
}

/// Transmission between the `OneShotHandler` and the `FloodsubHandler`.
pub enum InnerMessage {
    /// We received an RPC from a remote.
    Rx(FloodsubRpc),
    /// We successfully sent an RPC request.
    Sent,
}

impl From<FloodsubRpc> for InnerMessage {
    #[inline]
    fn from(rpc: FloodsubRpc) -> InnerMessage {
        InnerMessage::Rx(rpc)
    }
}

impl From<()> for InnerMessage {
    #[inline]
    fn from(_: ()) -> InnerMessage {
        InnerMessage::Sent
    }
}

/// Event that can happen on the floodsub behaviour.
#[derive(Debug)]
pub enum FloodsubEvent {
    /// A message has been received.
    Message(FloodsubMessage),

    /// A remote subscribed to a topic.
    Subscribed {
        /// Remote that has subscribed.
        peer_id: PeerId,
        /// The topic it has subscribed to.
        topic: Topic,
    },

    /// A remote unsubscribed from a topic.
    Unsubscribed {
        /// Remote that has unsubscribed.
        peer_id: PeerId,
        /// The topic it has subscribed from.
        topic: Topic,
    },
}
