// Copyright 2020 Sigma Prime Pty Ltd.
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

use std::{
    cmp::{max, Ordering},
    collections::HashSet,
    collections::VecDeque,
    collections::{BTreeSet, HashMap},
    fmt,
    iter::FromIterator,
    net::IpAddr,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures::StreamExt;
use log::{debug, error, info, trace, warn};
use prost::Message;
use rand::{seq::SliceRandom, thread_rng};
use wasm_timer::{Instant, Interval};

use libp2p_core::{
    connection::ConnectionId, identity::Keypair, multiaddr::Protocol::Ip4,
    multiaddr::Protocol::Ip6, ConnectedPoint, Multiaddr, PeerId,
};
use libp2p_swarm::{
    DialPeerCondition, NetworkBehaviour, NetworkBehaviourAction, NotifyHandler, PollParameters,
    ProtocolsHandler,
};

use crate::backoff::BackoffStorage;
use crate::config::{GossipsubConfig, ValidationMode};
use crate::error::{PublishError, SubscriptionError, ValidationError};
use crate::gossip_promises::GossipPromises;
use crate::handler::{GossipsubHandler, HandlerEvent};
use crate::mcache::MessageCache;
use crate::peer_score::{PeerScore, PeerScoreParams, PeerScoreThresholds, RejectReason};
use crate::protocol::SIGNING_PREFIX;
use crate::subscription_filter::{AllowAllSubscriptionFilter, TopicSubscriptionFilter};
use crate::time_cache::{DuplicateCache, TimeCache};
use crate::topic::{Hasher, Topic, TopicHash};
use crate::transform::{DataTransform, IdentityTransform};
use crate::types::{
    FastMessageId, GossipsubControlAction, GossipsubMessage, GossipsubSubscription,
    GossipsubSubscriptionAction, MessageAcceptance, MessageId, PeerInfo, RawGossipsubMessage,
};
use crate::types::{GossipsubRpc, PeerKind};
use crate::{rpc_proto, TopicScoreParams};
use std::{cmp::Ordering::Equal, fmt::Debug};

#[cfg(test)]
mod tests;

/// Determines if published messages should be signed or not.
///
/// Without signing, a number of privacy preserving modes can be selected.
///
/// NOTE: The default validation settings are to require signatures. The [`ValidationMode`]
/// should be updated in the [`GossipsubConfig`] to allow for unsigned messages.
#[derive(Clone)]
pub enum MessageAuthenticity {
    /// Message signing is enabled. The author will be the owner of the key and the sequence number
    /// will be a random number.
    Signed(Keypair),
    /// Message signing is disabled.
    ///
    /// The specified [`PeerId`] will be used as the author of all published messages. The sequence
    /// number will be randomized.
    Author(PeerId),
    /// Message signing is disabled.
    ///
    /// A random [`PeerId`] will be used when publishing each message. The sequence number will be
    /// randomized.
    RandomAuthor,
    /// Message signing is disabled.
    ///
    /// The author of the message and the sequence numbers are excluded from the message.
    ///
    /// NOTE: Excluding these fields may make these messages invalid by other nodes who
    /// enforce validation of these fields. See [`ValidationMode`] in the [`GossipsubConfig`]
    /// for how to customise this for rust-libp2p gossipsub.  A custom `message_id`
    /// function will need to be set to prevent all messages from a peer being filtered
    /// as duplicates.
    Anonymous,
}

impl MessageAuthenticity {
    /// Returns true if signing is enabled.
    pub fn is_signing(&self) -> bool {
        matches!(self, MessageAuthenticity::Signed(_))
    }

    pub fn is_anonymous(&self) -> bool {
        matches!(self, MessageAuthenticity::Anonymous)
    }
}

/// Event that can be emitted by the gossipsub behaviour.
#[derive(Debug)]
pub enum GossipsubEvent {
    /// A message has been received.
    Message {
        /// The peer that forwarded us this message.
        propagation_source: PeerId,
        /// The [`MessageId`] of the message. This should be referenced by the application when
        /// validating a message (if required).
        message_id: MessageId,
        /// The decompressed message itself.
        message: GossipsubMessage,
    },
    /// A remote subscribed to a topic.
    Subscribed {
        /// Remote that has subscribed.
        peer_id: PeerId,
        /// The topic it has subscribed to.
        topic: TopicHash,
    },
    /// A remote unsubscribed from a topic.
    Unsubscribed {
        /// Remote that has unsubscribed.
        peer_id: PeerId,
        /// The topic it has subscribed from.
        topic: TopicHash,
    },
}

/// A data structure for storing configuration for publishing messages. See [`MessageAuthenticity`]
/// for further details.
enum PublishConfig {
    Signing {
        keypair: Keypair,
        author: PeerId,
        inline_key: Option<Vec<u8>>,
    },
    Author(PeerId),
    RandomAuthor,
    Anonymous,
}

impl PublishConfig {
    pub fn get_own_id(&self) -> Option<&PeerId> {
        match self {
            Self::Signing { author, .. } => Some(&author),
            Self::Author(author) => Some(&author),
            _ => None,
        }
    }
}

impl From<MessageAuthenticity> for PublishConfig {
    fn from(authenticity: MessageAuthenticity) -> Self {
        match authenticity {
            MessageAuthenticity::Signed(keypair) => {
                let public_key = keypair.public();
                let key_enc = public_key.clone().into_protobuf_encoding();
                let key = if key_enc.len() <= 42 {
                    // The public key can be inlined in [`rpc_proto::Message::from`], so we don't include it
                    // specifically in the [`rpc_proto::Message::key`] field.
                    None
                } else {
                    // Include the protobuf encoding of the public key in the message.
                    Some(key_enc)
                };

                PublishConfig::Signing {
                    keypair,
                    author: public_key.into_peer_id(),
                    inline_key: key,
                }
            }
            MessageAuthenticity::Author(peer_id) => PublishConfig::Author(peer_id),
            MessageAuthenticity::RandomAuthor => PublishConfig::RandomAuthor,
            MessageAuthenticity::Anonymous => PublishConfig::Anonymous,
        }
    }
}

type GossipsubNetworkBehaviourAction = NetworkBehaviourAction<Arc<rpc_proto::Rpc>, GossipsubEvent>;

/// Network behaviour that handles the gossipsub protocol.
///
/// NOTE: Initialisation requires a [`MessageAuthenticity`] and [`GossipsubConfig`] instance. If
/// message signing is disabled, the [`ValidationMode`] in the config should be adjusted to an
/// appropriate level to accept unsigned messages.
///
/// The DataTransform trait allows applications to optionally add extra encoding/decoding
/// functionality to the underlying messages. This is intended for custom compression algorithms.
///
/// The TopicSubscriptionFilter allows applications to implement specific filters on topics to
/// prevent unwanted messages being propagated and evaluated.
pub struct Gossipsub<
    D: DataTransform = IdentityTransform,
    F: TopicSubscriptionFilter = AllowAllSubscriptionFilter,
> {
    /// Configuration providing gossipsub performance parameters.
    config: GossipsubConfig,

    /// Events that need to be yielded to the outside when polling.
    events: VecDeque<GossipsubNetworkBehaviourAction>,

    /// Pools non-urgent control messages between heartbeats.
    control_pool: HashMap<PeerId, Vec<GossipsubControlAction>>,

    /// Information used for publishing messages.
    publish_config: PublishConfig,

    /// An LRU Time cache for storing seen messages (based on their ID). This cache prevents
    /// duplicates from being propagated to the application and on the network.
    duplicate_cache: DuplicateCache<MessageId>,

    /// A map of peers to their protocol kind. This is to identify different kinds of gossipsub
    /// peers.
    peer_protocols: HashMap<PeerId, PeerKind>,

    /// A map of all connected peers - A map of topic hash to a list of gossipsub peer Ids.
    topic_peers: HashMap<TopicHash, BTreeSet<PeerId>>,

    /// A map of all connected peers to their subscribed topics.
    peer_topics: HashMap<PeerId, BTreeSet<TopicHash>>,

    /// A set of all explicit peers. These are peers that remain connected and we unconditionally
    /// forward messages to, outside of the scoring system.
    explicit_peers: HashSet<PeerId>,

    /// A list of peers that have been blacklisted by the user.
    /// Messages are not sent to and are rejected from these peers.
    blacklisted_peers: HashSet<PeerId>,

    /// Overlay network of connected peers - Maps topics to connected gossipsub peers.
    mesh: HashMap<TopicHash, BTreeSet<PeerId>>,

    /// Map of topics to list of peers that we publish to, but don't subscribe to.
    fanout: HashMap<TopicHash, BTreeSet<PeerId>>,

    /// The last publish time for fanout topics.
    fanout_last_pub: HashMap<TopicHash, Instant>,

    ///Storage for backoffs
    backoffs: BackoffStorage,

    /// Message cache for the last few heartbeats.
    mcache: MessageCache,

    /// Heartbeat interval stream.
    heartbeat: Interval,

    /// Number of heartbeats since the beginning of time; this allows us to amortize some resource
    /// clean up -- eg backoff clean up.
    heartbeat_ticks: u64,

    /// We remember all peers we found through peer exchange, since those peers are not considered
    /// as safe as randomly discovered outbound peers. This behaviour diverges from the go
    /// implementation to avoid possible love bombing attacks in PX. When disconnecting peers will
    /// be removed from this list which may result in a true outbound rediscovery.
    px_peers: HashSet<PeerId>,

    /// Set of connected outbound peers (we only consider true outbound peers found through
    /// discovery and not by PX).
    outbound_peers: HashSet<PeerId>,

    /// Stores optional peer score data together with thresholds, decay interval and gossip
    /// promises.
    peer_score: Option<(PeerScore, PeerScoreThresholds, Interval, GossipPromises)>,

    /// Counts the number of `IHAVE` received from each peer since the last heartbeat.
    count_received_ihave: HashMap<PeerId, usize>,

    /// Counts the number of `IWANT` that we sent the each peer since the last heartbeat.
    count_sent_iwant: HashMap<PeerId, usize>,

    /// Short term cache for published messsage ids. This is used for penalizing peers sending
    /// our own messages back if the messages are anonymous or use a random author.
    published_message_ids: DuplicateCache<MessageId>,

    /// Short term cache for fast message ids mapping them to the real message ids
    fast_messsage_id_cache: TimeCache<FastMessageId, MessageId>,

    /// The filter used to handle message subscriptions.
    subscription_filter: F,

    /// A general transformation function that can be applied to data received from the wire before
    /// calculating the message-id and sending to the application. This is designed to allow the
    /// user to implement arbitrary topic-based compression algorithms.
    data_transform: D,
}

impl<D, F> Gossipsub<D, F>
where
    D: DataTransform + Default,
    F: TopicSubscriptionFilter + Default,
{
    /// Creates a [`Gossipsub`] struct given a set of parameters specified via a
    /// [`GossipsubConfig`]. This has no subscription filter and uses no compression.
    pub fn new(
        privacy: MessageAuthenticity,
        config: GossipsubConfig,
    ) -> Result<Self, &'static str> {
        Self::new_with_subscription_filter_and_transform(
            privacy,
            config,
            F::default(),
            D::default(),
        )
    }
}

impl<D, F> Gossipsub<D, F>
where
    D: DataTransform + Default,
    F: TopicSubscriptionFilter,
{
    /// Creates a [`Gossipsub`] struct given a set of parameters specified via a
    /// [`GossipsubConfig`] and a custom subscription filter.
    pub fn new_with_subscription_filter(
        privacy: MessageAuthenticity,
        config: GossipsubConfig,
        subscription_filter: F,
    ) -> Result<Self, &'static str> {
        Self::new_with_subscription_filter_and_transform(
            privacy,
            config,
            subscription_filter,
            D::default(),
        )
    }
}

impl<D, F> Gossipsub<D, F>
where
    D: DataTransform,
    F: TopicSubscriptionFilter + Default,
{
    /// Creates a [`Gossipsub`] struct given a set of parameters specified via a
    /// [`GossipsubConfig`] and a custom data transform.
    pub fn new_with_transform(
        privacy: MessageAuthenticity,
        config: GossipsubConfig,
        data_transform: D,
    ) -> Result<Self, &'static str> {
        Self::new_with_subscription_filter_and_transform(
            privacy,
            config,
            F::default(),
            data_transform,
        )
    }
}

impl<D, F> Gossipsub<D, F>
where
    D: DataTransform,
    F: TopicSubscriptionFilter,
{
    /// Creates a [`Gossipsub`] struct given a set of parameters specified via a
    /// [`GossipsubConfig`] and a custom subscription filter and data transform.
    pub fn new_with_subscription_filter_and_transform(
        privacy: MessageAuthenticity,
        config: GossipsubConfig,
        subscription_filter: F,
        data_transform: D,
    ) -> Result<Self, &'static str> {
        // Set up the router given the configuration settings.

        // We do not allow configurations where a published message would also be rejected if it
        // were received locally.
        validate_config(&privacy, &config.validation_mode())?;

        // Set up message publishing parameters.

        Ok(Gossipsub {
            events: VecDeque::new(),
            control_pool: HashMap::new(),
            publish_config: privacy.into(),
            duplicate_cache: DuplicateCache::new(config.duplicate_cache_time()),
            fast_messsage_id_cache: TimeCache::new(config.duplicate_cache_time()),
            topic_peers: HashMap::new(),
            peer_topics: HashMap::new(),
            explicit_peers: HashSet::new(),
            blacklisted_peers: HashSet::new(),
            mesh: HashMap::new(),
            fanout: HashMap::new(),
            fanout_last_pub: HashMap::new(),
            backoffs: BackoffStorage::new(
                &config.prune_backoff(),
                config.heartbeat_interval(),
                config.backoff_slack(),
            ),
            mcache: MessageCache::new(config.history_gossip(), config.history_length()),
            heartbeat: Interval::new_at(
                Instant::now() + config.heartbeat_initial_delay(),
                config.heartbeat_interval(),
            ),
            heartbeat_ticks: 0,
            px_peers: HashSet::new(),
            outbound_peers: HashSet::new(),
            peer_score: None,
            count_received_ihave: HashMap::new(),
            count_sent_iwant: HashMap::new(),
            peer_protocols: HashMap::new(),
            published_message_ids: DuplicateCache::new(config.published_message_ids_cache_time()),
            config,
            subscription_filter,
            data_transform,
        })
    }
}

impl<D, F> Gossipsub<D, F>
where
    D: DataTransform,
    F: TopicSubscriptionFilter,
{
    /// Lists the hashes of the topics we are currently subscribed to.
    pub fn topics(&self) -> impl Iterator<Item = &TopicHash> {
        self.mesh.keys()
    }

    /// Lists all mesh peers for a certain topic hash.
    pub fn mesh_peers(&self, topic_hash: &TopicHash) -> impl Iterator<Item = &PeerId> {
        self.mesh
            .get(topic_hash)
            .into_iter()
            .map(|x| x.iter())
            .flatten()
    }

    /// Lists all mesh peers for all topics.
    pub fn all_mesh_peers(&self) -> impl Iterator<Item = &PeerId> {
        let mut res = BTreeSet::new();
        for peers in self.mesh.values() {
            res.extend(peers);
        }
        res.into_iter()
    }

    /// Lists all known peers and their associated subscribed topics.
    pub fn all_peers(&self) -> impl Iterator<Item = (&PeerId, Vec<&TopicHash>)> {
        self.peer_topics
            .iter()
            .map(|(peer_id, topic_set)| (peer_id, topic_set.iter().collect()))
    }

    /// Lists all known peers and their associated protocol.
    pub fn peer_protocol(&self) -> impl Iterator<Item = (&PeerId, &PeerKind)> {
        self.peer_protocols.iter()
    }

    /// Returns the gossipsub score for a given peer, if one exists.
    pub fn peer_score(&self, peer_id: &PeerId) -> Option<f64> {
        self.peer_score
            .as_ref()
            .map(|(score, ..)| score.score(peer_id))
    }

    /// Subscribe to a topic.
    ///
    /// Returns [`Ok(true)`] if the subscription worked. Returns [`Ok(false)`] if we were already
    /// subscribed.
    pub fn subscribe<H: Hasher>(&mut self, topic: &Topic<H>) -> Result<bool, SubscriptionError> {
        debug!("Subscribing to topic: {}", topic);
        let topic_hash = topic.hash();
        if !self.subscription_filter.can_subscribe(&topic_hash) {
            return Err(SubscriptionError::NotAllowed);
        }

        if self.mesh.get(&topic_hash).is_some() {
            debug!("Topic: {} is already in the mesh.", topic);
            return Ok(false);
        }

        // send subscription request to all peers
        let peer_list = self.peer_topics.keys().cloned().collect::<Vec<_>>();
        if !peer_list.is_empty() {
            let event = Arc::new(
                GossipsubRpc {
                    messages: Vec::new(),
                    subscriptions: vec![GossipsubSubscription {
                        topic_hash: topic_hash.clone(),
                        action: GossipsubSubscriptionAction::Subscribe,
                    }],
                    control_msgs: Vec::new(),
                }
                .into_protobuf(),
            );

            for peer in peer_list {
                debug!("Sending SUBSCRIBE to peer: {:?}", peer);
                self.send_message(peer, event.clone())
                    .map_err(SubscriptionError::PublishError)?;
            }
        }

        // call JOIN(topic)
        // this will add new peers to the mesh for the topic
        self.join(&topic_hash);
        info!("Subscribed to topic: {}", topic);
        Ok(true)
    }

    /// Unsubscribes from a topic.
    ///
    /// Returns [`Ok(true)`] if we were subscribed to this topic.
    pub fn unsubscribe<H: Hasher>(&mut self, topic: &Topic<H>) -> Result<bool, PublishError> {
        debug!("Unsubscribing from topic: {}", topic);
        let topic_hash = topic.hash();

        if self.mesh.get(&topic_hash).is_none() {
            debug!("Already unsubscribed from topic: {:?}", topic_hash);
            // we are not subscribed
            return Ok(false);
        }

        // announce to all peers
        let peer_list = self.peer_topics.keys().cloned().collect::<Vec<_>>();
        if !peer_list.is_empty() {
            let event = Arc::new(
                GossipsubRpc {
                    messages: Vec::new(),
                    subscriptions: vec![GossipsubSubscription {
                        topic_hash: topic_hash.clone(),
                        action: GossipsubSubscriptionAction::Unsubscribe,
                    }],
                    control_msgs: Vec::new(),
                }
                .into_protobuf(),
            );

            for peer in peer_list {
                debug!("Sending UNSUBSCRIBE to peer: {}", peer.to_string());
                self.send_message(peer, event.clone())?;
            }
        }

        // call LEAVE(topic)
        // this will remove the topic from the mesh
        self.leave(&topic_hash);

        info!("Unsubscribed from topic: {:?}", topic_hash);
        Ok(true)
    }

    /// Publishes a message with multiple topics to the network.
    pub fn publish<H: Hasher>(
        &mut self,
        topic: Topic<H>,
        data: impl Into<Vec<u8>>,
    ) -> Result<MessageId, PublishError> {
        let data = data.into();

        // Transform the data before building a raw_message.
        let transformed_data = self
            .data_transform
            .outbound_transform(&topic.hash(), data.clone())?;

        let raw_message = self.build_raw_message(topic.into(), transformed_data)?;

        // calculate the message id from the un-transformed data
        let msg_id = self.config.message_id(&GossipsubMessage {
            source: raw_message.source,
            data, // the uncompressed form
            sequence_number: raw_message.sequence_number,
            topic: raw_message.topic.clone(),
        });

        let event = Arc::new(
            GossipsubRpc {
                subscriptions: Vec::new(),
                messages: vec![raw_message.clone()],
                control_msgs: Vec::new(),
            }
            .into_protobuf(),
        );

        // check that the size doesn't exceed the max transmission size
        if event.encoded_len() > self.config.max_transmit_size() {
            return Err(PublishError::MessageTooLarge);
        }

        // Check the if the message has been published before
        if self.duplicate_cache.contains(&msg_id) {
            // This message has already been seen. We don't re-publish messages that have already
            // been published on the network.
            warn!(
                "Not publishing a message that has already been published. Msg-id {}",
                msg_id
            );
            return Err(PublishError::Duplicate);
        }

        debug!("Publishing message: {:?}", msg_id);

        let topic_hash = raw_message.topic.clone();

        // If we are not flood publishing forward the message to mesh peers.
        let mesh_peers_sent =
            !self.config.flood_publish() && self.forward_msg(&msg_id, raw_message.clone(), None)?;

        let mut recipient_peers = HashSet::new();
        if let Some(set) = self.topic_peers.get(&topic_hash) {
            if self.config.flood_publish() {
                // Forward to all peers above score and all explicit peers
                recipient_peers.extend(
                    set.iter()
                        .filter(|p| {
                            self.explicit_peers.contains(*p)
                                || !self.score_below_threshold(*p, |ts| ts.publish_threshold).0
                        })
                        .cloned(),
                );
            } else {
                // Explicit peers
                for peer in &self.explicit_peers {
                    if set.contains(peer) {
                        recipient_peers.insert(*peer);
                    }
                }

                // Floodsub peers
                for (peer, kind) in &self.peer_protocols {
                    if kind == &PeerKind::Floodsub
                        && !self
                            .score_below_threshold(peer, |ts| ts.publish_threshold)
                            .0
                    {
                        recipient_peers.insert(*peer);
                    }
                }

                // Gossipsub peers
                if self.mesh.get(&topic_hash).is_none() {
                    debug!("Topic: {:?} not in the mesh", topic_hash);
                    // If we have fanout peers add them to the map.
                    if self.fanout.contains_key(&topic_hash) {
                        for peer in self.fanout.get(&topic_hash).expect("Topic must exist") {
                            recipient_peers.insert(*peer);
                        }
                    } else {
                        // We have no fanout peers, select mesh_n of them and add them to the fanout
                        let mesh_n = self.config.mesh_n();
                        let new_peers = get_random_peers(
                            &self.topic_peers,
                            &self.peer_protocols,
                            &topic_hash,
                            mesh_n,
                            {
                                |p| {
                                    !self.explicit_peers.contains(p)
                                        && !self
                                            .score_below_threshold(p, |pst| pst.publish_threshold)
                                            .0
                                }
                            },
                        );
                        // Add the new peers to the fanout and recipient peers
                        self.fanout.insert(topic_hash.clone(), new_peers.clone());
                        for peer in new_peers {
                            debug!("Peer added to fanout: {:?}", peer);
                            recipient_peers.insert(peer);
                        }
                    }
                    // We are publishing to fanout peers - update the time we published
                    self.fanout_last_pub
                        .insert(topic_hash.clone(), Instant::now());
                }
            }
        }

        if recipient_peers.is_empty() && !mesh_peers_sent {
            return Err(PublishError::InsufficientPeers);
        }

        // If the message isn't a duplicate and we have sent it to some peers add it to the
        // duplicate cache and memcache.
        self.duplicate_cache.insert(msg_id.clone());
        self.mcache.put(&msg_id, raw_message);

        // If the message is anonymous or has a random author add it to the published message ids
        // cache.
        if let PublishConfig::RandomAuthor | PublishConfig::Anonymous = self.publish_config {
            if !self.config.allow_self_origin() {
                self.published_message_ids.insert(msg_id.clone());
            }
        }

        // Send to peers we know are subscribed to the topic.
        for peer_id in recipient_peers.iter() {
            debug!("Sending message to peer: {:?}", peer_id);
            self.send_message(*peer_id, event.clone())?;
        }

        info!("Published message: {:?}", &msg_id);
        Ok(msg_id)
    }

    /// This function should be called when [`GossipsubConfig::validate_messages()`] is `true` after
    /// the message got validated by the caller. Messages are stored in the ['Memcache'] and
    /// validation is expected to be fast enough that the messages should still exist in the cache.
    /// There are three possible validation outcomes and the outcome is given in acceptance.
    ///
    /// If acceptance = [`MessageAcceptance::Accept`] the message will get propagated to the
    /// network. The `propagation_source` parameter indicates who the message was received by and
    /// will not be forwarded back to that peer.
    ///
    /// If acceptance = [`MessageAcceptance::Reject`] the message will be deleted from the memcache
    /// and the P₄ penalty will be applied to the `propagation_source`.
    //
    /// If acceptance = [`MessageAcceptance::Ignore`] the message will be deleted from the memcache
    /// but no P₄ penalty will be applied.
    ///
    /// This function will return true if the message was found in the cache and false if was not
    /// in the cache anymore.
    ///
    /// This should only be called once per message.
    pub fn report_message_validation_result(
        &mut self,
        msg_id: &MessageId,
        propagation_source: &PeerId,
        acceptance: MessageAcceptance,
    ) -> Result<bool, PublishError> {
        let reject_reason = match acceptance {
            MessageAcceptance::Accept => {
                let raw_message = match self.mcache.validate(msg_id) {
                    Some(raw_message) => raw_message.clone(),
                    None => {
                        warn!(
                            "Message not in cache. Ignoring forwarding. Message Id: {}",
                            msg_id
                        );
                        return Ok(false);
                    }
                };
                self.forward_msg(msg_id, raw_message, Some(propagation_source))?;
                return Ok(true);
            }
            MessageAcceptance::Reject => RejectReason::ValidationFailed,
            MessageAcceptance::Ignore => RejectReason::ValidationIgnored,
        };

        if let Some(raw_message) = self.mcache.remove(msg_id) {
            // Tell peer_score about reject
            if let Some((peer_score, ..)) = &mut self.peer_score {
                peer_score.reject_message(
                    propagation_source,
                    msg_id,
                    &raw_message.topic,
                    reject_reason,
                );
            }
            Ok(true)
        } else {
            warn!("Rejected message not in cache. Message Id: {}", msg_id);
            Ok(false)
        }
    }

    /// Adds a new peer to the list of explicitly connected peers.
    pub fn add_explicit_peer(&mut self, peer_id: &PeerId) {
        debug!("Adding explicit peer {}", peer_id);

        self.explicit_peers.insert(*peer_id);

        self.check_explicit_peer_connection(peer_id);
    }

    /// This removes the peer from explicitly connected peers, note that this does not disconnect
    /// the peer.
    pub fn remove_explicit_peer(&mut self, peer_id: &PeerId) {
        debug!("Removing explicit peer {}", peer_id);
        self.explicit_peers.remove(peer_id);
    }

    /// Blacklists a peer. All messages from this peer will be rejected and any message that was
    /// created by this peer will be rejected.
    pub fn blacklist_peer(&mut self, peer_id: &PeerId) {
        if self.blacklisted_peers.insert(*peer_id) {
            debug!("Peer has been blacklisted: {}", peer_id);
        }
    }

    /// Removes a peer from the blacklist if it has previously been blacklisted.
    pub fn remove_blacklisted_peer(&mut self, peer_id: &PeerId) {
        if self.blacklisted_peers.remove(peer_id) {
            debug!("Peer has been removed from the blacklist: {}", peer_id);
        }
    }

    /// Activates the peer scoring system with the given parameters. This will reset all scores
    /// if there was already another peer scoring system activated. Returns an error if the
    /// params are not valid or if they got already set.
    pub fn with_peer_score(
        &mut self,
        params: PeerScoreParams,
        threshold: PeerScoreThresholds,
    ) -> Result<(), String> {
        self.with_peer_score_and_message_delivery_time_callback(params, threshold, None)
    }

    /// Activates the peer scoring system with the given parameters and a message delivery time
    /// callback. Returns an error if the parameters got already set.
    pub fn with_peer_score_and_message_delivery_time_callback(
        &mut self,
        params: PeerScoreParams,
        threshold: PeerScoreThresholds,
        callback: Option<fn(&PeerId, &TopicHash, f64)>,
    ) -> Result<(), String> {
        params.validate()?;
        threshold.validate()?;

        if self.peer_score.is_some() {
            return Err("Peer score set twice".into());
        }

        let interval = Interval::new(params.decay_interval);
        let peer_score = PeerScore::new_with_message_delivery_time_callback(params, callback);
        self.peer_score = Some((peer_score, threshold, interval, GossipPromises::default()));
        Ok(())
    }

    /// Sets scoring parameters for a topic.
    ///
    /// The [`Self::with_peer_score()`] must first be called to initialise peer scoring.
    pub fn set_topic_params<H: Hasher>(
        &mut self,
        topic: Topic<H>,
        params: TopicScoreParams,
    ) -> Result<(), &'static str> {
        if let Some((peer_score, ..)) = &mut self.peer_score {
            peer_score.set_topic_params(topic.hash(), params);
            Ok(())
        } else {
            Err("Peer score must be initialised with `with_peer_score()`")
        }
    }

    /// Sets the application specific score for a peer. Returns true if scoring is active and
    /// the peer is connected or if the score of the peer is not yet expired, false otherwise.
    pub fn set_application_score(&mut self, peer_id: &PeerId, new_score: f64) -> bool {
        if let Some((peer_score, ..)) = &mut self.peer_score {
            peer_score.set_application_score(peer_id, new_score)
        } else {
            false
        }
    }

    /// Gossipsub JOIN(topic) - adds topic peers to mesh and sends them GRAFT messages.
    fn join(&mut self, topic_hash: &TopicHash) {
        debug!("Running JOIN for topic: {:?}", topic_hash);

        // if we are already in the mesh, return
        if self.mesh.contains_key(topic_hash) {
            info!("JOIN: The topic is already in the mesh, ignoring JOIN");
            return;
        }

        let mut added_peers = HashSet::new();

        // check if we have mesh_n peers in fanout[topic] and add them to the mesh if we do,
        // removing the fanout entry.
        if let Some((_, mut peers)) = self.fanout.remove_entry(topic_hash) {
            debug!(
                "JOIN: Removing peers from the fanout for topic: {:?}",
                topic_hash
            );

            // remove explicit peers, peers with negative scores, and backoffed peers
            peers = peers
                .into_iter()
                .filter(|p| {
                    !self.explicit_peers.contains(p)
                        && !self.score_below_threshold(p, |_| 0.0).0
                        && !self.backoffs.is_backoff_with_slack(topic_hash, p)
                })
                .collect();

            // Add up to mesh_n of them them to the mesh
            // NOTE: These aren't randomly added, currently FIFO
            let add_peers = std::cmp::min(peers.len(), self.config.mesh_n());
            debug!(
                "JOIN: Adding {:?} peers from the fanout for topic: {:?}",
                add_peers, topic_hash
            );
            added_peers.extend(peers.iter().cloned().take(add_peers));
            self.mesh.insert(
                topic_hash.clone(),
                peers.into_iter().take(add_peers).collect(),
            );
            // remove the last published time
            self.fanout_last_pub.remove(topic_hash);
        }

        // check if we need to get more peers, which we randomly select
        if added_peers.len() < self.config.mesh_n() {
            // get the peers
            let new_peers = get_random_peers(
                &self.topic_peers,
                &self.peer_protocols,
                topic_hash,
                self.config.mesh_n() - added_peers.len(),
                |peer| {
                    !added_peers.contains(peer)
                        && !self.explicit_peers.contains(peer)
                        && !self.score_below_threshold(peer, |_| 0.0).0
                        && !self.backoffs.is_backoff_with_slack(topic_hash, peer)
                },
            );
            added_peers.extend(new_peers.clone());
            // add them to the mesh
            debug!(
                "JOIN: Inserting {:?} random peers into the mesh",
                new_peers.len()
            );
            let mesh_peers = self
                .mesh
                .entry(topic_hash.clone())
                .or_insert_with(Default::default);
            mesh_peers.extend(new_peers);
        }

        for peer_id in added_peers {
            // Send a GRAFT control message
            info!("JOIN: Sending Graft message to peer: {:?}", peer_id);
            if let Some((peer_score, ..)) = &mut self.peer_score {
                peer_score.graft(&peer_id, topic_hash.clone());
            }
            Self::control_pool_add(
                &mut self.control_pool,
                peer_id,
                GossipsubControlAction::Graft {
                    topic_hash: topic_hash.clone(),
                },
            );
        }
        debug!("Completed JOIN for topic: {:?}", topic_hash);
    }

    /// Creates a PRUNE gossipsub action.
    fn make_prune(
        &mut self,
        topic_hash: &TopicHash,
        peer: &PeerId,
        do_px: bool,
    ) -> GossipsubControlAction {
        if let Some((peer_score, ..)) = &mut self.peer_score {
            peer_score.prune(peer, topic_hash.clone());
        }

        match self.peer_protocols.get(peer) {
            Some(PeerKind::Floodsub) => {
                error!("Attempted to prune a Floodsub peer");
            }
            Some(PeerKind::Gossipsub) => {
                // GossipSub v1.0 -- no peer exchange, the peer won't be able to parse it anyway
                return GossipsubControlAction::Prune {
                    topic_hash: topic_hash.clone(),
                    peers: Vec::new(),
                    backoff: None,
                };
            }
            None => {
                error!("Attempted to Prune an unknown peer");
            }
            _ => {} // Gossipsub 1.1 peer perform the `Prune`
        }

        // Select peers for peer exchange
        let peers = if do_px {
            get_random_peers(
                &self.topic_peers,
                &self.peer_protocols,
                &topic_hash,
                self.config.prune_peers(),
                |p| p != peer && !self.score_below_threshold(p, |_| 0.0).0,
            )
            .into_iter()
            .map(|p| PeerInfo { peer_id: Some(p) })
            .collect()
        } else {
            Vec::new()
        };

        // update backoff
        self.backoffs
            .update_backoff(topic_hash, peer, self.config.prune_backoff());

        GossipsubControlAction::Prune {
            topic_hash: topic_hash.clone(),
            peers,
            backoff: Some(self.config.prune_backoff().as_secs()),
        }
    }

    /// Gossipsub LEAVE(topic) - Notifies mesh\[topic\] peers with PRUNE messages.
    fn leave(&mut self, topic_hash: &TopicHash) {
        debug!("Running LEAVE for topic {:?}", topic_hash);

        // If our mesh contains the topic, send prune to peers and delete it from the mesh
        if let Some((_, peers)) = self.mesh.remove_entry(topic_hash) {
            for peer in peers {
                // Send a PRUNE control message
                info!("LEAVE: Sending PRUNE to peer: {:?}", peer);
                let control = self.make_prune(topic_hash, &peer, self.config.do_px());
                Self::control_pool_add(&mut self.control_pool, peer, control);
            }
        }
        debug!("Completed LEAVE for topic: {:?}", topic_hash);
    }

    /// Checks if the given peer is still connected and if not dials the peer again.
    fn check_explicit_peer_connection(&mut self, peer_id: &PeerId) {
        if !self.peer_topics.contains_key(peer_id) {
            // Connect to peer
            debug!("Connecting to explicit peer {:?}", peer_id);
            self.events.push_back(NetworkBehaviourAction::DialPeer {
                peer_id: *peer_id,
                condition: DialPeerCondition::Disconnected,
            });
        }
    }

    /// Determines if a peer's score is below a given `PeerScoreThreshold` chosen via the
    /// `threshold` parameter.
    fn score_below_threshold(
        &self,
        peer_id: &PeerId,
        threshold: impl Fn(&PeerScoreThresholds) -> f64,
    ) -> (bool, f64) {
        Self::score_below_threshold_from_scores(&self.peer_score, peer_id, threshold)
    }

    fn score_below_threshold_from_scores(
        peer_score: &Option<(PeerScore, PeerScoreThresholds, Interval, GossipPromises)>,
        peer_id: &PeerId,
        threshold: impl Fn(&PeerScoreThresholds) -> f64,
    ) -> (bool, f64) {
        if let Some((peer_score, thresholds, ..)) = peer_score {
            let score = peer_score.score(peer_id);
            if score < threshold(thresholds) {
                return (true, score);
            }
            (false, score)
        } else {
            (false, 0.0)
        }
    }

    /// Handles an IHAVE control message. Checks our cache of messages. If the message is unknown,
    /// requests it with an IWANT control message.
    fn handle_ihave(&mut self, peer_id: &PeerId, ihave_msgs: Vec<(TopicHash, Vec<MessageId>)>) {
        // We ignore IHAVE gossip from any peer whose score is below the gossip threshold
        if let (true, score) = self.score_below_threshold(peer_id, |pst| pst.gossip_threshold) {
            debug!(
                "IHAVE: ignoring peer {:?} with score below threshold [score = {}]",
                peer_id, score
            );
            return;
        }

        // IHAVE flood protection
        let peer_have = self
            .count_received_ihave
            .entry(*peer_id)
            .or_insert(0);
        *peer_have += 1;
        if *peer_have > self.config.max_ihave_messages() {
            debug!(
                "IHAVE: peer {} has advertised too many times ({}) within this heartbeat \
            interval; ignoring",
                peer_id, *peer_have
            );
            return;
        }

        if let Some(iasked) = self.count_sent_iwant.get(peer_id) {
            if *iasked >= self.config.max_ihave_length() {
                debug!(
                    "IHAVE: peer {} has already advertised too many messages ({}); ignoring",
                    peer_id, *iasked
                );
                return;
            }
        }

        debug!("Handling IHAVE for peer: {:?}", peer_id);

        // use a hashset to avoid duplicates efficiently
        let mut iwant_ids = HashSet::new();

        for (topic, ids) in ihave_msgs {
            // only process the message if we are subscribed
            if !self.mesh.contains_key(&topic) {
                debug!(
                    "IHAVE: Ignoring IHAVE - Not subscribed to topic: {:?}",
                    topic
                );
                continue;
            }

            for id in ids {
                if !self.duplicate_cache.contains(&id) {
                    // have not seen this message, request it
                    iwant_ids.insert(id);
                }
            }
        }

        if !iwant_ids.is_empty() {
            let iasked = self.count_sent_iwant.entry(*peer_id).or_insert(0);
            let mut iask = iwant_ids.len();
            if *iasked + iask > self.config.max_ihave_length() {
                iask = self.config.max_ihave_length().saturating_sub(*iasked);
            }

            // Send the list of IWANT control messages
            debug!(
                "IHAVE: Asking for {} out of {} messages from {}",
                iask,
                iwant_ids.len(),
                peer_id
            );

            //ask in random order
            let mut iwant_ids_vec: Vec<_> = iwant_ids.iter().collect();
            let mut rng = thread_rng();
            iwant_ids_vec.partial_shuffle(&mut rng, iask as usize);

            iwant_ids_vec.truncate(iask as usize);
            *iasked += iask;

            let message_ids = iwant_ids_vec.into_iter().cloned().collect::<Vec<_>>();
            if let Some((_, _, _, gossip_promises)) = &mut self.peer_score {
                gossip_promises.add_promise(
                    *peer_id,
                    &message_ids,
                    Instant::now() + self.config.iwant_followup_time(),
                );
            }
            debug!(
                "IHAVE: Asking for the following messages from {}: {:?}",
                peer_id, message_ids
            );

            Self::control_pool_add(
                &mut self.control_pool,
                *peer_id,
                GossipsubControlAction::IWant { message_ids },
            );
        }
        debug!("Completed IHAVE handling for peer: {:?}", peer_id);
    }

    /// Handles an IWANT control message. Checks our cache of messages. If the message exists it is
    /// forwarded to the requesting peer.
    fn handle_iwant(&mut self, peer_id: &PeerId, iwant_msgs: Vec<MessageId>) {
        // We ignore IWANT gossip from any peer whose score is below the gossip threshold
        if let (true, score) = self.score_below_threshold(peer_id, |pst| pst.gossip_threshold) {
            debug!(
                "IWANT: ignoring peer {:?} with score below threshold [score = {}]",
                peer_id, score
            );
            return;
        }

        debug!("Handling IWANT for peer: {:?}", peer_id);
        // build a hashmap of available messages
        let mut cached_messages = HashMap::new();

        for id in iwant_msgs {
            // If we have it and the IHAVE count is not above the threshold, add it do the
            // cached_messages mapping
            if let Some((msg, count)) = self.mcache.get_with_iwant_counts(&id, peer_id) {
                if count > self.config.gossip_retransimission() {
                    debug!(
                        "IWANT: Peer {} has asked for message {} too many times; ignoring \
                    request",
                        peer_id, &id
                    );
                } else {
                    cached_messages.insert(id.clone(), msg.clone());
                }
            }
        }

        if !cached_messages.is_empty() {
            debug!("IWANT: Sending cached messages to peer: {:?}", peer_id);
            // Send the messages to the peer
            let message_list = cached_messages
                .into_iter()
                .map(|entry| entry.1)
                .collect();
            if self
                .send_message(
                    *peer_id,
                    GossipsubRpc {
                        subscriptions: Vec::new(),
                        messages: message_list,
                        control_msgs: Vec::new(),
                    }
                    .into_protobuf(),
                )
                .is_err()
            {
                error!("Failed to send cached messages. Messages too large");
            }
        }
        debug!("Completed IWANT handling for peer: {}", peer_id);
    }

    /// Handles GRAFT control messages. If subscribed to the topic, adds the peer to mesh, if not,
    /// responds with PRUNE messages.
    fn handle_graft(&mut self, peer_id: &PeerId, topics: Vec<TopicHash>) {
        debug!("Handling GRAFT message for peer: {}", peer_id);

        let mut to_prune_topics = HashSet::new();

        let mut do_px = self.config.do_px();

        // we don't GRAFT to/from explicit peers; complain loudly if this happens
        if self.explicit_peers.contains(peer_id) {
            warn!("GRAFT: ignoring request from direct peer {}", peer_id);
            // this is possibly a bug from non-reciprocal configuration; send a PRUNE for all topics
            to_prune_topics = HashSet::from_iter(topics.into_iter());
            // but don't PX
            do_px = false
        } else {
            let (below_zero, score) = self.score_below_threshold(peer_id, |_| 0.0);
            let now = Instant::now();
            for topic_hash in topics {
                if let Some(peers) = self.mesh.get_mut(&topic_hash) {
                    // if the peer is already in the mesh ignore the graft
                    if peers.contains(peer_id) {
                        debug!(
                            "GRAFT: Received graft for peer {:?} that is already in topic {:?}",
                            peer_id, &topic_hash
                        );
                        continue;
                    }

                    // make sure we are not backing off that peer
                    if let Some(backoff_time) = self.backoffs.get_backoff_time(&topic_hash, peer_id)
                    {
                        if backoff_time > now {
                            warn!(
                                "GRAFT: peer attempted graft within backoff time, penalizing {}",
                                peer_id
                            );
                            // add behavioural penalty
                            if let Some((peer_score, ..)) = &mut self.peer_score {
                                peer_score.add_penalty(peer_id, 1);

                                // check the flood cutoff
                                let flood_cutoff = (backoff_time
                                    + self.config.graft_flood_threshold())
                                    - self.config.prune_backoff();
                                if flood_cutoff > now {
                                    //extra penalty
                                    peer_score.add_penalty(peer_id, 1);
                                }
                            }
                            //no PX
                            do_px = false;

                            to_prune_topics.insert(topic_hash.clone());
                            continue;
                        }
                    }

                    //check the score
                    if below_zero {
                        // we don't GRAFT peers with negative score
                        debug!(
                            "GRAFT: ignoring peer {:?} with negative score [score = {}, \
                        topic = {}]",
                            peer_id, score, topic_hash
                        );
                        // we do send them PRUNE however, because it's a matter of protocol correctness
                        to_prune_topics.insert(topic_hash.clone());
                        // but we won't PX to them
                        do_px = false;
                        continue;
                    }

                    // check mesh upper bound and only allow graft if the upper bound is not reached or
                    // if it is an outbound peer
                    if peers.len() >= self.config.mesh_n_high()
                        && !self.outbound_peers.contains(peer_id)
                    {
                        to_prune_topics.insert(topic_hash.clone());
                        continue;
                    }

                    // add peer to the mesh
                    info!(
                        "GRAFT: Mesh link added for peer: {:?} in topic: {:?}",
                        peer_id, &topic_hash
                    );
                    peers.insert(*peer_id);

                    if let Some((peer_score, ..)) = &mut self.peer_score {
                        peer_score.graft(peer_id, topic_hash);
                    }
                } else {
                    // don't do PX when there is an unknown topic to avoid leaking our peers
                    do_px = false;
                    debug!(
                        "GRAFT: Received graft for unknown topic {:?} from peer {:?}",
                        &topic_hash, peer_id
                    );
                    // spam hardening: ignore GRAFTs for unknown topics
                    continue;
                }
            }
        }

        if !to_prune_topics.is_empty() {
            // build the prune messages to send
            let prune_messages = to_prune_topics
                .iter()
                .map(|t| self.make_prune(t, peer_id, do_px))
                .collect();
            // Send the prune messages to the peer
            info!(
                "GRAFT: Not subscribed to topics -  Sending PRUNE to peer: {}",
                peer_id
            );

            if self
                .send_message(
                    *peer_id,
                    GossipsubRpc {
                        subscriptions: Vec::new(),
                        messages: Vec::new(),
                        control_msgs: prune_messages,
                    }
                    .into_protobuf(),
                )
                .is_err()
            {
                error!("Failed to send graft. Message too large");
            }
        }
        debug!("Completed GRAFT handling for peer: {}", peer_id);
    }

    fn remove_peer_from_mesh(
        &mut self,
        peer_id: &PeerId,
        topic_hash: &TopicHash,
        backoff: Option<u64>,
        always_update_backoff: bool,
    ) {
        let mut update_backoff = always_update_backoff;
        if let Some(peers) = self.mesh.get_mut(&topic_hash) {
            // remove the peer if it exists in the mesh
            if peers.remove(peer_id) {
                info!(
                    "PRUNE: Removing peer: {} from the mesh for topic: {}",
                    peer_id.to_string(),
                    topic_hash
                );

                if let Some((peer_score, ..)) = &mut self.peer_score {
                    peer_score.prune(peer_id, topic_hash.clone());
                }

                update_backoff = true;
            }
        }
        if update_backoff {
            let time = if let Some(backoff) = backoff {
                Duration::from_secs(backoff)
            } else {
                self.config.prune_backoff()
            };
            // is there a backoff specified by the peer? if so obey it.
            self.backoffs.update_backoff(&topic_hash, peer_id, time);
        }
    }

    /// Handles PRUNE control messages. Removes peer from the mesh.
    fn handle_prune(
        &mut self,
        peer_id: &PeerId,
        prune_data: Vec<(TopicHash, Vec<PeerInfo>, Option<u64>)>,
    ) {
        debug!("Handling PRUNE message for peer: {}", peer_id);
        let (below_threshold, score) =
            self.score_below_threshold(peer_id, |pst| pst.accept_px_threshold);
        for (topic_hash, px, backoff) in prune_data {
            self.remove_peer_from_mesh(peer_id, &topic_hash, backoff, true);

            if self.mesh.contains_key(&topic_hash) {
                //connect to px peers
                if !px.is_empty() {
                    // we ignore PX from peers with insufficient score
                    if below_threshold {
                        debug!(
                            "PRUNE: ignoring PX from peer {:?} with insufficient score \
                             [score ={} topic = {}]",
                            peer_id, score, topic_hash
                        );
                        continue;
                    }

                    // NOTE: We cannot dial any peers from PX currently as we typically will not
                    // know their multiaddr. Until SignedRecords are spec'd this
                    // remains a stub. By default `config.prune_peers()` is set to zero and
                    // this is skipped. If the user modifies this, this will only be able to
                    // dial already known peers (from an external discovery mechanism for
                    // example).
                    if self.config.prune_peers() > 0 {
                        self.px_connect(px);
                    }
                }
            }
        }
        debug!("Completed PRUNE handling for peer: {}", peer_id.to_string());
    }

    fn px_connect(&mut self, mut px: Vec<PeerInfo>) {
        let n = self.config.prune_peers();
        // Ignore peerInfo with no ID
        //
        //TODO: Once signed records are spec'd: Can we use peerInfo without any IDs if they have a
        // signed peer record?
        px = px.into_iter().filter(|p| p.peer_id.is_some()).collect();
        if px.len() > n {
            // only use at most prune_peers many random peers
            let mut rng = thread_rng();
            px.partial_shuffle(&mut rng, n);
            px = px.into_iter().take(n).collect();
        }

        for p in px {
            // TODO: Once signed records are spec'd: extract signed peer record if given and handle
            // it, see https://github.com/libp2p/specs/pull/217
            if let Some(peer_id) = p.peer_id {
                // mark as px peer
                self.px_peers.insert(peer_id);

                // dial peer
                self.events.push_back(NetworkBehaviourAction::DialPeer {
                    peer_id,
                    condition: DialPeerCondition::Disconnected,
                });
            }
        }
    }

    /// Applies some basic checks to whether this message is valid. Does not apply user validation
    /// checks.
    fn message_is_valid(
        &mut self,
        msg_id: &MessageId,
        raw_message: &mut RawGossipsubMessage,
        propagation_source: &PeerId,
    ) -> bool {
        debug!(
            "Handling message: {:?} from peer: {}",
            msg_id,
            propagation_source.to_string()
        );

        // Reject any message from a blacklisted peer
        if self.blacklisted_peers.contains(propagation_source) {
            debug!(
                "Rejecting message from blacklisted peer: {}",
                propagation_source
            );
            if let Some((peer_score, .., gossip_promises)) = &mut self.peer_score {
                peer_score.reject_message(
                    propagation_source,
                    msg_id,
                    &raw_message.topic,
                    RejectReason::BlackListedPeer,
                );
                gossip_promises.reject_message(msg_id, &RejectReason::BlackListedPeer);
            }
            return false;
        }

        // Also reject any message that originated from a blacklisted peer
        if let Some(source) = raw_message.source.as_ref() {
            if self.blacklisted_peers.contains(source) {
                debug!(
                    "Rejecting message from peer {} because of blacklisted source: {}",
                    propagation_source, source
                );
                if let Some((peer_score, .., gossip_promises)) = &mut self.peer_score {
                    peer_score.reject_message(
                        propagation_source,
                        msg_id,
                        &raw_message.topic,
                        RejectReason::BlackListedSource,
                    );
                    gossip_promises.reject_message(msg_id, &RejectReason::BlackListedSource);
                }
                return false;
            }
        }

        // If we are not validating messages, assume this message is validated
        // This will allow the message to be gossiped without explicitly calling
        // `validate_message`.
        if !self.config.validate_messages() {
            raw_message.validated = true;
        }

        // reject messages claiming to be from ourselves but not locally published
        let self_published = !self.config.allow_self_origin()
            && if let Some(own_id) = self.publish_config.get_own_id() {
                own_id != propagation_source
                    && raw_message.source.as_ref().map_or(false, |s| s == own_id)
            } else {
                self.published_message_ids.contains(&msg_id)
            };

        if self_published {
            debug!(
                "Dropping message {} claiming to be from self but forwarded from {}",
                msg_id, propagation_source
            );
            if let Some((peer_score, _, _, gossip_promises)) = &mut self.peer_score {
                peer_score.reject_message(
                    propagation_source,
                    msg_id,
                    &raw_message.topic,
                    RejectReason::SelfOrigin,
                );
                gossip_promises.reject_message(msg_id, &RejectReason::SelfOrigin);
            }
            return false;
        }

        true
    }

    /// Handles a newly received [`RawGossipsubMessage`].
    ///
    /// Forwards the message to all peers in the mesh.
    fn handle_received_message(
        &mut self,
        mut raw_message: RawGossipsubMessage,
        propagation_source: &PeerId,
    ) {
        let fast_message_id = self.config.fast_message_id(&raw_message);
        if let Some(fast_message_id) = fast_message_id.as_ref() {
            if let Some(msg_id) = self.fast_messsage_id_cache.get(fast_message_id) {
                let msg_id = msg_id.clone();
                self.message_is_valid(&msg_id, &mut raw_message, propagation_source);
                if let Some((peer_score, ..)) = &mut self.peer_score {
                    peer_score.duplicated_message(propagation_source, &msg_id, &raw_message.topic);
                }
                return;
            }
        }

        // Try and perform the data transform to the message. If it fails, consider it invalid.
        let message = match self.data_transform.inbound_transform(raw_message.clone()) {
            Ok(message) => message,
            Err(e) => {
                debug!("Invalid message. Transform error: {:?}", e);
                // Reject the message and return
                self.handle_invalid_message(
                    propagation_source,
                    raw_message,
                    ValidationError::TransformFailed,
                );
                return;
            }
        };

        // Calculate the message id on the transformed data.
        let msg_id = self.config.message_id(&message);

        // Check the validity of the message
        // Peers get penalized if this message is invalid. We don't add it to the duplicate cache
        // and instead continually penalize peers that repeatedly send this message.
        if !self.message_is_valid(&msg_id, &mut raw_message, propagation_source) {
            return;
        }

        // Add the message to the duplicate caches
        if let Some(fast_message_id) = fast_message_id {
            // add id to cache
            self.fast_messsage_id_cache
                .entry(fast_message_id)
                .or_insert_with(|| msg_id.clone());
        }
        if !self.duplicate_cache.insert(msg_id.clone()) {
            debug!(
                "Message already received, ignoring. Message: {}",
                msg_id
            );
            if let Some((peer_score, ..)) = &mut self.peer_score {
                peer_score.duplicated_message(propagation_source, &msg_id, &message.topic);
            }
            return;
        }
        debug!(
            "Put message {:?} in duplicate_cache and resolve promises",
            msg_id
        );

        // Tells score that message arrived (but is maybe not fully validated yet).
        // Consider the message as delivered for gossip promises.
        if let Some((peer_score, .., gossip_promises)) = &mut self.peer_score {
            peer_score.validate_message(propagation_source, &msg_id, &message.topic);
            gossip_promises.message_delivered(&msg_id);
        }

        // Add the message to our memcache
        self.mcache.put(&msg_id, raw_message.clone());

        // Dispatch the message to the user if we are subscribed to any of the topics
        if self.mesh.contains_key(&message.topic) {
            debug!("Sending received message to user");
            self.events.push_back(NetworkBehaviourAction::GenerateEvent(
                GossipsubEvent::Message {
                    propagation_source: *propagation_source,
                    message_id: msg_id.clone(),
                    message,
                },
            ));
        } else {
            debug!(
                "Received message on a topic we are not subscribed to: {:?}",
                message.topic
            );
            return;
        }

        // forward the message to mesh peers, if no validation is required
        if !self.config.validate_messages() {
            if self
                .forward_msg(&msg_id, raw_message, Some(propagation_source))
                .is_err()
            {
                error!("Failed to forward message. Too large");
            }
            debug!("Completed message handling for message: {:?}", msg_id);
        }
    }

    // Handles invalid messages received.
    fn handle_invalid_message(
        &mut self,
        propagation_source: &PeerId,
        raw_message: RawGossipsubMessage,
        validation_error: ValidationError,
    ) {
        if let Some((peer_score, .., gossip_promises)) = &mut self.peer_score {
            let reason = RejectReason::ValidationError(validation_error);
            let fast_message_id_cache = &self.fast_messsage_id_cache;
            if let Some(msg_id) = self
                .config
                .fast_message_id(&raw_message)
                .and_then(|id| fast_message_id_cache.get(&id))
            {
                peer_score.reject_message(propagation_source, msg_id, &raw_message.topic, reason);
                gossip_promises.reject_message(msg_id, &reason);
            } else {
                // The message is invalid, we reject it ignoring any gossip promises. If a peer is
                // advertising this message via an IHAVE and it's invalid it will be double
                // penalized, one for sending us an invalid and again for breaking a promise.
                peer_score.reject_invalid_message(propagation_source, &raw_message.topic);
            }
        }
    }

    /// Handles received subscriptions.
    fn handle_received_subscriptions(
        &mut self,
        subscriptions: &[GossipsubSubscription],
        propagation_source: &PeerId,
    ) {
        debug!(
            "Handling subscriptions: {:?}, from source: {}",
            subscriptions,
            propagation_source.to_string()
        );

        let mut unsubscribed_peers = Vec::new();

        let subscribed_topics = match self.peer_topics.get_mut(propagation_source) {
            Some(topics) => topics,
            None => {
                error!(
                    "Subscription by unknown peer: {}",
                    propagation_source.to_string()
                );
                return;
            }
        };

        // Collect potential graft messages for the peer.
        let mut grafts = Vec::new();

        // Notify the application about the subscription, after the grafts are sent.
        let mut application_event = Vec::new();

        let filtered_topics = match self
            .subscription_filter
            .filter_incoming_subscriptions(subscriptions, subscribed_topics)
        {
            Ok(topics) => topics,
            Err(s) => {
                error!(
                    "Subscription filter error: {}; ignoring RPC from peer {}",
                    s,
                    propagation_source.to_string()
                );
                return;
            }
        };

        for subscription in filtered_topics {
            // get the peers from the mapping, or insert empty lists if the topic doesn't exist
            let peer_list = self
                .topic_peers
                .entry(subscription.topic_hash.clone())
                .or_insert_with(Default::default);

            match subscription.action {
                GossipsubSubscriptionAction::Subscribe => {
                    if peer_list.insert(*propagation_source) {
                        debug!(
                            "SUBSCRIPTION: Adding gossip peer: {} to topic: {:?}",
                            propagation_source.to_string(),
                            subscription.topic_hash
                        );
                    }

                    // add to the peer_topics mapping
                    subscribed_topics.insert(subscription.topic_hash.clone());

                    // if the mesh needs peers add the peer to the mesh
                    if !self.explicit_peers.contains(propagation_source)
                        && match self.peer_protocols.get(propagation_source) {
                            Some(PeerKind::Gossipsubv1_1) => true,
                            Some(PeerKind::Gossipsub) => true,
                            _ => false,
                        }
                        && !Self::score_below_threshold_from_scores(
                            &self.peer_score,
                            propagation_source,
                            |_| 0.0,
                        )
                        .0
                        && !self
                            .backoffs
                            .is_backoff_with_slack(&subscription.topic_hash, propagation_source)
                    {
                        if let Some(peers) = self.mesh.get_mut(&subscription.topic_hash) {
                            if peers.len() < self.config.mesh_n_low()
                                && peers.insert(*propagation_source)
                            {
                                debug!(
                                    "SUBSCRIPTION: Adding peer {} to the mesh for topic {:?}",
                                    propagation_source.to_string(),
                                    subscription.topic_hash
                                );
                                // send graft to the peer
                                debug!(
                                    "Sending GRAFT to peer {} for topic {:?}",
                                    propagation_source.to_string(),
                                    subscription.topic_hash
                                );
                                if let Some((peer_score, ..)) = &mut self.peer_score {
                                    peer_score
                                        .graft(propagation_source, subscription.topic_hash.clone());
                                }
                                grafts.push(GossipsubControlAction::Graft {
                                    topic_hash: subscription.topic_hash.clone(),
                                });
                            }
                        }
                    }
                    // generates a subscription event to be polled
                    application_event.push(NetworkBehaviourAction::GenerateEvent(
                        GossipsubEvent::Subscribed {
                            peer_id: *propagation_source,
                            topic: subscription.topic_hash.clone(),
                        },
                    ));
                }
                GossipsubSubscriptionAction::Unsubscribe => {
                    if peer_list.remove(propagation_source) {
                        info!(
                            "SUBSCRIPTION: Removing gossip peer: {} from topic: {:?}",
                            propagation_source.to_string(),
                            subscription.topic_hash
                        );
                    }
                    // remove topic from the peer_topics mapping
                    subscribed_topics.remove(&subscription.topic_hash);
                    unsubscribed_peers
                        .push((*propagation_source, subscription.topic_hash.clone()));
                    // generate an unsubscribe event to be polled
                    application_event.push(NetworkBehaviourAction::GenerateEvent(
                        GossipsubEvent::Unsubscribed {
                            peer_id: *propagation_source,
                            topic: subscription.topic_hash.clone(),
                        },
                    ));
                }
            }
        }

        // remove unsubscribed peers from the mesh if it exists
        for (peer_id, topic_hash) in unsubscribed_peers {
            self.remove_peer_from_mesh(&peer_id, &topic_hash, None, false);
        }

        // If we need to send grafts to peer, do so immediately, rather than waiting for the
        // heartbeat.
        if !grafts.is_empty()
            && self
                .send_message(
                    *propagation_source,
                    GossipsubRpc {
                        subscriptions: Vec::new(),
                        messages: Vec::new(),
                        control_msgs: grafts,
                    }
                    .into_protobuf(),
                )
                .is_err()
        {
            error!("Failed sending grafts. Message too large");
        }

        // Notify the application of the subscriptions
        for event in application_event {
            self.events.push_back(event);
        }

        trace!(
            "Completed handling subscriptions from source: {:?}",
            propagation_source
        );
    }

    /// Applies penalties to peers that did not respond to our IWANT requests.
    fn apply_iwant_penalties(&mut self) {
        if let Some((peer_score, .., gossip_promises)) = &mut self.peer_score {
            for (peer, count) in gossip_promises.get_broken_promises() {
                peer_score.add_penalty(&peer, count);
            }
        }
    }

    /// Heartbeat function which shifts the memcache and updates the mesh.
    fn heartbeat(&mut self) {
        debug!("Starting heartbeat");

        self.heartbeat_ticks += 1;

        let mut to_graft = HashMap::new();
        let mut to_prune = HashMap::new();
        let mut no_px = HashSet::new();

        // clean up expired backoffs
        self.backoffs.heartbeat();

        // clean up ihave counters
        self.count_sent_iwant.clear();
        self.count_received_ihave.clear();

        // apply iwant penalties
        self.apply_iwant_penalties();

        // check connections to explicit peers
        if self.heartbeat_ticks % self.config.check_explicit_peers_ticks() == 0 {
            for p in self.explicit_peers.clone() {
                self.check_explicit_peer_connection(&p);
            }
        }

        // cache scores throughout the heartbeat
        let mut scores = HashMap::new();
        let peer_score = &self.peer_score;
        let mut score = |p: &PeerId| match peer_score {
            Some((peer_score, ..)) => *scores
                .entry(*p)
                .or_insert_with(|| peer_score.score(p)),
            _ => 0.0,
        };

        // maintain the mesh for each topic
        for (topic_hash, peers) in self.mesh.iter_mut() {
            let explicit_peers = &self.explicit_peers;
            let backoffs = &self.backoffs;
            let topic_peers = &self.topic_peers;
            let outbound_peers = &self.outbound_peers;

            // drop all peers with negative score, without PX
            // if there is at some point a stable retain method for BTreeSet the following can be
            // written more efficiently with retain.
            let to_remove: Vec<_> = peers
                .iter()
                .filter(|&p| {
                    if score(p) < 0.0 {
                        debug!(
                            "HEARTBEAT: Prune peer {:?} with negative score [score = {}, topic = \
                             {}]",
                            p,
                            score(p),
                            topic_hash
                        );

                        let current_topic = to_prune.entry(*p).or_insert_with(Vec::new);
                        current_topic.push(topic_hash.clone());
                        no_px.insert(*p);
                        true
                    } else {
                        false
                    }
                })
                .cloned()
                .collect();
            for peer in to_remove {
                peers.remove(&peer);
            }

            // too little peers - add some
            if peers.len() < self.config.mesh_n_low() {
                debug!(
                    "HEARTBEAT: Mesh low. Topic: {} Contains: {} needs: {}",
                    topic_hash,
                    peers.len(),
                    self.config.mesh_n_low()
                );
                // not enough peers - get mesh_n - current_length more
                let desired_peers = self.config.mesh_n() - peers.len();
                let peer_list = get_random_peers(
                    topic_peers,
                    &self.peer_protocols,
                    topic_hash,
                    desired_peers,
                    |peer| {
                        !peers.contains(peer)
                            && !explicit_peers.contains(peer)
                            && !backoffs.is_backoff_with_slack(topic_hash, peer)
                            && score(peer) >= 0.0
                    },
                );
                for peer in &peer_list {
                    let current_topic = to_graft.entry(*peer).or_insert_with(Vec::new);
                    current_topic.push(topic_hash.clone());
                }
                // update the mesh
                debug!("Updating mesh, new mesh: {:?}", peer_list);
                peers.extend(peer_list);
            }

            // too many peers - remove some
            if peers.len() > self.config.mesh_n_high() {
                debug!(
                    "HEARTBEAT: Mesh high. Topic: {} Contains: {} needs: {}",
                    topic_hash,
                    peers.len(),
                    self.config.mesh_n_high()
                );
                let excess_peer_no = peers.len() - self.config.mesh_n();

                // shuffle the peers and then sort by score ascending beginning with the worst
                let mut rng = thread_rng();
                let mut shuffled = peers.iter().cloned().collect::<Vec<_>>();
                shuffled.shuffle(&mut rng);
                shuffled
                    .sort_by(|p1, p2| score(p1).partial_cmp(&score(p2)).unwrap_or(Ordering::Equal));
                // shuffle everything except the last retain_scores many peers (the best ones)
                shuffled[..peers.len() - self.config.retain_scores()].shuffle(&mut rng);

                // count total number of outbound peers
                let mut outbound = {
                    let outbound_peers = &self.outbound_peers;
                    shuffled
                        .iter()
                        .filter(|p| outbound_peers.contains(*p))
                        .count()
                };

                // remove the first excess_peer_no allowed (by outbound restrictions) peers adding
                // them to to_prune
                let mut removed = 0;
                for peer in shuffled {
                    if removed == excess_peer_no {
                        break;
                    }
                    if self.outbound_peers.contains(&peer) {
                        if outbound <= self.config.mesh_outbound_min() {
                            //do not remove anymore outbound peers
                            continue;
                        } else {
                            //an outbound peer gets removed
                            outbound -= 1;
                        }
                    }

                    //remove the peer
                    peers.remove(&peer);
                    let current_topic = to_prune.entry(peer).or_insert_with(Vec::new);
                    current_topic.push(topic_hash.clone());
                    removed += 1;
                }
            }

            // do we have enough outbound peers?
            if peers.len() >= self.config.mesh_n_low() {
                // count number of outbound peers we have
                let outbound = { peers.iter().filter(|p| outbound_peers.contains(*p)).count() };

                // if we have not enough outbound peers, graft to some new outbound peers
                if outbound < self.config.mesh_outbound_min() {
                    let needed = self.config.mesh_outbound_min() - outbound;
                    let peer_list = get_random_peers(
                        topic_peers,
                        &self.peer_protocols,
                        topic_hash,
                        needed,
                        |peer| {
                            !peers.contains(peer)
                                && !explicit_peers.contains(peer)
                                && !backoffs.is_backoff_with_slack(topic_hash, peer)
                                && score(peer) >= 0.0
                                && outbound_peers.contains(peer)
                        },
                    );
                    for peer in &peer_list {
                        let current_topic = to_graft.entry(*peer).or_insert_with(Vec::new);
                        current_topic.push(topic_hash.clone());
                    }
                    // update the mesh
                    debug!("Updating mesh, new mesh: {:?}", peer_list);
                    peers.extend(peer_list);
                }
            }

            // should we try to improve the mesh with opportunistic grafting?
            if self.heartbeat_ticks % self.config.opportunistic_graft_ticks() == 0
                && peers.len() > 1
                && self.peer_score.is_some()
            {
                if let Some((_, thresholds, _, _)) = &self.peer_score {
                    // Opportunistic grafting works as follows: we check the median score of peers
                    // in the mesh; if this score is below the opportunisticGraftThreshold, we
                    // select a few peers at random with score over the median.
                    // The intention is to (slowly) improve an underperforming mesh by introducing
                    // good scoring peers that may have been gossiping at us. This allows us to
                    // get out of sticky situations where we are stuck with poor peers and also
                    // recover from churn of good peers.

                    // now compute the median peer score in the mesh
                    let mut peers_by_score: Vec<_> = peers.iter().collect();
                    peers_by_score
                        .sort_by(|p1, p2| score(p1).partial_cmp(&score(p2)).unwrap_or(Equal));

                    let middle = peers_by_score.len() / 2;
                    let median = if peers_by_score.len() % 2 == 0 {
                        (score(
                            *peers_by_score.get(middle - 1).expect(
                                "middle < vector length and middle > 0 since peers.len() > 0",
                            ),
                        ) + score(*peers_by_score.get(middle).expect("middle < vector length")))
                            * 0.5
                    } else {
                        score(*peers_by_score.get(middle).expect("middle < vector length"))
                    };

                    // if the median score is below the threshold, select a better peer (if any) and
                    // GRAFT
                    if median < thresholds.opportunistic_graft_threshold {
                        let peer_list = get_random_peers(
                            topic_peers,
                            &self.peer_protocols,
                            topic_hash,
                            self.config.opportunistic_graft_peers(),
                            |peer| {
                                !peers.contains(peer)
                                    && !explicit_peers.contains(peer)
                                    && !backoffs.is_backoff_with_slack(topic_hash, peer)
                                    && score(peer) > median
                            },
                        );
                        for peer in &peer_list {
                            let current_topic = to_graft.entry(*peer).or_insert_with(Vec::new);
                            current_topic.push(topic_hash.clone());
                        }
                        // update the mesh
                        debug!(
                            "Opportunistically graft in topic {} with peers {:?}",
                            topic_hash, peer_list
                        );
                        peers.extend(peer_list);
                    }
                }
            }
        }

        // remove expired fanout topics
        {
            let fanout = &mut self.fanout; // help the borrow checker
            let fanout_ttl = self.config.fanout_ttl();
            self.fanout_last_pub.retain(|topic_hash, last_pub_time| {
                if *last_pub_time + fanout_ttl < Instant::now() {
                    debug!(
                        "HEARTBEAT: Fanout topic removed due to timeout. Topic: {:?}",
                        topic_hash
                    );
                    fanout.remove(&topic_hash);
                    return false;
                }
                true
            });
        }

        // maintain fanout
        // check if our peers are still a part of the topic
        for (topic_hash, peers) in self.fanout.iter_mut() {
            let mut to_remove_peers = Vec::new();
            let publish_threshold = match &self.peer_score {
                Some((_, thresholds, _, _)) => thresholds.publish_threshold,
                _ => 0.0,
            };
            for peer in peers.iter() {
                // is the peer still subscribed to the topic?
                match self.peer_topics.get(peer) {
                    Some(topics) => {
                        if !topics.contains(&topic_hash) || score(peer) < publish_threshold {
                            debug!(
                                "HEARTBEAT: Peer removed from fanout for topic: {:?}",
                                topic_hash
                            );
                            to_remove_peers.push(*peer);
                        }
                    }
                    None => {
                        // remove if the peer has disconnected
                        to_remove_peers.push(*peer);
                    }
                }
            }
            for to_remove in to_remove_peers {
                peers.remove(&to_remove);
            }

            // not enough peers
            if peers.len() < self.config.mesh_n() {
                debug!(
                    "HEARTBEAT: Fanout low. Contains: {:?} needs: {:?}",
                    peers.len(),
                    self.config.mesh_n()
                );
                let needed_peers = self.config.mesh_n() - peers.len();
                let explicit_peers = &self.explicit_peers;
                let new_peers = get_random_peers(
                    &self.topic_peers,
                    &self.peer_protocols,
                    topic_hash,
                    needed_peers,
                    |peer| {
                        !peers.contains(peer)
                            && !explicit_peers.contains(peer)
                            && score(peer) < publish_threshold
                    },
                );
                peers.extend(new_peers);
            }
        }

        if self.peer_score.is_some() {
            trace!("Peer_scores: {:?}", {
                for peer in self.peer_topics.keys() {
                    score(peer);
                }
                scores
            });
            trace!("Mesh message deliveries: {:?}", {
                self.mesh
                    .iter()
                    .map(|(t, peers)| {
                        (
                            t.clone(),
                            peers
                                .iter()
                                .map(|p| {
                                    (
                                        *p,
                                        peer_score
                                            .as_ref()
                                            .expect("peer_score.is_some()")
                                            .0
                                            .mesh_message_deliveries(p, t)
                                            .unwrap_or(0.0),
                                    )
                                })
                                .collect::<HashMap<PeerId, f64>>(),
                        )
                    })
                    .collect::<HashMap<TopicHash, HashMap<PeerId, f64>>>()
            })
        }

        self.emit_gossip();

        // send graft/prunes
        if !to_graft.is_empty() | !to_prune.is_empty() {
            self.send_graft_prune(to_graft, to_prune, no_px);
        }

        // piggyback pooled control messages
        self.flush_control_pool();

        // shift the memcache
        self.mcache.shift();

        debug!("Completed Heartbeat");
    }

    /// Emits gossip - Send IHAVE messages to a random set of gossip peers. This is applied to mesh
    /// and fanout peers
    fn emit_gossip(&mut self) {
        let mut rng = thread_rng();
        for (topic_hash, peers) in self.mesh.iter().chain(self.fanout.iter()) {
            let mut message_ids = self.mcache.get_gossip_message_ids(&topic_hash);
            if message_ids.is_empty() {
                return;
            }

            // if we are emitting more than GossipSubMaxIHaveLength message_ids, truncate the list
            if message_ids.len() > self.config.max_ihave_length() {
                // we do the truncation (with shuffling) per peer below
                debug!(
                    "too many messages for gossip; will truncate IHAVE list ({} messages)",
                    message_ids.len()
                );
            } else {
                // shuffle to emit in random order
                message_ids.shuffle(&mut rng);
            }

            // dynamic number of peers to gossip based on `gossip_factor` with minimum `gossip_lazy`
            let n_map = |m| {
                max(
                    self.config.gossip_lazy(),
                    (self.config.gossip_factor() * m as f64) as usize,
                )
            };
            // get gossip_lazy random peers
            let to_msg_peers = get_random_peers_dynamic(
                &self.topic_peers,
                &self.peer_protocols,
                &topic_hash,
                n_map,
                |peer| {
                    !peers.contains(peer)
                        && !self.explicit_peers.contains(peer)
                        && !self.score_below_threshold(peer, |ts| ts.gossip_threshold).0
                },
            );

            debug!("Gossiping IHAVE to {} peers.", to_msg_peers.len());

            for peer in to_msg_peers {
                let mut peer_message_ids = message_ids.clone();

                if peer_message_ids.len() > self.config.max_ihave_length() {
                    // We do this per peer so that we emit a different set for each peer.
                    // we have enough redundancy in the system that this will significantly increase
                    // the message coverage when we do truncate.
                    peer_message_ids.partial_shuffle(&mut rng, self.config.max_ihave_length());
                    peer_message_ids.truncate(self.config.max_ihave_length());
                }

                // send an IHAVE message
                Self::control_pool_add(
                    &mut self.control_pool,
                    peer,
                    GossipsubControlAction::IHave {
                        topic_hash: topic_hash.clone(),
                        message_ids: peer_message_ids,
                    },
                );
            }
        }
    }

    /// Handles multiple GRAFT/PRUNE messages and coalesces them into chunked gossip control
    /// messages.
    fn send_graft_prune(
        &mut self,
        to_graft: HashMap<PeerId, Vec<TopicHash>>,
        mut to_prune: HashMap<PeerId, Vec<TopicHash>>,
        no_px: HashSet<PeerId>,
    ) {
        // handle the grafts and overlapping prunes per peer
        for (peer, topics) in to_graft.iter() {
            for topic in topics {
                //inform scoring of graft
                if let Some((peer_score, ..)) = &mut self.peer_score {
                    peer_score.graft(peer, topic.clone());
                }
            }
            let mut control_msgs: Vec<GossipsubControlAction> = topics
                .iter()
                .map(|topic_hash| GossipsubControlAction::Graft {
                    topic_hash: topic_hash.clone(),
                })
                .collect();

            // If there are prunes associated with the same peer add them.
            if let Some(topics) = to_prune.remove(peer) {
                let mut prunes = topics
                    .iter()
                    .map(|topic_hash| {
                        self.make_prune(
                            topic_hash,
                            peer,
                            self.config.do_px() && !no_px.contains(peer),
                        )
                    })
                    .collect::<Vec<_>>();
                control_msgs.append(&mut prunes);
            }

            // send the control messages
            if self
                .send_message(
                    *peer,
                    GossipsubRpc {
                        subscriptions: Vec::new(),
                        messages: Vec::new(),
                        control_msgs,
                    }
                    .into_protobuf(),
                )
                .is_err()
            {
                error!("Failed to send control messages. Message too large");
            }
        }

        // handle the remaining prunes
        for (peer, topics) in to_prune.iter() {
            let remaining_prunes = topics
                .iter()
                .map(|topic_hash| {
                    self.make_prune(
                        topic_hash,
                        peer,
                        self.config.do_px() && !no_px.contains(peer),
                    )
                })
                .collect();
            if self
                .send_message(
                    *peer,
                    GossipsubRpc {
                        subscriptions: Vec::new(),
                        messages: Vec::new(),
                        control_msgs: remaining_prunes,
                    }
                    .into_protobuf(),
                )
                .is_err()
            {
                error!("Failed to send prune messages. Message too large");
            }
        }
    }

    /// Helper function which forwards a message to mesh\[topic\] peers.
    ///
    /// Returns true if at least one peer was messaged.
    fn forward_msg(
        &mut self,
        msg_id: &MessageId,
        message: RawGossipsubMessage,
        propagation_source: Option<&PeerId>,
    ) -> Result<bool, PublishError> {
        // message is fully validated inform peer_score
        if let Some((peer_score, ..)) = &mut self.peer_score {
            if let Some(peer) = propagation_source {
                peer_score.deliver_message(peer, msg_id, &message.topic);
            }
        }

        debug!("Forwarding message: {:?}", msg_id);
        let mut recipient_peers = HashSet::new();

        // add mesh peers
        let topic = &message.topic;
        // mesh
        if let Some(mesh_peers) = self.mesh.get(&topic) {
            for peer_id in mesh_peers {
                if Some(peer_id) != propagation_source && Some(peer_id) != message.source.as_ref() {
                    recipient_peers.insert(*peer_id);
                }
            }
        }

        // Add explicit peers
        for p in &self.explicit_peers {
            if let Some(topics) = self.peer_topics.get(p) {
                if Some(p) != propagation_source
                    && Some(p) != message.source.as_ref()
                    && topics.contains(&message.topic)
                {
                    recipient_peers.insert(*p);
                }
            }
        }

        // forward the message to peers
        if !recipient_peers.is_empty() {
            let event = Arc::new(
                GossipsubRpc {
                    subscriptions: Vec::new(),
                    messages: vec![message.clone()],
                    control_msgs: Vec::new(),
                }
                .into_protobuf(),
            );

            for peer in recipient_peers.iter() {
                debug!("Sending message: {:?} to peer {:?}", msg_id, peer);
                self.send_message(*peer, event.clone())?;
            }
            debug!("Completed forwarding message");
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Constructs a [`RawGossipsubMessage`] performing message signing if required.
    pub(crate) fn build_raw_message(
        &self,
        topic: TopicHash,
        data: Vec<u8>,
    ) -> Result<RawGossipsubMessage, PublishError> {
        match &self.publish_config {
            PublishConfig::Signing {
                ref keypair,
                author,
                inline_key,
            } => {
                // Build and sign the message
                let sequence_number: u64 = rand::random();

                let signature = {
                    let message = rpc_proto::Message {
                        from: Some(author.clone().to_bytes()),
                        data: Some(data.clone()),
                        seqno: Some(sequence_number.to_be_bytes().to_vec()),
                        topic: topic.clone().into_string(),
                        signature: None,
                        key: None,
                    };

                    let mut buf = Vec::with_capacity(message.encoded_len());
                    message
                        .encode(&mut buf)
                        .expect("Buffer has sufficient capacity");

                    // the signature is over the bytes "libp2p-pubsub:<protobuf-message>"
                    let mut signature_bytes = SIGNING_PREFIX.to_vec();
                    signature_bytes.extend_from_slice(&buf);
                    Some(keypair.sign(&signature_bytes)?)
                };

                Ok(RawGossipsubMessage {
                    source: Some(*author),
                    data,
                    // To be interoperable with the go-implementation this is treated as a 64-bit
                    // big-endian uint.
                    sequence_number: Some(sequence_number),
                    topic,
                    signature,
                    key: inline_key.clone(),
                    validated: true, // all published messages are valid
                })
            }
            PublishConfig::Author(peer_id) => {
                Ok(RawGossipsubMessage {
                    source: Some(*peer_id),
                    data,
                    // To be interoperable with the go-implementation this is treated as a 64-bit
                    // big-endian uint.
                    sequence_number: Some(rand::random()),
                    topic,
                    signature: None,
                    key: None,
                    validated: true, // all published messages are valid
                })
            }
            PublishConfig::RandomAuthor => {
                Ok(RawGossipsubMessage {
                    source: Some(PeerId::random()),
                    data,
                    // To be interoperable with the go-implementation this is treated as a 64-bit
                    // big-endian uint.
                    sequence_number: Some(rand::random()),
                    topic,
                    signature: None,
                    key: None,
                    validated: true, // all published messages are valid
                })
            }
            PublishConfig::Anonymous => {
                Ok(RawGossipsubMessage {
                    source: None,
                    data,
                    // To be interoperable with the go-implementation this is treated as a 64-bit
                    // big-endian uint.
                    sequence_number: None,
                    topic,
                    signature: None,
                    key: None,
                    validated: true, // all published messages are valid
                })
            }
        }
    }

    // adds a control action to control_pool
    fn control_pool_add(
        control_pool: &mut HashMap<PeerId, Vec<GossipsubControlAction>>,
        peer: PeerId,
        control: GossipsubControlAction,
    ) {
        control_pool
            .entry(peer)
            .or_insert_with(Vec::new)
            .push(control);
    }

    /// Takes each control action mapping and turns it into a message
    fn flush_control_pool(&mut self) {
        for (peer, controls) in self.control_pool.drain().collect::<Vec<_>>() {
            if self
                .send_message(
                    peer,
                    GossipsubRpc {
                        subscriptions: Vec::new(),
                        messages: Vec::new(),
                        control_msgs: controls,
                    }
                    .into_protobuf(),
                )
                .is_err()
            {
                error!("Failed to flush control pool. Message too large");
            }
        }
    }

    /// Send a GossipsubRpc message to a peer. This will wrap the message in an arc if it
    /// is not already an arc.
    fn send_message(
        &mut self,
        peer_id: PeerId,
        message: impl Into<Arc<rpc_proto::Rpc>>,
    ) -> Result<(), PublishError> {
        // If the message is oversized, try and fragment it. If it cannot be fragmented, log an
        // error and drop the message (all individual messages should be small enough to fit in the
        // max_transmit_size)

        let messages = self.fragment_message(message.into())?;

        for message in messages {
            self.events
                .push_back(NetworkBehaviourAction::NotifyHandler {
                    peer_id,
                    event: message,
                    handler: NotifyHandler::Any,
                })
        }
        Ok(())
    }

    // If a message is too large to be sent as-is, this attempts to fragment it into smaller RPC
    // messages to be sent.
    fn fragment_message(
        &self,
        rpc: Arc<rpc_proto::Rpc>,
    ) -> Result<Vec<Arc<rpc_proto::Rpc>>, PublishError> {
        if rpc.encoded_len() < self.config.max_transmit_size() {
            return Ok(vec![rpc]);
        }

        let new_rpc = rpc_proto::Rpc {
            subscriptions: Vec::new(),
            publish: Vec::new(),
            control: None,
        };

        let mut rpc_list = vec![new_rpc.clone()];

        // Gets an RPC if the object size will fit, otherwise create a new RPC. The last element
        // will be the RPC to add an object.
        macro_rules! create_or_add_rpc {
            ($object_size: ident ) => {
                let list_index = rpc_list.len() - 1; // the list is never empty

                // create a new RPC if the new object plus 5% of its size (for length prefix
                // buffers) exceeds the max transmit size.
                if rpc_list[list_index].encoded_len() + (($object_size as f64) * 1.05) as usize
                    > self.config.max_transmit_size()
                    && rpc_list[list_index] != new_rpc
                {
                    // create a new rpc and use this as the current
                    rpc_list.push(new_rpc.clone());
                }
            };
        };

        macro_rules! add_item {
            ($object: ident, $type: ident ) => {
                let object_size = $object.encoded_len();

                if object_size + 2 > self.config.max_transmit_size() {
                    // This should not be possible. All received and published messages have already
                    // been vetted to fit within the size.
                    error!("Individual message too large to fragment");
                    return Err(PublishError::MessageTooLarge);
                }

                create_or_add_rpc!(object_size);
                rpc_list
                    .last_mut()
                    .expect("Must have at least one element")
                    .$type
                    .push($object.clone());
            };
        }

        // Add messages until the limit
        for message in &rpc.publish {
            add_item!(message, publish);
        }
        for subscription in &rpc.subscriptions {
            add_item!(subscription, subscriptions);
        }

        // handle the control messages. If all are within the max_transmit_size, send them without
        // fragmenting, otherwise, fragment the control messages
        let empty_control = rpc_proto::ControlMessage::default();
        if let Some(control) = rpc.control.as_ref() {
            if control.encoded_len() + 2 > self.config.max_transmit_size() {
                // fragment the RPC
                for ihave in &control.ihave {
                    let len = ihave.encoded_len();
                    create_or_add_rpc!(len);
                    rpc_list
                        .last_mut()
                        .expect("Always an element")
                        .control
                        .get_or_insert_with(|| empty_control.clone())
                        .ihave
                        .push(ihave.clone());
                }
                for iwant in &control.iwant {
                    let len = iwant.encoded_len();
                    create_or_add_rpc!(len);
                    rpc_list
                        .last_mut()
                        .expect("Always an element")
                        .control
                        .get_or_insert_with(|| empty_control.clone())
                        .iwant
                        .push(iwant.clone());
                }
                for graft in &control.graft {
                    let len = graft.encoded_len();
                    create_or_add_rpc!(len);
                    rpc_list
                        .last_mut()
                        .expect("Always an element")
                        .control
                        .get_or_insert_with(|| empty_control.clone())
                        .graft
                        .push(graft.clone());
                }
                for prune in &control.prune {
                    let len = prune.encoded_len();
                    create_or_add_rpc!(len);
                    rpc_list
                        .last_mut()
                        .expect("Always an element")
                        .control
                        .get_or_insert_with(|| empty_control.clone())
                        .prune
                        .push(prune.clone());
                }
            } else {
                let len = control.encoded_len();
                create_or_add_rpc!(len);
                rpc_list.last_mut().expect("Always an element").control = Some(control.clone());
            }
        }

        Ok(rpc_list.into_iter().map(Arc::new).collect())
    }
}

fn get_ip_addr(addr: &Multiaddr) -> Option<IpAddr> {
    addr.iter().find_map(|p| match p {
        Ip4(addr) => Some(IpAddr::V4(addr)),
        Ip6(addr) => Some(IpAddr::V6(addr)),
        _ => None,
    })
}

impl<C, F> NetworkBehaviour for Gossipsub<C, F>
where
    C: Send + 'static + DataTransform,
    F: Send + 'static + TopicSubscriptionFilter,
{
    type ProtocolsHandler = GossipsubHandler;
    type OutEvent = GossipsubEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        GossipsubHandler::new(
            self.config.protocol_id_prefix().clone(),
            self.config.max_transmit_size(),
            self.config.validation_mode().clone(),
            self.config.support_floodsub(),
        )
    }

    fn addresses_of_peer(&mut self, _: &PeerId) -> Vec<Multiaddr> {
        Vec::new()
    }

    fn inject_connected(&mut self, peer_id: &PeerId) {
        // Ignore connections from blacklisted peers.
        if self.blacklisted_peers.contains(peer_id) {
            debug!("Ignoring connection from blacklisted peer: {}", peer_id);
            return;
        }

        info!("New peer connected: {}", peer_id);
        // We need to send our subscriptions to the newly-connected node.
        let mut subscriptions = vec![];
        for topic_hash in self.mesh.keys() {
            subscriptions.push(GossipsubSubscription {
                topic_hash: topic_hash.clone(),
                action: GossipsubSubscriptionAction::Subscribe,
            });
        }

        if !subscriptions.is_empty() {
            // send our subscriptions to the peer
            if self
                .send_message(
                    *peer_id,
                    GossipsubRpc {
                        messages: Vec::new(),
                        subscriptions,
                        control_msgs: Vec::new(),
                    }
                    .into_protobuf(),
                )
                .is_err()
            {
                error!("Failed to send subscriptions, message too large");
            }
        }

        // Insert an empty set of the topics of this peer until known.
        self.peer_topics.insert(*peer_id, Default::default());

        // By default we assume a peer is only a floodsub peer.
        //
        // The protocol negotiation occurs once a message is sent/received. Once this happens we
        // update the type of peer that this is in order to determine which kind of routing should
        // occur.
        self.peer_protocols
            .entry(*peer_id)
            .or_insert(PeerKind::Floodsub);

        if let Some((peer_score, ..)) = &mut self.peer_score {
            peer_score.add_peer(*peer_id);
        }
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId) {
        // remove from mesh, topic_peers, peer_topic and the fanout
        debug!("Peer disconnected: {}", peer_id);
        {
            let topics = match self.peer_topics.get(peer_id) {
                Some(topics) => (topics),
                None => {
                    if !self.blacklisted_peers.contains(peer_id) {
                        debug!("Disconnected node, not in connected nodes");
                    }
                    return;
                }
            };

            // remove peer from all mappings
            for topic in topics {
                // check the mesh for the topic
                if let Some(mesh_peers) = self.mesh.get_mut(&topic) {
                    // check if the peer is in the mesh and remove it
                    mesh_peers.remove(peer_id);
                }

                // remove from topic_peers
                if let Some(peer_list) = self.topic_peers.get_mut(&topic) {
                    if !peer_list.remove(peer_id) {
                        // debugging purposes
                        warn!(
                            "Disconnected node: {} not in topic_peers peer list",
                            peer_id
                        );
                    }
                } else {
                    warn!(
                        "Disconnected node: {} with topic: {:?} not in topic_peers",
                        &peer_id, &topic
                    );
                }

                // remove from fanout
                self.fanout
                    .get_mut(&topic)
                    .map(|peers| peers.remove(peer_id));
            }

            //forget px and outbound status for this peer
            self.px_peers.remove(peer_id);
            self.outbound_peers.remove(peer_id);
        }

        // Remove peer from peer_topics and peer_protocols
        // NOTE: It is possible the peer has already been removed from all mappings if it does not
        // support the protocol.
        self.peer_topics.remove(peer_id);
        self.peer_protocols.remove(peer_id);

        if let Some((peer_score, ..)) = &mut self.peer_score {
            peer_score.remove_peer(peer_id);
        }
    }

    fn inject_connection_established(
        &mut self,
        peer_id: &PeerId,
        _: &ConnectionId,
        endpoint: &ConnectedPoint,
    ) {
        // Ignore connections from blacklisted peers.
        if self.blacklisted_peers.contains(peer_id) {
            return;
        }

        // Check if the peer is an outbound peer
        if let ConnectedPoint::Dialer { .. } = endpoint {
            // Diverging from the go implementation we only want to consider a peer as outbound peer
            // if its first connection is outbound. To check if this connection is the first we
            // check if the peer isn't connected yet. This only works because the
            // `inject_connection_established` event for the first connection gets called immediately
            // before `inject_connected` gets called.
            if !self.peer_topics.contains_key(peer_id) && !self.px_peers.contains(peer_id) {
                // The first connection is outbound and it is not a peer from peer exchange => mark
                // it as outbound peer
                self.outbound_peers.insert(*peer_id);
            }
        }

        // Add the IP to the peer scoring system
        if let Some((peer_score, ..)) = &mut self.peer_score {
            if let Some(ip) = get_ip_addr(endpoint.get_remote_address()) {
                peer_score.add_ip(&peer_id, ip);
            } else {
                trace!(
                    "Couldn't extract ip from endpoint of peer {} with endpoint {:?}",
                    peer_id,
                    endpoint
                )
            }
        }
    }

    fn inject_connection_closed(
        &mut self,
        peer: &PeerId,
        _: &ConnectionId,
        endpoint: &ConnectedPoint,
    ) {
        // Remove IP from peer scoring system
        if let Some((peer_score, ..)) = &mut self.peer_score {
            if let Some(ip) = get_ip_addr(endpoint.get_remote_address()) {
                peer_score.remove_ip(peer, &ip);
            } else {
                trace!(
                    "Couldn't extract ip from endpoint of peer {} with endpoint {:?}",
                    peer,
                    endpoint
                )
            }
        }
    }

    fn inject_address_change(
        &mut self,
        peer: &PeerId,
        _: &ConnectionId,
        endpoint_old: &ConnectedPoint,
        endpoint_new: &ConnectedPoint,
    ) {
        // Exchange IP in peer scoring system
        if let Some((peer_score, ..)) = &mut self.peer_score {
            if let Some(ip) = get_ip_addr(endpoint_old.get_remote_address()) {
                peer_score.remove_ip(peer, &ip);
            } else {
                trace!(
                    "Couldn't extract ip from endpoint of peer {} with endpoint {:?}",
                    peer,
                    endpoint_old
                )
            }
            if let Some(ip) = get_ip_addr(endpoint_new.get_remote_address()) {
                peer_score.add_ip(&peer, ip);
            } else {
                trace!(
                    "Couldn't extract ip from endpoint of peer {} with endpoint {:?}",
                    peer,
                    endpoint_new
                )
            }
        }
    }

    fn inject_event(
        &mut self,
        propagation_source: PeerId,
        _: ConnectionId,
        handler_event: HandlerEvent,
    ) {
        match handler_event {
            HandlerEvent::PeerKind(kind) => {
                // We have identified the protocol this peer is using
                if let PeerKind::NotSupported = kind {
                    debug!(
                        "Peer does not support gossipsub protocols. {}",
                        propagation_source
                    );
                    // We treat this peer as disconnected
                    self.inject_disconnected(&propagation_source);
                } else if let Some(old_kind) = self.peer_protocols.get_mut(&propagation_source) {
                    // Only change the value if the old value is Floodsub (the default set in
                    // inject_connected). All other PeerKind changes are ignored.
                    debug!(
                        "New peer type found: {} for peer: {}",
                        kind, propagation_source
                    );
                    if let PeerKind::Floodsub = *old_kind {
                        *old_kind = kind;
                    }
                }
            }
            HandlerEvent::Message {
                rpc,
                invalid_messages,
            } => {
                // Handle the gossipsub RPC

                // Handle subscriptions
                // Update connected peers topics
                if !rpc.subscriptions.is_empty() {
                    self.handle_received_subscriptions(&rpc.subscriptions, &propagation_source);
                }

                // Check if peer is graylisted in which case we ignore the event
                if let (true, _) =
                    self.score_below_threshold(&propagation_source, |pst| pst.graylist_threshold)
                {
                    debug!("RPC Dropped from greylisted peer {}", propagation_source);
                    return;
                }

                // Handle any invalid messages from this peer
                if self.peer_score.is_some() {
                    for (raw_message, validation_error) in invalid_messages {
                        self.handle_invalid_message(
                            &propagation_source,
                            raw_message,
                            validation_error,
                        )
                    }
                } else {
                    // log the invalid messages
                    for (message, validation_error) in invalid_messages {
                        warn!(
                            "Invalid message. Reason: {:?} propagation_peer {} source {:?}",
                            validation_error,
                            propagation_source.to_string(),
                            message.source
                        );
                    }
                }

                // Handle messages
                for (count, raw_message) in rpc.messages.into_iter().enumerate() {
                    // Only process the amount of messages the configuration allows.
                    if self.config.max_messages_per_rpc().is_some()
                        && Some(count) >= self.config.max_messages_per_rpc()
                    {
                        warn!("Received more messages than permitted. Ignoring further messages. Processed: {}", count);
                        break;
                    }
                    self.handle_received_message(raw_message, &propagation_source);
                }

                // Handle control messages
                // group some control messages, this minimises SendEvents (code is simplified to handle each event at a time however)
                let mut ihave_msgs = vec![];
                let mut graft_msgs = vec![];
                let mut prune_msgs = vec![];
                for control_msg in rpc.control_msgs {
                    match control_msg {
                        GossipsubControlAction::IHave {
                            topic_hash,
                            message_ids,
                        } => {
                            ihave_msgs.push((topic_hash, message_ids));
                        }
                        GossipsubControlAction::IWant { message_ids } => {
                            self.handle_iwant(&propagation_source, message_ids)
                        }
                        GossipsubControlAction::Graft { topic_hash } => graft_msgs.push(topic_hash),
                        GossipsubControlAction::Prune {
                            topic_hash,
                            peers,
                            backoff,
                        } => prune_msgs.push((topic_hash, peers, backoff)),
                    }
                }
                if !ihave_msgs.is_empty() {
                    self.handle_ihave(&propagation_source, ihave_msgs);
                }
                if !graft_msgs.is_empty() {
                    self.handle_graft(&propagation_source, graft_msgs);
                }
                if !prune_msgs.is_empty() {
                    self.handle_prune(&propagation_source, prune_msgs);
                }
            }
        }
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
        _: &mut impl PollParameters,
    ) -> Poll<
        NetworkBehaviourAction<
            <Self::ProtocolsHandler as ProtocolsHandler>::InEvent,
            Self::OutEvent,
        >,
    > {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(match event {
                NetworkBehaviourAction::NotifyHandler {
                    peer_id,
                    handler,
                    event: send_event,
                } => {
                    // clone send event reference if others references are present
                    let event = Arc::try_unwrap(send_event).unwrap_or_else(|e| (*e).clone());
                    NetworkBehaviourAction::NotifyHandler {
                        peer_id,
                        event,
                        handler,
                    }
                }
                NetworkBehaviourAction::GenerateEvent(e) => {
                    NetworkBehaviourAction::GenerateEvent(e)
                }
                NetworkBehaviourAction::DialAddress { address } => {
                    NetworkBehaviourAction::DialAddress { address }
                }
                NetworkBehaviourAction::DialPeer { peer_id, condition } => {
                    NetworkBehaviourAction::DialPeer { peer_id, condition }
                }
                NetworkBehaviourAction::ReportObservedAddr { address, score } => {
                    NetworkBehaviourAction::ReportObservedAddr { address, score }
                }
            });
        }

        // update scores
        if let Some((peer_score, _, interval, _)) = &mut self.peer_score {
            while let Poll::Ready(Some(())) = interval.poll_next_unpin(cx) {
                peer_score.refresh_scores();
            }
        }

        while let Poll::Ready(Some(())) = self.heartbeat.poll_next_unpin(cx) {
            self.heartbeat();
        }

        Poll::Pending
    }
}

/// Helper function to get a subset of random gossipsub peers for a `topic_hash`
/// filtered by the function `f`. The number of peers to get equals the output of `n_map`
/// that gets as input the number of filtered peers.
fn get_random_peers_dynamic(
    topic_peers: &HashMap<TopicHash, BTreeSet<PeerId>>,
    peer_protocols: &HashMap<PeerId, PeerKind>,
    topic_hash: &TopicHash,
    // maps the number of total peers to the number of selected peers
    n_map: impl Fn(usize) -> usize,
    mut f: impl FnMut(&PeerId) -> bool,
) -> BTreeSet<PeerId> {
    let mut gossip_peers = match topic_peers.get(topic_hash) {
        // if they exist, filter the peers by `f`
        Some(peer_list) => peer_list
            .iter()
            .cloned()
            .filter(|p| {
                f(p) && match peer_protocols.get(p) {
                    Some(PeerKind::Gossipsub) => true,
                    Some(PeerKind::Gossipsubv1_1) => true,
                    _ => false,
                }
            })
            .collect(),
        None => Vec::new(),
    };

    // if we have less than needed, return them
    let n = n_map(gossip_peers.len());
    if gossip_peers.len() <= n {
        debug!("RANDOM PEERS: Got {:?} peers", gossip_peers.len());
        return gossip_peers.into_iter().collect();
    }

    // we have more peers than needed, shuffle them and return n of them
    let mut rng = thread_rng();
    gossip_peers.partial_shuffle(&mut rng, n);

    debug!("RANDOM PEERS: Got {:?} peers", n);

    gossip_peers.into_iter().take(n).collect()
}

/// Helper function to get a set of `n` random gossipsub peers for a `topic_hash`
/// filtered by the function `f`.
fn get_random_peers(
    topic_peers: &HashMap<TopicHash, BTreeSet<PeerId>>,
    peer_protocols: &HashMap<PeerId, PeerKind>,
    topic_hash: &TopicHash,
    n: usize,
    f: impl FnMut(&PeerId) -> bool,
) -> BTreeSet<PeerId> {
    get_random_peers_dynamic(topic_peers, peer_protocols, topic_hash, |_| n, f)
}

/// Validates the combination of signing, privacy and message validation to ensure the
/// configuration will not reject published messages.
fn validate_config(
    authenticity: &MessageAuthenticity,
    validation_mode: &ValidationMode,
) -> Result<(), &'static str> {
    match validation_mode {
        ValidationMode::Anonymous => {
            if authenticity.is_signing() {
                return Err("Cannot enable message signing with an Anonymous validation mode. Consider changing either the ValidationMode or MessageAuthenticity");
            }

            if !authenticity.is_anonymous() {
                return Err("Published messages contain an author but incoming messages with an author will be rejected. Consider adjusting the validation or privacy settings in the config");
            }
        }
        ValidationMode::Strict => {
            if !authenticity.is_signing() {
                return Err(
                    "Messages will be
                published unsigned and incoming unsigned messages will be rejected. Consider adjusting
                the validation or privacy settings in the config"
                );
            }
        }
        _ => {}
    }
    Ok(())
}

impl<C: DataTransform, F: TopicSubscriptionFilter> fmt::Debug for Gossipsub<C, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Gossipsub")
            .field("config", &self.config)
            .field("events", &self.events)
            .field("control_pool", &self.control_pool)
            .field("publish_config", &self.publish_config)
            .field("topic_peers", &self.topic_peers)
            .field("peer_topics", &self.peer_topics)
            .field("mesh", &self.mesh)
            .field("fanout", &self.fanout)
            .field("fanout_last_pub", &self.fanout_last_pub)
            .field("mcache", &self.mcache)
            .field("heartbeat", &self.heartbeat)
            .finish()
    }
}

impl fmt::Debug for PublishConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PublishConfig::Signing { author, .. } => {
                f.write_fmt(format_args!("PublishConfig::Signing({})", author))
            }
            PublishConfig::Author(author) => {
                f.write_fmt(format_args!("PublishConfig::Author({})", author))
            }
            PublishConfig::RandomAuthor => f.write_fmt(format_args!("PublishConfig::RandomAuthor")),
            PublishConfig::Anonymous => f.write_fmt(format_args!("PublishConfig::Anonymous")),
        }
    }
}

#[cfg(test)]
mod local_test {
    use super::*;
    use crate::IdentTopic;
    use asynchronous_codec::Encoder;
    use quickcheck::*;
    use rand::Rng;

    fn empty_rpc() -> GossipsubRpc {
        GossipsubRpc {
            subscriptions: Vec::new(),
            messages: Vec::new(),
            control_msgs: Vec::new(),
        }
    }

    fn test_message() -> RawGossipsubMessage {
        RawGossipsubMessage {
            source: Some(PeerId::random()),
            data: vec![0; 100],
            sequence_number: None,
            topic: TopicHash::from_raw("test_topic"),
            signature: None,
            key: None,
            validated: false,
        }
    }

    fn test_subscription() -> GossipsubSubscription {
        GossipsubSubscription {
            action: GossipsubSubscriptionAction::Subscribe,
            topic_hash: IdentTopic::new("TestTopic").hash(),
        }
    }

    fn test_control() -> GossipsubControlAction {
        GossipsubControlAction::IHave {
            topic_hash: IdentTopic::new("TestTopic").hash(),
            message_ids: vec![MessageId(vec![12u8]); 5],
        }
    }

    impl Arbitrary for GossipsubRpc {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let mut rpc = empty_rpc();

            for _ in 0..g.gen_range(0, 10) {
                rpc.subscriptions.push(test_subscription());
            }
            for _ in 0..g.gen_range(0, 10) {
                rpc.messages.push(test_message());
            }
            for _ in 0..g.gen_range(0, 10) {
                rpc.control_msgs.push(test_control());
            }
            rpc
        }
    }

    #[test]
    /// Tests RPC message fragmentation
    fn test_message_fragmentation_deterministic() {
        let max_transmit_size = 500;
        let config = crate::GossipsubConfigBuilder::default()
            .max_transmit_size(max_transmit_size)
            .validation_mode(ValidationMode::Permissive)
            .build()
            .unwrap();
        let gs: Gossipsub = Gossipsub::new(MessageAuthenticity::RandomAuthor, config).unwrap();

        // Message under the limit should be fine.
        let mut rpc = empty_rpc();
        rpc.messages.push(test_message());

        let mut rpc_proto = rpc.clone().into_protobuf();
        let fragmented_messages = gs.fragment_message(Arc::new(rpc_proto.clone())).unwrap();
        assert_eq!(
            fragmented_messages,
            vec![Arc::new(rpc_proto.clone())],
            "Messages under the limit shouldn't be fragmented"
        );

        // Messages over the limit should be split

        while rpc_proto.encoded_len() < max_transmit_size {
            rpc.messages.push(test_message());
            rpc_proto = rpc.clone().into_protobuf();
        }

        let fragmented_messages = gs
            .fragment_message(Arc::new(rpc_proto))
            .expect("Should be able to fragment the messages");

        assert!(
            fragmented_messages.len() > 1,
            "the message should be fragmented"
        );

        // all fragmented messages should be under the limit
        for message in fragmented_messages {
            assert!(
                message.encoded_len() < max_transmit_size,
                "all messages should be less than the transmission size"
            );
        }
    }

    #[test]
    fn test_message_fragmentation() {
        fn prop(rpc: GossipsubRpc) {
            let max_transmit_size = 500;
            let config = crate::GossipsubConfigBuilder::default()
                .max_transmit_size(max_transmit_size)
                .validation_mode(ValidationMode::Permissive)
                .build()
                .unwrap();
            let gs: Gossipsub = Gossipsub::new(MessageAuthenticity::RandomAuthor, config).unwrap();

            let mut length_codec = unsigned_varint::codec::UviBytes::default();
            length_codec.set_max_len(max_transmit_size);
            let mut codec =
                crate::protocol::GossipsubCodec::new(length_codec, ValidationMode::Permissive);

            let rpc_proto = rpc.into_protobuf();
            let fragmented_messages = gs
                .fragment_message(Arc::new(rpc_proto.clone()))
                .expect("Messages must be valid");

            if rpc_proto.encoded_len() < max_transmit_size {
                assert_eq!(
                    fragmented_messages.len(),
                    1,
                    "the message should not be fragmented"
                );
            } else {
                assert!(
                    fragmented_messages.len() > 1,
                    "the message should be fragmented"
                );
            }

            // all fragmented messages should be under the limit
            for message in fragmented_messages {
                assert!(
                    message.encoded_len() < max_transmit_size,
                    "all messages should be less than the transmission size: list size {} max size{}", message.encoded_len(), max_transmit_size
                );

                // ensure they can all be encoded
                let mut buf = bytes::BytesMut::with_capacity(message.encoded_len());
                codec
                    .encode(Arc::try_unwrap(message).unwrap(), &mut buf)
                    .unwrap()
            }
        }
        QuickCheck::new()
            .max_tests(100)
            .quickcheck(prop as fn(_) -> _)
    }
}
