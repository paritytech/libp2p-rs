use constants::{TARGET_MESH_DEGREE};
use errors::{GError, Result as GResult, GraftErrors, PruneErrors};
use handler::GossipsubHandler;
use mcache::MCache;
use mesh::Mesh;
use message::{ControlGraft, ControlIHave, ControlIWant, ControlMessage,
    GMessage,
    GossipsubRpc, GossipsubSubscription, GossipsubSubscriptionAction,
    GOutEvents, MsgHash, MsgMap};
use peers::Peers;
use {Topic, TopicHash};
use rpc_proto;

use libp2p_floodsub::Floodsub;
use libp2p_core::{
    PeerId,
    protocols_handler::ProtocolsHandler,
    swarm::{ConnectedPoint, NetworkBehaviour, NetworkBehaviourAction,
        PollParameters},
};
use cuckoofilter::CuckooFilter;
use futures::prelude::*;
use protobuf::Message;
use smallvec::SmallVec;
use std::{
    collections::{
        hash_map::{DefaultHasher, HashMap},
        VecDeque
    },
    iter,
    marker::PhantomData
};
use tokio_io::{AsyncRead, AsyncWrite};

/// Contains the state needed to maintain the Gossipsub protocol.
///
/// We need to duplicate the same fields as `Floodsub` in order to
/// differentiate the state of the two protocols.
// Doesn't derive `Debug` because `CuckooFilter` doesn't.
pub struct Gossipsub<'a, TSubstream> {
    /// Events that need to be yielded to the outside when polling.
    /// `TInEvent = GossipsubRpc`, `TOutEvent = GOutEvents` (the latter is
    /// either a `GMessage` or a `ControlMessage`).
    events: VecDeque<NetworkBehaviourAction<GossipsubRpc, GOutEvents>>,

    /// Peer id of the local node. Used for the source of the messages that we
    /// publish.
    local_peer_id: PeerId,

    /// List of peers the network is connected to, and the topics that they're
    /// subscribed to.
    // TODO: filter out peers that don't support gossipsub, so that we avoid
    //       hammering them with opened substreams
    // TODO: Handle floodsub peers for methods.
    // Contains gossipsub and floodsub peers, but this may be unnecessary
    // as the floodsub peers are in the below floodsub field.
    connected_peers: Peers,

    // List of topics we're subscribed to. Necessary to filter out messages
    // that we receive erroneously.
    subscribed_topics: SmallVec<[Topic; 16]>,

    // We keep track of the messages we received (in the format `hash(source
    // ID, seq_no)`) so that we don't dispatch the same message twice if we
    // receive it twice on the network.
    received: CuckooFilter<DefaultHasher>,

    /// See `Mesh` for details.
    mesh: Mesh,

    /// The `Mesh` peers to which we are publishing to without topic
    /// membership, as a map of topics to lists of peers.
    fanout: Mesh,

    mcache: MCache,

    last_pub: HashMap<TopicHash, i64>,

    /// Pending gossips.
    gossip: HashMap<PeerId, Vec<&'a ControlIHave>>,

    /// Pending control messages.
    control: HashMap<PeerId, Vec<&'a ControlMessage>>,

    /// Marker to pin the generics.
    marker: PhantomData<TSubstream>,

    /// For backwards-compatibility.
    // May not work, test.
    floodsub: Option<Floodsub<TSubstream>>,
}

impl<'a, TSubstream> Gossipsub<'a, TSubstream> {
    /// Creates a `Gossipsub`.
    pub fn new(local_peer_id: PeerId) -> Self {
        Gossipsub {
            events: VecDeque::new(),
            local_peer_id,
            connected_peers: Peers::new(),
            subscribed_topics: SmallVec::new(),
            received: CuckooFilter::new(),
            mesh: Mesh::new(),
            fanout: Mesh::new(),
            mcache: MCache::new(),
            last_pub: HashMap::new(),
            gossip: HashMap::new(),
            control: HashMap::new(),
            marker: PhantomData,
            floodsub: None,
        }
    }

    /// Convenience function that creates a `Gossipsub` with/using a
    /// previously existing `Floodsub`.
    pub fn new_w_existing_floodsub(local_peer_id: PeerId,
        fs: Floodsub<TSubstream>) -> Self {
        let mut gs = Gossipsub::new(local_peer_id);
        gs.floodsub = Some(fs);
        gs
    }

    // ---------------------------------------------------------------------
    // The following section is re-implemented from
    // Floodsub. This is needed to differentiate state.
    // TODO: write code to reduce re-implementation like this, where most code
    // is unmodified, except for types being renamed from `Floodsub*` to
    // `Gossipsub*`.

    /// Subscribes to a topic.
    ///
    /// Returns true if the subscription worked. Returns false if we were
    /// already subscribed.
    pub fn subscribe(&mut self, topic: Topic) -> bool {
        if self.subscribed_topics.iter().any(|t| t.hash() == topic.hash()) {
            return false;
        }

        for peer in self.connected_peers.gossipsub.keys() {
            self.events.push_back(NetworkBehaviourAction::SendEvent {
                peer_id: peer.clone(),
                event: GossipsubRpc {
                    messages: Vec::new(),
                    subscriptions: vec![GossipsubSubscription {
                        topic: topic.hash().clone(),
                        action: GossipsubSubscriptionAction::Subscribe,
                    }],
                    control: ::std::default::Default::default(),
                },
            });
        }

        self.subscribed_topics.push(topic);
        true
    }

    /// Unsubscribes from a topic.
    ///
    /// Note that this only requires a `TopicHash` and not a full `Topic`.
    ///
    /// Returns true if we were subscribed to this topic.
    pub fn unsubscribe(&mut self, topic: impl AsRef<TopicHash>) -> bool {
        let topic = topic.as_ref();
        let pos = match self.subscribed_topics.iter()
            .position(|t| t.hash() == topic) {
            Some(pos) => pos,
            None => return false
        };

        self.subscribed_topics.remove(pos);

        for peer in self.connected_peers.gossipsub.keys() {
            self.events.push_back(NetworkBehaviourAction::SendEvent {
                peer_id: peer.clone(),
                event: GossipsubRpc {
                    messages: Vec::new(),
                    subscriptions: vec![GossipsubSubscription {
                        topic: topic.clone(),
                        action: GossipsubSubscriptionAction::Unsubscribe,
                    }],
                    control: ::std::default::Default::default(),
                },
            });
        }

        true
    }

    /// Publishes a message to the network, optionally choosing to set
    /// a message ID.
    ///
    /// > **Note**: Doesn't do anything if we're not subscribed to the topic.
    pub fn publish_topic(&mut self, topic_hash: impl Into<TopicHash>,
        data: impl Into<Vec<u8>>,
        control: Option<ControlMessage>,
        msg_id: bool) {
        self.publish_many(iter::once(topic_hash), data, control, msg_id)
    }

    /// Publishes a message with multiple topics to the network, without any
    /// authentication or encryption.
    ///
    /// > **Note**: Doesn't do anything if we're not subscribed to any of the
    /// topics.
    //
    // Optionally add a message ID.
    pub fn publish_topics(&mut self,
        topic_hashes: impl IntoIterator<Item = impl Into<TopicHash>>,
        data: impl Into<Vec<u8>>,
        control: Option<ControlMessage>,
        _message_id: bool) {

        let message = GMessage {
            from: self.local_peer_id.clone(),
            data: data.into(),
            // If the sequence numbers are predictable, then an attacker could
            // flood the network with packets with the predetermined sequence
            // numbers and absorb our legitimate messages. We therefore use
            // a random number.
            seq_no: rand::random::<[u8; 20]>().to_vec(),
            topics: topic_hashes.into_iter().map(|t| t.into().clone())
                .collect(),
            // time_sent: Utc::now(),
            // hash: ::std::default::Default::default(),
            // id: ::std::default::Default::default(),
        };

        // if message_id {
        //     let m_id = MsgId::new(message);
        //     message.id = Some(m_id);
        // }

        self.mcache.put(message.clone());

        let proto_msg = rpc_proto::Message::from(&message);
        // Check that the message size is less than or equal to 1 MiB.
        // TODO: test
        assert!(proto_msg.compute_size() <= 1048576);

        // Don't publish the message if we're not subscribed ourselves to any
        // of the topics.
        if !self.subscribed_topics.iter().any(|t| message.topics.values()
            .any(|u| t == u)) {
            return;
        }

        self.received.add(&message);

        // TODO: Send to peers that are in our message link and that we know
        // are subscribed to the topic.
        for (peer_id, sub_topic) in self.connected_peers.gossipsub.iter() {
            if !sub_topic.iter().any(|t| message.topics.values()
                .any(|u| Topic::from(t) == *u)){
                continue;
            }

            // message.set_timestamp();

            self.events.push_back(NetworkBehaviourAction::SendEvent {
                peer_id: peer_id.clone(),
                event: GossipsubRpc {
                    subscriptions: Vec::new(),
                    messages: vec![message.clone()],
                    control: control.clone()
                }
            });
        }
    }
    // End of re-implementation from `Floodsub` methods.
    // ---------------------------------------------------------------------

    /// Tries to grafts a remote peer to a topic.
    ///
    /// This notifies the peer that it has been added to the local mesh view.
    /// Returns true if the graft succeeded. Returns false if we were
    /// already grafted.
    pub fn graft_peer_to_topic(
        &mut self,
        r_peer: impl AsRef<PeerId>,
        t_hash: impl AsRef<TopicHash>)
        -> GResult<Option<GraftErrors>> {
        self.graft_peers_to_topics(iter::once(r_peer), iter::once(t_hash))
    }

    /// Tries to grafts a remote peer to multiple topics.
    ///
    /// If the local peer is not connected to the remote peer, then an error
    /// is returned. It returns topics that the remote peer is not subscribed
    /// to (the first
    /// returned vector) or that aren't in the mesh (the second returned
    /// vector). For the latter case, the peer must first `join()` the topic.
    pub fn graft_peer_to_topics(&mut self, r_peer: impl AsRef<PeerId>,
        t_hashes: impl IntoIterator<Item = impl AsRef<TopicHash>>)
        -> GResult<Option<GraftErrors>> {
        self.graft_peers_to_topics(iter::once(r_peer), t_hashes)
    }

    /// Tries to graft peers to a topic.
    pub fn graft_peers_to_topic(&mut self,
        r_peers: impl IntoIterator<Item = impl AsRef<PeerId>>,
        t_hash: impl AsRef<TopicHash>
    ) -> GResult<Option<GraftErrors>> {
        self.graft_peers_to_topics(r_peers, iter::once(t_hash))
    }

    /// Tries to graft peers to many topics.
    ///
    /// ## Errors
    /// Errors, if any, are returned in an `Ok(Some(GraftErrors))`.
    pub fn graft_peers_to_topics(&mut self,
        // r_peers: Vec<&PeerId>,
        r_peers: impl IntoIterator<Item = impl AsRef<PeerId>>,
        // t_hashes: Vec<TopicHash>
        t_hashes: impl IntoIterator<Item = impl AsRef<TopicHash>>
    ) -> GResult<Option<GraftErrors>> {
        let r_peers_v: Vec<PeerId> = r_peers.into_iter()
            .map(|p| p.as_ref().clone()).collect();
        let mut t_hashes_v: Vec<TopicHash> = t_hashes.into_iter()
            .map(|th| th.as_ref().clone()).collect();
        let mut topics_not_in_mesh = Vec::new();
        let mut graft_errs = GraftErrors::new();
        let mut ts_already_grafted = Vec::new();
        let mut rem_peers_not_connected = Vec::new();
        let mut topics_not_subscribed_to = HashMap::new();
        let l_peer = &self.local_peer_id;
        let m = &mut self.mesh;

        for (i, t_hash) in t_hashes_v.clone().iter().enumerate() {
            match m.remove(t_hash) {
                Ok(mut ps) => {
                    // Do nothing, proceed to peer loop below.
                },
                Err(GError::TopicNotInMesh{t_hash: _t_hash, err}) => {
                    topics_not_in_mesh.push(t_hash.clone());
                    t_hashes_v.remove(i);
                },
                // Shouldn't happen, just for the compiler.
                Err(err) => {return Err(err);},
            }
        }
        let mut lp_as_rp_count = 0;
        for r_peer in r_peers_v {
            // let r_peer_str = r_peer.to_base58();
            if r_peer == self.local_peer_id {
                lp_as_rp_count += 1;
                graft_errs.lp_as_rp = Some(lp_as_rp_count);
                graft_errs.has_errors = true;
            }
            if let Some(topic_hashes) = self.connected_peers.gossipsub
                .get(&r_peer) {
                for (i, t_hash) in t_hashes_v.clone().iter().enumerate() {
                    // Check the remote peer is subscribed to the requested
                    // topic, if not add to return error map.
                    if !topic_hashes.iter().any(|th| th == t_hash) {
                        topics_not_subscribed_to.insert((&r_peer).clone(),      t_hash.clone());
                        t_hashes_v.remove(i);
                    } else {
                        // Graft

                        // Add remote to the topic in the local peer's mesh.
                        m.add_peer(t_hash.clone(), (&r_peer).clone());

                        // Notify remote peer
                        let ctrl = ControlMessage::new();
                        let graft = ControlGraft::new_with_thash(
                            t_hash.clone());
                        ctrl.graft.push(graft);
                        let grpc = GossipsubRpc::new();
                        grpc.control = ctrl;
                        self.inject_node_event(self.local_peer_id, grpc);
                    }
                }
            } else {
                rem_peers_not_connected.push(r_peer)
            }
        }
        if !topics_not_subscribed_to.is_empty() {
            graft_errs.topics_not_subscribed
                = Some(topics_not_subscribed_to);
            graft_errs.has_errors = true;
        }
        if !topics_not_in_mesh.is_empty() {
            graft_errs.topics_not_in_mesh = Some(topics_not_in_mesh);
            graft_errs.has_errors = true;
        }
        if !rem_peers_not_connected.is_empty() {
            graft_errs.r_peers_not_connected = Some(rem_peers_not_connected);
            graft_errs.has_errors = true;
        }
        if !ts_already_grafted.is_empty() {
            graft_errs.topics_already_grafted = Some(ts_already_grafted);
            graft_errs.has_errors = true;
        }
        if !graft_errs.is_empty() {
            return Ok(Some(graft_errs));
        } else {
            return Ok(None);
        }
    }

    /// Tries to prune a remote peer from a topic.
    ///
    /// Convenience function using `prune_peers_from_topics()`.
    pub fn prune_peer_from_topic(&mut self,
        r_peer: impl AsRef<PeerId>,
        t_hash: &mut (impl AsRef<TopicHash>))
    -> GResult<Option<PruneErrors>> {
        self.prune_peers_from_topics(iter::once(r_peer),
            &mut iter::once(t_hash))
    }

    /// Tries to prune a peer from multiple topics.
    ///
    /// Note that this only works if the peer is grafted to such topics.
    pub fn prune_peer_from_topics(&mut self, r_peer: impl AsRef<PeerId>,
        t_hashes: &mut (impl IntoIterator<
            Item = impl AsRef<TopicHash>>))
    -> GResult<Option<PruneErrors>> {
        self.prune_peers_from_topics(iter::once(r_peer), t_hashes)
    }

    /// Tries to prunes peers from a single topic.
    ///
    /// Convenience function using `prune_peers_from_topics()`.
    pub fn prune_peers_from_topic(
        &mut self,
        r_peers: impl IntoIterator<Item = impl AsRef<PeerId>>,
        t_hash: &mut (impl AsRef<TopicHash>)
    ) -> GResult<Option<PruneErrors>> {
        self.prune_peers_from_topics(r_peers, &mut iter::once(t_hash))
    }

    /// Tries to prune peers from many topics.
    ///
    /// ## Errors
    /// The following errors, if any, are returned in an Ok() in
    /// `PruneErrors`:
    /// - If a topic is not in the local peer's mesh, it is returned.
    /// - If a peer is not grafted to a topic, it is returned in a hashmap
    /// with the topic as a key and a vector of the non-grafted peer(s)
    /// as a value.
    /// - If a local peer is passed as an argument, an error is returned with
    /// a count of how many times it is passed.
    pub fn prune_peers_from_topics(
        &mut self,
        r_peers: impl IntoIterator<Item = impl AsRef<PeerId>>,
        t_hashes: &mut (impl IntoIterator<
            Item = impl AsRef<TopicHash>>)
    ) -> GResult<Option<PruneErrors>> {
        let r_peers_v: Vec<PeerId> = r_peers.into_iter()
            .map(|p| p.as_ref().clone()).collect();
        let mut t_hashes_v: Vec<TopicHash> = t_hashes.into_iter()
            .map(|th| th.as_ref().clone()).collect();
        let mut topics_not_in_mesh = Vec::new();
        let mut prune_errs = PruneErrors::new();
        let mut ps_ts_not_grafted = HashMap::new();
        let l_peer = &self.local_peer_id;
        // let l_peer_str = l_peer.to_base58();
        let m = &mut self.mesh;

        // Like graft, first check that the topic is in the mesh and handle.
        for (i, t_hash) in t_hashes_v.iter().enumerate() {
            match m.remove(t_hash) {
                Ok(mut ps) => {
                    // All good, proceed with pruning.
                },
                Err(GError::TopicNotInMesh{t_hash: _t_hash, err}) => {
                    // The topic needs to be in the local peer's mesh view
                    // in order to be able to prune peers from this topic.
                    topics_not_in_mesh.push(*t_hash);
                    t_hashes_v.remove(i);
                },
                // Shouldn't happen, just for the compiler.
                Err(err) => {return Err(err);},
            }
        }
        let mut lp_as_rp_count = 0;
        for r_peer in r_peers_v {
            if r_peer == self.local_peer_id {
                lp_as_rp_count += 1;
                prune_errs.lp_as_rp = Some(lp_as_rp_count);
                prune_errs.has_errors = true;
            }
            for t_hash in t_hashes_v {
                let thr = &t_hash;
                match m.remove_peer_from_topic(thr, r_peer.clone()) {
                    Ok(()) => {},
                    Err(GError::NotGraftedToTopic{..}) => {
                        ps_ts_not_grafted.insert(thr.clone(), r_peer.clone());
                    },
                    Err(GError::TopicNotInMesh{..}) => {
                        // Shouldn't happen, we already handled this above.
                    },
                    Err(err) => return Err(err), // Shouldn't happen.
                }
            }
        }
        if !topics_not_in_mesh.is_empty() {
            prune_errs.topics_not_in_mesh
                = Some(topics_not_in_mesh);
            prune_errs.has_errors = true;
        }
        if !ps_ts_not_grafted.is_empty() {
            prune_errs.ps_ts_not_grafted = Some(ps_ts_not_grafted);
            prune_errs.has_errors = true;
        }
        if prune_errs.has_errors() {
            return Ok(Some(prune_errs));
        } else {
            return Ok(None);
        }
    }

    /// Prunes all peers from a topic.
    ///
    /// Similar in implementation to `prune_peers_from_topics`. Used in
    /// `leave_topics`.
    pub fn prune_all_peers_from_topics(&mut self, topic_hashes: impl
        IntoIterator<Item = impl AsRef<TopicHash>>)
    -> GResult<Option<PruneErrors>> {
        let mut t_hashes_v: Vec<TopicHash> = topic_hashes.into_iter()
            .map(|th| th.as_ref().clone()).collect();
        let m = &mut self.mesh;
        let mut topics_not_in_mesh = Vec::new();
        let mut prune_errs = PruneErrors::new();
        // The same as `prune_peers_from_topics()` , first check that the
        // topic is in the mesh and handle.
        // You could move this duplication to a separate function, but AIUI
        // you'd have to pass the above parameters to it, so it wouldn't save
        // much.
        for (i, t_hash) in t_hashes_v.iter().enumerate() {
            match m.remove(t_hash) {
                Ok(mut ps) => {
                    // All good, proceed with pruning.

                },
                Err(GError::TopicNotInMesh{t_hash: _t_hash, err}) => {
                    // The topic needs to be in the local peer's mesh view
                    // in order to be able to prune peers from this topic.
                    topics_not_in_mesh.push(*t_hash);
                    t_hashes_v.remove(i);
                },
                // Shouldn't happen, just for the compiler.
                Err(err) => {return Err(err);},
            }
        }
        if !topics_not_in_mesh.is_empty() {
            prune_errs.topics_not_in_mesh
                = Some(topics_not_in_mesh);
            prune_errs.has_errors = true;
        }
        if prune_errs.has_errors() {
            return Ok(Some(prune_errs));
        } else {
            return Ok(None);
        }
    }

    /// gossip: this notifies the peer that the input messages
    /// were recently seen and are available on request.
    /// Checks the seen set and requests unknown messages with an IWANT
    /// message.
    pub fn i_have(&mut self,
        msg_hashes: impl IntoIterator<Item = impl AsRef<MsgHash>>) {
        let i_want = ControlIWant::new();
    }

    /// Requests transmission of messages announced in an IHAVE message.
    /// Forwards all request messages that are present in mcache to the
    /// requesting peer, as well as (unlike the spec and Go impl) returning
    /// those that are not, via reconstructing them from the message hashes.
    pub fn i_want(&mut self,
        msg_hashes: impl IntoIterator<Item = impl AsRef<MsgHash>>)
            -> GResult<(MsgMap, MsgMap)> {
        let mut return_msgs = MsgMap::new();
        let mut not_found = MsgMap::new();
        for msg_hash in msg_hashes {
            let mhr = msg_hash.as_ref();
            // let mh = |mhr: &MsgHash| {mhr.clone()};
            if let Some(msg) = self.mcache.get(mhr.clone()) {
                return_msgs.insert(mhr.clone(), msg.clone());
            } else {
                match GMessage::from_msg_hash(mhr.clone()) {
                    Ok(m) => {not_found.insert(mhr.clone(), m);},
                    Err(err) => {return Err(err);}
                }
            }
        }
        Ok((return_msgs, not_found))
    }

    /// Joins the local peer to a single topic as a singular iteration of
    /// `join_topics()`.
    pub fn join(&mut self, topic_hash: impl AsRef<TopicHash>+Clone)
        -> GResult<Option<Vec<TopicHash>>> {
        self.join_topics(&mut iter::once(topic_hash))
    }

    /// Joins the local peer to many topics.
    ///
    /// If a topic is not joined due to insufficient peers, after trying to
    /// select them first from the fanout of a topic and then in the local
    /// peer's connected peers that are subscribed to a topic, it returns its
    /// hash. If a topic is not in the local mesh view, it still tries to join
    /// via connected peers that are subscribed to the topic.
    pub fn join_topics(&mut self,
        topic_hashes: &mut(impl IntoIterator<Item = impl AsRef<TopicHash> + Clone>+ Clone))
        -> GResult<Option<Vec<TopicHash>>> {
        let mut topics_not_joined = Vec::new();
        for mut topic_hash in topic_hashes.clone() {
            let th_cl = (&mut topic_hash).clone();
            let mut thr = th_cl.as_ref();
            let mut fanout_peers = &mut self.fanout
                .get_peers_from_topic(thr);
            let mut peer_count = 0;
            let mut joined = false;
            let mut peers_to_add = Vec::new();
            match fanout_peers {
                Ok(fanout_peers) => {
                    self.select_fanout_peers(fanout_peers,
                    &mut peers_to_add, &mut peer_count, &mut joined,
                    &mut thr, &mut topic_hash, &mut topics_not_joined);
                    if joined == false {
                        self.try_select_connected_peers(
                                &mut peers_to_add, &mut peer_count,
                                &mut joined, thr, &mut topic_hash,
                                &mut topics_not_joined);
                    }
                },
                // Otherwise, with `select_from_connected_peers()`, it
                // selects `TARGET_MESH_DEGREE` peers from
                // `peers.gossipsub[topic]`, and likewise adds them to
                // `mesh[topic]` and notifies them with a `GRAFT(topic)`
                // control message.
                // The error should be TopicNotInMesh, therefore we continue
                // with `select_from_connected_peers()`.
                Err(err) => {self.try_select_connected_peers(
                                &mut peers_to_add, &mut peer_count,
                                &mut joined, thr, &mut topic_hash,
                                &mut topics_not_joined);},
            }
        }
        if topics_not_joined.is_empty() {
            return Ok(None)
        } else {
            return Ok(Some(topics_not_joined))
        }
    }

    fn inner_loop_for_peer(&mut self,
        peer: PeerId,
        peers_to_add: &mut Vec<PeerId>,
        peer_count: &mut u32,
        joined: &mut bool,
        th: &TopicHash,
        topic_hash: &mut impl AsRef<TopicHash>,
        topics_not_joined: &mut Vec<TopicHash>,
    ) -> (u32, bool) {
        peers_to_add.push(peer);
        *peer_count += 1;
        // If it already has `TARGET_MESH_DEGREE` peers from the
        // `fanout` peers of a topic, then it adds them to
        // `mesh[topic]`, and notifies them with a `GRAFT(topic)`
        // control message.
        if *peer_count == TARGET_MESH_DEGREE {
            self.mesh.insert(th.clone(), peers_to_add.clone());
            self.graft_peers_to_topic(peers_to_add, topic_hash);
            *joined = true;
        }
        return (*peer_count, *joined);
    }

    fn try_select_connected_peers(&mut self,
        peers_to_add: &mut Vec<PeerId>,
        peer_count: &mut u32,
        joined: &mut bool,
        th: &TopicHash,
        topic_hash: &mut impl AsRef<TopicHash>,
        topics_not_joined: &mut Vec<TopicHash>,
    ) {
        for peer in self.connected_peers.gossipsub.clone().keys() {
            let (peer_count, joined)
                = self.inner_loop_for_peer(peer.clone(), peers_to_add,
                peer_count, joined, th, topic_hash,
                topics_not_joined);
            if joined == true {
                return;
            }
        }
        topics_not_joined.push(th.clone());
    }

    fn select_fanout_peers(&mut self,
        fanout_peers: &mut Vec<PeerId>,
        peers_to_add: &mut Vec<PeerId>,
        peer_count: &mut u32,
        joined: &mut bool,
        th: &TopicHash,
        topic_hash: &mut impl AsRef<TopicHash>,
        topics_not_joined: &mut Vec<TopicHash>
    ) {
        for peer in fanout_peers {
            let return_tuple = self.inner_loop_for_peer(
                peer.clone(),
                peers_to_add, peer_count,
                joined, th, topic_hash,
                topics_not_joined);
            *peer_count = return_tuple.0;
            *joined = return_tuple.1;
            if *joined == true {
                break;
            }
        }
    }

    /// Tries to make a peer leave a topic.
    pub fn leave_topic(&mut self, topic: impl AsRef<TopicHash>) {
        //
    }

    /// Makes the local peer try to leave topics.
    ///
    /// It tries to notifies the peers in the
    /// mesh entry for the topic (`<TopicHash, Vec<PeerId>>`) via
    /// `prune_peers()` and subsequently removes the entry.
    pub fn foo() {}
    // pub fn leave_topics(&mut self, topic_hashes: impl IntoIterator<
    //     Item = impl AsRef<TopicHash>>) -> GResult<Option<Vec<TopicHash>>> {
        // match self.prune_peers_from_topics(topic_hashes) {}
        // let mut t_hashes_v: Vec<TopicHash> = topic_hashes.into_iter()
        //     .map(|th| th.as_ref().clone()).collect();
        // let m = &mut self.mesh;
        // let mut topics_not_in_mesh = Vec::new();
        // // Check the topic is in the mesh.
        // for (i, t_hash) in t_hashes_v.clone().iter().enumerate() {
        //     match m.remove(t_hash) {
        //         Ok(mut ps) => {
        //             self.prune_peers(ps, (*t_hash));
        //         },
        //         Err(GError::TopicNotInMesh{t_hash: _t_hash, err}) => {
        //             topics_not_in_mesh.push(t_hash.clone());
        //             t_hashes_v.remove(i);
        //         },
        //         // Shouldn't happen, just for the compiler.
        //         Err(err) => {return Err(err);},
        //     }
        // }
        // if !topics_not_in_mesh.is_empty() {
        //     return Ok(Some(topics_not_in_mesh));
        // } else {
        //     return Ok(None);
        // }
    // }
}

impl<'a, TSubstream, TTopology> NetworkBehaviour<TTopology> for
    Gossipsub<'a, TSubstream>
where
    TSubstream: AsyncRead + AsyncWrite,
{
    type ProtocolsHandler = GossipsubHandler<TSubstream>;
    // TODO: this seems rather complicated to implement instead of the above
    // e.g. with inject_node_event—the event input, etc.
    // type ProtocolsHandler =
    //     ProtocolsHandlerSelect<GossipsubHandler<TSubstream>,
    //     FloodsubHandler<TSubstream>>;
    type OutEvent = GOutEvents;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        GossipsubHandler::new()
        //.select(FloodsubHandler::new())
    }

    fn inject_connected(&mut self, id: PeerId, _: ConnectedPoint) {
        // We need to send our subscriptions to the newly-connected node.
        for topic in self.subscribed_topics.iter() {
            self.events.push_back(NetworkBehaviourAction::SendEvent {
                peer_id: id.clone(),
                event: GossipsubRpc {
                    messages: Vec::new(),
                    subscriptions: vec![GossipsubSubscription {
                        topic: topic.hash().clone(),
                        action: GossipsubSubscriptionAction::Subscribe,
                    }],
                    control: None
                },
            });
        }

        self.connected_peers.gossipsub.insert(id.clone(), SmallVec::new());
    }

    fn inject_disconnected(&mut self, id: &PeerId, _: ConnectedPoint) {
        let was_in = self.connected_peers.gossipsub.remove(id);
        debug_assert!(was_in.is_some());
    }

    fn inject_node_event(
        &mut self,
        propagation_source: PeerId,
        event: GossipsubRpc,
    ) {
        // Update connected peers topics
        for subscription in event.subscriptions {
            let mut remote_peer_topics = self.connected_peers.gossipsub
                .get_mut(&propagation_source)
                .expect("connected_peers is kept in sync with the peers we \
                are connected to; we are guaranteed to only receive events \
                from connected peers; QED");
            match subscription.action {
                GossipsubSubscriptionAction::Subscribe => {
                    if !remote_peer_topics.contains(&subscription.topic) {
                        remote_peer_topics.push(subscription.topic);
                    }
                }
                GossipsubSubscriptionAction::Unsubscribe => {
                    if let Some(pos) = remote_peer_topics.iter()
                        .position(|t| t == &subscription.topic) {
                        remote_peer_topics.remove(pos);
                    }
                }
            }
        }

        // List of messages we're going to propagate on the network.
        let mut rpcs_to_dispatch: Vec<(PeerId, GossipsubRpc)> = Vec::new();

        for message in event.messages {
            // Use `self.received` to skip the messages that we have already
            // received in the past.
            // Note that this can be a false positive.
            if !self.received.test_and_add(&message) {
                continue;
            }

            // Add the message to be dispatched to the user, if they're
            // subscribed to the topic.
            if self.subscribed_topics.iter().any(|t| message.topics.iter()
                .any(|u| t == u.1)) {
                self.events.push_back(NetworkBehaviourAction::GenerateEvent
                    (GOutEvents::GMsg(message.clone())));
            }

            // Propagate the message to everyone else who is subscribed to any
            // of the topics.
            for (peer_id, subscr_topics) in self.connected_peers.gossipsub
                .iter() {
                if peer_id == &propagation_source {
                    continue;
                }

                if !subscr_topics.iter().any(|t| message.topics.iter()
                    .any(|u| t == u.0)) {
                    continue;
                }

                if let Some(pos) = rpcs_to_dispatch.iter()
                    .position(|(p, _)| p == peer_id) {
                    rpcs_to_dispatch[pos].1.messages.push(message.clone());
                } else {
                    rpcs_to_dispatch.push((peer_id.clone(), GossipsubRpc {
                        subscriptions: Vec::new(),
                        messages: vec![message.clone()],
                        control: None,
                    }));
                }
            }
        }
        // let mut ctrl = event.control;
        if let Some(ctrl) = event.control {
            // Add the control message to be dispatched to the user. We should
            // only get a control message for a topic if we are subscribed to
            // it, IIUIC. If this were not the case, we would have to check
            // each topic in the control message to see that we are subscribed
            // to it.
            self.events.push_back(NetworkBehaviourAction::GenerateEvent
                (GOutEvents::CtrlMsg(ctrl.clone())));

            // Propagate the control message to everyone else who is
            // subscribed to any of the topics.
            for (peer_id, subscr_topics) in self.connected_peers.gossipsub
                .iter() {
                if peer_id == &propagation_source {
                    continue;
                }

                // Again, I'm assuming that the peer is already subscribed to // any topics in the control message.
                // TODO: double-check this.

                if let Some(pos) = rpcs_to_dispatch.iter()
                    .position(|(p, _)| p == peer_id) {
                    rpcs_to_dispatch[pos].1.control = Some(ctrl.clone());
                } else {
                    rpcs_to_dispatch.push((peer_id.clone(), GossipsubRpc {
                        subscriptions: Vec::new(),
                        messages: Vec::new(),
                        control: Some(ctrl.clone()),
                    }));
                }
            }
        }

        for (peer_id, rpc) in rpcs_to_dispatch {
            self.events.push_back(NetworkBehaviourAction::SendEvent {
                peer_id,
                event: rpc,
            });
        }
    }

    fn poll(
        &mut self,
        _: &mut PollParameters<TTopology>,
    ) -> Async<
        NetworkBehaviourAction<
            <Self::ProtocolsHandler as ProtocolsHandler>::InEvent,
            Self::OutEvent,
        >,
    > {
        if let Some(event) = self.events.pop_front() {
            return Async::Ready(event);
        }

        Async::NotReady
    }
}

// pub(crate) struct PeerLoopState {
//     peers_to_add: Vec<PeerId>,
//     peer_count: u32,
//     joined: bool,
//     th: &TopicHash,
//     topic_hash: impl AsRef<TopicHash>,
// }
