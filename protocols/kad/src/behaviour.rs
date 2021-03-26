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

//! Implementation of the `Kademlia` network behaviour.

mod test;

use crate::K_VALUE;
use crate::addresses::Addresses;
use crate::handler::{
    KademliaHandlerProto,
    KademliaHandlerConfig,
    KademliaRequestId,
    KademliaHandlerEvent,
    KademliaHandlerIn
};
use crate::jobs::*;
use crate::kbucket::{self, KBucketsTable, NodeStatus};
use crate::protocol::{KademliaProtocolConfig, KadConnectionType, KadPeer};
use crate::query::{Query, QueryId, QueryPool, QueryConfig, QueryPoolState};
use crate::record::{self, store::{self, RecordStore}, Record, ProviderRecord};
use fnv::{FnvHashMap, FnvHashSet};
use libp2p_core::{ConnectedPoint, Multiaddr, PeerId, connection::{ConnectionId, ListenerId}};
use libp2p_swarm::{
    DialPeerCondition,
    NetworkBehaviour,
    NetworkBehaviourAction,
    NotifyHandler,
    PollParameters,
};
use log::{info, debug, warn};
use smallvec::SmallVec;
use std::{borrow::Cow, error, time::Duration};
use std::collections::{HashSet, VecDeque, BTreeMap};
use std::fmt;
use std::num::NonZeroUsize;
use std::task::{Context, Poll};
use std::vec;
use wasm_timer::Instant;

pub use crate::query::QueryStats;

/// `Kademlia` is a `NetworkBehaviour` that implements the libp2p
/// Kademlia protocol.
pub struct Kademlia<TStore> {
    /// The Kademlia routing table.
    kbuckets: KBucketsTable<kbucket::Key<PeerId>, Addresses>,

    /// The k-bucket insertion strategy.
    kbucket_inserts: KademliaBucketInserts,

    /// Configuration of the wire protocol.
    protocol_config: KademliaProtocolConfig,

    /// The currently active (i.e. in-progress) queries.
    queries: QueryPool<QueryInner>,

    /// The currently connected peers.
    ///
    /// This is a superset of the connected peers currently in the routing table.
    connected_peers: FnvHashSet<PeerId>,

    /// Periodic job for re-publication of provider records for keys
    /// provided by the local node.
    add_provider_job: Option<AddProviderJob>,

    /// Periodic job for (re-)replication and (re-)publishing of
    /// regular (value-)records.
    put_record_job: Option<PutRecordJob>,

    /// The TTL of regular (value-)records.
    record_ttl: Option<Duration>,

    /// The TTL of provider records.
    provider_record_ttl: Option<Duration>,

    /// How long to keep connections alive when they're idle.
    connection_idle_timeout: Duration,

    /// Queued events to return when the behaviour is being polled.
    queued_events: VecDeque<NetworkBehaviourAction<KademliaHandlerIn<QueryId>, KademliaEvent>>,

    /// The currently known addresses of the local node.
    local_addrs: HashSet<Multiaddr>,

    /// See [`KademliaConfig::caching`].
    caching: KademliaCaching,

    /// The record storage.
    store: TStore,
}

/// The configurable strategies for the insertion of peers
/// and their addresses into the k-buckets of the Kademlia
/// routing table.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum KademliaBucketInserts {
    /// Whenever a connection to a peer is established as a
    /// result of a dialing attempt and that peer is not yet
    /// in the routing table, it is inserted as long as there
    /// is a free slot in the corresponding k-bucket. If the
    /// k-bucket is full but still has a free pending slot,
    /// it may be inserted into the routing table at a later time if an unresponsive
    /// disconnected peer is evicted from the bucket.
    OnConnected,
    /// New peers and addresses are only added to the routing table via
    /// explicit calls to [`Kademlia::add_address`].
    ///
    /// > **Note**: Even though peers can only get into the
    /// > routing table as a result of [`Kademlia::add_address`],
    /// > routing table entries are still updated as peers
    /// > connect and disconnect (i.e. the order of the entries
    /// > as well as the network addresses).
    Manual,
}

/// The configuration for the `Kademlia` behaviour.
///
/// The configuration is consumed by [`Kademlia::new`].
#[derive(Debug, Clone)]
pub struct KademliaConfig {
    kbucket_pending_timeout: Duration,
    query_config: QueryConfig,
    protocol_config: KademliaProtocolConfig,
    record_ttl: Option<Duration>,
    record_replication_interval: Option<Duration>,
    record_publication_interval: Option<Duration>,
    provider_record_ttl: Option<Duration>,
    provider_publication_interval: Option<Duration>,
    connection_idle_timeout: Duration,
    kbucket_inserts: KademliaBucketInserts,
    caching: KademliaCaching,
}

/// The configuration for Kademlia "write-back" caching after successful
/// lookups via [`Kademlia::get_record`].
#[derive(Debug, Clone)]
pub enum KademliaCaching {
    /// Caching is disabled and the peers closest to records being looked up
    /// that do not return a record are not tracked, i.e.
    /// [`GetRecordOk::cache_candidates`] is always empty.
    Disabled,
    /// Up to `max_peers` peers not returning a record that are closest to the key
    /// being looked up are tracked and returned in [`GetRecordOk::cache_candidates`].
    /// Furthermore, if [`Kademlia::get_record`] is used with a quorum of 1, the
    /// found record is automatically sent to (i.e. cached at) these peers. For lookups with a
    /// quorum > 1, the write-back operation must be performed explicitly, if
    /// desired and after choosing a record from the results, via [`Kademlia::put_record_to`].
    Enabled { max_peers: u16 },
}

impl Default for KademliaConfig {
    fn default() -> Self {
        KademliaConfig {
            kbucket_pending_timeout: Duration::from_secs(60),
            query_config: QueryConfig::default(),
            protocol_config: Default::default(),
            record_ttl: Some(Duration::from_secs(36 * 60 * 60)),
            record_replication_interval: Some(Duration::from_secs(60 * 60)),
            record_publication_interval: Some(Duration::from_secs(24 * 60 * 60)),
            provider_publication_interval: Some(Duration::from_secs(12 * 60 * 60)),
            provider_record_ttl: Some(Duration::from_secs(24 * 60 * 60)),
            connection_idle_timeout: Duration::from_secs(10),
            kbucket_inserts: KademliaBucketInserts::OnConnected,
            caching: KademliaCaching::Enabled { max_peers: 1 },
        }
    }
}

impl KademliaConfig {
    /// Sets a custom protocol name.
    ///
    /// Kademlia nodes only communicate with other nodes using the same protocol
    /// name. Using a custom name therefore allows to segregate the DHT from
    /// others, if that is desired.
    pub fn set_protocol_name(&mut self, name: impl Into<Cow<'static, [u8]>>) -> &mut Self {
        self.protocol_config.set_protocol_name(name);
        self
    }

    /// Sets the timeout for a single query.
    ///
    /// > **Note**: A single query usually comprises at least as many requests
    /// > as the replication factor, i.e. this is not a request timeout.
    ///
    /// The default is 60 seconds.
    pub fn set_query_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.query_config.timeout = timeout;
        self
    }

    /// Sets the replication factor to use.
    ///
    /// The replication factor determines to how many closest peers
    /// a record is replicated. The default is [`K_VALUE`].
    pub fn set_replication_factor(&mut self, replication_factor: NonZeroUsize) -> &mut Self {
        self.query_config.replication_factor = replication_factor;
        self
    }

    /// Sets the allowed level of parallelism for iterative queries.
    ///
    /// The `α` parameter in the Kademlia paper. The maximum number of peers
    /// that an iterative query is allowed to wait for in parallel while
    /// iterating towards the closest nodes to a target. Defaults to
    /// `ALPHA_VALUE`.
    ///
    /// This only controls the level of parallelism of an iterative query, not
    /// the level of parallelism of a query to a fixed set of peers.
    ///
    /// When used with [`KademliaConfig::disjoint_query_paths`] it equals
    /// the amount of disjoint paths used.
    pub fn set_parallelism(&mut self, parallelism: NonZeroUsize) -> &mut Self {
        self.query_config.parallelism = parallelism;
        self
    }

    /// Require iterative queries to use disjoint paths for increased resiliency
    /// in the presence of potentially adversarial nodes.
    ///
    /// When enabled the number of disjoint paths used equals the configured
    /// parallelism.
    ///
    /// See the S/Kademlia paper for more information on the high level design
    /// as well as its security improvements.
    pub fn disjoint_query_paths(&mut self, enabled: bool) -> &mut Self {
        self.query_config.disjoint_query_paths = enabled;
        self
    }

    /// Sets the TTL for stored records.
    ///
    /// The TTL should be significantly longer than the (re-)publication
    /// interval, to avoid premature expiration of records. The default is 36
    /// hours.
    ///
    /// `None` means records never expire.
    ///
    /// Does not apply to provider records.
    pub fn set_record_ttl(&mut self, record_ttl: Option<Duration>) -> &mut Self {
        self.record_ttl = record_ttl;
        self
    }

    /// Sets the (re-)replication interval for stored records.
    ///
    /// Periodic replication of stored records ensures that the records
    /// are always replicated to the available nodes closest to the key in the
    /// context of DHT topology changes (i.e. nodes joining and leaving), thus
    /// ensuring persistence until the record expires. Replication does not
    /// prolong the regular lifetime of a record (for otherwise it would live
    /// forever regardless of the configured TTL). The expiry of a record
    /// is only extended through re-publication.
    ///
    /// This interval should be significantly shorter than the publication
    /// interval, to ensure persistence between re-publications. The default
    /// is 1 hour.
    ///
    /// `None` means that stored records are never re-replicated.
    ///
    /// Does not apply to provider records.
    pub fn set_replication_interval(&mut self, interval: Option<Duration>) -> &mut Self {
        self.record_replication_interval = interval;
        self
    }

    /// Sets the (re-)publication interval of stored records.
    ///
    /// Records persist in the DHT until they expire. By default, published
    /// records are re-published in regular intervals for as long as the record
    /// exists in the local storage of the original publisher, thereby extending
    /// the records lifetime.
    ///
    /// This interval should be significantly shorter than the record TTL, to
    /// ensure records do not expire prematurely. The default is 24 hours.
    ///
    /// `None` means that stored records are never automatically re-published.
    ///
    /// Does not apply to provider records.
    pub fn set_publication_interval(&mut self, interval: Option<Duration>) -> &mut Self {
        self.record_publication_interval = interval;
        self
    }

    /// Sets the TTL for provider records.
    ///
    /// `None` means that stored provider records never expire.
    ///
    /// Must be significantly larger than the provider publication interval.
    pub fn set_provider_record_ttl(&mut self, ttl: Option<Duration>) -> &mut Self {
        self.provider_record_ttl = ttl;
        self
    }

    /// Sets the interval at which provider records for keys provided
    /// by the local node are re-published.
    ///
    /// `None` means that stored provider records are never automatically
    /// re-published.
    ///
    /// Must be significantly less than the provider record TTL.
    pub fn set_provider_publication_interval(&mut self, interval: Option<Duration>) -> &mut Self {
        self.provider_publication_interval = interval;
        self
    }

    /// Sets the amount of time to keep connections alive when they're idle.
    pub fn set_connection_idle_timeout(&mut self, duration: Duration) -> &mut Self {
        self.connection_idle_timeout = duration;
        self
    }

    /// Modifies the maximum allowed size of individual Kademlia packets.
    ///
    /// It might be necessary to increase this value if trying to put large
    /// records.
    pub fn set_max_packet_size(&mut self, size: usize) -> &mut Self {
        self.protocol_config.set_max_packet_size(size);
        self
    }

    /// Sets the k-bucket insertion strategy for the Kademlia routing table.
    pub fn set_kbucket_inserts(&mut self, inserts: KademliaBucketInserts) -> &mut Self {
        self.kbucket_inserts = inserts;
        self
    }

    /// Sets the [`KademliaCaching`] strategy to use for successful lookups.
    ///
    /// The default is [`KademliaCaching::Enabled`] with a `max_peers` of 1.
    /// Hence, with default settings and a lookup quorum of 1, a successful lookup
    /// will result in the record being cached at the closest node to the key that
    /// did not return the record, i.e. the standard Kademlia behaviour.
    pub fn set_caching(&mut self, c: KademliaCaching) -> &mut Self {
        self.caching = c;
        self
    }
}

impl<TStore> Kademlia<TStore>
where
    for<'a> TStore: RecordStore<'a>
{
    /// Creates a new `Kademlia` network behaviour with a default configuration.
    pub fn new(id: PeerId, store: TStore) -> Self {
        Self::with_config(id, store, Default::default())
    }

    /// Get the protocol name of this kademlia instance.
    pub fn protocol_name(&self) -> &[u8] {
        self.protocol_config.protocol_name()
    }

    /// Creates a new `Kademlia` network behaviour with the given configuration.
    pub fn with_config(id: PeerId, store: TStore, config: KademliaConfig) -> Self {
        let local_key = kbucket::Key::from(id);

        let put_record_job = config
            .record_replication_interval
            .or(config.record_publication_interval)
            .map(|interval| PutRecordJob::new(
                id,
                interval,
                config.record_publication_interval,
                config.record_ttl,
            ));

        let add_provider_job = config
            .provider_publication_interval
            .map(AddProviderJob::new);

        Kademlia {
            store,
            kbuckets: KBucketsTable::new(local_key, config.kbucket_pending_timeout),
            kbucket_inserts: config.kbucket_inserts,
            protocol_config: config.protocol_config,
            queued_events: VecDeque::with_capacity(config.query_config.replication_factor.get()),
            queries: QueryPool::new(config.query_config),
            connected_peers: Default::default(),
            add_provider_job,
            put_record_job,
            record_ttl: config.record_ttl,
            provider_record_ttl: config.provider_record_ttl,
            connection_idle_timeout: config.connection_idle_timeout,
            local_addrs: HashSet::new(),
            caching: config.caching,
        }
    }

    /// Gets an iterator over immutable references to all running queries.
    pub fn iter_queries(&self) -> impl Iterator<Item = QueryRef<'_>> {
        self.queries.iter().filter_map(|query|
            if !query.is_finished() {
                Some(QueryRef { query })
            } else {
                None
            })
    }

    /// Gets an iterator over mutable references to all running queries.
    pub fn iter_queries_mut(&mut self) -> impl Iterator<Item = QueryMut<'_>> {
        self.queries.iter_mut().filter_map(|query|
            if !query.is_finished() {
                Some(QueryMut { query })
            } else {
                None
            })
    }

    /// Gets an immutable reference to a running query, if it exists.
    pub fn query(&self, id: &QueryId) -> Option<QueryRef<'_>> {
        self.queries.get(id).and_then(|query|
            if !query.is_finished() {
                Some(QueryRef { query })
            } else {
                None
            })
    }

    /// Gets a mutable reference to a running query, if it exists.
    pub fn query_mut<'a>(&'a mut self, id: &QueryId) -> Option<QueryMut<'a>> {
        self.queries.get_mut(id).and_then(|query|
            if !query.is_finished() {
                Some(QueryMut { query })
            } else {
                None
            })
    }

    /// Adds a known listen address of a peer participating in the DHT to the
    /// routing table.
    ///
    /// Explicitly adding addresses of peers serves two purposes:
    ///
    ///   1. In order for a node to join the DHT, it must know about at least
    ///      one other node of the DHT.
    ///
    ///   2. When a remote peer initiates a connection and that peer is not
    ///      yet in the routing table, the `Kademlia` behaviour must be
    ///      informed of an address on which that peer is listening for
    ///      connections before it can be added to the routing table
    ///      from where it can subsequently be discovered by all peers
    ///      in the DHT.
    ///
    /// If the routing table has been updated as a result of this operation,
    /// a [`KademliaEvent::RoutingUpdated`] event is emitted.
    pub fn add_address(&mut self, peer: &PeerId, address: Multiaddr) -> RoutingUpdate {
        let key = kbucket::Key::from(*peer);
        match self.kbuckets.entry(&key) {
            kbucket::Entry::Present(mut entry, _) => {
                if entry.value().insert(address) {
                    self.queued_events.push_back(NetworkBehaviourAction::GenerateEvent(
                        KademliaEvent::RoutingUpdated {
                            peer: *peer,
                            addresses: entry.value().clone(),
                            old_peer: None,
                        }
                    ))
                }
                RoutingUpdate::Success
            }
            kbucket::Entry::Pending(mut entry, _) => {
                entry.value().insert(address);
                RoutingUpdate::Pending
            }
            kbucket::Entry::Absent(entry) => {
                let addresses = Addresses::new(address);
                let status =
                    if self.connected_peers.contains(peer) {
                        NodeStatus::Connected
                    } else {
                        NodeStatus::Disconnected
                    };
                match entry.insert(addresses.clone(), status) {
                    kbucket::InsertResult::Inserted => {
                        self.queued_events.push_back(NetworkBehaviourAction::GenerateEvent(
                            KademliaEvent::RoutingUpdated {
                                peer: *peer,
                                addresses,
                                old_peer: None,
                            }
                        ));
                        RoutingUpdate::Success
                    },
                    kbucket::InsertResult::Full => {
                        debug!("Bucket full. Peer not added to routing table: {}", peer);
                        RoutingUpdate::Failed
                    },
                    kbucket::InsertResult::Pending { disconnected } => {
                        self.queued_events.push_back(NetworkBehaviourAction::DialPeer {
                            peer_id: disconnected.into_preimage(),
                            condition: DialPeerCondition::Disconnected
                        });
                        RoutingUpdate::Pending
                    },
                }
            },
            kbucket::Entry::SelfEntry => RoutingUpdate::Failed,
        }
    }

    /// Removes an address of a peer from the routing table.
    ///
    /// If the given address is the last address of the peer in the
    /// routing table, the peer is removed from the routing table
    /// and `Some` is returned with a view of the removed entry.
    /// The same applies if the peer is currently pending insertion
    /// into the routing table.
    ///
    /// If the given peer or address is not in the routing table,
    /// this is a no-op.
    pub fn remove_address(&mut self, peer: &PeerId, address: &Multiaddr)
        -> Option<kbucket::EntryView<kbucket::Key<PeerId>, Addresses>>
    {
        let key = kbucket::Key::from(*peer);
        match self.kbuckets.entry(&key) {
            kbucket::Entry::Present(mut entry, _) => {
                if entry.value().remove(address).is_err() {
                    Some(entry.remove()) // it is the last address, thus remove the peer.
                } else {
                    None
                }
            }
            kbucket::Entry::Pending(mut entry, _) => {
                if entry.value().remove(address).is_err() {
                    Some(entry.remove()) // it is the last address, thus remove the peer.
                } else {
                    None
                }
            }
            kbucket::Entry::Absent(..) | kbucket::Entry::SelfEntry => {
                None
            }
        }
    }

    /// Removes a peer from the routing table.
    ///
    /// Returns `None` if the peer was not in the routing table,
    /// not even pending insertion.
    pub fn remove_peer(&mut self, peer: &PeerId)
        -> Option<kbucket::EntryView<kbucket::Key<PeerId>, Addresses>>
    {
        let key = kbucket::Key::from(*peer);
        match self.kbuckets.entry(&key) {
            kbucket::Entry::Present(entry, _) => {
                Some(entry.remove())
            }
            kbucket::Entry::Pending(entry, _) => {
                Some(entry.remove())
            }
            kbucket::Entry::Absent(..) | kbucket::Entry::SelfEntry => {
                None
            }
        }
    }

    /// Returns an iterator over all non-empty buckets in the routing table.
    pub fn kbuckets(&mut self)
        -> impl Iterator<Item = kbucket::KBucketRef<'_, kbucket::Key<PeerId>, Addresses>>
    {
        self.kbuckets.iter().filter(|b| !b.is_empty())
    }

    /// Returns the k-bucket for the distance to the given key.
    ///
    /// Returns `None` if the given key refers to the local key.
    pub fn kbucket<K>(&mut self, key: K)
        -> Option<kbucket::KBucketRef<'_, kbucket::Key<PeerId>, Addresses>>
    where
        K: Into<kbucket::Key<K>> + Clone
    {
        self.kbuckets.bucket(&key.into())
    }

    /// Initiates an iterative query for the closest peers to the given key.
    ///
    /// The result of the query is delivered in a
    /// [`KademliaEvent::QueryResult{QueryResult::GetClosestPeers}`].
    pub fn get_closest_peers<K>(&mut self, key: K) -> QueryId
    where
        K: Into<kbucket::Key<K>> + Into<Vec<u8>> + Clone
    {
        let info = QueryInfo::GetClosestPeers { key: key.clone().into() };
        let target: kbucket::Key<K> = key.into();
        let peers = self.kbuckets.closest_keys(&target);
        let inner = QueryInner::new(info);
        self.queries.add_iter_closest(target.clone(), peers, inner)
    }

    /// Performs a lookup for a record in the DHT.
    ///
    /// The result of this operation is delivered in a
    /// [`KademliaEvent::QueryResult{QueryResult::GetRecord}`].
    pub fn get_record(&mut self, key: &record::Key, quorum: Quorum) -> QueryId {
        let quorum = quorum.eval(self.queries.config().replication_factor);
        let mut records = Vec::with_capacity(quorum.get());

        if let Some(record) = self.store.get(key) {
            if record.is_expired(Instant::now()) {
                self.store.remove(key)
            } else {
                records.push(PeerRecord{ peer: None, record: record.into_owned()});
            }
        }

        let done = records.len() >= quorum.get();
        let target = kbucket::Key::new(key.clone());
        let info = QueryInfo::GetRecord {
            key: key.clone(),
            records,
            quorum,
            cache_candidates: BTreeMap::new(),
        };
        let peers = self.kbuckets.closest_keys(&target);
        let inner = QueryInner::new(info);
        let id = self.queries.add_iter_closest(target.clone(), peers, inner); // (*)

        // Instantly finish the query if we already have enough records.
        if done {
            self.queries.get_mut(&id).expect("by (*)").finish();
        }

        id
    }

    /// Stores a record in the DHT, locally as well as at the nodes
    /// closest to the key as per the xor distance metric.
    ///
    /// Returns `Ok` if a record has been stored locally, providing the
    /// `QueryId` of the initial query that replicates the record in the DHT.
    /// The result of the query is eventually reported as a
    /// [`KademliaEvent::QueryResult{QueryResult::PutRecord}`].
    ///
    /// The record is always stored locally with the given expiration. If the record's
    /// expiration is `None`, the common case, it does not expire in local storage
    /// but is still replicated with the configured record TTL. To remove the record
    /// locally and stop it from being re-published in the DHT, see [`Kademlia::remove_record`].
    ///
    /// After the initial publication of the record, it is subject to (re-)replication
    /// and (re-)publication as per the configured intervals. Periodic (re-)publication
    /// does not update the record's expiration in local storage, thus a given record
    /// with an explicit expiration will always expire at that instant and until then
    /// is subject to regular (re-)replication and (re-)publication.
    pub fn put_record(&mut self, mut record: Record, quorum: Quorum) -> Result<QueryId, store::Error> {
        record.publisher = Some(*self.kbuckets.local_key().preimage());
        self.store.put(record.clone())?;
        record.expires = record.expires.or_else(||
            self.record_ttl.map(|ttl| Instant::now() + ttl));
        let quorum = quorum.eval(self.queries.config().replication_factor);
        let target = kbucket::Key::new(record.key.clone());
        let peers = self.kbuckets.closest_keys(&target);
        let context = PutRecordContext::Publish;
        let info = QueryInfo::PutRecord {
            context,
            record,
            quorum,
            phase: PutRecordPhase::GetClosestPeers
        };
        let inner = QueryInner::new(info);
        Ok(self.queries.add_iter_closest(target.clone(), peers, inner))
    }

    /// Stores a record at specific peers, without storing it locally.
    ///
    /// The given [`Quorum`] is understood in the context of the total
    /// number of distinct peers given.
    ///
    /// If the record's expiration is `None`, the configured record TTL is used.
    ///
    /// > **Note**: This is not a regular Kademlia DHT operation. It may be
    /// > used to selectively update or store a record to specific peers
    /// > for the purpose of e.g. making sure these peers have the latest
    /// > "version" of a record or to "cache" a record at further peers
    /// > to increase the lookup success rate on the DHT for other peers.
    /// >
    /// > In particular, if lookups are performed with a quorum > 1 multiple
    /// > possibly different records may be returned and the standard Kademlia
    /// > procedure of "caching" (i.e. storing) a found record at the closest
    /// > node to the key that _did not_ return it cannot be employed
    /// > transparently. In that case, client code can explicitly choose
    /// > which record to store at which peers for analogous write-back
    /// > caching or for other reasons.
    pub fn put_record_to<I>(&mut self, mut record: Record, peers: I, quorum: Quorum) -> QueryId
    where
        I: ExactSizeIterator<Item = PeerId>
    {
        let quorum = if peers.len() > 0 {
            quorum.eval(NonZeroUsize::new(peers.len()).expect("> 0"))
        } else {
            // If no peers are given, we just let the query fail immediately
            // due to the fact that the quorum must be at least one, instead of
            // introducing a new kind of error.
            NonZeroUsize::new(1).expect("1 > 0")
        };
        record.expires = record.expires.or_else(||
            self.record_ttl.map(|ttl| Instant::now() + ttl));
        let context = PutRecordContext::Custom;
        let info = QueryInfo::PutRecord {
            context,
            record,
            quorum,
            phase: PutRecordPhase::PutRecord {
                success: Vec::new(),
                get_closest_peers_stats: QueryStats::empty()
            }
        };
        let inner = QueryInner::new(info);
        self.queries.add_fixed(peers, inner)
    }

    /// Removes the record with the given key from _local_ storage,
    /// if the local node is the publisher of the record.
    ///
    /// Has no effect if a record for the given key is stored locally but
    /// the local node is not a publisher of the record.
    ///
    /// This is a _local_ operation. However, it also has the effect that
    /// the record will no longer be periodically re-published, allowing the
    /// record to eventually expire throughout the DHT.
    pub fn remove_record(&mut self, key: &record::Key) {
        if let Some(r) = self.store.get(key) {
            if r.publisher.as_ref() == Some(self.kbuckets.local_key().preimage()) {
                self.store.remove(key)
            }
        }
    }

    /// Gets a mutable reference to the record store.
    pub fn store_mut(&mut self) -> &mut TStore {
        &mut self.store
    }

    /// Bootstraps the local node to join the DHT.
    ///
    /// Bootstrapping is a multi-step operation that starts with a lookup of the local node's
    /// own ID in the DHT. This introduces the local node to the other nodes
    /// in the DHT and populates its routing table with the closest neighbours.
    ///
    /// Subsequently, all buckets farther from the bucket of the closest neighbour are
    /// refreshed by initiating an additional bootstrapping query for each such
    /// bucket with random keys.
    ///
    /// Returns `Ok` if bootstrapping has been initiated with a self-lookup, providing the
    /// `QueryId` for the entire bootstrapping process. The progress of bootstrapping is
    /// reported via [`KademliaEvent::QueryResult{QueryResult::Bootstrap}`] events,
    /// with one such event per bootstrapping query.
    ///
    /// Returns `Err` if bootstrapping is impossible due an empty routing table.
    ///
    /// > **Note**: Bootstrapping requires at least one node of the DHT to be known.
    /// > See [`Kademlia::add_address`].
    pub fn bootstrap(&mut self) -> Result<QueryId, NoKnownPeers> {
        let local_key = self.kbuckets.local_key().clone();
        let info = QueryInfo::Bootstrap {
            peer: *local_key.preimage(),
            remaining: None
        };
        let peers = self.kbuckets.closest_keys(&local_key).collect::<Vec<_>>();
        if peers.is_empty() {
            Err(NoKnownPeers())
        } else {
            let inner = QueryInner::new(info);
            Ok(self.queries.add_iter_closest(local_key, peers, inner))
        }
    }

    /// Establishes the local node as a provider of a value for the given key.
    ///
    /// This operation publishes a provider record with the given key and
    /// identity of the local node to the peers closest to the key, thus establishing
    /// the local node as a provider.
    ///
    /// Returns `Ok` if a provider record has been stored locally, providing the
    /// `QueryId` of the initial query that announces the local node as a provider.
    ///
    /// The publication of the provider records is periodically repeated as per the
    /// configured interval, to renew the expiry and account for changes to the DHT
    /// topology. A provider record may be removed from local storage and
    /// thus no longer re-published by calling [`Kademlia::stop_providing`].
    ///
    /// In contrast to the standard Kademlia push-based model for content distribution
    /// implemented by [`Kademlia::put_record`], the provider API implements a
    /// pull-based model that may be used in addition or as an alternative.
    /// The means by which the actual value is obtained from a provider is out of scope
    /// of the libp2p Kademlia provider API.
    ///
    /// The results of the (repeated) provider announcements sent by this node are
    /// reported via [`KademliaEvent::QueryResult{QueryResult::StartProviding}`].
    pub fn start_providing(&mut self, key: record::Key) -> Result<QueryId, store::Error> {
        // Note: We store our own provider records locally without local addresses
        // to avoid redundant storage and outdated addresses. Instead these are
        // acquired on demand when returning a `ProviderRecord` for the local node.
        let local_addrs = Vec::new();
        let record = ProviderRecord::new(
            key.clone(),
            *self.kbuckets.local_key().preimage(),
            local_addrs);
        self.store.add_provider(record)?;
        let target = kbucket::Key::new(key.clone());
        let peers = self.kbuckets.closest_keys(&target);
        let context = AddProviderContext::Publish;
        let info = QueryInfo::AddProvider {
            context,
            key,
            phase: AddProviderPhase::GetClosestPeers
        };
        let inner = QueryInner::new(info);
        let id = self.queries.add_iter_closest(target.clone(), peers, inner);
        Ok(id)
    }

    /// Stops the local node from announcing that it is a provider for the given key.
    ///
    /// This is a local operation. The local node will still be considered as a
    /// provider for the key by other nodes until these provider records expire.
    pub fn stop_providing(&mut self, key: &record::Key) {
        self.store.remove_provider(key, self.kbuckets.local_key().preimage());
    }

    /// Performs a lookup for providers of a value to the given key.
    ///
    /// The result of this operation is delivered in a
    /// reported via [`KademliaEvent::QueryResult{QueryResult::GetProviders}`].
    pub fn get_providers(&mut self, key: record::Key) -> QueryId {
        let info = QueryInfo::GetProviders {
            key: key.clone(),
            providers: HashSet::new(),
        };
        let target = kbucket::Key::new(key);
        let peers = self.kbuckets.closest_keys(&target);
        let inner = QueryInner::new(info);
        self.queries.add_iter_closest(target.clone(), peers, inner)
    }

    /// Processes discovered peers from a successful request in an iterative `Query`.
    fn discovered<'a, I>(&'a mut self, query_id: &QueryId, source: &PeerId, peers: I)
    where
        I: Iterator<Item = &'a KadPeer> + Clone
    {
        let local_id = self.kbuckets.local_key().preimage();
        let others_iter = peers.filter(|p| &p.node_id != local_id);
        if let Some(query) = self.queries.get_mut(query_id) {
            log::trace!("Request to {:?} in query {:?} succeeded.", source, query_id);
            for peer in others_iter.clone() {
                log::trace!("Peer {:?} reported by {:?} in query {:?}.",
                            peer, source, query_id);
                let addrs = peer.multiaddrs.iter().cloned().collect();
                query.inner.addresses.insert(peer.node_id, addrs);
            }
            query.on_success(source, others_iter.cloned().map(|kp| kp.node_id))
        }
    }

    /// Finds the closest peers to a `target` in the context of a request by
    /// the `source` peer, such that the `source` peer is never included in the
    /// result.
    fn find_closest<T: Clone>(&mut self, target: &kbucket::Key<T>, source: &PeerId) -> Vec<KadPeer> {
        if target == self.kbuckets.local_key() {
            Vec::new()
        } else {
            self.kbuckets
                .closest(target)
                .filter(|e| e.node.key.preimage() != source)
                .take(self.queries.config().replication_factor.get())
                .map(KadPeer::from)
                .collect()
        }
    }

    /// Collects all peers who are known to be providers of the value for a given `Multihash`.
    fn provider_peers(&mut self, key: &record::Key, source: &PeerId) -> Vec<KadPeer> {
        let kbuckets = &mut self.kbuckets;
        let connected = &mut self.connected_peers;
        let local_addrs = &self.local_addrs;
        self.store.providers(key)
            .into_iter()
            .filter_map(move |p|
                if &p.provider != source {
                    let node_id = p.provider;
                    let multiaddrs = p.addresses;
                    let connection_ty = if connected.contains(&node_id) {
                        KadConnectionType::Connected
                    } else {
                        KadConnectionType::NotConnected
                    };
                    if multiaddrs.is_empty() {
                        // The provider is either the local node and we fill in
                        // the local addresses on demand, or it is a legacy
                        // provider record without addresses, in which case we
                        // try to find addresses in the routing table, as was
                        // done before provider records were stored along with
                        // their addresses.
                        if &node_id == kbuckets.local_key().preimage() {
                            Some(local_addrs.iter().cloned().collect::<Vec<_>>())
                        } else {
                            let key = kbucket::Key::from(node_id);
                            kbuckets.entry(&key).view().map(|e| e.node.value.clone().into_vec())
                        }
                    } else {
                        Some(multiaddrs)
                    }
                    .map(|multiaddrs| {
                        KadPeer {
                            node_id,
                            multiaddrs,
                            connection_ty,
                        }
                    })
                } else {
                    None
                })
            .take(self.queries.config().replication_factor.get())
            .collect()
    }

    /// Starts an iterative `ADD_PROVIDER` query for the given key.
    fn start_add_provider(&mut self, key: record::Key, context: AddProviderContext) {
        let info = QueryInfo::AddProvider {
            context,
            key: key.clone(),
            phase: AddProviderPhase::GetClosestPeers
        };
        let target = kbucket::Key::new(key);
        let peers = self.kbuckets.closest_keys(&target);
        let inner = QueryInner::new(info);
        self.queries.add_iter_closest(target.clone(), peers, inner);
    }

    /// Starts an iterative `PUT_VALUE` query for the given record.
    fn start_put_record(&mut self, record: Record, quorum: Quorum, context: PutRecordContext) {
        let quorum = quorum.eval(self.queries.config().replication_factor);
        let target = kbucket::Key::new(record.key.clone());
        let peers = self.kbuckets.closest_keys(&target);
        let info = QueryInfo::PutRecord {
            record, quorum, context, phase: PutRecordPhase::GetClosestPeers
        };
        let inner = QueryInner::new(info);
        self.queries.add_iter_closest(target.clone(), peers, inner);
    }

    /// Updates the routing table with a new connection status and address of a peer.
    fn connection_updated(&mut self, peer: PeerId, address: Option<Multiaddr>, new_status: NodeStatus) {
        let key = kbucket::Key::from(peer);
        match self.kbuckets.entry(&key) {
            kbucket::Entry::Present(mut entry, old_status) => {
                if let Some(address) = address {
                    if entry.value().insert(address) {
                        self.queued_events.push_back(NetworkBehaviourAction::GenerateEvent(
                            KademliaEvent::RoutingUpdated {
                                peer,
                                addresses: entry.value().clone(),
                                old_peer: None,
                            }
                        ))
                    }
                }
                if old_status != new_status {
                    entry.update(new_status);
                }
            },

            kbucket::Entry::Pending(mut entry, old_status) => {
                if let Some(address) = address {
                    entry.value().insert(address);
                }
                if old_status != new_status {
                    entry.update(new_status);
                }
            },

            kbucket::Entry::Absent(entry) => {
                // Only connected nodes with a known address are newly inserted.
                if new_status != NodeStatus::Connected {
                    return
                }
                match (address, self.kbucket_inserts) {
                    (None, _) => {
                        self.queued_events.push_back(NetworkBehaviourAction::GenerateEvent(
                            KademliaEvent::UnroutablePeer { peer }
                        ));
                    }
                    (Some(a), KademliaBucketInserts::Manual) => {
                        self.queued_events.push_back(NetworkBehaviourAction::GenerateEvent(
                            KademliaEvent::RoutablePeer { peer, address: a }
                        ));
                    }
                    (Some(a), KademliaBucketInserts::OnConnected) => {
                        let addresses = Addresses::new(a);
                        match entry.insert(addresses.clone(), new_status) {
                            kbucket::InsertResult::Inserted => {
                                let event = KademliaEvent::RoutingUpdated {
                                    peer,
                                    addresses,
                                    old_peer: None,
                                };
                                self.queued_events.push_back(
                                    NetworkBehaviourAction::GenerateEvent(event));
                            },
                            kbucket::InsertResult::Full => {
                                debug!("Bucket full. Peer not added to routing table: {}", peer);
                                let address = addresses.first().clone();
                                self.queued_events.push_back(NetworkBehaviourAction::GenerateEvent(
                                    KademliaEvent::RoutablePeer { peer, address }
                                ));
                            },
                            kbucket::InsertResult::Pending { disconnected } => {
                                debug_assert!(!self.connected_peers.contains(disconnected.preimage()));
                                let address = addresses.first().clone();
                                self.queued_events.push_back(NetworkBehaviourAction::GenerateEvent(
                                    KademliaEvent::PendingRoutablePeer { peer, address }
                                ));
                                self.queued_events.push_back(NetworkBehaviourAction::DialPeer {
                                    peer_id: disconnected.into_preimage(),
                                    condition: DialPeerCondition::Disconnected
                                })
                            },
                        }
                    }
                }
            },
            _ => {}
        }
    }

    /// Handles a finished (i.e. successful) query.
    fn query_finished(&mut self, q: Query<QueryInner>, params: &mut impl PollParameters)
        -> Option<KademliaEvent>
    {
        let query_id = q.id();
        log::trace!("Query {:?} finished.", query_id);
        let result = q.into_result();
        match result.inner.info {
            QueryInfo::Bootstrap { peer, remaining } => {
                let local_key = self.kbuckets.local_key().clone();
                let mut remaining = remaining.unwrap_or_else(|| {
                    debug_assert_eq!(&peer, local_key.preimage());
                    // The lookup for the local key finished. To complete the bootstrap process,
                    // a bucket refresh should be performed for every bucket farther away than
                    // the first non-empty bucket (which are most likely no more than the last
                    // few, i.e. farthest, buckets).
                    self.kbuckets.iter()
                        .skip_while(|b| b.is_empty())
                        .skip(1) // Skip the bucket with the closest neighbour.
                        .map(|b| {
                            // Try to find a key that falls into the bucket. While such keys can
                            // be generated fully deterministically, the current libp2p kademlia
                            // wire protocol requires transmission of the preimages of the actual
                            // keys in the DHT keyspace, hence for now this is just a "best effort"
                            // to find a key that hashes into a specific bucket. The probabilities
                            // of finding a key in the bucket `b` with as most 16 trials are as
                            // follows:
                            //
                            // Pr(bucket-255) = 1 - (1/2)^16   ~= 1
                            // Pr(bucket-254) = 1 - (3/4)^16   ~= 1
                            // Pr(bucket-253) = 1 - (7/8)^16   ~= 0.88
                            // Pr(bucket-252) = 1 - (15/16)^16 ~= 0.64
                            // ...
                            let mut target = kbucket::Key::from(PeerId::random());
                            for _ in 0 .. 16 {
                                let d = local_key.distance(&target);
                                if b.contains(&d) {
                                    break;
                                }
                                target = kbucket::Key::from(PeerId::random());
                            }
                            target
                        }).collect::<Vec<_>>().into_iter()
                });

                let num_remaining = remaining.len().saturating_sub(1) as u32;

                if let Some(target) = remaining.next() {
                    let info = QueryInfo::Bootstrap {
                        peer: target.clone().into_preimage(),
                        remaining: Some(remaining)
                    };
                    let peers = self.kbuckets.closest_keys(&target);
                    let inner = QueryInner::new(info);
                    self.queries.continue_iter_closest(query_id, target.clone(), peers, inner);
                }

                Some(KademliaEvent::QueryResult {
                    id: query_id,
                    stats: result.stats,
                    result: QueryResult::Bootstrap(Ok(BootstrapOk { peer, num_remaining }))
                })
            }

            QueryInfo::GetClosestPeers { key, .. } => {
                Some(KademliaEvent::QueryResult {
                    id: query_id,
                    stats: result.stats,
                    result: QueryResult::GetClosestPeers(Ok(
                        GetClosestPeersOk { key, peers: result.peers.collect() }
                    ))
                })
            }

            QueryInfo::GetProviders { key, providers } => {
                Some(KademliaEvent::QueryResult {
                    id: query_id,
                    stats: result.stats,
                    result: QueryResult::GetProviders(Ok(
                        GetProvidersOk {
                            key,
                            providers,
                            closest_peers: result.peers.collect()
                        }
                    ))
                })
            }

            QueryInfo::AddProvider {
                context,
                key,
                phase: AddProviderPhase::GetClosestPeers
            } => {
                let provider_id = *params.local_peer_id();
                let external_addresses = params.external_addresses().map(|r| r.addr).collect();
                let inner = QueryInner::new(QueryInfo::AddProvider {
                    context,
                    key,
                    phase: AddProviderPhase::AddProvider {
                        provider_id,
                        external_addresses,
                        get_closest_peers_stats: result.stats
                    }
                });
                self.queries.continue_fixed(query_id, result.peers, inner);
                None
            }

            QueryInfo::AddProvider {
                context,
                key,
                phase: AddProviderPhase::AddProvider { get_closest_peers_stats, .. }
            } => {
                match context {
                    AddProviderContext::Publish => {
                        Some(KademliaEvent::QueryResult {
                            id: query_id,
                            stats: get_closest_peers_stats.merge(result.stats),
                            result: QueryResult::StartProviding(Ok(AddProviderOk { key }))
                        })
                    }
                    AddProviderContext::Republish => {
                        Some(KademliaEvent::QueryResult {
                            id: query_id,
                            stats: get_closest_peers_stats.merge(result.stats),
                            result: QueryResult::RepublishProvider(Ok(AddProviderOk { key }))
                        })
                    }
                }
            }

            QueryInfo::GetRecord { key, records, quorum, cache_candidates } => {
                let results = if records.len() >= quorum.get() { // [not empty]
                    if quorum.get() == 1 && !cache_candidates.is_empty() {
                        // Cache the record at the closest node(s) to the key that
                        // did not return the record.
                        let record = records.first().expect("[not empty]").record.clone();
                        let quorum = NonZeroUsize::new(1).expect("1 > 0");
                        let context = PutRecordContext::Cache;
                        let info = QueryInfo::PutRecord {
                            context,
                            record,
                            quorum,
                            phase: PutRecordPhase::PutRecord {
                                success: vec![],
                                get_closest_peers_stats: QueryStats::empty()
                            }
                        };
                        let inner = QueryInner::new(info);
                        self.queries.add_fixed(cache_candidates.values().copied(), inner);
                    }
                    Ok(GetRecordOk { records, cache_candidates })
                } else if records.is_empty() {
                    Err(GetRecordError::NotFound {
                        key,
                        closest_peers: result.peers.collect()
                    })
                } else {
                    Err(GetRecordError::QuorumFailed { key, records, quorum })
                };
                Some(KademliaEvent::QueryResult {
                    id: query_id,
                    stats: result.stats,
                    result: QueryResult::GetRecord(results)
                })
            }

            QueryInfo::PutRecord {
                context,
                record,
                quorum,
                phase: PutRecordPhase::GetClosestPeers
            } => {
                let info = QueryInfo::PutRecord {
                    context,
                    record,
                    quorum,
                    phase: PutRecordPhase::PutRecord {
                        success: vec![],
                        get_closest_peers_stats: result.stats
                    }
                };
                let inner = QueryInner::new(info);
                self.queries.continue_fixed(query_id, result.peers, inner);
                None
            }

            QueryInfo::PutRecord {
                context,
                record,
                quorum,
                phase: PutRecordPhase::PutRecord { success, get_closest_peers_stats }
            } => {
                let mk_result = |key: record::Key| {
                    if success.len() >= quorum.get() {
                        Ok(PutRecordOk { key })
                    } else {
                        Err(PutRecordError::QuorumFailed { key, quorum, success })
                    }
                };
                match context {
                    PutRecordContext::Publish | PutRecordContext::Custom =>
                        Some(KademliaEvent::QueryResult {
                            id: query_id,
                            stats: get_closest_peers_stats.merge(result.stats),
                            result: QueryResult::PutRecord(mk_result(record.key))
                        }),
                    PutRecordContext::Republish =>
                        Some(KademliaEvent::QueryResult {
                            id: query_id,
                            stats: get_closest_peers_stats.merge(result.stats),
                            result: QueryResult::RepublishRecord(mk_result(record.key))
                        }),
                    PutRecordContext::Replicate => {
                        debug!("Record replicated: {:?}", record.key);
                        None
                    }
                    PutRecordContext::Cache => {
                        debug!("Record cached: {:?}", record.key);
                        None
                    }
                }
            }
        }
    }

    /// Handles a query that timed out.
    fn query_timeout(&mut self, query: Query<QueryInner>) -> Option<KademliaEvent> {
        let query_id = query.id();
        log::trace!("Query {:?} timed out.", query_id);
        let result = query.into_result();
        match result.inner.info {
            QueryInfo::Bootstrap { peer, mut remaining } => {
                let num_remaining = remaining.as_ref().map(|r| r.len().saturating_sub(1) as u32);

                if let Some(mut remaining) = remaining.take() {
                    // Continue with the next bootstrap query if `remaining` is not empty.
                    if let Some(target) = remaining.next() {
                        let info = QueryInfo::Bootstrap {
                            peer: target.clone().into_preimage(),
                            remaining: Some(remaining)
                        };
                        let peers = self.kbuckets.closest_keys(&target);
                        let inner = QueryInner::new(info);
                        self.queries.continue_iter_closest(query_id, target.clone(), peers, inner);
                    }
                }

                Some(KademliaEvent::QueryResult {
                    id: query_id,
                    stats: result.stats,
                    result: QueryResult::Bootstrap(Err(
                        BootstrapError::Timeout { peer, num_remaining }
                    ))
                })
            }

            QueryInfo::AddProvider { context, key, .. } =>
                Some(match context {
                    AddProviderContext::Publish =>
                        KademliaEvent::QueryResult {
                            id: query_id,
                            stats: result.stats,
                            result: QueryResult::StartProviding(Err(
                                AddProviderError::Timeout { key }
                            ))
                        },
                    AddProviderContext::Republish =>
                        KademliaEvent::QueryResult {
                            id: query_id,
                            stats: result.stats,
                            result: QueryResult::RepublishProvider(Err(
                                AddProviderError::Timeout { key }
                            ))
                        }
                }),

            QueryInfo::GetClosestPeers { key } => {
                Some(KademliaEvent::QueryResult {
                    id: query_id,
                    stats: result.stats,
                    result: QueryResult::GetClosestPeers(Err(
                        GetClosestPeersError::Timeout {
                            key,
                            peers: result.peers.collect()
                        }
                    ))
                })
            },

            QueryInfo::PutRecord { record, quorum, context, phase } => {
                let err = Err(PutRecordError::Timeout {
                    key: record.key,
                    quorum,
                    success: match phase {
                        PutRecordPhase::GetClosestPeers => vec![],
                        PutRecordPhase::PutRecord { ref success, .. } => success.clone(),
                    }
                });
                match context {
                    PutRecordContext::Publish | PutRecordContext::Custom =>
                        Some(KademliaEvent::QueryResult {
                            id: query_id,
                            stats: result.stats,
                            result: QueryResult::PutRecord(err)
                        }),
                    PutRecordContext::Republish =>
                        Some(KademliaEvent::QueryResult {
                            id: query_id,
                            stats: result.stats,
                            result: QueryResult::RepublishRecord(err)
                        }),
                    PutRecordContext::Replicate => match phase {
                        PutRecordPhase::GetClosestPeers => {
                            warn!("Locating closest peers for replication failed: {:?}", err);
                            None
                        }
                        PutRecordPhase::PutRecord { .. } => {
                            debug!("Replicating record failed: {:?}", err);
                            None
                        }
                    }
                    PutRecordContext::Cache => match phase {
                        PutRecordPhase::GetClosestPeers => {
                            // Caching a record at the closest peer to a key that did not return
                            // a record is never preceded by a lookup for the closest peers, i.e.
                            // it is a direct query to a single peer.
                            unreachable!()
                        }
                        PutRecordPhase::PutRecord { .. } => {
                            debug!("Caching record failed: {:?}", err);
                            None
                        }
                    }
                }
            }

            QueryInfo::GetRecord { key, records, quorum, .. } =>
                Some(KademliaEvent::QueryResult {
                    id: query_id,
                    stats: result.stats,
                    result: QueryResult::GetRecord(Err(
                        GetRecordError::Timeout { key, records, quorum },
                    ))
                }),

            QueryInfo::GetProviders { key, providers } =>
                Some(KademliaEvent::QueryResult {
                    id: query_id,
                    stats: result.stats,
                    result: QueryResult::GetProviders(Err(
                        GetProvidersError::Timeout {
                            key,
                            providers,
                            closest_peers: result.peers.collect()
                        }
                    ))
                })
            }
    }

    /// Processes a record received from a peer.
    fn record_received(
        &mut self,
        source: PeerId,
        connection: ConnectionId,
        request_id: KademliaRequestId,
        mut record: Record
    ) {
        if record.publisher.as_ref() == Some(self.kbuckets.local_key().preimage()) {
            // If the (alleged) publisher is the local node, do nothing. The record of
            // the original publisher should never change as a result of replication
            // and the publisher is always assumed to have the "right" value.
            self.queued_events.push_back(NetworkBehaviourAction::NotifyHandler {
                peer_id: source,
                handler: NotifyHandler::One(connection),
                event: KademliaHandlerIn::PutRecordRes {
                    key: record.key,
                    value: record.value,
                    request_id,
                },
            });
            return
        }

        let now = Instant::now();

        // Calculate the expiration exponentially inversely proportional to the
        // number of nodes between the local node and the closest node to the key
        // (beyond the replication factor). This ensures avoiding over-caching
        // outside of the k closest nodes to a key.
        let target = kbucket::Key::new(record.key.clone());
        let num_between = self.kbuckets.count_nodes_between(&target);
        let k = self.queries.config().replication_factor.get();
        let num_beyond_k = (usize::max(k, num_between) - k) as u32;
        let expiration = self.record_ttl.map(|ttl| now + exp_decrease(ttl, num_beyond_k));
        // The smaller TTL prevails. Only if neither TTL is set is the record
        // stored "forever".
        record.expires = record.expires.or(expiration).min(expiration);

        if let Some(job) = self.put_record_job.as_mut() {
            // Ignore the record in the next run of the replication
            // job, since we can assume the sender replicated the
            // record to the k closest peers. Effectively, only
            // one of the k closest peers performs a replication
            // in the configured interval, assuming a shared interval.
            job.skip(record.key.clone())
        }

        // While records received from a publisher, as well as records that do
        // not exist locally should always (attempted to) be stored, there is a
        // choice here w.r.t. the handling of replicated records whose keys refer
        // to records that exist locally: The value and / or the publisher may
        // either be overridden or left unchanged. At the moment and in the
        // absence of a decisive argument for another option, both are always
        // overridden as it avoids having to load the existing record in the
        // first place.

        if !record.is_expired(now) {
            // The record is cloned because of the weird libp2p protocol
            // requirement to send back the value in the response, although this
            // is a waste of resources.
            match self.store.put(record.clone()) {
                Ok(()) => debug!("Record stored: {:?}; {} bytes", record.key, record.value.len()),
                Err(e) => {
                    info!("Record not stored: {:?}", e);
                    self.queued_events.push_back(NetworkBehaviourAction::NotifyHandler {
                        peer_id: source,
                        handler: NotifyHandler::One(connection),
                        event: KademliaHandlerIn::Reset(request_id)
                    });

                    return
                }
            }
        }

        // The remote receives a [`KademliaHandlerIn::PutRecordRes`] even in the
        // case where the record is discarded due to being expired. Given that
        // the remote sent the local node a [`KademliaHandlerEvent::PutRecord`]
        // request, the remote perceives the local node as one node among the k
        // closest nodes to the target. In addition returning
        // [`KademliaHandlerIn::PutRecordRes`] does not reveal any internal
        // information to a possibly malicious remote node.
        self.queued_events.push_back(NetworkBehaviourAction::NotifyHandler {
            peer_id: source,
            handler: NotifyHandler::One(connection),
            event: KademliaHandlerIn::PutRecordRes {
                key: record.key,
                value: record.value,
                request_id,
            },
        })
    }

    /// Processes a provider record received from a peer.
    fn provider_received(&mut self, key: record::Key, provider: KadPeer) {
        if &provider.node_id != self.kbuckets.local_key().preimage() {
            let record = ProviderRecord {
                key,
                provider: provider.node_id,
                expires: self.provider_record_ttl.map(|ttl| Instant::now() + ttl),
                addresses: provider.multiaddrs,
            };
            if let Err(e) = self.store.add_provider(record) {
                info!("Provider record not stored: {:?}", e);
            }
        }
    }
}

/// Exponentially decrease the given duration (base 2).
fn exp_decrease(ttl: Duration, exp: u32) -> Duration {
    Duration::from_secs(ttl.as_secs().checked_shr(exp).unwrap_or(0))
}

impl<TStore> NetworkBehaviour for Kademlia<TStore>
where
    for<'a> TStore: RecordStore<'a>,
    TStore: Send + 'static,
{
    type ProtocolsHandler = KademliaHandlerProto<QueryId>;
    type OutEvent = KademliaEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        KademliaHandlerProto::new(KademliaHandlerConfig {
            protocol_config: self.protocol_config.clone(),
            allow_listening: true,
            idle_timeout: self.connection_idle_timeout,
        })
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        // We should order addresses from decreasing likelyhood of connectivity, so start with
        // the addresses of that peer in the k-buckets.
        let key = kbucket::Key::from(*peer_id);
        let mut peer_addrs =
            if let kbucket::Entry::Present(mut entry, _) = self.kbuckets.entry(&key) {
                let addrs = entry.value().iter().cloned().collect::<Vec<_>>();
                debug_assert!(!addrs.is_empty(), "Empty peer addresses in routing table.");
                addrs
            } else {
                Vec::new()
            };

        // We add to that a temporary list of addresses from the ongoing queries.
        for query in self.queries.iter() {
            if let Some(addrs) = query.inner.addresses.get(peer_id) {
                peer_addrs.extend(addrs.iter().cloned())
            }
        }

        peer_addrs
    }

    fn inject_connection_established(&mut self, _: &PeerId, _: &ConnectionId, _: &ConnectedPoint) {
        // When a connection is established, we don't know yet whether the
        // remote supports the configured protocol name. Only once a connection
        // handler reports [`KademliaHandlerEvent::ProtocolConfirmed`] do we
        // update the local routing table.
    }

    fn inject_connected(&mut self, peer: &PeerId) {
        // Queue events for sending pending RPCs to the connected peer.
        // There can be only one pending RPC for a particular peer and query per definition.
        for (peer_id, event) in self.queries.iter_mut().filter_map(|q|
            q.inner.pending_rpcs.iter()
                .position(|(p, _)| p == peer)
                .map(|p| q.inner.pending_rpcs.remove(p)))
        {
            self.queued_events.push_back(NetworkBehaviourAction::NotifyHandler {
                peer_id, event, handler: NotifyHandler::Any
            });
        }

        self.connected_peers.insert(*peer);
    }

    fn inject_address_change(
        &mut self,
        peer: &PeerId,
        _: &ConnectionId,
        old: &ConnectedPoint,
        new: &ConnectedPoint
    ) {
        let (old, new) = (old.get_remote_address(), new.get_remote_address());

        // Update routing table.
        if let Some(addrs) = self.kbuckets.entry(&kbucket::Key::from(*peer)).value() {
            if addrs.replace(old, new) {
                debug!("Address '{}' replaced with '{}' for peer '{}'.", old, new, peer);
            } else {
                debug!(
                    "Address '{}' not replaced with '{}' for peer '{}' as old address wasn't \
                     present.",
                    old, new, peer,
                );
            }
        } else {
            debug!(
                "Address '{}' not replaced with '{}' for peer '{}' as peer is not present in the \
                 routing table.",
                old, new, peer,
            );
        }

        // Update query address cache.
        //
        // Given two connected nodes: local node A and remote node B. Say node B
        // is not in node A's routing table. Additionally node B is part of the
        // `QueryInner::addresses` list of an ongoing query on node A. Say Node
        // B triggers an address change and then disconnects. Later on the
        // earlier mentioned query on node A would like to connect to node B.
        // Without replacing the address in the `QueryInner::addresses` set node
        // A would attempt to dial the old and not the new address.
        //
        // While upholding correctness, iterating through all discovered
        // addresses of a peer in all currently ongoing queries might have a
        // large performance impact. If so, the code below might be worth
        // revisiting.
        for query in self.queries.iter_mut() {
            if let Some(addrs) = query.inner.addresses.get_mut(peer) {
                for addr in addrs.iter_mut() {
                    if addr == old {
                        *addr = new.clone();
                    }
                }
            }
        }
    }

    fn inject_addr_reach_failure(
        &mut self,
        peer_id: Option<&PeerId>,
        addr: &Multiaddr,
        err: &dyn error::Error
    ) {
        if let Some(peer_id) = peer_id {
            let key = kbucket::Key::from(*peer_id);

            if let Some(addrs) = self.kbuckets.entry(&key).value() {
                // TODO: Ideally, the address should only be removed if the error can
                // be classified as "permanent" but since `err` is currently a borrowed
                // trait object without a `'static` bound, even downcasting for inspection
                // of the error is not possible (and also not truly desirable or ergonomic).
                // The error passed in should rather be a dedicated enum.
                if addrs.remove(addr).is_ok() {
                    debug!("Address '{}' removed from peer '{}' due to error: {}.",
                        addr, peer_id, err);
                } else {
                    // Despite apparently having no reachable address (any longer),
                    // the peer is kept in the routing table with the last address to avoid
                    // (temporary) loss of network connectivity to "flush" the routing
                    // table. Once in, a peer is only removed from the routing table
                    // if it is the least recently connected peer, currently disconnected
                    // and is unreachable in the context of another peer pending insertion
                    // into the same bucket. This is handled transparently by the
                    // `KBucketsTable` and takes effect through `KBucketsTable::take_applied_pending`
                    // within `Kademlia::poll`.
                    debug!("Last remaining address '{}' of peer '{}' is unreachable: {}.",
                        addr, peer_id, err)
                }
            }

            for query in self.queries.iter_mut() {
                if let Some(addrs) = query.inner.addresses.get_mut(peer_id) {
                    addrs.retain(|a| a != addr);
                }
            }
        }
    }

    fn inject_dial_failure(&mut self, peer_id: &PeerId) {
        for query in self.queries.iter_mut() {
            query.on_failure(peer_id);
        }
    }

    fn inject_disconnected(&mut self, id: &PeerId) {
        for query in self.queries.iter_mut() {
            query.on_failure(id);
        }
        self.connection_updated(*id, None, NodeStatus::Disconnected);
        self.connected_peers.remove(id);
    }

    fn inject_event(
        &mut self,
        source: PeerId,
        connection: ConnectionId,
        event: KademliaHandlerEvent<QueryId>
    ) {
        match event {
            KademliaHandlerEvent::ProtocolConfirmed { endpoint } => {
                debug_assert!(self.connected_peers.contains(&source));
                // The remote's address can only be put into the routing table,
                // and thus shared with other nodes, if the local node is the dialer,
                // since the remote address on an inbound connection may be specific
                // to that connection (e.g. typically the TCP port numbers).
                let address = match endpoint {
                    ConnectedPoint::Dialer { address } => Some(address),
                    ConnectedPoint::Listener { .. } => None,
                };
                self.connection_updated(source, address, NodeStatus::Connected);
            }

            KademliaHandlerEvent::FindNodeReq { key, request_id } => {
                let closer_peers = self.find_closest(&kbucket::Key::new(key), &source);
                self.queued_events.push_back(NetworkBehaviourAction::NotifyHandler {
                    peer_id: source,
                    handler: NotifyHandler::One(connection),
                    event: KademliaHandlerIn::FindNodeRes {
                        closer_peers,
                        request_id,
                    },
                });
            }

            KademliaHandlerEvent::FindNodeRes {
                closer_peers,
                user_data,
            } => {
                self.discovered(&user_data, &source, closer_peers.iter());
            }

            KademliaHandlerEvent::GetProvidersReq { key, request_id } => {
                let provider_peers = self.provider_peers(&key, &source);
                let closer_peers = self.find_closest(&kbucket::Key::new(key), &source);
                self.queued_events.push_back(NetworkBehaviourAction::NotifyHandler {
                    peer_id: source,
                    handler: NotifyHandler::One(connection),
                    event: KademliaHandlerIn::GetProvidersRes {
                        closer_peers,
                        provider_peers,
                        request_id,
                    },
                });
            }

            KademliaHandlerEvent::GetProvidersRes {
                closer_peers,
                provider_peers,
                user_data,
            } => {
                let peers = closer_peers.iter().chain(provider_peers.iter());
                self.discovered(&user_data, &source, peers);
                if let Some(query) = self.queries.get_mut(&user_data) {
                    if let QueryInfo::GetProviders {
                        providers, ..
                    } = &mut query.inner.info {
                        for peer in provider_peers {
                            providers.insert(peer.node_id);
                        }
                    }
                }
            }

            KademliaHandlerEvent::QueryError { user_data, error } => {
                log::debug!("Request to {:?} in query {:?} failed with {:?}",
                            source, user_data, error);
                // If the query to which the error relates is still active,
                // signal the failure w.r.t. `source`.
                if let Some(query) = self.queries.get_mut(&user_data) {
                    query.on_failure(&source)
                }
            }

            KademliaHandlerEvent::AddProvider { key, provider } => {
                // Only accept a provider record from a legitimate peer.
                if provider.node_id != source {
                    return
                }

                self.provider_received(key, provider)
            }

            KademliaHandlerEvent::GetRecord { key, request_id } => {
                // Lookup the record locally.
                let record = match self.store.get(&key) {
                    Some(record) => {
                        if record.is_expired(Instant::now()) {
                            self.store.remove(&key);
                            None
                        } else {
                            Some(record.into_owned())
                        }
                    },
                    None => None
                };

                let closer_peers = self.find_closest(&kbucket::Key::new(key), &source);

                self.queued_events.push_back(NetworkBehaviourAction::NotifyHandler {
                    peer_id: source,
                    handler: NotifyHandler::One(connection),
                    event: KademliaHandlerIn::GetRecordRes {
                        record,
                        closer_peers,
                        request_id,
                    },
                });
            }

            KademliaHandlerEvent::GetRecordRes {
                record,
                closer_peers,
                user_data,
            } => {
                if let Some(query) = self.queries.get_mut(&user_data) {
                    if let QueryInfo::GetRecord {
                        key, records, quorum, cache_candidates
                    } = &mut query.inner.info {
                        if let Some(record) = record {
                            records.push(PeerRecord{ peer: Some(source), record });

                            let quorum = quorum.get();
                            if records.len() >= quorum {
                                // Desired quorum reached. The query may finish. See
                                // [`Query::try_finish`] for details.
                                let peers = records.iter()
                                    .filter_map(|PeerRecord{ peer, .. }| peer.as_ref())
                                    .cloned()
                                    .collect::<Vec<_>>();
                                let finished = query.try_finish(peers.iter());
                                if !finished {
                                    debug!(
                                        "GetRecord query ({:?}) reached quorum ({}/{}) with \
                                         response from peer {} but could not yet finish.",
                                        user_data, peers.len(), quorum, source,
                                    );
                                }
                            }
                        } else {
                            log::trace!("Record with key {:?} not found at {}", key, source);
                            if let KademliaCaching::Enabled { max_peers } = self.caching {
                                let source_key = kbucket::Key::from(source);
                                let target_key = kbucket::Key::from(key.clone());
                                let distance = source_key.distance(&target_key);
                                cache_candidates.insert(distance, source);
                                if cache_candidates.len() > max_peers as usize {
                                    // TODO: `pop_last()` would be nice once stabilised.
                                    // See https://github.com/rust-lang/rust/issues/62924.
                                    let last = *cache_candidates.keys().next_back().expect("len > 0");
                                    cache_candidates.remove(&last);
                                }
                            }
                        }
                    }
                }

                self.discovered(&user_data, &source, closer_peers.iter());
            }

            KademliaHandlerEvent::PutRecord {
                record,
                request_id
            } => {
                self.record_received(source, connection, request_id, record);
            }

            KademliaHandlerEvent::PutRecordRes {
                user_data, ..
            } => {
                if let Some(query) = self.queries.get_mut(&user_data) {
                    query.on_success(&source, vec![]);
                    if let QueryInfo::PutRecord {
                        phase: PutRecordPhase::PutRecord { success, .. }, quorum, ..
                    } = &mut query.inner.info {
                        success.push(source);

                        let quorum = quorum.get();
                        if success.len() >= quorum {
                            let peers = success.clone();
                            let finished = query.try_finish(peers.iter());
                            if !finished {
                                debug!(
                                    "PutRecord query ({:?}) reached quorum ({}/{}) with response \
                                     from peer {} but could not yet finish.",
                                    user_data, peers.len(), quorum, source,
                                );
                            }
                        }
                    }
                }
            }
        };
    }

    fn inject_new_listen_addr(&mut self, _id: ListenerId, addr: &Multiaddr) {
        self.local_addrs.insert(addr.clone());
    }

    fn inject_expired_listen_addr(&mut self, _id: ListenerId, addr: &Multiaddr) {
        self.local_addrs.remove(addr);
    }

    fn inject_new_external_addr(&mut self, addr: &Multiaddr) {
        if self.local_addrs.len() < MAX_LOCAL_EXTERNAL_ADDRS {
            self.local_addrs.insert(addr.clone());
        }
    }

    fn poll(&mut self, cx: &mut Context<'_>, parameters: &mut impl PollParameters) -> Poll<
        NetworkBehaviourAction<
            KademliaHandlerIn<QueryId>,
            Self::OutEvent,
        >,
    > {
        let now = Instant::now();

        // Calculate the available capacity for queries triggered by background jobs.
        let mut jobs_query_capacity = JOBS_MAX_QUERIES.saturating_sub(self.queries.size());

        // Run the periodic provider announcement job.
        if let Some(mut job) = self.add_provider_job.take() {
            let num = usize::min(JOBS_MAX_NEW_QUERIES, jobs_query_capacity);
            for _ in 0 .. num {
                if let Poll::Ready(r) = job.poll(cx, &mut self.store, now) {
                    self.start_add_provider(r.key, AddProviderContext::Republish)
                } else {
                    break
                }
            }
            jobs_query_capacity -= num;
            self.add_provider_job = Some(job);
        }

        // Run the periodic record replication / publication job.
        if let Some(mut job) = self.put_record_job.take() {
            let num = usize::min(JOBS_MAX_NEW_QUERIES, jobs_query_capacity);
            for _ in 0 .. num {
                if let Poll::Ready(r) = job.poll(cx, &mut self.store, now) {
                    let context = if r.publisher.as_ref() == Some(self.kbuckets.local_key().preimage()) {
                        PutRecordContext::Republish
                    } else {
                        PutRecordContext::Replicate
                    };
                    self.start_put_record(r, Quorum::All, context)
                } else {
                    break
                }
            }
            self.put_record_job = Some(job);
        }

        loop {
            // Drain queued events first.
            if let Some(event) = self.queued_events.pop_front() {
                return Poll::Ready(event);
            }

            // Drain applied pending entries from the routing table.
            if let Some(entry) = self.kbuckets.take_applied_pending() {
                let kbucket::Node { key, value } = entry.inserted;
                let event = KademliaEvent::RoutingUpdated {
                    peer: key.into_preimage(),
                    addresses: value,
                    old_peer: entry.evicted.map(|n| n.key.into_preimage())
                };
                return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event))
            }

            // Look for a finished query.
            loop {
                match self.queries.poll(now) {
                    QueryPoolState::Finished(q) => {
                        if let Some(event) = self.query_finished(q, parameters) {
                            return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event))
                        }
                    }
                    QueryPoolState::Timeout(q) => {
                        if let Some(event) = self.query_timeout(q) {
                            return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event))
                        }
                    }
                    QueryPoolState::Waiting(Some((query, peer_id))) => {
                        let event = query.inner.info.to_request(query.id());
                        // TODO: AddProvider requests yield no response, so the query completes
                        // as soon as all requests have been sent. However, the handler should
                        // better emit an event when the request has been sent (and report
                        // an error if sending fails), instead of immediately reporting
                        // "success" somewhat prematurely here.
                        if let QueryInfo::AddProvider {
                            phase: AddProviderPhase::AddProvider { .. },
                            ..
                        } = &query.inner.info {
                            query.on_success(&peer_id, vec![])
                        }
                        if self.connected_peers.contains(&peer_id) {
                            self.queued_events.push_back(NetworkBehaviourAction::NotifyHandler {
                                peer_id, event, handler: NotifyHandler::Any
                            });
                        } else if &peer_id != self.kbuckets.local_key().preimage() {
                            query.inner.pending_rpcs.push((peer_id, event));
                            self.queued_events.push_back(NetworkBehaviourAction::DialPeer {
                                peer_id, condition: DialPeerCondition::Disconnected
                            });
                        }
                    }
                    QueryPoolState::Waiting(None) | QueryPoolState::Idle => break,
                }
            }

            // No immediate event was produced as a result of a finished query.
            // If no new events have been queued either, signal `NotReady` to
            // be polled again later.
            if self.queued_events.is_empty() {
                return Poll::Pending
            }
        }
    }
}

/// A quorum w.r.t. the configured replication factor specifies the minimum
/// number of distinct nodes that must be successfully contacted in order
/// for a query to succeed.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Quorum {
    One,
    Majority,
    All,
    N(NonZeroUsize)
}

impl Quorum {
    /// Evaluate the quorum w.r.t a given total (number of peers).
    fn eval(&self, total: NonZeroUsize) -> NonZeroUsize {
        match self {
            Quorum::One => NonZeroUsize::new(1).expect("1 != 0"),
            Quorum::Majority => NonZeroUsize::new(total.get() / 2 + 1).expect("n + 1 != 0"),
            Quorum::All => total,
            Quorum::N(n) => NonZeroUsize::min(total, *n)
        }
    }
}

/// A record either received by the given peer or retrieved from the local
/// record store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerRecord {
    /// The peer from whom the record was received. `None` if the record was
    /// retrieved from local storage.
    pub peer: Option<PeerId>,
    pub record: Record,
}

//////////////////////////////////////////////////////////////////////////////
// Events

/// The events produced by the `Kademlia` behaviour.
///
/// See [`NetworkBehaviour::poll`].
#[derive(Debug)]
pub enum KademliaEvent {
    /// A query has produced a result.
    QueryResult {
        /// The ID of the query that finished.
        id: QueryId,
        /// The result of the query.
        result: QueryResult,
        /// Execution statistics from the query.
        stats: QueryStats
    },

    /// The routing table has been updated with a new peer and / or
    /// address, thereby possibly evicting another peer.
    RoutingUpdated {
        /// The ID of the peer that was added or updated.
        peer: PeerId,
        /// The full list of known addresses of `peer`.
        addresses: Addresses,
        /// The ID of the peer that was evicted from the routing table to make
        /// room for the new peer, if any.
        old_peer: Option<PeerId>,
    },

    /// A peer has connected for whom no listen address is known.
    ///
    /// If the peer is to be added to the routing table, a known
    /// listen address for the peer must be provided via [`Kademlia::add_address`].
    UnroutablePeer {
        peer: PeerId
    },

    /// A connection to a peer has been established for whom a listen address
    /// is known but the peer has not been added to the routing table either
    /// because [`KademliaBucketInserts::Manual`] is configured or because
    /// the corresponding bucket is full.
    ///
    /// If the peer is to be included in the routing table, it must
    /// must be explicitly added via [`Kademlia::add_address`], possibly after
    /// removing another peer.
    ///
    /// See [`Kademlia::kbucket`] for insight into the contents of
    /// the k-bucket of `peer`.
    RoutablePeer {
        peer: PeerId,
        address: Multiaddr,
    },

    /// A connection to a peer has been established for whom a listen address
    /// is known but the peer is only pending insertion into the routing table
    /// if the least-recently disconnected peer is unresponsive, i.e. the peer
    /// may not make it into the routing table.
    ///
    /// If the peer is to be unconditionally included in the routing table,
    /// it should be explicitly added via [`Kademlia::add_address`] after
    /// removing another peer.
    ///
    /// See [`Kademlia::kbucket`] for insight into the contents of
    /// the k-bucket of `peer`.
    PendingRoutablePeer {
        peer: PeerId,
        address: Multiaddr,
    }
}

/// The results of Kademlia queries.
#[derive(Debug)]
pub enum QueryResult {
    /// The result of [`Kademlia::bootstrap`].
    Bootstrap(BootstrapResult),

    /// The result of [`Kademlia::get_closest_peers`].
    GetClosestPeers(GetClosestPeersResult),

    /// The result of [`Kademlia::get_providers`].
    GetProviders(GetProvidersResult),

    /// The result of [`Kademlia::start_providing`].
    StartProviding(AddProviderResult),

    /// The result of a (automatic) republishing of a provider record.
    RepublishProvider(AddProviderResult),

    /// The result of [`Kademlia::get_record`].
    GetRecord(GetRecordResult),

    /// The result of [`Kademlia::put_record`].
    PutRecord(PutRecordResult),

    /// The result of a (automatic) republishing of a (value-)record.
    RepublishRecord(PutRecordResult),
}

/// The result of [`Kademlia::get_record`].
pub type GetRecordResult = Result<GetRecordOk, GetRecordError>;

/// The successful result of [`Kademlia::get_record`].
#[derive(Debug, Clone)]
pub struct GetRecordOk {
    /// The records found, including the peer that returned them.
    pub records: Vec<PeerRecord>,
    /// If caching is enabled, these are the peers closest
    /// _to the record key_ (not the local node) that were queried but
    /// did not return the record, sorted by distance to the record key
    /// from closest to farthest. How many of these are tracked is configured
    /// by [`KademliaConfig::set_caching`]. If the lookup used a quorum of
    /// 1, these peers will be sent the record as a means of caching.
    /// If the lookup used a quorum > 1, you may wish to use these
    /// candidates with [`Kademlia::put_record_to`] after selecting
    /// one of the returned records.
    pub cache_candidates: BTreeMap<kbucket::Distance, PeerId>,
}

/// The error result of [`Kademlia::get_record`].
#[derive(Debug, Clone)]
pub enum GetRecordError {
    NotFound {
        key: record::Key,
        closest_peers: Vec<PeerId>
    },
    QuorumFailed {
        key: record::Key,
        records: Vec<PeerRecord>,
        quorum: NonZeroUsize
    },
    Timeout {
        key: record::Key,
        records: Vec<PeerRecord>,
        quorum: NonZeroUsize
    }
}

impl GetRecordError {
    /// Gets the key of the record for which the operation failed.
    pub fn key(&self) -> &record::Key {
        match self {
            GetRecordError::QuorumFailed { key, .. } => key,
            GetRecordError::Timeout { key, .. } => key,
            GetRecordError::NotFound { key, .. } => key,
        }
    }

    /// Extracts the key of the record for which the operation failed,
    /// consuming the error.
    pub fn into_key(self) -> record::Key {
        match self {
            GetRecordError::QuorumFailed { key, .. } => key,
            GetRecordError::Timeout { key, .. } => key,
            GetRecordError::NotFound { key, .. } => key,
        }
    }
}

/// The result of [`Kademlia::put_record`].
pub type PutRecordResult = Result<PutRecordOk, PutRecordError>;

/// The successful result of [`Kademlia::put_record`].
#[derive(Debug, Clone)]
pub struct PutRecordOk {
    pub key: record::Key
}

/// The error result of [`Kademlia::put_record`].
#[derive(Debug)]
pub enum PutRecordError {
    QuorumFailed {
        key: record::Key,
        /// [`PeerId`]s of the peers the record was successfully stored on.
        success: Vec<PeerId>,
        quorum: NonZeroUsize
    },
    Timeout {
        key: record::Key,
        /// [`PeerId`]s of the peers the record was successfully stored on.
        success: Vec<PeerId>,
        quorum: NonZeroUsize
    },
}

impl PutRecordError {
    /// Gets the key of the record for which the operation failed.
    pub fn key(&self) -> &record::Key {
        match self {
            PutRecordError::QuorumFailed { key, .. } => key,
            PutRecordError::Timeout { key, .. } => key,
        }
    }

    /// Extracts the key of the record for which the operation failed,
    /// consuming the error.
    pub fn into_key(self) -> record::Key {
        match self {
            PutRecordError::QuorumFailed { key, .. } => key,
            PutRecordError::Timeout { key, .. } => key,
        }
    }
}

/// The result of [`Kademlia::bootstrap`].
pub type BootstrapResult = Result<BootstrapOk, BootstrapError>;

/// The successful result of [`Kademlia::bootstrap`].
#[derive(Debug, Clone)]
pub struct BootstrapOk {
    pub peer: PeerId,
    pub num_remaining: u32,
}

/// The error result of [`Kademlia::bootstrap`].
#[derive(Debug, Clone)]
pub enum BootstrapError {
    Timeout {
        peer: PeerId,
        num_remaining: Option<u32>,
    }
}

/// The result of [`Kademlia::get_closest_peers`].
pub type GetClosestPeersResult = Result<GetClosestPeersOk, GetClosestPeersError>;

/// The successful result of [`Kademlia::get_closest_peers`].
#[derive(Debug, Clone)]
pub struct GetClosestPeersOk {
    pub key: Vec<u8>,
    pub peers: Vec<PeerId>
}

/// The error result of [`Kademlia::get_closest_peers`].
#[derive(Debug, Clone)]
pub enum GetClosestPeersError {
    Timeout {
        key: Vec<u8>,
        peers: Vec<PeerId>
    }
}

impl GetClosestPeersError {
    /// Gets the key for which the operation failed.
    pub fn key(&self) -> &Vec<u8> {
        match self {
            GetClosestPeersError::Timeout { key, .. } => key,
        }
    }

    /// Extracts the key for which the operation failed,
    /// consuming the error.
    pub fn into_key(self) -> Vec<u8> {
        match self {
            GetClosestPeersError::Timeout { key, .. } => key,
        }
    }
}

/// The result of [`Kademlia::get_providers`].
pub type GetProvidersResult = Result<GetProvidersOk, GetProvidersError>;

/// The successful result of [`Kademlia::get_providers`].
#[derive(Debug, Clone)]
pub struct GetProvidersOk {
    pub key: record::Key,
    pub providers: HashSet<PeerId>,
    pub closest_peers: Vec<PeerId>
}

/// The error result of [`Kademlia::get_providers`].
#[derive(Debug, Clone)]
pub enum GetProvidersError {
    Timeout {
        key: record::Key,
        providers: HashSet<PeerId>,
        closest_peers: Vec<PeerId>
    }
}

impl GetProvidersError {
    /// Gets the key for which the operation failed.
    pub fn key(&self) -> &record::Key {
        match self {
            GetProvidersError::Timeout { key, .. } => key,
        }
    }

    /// Extracts the key for which the operation failed,
    /// consuming the error.
    pub fn into_key(self) -> record::Key {
        match self {
            GetProvidersError::Timeout { key, .. } => key,
        }
    }
}

/// The result of publishing a provider record.
pub type AddProviderResult = Result<AddProviderOk, AddProviderError>;

/// The successful result of publishing a provider record.
#[derive(Debug, Clone)]
pub struct AddProviderOk {
    pub key: record::Key,
}

/// The possible errors when publishing a provider record.
#[derive(Debug)]
pub enum AddProviderError {
    /// The query timed out.
    Timeout {
        key: record::Key,
    },
}

impl AddProviderError {
    /// Gets the key for which the operation failed.
    pub fn key(&self) -> &record::Key {
        match self {
            AddProviderError::Timeout { key, .. } => key,
        }
    }

    /// Extracts the key for which the operation failed,
    pub fn into_key(self) -> record::Key {
        match self {
            AddProviderError::Timeout { key, .. } => key,
        }
    }
}

impl From<kbucket::EntryView<kbucket::Key<PeerId>, Addresses>> for KadPeer {
    fn from(e: kbucket::EntryView<kbucket::Key<PeerId>, Addresses>) -> KadPeer {
        KadPeer {
            node_id: e.node.key.into_preimage(),
            multiaddrs: e.node.value.into_vec(),
            connection_ty: match e.status {
                NodeStatus::Connected => KadConnectionType::Connected,
                NodeStatus::Disconnected => KadConnectionType::NotConnected
            }
        }
    }
}

//////////////////////////////////////////////////////////////////////////////
// Internal query state

struct QueryInner {
    /// The query-specific state.
    info: QueryInfo,
    /// Addresses of peers discovered during a query.
    addresses: FnvHashMap<PeerId, SmallVec<[Multiaddr; 8]>>,
    /// A map of pending requests to peers.
    ///
    /// A request is pending if the targeted peer is not currently connected
    /// and these requests are sent as soon as a connection to the peer is established.
    pending_rpcs: SmallVec<[(PeerId, KademliaHandlerIn<QueryId>); K_VALUE.get()]>
}

impl QueryInner {
    fn new(info: QueryInfo) -> Self {
        QueryInner {
            info,
            addresses: Default::default(),
            pending_rpcs: SmallVec::default()
        }
    }
}

/// The context of a [`QueryInfo::AddProvider`] query.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AddProviderContext {
    /// The context is a [`Kademlia::start_providing`] operation.
    Publish,
    /// The context is periodic republishing of provider announcements
    /// initiated earlier via [`Kademlia::start_providing`].
    Republish,
}

/// The context of a [`QueryInfo::PutRecord`] query.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum PutRecordContext {
    /// The context is a [`Kademlia::put_record`] operation.
    Publish,
    /// The context is periodic republishing of records stored
    /// earlier via [`Kademlia::put_record`].
    Republish,
    /// The context is periodic replication (i.e. without extending
    /// the record TTL) of stored records received earlier from another peer.
    Replicate,
    /// The context is an automatic write-back caching operation of a
    /// record found via [`Kademlia::get_record`] at the closest node
    /// to the key queried that did not return a record. This only
    /// occurs after a lookup quorum of 1 as per standard Kademlia.
    Cache,
    /// The context is a custom store operation targeting specific
    /// peers initiated by [`Kademlia::put_record_to`].
    Custom,
}

/// Information about a running query.
#[derive(Debug, Clone)]
pub enum QueryInfo {
    /// A query initiated by [`Kademlia::bootstrap`].
    Bootstrap {
        /// The targeted peer ID.
        peer: PeerId,
        /// The remaining random peer IDs to query, one per
        /// bucket that still needs refreshing.
        ///
        /// This is `None` if the initial self-lookup has not
        /// yet completed and `Some` with an exhausted iterator
        /// if bootstrapping is complete.
        remaining: Option<vec::IntoIter<kbucket::Key<PeerId>>>
    },

    /// A query initiated by [`Kademlia::get_closest_peers`].
    GetClosestPeers { key: Vec<u8> },

    /// A query initiated by [`Kademlia::get_providers`].
    GetProviders {
        /// The key for which to search for providers.
        key: record::Key,
        /// The found providers.
        providers: HashSet<PeerId>,
    },

    /// A (repeated) query initiated by [`Kademlia::start_providing`].
    AddProvider {
        /// The record key.
        key: record::Key,
        /// The current phase of the query.
        phase: AddProviderPhase,
        /// The execution context of the query.
        context: AddProviderContext,
    },

    /// A (repeated) query initiated by [`Kademlia::put_record`].
    PutRecord {
        record: Record,
        /// The expected quorum of responses w.r.t. the replication factor.
        quorum: NonZeroUsize,
        /// The current phase of the query.
        phase: PutRecordPhase,
        /// The execution context of the query.
        context: PutRecordContext,
    },

    /// A query initiated by [`Kademlia::get_record`].
    GetRecord {
        /// The key to look for.
        key: record::Key,
        /// The records with the id of the peer that returned them. `None` when
        /// the record was found in the local store.
        records: Vec<PeerRecord>,
        /// The number of records to look for.
        quorum: NonZeroUsize,
        /// The peers closest to the `key` that were queried but did not return a record,
        /// i.e. the peers that are candidates for caching the record.
        cache_candidates: BTreeMap<kbucket::Distance, PeerId>,
    },
}

impl QueryInfo {
    /// Creates an event for a handler to issue an outgoing request in the
    /// context of a query.
    fn to_request(&self, query_id: QueryId) -> KademliaHandlerIn<QueryId> {
        match &self {
            QueryInfo::Bootstrap { peer, .. } => KademliaHandlerIn::FindNodeReq {
                key: peer.to_bytes(),
                user_data: query_id,
            },
            QueryInfo::GetClosestPeers { key, .. } => KademliaHandlerIn::FindNodeReq {
                key: key.clone(),
                user_data: query_id,
            },
            QueryInfo::GetProviders { key, .. } => KademliaHandlerIn::GetProvidersReq {
                key: key.clone(),
                user_data: query_id,
            },
            QueryInfo::AddProvider { key, phase, .. } => match phase {
                AddProviderPhase::GetClosestPeers => KademliaHandlerIn::FindNodeReq {
                    key: key.to_vec(),
                    user_data: query_id,
                },
                AddProviderPhase::AddProvider { provider_id, external_addresses, .. } => {
                    KademliaHandlerIn::AddProvider {
                        key: key.clone(),
                        provider: crate::protocol::KadPeer {
                            node_id: *provider_id,
                            multiaddrs: external_addresses.clone(),
                            connection_ty: crate::protocol::KadConnectionType::Connected,
                        }
                    }
                }
            },
            QueryInfo::GetRecord { key, .. } => KademliaHandlerIn::GetRecord {
                key: key.clone(),
                user_data: query_id,
            },
            QueryInfo::PutRecord { record, phase, .. } => match phase {
                PutRecordPhase::GetClosestPeers => KademliaHandlerIn::FindNodeReq {
                    key: record.key.to_vec(),
                    user_data: query_id,
                },
                PutRecordPhase::PutRecord { .. } => KademliaHandlerIn::PutRecord {
                    record: record.clone(),
                    user_data: query_id
                }
            }
        }
    }
}

/// The phases of a [`QueryInfo::AddProvider`] query.
#[derive(Debug, Clone)]
pub enum AddProviderPhase {
    /// The query is searching for the closest nodes to the record key.
    GetClosestPeers,

    /// The query advertises the local node as a provider for the key to
    /// the closest nodes to the key.
    AddProvider {
        /// The local peer ID that is advertised as a provider.
        provider_id: PeerId,
        /// The external addresses of the provider being advertised.
        external_addresses: Vec<Multiaddr>,
        /// Query statistics from the finished `GetClosestPeers` phase.
        get_closest_peers_stats: QueryStats,
    },
}

/// The phases of a [`QueryInfo::PutRecord`] query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PutRecordPhase {
    /// The query is searching for the closest nodes to the record key.
    GetClosestPeers,

    /// The query is replicating the record to the closest nodes to the key.
    PutRecord {
        /// A list of peers the given record has been successfully replicated to.
        success: Vec<PeerId>,
        /// Query statistics from the finished `GetClosestPeers` phase.
        get_closest_peers_stats: QueryStats,
    },
}

/// A mutable reference to a running query.
pub struct QueryMut<'a> {
    query: &'a mut Query<QueryInner>,
}

impl<'a> QueryMut<'a> {
    pub fn id(&self) -> QueryId {
        self.query.id()
    }

    /// Gets information about the type and state of the query.
    pub fn info(&self) -> &QueryInfo {
        &self.query.inner.info
    }

    /// Gets execution statistics about the query.
    ///
    /// For a multi-phase query such as `put_record`, these are the
    /// statistics of the current phase.
    pub fn stats(&self) -> &QueryStats {
        self.query.stats()
    }

    /// Finishes the query asap, without waiting for the
    /// regular termination conditions.
    pub fn finish(&mut self) {
        self.query.finish()
    }
}

/// An immutable reference to a running query.
pub struct QueryRef<'a> {
    query: &'a Query<QueryInner>,
}

impl<'a> QueryRef<'a> {
    pub fn id(&self) -> QueryId {
        self.query.id()
    }

    /// Gets information about the type and state of the query.
    pub fn info(&self) -> &QueryInfo {
        &self.query.inner.info
    }

    /// Gets execution statistics about the query.
    ///
    /// For a multi-phase query such as `put_record`, these are the
    /// statistics of the current phase.
    pub fn stats(&self) -> &QueryStats {
        self.query.stats()
    }
}

/// An operation failed to due no known peers in the routing table.
#[derive(Debug, Clone)]
pub struct NoKnownPeers();

impl fmt::Display for NoKnownPeers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "No known peers.")
    }
}

impl std::error::Error for NoKnownPeers {}

/// The possible outcomes of [`Kademlia::add_address`].
pub enum RoutingUpdate {
    /// The given peer and address has been added to the routing
    /// table.
    Success,
    /// The peer and address is pending insertion into
    /// the routing table, if a disconnected peer fails
    /// to respond. If the given peer and address ends up
    /// in the routing table, [`KademliaEvent::RoutingUpdated`]
    /// is eventually emitted.
    Pending,
    /// The routing table update failed, either because the
    /// corresponding bucket for the peer is full and the
    /// pending slot(s) are occupied, or because the given
    /// peer ID is deemed invalid (e.g. refers to the local
    /// peer ID).
    Failed,
}

/// The maximum number of local external addresses. When reached any
/// further externally reported addresses are ignored. The behaviour always
/// tracks all its listen addresses.
const MAX_LOCAL_EXTERNAL_ADDRS: usize = 20;
