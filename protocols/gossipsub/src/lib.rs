//! Gossipsub is a P2P pubsub (publish/subscription) routing layer designed to extend upon
//! flooodsub and meshsub routing protocols.
//!
//! # Overview
//!
//! *Note: The gossipsub protocol specifications
//! (https://github.com/libp2p/specs/tree/master/pubsub/gossipsub) provide an outline for the
//! routing protocol. They should be consulted for further detail.*
//!
//! Gossipsub  is a blend of meshsub for data and randomsub for mesh metadata. It provides bounded
//! degree and amplification factor with the meshsub construction and augments it using gossip
//! propagation of metadata with the randomsub technique.
//!
//! The router maintains an overlay mesh network of peers on which to efficiently send messages and
//! metadata.  Peers use control messages to broadcast and request known messages and
//! subscribe/unsubscribe from topics in the mesh network.
//!
//! # Important Discrepancies
//!
//! This section outlines the current implementation's potential discrepancies from that of other
//! implementations, due to undefined elements in the current specification.
//!
//! - **Topics** -  In gossipsub, topics are utf-8 string's that are `base58` encoded. These are of
//! type `TopicHash`. The current go implementation uses raw utf-8 strings.  - **Sequence Numbers**
//! - A message on the gossipsub network is identified by the source `PeerId` and a nonce (sequence
//! number) of the message. The sequence numbers in this implementation are sent as raw bytes
//! across the wire. They are 64-bit big-endian   unsigned integers. They are chosen at random in
//! this implementation of gossipsub, but are sequential in the current go implementation.
//!
//! # Using Gossipsub
//!
//! ## GossipsubConfig
//!
//! The [`GossipsubConfig`] struct specifies various network performance/tuning configuration
//! parameters. Specifically it specifies:
//!
//! [`GossipsubConfig`]: struct.GossipsubConfig.html

//! - `history_length` - The number of heartbeats which past messages are kept in cache (default: 5).
//! - `history_gossip` - The number of past heartbeats that the node will send gossip metadata
//! about (default: 3).
//! - `mesh_n` - The target number of peers store in the local mesh network (default: 6).
//! - `mesh_n_low` - The minimum number of peers in the local mesh network before trying to add
//! more peers to the mesh from the connected peer pool (default: 4).
//! - `mesh_n_high` - The maximum number of peers in the local mesh network before removing peers
//! to reach `mesh_n` peers (default: 12).
//! - `gossip_lazy` - The number of peers that the local node will gossip to during a heartbeat
//! (default: `mesh_n` = 6).
//! - `heartbeat_initial_delay - The initial time delay before starting the first heartbeat
//! (default: 5 seconds).
//! - `heartbeat_interval` - The time between each heartbeat (default: 1 second).
//! - `fanout_ttl` - The fanout time to live time period. The timeout required before removing
//! peers from the fanout for a given topic (default: 1 minute).
//!
//! This struct implements the `Default` trait and can be initialised via
//! `GossipsubConfig::default()`.
//!
//!
//! ## Gossipsub
//!
//! The [`Gossipsub`] struct implements the `NetworkBehaviour` trait allowing it to act as the routing
//! behaviour in a `Swarm`. This struct requires an instance of `PeerId` and
//! [`GossipsubConfig`].
//!
//! [`Gossipsub`]: struct.Gossipsub.html

//! ## Example
//!
//! An example of initialising a gossipsub compatible swarm:
//!
//! ```ignore
//! #extern crate libp2p;
//! #extern crate futures;
//! #extern crate tokio;
//! #use libp2p::gossipsub::GossipsubEvent;
//! #use libp2p::{gossipsub, secio,
//! #    tokio_codec::{FramedRead, LinesCodec},
//! #};
//! let local_key = secio::SecioKeyPair::ed25519_generated().unwrap();
//! let local_pub_key = local_key.to_public_key();
//!
//! // Set up an encrypted TCP Transport over the Mplex and Yamux protocols
//! let transport = libp2p::build_development_transport(local_key);
//!
//! // Create a Floodsub/Gossipsub topic
//! let topic = libp2p::floodsub::TopicBuilder::new("example").build();
//!
//! // Create a Swarm to manage peers and events
//! let mut swarm = {
//!     // set default parameters for gossipsub
//!     let gossipsub_config = gossipsub::GossipsubConfig::default();
//!     // build a gossipsub network behaviour
//!     let mut gossipsub =
//!         gossipsub::Gossipsub::new(local_pub_key.clone().into_peer_id(), gossipsub_config);
//!     gossipsub.subscribe(topic.clone());
//!     libp2p::Swarm::new(
//!         transport,
//!         gossipsub,
//!         libp2p::core::topology::MemoryTopology::empty(local_pub_key),
//!     )
//! };
//!
//! // Listen on all interfaces and whatever port the OS assigns
//! let addr = libp2p::Swarm::listen_on(&mut swarm, "/ip4/0.0.0.0/tcp/0".parse().unwrap()).unwrap();
//! println!("Listening on {:?}", addr);
//! ```

pub mod protocol;

mod behaviour;
mod gossipsub_config;
mod handler;
mod mcache;
mod rpc_proto;

pub use self::behaviour::{Gossipsub, GossipsubEvent, GossipsubRpc};
pub use self::gossipsub_config::{GossipsubConfig, GossipsubConfigBuilder};
pub use self::protocol::GossipsubMessage;
