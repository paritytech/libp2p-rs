use constants::{
    ALPHA,
    KBUCKETS_TIMEOUT,
    REQUEST_TIMEOUT,
};
use futures::Future;
use libp2p_core::{
    PeerId,
    swarm::swarm,
    Transport
};
use libp2p_kad::{
    KadSystemConfig,
    KadConnecConfig,
    KadSystem
};
use libp2p_ping::Ping;
use libp2p_secio::SecioKeyPair;
use time::Duration;
use tokio_core;
use tokio_current_thread;

fn init_overlay() -> KadSystem {
    // The following Kademlia quotes are from:
    // https://github.com/libp2p/rust-libp2p/blob/8e07c18178ac43cad3fa8974a243a98d9bc8b896/kad/src/lib.rs#L21.

    // "Build a `KadSystemConfig` and a `KadConnecConfig` object that contain the way you want the
    // Kademlia protocol to behave."

    let secio_key_pair_inner = SecioKeyPairInner::Ed25519 {

    };
    let secio_key_pair = SecioKeyPair {
        inner : 
    }

//     // Inner content of `SecioKeyPair`.
// #[derive(Clone)]
// enum SecioKeyPairInner {
//     Rsa {
//         public: Vec<u8>,
//         // We use an `Arc` so that we can clone the enum.
//         private: Arc<RSAKeyPair>,
//     },
//     Ed25519 {
//         // We use an `Arc` so that we can clone the enum.
//         key_pair: Arc<Ed25519KeyPair>,
//     },
//     #[cfg(feature = "secp256k1")]
//     Secp256k1 { private: secp256k1::key::SecretKey },
// }

    let sample_peer_id = to_peer_id(ed25519_generated());

    // KadSystemConfig
    // https://github.com/libp2p/rust-libp2p/blob/7507e0bfd9f11520f2d6291120f1b68d0afce80a/kad/src/high_level.rs#L36
    let kad_system_config = KadSystemConfig {
        parallelism: ALPHA,
        local_peer_id: sample_peer_id,
        known_initial_peers: vec![],
        kbuckets_timeout: Duration.hour(KBUCKETS_TIMEOUT),
        request_timeout: Duration.minutes(REQUEST_TIMEOUT),
    };

    // KadConnecConfig
    // In https://github.com/libp2p/rust-libp2p/blob/master/kad/src/kad_server.rs
    let kad_connec_config = KadConnecConfig.new();

    // "Create a swarm that upgrades incoming connections with the `KadConnecConfig`.

    let mut core = tokio_core::reactor::Core::new().unwrap();

    let kad_connec_config_transport = kad_connec_config.with_dummy_muxing();

    let (swarm_controller, swarm_future) = swarm(kad_connec_config_transport,
            Ping, |(mut pinger, service), client_addr| {
        pinger.ping().map_err(|_| panic!())
            .select(service).map_err(|_| panic!())
            .map(|_| ())
    });

    // The `swarm_controller` can then be used to do some operations.
    swarm_controller.listen_on("/ip4/0.0.0.0/tcp/0".parse().unwrap());

    // Runs until everything is finished.
    tokio_current_thread::block_on_all(swarm_future).unwrap();

    // "Build a `KadSystem` from the `KadSystemConfig`. This requires passing a closure that provides
    // the Kademlia controller of a peer."
    // FMI see https://github.com/libp2p/rust-libp2p/blob/master/kad/src/high_level.rs
    let kad_peer_controller = |peer_id: &PeerId| peer_id;

    let kad_system = KadSystem {
        kbuckets : KBucketsTable {
            my_id: sample_peer_id,
        }
    }.start(kad_system_config, kad_peer_controller(sample_peer_id));
    kad_system

    pub struct KBucketsTable<Id, Val> {
    my_id: Id,
    tables: Vec<Mutex<KBucket<Id, Val>>>,
    // The timeout when pinging the first node after which we consider that it no longer responds.
    ping_timeout: Duration,
}
}

// "You can perform queries using the `KadSystem`." TODO: Test

// Join overlay
// 
// Obtain initial contact nodes via rendevous with DHT provider records.

// "Send a GETNODE message in order to obtain an up-to-date view of the overlay from the passive list of a 
// subscribed node regardless of age of Provider records.



// Once an up-to-date passive view of the overlay has been
// obtained, the node proceeds to join.

// In order to join, it picks `C_rand` nodes at random and sends
// `JOIN` messages to them with some initial TTL set as a design parameter.

// The `JOIN` message propagates with a random walk until a node is willing
// to accept it or the TTL expires. Upon receiving a `JOIN` message, a node Q
// evaluates it with the following criteria:
// - Q tries to open a connection to P. If the connection cannot be opened (e.g. because of NAT),
//   then it checks the TTL of the message.
//   If it is 0, the request is dropped, otherwise Q decrements the TTL and forwards
//   the message to a random node in its active list.
// - If the TTL of the request is 0 or if the size of Q's active list is less than `A`,
//   it accepts the join, adds P to its active list and sends a `NEIGHBOR` message.
// - Otherwise it decrements the TTL and forwards the message to a random node
//   in its active list.

// When Q accepts P as a new neighbor, it also sends a `FORWARDJOIN`
// message to a random node in its active list. The `FORWARDJOIN`
// propagates with a random walk until its TTL is 0, while being added to
// the passive list of the receiving nodes.

// If P fails to join because of connectivity issues, it decrements the
// TTL and tries another starting node. This is repeated until a TTL of zero
// reuses the connection in the case of NATed hosts.

// Once the first links have been established, P then needs to increase
// its active list size to `A` by connecting to more nodes.  This is
// accomplished by ordering the subscriber list by RTT and picking the
// nearest nodes and sending `NEIGHBOR` requests.  The neighbor requests
// may be accepted by `NEIGHBOR` message and rejected by a `DISCONNECT`
// message.

// Upon receiving a `NEIGHBOR` request a node Q evaluates it with the
// following criteria:
// - If the size of Q's active list is less than A, it accepts the new
//   node.
// - If P does not have enough active links (less than `C_rand`, as specified in the message),
//   it accepts P as a random neighbor.
// - Otherwise Q takes an RTT measurement to P.
//   If it's closer than any near neighbors by a factor of alpha, then
//   it evicts the near neighbor if it has enough active links and accepts
//   P as a new near neighbor.
// - Otherwise the request is rejected.

// Note that during joins, the size of the active list for some nodes may
// end up being larger than `A`. Similarly, P may end up with fewer links
// than `A` after an initial join. This follows [3] and tries to minimize
// fluttering in joins, leaving the active list pruning for the
// stabilization period of the protocol.

#[cfg(test)]
mod tests {
    use kad::KadSystem;

    #[test]
    fn get_local_peer_id() {
        let peer_id = KadSystem.local_peer_id();
        assert_eq!(sample_peer_id, peer_id)
    }
}