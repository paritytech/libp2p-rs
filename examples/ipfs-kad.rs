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

//! Demonstrates how to perform Kademlia queries on the IPFS network.
//!
//! You can pass as parameter a base58 peer ID to search for. If you don't pass any parameter, a
//! peer ID will be generated randomly.

use futures::prelude::*;
use libp2p::{
    Swarm,
    PeerId,
    identity,
    build_development_transport
};
use libp2p::protocols::kad::{Kademlia, KademliaConfig, KademliaEvent, GetClosestPeersError};
use libp2p::protocols::kad::record::store::MemoryStore;
use std::env;
use std::time::Duration;

fn main() {
    env_logger::init();

    // Create a random key for ourselves.
    let local_key = identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(local_key.public());

    // Set up a an encrypted DNS-enabled TCP Transport over the Mplex protocol
    let transport = build_development_transport(local_key);

    // Create a swarm to manage peers and events.
    let mut swarm = {
        // Create a Kademlia behaviour.
        // Note that normally the Kademlia process starts by performing lots of request in order
        // to insert our local node in the DHT. However here we use `without_init` because this
        // example is very ephemeral and we don't want to pollute the DHT. In a real world
        // application, you want to use `new` instead.
        let mut cfg = KademliaConfig::default();
        cfg.set_query_timeout(Duration::from_secs(5 * 60));
        let store = MemoryStore::new(local_peer_id.clone());
        let mut behaviour = Kademlia::with_config(local_peer_id.clone(), store, cfg);

        // TODO: the /dnsaddr/ scheme is not supported (https://github.com/libp2p/rust-libp2p/issues/967)
        /*behaviour.add_address(&"QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN".parse().unwrap(), "/dnsaddr/bootstrap.libp2p.io".parse().unwrap());
        behaviour.add_address(&"QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa".parse().unwrap(), "/dnsaddr/bootstrap.libp2p.io".parse().unwrap());
        behaviour.add_address(&"QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb".parse().unwrap(), "/dnsaddr/bootstrap.libp2p.io".parse().unwrap());
        behaviour.add_address(&"QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt".parse().unwrap(), "/dnsaddr/bootstrap.libp2p.io".parse().unwrap());*/

        // The only address that currently works.
        behaviour.add_address(&"QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ".parse().unwrap(), "/ip4/104.131.131.82/tcp/4001".parse().unwrap());

        // The following addresses always fail signature verification, possibly due to
        // RSA keys with < 2048 bits.
        // behaviour.add_address(&"QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM".parse().unwrap(), "/ip4/104.236.179.241/tcp/4001".parse().unwrap());
        // behaviour.add_address(&"QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu".parse().unwrap(), "/ip4/128.199.219.111/tcp/4001".parse().unwrap());
        // behaviour.add_address(&"QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64".parse().unwrap(), "/ip4/104.236.76.40/tcp/4001".parse().unwrap());
        // behaviour.add_address(&"QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd".parse().unwrap(), "/ip4/178.62.158.247/tcp/4001".parse().unwrap());

        // The following addresses are permanently unreachable:
        // Other(Other(A(Transport(A(Underlying(Os { code: 101, kind: Other, message: "Network is unreachable" }))))))
        // behaviour.add_address(&"QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM".parse().unwrap(), "/ip6/2604:a880:1:20::203:d001/tcp/4001".parse().unwrap());
        // behaviour.add_address(&"QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu".parse().unwrap(), "/ip6/2400:6180:0:d0::151:6001/tcp/4001".parse().unwrap());
        // behaviour.add_address(&"QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64".parse().unwrap(), "/ip6/2604:a880:800:10::4a:5001/tcp/4001".parse().unwrap());
        // behaviour.add_address(&"QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd".parse().unwrap(), "/ip6/2a03:b0c0:0:1010::23:1001/tcp/4001".parse().unwrap());
        Swarm::new(transport, behaviour, local_peer_id)
    };

    // Order Kademlia to search for a peer.
    let to_search: PeerId = if let Some(peer_id) = env::args().nth(1) {
        peer_id.parse().expect("Failed to parse peer ID to find")
    } else {
        identity::Keypair::generate_ed25519().public().into()
    };

    println!("Searching for the closest peers to {:?}", to_search);
    swarm.get_closest_peers(to_search);

    // Kick it off!
    tokio::run(futures::future::poll_fn(move || {
        loop {
            match swarm.poll().expect("Error while polling swarm") {
                Async::Ready(Some(KademliaEvent::GetClosestPeersResult(res))) => {
                    match res {
                        Ok(ok) => {
                            if !ok.peers.is_empty() {
                                println!("Query finished with closest peers: {:#?}", ok.peers);
                                return Ok(Async::Ready(()));
                            } else {
                                // The example is considered failed as there
                                // should always be at least 1 reachable peer.
                                panic!("Query finished with no closest peers.");
                            }
                        }
                        Err(GetClosestPeersError::Timeout { peers, .. }) => {
                            if !peers.is_empty() {
                                println!("Query timed out with closest peers: {:#?}", peers);
                                return Ok(Async::Ready(()));
                            } else {
                                // The example is considered failed as there
                                // should always be at least 1 reachable peer.
                                panic!("Query timed out with no closest peers.");
                            }
                        }
                    }
                },
                Async::Ready(Some(_)) => {},
                Async::Ready(None) | Async::NotReady => break,
            }
        }

        Ok(Async::NotReady)
    }));
}
