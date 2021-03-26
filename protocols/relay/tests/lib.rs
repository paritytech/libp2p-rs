// Copyright 2021 Parity Technologies (UK) Ltd.
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

use futures::executor::LocalPool;
use futures::future::{poll_fn, FutureExt};
use futures::stream::Stream;
use futures::task::Spawn;
use libp2p::kad::record::store::MemoryStore;
use libp2p::NetworkBehaviour;
use libp2p_core::connection::{ConnectedPoint, ConnectionId};
use libp2p_core::either::EitherTransport;
use libp2p_core::multiaddr::{Multiaddr, Protocol};
use libp2p_core::transport::{MemoryTransport, Transport, TransportError};
use libp2p_core::upgrade::{DeniedUpgrade, InboundUpgrade, OutboundUpgrade};
use libp2p_core::{identity, upgrade, PeerId};
use libp2p_identify::{Identify, IdentifyConfig, IdentifyEvent, IdentifyInfo};
use libp2p_kad::{GetClosestPeersOk, Kademlia, KademliaEvent, QueryResult};
use libp2p_ping::{Ping, PingConfig, PingEvent};
use libp2p_plaintext::PlainText2Config;
use libp2p_relay::{Relay, RelayConfig};
use libp2p_swarm::protocols_handler::{
    KeepAlive, ProtocolsHandler, ProtocolsHandlerEvent, ProtocolsHandlerUpgrErr, SubstreamProtocol,
};
use libp2p_swarm::{
    AddressRecord, NegotiatedSubstream, NetworkBehaviour, NetworkBehaviourAction,
    NetworkBehaviourEventProcess, PollParameters, Swarm, SwarmEvent,
};
use std::iter;
use std::task::{Context, Poll};
use std::time::Duration;
use void::Void;

#[test]
fn src_connect_to_dst_listening_via_relay() {
    let _ = env_logger::try_init();

    let mut pool = LocalPool::new();

    let mut src_swarm = build_swarm(Reachability::Firewalled, RelayMode::Passive);
    let mut dst_swarm = build_swarm(Reachability::Firewalled, RelayMode::Passive);
    let mut relay_swarm = build_swarm(Reachability::Routable, RelayMode::Passive);

    let src_peer_id = *src_swarm.local_peer_id();
    let dst_peer_id = *dst_swarm.local_peer_id();
    let relay_peer_id = *relay_swarm.local_peer_id();

    let relay_addr = Multiaddr::empty().with(Protocol::Memory(rand::random::<u64>()));
    let dst_listen_addr_via_relay = relay_addr
        .clone()
        .with(Protocol::P2p(relay_peer_id.into()))
        .with(Protocol::P2pCircuit);
    let dst_addr_via_relay = dst_listen_addr_via_relay
        .clone()
        .with(Protocol::P2p(dst_peer_id.into()));

    relay_swarm.listen_on(relay_addr.clone()).unwrap();
    spawn_swarm_on_pool(&pool, relay_swarm);

    dst_swarm.listen_on(dst_listen_addr_via_relay.clone()).unwrap();

    pool.run_until(async {
        // Destination Node dialing Relay.
        match dst_swarm.next_event().await {
            SwarmEvent::Dialing(peer_id) => assert_eq!(peer_id, relay_peer_id),
            e => panic!("{:?}", e),
        }

        // Destination Node establishing connection to Relay.
        match dst_swarm.next_event().await {
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                assert_eq!(peer_id, relay_peer_id);
            }
            e => panic!("{:?}", e),
        }

        // Destination Node reporting listen address via relay.
        loop {
            match dst_swarm.next_event().await {
                SwarmEvent::NewListenAddr(addr) if addr == dst_listen_addr_via_relay => break,
                SwarmEvent::Behaviour(CombinedEvent::Ping(_)) => {}
                SwarmEvent::Behaviour(CombinedEvent::Kad(KademliaEvent::RoutingUpdated {
                    ..
                })) => {}
                e => panic!("{:?}", e),
            }
        }

        let dst = async move {
            // Destination Node receiving connection from Source Node via Relay.
            loop {
                match dst_swarm.next_event().await {
                    SwarmEvent::IncomingConnection { send_back_addr, .. } => {
                        assert_eq!(
                            send_back_addr,
                            Protocol::P2p(src_peer_id.clone().into()).into()
                        );
                        break;
                    }
                    SwarmEvent::Behaviour(CombinedEvent::Ping(_)) => {}
                    e => panic!("{:?}", e),
                }
            }

            // Destination Node establishing connection from Source Node via Relay.
            loop {
                match dst_swarm.next_event().await {
                    SwarmEvent::ConnectionEstablished { peer_id, .. } if peer_id == src_peer_id => {
                        break;
                    }
                    SwarmEvent::Behaviour(CombinedEvent::Ping(_)) => {}
                    e => panic!("{:?}", e),
                }
            }

            // Destination Node waiting for Ping from Source Node via Relay.
            loop {
                match dst_swarm.next_event().await {
                    SwarmEvent::Behaviour(CombinedEvent::Ping(_)) => {}
                    SwarmEvent::ConnectionClosed { peer_id, .. } => {
                        assert_eq!(peer_id, src_peer_id);
                        break;
                    }
                    e => panic!("{:?}", e),
                }
            }
        };

        src_swarm.dial_addr(dst_addr_via_relay).unwrap();
        let src = async move {
            // Source Node dialing Relay to connect to Destination Node.
            match src_swarm.next_event().await {
                SwarmEvent::Dialing(peer_id) if peer_id == relay_peer_id => {}
                e => panic!("{:?}", e),
            }

            // Source Node establishing connection to Relay to connect to Destination Node.
            match src_swarm.next_event().await {
                SwarmEvent::ConnectionEstablished { peer_id, .. } if peer_id == relay_peer_id => {}
                e => panic!("{:?}", e),
            }

            // Source Node establishing connection to destination node via Relay.
            loop {
                match src_swarm.next_event().await {
                    SwarmEvent::ConnectionEstablished { peer_id, .. } if peer_id == dst_peer_id => {
                        break
                    }
                    SwarmEvent::Behaviour(CombinedEvent::Ping(_)) => {}
                    e => panic!("{:?}", e),
                }
            }

            // Source Node waiting for Ping from Destination Node via Relay.
            loop {
                match src_swarm.next_event().await {
                    SwarmEvent::Behaviour(CombinedEvent::Ping(PingEvent {
                        peer,
                        result: Ok(_),
                    })) => {
                        if peer == dst_peer_id {
                            break;
                        }
                    }
                    e => panic!("{:?}", e),
                }
            }
        };

        futures::future::join(dst, src).await
    });
}

#[test]
fn src_connect_to_dst_not_listening_via_active_relay() {
    let _ = env_logger::try_init();

    let mut pool = LocalPool::new();

    let mut src_swarm = build_swarm(Reachability::Firewalled, RelayMode::Passive);
    let mut dst_swarm = build_swarm(Reachability::Routable, RelayMode::Passive);
    let mut relay_swarm = build_swarm(Reachability::Routable, RelayMode::Active);

    let relay_peer_id = *relay_swarm.local_peer_id();
    let dst_peer_id = *dst_swarm.local_peer_id();

    let relay_addr: Multiaddr = Protocol::Memory(rand::random::<u64>()).into();
    let dst_addr: Multiaddr = Protocol::Memory(rand::random::<u64>()).into();
    let dst_addr_via_relay = relay_addr
        .clone()
        .with(Protocol::P2p(relay_peer_id.clone().into()))
        .with(Protocol::P2pCircuit)
        .with(dst_addr.into_iter().next().unwrap())
        .with(Protocol::P2p(dst_peer_id.clone().into()));

    relay_swarm.listen_on(relay_addr.clone()).unwrap();
    spawn_swarm_on_pool(&pool, relay_swarm);

    dst_swarm.listen_on(dst_addr.clone()).unwrap();
    // Instruct destination node to listen for incoming relayed connections from unknown relay nodes.
    dst_swarm.listen_on(Protocol::P2pCircuit.into()).unwrap();
    spawn_swarm_on_pool(&pool, dst_swarm);

    src_swarm.dial_addr(dst_addr_via_relay).unwrap();
    pool.run_until(async move {
        // Source Node dialing Relay to connect to Destination Node.
        match src_swarm.next_event().await {
            SwarmEvent::Dialing(peer_id) if peer_id == relay_peer_id => {}
            e => panic!("{:?}", e),
        }

        // Source Node establishing connection to Relay to connect to Destination Node.
        match src_swarm.next_event().await {
            SwarmEvent::ConnectionEstablished { peer_id, .. } if peer_id == relay_peer_id => {}
            e => panic!("{:?}", e),
        }

        // Source Node establishing connection to destination node via Relay.
        loop {
            match src_swarm.next_event().await {
                SwarmEvent::ConnectionEstablished { peer_id, .. } if peer_id == dst_peer_id => {
                    break
                }
                SwarmEvent::Behaviour(CombinedEvent::Ping(_)) => {}
                e => panic!("{:?}", e),
            }
        }

        // Source Node waiting for Ping from Destination Node via Relay.
        loop {
            match src_swarm.next_event().await {
                SwarmEvent::Behaviour(CombinedEvent::Ping(PingEvent {
                    peer,
                    result: Ok(_),
                })) => {
                    if peer == dst_peer_id {
                        break;
                    }
                }
                e => panic!("{:?}", e),
            }
        }
    });
}

#[test]
fn src_connect_to_dst_via_established_connection_to_relay() {
    let _ = env_logger::try_init();

    let mut pool = LocalPool::new();

    let mut src_swarm = build_swarm(Reachability::Firewalled, RelayMode::Passive);
    let mut dst_swarm = build_swarm(Reachability::Routable, RelayMode::Passive);
    let mut relay_swarm = build_swarm(Reachability::Routable, RelayMode::Passive);

    let relay_peer_id = *relay_swarm.local_peer_id();
    let dst_peer_id = *dst_swarm.local_peer_id();

    let relay_addr: Multiaddr = Protocol::Memory(rand::random::<u64>()).into();
    let dst_addr_via_relay = relay_addr
        .clone()
        .with(Protocol::P2p(relay_peer_id.clone().into()))
        .with(Protocol::P2pCircuit)
        .with(Protocol::P2p(dst_peer_id.clone().into()));

    relay_swarm.listen_on(relay_addr.clone()).unwrap();
    spawn_swarm_on_pool(&pool, relay_swarm);

    dst_swarm.listen_on(dst_addr_via_relay.clone()).unwrap();
    // Wait for destination to listen via relay.
    pool.run_until(async {
        loop {
            match dst_swarm.next_event().await {
                SwarmEvent::Dialing(_) => {}
                SwarmEvent::ConnectionEstablished { .. } => {}
                SwarmEvent::NewListenAddr(addr) if addr == dst_addr_via_relay => break,
                e => panic!("{:?}", e),
            }
        }
    });
    spawn_swarm_on_pool(&pool, dst_swarm);

    pool.run_until(async move {
        src_swarm.dial_addr(relay_addr).unwrap();

        // Source Node establishing connection to Relay.
        loop {
            match src_swarm.next_event().await {
                SwarmEvent::Dialing(peer_id) if peer_id == relay_peer_id => {}
                SwarmEvent::ConnectionEstablished { peer_id, .. } if peer_id == relay_peer_id => {
                    break
                }
                SwarmEvent::Behaviour(CombinedEvent::Ping(_)) => {}
                e => panic!("{:?}", e),
            }
        }

        src_swarm.dial_addr(dst_addr_via_relay).unwrap();

        // Source Node establishing connection to destination node via Relay.
        loop {
            match src_swarm.next_event().await {
                SwarmEvent::ConnectionEstablished { peer_id, .. } if peer_id == dst_peer_id => {
                    break
                }
                SwarmEvent::Behaviour(CombinedEvent::Ping(_)) => {}
                e => panic!("{:?}", e),
            }
        }

        // Source Node waiting for Ping from Destination Node via Relay.
        loop {
            match src_swarm.next_event().await {
                SwarmEvent::Behaviour(CombinedEvent::Ping(PingEvent {
                    peer,
                    result: Ok(_),
                })) => {
                    if peer == dst_peer_id {
                        break;
                    }
                }
                e => panic!("{:?}", e),
            }
        }
    });
}

#[test]
fn src_try_connect_to_offline_dst() {
    let _ = env_logger::try_init();

    let mut pool = LocalPool::new();

    let mut src_swarm = build_swarm(Reachability::Firewalled, RelayMode::Passive);
    let mut relay_swarm = build_swarm(Reachability::Routable, RelayMode::Passive);

    let relay_peer_id = *relay_swarm.local_peer_id();
    let dst_peer_id = PeerId::random();

    let relay_addr: Multiaddr = Protocol::Memory(rand::random::<u64>()).into();
    let dst_addr: Multiaddr = Protocol::Memory(rand::random::<u64>()).into();
    let dst_addr_via_relay = relay_addr
        .clone()
        .with(Protocol::P2p(relay_peer_id.clone().into()))
        .with(Protocol::P2pCircuit)
        .with(dst_addr.into_iter().next().unwrap())
        .with(Protocol::P2p(dst_peer_id.clone().into()));

    relay_swarm.listen_on(relay_addr.clone()).unwrap();
    spawn_swarm_on_pool(&pool, relay_swarm);

    src_swarm.dial_addr(dst_addr_via_relay.clone()).unwrap();
    pool.run_until(async move {
        // Source Node dialing Relay to connect to Destination Node.
        match src_swarm.next_event().await {
            SwarmEvent::Dialing(peer_id) if peer_id == relay_peer_id => {}
            e => panic!("{:?}", e),
        }

        // Source Node establishing connection to Relay to connect to Destination Node.
        match src_swarm.next_event().await {
            SwarmEvent::ConnectionEstablished { peer_id, .. } if peer_id == relay_peer_id => {}
            e => panic!("{:?}", e),
        }

        loop {
            match src_swarm.next_event().await {
                SwarmEvent::UnreachableAddr { address, peer_id, .. }
                    if address == dst_addr_via_relay =>
                {
                    assert_eq!(peer_id, dst_peer_id);
                    break;
                }
                SwarmEvent::Behaviour(CombinedEvent::Ping(_)) => {}
                e => panic!("{:?}", e),
            }
        }
    });
}

#[test]
fn src_try_connect_to_unsupported_dst() {
    let _ = env_logger::try_init();

    let mut pool = LocalPool::new();

    let mut src_swarm = build_swarm(Reachability::Firewalled, RelayMode::Passive);
    let mut relay_swarm = build_swarm(Reachability::Routable, RelayMode::Passive);
    let mut dst_swarm = build_keep_alive_only_swarm();

    let relay_peer_id = *relay_swarm.local_peer_id();
    let dst_peer_id = *dst_swarm.local_peer_id();

    let relay_addr: Multiaddr = Protocol::Memory(rand::random::<u64>()).into();
    let dst_addr: Multiaddr = Protocol::Memory(rand::random::<u64>()).into();
    let dst_addr_via_relay = relay_addr
        .clone()
        .with(Protocol::P2p(relay_peer_id.clone().into()))
        .with(Protocol::P2pCircuit)
        .with(dst_addr.into_iter().next().unwrap())
        .with(Protocol::P2p(dst_peer_id.clone().into()));

    relay_swarm.listen_on(relay_addr.clone()).unwrap();
    spawn_swarm_on_pool(&pool, relay_swarm);

    dst_swarm.listen_on(dst_addr.clone()).unwrap();
    spawn_swarm_on_pool(&pool, dst_swarm);

    src_swarm.dial_addr(dst_addr_via_relay.clone()).unwrap();
    pool.run_until(async move {
        // Source Node dialing Relay to connect to Destination Node.
        match src_swarm.next_event().await {
            SwarmEvent::Dialing(peer_id) if peer_id == relay_peer_id => {}
            e => panic!("{:?}", e),
        }

        // Source Node establishing connection to Relay to connect to Destination Node.
        match src_swarm.next_event().await {
            SwarmEvent::ConnectionEstablished { peer_id, .. } if peer_id == relay_peer_id => {}
            e => panic!("{:?}", e),
        }

        loop {
            match src_swarm.next_event().await {
                SwarmEvent::UnreachableAddr { address, peer_id, .. }
                    if address == dst_addr_via_relay =>
                {
                    assert_eq!(peer_id, dst_peer_id);
                    break;
                }
                SwarmEvent::ConnectionClosed { peer_id, .. } if peer_id == relay_peer_id => {}
                SwarmEvent::Behaviour(CombinedEvent::Ping(_)) => {}
                e => panic!("{:?}", e),
            }
        }
    });
}

#[test]
fn src_try_connect_to_offline_dst_via_offline_relay() {
    let _ = env_logger::try_init();

    let mut pool = LocalPool::new();

    let mut src_swarm = build_swarm(Reachability::Firewalled, RelayMode::Passive);

    let relay_peer_id = PeerId::random();
    let dst_peer_id = PeerId::random();

    let relay_addr: Multiaddr = Protocol::Memory(rand::random::<u64>()).into();
    let dst_addr: Multiaddr = Protocol::Memory(rand::random::<u64>()).into();
    let dst_addr_via_relay = relay_addr
        .clone()
        .with(Protocol::P2p(relay_peer_id.clone().into()))
        .with(Protocol::P2pCircuit)
        .with(dst_addr.into_iter().next().unwrap())
        .with(Protocol::P2p(dst_peer_id.clone().into()));

    src_swarm.dial_addr(dst_addr_via_relay.clone()).unwrap();
    pool.run_until(async move {
        // Source Node dialing Relay to connect to Destination Node.
        match src_swarm.next_event().await {
            SwarmEvent::Dialing(peer_id) if peer_id == relay_peer_id => {}
            e => panic!("{:?}", e),
        }

        // Source Node fail to reach Relay.
        match src_swarm.next_event().await {
            SwarmEvent::UnreachableAddr { peer_id, .. } if peer_id == relay_peer_id => {}
            e => panic!("{:?}", e),
        }

        // Source Node fail to reach Destination Node due to failure reaching Relay.
        match src_swarm.next_event().await {
            SwarmEvent::UnreachableAddr { address, peer_id, .. }
                if address == dst_addr_via_relay => {
                    assert_eq!(peer_id, dst_peer_id);
                }
            e => panic!("{:?}", e),
        }
    });
}

#[test]
fn firewalled_src_discover_firewalled_dst_via_kad_and_connect_to_dst_via_routable_relay() {
    let _ = env_logger::try_init();

    let mut pool = LocalPool::new();

    let mut src_swarm = build_swarm(Reachability::Firewalled, RelayMode::Passive);
    let mut dst_swarm = build_swarm(Reachability::Firewalled, RelayMode::Passive);
    let mut relay_swarm = build_swarm(Reachability::Routable, RelayMode::Passive);

    let src_peer_id = *src_swarm.local_peer_id();
    let dst_peer_id = *dst_swarm.local_peer_id();
    let relay_peer_id = *relay_swarm.local_peer_id();

    let relay_addr: Multiaddr = Protocol::Memory(rand::random::<u64>()).into();
    let dst_addr_via_relay = relay_addr
        .clone()
        .with(Protocol::P2p(relay_peer_id.into()))
        .with(Protocol::P2pCircuit)
        .with(Protocol::P2p(dst_peer_id.into()));

    src_swarm
        .behaviour_mut()
        .kad
        .add_address(&relay_peer_id, relay_addr.clone());
    dst_swarm
        .behaviour_mut()
        .kad
        .add_address(&relay_peer_id, relay_addr.clone());

    relay_swarm.listen_on(relay_addr.clone()).unwrap();
    spawn_swarm_on_pool(&pool, relay_swarm);

    // Destination Node listen via Relay.
    dst_swarm.listen_on(dst_addr_via_relay.clone()).unwrap();

    pool.run_until(async {
        // Destination Node dialing Relay.
        match dst_swarm.next_event().await {
            SwarmEvent::Dialing(peer_id) => assert_eq!(peer_id, relay_peer_id),
            e => panic!("{:?}", e),
        }

        // Destination Node establishing connection to Relay.
        loop {
            match dst_swarm.next_event().await {
                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    assert_eq!(peer_id, relay_peer_id);
                    break;
                }
                SwarmEvent::Behaviour(CombinedEvent::Kad(_)) => {}
                SwarmEvent::Behaviour(CombinedEvent::Ping(_)) => {}
                e => panic!("{:?}", e),
            }
        }

        // Destination Node reporting listen address via relay.
        loop {
            match dst_swarm.next_event().await {
                SwarmEvent::NewListenAddr(addr) if addr == dst_addr_via_relay => break,
                SwarmEvent::Behaviour(CombinedEvent::Ping(_)) => {}
                SwarmEvent::Behaviour(CombinedEvent::Kad(KademliaEvent::RoutingUpdated {
                    ..
                })) => {}
                e => panic!("{:?}", e),
            }
        }

        // Destination Node bootstrapping.
        let query_id = dst_swarm.behaviour_mut().kad.bootstrap().unwrap();
        loop {
            match dst_swarm.next_event().await {
                SwarmEvent::Behaviour(CombinedEvent::Kad(KademliaEvent::QueryResult {
                    id,
                    result: QueryResult::Bootstrap(Ok(_)),
                    ..
                })) if query_id == id => {
                    if dst_swarm.behaviour_mut().kad.iter_queries().count() == 0 {
                        break;
                    }
                }
                SwarmEvent::Behaviour(CombinedEvent::Ping(_)) => {}
                SwarmEvent::Behaviour(CombinedEvent::Kad(KademliaEvent::RoutingUpdated {
                    ..
                })) => {}
                e => panic!("{:?}", e),
            }
        }

        let dst = async move {
            // Destination Node receiving connection from Source Node via Relay.
            loop {
                match dst_swarm.next_event().await {
                    SwarmEvent::IncomingConnection { send_back_addr, .. } => {
                        assert_eq!(send_back_addr, Protocol::P2p(src_peer_id.into()).into());
                        break;
                    }
                    SwarmEvent::Behaviour(CombinedEvent::Ping(_)) => {}
                    e => panic!("{:?}", e),
                }
            }

            // Destination Node establishing connection from Source Node via Relay.
            loop {
                match dst_swarm.next_event().await {
                    SwarmEvent::ConnectionEstablished { peer_id, .. } if peer_id == src_peer_id => {
                        break;
                    }
                    SwarmEvent::Behaviour(CombinedEvent::Ping(_)) => {}
                    e => panic!("{:?}", e),
                }
            }

            // Destination Node waiting for Source Node to close connection.
            loop {
                match dst_swarm.next_event().await {
                    SwarmEvent::Behaviour(CombinedEvent::Ping(_)) => {}
                    SwarmEvent::ConnectionClosed { peer_id, .. } if peer_id == relay_peer_id => {}
                    SwarmEvent::ConnectionClosed { peer_id, .. } if peer_id == src_peer_id => {
                        break;
                    }
                    SwarmEvent::Behaviour(CombinedEvent::Kad(_)) => {}
                    e => panic!("{:?}", e),
                }
            }
        };

        let src = async move {
            // Source Node looking for Destination Node on the Kademlia DHT.
            let mut query_id = src_swarm.behaviour_mut().kad.get_closest_peers(dst_peer_id);
            // One has to retry multiple times to wait for Relay to receive Identify event from Node
            // B.
            let mut tries = 0;

            // Source Node establishing connection to Relay and, given that the DHT is small, Node
            // B.
            loop {
                match src_swarm.next_event().await {
                    SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        if peer_id == relay_peer_id {
                            continue;
                        } else if peer_id == dst_peer_id {
                            break;
                        } else {
                            panic!("Unexpected peer id {:?}", peer_id);
                        }
                    }
                    SwarmEvent::Dialing(peer_id)
                        if peer_id == relay_peer_id || peer_id == dst_peer_id => {}
                    SwarmEvent::Behaviour(CombinedEvent::Ping(_)) => {}
                    SwarmEvent::Behaviour(CombinedEvent::Kad(KademliaEvent::QueryResult {
                        id,
                        result: QueryResult::GetClosestPeers(Ok(GetClosestPeersOk { .. })),
                        ..
                    })) if id == query_id => {
                        tries += 1;
                        if tries > 300 {
                            panic!("Too many retries.");
                        }

                        query_id = src_swarm.behaviour_mut().kad.get_closest_peers(dst_peer_id);
                    }
                    SwarmEvent::Behaviour(CombinedEvent::Kad(KademliaEvent::RoutingUpdated {
                        ..
                    })) => {}
                    e => panic!("{:?}", e),
                }
            }

            // Source Node waiting for Ping from Destination Node via Relay.
            loop {
                match src_swarm.next_event().await {
                    SwarmEvent::Behaviour(CombinedEvent::Ping(PingEvent {
                        peer,
                        result: Ok(_),
                    })) => {
                        if peer == dst_peer_id {
                            break;
                        }
                    }
                    SwarmEvent::Behaviour(CombinedEvent::Kad(KademliaEvent::RoutingUpdated {
                        ..
                    })) => {}
                    e => panic!("{:?}", e),
                }
            }
        };

        futures::future::join(dst, src).await
    });
}

#[test]
fn inactive_connection_timeout() {
    let _ = env_logger::try_init();

    let mut pool = LocalPool::new();

    let mut src_swarm = build_keep_alive_swarm();
    let mut dst_swarm = build_keep_alive_swarm();
    let mut relay_swarm = build_keep_alive_swarm();

    // Connections only kept alive by Source Node and Destination Node.
    relay_swarm.behaviour_mut().keep_alive.keep_alive = KeepAlive::No;

    let relay_peer_id = *relay_swarm.local_peer_id();
    let dst_peer_id = *dst_swarm.local_peer_id();

    let relay_addr: Multiaddr = Protocol::Memory(rand::random::<u64>()).into();
    let dst_addr_via_relay = relay_addr
        .clone()
        .with(Protocol::P2p(relay_peer_id.clone().into()))
        .with(Protocol::P2pCircuit)
        .with(Protocol::P2p(dst_peer_id.clone().into()));

    relay_swarm.listen_on(relay_addr.clone()).unwrap();
    spawn_swarm_on_pool(&pool, relay_swarm);

    dst_swarm.listen_on(dst_addr_via_relay.clone()).unwrap();
    // Wait for destination to listen via relay.
    pool.run_until(async {
        loop {
            match dst_swarm.next_event().await {
                SwarmEvent::Dialing(_) => {}
                SwarmEvent::ConnectionEstablished { .. } => {}
                SwarmEvent::NewListenAddr(addr) if addr == dst_addr_via_relay => break,
                e => panic!("{:?}", e),
            }
        }
    });
    spawn_swarm_on_pool(&pool, dst_swarm);

    pool.run_until(async move {
        src_swarm.dial_addr(relay_addr).unwrap();
        // Source Node dialing Relay.
        loop {
            match src_swarm.next_event().await {
                SwarmEvent::Dialing(peer_id) if peer_id == relay_peer_id => {}
                SwarmEvent::ConnectionEstablished { peer_id, .. } if peer_id == relay_peer_id => {
                    break;
                }
                e => panic!("{:?}", e),
            }
        }

        src_swarm.dial_addr(dst_addr_via_relay).unwrap();

        // Source Node establishing connection to destination node via Relay.
        match src_swarm.next_event().await {
            SwarmEvent::ConnectionEstablished { peer_id, .. } if peer_id == dst_peer_id => {}
            e => panic!("{:?}", e),
        }

        // Relay should notice connection between Source Node and B to be idle. It should then close the
        // relayed connection and eventually also close the connection to Source Node given that no
        // connections are being relayed on the connection.
        loop {
            match src_swarm.next_event().await {
                SwarmEvent::ConnectionClosed { peer_id, .. } => {
                    if peer_id == relay_peer_id {
                        break;
                    }
                }
                e => panic!("{:?}", e),
            }
        }
    });
}

#[test]
fn concurrent_connection_same_relay_same_dst() {
    let _ = env_logger::try_init();

    let mut pool = LocalPool::new();

    let mut src_swarm = build_swarm(Reachability::Firewalled, RelayMode::Passive);
    let mut dst_swarm = build_swarm(Reachability::Routable, RelayMode::Passive);
    let mut relay_swarm = build_swarm(Reachability::Routable, RelayMode::Passive);

    let relay_peer_id = *relay_swarm.local_peer_id();
    let dst_peer_id = *dst_swarm.local_peer_id();

    let relay_addr: Multiaddr = Protocol::Memory(rand::random::<u64>()).into();
    let dst_addr_via_relay = relay_addr
        .clone()
        .with(Protocol::P2p(relay_peer_id.clone().into()))
        .with(Protocol::P2pCircuit)
        .with(Protocol::P2p(dst_peer_id.clone().into()));

    relay_swarm.listen_on(relay_addr.clone()).unwrap();
    spawn_swarm_on_pool(&pool, relay_swarm);

    dst_swarm.listen_on(dst_addr_via_relay.clone()).unwrap();
    // Wait for destination to listen via relay.
    pool.run_until(async {
        loop {
            match dst_swarm.next_event().await {
                SwarmEvent::Dialing(_) => {}
                SwarmEvent::ConnectionEstablished { .. } => {}
                SwarmEvent::NewListenAddr(addr) if addr == dst_addr_via_relay => break,
                e => panic!("{:?}", e),
            }
        }
    });
    spawn_swarm_on_pool(&pool, dst_swarm);

    pool.run_until(async move {
        src_swarm.dial_addr(dst_addr_via_relay.clone()).unwrap();
        src_swarm.dial_addr(dst_addr_via_relay).unwrap();

        // Source Node establishing two connections to destination node via Relay.
        let mut num_established = 0;
        loop {
            match src_swarm.next_event().await {
                SwarmEvent::ConnectionEstablished { peer_id, .. } if peer_id == relay_peer_id => {}
                SwarmEvent::ConnectionEstablished { peer_id, .. } if peer_id == dst_peer_id => {
                    num_established += 1;
                    if num_established == 2 {
                        break;
                    }
                }
                SwarmEvent::Behaviour(CombinedEvent::Ping(_)) => {}
                SwarmEvent::Dialing(peer_id) => {
                    assert_eq!(peer_id, relay_peer_id);
                }
                e => panic!("{:?}", e),
            }
        }
    });
}

/// Yield incoming connection through listener that listens via the relay node used by the
/// connection. In case the local node does not listen via the specific relay node, but has
/// registered a listener for all remaining incoming relayed connections, yield the connection via
/// that _catch-all_ listener. Drop the connection in all other cases.
///
/// ## Nodes
///
/// - Destination node explicitly listening via relay node 1 and 2.
///
/// - 3 relay nodes
///
/// - Source node 1 and 2 connecting to destination node via relay 1 and 2 respectively.
///
/// - Source node 3 connecting to destination node via relay 3, expecting first connection attempt
///   to fail as destination node is not listening for incoming connections from any relay node.
///   Expecting second connection attempt to succeed after destination node registered listener for
///   any incoming relayed connection.
#[test]
fn yield_incoming_connection_through_correct_listener() {
    let _ = env_logger::try_init();
    let mut pool = LocalPool::new();

    let mut dst_swarm = build_swarm(Reachability::Routable, RelayMode::Passive);
    let mut src_1_swarm = build_swarm(Reachability::Routable, RelayMode::Passive);
    let mut src_2_swarm = build_swarm(Reachability::Routable, RelayMode::Passive);
    let mut src_3_swarm = build_swarm(Reachability::Routable, RelayMode::Passive);
    let mut relay_1_swarm = build_swarm(Reachability::Routable, RelayMode::Passive);
    let mut relay_2_swarm = build_swarm(Reachability::Routable, RelayMode::Passive);
    let mut relay_3_swarm = build_swarm(Reachability::Routable, RelayMode::Active);

    let dst_peer_id = *dst_swarm.local_peer_id();
    let src_1_peer_id = *src_1_swarm.local_peer_id();
    let src_2_peer_id = *src_2_swarm.local_peer_id();
    let src_3_peer_id = *src_3_swarm.local_peer_id();
    let relay_1_peer_id = *relay_1_swarm.local_peer_id();
    let relay_2_peer_id = *relay_2_swarm.local_peer_id();
    let relay_3_peer_id = *relay_3_swarm.local_peer_id();

    let dst_memory_port = Protocol::Memory(rand::random::<u64>());
    let dst_addr = Multiaddr::empty().with(dst_memory_port.clone());

    let relay_1_addr = Multiaddr::empty().with(Protocol::Memory(rand::random::<u64>()));
    let relay_1_addr_incl_circuit = relay_1_addr
        .clone()
        .with(Protocol::P2p(relay_1_peer_id.into()))
        .with(Protocol::P2pCircuit);
    let dst_addr_via_relay_1 = relay_1_addr_incl_circuit
        .clone()
        .with(Protocol::P2p(dst_peer_id.into()));
    let relay_2_addr = Multiaddr::empty().with(Protocol::Memory(rand::random::<u64>()));
    let relay_2_addr_incl_circuit = relay_2_addr
        .clone()
        .with(Protocol::P2p(relay_2_peer_id.into()))
        .with(Protocol::P2pCircuit);
    let dst_addr_via_relay_2 = relay_2_addr_incl_circuit
        .clone()
        .with(Protocol::P2p(dst_peer_id.into()));
    let relay_3_addr = Multiaddr::empty().with(Protocol::Memory(rand::random::<u64>()));
    let relay_3_addr_incl_circuit = relay_3_addr
        .clone()
        .with(Protocol::P2p(relay_3_peer_id.into()))
        .with(Protocol::P2pCircuit);
    let dst_addr_via_relay_3 = relay_3_addr_incl_circuit
        .clone()
        .with(dst_memory_port)
        .with(Protocol::P2p(dst_peer_id.into()));

    relay_1_swarm.listen_on(relay_1_addr.clone()).unwrap();
    spawn_swarm_on_pool(&pool, relay_1_swarm);

    relay_2_swarm.listen_on(relay_2_addr.clone()).unwrap();
    spawn_swarm_on_pool(&pool, relay_2_swarm);

    relay_3_swarm.listen_on(relay_3_addr.clone()).unwrap();
    spawn_swarm_on_pool(&pool, relay_3_swarm);

    dst_swarm.listen_on(relay_1_addr_incl_circuit.clone()).unwrap();
    dst_swarm.listen_on(relay_2_addr_incl_circuit.clone()).unwrap();
    // Listen on own address in order for relay 3 to be able to connect to destination node.
    dst_swarm.listen_on(dst_addr.clone()).unwrap();

    // Wait for destination node to establish connections to relay 1 and 2.
    pool.run_until(async {
        let mut established = 0;
        loop {
            match dst_swarm.next_event().await {
                SwarmEvent::Dialing(peer_id)
                    if peer_id == relay_1_peer_id || peer_id == relay_2_peer_id => {}
                SwarmEvent::ConnectionEstablished { peer_id, .. }
                    if peer_id == relay_1_peer_id || peer_id == relay_2_peer_id =>
                {
                    established += 1;
                    if established == 2 {
                        break;
                    }
                }
                SwarmEvent::NewListenAddr(addr)
                    if addr == relay_1_addr_incl_circuit
                        || addr == relay_2_addr_incl_circuit
                        || addr == dst_addr => {}
                SwarmEvent::Behaviour(CombinedEvent::Ping(_)) => {}
                e => panic!("{:?}", e),
            }
        }
    });

    src_1_swarm.dial_addr(dst_addr_via_relay_1.clone()).unwrap();
    src_2_swarm.dial_addr(dst_addr_via_relay_2.clone()).unwrap();
    spawn_swarm_on_pool(&pool, src_1_swarm);
    spawn_swarm_on_pool(&pool, src_2_swarm);

    // Expect source node 1 and 2 to reach destination node via relay 1 and 2 respectively.
    pool.run_until(async {
        let mut src_1_established = false;
        let mut src_2_established = false;
        let mut src_1_ping = false;
        let mut src_2_ping = false;
        loop {
            match dst_swarm.next_event().await {
                SwarmEvent::IncomingConnection { .. } => {}
                SwarmEvent::ConnectionEstablished {
                    peer_id, endpoint, ..
                } => {
                    let local_addr = match endpoint {
                        ConnectedPoint::Dialer { .. } => unreachable!(),
                        ConnectedPoint::Listener { local_addr, .. } => local_addr,
                    };

                    if peer_id == src_1_peer_id {
                        assert_eq!(local_addr, relay_1_addr_incl_circuit);
                        src_1_established = true;
                    } else if peer_id == src_2_peer_id {
                        assert_eq!(local_addr, relay_2_addr_incl_circuit);
                        src_2_established = true;
                    } else {
                        unreachable!();
                    }
                }
                SwarmEvent::NewListenAddr(addr)
                    if addr == relay_1_addr_incl_circuit
                        || addr == relay_2_addr_incl_circuit
                        || addr == dst_addr => {}
                SwarmEvent::Behaviour(CombinedEvent::Ping(PingEvent {
                    peer,
                    result: Ok(_),
                })) => {
                    if peer == src_1_peer_id {
                        src_1_ping = true;
                    } else if peer == src_2_peer_id {
                        src_2_ping = true;
                    }
                }
                SwarmEvent::Behaviour(CombinedEvent::Kad(KademliaEvent::RoutingUpdated {
                    ..
                })) => {}
                e => panic!("{:?}", e),
            }

            if src_1_established && src_2_established && src_1_ping && src_2_ping {
                break;
            }
        }
    });

    // Expect destination node to reject incoming connection from unknown relay given that
    // destination node is not listening for such connections.
    src_3_swarm.dial_addr(dst_addr_via_relay_3.clone()).unwrap();
    pool.run_until(poll_fn(|cx| {
        match dst_swarm.next_event().boxed().poll_unpin(cx) {
            Poll::Ready(SwarmEvent::Behaviour(CombinedEvent::Ping(_))) => {}
            Poll::Ready(SwarmEvent::IncomingConnection { .. }) => {}
            Poll::Ready(SwarmEvent::ConnectionEstablished { peer_id, .. })
                if peer_id == relay_3_peer_id => {}
            Poll::Ready(SwarmEvent::ConnectionEstablished { peer_id, .. })
                if peer_id == src_3_peer_id =>
            {
                panic!(
                    "Expected destination node to reject incoming connection from unknown relay \
                     without a catch-all listener",
                );
            }
            Poll::Pending => {}
            e => panic!("{:?}", e),
        }

        match src_3_swarm.next_event().boxed().poll_unpin(cx) {
            Poll::Ready(SwarmEvent::UnreachableAddr { address, peer_id, .. })
                if address == dst_addr_via_relay_3 =>
            {
                assert_eq!(peer_id, dst_peer_id);
                return Poll::Ready(());
            }
            Poll::Ready(SwarmEvent::Dialing { .. }) => {}
            Poll::Ready(SwarmEvent::ConnectionEstablished { peer_id, .. })
                if peer_id == relay_3_peer_id => {}
            Poll::Ready(SwarmEvent::ConnectionEstablished { peer_id, .. })
                if peer_id == dst_peer_id =>
            {
                panic!(
                    "Expected destination node to reject incoming connection from unknown relay \
                     without a catch-all listener",
                );
            }
            Poll::Ready(SwarmEvent::Behaviour(CombinedEvent::Ping(_))) => {}
            Poll::Pending => {}
            e => panic!("{:?}", e),
        }

        Poll::Pending
    }));

    // Instruct destination node to listen for incoming relayed connections from unknown relay nodes.
    dst_swarm.listen_on(Protocol::P2pCircuit.into()).unwrap();
    // Wait for destination node to report new listen address.
    pool.run_until(async {
        loop {
            match dst_swarm.next_event().await {
                SwarmEvent::NewListenAddr(addr) if addr == Protocol::P2pCircuit.into() => break,
                SwarmEvent::Behaviour(CombinedEvent::Ping(_)) => {}
                SwarmEvent::Behaviour(CombinedEvent::Kad(KademliaEvent::RoutingUpdated {
                    ..
                })) => {}
                e => panic!("{:?}", e),
            }
        }
    });
    spawn_swarm_on_pool(&pool, dst_swarm);

    // Expect destination node to accept incoming connection from "unknown" relay, i.e. the
    // connection from source node 3 via relay 3.
    src_3_swarm.dial_addr(dst_addr_via_relay_3.clone()).unwrap();
    pool.run_until(async move {
        loop {
            match src_3_swarm.next_event().await {
                SwarmEvent::Dialing(_) => {}
                SwarmEvent::ConnectionEstablished { peer_id, .. } if peer_id == dst_peer_id => {
                    break
                }
                SwarmEvent::Behaviour(CombinedEvent::Ping(_)) => {}
                SwarmEvent::Behaviour(CombinedEvent::Kad(KademliaEvent::RoutingUpdated {
                    ..
                })) => {}
                e => panic!("{:?}", e),
            }
        }
    });
}

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "CombinedEvent", poll_method = "poll")]
struct CombinedBehaviour {
    relay: Relay,
    ping: Ping,
    kad: Kademlia<MemoryStore>,
    identify: Identify,

    #[behaviour(ignore)]
    events: Vec<CombinedEvent>,
}

#[derive(Debug)]
enum CombinedEvent {
    Kad(KademliaEvent),
    Ping(PingEvent),
}

impl CombinedBehaviour {
    fn poll<TEv>(
        &mut self,
        _: &mut Context,
        _: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<TEv, CombinedEvent>> {
        if !self.events.is_empty() {
            return Poll::Ready(NetworkBehaviourAction::GenerateEvent(self.events.remove(0)));
        }

        Poll::Pending
    }
}

impl NetworkBehaviourEventProcess<PingEvent> for CombinedBehaviour {
    fn inject_event(&mut self, event: PingEvent) {
        self.events.push(CombinedEvent::Ping(event));
    }
}

impl NetworkBehaviourEventProcess<KademliaEvent> for CombinedBehaviour {
    fn inject_event(&mut self, event: KademliaEvent) {
        self.events.push(CombinedEvent::Kad(event));
    }
}

impl NetworkBehaviourEventProcess<IdentifyEvent> for CombinedBehaviour {
    fn inject_event(&mut self, event: IdentifyEvent) {
        if let IdentifyEvent::Received {
            peer_id,
            info: IdentifyInfo { listen_addrs, .. },
            ..
        } = event
        {
            for addr in listen_addrs {
                self.kad.add_address(&peer_id, addr);
            }
        }
    }
}

impl NetworkBehaviourEventProcess<()> for CombinedBehaviour {
    fn inject_event(&mut self, _event: ()) {
        unreachable!();
    }
}

#[derive(NetworkBehaviour)]
struct CombinedKeepAliveBehaviour {
    relay: Relay,
    keep_alive: KeepAliveBehaviour,
}

impl NetworkBehaviourEventProcess<()> for CombinedKeepAliveBehaviour {
    fn inject_event(&mut self, _event: ()) {
        unreachable!();
    }
}

impl NetworkBehaviourEventProcess<Void> for CombinedKeepAliveBehaviour {
    fn inject_event(&mut self, _event: Void) {
        unreachable!();
    }
}

enum Reachability {
    Firewalled,
    Routable,
}

enum RelayMode {
    Active,
    Passive,
}

impl From<RelayMode> for bool {
    fn from(relay: RelayMode) -> Self {
        match relay {
            RelayMode::Active => true,
            RelayMode::Passive => false,
        }
    }
}

/// Wrapper around a [`Transport`] allowing outgoing but denying incoming connections.
///
/// Meant for testing purposes only.
#[derive(Clone)]
pub struct Firewall<T>(pub T);

impl<T: Transport> Transport for Firewall<T> {
    type Output = <T as Transport>::Output;
    type Error = <T as Transport>::Error;
    type Listener = futures::stream::Pending<<<T as Transport>::Listener as Stream>::Item>;
    type ListenerUpgrade = <T as Transport>::ListenerUpgrade;
    type Dial = <T as Transport>::Dial;

    fn listen_on(self, _: Multiaddr) -> Result<Self::Listener, TransportError<Self::Error>> {
        Ok(futures::stream::pending())
    }

    fn dial(self, addr: Multiaddr) -> Result<Self::Dial, TransportError<Self::Error>> {
        self.0.dial(addr)
    }

    fn address_translation(&self, server: &Multiaddr, observed: &Multiaddr) -> Option<Multiaddr> {
        self.0.address_translation(server, observed)
    }
}

fn build_swarm(reachability: Reachability, relay_mode: RelayMode) -> Swarm<CombinedBehaviour> {
    let local_key = identity::Keypair::generate_ed25519();
    let local_public_key = local_key.public();
    let plaintext = PlainText2Config {
        local_public_key: local_public_key.clone(),
    };
    let local_peer_id = local_public_key.clone().into_peer_id();

    let transport = MemoryTransport::default();

    let transport = match reachability {
        Reachability::Firewalled => EitherTransport::Left(Firewall(transport)),
        Reachability::Routable => EitherTransport::Right(transport),
    };

    let (transport, relay_behaviour) = libp2p_relay::new_transport_and_behaviour(
        RelayConfig {
            actively_connect_to_dst_nodes: relay_mode.into(),
            ..Default::default()
        },
        transport,
    );

    let transport = transport
        .upgrade(upgrade::Version::V1)
        .authenticate(plaintext)
        .multiplex(libp2p_yamux::YamuxConfig::default())
        .boxed();

    let combined_behaviour = CombinedBehaviour {
        relay: relay_behaviour,
        ping: Ping::new(PingConfig::new().with_interval(Duration::from_millis(100))),
        kad: Kademlia::new(
            local_peer_id.clone(),
            MemoryStore::new(local_peer_id.clone()),
        ),
        identify: Identify::new(IdentifyConfig::new(
            "test".to_string(),
            local_public_key.clone(),
        )),
        events: Default::default(),
    };

    Swarm::new(transport, combined_behaviour, local_peer_id)
}

fn build_keep_alive_swarm() -> Swarm<CombinedKeepAliveBehaviour> {
    let local_key = identity::Keypair::generate_ed25519();
    let local_public_key = local_key.public();
    let plaintext = PlainText2Config {
        local_public_key: local_public_key.clone(),
    };
    let local_peer_id = local_public_key.clone().into_peer_id();

    let transport = MemoryTransport::default();

    let (transport, relay_behaviour) =
        libp2p_relay::new_transport_and_behaviour(RelayConfig::default(), transport);

    let transport = transport
        .upgrade(upgrade::Version::V1)
        .authenticate(plaintext)
        .multiplex(libp2p_yamux::YamuxConfig::default())
        .boxed();

    let combined_behaviour = CombinedKeepAliveBehaviour {
        relay: relay_behaviour,
        keep_alive: KeepAliveBehaviour::default(),
    };

    Swarm::new(transport, combined_behaviour, local_peer_id)
}

fn build_keep_alive_only_swarm() -> Swarm<KeepAliveBehaviour> {
    let local_key = identity::Keypair::generate_ed25519();
    let local_public_key = local_key.public();
    let plaintext = PlainText2Config {
        local_public_key: local_public_key.clone(),
    };
    let local_peer_id = local_public_key.clone().into_peer_id();

    let transport = MemoryTransport::default();

    let transport = transport
        .upgrade(upgrade::Version::V1)
        .authenticate(plaintext)
        .multiplex(libp2p_yamux::YamuxConfig::default())
        .boxed();

    Swarm::new(transport, KeepAliveBehaviour::default(), local_peer_id)
}

#[derive(Clone)]
pub struct KeepAliveBehaviour {
    keep_alive: KeepAlive,
}

impl Default for KeepAliveBehaviour {
    fn default() -> Self {
        Self {
            keep_alive: KeepAlive::Yes,
        }
    }
}

impl libp2p_swarm::NetworkBehaviour for KeepAliveBehaviour {
    type ProtocolsHandler = KeepAliveProtocolsHandler;
    type OutEvent = void::Void;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        KeepAliveProtocolsHandler {
            keep_alive: self.keep_alive,
        }
    }

    fn addresses_of_peer(&mut self, _: &PeerId) -> Vec<Multiaddr> {
        Vec::new()
    }

    fn inject_connected(&mut self, _: &PeerId) {}

    fn inject_connection_established(&mut self, _: &PeerId, _: &ConnectionId, _: &ConnectedPoint) {}

    fn inject_disconnected(&mut self, _: &PeerId) {}

    fn inject_connection_closed(&mut self, _: &PeerId, _: &ConnectionId, _: &ConnectedPoint) {}

    fn inject_event(
        &mut self,
        _: PeerId,
        _: ConnectionId,
        _: <Self::ProtocolsHandler as ProtocolsHandler>::OutEvent,
    ) {
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
        Poll::Pending
    }
}

/// Implementation of `ProtocolsHandler` that doesn't handle anything.
#[derive(Clone, Debug)]
pub struct KeepAliveProtocolsHandler {
    pub keep_alive: KeepAlive,
}

impl ProtocolsHandler for KeepAliveProtocolsHandler {
    type InEvent = Void;
    type OutEvent = Void;
    type Error = Void;
    type InboundProtocol = DeniedUpgrade;
    type OutboundProtocol = DeniedUpgrade;
    type OutboundOpenInfo = Void;
    type InboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(DeniedUpgrade, ())
    }

    fn inject_fully_negotiated_inbound(
        &mut self,
        _: <Self::InboundProtocol as InboundUpgrade<NegotiatedSubstream>>::Output,
        _: Self::InboundOpenInfo,
    ) {
    }

    fn inject_fully_negotiated_outbound(
        &mut self,
        _: <Self::OutboundProtocol as OutboundUpgrade<NegotiatedSubstream>>::Output,
        _: Self::OutboundOpenInfo,
    ) {
    }

    fn inject_event(&mut self, _: Self::InEvent) {}

    fn inject_address_change(&mut self, _: &Multiaddr) {}

    fn inject_dial_upgrade_error(
        &mut self,
        _: Self::OutboundOpenInfo,
        _: ProtocolsHandlerUpgrErr<
            <Self::OutboundProtocol as OutboundUpgrade<NegotiatedSubstream>>::Error,
        >,
    ) {
    }

    fn inject_listen_upgrade_error(
        &mut self,
        _: Self::InboundOpenInfo,
        _: ProtocolsHandlerUpgrErr<
            <Self::InboundProtocol as InboundUpgrade<NegotiatedSubstream>>::Error,
        >,
    ) {
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        self.keep_alive
    }

    fn poll(
        &mut self,
        _: &mut Context<'_>,
    ) -> Poll<
        ProtocolsHandlerEvent<
            Self::OutboundProtocol,
            Self::OutboundOpenInfo,
            Self::OutEvent,
            Self::Error,
        >,
    > {
        Poll::Pending
    }
}

struct DummyPollParameters {}

impl PollParameters for DummyPollParameters {
    type SupportedProtocolsIter = iter::Empty<Vec<u8>>;
    type ListenedAddressesIter = iter::Empty<Multiaddr>;
    type ExternalAddressesIter = iter::Empty<AddressRecord>;

    fn supported_protocols(&self) -> Self::SupportedProtocolsIter {
        unimplemented!();
    }

    /// Returns the list of the addresses we're listening on.
    fn listened_addresses(&self) -> Self::ListenedAddressesIter {
        unimplemented!();
    }

    /// Returns the list of the addresses nodes can use to reach us.
    fn external_addresses(&self) -> Self::ExternalAddressesIter {
        unimplemented!();
    }

    /// Returns the peer id of the local node.
    fn local_peer_id(&self) -> &PeerId {
        unimplemented!();
    }
}

fn spawn_swarm_on_pool<B: NetworkBehaviour>(pool: &LocalPool, mut swarm: Swarm<B>) {
    pool.spawner()
        .spawn_obj(
            async move {
                loop {
                    swarm.next_event().await;
                }
            }
            .boxed()
            .into(),
        )
        .unwrap();
}
