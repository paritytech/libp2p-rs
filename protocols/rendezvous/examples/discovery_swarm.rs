use async_std::task;
use futures::executor::block_on;
use futures::{future, FutureExt, StreamExt};
use libp2p_core::multihash::Multihash;
use libp2p_core::muxing::StreamMuxerBox;
use libp2p_core::transport::MemoryTransport;
use libp2p_core::upgrade::{SelectUpgrade, Version};
use libp2p_core::PeerId;
use libp2p_core::{identity, Transport};
use libp2p_mplex::MplexConfig;
use libp2p_noise::NoiseConfig;
use libp2p_noise::{Keypair, X25519Spec};
use libp2p_rendezvous::behaviour::{Difficulty, Event, Rendezvous};
use libp2p_rendezvous::{behaviour, codec};
use libp2p_swarm::SwarmEvent;
use libp2p_swarm::{Swarm, SwarmBuilder};
use libp2p_tcp::TcpConfig;
use libp2p_yamux::YamuxConfig;
use std::error::Error;
use std::str::FromStr;
use std::task::Poll;
use std::time::Duration;
use Event::Discovered;

fn main() {
    let identity = identity::Keypair::generate_ed25519();
    let peer_id = PeerId::from(identity.public());

    let dh_keys = Keypair::<X25519Spec>::new()
        .into_authentic(&identity)
        .expect("failed to create dh_keys");
    let noise_config = libp2p_noise::NoiseConfig::xx(dh_keys).into_authenticated();

    let tcp_config = TcpConfig::new();
    let transport = tcp_config
        .upgrade(Version::V1)
        .authenticate(noise_config)
        .multiplex(SelectUpgrade::new(
            YamuxConfig::default(),
            MplexConfig::new(),
        ))
        .timeout(Duration::from_secs(20))
        .map(|(peer, muxer), _| (peer, StreamMuxerBox::new(muxer)))
        .boxed();

    let difficulty = Difficulty::from_u32(1).unwrap();
    let behaviour = Rendezvous::new(identity, 1000, difficulty);

    let mut swarm = Swarm::new(transport, behaviour, peer_id);

    swarm.dial_addr("/ip4/127.0.0.1/tcp/62649".parse().unwrap());

    let server_peer_id =
        PeerId::from_str("12D3KooWCagEX1wyQekJvzPGgBXuqt43LdaqxQj2VN64AyEh13vM").unwrap();

    task::block_on(async move {
        loop {
            let event = swarm.next_event().await;
            println!("swarm event: {:?}", event);
            if let SwarmEvent::ConnectionEstablished { peer_id, .. } = event {
                println!("connection establihed: {:?}", peer_id);
                swarm.behaviour_mut().discover(
                    Some("rendezvous".to_string()),
                    None,
                    server_peer_id,
                );
            };
            if let SwarmEvent::Behaviour(Discovered { registrations, .. }) = event {
                println!("discovered: {:?}", registrations.values());
            };
        }
    })
}