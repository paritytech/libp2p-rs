// Copyright 2021 Protocol Labs.
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

use futures::executor::block_on;
use futures::stream::StreamExt;
use libp2p::core::multiaddr::{Multiaddr, Protocol};
use libp2p::core::upgrade;
use libp2p::dcutr;
use libp2p::identify::{Identify, IdentifyConfig, IdentifyEvent};
use libp2p::noise;
use libp2p::ping::{Ping, PingConfig, PingEvent};
use libp2p::relay::v2::relay::{self, Relay};
use libp2p::relay::v2::client::{self, Client};
use libp2p::dns::DnsConfig;
use libp2p::swarm::{AddressScore, Swarm, SwarmEvent};
use libp2p::tcp::TcpConfig;
use libp2p::Transport;
use libp2p::{identity, NetworkBehaviour, PeerId};
use std::error::Error;
use std::net::Ipv4Addr;
use std::task::{Context, Poll};

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let local_key = generate_ed25519(std::env::args().nth(1).unwrap().parse().unwrap());
    let local_peer_id = PeerId::from(local_key.public());
    println!("Local peer id: {:?}", local_peer_id);


    let (transport, client) = Client::new_transport_and_behaviour(local_peer_id, block_on(DnsConfig::system( TcpConfig::new().port_reuse(true))).unwrap());

    let noise_keys = noise::Keypair::<noise::X25519Spec>::new()
        .into_authentic(&local_key)
        .expect("Signing libp2p-noise static DH keypair failed.");

    let transport = transport
        .upgrade()
        .authenticate_with_version(noise::NoiseConfig::xx(noise_keys).into_authenticated(), upgrade::Version::V1SimOpen)
        .multiplex(libp2p_yamux::YamuxConfig::default())
        .boxed();

    #[derive(NetworkBehaviour)]
    #[behaviour(out_event = "Event", event_process = false)]
    struct Behaviour {
        relay_client: Client,
        ping: Ping,
        identify: Identify,
        dcutr: dcutr::behaviour::Behaviour,
    }

    #[derive(Debug)]
    enum Event {
        Ping(PingEvent),
        Identify(IdentifyEvent),
        Relay(client::Event),
        Dcutr(dcutr::behaviour::Event),
    }

    impl From<PingEvent> for Event {
        fn from(e: PingEvent) -> Self {
            Event::Ping(e)
        }
    }

    impl From<IdentifyEvent> for Event {
        fn from(e: IdentifyEvent) -> Self {
            Event::Identify(e)
        }
    }

    impl From<client::Event> for Event {
        fn from(e: client::Event) -> Self {
            Event::Relay(e)
        }
    }

    impl From<dcutr::behaviour::Event> for Event {
        fn from(e: dcutr::behaviour::Event) -> Self {
            Event::Dcutr(e)
        }
    }

    let behaviour = Behaviour {
        relay_client: client,
        ping: Ping::new(PingConfig::new()),
        identify: Identify::new(IdentifyConfig::new(
            "/TODO/0.0.1".to_string(),
            local_key.public(),
        )),
        dcutr: dcutr::behaviour::Behaviour::new(),
    };

    let mut swarm = Swarm::new(transport, behaviour, local_peer_id);

    if let (Some(external_ip), Some(port)) = (std::env::args().nth(2), std::env::args().nth(3)) {
        let listen_addr = Multiaddr::empty()
            .with("0.0.0.0".parse::<Ipv4Addr>().unwrap().into())
            .with(Protocol::Tcp(port.parse().unwrap()));
        swarm.listen_on(listen_addr)?;

        let external_addr = Multiaddr::empty()
            .with(external_ip.parse::<Ipv4Addr>().unwrap().into())
            .with(Protocol::Tcp(port.parse().unwrap()));
        swarm.add_external_address(external_addr, AddressScore::Infinite);
    } else {
        panic!();
    }

    if let (Some(mode), Some(addr)) = (std::env::args().nth(4), std::env::args().nth(5)) {
        match mode.as_str() {
            "dial" => {
                swarm.dial_addr(addr.parse().unwrap()).unwrap();
            }
            "listen" => {
                swarm.listen_on(addr.parse().unwrap()).unwrap();
            }
            s => panic!("Unexpected string {:?}", s),
        }
    } else {
        panic!();
    }

    let mut listening = false;
    block_on(futures::future::poll_fn(move |cx: &mut Context<'_>| {
        loop {
            match swarm.poll_next_unpin(cx) {
                Poll::Ready(Some(SwarmEvent::NewListenAddr(addr))) => {
                    println!("Listening on {:?}", addr);
                }
                Poll::Ready(Some(SwarmEvent::Behaviour(Event::Relay(event)))) => {
                    println!("{:?}", event)
                }
                Poll::Ready(Some(SwarmEvent::Behaviour(Event::Identify(event)))) => {
                    println!("{:?}", event)
                }
                Poll::Ready(Some(_)) => {}
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => {
                    if !listening {
                        for addr in Swarm::listeners(&swarm) {
                            println!("Listening on {:?}", addr);
                            listening = true;
                        }
                    }
                    break;
                }
            }
        }
        Poll::Pending
    }))
}

fn generate_ed25519(secret_key_seed: u8) -> identity::Keypair {
    let mut bytes = [0u8; 32];
    bytes[0] = secret_key_seed;

    let secret_key = identity::ed25519::SecretKey::from_bytes(&mut bytes)
        .expect("this returns `Err` only if the length is wrong; the length is correct; qed");
    identity::Keypair::Ed25519(secret_key.into())
}
