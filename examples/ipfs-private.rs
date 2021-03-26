// Copyright 2020 Parity Technologies (UK) Ltd.
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

//! A minimal node that can interact with ipfs
//!
//! This node implements the gossipsub, ping and identify protocols. It supports
//! the ipfs private swarms feature by reading the pre shared key file `swarm.key`
//! from the IPFS_PATH environment variable or from the default location.
//!
//! You can pass any number of nodes to be dialed.
//!
//! On startup, this example will show a list of addresses that you can dial
//! from a go-ipfs or js-ipfs node.
//!
//! You can ping this node, or use pubsub (gossipsub) on the topic "chat". For this
//! to work, the ipfs node needs to be configured to use gossipsub.
use async_std::{io, task};
use futures::{future, prelude::*};
use libp2p::{
    core::{
        either::EitherTransport, muxing::StreamMuxerBox, transport, transport::upgrade::Version,
    },
    gossipsub::{self, Gossipsub, GossipsubConfigBuilder, GossipsubEvent, MessageAuthenticity},
    identify::{Identify, IdentifyConfig, IdentifyEvent},
    identity,
    multiaddr::Protocol,
    noise,
    ping::{self, Ping, PingConfig, PingEvent},
    pnet::{PnetConfig, PreSharedKey},
    swarm::NetworkBehaviourEventProcess,
    tcp::TcpConfig,
    yamux::YamuxConfig,
    Multiaddr, NetworkBehaviour, PeerId, Swarm, Transport,
};
use std::{
    env,
    error::Error,
    fs,
    path::Path,
    str::FromStr,
    task::{Context, Poll},
    time::Duration,
};

/// Builds the transport that serves as a common ground for all connections.
pub fn build_transport(
    key_pair: identity::Keypair,
    psk: Option<PreSharedKey>,
) -> transport::Boxed<(PeerId, StreamMuxerBox)> {
    let noise_keys = noise::Keypair::<noise::X25519Spec>::new()
        .into_authentic(&key_pair)
        .unwrap();
    let noise_config = noise::NoiseConfig::xx(noise_keys).into_authenticated();
    let yamux_config = YamuxConfig::default();

    let base_transport = TcpConfig::new().nodelay(true);
    let maybe_encrypted = match psk {
        Some(psk) => EitherTransport::Left(
            base_transport.and_then(move |socket, _| PnetConfig::new(psk).handshake(socket)),
        ),
        None => EitherTransport::Right(base_transport),
    };
    maybe_encrypted
        .upgrade(Version::V1)
        .authenticate(noise_config)
        .multiplex(yamux_config)
        .timeout(Duration::from_secs(20))
        .boxed()
}

/// Get the current ipfs repo path, either from the IPFS_PATH environment variable or
/// from the default $HOME/.ipfs
fn get_ipfs_path() -> Box<Path> {
    env::var("IPFS_PATH")
        .map(|ipfs_path| Path::new(&ipfs_path).into())
        .unwrap_or_else(|_| {
            env::var("HOME")
                .map(|home| Path::new(&home).join(".ipfs"))
                .expect("could not determine home directory")
                .into()
        })
}

/// Read the pre shared key file from the given ipfs directory
fn get_psk(path: Box<Path>) -> std::io::Result<Option<String>> {
    let swarm_key_file = path.join("swarm.key");
    match fs::read_to_string(swarm_key_file) {
        Ok(text) => Ok(Some(text)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// for a multiaddr that ends with a peer id, this strips this suffix. Rust-libp2p
/// only supports dialing to an address without providing the peer id.
fn strip_peer_id(addr: &mut Multiaddr) {
    let last = addr.pop();
    match last {
        Some(Protocol::P2p(peer_id)) => {
            let mut addr = Multiaddr::empty();
            addr.push(Protocol::P2p(peer_id));
            println!(
                "removing peer id {} so this address can be dialed by rust-libp2p",
                addr
            );
        }
        Some(other) => addr.push(other),
        _ => {}
    }
}

/// parse a legacy multiaddr (replace ipfs with p2p), and strip the peer id
/// so it can be dialed by rust-libp2p
fn parse_legacy_multiaddr(text: &str) -> Result<Multiaddr, Box<dyn Error>> {
    let sanitized = text
        .split('/')
        .map(|part| if part == "ipfs" { "p2p" } else { part })
        .collect::<Vec<_>>()
        .join("/");
    let mut res = Multiaddr::from_str(&sanitized)?;
    strip_peer_id(&mut res);
    Ok(res)
}

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let ipfs_path: Box<Path> = get_ipfs_path();
    println!("using IPFS_PATH {:?}", ipfs_path);
    let psk: Option<PreSharedKey> = get_psk(ipfs_path)?
        .map(|text| PreSharedKey::from_str(&text))
        .transpose()?;

    // Create a random PeerId
    let local_key = identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(local_key.public());
    println!("using random peer id: {:?}", local_peer_id);
    for psk in psk {
        println!("using swarm key with fingerprint: {}", psk.fingerprint());
    }

    // Set up a an encrypted DNS-enabled TCP Transport over and Yamux protocol
    let transport = build_transport(local_key.clone(), psk);

    // Create a Gosspipsub topic
    let gossipsub_topic = gossipsub::IdentTopic::new("chat");

    // We create a custom network behaviour that combines gossipsub, ping and identify.
    #[derive(NetworkBehaviour)]
    struct MyBehaviour {
        gossipsub: Gossipsub,
        identify: Identify,
        ping: Ping,
    }

    impl NetworkBehaviourEventProcess<IdentifyEvent> for MyBehaviour {
        // Called when `identify` produces an event.
        fn inject_event(&mut self, event: IdentifyEvent) {
            println!("identify: {:?}", event);
        }
    }

    impl NetworkBehaviourEventProcess<GossipsubEvent> for MyBehaviour {
        // Called when `gossipsub` produces an event.
        fn inject_event(&mut self, event: GossipsubEvent) {
            match event {
                GossipsubEvent::Message {
                    propagation_source: peer_id,
                    message_id: id,
                    message,
                } => println!(
                    "Got message: {} with id: {} from peer: {:?}",
                    String::from_utf8_lossy(&message.data),
                    id,
                    peer_id
                ),
                _ => {}
            }
        }
    }

    impl NetworkBehaviourEventProcess<PingEvent> for MyBehaviour {
        // Called when `ping` produces an event.
        fn inject_event(&mut self, event: PingEvent) {
            use ping::handler::{PingFailure, PingSuccess};
            match event {
                PingEvent {
                    peer,
                    result: Result::Ok(PingSuccess::Ping { rtt }),
                } => {
                    println!(
                        "ping: rtt to {} is {} ms",
                        peer.to_base58(),
                        rtt.as_millis()
                    );
                }
                PingEvent {
                    peer,
                    result: Result::Ok(PingSuccess::Pong),
                } => {
                    println!("ping: pong from {}", peer.to_base58());
                }
                PingEvent {
                    peer,
                    result: Result::Err(PingFailure::Timeout),
                } => {
                    println!("ping: timeout to {}", peer.to_base58());
                }
                PingEvent {
                    peer,
                    result: Result::Err(PingFailure::Other { error }),
                } => {
                    println!("ping: failure with {}: {}", peer.to_base58(), error);
                }
            }
        }
    }

    // Create a Swarm to manage peers and events
    let mut swarm = {
        let gossipsub_config = GossipsubConfigBuilder::default()
            .max_transmit_size(262144)
            .build()
            .expect("valid config");
        let mut behaviour = MyBehaviour {
            gossipsub: Gossipsub::new(
                MessageAuthenticity::Signed(local_key.clone()),
                gossipsub_config,
            )
            .expect("Valid configuration"),
            identify: Identify::new(IdentifyConfig::new(
                "/ipfs/0.1.0".into(),
                local_key.public(),
            )),
            ping: Ping::new(PingConfig::new()),
        };

        println!("Subscribing to {:?}", gossipsub_topic);
        behaviour.gossipsub.subscribe(&gossipsub_topic).unwrap();
        Swarm::new(transport, behaviour, local_peer_id.clone())
    };

    // Reach out to other nodes if specified
    for to_dial in std::env::args().skip(1) {
        let addr: Multiaddr = parse_legacy_multiaddr(&to_dial)?;
        swarm.dial_addr(addr)?;
        println!("Dialed {:?}", to_dial)
    }

    // Read full lines from stdin
    let mut stdin = io::BufReader::new(io::stdin()).lines();

    // Listen on all interfaces and whatever port the OS assigns
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    // Kick it off
    let mut listening = false;
    task::block_on(future::poll_fn(move |cx: &mut Context<'_>| {
        loop {
            if let Err(e) = match stdin.try_poll_next_unpin(cx)? {
                Poll::Ready(Some(line)) => swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(gossipsub_topic.clone(), line.as_bytes()),
                Poll::Ready(None) => panic!("Stdin closed"),
                Poll::Pending => break,
            } {
                println!("Publish error: {:?}", e);
            }
        }
        loop {
            match swarm.poll_next_unpin(cx) {
                Poll::Ready(Some(event)) => println!("{:?}", event),
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => {
                    if !listening {
                        for addr in Swarm::listeners(&swarm) {
                            println!("Address {}/ipfs/{}", addr, local_peer_id);
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
