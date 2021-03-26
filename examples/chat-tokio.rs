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

//! A basic chat application demonstrating libp2p with the mDNS and floodsub protocols
//! using tokio for all asynchronous tasks and I/O. In order for all used libp2p
//! crates to use tokio, it enables tokio-specific features for some crates.
//!
//! The example is run per node as follows:
//!
//! ```sh
//! cargo run --example chat-tokio --features="tcp-tokio mdns-tokio"
//! ```
//!
//! Alternatively, to run with the minimal set of features and crates:
//!
//! ```sh
//!cargo run --example chat-tokio \\
//!    --no-default-features \\
//!    --features="floodsub mplex noise tcp-tokio mdns-tokio"
//! ```

use libp2p::{
    Multiaddr,
    NetworkBehaviour,
    PeerId,
    Swarm,
    Transport,
    core::upgrade,
    identity,
    floodsub::{self, Floodsub, FloodsubEvent},
    mdns::{Mdns, MdnsEvent},
    mplex,
    noise,
    swarm::{NetworkBehaviourEventProcess, SwarmBuilder},
    // `TokioTcpConfig` is available through the `tcp-tokio` feature.
    tcp::TokioTcpConfig,
};
use std::error::Error;
use tokio::io::{self, AsyncBufReadExt};

/// The `tokio::main` attribute sets up a tokio runtime.
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    // Create a random PeerId
    let id_keys = identity::Keypair::generate_ed25519();
    let peer_id = PeerId::from(id_keys.public());
    println!("Local peer id: {:?}", peer_id);

    // Create a keypair for authenticated encryption of the transport.
    let noise_keys = noise::Keypair::<noise::X25519Spec>::new()
        .into_authentic(&id_keys)
        .expect("Signing libp2p-noise static DH keypair failed.");

    // Create a tokio-based TCP transport use noise for authenticated
    // encryption and Mplex for multiplexing of substreams on a TCP stream.
    let transport = TokioTcpConfig::new().nodelay(true)
        .upgrade(upgrade::Version::V1)
        .authenticate(noise::NoiseConfig::xx(noise_keys).into_authenticated())
        .multiplex(mplex::MplexConfig::new())
        .boxed();

    // Create a Floodsub topic
    let floodsub_topic = floodsub::Topic::new("chat");

    // We create a custom network behaviour that combines floodsub and mDNS.
    // The derive generates a delegating `NetworkBehaviour` impl which in turn
    // requires the implementations of `NetworkBehaviourEventProcess` for
    // the events of each behaviour.
    #[derive(NetworkBehaviour)]
    struct MyBehaviour {
        floodsub: Floodsub,
        mdns: Mdns,
    }

    impl NetworkBehaviourEventProcess<FloodsubEvent> for MyBehaviour {
        // Called when `floodsub` produces an event.
        fn inject_event(&mut self, message: FloodsubEvent) {
            if let FloodsubEvent::Message(message) = message {
                println!("Received: '{:?}' from {:?}", String::from_utf8_lossy(&message.data), message.source);
            }
        }
    }

    impl NetworkBehaviourEventProcess<MdnsEvent> for MyBehaviour {
        // Called when `mdns` produces an event.
        fn inject_event(&mut self, event: MdnsEvent) {
            match event {
                MdnsEvent::Discovered(list) =>
                    for (peer, _) in list {
                        self.floodsub.add_node_to_partial_view(peer);
                    }
                MdnsEvent::Expired(list) =>
                    for (peer, _) in list {
                        if !self.mdns.has_node(&peer) {
                            self.floodsub.remove_node_from_partial_view(&peer);
                        }
                    }
            }
        }
    }

    // Create a Swarm to manage peers and events.
    let mut swarm = {
        let mdns = Mdns::new(Default::default()).await?;
        let mut behaviour = MyBehaviour {
            floodsub: Floodsub::new(peer_id.clone()),
            mdns,
        };

        behaviour.floodsub.subscribe(floodsub_topic.clone());

        SwarmBuilder::new(transport, behaviour, peer_id)
            // We want the connection background tasks to be spawned
            // onto the tokio runtime.
            .executor(Box::new(|fut| { tokio::spawn(fut); }))
            .build()
    };

    // Reach out to another node if specified
    if let Some(to_dial) = std::env::args().nth(1) {
        let addr: Multiaddr = to_dial.parse()?;
        swarm.dial_addr(addr)?;
        println!("Dialed {:?}", to_dial)
    }

    // Read full lines from stdin
    let mut stdin = io::BufReader::new(io::stdin()).lines();

    // Listen on all interfaces and whatever port the OS assigns
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    // Kick it off
    let mut listening = false;
    loop {
        let to_publish = {
            tokio::select! {
                line = stdin.next_line() => {
                    let line = line?.expect("stdin closed");
                    Some((floodsub_topic.clone(), line))
                }
                event = swarm.next() => {
                    // All events are handled by the `NetworkBehaviourEventProcess`es.
                    // I.e. the `swarm.next()` future drives the `Swarm` without ever
                    // terminating.
                    panic!("Unexpected event: {:?}", event);
                }
            }
        };
        if let Some((topic, line)) = to_publish {
            swarm.behaviour_mut().floodsub.publish(topic, line.as_bytes());
        }
        if !listening {
            for addr in Swarm::listeners(&swarm) {
                println!("Listening on {:?}", addr);
                listening = true;
            }
        }
    }
}
