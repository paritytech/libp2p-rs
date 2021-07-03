// Copyright 2020 Parity Technologies (UK) Ltd.
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
use libp2p::core::upgrade;
use libp2p::identify::{Identify, IdentifyConfig, IdentifyEvent};
use libp2p::noise;
use libp2p::ping::{Ping, PingConfig, PingEvent};
use libp2p::relay::v2::relay::{self, Relay};
use libp2p::swarm::{Swarm, SwarmEvent};
use libp2p::tcp::TcpConfig;
use libp2p::Transport;
use libp2p::{identity, NetworkBehaviour, PeerId};
use std::error::Error;
use std::task::{Context, Poll};

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let local_key = identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(local_key.public());
    println!("Local peer id: {:?}", local_peer_id);

    let tcp_transport = TcpConfig::new();

    let noise_keys = noise::Keypair::<noise::X25519Spec>::new()
        .into_authentic(&local_key)
        .expect("Signing libp2p-noise static DH keypair failed.");

    let transport = tcp_transport
        .upgrade(upgrade::Version::V1)
        .authenticate(noise::NoiseConfig::xx(noise_keys).into_authenticated())
        .multiplex(libp2p_yamux::YamuxConfig::default())
        .boxed();

    #[derive(NetworkBehaviour)]
    #[behaviour(out_event = "Event", event_process = false)]
    struct Behaviour {
        relay: Relay,
        ping: Ping,
        identify: Identify,
    }

    #[derive(Debug)]
    enum Event {
        Ping(PingEvent),
        Identify(IdentifyEvent),
        Relay(relay::Event),
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

    impl From<relay::Event> for Event {
        fn from(e: relay::Event) -> Self {
            Event::Relay(e)
        }
    }

    let behaviour = Behaviour {
        relay: Relay::new(local_peer_id, Default::default()),
        ping: Ping::new(PingConfig::new()),
        identify: Identify::new(IdentifyConfig::new(
            "/TODO/0.0.1".to_string(),
            local_key.public(),
        )),
    };

    let mut swarm = Swarm::new(transport, behaviour, local_peer_id);

    // Listen on all interfaces and whatever port the OS assigns
    swarm.listen_on("/ip4/0.0.0.0/tcp/4001".parse()?)?;

    let mut listening = false;
    block_on(futures::future::poll_fn(move |cx: &mut Context<'_>| {
        loop {
            match swarm.poll_next_unpin(cx) {
                Poll::Ready(Some(SwarmEvent::Behaviour(Event::Relay(event)))) => {
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
