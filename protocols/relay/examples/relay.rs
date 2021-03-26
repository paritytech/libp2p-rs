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

use futures::executor::block_on;
use futures::stream::StreamExt;
use libp2p::core::upgrade;
use libp2p::plaintext;
use libp2p::relay::RelayConfig;
use libp2p::tcp::TcpConfig;
use libp2p::Transport;
use libp2p::{identity, PeerId, Swarm};
use std::error::Error;
use std::task::{Context, Poll};
use std::time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    // Create a random PeerId
    let local_key = identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(local_key.public());
    println!("Local peer id: {:?}", local_peer_id);

    let tcp_transport = TcpConfig::new();

    let relay_config = RelayConfig {
        connection_idle_timeout: Duration::from_secs(10 * 60),
        ..Default::default()
    };
    let (relay_wrapped_transport, relay_behaviour) =
        libp2p_relay::new_transport_and_behaviour(relay_config, tcp_transport);

    let plaintext = plaintext::PlainText2Config {
        local_public_key: local_key.public(),
    };

    let transport = relay_wrapped_transport
        .upgrade(upgrade::Version::V1)
        .authenticate(plaintext)
        .multiplex(libp2p_yamux::YamuxConfig::default())
        .boxed();

    let mut swarm = Swarm::new(transport, relay_behaviour, local_peer_id);

    // Listen on all interfaces and whatever port the OS assigns
    swarm.listen_on("/ip6/::/tcp/0".parse()?)?;

    let mut listening = false;
    block_on(futures::future::poll_fn(move |cx: &mut Context<'_>| {
        loop {
            match swarm.poll_next_unpin(cx) {
                Poll::Ready(Some(event)) => println!("{:?}", event),
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
