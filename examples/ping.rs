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

//! A basic example demonstrating some core APIs and concepts of libp2p.
//!
//! In the first terminal window, run:
//!
//! ```sh
//! cargo run --example ping
//! ```
//!
//! It will print the PeerId and the listening address, e.g. `Listening on
//! "/ip4/0.0.0.0/tcp/24915"`
//!
//! In the second terminal window, start a new instance of the example with:
//!
//! ```sh
//! cargo run --example ping -- /ip4/127.0.0.1/tcp/24915
//! ```
//!
//! The two nodes establish a connection, negotiate the ping protocol
//! and begin pinging each other.

use async_std::task;
use futures::{future, prelude::*};
use libp2p::{identity, PeerId, ping::{Ping, PingConfig}, Swarm};
use std::{error::Error, task::{Context, Poll}};

#[async_std::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    // Create a random PeerId.
    let id_keys = identity::Keypair::generate_ed25519();
    let peer_id = PeerId::from(id_keys.public());
    println!("Local peer id: {:?}", peer_id);

    // Create a transport.
    let transport = libp2p::development_transport(id_keys).await?;

    // Create a ping network behaviour.
    //
    // For illustrative purposes, the ping protocol is configured to
    // keep the connection alive, so a continuous sequence of pings
    // can be observed.
    let behaviour = Ping::new(PingConfig::new().with_keep_alive(true));

    // Create a Swarm that establishes connections through the given transport
    // and applies the ping behaviour on each connection.
    let mut swarm = Swarm::new(transport, behaviour, peer_id);

    // Dial the peer identified by the multi-address given as the second
    // command-line argument, if any.
    if let Some(addr) = std::env::args().nth(1) {
        let remote = addr.parse()?;
        swarm.dial_addr(remote)?;
        println!("Dialed {}", addr)
    }

    // Tell the swarm to listen on all interfaces and a random, OS-assigned port.
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    let mut listening = false;
    task::block_on(future::poll_fn(move |cx: &mut Context<'_>| {
        loop {
            match swarm.poll_next_unpin(cx) {
                Poll::Ready(Some(event)) => println!("{:?}", event),
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => {
                    if !listening {
                        for addr in Swarm::listeners(&swarm) {
                            println!("Listening on {}", addr);
                            listening = true;
                        }
                    }
                    return Poll::Pending
                }
            }
        }
    }));

    Ok(())
}
