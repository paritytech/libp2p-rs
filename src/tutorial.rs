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

//! # Ping Tutorial - Getting started with rust-libp2p
//!
//! This tutorial aims to give newcomers a hands-on overview on how to use the
//! Rust libp2p implementation. People new to Rust likely want to get started on
//! [Rust](https://www.rust-lang.org/) itself, before diving into all the
//! networking fun. This library makes heavy use of asynchronous Rust. In case
//! you are not familiar with these concepts the Rust
//! [async-book](https://rust-lang.github.io/async-book/) should prove useful.
//! People new to libp2p might prefer to get a general overview at libp2p.io
//! first, though libp2p knowledge is not required for this tutorial.
//!
//! We are going to build a small `ping` clone, sending a ping to a peer,
//! expecting a pong as a response.
//!
//! ## Scaffolding
//!
//! Let's start off by
//!
//! 1. Creating a new crate: `cargo init rust-libp2p-tutorial`
//!
//! 2. Adding `libp2p` as well as `futures` as a dependency in the
//!    `Cargo.toml` file:
//!
//!    ```yaml
//!    [package]
//!        name = "rust-libp2p-tutorial"
//!        version = "0.1.0"
//!        authors = ["Max Inden <mail@max-inden.de>"]
//!        edition = "2018"
//!
//!    [dependencies]
//!        libp2p = "<insert-current-version-here>"
//!        futures = "<insert-current-version-here>"
//!    ```
//!
//! ## Network identity
//!
//! With all the scaffolding in place, we can dive into the libp2p specifics. At
//! first we need to create a network identity for our local node in `fn
//! main()`. Identities in libp2p are handled via a public and private key pair.
//! Nodes identify each other via their [`PeerId`](crate::PeerId) which is
//! derived from the public key.
//!
//! ```rust
//! use libp2p::{identity, PeerId};
//! use std::error::Error;
//!
//! fn main() -> Result<(), Box<dyn Error>> {
//!     let local_key = identity::Keypair::generate_ed25519();
//!     let local_peer_id = PeerId::from(local_key.public());
//!     println!("Local peer id: {:?}", local_peer_id);
//!
//!     Ok(())
//! }
//! ```
//!
//! You can already run the code above via `cargo run` which should print a
//! different [`PeerId`](crate::PeerId) each time, given that we randomly
//! generate the key pair.
//!
//! ## Transport
//!
//! Next up we need to construct a transport. After all, we want to send some
//! bytes from A to B. A transport in libp2p provides connection-oriented
//! communication channels (e.g. TCP) as well as upgrades on top of those like
//! authentication and encryption protocols. Technically, a libp2p transport is
//! anything that implements the [`Transport`] trait.
//!
//! Instead of constructing a transport ourselves for this tutorial, we use the
//! convenience function [`development_transport`](crate::development_transport)
//! that creates a TCP transport with [`noise`](crate::noise) for authenticated
//! encryption.
//!
//! Furthermore, [`development_transport`] builds a multiplexed transport,
//! whereby multiple logical substreams can coexist on the same underlying (TCP)
//! connection. For further details on substream multiplexing, take a look at
//! [`crate::core::muxing`] and [`yamux`](crate::yamux).
//!
//! ```rust
//! use futures::executor::block_on;
//! use libp2p::{identity, PeerId};
//! use std::error::Error;
//!
//! fn main() -> Result<(), Box<dyn Error>> {
//!     let local_key = identity::Keypair::generate_ed25519();
//!     let local_peer_id = PeerId::from(local_key.public());
//!     println!("Local peer id: {:?}", local_peer_id);
//!
//!     let transport = block_on(libp2p::development_transport(local_key))?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Network behaviour
//!
//! Now it is time to look at another core trait of rust-libp2p - the
//! [`NetworkBehaviour`]. While the previously introduced trait [`Transport`]
//! defines _how_ to send bytes on the network, a [`NetworkBehaviour`] defines
//! _what_ bytes to send on the network.
//!
//! To make this more concrete, let's take a look at a simple implementation of
//! the [`NetworkBehaviour`] trait - the [`Ping`](crate::ping::Ping)
//! [`NetworkBehaviour`]. As you might have guessed, similar to the good old
//! `ping` network tool, libp2p [`Ping`](crate::ping::Ping) sends a ping to a
//! remote and expects to receive a pong in turn. The
//! [`Ping`](crate::ping::Ping) [`NetworkBehaviour`] does not care _how_ the
//! ping or pong messages are send on the network, whether they are sent via
//! TCP, whether they are encrypted via [noise](crate::noise) or just in
//! [plaintext](crate::plaintext). It only cares about _what_ messages are sent
//! on the network.
//!
//! The two traits [`Transport`] and [`NetworkBehaviour`] allow us to cleanly
//! separate _how_ to send bytes from _what_ bytes to send.
//!
//! With the above in mind, let's extend our example, creating a
//! [`Ping`](crate::ping::Ping) [`NetworkBehaviour`] at the end:
//!
//! ```rust
//! use futures::executor::block_on;
//! use libp2p::{identity, PeerId};
//! use libp2p::ping::{Ping, PingConfig};
//! use std::error::Error;
//!
//! fn main() -> Result<(), Box<dyn Error>> {
//!     let local_key = identity::Keypair::generate_ed25519();
//!     let local_peer_id = PeerId::from(local_key.public());
//!     println!("Local peer id: {:?}", local_peer_id);
//!
//!     let transport = block_on(libp2p::development_transport(local_key))?;
//!
//!     // Create a ping network behaviour.
//!     //
//!     // For illustrative purposes, the ping protocol is configured to
//!     // keep the connection alive, so a continuous sequence of pings
//!     // can be observed.
//!     let behaviour = Ping::new(PingConfig::new().with_keep_alive(true));
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Swarm
//!
//! Now that we have a [`Transport`] and a [`NetworkBehaviour`], we need
//! something that connects the two, allowing both to make progress. This job is
//! carried out by a [`Swarm`]. Put simply, a [`Swarm`] drives both a
//! [`Transport`] and a [`NetworkBehaviour`] forward, passing commands from the
//! [`NetworkBehaviour`] to the [`Transport`] as well as events from the
//! [`Transport`] to the [`NetworkBehaviour`].
//!
//! ```rust
//! use futures::executor::block_on;
//! use libp2p::{identity, PeerId};
//! use libp2p::ping::{Ping, PingConfig};
//! use libp2p::swarm::Swarm;
//! use std::error::Error;
//!
//! fn main() -> Result<(), Box<dyn Error>> {
//!     let local_key = identity::Keypair::generate_ed25519();
//!     let local_peer_id = PeerId::from(local_key.public());
//!     println!("Local peer id: {:?}", local_peer_id);
//!
//!     let transport = block_on(libp2p::development_transport(local_key))?;
//!
//!     // Create a ping network behaviour.
//!     //
//!     // For illustrative purposes, the ping protocol is configured to
//!     // keep the connection alive, so a continuous sequence of pings
//!     // can be observed.
//!     let behaviour = Ping::new(PingConfig::new().with_keep_alive(true));
//!
//!     let mut swarm = Swarm::new(transport, behaviour, local_peer_id);
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Multiaddr
//!
//! With the [`Swarm`] in place, we are all set to listen for incoming
//! connections. We only need to pass an address to the [`Swarm`], just like for
//! [`std::net::TcpListener::bind`]. But instead of passing an IP address, we
//! pass a [`Multiaddr`] which is yet another core concept of libp2p worth
//! taking a look at.
//!
//! A [`Multiaddr`] is a self-describing network address and protocol stack that
//! is used to establish connections to peers. A good introduction to
//! [`Multiaddr`] can be found on https://docs.libp2p.io/concepts/addressing/
//! and its specification repository https://github.com/multiformats/multiaddr.
//!
//! Let's make our local node listen on all interfaces as well as a random port.
//! In addition, if provided on the CLI, let's instruct our local node to dial a
//! remote peer.
//!
//! ```rust
//! use futures::executor::block_on;
//! use libp2p::{identity, PeerId};
//! use libp2p::ping::{Ping, PingConfig};
//! use libp2p::swarm::Swarm;
//! use std::error::Error;
//!
//! fn main() -> Result<(), Box<dyn Error>> {
//!     let local_key = identity::Keypair::generate_ed25519();
//!     let local_peer_id = PeerId::from(local_key.public());
//!     println!("Local peer id: {:?}", local_peer_id);
//!
//!     let transport = block_on(libp2p::development_transport(local_key))?;
//!
//!     // Create a ping network behaviour.
//!     //
//!     // For illustrative purposes, the ping protocol is configured to
//!     // keep the connection alive, so a continuous sequence of pings
//!     // can be observed.
//!     let behaviour = Ping::new(PingConfig::new().with_keep_alive(true));
//!
//!     let mut swarm = Swarm::new(transport, behaviour, local_peer_id);
//!
//!     // Tell the swarm to listen on all interfaces and a random, OS-assigned
//!     // port.
//!     swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
//!
//!     // Dial the peer identified by the multi-address given as the second
//!     // command-line argument, if any.
//!     if let Some(addr) = std::env::args().nth(1) {
//!         let remote = addr.parse()?;
//!         swarm.dial_addr(remote)?;
//!         println!("Dialed {}", addr)
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Continuously polling the Swarm
//!
//! We have everything in place now. The last step is to drive the [`Swarm`] in
//! a loop, allowing it to listen for incoming connections and establish an
//! outgoing connection in case we specify an address on the CLI.
//!
//! ```no_run
//! use futures::executor::block_on;
//! use futures::prelude::*;
//! use libp2p::ping::{Ping, PingConfig};
//! use libp2p::swarm::Swarm;
//! use libp2p::{identity, PeerId};
//! use std::error::Error;
//! use std::task::Poll;
//!
//! fn main() -> Result<(), Box<dyn Error>> {
//!     let local_key = identity::Keypair::generate_ed25519();
//!     let local_peer_id = PeerId::from(local_key.public());
//!     println!("Local peer id: {:?}", local_peer_id);
//!
//!     let transport = block_on(libp2p::development_transport(local_key))?;
//!
//!     // Create a ping network behaviour.
//!     //
//!     // For illustrative purposes, the ping protocol is configured to
//!     // keep the connection alive, so a continuous sequence of pings
//!     // can be observed.
//!     let behaviour = Ping::new(PingConfig::new().with_keep_alive(true));
//!
//!     let mut swarm = Swarm::new(transport, behaviour, local_peer_id);
//!
//!     // Tell the swarm to listen on all interfaces and a random, OS-assigned
//!     // port.
//!     swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
//!
//!     // Dial the peer identified by the multi-address given as the second
//!     // command-line argument, if any.
//!     if let Some(addr) = std::env::args().nth(1) {
//!         let remote = addr.parse()?;
//!         swarm.dial_addr(remote)?;
//!         println!("Dialed {}", addr)
//!     }
//!
//!     let mut listening = false;
//!     block_on(future::poll_fn(move |cx| loop {
//!         match swarm.poll_next_unpin(cx) {
//!             Poll::Ready(Some(event)) => println!("{:?}", event),
//!             Poll::Ready(None) => return Poll::Ready(()),
//!             Poll::Pending => {
//!                 if !listening {
//!                     for addr in Swarm::listeners(&swarm) {
//!                         println!("Listening on {}", addr);
//!                         listening = true;
//!                     }
//!                 }
//!                 return Poll::Pending;
//!             }
//!         }
//!     }));
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Running two nodes
//!
//! For convenience the example created above is also implemented in full in
//! `examples/ping.rs`. Thus, you can either run the commands below from your
//! own project created during the tutorial, or from the root of the rust-libp2p
//! repository. Note that in the former case you need to ignore the `--example
//! ping` argument.
//!
//! You need two terminals. In the first terminal window run:
//!
//! ```sh
//! cargo run --example ping
//! ```
//!
//! It will print the PeerId and the listening address, e.g. `Listening on
//! "/ip4/127.0.0.1/tcp/24915"`
//!
//! In the second terminal window, start a new instance of the example with:
//!
//! ```sh
//! cargo run --example ping -- /ip4/127.0.0.1/tcp/24915
//! ```
//!
//! Note: The [`Multiaddr`] at the end being the [`Multiaddr`] printed earlier
//! in terminal window one.
//!
//! The two nodes will establish a connection and send each other ping and pong
//! messages every 15 seconds.
//!
//! [`Multiaddr`]: crate::core::Multiaddr
//! [`NetworkBehaviour`]: crate::swarm::NetworkBehaviour
//! [`Transport`]: crate::core::Transport
//! [`PeerId`]: crate::core::PeerId
//! [`Swarm`]: crate::swarm::Swarm
//! [`development_transport`]: crate::development_transport
