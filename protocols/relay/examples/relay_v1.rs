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

//! A basic relay server and relay client implementation.
//!
//! The example below involves three nodes: (1) a relay server, (2) a listening
//! relay client listening via the relay server and (3) a dialing relay client
//! dialing the listening relay client via the relay server.
//!
//! 1. To start the relay server, run `cargo run --example=relay --package=libp2p-relay --mode relay --secret-key-seed 1 --address /ip4/<ip address>/tcp/<port>`.
//!    The `-secret-key-seed` helps create a static peer id using the given number argument as  a seed.
//!    The mode specifies whether the node should run as a relay server, a listening client or a dialing client.
//!    The address specifies a static address. Usually it will be some loop back address such as `/ip4/0.0.0.0/tcp/4444`.
//!    Example:
//!    `cargo run --example=relay --package=libp2p-relay -- --mode relay --secret-key-seed 1 --address /ip4/0.0.0.0/tcp/4444`
//!    `cargo run --example=relay --package=libp2p-relay -- --mode relay --secret-key-seed 1 --address /ip6/::/tcp/4444`
//!
//! 2. To start the listening relay client run `cargo run --example=relay --package=libp2p-relay -- --mode client-listen --secret-key-seed 2 --address
//! <addr-relay-server>/p2p/<peer-id-relay-server>/p2p-circuit` in a second terminal where:
//!
//!   - `<addr-relay-server>` is replaced by one of the listening addresses of the relay server.
//!   - `<peer-id-relay-server>` is replaced by the peer id of the relay server.
//!
//!    Example:
//!    `cargo run --example=relay --package=libp2p-relay -- --mode client-listen --secret-key-seed 2 --address /ip4/127.0.0.1/tcp/4444/p2p/12D3KooWPjceQrSwdWXPyLLeABRXmuqt69Rg3sBYbU1Nft9HyQ6X/p2p-circuit`
//!    `cargo run --example=relay --package=libp2p-relay -- --mode client-listen --secret-key-seed 2 --address /ip6/::1/tcp/4444/p2p/12D3KooWPjceQrSwdWXPyLLeABRXmuqt69Rg3sBYbU1Nft9HyQ6X/p2p-circuit`
//!
//! 3. To start the dialing relay client run `cargo run --example=relay --package=libp2p-relay -- --mode client-dial --secret-key-seed 3 --address
//! <addr-relay-server>/p2p/<peer-id-relay-server>/p2p-circuit/p2p/<peer-id-listening-relay-client>` in
//! a third terminal where:
//!
//!   - `<addr-relay-server>` is replaced by one of the listening addresses of the relay server.
//!   - `<peer-id-relay-server>` is replaced by the peer id of the relay server.
//!   - `<peer-id-listening-relay-client>` is replaced by the peer id of the listening relay client.
//!    Example:
//!    `cargo run --example=relay --package=libp2p-relay -- --mode client-dial --secret-key-seed 3 --address /ip4/127.0.0.1/tcp/4444/p2p/12D3KooWPjceQrSwdWXPyLLeABRXmuqt69Rg3sBYbU1Nft9HyQ6X/p2p-circuit/p2p/12D3KooWH3uVF6wv47WnArKHk5p6cvgCJEb74UTmxztmQDc298L3`
//!    `cargo run --example=relay --package=libp2p-relay -- --mode client-dial --secret-key-seed 3 --address /ip6/::1/tcp/4444/p2p/12D3KooWPjceQrSwdWXPyLLeABRXmuqt69Rg3sBYbU1Nft9HyQ6X/p2p-circuit/p2p/12D3KooWH3uVF6wv47WnArKHk5p6cvgCJEb74UTmxztmQDc298L3`
//!
//! In the third terminal you will see the dialing relay client to receive pings
//! from both the relay server AND from the listening relay client relayed via
//! the relay server.

use futures::executor::block_on;
use futures::stream::StreamExt;
use libp2p::dns::DnsConfig;
use libp2p::ping::{Ping, PingConfig, PingEvent};
use libp2p::plaintext;
use libp2p::relay::v1::{Relay, RelayConfig};
use libp2p::swarm::SwarmEvent;
use libp2p::tcp::TcpConfig;
use libp2p::Transport;
use libp2p::{core::upgrade, identity::ed25519};
use libp2p::{identity, NetworkBehaviour, PeerId, Swarm};
use std::error::Error;
use std::task::{Context, Poll};
use std::time::Duration;
use std::{fmt, str::FromStr};
use structopt::StructOpt;

// Listen on all interfaces and whatever port the OS assigns
const DEFAULT_RELAY_ADDRESS: &str = "/ip4/0.0.0.0/tcp/0";

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let opt = Opt::from_args();
    println!("opt: {:?}", opt);

    // Create a static known PeerId based on given secret
    let local_key: identity::Keypair = generate_ed25519(opt.secret_key_seed);
    let local_peer_id = PeerId::from(local_key.public());
    println!("Local peer id: {:?}", local_peer_id);

    let transport = block_on(DnsConfig::system(TcpConfig::new()))?;

    let relay_config = RelayConfig {
        connection_idle_timeout: Duration::from_secs(10 * 60),
        ..Default::default()
    };
    let (relay_wrapped_transport, relay_behaviour) =
        libp2p_relay::v1::new_transport_and_behaviour(relay_config, transport);

    let behaviour = Behaviour {
        relay: relay_behaviour,
        ping: Ping::new(
            PingConfig::new()
                .with_keep_alive(true)
                .with_interval(Duration::from_secs(1)),
        ),
    };

    let plaintext = plaintext::PlainText2Config {
        local_public_key: local_key.public(),
    };

    let transport = relay_wrapped_transport
        .upgrade(upgrade::Version::V1)
        .authenticate(plaintext)
        .multiplex(libp2p_yamux::YamuxConfig::default())
        .boxed();

    let mut swarm = Swarm::new(transport, behaviour, local_peer_id);

    match opt.mode {
        Mode::Relay => {
            let address = get_relay_address(&opt);
            swarm.listen_on(address.parse()?)?;
            println!("starting listening as relay on {}", &address);
        }
        Mode::ClientListen => {
            let relay_address = get_relay_peer_address(&opt);
            swarm.listen_on(relay_address.parse()?)?;
            println!("starting client listener via relay on {}", &relay_address);
        }
        Mode::ClientDial => {
            let client_listen_address = get_client_listen_address(&opt);
            swarm.dial_addr(client_listen_address.parse()?)?;
            println!("starting as client dialer on {}", client_listen_address);
        }
    }

    block_on(futures::future::poll_fn(move |cx: &mut Context<'_>| {
        loop {
            match swarm.poll_next_unpin(cx) {
                Poll::Ready(Some(event)) => match event {
                    SwarmEvent::NewListenAddr(addr) => {
                        print_listener_peer(&addr, &opt.mode, local_peer_id)
                    }
                    _ => println!("{:?}", event),
                },
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => break,
            }
        }
        Poll::Pending
    }))
}

fn print_listener_peer(addr: &libp2p::Multiaddr, mode: &Mode, local_peer_id: PeerId) -> () {
    match mode {
        Mode::Relay => {
            println!(
                "Peer that act as Relay can access on: `{}/p2p/{}/p2p-circuit`",
                addr, local_peer_id
            );
        }
        Mode::ClientListen => {
            println!(
                "Peer that act as Client Listen can access on: `/p2p/{}/{}`",
                addr, local_peer_id
            );
        }
        Mode::ClientDial => {
            println!("Peer that act as Client Dial Listening on {:?}", addr);
        }
    }
}

fn generate_ed25519(secret_key_seed: u8) -> identity::Keypair {
    let mut bytes = [0u8; 32];
    bytes[0] = secret_key_seed;

    let secret_key = ed25519::SecretKey::from_bytes(&mut bytes)
        .expect("this returns `Err` only if the length is wrong; the length is correct; qed");
    identity::Keypair::Ed25519(secret_key.into())
}

/// Get the address for relay mode
fn get_relay_address(opt: &Opt) -> String {
    match &opt.address {
        Some(address) => address.clone(),
        None => {
            println!("--address argument was not provided, will use the default listening relay address: {}",DEFAULT_RELAY_ADDRESS);
            DEFAULT_RELAY_ADDRESS.to_string()
        }
    }
}

/// Get the address for client_listen mode
fn get_relay_peer_address(opt: &Opt) -> String {
    match &opt.address {
        Some(address) => address.clone(),
        None => panic!("Please provide relayed listen address such as: <addr-relay-server>/p2p/<peer-id-relay-server>/p2p-circuit"),
    }
}

/// Get the address for client-dial mode
fn get_client_listen_address(opt: &Opt) -> String {
    match &opt.address {
        Some(address) => address.clone(),
        None => panic!("Please provide client listen address such as: <addr-relay-server>/p2p/<peer-id-relay-server>/p2p-circuit/p2p/<peer-id-listening-relay-client>")
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "Event", event_process = false)]
struct Behaviour {
    relay: Relay,
    ping: Ping,
}

#[derive(Debug)]
enum Event {
    Relay(()),
    Ping(PingEvent),
}

impl From<PingEvent> for Event {
    fn from(e: PingEvent) -> Self {
        Event::Ping(e)
    }
}

impl From<()> for Event {
    fn from(_: ()) -> Self {
        Event::Relay(())
    }
}

#[derive(Debug, StructOpt)]
enum Mode {
    Relay,
    ClientListen,
    ClientDial,
}

impl FromStr for Mode {
    type Err = ModeError;
    fn from_str(mode: &str) -> Result<Self, Self::Err> {
        match mode {
            "relay" => Ok(Mode::Relay),
            "client-listen" => Ok(Mode::ClientListen),
            "client-dial" => Ok(Mode::ClientDial),
            _ => Err(ModeError {}),
        }
    }
}

#[derive(Debug)]
struct ModeError {}
impl Error for ModeError {}
impl fmt::Display for ModeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Could not parse a mode")
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "libp2p relay")]
struct Opt {
    /// The mode (relay, client-listen, client-dial)
    #[structopt(long)]
    mode: Mode,

    /// Fixed value to generate deterministic peer id
    #[structopt(long)]
    secret_key_seed: u8,

    /// The listening address
    #[structopt(long)]
    address: Option<String>,
}
