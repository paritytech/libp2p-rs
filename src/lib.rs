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

//! Libp2p is a peer-to-peer framework.
//!
//! # Major libp2p concepts
//!
//! Here is a list of all the major concepts of libp2p.
//!
//! ## Multiaddr
//!
//! A [`Multiaddr`] is a self-describing network address and protocol stack
//! that is used to establish connections to peers. Some examples:
//!
//! * `/ip4/80.123.90.4/tcp/5432`
//! * `/ip6/[::1]/udp/10560/quic`
//! * `/unix//path/to/socket`
//!
//! ## Transport
//!
//! [`Transport`] is a trait for types that provide connection-oriented communication channels
//! based on dialing to or listening on a [`Multiaddr`]. To that end a transport
//! produces as output a type of data stream that varies depending on the concrete type of
//! transport.
//!
//! An implementation of transport typically supports only certain multi-addresses.
//! For example, the [`TcpConfig`] only supports multi-addresses of the format
//! `/ip4/.../tcp/...`.
//!
//! Example (Dialing a TCP/IP multi-address):
//!
//! ```rust
//! use libp2p::{Multiaddr, Transport, tcp::TcpConfig};
//! let tcp = TcpConfig::new();
//! let addr: Multiaddr = "/ip4/98.97.96.95/tcp/20500".parse().expect("invalid multiaddr");
//! let _conn = tcp.dial(addr);
//! ```
//! In the above example, `_conn` is a [`Future`] that needs to be polled in order for
//! the dialing to take place and eventually resolve to a connection. Polling
//! futures is typically done through a [tokio] runtime.
//!
//! The easiest way to create a transport is to use [`development_transport`].
//! This function provides support for the most common protocols but it is also
//! subject to change over time and should thus not be used in production
//! configurations.
//!
//! Example (Creating a development transport):
//!
//! ```rust
//! let keypair = libp2p::identity::Keypair::generate_ed25519();
//! let _transport = libp2p::development_transport(keypair);
//! // _transport.await?.dial(...);
//! ```
//!
//! The keypair that is passed as an argument in the above example is used
//! to set up transport-layer encryption using a newly generated long-term
//! identity keypair. The public key of this keypair uniquely identifies
//! the node in the network in the form of a [`PeerId`].
//!
//! See the documentation of the [`Transport`] trait for more details.
//!
//! ### Connection Upgrades
//!
//! Once a connection has been established with a remote through a [`Transport`], it can be
//! *upgraded*. Upgrading a transport is the process of negotiating an additional protocol
//! with the remote, mediated through a negotiation protocol called [`multistream-select`].
//!
//! Example ([`noise`] + [`yamux`] Protocol Upgrade):
//!
//! ```rust
//! # #[cfg(all(not(any(target_os = "emscripten", target_os = "wasi", target_os = "unknown")), feature = "tcp-async-io", feature = "noise", feature = "yamux"))] {
//! use libp2p::{Transport, core::upgrade, tcp::TcpConfig, noise, identity::Keypair, yamux};
//! let tcp = TcpConfig::new();
//! let id_keys = Keypair::generate_ed25519();
//! let noise_keys = noise::Keypair::<noise::X25519Spec>::new().into_authentic(&id_keys).unwrap();
//! let noise = noise::NoiseConfig::xx(noise_keys).into_authenticated();
//! let yamux = yamux::YamuxConfig::default();
//! let transport = tcp.upgrade(upgrade::Version::V1).authenticate(noise).multiplex(yamux);
//! # }
//! ```
//! In this example, `transport` is a new [`Transport`] that negotiates the
//! noise and yamux protocols on all connections.
//!
//! ## Network Behaviour
//!
//! The [`NetworkBehaviour`] trait is implemented on types that provide some capability to the
//! network. Examples of network behaviours include:
//!
//!   * Periodically pinging other nodes on established connections.
//!   * Periodically asking for information from other nodes.
//!   * Querying information from a DHT and propagating it to other nodes.
//!
//! ## Swarm
//!
//! A [`Swarm`] manages a pool of connections established through a [`Transport`]
//! and drives a [`NetworkBehaviour`] through emitting events triggered by activity
//! on the managed connections. Creating a [`Swarm`] thus involves combining a
//! [`Transport`] with a [`NetworkBehaviour`].
//!
//! See the documentation of the [`core`] module for more details about swarms.
//!
//! # Using libp2p
//!
//! The easiest way to get started with libp2p involves the following steps:
//!
//!   1. Creating an identity [`Keypair`] for the local node, obtaining the local
//!      [`PeerId`] from the [`PublicKey`].
//!   2. Creating an instance of a base [`Transport`], e.g. [`TcpConfig`], upgrading it with
//!      all the desired protocols, such as for transport security and multiplexing.
//!      In order to be usable with a [`Swarm`] later, the [`Output`](Transport::Output)
//!      of the final transport must be a tuple of a [`PeerId`] and a value whose type
//!      implements [`StreamMuxer`] (e.g. [`Yamux`]). The peer ID must be the
//!      identity of the remote peer of the established connection, which is
//!      usually obtained through a transport encryption protocol such as
//!      [`noise`] that authenticates the peer. See the implementation of
//!      [`development_transport`] for an example.
//!   3. Creating a struct that implements the [`NetworkBehaviour`] trait and combines all the
//!      desired network behaviours, implementing the event handlers as per the
//!      desired application's networking logic.
//!   4. Instantiating a [`Swarm`] with the transport, the network behaviour and the
//!      local peer ID from the previous steps.
//!
//! The swarm instance can then be polled e.g. with the [tokio] library, in order to
//! continuously drive the network activity of the program.
//!
//! [`Keypair`]: identity::Keypair
//! [`PublicKey`]: identity::PublicKey
//! [`Future`]: futures::Future
//! [`TcpConfig`]: tcp::TcpConfig
//! [`NetworkBehaviour`]: swarm::NetworkBehaviour
//! [`StreamMuxer`]: core::muxing::StreamMuxer
//! [`Yamux`]: yamux::Yamux
//!
//! [tokio]: https://tokio.rs
//! [`multistream-select`]: https://github.com/multiformats/multistream-select

#![doc(html_logo_url = "https://libp2p.io/img/logo_small.png")]
#![doc(html_favicon_url = "https://libp2p.io/img/favicon.png")]

pub use bytes;
pub use futures;
#[doc(inline)]
pub use multiaddr;
#[doc(inline)]
pub use libp2p_core::multihash;

#[doc(inline)]
pub use libp2p_core as core;
#[cfg(feature = "deflate")]
#[cfg_attr(docsrs, doc(cfg(feature = "deflate")))]
#[cfg(not(any(target_os = "emscripten", target_os = "wasi", target_os = "unknown")))]
#[doc(inline)]
pub use libp2p_deflate as deflate;
#[cfg(any(feature = "dns-async-std", feature = "dns-tokio"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "dns-async-std", feature = "dns-tokio"))))]
#[cfg(not(any(target_os = "emscripten", target_os = "wasi", target_os = "unknown")))]
#[doc(inline)]
pub use libp2p_dns as dns;
#[cfg(feature = "identify")]
#[cfg_attr(docsrs, doc(cfg(feature = "identify")))]
#[doc(inline)]
pub use libp2p_identify as identify;
#[cfg(feature = "kad")]
#[cfg_attr(docsrs, doc(cfg(feature = "kad")))]
#[doc(inline)]
pub use libp2p_kad as kad;
#[cfg(feature = "floodsub")]
#[cfg_attr(docsrs, doc(cfg(feature = "floodsub")))]
#[doc(inline)]
pub use libp2p_floodsub as floodsub;
#[cfg(feature = "gossipsub")]
#[cfg_attr(docsrs, doc(cfg(feature = "gossipsub")))]
#[doc(inline)]
pub use libp2p_gossipsub as gossipsub;
#[cfg(feature = "mplex")]
#[cfg_attr(docsrs, doc(cfg(feature = "mplex")))]
#[doc(inline)]
pub use libp2p_mplex as mplex;
#[cfg(feature = "mdns")]
#[cfg_attr(docsrs, doc(cfg(feature = "mdns")))]
#[cfg(not(any(target_os = "emscripten", target_os = "wasi", target_os = "unknown")))]
#[doc(inline)]
pub use libp2p_mdns as mdns;
#[cfg(feature = "noise")]
#[cfg_attr(docsrs, doc(cfg(feature = "noise")))]
#[doc(inline)]
pub use libp2p_noise as noise;
#[cfg(feature = "ping")]
#[cfg_attr(docsrs, doc(cfg(feature = "ping")))]
#[doc(inline)]
pub use libp2p_ping as ping;
#[cfg(feature = "plaintext")]
#[cfg_attr(docsrs, doc(cfg(feature = "plaintext")))]
#[doc(inline)]
pub use libp2p_plaintext as plaintext;
#[doc(inline)]
pub use libp2p_swarm as swarm;
#[cfg(any(feature = "tcp-async-io", feature = "tcp-tokio"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "tcp-async-io", feature = "tcp-tokio"))))]
#[cfg(not(any(target_os = "emscripten", target_os = "wasi", target_os = "unknown")))]
#[doc(inline)]
pub use libp2p_tcp as tcp;
#[cfg(feature = "uds")]
#[cfg_attr(docsrs, doc(cfg(feature = "uds")))]
#[doc(inline)]
pub use libp2p_uds as uds;
#[cfg(feature = "wasm-ext")]
#[cfg_attr(docsrs, doc(cfg(feature = "wasm-ext")))]
#[doc(inline)]
pub use libp2p_wasm_ext as wasm_ext;
#[cfg(feature = "websocket")]
#[cfg_attr(docsrs, doc(cfg(feature = "websocket")))]
#[cfg(not(any(target_os = "emscripten", target_os = "wasi", target_os = "unknown")))]
#[doc(inline)]
pub use libp2p_websocket as websocket;
#[cfg(feature = "yamux")]
#[cfg_attr(docsrs, doc(cfg(feature = "yamux")))]
#[doc(inline)]
pub use libp2p_yamux as yamux;
#[cfg(feature = "pnet")]
#[cfg_attr(docsrs, doc(cfg(feature = "pnet")))]
#[doc(inline)]
pub use libp2p_pnet as pnet;
#[cfg(feature = "relay")]
#[cfg_attr(docsrs, doc(cfg(feature = "relay")))]
#[doc(inline)]
pub use libp2p_relay as relay;
#[cfg(feature = "request-response")]
#[cfg_attr(docsrs, doc(cfg(feature = "request-response")))]
#[doc(inline)]
pub use libp2p_request_response as request_response;

mod transport_ext;

pub mod bandwidth;
pub mod simple;

pub use self::core::{
    identity,
    PeerId,
    Transport,
    transport::TransportError,
    upgrade::{InboundUpgrade, InboundUpgradeExt, OutboundUpgrade, OutboundUpgradeExt}
};
pub use libp2p_swarm_derive::NetworkBehaviour;
pub use self::multiaddr::{Multiaddr, multiaddr as build_multiaddr};
pub use self::simple::SimpleProtocol;
pub use self::swarm::Swarm;
pub use self::transport_ext::TransportExt;

/// Builds a `Transport` based on TCP/IP that supports the most commonly-used features of libp2p:
///
///  * DNS resolution.
///  * Noise protocol encryption.
///  * Websockets.
///  * Both Yamux and Mplex for substream multiplexing.
///
/// All async I/O of the transport is based on `async-std`.
///
/// > **Note**: This `Transport` is not suitable for production usage, as its implementation
/// >           reserves the right to support additional protocols or remove deprecated protocols.
#[cfg(all(not(any(target_os = "emscripten", target_os = "wasi", target_os = "unknown")), feature = "tcp-async-io", feature = "dns-async-std", feature = "websocket", feature = "noise", feature = "mplex", feature = "yamux"))]
#[cfg_attr(docsrs, doc(cfg(all(not(any(target_os = "emscripten", target_os = "wasi", target_os = "unknown")), feature = "tcp-async-io", feature = "dns-async-std", feature = "websocket", feature = "noise", feature = "mplex", feature = "yamux"))))]
pub async fn development_transport(keypair: identity::Keypair)
    -> std::io::Result<core::transport::Boxed<(PeerId, core::muxing::StreamMuxerBox)>>
{
    let transport = {
        let tcp = tcp::TcpConfig::new().nodelay(true);
        let dns_tcp = dns::DnsConfig::system(tcp).await?;
        let ws_dns_tcp = websocket::WsConfig::new(dns_tcp.clone());
        dns_tcp.or_transport(ws_dns_tcp)
    };

    let noise_keys = noise::Keypair::<noise::X25519Spec>::new()
        .into_authentic(&keypair)
        .expect("Signing libp2p-noise static DH keypair failed.");

    Ok(transport
        .upgrade(core::upgrade::Version::V1)
        .authenticate(noise::NoiseConfig::xx(noise_keys).into_authenticated())
        .multiplex(core::upgrade::SelectUpgrade::new(yamux::YamuxConfig::default(), mplex::MplexConfig::default()))
        .timeout(std::time::Duration::from_secs(20))
        .boxed())
}

/// Builds a `Transport` based on TCP/IP that supports the most commonly-used features of libp2p:
///
///  * DNS resolution.
///  * Noise protocol encryption.
///  * Websockets.
///  * Both Yamux and Mplex for substream multiplexing.
///
/// All async I/O of the transport is based on `tokio`.
///
/// > **Note**: This `Transport` is not suitable for production usage, as its implementation
/// >           reserves the right to support additional protocols or remove deprecated protocols.
#[cfg(all(not(any(target_os = "emscripten", target_os = "wasi", target_os = "unknown")), feature = "tcp-tokio", feature = "dns-tokio", feature = "websocket", feature = "noise", feature = "mplex", feature = "yamux"))]
#[cfg_attr(docsrs, doc(cfg(all(not(any(target_os = "emscripten", target_os = "wasi", target_os = "unknown")), feature = "tcp-tokio", feature = "dns-tokio", feature = "websocket", feature = "noise", feature = "mplex", feature = "yamux"))))]
pub fn tokio_development_transport(keypair: identity::Keypair)
    -> std::io::Result<core::transport::Boxed<(PeerId, core::muxing::StreamMuxerBox)>>
{
    let transport = {
        let tcp = tcp::TokioTcpConfig::new().nodelay(true);
        let dns_tcp = dns::TokioDnsConfig::system(tcp)?;
        let ws_dns_tcp = websocket::WsConfig::new(dns_tcp.clone());
        dns_tcp.or_transport(ws_dns_tcp)
    };

    let noise_keys = noise::Keypair::<noise::X25519Spec>::new()
        .into_authentic(&keypair)
        .expect("Signing libp2p-noise static DH keypair failed.");

    Ok(transport
        .upgrade(core::upgrade::Version::V1)
        .authenticate(noise::NoiseConfig::xx(noise_keys).into_authenticated())
        .multiplex(core::upgrade::SelectUpgrade::new(yamux::YamuxConfig::default(), mplex::MplexConfig::default()))
        .timeout(std::time::Duration::from_secs(20))
        .boxed())
}
