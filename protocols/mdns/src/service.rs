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

use crate::{SERVICE_NAME, META_QUERY_SERVICE, dns};
use dns_parser::{Packet, RData};
use either::Either::{Left, Right};
use futures::{future, prelude::*};
use libp2p_core::{multiaddr::{Multiaddr, Protocol}, PeerId};
use pnet::datalink;
use pnet::ipnetwork::IpNetwork;
use std::{convert::TryFrom as _, fmt, io, net::Ipv4Addr, net::SocketAddr, str, time::{Duration, Instant}};
use wasm_timer::Interval;
use lazy_static::lazy_static;

pub use dns::{MdnsResponseError, build_query_response, build_service_discovery_response};

lazy_static! {
    static ref IPV4_MDNS_MULTICAST_ADDRESS: SocketAddr = SocketAddr::from((
        Ipv4Addr::new(224, 0, 0, 251),
        5353,
    ));
}

macro_rules! codegen {
    ($feature_name:expr, $service_name:ident, $udp_socket:ty, $udp_socket_from_std:tt, $set_multicast_if_v4:tt) => {

/// A running service that discovers libp2p peers and responds to other libp2p peers' queries on
/// the local network.
///
/// # Usage
///
/// In order to use mDNS to discover peers on the local network, use the `MdnsService`. This is
/// done by creating a `MdnsService` then polling it in the same way as you would poll a stream.
///
/// Polling the `MdnsService` can produce either an `MdnsQuery`, corresponding to an mDNS query
/// received by another node on the local network, or an `MdnsResponse` corresponding to a response
/// to a query previously emitted locally. The `MdnsService` will automatically produce queries,
/// which means that you will receive responses automatically.
///
/// When you receive an `MdnsQuery`, use the `respond` method to send back an answer to the node
/// that emitted the query.
///
/// When you receive an `MdnsResponse`, use the provided methods to query the information received
/// in the response.
///
/// # Example
///
/// ```rust
/// # use futures::prelude::*;
/// # use futures::executor::block_on;
/// # use libp2p_core::{identity, Multiaddr, PeerId};
/// # use libp2p_mdns::service::{MdnsPacket, build_query_response, build_service_discovery_response};
/// # use std::{io, time::Duration, task::Poll};
/// # fn main() {
/// # let my_peer_id = PeerId::from(identity::Keypair::generate_ed25519().public());
/// # let my_listened_addrs: Vec<Multiaddr> = vec![];
/// # async {
/// # #[cfg(feature = "async-std")]
/// # let mut service = libp2p_mdns::service::MdnsService::new().unwrap();
/// # #[cfg(feature = "tokio")]
/// # let mut service = libp2p_mdns::service::TokioMdnsService::new().unwrap();
/// let _future_to_poll = async {
///     let (mut service, packet) = service.next().await;
///
///     match packet {
///         MdnsPacket::Query(query) => {
///             println!("Query from {:?}", query.remote_addr());
///             let resp = build_query_response(
///                 query.query_id(),
///                 my_peer_id.clone(),
///                 vec![].into_iter(),
///                 Duration::from_secs(120),
///             ).unwrap();
///             service.enqueue_response(resp);
///         }
///         MdnsPacket::Response(response) => {
///             for peer in response.discovered_peers() {
///                 println!("Discovered peer {:?}", peer.id());
///                 for addr in peer.addresses() {
///                     println!("Address = {:?}", addr);
///                 }
///             }
///         }
///         MdnsPacket::ServiceDiscovery(disc) => {
///             let resp = build_service_discovery_response(
///                 disc.query_id(),
///                 Duration::from_secs(120),
///             );
///             service.enqueue_response(resp);
///         }
///     }
/// };
/// # };
/// # }
#[cfg_attr(docsrs, doc(cfg(feature = $feature_name)))]
pub struct $service_name {
    /// Sockets for sending messages.
    ///
    /// We have one socket per interface. Sending via the `receiving` socket would only send on the
    /// default interfaces, which is picked by the operating system. This is usually what one
    /// wants, but not always. The better approach to just picking some arbitrary interface is to
    /// pick all interfaces, we do this by using a "sending" socket for each interfaces, which gets
    /// bound to the interface IP address. Those sockets are therefore unsuited for receiving
    /// multicast messages and will only be used for sending.
    ///
    /// Considerations: Having sockets only used for sending is obviously not ideal, as there are
    /// input queues managed by the kernel which occupy memory. Nevertheless this approach is the
    /// easiest to implement in a platform independent way.
    ///
    /// Other approaches evaluated:
    ///
    /// Using something like the [multicast-socket
    /// crate](https://crates.io/crates/multicast-socket), which works by means of platform
    /// specific functions like [sendmsg](https://linux.die.net/man/2/sendmsg)
    ///
    /// TODO: Correct above comment.
    ///
    socket: $udp_socket,
    /// IP addresses of the interfaces this implementation is sending packets out.
    interfaces: Vec<Ipv4Addr>,
    /// Interval for sending queries.
    query_interval: Interval,
    /// Whether we send queries on the network at all.
    /// Note that we still need to have an interval for querying, as we need to wake up the socket
    /// regularly to recover from errors. Otherwise we could simply use an `Option<Interval>`.
    silent: bool,
    /// Buffer used for receiving data from the main socket.
    recv_buffer: [u8; 2048],
    /// Buffers pending to send on the main socket.
    send_buffers: Vec<Vec<u8>>,
    /// Buffers pending to send on the query socket.
    query_send_buffers: Vec<Vec<u8>>,
}

impl $service_name {
    /// Starts a new mDNS service.
    pub fn new() -> io::Result<$service_name> {
        Self::new_inner(false)
    }

    /// Same as `new`, but we don't automatically send queries on the network.
    pub fn silent() -> io::Result<$service_name> {
        Self::new_inner(true)
    }

    /// Starts a new mDNS service.
    fn new_inner(silent: bool) -> io::Result<$service_name> {
        let std_socket = {
            #[cfg(unix)]
            fn platform_specific(s: &net2::UdpBuilder) -> io::Result<()> {
                net2::unix::UnixUdpBuilderExt::reuse_port(s, true)?;
                Ok(())
            }
            #[cfg(not(unix))]
            fn platform_specific(_: &net2::UdpBuilder) -> io::Result<()> { Ok(()) }
            let builder = net2::UdpBuilder::new_v4()?;
            builder.reuse_address(true)?;
            platform_specific(&builder)?;
            builder.bind(("0.0.0.0", 5353))?
        };

        let socket = $udp_socket_from_std(std_socket)?;

        socket.set_multicast_loop_v4(true)?;
        socket.set_multicast_ttl_v4(255)?;
        let interfaces = get_interface_addresses().collect();
        // Join multicast on all avaliable interfaces:
        for &addr in &interfaces {
            socket.join_multicast_v4(From::from([224, 0, 0, 251]), addr)?;
        }

        Ok($service_name {
            socket,
            interfaces,
            query_interval: Interval::new_at(Instant::now(), Duration::from_secs(20)),
            silent,
            recv_buffer: [0; 2048],
            send_buffers: Vec::new(),
            query_send_buffers: Vec::new(),
        })
    }

    pub fn enqueue_response(&mut self, rsp: Vec<u8>) {
        self.send_buffers.push(rsp);
    }

    /// Returns a future resolving to itself and the next received `MdnsPacket`.
    //
    // **Note**: Why does `next` take ownership of itself?
    //
    // `MdnsService::next` needs to be called from within `NetworkBehaviour`
    // implementations. Given that traits cannot have async methods the
    // respective `NetworkBehaviour` implementation needs to somehow keep the
    // Future returned by `MdnsService::next` across classic `poll`
    // invocations. The instance method `next` can either take a reference or
    // ownership of itself:
    //
    // 1. Taking a reference - If `MdnsService::poll` takes a reference to
    // `&self` the respective `NetworkBehaviour` implementation would need to
    // keep both the Future as well as its `MdnsService` instance across poll
    // invocations. Given that in this case the Future would have a reference
    // to `MdnsService`, the `NetworkBehaviour` implementation struct would
    // need to be self-referential which is not possible without unsafe code in
    // Rust.
    //
    // 2. Taking ownership - Instead `MdnsService::next` takes ownership of
    // self and returns it alongside an `MdnsPacket` once the actual future
    // resolves, not forcing self-referential structures on the caller.
    pub async fn next(mut self) -> (Self, MdnsPacket) {
        loop {
            let send_buffers = self.send_buffers;
            self.send_buffers = Vec::new();
            let query_send_buffers = self.query_send_buffers;
            self.query_send_buffers = Vec::new();

            for interface in &self.interfaces {
                $set_multicast_if_v4(&self.socket, interface)
                    .expect("set_multicast_if_v4 should work");
                // Flush the send buffer of the main socket.
                for to_send in &send_buffers {
                    match self.socket.send_to(&to_send, *IPV4_MDNS_MULTICAST_ADDRESS).await {
                        Ok(bytes_written) => {
                            debug_assert_eq!(bytes_written, to_send.len());
                        }
                        Err(_) => {
                            // Errors are non-fatal because they can happen for example if we lose
                            // connection to the network.
                            break;
                        }
                    }
                }

                // Flush the query send buffer.
                for to_send in &query_send_buffers {
                    match self.socket.send_to(&to_send, *IPV4_MDNS_MULTICAST_ADDRESS).await {
                        Ok(bytes_written) => {
                            debug_assert_eq!(bytes_written, to_send.len());
                        }
                        Err(_) => {
                            // Errors are non-fatal because they can happen for example if we lose
                            // connection to the network.
                            break;
                        }
                    }
                }
            }

            // Either (left) listen for incoming packets or (right) send query packets whenever the
            // query interval fires.
            let selected_output = match futures::future::select(
                Box::pin(self.socket.recv_from(&mut self.recv_buffer)),
                Box::pin(self.query_interval.next()),
            ).await {
                future::Either::Left((recved, _)) => Left(recved),
                future::Either::Right(_) => Right(()),
            };

            match selected_output {
                Left(left) => match left {
                    Ok((len, from)) => {
                        match MdnsPacket::new_from_bytes(&self.recv_buffer[..len], from) {
                            Some(packet) => return (self, packet),
                            None => {},
                        }
                    },
                    Err(_) => {
                        // Errors are non-fatal and can happen if we get disconnected from the network.
                        // The query interval will wake up the task at some point so that we can try again.
                    },
                },
                Right(()) => {
                    // Ensure underlying task is woken up on the next interval tick.
                    while let Some(_) = self.query_interval.next().now_or_never() {};

                    if !self.silent {
                        let query = dns::build_query();
                        self.query_send_buffers.push(query.to_vec());
                    }
                }
            };
        }
    }
}

impl fmt::Debug for $service_name {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("$service_name")
            .field("silent", &self.silent)
            .finish()
    }
}

};
}

#[cfg(feature = "async-std")]
codegen!("async-std", MdnsService, async_std::net::UdpSocket, (|socket| Ok::<_, std::io::Error>(async_std::net::UdpSocket::from(socket))), set_multicast_if_v4);

#[cfg(feature = "tokio")]
codegen!("tokio", TokioMdnsService, tokio::net::UdpSocket, (|socket| tokio::net::UdpSocket::from_std(socket)), set_multicast_if_v4_tokio);

// Make set_multicast_if_v4 available on asynchronous sockets:
#[cfg(feature = "async-std")]
fn set_multicast_if_v4(socket: &async_std::net::UdpSocket, interface: &Ipv4Addr) -> std::io::Result<()>  {
        use std::net::UdpSocket;
        use net2::UdpSocketExt;
        #[cfg(not(windows))]
        use async_std::os::unix::io::AsRawFd;
        #[cfg(not(windows))]
        use std::os::unix::io::IntoRawFd;
        #[cfg(not(windows))]
        use std::os::unix::io::FromRawFd;
        // Temporary unsafe double ownership:
        #[cfg(windows)]
        let std_sock: UdpSocket = unsafe {
            FromRawSocket::from_raw_socket(socket.as_raw_socket())
        };
        #[cfg(not(windows))]
        let std_sock: UdpSocket = unsafe {
            FromRawFd::from_raw_fd(socket.as_raw_fd())
        };
        // Don't use ? here, we need to drop the ownership in the end!
        let r = std_sock.set_multicast_if_v4(interface);
        // Drop ownership again, thus avoid double free!
        std_sock.into_raw_fd();
        r
}

// Make set_multicast_if_v4 available on asynchronous sockets:
#[cfg(feature = "tokio")]
fn set_multicast_if_v4_tokio(socket: &tokio::net::UdpSocket, interface: &Ipv4Addr) -> std::io::Result<()>  {
        use std::net::UdpSocket;
        use net2::UdpSocketExt;
        #[cfg(not(windows))]
        use std::os::unix::io::AsRawFd;
        #[cfg(not(windows))]
        use std::os::unix::io::IntoRawFd;
        #[cfg(not(windows))]
        use std::os::unix::io::FromRawFd;
        // Temporary unsafe double ownership:
        #[cfg(windows)]
        let std_sock: UdpSocket = unsafe {
            FromRawSocket::from_raw_socket(socket.as_raw_socket())
        };
        #[cfg(not(windows))]
        let std_sock: UdpSocket = unsafe {
            FromRawFd::from_raw_fd(socket.as_raw_fd())
        };
        // Don't use ? here, we need to drop the ownership in the end!
        let r = std_sock.set_multicast_if_v4(interface);
        // Drop ownership again, thus avoid double free!
        std_sock.into_raw_fd();
        r
}


/// Get IPv4 addresses of all external network interfaces.
fn get_interface_addresses() -> impl Iterator<Item = Ipv4Addr> {
    datalink::interfaces()
        .into_iter()
        .filter(|i| i.is_up() && !i.is_loopback())
        .filter_map(|i| {
            i.ips
                .into_iter()
                .filter_map(|n| match n {
                    IpNetwork::V4(n4) => Some(n4.ip()),
                    _ => None,
                })
                .next() // Simply get the first valid IPv4.
        })
}

/// A valid mDNS packet received by the service.
#[derive(Debug)]
pub enum MdnsPacket {
    /// A query made by a remote.
    Query(MdnsQuery),
    /// A response sent by a remote in response to one of our queries.
    Response(MdnsResponse),
    /// A request for service discovery.
    ServiceDiscovery(MdnsServiceDiscovery),
}

impl MdnsPacket {
    fn new_from_bytes(buf: &[u8], from: SocketAddr) -> Option<MdnsPacket> {
        match Packet::parse(buf) {
            Ok(packet) => {
                if packet.header.query {
                    if packet
                        .questions
                        .iter()
                        .any(|q| q.qname.to_string().as_bytes() == SERVICE_NAME)
                    {
                        let query = MdnsPacket::Query(MdnsQuery {
                            from,
                            query_id: packet.header.id,
                        });
                        return Some(query);
                    } else if packet
                        .questions
                        .iter()
                        .any(|q| q.qname.to_string().as_bytes() == META_QUERY_SERVICE)
                    {
                        // TODO: what if multiple questions, one with SERVICE_NAME and one with META_QUERY_SERVICE?
                        let discovery = MdnsPacket::ServiceDiscovery(
                            MdnsServiceDiscovery {
                                from,
                                query_id: packet.header.id,
                            },
                        );
                        return Some(discovery);
                    } else {
                        return None;
                    }
                } else {
                    let resp = MdnsPacket::Response(MdnsResponse::new (
                        packet,
                        from,
                    ));
                    return Some(resp);
                }
            }
            Err(_) => {
                return None;
            }
        }
    }
}

/// A received mDNS query.
pub struct MdnsQuery {
    /// Sender of the address.
    from: SocketAddr,
    /// Id of the received DNS query. We need to pass this ID back in the results.
    query_id: u16,
}

impl MdnsQuery {
    /// Source address of the packet.
    pub fn remote_addr(&self) -> &SocketAddr {
        &self.from
    }

    /// Query id of the packet.
    pub fn query_id(&self) -> u16 {
        self.query_id
    }
}

impl fmt::Debug for MdnsQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MdnsQuery")
            .field("from", self.remote_addr())
            .field("query_id", &self.query_id)
            .finish()
    }
}

/// A received mDNS service discovery query.
pub struct MdnsServiceDiscovery {
    /// Sender of the address.
    from: SocketAddr,
    /// Id of the received DNS query. We need to pass this ID back in the results.
    query_id: u16,
}

impl MdnsServiceDiscovery {
    /// Source address of the packet.
    pub fn remote_addr(&self) -> &SocketAddr {
        &self.from
    }

    /// Query id of the packet.
    pub fn query_id(&self) -> u16 {
        self.query_id
    }
}

impl fmt::Debug for MdnsServiceDiscovery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MdnsServiceDiscovery")
            .field("from", self.remote_addr())
            .field("query_id", &self.query_id)
            .finish()
    }
}

/// A received mDNS response.
pub struct MdnsResponse {
    peers: Vec<MdnsPeer>,
    from: SocketAddr,
}

impl MdnsResponse {
    /// Creates a new `MdnsResponse` based on the provided `Packet`.
    fn new(packet: Packet<'_>, from: SocketAddr) -> MdnsResponse {
        let peers = packet.answers.iter().filter_map(|record| {
            if record.name.to_string().as_bytes() != SERVICE_NAME {
                return None;
            }

            let record_value = match record.data {
                RData::PTR(record) => record.0.to_string(),
                _ => return None,
            };

            let mut peer_name = match record_value.rsplitn(4, |c| c == '.').last() {
                Some(n) => n.to_owned(),
                None => return None,
            };

            // if we have a segmented name, remove the '.'
            peer_name.retain(|c| c != '.');

            let peer_id = match data_encoding::BASE32_DNSCURVE.decode(peer_name.as_bytes()) {
                Ok(bytes) => match PeerId::from_bytes(bytes) {
                    Ok(id) => id,
                    Err(_) => return None,
                },
                Err(_) => return None,
            };

            Some(MdnsPeer::new (
                &packet,
                record_value,
                peer_id,
                record.ttl,
            ))
        }).collect();

        MdnsResponse {
            peers,
            from,
        }
    }

    /// Returns the list of peers that have been reported in this packet.
    ///
    /// > **Note**: Keep in mind that this will also contain the responses we sent ourselves.
    pub fn discovered_peers(&self) -> impl Iterator<Item = &MdnsPeer> {
        self.peers.iter()
    }

    /// Source address of the packet.
    #[inline]
    pub fn remote_addr(&self) -> &SocketAddr {
        &self.from
    }
}

impl fmt::Debug for MdnsResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MdnsResponse")
            .field("from", self.remote_addr())
            .finish()
    }
}

/// A peer discovered by the service.
pub struct MdnsPeer {
    addrs: Vec<Multiaddr>,
    /// Id of the peer.
    peer_id: PeerId,
    /// TTL of the record in seconds.
    ttl: u32,
}

impl MdnsPeer {
    /// Creates a new `MdnsPeer` based on the provided `Packet`.
    pub fn new(packet: &Packet<'_>, record_value: String, my_peer_id: PeerId, ttl: u32) -> MdnsPeer {
        let addrs = packet
            .additional
            .iter()
            .filter_map(|add_record| {
                if add_record.name.to_string() != record_value {
                    return None;
                }

                if let RData::TXT(ref txt) = add_record.data {
                    Some(txt)
                } else {
                    None
                }
            })
            .flat_map(|txt| txt.iter())
            .filter_map(|txt| {
                // TODO: wrong, txt can be multiple character strings
                let addr = match dns::decode_character_string(txt) {
                    Ok(a) => a,
                    Err(_) => return None,
                };
                if !addr.starts_with(b"dnsaddr=") {
                    return None;
                }
                let addr = match str::from_utf8(&addr[8..]) {
                    Ok(a) => a,
                    Err(_) => return None,
                };
                let mut addr = match addr.parse::<Multiaddr>() {
                    Ok(a) => a,
                    Err(_) => return None,
                };
                match addr.pop() {
                    Some(Protocol::P2p(peer_id)) => {
                        if let Ok(peer_id) = PeerId::try_from(peer_id) {
                            if peer_id != my_peer_id {
                                return None;
                            }
                        } else {
                            return None;
                        }
                    },
                    _ => return None,
                };
                Some(addr)
            }).collect();

        MdnsPeer {
            addrs,
            peer_id: my_peer_id.clone(),
            ttl,
        }
    }

    /// Returns the id of the peer.
    #[inline]
    pub fn id(&self) -> &PeerId {
        &self.peer_id
    }

    /// Returns the requested time-to-live for the record.
    #[inline]
    pub fn ttl(&self) -> Duration {
        Duration::from_secs(u64::from(self.ttl))
    }

    /// Returns the list of addresses the peer says it is listening on.
    ///
    /// Filters out invalid addresses.
    pub fn addresses(&self) -> &Vec<Multiaddr> {
        &self.addrs
    }
}

impl fmt::Debug for MdnsPeer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MdnsPeer")
            .field("peer_id", &self.peer_id)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    macro_rules! testgen {
        ($runtime_name:ident, $service_name:ty, $block_on_fn:tt) => {
    mod $runtime_name {
        use libp2p_core::{PeerId, multiaddr::multihash};
        use std::time::Duration;
        use crate::service::MdnsPacket;

        fn discover(peer_id: PeerId) {
            let fut = async {
                let mut service = <$service_name>::new().unwrap();

                loop {
                    let next = service.next().await;
                    service = next.0;

                    match next.1 {
                        MdnsPacket::Query(query) => {
                            let resp = crate::dns::build_query_response(
                                query.query_id(),
                                peer_id.clone(),
                                vec![].into_iter(),
                                Duration::from_secs(120),
                            ).unwrap();
                            service.enqueue_response(resp);
                        }
                        MdnsPacket::Response(response) => {
                            for peer in response.discovered_peers() {
                                if peer.id() == &peer_id {
                                    return;
                                }
                            }
                        }
                        MdnsPacket::ServiceDiscovery(_) => panic!(
                            "did not expect a service discovery packet",
                        )
                    }
                }
            };

            $block_on_fn(Box::pin(fut));
        }

        // As of today the underlying UDP socket is not stubbed out. Thus tests run in parallel to
        // this unit tests inter fear with it. Test needs to be run in sequence to ensure test
        // properties.
        #[test]
        fn respect_query_interval() {
            let own_ips: Vec<std::net::IpAddr> = get_if_addrs::get_if_addrs().unwrap()
                .into_iter()
                .map(|i| i.addr.ip())
                .collect();

            let fut = async {
                let mut service = <$service_name>::new().unwrap();

                let mut sent_queries = vec![];

                loop {
                    let next = service.next().await;
                    service = next.0;

                    match next.1 {
                        MdnsPacket::Query(query) => {
                            // Ignore queries from other nodes.
                            let source_ip = query.remote_addr().ip();
                            if !own_ips.contains(&source_ip) {
                                continue;
                            }

                            sent_queries.push(query);

                            if sent_queries.len() > 1 {
                                return;
                            }
                        }
                        // Ignore response packets. We don't stub out the UDP socket, thus this is
                        // either random noise from the network, or noise from other unit tests
                        // running in parallel.
                        MdnsPacket::Response(_) => {},
                        MdnsPacket::ServiceDiscovery(_) => {
                            panic!("Did not expect a service discovery packet.");
                        },
                    }
                }
            };

            $block_on_fn(Box::pin(fut));
        }

        #[test]
        fn discover_normal_peer_id() {
            discover(PeerId::random())
        }

        #[test]
        fn discover_long_peer_id() {
            let max_value = String::from_utf8(vec![b'f'; 42]).unwrap();
            let hash = multihash::Identity::digest(max_value.as_ref());
            discover(PeerId::from_multihash(hash).unwrap())
        }
    }
    }
    }

    #[cfg(feature = "async-std")]
    testgen!(
        async_std,
        crate::service::MdnsService,
        (|fut| async_std::task::block_on::<_, ()>(fut))
    );

    #[cfg(feature = "tokio")]
    testgen!(
        tokio,
        crate::service::TokioMdnsService,
        (|fut| tokio::runtime::Runtime::new().unwrap().block_on::<futures::future::BoxFuture<()>>(fut))
    );
}
