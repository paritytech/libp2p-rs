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

use crate::{dns, META_QUERY_SERVICE, SERVICE_NAME};
use dns_parser::{Packet, RData};
use libp2p_core::{
    multiaddr::{Multiaddr, Protocol},
    PeerId,
};
use std::{convert::TryFrom, fmt, net::SocketAddr, str, time::Duration};

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
    pub fn new_from_bytes(buf: &[u8], from: SocketAddr) -> Option<MdnsPacket> {
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
                        Some(query)
                    } else if packet
                        .questions
                        .iter()
                        .any(|q| q.qname.to_string().as_bytes() == META_QUERY_SERVICE)
                    {
                        // TODO: what if multiple questions, one with SERVICE_NAME and one with META_QUERY_SERVICE?
                        let discovery = MdnsPacket::ServiceDiscovery(MdnsServiceDiscovery {
                            from,
                            query_id: packet.header.id,
                        });
                        Some(discovery)
                    } else {
                        None
                    }
                } else {
                    let resp = MdnsPacket::Response(MdnsResponse::new(packet, from));
                    Some(resp)
                }
            }
            Err(err) => {
                log::debug!("Parsing mdns packet failed: {:?}", err);
                None
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
    pub fn new(packet: Packet<'_>, from: SocketAddr) -> MdnsResponse {
        let peers = packet
            .answers
            .iter()
            .filter_map(|record| {
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
                    Ok(bytes) => match PeerId::from_bytes(&bytes) {
                        Ok(id) => id,
                        Err(_) => return None,
                    },
                    Err(_) => return None,
                };

                Some(MdnsPeer::new(&packet, record_value, peer_id, record.ttl))
            })
            .collect();

        MdnsResponse { peers, from }
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
    pub fn new(
        packet: &Packet<'_>,
        record_value: String,
        my_peer_id: PeerId,
        ttl: u32,
    ) -> MdnsPeer {
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
                    }
                    _ => return None,
                };
                Some(addr)
            })
            .collect();

        MdnsPeer {
            addrs,
            peer_id: my_peer_id,
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
