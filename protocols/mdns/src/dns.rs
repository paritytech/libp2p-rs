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

//! (M)DNS encoding and decoding on top of the `dns_parser` library.

use crate::{META_QUERY_SERVICE, SERVICE_NAME};
use libp2p_core::{Multiaddr, PeerId};
use std::{borrow::Cow, cmp, error, fmt, str, time::Duration};

/// Maximum size of a DNS label as per RFC1035.
const MAX_LABEL_LENGTH: usize = 63;

/// DNS TXT records can have up to 255 characters as a single string value.
///
/// Current values are usually around 170-190 bytes long, varying primarily
/// with the length of the contained `Multiaddr`.
const MAX_TXT_VALUE_LENGTH: usize = 255;

/// A conservative maximum size (in bytes) of a complete TXT record,
/// as encoded by [`append_txt_record`].
const MAX_TXT_RECORD_SIZE: usize = MAX_TXT_VALUE_LENGTH + 45;

/// The maximum DNS packet size is 9000 bytes less the maximum
/// sizes of the IP (60) and UDP (8) headers.
const MAX_PACKET_SIZE: usize = 9000 - 68;

/// A conservative maximum number of records that can be packed into
/// a single DNS UDP packet, allowing up to 100 bytes of MDNS packet
/// header data to be added by [`query_response_packet()`].
const MAX_RECORDS_PER_PACKET: usize = (MAX_PACKET_SIZE - 100) / MAX_TXT_RECORD_SIZE;

/// An encoded MDNS packet.
pub type MdnsPacket = Vec<u8>;

/// Decodes a `<character-string>` (as defined by RFC1035) into a `Vec` of ASCII characters.
// TODO: better error type?
pub fn decode_character_string(mut from: &[u8]) -> Result<Cow<'_, [u8]>, ()> {
    if from.is_empty() {
        return Ok(Cow::Owned(Vec::new()));
    }

    // Remove the initial and trailing " if any.
    if from[0] == b'"' {
        if from.len() == 1 || from.last() != Some(&b'"') {
            return Err(());
        }
        let len = from.len();
        from = &from[1..len - 1];
    }

    // TODO: remove the backslashes if any
    Ok(Cow::Borrowed(from))
}

/// Builds the binary representation of a DNS query to send on the network.
pub fn build_query() -> MdnsPacket {
    let mut out = Vec::with_capacity(33);

    // Program-generated transaction ID; unused by our implementation.
    append_u16(&mut out, rand::random());

    // 0x0 flag for a regular query.
    append_u16(&mut out, 0x0);

    // Number of questions.
    append_u16(&mut out, 0x1);

    // Number of answers, authorities, and additionals.
    append_u16(&mut out, 0x0);
    append_u16(&mut out, 0x0);
    append_u16(&mut out, 0x0);

    // Our single question.
    // The name.
    append_qname(&mut out, SERVICE_NAME);

    // Flags.
    append_u16(&mut out, 0x0c);
    append_u16(&mut out, 0x01);

    // Since the output is constant, we reserve the right amount ahead of time.
    // If this assert fails, adjust the capacity of `out` in the source code.
    debug_assert_eq!(out.capacity(), out.len());
    out
}

/// Builds the response to an address discovery DNS query.
///
/// If there are more than 2^16-1 addresses, ignores the rest.
pub fn build_query_response(
    id: u16,
    peer_id: PeerId,
    addresses: impl ExactSizeIterator<Item = Multiaddr>,
    ttl: Duration,
) -> Vec<MdnsPacket> {
    // Convert the TTL into seconds.
    let ttl = duration_to_secs(ttl);

    // Add a limit to 2^16-1 addresses, as the protocol limits to this number.
    let addresses = addresses.take(65535);

    let peer_id_bytes = encode_peer_id(&peer_id);
    debug_assert!(peer_id_bytes.len() <= 0xffff);

    // The accumulated response packets.
    let mut packets = Vec::new();

    // The records accumulated per response packet.
    let mut records = Vec::with_capacity(addresses.len() * MAX_TXT_RECORD_SIZE);

    // Encode the addresses as TXT records, and multiple TXT records into a
    // response packet.
    for addr in addresses {
        let txt_to_send = format!("dnsaddr={}/p2p/{}", addr.to_string(), peer_id.to_base58());
        let mut txt_record = Vec::with_capacity(txt_to_send.len());
        match append_txt_record(&mut txt_record, &peer_id_bytes, ttl, &txt_to_send) {
            Ok(()) => {
                records.push(txt_record);
            }
            Err(e) => {
                log::warn!("Excluding address {} from response: {:?}", addr, e);
            }
        }

        if records.len() == MAX_RECORDS_PER_PACKET {
            packets.push(query_response_packet(id, &peer_id_bytes, &records, ttl));
            records.clear();
        }
    }

    // If there are still unpacked records, i.e. if the number of records is not
    // a multiple of `MAX_RECORDS_PER_PACKET`, create a final packet.
    if !records.is_empty() {
        packets.push(query_response_packet(id, &peer_id_bytes, &records, ttl));
    }

    // If no packets have been built at all, because `addresses` is empty,
    // construct an empty response packet.
    if packets.is_empty() {
        packets.push(query_response_packet(id, &peer_id_bytes, &Vec::new(), ttl));
    }

    packets
}

/// Builds the response to a service discovery DNS query.
pub fn build_service_discovery_response(id: u16, ttl: Duration) -> MdnsPacket {
    // Convert the TTL into seconds.
    let ttl = duration_to_secs(ttl);

    // This capacity was determined empirically.
    let mut out = Vec::with_capacity(69);

    append_u16(&mut out, id);
    // 0x84 flag for an answer.
    append_u16(&mut out, 0x8400);
    // Number of questions, answers, authorities, additionals.
    append_u16(&mut out, 0x0);
    append_u16(&mut out, 0x1);
    append_u16(&mut out, 0x0);
    append_u16(&mut out, 0x0);

    // Our single answer.
    // The name.
    append_qname(&mut out, META_QUERY_SERVICE);

    // Flags.
    append_u16(&mut out, 0x000c);
    append_u16(&mut out, 0x8001);

    // TTL for the answer
    append_u32(&mut out, ttl);

    // Service name.
    {
        let mut name = Vec::with_capacity(SERVICE_NAME.len() + 2);
        append_qname(&mut name, SERVICE_NAME);
        append_u16(&mut out, name.len() as u16);
        out.extend_from_slice(&name);
    }

    // Since the output size is constant, we reserve the right amount ahead of time.
    // If this assert fails, adjust the capacity of `out` in the source code.
    debug_assert_eq!(out.capacity(), out.len());
    out
}

/// Constructs an MDNS query response packet for an address lookup.
fn query_response_packet(id: u16, peer_id: &[u8], records: &[Vec<u8>], ttl: u32) -> MdnsPacket {
    let mut out = Vec::with_capacity(records.len() * MAX_TXT_RECORD_SIZE);

    append_u16(&mut out, id);
    // 0x84 flag for an answer.
    append_u16(&mut out, 0x8400);
    // Number of questions, answers, authorities, additionals.
    append_u16(&mut out, 0x0);
    append_u16(&mut out, 0x1);
    append_u16(&mut out, 0x0);
    append_u16(&mut out, records.len() as u16);

    // Our single answer.
    // The name.
    append_qname(&mut out, SERVICE_NAME);

    // Flags.
    append_u16(&mut out, 0x000c);
    append_u16(&mut out, 0x0001);

    // TTL for the answer
    append_u32(&mut out, ttl);

    // Peer Id.
    append_u16(&mut out, peer_id.len() as u16);
    out.extend_from_slice(&peer_id);

    // The TXT records.
    for record in records {
        out.extend_from_slice(&record);
    }

    out
}

/// Returns the number of secs of a duration.
fn duration_to_secs(duration: Duration) -> u32 {
    let secs = duration
        .as_secs()
        .saturating_add(if duration.subsec_nanos() > 0 { 1 } else { 0 });
    cmp::min(secs, From::from(u32::max_value())) as u32
}

/// Appends a big-endian u32 to `out`.
fn append_u32(out: &mut Vec<u8>, value: u32) {
    out.push(((value >> 24) & 0xff) as u8);
    out.push(((value >> 16) & 0xff) as u8);
    out.push(((value >> 8) & 0xff) as u8);
    out.push((value & 0xff) as u8);
}

/// Appends a big-endian u16 to `out`.
fn append_u16(out: &mut Vec<u8>, value: u16) {
    out.push(((value >> 8) & 0xff) as u8);
    out.push((value & 0xff) as u8);
}

/// If a peer ID is longer than 63 characters, split it into segments to
/// be compatible with RFC 1035.
fn segment_peer_id(peer_id: String) -> String {
    // Guard for the most common case
    if peer_id.len() <= MAX_LABEL_LENGTH {
        return peer_id;
    }

    // This will only perform one allocation except in extreme circumstances.
    let mut out = String::with_capacity(peer_id.len() + 8);

    for (idx, chr) in peer_id.chars().enumerate() {
        if idx > 0 && idx % MAX_LABEL_LENGTH == 0 {
            out.push('.');
        }
        out.push(chr);
    }
    out
}

/// Combines and encodes a `PeerId` and service name for a DNS query.
fn encode_peer_id(peer_id: &PeerId) -> Vec<u8> {
    // DNS-safe encoding for the Peer ID
    let raw_peer_id = data_encoding::BASE32_DNSCURVE.encode(&peer_id.to_bytes());
    // ensure we don't have any labels over 63 bytes long
    let encoded_peer_id = segment_peer_id(raw_peer_id);
    let service_name = str::from_utf8(SERVICE_NAME).expect("SERVICE_NAME is always ASCII");
    let peer_name = [&encoded_peer_id, service_name].join(".");

    // allocate with a little extra padding for QNAME encoding
    let mut peer_id_bytes = Vec::with_capacity(peer_name.len() + 32);
    append_qname(&mut peer_id_bytes, peer_name.as_bytes());

    peer_id_bytes
}

/// Appends a `QNAME` (as defined by RFC1035) to the `Vec`.
///
/// # Panic
///
/// Panics if `name` has a zero-length component or a component that is too long.
/// This is fine considering that this function is not public and is only called in a controlled
/// environment.
///
fn append_qname(out: &mut Vec<u8>, name: &[u8]) {
    debug_assert!(name.is_ascii());

    for element in name.split(|&c| c == b'.') {
        assert!(element.len() < 64, "Service name has a label too long");
        assert_ne!(element.len(), 0, "Service name contains zero length label");
        out.push(element.len() as u8);
        for chr in element.iter() {
            out.push(*chr);
        }
    }

    out.push(0);
}

/// Appends a `<character-string>` (as defined by RFC1035) to the `Vec`.
fn append_character_string(out: &mut Vec<u8>, ascii_str: &str) -> Result<(), MdnsResponseError> {
    if !ascii_str.is_ascii() {
        return Err(MdnsResponseError::NonAsciiMultiaddr);
    }

    if !ascii_str.bytes().any(|c| c == b' ') {
        out.extend_from_slice(ascii_str.as_bytes());
        return Ok(());
    }

    out.push(b'"');

    for &chr in ascii_str.as_bytes() {
        if chr == b'\\' {
            out.push(b'\\');
            out.push(b'\\');
        } else if chr == b'"' {
            out.push(b'\\');
            out.push(b'"');
        } else {
            out.push(chr);
        }
    }

    out.push(b'"');
    Ok(())
}

/// Appends a TXT record to `out`.
fn append_txt_record(
    out: &mut Vec<u8>,
    name: &[u8],
    ttl_secs: u32,
    value: &str,
) -> Result<(), MdnsResponseError> {
    // The name.
    out.extend_from_slice(name);

    // Flags.
    out.push(0x00);
    out.push(0x10); // TXT record.
    out.push(0x80);
    out.push(0x01);

    // TTL for the answer
    append_u32(out, ttl_secs);

    // Add the strings.
    if value.len() > MAX_TXT_VALUE_LENGTH {
        return Err(MdnsResponseError::TxtRecordTooLong);
    }
    let mut buffer = Vec::new();
    buffer.push(value.len() as u8);
    append_character_string(&mut buffer, value)?;

    append_u16(out, buffer.len() as u16);
    out.extend_from_slice(&buffer);
    Ok(())
}

/// Errors that can occur on encoding an MDNS response.
#[derive(Debug)]
enum MdnsResponseError {
    TxtRecordTooLong,
    NonAsciiMultiaddr,
}

impl fmt::Display for MdnsResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MdnsResponseError::TxtRecordTooLong => {
                write!(f, "TXT record invalid because it is too long")
            }
            MdnsResponseError::NonAsciiMultiaddr => write!(
                f,
                "A multiaddr contains non-ASCII characters when serialized"
            ),
        }
    }
}

impl error::Error for MdnsResponseError {}

#[cfg(test)]
mod tests {
    use super::*;
    use dns_parser::Packet;
    use libp2p_core::identity;
    use std::time::Duration;

    #[test]
    fn build_query_correct() {
        let query = build_query();
        assert!(Packet::parse(&query).is_ok());
    }

    #[test]
    fn build_query_response_correct() {
        let my_peer_id = identity::Keypair::generate_ed25519()
            .public()
            .into_peer_id();
        let addr1 = "/ip4/1.2.3.4/tcp/5000".parse().unwrap();
        let addr2 = "/ip6/::1/udp/10000".parse().unwrap();
        let packets = build_query_response(
            0xf8f8,
            my_peer_id,
            vec![addr1, addr2].into_iter(),
            Duration::from_secs(60),
        );
        for packet in packets {
            assert!(Packet::parse(&packet).is_ok());
        }
    }

    #[test]
    fn build_service_discovery_response_correct() {
        let query = build_service_discovery_response(0x1234, Duration::from_secs(120));
        assert!(Packet::parse(&query).is_ok());
    }

    #[test]
    fn test_segment_peer_id() {
        let str_32 = String::from_utf8(vec![b'x'; 32]).unwrap();
        let str_63 = String::from_utf8(vec![b'x'; 63]).unwrap();
        let str_64 = String::from_utf8(vec![b'x'; 64]).unwrap();
        let str_126 = String::from_utf8(vec![b'x'; 126]).unwrap();
        let str_127 = String::from_utf8(vec![b'x'; 127]).unwrap();

        assert_eq!(segment_peer_id(str_32.clone()), str_32);
        assert_eq!(segment_peer_id(str_63.clone()), str_63);

        assert_eq!(segment_peer_id(str_64), [&str_63, "x"].join("."));
        assert_eq!(
            segment_peer_id(str_126),
            [&str_63, str_63.as_str()].join(".")
        );
        assert_eq!(segment_peer_id(str_127), [&str_63, &str_63, "x"].join("."));
    }

    // TODO: test limits and errors
}
