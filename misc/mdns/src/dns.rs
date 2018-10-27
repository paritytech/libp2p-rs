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

use data_encoding;
use libp2p_core::{Multiaddr, PeerId};
use rand;
use std::{cmp, error, fmt, str, time::Duration};
use {META_QUERY_SERVICE, SERVICE_NAME};

/// Decodes a `<character-string>` (as defined by RFC1035) into a `Vec` of ASCII characters.
// TODO: better error type?
pub fn decode_character_string(mut from: &[u8]) -> Result<Vec<u8>, ()> {
    if from.is_empty() {
        return Ok(Vec::new());
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
    Ok(from.to_vec())
}

/// Builds the binary representation of a DNS query to send on the network.
pub fn build_query() -> Vec<u8> {
    let mut out = Vec::with_capacity(33);

    // Program-generated transaction ID ; unused by our implementation.
    append_u16(&mut out, rand::random());

    // Flags ; 0x0 for a regular query.
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
    append_u16(&mut out, 0xc);
    append_u16(&mut out, 0x1);

    // Since the output is constant, we reserve the right amount ahead of time.
    // If this assert fails, adjust the capacity of `out` in the source code.
    debug_assert_eq!(out.capacity(), out.len());
    out
}

/// Builds the response to the DNS query.
///
/// If there are more than 2^16-1 addresses, ignores the rest.
pub fn build_query_response(
    id: u16,
    peer_id: PeerId,
    addresses: impl ExactSizeIterator<Item = Multiaddr>,
    ttl: Duration,
) -> Result<Vec<u8>, MdnsResponseError> {
    // Convert the TTL into seconds.
    let ttl = duration_to_secs(ttl);

    // Add a limit to 2^16-1 addresses, as the protocol limits to this number.
    let addresses = addresses.take(65535);

    // This capacity was determined empirically and is a reasonable upper limit.
    let mut out = Vec::with_capacity(320);

    append_u16(&mut out, id);
    // Flags ; 0x80 for an answer.
    append_u16(&mut out, 0x8000);
    // Number of questions, answers, authorities, additionals.
    append_u16(&mut out, 0x0);
    append_u16(&mut out, 0x1);
    append_u16(&mut out, 0x0);
    append_u16(&mut out, addresses.len() as u16);

    // Our single answer.
    // The name.
    append_qname(&mut out, SERVICE_NAME);

    // Flags.
    out.push(0x00);
    out.push(0x0c);
    out.push(0x80);
    out.push(0x01);

    // TTL for the answer
    append_u32(&mut out, ttl);

    let peer_id_base58 = peer_id.to_base58();

    // Peer Id.
    let peer_name = format!(
        "{}.{}",
        data_encoding::BASE32_DNSCURVE.encode(&peer_id.into_bytes()),
        str::from_utf8(SERVICE_NAME).expect("SERVICE_NAME is always ASCII")
    );
    let mut peer_id_bytes = Vec::with_capacity(64);
    append_qname(&mut peer_id_bytes, peer_name.as_bytes());
    debug_assert!(peer_id_bytes.len() <= 0xffff);
    append_u16(&mut out, peer_id_bytes.len() as u16);
    out.extend_from_slice(&peer_id_bytes);

    // The TXT records for answers.
    for addr in addresses {
        let txt_to_send = format!("dnsaddr={}/p2p/{}", addr.to_string(), peer_id_base58);
        let mut txt_to_send_bytes = Vec::with_capacity(txt_to_send.len());
        append_character_string(&mut txt_to_send_bytes, txt_to_send.as_bytes())?;
        append_txt_record(&mut out, &peer_id_bytes, ttl, Some(&txt_to_send_bytes[..]))?;
    }

    if out.len() > 9000 {
        return Err(MdnsResponseError::ResponseTooLong);
    }

    Ok(out)
}

/// Builds the response to the DNS query.
pub fn build_service_discovery_response(id: u16, ttl: Duration) -> Vec<u8> {
    // Convert the TTL into seconds.
    let ttl = duration_to_secs(ttl);

    // This capacity was determined empirically.
    let mut out = Vec::with_capacity(69);

    append_u16(&mut out, id);
    // Flags ; 0x80 for an answer.
    append_u16(&mut out, 0x8000);
    // Number of questions, answers, authorities, additionals.
    append_u16(&mut out, 0x0);
    append_u16(&mut out, 0x1);
    append_u16(&mut out, 0x0);
    append_u16(&mut out, 0x0);

    // Our single answer.
    // The name.
    append_qname(&mut out, META_QUERY_SERVICE);

    // Flags.
    out.push(0x00);
    out.push(0x0c);
    out.push(0x80);
    out.push(0x01);

    // TTL for the answer
    append_u32(&mut out, ttl);

    // Service name.
    {
        let mut name = Vec::new();
        append_qname(&mut name, SERVICE_NAME);
        append_u16(&mut out, name.len() as u16);
        out.extend_from_slice(&name);
    }

    // Since the output size is constant, we reserve the right amount ahead of time.
    // If this assert fails, adjust the capacity of `out` in the source code.
    debug_assert_eq!(out.capacity(), out.len());
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
        assert!(element.len() < 256, "Service name has a label too long");
        assert_ne!(element.len(), 0, "Service name contains zero length label");
        out.push(element.len() as u8);
        for chr in element.iter() {
            out.push(*chr);
        }
    }

    out.push(0);
}

/// Appends a `<character-string>` (as defined by RFC1035) to the `Vec`.
fn append_character_string(out: &mut Vec<u8>, ascii_str: &[u8]) -> Result<(), MdnsResponseError> {
    if !ascii_str.is_ascii() {
        return Err(MdnsResponseError::NonAsciiMultiaddr);
    }

    if !ascii_str.iter().any(|&c| c == b' ') {
        for &chr in ascii_str.iter() {
            out.push(chr);
        }
        return Ok(());
    }

    out.push(b'"');

    for &chr in ascii_str.iter() {
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

/// Appends a TXT record to the answer in `out`.
fn append_txt_record<'a>(
    out: &mut Vec<u8>,
    name: &[u8],
    ttl_secs: u32,
    entries: impl IntoIterator<Item = &'a [u8]>,
) -> Result<(), MdnsResponseError> {
    // The name.
    out.extend_from_slice(name);

    // Flags.
    out.push(0x00);
    out.push(0x10);
    out.push(0x80);
    out.push(0x01);

    // TTL for the answer
    append_u32(out, ttl_secs);

    // Add the strings.
    let mut buffer = Vec::new();
    for entry in entries {
        if entry.len() > u8::max_value() as usize {
            return Err(MdnsResponseError::TxtRecordTooLong);
        }
        buffer.push(entry.len() as u8);
        buffer.extend_from_slice(entry);
    }

    // It is illegal to have an empty TXT record, but we can have one zero-bytes entry, which does
    // the same.
    if buffer.is_empty() {
        buffer.push(0);
    }

    if buffer.len() > u16::max_value() as usize {
        return Err(MdnsResponseError::TxtRecordTooLong);
    }
    append_u16(out, buffer.len() as u16);
    out.extend_from_slice(&buffer);
    Ok(())
}

/// Error that can happen when producing a DNS response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MdnsResponseError {
    TxtRecordTooLong,
    NonAsciiMultiaddr,
    ResponseTooLong,
}

impl fmt::Display for MdnsResponseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MdnsResponseError::TxtRecordTooLong => {
                write!(f, "TXT record invalid because it is too long")
            }
            MdnsResponseError::NonAsciiMultiaddr => write!(
                f,
                "A multiaddr contains non-ASCII characters when serializd"
            ),
            MdnsResponseError::ResponseTooLong => write!(f, "DNS response is too long"),
        }
    }
}

impl error::Error for MdnsResponseError {}

#[cfg(test)]
mod tests {
    use super::*;
    use dns_parser::Packet;
    use libp2p_core::{PeerId, PublicKey};
    use std::time::Duration;

    #[test]
    fn build_query_correct() {
        let query = build_query();
        assert!(Packet::parse(&query).is_ok());
    }

    #[test]
    fn build_query_response_correct() {
        let my_peer_id = PeerId::from_public_key(PublicKey::Rsa(vec![1, 2, 3, 4]));
        let addr1 = "/ip4/1.2.3.4/tcp/5000".parse().unwrap();
        let addr2 = "/ip6/::1/udp/10000".parse().unwrap();
        let query = build_query_response(
            0xf8f8,
            my_peer_id,
            vec![addr1, addr2].into_iter(),
            Duration::from_secs(60),
        )
        .unwrap();
        assert!(Packet::parse(&query).is_ok());
    }

    #[test]
    fn build_service_discovery_response_correct() {
        let query = build_service_discovery_response(0x1234, Duration::from_secs(120));
        assert!(Packet::parse(&query).is_ok());
    }

    // TODO: test limits and errors
}
