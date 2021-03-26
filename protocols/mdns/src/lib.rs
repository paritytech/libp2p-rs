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

//! mDNS is a protocol defined by [RFC 6762](https://tools.ietf.org/html/rfc6762) that allows
//! querying nodes that correspond to a certain domain name.
//!
//! In the context of libp2p, the mDNS protocol is used to discover other nodes on the local
//! network that support libp2p.
//!
//! # Usage
//!
//! This crate provides the `Mdns` struct which implements the `NetworkBehaviour` trait. This
//! struct will automatically discover other libp2p nodes on the local network.
//!

/// The DNS service name for all libp2p peers used to query for addresses.
const SERVICE_NAME: &[u8] = b"_p2p._udp.local";
/// The meta query for looking up the `SERVICE_NAME`.
const META_QUERY_SERVICE: &[u8] = b"_services._dns-sd._udp.local";

pub use crate::behaviour::{Mdns, MdnsConfig, MdnsEvent};

mod behaviour;
mod dns;
mod query;
