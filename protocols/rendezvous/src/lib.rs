// Copyright 2021 COMIT Network.
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

pub use self::behaviour::{Config, Event, RegisterError, Rendezvous};
pub use self::codec::{ErrorCode, Namespace, NamespaceTooLong, Registration, Ttl};

mod behaviour;
mod codec;
mod handler;
mod substream_handler;

/// If unspecified, rendezvous nodes should assume a TTL of 2h.
///
/// See <https://github.com/libp2p/specs/blob/d21418638d5f09f2a4e5a1ceca17058df134a300/rendezvous/README.md#L116-L117>.
pub const DEFAULT_TTL: Ttl = 60 * 60 * 2;

/// By default, nodes should require a minimum TTL of 2h
///
/// <https://github.com/libp2p/specs/tree/master/rendezvous#recommendations-for-rendezvous-points-configurations>.
pub const MIN_TTL: Ttl = 60 * 60 * 2;

/// By default, nodes should allow a maximum TTL of 72h
///
/// <https://github.com/libp2p/specs/tree/master/rendezvous#recommendations-for-rendezvous-points-configurations>.
pub const MAX_TTL: Ttl = 60 * 60 * 72;
