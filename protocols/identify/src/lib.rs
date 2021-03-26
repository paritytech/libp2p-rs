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

//! Implementation of the [Identify] protocol.
//!
//! This implementation of the protocol periodically exchanges
//! [`IdentifyInfo`] messages between the peers on an established connection.
//!
//! At least one identification request is sent on a newly established
//! connection, beyond which the behaviour does not keep connections alive.
//!
//! # Usage
//!
//! The [`Identify`] struct implements a `NetworkBehaviour` that negotiates
//! and executes the protocol on every established connection, emitting
//! [`IdentifyEvent`]s.
//!
//! [Identify]: https://github.com/libp2p/specs/tree/master/identify
//! [`Identify`]: self::Identify
//! [`IdentifyEvent`]: self::IdentifyEvent
//! [`IdentifyInfo`]: self::IdentifyInfo

pub use self::identify::{Identify, IdentifyConfig, IdentifyEvent};
pub use self::protocol::IdentifyInfo;

mod handler;
mod identify;
mod protocol;

mod structs_proto {
    include!(concat!(env!("OUT_DIR"), "/structs.rs"));
}

