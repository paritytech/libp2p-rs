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

//! Provides the `TransportExt` trait.

use crate::{bandwidth::BandwidthLogging, bandwidth::BandwidthSinks, Transport};
use std::sync::Arc;

/// Trait automatically implemented on all objects that implement `Transport`. Provides some
/// additional utilities.
pub trait TransportExt: Transport {
    /// Adds a layer on the `Transport` that logs all trafic that passes through the sockets
    /// created by it.
    ///
    /// This method returns an `Arc<BandwidthSinks>` that can be used to retreive the total number
    /// of bytes transferred through the sockets.
    fn with_bandwidth_logging(self) -> (BandwidthLogging<Self>, Arc<BandwidthSinks>)
    where
        Self: Sized
    {
        BandwidthLogging::new(self)
    }

    /// Adds a layer on the `Transport` that remembers addresses that we failed to reach, and
    /// adds a knock-back delay before trying them again.
    fn with_hammering_prevention(self) -> HammerPrevention<Self>
    where
        Self: Sized
    {
        HammerPrevention::new(self)
    }

    // TODO: add methods to easily upgrade for secio/mplex/yamux
}

impl<TTransport> TransportExt for TTransport where TTransport: Transport {}
