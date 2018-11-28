// Copyright 2017-2018 Parity Technologies (UK) Ltd.
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

use bytes::Bytes;
use crate::upgrade::Upgrade;
use futures::future;
use std::iter;
use void::{unreachable, Void};

/// Dummy implementation of `Upgrade` that doesn't support any protocol.
#[derive(Debug, Copy, Clone)]
pub struct DeniedUpgrade;

impl<T> Upgrade<T> for DeniedUpgrade {
    type UpgradeId = Void;
    type NamesIter = iter::Empty<(Bytes, Self::UpgradeId)>;
    type Output = Void;
    type Error = Void;
    type Future = future::Empty<Self::Output, Self::Error>;

    fn protocol_names(&self) -> Self::NamesIter {
        iter::empty()
    }

    fn upgrade(self, _: T, id: Self::UpgradeId) -> Self::Future {
        unreachable(id)
    }
}
