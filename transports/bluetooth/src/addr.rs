// Copyright 2019 Parity Technologies (UK) Ltd.
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

use std::{fmt, str::FromStr};

pub const ANY: Addr = Addr { inner: [0x00, 0x00, 0x00, 0x00, 0x00, 0x00] };
pub const ALL: Addr = Addr { inner: [0xff, 0xff, 0xff, 0xff, 0xff, 0xff] };
pub const LOCAL: Addr = Addr { inner: [0x00, 0x00, 0x00, 0xff, 0xff, 0xff] };

/// MAC address of a Bluetooth device.
#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Addr {
    /// Little-endian.
    inner: [u8; 6],
}

impl Addr {
    /// Builds an `Addr` from little-endian bytes.
    pub fn from_little_endian(bytes: [u8; 6]) -> Addr {
        Addr { inner: bytes }
    }

    /// Builds an `Addr` from big-endian bytes.
    pub fn from_big_endian(bytes: [u8; 6]) -> Addr {
        Addr {
            inner: [bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]]
        }
    }

    /// Returns the bytes as little endian.
    pub fn as_little_endian(&self) -> &[u8; 6] {
        &self.inner
    }

    /// Returns the bytes as little endian.
    pub fn to_little_endian(&self) -> [u8; 6] {
        self.inner
    }

    /// Returns the bytes as big endian.
    pub fn to_big_endian(&self) -> [u8; 6] {
        [self.inner[5], self.inner[4], self.inner[3], self.inner[2], self.inner[1], self.inner[0]]
    }
}

impl fmt::Display for Addr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X}",
            self.inner[5], self.inner[4], self.inner[3], self.inner[2], self.inner[1], self.inner[0])
    }
}

impl fmt::Debug for Addr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl FromStr for Addr {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut out = [0; 6];

        {
            let mut bytes = s.split(':');
            let mut out_iter = out.iter_mut();
            for (byte, o) in bytes.by_ref().zip(out_iter.by_ref()) {
                *o = u8::from_str_radix(byte, 16).map_err(|_| ())?;
            }
            if out_iter.next().is_some() || bytes.next().is_some() {
                return Err(());
            }
        }

        Ok(Addr::from_big_endian(out))
    }
}
