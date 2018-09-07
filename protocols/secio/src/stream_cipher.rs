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

use super::codec::StreamCipher;
use crypto::{aessafe, blockmodes::CtrModeX8, symmetriccipher::SynchronousStreamCipher};

#[derive(Clone, Copy, Debug)]
pub enum Cipher {
    Aes128,
    Aes256,
    Null
}

impl Cipher {
    /// Returns the size of in bytes of the key expected by the cipher.
    pub fn key_size(&self) -> usize {
        match *self {
            Cipher::Aes128 => 16,
            Cipher::Aes256 => 32,
            Cipher::Null => 0
        }
    }

    /// Returns the size of in bytes of the IV expected by the cipher.
    #[inline]
    pub fn iv_size(&self) -> usize {
        match self {
            Cipher::Aes128 | Cipher::Aes256 => 16,
            Cipher::Null => 0
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct NullCipher;

impl SynchronousStreamCipher for NullCipher {
    fn process(&mut self, input: &[u8], output: &mut [u8]) {
        output.copy_from_slice(input)
    }
}

/// Returns your stream cipher depending on `Cipher`.
#[inline]
pub fn ctr(c: Cipher, key: &[u8], iv: &[u8]) -> StreamCipher {
    match c {
        Cipher::Aes128 => {
            let aes_dec = aessafe::AesSafe128EncryptorX8::new(key);
            Box::new(CtrModeX8::new(aes_dec, iv))
        },
        Cipher::Aes256 => {
            let aes_dec = aessafe::AesSafe256EncryptorX8::new(key);
            Box::new(CtrModeX8::new(aes_dec, iv))
        },
        Cipher::Null => Box::new(NullCipher)
    }
}

