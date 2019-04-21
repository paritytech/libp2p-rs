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

use libp2p_core::PeerId;
use sha2::{Digest, Sha256};

/// Successful and verified proof of work.
pub struct Pow {
    nonce: u32,
}

impl Pow {
    /// Returns the nonce of the proof-of-work.
    #[inline]
    pub fn nonce(&self) -> u32 {
        self.nonce
    }

    /// Verifies that the `nonce` satisfies a proof of work.
    ///
    /// > **Note**: Keep in mind that if a remote sends you a proof of work, the "local" and
    /// >           "remote" `PeerId`s will be reversed.
    pub fn verify(local_peer_id: &PeerId, remote_peer_id: &PeerId, nonce: u32, leading_zeros: u8) -> Result<Self, ()> {
        let mut base = Sha256::new();
        base.input(local_peer_id.as_bytes());
        base.input(remote_peer_id.as_bytes());
        base.input(&[
            ((nonce >> 24) & 0xff) as u8,
            ((nonce >> 16) & 0xff) as u8,
            ((nonce >> 8) & 0xff) as u8,
            (nonce & 0xff) as u8
        ]);

        let hash = base.result();
        if slices_leading_zeros(hash.as_ref()) < u32::from(leading_zeros) {
            return Err(());
        }

        Ok(Pow { nonce })
    }

    /// Generates a proof-of-work for the given parameters.
    ///
    /// > **Note**: This function is CPU-intensive.
    ///
    /// In the unlikely situation where we cannot find a valid PoW, an error is returned.
    pub fn generate(local_peer_id: &PeerId, remote_peer_id: &PeerId, leading_zeros: u8) -> Result<Self, ()> {
        let leading_zeros = u32::from(leading_zeros);

        let mut base = Sha256::new();
        base.input(local_peer_id.as_bytes());
        base.input(remote_peer_id.as_bytes());

        let mut nonce: u32 = rand::random();
        let nonce_end = nonce.wrapping_sub(1);

        while nonce != nonce_end {
            let mut base = base.clone();
            base.input(&[
                ((nonce >> 24) & 0xff) as u8,
                ((nonce >> 16) & 0xff) as u8,
                ((nonce >> 8) & 0xff) as u8,
                (nonce & 0xff) as u8
            ]);

            let hash = base.result();
            if slices_leading_zeros(&hash) < leading_zeros {
                nonce = nonce.wrapping_add(1);
                continue;
            }

            return Ok(Pow {
                nonce
            });
        }

        Err(())
    }
}

/// Returns the number of leading zero bits of a slice of bytes. Stops as soon as a non-zero bit is
/// detected.
fn slices_leading_zeros(slice: &[u8]) -> u32 {
    let mut counter = 0;
    for &byte in slice.iter() {
        let byte_zeros = byte.leading_zeros();
        counter += byte_zeros;
        if byte_zeros != 8 {
            break;
        }
    }
    counter
}

#[cfg(test)]
mod tests {
    use crate::pow::{Pow, slices_leading_zeros};
    use libp2p_core::PeerId;

    #[test]
    fn finishes_in_time() {
        let id1 = PeerId::random();
        let id2 = PeerId::random();
        for _ in 0..10 {
            if Pow::generate(&id1, &id2, 20).is_ok() {
                return
            }
        }
        panic!()
    }

    #[test]
    fn verify_positive() {
        const DIFFICULTY: u8 = 8;

        for _ in 0 .. 1000 {
            let id1 = PeerId::random();
            let id2 = PeerId::random();

            let pow = Pow::generate(&id1, &id2, DIFFICULTY).unwrap();
            assert!(Pow::verify(&id1, &id2, pow.nonce(), DIFFICULTY).is_ok());
        }
    }

    #[test]
    fn verify_negative() {
        const DIFFICULTY: u8 = 48;

        for _ in 0 .. 10000 {
            let id1 = PeerId::random();
            let id2 = PeerId::random();
            assert!(Pow::verify(&id1, &id2, rand::random(), DIFFICULTY).is_err());
        }
    }

    #[test]
    fn correct_slices_leading_zeros() {
        assert_eq!(slices_leading_zeros(&[]), 0);
        assert_eq!(slices_leading_zeros(&[0xff]), 0);
        assert_eq!(slices_leading_zeros(&[0x80]), 0);
        assert_eq!(slices_leading_zeros(&[0x40]), 1);
        assert_eq!(slices_leading_zeros(&[0x40, 0x0, 0x0]), 1);
        assert_eq!(slices_leading_zeros(&[0x3f]), 2);
        assert_eq!(slices_leading_zeros(&[0x0]), 8);
        assert_eq!(slices_leading_zeros(&[0x0, 0x40]), 9);
        assert_eq!(slices_leading_zeros(&[0x0, 0x1]), 15);
        assert_eq!(slices_leading_zeros(&[0x0, 0x0]), 16);
        assert_eq!(slices_leading_zeros(&[0x0, 0x0, 0xff]), 16);
        assert_eq!(slices_leading_zeros(&[0x0, 0x0, 0x7f]), 17);
        assert_eq!(slices_leading_zeros(&[0x0, 0x0, 0x40]), 17);
        assert_eq!(slices_leading_zeros(&[0x0, 0x0, 0x40, 0x0]), 17);
        assert_eq!(slices_leading_zeros(&[0x0, 0x0, 0x7f, 0xff]), 17);
    }
}
