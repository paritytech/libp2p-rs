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

//! Contains the `Sampler` struct. See the doc-comment of this struct for more info.

use rand::distributions::{self, Distribution as _};
use smallvec::SmallVec;
use sha2::digest::{generic_array::GenericArray, BlockInput};
use sha2::{Digest, Sha256};

/// A sampler is a data structure where you can insert elements, and that can be sampled to obtain
/// a stream of elements. The output stream contains an approximately uniform distribution of the
/// content, ignoring redundancy.
///
/// For example, if you insert the value `A` one million times, the value `B` one thousand times,
/// and the value `C` once, then sampling has approximately 33% chance to produce `A`, 33% chance
/// to produce `B`, and 33% chance to produce `C`.
pub struct Sampler<TValue, TDigest: Digest + BlockInput = Sha256> {
    /// All the individual samplers. Each sampler holds one value.
    samplers: SmallVec<[IndivSampler<TValue, TDigest>; 32]>,
}

struct IndivSampler<TValue, TDigest: Digest + BlockInput> {
    /// Initial state of the hasher to set before we hash a value. Modified only when we reset
    /// the sampler.
    /// This ensure that the value has a different hash value in each sampler.
    xor: GenericArray<u8, TDigest::BlockSize>,
    /// The value currently stored in the sampler.
    cur_value: Option<TValue>,
    /// Hash of `cur_value`.
    cur_value_hash: Option<GenericArray<u8, TDigest::OutputSize>>,
}

impl<TValue, TDigest> Sampler<TValue, TDigest>
where
    TDigest: Digest + BlockInput,
{
    /// Initializes a new `Sampler` with the given number of samplers.
    ///
    /// The higher the number of samplers, the more precise the distribution is, but the more CPU
    /// will be consumed when inserting. Between two inserts, the sampler can only ever produce
    /// `num_samplers` different values.
    pub fn with_len(num_samplers: u32) -> Self {
        let mut samplers: SmallVec<[_; 32]> = (0..num_samplers as usize)
            .map(|_| IndivSampler {
                // Note that `rand` doesn't support our array type, so we first put 0s everywhere,
                // then we fill the arrays with random values below.
                xor: Default::default(),
                cur_value: None,
                cur_value_hash: None,
            })
            .collect();

        for sampler in samplers.iter_mut() {
            for elem in sampler.xor.iter_mut() {
                *elem = rand::random();
            }
        }

        Sampler { samplers }
    }

    /// Inserts a value into the sampler. This is an expensive operation.
    pub fn insert(&mut self, value: TValue)
    where
        TValue: AsRef<[u8]> + Clone,
    {
        for sampler in self.samplers.iter_mut() {
            let mut new_value_hasher = TDigest::new();
            new_value_hasher.input(&sampler.xor[..]);
            new_value_hasher.input(&value);

            let new_value_hash = new_value_hasher.result();

            let replace = match sampler.cur_value_hash {
                Some(ref h) if new_value_hash < *h => true,
                None => true,
                _ => false,
            };

            if replace {
                sampler.cur_value = Some(value.clone());
                sampler.cur_value_hash = Some(new_value_hash);
            }
        }
    }

    /// Resets all samplers whose value is equal to `value`.
    ///
    /// This not only removes the value, but also reinitializes the samplers that had this value
    /// so that if you insert the same value again there is a high chance that it will not be
    /// inserted.
    pub fn invalidate(&mut self, value: &impl PartialEq<TValue>) {
        for sampler in self.samplers.iter_mut() {
            match sampler.cur_value {
                Some(ref v) if *value == *v => (),
                _ => continue,
            }

            sampler.cur_value = None;
            sampler.cur_value_hash = None;
            for elem in sampler.xor.iter_mut() {
                *elem = rand::random();
            }
        }
    }

    /// Gets a random element from the sampler. Returns `None` if the `Sampler` is empty
    /// (ie. `insert()` has never been called).
    pub fn sample(&self) -> Option<&TValue> {
        let mut iter = distributions::Uniform::new(0, self.samplers.len())
            .sample(&mut rand::thread_rng());
        let end_num = iter.wrapping_sub(1) % self.samplers.len();

        loop {
            if let Some(value) = self.samplers[iter].cur_value.as_ref() {
                return Some(value);
            }

            if iter == end_num {
                return None;
            }

            iter = iter.wrapping_add(1) % self.samplers.len();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Sampler;
    use sha2::Sha256;

    #[test]
    fn empty() {
        let sampler = Sampler::<_, Sha256>::with_len(32);
        // Try multiple times because randomness is involved.
        for _ in 0 .. 300 {
            assert_eq!(sampler.sample(), None::<&Vec<u8>>);
        }
    }

    #[test]
    fn one_value() {
        let mut sampler = Sampler::<_, Sha256>::with_len(32);
        sampler.insert(vec![0]);
        // Try multiple times because randomness is involved.
        for _ in 0..1000 {
            assert_eq!(sampler.sample().unwrap(), &[0]);
        }
    }

    #[test]
    fn uniform_with_two() {
        let mut sampler = Sampler::<_, Sha256>::with_len(256);
        sampler.insert(vec![0]);
        sampler.insert(vec![1]);

        let mut num_first = 0;
        for _ in 0..10000 {
            let val = sampler.sample().unwrap();
            assert!(val == &[0] || val == &[1]);
            if val == &[0] {
                num_first += 1;
            }
        }

        assert!(
            num_first > 4000 && num_first < 6000,
            "Failed range: {:?}",
            num_first
        );
    }

    #[test]
    fn invalidate_one() {
        let mut sampler = Sampler::<_, Sha256>::with_len(32);
        sampler.insert(vec![1]);
        sampler.insert(vec![0]);
        sampler.invalidate(&[1]);
        for _ in 0..1000 {
            assert_eq!(sampler.sample().unwrap(), &[0]);
        }
    }
}
