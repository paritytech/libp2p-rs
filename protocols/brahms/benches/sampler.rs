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

use criterion::{Criterion, criterion_group, criterion_main};
use libp2p_brahms::sampler::Sampler;
use sha2::Sha256;

fn benchmarks(c: &mut Criterion) {
    c.bench_function_over_inputs(
        "sampler_insert",
        |b, &num_samplers| {
            let mut sampler = Sampler::<_, Sha256>::with_len(num_samplers);
            b.iter_with_setup(
                || rand::random::<[u8; 32]>().to_vec(),
                |val| sampler.insert(val)
            );
        },
        vec![8, 16, 32, 64, 128].into_iter()
    );
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
