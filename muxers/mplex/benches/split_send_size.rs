// Copyright 2020 Parity Technologies (UK) Ltd.
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

//! A benchmark for the `split_send_size` configuration option
//! using different transports.

use async_std::task;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use futures::channel::oneshot;
use futures::prelude::*;
use futures::future::poll_fn;
use libp2p_core::{PeerId, Transport, StreamMuxer, identity, upgrade, transport, muxing, multiaddr::multiaddr, Multiaddr};
use libp2p_mplex as mplex;
use libp2p_plaintext::PlainText2Config;
use std::time::Duration;

type BenchTransport = transport::Boxed<(PeerId, muxing::StreamMuxerBox)>;

/// The sizes (in bytes) used for the `split_send_size` configuration.
const BENCH_SIZES: [usize; 8] = [
    256,
    512,
    1024,
    8 * 1024,
    16 * 1024,
    64 * 1024,
    256 * 1024,
    1024 * 1024,
];

fn prepare(c: &mut Criterion) {
    let _ = env_logger::try_init();

    let payload: Vec<u8> = vec![1; 1024 * 1024 * 1];

    let mut tcp = c.benchmark_group("tcp");
    let tcp_addr = multiaddr![Ip4(std::net::Ipv4Addr::new(127,0,0,1)), Tcp(0u16)];
    for &size in BENCH_SIZES.iter() {
        tcp.throughput(Throughput::Bytes(payload.len() as u64));
        let trans = tcp_transport(size);
        tcp.bench_function(
            format!("{}", size),
            |b| b.iter(|| run(black_box(&trans), black_box(&payload), black_box(&tcp_addr)))
        );
    }
    tcp.finish();

    let mut mem = c.benchmark_group("memory");
    let mem_addr = multiaddr![Memory(0u64)];
    for &size in BENCH_SIZES.iter() {
        mem.throughput(Throughput::Bytes(payload.len() as u64));
        let trans = mem_transport(size);
        mem.bench_function(
            format!("{}", size),
            |b| b.iter(|| run(black_box(&trans), black_box(&payload), black_box(&mem_addr)))
        );
    }
    mem.finish();
}


/// Transfers the given payload between two nodes using the given transport.
fn run(transport: &BenchTransport, payload: &Vec<u8>, listen_addr: &Multiaddr) {
    let mut listener = transport.clone().listen_on(listen_addr.clone()).unwrap();
    let (addr_sender, addr_receiver) = oneshot::channel();
    let mut addr_sender = Some(addr_sender);
    let payload_len = payload.len();

    // Spawn the receiver.
    let receiver = task::spawn(async move {
        loop {
            match listener.next().await.unwrap().unwrap() {
                transport::ListenerEvent::NewAddress(a) => {
                    addr_sender.take().unwrap().send(a).unwrap();
                }
                transport::ListenerEvent::Upgrade { upgrade, .. } => {
                    let (_peer, conn) = upgrade.await.unwrap();
                    match poll_fn(|cx| conn.poll_event(cx)).await {
                        Ok(muxing::StreamMuxerEvent::InboundSubstream(mut s)) => {
                            let mut buf = vec![0u8; payload_len];
                            let mut off = 0;
                            loop {
                                // Read in typical chunk sizes of up to 8KiB.
                                let end = off + std::cmp::min(buf.len() - off, 8 * 1024);
                                let n = poll_fn(|cx| {
                                    conn.read_substream(cx, &mut s, &mut buf[off..end])
                                }).await.unwrap();
                                off += n;
                                if off == buf.len() {
                                    return
                                }
                            }
                        }
                        Ok(_) => panic!("Unexpected muxer event"),
                        Err(e) => panic!("Unexpected error: {:?}", e)
                    }
                }
                _ => panic!("Unexpected listener event")
            }
        }
    });

    // Spawn and block on the sender, i.e. until all data is sent.
    task::block_on(async move {
        let addr = addr_receiver.await.unwrap();
        let (_peer, conn) = transport.clone().dial(addr).unwrap().await.unwrap();
        let mut handle = conn.open_outbound();
        let mut stream = poll_fn(|cx| conn.poll_outbound(cx, &mut handle)).await.unwrap();
        let mut off = 0;
        loop {
            let n = poll_fn(|cx| {
                conn.write_substream(cx, &mut stream, &payload[off..])
            }).await.unwrap();
            off += n;
            if off == payload.len() {
                poll_fn(|cx| conn.flush_substream(cx, &mut stream)).await.unwrap();
                return
            }
        }
    });

    // Wait for all data to be received.
    task::block_on(receiver);
}

fn tcp_transport(split_send_size: usize) -> BenchTransport {
    let key = identity::Keypair::generate_ed25519();
    let local_public_key = key.public();

    let mut mplex = mplex::MplexConfig::default();
    mplex.set_split_send_size(split_send_size);

    libp2p_tcp::TcpConfig::new().nodelay(true)
        .upgrade()
        .authenticate(PlainText2Config { local_public_key })
        .multiplex(mplex)
        .timeout(Duration::from_secs(5))
        .boxed()
}

fn mem_transport(split_send_size: usize) -> BenchTransport {
    let key = identity::Keypair::generate_ed25519();
    let local_public_key = key.public();

    let mut mplex = mplex::MplexConfig::default();
    mplex.set_split_send_size(split_send_size);

    transport::MemoryTransport::default()
        .upgrade()
        .authenticate(PlainText2Config { local_public_key })
        .multiplex(mplex)
        .timeout(Duration::from_secs(5))
        .boxed()
}

criterion_group!(split_send_size, prepare);
criterion_main!(split_send_size);
