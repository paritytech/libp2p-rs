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

mod util;

use futures::{ready, future::poll_fn};
use libp2p_core::multiaddr::{multiaddr, Multiaddr};
use libp2p_core::{
    PeerId,
    connection::PendingConnectionError,
    network::{NetworkEvent, NetworkConfig, ConnectionLimits, DialError},
};
use rand::Rng;
use std::task::Poll;
use util::{TestHandler, test_network};

#[test]
fn max_outgoing() {
    let outgoing_limit = rand::thread_rng().gen_range(1, 10);

    let limits = ConnectionLimits::default().with_max_pending_outgoing(Some(outgoing_limit));
    let cfg = NetworkConfig::default().with_connection_limits(limits);
    let mut network = test_network(cfg);

    let target = PeerId::random();
    for _ in 0 .. outgoing_limit {
        network.peer(target.clone())
            .dial(Multiaddr::empty(), Vec::new(), TestHandler())
            .ok()
            .expect("Unexpected connection limit.");
    }

    match network.peer(target.clone())
        .dial(Multiaddr::empty(), Vec::new(), TestHandler())
        .expect_err("Unexpected dialing success.")
    {
        DialError::ConnectionLimit(err) => {
            assert_eq!(err.current, outgoing_limit);
            assert_eq!(err.limit, outgoing_limit);
        }
        e => panic!("Unexpected error: {:?}", e),
    }

    let info = network.info();
    assert_eq!(info.num_peers(), 0);
    assert_eq!(info.connection_counters().num_pending_outgoing(), outgoing_limit);

    // Abort all dialing attempts.
    let mut peer = network.peer(target.clone())
        .into_dialing()
        .expect("Unexpected peer state");

    let mut attempts = peer.attempts();
    while let Some(attempt) = attempts.next() {
        attempt.abort();
    }

    assert_eq!(network.info().connection_counters().num_pending_outgoing(), 0);
}

#[test]
fn max_established_incoming() {
    let limit = rand::thread_rng().gen_range(1, 10);

    fn config(limit: u32) -> NetworkConfig {
        let limits = ConnectionLimits::default().with_max_established_incoming(Some(limit));
        NetworkConfig::default().with_connection_limits(limits)
    }

    let mut network1 = test_network(config(limit));
    let mut network2 = test_network(config(limit));

    let listen_addr = multiaddr![Ip4(std::net::Ipv4Addr::new(127,0,0,1)), Tcp(0u16)];
    let _ = network1.listen_on(listen_addr.clone()).unwrap();
    let (addr_sender, addr_receiver) = futures::channel::oneshot::channel();
    let mut addr_sender = Some(addr_sender);

    // Spawn the listener.
    let listener = async_std::task::spawn(poll_fn(move |cx| {
        loop {
            match ready!(network1.poll(cx)) {
                NetworkEvent::NewListenerAddress { listen_addr, .. } => {
                    addr_sender.take().unwrap().send(listen_addr).unwrap();
                }
                NetworkEvent::IncomingConnection { connection, .. } => {
                    network1.accept(connection, TestHandler()).unwrap();
                }
                NetworkEvent::ConnectionEstablished { .. } => {}
                NetworkEvent::IncomingConnectionError {
                    error: PendingConnectionError::ConnectionLimit(err), ..
                } => {
                    assert_eq!(err.limit, limit);
                    assert_eq!(err.limit, err.current);
                    let info = network1.info();
                    let counters = info.connection_counters();
                    assert_eq!(counters.num_established_incoming(), limit);
                    assert_eq!(counters.num_established(), limit);
                    return Poll::Ready(())
                }
                e => panic!("Unexpected network event: {:?}", e)
            }
        }
    }));

    // Spawn and block on the dialer.
    async_std::task::block_on(async move {
        let addr = addr_receiver.await.unwrap();
        let mut n = 0;
        let _ = network2.dial(&addr, TestHandler()).unwrap();
        let mut expected_closed = None;
        poll_fn(|cx| {
            loop {
                match ready!(network2.poll(cx)) {
                    NetworkEvent::ConnectionEstablished { connection, .. } => {
                        n += 1;
                        if n <= limit {
                            // Dial again until the limit is exceeded.
                            let id = network2.dial(&addr, TestHandler()).unwrap();
                            if n == limit {
                                // The the next dialing attempt exceeds the limit, this
                                // is the connection we expected to get closed.
                                expected_closed = Some(id);
                            }
                        } else {
                            // This connection exceeds the limit for the listener and
                            // is expected to close shortly. For the dialer, these connections
                            // will first appear established before the listener closes them as
                            // a result of the limit violation.
                            assert_eq!(Some(connection.id()), expected_closed);
                        }
                    }
                    NetworkEvent::ConnectionClosed { id, .. } => {
                        assert_eq!(Some(id), expected_closed);
                        let info = network2.info();
                        let counters = info.connection_counters();
                        assert_eq!(counters.num_established_outgoing(), limit);
                        assert_eq!(counters.num_established(), limit);
                        return Poll::Ready(())
                    }
                    e => panic!("Unexpected network event: {:?}", e)
                }
            }
        }).await
    });

    // Wait for the listener to complete.
    async_std::task::block_on(listener);
}

