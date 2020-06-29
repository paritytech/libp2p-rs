// Copyright 2017-2020 Parity Technologies (UK) Ltd.
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

use futures::prelude::*;
use libp2p_core::{
    multiaddr::{Multiaddr, Protocol},
    muxing::StreamMuxer,
    transport::ListenerEvent,
    transport::Transport,
};
use libp2p_quic::{Config, Endpoint, QuicMuxer, QuicTransport};

use std::{
    io::Result,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tracing::{debug, error, info, trace};

#[derive(Debug)]
struct QuicStream<'a> {
    id: Option<quinn_proto::StreamId>,
    muxer: &'a QuicMuxer,
    shutdown: bool,
}

impl<'a> AsyncWrite for QuicStream<'a> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        assert!(!self.shutdown, "written after close");
        let Self { muxer, id, .. } = self.get_mut();
        let _ = muxer.poll_inbound(cx);
        muxer
            .write_substream(cx, id.as_mut().unwrap(), buf)
            .map_err(From::from)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.shutdown = true;
        let Self { muxer, id, .. } = self.get_mut();
        let _ = muxer.poll_inbound(cx);
        debug!("trying to close {:?}", id);
        match muxer.shutdown_substream(cx, id.as_mut().unwrap()) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(e) => e?,
        };
        debug!("closed {:?}", id);
        Poll::Ready(Ok(()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl<'a> AsyncRead for QuicStream<'a> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        let Self { id, muxer, .. } = self.get_mut();
        let _ = muxer.poll_inbound(cx);
        muxer
            .read_substream(cx, id.as_mut().unwrap(), buf)
            .map_err(From::from)
    }
}

impl<'a> Drop for QuicStream<'a> {
    fn drop(&mut self) {
        match self.id.take() {
            None => {}
            Some(id) => self.muxer.destroy_substream(id),
        }
    }
}

struct Outbound<'a>(&'a QuicMuxer);

impl<'a> futures::Future for Outbound<'a> {
    type Output = Result<QuicStream<'a>>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _ = self.0.poll_inbound(cx);
        let Outbound(conn) = &mut *self;
        conn.poll_outbound(cx, &mut ())
            .map_ok(|id| QuicStream {
                id: Some(id),
                muxer: self.get_mut().0.clone(),
                shutdown: false,
            })
            .map_err(From::from)
    }
}

#[derive(Debug)]
struct Inbound<'a>(&'a QuicMuxer);
impl<'a> futures::Stream for Inbound<'a> {
    type Item = QuicStream<'a>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        debug!("polling for inbound connections");
        self.0.poll_inbound(cx).map(|id| {
            Some(QuicStream {
                id: Some(id.expect("bug")),
                muxer: self.get_mut().0.clone(),
                shutdown: false,
            })
        })
    }
}

fn init() {
    use tracing_subscriber::{fmt::Subscriber, EnvFilter};
    let _ = Subscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();
}

struct Closer(Arc<QuicMuxer>);

impl Future for Closer {
    type Output = Result<()>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let i = &mut self.get_mut().0;
        let _ = i.poll_inbound(cx);
        i.close(cx).map_err(From::from)
    }
}

#[cfg(any())]
#[test]
fn wildcard_expansion() {
    init();
    let addr: Multiaddr = "/ip4/0.0.0.0/udp/1234/quic".parse().unwrap();
    let keypair = libp2p_core::identity::Keypair::generate_ed25519();
    let listener =
        QuicTransport(Endpoint::new(Config::new(&keypair, addr.clone()).unwrap()).unwrap());
    let mut incoming = listener.listen_on(addr).unwrap();
    // Process all initial `NewAddress` events and make sure they
    // do not contain wildcard address or port.
    futures::executor::block_on(async move {
        while let Some(event) = incoming.next().await.map(|e| e.unwrap()) {
            match event {
                ListenerEvent::NewAddress(a) => {
                    let mut iter = a.iter();
                    match iter.next().expect("ip address") {
                        Protocol::Ip4(ip) => assert!(!ip.is_unspecified()),
                        Protocol::Ip6(ip) => assert!(!ip.is_unspecified()),
                        other => panic!("Unexpected protocol: {}", other),
                    }
                    if let Protocol::Udp(port) = iter.next().expect("port") {
                        assert_ne!(0, port)
                    } else {
                        panic!("No UDP port in address: {}", a)
                    }
                    assert_eq!(iter.next(), Some(Protocol::Quic));
                    assert_eq!(iter.next(), None);
                }
                ListenerEvent::Upgrade { .. } => panic!(),
                ListenerEvent::AddressExpired { .. } => panic!(),
                ListenerEvent::Error { .. } => panic!(),
            }
            break;
        }
        drop(incoming);
    });
}

#[test]
fn communicating_between_dialer_and_listener() {
    init();
    for i in 0..1000u32 {
        trace!("running a test");
        do_test(i)
    }
}

fn do_test(_i: u32) {
    use std::error::Error as _;
    let (ready_tx, ready_rx) = futures::channel::oneshot::channel();
    let mut ready_tx = Some(ready_tx);
    let keypair = libp2p_core::identity::Keypair::generate_ed25519();
    let keypair2 = keypair.clone();
    let addr: Multiaddr = "/ip4/127.0.0.1/udp/0/quic".parse().expect("bad address?");
    let quic_config = Config::new(&keypair2, addr.clone()).unwrap();
    let quic_endpoint = Endpoint::new(quic_config).unwrap();
    let mut listener = QuicTransport(quic_endpoint.clone())
        .listen_on(addr)
        .unwrap();
    error!("running tests");
    let handle = async_std::task::spawn(async move {
        let key = loop {
            trace!("awaiting connection");
            match listener.next().await.unwrap().unwrap() {
                ListenerEvent::NewAddress(listen_addr) => {
                    if let Some(channel) = ready_tx.take() {
                        channel.send(listen_addr).unwrap();
                    }
                }
                ListenerEvent::Upgrade { upgrade, .. } => {
                    info!("got a connection upgrade!");
                    let (id, mut muxer): (_, QuicMuxer) = upgrade.await.expect("upgrade failed");
                    info!("got a new muxer!");
                    let muxer = Arc::new(muxer);
                    let mut socket: QuicStream =
                        Inbound(&*muxer).next().await.expect("no incoming stream");
                    {
                        let cloned = muxer.clone();
                        async_std::task::spawn(future::poll_fn(move |cx| cloned.poll_inbound(cx)));
                    }
                    let mut buf = [0u8; 3];
                    debug!("reading data from accepted stream!");
                    {
                        let mut count = 0;
                        while count < buf.len() {
                            count += socket.read(&mut buf[count..]).await.unwrap();
                        }
                    }
                    assert_eq!(buf, [4, 5, 6]);
                    debug!("writing data!");
                    socket.write_all(&[0x1, 0x2, 0x3]).await.unwrap();
                    debug!("data written!");
                    socket.close().await.unwrap();
                    // debug!("socket closed!");
                    assert_eq!(socket.read(&mut buf).await.unwrap(), 0);
                    debug!("end of stream");
                    drop(socket);
                    // Closer(muxer).await.unwrap();
                    debug!("finished!");
                    break id;
                }
                _ => unreachable!(),
            }
        };
        drop(listener);
        drop(quic_endpoint);
        key
    });

    let second_handle = async_std::task::spawn(async move {
        let addr = ready_rx.await.unwrap();
        let quic_config =
            Config::new(&keypair, "/ip4/127.0.0.1/udp/0/quic".parse().unwrap()).unwrap();
        let quic_endpoint = QuicTransport(Endpoint::new(quic_config).unwrap());
        // Obtain a future socket through dialing
        error!("Dialing a Connection: {:?}", addr);
        let (peer_id, mut connection) = quic_endpoint.dial(addr.clone()).unwrap().await.unwrap();
        let connection = Arc::new(connection);
        {
            let cloned = connection.clone();
            async_std::task::spawn(future::poll_fn(move |cx| cloned.poll_inbound(cx)));
        }
        trace!("Received a Connection: {:?}", connection);
        let () = connection.open_outbound();
        let mut stream = Outbound(&*connection).await.expect("failed");

        debug!("opened a stream: id {:?}", stream.id);
        // let result = stream.read(&mut [][..]).await;
        // let result = result.expect_err("reading from an unwritten stream cannot succeed");
        // assert_eq!(result.kind(), std::io::ErrorKind::NotConnected);
        // assert!(result.source().is_none());
        // let wrapped = result.get_ref().unwrap().downcast_ref().unwrap();
        // match wrapped {
        //     libp2p_quic::Error::CannotReadFromUnwrittenStream => {}
        //     e => panic!("Wrong error from reading unwritten stream: {}", e),
        // }
        stream.write_all(&[4u8, 5, 6]).await.unwrap();
        stream.close().await.unwrap();
        let mut buf = [0u8; 3];
        debug!("reading data!");
        {
            let mut count = 0;
            while count < buf.len() {
                let read = stream.read(&mut buf[count..]).await.unwrap();
                assert_ne!(read, 0usize, "premature end of file");
                count += read;
            }
        }
        assert_eq!(buf, [1u8, 2, 3]);
        debug!("data read ― checking for EOF");
        assert_eq!(stream.read(&mut buf).await.unwrap(), 0);
        drop(stream);
        debug!("have EOF");
        // Closer(connection).await.expect("closed successfully");
        debug!("awaiting handle");
        peer_id
    });
    assert_eq!(
        async_std::task::block_on(handle),
        async_std::task::block_on(second_handle)
    );
}

#[test]
#[cfg(any)]
fn replace_port_0_in_returned_multiaddr_ipv4() {
    init();
    let keypair = libp2p_core::identity::Keypair::generate_ed25519();

    let addr = "/ip4/127.0.0.1/udp/0/quic".parse::<Multiaddr>().unwrap();
    assert!(addr.to_string().ends_with("udp/0/quic"));

    let config = Config::new(&keypair, addr.clone()).unwrap();
    let quic = QuicTransport(Endpoint::new(config).expect("no error"));

    let new_addr = futures::executor::block_on_stream(quic.listen_on(addr).unwrap())
        .next()
        .expect("some event")
        .expect("no error")
        .into_new_address()
        .expect("listen address");

    if new_addr.to_string().contains("udp/0") {
        panic!("failed to expand address ― got {}", new_addr);
    }
}

#[test]
#[cfg(any)]
fn replace_port_0_in_returned_multiaddr_ipv6() {
    init();
    let keypair = libp2p_core::identity::Keypair::generate_ed25519();

    let addr: Multiaddr = "/ip6/::1/udp/0/quic".parse().unwrap();
    assert!(addr.to_string().contains("udp/0/quic"));
    let config = Config::new(&keypair, addr.clone()).unwrap();
    let quic = QuicTransport(Endpoint::new(config).expect("no error"));

    let new_addr = futures::executor::block_on_stream(quic.listen_on(addr).unwrap())
        .next()
        .expect("some event")
        .expect("no error")
        .into_new_address()
        .expect("listen address");

    assert!(!new_addr.to_string().contains("udp/0"));
}

#[test]
fn larger_addr_denied() {
    init();
    let keypair = libp2p_core::identity::Keypair::generate_ed25519();
    let addr = "/ip4/127.0.0.1/tcp/12345/tcp/12345"
        .parse::<Multiaddr>()
        .unwrap();
    let config = Config::new(&keypair, addr).unwrap();
    assert!(Endpoint::new(config).is_err())
}
