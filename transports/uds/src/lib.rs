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

//! Implementation of the libp2p `Transport` trait for Unix domain sockets.
//!
//! # Platform support
//!
//! This transport only works on Unix platforms.
//!
//! # Usage
//!
//! The `UdsConfig` transport supports multiaddresses of the form `/unix//tmp/foo`.
//!
//! The `UdsConfig` structs implements the `Transport` trait of the `core` library. See the
//! documentation of `core` and of libp2p in general to learn how to use the `Transport` trait.

#![cfg(all(unix, not(target_os = "emscripten")))]
#![cfg_attr(docsrs, doc(cfg(all(unix, not(target_os = "emscripten")))))]

use futures::{prelude::*, future::{BoxFuture, Ready}};
use futures::stream::BoxStream;
use libp2p_core::{
    Transport,
    multiaddr::{Protocol, Multiaddr},
    transport::{ListenerEvent, TransportError}
};
use log::debug;
use std::{io, path::PathBuf};

macro_rules! codegen {
    ($feature_name:expr, $uds_config:ident, $build_listener:expr, $unix_stream:ty, $($mut_or_not:tt)*) => {

/// Represents the configuration for a Unix domain sockets transport capability for libp2p.
#[cfg_attr(docsrs, doc(cfg(feature = $feature_name)))]
#[derive(Debug, Clone)]
pub struct $uds_config {
}

impl $uds_config {
    /// Creates a new configuration object for Unix domain sockets.
    pub fn new() -> $uds_config {
        $uds_config {}
    }
}

impl Transport for $uds_config {
    type Output = $unix_stream;
    type Error = io::Error;
    type Listener = BoxStream<'static, Result<ListenerEvent<Self::ListenerUpgrade, Self::Error>, Self::Error>>;
    type ListenerUpgrade = Ready<Result<Self::Output, Self::Error>>;
    type Dial = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn listen_on(self, addr: Multiaddr) -> Result<Self::Listener, TransportError<Self::Error>> {
        if let Ok(path) = multiaddr_to_path(&addr) {
            Ok(async move { $build_listener(&path).await }
                .map_ok(move |listener| {
                    stream::once({
                        let addr = addr.clone();
                        async move {
                            debug!("Now listening on {}", addr);
                            Ok(ListenerEvent::NewAddress(addr))
                        }
                    }).chain(stream::unfold(listener, move |$($mut_or_not)* listener| {
                        let addr = addr.clone();
                        async move {
                            let (stream, _) = match listener.accept().await {
                                Ok(v) => v,
                                Err(err) => return Some((Err(err), listener))
                            };
                            debug!("incoming connection on {}", addr);
                            let event = ListenerEvent::Upgrade {
                                upgrade: future::ok(stream),
                                local_addr: addr.clone(),
                                remote_addr: addr.clone()
                            };
                            Some((Ok(event), listener))
                        }
                    }))
                })
                .try_flatten_stream()
                .boxed())
        } else {
            Err(TransportError::MultiaddrNotSupported(addr))
        }
    }

    fn dial(self, addr: Multiaddr) -> Result<Self::Dial, TransportError<Self::Error>> {
        if let Ok(path) = multiaddr_to_path(&addr) {
            debug!("Dialing {}", addr);
            Ok(async move { <$unix_stream>::connect(&path).await }.boxed())
        } else {
            Err(TransportError::MultiaddrNotSupported(addr))
        }
    }

    fn address_translation(&self, _server: &Multiaddr, _observed: &Multiaddr) -> Option<Multiaddr> {
        None
    }
}

};
}

#[cfg(feature = "async-std")]
codegen!(
    "async-std",
    UdsConfig,
    |addr| async move { async_std::os::unix::net::UnixListener::bind(addr).await },
    async_std::os::unix::net::UnixStream,
);
#[cfg(feature = "tokio")]
codegen!(
    "tokio",
    TokioUdsConfig,
    |addr| async move { tokio::net::UnixListener::bind(addr) },
    tokio::net::UnixStream,
    mut
);

/// Turns a `Multiaddr` containing a single `Unix` component into a path.
///
/// Also returns an error if the path is not absolute, as we don't want to dial/listen on relative
/// paths.
// This type of logic should probably be moved into the multiaddr package
fn multiaddr_to_path(addr: &Multiaddr) -> Result<PathBuf, ()> {
    let mut protocols = addr.iter();
    match protocols.next() {
        Some(Protocol::Unix(ref path)) => {
            let path = PathBuf::from(path.as_ref());
            if !path.is_absolute() {
                return Err(())
            }
            match protocols.next() {
                None | Some(Protocol::P2p(_)) => Ok(path),
                Some(_) => Err(())
            }
        }
        _ => Err(())
    }
}

#[cfg(all(test, feature = "async-std"))]
mod tests {
    use super::{multiaddr_to_path, UdsConfig};
    use futures::{channel::oneshot, prelude::*};
    use std::{self, borrow::Cow, path::Path};
    use libp2p_core::{Transport, multiaddr::{Protocol, Multiaddr}};
    use tempfile;

    #[test]
    fn multiaddr_to_path_conversion() {
        assert!(
            multiaddr_to_path(&"/ip4/127.0.0.1/udp/1234".parse::<Multiaddr>().unwrap())
                .is_err()
        );

        assert_eq!(
            multiaddr_to_path(&Multiaddr::from(Protocol::Unix("/tmp/foo".into()))),
            Ok(Path::new("/tmp/foo").to_owned())
        );
        assert_eq!(
            multiaddr_to_path(&Multiaddr::from(Protocol::Unix("/home/bar/baz".into()))),
            Ok(Path::new("/home/bar/baz").to_owned())
        );
    }

    #[test]
    fn communicating_between_dialer_and_listener() {
        let temp_dir = tempfile::tempdir().unwrap();
        let socket = temp_dir.path().join("socket");
        let addr = Multiaddr::from(Protocol::Unix(Cow::Owned(socket.to_string_lossy().into_owned())));

        let (tx, rx) = oneshot::channel();

        async_std::task::spawn(async move {
            let mut listener = UdsConfig::new().listen_on(addr).unwrap();

            let listen_addr = listener.try_next().await.unwrap()
                .expect("some event")
                .into_new_address()
                .expect("listen address");

            tx.send(listen_addr).unwrap();

            let (sock, _addr) = listener.try_filter_map(|e| future::ok(e.into_upgrade()))
                .try_next()
                .await
                .unwrap()
                .expect("some event");

            let mut sock = sock.await.unwrap();
            let mut buf = [0u8; 3];
            sock.read_exact(&mut buf).await.unwrap();
            assert_eq!(buf, [1, 2, 3]);
        });

        async_std::task::block_on(async move {
            let uds = UdsConfig::new();
            let addr = rx.await.unwrap();
            let mut socket = uds.dial(addr).unwrap().await.unwrap();
            socket.write(&[1, 2, 3]).await.unwrap();
        });
    }

    #[test]
    #[ignore]       // TODO: for the moment unix addresses fail to parse
    fn larger_addr_denied() {
        let uds = UdsConfig::new();

        let addr = "/unix//foo/bar"
            .parse::<Multiaddr>()
            .unwrap();
        assert!(uds.listen_on(addr).is_err());
    }

    #[test]
    #[ignore]       // TODO: for the moment unix addresses fail to parse
    fn relative_addr_denied() {
        assert!("/unix/./foo/bar".parse::<Multiaddr>().is_err());
    }
}
