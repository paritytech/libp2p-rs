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

//! Implementation of the libp2p `Transport` trait for Bluetooth.

use futures::{future, prelude::*, try_ready};
use libp2p_core::{Multiaddr, multiaddr::Protocol, PeerId, Transport, transport::TransportError};
use std::{io, iter};

mod addr;
mod discoverable;
mod ffi;
mod hci_scan;
mod hci_socket;
mod l2cap;
mod profile_register;
mod rfcomm_socket;
mod rfcomm;
pub mod scan;           // TODO: shouldn't be pub
mod scan_behaviour;       // TODO:
mod sdp_client;

pub use self::addr::{Addr, ANY, ALL, LOCAL};
pub use self::scan_behaviour::{BluetoothDiscovery, BluetoothEvent};

/// UUID for the advertised service class. We advertise this UUID locally, and search for devices
/// around that advertise this UUID.
const LIBP2P_UUID: sdp_client::Uuid = sdp_client::Uuid::Uuid128(0xd8263f85_cdca_4ac8_8fee_81c64221d6d5);
/// ID of the attribute of the service that contains the `PeerId` in base58. The type of the
/// attribute's value must be "text".
const LIBP2P_PEER_ID_ATTRIB: u16 = 0x3000;

/// Represents the configuration for a Bluetooth transport capability for libp2p.
#[derive(Debug, Clone)]
pub struct BluetoothConfig {
    /// If `Some`, we will register the service with the given `PeerId`.
    register_sdp: Option<PeerId>,
}

impl BluetoothConfig {
    /// Builds a new `BluetoothConfig`.
    ///
    /// The `PeerId` is used to advertise our service through the SDP server so that other devices
    /// can discover it.
    pub fn new(peer_id: PeerId) -> BluetoothConfig {
        BluetoothConfig {
            register_sdp: Some(peer_id),
        }
    }

    /// Builds a new `BluetoothConfig` that allows listening for incoming connections, but doesn't
    /// advertise the service to other devices.
    pub fn without_registration() -> BluetoothConfig {
        BluetoothConfig {
            register_sdp: None,
        }
    }
}

impl Transport for BluetoothConfig {
    type Output = rfcomm::RfcommStream;
    type Error = io::Error;
    type Listener = RfcommListener;
    type ListenerUpgrade = future::FutureResult<Self::Output, Self::Error>;
    type Dial = rfcomm::RfcommStreamFuture;

    fn listen_on(self, addr: Multiaddr) -> Result<(Self::Listener, Multiaddr), TransportError<Self::Error>> {
        let (mac, port) = multiaddr_to_rfcomm(addr)?;
        discoverable::enable_discoverable(&mac).unwrap();      // TODO:
        let (listener,  actual_port) = rfcomm::RfcommListener::bind(mac, port).map_err(TransportError::Other)?;

        let actual_addr = iter::once(Protocol::Bluetooth(mac.to_big_endian()))
            .chain(iter::once(Protocol::L2cap(3)))
            .chain(iter::once(Protocol::Rfcomm(actual_port)))
            .collect();

        let sdp_registration = if let Some(peer_id) = self.register_sdp {
            profile_register::register_libp2p_profile(&peer_id, actual_port)
                .map_err(|err| { println!("registration error: {:?}", err); err }).ok()
        } else {
            None
        };

        let listener = RfcommListener {
            inner: listener,
            _sdp_registration: sdp_registration,
        };

        Ok((listener, actual_addr))
    }

    fn dial(self, addr: Multiaddr) -> Result<Self::Dial, TransportError<Self::Error>> {
        let (mac, port) = multiaddr_to_rfcomm(addr)?;
        Ok(rfcomm::RfcommStream::connect(mac, port))
    }

    fn nat_traversal(&self, _server: &Multiaddr, _observed: &Multiaddr) -> Option<Multiaddr> {
        // TODO: ?
        None
    }
}

pub struct RfcommListener {
    /// The inner listener.
    inner: rfcomm::RfcommListener,

    /// RAII object for the service advertisement. If destroyed, the service stops being
    /// advertised.
    _sdp_registration: Option<profile_register::Registration>,
}

impl Stream for RfcommListener {
    type Item = (future::FutureResult<rfcomm::RfcommStream, io::Error>, Multiaddr);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let socket = try_ready!(self.inner.poll());
        Ok(Async::Ready(socket.map(|(stream, addr)| {
            let addr = iter::once(Protocol::Bluetooth(addr.to_big_endian()))
                .chain(iter::once(Protocol::L2cap(3)))
                .chain(iter::once(Protocol::Rfcomm(0)))
                .collect();
            (future::ok(stream), addr)
        })))
    }
}

// This type of logic should probably be moved into the multiaddr package
fn multiaddr_to_rfcomm<T>(addr: Multiaddr) -> Result<(Addr, u8), TransportError<T>> {
    // TODO: TransportError::MultiaddrNotSupported(addr)
    let mut iter = addr.iter();
    let proto1 = match iter.next() {
        Some(p) => p,
        None => return Err(TransportError::MultiaddrNotSupported(addr)),
    };

    let proto2 = match iter.next() {
        Some(p) => p,
        None => return Err(TransportError::MultiaddrNotSupported(addr)),
    };

    let proto3 = match iter.next() {
        Some(p) => p,
        None => return Err(TransportError::MultiaddrNotSupported(addr)),
    };

    if iter.next().is_some() {
        return Err(TransportError::MultiaddrNotSupported(addr));
    }

    match (proto1, proto2, proto3) {
        (Protocol::Bluetooth(mac), Protocol::L2cap(3), Protocol::Rfcomm(port)) => {
            if port > 30 {
                return Err(TransportError::MultiaddrNotSupported(addr));
            }

            Ok((Addr::from_big_endian(mac), port))
        },
        _ => Err(TransportError::MultiaddrNotSupported(addr)),
    }
}

#[cfg(test)]
mod tests {
    use crate::BluetoothConfig;
    use futures::prelude::*;
    use libp2p_core::{PeerId, Transport};

    #[test]
    fn connect_to_self() {
        let config = BluetoothConfig::default(PeerId::random());

        // TODO: correct addresses; also, doesn't work
        let listener = config.clone().listen_on("/bluetooth/00:00:00:00:00:00/l2cap/3/rfcomm/5".parse().unwrap()).unwrap().0;
        let dialer = config.dial("/bluetooth/FF:FF:FF:00:00:00/l2cap/3/rfcomm/5".parse().unwrap()).unwrap();

        let listener = listener.into_future().map_err(|(err, _)| err).map(|(inc, _)| inc.unwrap().0).map(|_| ());
        let dialer = dialer.map(|_| ());

        let combined = listener.select(dialer).map_err(|_| panic!()).and_then(|(_, next)| next).map(|_| ());
        tokio::runtime::Runtime::new().unwrap().block_on(combined).unwrap();
    }
}
