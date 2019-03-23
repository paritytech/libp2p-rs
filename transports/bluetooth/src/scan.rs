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

//! Combines scanning for nearby devices with filtering them depending on whether they support
//! libp2p.

use crate::{LIBP2P_UUID, LIBP2P_PEER_ID_ATTRIB, Addr, hci_scan::HciScan, sdp_client};
use futures::prelude::*;
use libp2p_core::{Multiaddr, PeerId, multiaddr::Protocol};
use std::{io, iter};

/// A scan in progress.
pub struct Scan {
    /// The stream for scanning devices. If `None`, it means that the scan is finished.
    devices: Option<HciScan>,
    /// Clients for querying whether devices support.
    sdp_scans: Vec<(Addr, sdp_client::SdpClient)>,
    /// Events that are going to be returned at the next polling.
    pending_results: Vec<(Multiaddr, PeerId)>,
}

impl Scan {
    /// Initializes a new scan.
    pub fn new() -> Result<Scan, io::Error> {
        Ok(Scan {
            devices: Some(HciScan::new()?),
            sdp_scans: Vec::new(),
            pending_results: Vec::new(),
        })
    }

    /// Pulls the discovered devices, or `None` if we have finished enumerating.
    ///
    /// Each device will only appear once in the list.
    ///
    /// Just like `Stream::poll()`, must be executed within the context of a task. If `NotReady` is
    /// returned, the current task is registered then notified when something is ready.
    pub fn poll(&mut self) -> Async<Option<(Multiaddr, PeerId)>> {
        if !self.pending_results.is_empty() {
            return Async::Ready(Some(self.pending_results.remove(0)));
        }

        loop {
            match self.devices.as_mut().map(|d| d.poll()) {
                Some(Ok(Async::Ready(Some(addr)))) => {
                    let mut client = match sdp_client::SdpClient::connect(addr) {
                        Ok(c) => c,
                        Err(_) => continue,
                    };

                    client.start_request(LIBP2P_UUID,
                        // We ask for the attribute containing the peer ID, and the protocols
                        // stack.
                        // TODO: malformed query if we use individual attributes; probably because we have to force u16 for values such as 0x4
                        iter::once(sdp_client::AttributeSearch::Range(0x0 ..= 0xffff))
                        //iter::once(sdp_client::AttributeSearch::Attribute(0x4))
                            //.chain(iter::once(sdp_client::AttributeSearch::Attribute(LIBP2P_PEER_ID_ATTRIB)))
                    );

                    self.sdp_scans.push((addr, client));
                },
                Some(Ok(Async::Ready(None))) => { self.devices = None; break },
                Some(Err(err)) => { println!("error during inquiry: {:?}", err); self.devices = None; break },
                None | Some(Ok(Async::NotReady)) => break,
            };
        }

        for n in (0..self.sdp_scans.len()).rev() {
            let (addr, mut in_progress) = self.sdp_scans.swap_remove(n);
            match in_progress.poll() {
                Err(err) => {
                    println!("error during scan: {:?}", err);
                },
                Ok(Async::Ready(sdp_client::SdpClientEvent::RequestFailed { .. })) => {
                    println!("error during scan");
                },
                Ok(Async::Ready(sdp_client::SdpClientEvent::RequestSuccess { results, .. })) => {
                    for record in results {
                        let peer_id = record.iter().find(|r| r.0 == LIBP2P_PEER_ID_ATTRIB)
                            .and_then(|r| if let sdp_client::Data::Str(s) = &r.1 { Some(s) } else { None })
                            .and_then(|base58| -> Option<PeerId> { base58.parse().ok() });
                        let addresses = record.iter().find(|r| r.0 == 0x4)
                            .and_then(|r| protocols_stack_to_multiaddr(addr, &r.1).ok());
                        if let (Some(addrs), Some(peer_id)) = (addresses, peer_id) {
                            for addr in addrs {
                                self.pending_results.push((addr, peer_id.clone()));
                            }
                        }
                    }
                    if !self.pending_results.is_empty() {
                        return Async::Ready(Some(self.pending_results.remove(0)));
                    }
                },
                Ok(Async::NotReady) => {
                    self.sdp_scans.push((addr, in_progress));
                }
            }
        }

        if self.sdp_scans.is_empty() && self.devices.is_none() {
            Async::Ready(None)
        } else {
            Async::NotReady
        }
    }
}

fn protocols_stack_to_multiaddr(base: Addr, stack: &sdp_client::Data) -> Result<Vec<Multiaddr>, ()> {
    if let sdp_client::Data::Alternative(list) = stack {
        let mut out = Vec::with_capacity(list.len());
        for elem in list {
            out.push(protocols_stack_to_multiaddr_inner(base, elem)?);
        }
        Ok(out)
    } else {
        protocols_stack_to_multiaddr_inner(base, stack).map(|m| vec![m])
    }
}

fn protocols_stack_to_multiaddr_inner(base: Addr, stack: &sdp_client::Data) -> Result<Multiaddr, ()> {
    let stack = match stack {
        sdp_client::Data::Sequence(s) => s,
        _ => return Err(())
    };

    let mut addr = Multiaddr::from(Protocol::Bluetooth(base.to_big_endian()));

    for protocol in stack {
        let list = match protocol {
            sdp_client::Data::Sequence(s) => s,
            _ => return Err(()),
        };

        match list[0] {
            sdp_client::Data::Uuid(uuid) if uuid == sdp_client::Uuid::Uuid16(0x0100) => {
                let port = 3;// TODO: if list[0] == ;
                addr.append(Protocol::L2cap(port));
            },
            sdp_client::Data::Uuid(uuid) if uuid == sdp_client::Uuid::Uuid16(0x0003) => {
                let port = match list.get(1) {
                    Some(sdp_client::Data::Uint(port)) => *port as u8,
                    _ => return Err(())
                };
                addr.append(Protocol::Rfcomm(port));
            },
            sdp_client::Data::Uuid(uuid) if uuid == sdp_client::Uuid::Uuid16(0x0004) => {
                let port = match list.get(1) {
                    Some(sdp_client::Data::Uint(port)) => *port as u16,
                    _ => return Err(())
                };
                addr.append(Protocol::Tcp(port));
            },
            sdp_client::Data::Uuid(uuid) if uuid == sdp_client::Uuid::Uuid16(0x0002) => {
                let port = match list.get(1) {
                    Some(sdp_client::Data::Uint(port)) => *port as u16,
                    _ => return Err(())
                };
                addr.append(Protocol::Udp(port));
            },
            _ => return Err(())
        }
    }

    Ok(addr)
}
