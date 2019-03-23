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

use crate::Addr;
use super::hci_socket::HciSocket;
use futures::{prelude::*, sync::oneshot};
use std::{io, thread, time::Duration};

// TODO: use dbus instead, and rename this struct

/// Request to the HCI for the list of nearby Bluetooth devices.
pub struct HciScan {
    inner: HciScanInner,
}

enum HciScanInner {
    Waiting(oneshot::Receiver<Result<Vec<Addr>, io::Error>>),
    Dispatching(Vec<Addr>),
}

impl HciScan {
    /// Initializes a new scan.
    pub fn new() -> Result<HciScan, io::Error> {
        let (tx, rx) = oneshot::channel();
        start_thread(tx);
        Ok(HciScan {
            inner: HciScanInner::Waiting(rx)
        })
    }

    /// Pulls the discovered devices, or `None` if we have finished enumerating.
    ///
    /// Each device will only appear once in the list.
    ///
    /// Just like `Stream::poll()`, must be executed within the context of a task. If `NotReady` is
    /// returned, the current task is registered then notified when something is ready.
    pub fn poll(&mut self) -> Poll<Option<Addr>, io::Error> {
        loop {
            let list = match self.inner {
                HciScanInner::Waiting(ref mut rx) => {
                    match rx.poll().expect("The background thread panicked") {
                        Async::Ready(list) => list?,
                        Async::NotReady => return Ok(Async::NotReady),
                    }
                },
                HciScanInner::Dispatching(ref mut list) => {
                    if list.is_empty() {
                        return Ok(Async::Ready(None));
                    } else {
                        return Ok(Async::Ready(Some(list.remove(0))));
                    }
                },
            };

            self.inner = HciScanInner::Dispatching(list);
        };
    }
}

fn start_thread(sender: oneshot::Sender<Result<Vec<Addr>, io::Error>>) {
    thread::spawn(move || {
        let _ = sender.send(query());
    });
}

fn query() -> Result<Vec<Addr>, io::Error> {
    let socket = HciSocket::new()?;
    socket.inquiry(Duration::from_secs(10))
}
