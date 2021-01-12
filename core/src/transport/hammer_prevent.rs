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

use crate::{Multiaddr, Transport, transport::TransportError};

use fnv::FnvHashMap;
use futures::prelude::*;
use futures_timer::Delay;
use parking_lot::Mutex;
use pin_project::{pin_project, project};
use std::{collections::hash_map::Entry, cmp, pin::Pin, sync::Arc, task::{Context, Poll}, time::Duration};

/// First value for the back-off.
const FIRST_BACKOFF: Duration = Duration::from_secs(2);
/// Maximum allowed backoff.
const MAX_BACKOFF: Duration = Duration::from_secs(300);

/// Wraps around a `Transport`. Remembers addresses that couldn't be dialed and avoids dialing them
/// again.
#[derive(Clone)]
pub struct HammerPrevention<TInner> {
    /// The underlying transport.
    inner: TInner,

    /// For each address, when we can dial it again.
    /// Every address in this map has been dialed in the past with the underlying transport
    /// supporting it.
    addresses: Arc<Mutex<FnvHashMap<Multiaddr, AddrInfo>>>,
}

#[derive(Debug)]
struct AddrInfo {
    /// If we try to dial this address, how much delay we should add.
    next_dial_backoff: Duration,
    /// Future that resolves when we can remove this address from the list.
    expiration: Delay,
}

impl<TInner> HammerPrevention<TInner> {
    /// Creates a new `HammerPrevention` around the transport.
    pub fn new(inner: TInner) -> Self {
        HammerPrevention {
            inner,
            addresses: Arc::new(Mutex::new(Default::default())),
        }
    }
}

impl<TInner> Transport for HammerPrevention<TInner>
where
    TInner: Transport,
    TInner::Error: 'static,
{
    type Output = TInner::Output;
    type Error = TInner::Error;
    type Listener = TInner::Listener;
    type ListenerUpgrade = TInner::ListenerUpgrade;
    type Dial = HammerPreventionDial<TInner>;

    fn listen_on(self, addr: Multiaddr) -> Result<Self::Listener, TransportError<Self::Error>> {
        self.inner.listen_on(addr)
    }

    fn dial(self, addr: Multiaddr) -> Result<Self::Dial, TransportError<Self::Error>> {
        let mut addresses = self.addresses.lock();

        // Purge expired addresses.
        addresses.retain(move |_, a| (&mut a.expiration).now_or_never().is_none());

        let inner = match addresses.entry(addr.clone()) {
            Entry::Vacant(entry) => {
                // We don't know anything about that address. Start dialing immediately.
                // We filter out `MultiaddrNotSupported` so that we don't insert an entry about
                // the address if its address is not supported.
                let future = match self.inner.dial(addr.clone()) {
                    Ok(f) => Ok(f),
                    Err(TransportError::MultiaddrNotSupported(err))
                        => return Err(TransportError::MultiaddrNotSupported(err)),
                    Err(err) => Err(err),
                };

                entry.insert(AddrInfo {
                    next_dial_backoff: FIRST_BACKOFF,
                    expiration: Delay::new(FIRST_BACKOFF * 2),
                });

                // Dial immediately.
                HammerPreventionDialInner::Dialing(future?)
            }

            Entry::Occupied(entry) => {
                // Adding a waiting delay before the dial.
                let addr_info = entry.into_mut();
                let timer = Delay::new(addr_info.next_dial_backoff);
                addr_info.next_dial_backoff = cmp::min(addr_info.next_dial_backoff * 2, MAX_BACKOFF);
                addr_info.expiration = Delay::new(addr_info.next_dial_backoff * 2);
                HammerPreventionDialInner::Waiting(timer, Some(self.inner))
            }
        };

        Ok(HammerPreventionDial {
            inner,
            local_addr: addr,
            addresses: self.addresses.clone(),
        })
    }
}

/// Wraps around a `Future` that produces a connection. If necessary, waits for a little bit
/// before dialing.
#[pin_project]
pub struct HammerPreventionDial<TInner>
where
    TInner: Transport
{
    /// Inner state of the future.
    #[pin]
    inner: HammerPreventionDialInner<TInner>,
    /// Address we're trying to dial.
    local_addr: Multiaddr,
    /// The shared state containing the back-off for the addresses.
    addresses: Arc<Mutex<FnvHashMap<Multiaddr, AddrInfo>>>,
}

#[pin_project]
enum HammerPreventionDialInner<TInner>
where
    TInner: Transport
{
    /// We are during the dialing delay.
    Waiting(#[pin] Delay, Option<TInner>),
    /// We are in the process of dialing.
    Dialing(#[pin] TInner::Dial),
}

impl<TInner> Future for HammerPreventionDial<TInner>
where
    TInner: Transport,
{
    type Output = Result<TInner::Output, TInner::Error>;

    #[project]
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            #[project]
            match this.inner.as_mut().project() {
                HammerPreventionDialInner::Waiting(delay, transport) => {
                    match delay.poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(()) => {
                            let transport = transport.take()
                                .expect("future called after being finished");
                            match transport.dial(this.local_addr.clone()) {
                                Ok(f) => this.inner.set(HammerPreventionDialInner::Dialing(f)),
                                Err(TransportError::MultiaddrNotSupported(_)) => panic!(), // TODO:
                                Err(TransportError::Other(err)) => return Poll::Ready(Err(err)),
                            }
                        },
                    }
                }

                HammerPreventionDialInner::Dialing(dial) => {
                    match dial.poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Ok(output)) => {
                            // On success, remove the address from the list.
                            this.addresses.lock().remove(&this.local_addr);
                            return Poll::Ready(Ok(output))
                        },
                        Poll::Ready(Err(err)) => {
                            return Poll::Ready(Err(err))
                        },
                    }
                }
            }
        }
    }
}
