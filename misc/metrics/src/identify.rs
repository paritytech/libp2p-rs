// Copyright 2021 Protocol Labs.
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

use open_metrics_client::metrics::counter::Counter;
use open_metrics_client::metrics::histogram::{exponential_buckets, Histogram};
use open_metrics_client::registry::Registry;
use std::iter;

pub struct Metrics {
    error: Counter,
    pushed: Counter,
    received: Counter,
    received_info_listen_addrs: Histogram,
    received_info_protocols: Histogram,
    sent: Counter,
}

impl Metrics {
    pub fn new(registry: &mut Registry) -> Self {
        let sub_registry = registry.sub_registry("identify");

        let error = Counter::default();
        sub_registry.register(
            "errors",
            "Number of errors while attempting to identify the remote",
            Box::new(error.clone()),
        );

        let pushed = Counter::default();
        sub_registry.register(
            "pushed",
            "Number of times identification information of the local node has \
             been actively pushed to a peer.",
            Box::new(pushed.clone()),
        );

        let received = Counter::default();
        sub_registry.register(
            "received",
            "Number of times identification information has been received from \
             a peer",
            Box::new(received.clone()),
        );

        let received_info_listen_addrs =
            Histogram::new(iter::once(0.0).chain(exponential_buckets(1.0, 2.0, 9)));
        sub_registry.register(
            "received_info_listen_addrs",
            "Number of listen addresses for remote peer received in \
             identification information",
            Box::new(received_info_listen_addrs.clone()),
        );

        let received_info_protocols =
            Histogram::new(iter::once(0.0).chain(exponential_buckets(1.0, 2.0, 9)));
        sub_registry.register(
            "received_info_protocols",
            "Number of protocols supported by the remote peer received in \
             identification information",
            Box::new(received_info_protocols.clone()),
        );

        let sent = Counter::default();
        sub_registry.register(
            "sent",
            "Number of times identification information of the local node has \
             been sent to a peer in response to an identification request",
            Box::new(sent.clone()),
        );

        Self {
            error,
            pushed,
            received,
            received_info_listen_addrs,
            received_info_protocols,
            sent,
        }
    }
}

impl super::Recorder<libp2p_identify::IdentifyEvent> for super::Metrics {
    fn record(&self, event: &libp2p_identify::IdentifyEvent) {
        match event {
            libp2p_identify::IdentifyEvent::Error { .. } => {
                self.identify.error.inc();
            }
            libp2p_identify::IdentifyEvent::Pushed { .. } => {
                self.identify.pushed.inc();
            }
            libp2p_identify::IdentifyEvent::Received { info, .. } => {
                self.identify.received.inc();
                self.identify
                    .received_info_protocols
                    .observe(info.protocols.len() as f64);
                self.identify
                    .received_info_listen_addrs
                    .observe(info.listen_addrs.len() as f64);
            }
            libp2p_identify::IdentifyEvent::Sent { .. } => {
                self.identify.sent.inc();
            }
        }
    }
}
