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

use open_metrics_client::encoding::text::Encode;
use open_metrics_client::metrics::counter::Counter;
use open_metrics_client::metrics::family::Family;
use open_metrics_client::metrics::histogram::{exponential_buckets, Histogram};
use open_metrics_client::registry::{Registry, Unit};

#[derive(Clone, Hash, PartialEq, Eq, Encode)]
struct FailureLabels {
    reason: Failure,
}

impl From<&libp2p_ping::PingFailure> for FailureLabels {
    fn from(failure: &libp2p_ping::PingFailure) -> Self {
        match failure {
            libp2p_ping::PingFailure::Timeout => FailureLabels {
                reason: Failure::Timeout,
            },
            libp2p_ping::PingFailure::Other { .. } => FailureLabels {
                reason: Failure::Other,
            },
        }
    }
}

#[derive(Clone, Hash, PartialEq, Eq, Encode)]
enum Failure {
    Timeout,
    Other,
}

pub struct Metrics {
    rtt: Histogram,
    failure: Family<FailureLabels, Counter>,
    pong_received: Counter,
}

impl Metrics {
    pub fn new(registry: &mut Registry) -> Self {
        let sub_registry = registry.sub_registry("ping");

        let rtt = Histogram::new(exponential_buckets(0.001, 2.0, 12));
        sub_registry.register_with_unit(
            "rtt",
            "Round-trip time sending a 'ping' and receiving a 'pong'",
            Unit::Seconds,
            Box::new(rtt.clone()),
        );

        let failure = Family::default();
        sub_registry.register(
            "failure",
            "Failure while sending a 'ping' or receiving a 'pong'",
            Box::new(failure.clone()),
        );

        let pong_received = Counter::default();
        sub_registry.register(
            "pong_received",
            "Number of 'pong's received",
            Box::new(pong_received.clone()),
        );

        Self {
            rtt,
            failure,
            pong_received,
        }
    }
}

impl super::Recorder<libp2p_ping::PingEvent> for super::Metrics {
    fn record(&self, event: &libp2p_ping::PingEvent) {
        match &event.result {
            Ok(libp2p_ping::PingSuccess::Pong) => {
                self.ping.pong_received.inc();
            }
            Ok(libp2p_ping::PingSuccess::Ping { rtt }) => {
                self.ping.rtt.observe(rtt.as_secs_f64());
            }
            Err(failure) => {
                self.ping.failure.get_or_create(&failure.into()).inc();
            }
        }
    }
}
