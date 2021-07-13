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

//! Example demonstrating `libp2p-metrics`.
//!
//! In one terminal run:
//!
//! ```
//! cargo run --example metrics
//! ```
//!
//! In a second terminal run:
//!
//! ```
//! cargo run --example metrics -- <listen-addr-of-first-node>
//! ```
//!
//! Where `<listen-addr-of-first-node>` is replaced by the listen address of the
//! first node reported in the first terminal. Look for `NewListenAddr`.
//!
//! In a third terminal run:
//!
//! ```
//! curl localhost:<metrics-port-of-first-or-second-node>/metrics
//! ```
//!
//! Where `<metrics-port-of-first-or-second-node>` is replaced by the listen
//! port of the metrics server of the first or the second node. Look for
//! `tide::server Server listening on`.
//!
//! You should see a long list of metrics printed to the terminal. Check the
//! `libp2p_ping` metrics, they should be `>0`.

use futures::executor::block_on;
use futures::stream::StreamExt;
use libp2p::metrics::{Metrics, Recorder};
use libp2p::ping::{Ping, PingConfig};
use libp2p::swarm::SwarmEvent;
use libp2p::{identity, PeerId, Swarm};
use open_metrics_client::encoding::text::encode;
use open_metrics_client::registry::Registry;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::thread;

fn main() -> Result<(), Box<dyn Error>> {
    tide::log::start();

    let local_key = identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(local_key.public());
    tide::log::info!("Local peer id: {:?}", local_peer_id);

    let mut swarm = Swarm::new(
        block_on(libp2p::development_transport(local_key))?,
        Ping::new(PingConfig::new().with_keep_alive(true)),
        local_peer_id,
    );
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
    if let Some(addr) = std::env::args().nth(1) {
        let remote = addr.parse()?;
        swarm.dial_addr(remote)?;
        tide::log::info!("Dialed {}", addr)
    }

    let mut metric_registry = Registry::default();
    let metrics = Metrics::new(&mut metric_registry);
    thread::spawn(move || block_on(metrics_server(metric_registry)));

    block_on(async {
        loop {
            match swarm.select_next_some().await {
                SwarmEvent::Behaviour(ping_event) => {
                    tide::log::info!("{:?}", ping_event);
                    metrics.record(&ping_event);
                }
                swarm_event => {
                    tide::log::info!("{:?}", swarm_event);
                    metrics.record(&swarm_event);
                }
            }
        }
    })
}

pub async fn metrics_server(registry: Registry) -> std::result::Result<(), std::io::Error> {
    let mut app = tide::with_state(Arc::new(Mutex::new(registry)));

    app.at("/metrics")
        .get(|req: tide::Request<Arc<Mutex<Registry>>>| async move {
            let mut encoded = Vec::new();
            encode(&mut encoded, &req.state().lock().unwrap()).unwrap();
            Ok(String::from_utf8(encoded).unwrap())
        });

    app.listen("0.0.0.0:0").await?;

    Ok(())
}
