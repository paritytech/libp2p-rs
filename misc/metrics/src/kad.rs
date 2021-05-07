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
use open_metrics_client::metrics::histogram::{exponential_series, Histogram};
use open_metrics_client::registry::{Registry, Unit};

pub struct Metrics {
    query_result_get_record_ok: Histogram,
    query_result_get_record_error: Family<GetRecordResult, Counter>,

    query_result_get_closest_peers_ok: Histogram,
    query_result_get_closest_peers_error: Family<GetClosestPeersResult, Counter>,

    query_result_get_providers_ok: Histogram,
    query_result_get_providers_error: Family<GetProvidersResult, Counter>,

    query_result_num_requests: Family<QueryResult, Histogram>,
    query_result_num_success: Family<QueryResult, Histogram>,
    query_result_num_failure: Family<QueryResult, Histogram>,
    query_result_duration: Family<QueryResult, Histogram>,
}

impl Metrics {
    pub fn new(registry: &mut Registry) -> Self {
        let sub_registry = registry.sub_registry("kad");

        let query_result_get_record_ok = Histogram::new(exponential_series(1.0, 2.0, 10));
        sub_registry.register(
            "query_result_get_record_ok",
            "Number of records returned by a successful Kademlia get record query.",
            Box::new(query_result_get_record_ok.clone()),
        );

        let query_result_get_record_error = Family::default();
        sub_registry.register(
            "query_result_get_record_error",
            "Number of failed Kademlia get record queries.",
            Box::new(query_result_get_record_error.clone()),
        );

        let query_result_get_closest_peers_ok = Histogram::new(exponential_series(1.0, 2.0, 10));
        sub_registry.register(
            "query_result_get_closest_peers_ok",
            "Number of closest peers returned by a successful Kademlia get closest peers query.",
            Box::new(query_result_get_closest_peers_ok.clone()),
        );

        let query_result_get_closest_peers_error = Family::default();
        sub_registry.register(
            "query_result_get_closest_peers_error",
            "Number of failed Kademlia get closest peers queries.",
            Box::new(query_result_get_closest_peers_error.clone()),
        );

        let query_result_get_providers_ok = Histogram::new(exponential_series(1.0, 2.0, 10));
        sub_registry.register(
            "query_result_get_providers_ok",
            "Number of providers returned by a successful Kademlia get providers query.",
            Box::new(query_result_get_providers_ok.clone()),
        );

        let query_result_get_providers_error = Family::default();
        sub_registry.register(
            "query_result_get_providers_error",
            "Number of failed Kademlia get providers queries.",
            Box::new(query_result_get_providers_error.clone()),
        );

        let query_result_num_requests =
            Family::new_with_constructor(|| Histogram::new(exponential_series(1.0, 2.0, 10)));
        sub_registry.register(
            "query_result_num_requests",
            "Number of requests started for a Kademlia query.",
            Box::new(query_result_num_requests.clone()),
        );

        let query_result_num_success =
            Family::new_with_constructor(|| Histogram::new(exponential_series(1.0, 2.0, 10)));
        sub_registry.register(
            "query_result_num_success",
            "Number of successful requests of a Kademlia query.",
            Box::new(query_result_num_success.clone()),
        );

        let query_result_num_failure =
            Family::new_with_constructor(|| Histogram::new(exponential_series(1.0, 2.0, 10)));
        sub_registry.register(
            "query_result_num_failure",
            "Number of failed requests of a Kademlia query.",
            Box::new(query_result_num_failure.clone()),
        );

        let query_result_duration =
            Family::new_with_constructor(|| Histogram::new(exponential_series(0.001, 2.0, 12)));
        sub_registry.register_with_unit(
            "query_result_duration",
            "Duration of a Kademlia query.",
            Unit::Seconds,
            Box::new(query_result_duration.clone()),
        );

        Self {
            query_result_get_record_ok,
            query_result_get_record_error,

            query_result_get_closest_peers_ok,
            query_result_get_closest_peers_error,

            query_result_get_providers_ok,
            query_result_get_providers_error,

            query_result_num_requests,
            query_result_num_success,
            query_result_num_failure,
            query_result_duration,
        }
    }
}

impl super::Recorder<libp2p_kad::KademliaEvent> for super::Metrics {
    fn record(&self, event: &libp2p_kad::KademliaEvent) {
        match event {
            libp2p_kad::KademliaEvent::QueryResult { result, stats, .. } => {
                self.kad
                    .query_result_num_requests
                    .get_or_create(&result.into())
                    .observe(stats.num_requests().into());
                self.kad
                    .query_result_num_success
                    .get_or_create(&result.into())
                    .observe(stats.num_successes().into());
                self.kad
                    .query_result_num_failure
                    .get_or_create(&result.into())
                    .observe(stats.num_failures().into());
                if let Some(duration) = stats.duration() {
                    self.kad
                        .query_result_duration
                        .get_or_create(&result.into())
                        .observe(duration.as_secs_f64());
                }

                match result {
                    libp2p_kad::QueryResult::GetRecord(result) => match result {
                        Ok(ok) => self
                            .kad
                            .query_result_get_record_ok
                            .observe(ok.records.len() as f64),
                        Err(error) => {
                            self.kad
                                .query_result_get_record_error
                                .get_or_create(&error.into())
                                .inc();
                        }
                    },
                    libp2p_kad::QueryResult::GetClosestPeers(result) => match result {
                        Ok(ok) => self
                            .kad
                            .query_result_get_closest_peers_ok
                            .observe(ok.peers.len() as f64),
                        Err(error) => {
                            self.kad
                                .query_result_get_closest_peers_error
                                .get_or_create(&error.into())
                                .inc();
                        }
                    },
                    libp2p_kad::QueryResult::GetProviders(result) => match result {
                        Ok(ok) => self
                            .kad
                            .query_result_get_providers_ok
                            .observe(ok.providers.len() as f64),
                        Err(error) => {
                            self.kad
                                .query_result_get_providers_error
                                .get_or_create(&error.into())
                                .inc();
                        }
                    },
                    _ => {}
                }
            }
            _ => {}
        }
    }
}

#[derive(Encode, Hash, Clone, Eq, PartialEq)]
struct QueryResult {
    r#type: QueryType,
}

#[derive(Encode, Hash, Clone, Eq, PartialEq)]
enum QueryType {
    Bootstrap,
    GetClosestPeers,
    GetProviders,
    StartProviding,
    RepublishProvider,
    GetRecord,
    PutRecord,
    RepublishRecord,
}

impl From<&libp2p_kad::QueryResult> for QueryResult {
    fn from(result: &libp2p_kad::QueryResult) -> Self {
        match result {
            libp2p_kad::QueryResult::Bootstrap(_) => QueryResult {
                r#type: QueryType::Bootstrap,
            },
            libp2p_kad::QueryResult::GetClosestPeers(_) => QueryResult {
                r#type: QueryType::GetClosestPeers,
            },
            libp2p_kad::QueryResult::GetProviders(_) => QueryResult {
                r#type: QueryType::GetProviders,
            },
            libp2p_kad::QueryResult::StartProviding(_) => QueryResult {
                r#type: QueryType::StartProviding,
            },
            libp2p_kad::QueryResult::RepublishProvider(_) => QueryResult {
                r#type: QueryType::RepublishProvider,
            },
            libp2p_kad::QueryResult::GetRecord(_) => QueryResult {
                r#type: QueryType::GetRecord,
            },
            libp2p_kad::QueryResult::PutRecord(_) => QueryResult {
                r#type: QueryType::PutRecord,
            },
            libp2p_kad::QueryResult::RepublishRecord(_) => QueryResult {
                r#type: QueryType::RepublishRecord,
            },
        }
    }
}

#[derive(Encode, Hash, Clone, Eq, PartialEq)]
struct GetRecordResult {
    error: GetRecordError,
}

#[derive(Encode, Hash, Clone, Eq, PartialEq)]
enum GetRecordError {
    NotFound,
    QuorumFailed,
    Timeout,
}

impl From<&libp2p_kad::GetRecordError> for GetRecordResult {
    fn from(error: &libp2p_kad::GetRecordError) -> Self {
        match error {
            libp2p_kad::GetRecordError::NotFound { .. } => GetRecordResult {
                error: GetRecordError::NotFound,
            },
            libp2p_kad::GetRecordError::QuorumFailed { .. } => GetRecordResult {
                error: GetRecordError::QuorumFailed,
            },
            libp2p_kad::GetRecordError::Timeout { .. } => GetRecordResult {
                error: GetRecordError::Timeout,
            },
        }
    }
}

#[derive(Encode, Hash, Clone, Eq, PartialEq)]
struct GetClosestPeersResult {
    error: GetClosestPeersError,
}

#[derive(Encode, Hash, Clone, Eq, PartialEq)]
enum GetClosestPeersError {
    Timeout,
}

impl From<&libp2p_kad::GetClosestPeersError> for GetClosestPeersResult {
    fn from(error: &libp2p_kad::GetClosestPeersError) -> Self {
        match error {
            libp2p_kad::GetClosestPeersError::Timeout { .. } => GetClosestPeersResult {
                error: GetClosestPeersError::Timeout,
            },
        }
    }
}

#[derive(Encode, Hash, Clone, Eq, PartialEq)]
struct GetProvidersResult {
    error: GetProvidersError,
}

#[derive(Encode, Hash, Clone, Eq, PartialEq)]
enum GetProvidersError {
    Timeout,
}

impl From<&libp2p_kad::GetProvidersError> for GetProvidersResult {
    fn from(error: &libp2p_kad::GetProvidersError) -> Self {
        match error {
            libp2p_kad::GetProvidersError::Timeout { .. } => GetProvidersResult {
                error: GetProvidersError::Timeout,
            },
        }
    }
}
