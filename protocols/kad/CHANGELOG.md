# 0.30.0 [unreleased]

- Update `libp2p-swarm`.

# 0.29.0 [2021-03-17]

- Add `KademliaCaching` and `KademliaConfig::set_caching` to configure
  whether Kademlia should track, in lookups, the closest nodes to a key
  that did not return a record, via `GetRecordOk::cache_candidates`.
  As before, if a lookup used a quorum of 1, these candidates will
  automatically be sent the found record. Otherwise, with a lookup
  quorum of > 1, the candidates can be used with `Kademlia::put_record_to`
  after selecting one of the return records to cache. As is the current
  behaviour, caching is enabled by default with a `max_peers` of 1, i.e.
  it only tracks the closest node to the key that did not return a record.

- Add `Kademlia::put_record_to` for storing a record at specific nodes,
  e.g. for write-back caching after a successful read with quorum > 1.

- Update `libp2p-swarm`.

- Update dependencies.

# 0.28.1 [2021-02-15]

- Update dependencies.

# 0.28.0 [2021-01-12]

- Update dependencies.

# 0.27.1 [2021-01-11]

- Add From impls for `kbucket::Key`.
  [PR 1909](https://github.com/libp2p/rust-libp2p/pull/1909).

# 0.27.0 [2020-12-17]

- Update `libp2p-core` and `libp2p-swarm`.

# 0.26.0 [2020-11-25]

- Update `libp2p-core` and `libp2p-swarm`.

- Have two `ProviderRecord`s be equal iff their `key` and `provider` fields are
  equal. [PR 1850](https://github.com/libp2p/rust-libp2p/pull/1850/).

# 0.25.0 [2020-11-09]

- Upon newly established connections, delay routing table
  updates until after the configured protocol name has
  been confirmed by the connection handler, i.e. until
  after at least one substream has been successfully
  negotiated. In configurations with different protocol names,
  this avoids undesirable nodes being included in the
  local routing table at least temporarily.
  [PR 1821](https://github.com/libp2p/rust-libp2p/pull/1821).

- Update dependencies.

# 0.24.0 [2020-10-16]

- Update `libp2p-core` and `libp2p-swarm`.

- Update `sha2` dependency.

# 0.23.0 [2020-09-09]

- Increase default max packet size from 4KiB to 16KiB.
  See [issue 1622](https://github.com/libp2p/rust-libp2p/issues/1622).

- Add `Distance::log2` ([PR 1719](https://github.com/libp2p/rust-libp2p/pull/1719)).

- Update `libp2p-swarm` and `libp2p-core`.

# 0.22.1 [2020-08-19]

- Explicitly convert from u8 to usize in `BucketIndex::range` to prevent type
  inference issues ([PR 1716](https://github.com/libp2p/rust-libp2p/pull/1716)).

# 0.22.0 [2020-08-18]

- Store addresses in provider records.
  See [PR 1708](https://github.com/libp2p/rust-libp2p/pull/1708).

- Update `libp2p-core` and `libp2p-swarm` dependencies.

- Add `KBucketRef::range` exposing the minimum inclusive and maximum inclusive
  `Distance` for the bucket
  ([PR 1680](https://github.com/libp2p/rust-libp2p/pull/1680)).

- Add `NetworkBehaviour::inject_address_change` implementation
  ([PR 1649](https://github.com/libp2p/rust-libp2p/pull/1649)).

# 0.21.0 [2020-07-01]

- Remove `KademliaEvent::Discovered`
  ([PR 1632](https://github.com/libp2p/rust-libp2p/pull/1632))

- More control and insight for k-buckets
  ([PR 1628](https://github.com/libp2p/rust-libp2p/pull/1628)).
  In particular, `Kademlia::kbuckets_entries` has been removed and
  replaced by `Kademlia::kbuckets`/`Kademlia::kbucket` which provide
  more information than just the peer IDs. Furthermore `Kademlia::add_address`
  now returns a result and two new events, `KademliaEvent::RoutablePeer`
  and `KademliaEvent::PendingRoutablePeer` are introduced (but are not
  required to be acted upon in order to retain existing behaviour).
  For more details, see the PR description.

# 0.20.1 [2020-06-23]

- Maintenance release ([PR 1623](https://github.com/libp2p/rust-libp2p/pull/1623)).

# 0.20.0 [2020-06-22]

- Optionally require iterative queries to use disjoint paths based
  on S/Kademlia for increased resiliency in the presence of potentially
  adversarial nodes ([PR 1473](https://github.com/libp2p/rust-libp2p/pull/1473)).

- Re-export several types
  ([PR 1595](https://github.com/libp2p/rust-libp2p/pull/1595)).

- Updated dependencies.
