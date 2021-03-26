# 0.22.0 [2021-02-15]

- Rename the crate to `libp2p-swarm-derive`.

# 0.21.0 [2020-11-25]

- Update for compatibility with `libp2p-swarm-0.25`.

# 0.20.2 [2020-07-28]

- Generate fully-qualified method name for `poll` to avoid
ambiguity. [PR 1681](https://github.com/libp2p/rust-libp2p/pull/1681).

# 0.20.1 [2020-07-08]

- Allow users to opt out of the `NetworkBehaviourEventProcess`
mechanism through `#[behaviour(event_process = false)]`. This is
useful if users want to process all events while polling the
swarm through `SwarmEvent::Behaviour`.
