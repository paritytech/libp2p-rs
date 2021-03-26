// Copyright 2018 Parity Technologies (UK) Ltd.
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

//! # libp2p-dns
//!
//! This crate provides the type [`GenDnsConfig`] with its instantiations
//! [`DnsConfig`] and `TokioDnsConfig` for use with `async-std` and `tokio`,
//! respectively.
//!
//! A [`GenDnsConfig`] is an address-rewriting [`Transport`] wrapper around
//! an inner `Transport`. The composed transport behaves like the inner
//! transport, except that [`Transport::dial`] resolves `/dns/...`, `/dns4/...`,
//! `/dns6/...` and `/dnsaddr/...` components of the given `Multiaddr` through
//! a DNS, replacing them with the resolved protocols (typically TCP/IP).
//!
//! The `async-std` feature and hence the `DnsConfig` are
//! enabled by default. Tokio users can furthermore opt-in
//! to the `tokio-dns-over-rustls` and `tokio-dns-over-https-rustls`
//! features. For more information about these features, please
//! refer to the documentation of [trust-dns-resolver].
//!
//![trust-dns-resolver]: https://docs.rs/trust-dns-resolver/latest/trust_dns_resolver/#dns-over-tls-and-dns-over-https

use futures::{prelude::*, future::BoxFuture};
use libp2p_core::{
    Transport,
    multiaddr::{Protocol, Multiaddr},
    transport::{TransportError, ListenerEvent}
};
use smallvec::SmallVec;
use std::{borrow::Cow, convert::TryFrom, error, fmt, iter, net::IpAddr, str};
#[cfg(any(feature = "async-std", feature = "tokio"))]
use std::io;
#[cfg(any(feature = "async-std", feature = "tokio"))]
use trust_dns_resolver::system_conf;
use trust_dns_resolver::{
    AsyncResolver,
    ConnectionProvider,
    proto::xfer::dns_handle::DnsHandle,
};
#[cfg(feature = "tokio")]
use trust_dns_resolver::{TokioAsyncResolver, TokioConnection, TokioConnectionProvider};
#[cfg(feature = "async-std")]
use async_std_resolver::{AsyncStdConnection, AsyncStdConnectionProvider};

pub use trust_dns_resolver::config::{ResolverConfig, ResolverOpts};
pub use trust_dns_resolver::error::{ResolveError, ResolveErrorKind};

/// The prefix for `dnsaddr` protocol TXT record lookups.
const DNSADDR_PREFIX: &'static str = "_dnsaddr.";

/// The maximum number of dialing attempts to resolved addresses.
const MAX_DIAL_ATTEMPTS: usize = 16;

/// The maximum number of DNS lookups when dialing.
///
/// This limit is primarily a safeguard against too many, possibly
/// even cyclic, indirections in the addresses obtained from the
/// TXT records of a `/dnsaddr`.
const MAX_DNS_LOOKUPS: usize = 32;

/// The maximum number of TXT records applicable for the address
/// being dialed that are considered for further lookups as a
/// result of a single `/dnsaddr` lookup.
const MAX_TXT_RECORDS: usize = 16;

/// A `Transport` wrapper for performing DNS lookups when dialing `Multiaddr`esses
/// using `async-std` for all async I/O.
#[cfg(feature = "async-std")]
pub type DnsConfig<T> = GenDnsConfig<T, AsyncStdConnection, AsyncStdConnectionProvider>;

/// A `Transport` wrapper for performing DNS lookups when dialing `Multiaddr`esses
/// using `tokio` for all async I/O.
#[cfg(feature = "tokio")]
pub type TokioDnsConfig<T> = GenDnsConfig<T, TokioConnection, TokioConnectionProvider>;

/// A `Transport` wrapper for performing DNS lookups when dialing `Multiaddr`esses.
#[derive(Clone)]
pub struct GenDnsConfig<T, C, P>
where
    C: DnsHandle<Error = ResolveError>,
    P: ConnectionProvider<Conn = C>
{
    /// The underlying transport.
    inner: T,
    /// The DNS resolver used when dialing addresses with DNS components.
    resolver: AsyncResolver<C, P>,
}

#[cfg(feature = "async-std")]
impl<T> DnsConfig<T> {
    /// Creates a new [`DnsConfig`] from the OS's DNS configuration and defaults.
    pub async fn system(inner: T) -> Result<DnsConfig<T>, io::Error> {
        let (cfg, opts) = system_conf::read_system_conf()?;
        Self::custom(inner, cfg, opts).await
    }

    /// Creates a [`DnsConfig`] with a custom resolver configuration and options.
    pub async fn custom(inner: T, cfg: ResolverConfig, opts: ResolverOpts)
        -> Result<DnsConfig<T>, io::Error>
    {
        Ok(DnsConfig {
            inner,
            resolver: async_std_resolver::resolver(cfg, opts).await?
        })
    }
}

#[cfg(feature = "tokio")]
impl<T> TokioDnsConfig<T> {
    /// Creates a new [`TokioDnsConfig`] from the OS's DNS configuration and defaults.
    pub fn system(inner: T) -> Result<TokioDnsConfig<T>, io::Error> {
        let (cfg, opts) = system_conf::read_system_conf()?;
        Self::custom(inner, cfg, opts)
    }

    /// Creates a [`TokioDnsConfig`] with a custom resolver configuration
    /// and options.
    pub fn custom(inner: T, cfg: ResolverConfig, opts: ResolverOpts)
        -> Result<TokioDnsConfig<T>, io::Error>
    {
        Ok(TokioDnsConfig {
            inner,
            resolver: TokioAsyncResolver::tokio(cfg, opts)?
        })
    }
}

impl<T, C, P> fmt::Debug for GenDnsConfig<T, C, P>
where
    C: DnsHandle<Error = ResolveError>,
    P: ConnectionProvider<Conn = C>,
    T: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_tuple("GenDnsConfig").field(&self.inner).finish()
    }
}

impl<T, C, P> Transport for GenDnsConfig<T, C, P>
where
    T: Transport + Clone + Send + 'static,
    T::Error: Send,
    T::Dial: Send,
    C: DnsHandle<Error = ResolveError>,
    P: ConnectionProvider<Conn = C>,
{
    type Output = T::Output;
    type Error = DnsErr<T::Error>;
    type Listener = stream::MapErr<
        stream::MapOk<T::Listener,
            fn(ListenerEvent<T::ListenerUpgrade, T::Error>)
                -> ListenerEvent<Self::ListenerUpgrade, Self::Error>>,
        fn(T::Error) -> Self::Error>;
    type ListenerUpgrade = future::MapErr<T::ListenerUpgrade, fn(T::Error) -> Self::Error>;
    type Dial = future::Either<
        future::MapErr<T::Dial, fn(T::Error) -> Self::Error>,
        BoxFuture<'static, Result<Self::Output, Self::Error>>
    >;

    fn listen_on(self, addr: Multiaddr) -> Result<Self::Listener, TransportError<Self::Error>> {
        let listener = self.inner.listen_on(addr).map_err(|err| err.map(DnsErr::Transport))?;
        let listener = listener
            .map_ok::<_, fn(_) -> _>(|event| {
                event
                    .map(|upgr| {
                        upgr.map_err::<_, fn(_) -> _>(DnsErr::Transport)
                    })
                    .map_err(DnsErr::Transport)
            })
            .map_err::<_, fn(_) -> _>(DnsErr::Transport);
        Ok(listener)
    }

    fn dial(self, addr: Multiaddr) -> Result<Self::Dial, TransportError<Self::Error>> {
        // Asynchronlously resolve all DNS names in the address before proceeding
        // with dialing on the underlying transport.
        Ok(async move {
            let resolver = self.resolver;
            let inner = self.inner;

            let mut last_err = None;
            let mut dns_lookups = 0;
            let mut dial_attempts = 0;
            // We optimise for the common case of a single DNS component
            // in the address that is resolved with a single lookup.
            let mut unresolved = SmallVec::<[Multiaddr; 1]>::new();
            unresolved.push(addr.clone());

            // Resolve (i.e. replace) all DNS protocol components, initiating
            // dialing attempts as soon as there is another fully resolved
            // address.
            while let Some(addr) = unresolved.pop() {
                if let Some((i, name)) = addr.iter().enumerate().find(|(_, p)| match p {
                    Protocol::Dns(_) |
                    Protocol::Dns4(_) |
                    Protocol::Dns6(_) |
                    Protocol::Dnsaddr(_) => true,
                    _ => false
                }) {
                    if dns_lookups == MAX_DNS_LOOKUPS {
                        log::debug!("Too many DNS lookups. Dropping unresolved {}.", addr);
                        last_err = Some(DnsErr::TooManyLookups);
                        // There may still be fully resolved addresses in `unresolved`,
                        // so keep going until `unresolved` is empty.
                        continue
                    }
                    dns_lookups += 1;
                    match resolve(&name, &resolver).await {
                        Err(e) => {
                            if unresolved.is_empty() {
                                return Err(e)
                            }
                            // If there are still unresolved addresses, there is
                            // a chance of success, but we track the last error.
                            last_err = Some(e);
                        }
                        Ok(Resolved::One(ip)) => {
                            log::trace!("Resolved {} -> {}", name, ip);
                            let addr = addr.replace(i, |_| Some(ip)).expect("`i` is a valid index");
                            unresolved.push(addr);
                        }
                        Ok(Resolved::Many(ips)) => {
                            for ip in ips {
                                log::trace!("Resolved {} -> {}", name, ip);
                                let addr = addr.replace(i, |_| Some(ip)).expect("`i` is a valid index");
                                unresolved.push(addr);
                            }
                        }
                        Ok(Resolved::Addrs(addrs)) => {
                            let suffix = addr.iter().skip(i + 1).collect::<Multiaddr>();
                            let prefix = addr.iter().take(i).collect::<Multiaddr>();
                            let mut n = 0;
                            for a in addrs {
                                if a.ends_with(&suffix) {
                                    if n < MAX_TXT_RECORDS {
                                        n += 1;
                                        log::trace!("Resolved {} -> {}", name, a);
                                        let addr = prefix.iter().chain(a.iter()).collect::<Multiaddr>();
                                        unresolved.push(addr);
                                    } else {
                                        log::debug!("Too many TXT records. Dropping resolved {}.", a);
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // We have a fully resolved address, so try to dial it.
                    log::debug!("Dialing {}", addr);

                    let transport = inner.clone();
                    let result = match transport.dial(addr) {
                        Ok(out) => {
                            // We only count attempts that the inner transport
                            // actually accepted, i.e. for which it produced
                            // a dialing future.
                            dial_attempts += 1;
                            out.await.map_err(DnsErr::Transport)
                        }
                        Err(TransportError::MultiaddrNotSupported(a)) =>
                            Err(DnsErr::MultiaddrNotSupported(a)),
                        Err(TransportError::Other(err)) => Err(DnsErr::Transport(err))
                    };

                    match result {
                        Ok(out) => return Ok(out),
                        Err(err) => {
                            log::debug!("Dial error: {:?}.", err);
                            if unresolved.is_empty() {
                                return Err(err)
                            }
                            if dial_attempts == MAX_DIAL_ATTEMPTS {
                                log::debug!("Aborting dialing after {} attempts.", MAX_DIAL_ATTEMPTS);
                                return Err(err)
                            }
                            last_err = Some(err);
                        }
                    }
                }
            }

            // At this point, if there was at least one failed dialing
            // attempt, return that error. Otherwise there were no valid DNS records
            // for the given address to begin with (i.e. DNS lookups succeeded but
            // produced no records relevant for the given `addr`).
            Err(last_err.unwrap_or_else(||
                DnsErr::ResolveError(
                    ResolveErrorKind::Message("No matching records found.").into())))
        }.boxed().right_future())
    }

    fn address_translation(&self, server: &Multiaddr, observed: &Multiaddr) -> Option<Multiaddr> {
        self.inner.address_translation(server, observed)
    }
}

/// The possible errors of a [`GenDnsConfig`] wrapped transport.
#[derive(Debug)]
pub enum DnsErr<TErr> {
    /// The underlying transport encountered an error.
    Transport(TErr),
    /// DNS resolution failed.
    ResolveError(ResolveError),
    /// DNS resolution was successful, but the underlying transport refused the resolved address.
    MultiaddrNotSupported(Multiaddr),
    /// DNS resolution involved too many lookups.
    ///
    /// DNS resolution on dialing performs up to 32 DNS lookups. If these
    /// are not sufficient to obtain a fully-resolved address, this error
    /// is returned and the DNS records for the domain(s) being dialed
    /// should be investigated.
    TooManyLookups,
}

impl<TErr> fmt::Display for DnsErr<TErr>
where TErr: fmt::Display
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DnsErr::Transport(err) => write!(f, "{}", err),
            DnsErr::ResolveError(err) => write!(f, "{}", err),
            DnsErr::MultiaddrNotSupported(a) => write!(f, "Unsupported resolved address: {}", a),
            DnsErr::TooManyLookups => write!(f, "Too many DNS lookups"),
        }
    }
}

impl<TErr> error::Error for DnsErr<TErr>
where TErr: error::Error + 'static
{
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            DnsErr::Transport(err) => Some(err),
            DnsErr::ResolveError(err) => Some(err),
            DnsErr::MultiaddrNotSupported(_) => None,
            DnsErr::TooManyLookups => None,
        }
    }
}

/// The successful outcome of [`resolve`] for a given [`Protocol`].
enum Resolved<'a> {
    /// The given `Protocol` has been resolved to a single `Protocol`,
    /// which may be identical to the one given, in case it is not
    /// a DNS protocol component.
    One(Protocol<'a>),
    /// The given `Protocol` has been resolved to multiple alternative
    /// `Protocol`s as a result of a DNS lookup.
    Many(Vec<Protocol<'a>>),
    /// The given `Protocol` has been resolved to a new list of `Multiaddr`s
    /// obtained from DNS TXT records representing possible alternatives.
    /// These addresses may contain further DNS names that need resolving.
    Addrs(Vec<Multiaddr>),
}

/// Asynchronously resolves the domain name of a `Dns`, `Dns4`, `Dns6` or `Dnsaddr` protocol
/// component. If the given protocol is of a different type, it is returned unchanged as a
/// [`Resolved::One`].
fn resolve<'a, E: 'a + Send, C, P>(
    proto: &Protocol<'a>,
    resolver: &'a AsyncResolver<C,P>,
) -> BoxFuture<'a, Result<Resolved<'a>, DnsErr<E>>>
where
    C: DnsHandle<Error = ResolveError>,
    P: ConnectionProvider<Conn = C>,
{
    match proto {
        Protocol::Dns(ref name) => {
            resolver.lookup_ip(fqdn(name)).map(move |res| match res {
                Ok(ips) => {
                    let mut ips = ips.into_iter();
                    let one = ips.next()
                        .expect("If there are no results, `Err(NoRecordsFound)` is expected.");
                    if let Some(two) = ips.next() {
                        Ok(Resolved::Many(
                            iter::once(one).chain(iter::once(two))
                                .chain(ips)
                                .map(Protocol::from)
                                .collect()))
                    } else {
                        Ok(Resolved::One(Protocol::from(one)))
                    }
                }
                Err(e) => Err(DnsErr::ResolveError(e))
            }).boxed()
        }
        Protocol::Dns4(ref name) => {
            resolver.ipv4_lookup(fqdn(name)).map(move |res| match res {
                Ok(ips) => {
                    let mut ips = ips.into_iter();
                    let one = ips.next()
                        .expect("If there are no results, `Err(NoRecordsFound)` is expected.");
                    if let Some(two) = ips.next() {
                        Ok(Resolved::Many(
                            iter::once(one).chain(iter::once(two))
                                .chain(ips)
                                .map(IpAddr::from)
                                .map(Protocol::from)
                                .collect()))
                    } else {
                        Ok(Resolved::One(Protocol::from(IpAddr::from(one))))
                    }
                }
                Err(e) => Err(DnsErr::ResolveError(e))
            }).boxed()
        }
        Protocol::Dns6(ref name) => {
            resolver.ipv6_lookup(fqdn(name)).map(move |res| match res {
                Ok(ips) => {
                    let mut ips = ips.into_iter();
                    let one = ips.next()
                        .expect("If there are no results, `Err(NoRecordsFound)` is expected.");
                    if let Some(two) = ips.next() {
                        Ok(Resolved::Many(
                            iter::once(one).chain(iter::once(two))
                                .chain(ips)
                                .map(IpAddr::from)
                                .map(Protocol::from)
                                .collect()))
                    } else {
                        Ok(Resolved::One(Protocol::from(IpAddr::from(one))))
                    }
                }
                Err(e) => Err(DnsErr::ResolveError(e))
            }).boxed()
        },
        Protocol::Dnsaddr(ref name) => {
            let name = Cow::Owned([DNSADDR_PREFIX, name].concat());
            resolver.txt_lookup(fqdn(&name)).map(move |res| match res {
                Ok(txts) => {
                    let mut addrs = Vec::new();
                    for txt in txts {
                        if let Some(chars) = txt.txt_data().first() {
                            match parse_dnsaddr_txt(chars) {
                                Err(e) => {
                                    // Skip over seemingly invalid entries.
                                    log::debug!("Invalid TXT record: {:?}", e);
                                }
                                Ok(a) => {
                                    addrs.push(a);
                                }
                            }
                        }
                    }
                    Ok(Resolved::Addrs(addrs))
                }
                Err(e) => Err(DnsErr::ResolveError(e))
            }).boxed()
        }
        proto => future::ready(Ok(Resolved::One(proto.clone()))).boxed()
    }
}

/// Parses a `<character-string>` of a `dnsaddr` TXT record.
fn parse_dnsaddr_txt(txt: &[u8]) -> io::Result<Multiaddr> {
    let s = str::from_utf8(txt).map_err(invalid_data)?;
    match s.strip_prefix("dnsaddr=") {
        None => Err(invalid_data("Missing `dnsaddr=` prefix.")),
        Some(a) => Ok(Multiaddr::try_from(a).map_err(invalid_data)?)
    }
}

fn invalid_data(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e)
}

fn fqdn(name: &Cow<'_, str>) -> String {
    if name.ends_with('.') {
        name.to_string()
    } else {
        format!("{}.", name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{future::BoxFuture, stream::BoxStream};
    use libp2p_core::{
        Transport,
        PeerId,
        multiaddr::{Protocol, Multiaddr},
        transport::ListenerEvent,
        transport::TransportError,
    };

    #[test]
    fn basic_resolve() {
        let _ = env_logger::try_init();

        #[derive(Clone)]
        struct CustomTransport;

        impl Transport for CustomTransport {
            type Output = ();
            type Error = std::io::Error;
            type Listener = BoxStream<'static, Result<ListenerEvent<Self::ListenerUpgrade, Self::Error>, Self::Error>>;
            type ListenerUpgrade = BoxFuture<'static, Result<Self::Output, Self::Error>>;
            type Dial = BoxFuture<'static, Result<Self::Output, Self::Error>>;

            fn listen_on(self, _: Multiaddr) -> Result<Self::Listener, TransportError<Self::Error>> {
                unreachable!()
            }

            fn dial(self, addr: Multiaddr) -> Result<Self::Dial, TransportError<Self::Error>> {
                // Check that all DNS components have been resolved, i.e. replaced.
                assert!(!addr.iter().any(|p| match p {
                    Protocol::Dns(_) | Protocol::Dns4(_) | Protocol::Dns6(_) | Protocol::Dnsaddr(_)
                        => true,
                    _ => false,
                }));
                Ok(Box::pin(future::ready(Ok(()))))
            }

            fn address_translation(&self, _: &Multiaddr, _: &Multiaddr) -> Option<Multiaddr> {
                None
            }
        }

        async fn run<T, C, P>(transport: GenDnsConfig<T, C, P>)
        where
            C: DnsHandle<Error = ResolveError>,
            P: ConnectionProvider<Conn = C>,
            T: Transport + Clone + Send + 'static,
            T::Error: Send,
            T::Dial: Send,
        {
            // Success due to existing A record for example.com.
            let _ = transport
                .clone()
                .dial("/dns4/example.com/tcp/20000".parse().unwrap())
                .unwrap()
                .await
                .unwrap();

            // Success due to existing AAAA record for example.com.
            let _ = transport
                .clone()
                .dial("/dns6/example.com/tcp/20000".parse().unwrap())
                .unwrap()
                .await
                .unwrap();

            // Success due to pass-through, i.e. nothing to resolve.
            let _ = transport
                .clone()
                .dial("/ip4/1.2.3.4/tcp/20000".parse().unwrap())
                .unwrap()
                .await
                .unwrap();

            // Success due to the DNS TXT records at _dnsaddr.bootstrap.libp2p.io.
            let _ = transport
                .clone()
                .dial("/dnsaddr/bootstrap.libp2p.io".parse().unwrap())
                .unwrap()
                .await
                .unwrap();

            // Success due to the DNS TXT records at _dnsaddr.bootstrap.libp2p.io having
            // an entry with suffix `/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN`,
            // i.e. a bootnode with such a peer ID.
            let _ = transport
                .clone()
                .dial("/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN".parse().unwrap())
                .unwrap()
                .await
                .unwrap();

            // Failure due to the DNS TXT records at _dnsaddr.libp2p.io not having
            // an entry with a random `p2p` suffix.
            match transport
                .clone()
                .dial(format!("/dnsaddr/bootstrap.libp2p.io/p2p/{}", PeerId::random()).parse().unwrap())
                .unwrap()
                .await
            {
                Err(DnsErr::ResolveError(_)) => {},
                Err(e) => panic!("Unexpected error: {:?}", e),
                Ok(_) => panic!("Unexpected success.")
            }

            // Failure due to no records.
            match transport
                .clone()
                .dial("/dns4/example.invalid/tcp/20000".parse().unwrap())
                .unwrap()
                .await
            {
                Err(DnsErr::ResolveError(e)) => match e.kind() {
                    ResolveErrorKind::NoRecordsFound { .. } => {},
                    _ => panic!("Unexpected DNS error: {:?}", e),
                },
                Err(e) => panic!("Unexpected error: {:?}", e),
                Ok(_) => panic!("Unexpected success."),
            }
        }

        #[cfg(feature = "async-std")]
        {
            // Be explicit about the resolver used. At least on github CI, TXT
            // type record lookups may not work with the system DNS resolver.
            let config = ResolverConfig::quad9();
            let opts = ResolverOpts::default();
            async_std_crate::task::block_on(
                DnsConfig::custom(CustomTransport, config, opts).then(|dns| run(dns.unwrap()))
            );
        }

        #[cfg(feature = "tokio")]
        {
            // Be explicit about the resolver used. At least on github CI, TXT
            // type record lookups may not work with the system DNS resolver.
            let config = ResolverConfig::quad9();
            let opts = ResolverOpts::default();
            let rt = tokio_crate::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
                .unwrap();
            rt.block_on(run(TokioDnsConfig::custom(CustomTransport, config, opts).unwrap()));
        }
    }
}
