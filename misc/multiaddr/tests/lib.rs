
use data_encoding::HEXUPPER;
use multihash::Multihash;
use parity_multiaddr::*;
use quickcheck::{Arbitrary, Gen, QuickCheck};
use rand::Rng;
use std::{
    borrow::Cow,
    convert::TryFrom,
    iter::FromIterator,
    net::{Ipv4Addr, Ipv6Addr},
    str::FromStr
};

// Property tests

#[test]
fn to_from_bytes_identity() {
    fn prop(a: Ma) -> bool {
        let b = a.0.to_vec();
        Some(a) == Multiaddr::try_from(b).ok().map(Ma)
    }
    QuickCheck::new().quickcheck(prop as fn(Ma) -> bool)
}

#[test]
fn to_from_str_identity() {
    fn prop(a: Ma) -> bool {
        let b = a.0.to_string();
        Some(a) == Multiaddr::from_str(&b).ok().map(Ma)
    }
    QuickCheck::new().quickcheck(prop as fn(Ma) -> bool)
}

#[test]
fn byteswriter() {
    fn prop(a: Ma, b: Ma) -> bool {
        let mut x = a.0.clone();
        for p in b.0.iter() {
            x = x.with(p)
        }
        x.iter().zip(a.0.iter().chain(b.0.iter())).all(|(x, y)| x == y)
    }
    QuickCheck::new().quickcheck(prop as fn(Ma, Ma) -> bool)
}

#[test]
fn push_pop_identity() {
    fn prop(a: Ma, p: Proto) -> bool {
        let mut b = a.clone();
        let q = p.clone();
        b.0.push(q.0);
        assert_ne!(a.0, b.0);
        Some(p.0) == b.0.pop() && a.0 == b.0
    }
    QuickCheck::new().quickcheck(prop as fn(Ma, Proto) -> bool)
}

#[test]
fn ends_with() {
    fn prop(Ma(m): Ma) {
        let n = m.iter().count();
        for i in 0 .. n {
            let suffix = m.iter().skip(i).collect::<Multiaddr>();
            assert!(m.ends_with(&suffix));
        }
    }
    QuickCheck::new().quickcheck(prop as fn(_))
}


// Arbitrary impls


#[derive(PartialEq, Eq, Clone, Hash, Debug)]
struct Ma(Multiaddr);

impl Arbitrary for Ma {
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        let iter = (0 .. g.next_u32() % 128).map(|_| Proto::arbitrary(g).0);
        Ma(Multiaddr::from_iter(iter))
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
struct Proto(Protocol<'static>);

impl Arbitrary for Proto {
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        use Protocol::*;
        match g.gen_range(0, 25) { // TODO: Add Protocol::Quic
             0 => Proto(Dccp(g.gen())),
             1 => Proto(Dns(Cow::Owned(SubString::arbitrary(g).0))),
             2 => Proto(Dns4(Cow::Owned(SubString::arbitrary(g).0))),
             3 => Proto(Dns6(Cow::Owned(SubString::arbitrary(g).0))),
             4 => Proto(Http),
             5 => Proto(Https),
             6 => Proto(Ip4(Ipv4Addr::arbitrary(g))),
             7 => Proto(Ip6(Ipv6Addr::arbitrary(g))),
             8 => Proto(P2pWebRtcDirect),
             9 => Proto(P2pWebRtcStar),
            10 => Proto(P2pWebSocketStar),
            11 => Proto(Memory(g.gen())),
            // TODO: impl Arbitrary for Multihash:
            12 => Proto(P2p(multihash("QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC"))),
            13 => Proto(P2pCircuit),
            14 => Proto(Quic),
            15 => Proto(Sctp(g.gen())),
            16 => Proto(Tcp(g.gen())),
            17 => Proto(Udp(g.gen())),
            18 => Proto(Udt),
            19 => Proto(Unix(Cow::Owned(SubString::arbitrary(g).0))),
            20 => Proto(Utp),
            21 => Proto(Ws("/".into())),
            22 => Proto(Wss("/".into())),
            23 => {
                let mut a = [0; 10];
                g.fill(&mut a);
                Proto(Onion(Cow::Owned(a), g.gen_range(1, std::u16::MAX)))
            },
            24 => {
                let mut a = [0; 35];
                g.fill_bytes(&mut a);
                Proto(Onion3((a, g.gen_range(1, std::u16::MAX)).into()))
            },
             _ => panic!("outside range")
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
struct SubString(String); // ASCII string without '/'

impl Arbitrary for SubString {
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        let mut s = String::arbitrary(g);
        s.retain(|c| c.is_ascii() && c != '/');
        SubString(s)
    }
}


// other unit tests


fn ma_valid(source: &str, target: &str, protocols: Vec<Protocol<'_>>) {
    let parsed = source.parse::<Multiaddr>().unwrap();
    assert_eq!(HEXUPPER.encode(&parsed.to_vec()[..]), target);
    assert_eq!(parsed.iter().collect::<Vec<_>>(), protocols);
    assert_eq!(source.parse::<Multiaddr>().unwrap().to_string(), source);
    assert_eq!(Multiaddr::try_from(HEXUPPER.decode(target.as_bytes()).unwrap()).unwrap(), parsed);
}

fn multihash(s: &str) -> Multihash {
    Multihash::from_bytes(&bs58::decode(s).into_vec().unwrap()).unwrap()
}

#[test]
fn multiaddr_eq() {
    let m1 = "/ip4/127.0.0.1/udp/1234".parse::<Multiaddr>().unwrap();
    let m2 = "/ip4/127.0.0.1/tcp/1234".parse::<Multiaddr>().unwrap();
    let m3 = "/ip4/127.0.0.1/tcp/1234".parse::<Multiaddr>().unwrap();

    assert_ne!(m1, m2);
    assert_ne!(m2, m1);
    assert_eq!(m2, m3);
    assert_eq!(m1, m1);
}

#[test]
fn construct_success() {
    use Protocol::*;

    let local: Ipv4Addr = "127.0.0.1".parse().unwrap();
    let addr6: Ipv6Addr = "2001:8a0:7ac5:4201:3ac9:86ff:fe31:7095".parse().unwrap();

    ma_valid("/ip4/1.2.3.4", "0401020304", vec![Ip4("1.2.3.4".parse().unwrap())]);
    ma_valid("/ip4/0.0.0.0", "0400000000", vec![Ip4("0.0.0.0".parse().unwrap())]);
    ma_valid("/ip6/::1", "2900000000000000000000000000000001", vec![Ip6("::1".parse().unwrap())]);
    ma_valid("/ip6/2601:9:4f81:9700:803e:ca65:66e8:c21",
             "29260100094F819700803ECA6566E80C21",
             vec![Ip6("2601:9:4f81:9700:803e:ca65:66e8:c21".parse().unwrap())]);
    ma_valid("/udp/0", "91020000", vec![Udp(0)]);
    ma_valid("/tcp/0", "060000", vec![Tcp(0)]);
    ma_valid("/sctp/0", "84010000", vec![Sctp(0)]);
    ma_valid("/udp/1234", "910204D2", vec![Udp(1234)]);
    ma_valid("/tcp/1234", "0604D2", vec![Tcp(1234)]);
    ma_valid("/sctp/1234", "840104D2", vec![Sctp(1234)]);
    ma_valid("/udp/65535", "9102FFFF", vec![Udp(65535)]);
    ma_valid("/tcp/65535", "06FFFF", vec![Tcp(65535)]);
    ma_valid("/p2p/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC",
             "A503221220D52EBB89D85B02A284948203A62FF28389C57C9F42BEEC4EC20DB76A68911C0B",
             vec![P2p(multihash("QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC"))]);
    ma_valid("/udp/1234/sctp/1234", "910204D2840104D2", vec![Udp(1234), Sctp(1234)]);
    ma_valid("/udp/1234/udt", "910204D2AD02", vec![Udp(1234), Udt]);
    ma_valid("/udp/1234/utp", "910204D2AE02", vec![Udp(1234), Utp]);
    ma_valid("/tcp/1234/http", "0604D2E003", vec![Tcp(1234), Http]);
    ma_valid("/tcp/1234/https", "0604D2BB03", vec![Tcp(1234), Https]);
    ma_valid("/p2p/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC/tcp/1234",
             "A503221220D52EBB89D85B02A284948203A62FF28389C57C9F42BEEC4EC20DB76A68911C0B0604D2",
             vec![P2p(multihash("QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC")), Tcp(1234)]);
    ma_valid("/ip4/127.0.0.1/udp/1234", "047F000001910204D2", vec![Ip4(local.clone()), Udp(1234)]);
    ma_valid("/ip4/127.0.0.1/udp/0", "047F00000191020000", vec![Ip4(local.clone()), Udp(0)]);
    ma_valid("/ip4/127.0.0.1/tcp/1234", "047F0000010604D2", vec![Ip4(local.clone()), Tcp(1234)]);
    ma_valid("/ip4/127.0.0.1/p2p/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC",
             "047F000001A503221220D52EBB89D85B02A284948203A62FF28389C57C9F42BEEC4EC20DB76A68911C0B",
             vec![Ip4(local.clone()), P2p(multihash("QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC"))]);
    ma_valid("/ip4/127.0.0.1/p2p/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC/tcp/1234",
             "047F000001A503221220D52EBB89D85B02A284948203A62FF28389C57C9F42BEEC4EC20DB76A68911C0B0604D2",
             vec![Ip4(local.clone()), P2p(multihash("QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC")), Tcp(1234)]);
    // /unix/a/b/c/d/e,
    // /unix/stdio,
    // /ip4/1.2.3.4/tcp/80/unix/a/b/c/d/e/f,
    // /ip4/127.0.0.1/p2p/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC/tcp/1234/unix/stdio
    ma_valid("/ip6/2001:8a0:7ac5:4201:3ac9:86ff:fe31:7095/tcp/8000/ws/p2p/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC",
             "29200108A07AC542013AC986FFFE317095061F40DD03A503221220D52EBB89D85B02A284948203A62FF28389C57C9F42BEEC4EC20DB76A68911C0B",
             vec![Ip6(addr6.clone()), Tcp(8000), Ws("/".into()), P2p(multihash("QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC"))
             ]);
    ma_valid("/p2p-webrtc-star/ip4/127.0.0.1/tcp/9090/ws/p2p/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC",
             "9302047F000001062382DD03A503221220D52EBB89D85B02A284948203A62FF28389C57C9F42BEEC4EC20DB76A68911C0B",
             vec![P2pWebRtcStar, Ip4(local.clone()), Tcp(9090), Ws("/".into()), P2p(multihash("QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC"))
             ]);
    ma_valid("/ip6/2001:8a0:7ac5:4201:3ac9:86ff:fe31:7095/tcp/8000/wss/p2p/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC",
             "29200108A07AC542013AC986FFFE317095061F40DE03A503221220D52EBB89D85B02A284948203A62FF28389C57C9F42BEEC4EC20DB76A68911C0B",
             vec![Ip6(addr6.clone()), Tcp(8000), Wss("/".into()), P2p(multihash("QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC"))]);
    ma_valid("/ip4/127.0.0.1/tcp/9090/p2p-circuit/p2p/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC",
             "047F000001062382A202A503221220D52EBB89D85B02A284948203A62FF28389C57C9F42BEEC4EC20DB76A68911C0B",
             vec![Ip4(local.clone()), Tcp(9090), P2pCircuit, P2p(multihash("QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC"))]);

    ma_valid(
        "/onion/aaimaq4ygg2iegci:80",
        "BC030010C0439831B48218480050",
        vec![Onion(Cow::Owned([0, 16, 192, 67, 152, 49, 180, 130, 24, 72]), 80)],
    );
    ma_valid(
        "/onion3/vww6ybal4bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd:1234",
        "BD03ADADEC040BE047F9658668B11A504F3155001F231A37F54C4476C07FB4CC139ED7E30304D2",
        vec![Onion3(([173, 173, 236, 4, 11, 224, 71, 249, 101, 134, 104, 177, 26, 80, 79, 49, 85, 0, 31, 35, 26, 55, 245, 76, 68, 118, 192, 127, 180, 204, 19, 158, 215, 227, 3], 1234).into())],
    );
    ma_valid(
        "/dnsaddr/sjc-1.bootstrap.libp2p.io",
        "3819736A632D312E626F6F7473747261702E6C69627032702E696F",
        vec![Dnsaddr(Cow::Borrowed("sjc-1.bootstrap.libp2p.io"))]
    );
    ma_valid(
        "/dnsaddr/sjc-1.bootstrap.libp2p.io/tcp/1234/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
        "3819736A632D312E626F6F7473747261702E6C69627032702E696F0604D2A50322122006B3608AA000274049EB28AD8E793A26FF6FAB281A7D3BD77CD18EB745DFAABB",
        vec![Dnsaddr(Cow::Borrowed("sjc-1.bootstrap.libp2p.io")), Tcp(1234), P2p(multihash("QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN"))]
    );
}

#[test]
fn construct_fail() {
    let addresses = [
        "/ip4",
        "/ip4/::1",
        "/ip4/fdpsofodsajfdoisa",
        "/ip6",
        "/udp",
        "/tcp",
        "/sctp",
        "/udp/65536",
        "/tcp/65536",
        "/onion/9imaq4ygg2iegci7:80",
        "/onion/aaimaq4ygg2iegci7:80",
        "/onion/timaq4ygg2iegci7:0",
        "/onion/timaq4ygg2iegci7:-1",
        "/onion/timaq4ygg2iegci7",
        "/onion/timaq4ygg2iegci@:666",
        "/onion3/9ww6ybal4bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd:80",
        "/onion3/vww6ybal4bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd7:80",
        "/onion3/vww6ybal4bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd:0",
        "/onion3/vww6ybal4bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd:-1",
        "/onion3/vww6ybal4bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd",
        "/onion3/vww6ybal4bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyy@:666",
        "/udp/1234/sctp",
        "/udp/1234/udt/1234",
        "/udp/1234/utp/1234",
        "/ip4/127.0.0.1/udp/jfodsajfidosajfoidsa",
        "/ip4/127.0.0.1/udp",
        "/ip4/127.0.0.1/tcp/jfodsajfidosajfoidsa",
        "/ip4/127.0.0.1/tcp",
        "/ip4/127.0.0.1/p2p",
        "/ip4/127.0.0.1/p2p/tcp",
        "/p2p-circuit/50"
    ];

    for address in &addresses {
        assert!(address.parse::<Multiaddr>().is_err(), address.to_string());
    }
}


#[test]
fn to_multiaddr() {
    assert_eq!(Multiaddr::from(Ipv4Addr::new(127, 0, 0, 1)), "/ip4/127.0.0.1".parse().unwrap());
    assert_eq!(Multiaddr::from(Ipv6Addr::new(0x2601, 0x9, 0x4f81, 0x9700, 0x803e, 0xca65, 0x66e8, 0xc21)),
               "/ip6/2601:9:4f81:9700:803e:ca65:66e8:c21".parse().unwrap());
    assert_eq!(Multiaddr::try_from("/ip4/127.0.0.1/tcp/1234".to_string()).unwrap(),
               "/ip4/127.0.0.1/tcp/1234".parse::<Multiaddr>().unwrap());
    assert_eq!(Multiaddr::try_from("/ip6/2601:9:4f81:9700:803e:ca65:66e8:c21").unwrap(),
               "/ip6/2601:9:4f81:9700:803e:ca65:66e8:c21".parse::<Multiaddr>().unwrap());
    assert_eq!(Multiaddr::from(Ipv4Addr::new(127, 0, 0, 1)).with(Protocol::Tcp(1234)),
               "/ip4/127.0.0.1/tcp/1234".parse::<Multiaddr>().unwrap());
    assert_eq!(Multiaddr::from(Ipv6Addr::new(0x2601, 0x9, 0x4f81, 0x9700, 0x803e, 0xca65, 0x66e8, 0xc21))
                   .with(Protocol::Tcp(1234)),
               "/ip6/2601:9:4f81:9700:803e:ca65:66e8:c21/tcp/1234".parse::<Multiaddr>().unwrap());
}

#[test]
fn from_bytes_fail() {
    let bytes = vec![1, 2, 3, 4];
    assert!(Multiaddr::try_from(bytes).is_err());
}


#[test]
fn ser_and_deser_json() {
    let addr : Multiaddr = "/ip4/0.0.0.0/tcp/0".parse::<Multiaddr>().unwrap();
    let serialized = serde_json::to_string(&addr).unwrap();
    assert_eq!(serialized, "\"/ip4/0.0.0.0/tcp/0\"");
    let deserialized: Multiaddr = serde_json::from_str(&serialized).unwrap();
    assert_eq!(addr, deserialized);
}


#[test]
fn ser_and_deser_bincode() {
    let addr : Multiaddr = "/ip4/0.0.0.0/tcp/0".parse::<Multiaddr>().unwrap();
    let serialized = bincode::serialize(&addr).unwrap();
    // compact addressing
    assert_eq!(serialized, vec![8, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 6, 0, 0]);
    let deserialized: Multiaddr = bincode::deserialize(&serialized).unwrap();
    assert_eq!(addr, deserialized);
}

#[test]
fn append() {
    let mut a: Multiaddr = Protocol::Ip4(Ipv4Addr::new(1, 2, 3, 4)).into();
    a.push(Protocol::Tcp(80));
    a.push(Protocol::Http);

    let mut i = a.iter();
    assert_eq!(Some(Protocol::Ip4(Ipv4Addr::new(1, 2, 3, 4))), i.next());
    assert_eq!(Some(Protocol::Tcp(80)), i.next());
    assert_eq!(Some(Protocol::Http), i.next());
    assert_eq!(None, i.next())
}

fn replace_ip_addr(a: &Multiaddr, p: Protocol<'_>) -> Option<Multiaddr> {
    a.replace(0, move |x| match x {
        Protocol::Ip4(_) | Protocol::Ip6(_) => Some(p),
        _ => None
    })
}

#[test]
fn replace_ip4_with_ip4() {
    let server = multiaddr!(Ip4(Ipv4Addr::LOCALHOST), Tcp(10000u16));
    let result = replace_ip_addr(&server, Protocol::Ip4([80, 81, 82, 83].into())).unwrap();
    assert_eq!(result, multiaddr!(Ip4([80, 81, 82, 83]), Tcp(10000u16)))
}

#[test]
fn replace_ip6_with_ip4() {
    let server = multiaddr!(Ip6(Ipv6Addr::LOCALHOST), Tcp(10000u16));
    let result = replace_ip_addr(&server, Protocol::Ip4([80, 81, 82, 83].into())).unwrap();
    assert_eq!(result, multiaddr!(Ip4([80, 81, 82, 83]), Tcp(10000u16)))
}

#[test]
fn replace_ip4_with_ip6() {
    let server = multiaddr!(Ip4(Ipv4Addr::LOCALHOST), Tcp(10000u16));
    let result = replace_ip_addr(&server, "2001:db8::1".parse::<Ipv6Addr>().unwrap().into());
    assert_eq!(result.unwrap(), "/ip6/2001:db8::1/tcp/10000".parse::<Multiaddr>().unwrap())
}

#[test]
fn unknown_protocol_string() {
    match "/unknown/1.2.3.4".parse::<Multiaddr>() {
        Ok(_) => assert!(false, "The UnknownProtocolString error should be caused"),
        Err(e) => match e {
            crate::Error::UnknownProtocolString(protocol) => {
                assert_eq!(protocol, "unknown")
            },
            _ => assert!(false, "The UnknownProtocolString error should be caused")
        }
    }
}
