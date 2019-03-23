
use arrayref::array_ref;
use bs58;
use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};
use crate::{Result, Error};
use data_encoding::BASE32;
use multihash::Multihash;
use std::{
    borrow::Cow,
    convert::From,
    fmt,
    io::{Cursor, Write},
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    str::{self, FromStr}
};
use unsigned_varint::{encode, decode};

const BLUETOOTH: u32 = 802;     // TODO: link vs physical?
const DCCP: u32 = 33;
const DNS4: u32 = 54;
const DNS6: u32 = 55;
const HTTP: u32 = 480;
const HTTPS: u32 = 443;
const IP4: u32 = 4;
const IP6: u32 = 41;
const L2CAP: u32 = 803;
const P2P_WEBRTC_DIRECT: u32 = 276;
const P2P_WEBRTC_STAR: u32 = 275;
const P2P_WEBSOCKET_STAR: u32 = 479;
const MEMORY: u32 = 777;
const ONION: u32 = 444;
const P2P: u32 = 421;
const P2P_CIRCUIT: u32 = 290;
const RFCOMM: u32 = 804;
const QUIC: u32 = 460;
const SCTP: u32 = 132;
const TCP: u32 = 6;
const UDP: u32 = 273;
const UDT: u32 = 301;
const UNIX: u32 = 400;
const UTP: u32 = 302;
const WS: u32 = 477;
const WSS: u32 = 478;

/// `Protocol` describes all possible multiaddress protocols.
#[derive(PartialEq, Eq, Clone, Debug)]
pub enum Protocol<'a> {
    // TODO: document whether big endian or little endian
    Bluetooth([u8; 6]),
    Dccp(u16),
    Dns4(Cow<'a, str>),
    Dns6(Cow<'a, str>),
    Http,
    Https,
    Ip4(Ipv4Addr),
    Ip6(Ipv6Addr),
    L2cap(u16),
    P2pWebRtcDirect,
    P2pWebRtcStar,
    P2pWebSocketStar,
    /// Contains the "port" to contact. Similar to TCP or UDP, 0 means "assign me a port".
    Memory(u64),
    Onion(Cow<'a, [u8; 10]>, u16),
    P2p(Multihash),
    P2pCircuit,
    Quic,
    Rfcomm(u8),
    Sctp(u16),
    Tcp(u16),
    Udp(u16),
    Udt,
    /// For `Unix` we use `&str` instead of `Path` to allow cross-platform usage of
    /// `Protocol` since encoding `Paths` to bytes is platform-specific.
    /// This means that the actual validation of paths needs to happen separately.
    Unix(Cow<'a, str>),
    Utp,
    Ws,
    Wss
}

impl<'a> Protocol<'a> {
    /// Parse a protocol value from the given iterator of string slices.
    ///
    /// The parsing only consumes the minimum amount of string slices necessary to
    /// produce a well-formed protocol. The same iterator can thus be used to parse
    /// a sequence of protocols in succession. It is up to client code to check
    /// that iteration has finished whenever appropriate.
    pub fn from_str_parts<I>(mut iter: I) -> Result<Self>
    where
        I: Iterator<Item=&'a str>
    {
        match iter.next().ok_or(Error::InvalidProtocolString)? {
            "ip4" => {
                let s = iter.next().ok_or(Error::InvalidProtocolString)?;
                Ok(Protocol::Ip4(Ipv4Addr::from_str(s)?))
            }
            "tcp" => {
                let s = iter.next().ok_or(Error::InvalidProtocolString)?;
                Ok(Protocol::Tcp(s.parse()?))
            }
            "udp" => {
                let s = iter.next().ok_or(Error::InvalidProtocolString)?;
                Ok(Protocol::Udp(s.parse()?))
            }
            "dccp" => {
                let s = iter.next().ok_or(Error::InvalidProtocolString)?;
                Ok(Protocol::Dccp(s.parse()?))
            }
            "ip6" => {
                let s = iter.next().ok_or(Error::InvalidProtocolString)?;
                Ok(Protocol::Ip6(Ipv6Addr::from_str(s)?))
            }
            "dns4" => {
                let s = iter.next().ok_or(Error::InvalidProtocolString)?;
                Ok(Protocol::Dns4(Cow::Borrowed(s)))
            }
            "dns6" => {
                let s = iter.next().ok_or(Error::InvalidProtocolString)?;
                Ok(Protocol::Dns6(Cow::Borrowed(s)))
            }
            "sctp" => {
                let s = iter.next().ok_or(Error::InvalidProtocolString)?;
                Ok(Protocol::Sctp(s.parse()?))
            }
            "udt" => Ok(Protocol::Udt),
            "utp" => Ok(Protocol::Utp),
            "unix" => {
                let s = iter.next().ok_or(Error::InvalidProtocolString)?;
                Ok(Protocol::Unix(Cow::Borrowed(s)))
            }
            "p2p" => {
                let s = iter.next().ok_or(Error::InvalidProtocolString)?;
                let decoded = bs58::decode(s).into_vec()?;
                Ok(Protocol::P2p(Multihash::from_bytes(decoded)?))
            }
            "http" => Ok(Protocol::Http),
            "https" => Ok(Protocol::Https),
            "onion" =>
                iter.next()
                    .ok_or(Error::InvalidProtocolString)
                    .and_then(|s| read_onion(&s.to_uppercase()))
                    .map(|(a, p)| Protocol::Onion(Cow::Owned(a), p)),
            "quic" => Ok(Protocol::Quic),
            "ws" => Ok(Protocol::Ws),
            "wss" => Ok(Protocol::Wss),
            "p2p-websocket-star" => Ok(Protocol::P2pWebSocketStar),
            "p2p-webrtc-star" => Ok(Protocol::P2pWebRtcStar),
            "p2p-webrtc-direct" => Ok(Protocol::P2pWebRtcDirect),
            "p2p-circuit" => Ok(Protocol::P2pCircuit),
            "memory" => {
                let s = iter.next().ok_or(Error::InvalidProtocolString)?;
                Ok(Protocol::Memory(s.parse()?))
            }
            "bluetooth" => {
                let s = iter.next().ok_or(Error::InvalidProtocolString)?;
                let mut out = [0; 6];
                {
                    let mut bytes = s.split(':');
                    let mut out_iter = out.iter_mut();
                    for (byte, o) in bytes.by_ref().zip(out_iter.by_ref()) {
                        *o = u8::from_str_radix(byte, 16).map_err(|_| Error::InvalidProtocolString)?;
                    }
                    if out_iter.next().is_some() || bytes.next().is_some() {
                        return Err(Error::InvalidProtocolString)
                    }
                }
                Ok(Protocol::Bluetooth(out))
            }
            "l2cap" => {
                let s = iter.next().ok_or(Error::InvalidProtocolString)?;
                Ok(Protocol::L2cap(s.parse()?))
            }
            "rfcomm" => {
                let s = iter.next().ok_or(Error::InvalidProtocolString)?;
                Ok(Protocol::Rfcomm(s.parse()?))
            }
            _ => Err(Error::UnknownProtocolString)
        }
    }

    /// Parse a single `Protocol` value from its byte slice representation,
    /// returning the protocol as well as the remaining byte slice.
    pub fn from_bytes(input: &'a [u8]) -> Result<(Self, &'a [u8])> {
        fn split_at(n: usize, input: &[u8]) -> Result<(&[u8], &[u8])> {
            if input.len() < n {
                return Err(Error::DataLessThanLen)
            }
            Ok(input.split_at(n))
        }
        let (id, input) = decode::u32(input)?;
        match id {
            BLUETOOTH => {
                let (data, rest) = split_at(6, input)?;
                Ok((Protocol::Bluetooth([data[0], data[1], data[2], data[3], data[4], data[5]]), rest))
            }
            DCCP => {
                let (data, rest) = split_at(2, input)?;
                let mut rdr = Cursor::new(data);
                let num = rdr.read_u16::<BigEndian>()?;
                Ok((Protocol::Dccp(num), rest))
            }
            DNS4 => {
                let (n, input) = decode::usize(input)?;
                let (data, rest) = split_at(n, input)?;
                Ok((Protocol::Dns4(Cow::Borrowed(str::from_utf8(data)?)), rest))
            }
            DNS6 => {
                let (n, input) = decode::usize(input)?;
                let (data, rest) = split_at(n, input)?;
                Ok((Protocol::Dns6(Cow::Borrowed(str::from_utf8(data)?)), rest))
            }
            HTTP => Ok((Protocol::Http, input)),
            HTTPS => Ok((Protocol::Https, input)),
            IP4 => {
                let (data, rest) = split_at(4, input)?;
                Ok((Protocol::Ip4(Ipv4Addr::new(data[0], data[1], data[2], data[3])), rest))
            }
            IP6 => {
                let (data, rest) = split_at(16, input)?;
                let mut rdr = Cursor::new(data);
                let mut seg = [0_u16; 8];

                for x in seg.iter_mut() {
                    *x = rdr.read_u16::<BigEndian>()?;
                }

                let addr = Ipv6Addr::new(seg[0],
                                         seg[1],
                                         seg[2],
                                         seg[3],
                                         seg[4],
                                         seg[5],
                                         seg[6],
                                         seg[7]);

                Ok((Protocol::Ip6(addr), rest))
            }
            L2CAP => {
                let (data, rest) = split_at(2, input)?;
                let mut rdr = Cursor::new(data);
                let num = rdr.read_u16::<BigEndian>()?;
                Ok((Protocol::L2cap(num), rest))
            }
            P2P_WEBRTC_DIRECT => Ok((Protocol::P2pWebRtcDirect, input)),
            P2P_WEBRTC_STAR => Ok((Protocol::P2pWebRtcStar, input)),
            P2P_WEBSOCKET_STAR => Ok((Protocol::P2pWebSocketStar, input)),
            MEMORY => {
                let (data, rest) = split_at(8, input)?;
                let mut rdr = Cursor::new(data);
                let num = rdr.read_u64::<BigEndian>()?;
                Ok((Protocol::Memory(num), rest))
            }
            ONION => {
                let (data, rest) = split_at(12, input)?;
                let port = BigEndian::read_u16(&data[10 ..]);
                Ok((Protocol::Onion(Cow::Borrowed(array_ref!(data, 0, 10)), port), rest))
            }
            P2P => {
                let (n, input) = decode::usize(input)?;
                let (data, rest) = split_at(n, input)?;
                Ok((Protocol::P2p(Multihash::from_bytes(data.to_owned())?), rest))
            }
            P2P_CIRCUIT => Ok((Protocol::P2pCircuit, input)),
            QUIC => Ok((Protocol::Quic, input)),
            RFCOMM => {
                let (data, rest) = split_at(1, input)?;
                let mut rdr = Cursor::new(data);
                Ok((Protocol::Rfcomm(data[0]), rest))
            }
            SCTP => {
                let (data, rest) = split_at(2, input)?;
                let mut rdr = Cursor::new(data);
                let num = rdr.read_u16::<BigEndian>()?;
                Ok((Protocol::Sctp(num), rest))
            }
            TCP => {
                let (data, rest) = split_at(2, input)?;
                let mut rdr = Cursor::new(data);
                let num = rdr.read_u16::<BigEndian>()?;
                Ok((Protocol::Tcp(num), rest))
            }
            UDP => {
                let (data, rest) = split_at(2, input)?;
                let mut rdr = Cursor::new(data);
                let num = rdr.read_u16::<BigEndian>()?;
                Ok((Protocol::Udp(num), rest))
            }
            UDT => Ok((Protocol::Udt, input)),
            UNIX => {
                let (n, input) = decode::usize(input)?;
                let (data, rest) = split_at(n, input)?;
                Ok((Protocol::Unix(Cow::Borrowed(str::from_utf8(data)?)), rest))
            }
            UTP => Ok((Protocol::Utp, input)),
            WS => Ok((Protocol::Ws, input)),
            WSS => Ok((Protocol::Wss, input)),
            _ => Err(Error::UnknownProtocolId(id))
        }
    }

    /// Encode this protocol by writing its binary representation into
    /// the given `Write` impl.
    pub fn write_bytes<W: Write>(&self, w: &mut W) -> Result<()> {
        let mut buf = encode::u32_buffer();
        match self {
            Protocol::Ip4(addr) => {
                w.write_all(encode::u32(IP4, &mut buf))?;
                w.write_all(&addr.octets())?
            }
            Protocol::Ip6(addr) => {
                w.write_all(encode::u32(IP6, &mut buf))?;
                for &segment in &addr.segments() {
                    w.write_u16::<BigEndian>(segment)?
                }
            }
            Protocol::Tcp(port) => {
                w.write_all(encode::u32(TCP, &mut buf))?;
                w.write_u16::<BigEndian>(*port)?
            }
            Protocol::Udp(port) => {
                w.write_all(encode::u32(UDP, &mut buf))?;
                w.write_u16::<BigEndian>(*port)?
            }
            Protocol::Dccp(port) => {
                w.write_all(encode::u32(DCCP, &mut buf))?;
                w.write_u16::<BigEndian>(*port)?
            }
            Protocol::Sctp(port) => {
                w.write_all(encode::u32(SCTP, &mut buf))?;
                w.write_u16::<BigEndian>(*port)?
            }
            Protocol::Dns4(s) => {
                w.write_all(encode::u32(DNS4, &mut buf))?;
                let bytes = s.as_bytes();
                w.write_all(encode::usize(bytes.len(), &mut encode::usize_buffer()))?;
                w.write_all(&bytes)?
            }
            Protocol::Dns6(s) => {
                w.write_all(encode::u32(DNS6, &mut buf))?;
                let bytes = s.as_bytes();
                w.write_all(encode::usize(bytes.len(), &mut encode::usize_buffer()))?;
                w.write_all(&bytes)?
            }
            Protocol::Unix(s) => {
                w.write_all(encode::u32(UNIX, &mut buf))?;
                let bytes = s.as_bytes();
                w.write_all(encode::usize(bytes.len(), &mut encode::usize_buffer()))?;
                w.write_all(&bytes)?
            }
            Protocol::P2p(multihash) => {
                w.write_all(encode::u32(P2P, &mut buf))?;
                let bytes = multihash.as_bytes();
                w.write_all(encode::usize(bytes.len(), &mut encode::usize_buffer()))?;
                w.write_all(&bytes)?
            }
            Protocol::Onion(addr, port) => {
                w.write_all(encode::u32(ONION, &mut buf))?;
                w.write_all(addr.as_ref())?;
                w.write_u16::<BigEndian>(*port)?
            }
            Protocol::Quic => w.write_all(encode::u32(QUIC, &mut buf))?,
            Protocol::Utp => w.write_all(encode::u32(UTP, &mut buf))?,
            Protocol::Udt => w.write_all(encode::u32(UDT, &mut buf))?,
            Protocol::Http => w.write_all(encode::u32(HTTP, &mut buf))?,
            Protocol::Https => w.write_all(encode::u32(HTTPS, &mut buf))?,
            Protocol::Ws => w.write_all(encode::u32(WS, &mut buf))?,
            Protocol::Wss => w.write_all(encode::u32(WSS, &mut buf))?,
            Protocol::P2pWebSocketStar => w.write_all(encode::u32(P2P_WEBSOCKET_STAR, &mut buf))?,
            Protocol::P2pWebRtcStar => w.write_all(encode::u32(P2P_WEBRTC_STAR, &mut buf))?,
            Protocol::P2pWebRtcDirect => w.write_all(encode::u32(P2P_WEBRTC_DIRECT, &mut buf))?,
            Protocol::P2pCircuit => w.write_all(encode::u32(P2P_CIRCUIT, &mut buf))?,
            Protocol::Bluetooth(addr) => {
                w.write_all(encode::u32(BLUETOOTH, &mut buf))?;
                w.write_all(&addr[..])?
            }
            Protocol::L2cap(port) => {
                w.write_all(encode::u32(L2CAP, &mut buf))?;
                w.write_u16::<BigEndian>(*port)?
            }
            Protocol::Rfcomm(port) => {
                w.write_all(encode::u32(RFCOMM, &mut buf))?;
                w.write_all(&[*port])?
            }
            Protocol::Memory(port) => {
                w.write_all(encode::u32(MEMORY, &mut buf))?;
                w.write_u64::<BigEndian>(*port)?
            }
        }
        Ok(())
    }

    /// Turn this `Protocol` into one that owns its data, thus being valid for any lifetime.
    pub fn acquire<'b>(self) -> Protocol<'b> {
        use self::Protocol::*;
        match self {
            Bluetooth(a) => Bluetooth(a),
            Dccp(a) => Dccp(a),
            Dns4(cow) => Dns4(Cow::Owned(cow.into_owned())),
            Dns6(cow) => Dns6(Cow::Owned(cow.into_owned())),
            Http => Http,
            Https => Https,
            Ip4(a) => Ip4(a),
            Ip6(a) => Ip6(a),
            L2cap(a) => L2cap(a),
            P2pWebRtcDirect => P2pWebRtcDirect,
            P2pWebRtcStar => P2pWebRtcStar,
            P2pWebSocketStar => P2pWebSocketStar,
            Memory(a) => Memory(a),
            Onion(addr, port) => Onion(Cow::Owned(addr.into_owned()), port),
            P2p(a) => P2p(a),
            P2pCircuit => P2pCircuit,
            Quic => Quic,
            Rfcomm(a) => Rfcomm(a),
            Sctp(a) => Sctp(a),
            Tcp(a) => Tcp(a),
            Udp(a) => Udp(a),
            Udt => Udt,
            Unix(cow) => Unix(Cow::Owned(cow.into_owned())),
            Utp => Utp,
            Ws => Ws,
            Wss => Wss
        }
    }
}

impl<'a> fmt::Display for Protocol<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use self::Protocol::*;
        match self {
            Bluetooth(addr) => write!(f, "/bluetooth/{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                addr[0], addr[1], addr[2], addr[3], addr[4], addr[5]),
            Dccp(port) => write!(f, "/dccp/{}", port),
            Dns4(s) => write!(f, "/dns4/{}", s),
            Dns6(s) => write!(f, "/dns6/{}", s),
            Http => f.write_str("/http"),
            Https => f.write_str("/https"),
            Ip4(addr) => write!(f, "/ip4/{}", addr),
            Ip6(addr) => write!(f, "/ip6/{}", addr),
            L2cap(port) => write!(f, "/l2cap/{}", port),
            P2pWebRtcDirect => f.write_str("/p2p-webrtc-direct"),
            P2pWebRtcStar => f.write_str("/p2p-webrtc-star"),
            P2pWebSocketStar => f.write_str("/p2p-websocket-star"),
            Memory(port) => write!(f, "/memory/{}", port),
            Onion(addr, port) => {
                let s = BASE32.encode(addr.as_ref());
                write!(f, "/onion/{}:{}", s.to_lowercase(), port)
            }
            P2p(c) => write!(f, "/p2p/{}", bs58::encode(c.as_bytes()).into_string()),
            P2pCircuit => f.write_str("/p2p-circuit"),
            Quic => f.write_str("/quic"),
            Rfcomm(port) => write!(f, "/rfcomm/{}", port),
            Sctp(port) => write!(f, "/sctp/{}", port),
            Tcp(port) => write!(f, "/tcp/{}", port),
            Udp(port) => write!(f, "/udp/{}", port),
            Udt => f.write_str("/udt"),
            Unix(s) => write!(f, "/unix/{}", s),
            Utp => f.write_str("/utp"),
            Ws => f.write_str("/ws"),
            Wss => f.write_str("/wss"),
        }
    }
}

impl<'a> From<IpAddr> for Protocol<'a> {
    #[inline]
    fn from(addr: IpAddr) -> Self {
        match addr {
            IpAddr::V4(addr) => Protocol::Ip4(addr),
            IpAddr::V6(addr) => Protocol::Ip6(addr),
        }
    }
}

impl<'a> From<Ipv4Addr> for Protocol<'a> {
    #[inline]
    fn from(addr: Ipv4Addr) -> Self {
        Protocol::Ip4(addr)
    }
}

impl<'a> From<Ipv6Addr> for Protocol<'a> {
    #[inline]
    fn from(addr: Ipv6Addr) -> Self {
        Protocol::Ip6(addr)
    }
}

// Parse a version 2 onion address and return its binary representation.
//
// Format: <base-32 address> ":" <port number>
fn read_onion(s: &str) -> Result<([u8; 10], u16)> {
    let mut parts = s.split(':');

    // address part (without ".onion")
    let b32 = parts.next().ok_or(Error::InvalidMultiaddr)?;
    if b32.len() != 16 {
        return Err(Error::InvalidMultiaddr)
    }

    // port number
    let port = parts.next()
        .ok_or(Error::InvalidMultiaddr)
        .and_then(|p| str::parse(p).map_err(From::from))?;

    // nothing else expected
    if parts.next().is_some() {
        return Err(Error::InvalidMultiaddr)
    }

    if 10 != BASE32.decode_len(b32.len()).map_err(|_| Error::InvalidMultiaddr)? {
        return Err(Error::InvalidMultiaddr)
    }

    let mut buf = [0u8; 10];
    BASE32.decode_mut(b32.as_bytes(), &mut buf).map_err(|_| Error::InvalidMultiaddr)?;

    Ok((buf, port))
}

