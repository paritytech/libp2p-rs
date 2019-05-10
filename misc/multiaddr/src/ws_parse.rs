use crate::{Multiaddr, Protocol};
use std::{error, fmt, iter, net::IpAddr};

/// Attempts to parse a WebSockets URL into a multiaddress.
///
/// # Example
///
/// ```
/// let addr = parity_multiaddr::from_websockets_url("ws://127.0.0.1:8080/").unwrap();
/// assert_eq!(addr, "/ip4/127.0.0.1/tcp/8080/ws".parse().unwrap());
/// ```
///
pub fn from_websockets_url(url: &str) -> std::result::Result<Multiaddr, FromWsParseErr> {
    let url = urlparse::urlparse(url);

    let is_wss = match url.scheme.as_str() {
        "ws" => false,
        "wss" => true,
        _ => return Err(FromWsParseErr::WrongScheme)
    };

    let port = Protocol::Tcp(url.port.unwrap_or_else(|| if is_wss { 443 } else { 80 }));
    let path = if is_wss { Protocol::Wss } else { Protocol::Ws };
    let ip = if let Some(hostname) = url.hostname.as_ref() {
        if let Ok(ip) = hostname.parse::<IpAddr>() {
            Protocol::from(ip)
        } else {
            Protocol::Dns4(url.netloc.into())
        }
    } else {
        Protocol::Dns4(url.netloc.into())
    };

    Ok(iter::once(ip)
        .chain(iter::once(port))
        .chain(iter::once(path))
        .collect())
}

/// Error while parsing a WebSockets URL.
#[derive(Debug)]
pub enum FromWsParseErr {
    /// The URL scheme was not recognized.
    WrongScheme,
}

impl fmt::Display for FromWsParseErr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FromWsParseErr::WrongScheme => write!(f, "Unrecognized URL scheme"),
        }
    }
}

impl error::Error for FromWsParseErr {
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_garbage_doesnt_panic() {
        for _ in 0 .. 50 {
            let url = (0..16).map(|_| rand::random::<u8>()).collect::<Vec<_>>();
            let url = String::from_utf8_lossy(&url);
            assert!(from_websockets_url(&url).is_err());
        }
    }

    #[test]
    fn normal_usage_ws() {
        let addr = from_websockets_url("ws://127.0.0.1:8000").unwrap();
        assert_eq!(addr, "/ip4/127.0.0.1/tcp/8000/ws".parse().unwrap());
    }

    #[test]
    fn normal_usage_wss() {
        let addr = from_websockets_url("wss://127.0.0.1:8000").unwrap();
        assert_eq!(addr, "/ip4/127.0.0.1/tcp/8000/wss".parse().unwrap());
    }

    #[test]
    fn default_ws_port() {
        let addr = from_websockets_url("ws://127.0.0.1").unwrap();
        assert_eq!(addr, "/ip4/127.0.0.1/tcp/80/ws".parse().unwrap());
    }

    #[test]
    fn default_wss_port() {
        let addr = from_websockets_url("wss://127.0.0.1").unwrap();
        assert_eq!(addr, "/ip4/127.0.0.1/tcp/443/wss".parse().unwrap());
    }

    #[test]
    fn dns_addr_ws() {
        let addr = from_websockets_url("ws://example.com").unwrap();
        assert_eq!(addr, "/dns4/example.com/tcp/80/ws".parse().unwrap());
    }

    #[test]
    fn dns_addr_wss() {
        let addr = from_websockets_url("wss://example.com").unwrap();
        assert_eq!(addr, "/dns4/example.com/tcp/443/wss".parse().unwrap());
    }

    #[test]
    fn bad_hostname() {
        let addr = from_websockets_url("wss://127.0.0.1x").unwrap();
        assert_eq!(addr, "/dns4/127.0.0.1x/tcp/443/wss".parse().unwrap());
    }

    #[test]
    fn wrong_scheme() {
        match from_websockets_url("foo://127.0.0.1") {
            Err(FromWsParseErr::WrongScheme) => {}
            _ => panic!()
        }
    }
}
