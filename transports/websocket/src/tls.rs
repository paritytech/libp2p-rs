// Copyright 2019 Parity Technologies (UK) Ltd.
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

use futures_rustls::{rustls, webpki, TlsConnector, TlsAcceptor};
use std::{fmt, io, sync::Arc};

/// TLS configuration.
#[derive(Clone)]
pub struct Config {
    pub(crate) client: TlsConnector,
    pub(crate) server: Option<TlsAcceptor>
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Config")
    }
}

/// Private key, DER-encoded ASN.1 in either PKCS#8 or PKCS#1 format.
#[derive(Clone)]
pub struct PrivateKey(rustls::PrivateKey);

impl PrivateKey {
    /// Assert the given bytes are DER-encoded ASN.1 in either PKCS#8 or PKCS#1 format.
    pub fn new(bytes: Vec<u8>) -> Self {
        PrivateKey(rustls::PrivateKey(bytes))
    }
}

/// Certificate, DER-encoded X.509 format.
#[derive(Debug, Clone)]
pub struct Certificate(rustls::Certificate);

impl Certificate {
    /// Assert the given bytes are in DER-encoded X.509 format.
    pub fn new(bytes: Vec<u8>) -> Self {
        Certificate(rustls::Certificate(bytes))
    }
}

impl Config {
    /// Create a new TLS configuration with the given server key and certificate chain.
    pub fn new<I>(key: PrivateKey, certs: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = Certificate>
    {
        let mut builder = Config::builder();
        builder.server(key, certs)?;
        Ok(builder.finish())
    }

    /// Create a client-only configuration.
    pub fn client() -> Self {
        Config {
            client: Arc::new(client_config()).into(),
            server: None
        }
    }

    /// Create a new TLS configuration builder.
    pub fn builder() -> Builder {
        Builder { client: client_config(), server: None }
    }
}

/// Setup the rustls client configuration.
fn client_config() -> rustls::ClientConfig {
    let mut client = rustls::ClientConfig::new();
    client.root_store.add_server_trust_anchors(&webpki_roots::TLS_SERVER_ROOTS);
    client
}

/// TLS configuration builder.
pub struct Builder {
    client: rustls::ClientConfig,
    server: Option<rustls::ServerConfig>
}

impl Builder {
    /// Set server key and certificate chain.
    pub fn server<I>(&mut self, key: PrivateKey, certs: I) -> Result<&mut Self, Error>
    where
        I: IntoIterator<Item = Certificate>
    {
        let mut server = rustls::ServerConfig::new(rustls::NoClientAuth::new());
        let certs = certs.into_iter().map(|c| c.0).collect();
        server.set_single_cert(certs, key.0).map_err(|e| Error::Tls(Box::new(e)))?;
        self.server = Some(server);
        Ok(self)
    }

    /// Add an additional trust anchor.
    pub fn add_trust(&mut self, cert: &Certificate) -> Result<&mut Self, Error> {
        self.client.root_store.add(&cert.0).map_err(|e| Error::Tls(Box::new(e)))?;
        Ok(self)
    }

    /// Finish configuration.
    pub fn finish(self) -> Config {
        Config {
            client: Arc::new(self.client).into(),
            server: self.server.map(|s| Arc::new(s).into())
        }
    }
}

pub(crate) fn dns_name_ref(name: &str) -> Result<webpki::DNSNameRef<'_>, Error> {
    webpki::DNSNameRef::try_from_ascii_str(name).map_err(|_| Error::InvalidDnsName(name.into()))
}

// Error //////////////////////////////////////////////////////////////////////////////////////////

/// TLS related errors.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// An underlying I/O error.
    Io(io::Error),
    /// Actual TLS error.
    Tls(Box<dyn std::error::Error + Send + Sync>),
    /// The DNS name was invalid.
    InvalidDnsName(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "i/o error: {}", e),
            Error::Tls(e) => write!(f, "tls error: {}", e),
            Error::InvalidDnsName(n) => write!(f, "invalid DNS name: {}", n),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Tls(e) => Some(&**e),
            Error::InvalidDnsName(_) => None
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}
