#![no_main]
use futures::executor::block_on;
use futures::io::Cursor;
use futures::io::{AsyncRead, AsyncWrite};
use libfuzzer_sys::fuzz_target;
use multistream_select::{listener_select_proto, dialer_select_proto, Version};
use std::io::Error;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct Connection {
    read: Cursor<Vec<u8>>,
    write: Cursor<Vec<u8>>,
}

impl Connection {
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            read: Cursor::new(data),
            write: Cursor::new(Vec::new()),
        }
    }
}

impl AsyncWrite for Connection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        Pin::new(&mut self.write).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.write).poll_flush(cx)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.write).poll_close(cx)
    }
}

impl AsyncRead for Connection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Error>> {
        Pin::new(&mut self.read).poll_read(cx, buf)
    }
}
