#![no_main]
use futures::executor::block_on;
use futures::io::Cursor;
use futures::io::{AsyncRead, AsyncWrite};
use libfuzzer_sys::fuzz_target;
use multistream_select::{listener_select_proto, dialer_select_proto, Version};
use std::io::Error;
use std::pin::Pin;
use std::task::{Context, Poll};

mod utils;

fuzz_target!(|input: DialerInput| {
    block_on(async {
        let _ = dialer_select_proto(utils::Connection::new(input.read), input.protocols.into_iter(), input.version).await;
    })
});

#[derive(arbitrary::Arbitrary, Debug)]
struct DialerInput {
    read: Vec<u8>,
    protocols: Vec<Vec<u8>>,
    version: Version,
}