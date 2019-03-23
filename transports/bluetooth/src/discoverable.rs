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

use crate::{Addr, ffi};
use std::{io, ptr};

/// Makes the given local interface discoverable from the outside.
///
/// Without doing that, you will not receive any incoming connection.
///
/// Must be passed the address of a local interface.
pub fn enable_discoverable(local_addr: &Addr) -> Result<(), io::Error> {
    // TODO: take local_addr into account
    // TODO: quite bad, as we should revert to non-discoverable

    let connection = dbus::Connection::get_private(dbus::BusType::System).unwrap(); // TODO: don't unwrap
    let msg = dbus::Message::new_method_call("org.bluez", "/org/bluez/hci0", "org.freedesktop.DBus.Properties", "Set")
        .map_err(|s| io::Error::new(io::ErrorKind::Other, s))?
        .append3("org.bluez.Adapter1", "Discoverable", dbus::arg::Variant(true));

    // TODO: don't block
    let reply = connection.send_with_reply_and_block(msg, 5000).unwrap();
    Ok(())
}
