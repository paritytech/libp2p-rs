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

use crate::{LIBP2P_UUID, LIBP2P_PEER_ID_ATTRIB};
use lazy_static::lazy_static;
use libp2p_core::PeerId;
use std::{io, iter, mem};

/// Register a profile to BlueZ so that it gets added to the local SDP server.
// TODO: should return a future
pub fn register_libp2p_profile(peer_id: &PeerId, rfcomm_port: u8) -> Result<Registration, io::Error> {
    let interface_path = b"/io/libp2p".to_vec();

    let dict = {
        let key = "ServiceRecord".to_string();
        let val = dbus::arg::Variant(generate_xml(peer_id, rfcomm_port));     // TODO: correct values
        dbus::arg::Dict::<String, dbus::arg::Variant<String>, _>::new(iter::once((key, val)))
    };

    let connection = dbus::Connection::get_private(dbus::BusType::System).unwrap(); // TODO: don't unwrap
    let msg = dbus::Message::new_method_call("org.bluez", "/org/bluez", "org.bluez.ProfileManager1", "RegisterProfile")
        .map_err(|s| io::Error::new(io::ErrorKind::Other, s))?
        .append3(dbus::Path::from_slice(&interface_path[..]).unwrap(), UUID_STRING.clone(), dict);

    // TODO: confirm with answer
    connection.send(msg).unwrap();      // TODO: don't unwrap

    Ok(Registration {
        connection,
        interface_path,
    })
}

pub struct Registration {
    connection: dbus::Connection,
    interface_path: Vec<u8>,
}

// TODO: https://github.com/diwic/dbus-rs/issues/137
unsafe impl Send for Registration {}

impl Drop for Registration {
    fn drop(&mut self) {
        // TODO: are we supposed to watch for AddMatch/RemoveMatch messages and only send to the
        //       appropriate recipients?
        // Send an `InterfacesRemoved` signal to unregister the GATT.
        let msg = dbus::Message::new_signal(
            mem::replace(&mut self.interface_path, Vec::new()),
            "org.freedesktop.DBus.ObjectManager",
            "InterfacesRemoved"
        ).expect("message is always valid; QED");
        let _ = self.connection.send(msg).unwrap();
    }
}

fn generate_xml(peer_id: &PeerId, rfcomm_port: u8) -> String {
    format!(r#"
<?xml version="1.0" encoding="UTF-8" ?>

<record>
    <attribute id="0x001">
        <sequence>
            <uuid value="{uuid_string}" />
        </sequence>
    </attribute>
    <attribute id="0x0004">
        <sequence>
            <sequence>
                <uuid value="0x0100" />
            </sequence>
            <sequence>
                <uuid value="0x0003" />
                <uint8 value="0x{rfcomm_port:x}" />
            </sequence>
        </sequence>
    </attribute>
    <attribute id="0x000A">
        <url value="https://libp2p.io" />
    </attribute>
    <attribute id="0x000C">
        <url value="https://libp2p.io/img/favicon.png" />
    </attribute>
    <attribute id="0x0100">
        <text value="Libp2p" />
    </attribute>
    <attribute id="0x0101">
        <text value="Libp2p entry point" />
    </attribute>
    <attribute id="0x{peer_id_attr:x}">
        <text value="{peer_id}" />
    </attribute>
</record>
"#,
    uuid_string = &*UUID_STRING,
    rfcomm_port = rfcomm_port,
    peer_id_attr = LIBP2P_PEER_ID_ATTRIB,
    peer_id = peer_id.to_base58())
}

lazy_static!{
    static ref UUID_STRING: String = {
        let num = LIBP2P_UUID.to_u128();
        format!("{:x}-{:x}-{:x}-{:x}-{:x}",
            (num >> 96) & 0xffff_ffff,
            (num >> 80) & 0xffff,
            (num >> 64) & 0xffff,
            (num >> 48) & 0xffff,
            num & 0xffff_ffff_ffff,
        )
    };
}

#[cfg(test)]
mod tests {
    use super::register_libp2p_profile;
    use libp2p_core::PeerId;
    use std::{thread, time::Duration};

    #[test]
    fn register_libp2p_profile_working() {
        let _reg = register_libp2p_profile(&PeerId::random(), 1).unwrap();
        thread::sleep(Duration::from_millis(5000));     // TODO: smaller
    }
}
