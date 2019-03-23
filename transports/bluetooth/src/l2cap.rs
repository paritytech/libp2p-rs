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

use super::ffi;
use crate::Addr;
use std::{io, mem, os::raw::c_int, os::raw::c_void};

/// Non-blocking socket for L2CAP communication with a remote.
pub struct L2capSocket {
    socket: c_int,
}

impl L2capSocket {
    fn new() -> Result<L2capSocket, io::Error> {
        let socket = unsafe {
            libc::socket(
                libc::AF_BLUETOOTH,
                libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
                ffi::BTPROTO_L2CAP
            )
        };

        if socket == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(L2capSocket {
            socket,
        })
    }

    pub fn connect(dest: Addr, port: u16) -> Result<L2capSocket, io::Error> {
        unsafe {
            let me = Self::new()?;

            let params = ffi::sockaddr_l2 {
                l2_family: libc::AF_BLUETOOTH as u16,
                l2_bdaddr: ffi::bdaddr_t { b: dest.to_little_endian() },
                l2_psm: port,
                l2_bdaddr_type: 0,  // BDADDR_BREDR constant
                l2_cid: 0,
            };

            let status = libc::connect(
                me.socket,
                &params as *const ffi::sockaddr_l2 as *const _,
                mem::size_of_val(&params) as u32
            );

            if status == -1 {
                let err = io::Error::last_os_error();
                // TODO: handle that
                /*if err.kind() != io::ErrorKind::WouldBlock {
                    return Err(io::Error::last_os_error());
                }*/
            }

            Ok(me)
        }
    }
}

impl io::Read for L2capSocket {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        unsafe {
            let ret = libc::read(self.socket, buf.as_mut_ptr() as *mut c_void, buf.len());
            if ret == -1 {
                return Err(io::Error::last_os_error())
            }
            Ok(ret as usize)
        }
    }
}

impl io::Write for L2capSocket {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        unsafe {
            let ret = libc::write(self.socket, buf.as_ptr() as *mut c_void, buf.len());
            if ret == -1 {
                return Err(io::Error::last_os_error())
            }
            Ok(ret as usize)
        }
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        Ok(())
    }
}

impl mio::Evented for L2capSocket {
    fn register(&self, poll: &mio::Poll, token: mio::Token, interest: mio::Ready, opts: mio::PollOpt) -> io::Result<()> {
        mio::unix::EventedFd(&self.socket).register(poll, token, interest, opts)
    }

    fn reregister(&self, poll: &mio::Poll, token: mio::Token, interest: mio::Ready, opts: mio::PollOpt) -> io::Result<()> {
        mio::unix::EventedFd(&self.socket).reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &mio::Poll) -> std::io::Result<()> {
        mio::unix::EventedFd(&self.socket).deregister(poll)
    }
}

impl Drop for L2capSocket {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.socket);
        }
    }
}
