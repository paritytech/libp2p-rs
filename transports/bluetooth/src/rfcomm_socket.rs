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

/// Non-blocking socket for RFCOMM communication with a remote.
pub struct RfcommSocket {
    socket: c_int,
}

impl RfcommSocket {
    /// Initializes a new socket by calling `libc::socket`.
    fn new() -> Result<RfcommSocket, io::Error> {
        let socket = unsafe {
            libc::socket(
                libc::AF_BLUETOOTH,
                libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
                ffi::BTPROTO_RFCOMM
            )
        };

        if socket == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(RfcommSocket {
            socket,
        })
    }

    /// Initializes a new socket by connecting to a remote.
    pub fn connect(dest: Addr, port: u8) -> Result<RfcommSocket, io::Error> {
        unsafe {
            let me = Self::new()?;

            let params = ffi::sockaddr_rc {
                rc_family: libc::AF_BLUETOOTH as u16,
                rc_bdaddr: ffi::bdaddr_t { b: dest.to_little_endian() },
                rc_channel: port,
            };

            let status = libc::connect(
                me.socket,
                &params as *const ffi::sockaddr_rc as *const _,
                mem::size_of_val(&params) as u32
            );

            if status == -1 {
                let err = io::Error::last_os_error();
                println!("dial err: {:?}", err);
                // TODO: handle?
                /*if err.kind() != io::ErrorKind::WouldBlock {
                    libc::close(socket);
                    return Err(io::Error::last_os_error());
                }*/
            }

            Ok(me)
        }
    }

    /// Initializes a new socket by binding and listening to a port.
    pub fn bind(dest: Addr, port: u8) -> Result<RfcommSocket, io::Error> {
        unsafe {
            let me = Self::new()?;

            let params = ffi::sockaddr_rc {
                rc_family: libc::AF_BLUETOOTH as u16,
                rc_bdaddr: ffi::bdaddr_t { b: dest.to_little_endian() },
                rc_channel: port,
            };

            let status = libc::bind(
                me.socket,
                &params as *const ffi::sockaddr_rc as *const _,
                mem::size_of_val(&params) as u32
            );

            if status == -1 {
                return Err(io::Error::last_os_error());
            }

            let status = libc::listen(me.socket, 8);       // TODO: allow configuring this 8
            if status == -1 {
                return Err(io::Error::last_os_error());
            }

            Ok(me)
        }
    }

    /// Calls `accept` on the socket.
    pub fn accept(&self) -> Result<(RfcommSocket, Addr), io::Error> {
        unsafe {
            let mut out_addr: ffi::sockaddr_rc = mem::zeroed();
            let client = libc::accept4(
                self.socket,
                &mut out_addr as *mut _ as *mut _,
                &mut mem::size_of_val(&out_addr) as *mut _ as *mut _,
                libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC
            );

            if client == -1 {
                return Err(io::Error::last_os_error());
            }

            let addr = Addr::from_little_endian(out_addr.rc_bdaddr.b);
            let client = RfcommSocket {
                socket: client,
            };

            Ok((client, addr))
        }
    }

    /// Calls `getsockname` on the socket.
    pub fn getsockname(&self) -> Result<(Addr, u8), io::Error> {
        unsafe {
            let mut out_addr: ffi::sockaddr_rc = mem::zeroed();
            if libc::getsockname(
                self.socket,
                &mut out_addr as *mut _ as *mut _,
                &mut mem::size_of_val(&out_addr) as *mut _ as *mut _
            ) != 0 {
                return Err(io::Error::last_os_error());
            }

            let addr = Addr::from_little_endian(out_addr.rc_bdaddr.b);
            Ok((addr, out_addr.rc_channel))
        }
    }
}

impl io::Read for RfcommSocket {
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

impl io::Write for RfcommSocket {
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

impl mio::Evented for RfcommSocket {
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

impl Drop for RfcommSocket {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.socket);
        }
    }
}
