use std::iter;
use std::net::TcpStream;
use std::io::{Read, Cursor};
use std::net::ToSocketAddrs;
use std::collections::HashMap;

use protocol::{WireType, Header, Version, Flags, Opcode, StringMultiMap};

pub struct Client {
    conn: TcpStream,
}

impl Client {
    pub fn new<A: ToSocketAddrs>(addr: A) -> Client {
        Client {
            conn: TcpStream::connect(addr).unwrap(),
        }
    }

    pub fn get_options(&mut self) -> HashMap<String, Vec<String>> {
        let req = Header {
            version: Version::Request,
            flags: Flags { compression: false, tracing: false },
            stream: 0,
            opcode: Opcode::Options,
            length: 0,
        };
        req.encode(&mut self.conn);

        let header = Header::decode(&mut self.conn);
        let mut body = Cursor::new(self.read_bytes(header.length as usize));
        StringMultiMap::decode(&mut body)
    }

    fn read_bytes(&mut self, n: usize) -> Vec<u8> {
        let mut vec = Vec::with_capacity(n);
        vec.extend(iter::repeat(0).take(n));
        let bytes_read = self.conn.read(&mut vec[..]).unwrap();
        assert_eq!(bytes_read, n);
        vec
    }
}
