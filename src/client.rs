use std::iter;
use std::net::TcpStream;
use std::io::{Read, Write};
use std::net::ToSocketAddrs;
use std::collections::HashMap;

use parser::Parser;
use protocol::Header;

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
        let req = &[
            0x03, // version
            0x00, // flags
            0x00, // stream
            0x00, // stream
            0x05, // opcode
            0x00, // length
            0x00, // length
            0x00, // length
            0x00, // length
        ];
        self.conn.write(req).unwrap();

        let header = self.read_header();
        self.read_string_multimap(header.length as usize)
    }

    fn read_header(&mut self) -> Header {
        Parser::new(self.read_bytes(9)).parse_header()
    }

    fn read_string_multimap(&mut self, size: usize) -> HashMap<String, Vec<String>> {
        let mut parser = Parser::new(self.read_bytes(size));
        let mut map = HashMap::new();

        let key_count = parser.parse_u16();
        for _ in 0..key_count {
            let key = parser.parse_string();
            let val_count = parser.parse_u16();
            let mut vec = Vec::with_capacity(val_count as usize);
            for _ in 0..val_count {
                vec.push(parser.parse_string());
            }
            map.insert(key, vec);
        }
        assert!(parser.iter.next().is_none());
        map
    }

    fn read_bytes(&mut self, n: usize) -> Vec<u8> {
        let mut vec = Vec::with_capacity(n);
        vec.extend(iter::repeat(0).take(n));
        let bytes_read = self.conn.read(&mut vec[..]).unwrap();
        assert_eq!(bytes_read, n);
        vec
    }
}
