use std::io::prelude::*;
use std::net::TcpStream;

mod parser;

fn main() {
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

    let mut stream = TcpStream::connect("127.0.0.1:9042").unwrap();

    let bytes_written = stream.write(req).unwrap();
    println!("wrote {} bytes", bytes_written);

    let byte_vec = read_frame(&mut stream);

    let mut parser = parser::Parser::new(byte_vec);

    let header = parser.parse_header();
    println!("{:?}", header);

    let (lower, upper) = parser.iter.size_hint();
    println!("[{}, {}) bytes remaining", lower, upper.unwrap_or(0));

    // if opcode == SUPPORTED, parse_string_multimap
    let key_count = parser.parse_u16();
    println!("{} keys in multimap", key_count);

    for i in 0..key_count {
        let key = parser.parse_string();
        println!("key {}: {}", i, key);
        let val_count = parser.parse_u16();
        println!("{} vals:", val_count);
        for n in 0..val_count {
            let val = parser.parse_string();
            println!("  {}: {}", n, val);
        }
    }
    assert!(parser.iter.next().is_none());
}

fn read_frame(stream: &mut TcpStream) -> Vec<u8> {
    const BUF_SIZE : usize = 1024;
    let mut byte_vec = Vec::new();
    let mut buf = [0; BUF_SIZE];
    loop {
        let bytes_read = stream.read(&mut buf).unwrap();
        for i in 0..bytes_read {
            byte_vec.push(buf[i]);
        }
        if bytes_read < BUF_SIZE { break; }
    }
    byte_vec
}
