use std::io::{Read, Write};
use std::collections::HashMap;
use podio::{BigEndian, ReadPodExt, WritePodExt};

pub trait Encodable {
    fn encode<T: Write>(&self, buffer: &mut T);
}

pub trait Decodable {
    fn decode<T: Read>(buffer: &mut T) -> Self;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    version: Version,
    flags: Flags,
    stream: u16,
    opcode: Opcode,
    pub length: u32,
}

impl Encodable for Header {
    fn encode<T: Write>(&self, buffer: &mut T) {
        self.version.encode(buffer);
        self.flags.encode(buffer);
        buffer.write_u16::<BigEndian>(self.stream).unwrap();
        self.opcode.encode(buffer);
        buffer.write_u32::<BigEndian>(self.length).unwrap();
    }
}

impl Decodable for Header {
    fn decode<T: Read>(buffer: &mut T) -> Header {
        let header = Header {
            version: Version::decode(buffer),
            flags: Flags::decode(buffer),
            stream: buffer.read_u16::<BigEndian>().unwrap(),
            opcode: Opcode::decode(buffer),
            length: buffer.read_u32::<BigEndian>().unwrap(),
        };

        match header.opcode {
            Opcode::Error => {
                let code = buffer.read_u32::<BigEndian>().unwrap();
                let message = String::decode(buffer);
                panic!("Error 0x{:04X}: {}", code, message);
            },
            _ => header,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Version {
    Request,
    Response,
}

impl Encodable for Version {
    fn encode<T: Write>(&self, buffer: &mut T) {
        buffer.write_u8(match *self {
            Version::Request => 0x03,
            Version::Response => 0x83,
        }).unwrap();
    }
}

impl Decodable for Version {
    fn decode<T: Read>(buffer: &mut T) -> Version {
        let version = buffer.read_u8().unwrap();
        match version {
            0x03 => Version::Request,
            0x83 => Version::Response,
            _ => panic!("unknown version header: {:02x}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Flags {
    pub compression: bool,
    pub tracing: bool,
}

impl Flags {
    fn new() -> Flags {
        Flags { compression: false, tracing: false }
    }
}

impl Encodable for Flags {
    fn encode<T: Write>(&self, buffer: &mut T) {
        let compression = if self.compression { 0x01 } else { 0x00 };
        let tracing = if self.tracing { 0x02 } else { 0x00 };
        buffer.write_u8(compression | tracing).unwrap();
    }
}

impl Decodable for Flags {
    fn decode<T: Read>(buffer: &mut T) -> Flags {
        let flags = buffer.read_u8().unwrap();
        Flags {
            compression: (flags & 0x01) > 0,
            tracing: (flags & 0x02) > 0,
        }
    }
}

macro_rules! opcodes {
    ( $( $val:expr => $var:ident, )* ) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum Opcode {
            $(
                $var = $val,
             )*
        }

        impl Encodable for Opcode {
            fn encode<T: Write>(&self, buffer: &mut T) {
                let val = match *self {
                    $(
                        Opcode::$var => $val,
                     )*
                };
                buffer.write_u8(val).unwrap();
            }
        }

        impl Decodable for Opcode {
            fn decode<T: Read>(buffer: &mut T) -> Opcode {
                let opcode = buffer.read_u8().unwrap();
                match opcode {
                    $(
                        $val => Opcode::$var,
                     )*
                    _ => panic!("Unknown opcode: {:02x}", opcode),
                }
            }
        }
    }
}

opcodes!(
    0x00 => Error,
    0x01 => Startup,
    0x02 => Ready,
    0x03 => Authenticate,
    0x05 => Options,
    0x06 => Supported,
    0x07 => Query,
    0x08 => Result,
    0x09 => Prepare,
    0x0A => Execute,
    0x0B => Register,
    0x0C => Event,
    0x0D => Batch,
    0x0E => AuthChallenge,
    0x0F => AuthResponse,
    0x10 => AuthSuccess,
);

pub type StringMultiMap = HashMap<String, Vec<String>>;

impl Encodable for StringMultiMap {
    fn encode<T: Write>(&self, buffer: &mut T) {
        buffer.write_u16::<BigEndian>(self.len() as u16).unwrap();
        for (key, vals) in self.iter() {
            key.encode(buffer);
            buffer.write_u16::<BigEndian>(vals.len() as u16).unwrap();
            for val in vals {
                val.encode(buffer);
            }
        }
    }
}

impl Decodable for StringMultiMap {
    fn decode<T: Read>(buffer: &mut T) -> StringMultiMap {
        let mut map = HashMap::new();

        let key_count = buffer.read_u16::<BigEndian>().unwrap();
        for _ in 0..key_count {
            let key = String::decode(buffer);
            let val_count = buffer.read_u16::<BigEndian>().unwrap();
            let mut vec = Vec::with_capacity(val_count as usize);
            for _ in 0..val_count {
                vec.push(String::decode(buffer));
            }
            map.insert(key, vec);
        }
        map
    }
}

impl Encodable for String {
    fn encode<T: Write>(&self, buffer: &mut T) {
        buffer.write_u16::<BigEndian>(self.len() as u16).unwrap();
        buffer.write_all(self.clone().into_bytes().as_ref()).unwrap();
    }
}

impl Decodable for String {
    fn decode<T: Read>(buffer: &mut T) -> String {
        let len = buffer.read_u16::<BigEndian>().unwrap();
        let byte_vec = buffer.read_exact(len as usize).unwrap();
        String::from_utf8(byte_vec).unwrap()
    }
}

pub struct OptionsRequest {
    header: Header
}

impl OptionsRequest {
    pub fn new() -> OptionsRequest {
        OptionsRequest {
            header: Header {
                version: Version::Request,
                flags: Flags::new(),
                stream: 0,
                opcode: Opcode::Options,
                length: 0,
            }
        }
    }
}

impl Encodable for OptionsRequest {
    fn encode<T: Write>(&self, buffer: &mut T) {
        self.header.encode(buffer);
    }
}

type StringMap = HashMap<String, String>;

impl Encodable for StringMap {
    fn encode<T: Write>(&self, buffer: &mut T) {
        buffer.write_u16::<BigEndian>(self.len() as u16).unwrap();
        for (key, val) in self.iter() {
            key.encode(buffer);
            val.encode(buffer);
        }
    }
}

impl Decodable for StringMap {
    fn decode<T: Read>(buffer: &mut T) -> StringMap {
        let mut map = HashMap::new();

        let key_count = buffer.read_u16::<BigEndian>().unwrap();
        for _ in 0..key_count {
            let key = String::decode(buffer);
            let val = String::decode(buffer);
            map.insert(key, val);
        }
        map
    }
}

pub struct StartupRequest {
    header: Header,
    body: Vec<u8>,
}

impl StartupRequest {
    pub fn new(cql_version: &str) -> StartupRequest {
        let mut options = HashMap::new();
        options.insert("CQL_VERSION".into(), cql_version.to_string());
        let mut body = Vec::new();
        options.encode(&mut body);
        StartupRequest {
            header: Header {
                version: Version::Request,
                flags: Flags::new(),
                stream: 0,
                opcode: Opcode::Startup,
                length: body.len() as u32,
            },
            body: body,
        }
    }
}

impl Encodable for StartupRequest {
    fn encode<T: Write>(&self, buffer: &mut T) {
        self.header.encode(buffer);
        buffer.write(self.body.as_ref()).unwrap();
    }
}

pub struct QueryRequest {
    header: Header,
    query: String,
    consistency: u16,
    flags: u8,
}

impl QueryRequest {
    pub fn new(query: String) -> QueryRequest {
        QueryRequest {
            header: Header {
                version: Version::Request,
                flags: Flags::new(),
                stream: 0,
                opcode: Opcode::Query,
                length: 0,
            },
            query: query,
            consistency: 0x0001,
            flags: 0x00,
        }
    }
}

impl Encodable for QueryRequest {
    fn encode<T: Write>(&self, buffer: &mut T) {
        let mut body = Vec::new();
        let mut header = self.header;
        body.write_u32::<BigEndian>(self.query.len() as u32).unwrap();
        body.write_all(self.query.clone().into_bytes().as_ref()).unwrap();
        body.write_u16::<BigEndian>(self.consistency).unwrap();
        body.write_u8(self.flags).unwrap();
        header.length = body.len() as u32;
        header.encode(buffer);
        buffer.write_all(body.as_ref()).unwrap();
    }
}
