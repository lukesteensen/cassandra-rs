use std::iter;
use std::io::{Read, Write};
use std::collections::HashMap;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

pub trait WireType {
    fn encode<T: Write>(&self, buffer: &mut T);
    fn decode<T: Read>(buffer: &mut T) -> Self;
}

#[derive(Debug, PartialEq, Eq)]
pub struct Header {
    pub version: Version,
    pub flags: Flags,
    pub stream: u16,
    pub opcode: Opcode,
    pub length: u32,
}

impl WireType for Header {
    fn encode<T: Write>(&self, buffer: &mut T) {
        self.version.encode(buffer);
        self.flags.encode(buffer);
        buffer.write_u16::<BigEndian>(self.stream).unwrap();
        self.opcode.encode(buffer);
        buffer.write_u32::<BigEndian>(self.length).unwrap();
    }

    fn decode<T: Read>(buffer: &mut T) -> Header {
        Header {
            version: Version::decode(buffer),
            flags: Flags::decode(buffer),
            stream: buffer.read_u16::<BigEndian>().unwrap(),
            opcode: Opcode::decode(buffer),
            length: buffer.read_u32::<BigEndian>().unwrap(),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Version {
    Request,
    Response,
}

impl WireType for Version {
    fn encode<T: Write>(&self, buffer: &mut T) {
        buffer.write_u8(match *self {
            Version::Request => 0x03,
            Version::Response => 0x83,
        }).unwrap();
    }

    fn decode<T: Read>(buffer: &mut T) -> Version {
        let version = buffer.read_u8().unwrap();
        match version {
            0x03 => Version::Request,
            0x83 => Version::Response,
            _ => panic!("unknown version header: {:02x}"),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Flags {
    pub compression: bool,
    pub tracing: bool,
}

impl WireType for Flags {
    fn encode<T: Write>(&self, buffer: &mut T) {
        let compression = if self.compression { 0x01 } else { 0x00 };
        let tracing = if self.tracing { 0x02 } else { 0x00 };
        buffer.write_u8(compression | tracing).unwrap();
    }

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
        #[derive(Debug, PartialEq, Eq)]
        pub enum Opcode {
            $(
                $var = $val,
             )*
        }

        impl WireType for Opcode {
            fn encode<T: Write>(&self, buffer: &mut T) {
                let val = match *self {
                    $(
                        Opcode::$var => $val,
                     )*
                };
                buffer.write_u8(val).unwrap();
            }

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

impl WireType for StringMultiMap {
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

impl WireType for String {
    fn encode<T: Write>(&self, buffer: &mut T) {
        buffer.write_u16::<BigEndian>(self.len() as u16).unwrap();
        buffer.write_all(self.clone().into_bytes().as_ref()).unwrap();
    }

    fn decode<T: Read>(buffer: &mut T) -> String {
        let len = buffer.read_u16::<BigEndian>().unwrap();
        let mut byte_vec = Vec::with_capacity(len as usize);
        byte_vec.extend(iter::repeat(0).take(len as usize));
        let bytes_read = buffer.read(&mut byte_vec[..]).unwrap();
        assert_eq!(bytes_read, len as usize);
        String::from_utf8(byte_vec).unwrap()
    }
}
