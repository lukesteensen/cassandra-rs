use std::vec::IntoIter;

pub mod header;

pub struct Parser {
    pub iter: IntoIter<u8>,
}

impl Parser {
    pub fn new(vec: Vec<u8>) -> Parser {
        Parser { iter: vec.into_iter() }
    }

    fn parse_u8(&mut self) -> u8 {
        self.iter.next().unwrap()
    }

    pub fn parse_u16(&mut self) -> u16 {
        (0..2).rev().fold(0, |acc, i| {
            let mut part = self.iter.next().unwrap() as u16;
            part = part << (i * 8);
            acc + part
        })
    }

    fn parse_u32(&mut self) -> u32 {
        (0..4).rev().fold(0, |acc, i| {
            let mut part = self.iter.next().unwrap() as u32;
            part = part << (i * 8);
            acc + part
        })
    }

    pub fn parse_string(&mut self) -> String {
        let len = self.parse_u16();
        let byte_vec = (0..len).map(|_| self.iter.next().unwrap()).collect();
        String::from_utf8(byte_vec).unwrap()
    }

    fn parse_version(&mut self) -> header::Version {
        let version = self.parse_u8();
        match version {
            0x03 => header::Version::Request,
            0x83 => header::Version::Response,
            _    => panic!("unknown version: {:02x}", version),
        }
    }

    fn parse_flags(&mut self) -> header::Flags {
        let flags = self.parse_u8();
        header::Flags {
            compression: (flags & 0x01) > 0,
            tracing: (flags & 0x02) > 0,
        }
    }

    fn parse_opcode(&mut self) -> header::Opcode {
        header::parse_opcode(self.parse_u8())
    }

    pub fn parse_header(&mut self) -> header::Header {
        header::Header {
            version: self.parse_version(),
            flags: self.parse_flags(),
            stream: self.parse_u16(),
            opcode: self.parse_opcode(),
            length: self.parse_u32(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use super::header::*;

    #[test]
    fn it_parsers_headers() {
        let req = vec![
            0x03, // version
            0x00, // flags
            0x00, // stream
            0x00, // stream
            0x05, // opcode
            0x00, // length
            0x00, // length
            0x00, // length
            0x01, // length
        ];
        let mut parser = Parser::new(req);

        assert_eq!(
            parser.parse_header(),
            Header {
                version: Version::Request,
                flags: Flags {
                    compression: false,
                    tracing: false
                },
                stream: 0,
                opcode: Opcode::Options,
                length: 1,
            }
        )
    }
}
