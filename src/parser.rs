use std::vec::IntoIter;

pub struct Parser {
    pub iter: IntoIter<u8>,
}

impl Parser {
    pub fn new(vec: Vec<u8>) -> Parser {
        Parser { iter: vec.into_iter() }
    }

    pub fn parse_u8(&mut self) -> u8 {
        self.iter.next().unwrap()
    }

    pub fn parse_u16(&mut self) -> u16 {
        (0..2).rev().fold(0, |acc, i| {
            let mut part = self.iter.next().unwrap() as u16;
            part = part << (i * 8);
            acc + part
        })
    }

    pub fn parse_u32(&mut self) -> u32 {
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
}
