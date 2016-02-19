use uuid::Uuid;
use std::hash::Hash;
use std::collections::HashSet;
use std::io::{Cursor, Read, Write};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

#[derive(Debug)]
pub enum CQLType {
    Custom,
    Ascii,
    Bigint,
    Blob,
    Boolean,
    Counter,
    Decimal,
    Double,
    Float,
    Int,
    Timestamp,
    Uuid,
    Varchar,
    Varint,
    Timeuuid,
    Inet,
    List,
    Map,
    Set,
    UDT,
    Tuple,
}

pub trait FromCQL {
    fn parse(buf: Vec<u8>) -> Self;
}

pub trait ToCQL {
    fn serialize(&self) -> Vec<u8>;
}

impl FromCQL for i32 {
    fn parse(buf: Vec<u8>) -> Self {
        assert_eq!(buf.len(), 4);
        Cursor::new(buf).read_i32::<BigEndian>().unwrap()
    }
}

impl ToCQL for i32 {
    fn serialize(&self) -> Vec<u8> {
        let mut ret = Vec::with_capacity(4);
        ret.write_i32::<BigEndian>(*self).unwrap();
        ret
    }
}

impl FromCQL for String {
    fn parse(buf: Vec<u8>) -> String {
        String::from_utf8(buf).unwrap()
    }
}

impl ToCQL for String {
    fn serialize(&self) -> Vec<u8> {
        self.clone().into_bytes()
    }
}

impl<'a> ToCQL for &'a str {
    fn serialize(&self) -> Vec<u8> {
        self.as_bytes().to_owned()
    }
}

impl FromCQL for Uuid {
    fn parse(buf: Vec<u8>) -> Uuid {
        Uuid::from_bytes(buf.as_ref()).unwrap()
    }
}

impl ToCQL for Uuid {
    fn serialize(&self) -> Vec<u8> {
        self.as_bytes().to_owned()
    }
}

impl FromCQL for bool {
    fn parse(buf: Vec<u8>) -> bool {
        match buf[0] {
            0 => false,
            _ => true,
        }
    }
}

impl ToCQL for bool {
    fn serialize(&self) -> Vec<u8> {
        match *self {
            true => vec![1],
            false => vec![0],
        }
    }
}

impl<T: FromCQL + PartialEq + Eq + Hash> FromCQL for HashSet<T> {
    fn parse(buf: Vec<u8>) -> HashSet<T> {
        let mut bytes = Cursor::new(buf);
        let mut set = HashSet::new();
        let count = bytes.read_i32::<BigEndian>().unwrap();
        for _ in 0..count {
            let len = bytes.read_i32::<BigEndian>().unwrap();
            let mut buf = vec![0; len as usize];
            bytes.read_exact(&mut buf).unwrap();
            set.insert(T::parse(buf));
        }
        set
    }
}

impl<T: ToCQL + PartialEq + Eq + Hash> ToCQL for HashSet<T> {
    fn serialize(&self) -> Vec<u8> {
        let mut ret = Vec::new();
        ret.write_i32::<BigEndian>(self.len() as i32).unwrap();
        for item in self.iter() {
            let bytes = ToCQL::serialize(item);
            ret.write_i32::<BigEndian>(bytes.len() as i32).unwrap();
            ret.write_all(&bytes).unwrap();
        }
        ret
    }
}
