use uuid::Uuid;
use std::io::Cursor;
use std::hash::Hash;
use std::collections::HashSet;
use podio::{BigEndian, ReadPodExt};

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

impl FromCQL for String {
    fn parse(buf: Vec<u8>) -> String {
        String::from_utf8(buf).unwrap()
    }
}

impl FromCQL for Uuid {
    fn parse(buf: Vec<u8>) -> Uuid {
        Uuid::from_bytes(buf.as_ref()).unwrap()
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

impl<T: FromCQL + PartialEq + Eq + Hash> FromCQL for HashSet<T> {
    fn parse(buf: Vec<u8>) -> HashSet<T> {
        let mut bytes = Cursor::new(buf);
        let mut set = HashSet::new();
        let count = bytes.read_i32::<BigEndian>().unwrap();
        for _ in 0..count {
            let len = bytes.read_i32::<BigEndian>().unwrap();
            set.insert(T::parse(bytes.read_exact(len as usize).unwrap()));
        }
        set
    }
}
