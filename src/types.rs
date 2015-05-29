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
