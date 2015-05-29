use std::collections::HashMap;
use std::io::{Read, Write, Cursor};
use podio::{BigEndian, ReadPodExt, WritePodExt};

use types::CQLType;

pub trait Encodable {
    fn encode<T: Write>(&self, buffer: &mut T);
}

pub trait Decodable {
    fn decode<T: Read>(buffer: &mut T) -> Self;
}

#[derive(Debug, Copy, Clone)]
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

#[derive(Debug, Copy, Clone)]
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

#[derive(Debug, Copy, Clone)]
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
        #[derive(Debug, Copy, Clone)]
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

#[derive(Debug)]
pub struct QueryResult {
    header: Header,
    kind: ResultKind,
    flags: ResultFlags,
    table_spec: Option<TableSpec>,
    pub rows: Vec<Row>,
}

impl Decodable for QueryResult {
    fn decode<T: Read>(buffer: &mut T) -> QueryResult {
        let header = Header::decode(buffer);
        let mut body = Cursor::new(buffer.read_exact(header.length as usize).unwrap());
        let kind = ResultKind::decode(&mut body);
        if kind != ResultKind::Rows {
            panic!("Parsing for result of kind {:?} is unimplemented");
        };
        let flags = ResultFlags::decode(&mut body);
        if flags.has_more_pages {
            println!("warning: has_more_pages set on result but paging is unimplemented");
        };
        if flags.no_metadata {
            panic!("Parsing results with no_metadata set is unimplemented");
        };
        let column_count = body.read_i32::<BigEndian>().unwrap();
        let global_table_spec = if flags.global_table_spec {
            Some(TableSpec::decode(&mut body))
        } else {
            None
        };
        let mut column_specs = Vec::with_capacity(column_count as usize);
        for _ in 0..column_count {
            let table_spec = if flags.global_table_spec {
                global_table_spec.clone().unwrap()
            } else {
                TableSpec::decode(&mut body)
            };
            let spec = ColumnSpec {
                table_spec: table_spec,
                name: String::decode(&mut body),
                datatype: CQLType::decode(&mut body)
            };
            column_specs.push(spec);
        };
        let row_count = body.read_i32::<BigEndian>().unwrap();
        let mut rows = Vec::with_capacity(row_count as usize);
        for _ in 0..row_count {
            let mut columns = HashMap::with_capacity(column_count as usize);
            for column_spec in column_specs.iter() {
                let size = body.read_i32::<BigEndian>().unwrap() as usize;
                columns.insert(column_spec.name.clone(), body.read_exact(size).unwrap());
            }
            rows.push(Row { columns: columns });
        };
        QueryResult {
            header: header,
            kind: kind,
            flags: flags,
            table_spec: global_table_spec,
            rows: rows,
        }
    }
}

#[derive(Debug)]
pub struct Row {
    pub columns: HashMap<String, Vec<u8>>,
}

#[derive(Debug, Copy, Clone, PartialEq)]
enum ResultKind {
    Void,
    Rows,
    SetKeyspace,
    Prepared,
    SchemaChange,
}

impl Decodable for ResultKind {
    fn decode<T: Read>(buffer: &mut T) -> ResultKind {
        let kind = buffer.read_i32::<BigEndian>().unwrap();
        match kind {
            0x0001 => ResultKind::Void,
            0x0002 => ResultKind::Rows,
            0x0003 => ResultKind::SetKeyspace,
            0x0004 => ResultKind::Prepared,
            0x0005 => ResultKind::SchemaChange,
            _ => panic!("Unknown result kind: 0x{:04X}", kind),
        }
    }
}

#[derive(Debug)]
struct ResultFlags {
    global_table_spec: bool,
    has_more_pages: bool,
    no_metadata: bool,
}

impl Decodable for ResultFlags {
    fn decode<T: Read>(buffer: &mut T) -> ResultFlags {
        let flags = buffer.read_i32::<BigEndian>().unwrap();
        ResultFlags {
            global_table_spec: (flags & 0x01) > 0,
            has_more_pages: (flags & 0x02) > 0,
            no_metadata: (flags & 0x04) > 0,
        }
    }
}

#[derive(Debug, Clone)]
struct TableSpec {
    keyspace: String,
    table: String,
}

impl Decodable for TableSpec {
    fn decode<T: Read>(buffer: &mut T) -> TableSpec {
        TableSpec {
            keyspace: String::decode(buffer),
            table: String::decode(buffer),
        }
    }
}

#[derive(Debug)]
struct ColumnSpec {
    table_spec: TableSpec,
    name: String,
    datatype: CQLType,
}

impl Decodable for CQLType {
    fn decode<T: Read>(buffer: &mut T) -> CQLType {
        let option = buffer.read_u16::<BigEndian>().unwrap();
        match option {
            0x0000 => CQLType::Custom,
            0x0001 => CQLType::Ascii,
            0x0002 => CQLType::Bigint,
            0x0003 => CQLType::Blob,
            0x0004 => CQLType::Boolean,
            0x0005 => CQLType::Counter,
            0x0006 => CQLType::Decimal,
            0x0007 => CQLType::Double,
            0x0008 => CQLType::Float,
            0x0009 => CQLType::Int,
            0x000B => CQLType::Timestamp,
            0x000C => CQLType::Uuid,
            0x000D => CQLType::Varchar,
            0x000E => CQLType::Varint,
            0x000F => CQLType::Timeuuid,
            0x0010 => CQLType::Inet,
            0x0020 => CQLType::List,
            0x0021 => CQLType::Map,
            0x0022 => CQLType::Set,
            0x0030 => CQLType::UDT,
            0x0031 => CQLType::Tuple,
            _ => panic!("unknown type identifier: 0x{:04X}", option),
        }
    }
}
