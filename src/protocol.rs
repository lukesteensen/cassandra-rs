use std::result;
use std::collections::HashMap;
use std::io::{Read, Write, Cursor};
use podio::{BigEndian, ReadPodExt, WritePodExt};

use errors::MyError;
use types::{CQLType, FromCQL, ToCQL};

pub type Result<T> = result::Result<T, MyError>;

pub trait ToWire {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<()>;
}

pub trait FromWire {
    fn decode<T: Read>(buffer: &mut T) -> Result<Self>;
}

#[derive(Debug, Copy, Clone)]
pub struct Header {
    version: Version,
    flags: Flags,
    stream: u16,
    pub opcode: Opcode,
    pub length: u32,
}

impl ToWire for Header {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<()> {
        try!(self.version.encode(buffer));
        try!(self.flags.encode(buffer));
        try!(buffer.write_u16::<BigEndian>(self.stream));
        try!(self.opcode.encode(buffer));
        try!(buffer.write_u32::<BigEndian>(self.length));
        Ok(())
    }
}

impl FromWire for Header {
    fn decode<T: Read>(buffer: &mut T) -> Result<Header> {
        let header = Header {
            version: try!(Version::decode(buffer)),
            flags: try!(Flags::decode(buffer)),
            stream: try!(buffer.read_u16::<BigEndian>()),
            opcode: try!(Opcode::decode(buffer)),
            length: try!(buffer.read_u32::<BigEndian>()),
        };

        match header.opcode {
            Opcode::Error => {
                let code = try!(buffer.read_u32::<BigEndian>());
                let message = try!(String::decode(buffer));
                Err(MyError::Protocol(format!("Error 0x{:04X}: {}", code, message)))
            },
            _ => Ok(header),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum Version {
    Request,
    Response,
}

impl ToWire for Version {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<()> {
        try!(buffer.write_u8(match *self {
            Version::Request => 0x03,
            Version::Response => 0x83,
        }));
        Ok(())
    }
}

impl FromWire for Version {
    fn decode<T: Read>(buffer: &mut T) -> Result<Version> {
        let version = try!(buffer.read_u8());
        match version {
            0x03 => Ok(Version::Request),
            0x83 => Ok(Version::Response),
            _ => Err(MyError::Protocol(format!("unknown version header: {:02x}", version))),
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

impl ToWire for Flags {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<()> {
        let compression = if self.compression { 0x01 } else { 0x00 };
        let tracing = if self.tracing { 0x02 } else { 0x00 };
        try!(buffer.write_u8(compression | tracing));
        Ok(())
    }
}

impl FromWire for Flags {
    fn decode<T: Read>(buffer: &mut T) -> Result<Flags> {
        let flags = try!(buffer.read_u8());
        Ok(Flags {
            compression: (flags & 0x01) > 0,
            tracing: (flags & 0x02) > 0,
        })
    }
}

macro_rules! opcodes {
    ( $( $val:expr => $var:ident, )* ) => {
        #[derive(Debug, Copy, Clone, PartialEq)]
        pub enum Opcode {
            $(
                $var = $val,
             )*
        }

        impl ToWire for Opcode {
            fn encode<T: Write>(&self, buffer: &mut T) -> Result<()>{
                let val = match *self {
                    $(
                        Opcode::$var => $val,
                     )*
                };
                try!(buffer.write_u8(val));
                Ok(())
            }
        }

        impl FromWire for Opcode {
            fn decode<T: Read>(buffer: &mut T) -> Result<Opcode> {
                let opcode = try!(buffer.read_u8());
                match opcode {
                    $(
                        $val => Ok(Opcode::$var),
                     )*
                    _ => Err(MyError::Protocol(format!("Unknown opcode: {:02x}", opcode))),
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

impl FromWire for StringMultiMap {
    fn decode<T: Read>(buffer: &mut T) -> Result<StringMultiMap> {
        let mut map = HashMap::new();

        let key_count = try!(buffer.read_u16::<BigEndian>());
        for _ in 0..key_count {
            let key = try!(String::decode(buffer));
            let val_count = try!(buffer.read_u16::<BigEndian>());
            let mut vec = Vec::with_capacity(val_count as usize);
            for _ in 0..val_count {
                vec.push(try!(String::decode(buffer)));
            }
            map.insert(key, vec);
        }
        Ok(map)
    }
}

impl<'a> ToWire for &'a str {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<()> {
        try!(buffer.write_u16::<BigEndian>(self.len() as u16));
        try!(buffer.write_all(self.as_bytes()));
        Ok(())
    }
}

impl FromWire for String {
    fn decode<T: Read>(buffer: &mut T) -> Result<String> {
        let len = try!(buffer.read_u16::<BigEndian>());
        let byte_vec = try!(buffer.read_exact(len as usize));
        String::from_utf8(byte_vec).map_err(|e| MyError::Protocol(format!("{}", e)))
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

impl ToWire for OptionsRequest {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<()> {
        self.header.encode(buffer)
    }
}

type StringMap<'a> = HashMap<&'a str, &'a str>;

impl<'a> ToWire for StringMap<'a> {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<()> {
        try!(buffer.write_u16::<BigEndian>(self.len() as u16));
        for (key, val) in self.iter() {
            try!(key.encode(buffer));
            try!(val.encode(buffer));
        }
        Ok(())
    }
}

pub struct StartupRequest {
    header: Header,
    body: Vec<u8>,
}

impl StartupRequest {
    pub fn new(cql_version: &str) -> StartupRequest {
        let mut options = HashMap::new();
        options.insert("CQL_VERSION", cql_version);
        let mut body = Vec::new();
        options.encode(&mut body).unwrap();
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

impl ToWire for StartupRequest {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<()> {
        try!(self.header.encode(buffer));
        try!(buffer.write(self.body.as_ref()));
        Ok(())
    }
}

pub struct QueryRequest<'a> {
    header: Header,
    query: &'a str,
    consistency: u16,
    flags: u8,
    params: &'a [&'a ToCQL],
}

impl<'a> QueryRequest<'a> {
    pub fn new(query: &'a str, params: &'a [&'a ToCQL]) -> QueryRequest<'a> {
        let flags = match params.len() {
            0 => 0x00,
            _ => 0x01,
        };
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
            flags: flags,
            params: params,
        }
    }
}

impl<'a> ToWire for QueryRequest<'a> {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<()> {
        let mut body = Vec::new();
        let mut header = self.header;
        try!(body.write_u32::<BigEndian>(self.query.len() as u32));
        try!(body.write_all(self.query.as_bytes()));
        try!(body.write_u16::<BigEndian>(self.consistency));
        try!(body.write_u8(self.flags));
        if self.params.len() > 0 {
            try!(body.write_u16::<BigEndian>(self.params.len() as u16));
            for p in self.params {
                let bytes = p.serialize();
                try!(body.write_i32::<BigEndian>(bytes.len() as i32));
                try!(body.write_all(&bytes));
            }
        }
        header.length = body.len() as u32;
        try!(header.encode(buffer));
        try!(buffer.write_all(body.as_ref()));
        Ok(())
    }
}

#[derive(Debug)]
pub struct QueryResult {
    header: Header,
    kind: ResultKind, // TODO: always rows?
    flags: ResultFlags,
    table_spec: Option<TableSpec>,
    pub rows: Vec<Row>,
}

impl FromWire for QueryResult {
    fn decode<T: Read>(buffer: &mut T) -> Result<QueryResult> {
        let header = try!(Header::decode(buffer));
        let mut body = Cursor::new(try!(buffer.read_exact(header.length as usize)));
        let kind = try!(ResultKind::decode(&mut body));
        if kind != ResultKind::Rows {
            panic!("Parsing for result of kind {:?} is unimplemented");
        };
        let flags = try!(ResultFlags::decode(&mut body));
        if flags.has_more_pages {
            println!("warning: has_more_pages set on result but paging is unimplemented");
        };
        if flags.no_metadata {
            return Err(MyError::Protocol("Parsing results with no_metadata set is unimplemented".to_string()));
        };
        let column_count = try!(body.read_i32::<BigEndian>());
        let global_table_spec = if flags.global_table_spec {
            Some(try!(TableSpec::decode(&mut body)))
        } else {
            None
        };
        let mut column_specs = Vec::with_capacity(column_count as usize);
        for _ in 0..column_count {
            let table_spec = if flags.global_table_spec {
                global_table_spec.clone().unwrap()
            } else {
                try!(TableSpec::decode(&mut body))
            };
            let spec = ColumnSpec {
                table_spec: table_spec,
                name: try!(String::decode(&mut body)),
                datatype: try!(CQLType::decode(&mut body))
            };
            column_specs.push(spec);
        };
        let row_count = try!(body.read_i32::<BigEndian>());
        let mut rows = Vec::with_capacity(row_count as usize);
        for _ in 0..row_count {
            let mut columns = HashMap::with_capacity(column_count as usize);
            for column_spec in column_specs.iter() {
                let size = try!(body.read_i32::<BigEndian>());
                if size > 0 {
                    columns.insert(column_spec.name.clone(), try!(body.read_exact(size as usize)));
                } else {
                    // NULL or legacy "empty"
                    columns.insert(column_spec.name.clone(), vec![]);
                }
            }
            rows.push(Row { columns: columns });
        };
        Ok(QueryResult {
            header: header,
            kind: kind,
            flags: flags,
            table_spec: global_table_spec,
            rows: rows,
        })
    }
}

#[derive(Debug)]
pub struct Row {
    pub columns: HashMap<String, Vec<u8>>,
}

impl Row {
    pub fn get<T: FromCQL>(&self, col: &str) -> Option<T> {
        let bytes = self.columns.get(col).unwrap().clone();
        if bytes.len() > 0 {
            Some(T::parse(bytes))
        } else {
            None
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
enum ResultKind {
    Void,
    Rows,
    SetKeyspace,
    Prepared,
    SchemaChange,
}

impl FromWire for ResultKind {
    fn decode<T: Read>(buffer: &mut T) -> Result<ResultKind> {
        let kind = try!(buffer.read_i32::<BigEndian>());
        match kind {
            0x0001 => Ok(ResultKind::Void),
            0x0002 => Ok(ResultKind::Rows),
            0x0003 => Ok(ResultKind::SetKeyspace),
            0x0004 => Ok(ResultKind::Prepared),
            0x0005 => Ok(ResultKind::SchemaChange),
            _ => Err(MyError::Protocol(format!("Unknown result kind: 0x{:04X}", kind))),
        }
    }
}

#[derive(Debug)]
struct ResultFlags {
    global_table_spec: bool,
    has_more_pages: bool,
    no_metadata: bool,
}

impl FromWire for ResultFlags {
    fn decode<T: Read>(buffer: &mut T) -> Result<ResultFlags> {
        let flags = try!(buffer.read_i32::<BigEndian>());
        Ok(ResultFlags {
            global_table_spec: (flags & 0x01) > 0,
            has_more_pages: (flags & 0x02) > 0,
            no_metadata: (flags & 0x04) > 0,
        })
    }
}

#[derive(Debug, Clone)]
struct TableSpec {
    keyspace: String,
    table: String,
}

impl FromWire for TableSpec {
    fn decode<T: Read>(buffer: &mut T) -> Result<TableSpec> {
        Ok(TableSpec {
            keyspace: try!(String::decode(buffer)),
            table: try!(String::decode(buffer)),
        })
    }
}

#[derive(Debug)]
struct ColumnSpec {
    table_spec: TableSpec,
    name: String,
    datatype: CQLType,
}

impl FromWire for CQLType {
    fn decode<T: Read>(buffer: &mut T) -> Result<CQLType> {
        let option = try!(buffer.read_u16::<BigEndian>());
        match option {
            0x0000 => {
                try!(String::decode(buffer));
                Ok(CQLType::Custom)
            },
            0x0001 => Ok(CQLType::Ascii),
            0x0002 => Ok(CQLType::Bigint),
            0x0003 => Ok(CQLType::Blob),
            0x0004 => Ok(CQLType::Boolean),
            0x0005 => Ok(CQLType::Counter),
            0x0006 => Ok(CQLType::Decimal),
            0x0007 => Ok(CQLType::Double),
            0x0008 => Ok(CQLType::Float),
            0x0009 => Ok(CQLType::Int),
            0x000B => Ok(CQLType::Timestamp),
            0x000C => Ok(CQLType::Uuid),
            0x000D => Ok(CQLType::Varchar),
            0x000E => Ok(CQLType::Varint),
            0x000F => Ok(CQLType::Timeuuid),
            0x0010 => Ok(CQLType::Inet),
            0x0020 => {
                try!(CQLType::decode(buffer));
                Ok(CQLType::List)
            },
            0x0021 => {
                try!(CQLType::decode(buffer));
                try!(CQLType::decode(buffer));
                Ok(CQLType::Map)
            },
            0x0022 => {
                try!(CQLType::decode(buffer));
                Ok(CQLType::Set)
            },
            0x0030 => {
                Err(MyError::Protocol("UDTs are not currently supported".to_string()))
                // CQLType::UDT
            },
            0x0031 => {
                Err(MyError::Protocol("Tuples are not currently supported".to_string()))
                // CQLType::Tuple
            },
            _ => Err(MyError::Protocol(format!("unknown type identifier: 0x{:04X}", option))),
        }
    }
}

#[derive(Debug)]
pub struct NonRowResult {
    header: Header,
    kind: ResultKind,
}

impl FromWire for NonRowResult {
    fn decode<T: Read>(buffer: &mut T) -> Result<NonRowResult> {
        let header = try!(Header::decode(buffer));
        let mut body = Cursor::new(try!(buffer.read_exact(header.length as usize)));
        let kind = try!(ResultKind::decode(&mut body));
        if ![ResultKind::SchemaChange, ResultKind::Void].contains(&kind) {
            return Err(MyError::Protocol(format!("Unexpected result kind {:?}", kind)))
        };
        Ok(NonRowResult {
            header: header,
            kind: kind,
        })
    }
}
