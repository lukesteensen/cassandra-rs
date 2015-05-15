use std::io::Cursor;
use std::net::TcpStream;
use std::net::ToSocketAddrs;
use std::collections::HashMap;
use podio::{BigEndian, ReadPodExt};

use protocol::*;

pub struct Client {
    conn: TcpStream,
}

impl Client {
    pub fn new<A: ToSocketAddrs>(addr: A) -> Client {
        Client {
            conn: TcpStream::connect(addr).unwrap(),
        }
    }

    pub fn initialize(&mut self) {
        let options = self.get_options();
        let cql_version = options["CQL_VERSION"][0].clone();
        let req = StartupRequest::new(cql_version.as_ref());
        req.encode(&mut self.conn);
        let ready = Header::decode(&mut self.conn);
        println!("Connection initialized with CQL version {}", cql_version);
        println!("{:?}", ready);
    }

    pub fn query(&mut self, query: String) {
        let req = QueryRequest::new(query);
        println!("Sending query...");
        req.encode(&mut self.conn);
        let result_header = Header::decode(&mut self.conn);
        println!("{:?}", result_header);
        let kind = self.conn.read_u32::<BigEndian>().unwrap();
        println!("Result kind is {}", match kind {
            1 => "void",
            2 => "rows",
            3 => "set_keyspace",
            4 => "prepared",
            5 => "schema_change",
            _ => panic!("unknown result kind"),
        });
        let flags = self.conn.read_u32::<BigEndian>().unwrap();
        println!("Flags: {:032b}", flags);
        let col_count = self.conn.read_u32::<BigEndian>().unwrap();
        println!("{} columns returned", col_count);
        let keyspace_name = String::decode(&mut self.conn);
        let table_name = String::decode(&mut self.conn);
        println!("Result from {}.{}", keyspace_name, table_name);
        let column_name = String::decode(&mut self.conn);
        println!("Column name: {}", column_name);
        let type_id = self.conn.read_u16::<BigEndian>().unwrap();
        println!("type id: 0x{:04X}", type_id);
        let row_count = self.conn.read_u32::<BigEndian>().unwrap();
        println!("{} rows returned", row_count);
        for _ in 0..row_count {
            for _ in 0..col_count {
                let n = self.conn.read_i32::<BigEndian>().unwrap();
                println!("{} byte value", n);
                let bytes = self.conn.read_exact(n as usize).unwrap();
                println!("value: {}", String::from_utf8(bytes).unwrap());
            }
        }
    }

    fn get_options(&mut self) -> HashMap<String, Vec<String>> {
        let req = OptionsRequest::new();
        req.encode(&mut self.conn);

        let header = Header::decode(&mut self.conn);
        let mut body = Cursor::new(self.conn.read_exact(header.length as usize).unwrap());
        StringMultiMap::decode(&mut body)
    }
}
