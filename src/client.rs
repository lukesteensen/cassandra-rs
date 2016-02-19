use std::io::{Cursor, Read};
use std::collections::HashMap;
use std::net::{TcpStream, ToSocketAddrs};

use protocol::*;
use types::ToCQL;
use errors::MyError;

pub struct Client {
    conn: TcpStream,
}

impl Client {
    pub fn new<A: ToSocketAddrs>(addr: A) -> Client {
        Client {
            conn: TcpStream::connect(addr).unwrap(),
        }
    }

    pub fn initialize(&mut self) -> Result<()> {
        let options = try!(self.get_options());
        let cql_version = &options["CQL_VERSION"][0];
        let req = StartupRequest::new(cql_version);
        try!(req.encode(&mut self.conn));
        let ready = try!(Header::decode(&mut self.conn));
        println!("Connection initialized with CQL version {}", cql_version);
        assert_eq!(ready.opcode, Opcode::Ready);
        match ready.opcode {
            Opcode::Ready => Ok(()),
            _ => Err(MyError::Protocol(format!("Expected Ready opcode, got {:?}", ready.opcode)))
        }
    }

    pub fn query(&mut self, query: &str, params: &[&ToCQL]) -> Result<QueryResult> {
        let req = QueryRequest::new(query, params);
        try!(req.encode(&mut self.conn));
        QueryResult::decode(&mut self.conn)
    }

    pub fn execute(&mut self, statement: &str, params: &[&ToCQL]) -> Result<()> {
        let statement = QueryRequest::new(statement, params);
        try!(statement.encode(&mut self.conn));
        NonRowResult::decode(&mut self.conn).map(|_| ())
    }

    fn get_options(&mut self) -> Result<HashMap<String, Vec<String>>> {
        let req = OptionsRequest::new();
        try!(req.encode(&mut self.conn));

        let header = try!(Header::decode(&mut self.conn));
        let mut bytes = vec![0; header.length as usize];
        try!(self.conn.read_exact(&mut bytes));
        let mut body = Cursor::new(bytes);
        StringMultiMap::decode(&mut body)
    }
}
