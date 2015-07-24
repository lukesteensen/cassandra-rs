use std::io::Cursor;
use std::net::TcpStream;
use std::net::ToSocketAddrs;
use std::collections::HashMap;

use podio::ReadPodExt;

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

    pub fn execute(&mut self, statement: &str) -> Result<()> {
        let statement = QueryRequest::new(statement, &[]);
        try!(statement.encode(&mut self.conn));
        NonRowResult::decode(&mut self.conn).map(|_| ())
    }

    fn get_options(&mut self) -> Result<HashMap<String, Vec<String>>> {
        let req = OptionsRequest::new();
        try!(req.encode(&mut self.conn));

        let header = try!(Header::decode(&mut self.conn));
        let mut body = Cursor::new(try!(self.conn.read_exact(header.length as usize)));
        StringMultiMap::decode(&mut body)
    }
}
