use std::io::Cursor;
use std::net::TcpStream;
use std::net::ToSocketAddrs;
use std::collections::HashMap;

use podio::ReadPodExt;

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

    pub fn query(&mut self, query: String) -> QueryResult {
        let req = QueryRequest::new(query);
        println!("Sending query...");
        req.encode(&mut self.conn);
        QueryResult::decode(&mut self.conn)
    }

    fn get_options(&mut self) -> HashMap<String, Vec<String>> {
        let req = OptionsRequest::new();
        req.encode(&mut self.conn);

        let header = Header::decode(&mut self.conn);
        let mut body = Cursor::new(self.conn.read_exact(header.length as usize).unwrap());
        StringMultiMap::decode(&mut body)
    }
}
