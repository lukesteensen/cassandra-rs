extern crate podio;

mod client;
mod protocol;

fn main() {
    let mut client = client::Client::new("127.0.0.1:9042");
    client.initialize();
    let mut result = client.query("select keyspace_name from system.schema_keyspaces".to_string());
    for row in result.rows.iter_mut() {
        println!("{:?}", String::from_utf8(row.columns.remove("keyspace_name").unwrap()).unwrap());
    }
}
