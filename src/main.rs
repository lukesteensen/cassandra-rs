extern crate podio;

mod client;
mod protocol;

fn main() {
    let mut client = client::Client::new("127.0.0.1:9042");
    client.initialize();
    let mut result = client.query("select cluster_name from system.local".to_string());
    println!("{:?}", result);
    println!("{}", result.get_string("cluster_name"));
}
