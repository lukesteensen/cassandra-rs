extern crate byteorder;

mod client;
mod protocol;

fn main() {
    let mut client = client::Client::new("127.0.0.1:9042");
    client.initialize();

}
