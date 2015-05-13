mod client;
mod parser;
mod protocol;

fn main() {
    let mut client = client::Client::new("127.0.0.1:9042");

    let options = client.get_options();
    println!("{:?}", options);
}
