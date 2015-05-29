extern crate cassandra;

use cassandra::client::Client;

fn main() {
    let mut client = Client::new("127.0.0.1:9042");
    client.initialize();

    client.execute("DROP KEYSPACE IF EXISTS testing");
    client.execute("CREATE KEYSPACE testing WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
    client.execute("CREATE TABLE testing.people ( name text PRIMARY KEY )");

    client.execute("INSERT INTO testing.people (name) VALUES ('John')");

    let result = client.query("SELECT * FROM testing.people");
    assert_eq!(result.rows.len(), 1);
    let ref row = result.rows[0];
    assert_eq!(row.columns.len(), 1);
    let name: String = row.get("name");
    assert_eq!(name, "John".to_string());
}
