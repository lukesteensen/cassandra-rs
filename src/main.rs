extern crate uuid;
extern crate cassandra;

use uuid::Uuid;
use std::collections::HashSet;
use cassandra::client::Client;

fn main() {
    let mut client = Client::new("127.0.0.1:9042");
    client.initialize().unwrap();

    client.execute("DROP KEYSPACE IF EXISTS testing", &[]).unwrap();
    client.execute("CREATE KEYSPACE testing WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", &[]).unwrap();
    client.execute("CREATE TABLE testing.people ( id timeuuid PRIMARY KEY, name text, active boolean, friends set<text> )", &[]).unwrap();

    let given_id = Uuid::parse_str("3cceb492-1c19-11e5-92d8-28cfe91ca1e9").unwrap();
    client.execute("INSERT INTO testing.people (id, name, active, friends) VALUES (?, ?, ?, {'Sam', 'Larry'})", &[&given_id, &"John", &false]).unwrap();

    let result = client.query("SELECT * FROM testing.people where id = ?", &[&given_id]).unwrap();
    assert_eq!(result.rows.len(), 1);

    let ref row = result.rows[0];
    assert_eq!(row.columns.len(), 4);

    let id: Uuid = row.get("id");
    assert_eq!(id, Uuid::parse_str("3cceb492-1c19-11e5-92d8-28cfe91ca1e9").unwrap());

    let name: String = row.get("name");
    assert_eq!(name, "John".to_string());

    let active: bool = row.get("active");
    assert_eq!(active, false);

    let friends: HashSet<String> = row.get("friends");
    let mut expected_friends = HashSet::new();
    expected_friends.insert("Sam".to_string());
    expected_friends.insert("Larry".to_string());
    assert_eq!(friends, expected_friends);
}
