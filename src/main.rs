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

    let id = Uuid::parse_str("3cceb492-1c19-11e5-92d8-28cfe91ca1e9").unwrap();
    let mut friends = HashSet::new();
    friends.insert("Sam".to_string());
    friends.insert("Larry".to_string());
    client.execute("INSERT INTO testing.people (id, name, active, friends) VALUES (?, ?, ?, ?)", &[&id, &"John", &false, &friends]).unwrap();

    let result = client.query("SELECT * FROM testing.people where id = ?", &[&id]).unwrap();
    assert_eq!(result.rows.len(), 1);

    let ref row = result.rows[0];
    assert_eq!(row.columns.len(), 4);

    let returned_id: Uuid = row.get("id");
    assert_eq!(id, returned_id);

    let name: String = row.get("name");
    assert_eq!(name, "John".to_string());

    let active: bool = row.get("active");
    assert_eq!(active, false);

    let returned_friends: HashSet<String> = row.get("friends");
    assert_eq!(friends, returned_friends);
}
