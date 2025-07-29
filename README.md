# PG_RUSTED_WIRE

Postgres Wire Protocol implemented in Rust. Currently supports SASL `SCRAM-SHA-256` authentication only. Other authentication methods to be added soon

## Example Usage

Having an entry like this in your `pg_hba.conf` file
```
host <database> <user> <ip_address>/24 md5  # replace <database>, <user> and <ip_address> appropriately
```

Instantiate `StartupMsg` with message details
```
use bytes::BytesMut;
use std::net::Ipv4Addr;
use std::str::FromStr;

use pg_rusted_wire::*;

let mut startup_msg = StartupMsg::new(
    String::from(DEFAULT_USER),  // User
    Some(String::from(DEFAULT_USER)), // Database
    None, // Options
    None,  // true or false if replication protocol
);
```

Create client connection
```
let client = Client::new(Ipv4Addr::from_str(DEFAULT_IP).unwrap(), DEFAULT_PORT);
```

Use client to authenticate and send query
```
match client.connect() {  // Handle connection response(fail or success)
    Ok(mut stream) => {
        if let Err(e) = client.authenticate(&mut stream, &mut startup_msg, DEFAULT_USER) {
            eprintln!("Client Authentication: {}", e);
            return;
        }

        // Okay to use stream to send queries at this point
        let mut msg = "SELECT * FROM foo;"; // First Query; Ensure the user has access to table "foo"
        let result_buf = &mut BytesMut::new();  // Table columns
        let row_descr = &mut BytesMut::new();  // Returned row data

        if let Err(e) = process_simple_query(&mut stream, msg, result_buf, row_descr) {
            eprintln!("Error processing simple query: {}", e);
            return;
        }

        // Just print results here
        println!("Row Description: {:?}", String::from_utf8_lossy(row_descr));
        println!("Data Buffer: {:?}", String::from_utf8_lossy(result_buf));
    }
    Err(e) => {
        eprintln!("No stream available for client: {}", e);
        return;
    }
}
```
