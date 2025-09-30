# PG_RUSTED_WIRE

Postgres Wire Protocol implemented in Rust. Currently supports `SCRAM-SHA-256`, `md5` and `password`  authentication.

## Example Usage

Having an entry like this in your `pg_hba.conf` file
```
host <database> <user> <ip_address>/24 <auth>  # replace <database>, <user> and <ip_address> appropriately. <auth> should be one of (SCRAM-SHA-256, password, md5)
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

Try running the [example](examples/main.rs) (make sure to create table `foo` and update the connection details in the file)

Create table `foo`:
```
CREATE TABLE foo(id SERIAL PRIMARY KEY, k INT NOT NULL);
INSERT INTO foo SELECT i, 1001+i FROM generate_series(1, 10) i;
```

Command to run example `cargo run --example main`
Results
```
Authentication: MD5Password

// Results for first Query
Done with command tag: "SELECT 10"
Row Description: "id|k"  // Columns
Data Buffer: "1|1002 2|1003 3|1004 4|1005 5|1006 6|1007 7|1008 8|1009 9|1010 10|1011 "  // Row data

// Results for second Query
Done with command tag: "SELECT 4"
Row Description: "id|k"    // Columns
Data Buffer: "1|1002 2|1003 3|1004 4|1005 "  // Row Data
```

You can also use the simple `psql` like example tool with the `cargo run --example simpsql` command. Ensure to update the configuration values in the [psql](examples/psql.rs) file before running the command
This will show results as follows(assuming `foo` table was created using the SQL above)
```
Client connection
Use Ctrl+c to end

psql> SELECT * FROM foo;
id|k
1|1002
2|1003
3|1004
4|1005
5|1006
6|1007
7|1008
8|1009
9|1010
10|1011

psql>
```

# Logical Replication Support
To use logical replication use the example command below
```
cargo run --example lrepl -- -u <user> -P <password> -H <host> -d <database> -p <port> --table <name> --publication <pub1> --config-dir <dir>
```
Fill in the <> brackets with valid values for the flags. Use `cargo run --example lrepl -- -h` for details on the required flags and the available defaults.

Currently, you can stream changes to a local file on your system inside the `--config-dir` directory that you provided above. With the commnad, a snapshot of the table is first copied to the file named `<table_name.data>`. Later, DML changes streamed from PG are appended to the file. Only INSERT operation is supported at the moment. UPDATE and DELETE may be added at a later time. You can only replicate one table at a time.
