# PG_RUSTED_WIRE

`pg_rusted_wire` is a Rust implementation of the PostgreSQL Wire Protocol, enabling direct communication with PostgreSQL servers. It supports multiple authentication methods (`SCRAM-SHA-256`, `md5`, and `password`) and provides tools for logical replication and interactive SQL querying.

Stream table changes to Iceberg tables(or local files) or use the psql-like client for basic database operations, all with a focus on simplicity. This tool is helpful for basic database interactions and testing the wire protocol implementation.

## Requirements
Streaming to an iceberg table requires the dependencies listed below
1. Python shared library `python3-dev` for Ubuntu or `python3-devel` for RPM based distributions
2. PyIceberg which can be installed with the command `pip install "pyiceberg[s3fs,sql-postgres]"`. Replace `sql-postgres` with a Catalog store of your choice. Check [https://py.iceberg.apache.org/#installation](https://py.iceberg.apache.org/#installation) for more information.
3. PyArrow. Install with `pip install pyarrow`
4. Sqlite database. For running tests locally

## Example Usage

### Logical Replication

#### Streaming to a local file
This feature allows you to stream real-time changes from a PostgreSQL table directly to a local file using logical replication. When you start the process, a snapshot of the specified table is first saved as `<table_name>.data` in your chosen directory configured in `with_config_dir`.

As new INSERT operations occur in the database, they are appended to this file, enabling you to track changes over time.

```
use std::net::Ipv4Addr;
use std::str::FromStr;

use pg_rusted_wire::wire::*;

let mut client = Client::new(
    Ipv4Addr::from_str(&args.host).expect("IPV4 address error"),
    args.port,
)
.with_database("my_database") // database to connect to
.with_user("user") // user with permissions to connect to the database table to be streamed
.with_replication("database") // this is required for logical replication
.with_protocol(PROTOCOL_VERSION) // Supported protocol version to use
.with_config_dir("/my/dir"); // Directory with the right permisions to store the state in. The streamed table file data is also stored here.

// Connect and start streaming changes from the table
match client.connect() {
    Ok(mut stream) => {
        if let Err(e) = client.authenticate2(&mut stream, &args.password) {
            eprintln!("Client Authentication: {}", e);
            return;
        }

        client.run(
            &mut stream,
            "table_name",
            "publication_name",
            None,
        );
    }
    Err(e) => {
        eprintln!("No stream available for client: {}", e);
        return;
    }
}
```
When you start the process, a snapshot of the specified table is first saved as `<table_name>.data` in your chosen `--config-dir` directory

#### Streaming to a Iceberg Table
You can also stream real-time changes from a PostgreSQL table directly into an existing Apache Iceberg table. This feature is useful for integrating PostgreSQL logical replication with modern data lake architectures. This has only been tested with PostgreSQL(as the catalog) and Minio(as the object storage backend).

Note that the target Iceberg table must exist before starting replication.

Configuration: Create a configuration file with your Iceberg and storage details:
```
S3_SECRET_KEY=pass1234
S3_ENDPOINT=http://1.1.1.1:9000
S3_ACCESS_KEY=minio_user
CATALOG_URI=postgresql+psycopg2://username:password@1.1.1.1:5432/iceberg

# TABLE_NAME is in the format catalog.schema.table
TABLE_NAME=catalog_todo.example_s3_schema.employees_test
```

Below is sample code for streaming to iceberg. 
```
use std::net::Ipv4Addr;
use std::str::FromStr;

use pg_rusted_wire::wire::*;

let mut client = Client::new(
    Ipv4Addr::from_str(&args.host).expect("IPV4 address error"),
    args.port,
)
.with_database("my_database") // database to connect to
.with_user("user") // user with permissions to connect to the database table to be streamed
.with_replication("database") // this is required for logical replication
.with_protocol(PROTOCOL_VERSION) // Supported protocol version to use
.with_config_dir("/my/dir"); // Directory with the right permisions to store the state in. The streamed table file data is also stored here.

// Connect and start streaming changes from the table
match client.connect() {
    Ok(mut stream) => {
        if let Err(e) = client.authenticate2(&mut stream, &args.password) {
            eprintln!("Client Authentication: {}", e);
            return;
        }

        client.run(
            &mut stream,
            "table_name",
            "publication_name",
            Some(OutResource::Iceberg{config_path: ".tmp/config".to_string(), schema: None, key: None}), // Provide actual path to the iceberg config file described above
        );

    }
    Err(e) => {
        eprintln!("No stream available for client: {}", e);
        return;
    }
}
```

This integration enables you to capture and store PostgreSQL table changes in Iceberg, supporting scalable analytics and simple data lake workflows.



You can also use the command-line interface with the example code in [lrepl](examples/lrepl.rs) for convenience:

```
cargo run --example lrepl -- -u <user> -P <password> -H <host> -d <database> -p <port> --table <name> --publication <pub1> --config-dir <dir>
```
> Replace the placeholders with your actual connection information. Use `cargo run --example lrepl -- -h` for help on available flags and defaults.

Currently, only INSERT and DELETE operations are fully supported. There is partial support for UPDATE operations -- updates to columns that make the key may be added later. ~~Also, support for DELETEs will be added in the future.~~

Note that only one table can be replicated at a time. This tool is ideal for capturing and auditing table changes or for simple data synchronization tasks.


### PSQL-Like CLient
A simple interactive SQL client, similar to psql, is also included as an example. You can use it to connect to your PostgreSQL database and run queries directly from the terminal.

To start the client, run:
```
cargo run --example simpsql
```
> Before running, update the connection settings (database name, user, password, host, and port) in the [simpsql](examples/simpsql.rs) file.

Once running, you can enter SQL commands interactively. Example usage:
```
Client connection
Use Ctrl+c to end

psql> CREATE TABLE foo(id SERIAL PRIMARY KEY, k INT NOT NULL);
psql> INSERT INTO foo(k) SELECT i FROM generate_series(1002, 1011) i;
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

Results are displayed in a simple, readable format. Use Ctrl+C to exit the client. This tool is useful for basic database operations and testing your PostgreSQL connection.
