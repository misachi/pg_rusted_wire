/// WIP: PostgreSQL logical replication client implementation
/// Run: [cargo run --example lrepl -- -u <user> -P <password> -H 172.17.0.1 -d postgres -p 5432 --table <name> --publication <pub1> --config-dir <dir>] or
///     [lrepl -u <user> -P <password> -H 172.17.0.1 -d postgres -p 5432 --table <name> --publication <pub1> --config-dir <dir>]
/// Help: lrepl -h
use clap::Parser;
use std::net::Ipv4Addr;
use std::str::FromStr;

use pg_rusted_wire::wire::*;

const DEFAULT_USER: &str = "postgres";
const DEFAULT_PORT: u16 = 5432;
const DEFAULT_IP: &str = "127.0.0.1";

const DEFAULT_DATABASE: &str = "postgres";

#[derive(Debug, Parser)]
struct RustedCli {
    #[arg(short = 'u', long, default_value_t = DEFAULT_USER.to_string())]
    user: String,
    #[arg(short = 'p', long, default_value_t = DEFAULT_PORT)]
    port: u16,
    #[arg(short = 'H', long, default_value_t = DEFAULT_IP.to_string())]
    host: String,
    #[arg(short = 'P', long)]
    password: String,
    #[arg(short = 'd', long, default_value_t = DEFAULT_DATABASE.to_string())]
    database: String,
    #[arg(short, long)]
    table: String,
    #[arg(short = 'L', long)]
    publication: String,
    #[arg(short = 'c', long)]
    config_dir: String,
}

fn main() {
    let args = RustedCli::parse();

    let mut client = Client::new(
        Ipv4Addr::from_str(&args.host).expect("IPV4 address error"),
        args.port,
    )
    .with_database(&args.database)
    .with_user(&args.user)
    .with_replication("database")
    .with_protocol(PROTOCOL_VERSION)
    .with_config_dir(&args.config_dir);

    match client.connect() {
        Ok(mut stream) => {
            if let Err(e) = client.authenticate2(&mut stream, &args.password) {
                eprintln!("Client Authentication: {}", e);
                return;
            }

            // Start streaming to a local CSV file
            client.run(
                &mut stream,
                &args.table,
                &args.publication,
                None,
            );

            // Start streaming to an iceberg file
            // client.run(
            //     &mut stream,
            //     &args.table,
            //     &args.publication,
            //     Some(OutResource::Iceberg{config_path: ".tmp/config".to_string(), schema: None, key: None}),
            // );
        }
        Err(e) => {
            eprintln!("No stream available for client: {}", e);
            return;
        }
    }
}
