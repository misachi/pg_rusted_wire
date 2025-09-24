/// WIP: PostgreSQL logical replication client implementation
use bytes::{BufMut, BytesMut};
use std::io::{self, Write};
use std::net::Ipv4Addr;
use std::str::FromStr;

use pg_rusted_wire::wire::*;

const DEFAULT_USER: &str = "postgres";
const DEFAULT_PORT: u16 = 5432;
const DEFAULT_IP: &str = "127.0.0.1";
const DEFAULT_PASS: &str = "pass12234";

const DEFAULT_DATABASE: &str = "postgres";

fn process_simple(
    stream: &mut std::net::TcpStream,
    state: &mut QueryState,
    result_buf: &mut [u8],
    row_descr: &mut BytesMut,
) -> Result<SimpleQueryCompletion, String> {
    // stream.set_read_timeout(Some(Duration::from_millis(1000))).unwrap(); // Set a timeout to avoid blocking indefinitely
    state.data_buf_off = 0;

    let ret = process_logical_repl(stream, result_buf, row_descr, state);
    row_descr.put_u8(b'\n');
    if let Err(e) = io::stdout().write_all(&row_descr) {
        return Err(format!(
            "Error when writing to stdout for RowDescription: {}",
            e
        ));
    }
    if let Err(e) = io::stdout().write_all(&result_buf) {
        eprintln!("Error when writing to stdout for DataRow: {}", e);
    }

    ret
}

fn main() {
    let mut startup_msg = StartupMsg::new(
        String::from(DEFAULT_USER),
        Some(String::from(DEFAULT_DATABASE)),
        None,
        Some(String::from("database")),
    );

    let client = Client::new(
        Ipv4Addr::from_str(DEFAULT_IP).expect("IPV4 address error"),
        DEFAULT_PORT,
    );
    let mut state = QueryState {
        overflowed: false,
        skip_bytes: 0,
        overflow_buf: BytesMut::new(),
        data_buf_off: 0,
    };

    match client.connect() {
        Ok(mut stream) => {
            if let Err(e) = client.authenticate(&mut stream, &mut startup_msg, DEFAULT_PASS) {
                eprintln!("Client Authentication: {}", e);
                return;
            }

            let mut result_buf = [0; 4096];
            let mut row_descr = BytesMut::new();

            let mut msg = String::from("IDENTIFY_SYSTEM");

            if let Some(e) = send_simple_query(&mut stream, &msg) {
                eprintln!("Query Error: {}", e);
                return;
            }
            if let Err(e) = process_simple(&mut stream, &mut state, &mut result_buf, &mut row_descr)
            {
                eprintln!("IDENTIFY SYSTEM Error: {}", e);
                return;
            }
            let parts: &Vec<&[u8]> = &result_buf.split(|&b| b == b'|').collect();
            let start_lsn: &[u8] = &parts[2].to_vec();

            result_buf.fill(0);
            row_descr.clear();

            msg = String::from("COPY foo TO STDOUT");

            if let Some(e) = send_simple_query(&mut stream, &msg) {
                eprintln!("Copy Error: {}", e);
                return;
            }

            loop {
                match process_simple(&mut stream, &mut state, &mut result_buf, &mut row_descr) {
                    Ok(SimpleQueryCompletion::CommandComplete) => {
                        row_descr.put_u8(b'\n');
                        if let Err(e) = io::stdout().write_all(&row_descr) {
                            eprintln!("Error when writing to stdout for RowDescription: {}", e);
                        }
                        if let Err(e) = io::stdout().write_all(&result_buf) {
                            eprintln!("Error when writing to stdout for DataRow: {}", e);
                        }
                        break;
                    }
                    Ok(SimpleQueryCompletion::CopyComplete) => {
                        break;
                    }
                    Ok(SimpleQueryCompletion::InProgress) => (),
                    Ok(SimpleQueryCompletion::InProgressReadStream) => (),
                    Ok(SimpleQueryCompletion::CommandError) => {
                        break;
                    }
                    Ok(SimpleQueryCompletion::CopyError) => {
                        break;
                    }
                    Err(e) => {
                        eprintln!("Error processing simple query: {}", e);
                    }
                    _ => (),
                }

                result_buf.fill(0);
                row_descr.clear();
            }

            // fun_name(&mut stream, &mut state, &mut result_buf, &mut row_descr);
            //     result_buf.fill(0);
            //     row_descr.clear();
            // break;

            // result_buf.fill(0);
            // row_descr.clear();

            // msg = String::from("CREATE_REPLICATION_SLOT test_slot3 LOGICAL pgoutput");
            // process_simple(&mut stream, msg, &mut state, &mut result_buf, &mut row_descr);

            result_buf.fill(0);
            row_descr.clear();

            msg = String::from(format!(
                "START_REPLICATION SLOT test_slot3 LOGICAL {} (proto_version '4', publication_names 'pub1')",
                String::from_utf8_lossy(start_lsn)
            ));

            if let Some(e) = send_simple_query(&mut stream, &msg) {
                eprintln!("START REPLICATION Command Error: {}", e);
                return;
            }

            loop {
                match process_simple(&mut stream, &mut state, &mut result_buf, &mut row_descr) {
                    Ok(SimpleQueryCompletion::CommandComplete) => {
                        if let Err(e) = io::stdout().write_all(&result_buf) {
                            eprintln!("Error when writing to stdout for DataRow: {}", e);
                        }
                        break;
                    }
                    Ok(SimpleQueryCompletion::CopyComplete) => (),
                    Ok(SimpleQueryCompletion::InProgress) => (),
                    Ok(SimpleQueryCompletion::InProgressReadStream) => (),
                    Ok(SimpleQueryCompletion::CommandError) => {
                        break;
                    }
                    Ok(SimpleQueryCompletion::CopyError) => {
                        break;
                    }
                    Err(e) => {
                        eprintln!("Error processing simple query: {}", e);
                        break;
                    }
                    _ => (),
                }

                result_buf.fill(0);
                row_descr.clear();
            }
        }
        Err(e) => {
            eprintln!("No stream available for client: {}", e);
            return;
        }
    }
}
