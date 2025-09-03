use bytes::{BufMut, BytesMut};
use std::cmp::Ordering;
use std::io::{self, Write};
use std::net::Ipv4Addr;
use std::str::FromStr;

use pg_rusted_wire::wire::*;

const DEFAULT_USER: &str = "postgres";
const DEFAULT_PORT: u16 = 5432;
const DEFAULT_IP: &str = "127.0.0.1";
const DEFAULT_PASS: &str = "pass12234";

const DEFAULT_DATABASE: &str = "postgres";


fn main() {
    let mut startup_msg = StartupMsg::new(
        String::from(DEFAULT_USER),
        Some(String::from(DEFAULT_DATABASE)),
        None,
        None,
    );

    let client = Client::new(
        Ipv4Addr::from_str(DEFAULT_IP).expect("IPV4 address error"),
        DEFAULT_PORT,
    );

    match client.connect() {
        Ok(mut stream) => {
            if let Err(e) = client.authenticate(&mut stream, &mut startup_msg, DEFAULT_PASS) {
                eprintln!("Client Authentication: {}", e);
                return;
            }

            println!("Client connection\nUse Ctrl+c or type \"exit\" to end\n");

            let mut result_buf = BytesMut::new();
            let mut row_descr = BytesMut::new();

            loop {
                // Okay to use stream to send queries at this point
                print!("psql> ");
                let _ = io::stdout().flush(); // Flush immediately so we can have query in single line
                let mut msg = String::new();

                io::stdin().read_line(&mut msg).expect("Error in read line");
                msg = msg.trim().to_string();

                if msg.len() <= 1 {
                    // Ignore empty. Account for newline char
                    continue;
                }

                match msg.cmp(&String::from("exit")) {
                    Ordering::Equal => break,
                    _ => (),
                }

                match send_simple_query(&mut stream, &msg) {
                    Some(e) => {
                        eprintln!("Query Error: {}", e);
                        return;
                    }
                    _ => (),
                }

                loop {
                    match process_simple_query(&mut stream, &mut result_buf, &mut row_descr) {
                        Ok(done) => {
                            if done {
                                result_buf.put_u8(b'\n');
                                row_descr.put_u8(b'\n');
                                if let Err(e) = io::stdout().write_all(&row_descr) {
                                    eprintln!(
                                        "Error when writing to stdout for RowDescription: {}",
                                        e
                                    );
                                }
                                if let Err(e) = io::stdout().write_all(&result_buf) {
                                    eprintln!("Error when writing to stdout for DataRow: {}", e);
                                }
                                break;
                            }
                        }
                        Err(e) => {
                            eprintln!("Error processing simple query: {}", e);
                            return;
                        }
                    }

                    result_buf.put_u8(b'\n');
                    row_descr.put_u8(b'\n');
                    if let Err(e) = io::stdout().write_all(&row_descr) {
                        eprintln!("Error when writing to stdout for RowDescription: {}", e);
                    }
                    if let Err(e) = io::stdout().write_all(&result_buf) {
                        eprintln!("Error when writing to stdout for DataRow: {}", e);
                    }

                    result_buf.clear();
                    row_descr.clear();
                }
            }
        }
        Err(e) => {
            eprintln!("No stream available for client: {}", e);
            return;
        }
    }
}
