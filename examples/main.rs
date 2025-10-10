use bytes::BytesMut;
use std::net::Ipv4Addr;
use std::str::FromStr;

use pg_rusted_wire::auth::StartupMsg;
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
    let mut state = QueryState::default();

    match client.connect() {
        Ok(mut stream) => {
            if let Err(e) = client.authenticate(&mut stream, &mut startup_msg, DEFAULT_PASS) {
                eprintln!("Client Authentication: {}", e);
                return;
            }

            // Okay to use stream to send queries at this point
            let mut msg = "SELECT * FROM foo LIMIT 100;"; // First Query
            let mut result_buf = [0; 4096];
            let mut row_descr = BytesMut::new();

            match send_simple_query(&mut stream, &msg) {
                Some(e) => {
                    eprintln!("Query Error: {}", e);
                    return;
                }
                _ => (),
            }

            if let Err(e) =
                process_simple_query(&mut stream, &mut result_buf, &mut row_descr, &mut state)
            {
                eprintln!("Error processing simple query: {}", e);
                return;
            }

            // Just print results here
            println!("Row Description: {:?}", String::from_utf8_lossy(&row_descr));
            println!(
                "Data Buffer: {:?}",
                String::from_utf8_lossy(&result_buf[..state.data_buf_off])
            );

            result_buf.fill(0);
            row_descr.clear();

            msg = "SELECT * FROM foo WHERE id < 5 LIMIT 100;"; // Second Query

            match send_simple_query(&mut stream, &msg) {
                Some(e) => {
                    eprintln!("Query Error: {}", e);
                    return;
                }
                _ => (),
            }
            if let Err(e) =
                process_simple_query(&mut stream, &mut result_buf, &mut row_descr, &mut state)
            {
                eprintln!("Error processing simple query: {}", e);
                return;
            }

            println!("Row Description: {:?}", String::from_utf8_lossy(&row_descr));
            println!(
                "Data Buffer: {:?}",
                String::from_utf8_lossy(&result_buf[..state.data_buf_off])
            );
        }
        Err(e) => {
            eprintln!("No stream available for client: {}", e);
            return;
        }
    }
}
