use bytes::BytesMut;
use std::net::Ipv4Addr;
use std::str::FromStr;

use pg_rusted_wire::wire::*;

const DEFAULT_USER: &str = "postgres";
const DEFAULT_PORT: u16 = 5432;
const DEFAULT_IP: &str = "127.0.0.1";
const DEFAULT_PASS: &str = "pass12234";

fn main() {
    let mut startup_msg = StartupMsg::new(
        String::from(DEFAULT_USER),
        Some(String::from(DEFAULT_USER)),
        None,
        None,
    );

    let client = Client::new(Ipv4Addr::from_str(DEFAULT_IP).expect("IPV4 address error"), DEFAULT_PORT);

    match client.connect() {
        Ok(mut stream) => {
            if let Err(e) = client.authenticate(&mut stream, &mut startup_msg, DEFAULT_PASS) {
                eprintln!("Client Authentication: {}", e);
                return;
            }

            // Okay to use stream to send queries at this point
            let mut msg = "SELECT * FROM foo;"; // First Query
            let result_buf = &mut BytesMut::new();
            let row_descr = &mut BytesMut::new();

            if let Err(e) = process_simple_query(&mut stream, msg, result_buf, row_descr) {
                eprintln!("Error processing simple query: {}", e);
                return;
            }

            // Just print results here
            println!("Row Description: {:?}", String::from_utf8_lossy(row_descr));
            println!("Data Buffer: {:?}", String::from_utf8_lossy(result_buf));

            // Reset buffers for reuse before sending second query
            result_buf.clear();
            row_descr.clear();

            msg = "SELECT * FROM foo WHERE id < 5;"; // Second Query
            if let Err(e) = process_simple_query(&mut stream, msg, result_buf, row_descr) {
                eprintln!("Error processing simple query: {}", e);
                return;
            }
            println!("Row Description: {:?}", String::from_utf8_lossy(row_descr));
            println!("Data Buffer: {:?}", String::from_utf8_lossy(result_buf));
        }
        Err(e) => {
            eprintln!("No stream available for client: {}", e);
            return;
        }
    }
}
