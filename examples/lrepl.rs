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
    msg: String,
    state: &mut QueryState,
    result_buf: &mut [u8],
    row_descr: &mut BytesMut,
) {
    match send_simple_query(stream, &msg) {
        Some(e) => {
            eprintln!("Query Error: {}", e);
            return;
        }
        _ => (),
    }

    fun_name(stream, state, result_buf, row_descr);
}

fn fun_name(
    stream: &mut std::net::TcpStream,
    state: &mut QueryState,
    result_buf: &mut [u8],
    row_descr: &mut BytesMut,
) {
    let buf_is_cleared = false;
    loop {
        state.data_buf_off = 0;
        match process_simple_query(stream, result_buf, row_descr, state) {
            Ok(done) => {
                if done {
                    row_descr.put_u8(b'\n');
                    if let Err(e) = io::stdout().write_all(&row_descr) {
                        eprintln!("Error when writing to stdout for RowDescription: {}", e);
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

        row_descr.put_u8(b'\n');
        if let Err(e) = io::stdout().write_all(&result_buf[..state.data_buf_off]) {
            eprintln!("Error when writing to stdout for DataRow: {}", e);
        }

        result_buf.fill(0);

        if !buf_is_cleared {
            // Clear only once
            row_descr.clear();
        }
    }
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
            process_simple(
                &mut stream,
                msg,
                &mut state,
                &mut result_buf,
                &mut row_descr,
            );
            let start_lsn: &str = "0/3CD2800";

            result_buf.fill(0);
            row_descr.clear();

            // msg = String::from("CREATE_REPLICATION_SLOT test_slot3 LOGICAL pgoutput");
            // process_simple(&mut stream, msg, &mut state, &mut result_buf, &mut row_descr);

            // result_buf.fill(0);
            // row_descr.clear();

            msg = String::from(format!(
                "START_REPLICATION SLOT test_slot3 LOGICAL {start_lsn} (proto_version '4', publication_names 'pub1')"
            ));
            process_simple(
                &mut stream,
                msg,
                &mut state,
                &mut result_buf,
                &mut row_descr,
            );

            loop {
                fun_name(&mut stream, &mut state, &mut result_buf, &mut row_descr);
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
