use bytes::{Buf, BufMut, BytesMut};
use std::fmt::Debug;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};

const BUF_LEN: usize = 1024; // Buffer size for reading from the stream
const PROTOCOL_VERSION: i32 = 196608; // 3.0.0 in PostgreSQL protocol versioning

#[derive(Debug)]
pub struct StartupMsg {
    protocol: i32,
    user: String,
    database: Option<String>,
    options: Option<String>,
    replication: Option<bool>,
}

#[derive(Debug)]
pub struct Client {
    addr: Ipv4Addr,
    port: u16,
    // stream: Option<TcpStream>,
}

impl Client {
    pub fn new(addr: Ipv4Addr, port: u16) -> Self {
        Client {
            addr: addr,
            port: port,
            // stream: None,
        }
    }

    pub fn connect(&self) -> Result<TcpStream, String> {
        match TcpStream::connect(SocketAddrV4::new(self.addr, self.port)) {
            Ok(stream) => Ok(stream),
            Err(e) => {
                return Err(format!("Failed to connect to server: {}", e));
            }
        }
    }

    pub fn authenticate(
        &self,
        stream: &mut TcpStream,
        startup_msg: &mut StartupMsg,
        pass: &str,
    ) -> Result<(), String> {
        let msg_bytes = startup_msg.to_bytes();
        let mut buf = [0; BUF_LEN]; // Buffer to read response

        if let Err(e) = stream.write(&msg_bytes) {
            return Err(format!("Failed to write to stream: {}", e));
        }

        loop {
            match stream.read(&mut buf) {
                Ok(size) => {
                    if size <= 0 {
                        return Err(format!("Server closed the connection"));
                    }

                    if size > BUF_LEN {
                        return Err(format!("Received data exceeds buffer size"));
                    }
                    let response = &buf[..size];

                    match response[0] {
                        b'R' => {
                            // 'R' for Authentication
                            let msg_len: i32 = (&buf[1..5]).get_i32();
                            let auth_type: &[u8] = &buf[9..msg_len as usize - 1];
                            let _val = &buf[5..9];

                            match get_auth_type(auth_type) {
                                AuthenticationType::CleartextPassword => {
                                    println!("Authentication: CleartextPassword")
                                }
                                AuthenticationType::MD5Password => {
                                    println!("Authentication: MD5Password")
                                }
                                AuthenticationType::SASL => {
                                    let sasl = sasl::SASL::new(&pass, &startup_msg.user);
                                    match sasl.handle_sasl_authentication(stream, &auth_type) {
                                        Ok(_) => {
                                            if let Err(e) = sasl.handle_sasl_auth_ok(stream) {
                                                return Err(format!("Final Auth: {}", e));
                                            }
                                            break;
                                        }
                                        Err(e) => {
                                            return Err(format!(
                                                "Error handling SASL authentication: {}",
                                                e
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                        _ => {
                            return Err(format!("Unexpected message type: {}", response[0]));
                        }
                    }
                }
                Err(e) => {
                    return Err(format!("Failed to read from stream: {}", e));
                }
            }
        }
        Ok(())
    }
}

pub enum AuthenticationType {
    CleartextPassword,
    MD5Password,
    SASL,
}

pub fn put_cstring(buf: &mut BytesMut, input: &str) {
    buf.put_slice(input.as_bytes());
    buf.put_u8(b'\0');
}

pub fn get_auth_type(_type: &[u8]) -> AuthenticationType {
    // This function is a placeholder for determining the authentication type
    // In a real implementation, this would likely involve a bit more complex logic
    if _type == b"md5\0" {
        AuthenticationType::MD5Password
    } else if _type == b"SCRAM-SHA-256" {
        AuthenticationType::SASL
    } else if _type == b"c\0\0\0" {
        AuthenticationType::CleartextPassword
    } else {
        panic!("Unknown authentication type: {:?}", _type);
    }
}

/// Add length to specified position in the buffer
fn add_buf_len(buf: &mut BytesMut, start_pos: usize, buf_len: i32) {
    let mut temp_buf = vec![];
    temp_buf.put_i32(buf_len);
    buf[start_pos..start_pos + 4].copy_from_slice(&temp_buf);
}

/// Example function for how data rows will be formatted
/// Each column will be separated by '|' and rows will be
/// separated by newline '\n' character e.g `1|1\n`
fn format_data_row(off: usize, num_cols: i16, resp_buf: &[u8], out_buf: &mut BytesMut) {
    let mut off = off; // coerce offset to mutable type
    for i in 0..num_cols {
        let col_len = (&resp_buf[off..off + 4]).get_i32();
        off += 4;
        let row_data = &resp_buf[off..off + col_len as usize];
        out_buf.put_slice(row_data);
        if (i + 1) < num_cols {
            out_buf.put_i8(b'|' as i8);
        }
        off += col_len as usize;
    }
    out_buf.put_u8(b'\n'); // Add newline for better readability
}

pub fn process_simple_query(
    stream: &mut TcpStream,
    msg: &str,
    data_buf: &mut BytesMut,
    row_descr: &mut BytesMut,
) -> Result<(), String> {
    let mut bytes = BytesMut::new();
    let mut overflowed = false;
    let mut skip_bytes = 0;
    let mut overflow_buf = BytesMut::new();
    let mut buf = [0; BUF_LEN]; // Buffer to read response

    bytes.put_u8(b'Q'); // Query message type
    let start_pos = bytes.len();
    bytes.put_i32(0); // Placeholder for length
    bytes.put_slice(msg.as_bytes()); // Query
    bytes.put_u8(0);

    let buf_len = bytes[start_pos..].len() as i32;
    add_buf_len(&mut bytes, start_pos, buf_len);

    if let Err(e) = stream.write(&bytes) {
        return Err(format!("Failed to write query to stream: {}", e));
    }

    'attempt_read: loop {
        match stream.read(&mut buf) {
            Ok(mut size) => {
                if size <= 0 {
                    return Err("Server closed the connection".to_string());
                }

                if size > BUF_LEN {
                    return Err("Received data exceeds buffer size".to_string());
                }

                let response = &buf[..size];
                let mut off = 0;
                let cpy_size = size;
                while size > 0 && off < cpy_size {
                    match response[off..][0] {
                        b'C' => {
                            // 'C' for CommandComplete
                            let msg_len: i32 = (&buf[off + 1..off + 5]).get_i32();
                            let cmd_tag: &[u8] = &buf[off + 5..off + msg_len as usize];

                            println!(
                                "Done with command tag: {:?}",
                                String::from_utf8_lossy(cmd_tag)
                            );
                            break 'attempt_read;
                        }
                        b'I' => {
                            // 'I' for EmptyQueryResponse
                            let msg_len: i32 = (&buf[1..5]).get_i32();
                            println!("Empty Query Response with length: {}", msg_len);
                            break 'attempt_read;
                        }
                        b'E' => {
                            // 'E' for ErrorResponse
                            let msg_len: i32 = (&buf[1..5]).get_i32();
                            let err_msg: &[u8] = &buf[6..msg_len as usize];

                            eprintln!("Error message: {:?}", String::from_utf8_lossy(err_msg));
                            break 'attempt_read;
                        }
                        b'T' => {
                            // T for RowDescription
                            let msg_len: i32 = (&buf[1..5]).get_i32();
                            let row_desc: &[u8] = &buf[7..msg_len as usize];

                            row_descr.put_slice(row_desc);

                            size = if size > msg_len as usize {
                                size - msg_len as usize - 1
                            } else {
                                0
                            };
                            off += msg_len as usize + 1;
                        }
                        b'D' => {
                            // 'D' for DataRow
                            let msg_len: i32 = (&buf[off + 1..off + 5]).get_i32();

                            // Check if the message length exceeds the available data
                            // If so, put in overflow buffer to be handled later
                            if off + msg_len as usize + 1 > cpy_size {
                                overflowed = true;
                                overflow_buf.put_slice(&buf[off..]);
                                skip_bytes = (msg_len + 1) - buf[off..].len() as i32;
                                buf.fill(0); // We'll need to reuse buffer
                                break;
                            }
                            let num_cols = (&buf[off + 5..off + 7]).get_i16();

                            let val_off = off + 5 + 2; // Account for byte, 4 byte for content size, 2 bytes for column number
                            format_data_row(val_off, num_cols, &buf, data_buf);

                            size = if size > msg_len as usize {
                                size - msg_len as usize - 1
                            } else {
                                0
                            };
                            off += msg_len as usize + 1;
                        }
                        _ => {
                            // Handle incomplete data in read buffer
                            if overflowed {
                                overflow_buf.put_slice(&buf[..skip_bytes as usize]);

                                size -= skip_bytes as usize;
                                off += skip_bytes as usize;

                                let num_cols = (&overflow_buf[5..7]).get_i16();

                                let val_off = 5 + 2; // Account for byte, 4 byte for content size, 2 bytes for column number
                                format_data_row(val_off, num_cols, &overflow_buf, data_buf);

                                overflow_buf.clear();
                                overflowed = false;
                                skip_bytes = 0;
                            } else {
                                let msg_len: i32 = (&buf[1..5]).get_i32();
                                eprintln!(
                                    "Unexpected message type when processing simple query: {:?}",
                                    String::from_utf8_lossy(&buf[0].to_le_bytes())
                                );
                                size = if size >= msg_len as usize {
                                    size - msg_len as usize
                                } else {
                                    0
                                };
                            }
                        }
                    }
                }
            }
            Err(e) => {
                return Err(format!("Failed to read from stream: {}", e));
            }
        }
    }

    Ok(())
}

mod sasl {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    use bytes::{Buf, BufMut, BytesMut};
    use hmac::{Hmac, Mac};
    use pbkdf2::hmac::Hmac as p_hmac;
    use sha2::{Digest, Sha256};
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::ops::BitXor;

    use crate::BUF_LEN;
    use crate::add_buf_len;

    #[derive(Debug)]
    pub struct SASL {
        password: String,
        user: String,
        pub nonce: String,
    }

    impl SASL {
        pub fn new(password: &str, user: &str) -> Self {
            SASL {
                // password: String::from_utf8(normalize_password(password)).expect("Unable to normalize password"),
                password: STANDARD.encode(password),
                user: user.to_string(),
                nonce: STANDARD.encode(rand::random::<[u8; 24]>()), // Random nonce
            }
        }

        fn retrieve_password(&self) -> Option<String> {
            match STANDARD.decode(&self.password) {
                Ok(pass) => Some(String::from_utf8_lossy(&pass).to_string()),
                Err(_) => None,
            }
        }

        fn initial_response_body(&self, auth_type: &[u8], user: &str, nonce: &str) -> BytesMut {
            let mut buf = BytesMut::new();
            let resp_msg = format!("n,,n={},r={}", user, nonce);
            buf.put_u8(b'p'); // SASLInitialResponse

            let start_pos = buf.len();
            buf.put_i32(0); // Placeholder for length
            buf.put_slice(auth_type); // Mechanism name
            buf.put_u8(0);
            buf.put_i32(resp_msg.len() as i32); // Length of the initial response
            buf.put_slice(resp_msg.as_bytes()); // Initial response

            let total_len = buf[start_pos..].len() as i32;
            add_buf_len(&mut buf, start_pos, total_len);

            buf
        }

        fn sasl_response_body(&self, server_nonce: &str, client_proof: &[u8]) -> BytesMut {
            let mut buf = BytesMut::new();
            let client_msg = format!(
                "c=biws,r={},p={}",
                server_nonce,
                STANDARD.encode(client_proof)
            );
            buf.put_u8(b'p'); // SASLResponse
            buf.put_i32(client_msg.len() as i32 + 4); // Length of the message
            buf.put_slice(client_msg.as_bytes()); // Client message
            buf
        }

        // Handles the client SASL response: See RFCs 7677 and 5802 for details
        fn send_client_sasl_response(
            &self,
            pass: &[u8],
            stream: &mut TcpStream,
            resp_data: &[u8],
        ) -> Result<(), String> {
            let resp_str = String::from_utf8(resp_data.to_vec());
            if let Ok(val) = resp_str {
                let data = val.split(',').collect::<Vec<&str>>();
                let server_nonce = &data[0][2..];
                let decoded_salt = match STANDARD.decode(&data[1][2..]) {
                    Ok(s) => s,
                    Err(e) => {
                        return Err(format!("Decoding error: {}", e));
                    }
                };

                let iterations = &data[2][2..].parse::<usize>().unwrap_or_else(|_| 4096);

                let salted_password = hi(&pass, &decoded_salt, *iterations);

                let client_key = hmac(&salted_password, b"Client Key");
                let stored_key = h(&client_key);

                let auth_message = format!(
                    "{},{},{}",
                    format!("n={},r={}", self.user, self.nonce), // client-first-message-bare
                    String::from_utf8(resp_data.to_vec()).expect("Invalid utf-8"), // server-first-message
                    format!("c=biws,r={}", server_nonce) // client-final-message-without-proof
                );
                let client_signature = hmac(&stored_key, &auth_message.as_bytes());
                let client_proof = xor(&client_key, &client_signature);

                let buf = self.sasl_response_body(server_nonce, &client_proof);

                if let Err(e) = stream.write(&buf) {
                    return Err(format!(
                        "Failed to write to stream for client SASL response: {}",
                        e
                    ));
                }
                Ok(())
            } else {
                return Err(format!("Failed to parse response data: {:?}", resp_str));
            }
        }

        fn handle_sasl_auth_ok(&self, stream: &mut TcpStream) -> Result<(), String> {
            let mut buf = [0; BUF_LEN];
            match stream.read(&mut buf) {
                Ok(size) => {
                    let mut response = &buf[..size];
                    if response[0] != b'R' {
                        return Err(format!(
                            "Invalid response in AuthenticationSASLFinal message: {:?}",
                            response[0]
                        ));
                    }

                    let msg_len: i32 = (&buf[1..5]).get_i32();
                    let mut complete_tag = (&buf[5..9]).get_i32();
                    if complete_tag != 12 {
                        // 12 signifies SASL authentication has completed(AuthenticationSASLFinal)
                        return Err(format!("Authentication incomplete: {}", complete_tag));
                    }

                    response = &buf[msg_len as usize + 1..size];
                    if response[0] != b'R' {
                        return Err(format!(
                            "Invalid response in AuthenticationOk message: {:?}",
                            response[0]
                        ));
                    }

                    complete_tag = (&response[5..9]).get_i32();
                    if complete_tag != 0 {
                        // 0 signifies SASL authentication was successful(AuthenticationOk )
                        return Err(format!("Authentication incomplete: {}", complete_tag));
                    }
                }
                Err(e) => return Err(format!("Final Authentication Error: {}", e)),
            }
            Ok(())
        }

        fn handle_sasl_authentication(
            &self,
            stream: &mut TcpStream,
            auth_type: &[u8],
        ) -> Result<(), String> {
            let pass =
                normalize_password(&self.retrieve_password().expect("Could not decode password"));

            let buf = self.initial_response_body(auth_type, &self.user, &self.nonce);

            if let Err(e) = stream.write(&buf) {
                return Err(format!(
                    "Failed to write to stream for initial response: {}",
                    e
                ));
            }

            {
                let mut buf = [0; BUF_LEN];
                match stream.read(&mut buf) {
                    Ok(size) => {
                        let response = &buf[..size];

                        if response[0] == b'R' {
                            // 'R' for SASLFinalResponse
                            let msg_len: i32 = (&buf[1..5]).get_i32();
                            let resp_data: &[u8] = &buf[9..msg_len as usize + 1]; // +1 since the limit is exlusive

                            if let Err(e) = self.send_client_sasl_response(&pass, stream, resp_data)
                            {
                                return Err(format!("Error handling client SASL response: {}", e));
                            }
                        } else {
                            return Err(format!("Unexpected message type: {}", response[0]));
                        }
                    }
                    Err(e) => {
                        return Err(format!(
                            "Failed to read from stream for initial response: {}",
                            e
                        ));
                    }
                }
            }
            Ok(())
        }
    }

    fn hi(normalized_password: &[u8], salt: &[u8], iterations: usize) -> Vec<u8> {
        let mut buf = [0u8; 32];

        match pbkdf2::pbkdf2::<p_hmac<Sha256>>(
            normalized_password,
            salt,
            iterations as u32,
            &mut buf,
        ) {
            Ok(_) => buf.to_vec(),
            Err(e) => panic!("Failed to derive key: {}", e),
        }
    }

    fn hmac(key: &[u8], msg: &[u8]) -> Vec<u8> {
        // Use HMAC with SHA256 to generate a keyed hash
        let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC generation failed");
        mac.update(msg);
        mac.finalize().into_bytes().to_vec()
    }

    fn h(msg: &[u8]) -> Vec<u8> {
        Sha256::digest(msg).as_slice().to_vec()
    }

    fn xor(lhs: &[u8], rhs: &[u8]) -> Vec<u8> {
        lhs.iter()
            .zip(rhs.iter())
            .map(|(l, r)| l.bitxor(r))
            .collect()
    }

    fn normalize_password(password: &str) -> Vec<u8> {
        match stringprep::saslprep(password) {
            Ok(prepped) => prepped.into_owned().into_bytes(),
            Err(_) => Vec::from(password.as_bytes()),
        }
    }
}

impl StartupMsg {
    pub fn new(
        user: String,
        database: Option<String>,
        options: Option<String>,
        replication: Option<bool>,
    ) -> Self {
        StartupMsg {
            protocol: PROTOCOL_VERSION,
            user,
            database,
            options,
            replication,
        }
    }

    fn to_bytes(&mut self) -> BytesMut {
        let mut bytes: BytesMut = BytesMut::new();
        let start_pos = bytes.len();

        bytes.put_i32(0); // Placeholder for total length
        bytes.put_i32(self.protocol);
        put_cstring(&mut bytes, "user");
        put_cstring(&mut bytes, &self.user);
        if let Some(ref db) = self.database {
            put_cstring(&mut bytes, "database");
            put_cstring(&mut bytes, db);
        }
        if let Some(ref opts) = self.options {
            put_cstring(&mut bytes, "options");
            put_cstring(&mut bytes, opts);
        }
        put_cstring(&mut bytes, "replication");
        match self.replication {
            None => {
                self.replication = Some(false);
            }
            _ => {}
        }
        put_cstring(&mut bytes, &self.replication.unwrap_or(false).to_string());
        put_cstring(&mut bytes, "");

        let total_len = bytes.len() as i32;
        add_buf_len(&mut bytes, start_pos, total_len);

        bytes
    }
}
