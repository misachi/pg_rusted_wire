use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bytes::{BufMut, BytesMut};
use std::fmt::{self, Debug};

pub const BUF_LEN: usize = 1024; // Buffer size for reading from the stream
pub const PROTOCOL_VERSION: i32 = 196608; // 3.0.0 in PostgreSQL protocol versioning

pub(crate) enum AuthenticationType {
    CleartextPassword,
    MD5Password,
    SASL,
}

#[derive(Debug)]
pub struct AuthError(pub String);

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub(crate) fn decoded_password(password: &str) -> Option<String> {
    match STANDARD.decode(&password) {
        Ok(pass) => Some(String::from_utf8_lossy(&pass).to_string()),
        Err(_) => None,
    }
}

pub(crate) fn encode_password(password: &str) -> String {
    STANDARD.encode(password)
}

pub(crate) fn put_cstring(buf: &mut BytesMut, input: &str) {
    buf.put_slice(input.as_bytes());
    buf.put_u8(b'\0');
}

/// Add length to specified position in the buffer
pub(crate) fn add_buf_len(buf: &mut BytesMut, start_pos: usize, buf_len: i32) {
    let mut temp_buf = vec![];
    temp_buf.put_i32(buf_len);
    buf[start_pos..start_pos + 4].copy_from_slice(&temp_buf);
}

pub(crate) fn get_auth_type(_type: i32) -> AuthenticationType {
    // This function is a placeholder for determining the authentication type
    // In a real implementation, this would likely involve a bit more complex logic
    if _type == 5 {
        AuthenticationType::MD5Password
    } else if _type == 10 {
        AuthenticationType::SASL
    } else if _type == 3 {
        AuthenticationType::CleartextPassword
    } else {
        panic!("Unknown authentication type: {:?}", _type);
    }
}

#[derive(Debug, Default)]
pub struct StartupMsg {
    pub(crate) protocol: i32,
    pub(crate) user: String,
    pub(crate) database: Option<String>,
    pub(crate) options: Option<String>,
    pub(crate) replication: Option<String>,
}

pub(crate) mod md5password {
    use bytes::{Buf, BufMut, BytesMut};
    use md5::{Digest, Md5};
    use std::io::{Read, Write};
    use std::net::TcpStream;

    use crate::auth::{AuthError, BUF_LEN, add_buf_len, decoded_password, encode_password};

    #[derive(Debug)]
    pub struct MD5Pass {
        password: String,
        user: String,
    }

    impl MD5Pass {
        pub(crate) fn new(password: &str, _user: &str) -> Self {
            MD5Pass {
                password: encode_password(password),
                user: _user.to_string(),
            }
        }

        /// Generate MD5 string to authenticate. See AuthenticationMD5Password in
        /// https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-START-UP
        /// Postgres does checks in  md5_crypt_verify in src/backend/libpq/crypt.c
        fn hash(&self, salt: &[u8]) -> Vec<u8> {
            // The final string is generated in SQL as
            // concat('md5', md5(concat(md5(concat(password, username)), random-salt))) with
            // md5() function returning a hex string

            let mut ret_vec = vec![];
            let mut hasher = Md5::new();
            ret_vec.put_slice(
                decoded_password(&self.password)
                    .expect("md5: Could not decode password")
                    .as_bytes(),
            );
            ret_vec.put_slice(self.user.as_bytes());
            hasher.update(&ret_vec);

            let mut res = hasher.finalize().to_vec();
            hasher = Md5::new();
            ret_vec.clear();
            ret_vec.put_slice(hex::encode(&res).as_bytes());
            ret_vec.put_slice(salt);
            hasher.update(&ret_vec[..]);

            res = hasher.finalize().to_vec();
            ret_vec.clear();
            ret_vec.put_slice(b"md5");
            ret_vec.put_slice(hex::encode(&res).as_bytes());
            ret_vec
        }

        pub(crate) fn authenticate(
            &self,
            stream: &mut TcpStream,
            _read_buf: &[u8],
        ) -> Result<(), AuthError> {
            let mut buf = BytesMut::with_capacity(BUF_LEN);
            buf.put_u8(b'p'); // identify message as PasswordMessage

            let start_pos = buf.len();
            buf.put_i32(0); // Placeholder for length
            buf.put_slice(&self.hash(&_read_buf[9..13]));
            buf.put_u8(0); // Terminate password string

            let total_len = buf[start_pos..].len() as i32;
            add_buf_len(&mut buf, start_pos, total_len);

            if let Err(e) = stream.write(&buf) {
                return Err(AuthError(format!(
                    "Failed to write to stream for clear md5 password initial response: {}",
                    e
                )));
            }

            buf.fill(0);
            match stream.read(&mut buf) {
                Ok(size) => {
                    let response = &buf[..size];

                    if response[0] != b'R' {
                        return Err(AuthError(format!(
                            "Invalid response in AuthenticationOk message: {:?}",
                            response[0]
                        )));
                    }

                    // Get word indicating if authentication was successful. Starts at byte 5, after byte identifier and length
                    let complete_tag = (&response[5..9]).get_i32();

                    // 0 signifies SASL authentication was successful(AuthenticationOk )
                    if complete_tag != 0 {
                        return Err(AuthError(format!("Auth incomplete: {}", complete_tag)));
                    }
                }
                Err(e) => {
                    return Err(AuthError(format!(
                        "Failed to read from stream for AuthenticationOk: {}",
                        e
                    )));
                }
            }

            return Ok(());
        }
    }
}

pub(crate) mod textpassword {
    use bytes::{Buf, BufMut, BytesMut};
    use std::io::{Read, Write};
    use std::net::TcpStream;

    use crate::auth::{AuthError, BUF_LEN, add_buf_len, decoded_password, encode_password};

    #[derive(Debug)]
    pub(crate) struct ClearTextPass {
        password: String,
    }

    impl ClearTextPass {
        pub(crate) fn new(password: &str, _user: &str) -> Self {
            ClearTextPass {
                password: encode_password(password),
            }
        }

        pub(crate) fn authenticate(
            &self,
            stream: &mut TcpStream,
            _read_buf: &[u8],
        ) -> Result<(), AuthError> {
            let mut buf = BytesMut::with_capacity(BUF_LEN);
            buf.put_u8(b'p'); // identify message as PasswordMessage

            let start_pos = buf.len();
            buf.put_i32(0); // Placeholder for length
            buf.put_slice(
                decoded_password(&self.password)
                    .expect("Could not decode password")
                    .as_bytes(),
            );
            buf.put_u8(0); // Terminate password string

            let total_len = buf[start_pos..].len() as i32;
            add_buf_len(&mut buf, start_pos, total_len);

            if let Err(e) = stream.write(&buf) {
                return Err(AuthError(format!(
                    "Failed to write to stream for clear text password initial response: {}",
                    e
                )));
            }

            buf.fill(0);
            match stream.read(&mut buf) {
                Ok(size) => {
                    let response = &buf[..size];

                    if response[0] != b'R' {
                        return Err(AuthError(format!(
                            "Invalid response in AuthenticationOk message: {:?}",
                            response[0]
                        )));
                    }

                    // Get word indicating if authentication was successful. Starts at byte 5, after byte identifier and length
                    let complete_tag = (&response[5..9]).get_i32();

                    // 0 signifies SASL authentication was successful(AuthenticationOk )
                    if complete_tag != 0 {
                        return Err(AuthError(format!("Auth incomplete: {}", complete_tag)));
                    }
                }
                Err(e) => {
                    return Err(AuthError(format!(
                        "Failed to read from stream for AuthenticationOk: {}",
                        e
                    )));
                }
            }

            return Ok(());
        }
    }
}

pub(crate) mod sasl {
    // See https://www.postgresql.org/docs/current/sasl-authentication.html#SASL-SCRAM-SHA-256
    // and RFCs 7677 and 5802 for details
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    use bytes::{Buf, BufMut, BytesMut};
    use hmac::{Hmac, Mac};
    use pbkdf2::hmac::Hmac as p_hmac;
    use sha2::{Digest, Sha256};
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::ops::BitXor;

    use crate::auth::{AuthError, BUF_LEN, add_buf_len, decoded_password, encode_password};

    #[derive(Debug)]
    pub(crate) struct SASL {
        password: String,
        user: String,
        pub nonce: String,
    }

    impl SASL {
        pub(crate) fn new(password: &str, user: &str) -> Self {
            SASL {
                // password: String::from_utf8(normalize_password(password)).expect("Unable to normalize password"),
                password: encode_password(password),
                user: user.to_string(),
                nonce: STANDARD.encode(rand::random::<[u8; 24]>()), // Random nonce
            }
        }

        fn retrieve_password(&self) -> Option<String> {
            decoded_password(&self.password)
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
        ) -> Result<(), AuthError> {
            let resp_str = String::from_utf8(resp_data.to_vec());
            if let Ok(val) = resp_str {
                let data = val.split(',').collect::<Vec<&str>>();
                let server_nonce = &data[0][2..];
                let decoded_salt = match STANDARD.decode(&data[1][2..]) {
                    Ok(s) => s,
                    Err(e) => {
                        return Err(AuthError(format!("Decoding error: {}", e)));
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
                    return Err(AuthError(format!(
                        "Failed to write to stream for client SASL response: {}",
                        e
                    )));
                }
                Ok(())
            } else {
                return Err(AuthError(format!(
                    "Failed to parse response data: {:?}",
                    resp_str
                )));
            }
        }

        fn handle_sasl_auth_ok(&self, stream: &mut TcpStream) -> Result<(), AuthError> {
            let mut buf = [0; BUF_LEN];
            match stream.read(&mut buf) {
                Ok(size) => {
                    let mut response = &buf[..size];
                    if response[0] != b'R' {
                        return Err(AuthError(format!(
                            "Invalid response in AuthenticationSASLFinal message: {:?}",
                            response[0]
                        )));
                    }

                    // Length of the message is 4 bytes after the byte identifier
                    let msg_len: i32 = (&buf[1..5]).get_i32();

                    // Get word indicating if authentication was successful. Starts at byte 5, after byte identifier and length
                    let mut complete_tag = (&buf[5..9]).get_i32();

                    // 12 signifies SASL authentication has completed(AuthenticationSASLFinal)
                    if complete_tag != 12 {
                        return Err(AuthError(format!(
                            "Authentication incomplete: {}",
                            complete_tag
                        )));
                    }

                    response = &buf[msg_len as usize + 1..size];
                    if response[0] != b'R' {
                        return Err(AuthError(format!(
                            "Invalid response in AuthenticationOk message: {:?}",
                            response[0]
                        )));
                    }

                    // Get word indicating if authentication was successful. Starts at byte 5, after byte identifier and length
                    complete_tag = (&response[5..9]).get_i32();

                    // 0 signifies SASL authentication was successful(AuthenticationOk )
                    if complete_tag != 0 {
                        return Err(AuthError(format!(
                            "Authentication incomplete: {}",
                            complete_tag
                        )));
                    }
                }
                Err(e) => return Err(AuthError(format!("Final Authentication Error: {}", e))),
            }
            Ok(())
        }

        fn handle_sasl_authentication(
            &self,
            stream: &mut TcpStream,
            auth_type: &[u8],
        ) -> Result<(), AuthError> {
            let pass =
                normalize_password(&self.retrieve_password().expect("Could not decode password"));

            let buf = self.initial_response_body(auth_type, &self.user, &self.nonce);

            if let Err(e) = stream.write(&buf) {
                return Err(AuthError(format!(
                    "Failed to write to stream for initial response: {}",
                    e
                )));
            }

            {
                let mut buf = [0; BUF_LEN];
                match stream.read(&mut buf) {
                    Ok(size) => {
                        let response = &buf[..size];

                        if response[0] == b'R' {
                            // 'R' for SASLFinalResponse

                            // Length of the message is 4 bytes after the byte identifier
                            let msg_len: i32 = (&buf[1..5]).get_i32();

                            // +1 since the limit is exclusive
                            let resp_data: &[u8] = &buf[9..msg_len as usize + 1];

                            if let Err(e) = self.send_client_sasl_response(&pass, stream, resp_data)
                            {
                                return Err(AuthError(format!(
                                    "Error handling client SASL response: {}",
                                    e
                                )));
                            }
                        } else {
                            return Err(AuthError(format!(
                                "Unexpected message type: {}",
                                response[0]
                            )));
                        }
                    }
                    Err(e) => {
                        return Err(AuthError(format!(
                            "Failed to read from stream for initial response: {}",
                            e
                        )));
                    }
                }
            }
            Ok(())
        }

        pub(crate) fn authenticate(
            &self,
            stream: &mut TcpStream,
            _read_buf: &[u8],
        ) -> Result<(), AuthError> {
            let auth_type_text: &[u8] = b"SCRAM-SHA-256";
            match self.handle_sasl_authentication(stream, &auth_type_text) {
                Ok(_) => {
                    if let Err(e) = self.handle_sasl_auth_ok(stream) {
                        return Err(AuthError(format!("Final Auth: {}", e)));
                    }
                    Ok(())
                }
                Err(e) => {
                    return Err(AuthError(format!(
                        "Error handling SASL authentication: {}",
                        e
                    )));
                }
            }
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
        replication: Option<String>,
    ) -> Self {
        StartupMsg {
            protocol: PROTOCOL_VERSION,
            user,
            database,
            options,
            replication,
        }
    }

    pub(crate) fn to_bytes(&mut self) -> BytesMut {
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

        match &self.replication {
            Some(dat) => {
                put_cstring(&mut bytes, "replication");
                put_cstring(&mut bytes, &dat);
                // self.replication = Some("".to_string());
            }
            _ => (),
        }

        put_cstring(&mut bytes, "");

        let total_len = bytes.len() as i32;
        add_buf_len(&mut bytes, start_pos, total_len);

        bytes
    }
}
