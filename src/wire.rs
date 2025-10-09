use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bytes::{Buf, BufMut, BytesMut};
use chrono::{DateTime, Utc};
use pyo3::ffi::c_str;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::fs::{File, OpenOptions, read_to_string};
use std::io;
use std::io::{Read, Seek, Write, stdout};
use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};
use std::path::Path;
use std::{thread, time};

use std::ffi::CString;
// use std::fs;

const BUF_LEN: usize = 1024; // Buffer size for reading from the stream
pub const PROTOCOL_VERSION: i32 = 196608; // 3.0.0 in PostgreSQL protocol versioning
const REPLICATION_META_FILE: &str = "meta.json";
const REPLICATION_DATA_FILE_EXT: &str = ".data";
const WAIT_OFF_CPU: u64 = 50; // In milliseconds

#[derive(Debug)]
pub struct SimpleQueryError(pub String);

impl fmt::Display for SimpleQueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug)]
pub struct AuthError(pub String);

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug)]
pub struct ReplicationError(pub String);

impl fmt::Display for ReplicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Replication: {}", self.0)
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct TableInfo {
    name: String,
    oid: i32,
    namespace: String,
    ncols: i16,        // Number of columns
    cols: Vec<String>, // Column names
    slot: String,      // Slot name
    publication: String,
    snapshot_done: bool,
    out_resource: OutResource,
}

impl TableInfo {
    fn new(out_res: OutResource, name: &str) -> Self {
        Self {
            name: name.to_string(),
            slot: format!("{}_slot", name),
            out_resource: out_res,
            ..Default::default()
        }
    }
}

pub trait DoIO {
    fn write(&self, data: &[u8]) -> io::Result<()>;
    fn read(&self, data: &mut [u8], pos: u64) -> io::Result<usize>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OutResource {
    CSVFile(String),
    Iceberg(String),
}

impl Default for OutResource {
    fn default() -> Self {
        OutResource::CSVFile(String::new())
    }
}

impl DoIO for OutResource {
    fn write(&self, data: &[u8]) -> io::Result<()> {
        match self {
            OutResource::CSVFile(path) => {
                let path = Path::new(&path);

                let mut handle = OpenOptions::new().append(true).create(true).open(path)?;

                handle.write_all(data)?;
                handle.sync_all()?; // Flush to disk immediately

                Ok(())
            }
            OutResource::Iceberg(path) => Python::attach(|py| {
                let py_app = CString::new(read_to_string(Path::new("../pyiceberg.py"))?)?;
                let app: Py<PyAny> =
                    PyModule::from_code(py, py_app.as_c_str(), c_str!("pyiceberg.py"), c_str!(""))?
                        .getattr("write_to_table")?
                        .into();

                app.call1(py, (data, Path::new(path)))?;

                Ok(())
            }),
        }
    }

    fn read(&self, data: &mut [u8], pos: u64) -> io::Result<usize> {
        match self {
            OutResource::CSVFile(path) => {
                let mut handle = File::open(&path)?;
                let _ = handle.seek(io::SeekFrom::Start(pos));
                return handle.read(data);
            }
            OutResource::Iceberg(_) => {
                // Placeholder for Iceberg read logic
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Iceberg read not implemented",
                ))
            }
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ReplicationInfo {
    sys_id: String,
    timeline: u8,
    start_lsn: String,
    table: HashMap<String, TableInfo>,
    meta_file_path: String,
}

impl ReplicationInfo {
    fn new(config_dir: &str) -> Self {
        Self {
            meta_file_path: format!("{}/{}", config_dir, REPLICATION_META_FILE),
            ..Default::default()
        }
    }

    fn encode(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    fn decode(&self, data: &str) -> Result<ReplicationInfo, ReplicationError> {
        let info: ReplicationInfo = serde_json::from_str(data).expect("Error decoding from json");
        Ok(info)
    }

    fn path_exists(path: &str) -> bool {
        Path::new(&path).exists()
    }

    // Load replication state from file
    fn load(&self) -> io::Result<ReplicationInfo> {
        if !ReplicationInfo::path_exists(&self.meta_file_path) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Info Path does not exist",
            ));
        }
        let path = Path::new(&self.meta_file_path);
        let mut buf = vec![];
        let mut handle = File::open(path)?;

        handle.read_to_end(&mut buf)?;

        match self.decode(&String::from_utf8_lossy(&buf)) {
            Ok(data) => Ok(data),
            Err(e) => {
                eprintln!("ReplicationInfo: {}", e);
                Err(io::Error::new(io::ErrorKind::Other, e.0))
            }
        }
    }

    // Save replication state to file
    fn dump(&self) -> io::Result<()> {
        let path = Path::new(&self.meta_file_path);
        let mut buf = [0; BUF_LEN];

        let mut handle = File::create(path)?;

        match self.encode() {
            Ok(serialized_data) => {
                buf[..serialized_data.as_bytes().len()].copy_from_slice(serialized_data.as_bytes());
                handle.write_all(&buf[..serialized_data.as_bytes().len()])?;
                handle.sync_all()?; // Flush to disk immediately
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Replication {
    info: ReplicationInfo,
    config_dir: Option<String>,
}

impl Replication {
    pub fn new(config_dir: &str) -> Self {
        let repl_info = ReplicationInfo::new(config_dir);
        match repl_info.load() {
            Ok(info) => Self {
                info: info,
                config_dir: Some(config_dir.to_string()),
            },
            Err(_) => Self {
                info: repl_info,
                config_dir: Some(config_dir.to_string()),
            },
        }
    }

    fn snapshot_taken(&self, table: &str) -> bool {
        if let Some(info) = self.info.table.get(table) {
            return info.snapshot_done;
        }
        false
    }

    fn system_valid(&self, sys_id: &str) -> bool {
        self.info.sys_id == sys_id
    }

    /// Confirm the replication slot exists
    fn check_slot_exists(
        &mut self,
        stream: &mut std::net::TcpStream,
        table: &str,
    ) -> Result<bool, ReplicationError> {
        let mut result_buf = [0; BUF_LEN];
        let mut row_descr = BytesMut::new();

        let mut state = QueryState::default();
        let table_info: &TableInfo;

        if let Some(info) = self.info.table.get(table) {
            table_info = info;
        } else {
            return Err(ReplicationError(format!("Table does not exist")));
        }

        let msg = String::from(format!(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = '{}' AND slot_type = 'logical'",
            table_info.slot
        ));
        if let Some(e) = send_simple_query(stream, &msg) {
            return Err(ReplicationError(format!(
                "Check Replication Slot Query Error: {}",
                e
            )));
        }

        if let Err(e) = process_simple(stream, &mut state, &mut result_buf, &mut row_descr, self) {
            return Err(ReplicationError(format!(
                "Check Replication Slot Exists: {}",
                e
            )));
        }

        if state.data_buf_off > 0 {
            return Ok(true);
        }
        Ok(false)
    }

    /// Identify the server and populate ReplicationInfo
    fn identify(&mut self, stream: &mut std::net::TcpStream) -> Result<(), ReplicationError> {
        let mut result_buf = [0; BUF_LEN];
        let mut row_descr = BytesMut::new();

        let mut state = QueryState::default();

        let msg = String::from("IDENTIFY_SYSTEM");

        if let Some(e) = send_simple_query(stream, &msg) {
            return Err(ReplicationError(format!("Identify Query Error: {}", e)));
        }
        if let Err(e) = process_simple(stream, &mut state, &mut result_buf, &mut row_descr, self) {
            return Err(ReplicationError(format!("IDENTIFY SYSTEM Error: {}", e)));
        }
        let parts: Vec<&[u8]> = result_buf.split(|&b| b == b'|').collect();

        let sys_id = String::from_utf8_lossy(&parts[0]).to_string();
        if !self.system_valid(&sys_id) && self.info.sys_id.len() > 0 {
            return Err(ReplicationError(format!("Invalid System ID")));
        }

        let timeline = String::from_utf8_lossy(parts[1]);

        self.info.sys_id = sys_id;
        self.info.timeline = timeline.parse().unwrap();
        self.info.start_lsn = String::from_utf8_lossy(&parts[2]).to_string();

        if let Err(e) = self.info.dump() {
            return Err(ReplicationError(format!("Error saving state: {}", e)));
        }
        Ok(())
    }

    /// Create a logical replication slot if it does not exist
    fn create_slot(
        &mut self,
        stream: &mut std::net::TcpStream,
        table: &str,
    ) -> Result<(), ReplicationError> {
        let mut result_buf = [0; BUF_LEN];
        let mut row_descr = BytesMut::new();

        let mut state = QueryState::default();
        let table_info: &TableInfo = self.info.table.get(table).expect("Table must exist");

        let msg = String::from(format!(
            "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput",
            table_info.slot
        ));

        if let Err(e) = self.identify(stream) {
            return Err(ReplicationError(format!("System Identity: {}", e)));
        }

        match self.check_slot_exists(stream, table) {
            Ok(b) => {
                if b {
                    return Ok(());
                }
            }
            Err(e) => return Err(ReplicationError(format!("Check slot: {}", e))),
        }

        if let Some(e) = send_simple_query(stream, &msg) {
            return Err(ReplicationError(format!("Create Slot Query Error: {}", e)));
        }
        if let Err(e) = process_simple(stream, &mut state, &mut result_buf, &mut row_descr, self) {
            return Err(ReplicationError(format!(
                "Replication Slot Create Error: {}",
                e
            )));
        }

        Ok(())
    }

    /// Perform a COPY TO STDOUT to get a consistent snapshot of the table
    fn copy_snapshot(
        &mut self,
        stream: &mut std::net::TcpStream,
        table: &str,
    ) -> Result<(), ReplicationError> {
        let mut result_buf = [0; 4096];
        let mut row_descr = BytesMut::new();

        let mut state = QueryState::default();
        let msg = String::from(format!("COPY {} TO STDOUT DELIMITER ','", table));

        if let Some(e) = send_simple_query(stream, &msg) {
            return Err(ReplicationError(format!("Copy Error: {}", e)));
        }

        loop {
            match process_simple(stream, &mut state, &mut result_buf, &mut row_descr, self) {
                Ok(SimpleQueryCompletion::CommandComplete) => break,
                Ok(SimpleQueryCompletion::CopyComplete) => {
                    let table_info: &mut TableInfo;
                    if let Some(info) = self.info.table.get_mut(table) {
                        table_info = info;
                    } else {
                        return Err(ReplicationError(format!("Table does not exist")));
                    }

                    let out_res = table_info.out_resource.clone();

                    table_info.snapshot_done = true;
                    self.info.dump().expect("Error saving state");

                    self.write_to(&out_res, &result_buf, &state);
                    break;
                }
                Ok(SimpleQueryCompletion::InProgress) => {
                    let table_info: &mut TableInfo;
                    if let Some(info) = self.info.table.get_mut(table) {
                        table_info = info;
                    } else {
                        return Err(ReplicationError(format!("Table does not exist")));
                    }

                    let out_res = table_info.out_resource.clone();

                    self.write_to(&out_res, &result_buf, &state);
                }
                Ok(SimpleQueryCompletion::InProgressReadStream) => {
                    let table_info: &mut TableInfo;
                    if let Some(info) = self.info.table.get_mut(table) {
                        table_info = info;
                    } else {
                        return Err(ReplicationError(format!("Table does not exist")));
                    }

                    let out_res = table_info.out_resource.clone();

                    self.write_to(&out_res, &result_buf, &state);
                }
                Ok(SimpleQueryCompletion::CommandError) => {
                    break;
                }
                Ok(SimpleQueryCompletion::CopyError) => {
                    break;
                }
                Err(e) => {
                    return Err(ReplicationError(format!(
                        "Error processing simple query: {}",
                        e
                    )));
                }
                _ => (),
            }

            result_buf.fill(0);
            row_descr.clear();
        }
        Ok(())
    }

    fn write_to(&mut self, out_res: &OutResource, result_buf: &[u8], state: &QueryState) {
        let mut off = state.data_buf_off;
        if off <= 0 {
            off = state.recycle_buf_off;
        }

        // Ignore if no data to write
        if off <= 0 {
            return;
        }
        if let Err(e) = out_res.write(&result_buf[..off]) {
            eprintln!("Error when writing to stdout for DataRow: {}", e);
        }
    }

    /// Start the replication stream
    fn start(
        &mut self,
        stream: &mut std::net::TcpStream,
        table: &str,
    ) -> Result<(), ReplicationError> {
        let mut result_buf = [0; 4096];
        let mut row_descr = BytesMut::new();

        let mut state = QueryState::default();
        let table_info: &TableInfo;

        if let Some(info) = self.info.table.get(table) {
            table_info = info;
        } else {
            return Err(ReplicationError(format!("Table does not exist")));
        }
        let out_res = table_info.out_resource.clone();

        let msg = String::from(format!(
            "START_REPLICATION SLOT {} LOGICAL {} (proto_version '4', publication_names '{}')",
            table_info.slot, self.info.start_lsn, table_info.publication
        ));

        if let Some(e) = send_simple_query(stream, &msg) {
            return Err(ReplicationError(format!(
                "START REPLICATION Command Error: {}",
                e
            )));
        }

        loop {
            match process_simple(stream, &mut state, &mut result_buf, &mut row_descr, self) {
                Ok(SimpleQueryCompletion::CommandComplete) => break,
                Ok(SimpleQueryCompletion::CopyComplete) => {
                    self.write_to(&out_res, &result_buf, &state)
                }
                Ok(SimpleQueryCompletion::InProgress) => {
                    self.write_to(&out_res, &result_buf, &state)
                }
                Ok(SimpleQueryCompletion::InProgressReadStream) => {
                    self.write_to(&out_res, &result_buf, &state)
                }
                Ok(SimpleQueryCompletion::CommandError) => {
                    break;
                }
                Ok(SimpleQueryCompletion::CopyError) => {
                    break;
                }
                Ok(SimpleQueryCompletion::ReadStreamTimeout) => {
                    return Err(ReplicationError(format!("Server closed the connection")));
                }
                Err(e) => {
                    return Err(ReplicationError(format!(
                        "Error processing simple query: {}",
                        e
                    )));
                }
                _ => (),
            }

            result_buf.fill(0);
            row_descr.clear();

            // Slow down a bit to avoid busy waiting
            thread::sleep(time::Duration::from_millis(WAIT_OFF_CPU));
        }
        Ok(())
    }
}

pub struct QueryState {
    pub overflowed: bool,
    pub skip_bytes: u32,
    pub overflow_buf: BytesMut,
    pub data_buf_off: usize,
    recycle_buf_off: usize,
}

impl Default for QueryState {
    fn default() -> Self {
        Self {
            overflowed: false,
            skip_bytes: 0,
            overflow_buf: BytesMut::with_capacity(BUF_LEN),
            data_buf_off: 0,
            recycle_buf_off: 0,
        }
    }
}

#[derive(Debug, Default)]
pub struct StartupMsg {
    protocol: i32,
    user: String,
    database: Option<String>,
    options: Option<String>,
    replication: Option<String>,
}

#[derive(Debug)]
pub struct Client {
    addr: Ipv4Addr,
    port: u16,
    replication: Option<Replication>,
    startup: StartupMsg,
}

impl Client {
    pub fn new(addr: Ipv4Addr, port: u16) -> Self {
        Self {
            addr: addr,
            port: port,
            startup: StartupMsg::default(),
            replication: None,
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

    pub fn with_database(mut self, db: &str) -> Self {
        self.startup.database = Some(db.to_string());
        self
    }

    pub fn with_user(mut self, user: &str) -> Self {
        self.startup.user = user.to_string();
        self
    }

    pub fn with_replication(mut self, repl: &str) -> Self {
        self.startup.replication = Some(repl.to_string());
        self
    }

    pub fn with_protocol(mut self, prot: i32) -> Self {
        self.startup.protocol = prot;
        self
    }

    pub fn with_config_dir(mut self, config_dir: &str) -> Self {
        self.replication = Some(Replication::new(config_dir));
        self
    }

    pub fn authenticate2(
        &mut self,
        stream: &mut TcpStream,
        // startup_msg: &mut StartupMsg,
        pass: &str,
    ) -> Result<(), AuthError> {
        let msg_bytes = self.startup.to_bytes();
        let mut buf = [0; BUF_LEN]; // Buffer to read response

        if let Err(e) = stream.write(&msg_bytes) {
            return Err(AuthError(format!("Failed to write to stream: {}", e)));
        }

        loop {
            match stream.read(&mut buf) {
                Ok(size) => {
                    if size <= 0 {
                        return Err(AuthError(format!("Server closed the connection")));
                    }

                    if size > BUF_LEN {
                        return Err(AuthError(format!("Received data exceeds buffer size")));
                    }
                    let response = &buf[..size];

                    match response[0] {
                        b'R' => {
                            // 'R' for Authentication
                            let auth_type_num = (&buf[5..9]).get_i32();

                            match get_auth_type(auth_type_num) {
                                AuthenticationType::CleartextPassword => {
                                    let auth =
                                        textpassword::ClearTextPass::new(&pass, &self.startup.user);
                                    auth.authenticate(stream, &buf)?;
                                    break;
                                }
                                AuthenticationType::MD5Password => {
                                    let auth = md5password::MD5Pass::new(&pass, &self.startup.user);
                                    auth.authenticate(stream, &buf)?;
                                    break;
                                }
                                AuthenticationType::SASL => {
                                    let auth = sasl::SASL::new(&pass, &self.startup.user);
                                    auth.authenticate(stream, &buf)?;
                                    break;
                                }
                            }
                        }
                        _ => {
                            return Err(AuthError(format!(
                                "Unexpected message type: {}",
                                response[0]
                            )));
                        }
                    }
                }
                Err(e) => {
                    return Err(AuthError(format!("Failed to read from stream: {}", e)));
                }
            }
        }
        Ok(())
    }

    // TODO: To be deleted once examples using it are updated
    pub fn authenticate(
        &self,
        stream: &mut TcpStream,
        startup_msg: &mut StartupMsg,
        pass: &str,
    ) -> Result<(), AuthError> {
        let msg_bytes = startup_msg.to_bytes();
        let mut buf = [0; BUF_LEN]; // Buffer to read response

        if let Err(e) = stream.write(&msg_bytes) {
            return Err(AuthError(format!("Failed to write to stream: {}", e)));
        }

        loop {
            match stream.read(&mut buf) {
                Ok(size) => {
                    if size <= 0 {
                        return Err(AuthError(format!("Server closed the connection")));
                    }

                    if size > BUF_LEN {
                        return Err(AuthError(format!("Received data exceeds buffer size")));
                    }
                    let response = &buf[..size];

                    match response[0] {
                        b'R' => {
                            // 'R' for Authentication
                            let auth_type_num = (&buf[5..9]).get_i32();

                            match get_auth_type(auth_type_num) {
                                AuthenticationType::CleartextPassword => {
                                    let auth =
                                        textpassword::ClearTextPass::new(&pass, &startup_msg.user);
                                    auth.authenticate(stream, &buf)?;
                                    break;
                                }
                                AuthenticationType::MD5Password => {
                                    let auth = md5password::MD5Pass::new(&pass, &startup_msg.user);
                                    auth.authenticate(stream, &buf)?;
                                    break;
                                }
                                AuthenticationType::SASL => {
                                    let auth = sasl::SASL::new(&pass, &startup_msg.user);
                                    auth.authenticate(stream, &buf)?;
                                    break;
                                }
                            }
                        }
                        _ => {
                            return Err(AuthError(format!(
                                "Unexpected message type: {}",
                                response[0]
                            )));
                        }
                    }
                }
                Err(e) => {
                    return Err(AuthError(format!("Failed to read from stream: {}", e)));
                }
            }
        }
        Ok(())
    }

    pub fn run(
        &mut self,
        stream: &mut std::net::TcpStream,
        table: &str,
        publication_name: &str,
        out_res: Option<OutResource>,
    ) {
        let replication = self.replication.as_mut().expect("Replication must be set");

        let out_res = match out_res {
            Some(res) => res,
            None => OutResource::CSVFile(format!(
                "{}/{}{}",
                replication.config_dir.as_ref().unwrap(),
                table,
                REPLICATION_DATA_FILE_EXT
            )),
        };

        if let Some(info) = replication.info.table.get_mut(table) {
            info.out_resource = out_res;
            replication
                .info
                .dump()
                .expect("Unable to save table details");
        } else {
            let mut info = TableInfo::new(out_res, table);
            info.publication = publication_name.to_string();
            replication.info.table.insert(table.to_string(), info);
            replication
                .info
                .dump()
                .expect("Unable to save table details");
        }

        replication.create_slot(stream, table).unwrap();

        if !replication.snapshot_taken(table) {
            replication.copy_snapshot(stream, table).unwrap()
        }

        replication.start(stream, table).unwrap()
    }
}

fn decoded_password(password: &str) -> Option<String> {
    match STANDARD.decode(&password) {
        Ok(pass) => Some(String::from_utf8_lossy(&pass).to_string()),
        Err(_) => None,
    }
}

fn encode_password(password: &str) -> String {
    STANDARD.encode(password)
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

#[derive(Debug)]
pub enum SimpleQueryCompletion {
    InProgress,
    InProgressReadStream,
    CopyComplete,
    CommandComplete,
    CommandError,
    CopyError,
    NoMatch,
    ReadStreamTimeout,
}

pub fn get_auth_type(_type: i32) -> AuthenticationType {
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

/// Add length to specified position in the buffer
fn add_buf_len(buf: &mut BytesMut, start_pos: usize, buf_len: i32) {
    let mut temp_buf = vec![];
    temp_buf.put_i32(buf_len);
    buf[start_pos..start_pos + 4].copy_from_slice(&temp_buf);
}

/// Example function for how row descriptions will be formatted
fn format_row_desc(off: usize, num_cols: i16, resp_buf: &[u8], out_buf: &mut BytesMut) {
    let mut off = off; // coerce offset to mutable type
    let idx_fn = |buf: &[u8]| -> u32 {
        let mut r = 0;
        for c in buf {
            if *c == b'\0' {
                break;
            }
            r += 1;
        }
        r
    };

    for i in 0..num_cols {
        let col_len = idx_fn(&resp_buf[off..]);
        let row_data = &resp_buf[off..off + col_len as usize];
        out_buf.put_slice(row_data);
        if (i + 1) < num_cols {
            out_buf.put_i8(b'|' as i8);
        }
        off += col_len as usize + 18 + 1; // See https://www.postgresql.org/docs/17/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-ROWDESCRIPTION
    }
}

/// Example function for how data rows will be formatted
/// Each column will be separated by '|' and rows will be
/// separated by newline '\n' character e.g `1|1\n2|2`
fn format_data_row(
    off: usize,
    num_cols: i16,
    resp_buf: &[u8],
    out_buf: &mut [u8],
    state: &mut QueryState,
) {
    let mut off = off; // coerce offset to mutable type
    // println!("Row: {:?}", String::from_utf8_lossy(&resp_buf[off..]));
    let row_start = off - 7; // Start of DataRow message
    let buf_off = state.data_buf_off; // Save current offset into output buffer
    for i in 0..num_cols {
        let col_len = (&resp_buf[off..off + 4]).get_i32();
        off += 4;
        let row_data = &resp_buf[off..off + col_len as usize];
        if state.data_buf_off + col_len as usize >= out_buf.len() {
            // Prevent overflow
            state.overflow_buf.clear();
            state
                .overflow_buf
                .put_slice(&resp_buf[row_start as usize..]);
            state.skip_bytes = resp_buf[row_start as usize..].len() as u32;
            state.data_buf_off = buf_off; // Reset offset
            off = row_start;
            _ = off;
            return;
        }
        out_buf[state.data_buf_off..state.data_buf_off + col_len as usize]
            .copy_from_slice(row_data);
        state.data_buf_off += col_len as usize;

        if (i + 1) < num_cols {
            out_buf[state.data_buf_off] = b'|';
            state.data_buf_off += 1;
        }
        off += col_len as usize;
    }
    out_buf[state.data_buf_off] = b'\n'; // Add newline for better readability
    state.data_buf_off += 1;
}

/// Send simple query message to the server
pub fn send_simple_query(stream: &mut TcpStream, msg: &str) -> Option<SimpleQueryError> {
    let mut bytes = BytesMut::new();

    bytes.put_u8(b'Q'); // Query message type
    let start_pos = bytes.len();
    bytes.put_i32(0); // Placeholder for length
    bytes.put_slice(msg.as_bytes()); // Query
    bytes.put_u8(0);

    let buf_len = bytes[start_pos..].len() as i32;
    add_buf_len(&mut bytes, start_pos, buf_len);

    if let Err(e) = stream.write(&bytes) {
        return Some(SimpleQueryError(format!(
            "Failed to write query to stream: {}",
            e
        )));
    }
    None
}

/// Processing simple query message formats.
/// See https://www.postgresql.org/docs/current/protocol-message-formats.html
fn process_simple_query_codes(
    buf: &[u8],
    off: &mut usize,
    cpy_size: usize,
    size: &mut usize,
    data_buf: &mut [u8],
    row_descr: &mut BytesMut,
    state: &mut QueryState,
) -> SimpleQueryCompletion {
    let mut is_done = SimpleQueryCompletion::NoMatch;
    let code = buf[*off..][0];
    match code {
        b'C' => {
            // 'C' for CommandComplete
            // let msg_len: i32 = (&buf[off + 1..off + 5]).get_i32();

            // // Check for Query ready
            // if (off + msg_len as usize + 1) < response.len() && response[off + msg_len as usize + 1..][0] == b'Z' {
            //     is_done = true;
            //     // break 'attempt_read;
            // }
            is_done = SimpleQueryCompletion::CommandComplete;
            // break 'attempt_read;
        }
        b'Z' => {
            // 'Z' for ReadyForQuery. TODO: Process different states
            let msg_len: i32 = (&buf[1..5]).get_i32();

            *size = if *size >= msg_len as usize {
                *size - msg_len as usize
            } else {
                0
            };
            *off += msg_len as usize + 1;
            is_done = SimpleQueryCompletion::InProgress;
        }
        b'I' => {
            // 'I' for EmptyQueryResponse
            let msg_len: i32 = (&buf[1..5]).get_i32();
            eprintln!("Empty Query Response with length: {}", msg_len);
            is_done = SimpleQueryCompletion::CommandError;
            // break 'attempt_read;
        }
        b'E' => {
            // 'E' for ErrorResponse
            let msg_len: i32 = (&buf[1..5]).get_i32();
            let char_idx_fn = |buf: &[u8], key: u8| -> usize {
                let mut r = 0;
                for ch in buf {
                    if *ch == key {
                        break;
                    }
                    r += 1;
                }
                r as usize
            };

            // Parse error message from server. See https://www.postgresql.org/docs/current/protocol-error-fields.html#PROTOCOL-ERROR-FIELDS
            let out_msg = || -> BytesMut {
                let mut _off: usize = 0;
                let mut msg_out = BytesMut::with_capacity(BUF_LEN);

                _off += 6;
                while _off < msg_len as usize {
                    let mut err_msg: &[u8];

                    if buf[_off] == b'V' {
                        // Severity
                        _off += 1; // Skip error severity character
                        err_msg = &buf[_off..];
                        let end = char_idx_fn(err_msg, b'\0');
                        msg_out.put_slice(&err_msg[..end]);
                        _off += end; // Advance
                        msg_out.put_slice(": ".as_bytes());
                    }

                    if buf[_off] == b'M' {
                        // Message containing error details
                        _off += 1; // Skip error message character
                        err_msg = &buf[_off..];
                        let end = char_idx_fn(err_msg, b'\0');
                        msg_out.put_slice(&err_msg[..end]);
                        _off += end; // Advance
                        break;
                    }
                    _off += 1;
                }
                msg_out
            }();

            if let Err(e) = stdout().write_all(&out_msg) {
                eprintln!("Simple Query Error: {}", e);
            }
            is_done = SimpleQueryCompletion::CommandError;
        }
        b'T' => {
            // T for RowDescription
            let msg_len: i32 = (&buf[1..5]).get_i32();
            let num_cols = (&buf[*off + 5..*off + 7]).get_i16();

            let val_off = *off + 5 + 2; // Account for byte, 4 byte for content size, 2 bytes for column number
            format_row_desc(val_off, num_cols, &buf, row_descr);

            *size = if *size >= msg_len as usize {
                *size - msg_len as usize
            } else {
                0
            };
            *off += msg_len as usize + 1;
            is_done = SimpleQueryCompletion::InProgress;
        }
        b'D' => {
            // 'D' for DataRow

            // Check if we have enough data to read full message length. If not,
            // put in overflow buffer to be handled later
            if *off + 5 > cpy_size {
                state.overflowed = true;
                state.overflow_buf.clear();
                state.overflow_buf.put_slice(&buf[*off..cpy_size]);
                state.skip_bytes = buf[*off..cpy_size].len() as u32;
                return SimpleQueryCompletion::InProgressReadStream;
            }
            let msg_len: i32 = (&buf[*off + 1..*off + 5]).get_i32();

            // Check if the message length exceeds the available data
            // If so, put in overflow buffer to be handled later
            if *off + msg_len as usize + 1 > cpy_size {
                state.overflowed = true;
                state.overflow_buf.clear();
                state.overflow_buf.put_slice(&buf[*off..cpy_size]);
                state.skip_bytes = buf[*off..cpy_size].len() as u32;
                return SimpleQueryCompletion::InProgressReadStream;
            }
            let num_cols = (&buf[*off + 5..*off + 7]).get_i16();

            let val_off = *off + 5 + 2; // Account for byte, 4 byte for content size, 2 bytes for column number
            format_data_row(val_off, num_cols, &buf, data_buf, state);

            if state.skip_bytes > 0 {
                return is_done;
            }

            *size = if *size >= msg_len as usize {
                *size - msg_len as usize
            } else {
                0
            };
            *off += msg_len as usize + 1;
            if *off >= cpy_size {
                return is_done;
            }
            is_done = SimpleQueryCompletion::InProgress;
        }
        _ => (),
    }

    is_done
}

/// See https://www.postgresql.org/docs/current/protocol-message-formats.html
pub fn process_simple_query(
    stream: &mut TcpStream,
    data_buf: &mut [u8],
    row_descr: &mut BytesMut,
    state: &mut QueryState,
) -> Result<bool, SimpleQueryError> {
    let mut buf = [0; BUF_LEN]; // Buffer to read response
    let mut buf_off = 0;
    let mut is_done = false;

    if state.skip_bytes > 0 {
        buf[..state.skip_bytes as usize]
            .copy_from_slice(&state.overflow_buf[..state.skip_bytes as usize]);
        buf_off += state.skip_bytes as usize;
        state.skip_bytes = 0;
    }

    'attempt_read: loop {
        let mut size: usize;
        match stream.read(&mut buf[buf_off..]) {
            Ok(r_size) => {
                if r_size <= 0 {
                    return Err(SimpleQueryError("Server closed the connection".to_string()));
                }

                if r_size > BUF_LEN {
                    return Err(SimpleQueryError(
                        "Received data exceeds buffer size".to_string(),
                    ));
                }
                size = r_size + buf_off;
                buf_off = 0;
            }
            Err(e) => {
                return Err(SimpleQueryError(format!(
                    "Failed to read from stream: {}",
                    e
                )));
            }
        }

        let response = &buf[..size];
        let mut off = 0;
        let cpy_size = size;
        while size > 0 && off < cpy_size {
            match response[off..][0] {
                b'C' => {
                    // 'C' for CommandComplete
                    // let msg_len: i32 = (&buf[off + 1..off + 5]).get_i32();

                    // // Check for Query ready
                    // if (off + msg_len as usize + 1) < response.len() && response[off + msg_len as usize + 1..][0] == b'Z' {
                    //     is_done = true;
                    //     // break 'attempt_read;
                    // }
                    is_done = true;
                    break 'attempt_read;
                }
                b'Z' => {
                    // 'Z' for ReadyForQuery. TODO: Process different states
                    let msg_len: i32 = (&buf[1..5]).get_i32();

                    size = if size >= msg_len as usize {
                        size - msg_len as usize
                    } else {
                        0
                    };
                    off += msg_len as usize + 1;
                }
                b'I' => {
                    // 'I' for EmptyQueryResponse
                    let msg_len: i32 = (&buf[1..5]).get_i32();
                    eprintln!("Empty Query Response with length: {}", msg_len);
                    is_done = true;
                    break 'attempt_read;
                }
                b'E' => {
                    // 'E' for ErrorResponse
                    let msg_len: i32 = (&buf[1..5]).get_i32();
                    let char_idx_fn = |buf: &[u8], key: u8| -> usize {
                        let mut r = 0;
                        for ch in buf {
                            if *ch == key {
                                break;
                            }
                            r += 1;
                        }
                        r as usize
                    };

                    // See https://www.postgresql.org/docs/17/protocol-error-fields.html#PROTOCOL-ERROR-FIELDS
                    let out_msg = || -> BytesMut {
                        let mut _off: usize = 0;
                        let mut msg_out = BytesMut::with_capacity(BUF_LEN);

                        _off += 6;
                        while _off < msg_len as usize {
                            let mut err_msg: &[u8];

                            if buf[_off] == b'V' {
                                _off += 1; // Skip error severity character
                                err_msg = &buf[_off..];
                                let end = char_idx_fn(err_msg, b'\0');
                                msg_out.put_slice(&err_msg[..end]);
                                _off += end; // Advance
                                msg_out.put_slice(": ".as_bytes());
                            }

                            if buf[_off] == b'M' {
                                _off += 1; // Skip error message character
                                err_msg = &buf[_off..];
                                let end = char_idx_fn(err_msg, b'\0');
                                msg_out.put_slice(&err_msg[..end]);
                                _off += end; // Advance
                                break;
                            }
                            _off += 1;
                        }
                        msg_out
                    }();

                    if let Err(e) = stdout().write_all(&out_msg) {
                        eprintln!("Simple Query Error: {}", e);
                    }
                    is_done = true;
                    break 'attempt_read;
                }
                b'T' => {
                    // T for RowDescription
                    let msg_len: i32 = (&buf[1..5]).get_i32();
                    let num_cols = (&buf[off + 5..off + 7]).get_i16();

                    let val_off = off + 5 + 2; // Account for byte, 4 byte for content size, 2 bytes for column number
                    format_row_desc(val_off, num_cols, &buf, row_descr);

                    size = if size > msg_len as usize {
                        size - msg_len as usize - 1
                    } else {
                        0
                    };
                    off += msg_len as usize + 1;
                }
                b'D' => {
                    // 'D' for DataRow

                    // Check if we have enough data to read full message length. If not,
                    // put in overflow buffer to be handled later
                    if off + 5 > cpy_size {
                        state.overflowed = true;
                        state.overflow_buf.clear();
                        state.overflow_buf.put_slice(&buf[off..cpy_size]);
                        state.skip_bytes = buf[off..cpy_size].len() as u32;
                        break;
                    }
                    let msg_len: i32 = (&buf[off + 1..off + 5]).get_i32();

                    // Check if the message length exceeds the available data
                    // If so, put in overflow buffer to be handled later
                    if off + msg_len as usize + 1 > cpy_size {
                        state.overflowed = true;
                        state.overflow_buf.clear();
                        state.overflow_buf.put_slice(&buf[off..cpy_size]);
                        state.skip_bytes = buf[off..cpy_size].len() as u32;
                        break;
                    }
                    let num_cols = (&buf[off + 5..off + 7]).get_i16();

                    let val_off = off + 5 + 2; // Account for byte, 4 byte for content size, 2 bytes for column number
                    format_data_row(val_off, num_cols, &buf, data_buf, state);
                    if state.skip_bytes > 0 {
                        return Ok(is_done);
                    }

                    size = if size > msg_len as usize {
                        size - msg_len as usize - 1
                    } else {
                        0
                    };
                    off += msg_len as usize + 1;
                    if off >= cpy_size {
                        return Ok(is_done);
                    }
                }
                _ => {
                    // Handle incomplete data in read buffer(Alot to improve here)
                    if state.overflowed && state.skip_bytes > 0 {
                        state.overflow_buf.put_slice(&buf[..cpy_size]); // What??
                        let msg_len: i32 = (&state.overflow_buf[1..5]).get_i32();
                        state.skip_bytes = (msg_len + 1) as u32 - state.skip_bytes;

                        size -= state.skip_bytes as usize;
                        off += state.skip_bytes as usize;

                        let num_cols = (&state.overflow_buf[5..7]).get_i16();

                        let val_off = 5 + 2; // Account for byte, 4 byte for content size, 2 bytes for column number

                        //TODO: CLean this up. Not looking good
                        let mut buf = BytesMut::with_capacity(state.overflow_buf.len());
                        buf.put_slice(&state.overflow_buf);
                        format_data_row(val_off, num_cols, &buf, data_buf, state);

                        state.overflow_buf.clear();
                        state.overflowed = false;
                        state.skip_bytes = 0;
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

    Ok(is_done)
}

/// When we read incomplete data at the end of the response buffer, we copy the remaining slice to overflow buffer
/// and set the skip_bytes to the number of bytes we need to skip in the next read
/// to complete the message. Once we have enough data, we process the overflow buffer
/// to extract the complete message and copy to the data buffer.
fn handle_overflowed(
    data_buf: &mut [u8],
    state: &mut QueryState,
    buf: &[u8],
    size: &mut usize,
    off: &mut usize,
    cpy_size: usize,
) {
    // What??
    state.overflow_buf.put_slice(&buf[..cpy_size]);
    let msg_len: i32 = (&state.overflow_buf[1..5]).get_i32();
    state.skip_bytes = (msg_len + 1) as u32 - state.skip_bytes;

    *size = if *size >= (state.skip_bytes as usize) {
        *size - state.skip_bytes as usize
    } else {
        0
    };
    *off += state.skip_bytes as usize;

    // Account for byte, 4 byte for content size
    let val_off = 5;

    //TODO: CLean this up. Not looking good
    let mut buf = BytesMut::with_capacity(state.overflow_buf.len());
    buf.put_slice(&state.overflow_buf);

    let data: &[u8];
    if msg_len as usize + 1 < buf.len() {
        data = &buf[val_off..msg_len as usize + 1];
    } else {
        data = &buf[val_off..];
    }
    data_buf[state.data_buf_off..state.data_buf_off + data.len()].copy_from_slice(data);
    state.data_buf_off += data.len();

    state.overflow_buf.clear();
    state.overflowed = false;
    state.skip_bytes = 0;
}

fn process_simple(
    stream: &mut std::net::TcpStream,
    state: &mut QueryState,
    result_buf: &mut [u8],
    row_descr: &mut BytesMut,
    replication: &mut Replication,
) -> Result<SimpleQueryCompletion, SimpleQueryError> {
    // stream.set_read_timeout(Some(Duration::from_millis(1000))).unwrap(); // Set a timeout to avoid blocking indefinitely
    state.data_buf_off = 0;

    let ret = process_logical_repl(stream, result_buf, row_descr, state, replication);

    ret
}

/// Logical replication protocol messages are processed in this function. Copy is also handled here
fn process_logical_repl(
    stream: &mut TcpStream,
    data_buf: &mut [u8],
    row_descr: &mut BytesMut,
    state: &mut QueryState,
    replication: &mut Replication,
) -> Result<SimpleQueryCompletion, SimpleQueryError> {
    let mut buf = [0; BUF_LEN]; // Buffer to read response
    let mut buf_off = 0;
    let mut is_done: SimpleQueryCompletion;

    // Handle any overflowed data(if any) from previous read
    if state.skip_bytes > 0 {
        buf[..state.skip_bytes as usize]
            .copy_from_slice(&state.overflow_buf[..state.skip_bytes as usize]);
        buf_off += state.skip_bytes as usize;
        state.skip_bytes = 0;
    }

    'attempt_read: loop {
        let mut size: usize;
        match stream.read(&mut buf[buf_off..]) {
            Ok(r_size) => {
                if r_size <= 0 {
                    is_done = SimpleQueryCompletion::ReadStreamTimeout;
                    break;
                }

                if r_size > BUF_LEN {
                    return Err(SimpleQueryError(
                        "Received data exceeds buffer size".to_string(),
                    ));
                }
                size = r_size + buf_off;
                buf_off = 0;
            }
            Err(e) => {
                return Err(SimpleQueryError(format!(
                    "Failed to read from stream: {}",
                    e
                )));
            }
        }

        let response = &buf[..size];
        let mut off = 0;
        let cpy_size = size;
        while size > 0 && off < cpy_size {
            let code = response[off..][0];
            is_done = process_simple_query_codes(
                &buf, &mut off, cpy_size, &mut size, data_buf, row_descr, state,
            );
            match is_done {
                SimpleQueryCompletion::CommandComplete => break 'attempt_read,
                SimpleQueryCompletion::CommandError => break 'attempt_read,
                SimpleQueryCompletion::InProgressReadStream => break,
                SimpleQueryCompletion::InProgress => (),
                _ => {
                    is_done = SimpleQueryCompletion::InProgress;
                    match code {
                        b'f' => {
                            // CopyFail
                            let msg_len: i32 = (&buf[off + 1..off + 5]).get_i32();

                            // size = if size >= msg_len as usize {
                            //     size - msg_len as usize
                            // } else {
                            //     0
                            // };
                            let err_msg = &buf[off + 5..off + msg_len as usize + 1];
                            // off += msg_len as usize + 1;
                            eprintln!("Copy Error: {:?}", String::from_utf8_lossy(err_msg));
                            is_done = SimpleQueryCompletion::CopyError;
                            break 'attempt_read;
                            // Stop??
                        }
                        b'W' => {
                            // 'W' for CopyBothResponse
                            let msg_len: i32 = (&buf[off + 1..off + 5]).get_i32();
                            // Do something useful here?..
                            size = if size >= msg_len as usize {
                                size - msg_len as usize
                            } else {
                                0
                            };
                            off += msg_len as usize + 1;
                        }
                        b'c' => {
                            // CopyDone
                            is_done = SimpleQueryCompletion::CopyComplete;
                            break 'attempt_read;
                        }
                        b'H' => {
                            // CopyOutResponse
                            let msg_len: i32 = (&buf[off + 1..off + 5]).get_i32();

                            size = if size >= msg_len as usize {
                                size - msg_len as usize
                            } else {
                                0
                            };
                            off += msg_len as usize + 1;
                        }
                        b'd' => {
                            // 'd' for CopyData
                            if state.skip_bytes > 0 {
                                handle_overflowed(
                                    data_buf, state, &buf, &mut size, &mut off, cpy_size,
                                );
                                continue;
                            }
                            if off + 5 > cpy_size {
                                state.overflowed = true;
                                state.overflow_buf.clear();
                                state.overflow_buf.put_slice(&buf[off..cpy_size]);
                                state.skip_bytes = buf[off..cpy_size].len() as u32;
                                break;
                            }

                            let msg_len: i32 = (&buf[off + 1..off + 5]).get_i32();
                            if state.data_buf_off + msg_len as usize >= data_buf.len() {
                                // Prevent overflow
                                state.overflow_buf.clear();
                                state.overflow_buf.put_slice(&buf[off..cpy_size]);
                                state.skip_bytes = buf[off..cpy_size].len() as u32;
                                state.recycle_buf_off = state.data_buf_off;
                                state.data_buf_off = 0; // Reset offset
                                return Ok(is_done);
                            }

                            if off + msg_len as usize + 1 > cpy_size {
                                state.overflowed = true;
                                state.overflow_buf.clear();
                                state.overflow_buf.put_slice(&buf[off..cpy_size]);
                                state.skip_bytes = buf[off..cpy_size].len() as u32;
                                break;
                            }

                            let msg_data = &buf[off + 5..off + msg_len as usize + 1];
                            match msg_data[0] {
                                b'k' => {
                                    // 'k' for Keepalive message
                                    let end_of_wal = (&msg_data[1..9]).get_i64();
                                    if msg_data[17] == 1 {
                                        // Reply requested
                                        println!("Keepalive reply requested");
                                        let mut resp_buf = BytesMut::new();
                                        let now: DateTime<Utc> = Utc::now();

                                        // Respond to keep the connection alive
                                        resp_buf.put_u8(b'd'); // identify message as CopyData
                                        let start_pos = resp_buf.len();
                                        resp_buf.put_i32(0); // Placeholder for length
                                        resp_buf.put_u8(b'r'); // Standby status update
                                        resp_buf.put_i64(end_of_wal + 1); // The location of the last WAL byte + 1 received and written to disk in the standby.
                                        resp_buf.put_i64(end_of_wal + 1); // The location of the last WAL byte + 1 flushed to disk in the standby.
                                        resp_buf.put_i64(end_of_wal + 1); // The location of the last WAL byte + 1 applied in the standby.
                                        resp_buf.put_i64(now.timestamp_micros()); // The client's system clock at the time of transmission, as microseconds since midnight on 2000-01-01.
                                        resp_buf.put_u8(0); // Reply is not required

                                        let total_len = resp_buf[start_pos..].len() as i32;
                                        add_buf_len(&mut resp_buf, start_pos, total_len);

                                        if let Err(e) = stream.write(&resp_buf) {
                                            return Err(SimpleQueryError(format!(
                                                "Failed to write keepalive response to stream: {}",
                                                e
                                            )));
                                        }
                                    }
                                }
                                b'w' => {
                                    let xlog_data = &msg_data[25..];
                                    // https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
                                    match xlog_data[0] {
                                        b'B' => {
                                            // BEGIN
                                            println!("XLogData: BEGIN");
                                        }
                                        b'I' => {
                                            // INSERT
                                            let tuple_data = &xlog_data[6..];
                                            let col_num: i16 = (&tuple_data[..2]).get_i16();

                                            // Number of columns
                                            let mut off = 2;
                                            for k in 0..col_num {
                                                // Submessage
                                                off += 1;
                                                let val_len = (&tuple_data[off..off + 4]).get_i32();
                                                off += 4;

                                                data_buf[state.data_buf_off
                                                    ..state.data_buf_off + val_len as usize]
                                                    .copy_from_slice(
                                                        &tuple_data[off..off + val_len as usize],
                                                    );
                                                state.data_buf_off += val_len as usize;
                                                off += val_len as usize;

                                                if (k + 1) < col_num {
                                                    data_buf[state.data_buf_off] = b',';
                                                    state.data_buf_off += 1;
                                                }
                                            }
                                            data_buf[state.data_buf_off] = b'\n';
                                            state.data_buf_off += 1;
                                        }
                                        b'U' => { // UPDATE
                                        }
                                        b'D' => {
                                            // DELETE
                                        }
                                        b'R' => {
                                            // RELATION: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-RELATION
                                            // Assume non-streamed transactions, so TransactionId is ignored
                                            let oid = (&xlog_data[1..5]).get_i32();
                                            let parts: &Vec<&[u8]> =
                                                &xlog_data[5..].split(|&b| b == b'\0').collect();
                                            let namespace = parts[0];
                                            let rel_name = parts[1];
                                            // Adding 8 to account for the 2 c-string terminators
                                            let mut off = namespace.len() + rel_name.len() + 8;
                                            let num_cols = (&xlog_data[off..off + 2]).get_i16();
                                            let table_info: &mut TableInfo;

                                            if let Some(info) = replication.info.table.get_mut(
                                                &String::from_utf8_lossy(rel_name).to_string(),
                                            ) {
                                                table_info = info;
                                            } else {
                                                return Err(SimpleQueryError(format!(
                                                    "Table does not exist"
                                                )));
                                            }

                                            table_info.name =
                                                String::from_utf8_lossy(rel_name).to_string();
                                            table_info.oid = oid;
                                            table_info.namespace =
                                                String::from_utf8_lossy(namespace).to_string();
                                            table_info.ncols = num_cols;
                                            table_info.cols.clear();

                                            // Include column number
                                            off += 2;
                                            for _ in 0..num_cols {
                                                // Ignore flags byte
                                                off += 1;
                                                let parts: &Vec<&[u8]> = &xlog_data[off..]
                                                    .split(|&b| b == b'\0')
                                                    .collect();
                                                table_info.cols.push(
                                                    String::from_utf8_lossy(parts[0]).to_string(),
                                                );
                                                // 2 ints for column oid and column type modifier and C-string terminator
                                                off += 9 + parts[0].len();
                                            }

                                            if let Err(e) = replication.info.dump() {
                                                return Err(SimpleQueryError(format!(
                                                    "Saving relation data error: {}",
                                                    e
                                                )));
                                            }
                                        }
                                        b'C' => {
                                            // COMMIT
                                            println!("XLogData: COMMIT");
                                        }
                                        _ => {}
                                    }
                                }
                                _ => {
                                    data_buf
                                        [state.data_buf_off..state.data_buf_off + msg_data.len()]
                                        .copy_from_slice(msg_data);
                                    state.data_buf_off += msg_data.len();
                                }
                            }

                            size = if size >= (msg_len as usize + 1) {
                                size - (msg_len as usize + 1)
                            } else {
                                0
                            };
                            off += msg_len as usize + 1;

                            if off >= cpy_size {
                                return Ok(is_done);
                            }
                        }
                        _ => {
                            // Handle incomplete data in read buffer(Alot to improve here)
                            if state.overflowed {
                                handle_overflowed(
                                    data_buf, state, &buf, &mut size, &mut off, cpy_size,
                                );
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
        }
    }

    Ok(is_done)
}

mod md5password {
    use bytes::{Buf, BufMut, BytesMut};
    use md5::{Digest, Md5};
    use std::io::{Read, Write};
    use std::net::TcpStream;

    use crate::wire::{AuthError, BUF_LEN, add_buf_len, decoded_password, encode_password};

    #[derive(Debug)]
    pub struct MD5Pass {
        password: String,
        user: String,
    }

    impl MD5Pass {
        pub fn new(password: &str, _user: &str) -> Self {
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

        pub fn authenticate(
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

mod textpassword {
    use bytes::{Buf, BufMut, BytesMut};
    use std::io::{Read, Write};
    use std::net::TcpStream;

    use crate::wire::{AuthError, BUF_LEN, add_buf_len, decoded_password, encode_password};

    #[derive(Debug)]
    pub struct ClearTextPass {
        password: String,
    }

    impl ClearTextPass {
        pub fn new(password: &str, _user: &str) -> Self {
            ClearTextPass {
                password: encode_password(password),
            }
        }

        pub fn authenticate(
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

mod sasl {
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

    use crate::wire::{AuthError, BUF_LEN, add_buf_len, decoded_password, encode_password};

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

        pub fn authenticate(
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

#[cfg(test)]
mod tests {
    use std::{net::TcpListener, thread};

    use super::*;

    #[test]
    fn test_encode_decode_password() {
        let password = "secret123";
        let encoded = encode_password(password);
        let decoded = decoded_password(&encoded).unwrap();
        assert_eq!(decoded, password);
    }

    #[test]
    #[should_panic(expected = "Unknown authentication type")]
    fn test_get_auth_type_unknown() {
        get_auth_type(999);
    }

    #[test]
    fn test_get_auth_type_variants() {
        assert!(matches!(
            get_auth_type(3),
            AuthenticationType::CleartextPassword
        ));
        assert!(matches!(get_auth_type(5), AuthenticationType::MD5Password));
        assert!(matches!(get_auth_type(10), AuthenticationType::SASL));
    }

    #[test]
    fn test_put_cstring() {
        let mut buf = BytesMut::new();
        put_cstring(&mut buf, "hello");
        assert_eq!(&buf[..], b"hello\0");
    }

    #[test]
    fn test_add_buf_len() {
        let mut buf = BytesMut::with_capacity(8);
        buf.put_i32(0);
        buf.put_i32(1234);
        add_buf_len(&mut buf, 0, 8);
        assert_eq!((&buf[0..4]).get_i32(), 8);
    }

    #[test]
    fn test_startup_msg_to_bytes() {
        let mut msg = StartupMsg::new(
            "user1".to_string(),
            Some("db1".to_string()),
            Some("opt1".to_string()),
            None,
        );
        let bytes = msg.to_bytes();
        // Should start with length and protocol version
        assert_eq!(bytes[4..8], PROTOCOL_VERSION.to_be_bytes());
        assert!(bytes.windows(5).any(|w| w == b"user\0"));
        assert!(bytes.windows(4).any(|w| *w == b"db1\0"[..4]));
    }

    #[test]
    fn test_client_new() {
        let client = Client::new(Ipv4Addr::LOCALHOST, 5432);
        assert_eq!(client.addr, Ipv4Addr::LOCALHOST);
        assert_eq!(client.port, 5432);
    }

    #[test]
    fn test_send_simple_query_error() {
        // Use a dummy stream that will fail
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut buf = [0u8; 128];
            socket.read(&mut buf).unwrap();
        });

        let mut stream = TcpStream::connect(addr).unwrap();
        stream.shutdown(std::net::Shutdown::Both).unwrap(); // force error
        let err = send_simple_query(&mut stream, "SELECT 1");
        assert!(err.is_some());
        let _ = handle.join();
    }

    #[test]
    fn test_format_row_desc_and_data_row() {
        let mut row_descr = BytesMut::new();
        // Simulate a RowDescription message with two columns, names "id" and "name"
        let resp_buf = b"id\0\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00name\0\x00\x00";
        format_row_desc(0, 2, resp_buf, &mut row_descr);
        assert!(row_descr.windows(1).any(|w| w == b"|"));

        let mut data_buf = [0u8; 128];
        // Simulate DataRow: 2 columns, "1" and "Alice"
        let mut resp_buf = vec![];
        resp_buf.extend_from_slice(b"0000000"); // 7 bytes to account for message type byte, 4 byte for content size, 2 bytes for column number
        resp_buf.extend(&(1i32.to_be_bytes())); // col1 len
        resp_buf.extend(b"1");
        resp_buf.extend(&(5i32.to_be_bytes())); // col2 len
        resp_buf.extend(b"Alice");
        let state = &mut QueryState::default();
        format_data_row(7, 2, &resp_buf, &mut data_buf, state);
        let row = std::str::from_utf8(&data_buf[..state.data_buf_off]).unwrap();
        assert!(row.contains("1|Alice\n"));
    }

    #[test]
    fn test_get_auth_type() {
        assert!(matches!(get_auth_type(5), AuthenticationType::MD5Password));
        assert!(matches!(get_auth_type(10), AuthenticationType::SASL));
        assert!(matches!(
            get_auth_type(3),
            AuthenticationType::CleartextPassword
        ));
    }

    #[test]
    fn test_startup_msg_to_bytes_contains_user() {
        let mut msg = StartupMsg::new(
            "testuser".to_string(),
            Some("testdb".to_string()),
            None,
            None,
        );
        let bytes = msg.to_bytes();
        let as_str = String::from_utf8_lossy(&bytes);
        assert!(as_str.contains("testuser"));
        assert!(as_str.contains("testdb"));
    }

    #[test]
    fn test_format_row_desc() {
        let mut out_buf = BytesMut::new();
        // Simulate a row description with two columns: "id\0" and "name\0"
        let resp_buf =
            b"id\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0name\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";
        format_row_desc(0, 2, resp_buf, &mut out_buf);
        let as_str = String::from_utf8_lossy(&out_buf);
        assert!(as_str.contains("id|name"));
    }

    #[test]
    fn test_format_data_row_simple() {
        let mut state = QueryState::default();
        // Simulate a data row with 1 column of length 3 ("abc")
        let mut resp_buf = vec![];
        resp_buf.extend_from_slice(b"0000000"); // 7 bytes to account for message type byte, 4 byte for content size, 2 bytes for column number
        resp_buf.extend_from_slice(&3i32.to_be_bytes()); // column length
        resp_buf.extend_from_slice(b"abc"); // column data
        let mut out_buf = [0u8; 16];
        format_data_row(7, 1, &resp_buf, &mut out_buf, &mut state);
        let result = std::str::from_utf8(&out_buf[..state.data_buf_off]).unwrap();
        assert_eq!(result, "abc\n");
    }
}
