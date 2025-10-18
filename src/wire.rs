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

use crate::auth::*;

const REPLICATION_META_FILE: &str = "meta.json";
const REPLICATION_DATA_FILE_EXT: &str = ".data";
const WAIT_OFF_CPU: u64 = 50; // In milliseconds
const OUT_BUF_SIZE: usize = 4096; // Bytes

#[derive(Debug)]
pub struct SimpleQueryError(pub String);

impl fmt::Display for SimpleQueryError {
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
    key_columns: Vec<String>, // Primary key columns
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
    fn delete(&self, data: &[u8]) -> io::Result<()>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OutResource {
    CSVFile(String),
    Iceberg {
        config_path: String,
        schema: Option<Vec<String>>,
        key: Option<Vec<String>>,
    }, // Path to config file and optional list of key columns
}

impl Default for OutResource {
    fn default() -> Self {
        OutResource::CSVFile(String::new())
    }
}

impl DoIO for OutResource {
    fn delete(&self, data: &[u8]) -> io::Result<()> {
        match self {
            OutResource::Iceberg {
                config_path,
                schema,
                key,
            } => Python::attach(|py| {
                let py_app = CString::new(read_to_string(Path::new("py_iceberg.py"))?)?;
                let app: Py<PyAny> =
                    PyModule::from_code(py, py_app.as_c_str(), c_str!("py_iceberg.py"), c_str!(""))?
                        .getattr("delete_from_table")?
                        .into();

                app.call1(
                    py,
                    (data, Path::new(config_path), schema.clone(), key.clone()),
                )?;

                Ok(())
            }),
            _ => {
                // No delete for other types
                Ok(())
            }
        }
    }

    fn write(&self, data: &[u8]) -> io::Result<()> {
        match self {
            OutResource::CSVFile(path) => {
                let path = Path::new(&path);

                let mut handle = OpenOptions::new().append(true).create(true).open(path)?;

                handle.write_all(data)?;
                handle.sync_all()?; // Flush to disk immediately

                Ok(())
            }
            OutResource::Iceberg {
                config_path,
                schema,
                key,
            } => Python::attach(|py| {
                let py_app = CString::new(read_to_string(Path::new("py_iceberg.py"))?)?;
                let app: Py<PyAny> =
                    PyModule::from_code(py, py_app.as_c_str(), c_str!("py_iceberg.py"), c_str!(""))?
                        .getattr("write_to_table")?
                        .into();

                app.call1(
                    py,
                    (data, Path::new(config_path), schema.clone(), key.clone()),
                )?;

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
            OutResource::Iceberg {
                config_path: _,
                schema: _,
                key: _,
            } => {
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

                    self.write_to(&out_res, &result_buf, &mut state);
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

                    self.write_to(&out_res, &result_buf, &mut state);
                }
                Ok(SimpleQueryCompletion::InProgressReadStream) => {
                    let table_info: &mut TableInfo;
                    if let Some(info) = self.info.table.get_mut(table) {
                        table_info = info;
                    } else {
                        return Err(ReplicationError(format!("Table does not exist")));
                    }

                    let out_res = table_info.out_resource.clone();

                    self.write_to(&out_res, &result_buf, &mut state);
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

    fn write_to(&mut self, out_res: &OutResource, result_buf: &[u8], state: &mut QueryState) {
        let mut off = state.data_buf_off;
        if off <= 0 {
            off = state.recycle_buf_off;
            state.recycle_buf_off = 0;
        }

        // Ignore if no data to write
        if off <= 0 {
            return;
        }

        if let Err(e) = out_res.write(&result_buf[..off]) {
            eprintln!("Error when appending rows: {}", e);
        }

        if state.delete_rows_off > 0 {
            if let Some(del_buf) = &state.delete_rows {
                if let Err(e) = out_res.delete(&del_buf[..state.delete_rows_off]) {
                    eprintln!("Error when deleting rows: {}", e);
                }
            }
            state.delete_rows_off = 0; // Reset after delete
        }
    }

    /// Start the replication stream
    fn start(
        &mut self,
        stream: &mut std::net::TcpStream,
        table: &str,
    ) -> Result<(), ReplicationError> {
        let mut result_buf = [0; OUT_BUF_SIZE];
        let mut row_descr = BytesMut::new();
        let mut delete_rows = [0; OUT_BUF_SIZE]; // Buffer to hold deleted rows

        let mut state = QueryState::new(delete_rows.as_mut());
        // let table_info: &TableInfo;
        let msg: String;

        if let Some(info) = self.info.table.get(table) {
            msg = String::from(format!(
                "START_REPLICATION SLOT {} LOGICAL {} (proto_version '4', publication_names '{}')",
                info.slot, self.info.start_lsn, info.publication
            ));
        } else {
            return Err(ReplicationError(format!("Table does not exist")));
        }

        if let Some(e) = send_simple_query(stream, &msg) {
            return Err(ReplicationError(format!(
                "START REPLICATION Command Error: {}",
                e
            )));
        }

        loop {
            match process_simple(stream, &mut state, &mut result_buf, &mut row_descr, self) {
                Ok(SimpleQueryCompletion::CommandComplete) => break,
                Ok(SimpleQueryCompletion::CopyComplete) => (),
                Ok(SimpleQueryCompletion::InProgress) => (),
                Ok(SimpleQueryCompletion::InProgressReadStream) => (),
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
                _ => continue,
            }

            if let Some(info) = self.info.table.get(table) {
                let out_res = &info.out_resource.clone();
                self.write_to(&out_res, &result_buf, &mut state);
            }

            result_buf.fill(0);
            row_descr.clear();

            // Slow down a bit to avoid busy waiting
            thread::sleep(time::Duration::from_millis(WAIT_OFF_CPU));
        }
        Ok(())
    }
}

pub struct QueryState<'a> {
    pub overflowed: bool,
    pub skip_bytes: u32,
    pub overflow_buf: BytesMut,
    pub data_buf_off: usize,
    delete_rows: Option<&'a mut [u8]>,
    delete_rows_off: usize,
    recycle_buf_off: usize,
}

impl Default for QueryState<'_> {
    fn default() -> Self {
        Self {
            overflowed: false,
            skip_bytes: 0,
            overflow_buf: BytesMut::with_capacity(BUF_LEN),
            data_buf_off: 0,
            recycle_buf_off: 0,
            delete_rows: None,
            delete_rows_off: 0,
        }
    }
}

impl<'a> QueryState<'a> {
    fn new(delete_rows: &'a mut [u8]) -> QueryState<'a> {
        Self {
            delete_rows: Some(delete_rows),
            ..Default::default()
        }
    }
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
        let table_info: &mut TableInfo;

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
            table_info = info;
        } else {
            let mut info = TableInfo::new(out_res, table);
            info.publication = publication_name.to_string();
            replication.info.table.insert(table.to_string(), info);
            table_info = replication.info.table.get_mut(table).unwrap();
        }

        // Update out_resource with schema and key info if Iceberg
        match &table_info.out_resource {
            OutResource::CSVFile(_) => (),
            OutResource::Iceberg {
                config_path,
                schema: _,
                key: _,
            } => {
                table_info.out_resource = OutResource::Iceberg {
                    config_path: config_path.to_string(),
                    schema: Some(table_info.cols.clone()),
                    key: Some(table_info.key_columns.clone()),
                }
            }
        }

        replication
            .info
            .dump()
            .expect("Unable to save table details");

        replication.create_slot(stream, table).unwrap();

        if !replication.snapshot_taken(table) {
            replication.copy_snapshot(stream, table).unwrap()
        }

        replication.start(stream, table).unwrap()
    }
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
                                        b'U' => {
                                            // UPDATE
                                            let msg_id = xlog_data[9];

                                            let mut off = 0;
                                            if msg_id == b'K' || msg_id == b'O' {
                                                // Key of the row to be updated
                                                // let tuple_data = &xlog_data[6..];
                                                off += 6;
                                                let col_num: i16 =
                                                    (&xlog_data[off..off + 2]).get_i16();

                                                // Number of columns with 2 bytes
                                                off += 2;

                                                // Ignore key updates and REPLICA IDENTITY FULL updates for now
                                                // Loop through to move offset forward
                                                for _ in 0..col_num {
                                                    // Submessage
                                                    off += 1;
                                                    let val_len =
                                                        (&xlog_data[off..off + 4]).get_i32();
                                                    off += 4;
                                                    off += val_len as usize;
                                                }
                                            } else {
                                                off += 6; // Move offset to new tuple data
                                            }

                                            // Go back one byte to read message type('K' or 'O')
                                            let msg_id = xlog_data[off - 1];

                                            // Handle new tuple data
                                            if msg_id == b'N' {
                                                let col_num: i16 =
                                                    (&xlog_data[off..off + 2]).get_i16();

                                                // Number of columns with 2 bytes
                                                off += 2;
                                                for k in 0..col_num {
                                                    // Submessage
                                                    off += 1;
                                                    let val_len =
                                                        (&xlog_data[off..off + 4]).get_i32();
                                                    off += 4;

                                                    data_buf[state.data_buf_off
                                                        ..state.data_buf_off + val_len as usize]
                                                        .copy_from_slice(
                                                            &xlog_data[off..off + val_len as usize],
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
                                        }
                                        b'D' => {
                                            // DELETE
                                            let msg_id = xlog_data[5];

                                            let mut off = 0;
                                            let add_delimiter_fn =
                                                |b: u8, data_buf: &mut [u8], state: &mut QueryState<'_>| match &mut state
                                                    .delete_rows
                                                {
                                                    Some(buf) => {
                                                        if state.delete_rows_off < buf.len() {
                                                            buf[state.delete_rows_off] = b;
                                                            state.delete_rows_off += 1;
                                                            data_buf[state.data_buf_off] = b;
                                                            state.data_buf_off += 1;
                                                        } else {
                                                            eprintln!(
                                                                "Delete rows buffer overflow. Consider increasing buffer size."
                                                            );
                                                        }
                                                    }
                                                    None => {
                                                        eprintln!(
                                                            "Delete rows buffer not initialized."
                                                        );
                                                    }
                                                };

                                            if msg_id == b'K' {
                                                // || msg_id == b'O' {
                                                // Key of the row to be updated
                                                // let tuple_data = &xlog_data[6..];
                                                off += 6;
                                                let col_num: i16 =
                                                    (&xlog_data[off..off + 2]).get_i16();

                                                // Number of columns with 2 bytes
                                                off += 2;

                                                // Ignore key updates and REPLICA IDENTITY FULL updates for now
                                                // Loop through to move offset forward
                                                for i in 0..col_num {
                                                    // skip if null
                                                    if xlog_data[off] != b'n' {
                                                        off += 1;
                                                        let val_len =
                                                            (&xlog_data[off..off + 4]).get_i32();

                                                        off += 4;

                                                        data_buf[state.data_buf_off
                                                            ..state.data_buf_off
                                                                + val_len as usize]
                                                            .copy_from_slice(
                                                                &xlog_data
                                                                    [off..off + val_len as usize],
                                                            );

                                                        match &mut state.delete_rows {
                                                            Some(buf) => {
                                                                if state.delete_rows_off
                                                                    + val_len as usize
                                                                    >= buf.len()
                                                                {
                                                                    eprintln!(
                                                                        "Delete rows buffer overflow. Consider increasing buffer size."
                                                                    );
                                                                } else {
                                                                    buf[state.delete_rows_off
                                                                        ..state.delete_rows_off
                                                                            + val_len as usize]
                                                                        .copy_from_slice(
                                                                            &xlog_data[off..off
                                                                                + val_len as usize],
                                                                        );
                                                                    state.delete_rows_off +=
                                                                        val_len as usize;
                                                                }
                                                            }
                                                            None => {
                                                                eprintln!(
                                                                    "Delete rows buffer not initialized."
                                                                );
                                                            }
                                                        }

                                                        state.data_buf_off += val_len as usize;
                                                        off += val_len as usize;
                                                    } else {
                                                        off += 1;
                                                    }

                                                    if (i + 1) < col_num {
                                                        add_delimiter_fn(b',', data_buf, state);
                                                    }
                                                }
                                                add_delimiter_fn(b'\n', data_buf, state);
                                            }
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
                                                let parts: &Vec<&[u8]> = &xlog_data[off + 1..]
                                                    .split(|&b| b == b'\0')
                                                    .collect();
                                                table_info.cols.push(
                                                    String::from_utf8_lossy(parts[0]).to_string(),
                                                );

                                                // Check if column is part of the key
                                                // '1' indicates part of key, '0' otherwise
                                                if xlog_data[off] == 1 {
                                                    let temp = String::from_utf8_lossy(parts[0])
                                                        .to_string();
                                                    if !table_info.key_columns.contains(&temp) {
                                                        table_info.key_columns.push(
                                                            String::from_utf8_lossy(parts[0])
                                                                .to_string(),
                                                        );
                                                    }
                                                }
                                                // 2 ints for column oid and column type modifier and C-string terminator and flags byte
                                                off += 10 + parts[0].len();
                                            }

                                            // Update out_resource with schema and key info if Iceberg
                                            match &table_info.out_resource {
                                                OutResource::CSVFile(_) => (),
                                                OutResource::Iceberg {
                                                    config_path,
                                                    schema: _,
                                                    key: _,
                                                } => {
                                                    table_info.out_resource = OutResource::Iceberg {
                                                        config_path: config_path.to_string(),
                                                        schema: Some(table_info.cols.clone()),
                                                        key: Some(table_info.key_columns.clone()),
                                                    }
                                                }
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
    fn test_build_valid_simple_query_message() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let server_addr = listener.local_addr().unwrap();
        let server_handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0u8; BUF_LEN];
            let resp_msg: &[u8] = b"Q\0\0\0\x16SELECT * FROM foo\0";
            let bytes_read = stream.read(&mut buf).unwrap();

            assert_eq!(bytes_read, 23);
            assert_eq!(resp_msg, &buf[..bytes_read]);
        });

        let mut stream = TcpStream::connect(server_addr).unwrap();
        let err = send_simple_query(&mut stream, "SELECT * FROM foo");
        assert!(err.is_none());
        server_handle.join().unwrap();
    }

    #[test]
    fn test_process_valid_data_rows() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let server_addr = listener.local_addr().unwrap();
        let server_handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = BytesMut::new();

            // Dummy table results sent by server has two columns and contains 2 rows
            buf.put_u8(b'D');
            buf.put_i32(26);
            buf.put_i16(2);
            buf.put_i32(5);
            buf.put_slice(b"Simba");
            buf.put_i32(7);
            buf.put_slice(b"Onyango");

            buf.put_u8(b'D');
            buf.put_i32(22);
            buf.put_i16(2);
            buf.put_i32(3);
            buf.put_slice(b"Lee");
            buf.put_i32(5);
            buf.put_slice(b"Kwach");

            stream.write_all(&buf).unwrap();
        });

        let mut stream = TcpStream::connect(server_addr).unwrap();
        let delete_rows = &mut [0u8; OUT_BUF_SIZE];
        let result_buf = &mut [0u8; OUT_BUF_SIZE];
        let state = &mut QueryState::new(delete_rows);
        let row_descr_buf = &mut BytesMut::new();
        let replication = &mut Replication::new("/");

        process_simple(&mut stream, state, result_buf, row_descr_buf, replication).unwrap();

        let resp_buf: &[u8] = b"Simba|Onyango\nLee|Kwach\n";
        assert_eq!(&result_buf[..state.data_buf_off], resp_buf);
        assert_eq!(
            state.data_buf_off,
            resp_buf.len(),
            "Should be 24 bytes given the contents of resp_buf"
        );

        server_handle.join().unwrap();
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
