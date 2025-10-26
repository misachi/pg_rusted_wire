use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{self, Seek};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

pub const HEADER_LEN: usize = 4096;
pub const SEGMENT_FILE_EXT: &str = ".seg";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    pub(crate) id: u64,
    pub(crate) size: u64,
    pub(crate) read_only: bool,
    pub(crate) has_merged: bool, // Whether the segment has been added to final destination e.g Iceberg
    pub(crate) last_written_pos: Option<u64>,
    pub(crate) last_written_time: Option<std::time::SystemTime>,
    pub(crate) max_size: u64,
    pub(crate) data_dir: String,
    pub(crate) created_at: std::time::SystemTime,
}

impl Segment {
    pub fn new(id: u64, dir: String) -> Self {
        let seg = Segment {
            id,
            size: HEADER_LEN as u64,
            read_only: false,
            has_merged: false,
            created_at: std::time::SystemTime::now(),
            last_written_pos: None,
            last_written_time: None,
            max_size: 10 * 1024 * 1024, // 10 MB
            data_dir: dir,
        };

        let path = Self::create_path_name(&seg.data_dir, seg.id);

        let segment_handle = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .expect("Error creating segment file");

        segment_handle
            .sync_all()
            .expect("Error creating segment file");
        seg
    }

    pub(crate) fn from_file_name(file_name: &str) -> io::Result<Self> {
        let id_str = file_name.trim_end_matches(SEGMENT_FILE_EXT);
        match id_str.parse::<u64>() {
            Ok(_) => {
                let path = Path::new(&file_name);
                let mut buf = [0; HEADER_LEN];
                let mut segment_handle = File::open(path)?;

                segment_handle.seek(io::SeekFrom::Start(0))?;
                segment_handle.read_exact(&mut buf)?;

                match Segment::decode(&String::from_utf8_lossy(&buf)) {
                    Ok(segment) => Ok(segment),
                    Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
                }
            }
            Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        }
    }

    pub(crate) fn last_write_interval(&self) -> io::Result<std::time::Duration> {
        // Compare last written time with created time
        let last_written_time = match self.last_written_time {
            Some(t) => t,
            None => self.created_at,
        };

        match last_written_time.elapsed() {
            Ok(dur) => Ok(dur),
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
        }
    }

    fn encode(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    fn decode(data: &str) -> Result<Segment, serde_json::Error> {
        let info: Segment = serde_json::from_str(data)?;
        Ok(info)
    }

    pub(crate) fn load_data(&self, pos: u64, size: u64) -> io::Result<Vec<u8>> {
        let segment_path = Self::create_path_name(&self.data_dir, self.id);
        let path = Path::new(&segment_path);
        let mut buf: Vec<u8>;
        if size <= 0 {
            buf = vec![0u8; self.size as usize + HEADER_LEN];
        } else {
            buf = vec![0u8; size as usize + HEADER_LEN];
        }
        let mut segment_handle = File::open(path)?;

        let pos = HEADER_LEN as u64 + pos; // Adjust offset by header length

        segment_handle.seek(io::SeekFrom::Start(pos))?;
        segment_handle.read(&mut buf)?;

        Ok(buf)
    }

    // Save replication state to file
    pub(crate) fn write(&mut self, data: &[u8]) -> io::Result<()> {
        // let path = Path::new(&self.meta_file_path);
        let mut buf = [0; HEADER_LEN];

        match self.encode() {
            Ok(serialized_data) => {
                buf[..serialized_data.as_bytes().len()].copy_from_slice(serialized_data.as_bytes());
                // handle.write_all(&buf[..serialized_data.as_bytes().len()])?;
                // handle.sync_all()?; // Flush to disk immediately
                self.write_to_disk(&buf, data)?;

                self.size += data.len() as u64;
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) fn write_header(&self) -> io::Result<()> {
        let segment_path = Self::create_path_name(&self.data_dir, self.id);
        let path = Path::new(&segment_path);
        let mut buf = [0; HEADER_LEN];

        match self.encode() {
            Ok(serialized_data) => {
                buf[..serialized_data.as_bytes().len()].copy_from_slice(serialized_data.as_bytes());
                let mut segment_handle = OpenOptions::new().write(true).create(true).open(path)?;

                segment_handle.seek(io::SeekFrom::Start(0))?;
                segment_handle.write(&buf[..serialized_data.as_bytes().len()])?;
                segment_handle.sync_all()?; // Flush to disk immediately

                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    fn create_path_name(dir: &str, id: u64) -> PathBuf {
        let segment_path = Path::new(dir).join(format!("{}{}", id, SEGMENT_FILE_EXT));
        segment_path
    }

    fn write_to_disk(&self, metadata: &[u8], data: &[u8]) -> io::Result<()> {
        let segment_path = Self::create_path_name(&self.data_dir, self.id);

        let mut segment_handle = OpenOptions::new()
            .write(true)
            .create(true)
            .open(segment_path)?;

        if metadata.len() <= 0 {
            return Ok(());
        }

        segment_handle.seek(io::SeekFrom::Start(0))?;
        segment_handle.write(metadata)?;
        segment_handle.seek(io::SeekFrom::Start(self.size))?;
        segment_handle.write(data)?;

        segment_handle.sync_all()?; // Flush to disk immediately

        Ok(())
    }
}
