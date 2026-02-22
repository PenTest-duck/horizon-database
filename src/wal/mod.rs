//! # Write-Ahead Log (WAL) Manager
//!
//! The WAL ensures durability and crash recovery for Horizon DB.
//! All page modifications are written to the WAL before the main database file.
//!
//! ## WAL File Format
//!
//! The WAL file (`.hdb-wal`) consists of a header followed by frames.
//!
//! ### WAL Header (32 bytes)
//! - [0..16]: Magic bytes `b"HorizonWAL v01\x00\x00"`
//! - [16..20]: Page size (u32 big-endian)
//! - [20..24]: Checkpoint sequence number (u32 big-endian)
//! - [24..32]: Salt (two u32 values for integrity checking)
//!
//! ### WAL Frame (PAGE_SIZE + 24 bytes each)
//! - [0..4]: Page number (u32 big-endian)
//! - [4..8]: Size of database in pages after commit (u32 big-endian, 0 if not a commit frame)
//! - [8..16]: Transaction ID (u64 big-endian)
//! - [16..20]: Checksum part 1 (u32)
//! - [20..24]: Checksum part 2 (u32)
//! - [24..24+PAGE_SIZE]: Page data

use crate::error::{HorizonError, Result};
use crate::pager::{PageId, PAGE_SIZE};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const WAL_MAGIC: &[u8; 16] = b"HorizonWAL v01\x00\x00";
const WAL_HEADER_SIZE: usize = 32;
const FRAME_HEADER_SIZE: usize = 24;
const FRAME_SIZE: usize = FRAME_HEADER_SIZE + PAGE_SIZE;

/// A WAL frame containing a page image.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct WalFrame {
    /// The page number this frame belongs to.
    page_id: PageId,
    /// Database size in pages after this commit (0 if not a commit frame).
    db_size: u32,
    /// Transaction ID that wrote this frame.
    txn_id: u64,
    /// The page data.
    data: Box<[u8; PAGE_SIZE]>,
}

/// Manages the Write-Ahead Log for crash recovery and durability.
pub struct WalManager {
    /// Path to the WAL file.
    path: PathBuf,
    /// WAL file handle.
    file: Option<File>,
    /// Number of frames in the WAL.
    frame_count: u32,
    /// Checkpoint sequence number.
    checkpoint_seq: u32,
    /// Index mapping page_id -> most recent frame index (0-based).
    /// Used for reading the latest version of a page from the WAL.
    page_index: HashMap<PageId, u32>,
    /// Salt values for integrity.
    salt: [u32; 2],
}

impl WalManager {
    /// Open or create a WAL file.
    pub fn open(path: &Path) -> Result<Self> {
        let exists = path.exists();

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        if exists && file.metadata()?.len() >= WAL_HEADER_SIZE as u64 {
            // Read existing WAL
            let mut header = [0u8; WAL_HEADER_SIZE];
            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut header)?;

            // Validate magic
            if &header[0..16] != WAL_MAGIC {
                return Err(HorizonError::CorruptDatabase(
                    "invalid WAL magic bytes".into(),
                ));
            }

            let page_size = u32::from_be_bytes(header[16..20].try_into().unwrap());
            if page_size != PAGE_SIZE as u32 {
                return Err(HorizonError::CorruptDatabase(format!(
                    "WAL page size mismatch: expected {}, got {}",
                    PAGE_SIZE, page_size
                )));
            }

            let checkpoint_seq = u32::from_be_bytes(header[20..24].try_into().unwrap());
            let salt = [
                u32::from_be_bytes(header[24..28].try_into().unwrap()),
                u32::from_be_bytes(header[28..32].try_into().unwrap()),
            ];

            // Build page index by scanning all frames
            let file_len = file.metadata()?.len();
            let data_len = file_len as usize - WAL_HEADER_SIZE;
            let frame_count = (data_len / FRAME_SIZE) as u32;

            let mut page_index = HashMap::new();
            for i in 0..frame_count {
                let offset = WAL_HEADER_SIZE as u64 + (i as u64) * FRAME_SIZE as u64;
                file.seek(SeekFrom::Start(offset))?;
                let mut frame_header = [0u8; FRAME_HEADER_SIZE];
                file.read_exact(&mut frame_header)?;

                let page_id = u32::from_be_bytes(frame_header[0..4].try_into().unwrap());
                page_index.insert(page_id, i);
            }

            Ok(WalManager {
                path: path.to_path_buf(),
                file: Some(file),
                frame_count,
                checkpoint_seq,
                page_index,
                salt,
            })
        } else {
            // Initialize new WAL
            let salt = [0x12345678u32, 0x9ABCDEF0u32];
            let mut header = [0u8; WAL_HEADER_SIZE];
            header[0..16].copy_from_slice(WAL_MAGIC);
            header[16..20].copy_from_slice(&(PAGE_SIZE as u32).to_be_bytes());
            header[20..24].copy_from_slice(&0u32.to_be_bytes());
            header[24..28].copy_from_slice(&salt[0].to_be_bytes());
            header[28..32].copy_from_slice(&salt[1].to_be_bytes());

            file.seek(SeekFrom::Start(0))?;
            file.write_all(&header)?;
            file.sync_all()?;

            Ok(WalManager {
                path: path.to_path_buf(),
                file: Some(file),
                frame_count: 0,
                checkpoint_seq: 0,
                page_index: HashMap::new(),
                salt,
            })
        }
    }

    /// Write a page to the WAL.
    ///
    /// The page data is appended as a new frame. If `is_commit` is true,
    /// the `db_size` field is set to indicate this is the last frame of a transaction.
    pub fn write_frame(
        &mut self,
        page_id: PageId,
        data: &[u8; PAGE_SIZE],
        txn_id: u64,
        is_commit: bool,
        db_size: u32,
    ) -> Result<()> {
        // Compute checksum before borrowing file mutably
        let checksum = self.compute_checksum(data);

        let file = self
            .file
            .as_mut()
            .ok_or_else(|| HorizonError::Internal("WAL file not open".into()))?;

        let offset = WAL_HEADER_SIZE as u64 + (self.frame_count as u64) * FRAME_SIZE as u64;
        file.seek(SeekFrom::Start(offset))?;

        // Write frame header
        let mut frame_header = [0u8; FRAME_HEADER_SIZE];
        frame_header[0..4].copy_from_slice(&page_id.to_be_bytes());
        let commit_size = if is_commit { db_size } else { 0 };
        frame_header[4..8].copy_from_slice(&commit_size.to_be_bytes());
        frame_header[8..16].copy_from_slice(&txn_id.to_be_bytes());
        frame_header[16..20].copy_from_slice(&checksum.0.to_be_bytes());
        frame_header[20..24].copy_from_slice(&checksum.1.to_be_bytes());

        file.write_all(&frame_header)?;
        file.write_all(data)?;

        // Update index
        self.page_index.insert(page_id, self.frame_count);
        self.frame_count += 1;

        if is_commit {
            file.sync_data()?;
        }

        Ok(())
    }

    /// Read the most recent version of a page from the WAL.
    ///
    /// Returns `None` if the page is not in the WAL.
    pub fn read_page(&self, page_id: PageId) -> Result<Option<[u8; PAGE_SIZE]>> {
        let frame_index = match self.page_index.get(&page_id) {
            Some(&idx) => idx,
            None => return Ok(None),
        };

        let _file = self
            .file
            .as_ref()
            .ok_or_else(|| HorizonError::Internal("WAL file not open".into()))?;

        let offset = WAL_HEADER_SIZE as u64
            + (frame_index as u64) * FRAME_SIZE as u64
            + FRAME_HEADER_SIZE as u64;

        let mut data = [0u8; PAGE_SIZE];
        // Use a separate reader to avoid needing &mut self
        let mut reader = File::open(&self.path)?;
        reader.seek(SeekFrom::Start(offset))?;
        reader.read_exact(&mut data)?;

        Ok(Some(data))
    }

    /// Check if the WAL contains a specific page.
    pub fn contains_page(&self, page_id: PageId) -> bool {
        self.page_index.contains_key(&page_id)
    }

    /// Checkpoint: write all WAL pages back to the main database file.
    ///
    /// This transfers all committed frames from the WAL into the main database
    /// file, then resets the WAL.
    pub fn checkpoint(&mut self, pager: &mut crate::pager::Pager) -> Result<()> {
        if self.frame_count == 0 {
            return Ok(());
        }

        let file = self
            .file
            .as_mut()
            .ok_or_else(|| HorizonError::Internal("WAL file not open".into()))?;

        // Read each frame and write it to the pager
        for frame_idx in 0..self.frame_count {
            let offset = WAL_HEADER_SIZE as u64 + (frame_idx as u64) * FRAME_SIZE as u64;
            file.seek(SeekFrom::Start(offset))?;

            // Read frame header
            let mut frame_header = [0u8; FRAME_HEADER_SIZE];
            file.read_exact(&mut frame_header)?;
            let page_id = u32::from_be_bytes(frame_header[0..4].try_into().unwrap());

            // Read page data
            let mut data = [0u8; PAGE_SIZE];
            file.read_exact(&mut data)?;

            // Write to main database
            pager.write_page(page_id, &data)?;
        }

        pager.sync()?;

        // Reset WAL
        self.frame_count = 0;
        self.checkpoint_seq += 1;
        self.page_index.clear();

        // Rewrite WAL header with new checkpoint sequence
        file.seek(SeekFrom::Start(0))?;
        let mut header = [0u8; WAL_HEADER_SIZE];
        header[0..16].copy_from_slice(WAL_MAGIC);
        header[16..20].copy_from_slice(&(PAGE_SIZE as u32).to_be_bytes());
        header[20..24].copy_from_slice(&self.checkpoint_seq.to_be_bytes());
        header[24..28].copy_from_slice(&self.salt[0].to_be_bytes());
        header[28..32].copy_from_slice(&self.salt[1].to_be_bytes());
        file.write_all(&header)?;

        // Truncate WAL file to just the header
        file.set_len(WAL_HEADER_SIZE as u64)?;
        file.sync_all()?;

        Ok(())
    }

    /// Get the number of frames in the WAL.
    pub fn frame_count(&self) -> u32 {
        self.frame_count
    }

    /// Compute a simple checksum for integrity checking.
    fn compute_checksum(&self, data: &[u8; PAGE_SIZE]) -> (u32, u32) {
        let mut s1: u32 = self.salt[0];
        let mut s2: u32 = self.salt[1];
        for chunk in data.chunks(4) {
            let val = if chunk.len() == 4 {
                u32::from_be_bytes(chunk.try_into().unwrap())
            } else {
                let mut buf = [0u8; 4];
                buf[..chunk.len()].copy_from_slice(chunk);
                u32::from_be_bytes(buf)
            };
            s1 = s1.wrapping_add(val);
            s2 = s2.wrapping_add(s1);
        }
        (s1, s2)
    }

    /// Reset the WAL, discarding all uncommitted changes.
    pub fn reset(&mut self) -> Result<()> {
        self.frame_count = 0;
        self.page_index.clear();

        if let Some(ref mut file) = self.file {
            file.set_len(WAL_HEADER_SIZE as u64)?;
            file.sync_all()?;
        }

        Ok(())
    }
}

impl Drop for WalManager {
    fn drop(&mut self) {
        // Best-effort cleanup
        if let Some(ref mut file) = self.file {
            let _ = file.sync_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_wal() -> (TempDir, WalManager) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.hdb-wal");
        let wal = WalManager::open(&path).unwrap();
        (dir, wal)
    }

    #[test]
    fn test_create_new_wal() {
        let (_dir, wal) = test_wal();
        assert_eq!(wal.frame_count(), 0);
    }

    #[test]
    fn test_write_and_read_frame() {
        let (_dir, mut wal) = test_wal();

        let mut page_data = [0u8; PAGE_SIZE];
        page_data[0] = 42;
        page_data[100] = 99;

        wal.write_frame(5, &page_data, 1, true, 10).unwrap();

        assert_eq!(wal.frame_count(), 1);
        assert!(wal.contains_page(5));
        assert!(!wal.contains_page(6));

        let read_back = wal.read_page(5).unwrap().unwrap();
        assert_eq!(read_back[0], 42);
        assert_eq!(read_back[100], 99);
    }

    #[test]
    fn test_multiple_frames_same_page() {
        let (_dir, mut wal) = test_wal();

        let mut data1 = [0u8; PAGE_SIZE];
        data1[0] = 1;
        wal.write_frame(3, &data1, 1, false, 0).unwrap();

        let mut data2 = [0u8; PAGE_SIZE];
        data2[0] = 2;
        wal.write_frame(3, &data2, 1, true, 10).unwrap();

        // Should read the latest version
        let read_back = wal.read_page(3).unwrap().unwrap();
        assert_eq!(read_back[0], 2);
    }

    #[test]
    fn test_reopen_wal() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.hdb-wal");

        {
            let mut wal = WalManager::open(&path).unwrap();
            let mut data = [0u8; PAGE_SIZE];
            data[0] = 77;
            wal.write_frame(1, &data, 1, true, 5).unwrap();
        }

        // Reopen
        let wal = WalManager::open(&path).unwrap();
        assert_eq!(wal.frame_count(), 1);
        assert!(wal.contains_page(1));

        let read_back = wal.read_page(1).unwrap().unwrap();
        assert_eq!(read_back[0], 77);
    }

    #[test]
    fn test_page_not_in_wal() {
        let (_dir, wal) = test_wal();
        assert!(wal.read_page(999).unwrap().is_none());
    }

    #[test]
    fn test_reset_wal() {
        let (_dir, mut wal) = test_wal();

        let data = [0u8; PAGE_SIZE];
        wal.write_frame(1, &data, 1, true, 5).unwrap();
        assert_eq!(wal.frame_count(), 1);

        wal.reset().unwrap();
        assert_eq!(wal.frame_count(), 0);
        assert!(!wal.contains_page(1));
    }
}
