//! Low-level page I/O for Horizon DB.
//!
//! The [`Pager`] is the lowest-level storage abstraction in the database
//! engine.  It views the database file as a flat sequence of fixed-size
//! **pages** ([`PAGE_SIZE`] = 4 096 bytes each) and provides simple
//! read / write / allocate / free operations on those pages.
//!
//! The pager knows nothing about the *contents* of pages -- it deals
//! exclusively in raw `[u8; PAGE_SIZE]` buffers.  Higher layers (the
//! B-tree module, the buffer pool, the WAL) build their own structure on
//! top.
//!
//! # File header
//!
//! Page 0 contains a 100-byte file header that stores database-wide
//! metadata.  See [`HEADER_SIZE`] and [`MAGIC`] for the exact layout.
//!
//! # Free list
//!
//! Freed pages are chained together in a singly-linked list.  Each free
//! page stores the [`PageId`] of the next free page in its first four
//! bytes (big-endian).  [`Pager::allocate_page`] pops the head of this
//! list; [`Pager::free_page`] pushes onto it.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::{HorizonError, Result};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Every page in the database file is exactly this many bytes.
pub const PAGE_SIZE: usize = 4096;

/// The file header occupies the first 100 bytes of page 0.
pub const HEADER_SIZE: usize = 100;

/// Magic bytes written at offset 0 of every valid Horizon DB file.
///
/// Layout: `b"HorizonDB v001\x00\x00"` (16 bytes, null-padded).
pub const MAGIC: &[u8; 16] = b"HorizonDB v001\x00\x00";

// ---------------------------------------------------------------------------
// PageId
// ---------------------------------------------------------------------------

/// A zero-based page number.  Page 0 is always the header page.
pub type PageId = u32;

// ---------------------------------------------------------------------------
// Pager
// ---------------------------------------------------------------------------

/// Manages a database file as a flat array of [`PAGE_SIZE`]-byte pages.
///
/// The pager is responsible for:
///
/// * Reading and writing individual pages by [`PageId`].
/// * Maintaining the file header (magic, page count, free-list head, etc.).
/// * Allocating new pages (from the free list or by extending the file).
/// * Freeing pages (pushing them onto the free list).
/// * Assigning monotonically-increasing transaction IDs.
///
/// It does **not** cache pages in memory -- that is the job of the buffer
/// pool sitting above it.
#[derive(Debug)]
pub struct Pager {
    /// The underlying database file handle.
    file: File,
    /// Total number of pages currently in the file (including page 0).
    page_count: u32,
    /// Head of the singly-linked free-page list (`0` means empty).
    free_list_head: PageId,
    /// The next transaction ID to hand out.
    next_txn_id: u64,
    /// Root page of the schema table B-tree (`0` means not yet created).
    schema_root: PageId,
    /// Current schema version number.
    schema_version: u32,
    /// When `true`, all mutating operations will return
    /// [`HorizonError::ReadOnly`].
    read_only: bool,
}

impl Pager {
    // ---------------------------------------------------------------------
    // Construction
    // ---------------------------------------------------------------------

    /// Open an existing database file, or create a new one if it does not
    /// exist.
    ///
    /// When `read_only` is `true` the file is opened without write
    /// permissions and every mutating method will return
    /// [`HorizonError::ReadOnly`].
    ///
    /// # Errors
    ///
    /// * [`HorizonError::Io`] -- the file could not be opened or created.
    /// * [`HorizonError::CorruptDatabase`] -- the file exists but contains
    ///   invalid magic bytes or is shorter than a single page.
    pub fn open(path: &Path, read_only: bool) -> Result<Self> {
        let file = if read_only {
            OpenOptions::new().read(true).open(path)?
        } else {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?
        };

        let file_len = file.metadata()?.len();

        if file_len == 0 {
            // ---- brand-new database -----------------------------------
            if read_only {
                return Err(HorizonError::ReadOnly(
                    "cannot create a new database in read-only mode".into(),
                ));
            }

            let mut pager = Pager {
                file,
                page_count: 1,
                free_list_head: 0,
                next_txn_id: 1,
                schema_root: 0,
                schema_version: 0,
                read_only,
            };

            // Write a zeroed page 0 first, then stamp the header on it.
            let blank = [0u8; PAGE_SIZE];
            pager.file.seek(SeekFrom::Start(0))?;
            pager.file.write_all(&blank)?;
            pager.flush_header()?;
            pager.file.sync_all()?;

            Ok(pager)
        } else {
            // ---- existing database ------------------------------------
            if file_len < PAGE_SIZE as u64 {
                return Err(HorizonError::CorruptDatabase(
                    "file is shorter than a single page".into(),
                ));
            }

            let mut pager = Pager {
                file,
                page_count: 0,
                free_list_head: 0,
                next_txn_id: 0,
                schema_root: 0,
                schema_version: 0,
                read_only,
            };

            pager.read_header()?;

            Ok(pager)
        }
    }

    // ---------------------------------------------------------------------
    // Page I/O
    // ---------------------------------------------------------------------

    /// Read the page identified by `page_id` into a `[u8; PAGE_SIZE]`
    /// buffer and return it.
    ///
    /// # Errors
    ///
    /// * [`HorizonError::PageNotFound`] -- `page_id` is out of range.
    /// * [`HorizonError::Io`] -- the underlying read failed.
    pub fn read_page(&self, page_id: PageId) -> Result<[u8; PAGE_SIZE]> {
        if page_id >= self.page_count {
            return Err(HorizonError::PageNotFound(page_id));
        }

        let offset = page_id as u64 * PAGE_SIZE as u64;
        let mut buf = [0u8; PAGE_SIZE];

        // `File` does not require `&mut self` for `read_at`-style access
        // when using pread under the hood, but the `Read` trait does.
        // We use a second reference obtained via `(&self.file)` so that
        // we can call `seek` + `read_exact` without requiring `&mut self`.
        let file = &self.file;
        (&*file).seek(SeekFrom::Start(offset))?;
        (&*file).read_exact(&mut buf)?;

        Ok(buf)
    }

    /// Write `data` to the page identified by `page_id`.
    ///
    /// # Errors
    ///
    /// * [`HorizonError::ReadOnly`] -- the pager was opened read-only.
    /// * [`HorizonError::PageNotFound`] -- `page_id` is out of range.
    /// * [`HorizonError::Io`] -- the underlying write failed.
    pub fn write_page(&mut self, page_id: PageId, data: &[u8; PAGE_SIZE]) -> Result<()> {
        self.ensure_writable()?;

        if page_id >= self.page_count {
            return Err(HorizonError::PageNotFound(page_id));
        }

        let offset = page_id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(data)?;

        Ok(())
    }

    // ---------------------------------------------------------------------
    // Page allocation / deallocation
    // ---------------------------------------------------------------------

    /// Allocate a page and return its [`PageId`].
    ///
    /// If the free list is non-empty the head page is recycled; otherwise
    /// the file is extended by one page.
    ///
    /// # Errors
    ///
    /// * [`HorizonError::ReadOnly`] -- the pager was opened read-only.
    /// * [`HorizonError::Io`] -- disk I/O failed.
    pub fn allocate_page(&mut self) -> Result<PageId> {
        self.ensure_writable()?;

        if self.free_list_head != 0 {
            // Pop the head of the free list.
            let page_id = self.free_list_head;
            let page = self.read_page(page_id)?;
            let next = u32::from_be_bytes([page[0], page[1], page[2], page[3]]);
            self.free_list_head = next;

            // Zero out the recycled page so callers start with a clean
            // slate.
            let blank = [0u8; PAGE_SIZE];
            self.write_page(page_id, &blank)?;

            self.flush_header()?;
            Ok(page_id)
        } else {
            // Extend the file by one page.
            let page_id = self.page_count;
            self.page_count += 1;

            let blank = [0u8; PAGE_SIZE];
            let offset = page_id as u64 * PAGE_SIZE as u64;
            self.file.seek(SeekFrom::Start(offset))?;
            self.file.write_all(&blank)?;

            self.flush_header()?;
            Ok(page_id)
        }
    }

    /// Return `page_id` to the free list so it can be reused by a future
    /// [`allocate_page`](Self::allocate_page) call.
    ///
    /// The page is overwritten: the first four bytes store the previous
    /// free-list head (big-endian), and the remainder is zeroed.
    ///
    /// # Errors
    ///
    /// * [`HorizonError::ReadOnly`] -- the pager was opened read-only.
    /// * [`HorizonError::PageNotFound`] -- `page_id` is out of range.
    /// * [`HorizonError::CorruptDatabase`] -- attempt to free page 0.
    /// * [`HorizonError::Io`] -- disk I/O failed.
    pub fn free_page(&mut self, page_id: PageId) -> Result<()> {
        self.ensure_writable()?;

        if page_id == 0 {
            return Err(HorizonError::CorruptDatabase(
                "cannot free the header page (page 0)".into(),
            ));
        }
        if page_id >= self.page_count {
            return Err(HorizonError::PageNotFound(page_id));
        }

        // Build the free-page payload: [next_free (4 bytes)] ++ [zeros].
        let mut page = [0u8; PAGE_SIZE];
        page[0..4].copy_from_slice(&self.free_list_head.to_be_bytes());

        self.write_page(page_id, &page)?;

        self.free_list_head = page_id;
        self.flush_header()?;

        Ok(())
    }

    // ---------------------------------------------------------------------
    // Accessors
    // ---------------------------------------------------------------------

    /// Return the total number of pages in the database file, including the
    /// header page.
    #[inline]
    pub fn page_count(&self) -> u32 {
        self.page_count
    }

    /// Atomically increment the internal transaction counter and return the
    /// new value.
    ///
    /// The caller is responsible for calling [`flush_header`](Self::flush_header)
    /// at an appropriate time to persist the updated counter.
    pub fn next_txn_id(&mut self) -> u64 {
        let id = self.next_txn_id;
        self.next_txn_id += 1;
        id
    }

    /// Return the [`PageId`] of the schema-table B-tree root (`0` if no
    /// schema has been created yet).
    #[inline]
    pub fn schema_root(&self) -> PageId {
        self.schema_root
    }

    /// Set the schema-table root page and persist the change to the file
    /// header.
    ///
    /// # Errors
    ///
    /// * [`HorizonError::ReadOnly`] -- the pager was opened read-only.
    /// * [`HorizonError::Io`] -- disk I/O failed.
    pub fn set_schema_root(&mut self, page_id: PageId) -> Result<()> {
        self.ensure_writable()?;
        self.schema_root = page_id;
        self.flush_header()
    }

    // ---------------------------------------------------------------------
    // Header persistence
    // ---------------------------------------------------------------------

    /// Serialize all in-memory metadata fields into the first
    /// [`HEADER_SIZE`] bytes of page 0 and write the entire page back to
    /// disk.
    ///
    /// Fields that occupy the "reserved" portion of the header
    /// ([44..100]) are written as zeros.
    ///
    /// # Errors
    ///
    /// * [`HorizonError::ReadOnly`] -- the pager was opened read-only.
    /// * [`HorizonError::Io`] -- disk I/O failed.
    pub fn flush_header(&mut self) -> Result<()> {
        self.ensure_writable()?;

        // Read the current page 0 so that we preserve any data that lives
        // *after* the header (bytes [100..4096]).
        let mut page = [0u8; PAGE_SIZE];
        self.file.seek(SeekFrom::Start(0))?;
        // It is fine if we read fewer bytes (e.g. during initial creation);
        // the buffer is already zeroed.
        let _ = (&self.file).read(&mut page);

        // Stamp the header fields.
        page[0..16].copy_from_slice(MAGIC);
        page[16..20].copy_from_slice(&(PAGE_SIZE as u32).to_be_bytes());
        page[20..24].copy_from_slice(&self.page_count.to_be_bytes());
        page[24..28].copy_from_slice(&self.free_list_head.to_be_bytes());
        page[28..32].copy_from_slice(&self.schema_version.to_be_bytes());
        page[32..40].copy_from_slice(&self.next_txn_id.to_be_bytes());
        page[40..44].copy_from_slice(&self.schema_root.to_be_bytes());
        // [44..100] reserved -- ensure they are zeroed.
        page[44..HEADER_SIZE].fill(0);

        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&page)?;

        Ok(())
    }

    /// Call `fsync` (or the platform equivalent) to ensure that all
    /// previously written data has been durably flushed to the underlying
    /// storage device.
    ///
    /// # Errors
    ///
    /// * [`HorizonError::Io`] -- the sync failed.
    pub fn sync(&self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    // ---------------------------------------------------------------------
    // Private helpers
    // ---------------------------------------------------------------------

    /// Read and validate the file header from page 0, populating all
    /// in-memory metadata fields.
    fn read_header(&mut self) -> Result<()> {
        let mut header = [0u8; HEADER_SIZE];
        self.file.seek(SeekFrom::Start(0))?;
        self.file.read_exact(&mut header)?;

        // Validate magic bytes.
        if &header[0..16] != MAGIC {
            return Err(HorizonError::CorruptDatabase(
                "invalid magic bytes -- not a Horizon DB file".into(),
            ));
        }

        // Validate page size.
        let stored_page_size = u32::from_be_bytes([
            header[16], header[17], header[18], header[19],
        ]) as usize;
        if stored_page_size != PAGE_SIZE {
            return Err(HorizonError::CorruptDatabase(format!(
                "unexpected page size {stored_page_size} (expected {PAGE_SIZE})"
            )));
        }

        self.page_count = u32::from_be_bytes([
            header[20], header[21], header[22], header[23],
        ]);
        self.free_list_head = u32::from_be_bytes([
            header[24], header[25], header[26], header[27],
        ]);
        self.schema_version = u32::from_be_bytes([
            header[28], header[29], header[30], header[31],
        ]);
        self.next_txn_id = u64::from_be_bytes([
            header[32], header[33], header[34], header[35],
            header[36], header[37], header[38], header[39],
        ]);
        self.schema_root = u32::from_be_bytes([
            header[40], header[41], header[42], header[43],
        ]);

        Ok(())
    }

    /// Return `Err(HorizonError::ReadOnly(..))` when the pager was opened
    /// in read-only mode.
    #[inline]
    fn ensure_writable(&self) -> Result<()> {
        if self.read_only {
            Err(HorizonError::ReadOnly(
                "cannot mutate a read-only database".into(),
            ))
        } else {
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    /// Helper: create a fresh `Pager` backed by a temporary file.
    fn new_pager() -> (Pager, NamedTempFile) {
        let tmp = NamedTempFile::new().expect("failed to create temp file");
        let pager = Pager::open(tmp.path(), false).expect("failed to open pager");
        (pager, tmp)
    }

    // ----- Creation & header -------------------------------------------

    #[test]
    fn new_database_has_correct_defaults() {
        let (pager, _tmp) = new_pager();
        assert_eq!(pager.page_count(), 1);
        assert_eq!(pager.schema_root(), 0);
    }

    #[test]
    fn magic_bytes_are_written() {
        let (pager, _tmp) = new_pager();
        let page0 = pager.read_page(0).unwrap();
        assert_eq!(&page0[0..16], MAGIC);
    }

    #[test]
    fn header_round_trips() {
        let tmp = NamedTempFile::new().unwrap();

        {
            let mut pager = Pager::open(tmp.path(), false).unwrap();
            pager.set_schema_root(42).unwrap();
            let _id = pager.next_txn_id(); // bumps to 2
            pager.flush_header().unwrap();
            pager.sync().unwrap();
        }

        // Re-open and verify.
        let pager = Pager::open(tmp.path(), true).unwrap();
        assert_eq!(pager.schema_root(), 42);
        assert_eq!(pager.page_count(), 1);
    }

    #[test]
    fn reopen_preserves_page_count() {
        let tmp = NamedTempFile::new().unwrap();

        {
            let mut pager = Pager::open(tmp.path(), false).unwrap();
            let _ = pager.allocate_page().unwrap(); // page 1
            let _ = pager.allocate_page().unwrap(); // page 2
            pager.sync().unwrap();
        }

        let pager = Pager::open(tmp.path(), true).unwrap();
        assert_eq!(pager.page_count(), 3);
    }

    // ----- Read / write ------------------------------------------------

    #[test]
    fn write_then_read_page() {
        let (mut pager, _tmp) = new_pager();
        let page_id = pager.allocate_page().unwrap();

        let mut data = [0u8; PAGE_SIZE];
        data[0] = 0xCA;
        data[1] = 0xFE;
        data[PAGE_SIZE - 1] = 0xFF;
        pager.write_page(page_id, &data).unwrap();

        let read_back = pager.read_page(page_id).unwrap();
        assert_eq!(read_back[0], 0xCA);
        assert_eq!(read_back[1], 0xFE);
        assert_eq!(read_back[PAGE_SIZE - 1], 0xFF);
    }

    #[test]
    fn read_out_of_range_returns_error() {
        let (pager, _tmp) = new_pager();
        let err = pager.read_page(999).unwrap_err();
        assert!(matches!(err, HorizonError::PageNotFound(999)));
    }

    #[test]
    fn write_out_of_range_returns_error() {
        let (mut pager, _tmp) = new_pager();
        let data = [0u8; PAGE_SIZE];
        let err = pager.write_page(999, &data).unwrap_err();
        assert!(matches!(err, HorizonError::PageNotFound(999)));
    }

    // ----- Allocation --------------------------------------------------

    #[test]
    fn allocate_extends_file() {
        let (mut pager, _tmp) = new_pager();
        assert_eq!(pager.page_count(), 1);

        let p1 = pager.allocate_page().unwrap();
        assert_eq!(p1, 1);
        assert_eq!(pager.page_count(), 2);

        let p2 = pager.allocate_page().unwrap();
        assert_eq!(p2, 2);
        assert_eq!(pager.page_count(), 3);
    }

    #[test]
    fn allocated_page_is_zeroed() {
        let (mut pager, _tmp) = new_pager();
        let pid = pager.allocate_page().unwrap();
        let page = pager.read_page(pid).unwrap();
        assert!(page.iter().all(|&b| b == 0));
    }

    // ----- Free list ---------------------------------------------------

    #[test]
    fn free_and_reuse_page() {
        let (mut pager, _tmp) = new_pager();

        let p1 = pager.allocate_page().unwrap();
        let p2 = pager.allocate_page().unwrap();
        assert_eq!(pager.page_count(), 3);

        // Free p1, then allocate -- should get p1 back.
        pager.free_page(p1).unwrap();
        let recycled = pager.allocate_page().unwrap();
        assert_eq!(recycled, p1);

        // Page count should not have grown.
        assert_eq!(pager.page_count(), 3);

        // The recycled page must be zeroed.
        let page = pager.read_page(recycled).unwrap();
        assert!(page.iter().all(|&b| b == 0));

        // Next allocation should extend again because p2 was never freed.
        let p3 = pager.allocate_page().unwrap();
        assert_eq!(p3, 3);
        assert_eq!(pager.page_count(), 4);

        // Drop p2 silently -- just making sure we used it.
        let _ = p2;
    }

    #[test]
    fn free_list_is_lifo() {
        let (mut pager, _tmp) = new_pager();

        let p1 = pager.allocate_page().unwrap();
        let p2 = pager.allocate_page().unwrap();
        let p3 = pager.allocate_page().unwrap();

        // Free in order: p1, p2, p3  -->  head is p3.
        pager.free_page(p1).unwrap();
        pager.free_page(p2).unwrap();
        pager.free_page(p3).unwrap();

        // Allocations should come back in LIFO order.
        assert_eq!(pager.allocate_page().unwrap(), p3);
        assert_eq!(pager.allocate_page().unwrap(), p2);
        assert_eq!(pager.allocate_page().unwrap(), p1);
    }

    #[test]
    fn cannot_free_page_zero() {
        let (mut pager, _tmp) = new_pager();
        let err = pager.free_page(0).unwrap_err();
        assert!(matches!(err, HorizonError::CorruptDatabase(_)));
    }

    #[test]
    fn free_out_of_range_returns_error() {
        let (mut pager, _tmp) = new_pager();
        let err = pager.free_page(999).unwrap_err();
        assert!(matches!(err, HorizonError::PageNotFound(999)));
    }

    #[test]
    fn free_list_survives_reopen() {
        let tmp = NamedTempFile::new().unwrap();

        {
            let mut pager = Pager::open(tmp.path(), false).unwrap();
            let p1 = pager.allocate_page().unwrap();
            let _p2 = pager.allocate_page().unwrap();
            pager.free_page(p1).unwrap();
            pager.sync().unwrap();
        }

        {
            let mut pager = Pager::open(tmp.path(), false).unwrap();
            // The free list should still contain p1.
            let recycled = pager.allocate_page().unwrap();
            assert_eq!(recycled, 1);
        }
    }

    // ----- Transaction IDs ---------------------------------------------

    #[test]
    fn next_txn_id_increments() {
        let (mut pager, _tmp) = new_pager();
        let first = pager.next_txn_id();
        let second = pager.next_txn_id();
        assert_eq!(second, first + 1);
    }

    // ----- Schema root -------------------------------------------------

    #[test]
    fn set_and_get_schema_root() {
        let (mut pager, _tmp) = new_pager();
        assert_eq!(pager.schema_root(), 0);
        pager.set_schema_root(7).unwrap();
        assert_eq!(pager.schema_root(), 7);
    }

    // ----- Read-only mode ----------------------------------------------

    #[test]
    fn read_only_rejects_writes() {
        let tmp = NamedTempFile::new().unwrap();

        // Create a valid database first.
        {
            let _pager = Pager::open(tmp.path(), false).unwrap();
        }

        let mut pager = Pager::open(tmp.path(), true).unwrap();
        let data = [0u8; PAGE_SIZE];

        assert!(matches!(
            pager.write_page(0, &data).unwrap_err(),
            HorizonError::ReadOnly(_)
        ));
        assert!(matches!(
            pager.allocate_page().unwrap_err(),
            HorizonError::ReadOnly(_)
        ));
        assert!(matches!(
            pager.free_page(1).unwrap_err(),
            HorizonError::ReadOnly(_)
        ));
        assert!(matches!(
            pager.set_schema_root(5).unwrap_err(),
            HorizonError::ReadOnly(_)
        ));
        assert!(matches!(
            pager.flush_header().unwrap_err(),
            HorizonError::ReadOnly(_)
        ));
    }

    #[test]
    fn read_only_allows_reads() {
        let tmp = NamedTempFile::new().unwrap();

        {
            let mut pager = Pager::open(tmp.path(), false).unwrap();
            let _ = pager.allocate_page().unwrap();
            pager.sync().unwrap();
        }

        let pager = Pager::open(tmp.path(), true).unwrap();
        assert!(pager.read_page(0).is_ok());
        assert!(pager.read_page(1).is_ok());
        assert_eq!(pager.page_count(), 2);
    }

    // ----- Corrupt / invalid files -------------------------------------

    #[test]
    fn rejects_bad_magic() {
        let tmp = NamedTempFile::new().unwrap();

        // Write a page-sized file with garbage magic.
        {
            let mut f = File::create(tmp.path()).unwrap();
            let page = [0xFFu8; PAGE_SIZE];
            f.write_all(&page).unwrap();
            f.sync_all().unwrap();
        }

        let err = Pager::open(tmp.path(), false).unwrap_err();
        assert!(matches!(err, HorizonError::CorruptDatabase(_)));
    }

    #[test]
    fn rejects_truncated_file() {
        let tmp = NamedTempFile::new().unwrap();

        // Write fewer than PAGE_SIZE bytes.
        {
            let mut f = File::create(tmp.path()).unwrap();
            f.write_all(&[0u8; 50]).unwrap();
            f.sync_all().unwrap();
        }

        let err = Pager::open(tmp.path(), false).unwrap_err();
        assert!(matches!(err, HorizonError::CorruptDatabase(_)));
    }

    // ----- Sync --------------------------------------------------------

    #[test]
    fn sync_does_not_error() {
        let (pager, _tmp) = new_pager();
        pager.sync().unwrap();
    }

    // ----- Page-size stored in header ----------------------------------

    #[test]
    fn header_stores_page_size() {
        let (pager, _tmp) = new_pager();
        let page0 = pager.read_page(0).unwrap();
        let stored = u32::from_be_bytes([page0[16], page0[17], page0[18], page0[19]]);
        assert_eq!(stored as usize, PAGE_SIZE);
    }

    // ----- Large allocation sequence -----------------------------------

    #[test]
    fn allocate_many_pages() {
        let (mut pager, _tmp) = new_pager();
        for i in 1..=100 {
            let pid = pager.allocate_page().unwrap();
            assert_eq!(pid, i);
        }
        assert_eq!(pager.page_count(), 101);
    }

    // ----- Mixed allocate / free / reopen ------------------------------

    #[test]
    fn complex_alloc_free_reopen() {
        let tmp = NamedTempFile::new().unwrap();

        {
            let mut pager = Pager::open(tmp.path(), false).unwrap();
            let p1 = pager.allocate_page().unwrap(); // 1
            let p2 = pager.allocate_page().unwrap(); // 2
            let p3 = pager.allocate_page().unwrap(); // 3

            // Write some data so we can verify after reopen.
            let mut data = [0u8; PAGE_SIZE];
            data[100] = 0xAB;
            pager.write_page(p3, &data).unwrap();

            // Free p1 and p2.
            pager.free_page(p1).unwrap();
            pager.free_page(p2).unwrap();

            pager.sync().unwrap();
        }

        {
            let mut pager = Pager::open(tmp.path(), false).unwrap();
            assert_eq!(pager.page_count(), 4);

            // p3's data should still be there.
            let p3_data = pager.read_page(3).unwrap();
            assert_eq!(p3_data[100], 0xAB);

            // Allocations should recycle p2 then p1 (LIFO).
            let r1 = pager.allocate_page().unwrap();
            let r2 = pager.allocate_page().unwrap();
            assert_eq!(r1, 2);
            assert_eq!(r2, 1);

            // Next alloc extends file.
            let p4 = pager.allocate_page().unwrap();
            assert_eq!(p4, 4);
            assert_eq!(pager.page_count(), 5);
        }
    }
}
