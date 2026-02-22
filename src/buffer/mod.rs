//! # Buffer Pool
//!
//! The buffer pool is an in-memory page cache that sits between the
//! B+Tree layer and the Pager/WAL layer. It caches frequently accessed
//! pages and uses LRU eviction when the pool is full.
//!
//! All page access in the system goes through the buffer pool.

use crate::error::{HorizonError, Result};
use crate::pager::{PageId, Pager, PAGE_SIZE};
use crate::wal::WalManager;
use std::collections::HashMap;

/// A page frame in the buffer pool.
#[derive(Debug)]
struct Frame {
    /// The page data.
    data: Box<[u8; PAGE_SIZE]>,
    /// The page ID this frame holds.
    page_id: PageId,
    /// Whether this page has been modified.
    dirty: bool,
    /// Reference count (number of active pins).
    pin_count: u32,
    /// LRU timestamp (higher = more recently used).
    last_accessed: u64,
}

/// A handle to a pinned page in the buffer pool.
/// The page remains pinned (cannot be evicted) while this handle exists.
pub struct PageHandle<'a> {
    pool: &'a BufferPool,
    frame_index: usize,
    page_id: PageId,
}

impl<'a> PageHandle<'a> {
    /// Get a read-only reference to the page data.
    pub fn data(&self) -> &[u8; PAGE_SIZE] {
        &self.pool.frames[self.frame_index].data
    }

    /// Get the page ID.
    pub fn page_id(&self) -> PageId {
        self.page_id
    }
}

impl<'a> Drop for PageHandle<'a> {
    fn drop(&mut self) {
        // Note: Cannot mutate pool through shared reference in Drop.
        // Unpin must be called explicitly. This is a design trade-off.
        // In practice, we use internal mutability patterns.
    }
}

/// A mutable handle to a pinned page in the buffer pool.
pub struct PageHandleMut<'a> {
    pool: &'a mut BufferPool,
    frame_index: usize,
    page_id: PageId,
}

impl<'a> PageHandleMut<'a> {
    /// Get a read-only reference to the page data.
    pub fn data(&self) -> &[u8; PAGE_SIZE] {
        &self.pool.frames[self.frame_index].data
    }

    /// Get a mutable reference to the page data.
    /// Automatically marks the page as dirty.
    pub fn data_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        self.pool.frames[self.frame_index].dirty = true;
        &mut self.pool.frames[self.frame_index].data
    }

    /// Get the page ID.
    pub fn page_id(&self) -> PageId {
        self.page_id
    }
}

/// The buffer pool manages an in-memory cache of database pages.
pub struct BufferPool {
    /// The underlying pager for disk I/O.
    pager: Pager,
    /// Optional WAL manager for durability.
    wal: Option<WalManager>,
    /// The page frames.
    frames: Vec<Frame>,
    /// Maximum number of frames.
    capacity: usize,
    /// Mapping from page_id to frame index.
    page_table: HashMap<PageId, usize>,
    /// Monotonic counter for LRU ordering.
    access_counter: u64,
}

impl BufferPool {
    /// Create a new buffer pool with the given capacity (number of pages).
    pub fn new(pager: Pager, wal: Option<WalManager>, capacity: usize) -> Result<Self> {
        Ok(BufferPool {
            pager,
            wal,
            frames: Vec::with_capacity(capacity),
            capacity,
            page_table: HashMap::with_capacity(capacity),
            access_counter: 0,
        })
    }

    /// Fetch a page into the buffer pool, returning its frame index.
    /// If the page is already cached, returns the existing frame.
    fn fetch_page(&mut self, page_id: PageId) -> Result<usize> {
        // Check if already in buffer pool
        if let Some(&frame_idx) = self.page_table.get(&page_id) {
            self.access_counter += 1;
            self.frames[frame_idx].last_accessed = self.access_counter;
            self.frames[frame_idx].pin_count += 1;
            return Ok(frame_idx);
        }

        // Need to load from disk. First try WAL, then pager.
        let data = if let Some(ref wal) = self.wal {
            if let Some(wal_data) = wal.read_page(page_id)? {
                wal_data
            } else {
                self.pager.read_page(page_id)?
            }
        } else {
            self.pager.read_page(page_id)?
        };

        // Find a frame to use
        let frame_idx = if self.frames.len() < self.capacity {
            // Pool not full, allocate a new frame
            let idx = self.frames.len();
            self.frames.push(Frame {
                data: Box::new(data),
                page_id,
                dirty: false,
                pin_count: 1,
                last_accessed: 0,
            });
            idx
        } else {
            // Evict LRU unpinned frame
            self.evict_one()?
        };

        // Update frame
        if frame_idx < self.frames.len() {
            // Remove old mapping if we're reusing a frame
            let old_page_id = self.frames[frame_idx].page_id;
            if self.page_table.get(&old_page_id) == Some(&frame_idx) {
                self.page_table.remove(&old_page_id);
            }

            self.frames[frame_idx].data = Box::new(data);
            self.frames[frame_idx].page_id = page_id;
            self.frames[frame_idx].dirty = false;
            self.frames[frame_idx].pin_count = 1;
        }

        self.access_counter += 1;
        self.frames[frame_idx].last_accessed = self.access_counter;
        self.page_table.insert(page_id, frame_idx);

        Ok(frame_idx)
    }

    /// Get a page for reading.
    pub fn get_page(&mut self, page_id: PageId) -> Result<&[u8; PAGE_SIZE]> {
        let frame_idx = self.fetch_page(page_id)?;
        Ok(&self.frames[frame_idx].data)
    }

    /// Get a mutable reference to a page for writing.
    /// The page is automatically marked as dirty.
    pub fn get_page_mut(&mut self, page_id: PageId) -> Result<&mut [u8; PAGE_SIZE]> {
        let frame_idx = self.fetch_page(page_id)?;
        self.frames[frame_idx].dirty = true;
        Ok(&mut self.frames[frame_idx].data)
    }

    /// Unpin a page, allowing it to be evicted.
    pub fn unpin(&mut self, page_id: PageId) {
        if let Some(&frame_idx) = self.page_table.get(&page_id) {
            if self.frames[frame_idx].pin_count > 0 {
                self.frames[frame_idx].pin_count -= 1;
            }
        }
    }

    /// Allocate a new page through the pager and bring it into the pool.
    pub fn allocate_page(&mut self) -> Result<PageId> {
        let page_id = self.pager.allocate_page()?;

        // Bring the new (zeroed) page into the buffer pool
        let data = [0u8; PAGE_SIZE];

        let frame_idx = if self.frames.len() < self.capacity {
            let idx = self.frames.len();
            self.frames.push(Frame {
                data: Box::new(data),
                page_id,
                dirty: true,
                pin_count: 1,
                last_accessed: 0,
            });
            idx
        } else {
            let idx = self.evict_one()?;
            let old_page_id = self.frames[idx].page_id;
            if self.page_table.get(&old_page_id) == Some(&idx) {
                self.page_table.remove(&old_page_id);
            }
            self.frames[idx] = Frame {
                data: Box::new(data),
                page_id,
                dirty: true,
                pin_count: 1,
                last_accessed: 0,
            };
            idx
        };

        self.access_counter += 1;
        self.frames[frame_idx].last_accessed = self.access_counter;
        self.page_table.insert(page_id, frame_idx);

        Ok(page_id)
    }

    /// Evict one unpinned frame using LRU policy.
    /// Returns the frame index that was evicted.
    fn evict_one(&mut self) -> Result<usize> {
        // Find the LRU unpinned frame
        let victim = self
            .frames
            .iter()
            .enumerate()
            .filter(|(_, f)| f.pin_count == 0)
            .min_by_key(|(_, f)| f.last_accessed)
            .map(|(idx, _)| idx);

        let victim_idx = victim.ok_or(HorizonError::BufferPoolFull)?;

        // If dirty, flush to WAL/disk
        if self.frames[victim_idx].dirty {
            self.flush_frame(victim_idx)?;
        }

        Ok(victim_idx)
    }

    /// Flush a single dirty frame to disk (through WAL if available).
    fn flush_frame(&mut self, frame_idx: usize) -> Result<()> {
        let frame = &self.frames[frame_idx];
        if !frame.dirty {
            return Ok(());
        }

        let page_id = frame.page_id;
        let data = &*frame.data;

        if let Some(ref mut wal) = self.wal {
            // Write through WAL
            let db_size = self.pager.page_count();
            wal.write_frame(page_id, data, 0, false, db_size)?;
        } else {
            // Write directly to pager
            self.pager.write_page(page_id, data)?;
        }

        self.frames[frame_idx].dirty = false;
        Ok(())
    }

    /// Flush all dirty pages to disk.
    pub fn flush_all(&mut self) -> Result<()> {
        for i in 0..self.frames.len() {
            if self.frames[i].dirty {
                self.flush_frame(i)?;
            }
        }

        if let Some(ref mut wal) = self.wal {
            wal.checkpoint(&mut self.pager)?;
        }

        self.pager.sync()?;
        Ok(())
    }

    /// Get a reference to the underlying pager.
    pub fn pager(&self) -> &Pager {
        &self.pager
    }

    /// Get a mutable reference to the underlying pager.
    pub fn pager_mut(&mut self) -> &mut Pager {
        &mut self.pager
    }

    /// Get a reference to the WAL manager.
    pub fn wal(&self) -> Option<&WalManager> {
        self.wal.as_ref()
    }

    /// Get a mutable reference to the WAL manager.
    pub fn wal_mut(&mut self) -> Option<&mut WalManager> {
        self.wal.as_mut()
    }

    /// Get the number of pages currently in the buffer pool.
    pub fn size(&self) -> usize {
        self.frames.len()
    }

    /// Get the capacity of the buffer pool.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Mark a page as dirty (modified).
    pub fn mark_dirty(&mut self, page_id: PageId) {
        if let Some(&frame_idx) = self.page_table.get(&page_id) {
            self.frames[frame_idx].dirty = true;
        }
    }

    /// Free a page (return it to the pager's free list).
    pub fn free_page(&mut self, page_id: PageId) -> Result<()> {
        // Remove from buffer pool if cached
        if let Some(&frame_idx) = self.page_table.get(&page_id) {
            self.frames[frame_idx].dirty = false;
            self.frames[frame_idx].pin_count = 0;
            self.page_table.remove(&page_id);
        }

        self.pager.free_page(page_id)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_pool(capacity: usize) -> (TempDir, BufferPool) {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.hdb");
        let pager = Pager::open(&db_path, false).unwrap();
        let pool = BufferPool::new(pager, None, capacity).unwrap();
        (dir, pool)
    }

    #[test]
    fn test_allocate_and_read() {
        let (_dir, mut pool) = test_pool(10);

        let page_id = pool.allocate_page().unwrap();
        assert!(page_id > 0); // Page 0 is the header

        // Write to the page
        {
            let data = pool.get_page_mut(page_id).unwrap();
            data[0] = 42;
            data[100] = 99;
        }
        pool.unpin(page_id);

        // Read it back
        let data = pool.get_page(page_id).unwrap();
        assert_eq!(data[0], 42);
        assert_eq!(data[100], 99);
    }

    #[test]
    fn test_eviction() {
        let (_dir, mut pool) = test_pool(3);

        // Allocate 3 pages (fills the pool)
        let p1 = pool.allocate_page().unwrap();
        pool.unpin(p1);
        let p2 = pool.allocate_page().unwrap();
        pool.unpin(p2);
        let p3 = pool.allocate_page().unwrap();
        pool.unpin(p3);

        assert_eq!(pool.size(), 3);

        // Write data to p1
        {
            let data = pool.get_page_mut(p1).unwrap();
            data[0] = 11;
        }
        pool.unpin(p1);

        // Allocate one more (should evict one)
        let p4 = pool.allocate_page().unwrap();
        pool.unpin(p4);

        // Pool should still be at capacity
        assert_eq!(pool.size(), 3);
    }

    #[test]
    fn test_flush_all() {
        let (_dir, mut pool) = test_pool(10);

        let page_id = pool.allocate_page().unwrap();
        {
            let data = pool.get_page_mut(page_id).unwrap();
            data[0] = 55;
        }
        pool.unpin(page_id);

        pool.flush_all().unwrap();
    }

    #[test]
    fn test_buffer_pool_full_error() {
        let (_dir, mut pool) = test_pool(2);

        // Allocate 2 pages and keep them pinned
        let _p1 = pool.allocate_page().unwrap();
        let _p2 = pool.allocate_page().unwrap();
        // Don't unpin!

        // Third allocation should fail (all frames pinned)
        let result = pool.allocate_page();
        assert!(result.is_err());
    }
}
