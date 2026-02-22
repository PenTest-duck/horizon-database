//! # B+Tree
//!
//! The B+Tree is the primary on-disk data structure for Horizon DB. Every
//! table is stored as a B+Tree keyed by `rowid`, and every index is a
//! B+Tree keyed by the indexed column values.
//!
//! ## Page Layout
//!
//! Every B+Tree node occupies exactly one database page ([`PAGE_SIZE`] bytes).
//!
//! **Header** (8 bytes):
//!
//! | Offset | Size | Field                                      |
//! |--------|------|--------------------------------------------|
//! | 0      | 1    | `page_type` (`0x01` = internal, `0x02` = leaf) |
//! | 1      | 1    | `flags` (reserved, currently 0)            |
//! | 2..4   | 2    | `cell_count` (u16 big-endian)              |
//! | 4..8   | 4    | `rightmost_child` (internal) or `next_leaf` (leaf), u32 BE |
//!
//! **Cell pointer array** starts at offset 8. Each pointer is a 2-byte
//! big-endian offset from the start of the page to the cell body.
//!
//! **Cell bodies** grow from the *end* of the page toward the pointer array.
//!
//! ### Internal cell format
//!
//! `[child_page: u32 BE][key_size: u16 BE][key_data: ...]`
//!
//! ### Leaf cell format
//!
//! `[key_size: u16 BE][key_data: ...][value_size: u32 BE][value_data: ...]`

use crate::buffer::BufferPool;
use crate::error::Result;
use crate::pager::{PageId, PAGE_SIZE};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Page type marker for internal (non-leaf) nodes.
const PAGE_TYPE_INTERNAL: u8 = 0x01;

/// Page type marker for leaf nodes.
const PAGE_TYPE_LEAF: u8 = 0x02;

/// Size of the page header in bytes.
const HEADER_SIZE: usize = 8;

/// Size of each cell pointer in the pointer array (u16 BE).
const CELL_PTR_SIZE: usize = 2;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A key/value entry stored in the B+Tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BTreeEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// A B+Tree rooted at a specific page.
///
/// The tree stores arbitrary byte-string keys and values. Keys are compared
/// lexicographically. All values live in leaf nodes, and the leaves are
/// chained via `next_leaf` pointers so that range scans can be performed
/// efficiently.
pub struct BTree {
    root_page: PageId,
}

// ---------------------------------------------------------------------------
// Result of a recursive insert that may need to propagate a split upward.
// ---------------------------------------------------------------------------

/// When an insertion causes a node to split, the caller needs the separator
/// key and the page id of the newly created right sibling.
enum InsertResult {
    /// The insertion was absorbed without a split.
    Done,
    /// The node was split. The caller must insert `(split_key, new_page)` into
    /// the parent.
    Split {
        split_key: Vec<u8>,
        new_page: PageId,
    },
}

// ---------------------------------------------------------------------------
// Helper: read / write primitives for page headers and cells
// ---------------------------------------------------------------------------

#[inline]
fn read_u16(buf: &[u8], off: usize) -> u16 {
    u16::from_be_bytes([buf[off], buf[off + 1]])
}

#[inline]
fn read_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_be_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}

#[inline]
fn write_u16(buf: &mut [u8], off: usize, val: u16) {
    let bytes = val.to_be_bytes();
    buf[off] = bytes[0];
    buf[off + 1] = bytes[1];
}

#[inline]
fn write_u32(buf: &mut [u8], off: usize, val: u32) {
    let bytes = val.to_be_bytes();
    buf[off] = bytes[0];
    buf[off + 1] = bytes[1];
    buf[off + 2] = bytes[2];
    buf[off + 3] = bytes[3];
}

// ---------------------------------------------------------------------------
// Page-level accessors (operate on a `[u8; PAGE_SIZE]` buffer)
// ---------------------------------------------------------------------------

fn page_type(page: &[u8]) -> u8 {
    page[0]
}

fn cell_count(page: &[u8]) -> u16 {
    read_u16(page, 2)
}

fn set_cell_count(page: &mut [u8], count: u16) {
    write_u16(page, 2, count);
}

/// For internal pages: rightmost child pointer.
/// For leaf pages: next-leaf page id (0 means none).
fn trailer(page: &[u8]) -> u32 {
    read_u32(page, 4)
}

fn set_trailer(page: &mut [u8], val: u32) {
    write_u32(page, 4, val);
}

/// Offset of the i-th cell pointer in the pointer array.
fn cell_ptr_offset(i: u16) -> usize {
    HEADER_SIZE + (i as usize) * CELL_PTR_SIZE
}

/// Read the i-th cell pointer (the offset within the page where the cell body
/// starts).
fn cell_ptr(page: &[u8], i: u16) -> u16 {
    read_u16(page, cell_ptr_offset(i))
}

/// Write the i-th cell pointer.
fn set_cell_ptr(page: &mut [u8], i: u16, offset: u16) {
    write_u16(page, cell_ptr_offset(i), offset);
}

/// Compute the end of the cell pointer array (= first byte that must not be
/// overwritten by cell data).
fn cell_area_start(count: u16) -> usize {
    HEADER_SIZE + count as usize * CELL_PTR_SIZE
}

/// Find the lowest cell body offset currently stored (the "content area
/// start"). Returns `PAGE_SIZE` if there are no cells.
fn content_start(page: &[u8]) -> usize {
    let n = cell_count(page);
    if n == 0 {
        return PAGE_SIZE;
    }
    let mut min = PAGE_SIZE as u16;
    for i in 0..n {
        let ptr = cell_ptr(page, i);
        if ptr < min {
            min = ptr;
        }
    }
    min as usize
}

// ---------------------------------------------------------------------------
// Internal cell helpers
// ---------------------------------------------------------------------------

/// Parse an internal cell at `off` and return `(child_page, key)`.
fn read_internal_cell(page: &[u8], off: usize) -> (PageId, Vec<u8>) {
    let child = read_u32(page, off);
    let key_size = read_u16(page, off + 4) as usize;
    let key = page[off + 6..off + 6 + key_size].to_vec();
    (child, key)
}

/// Size in bytes of an internal cell with the given key.
fn internal_cell_size(key: &[u8]) -> usize {
    4 /* child */ + 2 /* key_size */ + key.len()
}

// ---------------------------------------------------------------------------
// Leaf cell helpers
// ---------------------------------------------------------------------------

/// Parse a leaf cell at `off` and return `(key, value)`.
fn read_leaf_cell(page: &[u8], off: usize) -> (Vec<u8>, Vec<u8>) {
    let key_size = read_u16(page, off) as usize;
    let key = page[off + 2..off + 2 + key_size].to_vec();
    let val_off = off + 2 + key_size;
    let val_size = read_u32(page, val_off) as usize;
    let value = page[val_off + 4..val_off + 4 + val_size].to_vec();
    (key, value)
}

/// Size in bytes of a leaf cell with the given key and value.
fn leaf_cell_size(key: &[u8], value: &[u8]) -> usize {
    2 /* key_size */ + key.len() + 4 /* value_size */ + value.len()
}

// ---------------------------------------------------------------------------
// Page initialisation helpers
// ---------------------------------------------------------------------------

fn init_leaf_page(page: &mut [u8]) {
    page.fill(0);
    page[0] = PAGE_TYPE_LEAF;
}

fn init_internal_page(page: &mut [u8]) {
    page.fill(0);
    page[0] = PAGE_TYPE_INTERNAL;
}

// ---------------------------------------------------------------------------
// Check whether a new cell fits on the page
// ---------------------------------------------------------------------------

/// Returns `true` if there is enough free space on `page` to insert a cell
/// of `cell_bytes` plus a new cell pointer.
fn has_space(page: &[u8], cell_bytes: usize) -> bool {
    let n = cell_count(page);
    let ptrs_end = cell_area_start(n + 1); // after adding one more pointer
    let content = content_start(page);
    // `content - ptrs_end` is the available gap.
    content >= ptrs_end + cell_bytes
}

// ---------------------------------------------------------------------------
// Write a cell to the page, appending a pointer and copying the body.
// The caller must have already verified there is enough space.
// ---------------------------------------------------------------------------

/// Append a cell body at the end (growing from the bottom) and record the
/// pointer at position `slot`.
fn write_cell(page: &mut [u8], slot: u16, cell_data: &[u8]) {
    let body_offset = content_start(page) - cell_data.len();
    page[body_offset..body_offset + cell_data.len()].copy_from_slice(cell_data);

    // Shift pointers at positions >= slot to make room.
    let n = cell_count(page);
    // Shift from the end to avoid overwriting.
    for i in (slot..n).rev() {
        let ptr = cell_ptr(page, i);
        set_cell_ptr(page, i + 1, ptr);
    }
    set_cell_ptr(page, slot, body_offset as u16);
    set_cell_count(page, n + 1);
}

/// Build the byte representation of an internal cell.
fn build_internal_cell(child: PageId, key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(internal_cell_size(key));
    buf.extend_from_slice(&child.to_be_bytes());
    buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
    buf.extend_from_slice(key);
    buf
}

/// Build the byte representation of a leaf cell.
fn build_leaf_cell(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(leaf_cell_size(key, value));
    buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
    buf.extend_from_slice(value);
    buf
}

// ---------------------------------------------------------------------------
// BTree implementation
// ---------------------------------------------------------------------------

impl BTree {
    /// Create a brand-new, empty B+Tree. Allocates one leaf page as the root.
    pub fn create(pool: &mut BufferPool) -> Result<Self> {
        let page_id = pool.allocate_page()?;
        {
            let data = pool.get_page_mut(page_id)?;
            init_leaf_page(data);
        }
        pool.unpin(page_id);
        Ok(BTree { root_page: page_id })
    }

    /// Open an existing B+Tree whose root page is already known.
    pub fn open(root_page: PageId) -> Self {
        BTree { root_page }
    }

    /// Return the root page id of this tree.
    pub fn root_page(&self) -> PageId {
        self.root_page
    }

    // -----------------------------------------------------------------------
    // Search
    // -----------------------------------------------------------------------

    /// Look up a single key. Returns `Some(value)` if found, `None` otherwise.
    pub fn search(&self, pool: &mut BufferPool, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let leaf_id = self.find_leaf(pool, key)?;
        let page = pool.get_page(leaf_id)?.clone();
        pool.unpin(leaf_id);

        let n = cell_count(&page);
        for i in 0..n {
            let off = cell_ptr(&page, i) as usize;
            let (k, v) = read_leaf_cell(&page, off);
            if k == key {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    /// Walk down the tree to find the leaf page that should contain `key`.
    fn find_leaf(&self, pool: &mut BufferPool, key: &[u8]) -> Result<PageId> {
        let mut current = self.root_page;
        loop {
            let page = pool.get_page(current)?.clone();
            pool.unpin(current);

            if page_type(&page) == PAGE_TYPE_LEAF {
                return Ok(current);
            }

            // Internal page -- find the child to descend into.
            let n = cell_count(&page);
            let mut child = trailer(&page); // default: rightmost child
            for i in 0..n {
                let off = cell_ptr(&page, i) as usize;
                let (c, k) = read_internal_cell(&page, off);
                if key < k.as_slice() {
                    child = c;
                    break;
                }
            }
            current = child;
        }
    }

    // -----------------------------------------------------------------------
    // Insert
    // -----------------------------------------------------------------------

    /// Insert a key/value pair. If the key already exists its value is
    /// replaced (upsert semantics).
    pub fn insert(&mut self, pool: &mut BufferPool, key: &[u8], value: &[u8]) -> Result<()> {
        let result = self.insert_recursive(pool, self.root_page, key, value)?;
        if let InsertResult::Split { split_key, new_page } = result {
            // The root was split. Create a new root.
            let new_root = pool.allocate_page()?;
            {
                let data = pool.get_page_mut(new_root)?;
                init_internal_page(data);
                // The old root becomes the left child of the first cell.
                let cell = build_internal_cell(self.root_page, &split_key);
                write_cell(data, 0, &cell);
                // The new sibling becomes the rightmost child.
                set_trailer(data, new_page);
            }
            pool.unpin(new_root);
            self.root_page = new_root;
        }
        Ok(())
    }

    /// Recursively insert into the subtree rooted at `page_id`.
    fn insert_recursive(
        &mut self,
        pool: &mut BufferPool,
        page_id: PageId,
        key: &[u8],
        value: &[u8],
    ) -> Result<InsertResult> {
        let page = pool.get_page(page_id)?.clone();
        pool.unpin(page_id);

        if page_type(&page) == PAGE_TYPE_LEAF {
            return self.insert_into_leaf(pool, page_id, key, value);
        }

        // Internal node -- find which child to recurse into.
        let n = cell_count(&page);
        let mut child_idx: Option<u16> = None; // index into internal cells
        let mut child_page = trailer(&page);
        for i in 0..n {
            let off = cell_ptr(&page, i) as usize;
            let (c, k) = read_internal_cell(&page, off);
            if key < k.as_slice() {
                child_page = c;
                child_idx = Some(i);
                break;
            }
        }

        let result = self.insert_recursive(pool, child_page, key, value)?;

        match result {
            InsertResult::Done => Ok(InsertResult::Done),
            InsertResult::Split { split_key, new_page } => {
                // Insert (split_key, new_page) into this internal node.
                let insert_slot = child_idx.unwrap_or(n);
                self.insert_into_internal(pool, page_id, insert_slot, &split_key, new_page)
            }
        }
    }

    /// Insert a key/value into a leaf page. Handles the in-place update case
    /// and the split case.
    fn insert_into_leaf(
        &self,
        pool: &mut BufferPool,
        page_id: PageId,
        key: &[u8],
        value: &[u8],
    ) -> Result<InsertResult> {
        // First check for an existing key (upsert). We work on a clone to
        // determine the slot, then apply the mutation.
        let page = pool.get_page(page_id)?.clone();
        pool.unpin(page_id);

        let n = cell_count(&page);
        let mut insert_slot = n; // default: append at end

        // Check if key already exists; also find the correct sorted position.
        let mut existing_idx: Option<u16> = None;
        for i in 0..n {
            let off = cell_ptr(&page, i) as usize;
            let (k, _v) = read_leaf_cell(&page, off);
            match key.cmp(k.as_slice()) {
                std::cmp::Ordering::Equal => {
                    existing_idx = Some(i);
                    break;
                }
                std::cmp::Ordering::Less => {
                    if insert_slot == n {
                        insert_slot = i;
                    }
                }
                std::cmp::Ordering::Greater => {}
            }
        }

        if let Some(idx) = existing_idx {
            // Key exists -- rebuild the page replacing this cell's value.
            self.replace_leaf_cell(pool, page_id, idx, key, value)?;
            return Ok(InsertResult::Done);
        }

        // Key does not exist. Try to insert.
        let cell = build_leaf_cell(key, value);

        // Re-read to check space (the page has not been modified yet).
        if has_space(&page, cell.len()) {
            // Fits -- write directly.
            let data = pool.get_page_mut(page_id)?;
            write_cell(data, insert_slot, &cell);
            pool.unpin(page_id);
            return Ok(InsertResult::Done);
        }

        // Does not fit -- split.
        self.split_leaf(pool, page_id, key, value, insert_slot)
    }

    /// Replace the value of an existing leaf cell at `cell_idx`.
    ///
    /// We rebuild the entire page to avoid fragmentation issues.
    fn replace_leaf_cell(
        &self,
        pool: &mut BufferPool,
        page_id: PageId,
        cell_idx: u16,
        key: &[u8],
        new_value: &[u8],
    ) -> Result<()> {
        let old_page = pool.get_page(page_id)?.clone();
        pool.unpin(page_id);

        let n = cell_count(&old_page);
        let next_leaf = trailer(&old_page);

        // Collect all cells, replacing the one at cell_idx.
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(n as usize);
        for i in 0..n {
            let off = cell_ptr(&old_page, i) as usize;
            let (k, v) = read_leaf_cell(&old_page, off);
            if i == cell_idx {
                entries.push((key.to_vec(), new_value.to_vec()));
            } else {
                entries.push((k, v));
            }
        }

        // Rebuild page.
        let data = pool.get_page_mut(page_id)?;
        init_leaf_page(data);
        set_trailer(data, next_leaf);
        for (k, v) in &entries {
            let cell = build_leaf_cell(k, v);
            let slot = cell_count(data);
            write_cell(data, slot, &cell);
        }
        pool.unpin(page_id);
        Ok(())
    }

    /// Split a full leaf page. The new entry (key, value) at `insert_slot` is
    /// included in the split decision. Returns the split key and the new
    /// right sibling page id.
    fn split_leaf(
        &self,
        pool: &mut BufferPool,
        page_id: PageId,
        key: &[u8],
        value: &[u8],
        insert_slot: u16,
    ) -> Result<InsertResult> {
        let old_page = pool.get_page(page_id)?.clone();
        pool.unpin(page_id);

        let n = cell_count(&old_page);
        let old_next_leaf = trailer(&old_page);

        // Collect all existing entries plus the new one.
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(n as usize + 1);
        let mut inserted = false;
        for i in 0..n {
            if i == insert_slot && !inserted {
                entries.push((key.to_vec(), value.to_vec()));
                inserted = true;
            }
            let off = cell_ptr(&old_page, i) as usize;
            let (k, v) = read_leaf_cell(&old_page, off);
            entries.push((k, v));
        }
        if !inserted {
            entries.push((key.to_vec(), value.to_vec()));
        }

        let total = entries.len();
        let split_point = total / 2;

        // Allocate the new right sibling.
        let new_page_id = pool.allocate_page()?;
        pool.unpin(new_page_id);

        // Rewrite the left (original) page with the first half.
        {
            let data = pool.get_page_mut(page_id)?;
            init_leaf_page(data);
            set_trailer(data, new_page_id); // left -> right sibling
            for (k, v) in &entries[..split_point] {
                let cell = build_leaf_cell(k, v);
                let slot = cell_count(data);
                write_cell(data, slot, &cell);
            }
            pool.unpin(page_id);
        }

        // Write the right sibling with the second half.
        {
            let data = pool.get_page_mut(new_page_id)?;
            init_leaf_page(data);
            set_trailer(data, old_next_leaf); // right -> old next leaf
            for (k, v) in &entries[split_point..] {
                let cell = build_leaf_cell(k, v);
                let slot = cell_count(data);
                write_cell(data, slot, &cell);
            }
            pool.unpin(new_page_id);
        }

        // The split key is the first key of the right sibling.
        let split_key = entries[split_point].0.clone();

        Ok(InsertResult::Split {
            split_key,
            new_page: new_page_id,
        })
    }

    /// Insert a separator key and child pointer into an internal node.
    /// `slot` is the position for the new cell. If the page is full, the
    /// internal node is split.
    fn insert_into_internal(
        &self,
        pool: &mut BufferPool,
        page_id: PageId,
        slot: u16,
        key: &[u8],
        new_child: PageId,
    ) -> Result<InsertResult> {
        let page = pool.get_page(page_id)?.clone();
        pool.unpin(page_id);

        // The new_child is actually the right sibling produced by a split
        // below. The separator cell we insert references the *left* child
        // that was already present. We need to adjust: the cell at `slot`
        // should have `new_child` as the pointer that replaces the
        // rightmost_child (or the next cell's child pointer).
        //
        // Convention: cell[i] = (child_i, key_i). Keys between child_i and
        // child_{i+1} satisfy child_i <= k < child_{i+1}. The rightmost
        // child handles all keys >= last key.
        //
        // When a child at position `slot` splits, the original child stays
        // in its current position and the new right sibling goes *after*
        // the separator key. So we build the cell as
        //   (old_child_at_slot, split_key)
        // and then set the new right sibling as:
        //   - if slot < n: the child pointer of the *next* cell (we insert
        //     before `slot`, shifting the existing cell right, which keeps
        //     the existing child pointer correctly for the range after the
        //     next key).
        //   - if slot == n: the new rightmost child.
        //
        // Actually, the simplest correct approach: in `insert_recursive` we
        // already determined which child we descended into. After the child
        // splits, the left half stays at the old child page and the right
        // half is at `new_child`. We insert a *new* cell whose child
        // pointer is the *old* left half (which is already where the
        // existing pointer points), and the split key separates it from the
        // right half.
        //
        // But we should think of it differently. The internal cell format
        // is: cell[i] = (left_child_i, key_i). The pointer *after* the
        // last cell is `rightmost_child`. So the tree order is:
        //   left_child_0, key_0, left_child_1, key_1, ..., rightmost_child
        //
        // When we descend to `child_page` via cell[slot].child (or
        // rightmost_child if slot==n), and that child splits into
        // (child_page=left, new_child=right) with split_key, we must
        // insert split_key such that left is to its left and right is to
        // its right.
        //
        // If we descended through cell[slot].child (slot < n):
        //   Cell[slot].child already points to left. We insert a new cell
        //   at position (slot+1) with child = right (actually no, the
        //   child pointer in a cell points to the LEFT side).
        //
        // Let me rethink. In our format:
        //   The search scans cells left to right. For cell[i] = (child_i, key_i):
        //     if search_key < key_i, descend to child_i.
        //   If no cell matched, descend to rightmost_child.
        //
        // So the key space partitioning is:
        //   child_0: keys < key_0
        //   child_1: key_0 <= keys < key_1
        //   ...
        //   rightmost_child: keys >= key_{n-1}
        //
        // After descending to child_j (which matched search_key < key_j,
        // or rightmost_child), child_j splits into (left=child_j,
        // right=new_child) with separator split_key. We need:
        //   child_j: keys < split_key  (left half, stays at child_j)
        //   new_child: split_key <= keys  (right half)
        //
        // If j < n (descended via cell[j].child):
        //   cell[j].child still points to left half (good).
        //   Insert new cell at position j+1 ... wait, no.
        //   Actually we need to insert between cell[j-1] and cell[j]?
        //   No. cell[j].child covers keys < key_j. After split we need:
        //     child_j: keys < split_key
        //     new_child: split_key <= keys < key_j
        //   So we insert a new cell BEFORE cell[j] with child=child_j,
        //   key=split_key, and then cell[j] becomes child=new_child, key=key_j.
        //   BUT that changes cell[j]'s child pointer which is complex.
        //
        //   Simpler: insert a new cell at position `slot` with
        //     child = child_j (same as the current cell[slot].child),
        //     key = split_key
        //   Then update cell[slot+1]'s child (which is the old cell[slot])
        //   to point to new_child.
        //   But internal cells have their child pointer embedded...
        //
        // OK, let me use the standard B+tree approach: collect all cells,
        // insert the new separator, rebuild.

        let n = cell_count(&page);
        let rightmost = trailer(&page);

        // Collect all (child, key) plus the rightmost.
        // The pointers partition the key space as described above.
        struct InternalEntry {
            child: PageId,
            key: Vec<u8>,
        }

        let mut cells: Vec<InternalEntry> = Vec::with_capacity(n as usize + 1);
        for i in 0..n {
            let off = cell_ptr(&page, i) as usize;
            let (c, k) = read_internal_cell(&page, off);
            cells.push(InternalEntry { child: c, key: k });
        }

        // Insert the new separator. The `slot` we received from
        // `insert_recursive` is the cell index whose child we descended
        // into (or n if we used rightmost_child).
        //
        // After the child at `slot` splits:
        //   - The original child (left) stays where it was.
        //   - `new_child` (right) handles keys >= split_key.
        //
        // For slot < n: cell[slot].child = left. We insert a new entry
        //   after slot with child = new_child, key = split_key.
        //   Wait, that is wrong too because then new_child would cover
        //   split_key <= keys < key[slot]. But we want:
        //     cell[slot].child covers keys < split_key
        //     new entry covers split_key <= keys < key[slot]
        //   So the new entry should be:
        //     child = new_child, key = key[slot]??
        //   No...
        //
        // Let me think again very carefully.
        //
        // Current state before split of child at slot j:
        //   cell[j] = (child_j, key_j)
        //   child_j covers: if j==0 then (-inf, key_0), else [key_{j-1}, key_j)
        //
        // After child_j splits into left (stays at child_j) and right (new_child):
        //   left covers [key_{j-1}, split_key)  (or (-inf, split_key) if j==0)
        //   right covers [split_key, key_j)
        //
        // We insert a new cell between j-1 and j (i.e., at position j):
        //   new_cell = (child_j, split_key)
        //   old cell[j] becomes (new_child, key_j)
        //
        // This way:
        //   cell[j] = (child_j, split_key) -> child_j covers [key_{j-1}, split_key) CORRECT
        //   cell[j+1] = (new_child, key_j) -> new_child covers [split_key, key_j) CORRECT
        //
        // For slot == n (descended into rightmost_child):
        //   left stays as rightmost_child... well, no. We need:
        //   Insert new cell at position n: (left=old_rightmost, split_key)
        //   Set rightmost_child = new_child
        //
        //   cell[n] = (rightmost, split_key) -> rightmost covers [key_{n-1}, split_key)
        //   new rightmost = new_child -> covers [split_key, +inf)

        if slot < n {
            // Insert new cell at position `slot` with (child=old_child, key=split_key).
            // Change old cell[slot]'s child to new_child.
            let old_child = cells[slot as usize].child;
            cells[slot as usize].child = new_child;
            cells.insert(
                slot as usize,
                InternalEntry {
                    child: old_child,
                    key: key.to_vec(),
                },
            );
        } else {
            // Descended into rightmost child.
            cells.push(InternalEntry {
                child: rightmost,
                key: key.to_vec(),
            });
            // new rightmost will be new_child, set below.
        }
        let new_rightmost = if slot < n { rightmost } else { new_child };

        // Now try to fit all cells on the page.
        let total_cell_bytes: usize = cells.iter().map(|e| internal_cell_size(&e.key)).sum();
        let total_ptrs = cells.len() * CELL_PTR_SIZE;
        let needed = HEADER_SIZE + total_ptrs + total_cell_bytes;

        if needed <= PAGE_SIZE {
            // Fits -- rebuild in place.
            let data = pool.get_page_mut(page_id)?;
            init_internal_page(data);
            set_trailer(data, new_rightmost);
            for entry in &cells {
                let c = build_internal_cell(entry.child, &entry.key);
                let s = cell_count(data);
                write_cell(data, s, &c);
            }
            pool.unpin(page_id);
            return Ok(InsertResult::Done);
        }

        // Internal node must be split.
        let total = cells.len();
        let split_point = total / 2;

        // The key at split_point becomes the separator pushed up.
        // Left gets cells[..split_point], right gets cells[split_point+1..].
        let promoted_key = cells[split_point].key.clone();
        let left_rightmost = cells[split_point].child;

        let new_page_id = pool.allocate_page()?;
        pool.unpin(new_page_id);

        // Rewrite the left (original) page.
        {
            let data = pool.get_page_mut(page_id)?;
            init_internal_page(data);
            set_trailer(data, left_rightmost);
            for entry in &cells[..split_point] {
                let c = build_internal_cell(entry.child, &entry.key);
                let s = cell_count(data);
                write_cell(data, s, &c);
            }
            pool.unpin(page_id);
        }

        // Write the right sibling.
        {
            let data = pool.get_page_mut(new_page_id)?;
            init_internal_page(data);
            set_trailer(data, new_rightmost);
            for entry in &cells[split_point + 1..] {
                let c = build_internal_cell(entry.child, &entry.key);
                let s = cell_count(data);
                write_cell(data, s, &c);
            }
            pool.unpin(new_page_id);
        }

        Ok(InsertResult::Split {
            split_key: promoted_key,
            new_page: new_page_id,
        })
    }

    // -----------------------------------------------------------------------
    // Delete
    // -----------------------------------------------------------------------

    /// Delete a key from the tree. Returns `true` if the key was found and
    /// removed, `false` if it was not present.
    ///
    /// This implementation performs a simple removal from the leaf page
    /// without any rebalancing. The tree remains correct (all searches and
    /// scans still work), but pages may become under-utilised.
    pub fn delete(&mut self, pool: &mut BufferPool, key: &[u8]) -> Result<bool> {
        let leaf_id = self.find_leaf(pool, key)?;
        let page = pool.get_page(leaf_id)?.clone();
        pool.unpin(leaf_id);

        let n = cell_count(&page);
        let mut found_idx: Option<u16> = None;
        for i in 0..n {
            let off = cell_ptr(&page, i) as usize;
            let (k, _) = read_leaf_cell(&page, off);
            if k == key {
                found_idx = Some(i);
                break;
            }
        }

        let idx = match found_idx {
            Some(i) => i,
            None => return Ok(false),
        };

        // Rebuild the page without the deleted cell.
        let next_leaf = trailer(&page);
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(n as usize - 1);
        for i in 0..n {
            if i == idx {
                continue;
            }
            let off = cell_ptr(&page, i) as usize;
            let (k, v) = read_leaf_cell(&page, off);
            entries.push((k, v));
        }

        let data = pool.get_page_mut(leaf_id)?;
        init_leaf_page(data);
        set_trailer(data, next_leaf);
        for (k, v) in &entries {
            let cell = build_leaf_cell(k, v);
            let slot = cell_count(data);
            write_cell(data, slot, &cell);
        }
        pool.unpin(leaf_id);
        Ok(true)
    }

    // -----------------------------------------------------------------------
    // Scan operations
    // -----------------------------------------------------------------------

    /// Return all entries in the tree, in key order.
    pub fn scan_all(&self, pool: &mut BufferPool) -> Result<Vec<BTreeEntry>> {
        // Find the leftmost leaf.
        let leftmost = self.find_leftmost_leaf(pool)?;
        self.scan_leaves_from_page(pool, leftmost, None, None)
    }

    /// Return all entries whose key is >= `start_key`, in key order.
    pub fn scan_from(&self, pool: &mut BufferPool, start_key: &[u8]) -> Result<Vec<BTreeEntry>> {
        let leaf_id = self.find_leaf(pool, start_key)?;
        self.scan_leaves_from_page(pool, leaf_id, Some(start_key), None)
    }

    /// Return all entries whose key is in `[start_key, end_key)` (start
    /// inclusive, end exclusive), in key order.
    pub fn scan_range(
        &self,
        pool: &mut BufferPool,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<BTreeEntry>> {
        let leaf_id = self.find_leaf(pool, start_key)?;
        self.scan_leaves_from_page(pool, leaf_id, Some(start_key), Some(end_key))
    }

    /// Count the total number of entries in the tree.
    pub fn count(&self, pool: &mut BufferPool) -> Result<u64> {
        let leftmost = self.find_leftmost_leaf(pool)?;
        let mut total: u64 = 0;
        let mut current = leftmost;
        loop {
            let page = pool.get_page(current)?.clone();
            pool.unpin(current);
            total += cell_count(&page) as u64;
            let next = trailer(&page);
            if next == 0 {
                break;
            }
            current = next;
        }
        Ok(total)
    }

    // -----------------------------------------------------------------------
    // Internal scan helpers
    // -----------------------------------------------------------------------

    /// Find the leftmost leaf by always descending to the first child.
    fn find_leftmost_leaf(&self, pool: &mut BufferPool) -> Result<PageId> {
        let mut current = self.root_page;
        loop {
            let page = pool.get_page(current)?.clone();
            pool.unpin(current);

            if page_type(&page) == PAGE_TYPE_LEAF {
                return Ok(current);
            }

            let n = cell_count(&page);
            if n == 0 {
                // An internal node with zero cells -- the only child is
                // the rightmost pointer.
                current = trailer(&page);
            } else {
                let off = cell_ptr(&page, 0) as usize;
                let (child, _) = read_internal_cell(&page, off);
                current = child;
            }
        }
    }

    /// Walk the leaf chain starting at `start_page`, collecting entries.
    /// If `start_key` is given, skip entries with keys < start_key.
    /// If `end_key` is given, stop at the first entry with key >= end_key.
    fn scan_leaves_from_page(
        &self,
        pool: &mut BufferPool,
        start_page: PageId,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
    ) -> Result<Vec<BTreeEntry>> {
        let mut result = Vec::new();
        let mut current = start_page;

        loop {
            let page = pool.get_page(current)?.clone();
            pool.unpin(current);

            let n = cell_count(&page);
            for i in 0..n {
                let off = cell_ptr(&page, i) as usize;
                let (k, v) = read_leaf_cell(&page, off);
                if let Some(sk) = start_key {
                    if k.as_slice() < sk {
                        continue;
                    }
                }
                if let Some(ek) = end_key {
                    if k.as_slice() >= ek {
                        return Ok(result);
                    }
                }
                result.push(BTreeEntry { key: k, value: v });
            }

            let next = trailer(&page);
            if next == 0 {
                break;
            }
            current = next;
        }

        Ok(result)
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pager::Pager;
    use tempfile::TempDir;

    /// Create a fresh buffer pool backed by a temporary directory.
    fn test_pool() -> (TempDir, BufferPool) {
        let dir = TempDir::new().unwrap();
        let pager = Pager::open(&dir.path().join("test.hdb"), false).unwrap();
        let pool = BufferPool::new(pager, None, 100).unwrap();
        (dir, pool)
    }

    // -----------------------------------------------------------------------
    // Basic creation & search on empty tree
    // -----------------------------------------------------------------------

    #[test]
    fn create_empty_tree() {
        let (_dir, mut pool) = test_pool();
        let tree = BTree::create(&mut pool).unwrap();
        assert!(tree.root_page() > 0);
    }

    #[test]
    fn search_empty_tree_returns_none() {
        let (_dir, mut pool) = test_pool();
        let tree = BTree::create(&mut pool).unwrap();
        let result = tree.search(&mut pool, b"hello").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn scan_empty_tree_returns_empty() {
        let (_dir, mut pool) = test_pool();
        let tree = BTree::create(&mut pool).unwrap();
        let entries = tree.scan_all(&mut pool).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn count_empty_tree_returns_zero() {
        let (_dir, mut pool) = test_pool();
        let tree = BTree::create(&mut pool).unwrap();
        assert_eq!(tree.count(&mut pool).unwrap(), 0);
    }

    // -----------------------------------------------------------------------
    // Single key insert & search
    // -----------------------------------------------------------------------

    #[test]
    fn insert_and_search_single_key() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();
        tree.insert(&mut pool, b"key1", b"value1").unwrap();

        let val = tree.search(&mut pool, b"key1").unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[test]
    fn search_missing_key_returns_none() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();
        tree.insert(&mut pool, b"key1", b"value1").unwrap();

        let val = tree.search(&mut pool, b"key2").unwrap();
        assert!(val.is_none());
    }

    // -----------------------------------------------------------------------
    // Multiple keys
    // -----------------------------------------------------------------------

    #[test]
    fn insert_and_search_multiple_keys() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();

        tree.insert(&mut pool, b"banana", b"yellow").unwrap();
        tree.insert(&mut pool, b"apple", b"red").unwrap();
        tree.insert(&mut pool, b"cherry", b"dark red").unwrap();

        assert_eq!(
            tree.search(&mut pool, b"apple").unwrap(),
            Some(b"red".to_vec())
        );
        assert_eq!(
            tree.search(&mut pool, b"banana").unwrap(),
            Some(b"yellow".to_vec())
        );
        assert_eq!(
            tree.search(&mut pool, b"cherry").unwrap(),
            Some(b"dark red".to_vec())
        );
        assert!(tree.search(&mut pool, b"date").unwrap().is_none());
    }

    // -----------------------------------------------------------------------
    // Upsert (update existing key)
    // -----------------------------------------------------------------------

    #[test]
    fn update_existing_key() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();

        tree.insert(&mut pool, b"key1", b"v1").unwrap();
        assert_eq!(
            tree.search(&mut pool, b"key1").unwrap(),
            Some(b"v1".to_vec())
        );

        tree.insert(&mut pool, b"key1", b"v2").unwrap();
        assert_eq!(
            tree.search(&mut pool, b"key1").unwrap(),
            Some(b"v2".to_vec())
        );

        // Only one entry should exist.
        assert_eq!(tree.count(&mut pool).unwrap(), 1);
    }

    #[test]
    fn update_multiple_existing_keys() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();

        for i in 0u32..10 {
            let key = format!("key{:04}", i);
            let val = format!("val_v1_{}", i);
            tree.insert(&mut pool, key.as_bytes(), val.as_bytes())
                .unwrap();
        }
        assert_eq!(tree.count(&mut pool).unwrap(), 10);

        // Update all values.
        for i in 0u32..10 {
            let key = format!("key{:04}", i);
            let val = format!("val_v2_{}", i);
            tree.insert(&mut pool, key.as_bytes(), val.as_bytes())
                .unwrap();
        }
        assert_eq!(tree.count(&mut pool).unwrap(), 10);

        // Verify updated values.
        for i in 0u32..10 {
            let key = format!("key{:04}", i);
            let expected = format!("val_v2_{}", i);
            let found = tree.search(&mut pool, key.as_bytes()).unwrap().unwrap();
            assert_eq!(found, expected.as_bytes());
        }
    }

    // -----------------------------------------------------------------------
    // Many inserts causing splits
    // -----------------------------------------------------------------------

    #[test]
    fn insert_100_keys_all_searchable() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();

        for i in 0u32..100 {
            let key = format!("key{:04}", i);
            let val = format!("value{:04}", i);
            tree.insert(&mut pool, key.as_bytes(), val.as_bytes())
                .unwrap();
        }

        // All 100 should be searchable.
        for i in 0u32..100 {
            let key = format!("key{:04}", i);
            let expected = format!("value{:04}", i);
            let found = tree.search(&mut pool, key.as_bytes()).unwrap();
            assert_eq!(
                found,
                Some(expected.as_bytes().to_vec()),
                "failed to find key {}",
                key
            );
        }

        assert_eq!(tree.count(&mut pool).unwrap(), 100);
    }

    #[test]
    fn insert_200_keys_reverse_order() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();

        // Insert in reverse order to exercise different split paths.
        for i in (0u32..200).rev() {
            let key = format!("k{:05}", i);
            let val = format!("v{:05}", i);
            tree.insert(&mut pool, key.as_bytes(), val.as_bytes())
                .unwrap();
        }

        for i in 0u32..200 {
            let key = format!("k{:05}", i);
            let expected = format!("v{:05}", i);
            let found = tree.search(&mut pool, key.as_bytes()).unwrap();
            assert_eq!(
                found,
                Some(expected.as_bytes().to_vec()),
                "missing key {}",
                key
            );
        }

        assert_eq!(tree.count(&mut pool).unwrap(), 200);
    }

    #[test]
    fn insert_500_keys_random_order() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();

        // Use a simple deterministic pseudo-random order.
        let mut order: Vec<u32> = (0..500).collect();
        // Simple shuffle using a linear congruential generator.
        let mut rng: u64 = 42;
        for i in (1..order.len()).rev() {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let j = (rng >> 33) as usize % (i + 1);
            order.swap(i, j);
        }

        for &i in &order {
            let key = format!("key{:06}", i);
            let val = format!("val{:06}", i);
            tree.insert(&mut pool, key.as_bytes(), val.as_bytes())
                .unwrap();
        }

        // Verify all are searchable.
        for i in 0u32..500 {
            let key = format!("key{:06}", i);
            let expected = format!("val{:06}", i);
            let found = tree.search(&mut pool, key.as_bytes()).unwrap();
            assert_eq!(found, Some(expected.as_bytes().to_vec()), "missing {}", key);
        }

        assert_eq!(tree.count(&mut pool).unwrap(), 500);
    }

    // -----------------------------------------------------------------------
    // Scan all in sorted order
    // -----------------------------------------------------------------------

    #[test]
    fn scan_all_sorted_order() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();

        // Insert in arbitrary order.
        let keys = ["cherry", "apple", "banana", "date", "elderberry"];
        for &k in &keys {
            tree.insert(&mut pool, k.as_bytes(), k.as_bytes()).unwrap();
        }

        let entries = tree.scan_all(&mut pool).unwrap();
        let result_keys: Vec<&[u8]> = entries.iter().map(|e| e.key.as_slice()).collect();
        assert_eq!(
            result_keys,
            vec![
                b"apple".as_slice(),
                b"banana",
                b"cherry",
                b"date",
                b"elderberry",
            ]
        );
    }

    #[test]
    fn scan_all_after_many_inserts() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();

        for i in (0u32..150).rev() {
            let key = format!("k{:04}", i);
            let val = format!("v{:04}", i);
            tree.insert(&mut pool, key.as_bytes(), val.as_bytes())
                .unwrap();
        }

        let entries = tree.scan_all(&mut pool).unwrap();
        assert_eq!(entries.len(), 150);

        // Verify sorted order.
        for i in 1..entries.len() {
            assert!(
                entries[i - 1].key <= entries[i].key,
                "entries not sorted at index {}",
                i
            );
        }

        // Verify first and last.
        assert_eq!(entries[0].key, b"k0000");
        assert_eq!(entries[149].key, b"k0149");
    }

    // -----------------------------------------------------------------------
    // Scan from (range start)
    // -----------------------------------------------------------------------

    #[test]
    fn scan_from_key() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();

        for i in 0u32..50 {
            let key = format!("k{:04}", i);
            tree.insert(&mut pool, key.as_bytes(), b"v").unwrap();
        }

        let entries = tree.scan_from(&mut pool, b"k0025").unwrap();
        assert_eq!(entries.len(), 25);
        assert_eq!(entries[0].key, b"k0025");
        assert_eq!(entries[24].key, b"k0049");
    }

    #[test]
    fn scan_from_nonexistent_key() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();

        tree.insert(&mut pool, b"b", b"2").unwrap();
        tree.insert(&mut pool, b"d", b"4").unwrap();
        tree.insert(&mut pool, b"f", b"6").unwrap();

        // Start from "c" which does not exist; should return "d" and "f".
        let entries = tree.scan_from(&mut pool, b"c").unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key, b"d");
        assert_eq!(entries[1].key, b"f");
    }

    // -----------------------------------------------------------------------
    // Scan range
    // -----------------------------------------------------------------------

    #[test]
    fn scan_range_basic() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();

        for i in 0u32..100 {
            let key = format!("k{:04}", i);
            let val = format!("v{:04}", i);
            tree.insert(&mut pool, key.as_bytes(), val.as_bytes())
                .unwrap();
        }

        // Range [k0010, k0020) should return 10 entries.
        let entries = tree
            .scan_range(&mut pool, b"k0010", b"k0020")
            .unwrap();
        assert_eq!(entries.len(), 10);
        assert_eq!(entries[0].key, b"k0010");
        assert_eq!(entries[9].key, b"k0019");
    }

    #[test]
    fn scan_range_empty_result() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();

        tree.insert(&mut pool, b"a", b"1").unwrap();
        tree.insert(&mut pool, b"z", b"2").unwrap();

        let entries = tree.scan_range(&mut pool, b"m", b"n").unwrap();
        assert!(entries.is_empty());
    }

    // -----------------------------------------------------------------------
    // Delete
    // -----------------------------------------------------------------------

    #[test]
    fn delete_single_key() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();

        tree.insert(&mut pool, b"key1", b"val1").unwrap();
        assert!(tree.search(&mut pool, b"key1").unwrap().is_some());

        let removed = tree.delete(&mut pool, b"key1").unwrap();
        assert!(removed);
        assert!(tree.search(&mut pool, b"key1").unwrap().is_none());
        assert_eq!(tree.count(&mut pool).unwrap(), 0);
    }

    #[test]
    fn delete_nonexistent_key_returns_false() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();

        tree.insert(&mut pool, b"key1", b"val1").unwrap();
        let removed = tree.delete(&mut pool, b"key_missing").unwrap();
        assert!(!removed);
        assert_eq!(tree.count(&mut pool).unwrap(), 1);
    }

    #[test]
    fn delete_from_empty_tree() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();
        let removed = tree.delete(&mut pool, b"key1").unwrap();
        assert!(!removed);
    }

    #[test]
    fn delete_multiple_keys() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();

        for i in 0u32..20 {
            let key = format!("k{:04}", i);
            tree.insert(&mut pool, key.as_bytes(), b"v").unwrap();
        }
        assert_eq!(tree.count(&mut pool).unwrap(), 20);

        // Delete every other key.
        for i in (0u32..20).step_by(2) {
            let key = format!("k{:04}", i);
            let removed = tree.delete(&mut pool, key.as_bytes()).unwrap();
            assert!(removed, "should have deleted {}", key);
        }

        assert_eq!(tree.count(&mut pool).unwrap(), 10);

        // Verify remaining keys.
        for i in 0u32..20 {
            let key = format!("k{:04}", i);
            let found = tree.search(&mut pool, key.as_bytes()).unwrap();
            if i % 2 == 0 {
                assert!(found.is_none(), "{} should be deleted", key);
            } else {
                assert!(found.is_some(), "{} should still exist", key);
            }
        }
    }

    #[test]
    fn delete_then_reinsert() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();

        tree.insert(&mut pool, b"key1", b"v1").unwrap();
        tree.delete(&mut pool, b"key1").unwrap();
        assert!(tree.search(&mut pool, b"key1").unwrap().is_none());

        tree.insert(&mut pool, b"key1", b"v2").unwrap();
        assert_eq!(
            tree.search(&mut pool, b"key1").unwrap(),
            Some(b"v2".to_vec())
        );
    }

    // -----------------------------------------------------------------------
    // Delete with splits (larger tree)
    // -----------------------------------------------------------------------

    #[test]
    fn delete_from_large_tree() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();

        for i in 0u32..200 {
            let key = format!("key{:05}", i);
            let val = format!("val{:05}", i);
            tree.insert(&mut pool, key.as_bytes(), val.as_bytes())
                .unwrap();
        }

        // Delete the first 100.
        for i in 0u32..100 {
            let key = format!("key{:05}", i);
            let removed = tree.delete(&mut pool, key.as_bytes()).unwrap();
            assert!(removed);
        }

        assert_eq!(tree.count(&mut pool).unwrap(), 100);

        // The remaining 100 should still be searchable.
        for i in 100u32..200 {
            let key = format!("key{:05}", i);
            let expected = format!("val{:05}", i);
            let found = tree.search(&mut pool, key.as_bytes()).unwrap();
            assert_eq!(found, Some(expected.as_bytes().to_vec()));
        }
    }

    // -----------------------------------------------------------------------
    // Count
    // -----------------------------------------------------------------------

    #[test]
    fn count_tracks_inserts_and_deletes() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();

        assert_eq!(tree.count(&mut pool).unwrap(), 0);

        tree.insert(&mut pool, b"a", b"1").unwrap();
        assert_eq!(tree.count(&mut pool).unwrap(), 1);

        tree.insert(&mut pool, b"b", b"2").unwrap();
        tree.insert(&mut pool, b"c", b"3").unwrap();
        assert_eq!(tree.count(&mut pool).unwrap(), 3);

        tree.delete(&mut pool, b"b").unwrap();
        assert_eq!(tree.count(&mut pool).unwrap(), 2);
    }

    // -----------------------------------------------------------------------
    // Open existing tree
    // -----------------------------------------------------------------------

    #[test]
    fn open_existing_tree() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();
        let root = tree.root_page();

        tree.insert(&mut pool, b"hello", b"world").unwrap();

        // "Re-open" the tree by its root page.
        let tree2 = BTree::open(root);
        assert_eq!(tree2.root_page(), root);
        let val = tree2.search(&mut pool, b"hello").unwrap();
        assert_eq!(val, Some(b"world".to_vec()));
    }

    // -----------------------------------------------------------------------
    // Stress: large values
    // -----------------------------------------------------------------------

    #[test]
    fn insert_large_values() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();

        // Insert entries with reasonably large values that will force many
        // splits.
        for i in 0u32..50 {
            let key = format!("k{:04}", i);
            let val = vec![b'x'; 200]; // 200-byte values
            tree.insert(&mut pool, key.as_bytes(), &val).unwrap();
        }

        for i in 0u32..50 {
            let key = format!("k{:04}", i);
            let val = tree.search(&mut pool, key.as_bytes()).unwrap().unwrap();
            assert_eq!(val.len(), 200);
            assert!(val.iter().all(|&b| b == b'x'));
        }

        assert_eq!(tree.count(&mut pool).unwrap(), 50);
    }

    // -----------------------------------------------------------------------
    // Edge case: duplicate insert same key many times
    // -----------------------------------------------------------------------

    #[test]
    fn repeated_upsert_same_key() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();

        for i in 0u32..100 {
            let val = format!("version_{}", i);
            tree.insert(&mut pool, b"only_key", val.as_bytes())
                .unwrap();
        }

        assert_eq!(tree.count(&mut pool).unwrap(), 1);
        let val = tree.search(&mut pool, b"only_key").unwrap().unwrap();
        assert_eq!(val, b"version_99");
    }

    // -----------------------------------------------------------------------
    // Scan all correctness after splits
    // -----------------------------------------------------------------------

    #[test]
    fn scan_all_after_splits_is_complete_and_sorted() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();

        let n = 300u32;
        for i in (0..n).rev() {
            let key = format!("{:06}", i);
            let val = format!("val_{}", i);
            tree.insert(&mut pool, key.as_bytes(), val.as_bytes())
                .unwrap();
        }

        let entries = tree.scan_all(&mut pool).unwrap();
        assert_eq!(entries.len(), n as usize);

        // Verify sorted.
        for i in 1..entries.len() {
            assert!(entries[i - 1].key < entries[i].key);
        }

        // Verify completeness.
        for i in 0..n {
            let expected_key = format!("{:06}", i);
            assert_eq!(entries[i as usize].key, expected_key.as_bytes());
        }
    }

    // -----------------------------------------------------------------------
    // Integration: mixed inserts, deletes, updates, scans
    // -----------------------------------------------------------------------

    #[test]
    fn mixed_operations() {
        let (_dir, mut pool) = test_pool();
        let mut tree = BTree::create(&mut pool).unwrap();

        // Insert 50 keys.
        for i in 0u32..50 {
            let key = format!("item{:04}", i);
            let val = format!("original_{}", i);
            tree.insert(&mut pool, key.as_bytes(), val.as_bytes())
                .unwrap();
        }

        // Delete keys 10..20.
        for i in 10u32..20 {
            let key = format!("item{:04}", i);
            tree.delete(&mut pool, key.as_bytes()).unwrap();
        }
        assert_eq!(tree.count(&mut pool).unwrap(), 40);

        // Update keys 30..40.
        for i in 30u32..40 {
            let key = format!("item{:04}", i);
            let val = format!("updated_{}", i);
            tree.insert(&mut pool, key.as_bytes(), val.as_bytes())
                .unwrap();
        }
        assert_eq!(tree.count(&mut pool).unwrap(), 40);

        // Scan all -- should have 40 entries, sorted.
        let entries = tree.scan_all(&mut pool).unwrap();
        assert_eq!(entries.len(), 40);
        for i in 1..entries.len() {
            assert!(entries[i - 1].key < entries[i].key);
        }

        // Verify updated values.
        for i in 30u32..40 {
            let key = format!("item{:04}", i);
            let expected = format!("updated_{}", i);
            let found = tree.search(&mut pool, key.as_bytes()).unwrap().unwrap();
            assert_eq!(found, expected.as_bytes());
        }

        // Verify deleted keys are gone.
        for i in 10u32..20 {
            let key = format!("item{:04}", i);
            assert!(tree.search(&mut pool, key.as_bytes()).unwrap().is_none());
        }
    }
}
