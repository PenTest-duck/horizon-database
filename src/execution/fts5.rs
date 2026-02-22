//! FTS5 (Full-Text Search) module for Horizon DB.
//!
//! Provides a simplified but functional FTS5 implementation:
//! - Inverted index mapping terms to document positions
//! - Simple whitespace + punctuation tokenizer
//! - AND semantics for multi-term queries
//! - BM25 relevance scoring
//! - highlight() and snippet() auxiliary functions

use std::collections::HashMap;
use crate::error::{HorizonError, Result};
use crate::types::Value;

/// Metadata for an FTS5 virtual table.
#[derive(Debug, Clone)]
pub struct Fts5TableInfo {
    /// Table name.
    pub name: String,
    /// Column names in the FTS table.
    pub columns: Vec<String>,
    /// Next document rowid.
    pub next_rowid: i64,
}

/// A posting: identifies where a term occurs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Posting {
    pub rowid: i64,
    pub col_idx: usize,
    pub position: usize,
}

/// In-memory storage for an FTS5 virtual table.
/// Documents are stored in a HashMap (rowid -> column values).
/// An inverted index maps each term to its postings list.
pub struct Fts5Index {
    /// Table metadata.
    pub info: Fts5TableInfo,
    /// Document storage: rowid -> list of column text values.
    pub documents: HashMap<i64, Vec<String>>,
    /// Inverted index: term -> list of postings.
    pub inverted: HashMap<String, Vec<Posting>>,
    /// Per-document token counts: rowid -> total number of tokens.
    pub doc_lengths: HashMap<i64, usize>,
    /// Total number of tokens across all documents.
    pub total_tokens: usize,
}

impl Fts5Index {
    /// Create a new, empty FTS5 index.
    pub fn new(info: Fts5TableInfo) -> Self {
        Fts5Index {
            info,
            documents: HashMap::new(),
            inverted: HashMap::new(),
            doc_lengths: HashMap::new(),
            total_tokens: 0,
        }
    }

    /// Insert a document into the index.
    /// `col_values` should contain the text for each column in the FTS table.
    /// Returns the assigned rowid.
    pub fn insert(&mut self, col_values: Vec<String>) -> i64 {
        let rowid = self.info.next_rowid;
        self.info.next_rowid += 1;

        let mut total_doc_tokens = 0;

        for (col_idx, text) in col_values.iter().enumerate() {
            let tokens = tokenize(text);
            for (position, token) in tokens.iter().enumerate() {
                let posting = Posting {
                    rowid,
                    col_idx,
                    position,
                };
                self.inverted
                    .entry(token.clone())
                    .or_default()
                    .push(posting);
            }
            total_doc_tokens += tokens.len();
        }

        self.doc_lengths.insert(rowid, total_doc_tokens);
        self.total_tokens += total_doc_tokens;
        self.documents.insert(rowid, col_values);

        rowid
    }

    /// Insert a document with a specific rowid.
    pub fn insert_with_rowid(&mut self, rowid: i64, col_values: Vec<String>) {
        if rowid >= self.info.next_rowid {
            self.info.next_rowid = rowid + 1;
        }

        let mut total_doc_tokens = 0;

        for (col_idx, text) in col_values.iter().enumerate() {
            let tokens = tokenize(text);
            for (position, token) in tokens.iter().enumerate() {
                let posting = Posting {
                    rowid,
                    col_idx,
                    position,
                };
                self.inverted
                    .entry(token.clone())
                    .or_default()
                    .push(posting);
            }
            total_doc_tokens += tokens.len();
        }

        self.doc_lengths.insert(rowid, total_doc_tokens);
        self.total_tokens += total_doc_tokens;
        self.documents.insert(rowid, col_values);
    }

    /// Delete a document from the index by rowid.
    pub fn delete(&mut self, rowid: i64) -> bool {
        if let Some(col_values) = self.documents.remove(&rowid) {
            // Remove all postings for this document
            for (_term, postings) in self.inverted.iter_mut() {
                postings.retain(|p| p.rowid != rowid);
            }
            // Clean up empty term entries
            self.inverted.retain(|_, postings| !postings.is_empty());

            // Update token counts
            if let Some(doc_len) = self.doc_lengths.remove(&rowid) {
                self.total_tokens = self.total_tokens.saturating_sub(doc_len);
            }

            let _ = col_values; // suppress unused warning
            true
        } else {
            false
        }
    }

    /// Search for documents matching all terms (AND semantics).
    /// Returns a list of matching rowids.
    pub fn search(&self, query: &str) -> Vec<i64> {
        let terms = tokenize(query);
        if terms.is_empty() {
            return vec![];
        }

        // For each term, get the set of matching rowids.
        let mut term_rowid_sets: Vec<Vec<i64>> = Vec::new();
        for term in &terms {
            if let Some(postings) = self.inverted.get(term) {
                let mut rowids: Vec<i64> = postings.iter().map(|p| p.rowid).collect();
                rowids.sort();
                rowids.dedup();
                term_rowid_sets.push(rowids);
            } else {
                // Term not found -> no results (AND semantics)
                return vec![];
            }
        }

        // Intersect all sets
        let mut result = term_rowid_sets[0].clone();
        for set in &term_rowid_sets[1..] {
            result.retain(|rid| set.contains(rid));
        }

        result
    }

    /// Compute BM25 score for a document given a query.
    /// Uses standard BM25 parameters: k1=1.2, b=0.75.
    pub fn bm25(&self, rowid: i64, query: &str) -> f64 {
        let terms = tokenize(query);
        if terms.is_empty() || self.documents.is_empty() {
            return 0.0;
        }

        let k1: f64 = 1.2;
        let b: f64 = 0.75;
        let n = self.documents.len() as f64; // total number of documents
        let avgdl = if self.documents.is_empty() {
            1.0
        } else {
            self.total_tokens as f64 / n
        };
        let dl = *self.doc_lengths.get(&rowid).unwrap_or(&0) as f64;

        let mut score = 0.0;

        for term in &terms {
            if let Some(postings) = self.inverted.get(term) {
                // df = number of documents containing this term
                let mut doc_ids: Vec<i64> = postings.iter().map(|p| p.rowid).collect();
                doc_ids.sort();
                doc_ids.dedup();
                let df = doc_ids.len() as f64;

                // tf = term frequency in this document
                let tf = postings.iter().filter(|p| p.rowid == rowid).count() as f64;

                // IDF component: log((N - df + 0.5) / (df + 0.5) + 1)
                let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();

                // TF component with length normalization
                let tf_norm = (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * dl / avgdl));

                score += idf * tf_norm;
            }
        }

        score
    }

    /// Generate highlighted text for a specific column of a document.
    /// Wraps matching terms with `before_tag` and `after_tag`.
    pub fn highlight(
        &self,
        rowid: i64,
        col_idx: usize,
        before_tag: &str,
        after_tag: &str,
        query: &str,
    ) -> String {
        let query_terms = tokenize(query);
        let doc = match self.documents.get(&rowid) {
            Some(d) => d,
            None => return String::new(),
        };
        let text = match doc.get(col_idx) {
            Some(t) => t,
            None => return String::new(),
        };

        highlight_text(text, &query_terms, before_tag, after_tag)
    }

    /// Generate a snippet with highlighted terms.
    /// Returns a fragment of the text around matching terms.
    pub fn snippet(
        &self,
        rowid: i64,
        col_idx: usize,
        before_tag: &str,
        after_tag: &str,
        ellipsis: &str,
        max_tokens: usize,
        query: &str,
    ) -> String {
        let query_terms = tokenize(query);
        let doc = match self.documents.get(&rowid) {
            Some(d) => d,
            None => return String::new(),
        };
        let text = match doc.get(col_idx) {
            Some(t) => t,
            None => return String::new(),
        };

        snippet_text(text, &query_terms, before_tag, after_tag, ellipsis, max_tokens)
    }

    /// Get the column values for a document.
    pub fn get_document(&self, rowid: i64) -> Option<&Vec<String>> {
        self.documents.get(&rowid)
    }

    /// Build result rows for a query, including the rank column.
    pub fn query_rows(
        &self,
        query: &str,
    ) -> Vec<(i64, Vec<Value>, f64)> {
        let rowids = self.search(query);
        let mut results: Vec<(i64, Vec<Value>, f64)> = Vec::new();

        for rowid in rowids {
            if let Some(doc) = self.documents.get(&rowid) {
                let values: Vec<Value> = doc.iter().map(|s| Value::Text(s.clone())).collect();
                let score = self.bm25(rowid, query);
                results.push((rowid, values, score));
            }
        }

        // Sort by BM25 score descending (best matches first)
        results.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));

        results
    }
}

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

/// Tokenize text into lowercase terms.
/// Splits on whitespace and punctuation, strips common punctuation.
pub fn tokenize(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();

    for ch in text.chars() {
        if ch.is_alphanumeric() || ch == '_' || ch == '\'' {
            // Keep apostrophes within words (e.g., "don't")
            // but strip them from the beginning/end during finalization
            current.push(ch);
        } else {
            if !current.is_empty() {
                let token = normalize_token(&current);
                if !token.is_empty() {
                    tokens.push(token);
                }
                current.clear();
            }
        }
    }

    // Don't forget the last token
    if !current.is_empty() {
        let token = normalize_token(&current);
        if !token.is_empty() {
            tokens.push(token);
        }
    }

    tokens
}

/// Normalize a token: lowercase and strip leading/trailing punctuation.
fn normalize_token(token: &str) -> String {
    let trimmed = token.trim_matches(|c: char| !c.is_alphanumeric());
    trimmed.to_lowercase()
}

// ---------------------------------------------------------------------------
// Highlight and Snippet helpers
// ---------------------------------------------------------------------------

/// Highlight matching terms in text.
fn highlight_text(text: &str, query_terms: &[String], before: &str, after: &str) -> String {
    let words: Vec<&str> = split_preserving_whitespace(text);
    let mut result = String::new();

    for part in &words {
        let normalized = normalize_token(part);
        if !normalized.is_empty() && query_terms.contains(&normalized) {
            // Reconstruct with highlighting
            result.push_str(before);
            result.push_str(part);
            result.push_str(after);
        } else {
            result.push_str(part);
        }
    }

    result
}

/// Split text into words while preserving whitespace as separate items.
fn split_preserving_whitespace(text: &str) -> Vec<&str> {
    let mut parts: Vec<&str> = Vec::new();
    let mut start = 0;
    let mut in_word = false;
    let bytes = text.as_bytes();

    for (i, &b) in bytes.iter().enumerate() {
        let is_space = b == b' ' || b == b'\t' || b == b'\n' || b == b'\r';
        if in_word && is_space {
            parts.push(&text[start..i]);
            start = i;
            in_word = false;
        } else if !in_word && !is_space {
            if i > start {
                parts.push(&text[start..i]);
            }
            start = i;
            in_word = true;
        }
    }
    if start < text.len() {
        parts.push(&text[start..]);
    }
    parts
}

/// Generate a snippet of text around matching terms.
fn snippet_text(
    text: &str,
    query_terms: &[String],
    before: &str,
    after: &str,
    ellipsis: &str,
    max_tokens: usize,
) -> String {
    let tokens = tokenize(text);
    if tokens.is_empty() {
        return String::new();
    }

    // Find the first matching token position
    let first_match = tokens
        .iter()
        .position(|t| query_terms.contains(t))
        .unwrap_or(0);

    // Determine the window around the first match
    let max_tokens = if max_tokens == 0 { tokens.len() } else { max_tokens };
    let half = max_tokens / 2;
    let start = if first_match > half {
        first_match - half
    } else {
        0
    };
    let end = (start + max_tokens).min(tokens.len());

    // Rebuild the snippet from the original text using word boundaries
    // We'll use a simpler approach: split the original text into words and rebuild
    let words: Vec<&str> = text.split_whitespace().collect();
    let w_start = start.min(words.len());
    let w_end = end.min(words.len());

    let mut parts: Vec<String> = Vec::new();
    if w_start > 0 {
        parts.push(ellipsis.to_string());
    }

    for word in &words[w_start..w_end] {
        let normalized = normalize_token(word);
        if !normalized.is_empty() && query_terms.contains(&normalized) {
            parts.push(format!("{}{}{}", before, word, after));
        } else {
            parts.push(word.to_string());
        }
    }

    if w_end < words.len() {
        parts.push(ellipsis.to_string());
    }

    parts.join(" ")
}

// ---------------------------------------------------------------------------
// Global FTS5 Table Registry
// ---------------------------------------------------------------------------

use std::sync::Mutex;

// We use a simple module-level HashMap guarded by a Mutex for the FTS5 indexes.
// In a production system, this would be integrated into the Database struct.
static FTS5_INDEXES: std::sync::LazyLock<Mutex<HashMap<String, Fts5Index>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

/// Register a new FTS5 virtual table.
pub fn create_fts5_table(name: &str, columns: Vec<String>) -> Result<()> {
    let mut indexes = FTS5_INDEXES.lock().map_err(|_| {
        HorizonError::Internal("FTS5 mutex poisoned".into())
    })?;

    if indexes.contains_key(name) {
        return Err(HorizonError::DuplicateTable(name.to_string()));
    }

    let info = Fts5TableInfo {
        name: name.to_string(),
        columns,
        next_rowid: 1,
    };
    indexes.insert(name.to_string(), Fts5Index::new(info));
    Ok(())
}

/// Check if an FTS5 table exists.
pub fn fts5_table_exists(name: &str) -> bool {
    FTS5_INDEXES.lock().map(|i| i.contains_key(name)).unwrap_or(false)
}

/// Get the column names for an FTS5 table.
pub fn fts5_get_columns(name: &str) -> Result<Vec<String>> {
    let indexes = FTS5_INDEXES.lock().map_err(|_| {
        HorizonError::Internal("FTS5 mutex poisoned".into())
    })?;
    indexes
        .get(name)
        .map(|idx| idx.info.columns.clone())
        .ok_or_else(|| HorizonError::TableNotFound(name.to_string()))
}

/// Insert a row into an FTS5 table.
pub fn fts5_insert(name: &str, col_values: Vec<String>) -> Result<i64> {
    let mut indexes = FTS5_INDEXES.lock().map_err(|_| {
        HorizonError::Internal("FTS5 mutex poisoned".into())
    })?;
    let index = indexes.get_mut(name).ok_or_else(|| {
        HorizonError::TableNotFound(name.to_string())
    })?;
    Ok(index.insert(col_values))
}

/// Insert a row into an FTS5 table with a specific rowid.
pub fn fts5_insert_with_rowid(name: &str, rowid: i64, col_values: Vec<String>) -> Result<()> {
    let mut indexes = FTS5_INDEXES.lock().map_err(|_| {
        HorizonError::Internal("FTS5 mutex poisoned".into())
    })?;
    let index = indexes.get_mut(name).ok_or_else(|| {
        HorizonError::TableNotFound(name.to_string())
    })?;
    index.insert_with_rowid(rowid, col_values);
    Ok(())
}

/// Delete a row from an FTS5 table.
pub fn fts5_delete(name: &str, rowid: i64) -> Result<bool> {
    let mut indexes = FTS5_INDEXES.lock().map_err(|_| {
        HorizonError::Internal("FTS5 mutex poisoned".into())
    })?;
    let index = indexes.get_mut(name).ok_or_else(|| {
        HorizonError::TableNotFound(name.to_string())
    })?;
    Ok(index.delete(rowid))
}

/// Query an FTS5 table. Returns (rowid, column_values, bm25_score) tuples.
pub fn fts5_query(name: &str, query: &str) -> Result<Vec<(i64, Vec<Value>, f64)>> {
    let indexes = FTS5_INDEXES.lock().map_err(|_| {
        HorizonError::Internal("FTS5 mutex poisoned".into())
    })?;
    let index = indexes.get(name).ok_or_else(|| {
        HorizonError::TableNotFound(name.to_string())
    })?;
    Ok(index.query_rows(query))
}

/// Get all rows from an FTS5 table (full scan).
pub fn fts5_scan_all(name: &str) -> Result<Vec<(i64, Vec<Value>)>> {
    let indexes = FTS5_INDEXES.lock().map_err(|_| {
        HorizonError::Internal("FTS5 mutex poisoned".into())
    })?;
    let index = indexes.get(name).ok_or_else(|| {
        HorizonError::TableNotFound(name.to_string())
    })?;
    let mut results = Vec::new();
    let mut rowids: Vec<i64> = index.documents.keys().copied().collect();
    rowids.sort();
    for rowid in rowids {
        if let Some(doc) = index.documents.get(&rowid) {
            let values: Vec<Value> = doc.iter().map(|s| Value::Text(s.clone())).collect();
            results.push((rowid, values));
        }
    }
    Ok(results)
}

/// Delete rows from an FTS5 table matching a MATCH query.
pub fn fts5_delete_matching(name: &str, query: &str) -> Result<usize> {
    let mut indexes = FTS5_INDEXES.lock().map_err(|_| {
        HorizonError::Internal("FTS5 mutex poisoned".into())
    })?;
    let index = indexes.get_mut(name).ok_or_else(|| {
        HorizonError::TableNotFound(name.to_string())
    })?;
    let rowids = index.search(query);
    let count = rowids.len();
    for rowid in rowids {
        index.delete(rowid);
    }
    Ok(count)
}

/// Delete all rows from an FTS5 table (DELETE without WHERE).
pub fn fts5_delete_all(name: &str) -> Result<usize> {
    let mut indexes = FTS5_INDEXES.lock().map_err(|_| {
        HorizonError::Internal("FTS5 mutex poisoned".into())
    })?;
    let index = indexes.get_mut(name).ok_or_else(|| {
        HorizonError::TableNotFound(name.to_string())
    })?;
    let count = index.documents.len();
    index.documents.clear();
    index.inverted.clear();
    index.doc_lengths.clear();
    index.total_tokens = 0;
    Ok(count)
}

/// Compute the highlight() function for a given FTS5 table.
pub fn fts5_highlight(
    name: &str,
    rowid: i64,
    col_idx: usize,
    before_tag: &str,
    after_tag: &str,
    query: &str,
) -> Result<String> {
    let indexes = FTS5_INDEXES.lock().map_err(|_| {
        HorizonError::Internal("FTS5 mutex poisoned".into())
    })?;
    let index = indexes.get(name).ok_or_else(|| {
        HorizonError::TableNotFound(name.to_string())
    })?;
    Ok(index.highlight(rowid, col_idx, before_tag, after_tag, query))
}

/// Compute the snippet() function for a given FTS5 table.
pub fn fts5_snippet(
    name: &str,
    rowid: i64,
    col_idx: usize,
    before_tag: &str,
    after_tag: &str,
    ellipsis: &str,
    max_tokens: usize,
    query: &str,
) -> Result<String> {
    let indexes = FTS5_INDEXES.lock().map_err(|_| {
        HorizonError::Internal("FTS5 mutex poisoned".into())
    })?;
    let index = indexes.get(name).ok_or_else(|| {
        HorizonError::TableNotFound(name.to_string())
    })?;
    Ok(index.snippet(rowid, col_idx, before_tag, after_tag, ellipsis, max_tokens, query))
}

/// Compute the bm25() function for a given FTS5 table.
pub fn fts5_bm25(name: &str, rowid: i64, query: &str) -> Result<f64> {
    let indexes = FTS5_INDEXES.lock().map_err(|_| {
        HorizonError::Internal("FTS5 mutex poisoned".into())
    })?;
    let index = indexes.get(name).ok_or_else(|| {
        HorizonError::TableNotFound(name.to_string())
    })?;
    Ok(index.bm25(rowid, query))
}

/// Drop an FTS5 table.
pub fn fts5_drop_table(name: &str) -> Result<()> {
    let mut indexes = FTS5_INDEXES.lock().map_err(|_| {
        HorizonError::Internal("FTS5 mutex poisoned".into())
    })?;
    indexes.remove(name).ok_or_else(|| {
        HorizonError::TableNotFound(name.to_string())
    })?;
    Ok(())
}

/// Delete a row from an FTS5 table by rowid (for DELETE WHERE rowid = ...).
pub fn fts5_delete_by_rowid(name: &str, rowid: i64) -> Result<bool> {
    fts5_delete(name, rowid)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_basic() {
        let tokens = tokenize("Hello, World! This is a test.");
        assert_eq!(tokens, vec!["hello", "world", "this", "is", "a", "test"]);
    }

    #[test]
    fn test_tokenize_empty() {
        let tokens = tokenize("");
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_tokenize_punctuation() {
        let tokens = tokenize("don't stop! (hello)");
        assert_eq!(tokens, vec!["don't", "stop", "hello"]);
    }

    #[test]
    fn test_fts5_index_insert_and_search() {
        let info = Fts5TableInfo {
            name: "test".into(),
            columns: vec!["content".into()],
            next_rowid: 1,
        };
        let mut idx = Fts5Index::new(info);
        idx.insert(vec!["hello world".into()]);
        idx.insert(vec!["goodbye world".into()]);
        idx.insert(vec!["hello rust".into()]);

        let results = idx.search("hello");
        assert_eq!(results.len(), 2);

        let results = idx.search("world");
        assert_eq!(results.len(), 2);

        let results = idx.search("hello world");
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_fts5_index_delete() {
        let info = Fts5TableInfo {
            name: "test".into(),
            columns: vec!["content".into()],
            next_rowid: 1,
        };
        let mut idx = Fts5Index::new(info);
        let rid1 = idx.insert(vec!["hello world".into()]);
        idx.insert(vec!["goodbye world".into()]);

        assert_eq!(idx.search("hello").len(), 1);
        idx.delete(rid1);
        assert_eq!(idx.search("hello").len(), 0);
        assert_eq!(idx.search("world").len(), 1);
    }

    #[test]
    fn test_bm25_scoring() {
        let info = Fts5TableInfo {
            name: "test".into(),
            columns: vec!["content".into()],
            next_rowid: 1,
        };
        let mut idx = Fts5Index::new(info);
        let rid1 = idx.insert(vec!["rust programming language rust rust".into()]);
        let rid2 = idx.insert(vec!["programming is fun".into()]);
        let rid3 = idx.insert(vec!["hello world".into()]);

        // Document 1 mentions "rust" more often, should score higher for "rust"
        let s1 = idx.bm25(rid1, "rust");
        let s2 = idx.bm25(rid2, "rust");
        let s3 = idx.bm25(rid3, "rust");
        assert!(s1 > s2);
        assert!(s1 > s3);
        assert_eq!(s3, 0.0); // "rust" doesn't appear in doc 3
    }

    #[test]
    fn test_highlight() {
        let info = Fts5TableInfo {
            name: "test".into(),
            columns: vec!["content".into()],
            next_rowid: 1,
        };
        let mut idx = Fts5Index::new(info);
        let rid = idx.insert(vec!["the quick brown fox".into()]);

        let result = idx.highlight(rid, 0, "<b>", "</b>", "quick fox");
        assert!(result.contains("<b>quick</b>"));
        assert!(result.contains("<b>fox</b>"));
        assert!(result.contains("the"));
        assert!(result.contains("brown"));
    }

    #[test]
    fn test_snippet() {
        let info = Fts5TableInfo {
            name: "test".into(),
            columns: vec!["content".into()],
            next_rowid: 1,
        };
        let mut idx = Fts5Index::new(info);
        let rid = idx.insert(vec!["one two three four five six seven eight nine ten".into()]);

        let result = idx.snippet(rid, 0, "<b>", "</b>", "...", 5, "five");
        assert!(result.contains("<b>five</b>"));
    }
}
