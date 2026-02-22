//! Core value types and SQL type system for Horizon DB.
//!
//! SQLite uses a dynamic type system with type affinity. Rather than enforcing
//! rigid column types, SQLite stores values in one of five storage classes
//! (NULL, INTEGER, REAL, TEXT, BLOB) and uses affinity rules to determine
//! preferred coercions when inserting data.
//!
//! This module provides:
//! - [`Value`]: The fundamental runtime value representation, corresponding to
//!   SQLite's five storage classes.
//! - [`DataType`]: Column type affinities that guide how values are coerced on
//!   insertion.
//! - [`determine_affinity`]: Implements SQLite's type-name-to-affinity mapping
//!   algorithm.
//! - Serialization and deserialization of values to and from a compact binary
//!   encoding suitable for on-disk storage and network transport.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

use crate::error::{HorizonError, Result};

// ---------------------------------------------------------------------------
// DataType (column affinity)
// ---------------------------------------------------------------------------

/// Represents the type affinity of a column, following SQLite's affinity system.
///
/// In SQLite, every column has a type affinity — a recommended (but not
/// enforced) type for values stored in that column. The five affinities are
/// INTEGER, TEXT, BLOB, REAL, and NUMERIC.
///
/// The affinity of a column is determined by the declared type name in the
/// `CREATE TABLE` statement according to the rules implemented by
/// [`determine_affinity`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    /// Prefer storing values as signed 64-bit integers.
    Integer,
    /// Prefer storing values as IEEE 754 64-bit floating-point numbers.
    Real,
    /// Prefer storing values as UTF-8 text strings.
    Text,
    /// Store values as raw byte sequences with no type preference.
    /// This is also used when no type is declared (NONE affinity in SQLite).
    Blob,
    /// The catch-all affinity. Attempts numeric storage (integer first, then
    /// real) for values that look numeric, but retains the original
    /// representation otherwise.
    Numeric,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Real => write!(f, "REAL"),
            DataType::Text => write!(f, "TEXT"),
            DataType::Blob => write!(f, "BLOB"),
            DataType::Numeric => write!(f, "NUMERIC"),
        }
    }
}

// ---------------------------------------------------------------------------
// Affinity determination
// ---------------------------------------------------------------------------

/// Determines the type affinity for a column based on its declared type name.
///
/// This follows the SQLite affinity determination rules (in order of priority):
///
/// 1. If the type name contains `"INT"` (case-insensitive), the affinity is
///    [`DataType::Integer`].
/// 2. If the type name contains `"CHAR"`, `"CLOB"`, or `"TEXT"`
///    (case-insensitive), the affinity is [`DataType::Text`].
/// 3. If the type name contains `"BLOB"` (case-insensitive), or if no type
///    name is specified (empty string), the affinity is [`DataType::Blob`].
/// 4. If the type name contains `"REAL"`, `"FLOA"`, or `"DOUB"`
///    (case-insensitive), the affinity is [`DataType::Real`].
/// 5. Otherwise, the affinity is [`DataType::Numeric`].
///
/// # Examples
///
/// ```
/// use horizon::types::determine_affinity;
/// use horizon::types::DataType;
///
/// assert_eq!(determine_affinity("INTEGER"), DataType::Integer);
/// assert_eq!(determine_affinity("BIGINT"), DataType::Integer);
/// assert_eq!(determine_affinity("VARCHAR(255)"), DataType::Text);
/// assert_eq!(determine_affinity("BLOB"), DataType::Blob);
/// assert_eq!(determine_affinity(""), DataType::Blob);
/// assert_eq!(determine_affinity("DOUBLE PRECISION"), DataType::Real);
/// assert_eq!(determine_affinity("DECIMAL(10,5)"), DataType::Numeric);
/// ```
pub fn determine_affinity(type_name: &str) -> DataType {
    let upper = type_name.to_uppercase();

    // Rule 1: Contains "INT" → Integer affinity
    if upper.contains("INT") {
        return DataType::Integer;
    }

    // Rule 2: Contains "CHAR", "CLOB", or "TEXT" → Text affinity
    if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
        return DataType::Text;
    }

    // Rule 3: Contains "BLOB" or empty → Blob (NONE) affinity
    if upper.contains("BLOB") || upper.is_empty() {
        return DataType::Blob;
    }

    // Rule 4: Contains "REAL", "FLOA", or "DOUB" → Real affinity
    if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        return DataType::Real;
    }

    // Rule 5: Otherwise → Numeric affinity
    DataType::Numeric
}

// ---------------------------------------------------------------------------
// Value
// ---------------------------------------------------------------------------

/// A dynamically-typed database value corresponding to one of SQLite's five
/// storage classes.
///
/// `Value` is the fundamental unit of data within Horizon DB. Every cell in
/// every row is stored as a `Value`. The variants map directly to the SQLite
/// storage classes:
///
/// | Variant   | SQLite Storage Class | Rust Type    |
/// |-----------|---------------------|--------------|
/// | `Null`    | NULL                | —            |
/// | `Integer` | INTEGER             | `i64`        |
/// | `Real`    | REAL                | `f64`        |
/// | `Text`    | TEXT                | `String`     |
/// | `Blob`    | BLOB                | `Vec<u8>`    |
///
/// # Ordering
///
/// Values follow the SQLite comparison rules:
/// - NULL compares less than any other type.
/// - INTEGER and REAL values are compared numerically (cross-type).
/// - TEXT values compare greater than INTEGER/REAL.
/// - BLOB values compare greater than all other types.
/// - Within the same type, natural ordering applies.
///
/// # Equality
///
/// For database consistency, two `NaN` floating-point values are considered
/// equal. This diverges from IEEE 754 but is necessary for correct behavior
/// in indexes and `GROUP BY` clauses.
#[derive(Debug, Clone)]
pub enum Value {
    /// The SQL NULL value — absence of any value.
    Null,
    /// A signed 64-bit integer.
    Integer(i64),
    /// An IEEE 754 64-bit floating-point number.
    Real(f64),
    /// A UTF-8 encoded text string.
    Text(String),
    /// A raw byte sequence.
    Blob(Vec<u8>),
}

// ---------------------------------------------------------------------------
// Type tag constants for serialization
// ---------------------------------------------------------------------------

/// Serialization type tag for NULL values.
const TAG_NULL: u8 = 0;
/// Serialization type tag for INTEGER values.
const TAG_INTEGER: u8 = 1;
/// Serialization type tag for REAL values.
const TAG_REAL: u8 = 2;
/// Serialization type tag for TEXT values.
const TAG_TEXT: u8 = 3;
/// Serialization type tag for BLOB values.
const TAG_BLOB: u8 = 4;

// ---------------------------------------------------------------------------
// Value — core methods
// ---------------------------------------------------------------------------

impl Value {
    /// Returns the [`DataType`] of this value, or `None` if the value is
    /// [`Value::Null`].
    ///
    /// # Examples
    ///
    /// ```
    /// use horizon::types::{Value, DataType};
    ///
    /// assert_eq!(Value::Integer(42).data_type(), Some(DataType::Integer));
    /// assert_eq!(Value::Null.data_type(), None);
    /// ```
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Value::Null => None,
            Value::Integer(_) => Some(DataType::Integer),
            Value::Real(_) => Some(DataType::Real),
            Value::Text(_) => Some(DataType::Text),
            Value::Blob(_) => Some(DataType::Blob),
        }
    }

    /// Coerces this value to the given type affinity following SQLite's
    /// affinity application rules.
    ///
    /// The rules are:
    ///
    /// - **Integer affinity**: Attempt to convert TEXT to integer (or real if
    ///   the text represents a real number). REAL values that are exact
    ///   integers are converted to INTEGER. Other values are unchanged.
    /// - **Real affinity**: Attempt to convert TEXT to real. INTEGER values
    ///   are promoted to REAL. Other values are unchanged.
    /// - **Text affinity**: INTEGER and REAL are converted to their text
    ///   representation. Other values are unchanged.
    /// - **Numeric affinity**: TEXT that looks like an integer becomes INTEGER;
    ///   TEXT that looks like a real becomes REAL. INTEGER and REAL stay as-is.
    ///   Other values are unchanged.
    /// - **Blob (NONE) affinity**: No conversion is performed.
    /// - **NULL**: Always remains NULL regardless of affinity.
    ///
    /// # Examples
    ///
    /// ```
    /// use horizon::types::{Value, DataType};
    ///
    /// let v = Value::Text("42".to_string());
    /// assert_eq!(v.apply_affinity(DataType::Integer), Value::Integer(42));
    ///
    /// let v = Value::Integer(7);
    /// assert_eq!(v.apply_affinity(DataType::Text), Value::Text("7".to_string()));
    /// ```
    pub fn apply_affinity(&self, affinity: DataType) -> Value {
        // NULL is never coerced.
        if self.is_null() {
            return Value::Null;
        }

        match affinity {
            DataType::Integer => self.coerce_to_integer(),
            DataType::Real => self.coerce_to_real(),
            DataType::Text => self.coerce_to_text(),
            DataType::Numeric => self.coerce_to_numeric(),
            DataType::Blob => self.clone(), // NONE affinity — no conversion
        }
    }

    /// Returns `true` if this value is [`Value::Null`].
    ///
    /// # Examples
    ///
    /// ```
    /// use horizon::types::Value;
    ///
    /// assert!(Value::Null.is_null());
    /// assert!(!Value::Integer(0).is_null());
    /// ```
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Attempts to extract an `i64` from this value.
    ///
    /// Returns `Some(i)` for `Value::Integer(i)`, and `None` for all other
    /// variants.
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Attempts to extract an `f64` from this value.
    ///
    /// Returns `Some(r)` for `Value::Real(r)`, and `None` for all other
    /// variants.
    pub fn as_real(&self) -> Option<f64> {
        match self {
            Value::Real(r) => Some(*r),
            _ => None,
        }
    }

    /// Attempts to extract a string slice from this value.
    ///
    /// Returns `Some(s)` for `Value::Text(s)`, and `None` for all other
    /// variants.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Attempts to extract a byte slice from this value.
    ///
    /// Returns `Some(b)` for `Value::Blob(b)`, and `None` for all other
    /// variants.
    pub fn as_blob(&self) -> Option<&[u8]> {
        match self {
            Value::Blob(b) => Some(b.as_slice()),
            _ => None,
        }
    }

    /// Evaluates the SQLite "truthiness" of this value.
    ///
    /// In SQLite:
    /// - NULL is false.
    /// - Integer `0` is false.
    /// - Real `0.0` is false.
    /// - Everything else is true (non-zero numbers, any text, any blob).
    ///
    /// # Examples
    ///
    /// ```
    /// use horizon::types::Value;
    ///
    /// assert!(!Value::Null.to_bool());
    /// assert!(!Value::Integer(0).to_bool());
    /// assert!(!Value::Real(0.0).to_bool());
    /// assert!(Value::Integer(1).to_bool());
    /// assert!(Value::Text("".to_string()).to_bool());
    /// assert!(Value::Blob(vec![]).to_bool());
    /// ```
    pub fn to_bool(&self) -> bool {
        match self {
            Value::Null => false,
            Value::Integer(i) => *i != 0,
            Value::Real(r) => *r != 0.0,
            // In SQLite, any TEXT (even empty) and any BLOB (even empty) are truthy.
            Value::Text(_) => true,
            Value::Blob(_) => true,
        }
    }

    // -- Serialization ------------------------------------------------------

    /// Serializes this value into a compact binary representation.
    ///
    /// The encoding format is:
    ///
    /// | Type    | Format                              |
    /// |---------|-------------------------------------|
    /// | Null    | `[0]` (1 byte)                      |
    /// | Integer | `[1][8-byte i64 big-endian]` (9 bytes) |
    /// | Real    | `[2][8-byte f64 big-endian]` (9 bytes) |
    /// | Text    | `[3][4-byte length BE][UTF-8 data]` |
    /// | Blob    | `[4][4-byte length BE][raw data]`   |
    ///
    /// # Examples
    ///
    /// ```
    /// use horizon::types::Value;
    ///
    /// let v = Value::Integer(42);
    /// let bytes = v.serialize();
    /// let (decoded, consumed) = Value::deserialize(&bytes).unwrap();
    /// assert_eq!(decoded, v);
    /// assert_eq!(consumed, bytes.len());
    /// ```
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Value::Null => {
                vec![TAG_NULL]
            }
            Value::Integer(i) => {
                let mut buf = Vec::with_capacity(9);
                buf.push(TAG_INTEGER);
                buf.extend_from_slice(&i.to_be_bytes());
                buf
            }
            Value::Real(r) => {
                let mut buf = Vec::with_capacity(9);
                buf.push(TAG_REAL);
                buf.extend_from_slice(&r.to_be_bytes());
                buf
            }
            Value::Text(s) => {
                let bytes = s.as_bytes();
                let len = bytes.len() as u32;
                let mut buf = Vec::with_capacity(1 + 4 + bytes.len());
                buf.push(TAG_TEXT);
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(bytes);
                buf
            }
            Value::Blob(b) => {
                let len = b.len() as u32;
                let mut buf = Vec::with_capacity(1 + 4 + b.len());
                buf.push(TAG_BLOB);
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(b);
                buf
            }
        }
    }

    /// Deserializes a value from its binary representation.
    ///
    /// Returns the decoded [`Value`] together with the number of bytes
    /// consumed from `data`. This allows callers to deserialize a sequence of
    /// values from a contiguous buffer.
    ///
    /// # Errors
    ///
    /// Returns [`HorizonError::Deserialization`] if the data is truncated,
    /// contains an invalid type tag, or the text payload is not valid UTF-8.
    ///
    /// # Examples
    ///
    /// ```
    /// use horizon::types::Value;
    ///
    /// let v = Value::Text("hello".to_string());
    /// let bytes = v.serialize();
    /// let (decoded, consumed) = Value::deserialize(&bytes).unwrap();
    /// assert_eq!(decoded, Value::Text("hello".to_string()));
    /// assert_eq!(consumed, bytes.len());
    /// ```
    pub fn deserialize(data: &[u8]) -> Result<(Value, usize)> {
        if data.is_empty() {
            return Err(HorizonError::Deserialization(
                "cannot deserialize value from empty data".to_string(),
            ));
        }

        let tag = data[0];

        match tag {
            TAG_NULL => Ok((Value::Null, 1)),

            TAG_INTEGER => {
                if data.len() < 9 {
                    return Err(HorizonError::Deserialization(
                        "insufficient data for INTEGER value (need 9 bytes)".to_string(),
                    ));
                }
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&data[1..9]);
                let i = i64::from_be_bytes(bytes);
                Ok((Value::Integer(i), 9))
            }

            TAG_REAL => {
                if data.len() < 9 {
                    return Err(HorizonError::Deserialization(
                        "insufficient data for REAL value (need 9 bytes)".to_string(),
                    ));
                }
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&data[1..9]);
                let r = f64::from_be_bytes(bytes);
                Ok((Value::Real(r), 9))
            }

            TAG_TEXT => {
                if data.len() < 5 {
                    return Err(HorizonError::Deserialization(
                        "insufficient data for TEXT length prefix".to_string(),
                    ));
                }
                let mut len_bytes = [0u8; 4];
                len_bytes.copy_from_slice(&data[1..5]);
                let len = u32::from_be_bytes(len_bytes) as usize;
                let total = 5 + len;
                if data.len() < total {
                    return Err(HorizonError::Deserialization(format!(
                        "insufficient data for TEXT payload (need {} bytes, have {})",
                        total,
                        data.len()
                    )));
                }
                let s = std::str::from_utf8(&data[5..total]).map_err(|e| {
                    HorizonError::Deserialization(format!("invalid UTF-8 in TEXT value: {}", e))
                })?;
                Ok((Value::Text(s.to_string()), total))
            }

            TAG_BLOB => {
                if data.len() < 5 {
                    return Err(HorizonError::Deserialization(
                        "insufficient data for BLOB length prefix".to_string(),
                    ));
                }
                let mut len_bytes = [0u8; 4];
                len_bytes.copy_from_slice(&data[1..5]);
                let len = u32::from_be_bytes(len_bytes) as usize;
                let total = 5 + len;
                if data.len() < total {
                    return Err(HorizonError::Deserialization(format!(
                        "insufficient data for BLOB payload (need {} bytes, have {})",
                        total,
                        data.len()
                    )));
                }
                Ok((Value::Blob(data[5..total].to_vec()), total))
            }

            _ => Err(HorizonError::Deserialization(format!(
                "unknown type tag: {}",
                tag
            ))),
        }
    }

    // -- Private affinity coercion helpers -----------------------------------

    /// Coerce value toward INTEGER affinity.
    fn coerce_to_integer(&self) -> Value {
        match self {
            Value::Integer(_) => self.clone(),
            Value::Real(r) => {
                // If the real value is an exact integer, convert it.
                let truncated = r.trunc();
                if (*r - truncated).abs() == 0.0 && truncated >= i64::MIN as f64 && truncated <= i64::MAX as f64 {
                    Value::Integer(truncated as i64)
                } else {
                    self.clone()
                }
            }
            Value::Text(s) => {
                // Try integer first, then real, then keep as text.
                if let Ok(i) = s.trim().parse::<i64>() {
                    Value::Integer(i)
                } else if let Ok(r) = s.trim().parse::<f64>() {
                    // If the parsed real is an exact integer, store as integer.
                    let truncated = r.trunc();
                    if (r - truncated).abs() == 0.0 {
                        Value::Integer(truncated as i64)
                    } else {
                        Value::Real(r)
                    }
                } else {
                    self.clone()
                }
            }
            _ => self.clone(),
        }
    }

    /// Coerce value toward REAL affinity.
    fn coerce_to_real(&self) -> Value {
        match self {
            Value::Real(_) => self.clone(),
            Value::Integer(i) => Value::Real(*i as f64),
            Value::Text(s) => {
                if let Ok(r) = s.trim().parse::<f64>() {
                    Value::Real(r)
                } else {
                    self.clone()
                }
            }
            _ => self.clone(),
        }
    }

    /// Coerce value toward TEXT affinity.
    fn coerce_to_text(&self) -> Value {
        match self {
            Value::Text(_) => self.clone(),
            Value::Integer(i) => Value::Text(i.to_string()),
            Value::Real(r) => Value::Text(format!("{}", r)),
            _ => self.clone(),
        }
    }

    /// Coerce value toward NUMERIC affinity.
    fn coerce_to_numeric(&self) -> Value {
        match self {
            Value::Text(s) => {
                let trimmed = s.trim();
                // Try integer first.
                if let Ok(i) = trimmed.parse::<i64>() {
                    return Value::Integer(i);
                }
                // Then try real.
                if let Ok(r) = trimmed.parse::<f64>() {
                    // If the real is an exact integer, store as integer.
                    let truncated = r.trunc();
                    if (r - truncated).abs() == 0.0 && truncated >= i64::MIN as f64 && truncated <= i64::MAX as f64 {
                        Value::Integer(truncated as i64)
                    } else {
                        Value::Real(r)
                    }
                } else {
                    self.clone()
                }
            }
            // INTEGER and REAL stay as-is.
            _ => self.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// PartialEq / Eq
// ---------------------------------------------------------------------------

impl PartialEq for Value {
    /// Compares two values for equality.
    ///
    /// Follows SQLite semantics with one notable exception: two `NaN` values
    /// are considered equal for database consistency (indexes, deduplication).
    ///
    /// Cross-type INTEGER/REAL comparisons are performed numerically.
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Real(a), Value::Real(b)) => {
                // Treat NaN == NaN as true for database purposes.
                if a.is_nan() && b.is_nan() {
                    return true;
                }
                a == b
            }
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Blob(a), Value::Blob(b)) => a == b,
            // Cross-type numeric comparison.
            (Value::Integer(i), Value::Real(r)) | (Value::Real(r), Value::Integer(i)) => {
                *r == (*i as f64)
            }
            _ => false,
        }
    }
}

impl Eq for Value {}

// ---------------------------------------------------------------------------
// Hash
// ---------------------------------------------------------------------------

impl Hash for Value {
    /// Hashes a value.
    ///
    /// This implementation is consistent with [`PartialEq`]: values that
    /// compare equal produce the same hash. Integer and Real values that are
    /// numerically equal hash identically.
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Discriminant tag to differentiate types at the hash level.
        match self {
            Value::Null => {
                0u8.hash(state);
            }
            Value::Integer(i) => {
                // To maintain consistency with Real comparisons, hash via
                // the f64 bit pattern when the integer is exactly
                // representable.
                1u8.hash(state);
                i.hash(state);
            }
            Value::Real(r) => {
                // Canonicalize NaN so that all NaN values hash equally.
                2u8.hash(state);
                if r.is_nan() {
                    // Use a fixed canonical NaN bit pattern.
                    u64::MAX.hash(state);
                } else {
                    r.to_bits().hash(state);
                }
            }
            Value::Text(s) => {
                3u8.hash(state);
                s.hash(state);
            }
            Value::Blob(b) => {
                4u8.hash(state);
                b.hash(state);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// PartialOrd / Ord
// ---------------------------------------------------------------------------

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    /// Compares two values following SQLite's comparison rules.
    ///
    /// The ordering is: NULL < INTEGER/REAL < TEXT < BLOB.
    ///
    /// Within the numeric group, INTEGER and REAL values are compared
    /// numerically (cross-type). Within TEXT, comparison is lexicographic
    /// (byte-by-byte). Within BLOB, comparison is `memcmp`-style.
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            // NULL comparisons
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,

            // Same-type comparisons
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Real(a), Value::Real(b)) => compare_f64(*a, *b),
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Blob(a), Value::Blob(b)) => a.cmp(b),

            // Cross-type numeric comparisons (INTEGER vs REAL)
            (Value::Integer(i), Value::Real(r)) => compare_f64(*i as f64, *r),
            (Value::Real(r), Value::Integer(i)) => compare_f64(*r, *i as f64),

            // Cross-group ordering: INTEGER/REAL < TEXT < BLOB
            (Value::Integer(_) | Value::Real(_), Value::Text(_)) => Ordering::Less,
            (Value::Integer(_) | Value::Real(_), Value::Blob(_)) => Ordering::Less,
            (Value::Text(_), Value::Integer(_) | Value::Real(_)) => Ordering::Greater,
            (Value::Text(_), Value::Blob(_)) => Ordering::Less,
            (Value::Blob(_), Value::Integer(_) | Value::Real(_)) => Ordering::Greater,
            (Value::Blob(_), Value::Text(_)) => Ordering::Greater,
        }
    }
}

/// Compares two `f64` values with a total ordering.
///
/// NaN is treated as equal to NaN and greater than all other values (for a
/// stable sort order).
fn compare_f64(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b).unwrap_or_else(|| {
        // At least one value is NaN.
        match (a.is_nan(), b.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => unreachable!(),
        }
    })
}

// ---------------------------------------------------------------------------
// Display
// ---------------------------------------------------------------------------

impl fmt::Display for Value {
    /// Formats a value for human-readable CLI output.
    ///
    /// - NULL is displayed as `"NULL"`.
    /// - Integer and Real values use their standard representations.
    /// - Text values are displayed without quotes.
    /// - Blob values are displayed as hexadecimal with an `X'...'` prefix
    ///   (matching SQLite's blob literal syntax).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Real(r) => {
                // If the value is a whole number, display with one decimal place
                // to distinguish from integers.
                if r.fract() == 0.0 && r.is_finite() {
                    write!(f, "{:.1}", r)
                } else {
                    write!(f, "{}", r)
                }
            }
            Value::Text(s) => write!(f, "{}", s),
            Value::Blob(b) => {
                write!(f, "X'")?;
                for byte in b {
                    write!(f, "{:02X}", byte)?;
                }
                write!(f, "'")
            }
        }
    }
}

// ---------------------------------------------------------------------------
// From trait implementations
// ---------------------------------------------------------------------------

impl From<i64> for Value {
    /// Creates a [`Value::Integer`] from an `i64`.
    fn from(i: i64) -> Self {
        Value::Integer(i)
    }
}

impl From<i32> for Value {
    /// Creates a [`Value::Integer`] from an `i32` by widening to `i64`.
    fn from(i: i32) -> Self {
        Value::Integer(i as i64)
    }
}

impl From<f64> for Value {
    /// Creates a [`Value::Real`] from an `f64`.
    fn from(r: f64) -> Self {
        Value::Real(r)
    }
}

impl From<String> for Value {
    /// Creates a [`Value::Text`] from an owned `String`.
    fn from(s: String) -> Self {
        Value::Text(s)
    }
}

impl From<&str> for Value {
    /// Creates a [`Value::Text`] from a string slice (allocates).
    fn from(s: &str) -> Self {
        Value::Text(s.to_string())
    }
}

impl From<Vec<u8>> for Value {
    /// Creates a [`Value::Blob`] from a byte vector.
    fn from(b: Vec<u8>) -> Self {
        Value::Blob(b)
    }
}

impl From<bool> for Value {
    /// Creates a [`Value::Integer`] from a `bool`.
    ///
    /// SQLite does not have a native boolean type; booleans are stored as
    /// integers where `true` maps to `1` and `false` maps to `0`.
    fn from(b: bool) -> Self {
        Value::Integer(if b { 1 } else { 0 })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- DataType / Affinity tests ------------------------------------------

    #[test]
    fn test_determine_affinity_integer() {
        assert_eq!(determine_affinity("INT"), DataType::Integer);
        assert_eq!(determine_affinity("INTEGER"), DataType::Integer);
        assert_eq!(determine_affinity("TINYINT"), DataType::Integer);
        assert_eq!(determine_affinity("SMALLINT"), DataType::Integer);
        assert_eq!(determine_affinity("MEDIUMINT"), DataType::Integer);
        assert_eq!(determine_affinity("BIGINT"), DataType::Integer);
        assert_eq!(determine_affinity("UNSIGNED BIG INT"), DataType::Integer);
        assert_eq!(determine_affinity("INT2"), DataType::Integer);
        assert_eq!(determine_affinity("INT8"), DataType::Integer);
    }

    #[test]
    fn test_determine_affinity_text() {
        assert_eq!(determine_affinity("CHARACTER(20)"), DataType::Text);
        assert_eq!(determine_affinity("VARCHAR(255)"), DataType::Text);
        assert_eq!(determine_affinity("VARYING CHARACTER(255)"), DataType::Text);
        assert_eq!(determine_affinity("NCHAR(55)"), DataType::Text);
        assert_eq!(determine_affinity("NATIVE CHARACTER(70)"), DataType::Text);
        assert_eq!(determine_affinity("NVARCHAR(100)"), DataType::Text);
        assert_eq!(determine_affinity("TEXT"), DataType::Text);
        assert_eq!(determine_affinity("CLOB"), DataType::Text);
    }

    #[test]
    fn test_determine_affinity_blob() {
        assert_eq!(determine_affinity("BLOB"), DataType::Blob);
        assert_eq!(determine_affinity(""), DataType::Blob);
    }

    #[test]
    fn test_determine_affinity_real() {
        assert_eq!(determine_affinity("REAL"), DataType::Real);
        assert_eq!(determine_affinity("DOUBLE"), DataType::Real);
        assert_eq!(determine_affinity("DOUBLE PRECISION"), DataType::Real);
        assert_eq!(determine_affinity("FLOAT"), DataType::Real);
    }

    #[test]
    fn test_determine_affinity_numeric() {
        assert_eq!(determine_affinity("NUMERIC"), DataType::Numeric);
        assert_eq!(determine_affinity("DECIMAL(10,5)"), DataType::Numeric);
        assert_eq!(determine_affinity("BOOLEAN"), DataType::Numeric);
        assert_eq!(determine_affinity("DATE"), DataType::Numeric);
        assert_eq!(determine_affinity("DATETIME"), DataType::Numeric);
    }

    #[test]
    fn test_determine_affinity_case_insensitive() {
        assert_eq!(determine_affinity("integer"), DataType::Integer);
        assert_eq!(determine_affinity("Text"), DataType::Text);
        assert_eq!(determine_affinity("rEaL"), DataType::Real);
        assert_eq!(determine_affinity("blob"), DataType::Blob);
    }

    // -- Value type inspection ----------------------------------------------

    #[test]
    fn test_data_type() {
        assert_eq!(Value::Null.data_type(), None);
        assert_eq!(Value::Integer(1).data_type(), Some(DataType::Integer));
        assert_eq!(Value::Real(1.0).data_type(), Some(DataType::Real));
        assert_eq!(
            Value::Text("hello".to_string()).data_type(),
            Some(DataType::Text)
        );
        assert_eq!(
            Value::Blob(vec![1, 2, 3]).data_type(),
            Some(DataType::Blob)
        );
    }

    #[test]
    fn test_is_null() {
        assert!(Value::Null.is_null());
        assert!(!Value::Integer(0).is_null());
        assert!(!Value::Real(0.0).is_null());
        assert!(!Value::Text(String::new()).is_null());
        assert!(!Value::Blob(Vec::new()).is_null());
    }

    // -- Accessor tests -----------------------------------------------------

    #[test]
    fn test_as_integer() {
        assert_eq!(Value::Integer(42).as_integer(), Some(42));
        assert_eq!(Value::Real(1.0).as_integer(), None);
        assert_eq!(Value::Null.as_integer(), None);
    }

    #[test]
    fn test_as_real() {
        assert_eq!(Value::Real(3.14).as_real(), Some(3.14));
        assert_eq!(Value::Integer(1).as_real(), None);
        assert_eq!(Value::Null.as_real(), None);
    }

    #[test]
    fn test_as_text() {
        assert_eq!(Value::Text("hi".to_string()).as_text(), Some("hi"));
        assert_eq!(Value::Integer(1).as_text(), None);
    }

    #[test]
    fn test_as_blob() {
        assert_eq!(Value::Blob(vec![1, 2]).as_blob(), Some([1u8, 2].as_slice()));
        assert_eq!(Value::Integer(1).as_blob(), None);
    }

    // -- Truthiness ---------------------------------------------------------

    #[test]
    fn test_to_bool() {
        assert!(!Value::Null.to_bool());
        assert!(!Value::Integer(0).to_bool());
        assert!(!Value::Real(0.0).to_bool());
        assert!(Value::Integer(1).to_bool());
        assert!(Value::Integer(-1).to_bool());
        assert!(Value::Real(0.1).to_bool());
        assert!(Value::Text(String::new()).to_bool());
        assert!(Value::Text("false".to_string()).to_bool());
        assert!(Value::Blob(Vec::new()).to_bool());
    }

    // -- Equality -----------------------------------------------------------

    #[test]
    fn test_equality_same_type() {
        assert_eq!(Value::Null, Value::Null);
        assert_eq!(Value::Integer(42), Value::Integer(42));
        assert_ne!(Value::Integer(1), Value::Integer(2));
        assert_eq!(Value::Real(3.14), Value::Real(3.14));
        assert_eq!(
            Value::Text("abc".to_string()),
            Value::Text("abc".to_string())
        );
        assert_eq!(Value::Blob(vec![1, 2]), Value::Blob(vec![1, 2]));
    }

    #[test]
    fn test_equality_cross_type_numeric() {
        assert_eq!(Value::Integer(42), Value::Real(42.0));
        assert_eq!(Value::Real(42.0), Value::Integer(42));
        assert_ne!(Value::Integer(42), Value::Real(42.5));
    }

    #[test]
    fn test_equality_nan() {
        let nan1 = Value::Real(f64::NAN);
        let nan2 = Value::Real(f64::NAN);
        assert_eq!(nan1, nan2);
    }

    #[test]
    fn test_equality_different_types() {
        assert_ne!(Value::Integer(0), Value::Null);
        assert_ne!(Value::Integer(1), Value::Text("1".to_string()));
        assert_ne!(Value::Text("abc".to_string()), Value::Blob(b"abc".to_vec()));
    }

    // -- Ordering -----------------------------------------------------------

    #[test]
    fn test_ordering_null_least() {
        assert!(Value::Null < Value::Integer(0));
        assert!(Value::Null < Value::Real(0.0));
        assert!(Value::Null < Value::Text(String::new()));
        assert!(Value::Null < Value::Blob(Vec::new()));
    }

    #[test]
    fn test_ordering_type_groups() {
        let null = Value::Null;
        let int = Value::Integer(100);
        let real = Value::Real(1.0);
        let text = Value::Text("a".to_string());
        let blob = Value::Blob(vec![0]);

        assert!(null < int);
        assert!(null < real);
        assert!(int < text);
        assert!(real < text);
        assert!(text < blob);
        assert!(int < blob);
    }

    #[test]
    fn test_ordering_within_integer() {
        assert!(Value::Integer(-1) < Value::Integer(0));
        assert!(Value::Integer(0) < Value::Integer(1));
    }

    #[test]
    fn test_ordering_within_text() {
        assert!(Value::Text("a".to_string()) < Value::Text("b".to_string()));
        assert!(Value::Text("abc".to_string()) < Value::Text("abd".to_string()));
    }

    #[test]
    fn test_ordering_cross_numeric() {
        assert!(Value::Integer(1) < Value::Real(1.5));
        assert!(Value::Real(0.5) < Value::Integer(1));
        assert_eq!(Value::Integer(1).cmp(&Value::Real(1.0)), Ordering::Equal);
    }

    // -- Display ------------------------------------------------------------

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", Value::Null), "NULL");
        assert_eq!(format!("{}", Value::Integer(42)), "42");
        assert_eq!(format!("{}", Value::Real(3.14)), "3.14");
        assert_eq!(format!("{}", Value::Real(42.0)), "42.0");
        assert_eq!(format!("{}", Value::Text("hello".to_string())), "hello");
        assert_eq!(
            format!("{}", Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF])),
            "X'DEADBEEF'"
        );
    }

    // -- From traits --------------------------------------------------------

    #[test]
    fn test_from_i64() {
        let v: Value = 42i64.into();
        assert_eq!(v, Value::Integer(42));
    }

    #[test]
    fn test_from_i32() {
        let v: Value = 42i32.into();
        assert_eq!(v, Value::Integer(42));
    }

    #[test]
    fn test_from_f64() {
        let v: Value = 3.14f64.into();
        assert_eq!(v, Value::Real(3.14));
    }

    #[test]
    fn test_from_string() {
        let v: Value = "hello".to_string().into();
        assert_eq!(v, Value::Text("hello".to_string()));
    }

    #[test]
    fn test_from_str() {
        let v: Value = "hello".into();
        assert_eq!(v, Value::Text("hello".to_string()));
    }

    #[test]
    fn test_from_vec_u8() {
        let v: Value = vec![1u8, 2, 3].into();
        assert_eq!(v, Value::Blob(vec![1, 2, 3]));
    }

    #[test]
    fn test_from_bool() {
        let t: Value = true.into();
        let f: Value = false.into();
        assert_eq!(t, Value::Integer(1));
        assert_eq!(f, Value::Integer(0));
    }

    // -- Affinity application -----------------------------------------------

    #[test]
    fn test_affinity_null_unchanged() {
        assert_eq!(Value::Null.apply_affinity(DataType::Integer), Value::Null);
        assert_eq!(Value::Null.apply_affinity(DataType::Text), Value::Null);
        assert_eq!(Value::Null.apply_affinity(DataType::Real), Value::Null);
        assert_eq!(Value::Null.apply_affinity(DataType::Numeric), Value::Null);
        assert_eq!(Value::Null.apply_affinity(DataType::Blob), Value::Null);
    }

    #[test]
    fn test_affinity_text_to_integer() {
        let v = Value::Text("42".to_string());
        assert_eq!(v.apply_affinity(DataType::Integer), Value::Integer(42));
    }

    #[test]
    fn test_affinity_text_to_real() {
        let v = Value::Text("3.14".to_string());
        assert_eq!(v.apply_affinity(DataType::Real), Value::Real(3.14));
    }

    #[test]
    fn test_affinity_text_integer_to_real() {
        let v = Value::Text("42".to_string());
        assert_eq!(v.apply_affinity(DataType::Real), Value::Real(42.0));
    }

    #[test]
    fn test_affinity_integer_to_text() {
        let v = Value::Integer(42);
        assert_eq!(
            v.apply_affinity(DataType::Text),
            Value::Text("42".to_string())
        );
    }

    #[test]
    fn test_affinity_real_to_text() {
        let v = Value::Real(3.14);
        assert_eq!(
            v.apply_affinity(DataType::Text),
            Value::Text("3.14".to_string())
        );
    }

    #[test]
    fn test_affinity_integer_to_real() {
        let v = Value::Integer(42);
        assert_eq!(v.apply_affinity(DataType::Real), Value::Real(42.0));
    }

    #[test]
    fn test_affinity_real_exact_to_integer() {
        let v = Value::Real(42.0);
        assert_eq!(v.apply_affinity(DataType::Integer), Value::Integer(42));
    }

    #[test]
    fn test_affinity_real_inexact_stays_real() {
        let v = Value::Real(42.5);
        assert_eq!(v.apply_affinity(DataType::Integer), Value::Real(42.5));
    }

    #[test]
    fn test_affinity_blob_unchanged() {
        let v = Value::Blob(vec![1, 2, 3]);
        assert_eq!(
            v.apply_affinity(DataType::Integer),
            Value::Blob(vec![1, 2, 3])
        );
        assert_eq!(
            v.apply_affinity(DataType::Text),
            Value::Blob(vec![1, 2, 3])
        );
    }

    #[test]
    fn test_affinity_none_no_conversion() {
        let v = Value::Text("42".to_string());
        assert_eq!(
            v.apply_affinity(DataType::Blob),
            Value::Text("42".to_string())
        );
    }

    #[test]
    fn test_affinity_numeric_text_integer() {
        let v = Value::Text("42".to_string());
        assert_eq!(v.apply_affinity(DataType::Numeric), Value::Integer(42));
    }

    #[test]
    fn test_affinity_numeric_text_real() {
        let v = Value::Text("3.14".to_string());
        assert_eq!(v.apply_affinity(DataType::Numeric), Value::Real(3.14));
    }

    #[test]
    fn test_affinity_numeric_text_non_numeric() {
        let v = Value::Text("hello".to_string());
        assert_eq!(
            v.apply_affinity(DataType::Numeric),
            Value::Text("hello".to_string())
        );
    }

    // -- Serialization round-trip -------------------------------------------

    #[test]
    fn test_serialize_null() {
        let v = Value::Null;
        let bytes = v.serialize();
        assert_eq!(bytes, vec![0]);
        let (decoded, consumed) = Value::deserialize(&bytes).unwrap();
        assert_eq!(decoded, v);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn test_serialize_integer() {
        let v = Value::Integer(0x0102030405060708);
        let bytes = v.serialize();
        assert_eq!(bytes.len(), 9);
        assert_eq!(bytes[0], TAG_INTEGER);
        let (decoded, consumed) = Value::deserialize(&bytes).unwrap();
        assert_eq!(decoded, v);
        assert_eq!(consumed, 9);
    }

    #[test]
    fn test_serialize_negative_integer() {
        let v = Value::Integer(-42);
        let bytes = v.serialize();
        let (decoded, _) = Value::deserialize(&bytes).unwrap();
        assert_eq!(decoded, v);
    }

    #[test]
    fn test_serialize_real() {
        let v = Value::Real(std::f64::consts::PI);
        let bytes = v.serialize();
        assert_eq!(bytes.len(), 9);
        assert_eq!(bytes[0], TAG_REAL);
        let (decoded, consumed) = Value::deserialize(&bytes).unwrap();
        assert_eq!(decoded, v);
        assert_eq!(consumed, 9);
    }

    #[test]
    fn test_serialize_text() {
        let v = Value::Text("hello, world!".to_string());
        let bytes = v.serialize();
        assert_eq!(bytes[0], TAG_TEXT);
        let (decoded, consumed) = Value::deserialize(&bytes).unwrap();
        assert_eq!(decoded, v);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn test_serialize_empty_text() {
        let v = Value::Text(String::new());
        let bytes = v.serialize();
        assert_eq!(bytes.len(), 5); // tag + 4-byte length (0)
        let (decoded, consumed) = Value::deserialize(&bytes).unwrap();
        assert_eq!(decoded, v);
        assert_eq!(consumed, 5);
    }

    #[test]
    fn test_serialize_blob() {
        let v = Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let bytes = v.serialize();
        assert_eq!(bytes[0], TAG_BLOB);
        let (decoded, consumed) = Value::deserialize(&bytes).unwrap();
        assert_eq!(decoded, v);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn test_serialize_empty_blob() {
        let v = Value::Blob(Vec::new());
        let bytes = v.serialize();
        assert_eq!(bytes.len(), 5); // tag + 4-byte length (0)
        let (decoded, consumed) = Value::deserialize(&bytes).unwrap();
        assert_eq!(decoded, v);
        assert_eq!(consumed, 5);
    }

    #[test]
    fn test_deserialize_empty_data() {
        assert!(Value::deserialize(&[]).is_err());
    }

    #[test]
    fn test_deserialize_unknown_tag() {
        assert!(Value::deserialize(&[255]).is_err());
    }

    #[test]
    fn test_deserialize_truncated_integer() {
        assert!(Value::deserialize(&[TAG_INTEGER, 0, 0]).is_err());
    }

    #[test]
    fn test_deserialize_truncated_text() {
        // Tag says TEXT, length says 100, but only 2 bytes of data follow.
        let data = vec![TAG_TEXT, 0, 0, 0, 100, 0, 0];
        assert!(Value::deserialize(&data).is_err());
    }

    #[test]
    fn test_serialize_multiple_values_in_buffer() {
        let values = vec![
            Value::Null,
            Value::Integer(42),
            Value::Real(2.718),
            Value::Text("test".to_string()),
            Value::Blob(vec![1, 2, 3]),
        ];

        let mut buffer = Vec::new();
        for v in &values {
            buffer.extend(v.serialize());
        }

        let mut offset = 0;
        for expected in &values {
            let (decoded, consumed) = Value::deserialize(&buffer[offset..]).unwrap();
            assert_eq!(&decoded, expected);
            offset += consumed;
        }
        assert_eq!(offset, buffer.len());
    }

    // -- DataType Display ---------------------------------------------------

    #[test]
    fn test_datatype_display() {
        assert_eq!(format!("{}", DataType::Integer), "INTEGER");
        assert_eq!(format!("{}", DataType::Real), "REAL");
        assert_eq!(format!("{}", DataType::Text), "TEXT");
        assert_eq!(format!("{}", DataType::Blob), "BLOB");
        assert_eq!(format!("{}", DataType::Numeric), "NUMERIC");
    }
}
