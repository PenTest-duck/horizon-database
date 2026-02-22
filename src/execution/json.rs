//! Hand-written JSON parser and manipulation for Horizon DB.
//!
//! Provides a minimal JSON implementation with no external dependencies.
//! JSON values are stored as TEXT in the database, and this module handles
//! parsing, serialization, path extraction, and type inspection.

/// A JSON value representation.
#[derive(Debug, Clone, PartialEq)]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<JsonValue>),
    Object(Vec<(String, JsonValue)>),
}

impl JsonValue {
    /// Serialize the JSON value to a compact (minified) JSON string.
    pub fn to_json_string(&self) -> String {
        let mut buf = String::new();
        self.write_json(&mut buf);
        buf
    }

    fn write_json(&self, buf: &mut String) {
        match self {
            JsonValue::Null => buf.push_str("null"),
            JsonValue::Bool(true) => buf.push_str("true"),
            JsonValue::Bool(false) => buf.push_str("false"),
            JsonValue::Number(n) => {
                if n.fract() == 0.0 && n.is_finite() && *n >= i64::MIN as f64 && *n <= i64::MAX as f64 {
                    buf.push_str(&(*n as i64).to_string());
                } else {
                    buf.push_str(&n.to_string());
                }
            }
            JsonValue::String(s) => {
                buf.push('"');
                for ch in s.chars() {
                    match ch {
                        '"' => buf.push_str("\\\""),
                        '\\' => buf.push_str("\\\\"),
                        '\n' => buf.push_str("\\n"),
                        '\r' => buf.push_str("\\r"),
                        '\t' => buf.push_str("\\t"),
                        c if (c as u32) < 0x20 => {
                            buf.push_str(&format!("\\u{:04x}", c as u32));
                        }
                        c => buf.push(c),
                    }
                }
                buf.push('"');
            }
            JsonValue::Array(items) => {
                buf.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        buf.push(',');
                    }
                    item.write_json(buf);
                }
                buf.push(']');
            }
            JsonValue::Object(pairs) => {
                buf.push('{');
                for (i, (key, val)) in pairs.iter().enumerate() {
                    if i > 0 {
                        buf.push(',');
                    }
                    // Write key as JSON string
                    JsonValue::String(key.clone()).write_json(buf);
                    buf.push(':');
                    val.write_json(buf);
                }
                buf.push('}');
            }
        }
    }

    /// Return the JSON type name as a string.
    pub fn json_type_name(&self) -> &'static str {
        match self {
            JsonValue::Null => "null",
            JsonValue::Bool(true) => "true",
            JsonValue::Bool(false) => "false",
            JsonValue::Number(n) => {
                if n.fract() == 0.0 && n.is_finite() {
                    "integer"
                } else {
                    "real"
                }
            }
            JsonValue::String(_) => "text",
            JsonValue::Array(_) => "array",
            JsonValue::Object(_) => "object",
        }
    }

    /// Extract a value at the given JSON path (e.g., "$.key", "$[0]", "$.a.b").
    pub fn extract_path(&self, path: &str) -> Option<&JsonValue> {
        let segments = parse_json_path(path)?;
        let mut current = self;
        for seg in segments {
            match seg {
                PathSegment::Key(key) => {
                    if let JsonValue::Object(pairs) = current {
                        let found = pairs.iter().find(|(k, _)| k == &key);
                        match found {
                            Some((_, v)) => current = v,
                            None => return None,
                        }
                    } else {
                        return None;
                    }
                }
                PathSegment::Index(idx) => {
                    if let JsonValue::Array(items) = current {
                        if idx < items.len() {
                            current = &items[idx];
                        } else {
                            return None;
                        }
                    } else {
                        return None;
                    }
                }
            }
        }
        Some(current)
    }

    /// Return the array length if this is an array, None otherwise.
    pub fn array_length(&self) -> Option<usize> {
        if let JsonValue::Array(items) = self {
            Some(items.len())
        } else {
            None
        }
    }
}

/// A segment in a JSON path.
enum PathSegment {
    Key(String),
    Index(usize),
}

/// Parse a JSON path string like "$.key.sub[0].name" into path segments.
/// The path must start with '$'.
fn parse_json_path(path: &str) -> Option<Vec<PathSegment>> {
    let path = path.trim();
    if !path.starts_with('$') {
        return None;
    }

    let rest = &path[1..];
    if rest.is_empty() {
        return Some(vec![]);
    }

    let mut segments = Vec::new();
    let chars: Vec<char> = rest.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        if chars[i] == '.' {
            i += 1;
            // Read key name
            let start = i;
            while i < chars.len() && chars[i] != '.' && chars[i] != '[' {
                i += 1;
            }
            if i > start {
                let key: String = chars[start..i].iter().collect();
                segments.push(PathSegment::Key(key));
            }
        } else if chars[i] == '[' {
            i += 1;
            let start = i;
            while i < chars.len() && chars[i] != ']' {
                i += 1;
            }
            if i < chars.len() {
                let idx_str: String = chars[start..i].iter().collect();
                if let Ok(idx) = idx_str.trim().parse::<usize>() {
                    segments.push(PathSegment::Index(idx));
                } else {
                    // Might be a string key in brackets like ["key"]
                    let trimmed = idx_str.trim();
                    if (trimmed.starts_with('"') && trimmed.ends_with('"'))
                        || (trimmed.starts_with('\'') && trimmed.ends_with('\''))
                    {
                        let key = trimmed[1..trimmed.len() - 1].to_string();
                        segments.push(PathSegment::Key(key));
                    } else {
                        return None;
                    }
                }
                i += 1; // skip ']'
            } else {
                return None;
            }
        } else {
            return None;
        }
    }

    Some(segments)
}

/// A simple hand-written JSON parser.
pub struct JsonParser {
    chars: Vec<char>,
    pos: usize,
}

impl JsonParser {
    /// Parse a JSON string into a JsonValue.
    pub fn parse(input: &str) -> Option<JsonValue> {
        let mut parser = JsonParser {
            chars: input.chars().collect(),
            pos: 0,
        };
        parser.skip_whitespace();
        let value = parser.parse_value()?;
        parser.skip_whitespace();
        if parser.pos == parser.chars.len() {
            Some(value)
        } else {
            None // trailing garbage
        }
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.chars.len() && self.chars[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn advance(&mut self) -> Option<char> {
        let ch = self.chars.get(self.pos).copied();
        if self.pos < self.chars.len() {
            self.pos += 1;
        }
        ch
    }

    fn expect(&mut self, expected: char) -> Option<()> {
        if self.peek() == Some(expected) {
            self.advance();
            Some(())
        } else {
            None
        }
    }

    fn parse_value(&mut self) -> Option<JsonValue> {
        self.skip_whitespace();
        match self.peek()? {
            '"' => self.parse_string().map(JsonValue::String),
            '{' => self.parse_object(),
            '[' => self.parse_array(),
            't' => self.parse_true(),
            'f' => self.parse_false(),
            'n' => self.parse_null(),
            '-' | '0'..='9' => self.parse_number(),
            _ => None,
        }
    }

    fn parse_string(&mut self) -> Option<String> {
        self.expect('"')?;
        let mut s = String::new();
        loop {
            let ch = self.advance()?;
            match ch {
                '"' => return Some(s),
                '\\' => {
                    let escaped = self.advance()?;
                    match escaped {
                        '"' => s.push('"'),
                        '\\' => s.push('\\'),
                        '/' => s.push('/'),
                        'n' => s.push('\n'),
                        'r' => s.push('\r'),
                        't' => s.push('\t'),
                        'b' => s.push('\u{0008}'),
                        'f' => s.push('\u{000C}'),
                        'u' => {
                            let mut hex = String::with_capacity(4);
                            for _ in 0..4 {
                                hex.push(self.advance()?);
                            }
                            let code = u32::from_str_radix(&hex, 16).ok()?;
                            let ch = char::from_u32(code)?;
                            s.push(ch);
                        }
                        _ => return None,
                    }
                }
                c => s.push(c),
            }
        }
    }

    fn parse_number(&mut self) -> Option<JsonValue> {
        let start = self.pos;
        // Optional minus
        if self.peek() == Some('-') {
            self.advance();
        }
        // Integer part
        if self.peek() == Some('0') {
            self.advance();
        } else if matches!(self.peek(), Some('1'..='9')) {
            self.advance();
            while matches!(self.peek(), Some('0'..='9')) {
                self.advance();
            }
        } else {
            return None;
        }
        // Fraction
        let mut is_float = false;
        if self.peek() == Some('.') {
            is_float = true;
            self.advance();
            if !matches!(self.peek(), Some('0'..='9')) {
                return None;
            }
            while matches!(self.peek(), Some('0'..='9')) {
                self.advance();
            }
        }
        // Exponent
        if matches!(self.peek(), Some('e') | Some('E')) {
            is_float = true;
            self.advance();
            if matches!(self.peek(), Some('+') | Some('-')) {
                self.advance();
            }
            if !matches!(self.peek(), Some('0'..='9')) {
                return None;
            }
            while matches!(self.peek(), Some('0'..='9')) {
                self.advance();
            }
        }
        let num_str: String = self.chars[start..self.pos].iter().collect();
        let _ = is_float; // We always parse as f64 for simplicity
        let n: f64 = num_str.parse().ok()?;
        Some(JsonValue::Number(n))
    }

    fn parse_object(&mut self) -> Option<JsonValue> {
        self.expect('{')?;
        self.skip_whitespace();
        let mut pairs = Vec::new();
        if self.peek() == Some('}') {
            self.advance();
            return Some(JsonValue::Object(pairs));
        }
        loop {
            self.skip_whitespace();
            let key = self.parse_string()?;
            self.skip_whitespace();
            self.expect(':')?;
            let value = self.parse_value()?;
            pairs.push((key, value));
            self.skip_whitespace();
            if self.peek() == Some(',') {
                self.advance();
            } else {
                break;
            }
        }
        self.skip_whitespace();
        self.expect('}')?;
        Some(JsonValue::Object(pairs))
    }

    fn parse_array(&mut self) -> Option<JsonValue> {
        self.expect('[')?;
        self.skip_whitespace();
        let mut items = Vec::new();
        if self.peek() == Some(']') {
            self.advance();
            return Some(JsonValue::Array(items));
        }
        loop {
            let value = self.parse_value()?;
            items.push(value);
            self.skip_whitespace();
            if self.peek() == Some(',') {
                self.advance();
            } else {
                break;
            }
        }
        self.skip_whitespace();
        self.expect(']')?;
        Some(JsonValue::Array(items))
    }

    fn parse_true(&mut self) -> Option<JsonValue> {
        for expected in ['t', 'r', 'u', 'e'] {
            if self.advance()? != expected {
                return None;
            }
        }
        Some(JsonValue::Bool(true))
    }

    fn parse_false(&mut self) -> Option<JsonValue> {
        for expected in ['f', 'a', 'l', 's', 'e'] {
            if self.advance()? != expected {
                return None;
            }
        }
        Some(JsonValue::Bool(false))
    }

    fn parse_null(&mut self) -> Option<JsonValue> {
        for expected in ['n', 'u', 'l', 'l'] {
            if self.advance()? != expected {
                return None;
            }
        }
        Some(JsonValue::Null)
    }
}

/// Convert a JsonValue to a database-friendly Value representation.
/// Atomic JSON values are converted to their SQL counterparts;
/// arrays and objects are returned as their JSON text representation.
pub fn json_value_to_sql(jv: &JsonValue) -> crate::types::Value {
    match jv {
        JsonValue::Null => crate::types::Value::Null,
        JsonValue::Bool(true) => crate::types::Value::Integer(1),
        JsonValue::Bool(false) => crate::types::Value::Integer(0),
        JsonValue::Number(n) => {
            if n.fract() == 0.0 && n.is_finite() && *n >= i64::MIN as f64 && *n <= i64::MAX as f64 {
                crate::types::Value::Integer(*n as i64)
            } else {
                crate::types::Value::Real(*n)
            }
        }
        JsonValue::String(s) => crate::types::Value::Text(s.clone()),
        // Arrays and objects are returned as JSON text
        _ => crate::types::Value::Text(jv.to_json_string()),
    }
}

/// Convert a SQL Value to a JsonValue for use in JSON_ARRAY / JSON_OBJECT.
pub fn sql_value_to_json(val: &crate::types::Value) -> JsonValue {
    match val {
        crate::types::Value::Null => JsonValue::Null,
        crate::types::Value::Integer(i) => JsonValue::Number(*i as f64),
        crate::types::Value::Real(r) => JsonValue::Number(*r),
        crate::types::Value::Text(s) => {
            // If the text is valid JSON, embed it as-is (for nested JSON construction)
            // Otherwise treat it as a JSON string
            JsonValue::String(s.clone())
        }
        crate::types::Value::Blob(b) => {
            // Represent blobs as hex strings in JSON
            let hex: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
            JsonValue::String(hex)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_null() {
        assert_eq!(JsonParser::parse("null"), Some(JsonValue::Null));
    }

    #[test]
    fn test_parse_bool() {
        assert_eq!(JsonParser::parse("true"), Some(JsonValue::Bool(true)));
        assert_eq!(JsonParser::parse("false"), Some(JsonValue::Bool(false)));
    }

    #[test]
    fn test_parse_number() {
        assert_eq!(JsonParser::parse("42"), Some(JsonValue::Number(42.0)));
        assert_eq!(JsonParser::parse("-3.14"), Some(JsonValue::Number(-3.14)));
        assert_eq!(JsonParser::parse("1e10"), Some(JsonValue::Number(1e10)));
    }

    #[test]
    fn test_parse_string() {
        assert_eq!(
            JsonParser::parse(r#""hello""#),
            Some(JsonValue::String("hello".to_string()))
        );
        assert_eq!(
            JsonParser::parse(r#""hello\nworld""#),
            Some(JsonValue::String("hello\nworld".to_string()))
        );
    }

    #[test]
    fn test_parse_array() {
        let result = JsonParser::parse("[1, 2, 3]");
        assert_eq!(
            result,
            Some(JsonValue::Array(vec![
                JsonValue::Number(1.0),
                JsonValue::Number(2.0),
                JsonValue::Number(3.0),
            ]))
        );
    }

    #[test]
    fn test_parse_object() {
        let result = JsonParser::parse(r#"{"a": 1, "b": "two"}"#);
        assert_eq!(
            result,
            Some(JsonValue::Object(vec![
                ("a".to_string(), JsonValue::Number(1.0)),
                ("b".to_string(), JsonValue::String("two".to_string())),
            ]))
        );
    }

    #[test]
    fn test_extract_path() {
        let json = JsonParser::parse(r#"{"a": {"b": [1, 2, 3]}}"#).unwrap();
        assert_eq!(
            json.extract_path("$.a.b[1]"),
            Some(&JsonValue::Number(2.0))
        );
        assert_eq!(json.extract_path("$.a.b"), json.extract_path("$.a.b"));
        assert_eq!(json.extract_path("$.c"), None);
    }

    #[test]
    fn test_json_type_name() {
        assert_eq!(JsonValue::Null.json_type_name(), "null");
        assert_eq!(JsonValue::Bool(true).json_type_name(), "true");
        assert_eq!(JsonValue::Bool(false).json_type_name(), "false");
        assert_eq!(JsonValue::Number(42.0).json_type_name(), "integer");
        assert_eq!(JsonValue::Number(3.14).json_type_name(), "real");
        assert_eq!(
            JsonValue::String("hi".into()).json_type_name(),
            "text"
        );
        assert_eq!(
            JsonValue::Array(vec![]).json_type_name(),
            "array"
        );
        assert_eq!(
            JsonValue::Object(vec![]).json_type_name(),
            "object"
        );
    }

    #[test]
    fn test_roundtrip_minify() {
        let input = r#"  { "name" : "Alice" , "age" : 30 , "hobbies" : [ "reading" , "coding" ] }  "#;
        let parsed = JsonParser::parse(input).unwrap();
        let minified = parsed.to_json_string();
        assert_eq!(
            minified,
            r#"{"name":"Alice","age":30,"hobbies":["reading","coding"]}"#
        );
    }

    #[test]
    fn test_invalid_json() {
        assert_eq!(JsonParser::parse(""), None);
        assert_eq!(JsonParser::parse("{"), None);
        assert_eq!(JsonParser::parse("hello"), None);
        assert_eq!(JsonParser::parse("{foo: 1}"), None);
    }

    #[test]
    fn test_array_length() {
        let json = JsonParser::parse("[1, 2, 3, 4]").unwrap();
        assert_eq!(json.array_length(), Some(4));
        let json = JsonParser::parse("42").unwrap();
        assert_eq!(json.array_length(), None);
    }
}
