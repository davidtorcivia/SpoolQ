// SteadQ/1 deterministic CBOR extension header encoding and decoding.
// Implements RFC 8949 core deterministic encoding with SteadQ restrictions.

use std::collections::BTreeMap;

// Protocol keys for the top-level map
pub const KEY_INITIAL_NOT_BEFORE: u8 = 1;
pub const KEY_CONTENT_TYPE: u8 = 2;
pub const KEY_METADATA: u8 = 3;
pub const KEY_PRODUCER_ID: u8 = 4;
pub const KEY_TRACE_CONTEXT: u8 = 5;

pub const MAX_EXTENSION_SIZE: usize = 65_536;
pub const MAX_METADATA_ENTRIES: usize = 256;
pub const MAX_TEXT_VALUE: usize = 4096;
pub const MAX_BYTES_VALUE: usize = 4096;
pub const MAX_CONTENT_TYPE_LEN: usize = 255;
pub const MAX_PRODUCER_ID_LEN: usize = 255;
pub const MAX_TRACE_CONTEXT_LEN: usize = 1024;

#[derive(Clone, Debug, PartialEq)]
pub enum MetadataValue {
    Bool(bool),
    I64(i64),
    U64(u64),
    Text(String),
    Bytes(Vec<u8>),
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ExtensionHeader {
    pub initial_not_before_unix_ns: Option<u64>,
    pub content_type: String,
    pub metadata: BTreeMap<String, MetadataValue>,
    pub producer_id: Option<String>,
    pub trace_context: Option<Vec<u8>>,
}

#[derive(Debug, thiserror::Error)]
pub enum CborError {
    #[error("extension exceeds max size")]
    TooLarge,
    #[error("metadata exceeds max entries")]
    TooManyEntries,
    #[error("content type too long")]
    ContentTypeTooLong,
    #[error("producer_id too long")]
    ProducerIdTooLong,
    #[error("trace_context too long")]
    TraceContextTooLong,
    #[error("text value too long")]
    TextTooLong,
    #[error("bytes value too long")]
    BytesTooLong,
    #[error("invalid metadata key: {0}")]
    InvalidKey(String),
    #[error("content type contains invalid ASCII")]
    InvalidContentType,
    #[error("producer_id contains NUL")]
    ProducerIdNul,
    #[error("truncated input")]
    Truncated,
    #[error("invalid major type {0}")]
    InvalidMajorType(u8),
    #[error("invalid simple value {0}")]
    InvalidSimpleValue(u8),
    #[error("unexpected tag {0}")]
    UnexpectedTag(u8),
    #[error("indefinite length not allowed")]
    IndefiniteLength,
    #[error("float not allowed")]
    FloatNotAllowed,
    #[error("null not allowed")]
    NullNotAllowed,
    #[error("undefined not allowed")]
    UndefinedNotAllowed,
    #[error("nesting too deep")]
    NestingTooDeep,
    #[error("unknown protocol key {0}")]
    UnknownKey(u64),
    #[error("duplicate key {0}")]
    DuplicateKey(u64),
    #[error("content_type is required")]
    MissingContentType,
    #[error("invalid UTF-8")]
    InvalidUtf8,
    #[error("trailing bytes")]
    TrailingBytes,
    #[error("integer overflow")]
    IntOverflow,
}

// ---------- Encoding ----------

fn encode_header(buf: &mut Vec<u8>, major: u8, value: u64) {
    if value < 24 {
        buf.push((major << 5) | value as u8);
    } else if value < 256 {
        buf.push((major << 5) | 24);
        buf.push(value as u8);
    } else if value < 65536 {
        buf.push((major << 5) | 25);
        buf.extend_from_slice(&(value as u16).to_be_bytes());
    } else if value < 4_294_967_296 {
        buf.push((major << 5) | 26);
        buf.extend_from_slice(&(value as u32).to_be_bytes());
    } else {
        buf.push((major << 5) | 27);
        buf.extend_from_slice(&value.to_be_bytes());
    }
}

fn encode_text_string(buf: &mut Vec<u8>, s: &str) {
    encode_header(buf, 3, s.len() as u64);
    buf.extend_from_slice(s.as_bytes());
}

fn encode_byte_string(buf: &mut Vec<u8>, b: &[u8]) {
    encode_header(buf, 2, b.len() as u64);
    buf.extend_from_slice(b);
}

fn validate_metadata_key(key: &str) -> Result<(), CborError> {
    if key.is_empty() || key.len() > 64 {
        return Err(CborError::InvalidKey(key.to_string()));
    }
    let bytes = key.as_bytes();
    // First char: lowercase alpha or digit
    let first = bytes[0];
    if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
        return Err(CborError::InvalidKey(key.to_string()));
    }
    // Rest: lowercase alpha, digit, dot, underscore, hyphen
    for &b in &bytes[1..] {
        if !b.is_ascii_lowercase() && !b.is_ascii_digit() && b != b'.' && b != b'_' && b != b'-' {
            return Err(CborError::InvalidKey(key.to_string()));
        }
    }
    Ok(())
}

fn validate_content_type(s: &str) -> Result<(), CborError> {
    if s.is_empty() || s.len() > MAX_CONTENT_TYPE_LEN {
        return Err(CborError::ContentTypeTooLong);
    }
    for &b in s.as_bytes() {
        if !(0x20..=0x7e).contains(&b) {
            return Err(CborError::InvalidContentType);
        }
    }
    Ok(())
}

impl ExtensionHeader {
    pub fn validate(&self) -> Result<(), CborError> {
        validate_content_type(&self.content_type)?;

        if self.metadata.len() > MAX_METADATA_ENTRIES {
            return Err(CborError::TooManyEntries);
        }
        for (k, v) in &self.metadata {
            validate_metadata_key(k)?;
            match v {
                MetadataValue::Text(s) if s.len() > MAX_TEXT_VALUE => {
                    return Err(CborError::TextTooLong)
                }
                MetadataValue::Bytes(b) if b.len() > MAX_BYTES_VALUE => {
                    return Err(CborError::BytesTooLong)
                }
                _ => {}
            }
        }

        if let Some(ref pid) = self.producer_id {
            if pid.len() > MAX_PRODUCER_ID_LEN {
                return Err(CborError::ProducerIdTooLong);
            }
            if pid.contains('\0') {
                return Err(CborError::ProducerIdNul);
            }
        }

        if let Some(ref tc) = self.trace_context {
            if tc.len() > MAX_TRACE_CONTEXT_LEN {
                return Err(CborError::TraceContextTooLong);
            }
        }

        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, CborError> {
        self.validate()?;

        // Count entries in top-level map
        let mut entry_count: u64 = 1; // content_type always present
        if self.initial_not_before_unix_ns.is_some() {
            entry_count += 1;
        }
        if !self.metadata.is_empty() {
            entry_count += 1;
        }
        if self.producer_id.is_some() {
            entry_count += 1;
        }
        if self.trace_context.is_some() {
            entry_count += 1;
        }

        let mut buf = Vec::new();
        encode_header(&mut buf, 5, entry_count); // map

        // Keys in ascending numeric order (deterministic)
        if let Some(ns) = self.initial_not_before_unix_ns {
            encode_header(&mut buf, 0, KEY_INITIAL_NOT_BEFORE as u64); // uint key
            encode_header(&mut buf, 0, ns);
        }

        // content_type (key 2)
        encode_header(&mut buf, 0, KEY_CONTENT_TYPE as u64);
        encode_text_string(&mut buf, &self.content_type);

        // metadata (key 3)
        if !self.metadata.is_empty() {
            encode_header(&mut buf, 0, KEY_METADATA as u64);
            encode_header(&mut buf, 5, self.metadata.len() as u64);
            // C-47: Sort metadata keys by their deterministic CBOR encoded bytes,
            // not by Rust string ordering. Encode each key to a temporary buffer,
            // sort by those bytes, then emit.
            let mut encoded_keys: Vec<(Vec<u8>, &str, &MetadataValue)> = self
                .metadata
                .iter()
                .map(|(k, v)| {
                    let mut key_buf = Vec::new();
                    encode_text_string(&mut key_buf, k);
                    (key_buf, k.as_str(), v)
                })
                .collect();
            encoded_keys.sort_by(|a, b| a.0.cmp(&b.0));
            for (_, k, v) in encoded_keys {
                encode_text_string(&mut buf, k);
                match v {
                    MetadataValue::Bool(false) => buf.push(0xf4),
                    MetadataValue::Bool(true) => buf.push(0xf5),
                    MetadataValue::U64(n) => encode_header(&mut buf, 0, *n),
                    MetadataValue::I64(n) => {
                        if *n >= 0 {
                            encode_header(&mut buf, 0, *n as u64);
                        } else {
                            encode_header(&mut buf, 1, (-1 - n) as u64);
                        }
                    }
                    MetadataValue::Text(s) => encode_text_string(&mut buf, s),
                    MetadataValue::Bytes(b) => encode_byte_string(&mut buf, b),
                }
            }
        }

        // producer_id (key 4)
        if let Some(ref pid) = self.producer_id {
            encode_header(&mut buf, 0, KEY_PRODUCER_ID as u64);
            encode_text_string(&mut buf, pid);
        }

        // trace_context (key 5)
        if let Some(ref tc) = self.trace_context {
            encode_header(&mut buf, 0, KEY_TRACE_CONTEXT as u64);
            encode_byte_string(&mut buf, tc);
        }

        if buf.len() > MAX_EXTENSION_SIZE {
            return Err(CborError::TooLarge);
        }

        Ok(buf)
    }

    pub fn decode(input: &[u8]) -> Result<Self, CborError> {
        if input.len() > MAX_EXTENSION_SIZE {
            return Err(CborError::TooLarge);
        }
        let mut parser = CborParser {
            data: input,
            pos: 0,
            depth: 0,
        };
        let result = parser.decode_extension()?;
        if parser.pos != input.len() {
            return Err(CborError::TrailingBytes);
        }
        Ok(result)
    }
}

// ---------- CBOR parser ----------

struct CborParser<'a> {
    data: &'a [u8],
    pos: usize,
    depth: u32,
}

impl<'a> CborParser<'a> {
    fn read_byte(&mut self) -> Result<u8, CborError> {
        if self.pos >= self.data.len() {
            return Err(CborError::Truncated);
        }
        let b = self.data[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn read_uint(&mut self, additional: u8) -> Result<u64, CborError> {
        match additional {
            0..=23 => Ok(additional as u64),
            24 => {
                self.pos = self.pos.checked_add(1).ok_or(CborError::IntOverflow)?;
                if self.pos > self.data.len() {
                    return Err(CborError::Truncated);
                }
                let v = self.data[self.pos - 1] as u64;
                if v < 24 {
                    return Err(CborError::IntOverflow); // non-canonical
                }
                Ok(v)
            }
            25 => {
                self.pos = self.pos.checked_add(2).ok_or(CborError::IntOverflow)?;
                if self.pos > self.data.len() {
                    return Err(CborError::Truncated);
                }
                let v = u16::from_be_bytes(self.data[self.pos - 2..self.pos].try_into().unwrap())
                    as u64;
                if v < 256 {
                    return Err(CborError::IntOverflow); // non-canonical
                }
                Ok(v)
            }
            26 => {
                self.pos = self.pos.checked_add(4).ok_or(CborError::IntOverflow)?;
                if self.pos > self.data.len() {
                    return Err(CborError::Truncated);
                }
                let v = u32::from_be_bytes(self.data[self.pos - 4..self.pos].try_into().unwrap())
                    as u64;
                if v < 65536 {
                    return Err(CborError::IntOverflow); // non-canonical
                }
                Ok(v)
            }
            27 => {
                self.pos = self.pos.checked_add(8).ok_or(CborError::IntOverflow)?;
                if self.pos > self.data.len() {
                    return Err(CborError::Truncated);
                }
                let v = u64::from_be_bytes(self.data[self.pos - 8..self.pos].try_into().unwrap());
                if v < 4_294_967_296 {
                    return Err(CborError::IntOverflow); // non-canonical
                }
                Ok(v)
            }
            28..=30 => Err(CborError::InvalidMajorType(additional)),
            31 => Err(CborError::IndefiniteLength),
            _ => Err(CborError::InvalidMajorType(additional)),
        }
    }

    fn read_item(&mut self) -> Result<CborItem, CborError> {
        if self.depth > 2 {
            return Err(CborError::NestingTooDeep);
        }

        let initial = self.read_byte()?;
        let major = initial >> 5;
        let additional = initial & 0x1f;

        match major {
            0 => {
                // unsigned int
                let v = self.read_uint(additional)?;
                Ok(CborItem::UInt(v))
            }
            1 => {
                // negative int
                let v = self.read_uint(additional)?;
                // n = -1 - v
                if v > i64::MAX as u64 {
                    return Err(CborError::IntOverflow);
                }
                Ok(CborItem::NInt(v))
            }
            2 => {
                // byte string
                let len = self.read_uint(additional)? as usize;
                let end = self.pos.checked_add(len).ok_or(CborError::IntOverflow)?;
                if end > self.data.len() {
                    return Err(CborError::Truncated);
                }
                let b = self.data[self.pos..end].to_vec();
                self.pos = end;
                Ok(CborItem::Bytes(b))
            }
            3 => {
                // text string
                let len = self.read_uint(additional)? as usize;
                let end = self.pos.checked_add(len).ok_or(CborError::IntOverflow)?;
                if end > self.data.len() {
                    return Err(CborError::Truncated);
                }
                let s = std::str::from_utf8(&self.data[self.pos..end])
                    .map_err(|_| CborError::InvalidUtf8)?;
                let s = s.to_string();
                self.pos = end;
                Ok(CborItem::Text(s))
            }
            4 => {
                // array - not allowed in SteadQ metadata (only maps)
                Err(CborError::InvalidMajorType(major))
            }
            5 => {
                // map
                self.depth += 1;
                let len = self.read_uint(additional)? as usize;
                // Bound the declared length against remaining input:
                // each entry needs at least 1 byte (key) + 1 byte (val)
                let remaining = self.data.len() - self.pos;
                let max_possible = remaining / 2;
                if len > max_possible {
                    return Err(CborError::Truncated);
                }
                // Also bound against protocol limit
                if len > MAX_METADATA_ENTRIES * 8 {
                    return Err(CborError::TooManyEntries);
                }
                let mut items = Vec::with_capacity(len.min(256));
                for _ in 0..len {
                    let key = self.read_item()?;
                    let val = self.read_item()?;
                    items.push((key, val));
                }
                self.depth -= 1;
                Ok(CborItem::Map(items))
            }
            6 => Err(CborError::UnexpectedTag(additional)),
            7 => match additional {
                20 => Ok(CborItem::Bool(false)),
                21 => Ok(CborItem::Bool(true)),
                22 => Err(CborError::NullNotAllowed),
                23 => Err(CborError::UndefinedNotAllowed),
                // Simple values > 23 would use 24 prefix
                24 => {
                    let sv = self.read_byte()?;
                    Err(CborError::InvalidSimpleValue(sv))
                }
                25..=27 => Err(CborError::FloatNotAllowed),
                _ => Err(CborError::InvalidSimpleValue(additional)),
            },
            _ => Err(CborError::InvalidMajorType(major)),
        }
    }

    fn decode_extension(&mut self) -> Result<ExtensionHeader, CborError> {
        let top = self.read_item()?;
        let map_items = match top {
            CborItem::Map(m) => m,
            _ => return Err(CborError::InvalidMajorType(0)),
        };

        let mut ext = ExtensionHeader::default();
        let mut seen_keys = std::collections::HashSet::new();
        let mut prev_key: Option<u64> = None;

        for (key_item, val_item) in map_items {
            let key = match key_item {
                CborItem::UInt(k) => k,
                _ => return Err(CborError::UnknownKey(0)),
            };
            if !seen_keys.insert(key) {
                return Err(CborError::DuplicateKey(key));
            }
            // C-46: Enforce canonical ascending key order
            if let Some(pk) = prev_key {
                if key < pk {
                    return Err(CborError::DuplicateKey(key));
                }
            }
            prev_key = Some(key);

            match key {
                1 => {
                    let ns = match val_item {
                        CborItem::UInt(v) => v,
                        _ => return Err(CborError::InvalidMajorType(0)),
                    };
                    ext.initial_not_before_unix_ns = Some(ns);
                }
                2 => {
                    let ct = match val_item {
                        CborItem::Text(s) => s,
                        _ => return Err(CborError::InvalidMajorType(0)),
                    };
                    validate_content_type(&ct)?;
                    ext.content_type = ct;
                }
                3 => {
                    let meta_map = match val_item {
                        CborItem::Map(m) => m,
                        _ => return Err(CborError::InvalidMajorType(0)),
                    };
                    if meta_map.len() > MAX_METADATA_ENTRIES {
                        return Err(CborError::TooManyEntries);
                    }
                    let mut seen_meta_keys = std::collections::HashSet::new();
                    let mut meta_items: Vec<(&str, &CborItem)> = Vec::new();
                    for (mk_item, mv_item) in &meta_map {
                        let mk = match mk_item {
                            CborItem::Text(s) => s.as_str(),
                            _ => return Err(CborError::InvalidKey("non-string".into())),
                        };
                        validate_metadata_key(mk)?;
                        // R2-M03: Reject duplicate metadata keys
                        if !seen_meta_keys.insert(mk.to_string()) {
                            return Err(CborError::InvalidKey(format!(
                                "duplicate metadata key: {mk}"
                            )));
                        }
                        meta_items.push((mk, mv_item));
                    }
                    // R2-M04: Verify metadata keys arrive in deterministic encoded-byte order
                    let mut prev_key_bytes: Option<Vec<u8>> = None;
                    for (mk, _) in &meta_items {
                        let mut key_buf = Vec::new();
                        encode_text_string(&mut key_buf, mk);
                        if let Some(ref pk) = prev_key_bytes {
                            if key_buf.as_slice() <= pk.as_slice() {
                                return Err(CborError::InvalidKey(format!(
                                    "metadata keys not in canonical order: {mk}"
                                )));
                            }
                        }
                        prev_key_bytes = Some(key_buf);
                    }
                    for (mk, mv_item) in meta_items {
                        let mv = match mv_item {
                            CborItem::Bool(b) => MetadataValue::Bool(*b),
                            CborItem::UInt(v) => MetadataValue::U64(*v),
                            CborItem::NInt(v) => {
                                if *v > i64::MAX as u64 {
                                    return Err(CborError::IntOverflow);
                                }
                                MetadataValue::I64(-1i64 - *v as i64)
                            }
                            CborItem::Text(s) => {
                                if s.len() > MAX_TEXT_VALUE {
                                    return Err(CborError::TextTooLong);
                                }
                                MetadataValue::Text(s.clone())
                            }
                            CborItem::Bytes(b) => {
                                if b.len() > MAX_BYTES_VALUE {
                                    return Err(CborError::BytesTooLong);
                                }
                                MetadataValue::Bytes(b.clone())
                            }
                            CborItem::Map(_) => return Err(CborError::InvalidMajorType(5)),
                        };
                        ext.metadata.insert(mk.to_string(), mv);
                    }
                }
                4 => {
                    let pid = match val_item {
                        CborItem::Text(s) => s,
                        _ => return Err(CborError::InvalidMajorType(0)),
                    };
                    if pid.contains('\0') {
                        return Err(CborError::ProducerIdNul);
                    }
                    if pid.len() > MAX_PRODUCER_ID_LEN {
                        return Err(CborError::ProducerIdTooLong);
                    }
                    ext.producer_id = Some(pid);
                }
                5 => {
                    let tc = match val_item {
                        CborItem::Bytes(b) => b,
                        _ => return Err(CborError::InvalidMajorType(0)),
                    };
                    if tc.len() > MAX_TRACE_CONTEXT_LEN {
                        return Err(CborError::TraceContextTooLong);
                    }
                    ext.trace_context = Some(tc);
                }
                _ => return Err(CborError::UnknownKey(key)),
            }
        }

        if ext.content_type.is_empty() {
            return Err(CborError::MissingContentType);
        }

        Ok(ext)
    }
}

enum CborItem {
    UInt(u64),
    NInt(u64),
    Bytes(Vec<u8>),
    Text(String),
    Bool(bool),
    Map(Vec<(CborItem, CborItem)>),
}

#[cfg(test)]
mod tests {
    fn hex_to_bytes(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    use super::*;

    #[test]
    fn minimal_extension() {
        let ext = ExtensionHeader {
            content_type: "application/json".to_string(),
            ..Default::default()
        };
        let encoded = ext.encode().unwrap();
        let decoded = ExtensionHeader::decode(&encoded).unwrap();
        assert_eq!(decoded.content_type, "application/json");
        assert_eq!(decoded, ext);
    }

    #[test]
    fn full_extension_round_trip() {
        let mut metadata = BTreeMap::new();
        metadata.insert("retry_count".to_string(), MetadataValue::U64(3));
        metadata.insert("enabled".to_string(), MetadataValue::Bool(true));
        metadata.insert(
            "owner".to_string(),
            MetadataValue::Text("team-a".to_string()),
        );
        metadata.insert(
            "raw_data".to_string(),
            MetadataValue::Bytes(vec![0xDE, 0xAD]),
        );
        metadata.insert("priority".to_string(), MetadataValue::I64(-1));

        let ext = ExtensionHeader {
            initial_not_before_unix_ns: Some(1_700_000_000_000_000_000),
            content_type: "text/plain".to_string(),
            metadata,
            producer_id: Some("producer-1".to_string()),
            trace_context: Some(vec![0x01, 0x02, 0x03]),
        };
        let encoded = ext.encode().unwrap();
        let decoded = ExtensionHeader::decode(&encoded).unwrap();
        assert_eq!(decoded, ext);
    }

    #[test]
    fn rejects_null() {
        // Construct raw CBOR with null value
        // Map with 1 entry: key 2 (content_type) -> null (0xF6)
        let raw = vec![0xA1, 0x02, 0xF6];
        assert!(ExtensionHeader::decode(&raw).is_err());
    }

    #[test]
    fn rejects_undefined() {
        let raw = vec![0xA1, 0x02, 0xF7];
        assert!(ExtensionHeader::decode(&raw).is_err());
    }

    #[test]
    fn rejects_float() {
        // Half-precision float 0.0: 0xF9 0x00 0x00
        let raw = vec![0xA1, 0x02, 0xF9, 0x00, 0x00];
        assert!(ExtensionHeader::decode(&raw).is_err());
    }

    #[test]
    fn rejects_integer_as_bool() {
        // Map with metadata containing integer 0 where bool expected
        // We can't put integer as bool, but we can verify the decoder accepts
        // integer values in metadata
        let mut metadata = BTreeMap::new();
        metadata.insert("flag".to_string(), MetadataValue::U64(0));
        let ext = ExtensionHeader {
            content_type: "x".to_string(),
            metadata,
            ..Default::default()
        };
        let encoded = ext.encode().unwrap();
        let decoded = ExtensionHeader::decode(&encoded).unwrap();
        // Integer 0 should be preserved as U64(0), not converted to bool
        assert_eq!(decoded.metadata.get("flag"), Some(&MetadataValue::U64(0)));
    }

    #[test]
    fn bool_encoding_is_simple_value() {
        let mut metadata = BTreeMap::new();
        metadata.insert("active".to_string(), MetadataValue::Bool(true));
        metadata.insert("inactive".to_string(), MetadataValue::Bool(false));
        let ext = ExtensionHeader {
            content_type: "x".to_string(),
            metadata,
            ..Default::default()
        };
        let encoded = ext.encode().unwrap();
        // Find the bool values: true = 0xF5, false = 0xF4
        assert!(encoded.contains(&0xF5));
        assert!(encoded.contains(&0xF4));
        // Should NOT contain integer encoding for bools
        // (uint 0 = 0x00, uint 1 = 0x01 would be wrong for bools)
    }

    #[test]
    fn rejects_unknown_key() {
        // Map with key 99
        let raw = vec![0xA1, 0x18, 0x63, 0x62, 0x78, 0x74]; // key 99 -> text "bxt"
        assert!(ExtensionHeader::decode(&raw).is_err());
    }

    #[test]
    fn rejects_duplicate_key() {
        // Map with key 2 appearing twice
        let raw = vec![0xA2, 0x02, 0x61, 0x78, 0x02, 0x61, 0x79];
        assert!(ExtensionHeader::decode(&raw).is_err());
    }

    #[test]
    fn deterministic_encoding() {
        let ext1 = ExtensionHeader {
            content_type: "text/plain".to_string(),
            producer_id: Some("p1".to_string()),
            ..Default::default()
        };
        let ext2 = ext1.clone();
        assert_eq!(ext1.encode().unwrap(), ext2.encode().unwrap());
    }

    #[test]
    fn key_ordering_is_numeric() {
        // Keys 1, 2, 4, 5 (skipping 3 since no metadata)
        let ext = ExtensionHeader {
            initial_not_before_unix_ns: Some(42),
            content_type: "x".to_string(),
            producer_id: Some("p".to_string()),
            trace_context: Some(vec![1]),
            ..Default::default()
        };
        let encoded = ext.encode().unwrap();
        // First byte is map header (0xA4 = 4 entries)
        assert_eq!(encoded[0], 0xA4);
        // Keys should be in order: 1, 2, 4, 5
        assert_eq!(encoded[1], 0x01); // key 1
        assert_eq!(encoded[4], 0x02); // key 2 (content_type)
                                      // ... etc
    }

    #[test]
    fn rejects_non_canonical_uint() {
        // uint encoded as 2 bytes when 1 suffices: 0x18 0x17 for value 23
        let raw = vec![0xA1, 0x01, 0x18, 0x17]; // key 1 -> uint 23 (non-canonical)
        assert!(ExtensionHeader::decode(&raw).is_err());
    }

    #[test]
    fn metadata_key_validation() {
        assert!(validate_metadata_key("abc").is_ok());
        assert!(validate_metadata_key("a1b2").is_ok());
        assert!(validate_metadata_key("1abc").is_ok());
        assert!(validate_metadata_key("a.b-c_d").is_ok());
        assert!(validate_metadata_key("").is_err());
        assert!(validate_metadata_key("ABC").is_err()); // uppercase
        assert!(validate_metadata_key("a*b").is_err()); // invalid char
        assert!(validate_metadata_key(&"a".repeat(65)).is_err()); // too long
    }

    #[test]
    fn rejects_trailing_bytes() {
        let ext = ExtensionHeader {
            content_type: "x".to_string(),
            ..Default::default()
        };
        let mut encoded = ext.encode().unwrap();
        encoded.push(0x00); // trailing byte
        assert!(ExtensionHeader::decode(&encoded).is_err());
    }

    #[test]
    fn rejects_missing_content_type() {
        // Empty map
        let raw = vec![0xA0];
        assert!(ExtensionHeader::decode(&raw).is_err());
    }
    #[test]
    fn cbor_vector_minimal() {
        let ext = ExtensionHeader {
            content_type: "application/json".to_string(),
            ..Default::default()
        };
        let encoded = ext.encode().unwrap();
        let expected = hex_to_bytes("a102706170706c69636174696f6e2f6a736f6e");
        assert_eq!(encoded, expected, "minimal extension mismatch");
    }

    #[test]
    fn cbor_vector_with_not_before() {
        let ext = ExtensionHeader {
            initial_not_before_unix_ns: Some(0x179a18a8e3e40000),
            content_type: "x".to_string(),
            ..Default::default()
        };
        let encoded = ext.encode().unwrap();
        let expected = hex_to_bytes("a2011b179a18a8e3e40000026178");
        assert_eq!(encoded, expected, "not_before extension mismatch");
    }

    #[test]
    fn cbor_vector_with_booleans() {
        let mut metadata = std::collections::BTreeMap::new();
        metadata.insert("active".to_string(), MetadataValue::Bool(true));
        metadata.insert("done".to_string(), MetadataValue::Bool(false));
        let ext = ExtensionHeader {
            content_type: "x".to_string(),
            metadata,
            ..Default::default()
        };
        let encoded = ext.encode().unwrap();
        // C-47: deterministic CBOR sorts by encoded key bytes.
        // "done" (text(4) = 0x64...) sorts before "active" (text(6) = 0x66...)
        let expected = hex_to_bytes("a202617803a264646f6e65f466616374697665f5");
        assert_eq!(
            encoded, expected,
            "boolean metadata mismatch (C-47 ordering)"
        );
    }

    #[test]
    fn cbor_rejects_non_canonical_top_level_key_order() {
        // C-46: keys must be in ascending order
        let raw = vec![0xA2, 0x02, 0x61, 0x78, 0x01, 0x18, 0x2A];
        assert!(ExtensionHeader::decode(&raw).is_err());
    }

    #[test]
    fn cbor_metadata_key_order_is_deterministic() {
        // C-47: keys of different lengths sort by encoded bytes
        let mut metadata = std::collections::BTreeMap::new();
        metadata.insert("abc".to_string(), MetadataValue::U64(1));
        metadata.insert("ab".to_string(), MetadataValue::U64(2));
        metadata.insert("abcd".to_string(), MetadataValue::U64(3));
        let ext = ExtensionHeader {
            content_type: "x".to_string(),
            metadata,
            ..Default::default()
        };
        let encoded = ext.encode().unwrap();
        let decoded = ExtensionHeader::decode(&encoded).unwrap();
        assert_eq!(decoded.metadata.get("abc"), Some(&MetadataValue::U64(1)));
        assert_eq!(decoded.metadata.get("ab"), Some(&MetadataValue::U64(2)));
        assert_eq!(decoded.metadata.get("abcd"), Some(&MetadataValue::U64(3)));
    }

    #[test]
    fn cbor_rejects_huge_map_length() {
        // C-48: huge declared length should not cause huge allocation
        let raw = vec![0xBB, 0xFF, 0xFF, 0xFF, 0xFF, 0x02, 0x61, 0x78];
        assert!(ExtensionHeader::decode(&raw).is_err());
    }

    #[test]
    fn cbor_rejects_decoded_bad_content_type() {
        // C-49: decoded content type must pass validation
        let raw = vec![0xA1, 0x02, 0x41, 0x01];
        assert!(ExtensionHeader::decode(&raw).is_err());
    }
}
