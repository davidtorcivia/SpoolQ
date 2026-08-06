// SpoolQ/1 canonical filename parsing, formatting, name tags, and shard math.

use sha2::{Digest, Sha256};

// ---------- Hex helpers ----------

pub fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

pub fn hex_decode_16(s: &str) -> Option<[u8; 16]> {
    if s.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        out[i] = u8::from_str_radix(std::str::from_utf8(chunk).ok()?, 16).ok()?;
    }
    Some(out)
}

pub fn hex_decode_u64(s: &str) -> Option<u64> {
    let bytes = hex_decode_bytes(s)?;
    if bytes.len() > 8 {
        return None;
    }
    let mut buf = [0u8; 8];
    buf[8 - bytes.len()..].copy_from_slice(&bytes);
    Some(u64::from_be_bytes(buf))
}

pub fn hex_decode_u32(s: &str) -> Option<u32> {
    if s.len() != 8 {
        return None;
    }
    let mut out = [0u8; 4];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        out[i] = u8::from_str_radix(std::str::from_utf8(chunk).ok()?, 16).ok()?;
    }
    Some(u32::from_be_bytes(out))
}

pub fn hex_decode_u16(s: &str) -> Option<u16> {
    if s.len() != 4 {
        return None;
    }
    let mut out = [0u8; 2];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        out[i] = u8::from_str_radix(std::str::from_utf8(chunk).ok()?, 16).ok()?;
    }
    Some(u16::from_be_bytes(out))
}

fn hex_decode_bytes(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    for chunk in s.as_bytes().chunks(2) {
        out.push(u8::from_str_radix(std::str::from_utf8(chunk).ok()?, 16).ok()?);
    }
    Some(out)
}

fn hex_job_id(id: &[u8; 16]) -> String {
    hex_encode(id)
}

fn hex_u64(v: u64) -> String {
    format!("{:016x}", v)
}

fn hex_u32(v: u32) -> String {
    format!("{:08x}", v)
}

fn hex_u16(v: u16) -> String {
    format!("{:04x}", v)
}

// ---------- States ----------

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum State {
    Ready,
    Leased,
    Delayed,
    Receipt,
    Dead,
    Quarantine,
}

impl State {
    pub fn dir_name(&self) -> &'static str {
        match self {
            State::Ready => "ready",
            State::Leased => "leased",
            State::Delayed => "delayed",
            State::Receipt => "receipts",
            State::Dead => "dead",
            State::Quarantine => "quarantine",
        }
    }
}

// ---------- Name tag ----------

/// Compute the 64-bit name integrity tag.
/// tag = first 8 bytes of SHA256("SpoolQ-1-name\0" || queue_id || ascii_context)
pub fn compute_name_tag(queue_id: &[u8; 16], canonical_context: &str) -> [u8; 8] {
    let mut hasher = Sha256::new();
    hasher.update(b"SpoolQ-1-name\0");
    hasher.update(queue_id);
    hasher.update(canonical_context.as_bytes());
    let result = hasher.finalize();
    let mut out = [0u8; 8];
    out.copy_from_slice(&result[..8]);
    out
}

pub fn name_tag_hex(tag: &[u8; 8]) -> String {
    hex_encode(tag)
}

// ---------- Shard derivation ----------

/// shard_hash = SHA256("SpoolQ-1-shard\0" || queue_id || job_id)
/// shard = low_log2(shard_count)_bits(shard_hash)
pub fn compute_shard(queue_id: &[u8; 16], job_id: &[u8; 16], shard_count: u32) -> u32 {
    let mut hasher = Sha256::new();
    hasher.update(b"SpoolQ-1-shard\0");
    hasher.update(queue_id);
    hasher.update(job_id);
    let result = hasher.finalize();

    let k = shard_count.trailing_zeros();
    let val = u32::from_be_bytes(result[..4].try_into().unwrap());
    val & ((1u32 << k) - 1)
}

pub fn shard_hex(shard: u32) -> String {
    format!("{:04x}", shard)
}

pub fn shard_from_hex(s: &str) -> Option<u32> {
    let val = u16::from_str_radix(s, 16).ok()?;
    Some(val as u32)
}

// ---------- Shard scan permutation ----------

/// scan_hash = SHA256("SpoolQ-1-scan\0" || queue_id || boot_id || worker_nonce || u64be(scan_round))
/// start = u64(h[0:8]) & (S - 1)
/// stride = (u64(h[8:16]) | 1) & (S - 1), min 1
pub fn shard_scan_params(
    queue_id: &[u8; 16],
    boot_id: &[u8; 16],
    worker_nonce: &[u8; 16],
    scan_round: u64,
    shard_count: u32,
) -> (u32, u32) {
    let mut hasher = Sha256::new();
    hasher.update(b"SpoolQ-1-scan\0");
    hasher.update(queue_id);
    hasher.update(boot_id);
    hasher.update(worker_nonce);
    hasher.update(scan_round.to_be_bytes());
    let result = hasher.finalize();

    let mask = shard_count - 1;
    let start = u64::from_be_bytes(result[..8].try_into().unwrap()) & mask as u64;
    let mut stride = (u64::from_be_bytes(result[8..16].try_into().unwrap()) | 1) & mask as u64;
    if stride == 0 {
        stride = 1;
    }
    (start as u32, stride as u32)
}

/// Get the i-th shard in the permutation.
pub fn shard_at(start: u32, stride: u32, i: u32, shard_count: u32) -> u32 {
    let mask = shard_count - 1;
    (start.wrapping_add(stride.wrapping_mul(i))) & mask
}

// ---------- Bucket names ----------

pub fn bucket_hex(bucket: u64) -> String {
    format!("{:016x}", bucket)
}

pub fn bucket_from_hex(s: &str) -> Option<u64> {
    if s.len() != 16 {
        return None;
    }
    hex_decode_u64(s)
}

// ---------- Boot ID ----------

pub fn boot_id_string(raw: &str) -> String {
    raw.trim().to_string()
}

pub fn boot_id_bytes(s: &str) -> Option<[u8; 16]> {
    // canonical 36-char lowercase uuid: 8-4-4-4-12 hex digits
    if s.len() != 36 {
        return None;
    }
    let bytes = s.as_bytes();
    // Check hyphens at positions 8, 13, 18, 23
    for &pos in &[8, 13, 18, 23] {
        if bytes[pos] != b'-' {
            return None;
        }
    }
    // Verify lowercase hex at all other positions
    for (i, &b) in bytes.iter().enumerate() {
        if matches!(i, 8 | 13 | 18 | 23) {
            continue;
        }
        if !b.is_ascii_digit() && !(b'a'..=b'f').contains(&b) {
            return None;
        }
    }
    // Collect hex digits
    let hex_str: String = bytes
        .iter()
        .enumerate()
        .filter(|(i, _)| !matches!(i, 8 | 13 | 18 | 23))
        .map(|(_, &b)| b as char)
        .collect();
    hex_decode_16(&hex_str)
}

// ---------- Canonical filenames ----------

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommonFields {
    pub job_id: [u8; 16],
    pub generation: u64,
    pub attempt: u32,
    pub maximum_attempts: u32,
}

impl CommonFields {
    fn base_name(&self) -> String {
        format!(
            "{}.g{}.a{}.m{}",
            hex_job_id(&self.job_id),
            hex_u64(self.generation),
            hex_u32(self.attempt),
            hex_u32(self.maximum_attempts),
        )
    }
}

// Ready: <job-id>.g<gen>.a<att>.m<max>.k<tag>.sqj
pub fn ready_filename(fields: &CommonFields, tag: &[u8; 8]) -> String {
    format!("{}.k{}.sqj", fields.base_name(), name_tag_hex(tag))
}

// Delayed: <job-id>.g<gen>.a<att>.m<max>.d<ns>.k<tag>.sqj
pub fn delayed_filename(fields: &CommonFields, not_before_ns: u64, tag: &[u8; 8]) -> String {
    format!(
        "{}.d{}.k{}.sqj",
        fields.base_name(),
        hex_u64(not_before_ns),
        name_tag_hex(tag)
    )
}

// Dead: <job-id>.g<gen>.a<att>.m<max>.x<reason>.k<tag>.sqj
pub fn dead_filename(fields: &CommonFields, reason: u16, tag: &[u8; 8]) -> String {
    format!(
        "{}.x{}.k{}.sqj",
        fields.base_name(),
        hex_u16(reason),
        name_tag_hex(tag)
    )
}

// Receipt: <job-id>.g<gen>.a<att>.m<max>.t<token>.k<tag>.rct
pub fn receipt_filename(fields: &CommonFields, token: &[u8; 16], tag: &[u8; 8]) -> String {
    format!(
        "{}.t{}.k{}.rct",
        fields.base_name(),
        hex_encode(token),
        name_tag_hex(tag)
    )
}

// Leased: <job-id>.g<gen>.a<att>.m<max>.b<boottime_dl>.w<wall_dl>.t<token>.k<tag>.sqj
pub fn leased_filename(
    fields: &CommonFields,
    boottime_deadline_ns: u64,
    wall_deadline_ns: u64,
    token: &[u8; 16],
    tag: &[u8; 8],
) -> String {
    format!(
        "{}.b{}.w{}.t{}.k{}.sqj",
        fields.base_name(),
        hex_u64(boottime_deadline_ns),
        hex_u64(wall_deadline_ns),
        hex_encode(token),
        name_tag_hex(tag)
    )
}

// Temp: <created-boottime-ns-hex>.<random-128-bit-hex>.tmp
pub fn temp_filename(created_boottime_ns: u64, random: &[u8; 16]) -> String {
    format!(
        "{}.{}.tmp",
        hex_u64(created_boottime_ns),
        hex_encode(random)
    )
}

// Quarantine: q<quarantine-id>.x<reason>.raw
pub fn quarantine_filename(quarantine_id: &[u8; 16], reason: u16) -> String {
    format!("q{}.x{}.raw", hex_encode(quarantine_id), hex_u16(reason))
}

// ---------- Canonical context for name tag ----------

/// Build the canonical context string used for name tag computation.
/// Format: <state>/<boot-id-or-dash>/<bucket-or-dash>/<shard-hex>/<filename-without-k-and-ext>
pub fn ready_context(shard_hex: &str, filename_without_tag_ext: &str) -> String {
    format!("ready/-/-/{}/{}", shard_hex, filename_without_tag_ext)
}

pub fn leased_context(
    boot_id: &str,
    bucket: &str,
    shard_hex: &str,
    filename_without_tag_ext: &str,
) -> String {
    format!(
        "leased/{}/{}/{}/{}",
        boot_id, bucket, shard_hex, filename_without_tag_ext
    )
}

pub fn delayed_context(bucket: &str, shard_hex: &str, filename_without_tag_ext: &str) -> String {
    format!(
        "delayed/-/{}/{}/{}",
        bucket, shard_hex, filename_without_tag_ext
    )
}

pub fn terminal_context(
    state: State,
    bucket: &str,
    shard_hex: &str,
    filename_without_tag_ext: &str,
) -> String {
    format!(
        "{}/-/{}/{}/{}",
        state.dir_name(),
        bucket,
        shard_hex,
        filename_without_tag_ext
    )
}

// ---------- Filename parser ----------

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadyName {
    pub common: CommonFields,
    pub tag: [u8; 8],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DelayedName {
    pub common: CommonFields,
    pub not_before_ns: u64,
    pub tag: [u8; 8],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeadName {
    pub common: CommonFields,
    pub reason: u16,
    pub tag: [u8; 8],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceiptName {
    pub common: CommonFields,
    pub token: [u8; 16],
    pub tag: [u8; 8],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeasedName {
    pub common: CommonFields,
    pub boottime_deadline_ns: u64,
    pub wall_deadline_ns: u64,
    pub token: [u8; 16],
    pub tag: [u8; 8],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TempName {
    pub created_boottime_ns: u64,
    pub random: [u8; 16],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuarantineName {
    pub quarantine_id: [u8; 16],
    pub reason: u16,
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("invalid extension")]
    BadExtension,
    #[error("missing field {0}")]
    MissingField(&'static str),
    #[error("invalid hex field {0}")]
    BadHex(&'static str),
    #[error("malformed filename")]
    Malformed,
}

fn parse_field<'a>(parts: &[(&'a str, &'a str)], prefix: &str) -> Option<&'a str> {
    for (p, v) in parts {
        if *p == prefix {
            return Some(*v);
        }
    }
    None
}

fn split_fields(s: &str) -> Vec<(&str, &str)> {
    let mut result = Vec::new();
    for part in s.split('.') {
        if part.is_empty() {
            continue;
        }
        let prefix = &part[..1];
        let value = &part[1..];
        result.push((prefix, value));
    }
    result
}

fn parse_common(first: &str, fields: &[(&str, &str)]) -> Result<CommonFields, ParseError> {
    let job_id = hex_decode_16(first).ok_or(ParseError::BadHex("job_id"))?;
    let generation = parse_field(fields, "g")
        .and_then(hex_decode_u64)
        .ok_or(ParseError::MissingField("g"))?;
    let attempt = parse_field(fields, "a")
        .and_then(hex_decode_u32)
        .ok_or(ParseError::MissingField("a"))?;
    let maximum_attempts = parse_field(fields, "m")
        .and_then(hex_decode_u32)
        .ok_or(ParseError::MissingField("m"))?;
    Ok(CommonFields {
        job_id,
        generation,
        attempt,
        maximum_attempts,
    })
}

pub fn parse_ready(filename: &str) -> Result<ReadyName, ParseError> {
    let filename = filename
        .strip_suffix(".sqj")
        .ok_or(ParseError::BadExtension)?;
    let mut parts = filename.split('.');
    let first = parts.next().ok_or(ParseError::Malformed)?;
    let rest: String = parts.collect::<Vec<_>>().join(".");
    let fields = split_fields(&rest);
    let common = parse_common(first, &fields)?;
    let tag_hex = parse_field(&fields, "k").ok_or(ParseError::MissingField("k"))?;
    let tag_bytes = hex_decode_bytes(tag_hex).ok_or(ParseError::BadHex("k"))?;
    let mut tag = [0u8; 8];
    if tag_bytes.len() != 8 {
        return Err(ParseError::BadHex("k"));
    }
    tag.copy_from_slice(&tag_bytes);
    Ok(ReadyName { common, tag })
}

pub fn parse_delayed(filename: &str) -> Result<DelayedName, ParseError> {
    let filename = filename
        .strip_suffix(".sqj")
        .ok_or(ParseError::BadExtension)?;
    let mut parts = filename.split('.');
    let first = parts.next().ok_or(ParseError::Malformed)?;
    let rest: String = parts.collect::<Vec<_>>().join(".");
    let fields = split_fields(&rest);
    let common = parse_common(first, &fields)?;
    let not_before_ns = parse_field(&fields, "d")
        .and_then(hex_decode_u64)
        .ok_or(ParseError::MissingField("d"))?;
    let tag_hex = parse_field(&fields, "k").ok_or(ParseError::MissingField("k"))?;
    let mut tag = [0u8; 8];
    let tag_bytes = hex_decode_bytes(tag_hex).ok_or(ParseError::BadHex("k"))?;
    if tag_bytes.len() != 8 {
        return Err(ParseError::BadHex("k"));
    }
    tag.copy_from_slice(&tag_bytes);
    Ok(DelayedName {
        common,
        not_before_ns,
        tag,
    })
}

pub fn parse_dead(filename: &str) -> Result<DeadName, ParseError> {
    let filename = filename
        .strip_suffix(".sqj")
        .ok_or(ParseError::BadExtension)?;
    let mut parts = filename.split('.');
    let first = parts.next().ok_or(ParseError::Malformed)?;
    let rest: String = parts.collect::<Vec<_>>().join(".");
    let fields = split_fields(&rest);
    let common = parse_common(first, &fields)?;
    let reason = parse_field(&fields, "x")
        .and_then(hex_decode_u16)
        .ok_or(ParseError::MissingField("x"))?;
    let tag_hex = parse_field(&fields, "k").ok_or(ParseError::MissingField("k"))?;
    let mut tag = [0u8; 8];
    let tag_bytes = hex_decode_bytes(tag_hex).ok_or(ParseError::BadHex("k"))?;
    if tag_bytes.len() != 8 {
        return Err(ParseError::BadHex("k"));
    }
    tag.copy_from_slice(&tag_bytes);
    Ok(DeadName {
        common,
        reason,
        tag,
    })
}

pub fn parse_receipt(filename: &str) -> Result<ReceiptName, ParseError> {
    let filename = filename
        .strip_suffix(".rct")
        .ok_or(ParseError::BadExtension)?;
    let mut parts = filename.split('.');
    let first = parts.next().ok_or(ParseError::Malformed)?;
    let rest: String = parts.collect::<Vec<_>>().join(".");
    let fields = split_fields(&rest);
    let common = parse_common(first, &fields)?;
    let token_hex = parse_field(&fields, "t").ok_or(ParseError::MissingField("t"))?;
    let token = hex_decode_16(token_hex).ok_or(ParseError::BadHex("t"))?;
    let tag_hex = parse_field(&fields, "k").ok_or(ParseError::MissingField("k"))?;
    let mut tag = [0u8; 8];
    let tag_bytes = hex_decode_bytes(tag_hex).ok_or(ParseError::BadHex("k"))?;
    if tag_bytes.len() != 8 {
        return Err(ParseError::BadHex("k"));
    }
    tag.copy_from_slice(&tag_bytes);
    Ok(ReceiptName { common, token, tag })
}

pub fn parse_leased(filename: &str) -> Result<LeasedName, ParseError> {
    let filename = filename
        .strip_suffix(".sqj")
        .ok_or(ParseError::BadExtension)?;
    let mut parts = filename.split('.');
    let first = parts.next().ok_or(ParseError::Malformed)?;
    let rest: String = parts.collect::<Vec<_>>().join(".");
    let fields = split_fields(&rest);
    let common = parse_common(first, &fields)?;
    let boottime_deadline_ns = parse_field(&fields, "b")
        .and_then(hex_decode_u64)
        .ok_or(ParseError::MissingField("b"))?;
    let wall_deadline_ns = parse_field(&fields, "w")
        .and_then(hex_decode_u64)
        .ok_or(ParseError::MissingField("w"))?;
    let token_hex = parse_field(&fields, "t").ok_or(ParseError::MissingField("t"))?;
    let token = hex_decode_16(token_hex).ok_or(ParseError::BadHex("t"))?;
    let tag_hex = parse_field(&fields, "k").ok_or(ParseError::MissingField("k"))?;
    let mut tag = [0u8; 8];
    let tag_bytes = hex_decode_bytes(tag_hex).ok_or(ParseError::BadHex("k"))?;
    if tag_bytes.len() != 8 {
        return Err(ParseError::BadHex("k"));
    }
    tag.copy_from_slice(&tag_bytes);
    Ok(LeasedName {
        common,
        boottime_deadline_ns,
        wall_deadline_ns,
        token,
        tag,
    })
}

pub fn parse_temp(filename: &str) -> Result<TempName, ParseError> {
    let filename = filename
        .strip_suffix(".tmp")
        .ok_or(ParseError::BadExtension)?;
    let parts: Vec<&str> = filename.split('.').collect();
    if parts.len() != 2 {
        return Err(ParseError::Malformed);
    }
    let created_boottime_ns = hex_decode_u64(parts[0]).ok_or(ParseError::BadHex("boottime"))?;
    let random = hex_decode_16(parts[1]).ok_or(ParseError::BadHex("random"))?;
    Ok(TempName {
        created_boottime_ns,
        random,
    })
}

pub fn parse_quarantine(filename: &str) -> Result<QuarantineName, ParseError> {
    let filename = filename
        .strip_suffix(".raw")
        .ok_or(ParseError::BadExtension)?;
    // Must start with 'q'
    if !filename.starts_with('q') {
        return Err(ParseError::Malformed);
    }
    let rest = &filename[1..];
    let mut parts = rest.split('.');
    let id_hex = parts.next().ok_or(ParseError::Malformed)?;
    let quarantine_id = hex_decode_16(id_hex).ok_or(ParseError::BadHex("id"))?;
    let reason_part = parts.next().ok_or(ParseError::Malformed)?;
    if !reason_part.starts_with('x') {
        return Err(ParseError::Malformed);
    }
    let reason = hex_decode_u16(&reason_part[1..]).ok_or(ParseError::BadHex("reason"))?;
    Ok(QuarantineName {
        quarantine_id,
        reason,
    })
}

// ---------- Name tag verification helpers ----------

/// Get the filename part without the .k<tag> field and extension.
/// This is used to build the canonical context for tag verification.
pub fn filename_without_tag_and_ext(filename: &str, ext: &str) -> String {
    let stripped = filename.strip_suffix(ext).unwrap_or(filename);
    // Remove the .k<16hex> suffix
    if let Some(pos) = stripped.rfind(".k") {
        stripped[..pos].to_string()
    } else {
        stripped.to_string()
    }
}

/// Compute and verify a name tag for a ready filename.
pub fn verify_ready_tag(queue_id: &[u8; 16], shard: u32, filename: &str) -> bool {
    let parsed = match parse_ready(filename) {
        Ok(p) => p,
        Err(_) => return false,
    };
    let sh = shard_hex(shard);
    let without = filename_without_tag_and_ext(filename, ".sqj");
    let ctx = ready_context(&sh, &without);
    let expected = compute_name_tag(queue_id, &ctx);
    expected == parsed.tag
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_queue_id() -> [u8; 16] {
        [0x42; 16]
    }

    fn test_job_id() -> [u8; 16] {
        [0xAB; 16]
    }

    #[test]
    fn ready_filename_round_trip() {
        let common = CommonFields {
            job_id: test_job_id(),
            generation: 1,
            attempt: 0,
            maximum_attempts: 3,
        };
        let tag = [0x11; 8];
        let filename = ready_filename(&common, &tag);
        let parsed = parse_ready(&filename).unwrap();
        assert_eq!(parsed.common, common);
        assert_eq!(parsed.tag, tag);
    }

    #[test]
    fn leased_filename_round_trip() {
        let common = CommonFields {
            job_id: test_job_id(),
            generation: 2,
            attempt: 1,
            maximum_attempts: 5,
        };
        let tag = [0x22; 8];
        let token = [0x33; 16];
        let filename = leased_filename(&common, 999_999_999, 1_000_000_000, &token, &tag);
        let parsed = parse_leased(&filename).unwrap();
        assert_eq!(parsed.common, common);
        assert_eq!(parsed.boottime_deadline_ns, 999_999_999);
        assert_eq!(parsed.wall_deadline_ns, 1_000_000_000);
        assert_eq!(parsed.token, token);
        assert_eq!(parsed.tag, tag);
    }

    #[test]
    fn delayed_filename_round_trip() {
        let common = CommonFields {
            job_id: test_job_id(),
            generation: 0,
            attempt: 0,
            maximum_attempts: 1,
        };
        let tag = [0x44; 8];
        let filename = delayed_filename(&common, 1_700_000_000_000_000_000, &tag);
        let parsed = parse_delayed(&filename).unwrap();
        assert_eq!(parsed.common, common);
        assert_eq!(parsed.not_before_ns, 1_700_000_000_000_000_000);
        assert_eq!(parsed.tag, tag);
    }

    #[test]
    fn dead_filename_round_trip() {
        let common = CommonFields {
            job_id: test_job_id(),
            generation: 5,
            attempt: 3,
            maximum_attempts: 3,
        };
        let tag = [0x55; 8];
        let filename = dead_filename(&common, 0x0004, &tag);
        let parsed = parse_dead(&filename).unwrap();
        assert_eq!(parsed.common, common);
        assert_eq!(parsed.reason, 0x0004);
        assert_eq!(parsed.tag, tag);
    }

    #[test]
    fn receipt_filename_round_trip() {
        let common = CommonFields {
            job_id: test_job_id(),
            generation: 4,
            attempt: 2,
            maximum_attempts: 5,
        };
        let tag = [0x66; 8];
        let token = [0x77; 16];
        let filename = receipt_filename(&common, &token, &tag);
        let parsed = parse_receipt(&filename).unwrap();
        assert_eq!(parsed.common, common);
        assert_eq!(parsed.token, token);
        assert_eq!(parsed.tag, tag);
    }

    #[test]
    fn temp_filename_round_trip() {
        let random = [0x88; 16];
        let filename = temp_filename(1_700_000_000_000_000_000, &random);
        let parsed = parse_temp(&filename).unwrap();
        assert_eq!(parsed.created_boottime_ns, 1_700_000_000_000_000_000);
        assert_eq!(parsed.random, random);
    }

    #[test]
    fn quarantine_filename_round_trip() {
        let id = [0x99; 16];
        let filename = quarantine_filename(&id, 0x0001);
        let parsed = parse_quarantine(&filename).unwrap();
        assert_eq!(parsed.quarantine_id, id);
        assert_eq!(parsed.reason, 0x0001);
    }

    #[test]
    fn shard_computation() {
        let qid = test_queue_id();
        let jid = test_job_id();
        let shard = compute_shard(&qid, &jid, 64);
        assert!(shard < 64);
        // Same inputs give same shard
        let shard2 = compute_shard(&qid, &jid, 64);
        assert_eq!(shard, shard2);
    }

    #[test]
    fn shard_scan_visits_all() {
        let qid = test_queue_id();
        let boot = [0xFE; 16];
        let nonce = [0xDC; 16];
        let count = 64u32;
        let (start, stride) = shard_scan_params(&qid, &boot, &nonce, 0, count);

        let mut visited = vec![false; count as usize];
        for i in 0..count {
            let s = shard_at(start, stride, i, count);
            assert!(!visited[s as usize], "shard {} visited twice", s);
            visited[s as usize] = true;
        }
        assert!(visited.iter().all(|&v| v));
    }

    #[test]
    fn boot_id_parse() {
        let s = "12345678-1234-1234-1234-123456789abc";
        let bytes = boot_id_bytes(s).unwrap();
        assert_eq!(bytes[0], 0x12);
        assert_eq!(bytes[15], 0xbc);
    }

    #[test]
    fn boot_id_rejects_uppercase() {
        let s = "12345678-1234-1234-1234-123456789ABC";
        // spec requires lowercase
        assert!(boot_id_bytes(s).is_none());
    }

    #[test]
    fn name_tag_deterministic() {
        let qid = test_queue_id();
        let ctx = "ready/-/-/000f/test";
        let tag1 = compute_name_tag(&qid, ctx);
        let tag2 = compute_name_tag(&qid, ctx);
        assert_eq!(tag1, tag2);
    }

    #[test]
    fn verify_ready_tag_works() {
        let qid = test_queue_id();
        let jid = test_job_id();
        let shard = compute_shard(&qid, &jid, 64);
        let common = CommonFields {
            job_id: jid,
            generation: 0,
            attempt: 0,
            maximum_attempts: 3,
        };
        // Build the context and compute the real tag
        let sh = shard_hex(shard);
        let base = format!(
            "{}.g{:016x}.a{:08x}.m{:08x}",
            hex_encode(&jid),
            0u64,
            0u32,
            3u32
        );
        let ctx = ready_context(&sh, &base);
        let tag = compute_name_tag(&qid, &ctx);
        let filename = ready_filename(&common, &tag);
        assert!(verify_ready_tag(&qid, shard, &filename));
    }

    #[test]
    fn parse_rejects_bad_ext() {
        assert!(parse_ready("foo.bar").is_err());
    }
}
