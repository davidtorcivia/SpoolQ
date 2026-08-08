// SteadQ/1 binary format constants, encoding, and decoding.

pub mod cbor;

use sha2::{Digest, Sha256};

pub const FORMAT_MAGIC: &[u8; 8] = b"SDQFMT1\0";
pub const JOB_MAGIC: &[u8; 8] = b"SDQJOB1\0";
pub const RECEIPT_MAGIC: &[u8; 8] = b"SDQRCPT\0";
pub const WATERMARK_MAGIC: &[u8; 8] = b"SDQWMR1\0";

pub const FORMAT_SIZE: usize = 160;
pub const FIXED_HEADER_SIZE: usize = 128;
pub const COMPACT_RECEIPT_SIZE: usize = 128;
pub const WATERMARK_SIZE: usize = 64;

pub const DIGEST_ALGORITHM_SHA256: u8 = 1;
pub const NAME_TAG_BITS: u8 = 64;

pub const FORMAT_MAJOR: u16 = 1;
pub const FORMAT_MINOR: u16 = 0;

pub const MAX_PAYLOAD_LENGTH: u64 = (1u64 << 40) - 1;
pub const MAX_EXTENSION_HEADER_LENGTH: u64 = 65_536;

// ---------- FORMAT record ----------

#[derive(Clone, Debug)]
pub struct FormatRecord {
    pub queue_id: [u8; 16],
    pub created_at_unix_ns: u64,
    pub shard_count: u32,
    pub lease_bucket_width_ns: u64,
    pub delayed_bucket_width_ns: u64,
    pub terminal_bucket_width_ns: u64,
    pub max_payload_length: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum FormatError {
    #[error("magic mismatch")]
    BadMagic,
    #[error("unsupported version {0}.{1}")]
    UnsupportedVersion(u16, u16),
    #[error("nonzero flags")]
    NonzeroFlags,
    #[error("nonzero reserved bytes")]
    NonzeroReserved,
    #[error("unsupported digest algorithm {0}")]
    UnsupportedDigestAlgo(u8),
    #[error("invalid name tag bits {0}")]
    InvalidNameTagBits(u8),
    #[error("invalid shard count {0}")]
    InvalidShardCount(u32),
    #[error("invalid bucket width")]
    InvalidBucketWidth,
    #[error("payload limit exceeds maximum")]
    PayloadLimitExceeded,
    #[error("digest mismatch")]
    DigestMismatch,
    #[error("nonzero feature bits")]
    NonzeroFeatureBits,
    #[error("wrong size: expected {expected}, got {actual}")]
    WrongSize { expected: usize, actual: usize },
}

impl FormatRecord {
    pub fn encode(&self) -> [u8; FORMAT_SIZE] {
        let mut buf = [0u8; FORMAT_SIZE];
        buf[0..8].copy_from_slice(FORMAT_MAGIC);
        buf[8..10].copy_from_slice(&FORMAT_MAJOR.to_be_bytes());
        buf[10..12].copy_from_slice(&FORMAT_MINOR.to_be_bytes());
        // flags = 0 at [12..16]
        buf[16..32].copy_from_slice(&self.queue_id);
        buf[32..40].copy_from_slice(&self.created_at_unix_ns.to_be_bytes());
        buf[40..44].copy_from_slice(&self.shard_count.to_be_bytes());
        // reserved [44..48] = 0
        buf[48..56].copy_from_slice(&self.lease_bucket_width_ns.to_be_bytes());
        buf[56..64].copy_from_slice(&self.delayed_bucket_width_ns.to_be_bytes());
        buf[64..72].copy_from_slice(&self.terminal_bucket_width_ns.to_be_bytes());
        buf[72..80].copy_from_slice(&self.max_payload_length.to_be_bytes());
        buf[80] = DIGEST_ALGORITHM_SHA256;
        buf[81] = NAME_TAG_BITS;
        // reserved [82..88] = 0
        // required_feature_bits [88..96] = 0
        // optional_feature_bits [96..104] = 0
        // reserved [104..128] = 0

        let digest = format_digest(&buf[0..128]);
        buf[128..160].copy_from_slice(&digest);

        buf
    }

    pub fn decode(buf: &[u8]) -> Result<Self, FormatError> {
        if buf.len() != FORMAT_SIZE {
            return Err(FormatError::WrongSize {
                expected: FORMAT_SIZE,
                actual: buf.len(),
            });
        }

        if &buf[0..8] != FORMAT_MAGIC {
            return Err(FormatError::BadMagic);
        }
        let major = u16::from_be_bytes(buf[8..10].try_into().unwrap());
        let minor = u16::from_be_bytes(buf[10..12].try_into().unwrap());
        if major != FORMAT_MAJOR {
            return Err(FormatError::UnsupportedVersion(major, minor));
        }

        let flags = u32::from_be_bytes(buf[12..16].try_into().unwrap());
        if flags != 0 {
            return Err(FormatError::NonzeroFlags);
        }

        let queue_id: [u8; 16] = buf[16..32].try_into().unwrap();
        let created_at_unix_ns = u64::from_be_bytes(buf[32..40].try_into().unwrap());
        let shard_count = u32::from_be_bytes(buf[40..44].try_into().unwrap());

        let reserved_44 = u32::from_be_bytes(buf[44..48].try_into().unwrap());
        if reserved_44 != 0 {
            return Err(FormatError::NonzeroReserved);
        }

        let lease_bucket_width_ns = u64::from_be_bytes(buf[48..56].try_into().unwrap());
        let delayed_bucket_width_ns = u64::from_be_bytes(buf[56..64].try_into().unwrap());
        let terminal_bucket_width_ns = u64::from_be_bytes(buf[64..72].try_into().unwrap());
        let max_payload_length = u64::from_be_bytes(buf[72..80].try_into().unwrap());

        let digest_algo = buf[80];
        if digest_algo != DIGEST_ALGORITHM_SHA256 {
            return Err(FormatError::UnsupportedDigestAlgo(digest_algo));
        }
        let name_tag_bits = buf[81];
        if name_tag_bits != NAME_TAG_BITS {
            return Err(FormatError::InvalidNameTagBits(name_tag_bits));
        }

        // Check all reserved bytes are zero
        if buf[82..88].iter().any(|&b| b != 0) || buf[104..128].iter().any(|&b| b != 0) {
            return Err(FormatError::NonzeroReserved);
        }

        // Check feature bits
        let req_features = u64::from_be_bytes(buf[88..96].try_into().unwrap());
        let opt_features = u64::from_be_bytes(buf[96..104].try_into().unwrap());
        if req_features != 0 || opt_features != 0 {
            return Err(FormatError::NonzeroFeatureBits);
        }

        // Verify digest
        let expected_digest = format_digest(&buf[0..128]);
        let stored_digest: [u8; 32] = buf[128..160].try_into().unwrap();
        if expected_digest != stored_digest {
            return Err(FormatError::DigestMismatch);
        }

        // Validate shard count: power of two, 1..=4096
        if shard_count == 0 || !shard_count.is_power_of_two() || shard_count > 4096 {
            return Err(FormatError::InvalidShardCount(shard_count));
        }

        // Validate widths
        if lease_bucket_width_ns == 0 || delayed_bucket_width_ns == 0 {
            return Err(FormatError::InvalidBucketWidth);
        }
        if !(60_000_000_000..=86_400_000_000_000).contains(&terminal_bucket_width_ns) {
            return Err(FormatError::InvalidBucketWidth);
        }

        if max_payload_length > MAX_PAYLOAD_LENGTH {
            return Err(FormatError::PayloadLimitExceeded);
        }

        Ok(FormatRecord {
            queue_id,
            created_at_unix_ns,
            shard_count,
            lease_bucket_width_ns,
            delayed_bucket_width_ns,
            terminal_bucket_width_ns,
            max_payload_length,
        })
    }
}

pub fn format_digest(header: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"SteadQ-1-format\0");
    hasher.update(header);
    let result = hasher.finalize();
    result.into()
}

// ---------- Fixed job header ----------

#[derive(Clone, Debug)]
pub struct FixedHeader {
    pub extension_header_length: u32,
    pub payload_length: u64,
    pub flags: u32,
    pub digest_algorithm: u8,
    pub job_id: [u8; 16],
    pub maximum_attempts: u32,
    pub created_at_unix_ns: u64,
    pub payload_digest: [u8; 32],
    pub envelope_digest: [u8; 32],
}

#[derive(Debug, thiserror::Error)]
pub enum HeaderError {
    #[error("magic mismatch")]
    BadMagic,
    #[error("unsupported version {0}.{1}")]
    UnsupportedVersion(u16, u16),
    #[error("nonzero flags")]
    NonzeroFlags,
    #[error("digest algorithm mismatch: {0}")]
    DigestAlgoMismatch(u8),
    #[error("nonzero reserved bytes")]
    NonzeroReserved,
    #[error("wrong size: expected {expected}, got {actual}")]
    WrongSize { expected: usize, actual: usize },
    #[error("payload length exceeds maximum")]
    PayloadTooLarge,
    #[error("extension header exceeds maximum")]
    ExtensionTooLarge,
    #[error("maximum_attempts is zero")]
    ZeroMaxAttempts,
    #[error("envelope digest mismatch")]
    EnvelopeDigestMismatch,
    #[error("envelope digest mismatch in extension")]
    ExtensionDigestMismatch,
    #[error("extension_header_length does not match actual extension bytes")]
    ExtensionLengthMismatch,
}

impl FixedHeader {
    /// Encode the fixed header. Validates that extension_header_length matches
    /// the actual extension bytes length (C-52).
    pub fn encode(&self, extension: &[u8]) -> Result<[u8; FIXED_HEADER_SIZE], HeaderError> {
        if extension.len() as u32 != self.extension_header_length {
            return Err(HeaderError::ExtensionLengthMismatch);
        }
        let mut buf = [0u8; FIXED_HEADER_SIZE];
        buf[0..8].copy_from_slice(JOB_MAGIC);
        buf[8..10].copy_from_slice(&FORMAT_MAJOR.to_be_bytes());
        buf[10..12].copy_from_slice(&FORMAT_MINOR.to_be_bytes());
        buf[12..16].copy_from_slice(&self.extension_header_length.to_be_bytes());
        buf[16..24].copy_from_slice(&self.payload_length.to_be_bytes());
        buf[24..28].copy_from_slice(&self.flags.to_be_bytes());
        buf[28] = self.digest_algorithm;
        // reserved [29..32] = 0
        buf[32..48].copy_from_slice(&self.job_id);
        buf[48..52].copy_from_slice(&self.maximum_attempts.to_be_bytes());
        // reserved [52..56] = 0
        buf[56..64].copy_from_slice(&self.created_at_unix_ns.to_be_bytes());
        buf[64..96].copy_from_slice(&self.payload_digest);
        buf[96..128].copy_from_slice(&self.envelope_digest);
        Ok(buf)
    }

    pub fn decode(buf: &[u8]) -> Result<Self, HeaderError> {
        if buf.len() != FIXED_HEADER_SIZE {
            return Err(HeaderError::WrongSize {
                expected: FIXED_HEADER_SIZE,
                actual: buf.len(),
            });
        }

        if &buf[0..8] != JOB_MAGIC {
            return Err(HeaderError::BadMagic);
        }
        let major = u16::from_be_bytes(buf[8..10].try_into().unwrap());
        let minor = u16::from_be_bytes(buf[10..12].try_into().unwrap());
        if major != FORMAT_MAJOR {
            return Err(HeaderError::UnsupportedVersion(major, minor));
        }

        let flags = u32::from_be_bytes(buf[24..28].try_into().unwrap());
        if flags != 0 {
            return Err(HeaderError::NonzeroFlags);
        }

        let digest_algorithm = buf[28];
        if digest_algorithm != DIGEST_ALGORITHM_SHA256 {
            return Err(HeaderError::DigestAlgoMismatch(digest_algorithm));
        }

        if buf[29..32].iter().any(|&b| b != 0) || buf[52..56].iter().any(|&b| b != 0) {
            return Err(HeaderError::NonzeroReserved);
        }

        let extension_header_length = u32::from_be_bytes(buf[12..16].try_into().unwrap());
        let payload_length = u64::from_be_bytes(buf[16..24].try_into().unwrap());
        let job_id: [u8; 16] = buf[32..48].try_into().unwrap();
        let maximum_attempts = u32::from_be_bytes(buf[48..52].try_into().unwrap());
        let created_at_unix_ns = u64::from_be_bytes(buf[56..64].try_into().unwrap());
        let payload_digest: [u8; 32] = buf[64..96].try_into().unwrap();
        let envelope_digest: [u8; 32] = buf[96..128].try_into().unwrap();

        if maximum_attempts == 0 {
            return Err(HeaderError::ZeroMaxAttempts);
        }
        if payload_length > MAX_PAYLOAD_LENGTH {
            return Err(HeaderError::PayloadTooLarge);
        }
        if extension_header_length as u64 > MAX_EXTENSION_HEADER_LENGTH {
            return Err(HeaderError::ExtensionTooLarge);
        }

        Ok(FixedHeader {
            extension_header_length,
            payload_length,
            flags,
            digest_algorithm,
            job_id,
            maximum_attempts,
            created_at_unix_ns,
            payload_digest,
            envelope_digest,
        })
    }
}

pub fn payload_digest(payload: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(payload);
    hasher.finalize().into()
}

/// R2-M01: Fallible envelope digest. Returns None on extension length mismatch.
pub fn envelope_digest(fixed_header: &FixedHeader, extension: &[u8]) -> Option<[u8; 32]> {
    let mut header_with_zero_digest = fixed_header.encode(extension).ok()?;
    // Zero out bytes 96..128 (envelope_digest field)
    header_with_zero_digest[96..128].fill(0);

    let mut hasher = Sha256::new();
    hasher.update(b"SteadQ-1-envelope\0");
    hasher.update(header_with_zero_digest);
    hasher.update(extension);
    let result: [u8; 32] = hasher.finalize().into();
    Some(result)
}

/// Verify envelope digest given the fixed header and extension bytes.
pub fn verify_envelope_digest(header: &FixedHeader, extension: &[u8]) -> bool {
    match envelope_digest(header, extension) {
        Some(computed) => computed == header.envelope_digest,
        None => false,
    }
}

// ---------- Compact receipt ----------

#[derive(Clone, Debug)]
pub struct CompactReceipt {
    pub job_id: [u8; 16],
    pub envelope_digest: [u8; 32],
    pub final_attempt: u32,
    pub lease_token: [u8; 16],
    pub receipt_bucket_start_unix_ns: u64,
    pub original_payload_length: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ReceiptError {
    #[error("magic mismatch")]
    BadMagic,
    #[error("unsupported version {0}.{1}")]
    UnsupportedVersion(u16, u16),
    #[error("digest mismatch")]
    DigestMismatch,
    #[error("wrong size: expected {expected}, got {actual}")]
    WrongSize { expected: usize, actual: usize },
}

impl CompactReceipt {
    pub fn encode(&self) -> [u8; COMPACT_RECEIPT_SIZE] {
        let mut buf = [0u8; COMPACT_RECEIPT_SIZE];
        buf[0..8].copy_from_slice(RECEIPT_MAGIC);
        buf[8..10].copy_from_slice(&FORMAT_MAJOR.to_be_bytes());
        buf[10..12].copy_from_slice(&FORMAT_MINOR.to_be_bytes());
        buf[12..28].copy_from_slice(&self.job_id);
        buf[28..60].copy_from_slice(&self.envelope_digest);
        buf[60..64].copy_from_slice(&self.final_attempt.to_be_bytes());
        buf[64..80].copy_from_slice(&self.lease_token);
        buf[80..88].copy_from_slice(&self.receipt_bucket_start_unix_ns.to_be_bytes());
        buf[88..96].copy_from_slice(&self.original_payload_length.to_be_bytes());

        let digest = receipt_digest(&buf[0..96]);
        buf[96..128].copy_from_slice(&digest);

        buf
    }

    pub fn decode(buf: &[u8]) -> Result<Self, ReceiptError> {
        if buf.len() != COMPACT_RECEIPT_SIZE {
            return Err(ReceiptError::WrongSize {
                expected: COMPACT_RECEIPT_SIZE,
                actual: buf.len(),
            });
        }

        if &buf[0..8] != RECEIPT_MAGIC {
            return Err(ReceiptError::BadMagic);
        }
        let major = u16::from_be_bytes(buf[8..10].try_into().unwrap());
        let minor = u16::from_be_bytes(buf[10..12].try_into().unwrap());
        if major != FORMAT_MAJOR {
            return Err(ReceiptError::UnsupportedVersion(major, minor));
        }

        let job_id: [u8; 16] = buf[12..28].try_into().unwrap();
        let envelope_digest: [u8; 32] = buf[28..60].try_into().unwrap();
        let final_attempt = u32::from_be_bytes(buf[60..64].try_into().unwrap());
        let lease_token: [u8; 16] = buf[64..80].try_into().unwrap();
        let receipt_bucket_start_unix_ns = u64::from_be_bytes(buf[80..88].try_into().unwrap());
        let original_payload_length = u64::from_be_bytes(buf[88..96].try_into().unwrap());

        let expected = receipt_digest(&buf[0..96]);
        let stored: [u8; 32] = buf[96..128].try_into().unwrap();
        if expected != stored {
            return Err(ReceiptError::DigestMismatch);
        }

        Ok(CompactReceipt {
            job_id,
            envelope_digest,
            final_attempt,
            lease_token,
            receipt_bucket_start_unix_ns,
            original_payload_length,
        })
    }
}

pub fn receipt_digest(record: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"SteadQ-1-receipt\0");
    hasher.update(record);
    hasher.finalize().into()
}

// ---------- Wall watermark record ----------

#[derive(Clone, Debug)]
pub struct WatermarkRecord {
    pub highest_observed_bucket: u64,
    pub sequence: u64,
}

impl WatermarkRecord {
    pub fn encode(&self) -> [u8; WATERMARK_SIZE] {
        let mut buf = [0u8; WATERMARK_SIZE];
        buf[0..8].copy_from_slice(WATERMARK_MAGIC);
        buf[8..10].copy_from_slice(&FORMAT_MAJOR.to_be_bytes());
        buf[10..12].copy_from_slice(&FORMAT_MINOR.to_be_bytes());
        // reserved [12..16] = 0
        buf[16..24].copy_from_slice(&self.highest_observed_bucket.to_be_bytes());
        buf[24..32].copy_from_slice(&self.sequence.to_be_bytes());

        let digest = watermark_digest(&buf[0..32]);
        buf[32..64].copy_from_slice(&digest);

        buf
    }

    pub fn decode(buf: &[u8]) -> Result<Self, WatermarkError> {
        if buf.len() != WATERMARK_SIZE {
            return Err(WatermarkError::WrongSize {
                expected: WATERMARK_SIZE,
                actual: buf.len(),
            });
        }
        if &buf[0..8] != WATERMARK_MAGIC {
            return Err(WatermarkError::BadMagic);
        }
        let major = u16::from_be_bytes(buf[8..10].try_into().unwrap());
        let minor = u16::from_be_bytes(buf[10..12].try_into().unwrap());
        if major != FORMAT_MAJOR {
            return Err(WatermarkError::UnsupportedVersion(major, minor));
        }
        let reserved = u32::from_be_bytes(buf[12..16].try_into().unwrap());
        if reserved != 0 {
            return Err(WatermarkError::NonzeroReserved);
        }

        let expected = watermark_digest(&buf[0..32]);
        let stored: [u8; 32] = buf[32..64].try_into().unwrap();
        if expected != stored {
            return Err(WatermarkError::DigestMismatch);
        }

        Ok(WatermarkRecord {
            highest_observed_bucket: u64::from_be_bytes(buf[16..24].try_into().unwrap()),
            sequence: u64::from_be_bytes(buf[24..32].try_into().unwrap()),
        })
    }
}

pub fn watermark_digest(record: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"SteadQ-1-wall-watermark\0");
    hasher.update(record);
    hasher.finalize().into()
}

#[derive(Debug, thiserror::Error)]
pub enum WatermarkError {
    #[error("magic mismatch")]
    BadMagic,
    #[error("unsupported version {0}.{1}")]
    UnsupportedVersion(u16, u16),
    #[error("nonzero reserved")]
    NonzeroReserved,
    #[error("digest mismatch")]
    DigestMismatch,
    #[error("wrong size: expected {expected}, got {actual}")]
    WrongSize { expected: usize, actual: usize },
}

// ---------- Job envelope reader/validator (C-53) ----------

/// Errors from envelope validation.
#[derive(Debug, thiserror::Error)]
pub enum EnvelopeError {
    #[error("header decode error: {0}")]
    Header(#[from] HeaderError),
    #[error("envelope digest mismatch")]
    EnvelopeDigestMismatch,
    #[error("payload digest mismatch")]
    PayloadDigestMismatch,
    #[error("file too short: expected at least {expected}, got {actual}")]
    TooShort { expected: usize, actual: usize },
    #[error("trailing bytes after payload")]
    TrailingBytes,
    #[error("extension decode error: {0}")]
    Extension(String),
    #[error("I/O error: {0}")]
    Io(String),
}

/// Result of reading and validating a job envelope from a buffer.
/// Provides verified access to the header and payload.
pub struct ValidatedEnvelope<'a> {
    pub header: FixedHeader,
    pub extension: &'a [u8],
    pub payload: &'a [u8],
}

impl<'a> ValidatedEnvelope<'a> {
    /// Parse and validate a complete job envelope from a byte buffer.
    /// Validates: header magic/version/reserved, envelope digest,
    /// exact total file length, and optionally payload digest.
    /// Rejects trailing bytes.
    pub fn from_bytes(data: &'a [u8], verify_payload: bool) -> Result<Self, EnvelopeError> {
        if data.len() < FIXED_HEADER_SIZE {
            return Err(EnvelopeError::TooShort {
                expected: FIXED_HEADER_SIZE,
                actual: data.len(),
            });
        }

        let header = FixedHeader::decode(&data[..FIXED_HEADER_SIZE])?;
        let ext_len = header.extension_header_length as usize;
        let payload_len = header.payload_length as usize;
        let expected_total = FIXED_HEADER_SIZE
            .checked_add(ext_len)
            .ok_or(EnvelopeError::Io("size overflow".into()))?
            .checked_add(payload_len)
            .ok_or(EnvelopeError::Io("size overflow".into()))?;

        if data.len() != expected_total {
            if data.len() > expected_total {
                return Err(EnvelopeError::TrailingBytes);
            }
            return Err(EnvelopeError::TooShort {
                expected: expected_total,
                actual: data.len(),
            });
        }

        let extension = &data[FIXED_HEADER_SIZE..FIXED_HEADER_SIZE + ext_len];
        let payload = &data[FIXED_HEADER_SIZE + ext_len..];

        // Verify envelope digest
        if !verify_envelope_digest(&header, extension) {
            return Err(EnvelopeError::EnvelopeDigestMismatch);
        }

        // Optionally verify payload digest
        if verify_payload {
            let computed = payload_digest(payload);
            if computed != header.payload_digest {
                return Err(EnvelopeError::PayloadDigestMismatch);
            }
        }

        Ok(ValidatedEnvelope {
            header,
            extension,
            payload,
        })
    }

    /// Decode and validate the extension header.
    pub fn decode_extension(&self) -> Result<cbor::ExtensionHeader, cbor::CborError> {
        cbor::ExtensionHeader::decode(self.extension)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_round_trip() {
        let rec = FormatRecord {
            queue_id: [0x42; 16],
            created_at_unix_ns: 1_700_000_000_000_000_000,
            shard_count: 64,
            lease_bucket_width_ns: 10_000_000_000,
            delayed_bucket_width_ns: 10_000_000_000,
            terminal_bucket_width_ns: 3_600_000_000_000,
            max_payload_length: MAX_PAYLOAD_LENGTH,
        };
        let encoded = rec.encode();
        let decoded = FormatRecord::decode(&encoded).unwrap();
        assert_eq!(decoded.queue_id, rec.queue_id);
        assert_eq!(decoded.shard_count, rec.shard_count);
    }

    #[test]
    fn format_rejects_bad_magic() {
        let rec = FormatRecord {
            queue_id: [1; 16],
            created_at_unix_ns: 0,
            shard_count: 1,
            lease_bucket_width_ns: 10_000_000_000,
            delayed_bucket_width_ns: 10_000_000_000,
            terminal_bucket_width_ns: 3_600_000_000_000,
            max_payload_length: MAX_PAYLOAD_LENGTH,
        };
        let mut buf = rec.encode();
        buf[0] = b'X';
        assert!(FormatRecord::decode(&buf).is_err());
    }

    #[test]
    fn format_rejects_non_power_of_two_shards() {
        let rec = FormatRecord {
            queue_id: [1; 16],
            created_at_unix_ns: 0,
            shard_count: 3,
            lease_bucket_width_ns: 10_000_000_000,
            delayed_bucket_width_ns: 10_000_000_000,
            terminal_bucket_width_ns: 3_600_000_000_000,
            max_payload_length: MAX_PAYLOAD_LENGTH,
        };
        let encoded = rec.encode();
        // The digest is computed over bytes including shard_count=3,
        // so decode will pass digest check but fail shard validation.
        assert!(FormatRecord::decode(&encoded).is_err());
    }

    #[test]
    fn header_round_trip() {
        let mut header = FixedHeader {
            extension_header_length: 0,
            payload_length: 5,
            flags: 0,
            digest_algorithm: DIGEST_ALGORITHM_SHA256,
            job_id: [0xAB; 16],
            maximum_attempts: 3,
            created_at_unix_ns: 1_700_000_000_000_000_000,
            payload_digest: payload_digest(b"hello"),
            envelope_digest: [0; 32], // placeholder
        };
        let extension: &[u8] = &[];
        header.envelope_digest = envelope_digest(&header, extension).unwrap();
        let encoded = header.encode(extension).unwrap();
        let decoded = FixedHeader::decode(&encoded).unwrap();
        assert_eq!(decoded.job_id, header.job_id);
        assert_eq!(decoded.payload_length, 5);
        assert!(verify_envelope_digest(&decoded, extension));
    }

    #[test]
    fn receipt_round_trip() {
        let rec = CompactReceipt {
            job_id: [0xCD; 16],
            envelope_digest: [0xEE; 32],
            final_attempt: 2,
            lease_token: [0x11; 16],
            receipt_bucket_start_unix_ns: 1_700_000_000_000_000_000,
            original_payload_length: 1024,
        };
        let encoded = rec.encode();
        let decoded = CompactReceipt::decode(&encoded).unwrap();
        assert_eq!(decoded.job_id, rec.job_id);
        assert_eq!(decoded.final_attempt, rec.final_attempt);
    }

    #[test]
    fn watermark_round_trip() {
        let rec = WatermarkRecord {
            highest_observed_bucket: 42,
            sequence: 7,
        };
        let encoded = rec.encode();
        let decoded = WatermarkRecord::decode(&encoded).unwrap();
        assert_eq!(decoded.highest_observed_bucket, 42);
        assert_eq!(decoded.sequence, 7);
    }

    #[test]
    fn empty_payload_digest() {
        let d = payload_digest(b"");
        let expected = hex_literal();
        assert_eq!(&d[..], &expected[..]);
    }

    fn hex_literal() -> [u8; 32] {
        let s = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let mut out = [0u8; 32];
        for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
            out[i] = u8::from_str_radix(std::str::from_utf8(chunk).unwrap(), 16).unwrap();
        }
        out
    }
    #[test]
    fn format_truncation_fails_at_every_offset() {
        let rec = FormatRecord {
            queue_id: [0x42; 16],
            created_at_unix_ns: 1_700_000_000_000_000_000,
            shard_count: 64,
            lease_bucket_width_ns: 10_000_000_000,
            delayed_bucket_width_ns: 10_000_000_000,
            terminal_bucket_width_ns: 3_600_000_000_000,
            max_payload_length: MAX_PAYLOAD_LENGTH,
        };
        let encoded = rec.encode();
        for i in 0..FORMAT_SIZE {
            let truncated = &encoded[..i];
            assert!(
                FormatRecord::decode(truncated).is_err(),
                "FORMAT decode should fail at truncation offset {i}"
            );
        }
    }

    #[test]
    fn header_truncation_fails_at_every_offset() {
        let header = FixedHeader {
            extension_header_length: 0,
            payload_length: 5,
            flags: 0,
            digest_algorithm: DIGEST_ALGORITHM_SHA256,
            job_id: [0xAB; 16],
            maximum_attempts: 3,
            created_at_unix_ns: 1_700_000_000_000_000_000,
            payload_digest: payload_digest(b"hello"),
            envelope_digest: [0; 32],
        };
        let ext: &[u8] = &[];
        let encoded = header.encode(ext).unwrap();
        for i in 0..FIXED_HEADER_SIZE {
            let truncated = &encoded[..i];
            assert!(
                FixedHeader::decode(truncated).is_err(),
                "header decode should fail at truncation offset {i}"
            );
        }
    }

    #[test]
    fn receipt_truncation_fails_at_every_offset() {
        let rec = CompactReceipt {
            job_id: [0xCD; 16],
            envelope_digest: [0xEE; 32],
            final_attempt: 2,
            lease_token: [0x11; 16],
            receipt_bucket_start_unix_ns: 1_700_000_000_000_000_000,
            original_payload_length: 1024,
        };
        let encoded = rec.encode();
        for i in 0..COMPACT_RECEIPT_SIZE {
            let truncated = &encoded[..i];
            assert!(
                CompactReceipt::decode(truncated).is_err(),
                "receipt decode should fail at truncation offset {i}"
            );
        }
    }

    #[test]
    fn format_extra_byte_fails() {
        let rec = FormatRecord {
            queue_id: [0x42; 16],
            created_at_unix_ns: 0,
            shard_count: 1,
            lease_bucket_width_ns: 10_000_000_000,
            delayed_bucket_width_ns: 10_000_000_000,
            terminal_bucket_width_ns: 3_600_000_000_000,
            max_payload_length: MAX_PAYLOAD_LENGTH,
        };
        let mut encoded = rec.encode().to_vec();
        encoded.push(0x00);
        assert!(FormatRecord::decode(&encoded).is_err());
    }

    #[test]
    fn watermark_truncation_fails_at_every_offset() {
        let rec = WatermarkRecord {
            highest_observed_bucket: 42,
            sequence: 7,
        };
        let encoded = rec.encode();
        for i in 0..WATERMARK_SIZE {
            let truncated = &encoded[..i];
            assert!(
                WatermarkRecord::decode(truncated).is_err(),
                "watermark decode should fail at truncation offset {i}"
            );
        }
    }
    #[test]
    fn envelope_reader_validates_complete_file() {
        let mut header = FixedHeader {
            extension_header_length: 0,
            payload_length: 5,
            flags: 0,
            digest_algorithm: DIGEST_ALGORITHM_SHA256,
            job_id: [0xAB; 16],
            maximum_attempts: 3,
            created_at_unix_ns: 1_700_000_000_000_000_000,
            payload_digest: payload_digest(b"hello"),
            envelope_digest: [0; 32],
        };
        let ext: &[u8] = &[];
        header.envelope_digest = envelope_digest(&header, ext).unwrap();
        let header_bytes = header.encode(ext).unwrap();
        let mut data = header_bytes.to_vec();
        data.extend_from_slice(b"hello");
        let env = ValidatedEnvelope::from_bytes(&data, true).unwrap();
        assert_eq!(env.header.job_id, [0xAB; 16]);
        assert_eq!(env.payload, b"hello");
    }

    #[test]
    fn envelope_reader_rejects_trailing_bytes() {
        let mut header = FixedHeader {
            extension_header_length: 0,
            payload_length: 5,
            flags: 0,
            digest_algorithm: DIGEST_ALGORITHM_SHA256,
            job_id: [0xAB; 16],
            maximum_attempts: 3,
            created_at_unix_ns: 0,
            payload_digest: payload_digest(b"hello"),
            envelope_digest: [0; 32],
        };
        let ext: &[u8] = &[];
        header.envelope_digest = envelope_digest(&header, ext).unwrap();
        let header_bytes = header.encode(ext).unwrap();
        let mut data = header_bytes.to_vec();
        data.extend_from_slice(b"hello");
        data.push(0xFF); // trailing byte
        assert!(ValidatedEnvelope::from_bytes(&data, false).is_err());
    }

    #[test]
    fn envelope_reader_rejects_bad_payload_digest() {
        let mut header = FixedHeader {
            extension_header_length: 0,
            payload_length: 5,
            flags: 0,
            digest_algorithm: DIGEST_ALGORITHM_SHA256,
            job_id: [0xAB; 16],
            maximum_attempts: 3,
            created_at_unix_ns: 0,
            payload_digest: payload_digest(b"hello"),
            envelope_digest: [0; 32],
        };
        let ext: &[u8] = &[];
        header.envelope_digest = envelope_digest(&header, ext).unwrap();
        let header_bytes = header.encode(ext).unwrap();
        let mut data = header_bytes.to_vec();
        data.extend_from_slice(b"world"); // wrong payload
        assert!(ValidatedEnvelope::from_bytes(&data, true).is_err());
    }

    #[test]
    fn fixed_header_encode_validates_extension_length() {
        // C-52: encode must reject mismatched extension_header_length
        let header = FixedHeader {
            extension_header_length: 10, // claims 10 but ext is empty
            payload_length: 0,
            flags: 0,
            digest_algorithm: DIGEST_ALGORITHM_SHA256,
            job_id: [0xAB; 16],
            maximum_attempts: 3,
            created_at_unix_ns: 0,
            payload_digest: [0; 32],
            envelope_digest: [0; 32],
        };
        let ext: &[u8] = &[];
        assert!(header.encode(ext).is_err());
    }

    fn hex_to_32(s: &str) -> [u8; 32] {
        let mut out = [0u8; 32];
        for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
            out[i] = u8::from_str_radix(std::str::from_utf8(chunk).unwrap(), 16).unwrap();
        }
        out
    }

    #[test]
    fn format_digest_known_value() {
        let d = format_digest(&[0u8; 128]);
        let expected =
            hex_to_32("9707cc37ed7f1025a4c3b066c83051c6353c5aa3be9b6650940f03acf288961c");
        assert_eq!(d, expected);
    }

    #[test]
    fn envelope_digest_known_value() {
        let header = FixedHeader {
            extension_header_length: 0,
            payload_length: 5,
            flags: 0,
            digest_algorithm: DIGEST_ALGORITHM_SHA256,
            job_id: [0xAB; 16],
            maximum_attempts: 3,
            created_at_unix_ns: 0,
            payload_digest: payload_digest(b"hello"),
            envelope_digest: [0; 32],
        };
        let ext: &[u8] = &[];
        let d = envelope_digest(&header, ext).unwrap();
        let expected =
            hex_to_32("58490679fad0f92ecbea9ab1de222052f31e815331855c88bbfe5ac01503d88c");
        assert_eq!(d, expected);
    }

    #[test]
    fn receipt_digest_known_value() {
        let d = receipt_digest(&[0u8; 96]);
        let expected =
            hex_to_32("544b0c70aa840523646a75befda7f513967631287f0ed4430993212840ae16c9");
        assert_eq!(d, expected);
    }

    #[test]
    fn watermark_digest_known_value() {
        let d = watermark_digest(&[0u8; 32]);
        let expected =
            hex_to_32("58e096acae632c52bb6cc30d24c156eb7d45b7586475d987f13670e1193dbaca");
        assert_eq!(d, expected);
    }
}
