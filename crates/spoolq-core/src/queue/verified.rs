// Central object verifier: single source of truth for envelope and payload validation.
//
// Callers obtain a VerifiedJob only after the full chain has been checked:
// header decode, extension length bound, envelope digest, size, and payload digest.
// This prevents delivery of corrupt objects via lease or read paths.

use sha2::Digest;

use spoolq_format::FixedHeader;
use spoolq_fs_linux as fs;

use crate::errors::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerificationError {
    Io(String),
    Corrupt(String),
    PayloadCorrupt,
}

impl std::fmt::Display for VerificationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(s) => write!(f, "I/O: {s}"),
            Self::Corrupt(s) => write!(f, "corrupt: {s}"),
            Self::PayloadCorrupt => write!(f, "payload corrupt"),
        }
    }
}

impl std::error::Error for VerificationError {}

impl From<VerificationError> for Error {
    fn from(e: VerificationError) -> Self {
        match e {
            VerificationError::Io(s) => Error::IoFailure(s),
            VerificationError::Corrupt(s) => Error::QueueCorrupt(s),
            VerificationError::PayloadCorrupt => Error::PayloadCorrupt,
        }
    }
}

/// A job envelope that has passed full verification on its held fd.
#[derive(Debug, Clone)]
pub struct VerifiedJob {
    pub header: FixedHeader,
    pub extension: Vec<u8>,
}

impl VerifiedJob {}

/// Verify the envelope and payload on an already-open fd. The fd must remain
/// open across any subsequent operation to prevent TOCTOU swap.
pub fn verify_job_on_fd(fd: std::os::unix::io::RawFd) -> Result<VerifiedJob, VerificationError> {
    let header = read_and_verify_header(fd)?;
    verify_size(fd, &header, header.extension_header_length as usize)?;
    verify_payload(fd, &header, header.extension_header_length as usize)?;
    let ext = read_extension(fd, header.extension_header_length as usize)?;
    if !spoolq_format::verify_envelope_digest(&header, &ext) {
        return Err(VerificationError::Corrupt(
            "envelope digest mismatch".into(),
        ));
    }
    Ok(VerifiedJob {
        header,
        extension: ext,
    })
}

/// Light envelope-only verification (no payload hash). Used for inspection paths
/// that have not yet delivered payload to a consumer.
pub fn verify_envelope_on_fd(
    fd: std::os::unix::io::RawFd,
) -> Result<VerifiedJob, VerificationError> {
    let header = read_and_verify_header(fd)?;
    let ext = read_extension(fd, header.extension_header_length as usize)?;
    if !spoolq_format::verify_envelope_digest(&header, &ext) {
        return Err(VerificationError::Corrupt(
            "envelope digest mismatch".into(),
        ));
    }
    verify_size(fd, &header, ext.len())?;
    Ok(VerifiedJob {
        header,
        extension: ext,
    })
}

fn read_and_verify_header(fd: std::os::unix::io::RawFd) -> Result<FixedHeader, VerificationError> {
    let mut header_buf = [0u8; 128];
    fs::pread_exact(fd, &mut header_buf, 0).map_err(|e| VerificationError::Io(e.to_string()))?;
    let header = FixedHeader::decode(&header_buf)
        .map_err(|e| VerificationError::Corrupt(format!("header decode: {e}")))?;
    let ext_len = header.extension_header_length as usize;
    if is_extension_too_large(ext_len) {
        return Err(VerificationError::Corrupt(
            "extension header too large".into(),
        ));
    }
    Ok(header)
}

fn read_extension(
    fd: std::os::unix::io::RawFd,
    ext_len: usize,
) -> Result<Vec<u8>, VerificationError> {
    let mut ext_buf = vec![0u8; ext_len];
    if is_extension_present(ext_len) {
        fs::pread_exact(fd, &mut ext_buf, 128).map_err(|e| VerificationError::Io(e.to_string()))?;
    }
    Ok(ext_buf)
}

fn verify_size(
    fd: std::os::unix::io::RawFd,
    header: &FixedHeader,
    ext_len: usize,
) -> Result<(), VerificationError> {
    let file_stat = fs::fstat(fd).map_err(|e| VerificationError::Io(e.to_string()))?;
    let expected_size = (128 + ext_len + header.payload_length as usize) as u64;
    if is_size_mismatch(expected_size, file_stat.st_size as u64) {
        return Err(VerificationError::Corrupt(format!(
            "file size mismatch: expected {}, got {}",
            expected_size, file_stat.st_size
        )));
    }
    Ok(())
}

fn verify_payload(
    fd: std::os::unix::io::RawFd,
    header: &FixedHeader,
    ext_len: usize,
) -> Result<(), VerificationError> {
    let data_offset = (128 + ext_len) as u64;
    let mut hasher = sha2::Sha256::new();
    let mut offset = data_offset;
    let mut remaining = header.payload_length as usize;
    let mut buf = vec![0u8; 65536];
    while remaining > 0 {
        let to_read = remaining.min(buf.len());
        let n = fs::pread(fd, &mut buf[..to_read], offset)
            .map_err(|e| VerificationError::Io(e.to_string()))?;
        if n == 0 {
            return Err(VerificationError::Corrupt("unexpected EOF".into()));
        }
        hasher.update(&buf[..n]);
        offset += n as u64;
        remaining -= n;
    }
    let computed: [u8; 32] = hasher.finalize().into();
    if !is_payload_digest_match(header, &computed) {
        return Err(VerificationError::PayloadCorrupt);
    }
    Ok(())
}

// helpers extracted for mutant killing
pub fn is_envelope_digest_valid(header: &FixedHeader, ext: &[u8]) -> bool {
    spoolq_format::verify_envelope_digest(header, ext)
}

pub fn is_payload_digest_match(header: &FixedHeader, computed: &[u8; 32]) -> bool {
    &header.payload_digest == computed
}

pub fn is_extension_too_large(ext_len: usize) -> bool {
    ext_len > 65536
}

pub fn is_extension_present(ext_len: usize) -> bool {
    ext_len > 0
}

pub fn is_size_mismatch(expected: u64, actual: u64) -> bool {
    expected != actual
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_extension_too_large_table() {
        assert!(!is_extension_too_large(0));
        assert!(!is_extension_too_large(65536));
        assert!(is_extension_too_large(65537));
        assert!(is_extension_too_large(usize::MAX));
    }

    #[test]
    fn is_payload_digest_match_table() {
        let mut h = FixedHeader {
            extension_header_length: 0,
            payload_length: 0,
            flags: 0,
            digest_algorithm: 1,
            job_id: [1; 16],
            maximum_attempts: 1,
            created_at_unix_ns: 0,
            payload_digest: [0xAB; 32],
            envelope_digest: [0; 32],
        };
        assert!(is_payload_digest_match(&h, &[0xAB; 32]));
        assert!(!is_payload_digest_match(&h, &[0x00; 32]));
        h.payload_digest = [0x00; 32];
        assert!(is_payload_digest_match(&h, &[0x00; 32]));
        assert!(!is_payload_digest_match(&h, &[0x01; 32]));
    }

    #[test]
    fn verification_error_display() {
        assert_eq!(
            format!("{}", VerificationError::Io("boom".into())),
            "I/O: boom"
        );
        assert_eq!(
            format!("{}", VerificationError::Corrupt("bad".into())),
            "corrupt: bad"
        );
        assert_eq!(
            format!("{}", VerificationError::PayloadCorrupt),
            "payload corrupt"
        );
    }

    #[test]
    fn verification_error_into_core_error() {
        let e: Error = VerificationError::Io("x".into()).into();
        assert!(matches!(e, Error::IoFailure(_)));
        let e: Error = VerificationError::Corrupt("y".into()).into();
        assert!(matches!(e, Error::QueueCorrupt(_)));
        let e: Error = VerificationError::PayloadCorrupt.into();
        assert!(matches!(e, Error::PayloadCorrupt));
    }

    #[test]
    fn is_extension_present_table() {
        assert!(!is_extension_present(0));
        assert!(is_extension_present(1));
        assert!(is_extension_present(65536));
        assert!(is_extension_present(usize::MAX));
    }

    #[test]
    fn is_size_mismatch_table() {
        assert!(!is_size_mismatch(0, 0));
        assert!(!is_size_mismatch(100, 100));
        assert!(is_size_mismatch(100, 99));
        assert!(is_size_mismatch(100, 101));
        assert!(is_size_mismatch(u64::MAX, 0));
        assert!(is_size_mismatch(0, u64::MAX));
    }

    #[test]
    fn is_envelope_digest_valid_table() {
        let header = FixedHeader {
            extension_header_length: 0,
            payload_length: 0,
            flags: 0,
            digest_algorithm: 1,
            job_id: [0x11; 16],
            maximum_attempts: 1,
            created_at_unix_ns: 0,
            payload_digest: [0; 32],
            envelope_digest: [0; 32],
        };
        // empty extension with zero digest will not match unless header was computed for it;
        // we just verify predicate returns false for mismatched and is functionally wired.
        // Create a header whose envelope_digest is computed correctly for empty extension.
        let mut h = header.clone();
        let ext: Vec<u8> = vec![];
        // compute correct envelope: not trivial without helper, but we can verify that
        // the predicate is equivalent to spoolq_format::verify_envelope_digest by checking
        // that negating the result matches the helper.
        let valid = is_envelope_digest_valid(&h, &ext);
        let expected = spoolq_format::verify_envelope_digest(&h, &ext);
        assert_eq!(valid, expected);
        // flip a byte in envelope digest to make invalid
        h.envelope_digest = [0xFF; 32];
        assert!(!is_envelope_digest_valid(&h, &ext));
    }

    #[test]
    fn verify_size_detects_mismatch_via_tmpfile() {
        use std::os::unix::io::AsRawFd;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("size_test.raw");
        let header = FixedHeader {
            extension_header_length: 0,
            payload_length: 10,
            flags: 0,
            digest_algorithm: 1,
            job_id: [0x22; 16],
            maximum_attempts: 1,
            created_at_unix_ns: 0,
            payload_digest: [0; 32],
            envelope_digest: [0; 32],
        };
        let ext: Vec<u8> = vec![];
        let mut h = header.clone();
        h.envelope_digest = spoolq_format::envelope_digest(&h, &ext).unwrap_or([0; 32]);
        let header_buf = h.encode(&ext).unwrap();
        std::fs::write(&path, header_buf).unwrap();
        let file = std::fs::OpenOptions::new().read(true).open(&path).unwrap();
        let res = verify_size(file.as_raw_fd(), &h, 0);
        assert!(matches!(res, Err(VerificationError::Corrupt(_))));
        drop(file);
        let mut full = Vec::with_capacity(138);
        full.extend_from_slice(&header_buf);
        full.extend_from_slice(&[0u8; 10]);
        std::fs::write(&path, &full).unwrap();
        let file2 = std::fs::OpenOptions::new().read(true).open(&path).unwrap();
        let res2 = verify_size(file2.as_raw_fd(), &h, 0);
        assert!(res2.is_ok());
    }
}
