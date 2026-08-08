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

impl VerifiedJob {
    pub fn payload_length(&self) -> u64 {
        self.header.payload_length
    }
    pub fn payload_digest(&self) -> [u8; 32] {
        self.header.payload_digest
    }
    pub fn job_id(&self) -> [u8; 16] {
        self.header.job_id
    }
}

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
    if ext_len > 65536 {
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
    if ext_len > 0 {
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
    if file_stat.st_size as u64 != expected_size {
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
    if computed != header.payload_digest {
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
}
