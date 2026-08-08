// SteadQ/1 error and outcome types.

/// Error categories for all operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("not committed: {0}")]
    NotCommitted(String),
    #[error("resource exhausted")]
    ResourceExhausted,
    #[error("state exhausted")]
    StateExhausted,
    #[error("identity collision")]
    IdentityCollision,
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("unsupported filesystem")]
    UnsupportedFilesystem,
    #[error("unsupported format")]
    UnsupportedFormat,
    #[error("invalid clock")]
    InvalidClock,
    #[error("maintenance busy")]
    MaintenanceBusy,
    #[error("queue corrupt: {0}")]
    QueueCorrupt(String),
    #[error("payload corrupt")]
    PayloadCorrupt,
    #[error("queue poisoned: {0}")]
    QueuePoisoned(String),
    #[error("permission denied")]
    PermissionDenied,
    #[error("io failure: {0}")]
    IoFailure(String),
}

/// Operation result for mutations. Every mutating operation returns one of these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationResult {
    Committed,
    NotCommitted(Error),
    OutcomeUnknown(TransitionTicket),
}

/// Enqueue outcomes.
#[derive(Debug, Clone)]
pub enum EnqueueOutcome {
    Committed(EnqueueTicket),
    NotCommitted(EnqueueTicket, Error),
    OutcomeUnknown(EnqueueTicket, Error),
}

/// Lease outcomes.
#[derive(Debug, Clone)]
pub enum LeaseOutcome {
    Leased(LeaseInfo),
    Empty,
    NotCommitted(Error),
    OutcomeUnknown(TransitionTicket),
}

/// Renew outcomes.
#[derive(Debug, Clone)]
pub enum RenewOutcome {
    Renewed(LeaseInfo),
    LeaseLost,
    NotCommitted(Error),
    OutcomeUnknown(TransitionTicket),
}

/// Ack outcomes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AckOutcome {
    Acked,
    AlreadyAcked,
    LeaseLost,
    NotCommitted(Error),
    OutcomeUnknown(TransitionTicket),
}

/// Transition outcomes (retry, bury).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransitionOutcome {
    Committed,
    LeaseLost,
    NotCommitted(Error),
    OutcomeUnknown(TransitionTicket),
}

/// Ticket for resolving an indeterminate enqueue.
#[derive(Debug, Clone)]
pub struct EnqueueTicket {
    pub job_id: [u8; 16],
    pub envelope_digest: [u8; 32],
    pub expected_initial_state: InitialState,
    pub expected_relative_path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitialState {
    Ready,
    Delayed,
}

/// Ticket for resolving an indeterminate transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransitionTicket {
    pub job_id: [u8; 16],
    pub source_state: String,
    pub source_generation: u64,
    pub source_attempt: u32,
    pub source_relative_path: String,
    pub attempted_destination_state: String,
    pub attempted_destination_relative_path: String,
    pub lease_token: Option<[u8; 16]>,
    pub envelope_digest: [u8; 32],
}

impl TransitionTicket {
    /// Validate both serialized ticket paths before either is used for I/O.
    pub fn validate_paths(&self) -> Result<(), Error> {
        validate_transition_path(&self.source_state, &self.source_relative_path)
            .map_err(|message| Error::InvalidInput(format!("invalid source path: {message}")))?;
        validate_transition_path(
            &self.attempted_destination_state,
            &self.attempted_destination_relative_path,
        )
        .map_err(|message| Error::InvalidInput(format!("invalid destination path: {message}")))?;
        Ok(())
    }
}

fn validate_transition_path(state: &str, path: &str) -> Result<(), String> {
    let validated =
        steadq_fs_linux::ValidatedRelativePath::new(path).map_err(|error| error.to_string())?;
    let parts = validated.components().collect::<Vec<_>>();
    if parts.first().copied() != Some(state) {
        return Err("declared state does not match path".into());
    }

    let valid_shape = match state {
        "ready" => {
            parts.len() == 3
                && steadq_names::shard_from_hex(parts[1]).is_some()
                && steadq_names::parse_ready(parts[2]).is_ok()
        }
        "leased" => {
            parts.len() == 5
                && steadq_names::boot_id_bytes(parts[1]).is_some()
                && steadq_names::bucket_from_hex(parts[2]).is_some()
                && steadq_names::shard_from_hex(parts[3]).is_some()
                && steadq_names::parse_leased(parts[4]).is_ok()
        }
        "delayed" => {
            parts.len() == 4
                && steadq_names::bucket_from_hex(parts[1]).is_some()
                && steadq_names::shard_from_hex(parts[2]).is_some()
                && steadq_names::parse_delayed(parts[3]).is_ok()
        }
        "receipts" => {
            parts.len() == 4
                && steadq_names::bucket_from_hex(parts[1]).is_some()
                && steadq_names::shard_from_hex(parts[2]).is_some()
                && steadq_names::parse_receipt(parts[3]).is_ok()
        }
        "dead" => {
            parts.len() == 4
                && steadq_names::bucket_from_hex(parts[1]).is_some()
                && steadq_names::shard_from_hex(parts[2]).is_some()
                && steadq_names::parse_dead(parts[3]).is_ok()
        }
        _ => false,
    };
    if !valid_shape {
        return Err("path does not have the canonical state shape".into());
    }
    Ok(())
}

/// Lease info returned from claim or renew.
#[derive(Debug, Clone)]
pub struct LeaseInfo {
    pub job_id: [u8; 16],
    pub envelope_digest: [u8; 32],
    pub generation: u64,
    pub attempt: u32,
    pub maximum_attempts: u32,
    pub token: [u8; 16],
    pub boot_id: String,
    pub expires_boottime_ns: u64,
    pub expires_wall_ns: u64,
    pub content_type: String,
    pub payload_length: u64,
    pub payload_digest: [u8; 32],
    pub expected_dev: u64,
    pub expected_inode: u64,
    pub exact_source_path: String,
}

impl LeaseInfo {
    /// Remaining lease time in nanoseconds based on CLOCK_BOOTTIME.
    pub fn remaining_ns(&self, current_boottime_ns: u64) -> u64 {
        self.expires_boottime_ns.saturating_sub(current_boottime_ns)
    }
}

/// Dead reason codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum DeadReason {
    Unspecified = 0x0000,
    ConsumerRejected = 0x0001,
    UnsupportedContentType = 0x0002,
    AdministrativeBury = 0x0003,
    AttemptsExhausted = 0x0004,
}

impl DeadReason {
    pub fn from_u16(v: u16) -> Option<Self> {
        match v {
            0x0000 => Some(Self::Unspecified),
            0x0001 => Some(Self::ConsumerRejected),
            0x0002 => Some(Self::UnsupportedContentType),
            0x0003 => Some(Self::AdministrativeBury),
            0x0004 => Some(Self::AttemptsExhausted),
            _ => None,
        }
    }
}

/// Quarantine reason codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum QuarantineReason {
    EnvelopeCorrupt = 0x0001,
    PayloadCorrupt = 0x0002,
    FilenameParseFailed = 0x0003,
    FilenameTagFailed = 0x0004,
    FilenameHeaderMismatch = 0x0005,
    UnsupportedRequiredFeature = 0x0006,
    DuplicateStateConflict = 0x0007,
    NonRegularFile = 0x0008,
    UnexpectedHardLink = 0x0009,
    CrossDeviceObject = 0x000a,
    ImpossibleStateTransition = 0x000b,
}

impl QuarantineReason {
    pub fn from_u16(v: u16) -> Option<Self> {
        match v {
            0x0001 => Some(Self::EnvelopeCorrupt),
            0x0002 => Some(Self::PayloadCorrupt),
            0x0003 => Some(Self::FilenameParseFailed),
            0x0004 => Some(Self::FilenameTagFailed),
            0x0005 => Some(Self::FilenameHeaderMismatch),
            0x0006 => Some(Self::UnsupportedRequiredFeature),
            0x0007 => Some(Self::DuplicateStateConflict),
            0x0008 => Some(Self::NonRegularFile),
            0x0009 => Some(Self::UnexpectedHardLink),
            0x000a => Some(Self::CrossDeviceObject),
            0x000b => Some(Self::ImpossibleStateTransition),
            _ => None,
        }
    }
}

/// Resolution outcome from resolve().
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolutionOutcome {
    DestinationObserved,
    DestinationStabilized,
    SourceObserved,
    SourceStabilized,
    BothObserved,
    NeitherObserved,
    ConflictingObject,
    ResolutionFailed(Error),
}

/// Diagnostic snapshot of a job's current state.
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub job_id: [u8; 16],
    pub state: String,
    pub generation: u64,
    pub attempt: u32,
    pub maximum_attempts: u32,
    pub shard: u32,
    pub relative_path: String,
    pub size: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn canonical_transition_paths() -> Vec<(&'static str, String)> {
        let queue_id = [0; 16];
        let common = steadq_names::CommonFields {
            job_id: [1; 16],
            generation: 0,
            attempt: 1,
            maximum_attempts: 3,
        };
        let boot = "00000000-0000-0000-0000-000000000000";
        let bucket = "0000000000000000";
        let shard = "0000";
        let token = [2; 16];
        vec![
            (
                "ready",
                format!(
                    "ready/{shard}/{}",
                    steadq_names::make_ready_name(&queue_id, shard, &common)
                ),
            ),
            (
                "leased",
                format!(
                    "leased/{boot}/{bucket}/{shard}/{}",
                    steadq_names::make_leased_name(
                        &queue_id, boot, bucket, shard, &common, 1, 2, &token,
                    )
                ),
            ),
            (
                "delayed",
                format!(
                    "delayed/{bucket}/{shard}/{}",
                    steadq_names::make_delayed_name(&queue_id, bucket, shard, &common, 1)
                ),
            ),
            (
                "receipts",
                format!(
                    "receipts/{bucket}/{shard}/{}",
                    steadq_names::make_receipt_name(&queue_id, bucket, shard, &common, &token)
                ),
            ),
            (
                "dead",
                format!(
                    "dead/{bucket}/{shard}/{}",
                    steadq_names::make_dead_name(&queue_id, bucket, shard, &common, 1)
                ),
            ),
        ]
    }

    #[test]
    fn dead_reason_round_trip() {
        for code in [0x0000u16, 0x0001, 0x0002, 0x0003, 0x0004] {
            let reason = DeadReason::from_u16(code).unwrap();
            assert_eq!(reason as u16, code);
        }
        assert!(DeadReason::from_u16(0x0005).is_none());
    }

    #[test]
    fn quarantine_reason_round_trip() {
        for code in 0x0001u16..=0x000b {
            let reason = QuarantineReason::from_u16(code).unwrap();
            assert_eq!(reason as u16, code);
        }
        assert!(QuarantineReason::from_u16(0x000c).is_none());
    }

    #[test]
    fn lease_remaining() {
        let lease = LeaseInfo {
            job_id: [0; 16],
            envelope_digest: [0; 32],
            generation: 0,
            attempt: 0,
            maximum_attempts: 1,
            token: [0; 16],
            boot_id: "00000000-0000-0000-0000-000000000000".to_string(),
            expires_boottime_ns: 10_000_000_000,
            expires_wall_ns: 0,
            content_type: "x".to_string(),
            payload_length: 0,
            payload_digest: [0; 32],
            expected_dev: 0,
            expected_inode: 0,
            exact_source_path: "ready/0000/x.sqj".to_string(),
        };
        assert_eq!(lease.remaining_ns(5_000_000_000), 5_000_000_000);
        assert_eq!(lease.remaining_ns(10_000_000_000), 0);
        assert_eq!(lease.remaining_ns(15_000_000_000), 0);
    }

    fn valid_transition_ticket() -> TransitionTicket {
        let paths = canonical_transition_paths();
        TransitionTicket {
            job_id: [1; 16],
            source_state: "ready".into(),
            source_generation: 0,
            source_attempt: 0,
            source_relative_path: paths[0].1.clone(),
            attempted_destination_state: "leased".into(),
            attempted_destination_relative_path: paths[1].1.clone(),
            lease_token: Some([2; 16]),
            envelope_digest: [3; 32],
        }
    }

    #[test]
    fn transition_path_state_shapes() {
        let cases = canonical_transition_paths();
        for (state, path) in cases {
            assert_eq!(validate_transition_path(state, &path), Ok(()));
        }
    }

    #[test]
    fn transition_path_rejects_unsafe_or_noncanonical_shapes() {
        let cases = [
            ("ready", ""),
            ("ready", "/ready/0000/missing.sqj"),
            ("ready", "ready/../outside.sqj"),
            ("ready", "ready//missing.sqj"),
            ("ready", "ready/0000/missing.sqj/"),
            ("ready", "ready/0000/extra/missing.sqj"),
            ("ready", "delayed/0000000000000000/0000/missing.sqj"),
            ("unknown", "unknown/0000/missing.sqj"),
            (
                "leased",
                "leased/not-a-boot/0000000000000000/0000/missing.sqj",
            ),
            ("delayed", "delayed/not-a-bucket/0000/missing.sqj"),
            ("dead", "dead/0000000000000000/not-a-shard/missing.sqj"),
            ("ready", "ready/0000/missing.sqj"),
            (
                "leased",
                "leased/00000000-0000-0000-0000-000000000000/0000000000000000/0000/missing.sqj",
            ),
            ("delayed", "delayed/0000000000000000/0000/missing.sqj"),
            ("receipts", "receipts/0000000000000000/0000/missing.rct"),
            ("dead", "dead/0000000000000000/0000/missing.sqj"),
        ];
        for (state, path) in cases {
            assert!(
                validate_transition_path(state, path).is_err(),
                "accepted {state} path {path}"
            );
        }
    }

    #[test]
    fn transition_ticket_validates_both_paths() {
        let mut ticket = valid_transition_ticket();
        assert_eq!(ticket.validate_paths(), Ok(()));

        ticket.source_relative_path = "../outside.sqj".into();
        assert!(matches!(
            ticket.validate_paths(),
            Err(Error::InvalidInput(_))
        ));

        ticket = valid_transition_ticket();
        ticket.attempted_destination_relative_path = "leased/../../outside.sqj".into();
        assert!(matches!(
            ticket.validate_paths(),
            Err(Error::InvalidInput(_))
        ));
    }
}
