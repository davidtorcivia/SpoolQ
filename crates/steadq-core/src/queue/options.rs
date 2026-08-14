// Creation and open configuration.
use super::*;

/// Configuration for creating a new queue.
#[derive(Clone, Debug)]
pub struct CreateOptions {
    pub shard_count: u32,
    pub lease_bucket_width_ns: u64,
    pub delayed_bucket_width_ns: u64,
    pub terminal_bucket_width_ns: u64,
    pub max_payload_length: u64,
}

impl Default for CreateOptions {
    fn default() -> Self {
        Self {
            shard_count: 64,
            lease_bucket_width_ns: 10_000_000_000,
            delayed_bucket_width_ns: 10_000_000_000,
            terminal_bucket_width_ns: 3_600_000_000_000,
            max_payload_length: MAX_PAYLOAD_LENGTH,
        }
    }
}

/// Validate all CreateOptions before any filesystem mutation (C-01).
/// Same validation used in encoding and tests.
pub fn validate_create_options(opts: &CreateOptions) -> Result<(), Error> {
    if opts.shard_count == 0 || !opts.shard_count.is_power_of_two() || opts.shard_count > 4096 {
        return Err(Error::InvalidInput("invalid shard count".into()));
    }
    if opts.lease_bucket_width_ns == 0 {
        return Err(Error::InvalidInput(
            "lease bucket width must be non-zero".into(),
        ));
    }
    if opts.delayed_bucket_width_ns == 0 {
        return Err(Error::InvalidInput(
            "delayed bucket width must be non-zero".into(),
        ));
    }
    if opts.terminal_bucket_width_ns == 0 {
        return Err(Error::InvalidInput(
            "terminal bucket width must be non-zero".into(),
        ));
    }
    if !(60_000_000_000..=86_400_000_000_000).contains(&opts.terminal_bucket_width_ns) {
        return Err(Error::InvalidInput("invalid terminal bucket width".into()));
    }
    if !opts
        .terminal_bucket_width_ns
        .is_multiple_of(opts.delayed_bucket_width_ns)
    {
        return Err(Error::InvalidInput(
            "delayed bucket width must divide terminal bucket width".into(),
        ));
    }
    if opts.max_payload_length > MAX_PAYLOAD_LENGTH {
        return Err(Error::InvalidInput("payload limit exceeds maximum".into()));
    }
    Ok(())
}

pub(super) const MIN_LEASE_DURATION_NS: u64 = 1_000_000_000;
pub(super) const MAX_LEASE_DURATION_NS: u64 = 604_800_000_000_000;

pub(super) fn lease_duration_is_valid(duration_ns: u64) -> bool {
    (MIN_LEASE_DURATION_NS..=MAX_LEASE_DURATION_NS).contains(&duration_ns)
}

pub(super) fn payload_length_is_valid(payload_length: u64, maximum: u64) -> bool {
    payload_length <= maximum
}

/// Operational options for opening a queue.
#[derive(Clone, Debug)]
pub struct OpenOptions {
    pub allow_unsupported_fs: bool,
    pub receipt_retention_ns: u64,
    pub temporary_file_ttl_ns: u64,
    /// When true, directory fsync calls after state transitions are deferred.
    /// The caller must call `sync()` to make directory changes durable.
    /// Enqueue returns `EnqueueOutcome::Deferred` until that barrier is run.
    /// File data is always synced before publication regardless of this flag.
    /// Default: false (maximum durability).
    pub deferred_dir_sync: bool,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            allow_unsupported_fs: false,
            receipt_retention_ns: 7 * 24 * 60 * 60 * 1_000_000_000,
            temporary_file_ttl_ns: 24 * 60 * 60 * 1_000_000_000,
            deferred_dir_sync: false,
        }
    }
}
