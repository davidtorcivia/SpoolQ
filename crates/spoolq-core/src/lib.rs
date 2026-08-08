pub mod errors;
pub mod quarantine;
pub mod queue;
pub mod recovery;
pub mod state_machine;

#[cfg(test)]
pub mod power_loss_harness;

pub use errors::*;
pub use quarantine::{
    CorruptionFinding, FindingSeverity, FsckDepth, FsckMode, FsckOptions, FsckReport,
    QuarantineEntry,
};
pub use queue::*;
pub use recovery::{RecoveryStats, WorkBudget};

/// Re-export RetryPolicy from spoolq_math so callers don't need two types.
pub use spoolq_math::RetryPolicy;
