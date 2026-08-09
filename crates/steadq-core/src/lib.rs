#[cfg(not(all(
    target_os = "linux",
    target_arch = "x86_64",
    target_env = "gnu",
    target_pointer_width = "64"
)))]
compile_error!(
    "steadq-core supports only 64-bit x86_64 Linux targets with the GNU environment; the certified release target is x86_64-unknown-linux-gnu"
);

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
pub use recovery::{
    RecoveryReport, RecoveryScanBudget, RecoveryScanStats, RecoveryStats, WorkBudget,
};

/// Re-export RetryPolicy from steadq_math so callers don't need two types.
pub use steadq_math::RetryPolicy;
