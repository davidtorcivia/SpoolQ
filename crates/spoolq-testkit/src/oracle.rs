// Pure in-memory logical oracle for SpoolQ/1.
// Tracks expected queue state without any filesystem calls.

use std::collections::HashMap;

/// Logical state of a single job in the oracle.
#[derive(Clone, Debug, PartialEq)]
pub struct OracleJob {
    pub job_id: [u8; 16],
    pub state: OracleState,
    pub generation: u64,
    pub attempt: u32,
    pub maximum_attempts: u32,
    pub token: Option<[u8; 16]>,
    pub file_synced: bool,
    pub dest_dir_synced: bool,
    pub src_dir_synced: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OracleState {
    Hidden,
    Ready,
    Leased,
    Delayed,
    Dead,
    Receipt,
    Quarantine,
}

/// The pure logical oracle. Tracks the expected state of all jobs.
#[derive(Clone, Debug)]
pub struct Oracle {
    jobs: HashMap<[u8; 16], OracleJob>,
    next_job_counter: u64,
}

impl Oracle {
    pub fn new() -> Self {
        Oracle {
            jobs: HashMap::new(),
            next_job_counter: 0,
        }
    }

    /// Generate a deterministic job ID for testing.
    pub fn gen_job_id(&mut self) -> [u8; 16] {
        let id = self.next_job_counter.to_be_bytes();
        self.next_job_counter += 1;
        let mut full = [0u8; 16];
        full[..8].copy_from_slice(&id);
        full
    }

    /// Record an enqueue in the oracle.
    pub fn record_enqueue(&mut self, job_id: [u8; 16], max_attempts: u32) {
        self.jobs.insert(
            job_id,
            OracleJob {
                job_id,
                state: OracleState::Hidden,
                generation: 0,
                attempt: 0,
                maximum_attempts: max_attempts,
                token: None,
                file_synced: false,
                dest_dir_synced: false,
                src_dir_synced: false,
            },
        );
    }

    /// Record file sync.
    pub fn record_file_sync(&mut self, job_id: &[u8; 16]) {
        if let Some(job) = self.jobs.get_mut(job_id) {
            job.file_synced = true;
        }
    }

    /// Record destination dir sync.
    pub fn record_dest_sync(&mut self, job_id: &[u8; 16]) {
        if let Some(job) = self.jobs.get_mut(job_id) {
            job.dest_dir_synced = true;
        }
    }

    /// Record source dir sync.
    pub fn record_src_sync(&mut self, job_id: &[u8; 16]) {
        if let Some(job) = self.jobs.get_mut(job_id) {
            job.src_dir_synced = true;
        }
    }

    /// Record publication (hidden -> ready or delayed).
    pub fn record_publish(&mut self, job_id: &[u8; 16], to_ready: bool) {
        if let Some(job) = self.jobs.get_mut(job_id) {
            job.state = if to_ready {
                OracleState::Ready
            } else {
                OracleState::Delayed
            };
        }
    }

    /// Record claim (ready -> leased).
    pub fn record_claim(&mut self, job_id: &[u8; 16], token: [u8; 16]) {
        if let Some(job) = self.jobs.get_mut(job_id) {
            if job.state == OracleState::Ready {
                job.state = OracleState::Leased;
                job.generation += 1;
                job.attempt += 1;
                job.token = Some(token);
                job.dest_dir_synced = false;
                job.src_dir_synced = false;
            }
        }
    }

    /// Record ack (leased -> receipt).
    pub fn record_ack(&mut self, job_id: &[u8; 16]) {
        if let Some(job) = self.jobs.get_mut(job_id) {
            if job.state == OracleState::Leased {
                job.state = OracleState::Receipt;
                job.generation += 1;
                job.token = None;
            }
        }
    }

    /// Record retry (leased -> ready).
    pub fn record_retry(&mut self, job_id: &[u8; 16]) {
        if let Some(job) = self.jobs.get_mut(job_id) {
            if job.state == OracleState::Leased {
                job.state = OracleState::Ready;
                job.generation += 1;
                job.token = None;
            }
        }
    }

    /// Record bury (leased -> dead).
    pub fn record_bury(&mut self, job_id: &[u8; 16]) {
        if let Some(job) = self.jobs.get_mut(job_id) {
            if job.state == OracleState::Leased {
                job.state = OracleState::Dead;
                job.generation += 1;
                job.token = None;
            }
        }
    }

    /// Record crash: reset sync flags, roll back uncommitted transitions.
    pub fn record_crash(&mut self) {
        for job in self.jobs.values_mut() {
            job.dest_dir_synced = false;
            job.src_dir_synced = false;
            // Strong profile: rename is atomic
            // Leased with synced file stays leased
            // Leased without synced file rolls back to ready
            if job.state == OracleState::Leased && !job.file_synced {
                job.state = OracleState::Ready;
                job.generation = job.generation.saturating_sub(1);
                job.attempt = job.attempt.saturating_sub(1);
                job.token = None;
            }
            // Ready without any sync rolls back to hidden
            if job.state == OracleState::Ready && !job.file_synced && !job.dest_dir_synced {
                job.state = OracleState::Hidden;
            }
        }
    }

    /// Check invariant I1: no visible active object has an incomplete envelope.
    pub fn check_i1(&self) -> bool {
        self.jobs.values().all(|j| {
            !matches!(
                j.state,
                OracleState::Ready
                    | OracleState::Leased
                    | OracleState::Delayed
                    | OracleState::Dead
                    | OracleState::Receipt
            ) || j.file_synced
        })
    }

    /// Check invariant I9: committed leases never exceed maximum_attempts.
    pub fn check_i9(&self) -> bool {
        self.jobs.values().all(|j| j.attempt <= j.maximum_attempts)
    }

    /// Get a job's current state.
    pub fn get(&self, job_id: &[u8; 16]) -> Option<&OracleJob> {
        self.jobs.get(job_id)
    }

    /// List all jobs.
    pub fn jobs(&self) -> impl Iterator<Item = &OracleJob> {
        self.jobs.values()
    }
}

impl Default for Oracle {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oracle_tracks_enqueue_and_claim() {
        let mut oracle = Oracle::new();
        let id = oracle.gen_job_id();
        oracle.record_enqueue(id, 3);
        oracle.record_file_sync(&id);
        oracle.record_publish(&id, true);
        oracle.record_dest_sync(&id);
        oracle.record_claim(&id, [0xFF; 16]);

        let job = oracle.get(&id).unwrap();
        assert_eq!(job.state, OracleState::Leased);
        assert_eq!(job.generation, 1);
        assert_eq!(job.attempt, 1);
    }

    #[test]
    fn oracle_crash_rollback() {
        let mut oracle = Oracle::new();
        let id = oracle.gen_job_id();
        oracle.record_enqueue(id, 3);
        // Don't sync the file
        oracle.record_publish(&id, true);
        // Crash before any sync
        oracle.record_crash();
        let job = oracle.get(&id).unwrap();
        assert_eq!(job.state, OracleState::Hidden);
    }

    #[test]
    fn oracle_i1_holds() {
        let mut oracle = Oracle::new();
        let id = oracle.gen_job_id();
        oracle.record_enqueue(id, 3);
        oracle.record_file_sync(&id);
        oracle.record_publish(&id, true);
        assert!(oracle.check_i1());
    }

    #[test]
    fn oracle_i1_violated_without_sync() {
        let mut oracle = Oracle::new();
        let id = oracle.gen_job_id();
        oracle.record_enqueue(id, 3);
        oracle.record_publish(&id, true);
        // file not synced
        assert!(!oracle.check_i1());
    }
}
