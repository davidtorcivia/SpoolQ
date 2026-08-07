// Seeded in-memory filesystem simulator and trace event schema.

use std::collections::HashMap;

/// Seeded pseudo-random number generator for deterministic simulation.
#[derive(Clone, Debug)]
pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        self.state
    }

    pub fn next_bool(&mut self) -> bool {
        self.next_u64() & 1 == 1
    }

    pub fn next_range(&mut self, max: u64) -> u64 {
        if max == 0 {
            return 0;
        }
        self.next_u64() % max
    }
}

/// A simulated file.
#[derive(Clone, Debug)]
pub struct SimFile {
    pub content: Vec<u8>,
    pub synced: bool,
}

/// The seeded in-memory filesystem simulator. Uses a flat path map.
/// P1-27: This simulator does NOT model directory-entry durability.
/// crash() retains synced files but does not restore durable namespace state.
/// A proper model would track volatile vs durable directory entry sets.
#[derive(Clone, Debug)]
pub struct Simulator {
    files: HashMap<String, SimFile>,
    dirs: HashMap<String, bool>, // path -> synced
    rng: Rng,
}

impl Simulator {
    pub fn new(seed: u64) -> Self {
        let mut dirs = HashMap::new();
        dirs.insert(String::new(), true); // root
        Simulator {
            files: HashMap::new(),
            dirs,
            rng: Rng::new(seed),
        }
    }

    pub fn create_dir(&mut self, path: &str) {
        let path = normalize_path(path);
        self.dirs.insert(path, false);
    }

    pub fn write_file(&mut self, path: &str, content: Vec<u8>) {
        let path = normalize_path(path);
        self.files.insert(
            path,
            SimFile {
                content,
                synced: false,
            },
        );
    }

    pub fn fsync_file(&mut self, path: &str) {
        let path = normalize_path(path);
        if let Some(f) = self.files.get_mut(&path) {
            f.synced = true;
        }
    }

    pub fn fsync_dir(&mut self, path: &str) {
        let path = normalize_path(path);
        self.dirs.insert(path, true);
    }

    pub fn rename_noreplace(&mut self, src: &str, dest: &str) -> Result<(), SimError> {
        let src = normalize_path(src);
        let dest = normalize_path(dest);
        if self.files.contains_key(&dest) || self.dirs.contains_key(&dest) {
            return Err(SimError::AlreadyExists);
        }
        match self.files.remove(&src) {
            Some(entry) => {
                self.files.insert(dest, entry);
                Ok(())
            }
            None => Err(SimError::NotFound),
        }
    }

    pub fn unlink(&mut self, path: &str) -> Result<(), SimError> {
        let path = normalize_path(path);
        if self.files.remove(&path).is_some() {
            Ok(())
        } else {
            Err(SimError::NotFound)
        }
    }

    /// Simulate a crash: all unsynced files are lost.
    pub fn crash(&mut self) {
        self.files.retain(|_, f| f.synced);
        for synced in self.dirs.values_mut() {
            *synced = false;
        }
    }

    pub fn exists(&self, path: &str) -> bool {
        let path = normalize_path(path);
        self.files.contains_key(&path) || self.dirs.contains_key(&path)
    }

    pub fn read_file(&self, path: &str) -> Option<&[u8]> {
        let path = normalize_path(path);
        self.files.get(&path).map(|f| f.content.as_slice())
    }

    pub fn maybe_inject_fault(&mut self, probability: u64) -> bool {
        self.rng.next_range(100) < probability
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SimError {
    NotFound,
    AlreadyExists,
}

fn normalize_path(path: &str) -> String {
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    parts.join("/")
}

/// Trace event schema (versioned).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TraceEvent {
    pub schema_version: u32,
    pub operation_id: u64,
    pub job_id_hex: String,
    pub source_state: Option<String>,
    pub destination_state: Option<String>,
    pub pre_generation: Option<u64>,
    pub post_generation: Option<u64>,
    pub attempt: Option<u32>,
    pub syscall_result: Option<String>,
    pub sync_result: Option<String>,
    pub fault_point: Option<String>,
}

impl TraceEvent {
    pub fn schema_version() -> u32 {
        1
    }

    pub fn new(operation_id: u64) -> Self {
        TraceEvent {
            schema_version: Self::schema_version(),
            operation_id,
            job_id_hex: String::new(),
            source_state: None,
            destination_state: None,
            pre_generation: None,
            post_generation: None,
            attempt: None,
            syscall_result: None,
            sync_result: None,
            fault_point: None,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != Self::schema_version() {
            return Err(format!(
                "schema version mismatch: expected {}, got {}",
                Self::schema_version(),
                self.schema_version
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simulator_crash_removes_unsynced() {
        let mut sim = Simulator::new(42);
        sim.create_dir("ready/0000");
        sim.write_file("ready/0000/job.sqj", vec![0xAB; 128]);
        assert!(sim.exists("ready/0000/job.sqj"));
        sim.crash();
        assert!(!sim.exists("ready/0000/job.sqj"));
    }

    #[test]
    fn simulator_crash_preserves_synced() {
        let mut sim = Simulator::new(42);
        sim.create_dir("ready/0000");
        sim.write_file("ready/0000/job.sqj", vec![0xAB; 128]);
        sim.fsync_file("ready/0000/job.sqj");
        sim.crash();
        assert!(sim.exists("ready/0000/job.sqj"));
    }

    #[test]
    fn simulator_rename_noreplace() {
        let mut sim = Simulator::new(42);
        sim.create_dir("ready/0000");
        sim.create_dir("leased/boot/0/0000");
        sim.write_file("ready/0000/job.sqj", vec![0x42; 128]);
        sim.fsync_file("ready/0000/job.sqj");

        sim.rename_noreplace("ready/0000/job.sqj", "leased/boot/0/0000/job.sqj")
            .unwrap();
        assert!(!sim.exists("ready/0000/job.sqj"));
        assert!(sim.exists("leased/boot/0/0000/job.sqj"));
    }

    #[test]
    fn simulator_rename_rejects_collision() {
        let mut sim = Simulator::new(42);
        sim.create_dir("ready/0000");
        sim.write_file("ready/0000/a.sqj", vec![0x42; 128]);
        sim.write_file("ready/0000/b.sqj", vec![0x43; 128]);
        assert_eq!(
            sim.rename_noreplace("ready/0000/a.sqj", "ready/0000/b.sqj"),
            Err(SimError::AlreadyExists)
        );
    }

    #[test]
    fn rng_deterministic() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn trace_event_validation() {
        let event = TraceEvent::new(1);
        assert!(event.validate().is_ok());

        let mut bad = event;
        bad.schema_version = 999;
        assert!(bad.validate().is_err());
    }
}
