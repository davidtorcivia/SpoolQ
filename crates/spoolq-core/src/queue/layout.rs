// Typed physical locations for queue objects.

use spoolq_names::{self, bucket_hex, shard_hex, CommonFields};

use crate::errors::Error;

/// Typed location of a queue object on disk.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Location {
    Ready { shard: u32 },
    Leased { boot_id: String, bucket: u64, shard: u32 },
    Delayed { bucket: u64, shard: u32 },
    Receipt { bucket: u64, shard: u32 },
    Dead { bucket: u64, shard: u32 },
}

/// Target filename plus its typed location.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Target {
    pub location: Location,
    pub filename: String,
}

impl Target {
    pub fn directory(&self) -> String {
        match &self.location {
            Location::Ready { shard } => format!("ready/{}", shard_hex(*shard)),
            Location::Leased { boot_id, bucket, shard } => {
                format!("leased/{}/{}/{}", boot_id, bucket_hex(*bucket), shard_hex(*shard))
            }
            Location::Delayed { bucket, shard } => {
                format!("delayed/{}/{}", bucket_hex(*bucket), shard_hex(*shard))
            }
            Location::Receipt { bucket, shard } => {
                format!("receipts/{}/{}", bucket_hex(*bucket), shard_hex(*shard))
            }
            Location::Dead { bucket, shard } => {
                format!("dead/{}/{}", bucket_hex(*bucket), shard_hex(*shard))
            }
        }
    }

    pub fn relative_path(&self) -> String {
        format!("{}/{}", self.directory(), self.filename)
    }

    pub fn state(&self) -> spoolq_names::State {
        match self.location {
            Location::Ready { .. } => spoolq_names::State::Ready,
            Location::Leased { .. } => spoolq_names::State::Leased,
            Location::Delayed { .. } => spoolq_names::State::Delayed,
            Location::Receipt { .. } => spoolq_names::State::Receipt,
            Location::Dead { .. } => spoolq_names::State::Dead,
        }
    }
}

/// Layout helper that owns queue configuration for path construction.
pub struct Layout<'a> {
    queue_id: &'a [u8; 16],
    shard_count: u32,
    lease_bucket_width_ns: u64,
    delayed_bucket_width_ns: u64,
    terminal_bucket_width_ns: u64,
    boot_id: &'a str,
}

impl<'a> Layout<'a> {
    pub fn new(
        queue_id: &'a [u8; 16],
        shard_count: u32,
        lease_bucket_width_ns: u64,
        delayed_bucket_width_ns: u64,
        terminal_bucket_width_ns: u64,
        boot_id: &'a str,
    ) -> Self {
        Self {
            queue_id,
            shard_count,
            lease_bucket_width_ns,
            delayed_bucket_width_ns,
            terminal_bucket_width_ns,
            boot_id,
        }
    }

    fn shard_for(&self, job_id: &[u8; 16]) -> u32 {
        spoolq_names::compute_shard(self.queue_id, job_id, self.shard_count)
    }

    pub fn ready(&self, common: &CommonFields) -> Target {
        let shard = self.shard_for(&common.job_id);
        let filename = spoolq_names::make_ready_name(self.queue_id, &shard_hex(shard), common);
        Target {
            location: Location::Ready { shard },
            filename,
        }
    }

    pub fn delayed(&self, common: &CommonFields, not_before_ns: u64) -> Result<Target, Error> {
        let (bucket, _) = spoolq_math::eligibility_bucket_and_ns(
            not_before_ns,
            self.delayed_bucket_width_ns,
        )
        .ok_or_else(|| Error::InvalidInput("eligibility overflow".into()))?;
        let shard = self.shard_for(&common.job_id);
        let filename = spoolq_names::make_delayed_name(
            self.queue_id,
            &bucket_hex(bucket),
            &shard_hex(shard),
            common,
            not_before_ns,
        );
        Ok(Target {
            location: Location::Delayed { bucket, shard },
            filename,
        })
    }

    pub fn leased(
        &self,
        common: &CommonFields,
        boottime_deadline_ns: u64,
        wall_deadline_ns: u64,
        token: &[u8; 16],
    ) -> Result<Target, Error> {
        let bucket = spoolq_math::lease_bucket(boottime_deadline_ns, self.lease_bucket_width_ns).unwrap_or(0);
        let shard = self.shard_for(&common.job_id);
        let filename = spoolq_names::make_leased_name(
            self.queue_id,
            self.boot_id,
            &bucket_hex(bucket),
            &shard_hex(shard),
            common,
            boottime_deadline_ns,
            wall_deadline_ns,
            token,
        );
        Ok(Target {
            location: Location::Leased {
                boot_id: self.boot_id.to_string(),
                bucket,
                shard,
            },
            filename,
        })
    }

    pub fn receipt(&self, common: &CommonFields, token: &[u8; 16], wall_ns: u64) -> Result<Target, Error> {
        let bucket = spoolq_math::bucket_number(wall_ns, self.terminal_bucket_width_ns).unwrap_or(0);
        let shard = self.shard_for(&common.job_id);
        let filename = spoolq_names::make_receipt_name(
            self.queue_id,
            &bucket_hex(bucket),
            &shard_hex(shard),
            common,
            token,
        );
        Ok(Target {
            location: Location::Receipt { bucket, shard },
            filename,
        })
    }

    pub fn dead(&self, common: &CommonFields, reason: u16, wall_ns: u64) -> Result<Target, Error> {
        let bucket = spoolq_math::bucket_number(wall_ns, self.terminal_bucket_width_ns).unwrap_or(0);
        let shard = self.shard_for(&common.job_id);
        let filename = spoolq_names::make_dead_name(
            self.queue_id,
            &bucket_hex(bucket),
            &shard_hex(shard),
            common,
            reason,
        );
        Ok(Target {
            location: Location::Dead { bucket, shard },
            filename,
        })
    }

    /// Parse a leased relative path into typed location and filename.
    /// Validates leased/<boot>/<bucket>/<shard>/<name> with canonical hex.
    pub fn parse_leased_path(&self, relative: &str) -> Result<(Location, String), Error> {
        let parts: Vec<&str> = relative.split('/').collect();
        if parts.len() != 5 || parts[0] != "leased" {
            return Err(Error::QueueCorrupt("invalid leased path".into()));
        }
        let boot_id = parts[1];
        if spoolq_names::boot_id_bytes(boot_id).is_none() {
            return Err(Error::QueueCorrupt("invalid boot id".into()));
        }
        let bucket = spoolq_names::bucket_from_hex(parts[2])
            .ok_or_else(|| Error::QueueCorrupt("invalid bucket hex".into()))?;
        let shard = spoolq_names::shard_from_hex(parts[3])
            .ok_or_else(|| Error::QueueCorrupt("invalid shard hex".into()))?;
        if shard >= self.shard_count {
            return Err(Error::QueueCorrupt("shard out of range".into()));
        }
        let filename = parts[4].to_string();
        let loc = Location::Leased {
            boot_id: boot_id.to_string(),
            bucket,
            shard,
        };
        Ok((loc, filename))
    }
}
