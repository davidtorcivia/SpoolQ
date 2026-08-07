// SpoolQ/1 deterministic bucket arithmetic, jitter computation, and checked arithmetic helpers.

use sha2::{Digest, Sha256};

// ---------- Checked arithmetic ----------

pub fn checked_add_u64(a: u64, b: u64) -> Option<u64> {
    a.checked_add(b)
}

pub fn checked_mul_u64(a: u64, b: u64) -> Option<u64> {
    a.checked_mul(b)
}

// ---------- Bucket arithmetic ----------

/// floor(timestamp_ns / bucket_width_ns).
/// Returns None if bucket_width_ns is zero (C-50).
pub fn bucket_number(timestamp_ns: u64, bucket_width_ns: u64) -> Option<u64> {
    if bucket_width_ns == 0 {
        return None;
    }
    Some(timestamp_ns / bucket_width_ns)
}

/// ceiling(timestamp_ns / bucket_width_ns).
/// Returns None if bucket_width_ns is zero (C-50).
pub fn ceiling_bucket(timestamp_ns: u64, bucket_width_ns: u64) -> Option<u64> {
    if bucket_width_ns == 0 {
        return None;
    }
    let q = timestamp_ns / bucket_width_ns;
    let r = timestamp_ns % bucket_width_ns;
    Some(if r != 0 { q + 1 } else { q })
}

/// Rounded-up eligibility bucket for delayed scheduling.
/// eligibility_bucket = ceiling(requested_ns / bucket_width_ns)
/// eligibility_ns = eligibility_bucket * bucket_width_ns (checked)
pub fn eligibility_bucket_and_ns(requested_ns: u64, bucket_width_ns: u64) -> Option<(u64, u64)> {
    let bucket = ceiling_bucket(requested_ns, bucket_width_ns)?;
    let ns = checked_mul_u64(bucket, bucket_width_ns)?;
    Some((bucket, ns))
}

/// bucket_start_ns = bucket * bucket_width_ns
pub fn bucket_start_ns(bucket: u64, bucket_width_ns: u64) -> Option<u64> {
    checked_mul_u64(bucket, bucket_width_ns)
}

/// bucket_end_ns = bucket_start_ns + bucket_width_ns
pub fn bucket_end_ns(bucket: u64, bucket_width_ns: u64) -> Option<u64> {
    let start = bucket_start_ns(bucket, bucket_width_ns)?;
    checked_add_u64(start, bucket_width_ns)
}

// ---------- Lease bucket ----------

pub fn lease_bucket(boottime_deadline_ns: u64, lease_bucket_width_ns: u64) -> Option<u64> {
    bucket_number(boottime_deadline_ns, lease_bucket_width_ns)
}

// ---------- Retry jitter ----------

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    pub base_ms: u64,
    pub cap_ms: u64,
    pub use_jitter: bool,
    pub max_delay_ms: Option<u64>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            base_ms: 1_000,
            cap_ms: 300_000,
            use_jitter: true,
            max_delay_ms: None,
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RetryError {
    #[error("base_ms must be positive")]
    ZeroBase,
    #[error("cap_ms must be >= base_ms")]
    CapTooSmall,
    #[error("deadline overflow")]
    Overflow,
}

impl RetryPolicy {
    pub fn validate(&self) -> Result<(), RetryError> {
        if self.base_ms == 0 {
            return Err(RetryError::ZeroBase);
        }
        let effective_cap = self.effective_cap_ms();
        if effective_cap < self.base_ms {
            return Err(RetryError::CapTooSmall);
        }
        Ok(())
    }

    pub fn effective_cap_ms(&self) -> u64 {
        match self.max_delay_ms {
            Some(max) => self.cap_ms.min(max),
            None => self.cap_ms,
        }
    }
}

/// Saturating multiplication: a * 2^(exp-1)
fn saturating_double(base: u64, exp: u32) -> u64 {
    if exp == 0 || base == 0 {
        return base;
    }
    // base * 2^(exp-1)
    if exp > 64 {
        return u64::MAX;
    }
    let shift = exp - 1;
    if shift >= 64 || base > (u64::MAX >> shift) {
        u64::MAX
    } else {
        base << shift
    }
}

/// Compute retry delay in milliseconds for a given attempt.
/// For attempt >= 1:
///   ceiling = min(cap, saturating(base * 2^(attempt-1)))
///   lower = ceil(ceiling / 2)
///   span = ceiling - lower + 1
///   if jitter: rejection-sample offset in [0, span)
///   delay = lower + offset
/// Returns delay in ms.
pub fn retry_delay_ms(
    queue_id: &[u8; 16],
    job_id: &[u8; 16],
    attempt: u32,
    policy: &RetryPolicy,
) -> Result<u64, RetryError> {
    policy.validate()?;

    if attempt == 0 {
        return Ok(0);
    }

    let cap = policy.effective_cap_ms();
    let ceiling = cap.min(saturating_double(policy.base_ms, attempt));

    if !policy.use_jitter {
        return Ok(ceiling);
    }

    let lower = ceiling.div_ceil(2);
    let span = ceiling - lower + 1;

    // Rejection sampling
    let threshold = span.wrapping_neg() % span;
    let mut counter = 0u32;
    loop {
        let mut hasher = Sha256::new();
        hasher.update(b"SpoolQ-1-jitter\0");
        hasher.update(queue_id);
        hasher.update(job_id);
        hasher.update(attempt.to_be_bytes());
        hasher.update(counter.to_be_bytes());
        let result = hasher.finalize();
        let x = u64::from_be_bytes(result[..8].try_into().unwrap());

        if x >= threshold {
            let offset = x % span;
            return Ok(lower + offset);
        }
        counter += 1;
        // B4: Cap iterations to prevent theoretical infinite loop.
        // Fallback to the midpoint of the span, which is unbiased.
        if counter >= 64 {
            return Ok(lower + span / 2);
        }
    }
}

/// Compute the absolute wall deadline for retry.
/// Returns None if the deadline would exceed 2^63 - 1 ns from the Unix epoch.
pub fn retry_wall_deadline(effective_wall_floor_ns: u64, delay_ns: u64) -> Option<u64> {
    let deadline = checked_add_u64(effective_wall_floor_ns, delay_ns)?;
    if deadline > i64::MAX as u64 {
        return None;
    }
    Some(deadline)
}

// ---------- Wall watermark floor ----------

/// effective_wall_floor_ns = max(clock_realtime_ns, stored_bucket * bucket_width_ns).
/// Returns None if the watermark computation overflows (C-51).
pub fn effective_wall_floor(
    clock_realtime_ns: u64,
    stored_bucket: u64,
    bucket_width_ns: u64,
) -> Option<u64> {
    if bucket_width_ns == 0 {
        return None;
    }
    let watermark_ns = checked_mul_u64(stored_bucket, bucket_width_ns)?;
    Some(clock_realtime_ns.max(watermark_ns))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_arithmetic() {
        let width = 10_000_000_000u64; // 10s
        assert_eq!(bucket_number(0, width), Some(0));
        assert_eq!(bucket_number(1, width), Some(0));
        assert_eq!(bucket_number(width, width), Some(1));
        assert_eq!(bucket_number(width + 1, width), Some(1));
        assert_eq!(bucket_number(2 * width, width), Some(2));
    }

    #[test]
    fn ceiling_bucket_test() {
        let width = 10_000_000_000u64;
        assert_eq!(ceiling_bucket(0, width), Some(0));
        assert_eq!(ceiling_bucket(1, width), Some(1));
        assert_eq!(ceiling_bucket(width, width), Some(1));
        assert_eq!(ceiling_bucket(width + 1, width), Some(2));
    }

    #[test]
    fn eligibility_rounding() {
        let width = 10_000_000_000u64;
        // Exact multiple stays at same bucket
        let (bucket, ns) = eligibility_bucket_and_ns(width, width).unwrap();
        assert_eq!(bucket, 1);
        assert_eq!(ns, width);

        // Non-multiple rounds up
        let (bucket2, ns2) = eligibility_bucket_and_ns(width + 1, width).unwrap();
        assert_eq!(bucket2, 2);
        assert_eq!(ns2, 2 * width);
    }

    #[test]
    fn retry_no_jitter() {
        let policy = RetryPolicy {
            base_ms: 1000,
            cap_ms: 300_000,
            use_jitter: false,
            max_delay_ms: None,
        };
        let qid = [1u8; 16];
        let jid = [2u8; 16];
        // attempt 1: ceiling = min(300_000, 1000*1) = 1000
        assert_eq!(retry_delay_ms(&qid, &jid, 1, &policy).unwrap(), 1000);
        // attempt 2: ceiling = min(300_000, 1000*2) = 2000
        assert_eq!(retry_delay_ms(&qid, &jid, 2, &policy).unwrap(), 2000);
        // attempt 3: ceiling = min(300_000, 1000*4) = 4000
        assert_eq!(retry_delay_ms(&qid, &jid, 3, &policy).unwrap(), 4000);
        // attempt 9: ceiling = min(300_000, 1000*256) = 256_000
        assert_eq!(retry_delay_ms(&qid, &jid, 9, &policy).unwrap(), 256_000);
        // attempt 10: ceiling = min(300_000, 1000*512) = 300_000 (capped)
        assert_eq!(retry_delay_ms(&qid, &jid, 10, &policy).unwrap(), 300_000);
    }

    #[test]
    fn retry_jitter_in_range() {
        let policy = RetryPolicy::default();
        let qid = [1u8; 16];
        let jid = [2u8; 16];

        for attempt in 1..=5 {
            let delay = retry_delay_ms(&qid, &jid, attempt, &policy).unwrap();
            let ceiling = policy
                .effective_cap_ms()
                .min(saturating_double(policy.base_ms, attempt));
            let lower = ceiling.div_ceil(2);
            assert!(
                (lower..=ceiling).contains(&delay),
                "attempt {attempt}: delay {delay} not in [{lower}, {ceiling}]"
            );
        }
    }

    #[test]
    fn retry_deterministic() {
        let policy = RetryPolicy::default();
        let qid = [1u8; 16];
        let jid = [2u8; 16];
        let d1 = retry_delay_ms(&qid, &jid, 3, &policy).unwrap();
        let d2 = retry_delay_ms(&qid, &jid, 3, &policy).unwrap();
        assert_eq!(d1, d2);
    }

    #[test]
    fn retry_zero_attempt() {
        let policy = RetryPolicy::default();
        let qid = [1u8; 16];
        let jid = [2u8; 16];
        assert_eq!(retry_delay_ms(&qid, &jid, 0, &policy).unwrap(), 0);
    }

    #[test]
    fn retry_validation() {
        let bad = RetryPolicy {
            base_ms: 0,
            ..Default::default()
        };
        assert_eq!(bad.validate(), Err(RetryError::ZeroBase));

        let bad2 = RetryPolicy {
            base_ms: 1000,
            cap_ms: 500,
            ..Default::default()
        };
        assert_eq!(bad2.validate(), Err(RetryError::CapTooSmall));
    }

    #[test]
    fn effective_wall_floor_test() {
        let width = 10_000_000_000u64;
        // Clock is ahead of watermark
        assert_eq!(effective_wall_floor(100, 5, width), Some(50_000_000_000));
        // Watermark is ahead of clock
        assert_eq!(effective_wall_floor(100, 0, width), Some(100));
    }

    #[test]
    fn bucket_arithmetic_zero_width_is_error() {
        // C-50: zero bucket width must return None, not panic
        assert_eq!(bucket_number(100, 0), None);
        assert_eq!(ceiling_bucket(100, 0), None);
    }

    #[test]
    fn effective_wall_floor_zero_width_is_error() {
        // C-50/C-51: zero width must return None
        assert_eq!(effective_wall_floor(100, 5, 0), None);
    }

    #[test]
    fn saturating_double_test() {
        assert_eq!(saturating_double(1000, 1), 1000); // 1000 * 2^0
        assert_eq!(saturating_double(1000, 2), 2000); // 1000 * 2^1
        assert_eq!(saturating_double(1000, 3), 4000); // 1000 * 2^2
        assert_eq!(saturating_double(1000, 4), 8000); // 1000 * 2^3
                                                      // Large shift saturates
                                                      // 1 << 63 is still valid
        assert_eq!(saturating_double(1, 64), 1u64 << 63);
        // exp > 64 saturates
        assert_eq!(saturating_double(1, 65), u64::MAX);
        assert_eq!(saturating_double(u64::MAX, 2), u64::MAX);
    }
}
