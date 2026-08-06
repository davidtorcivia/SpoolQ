// SpoolQ/1 C ABI.
#![allow(clippy::not_unsafe_ptr_arg_deref)]
#![allow(clippy::unnecessary_cast)]
// Exposes the core queue operations through a stable C interface.
// Opaque handles wrap Rust types. All strings are null-terminated UTF-8.

use std::cell::Cell;

thread_local! {
    static LAST_ERROR: Cell<Option<&'static str>> = const { Cell::new(None) };
}

/// Set the last error message (thread-local).
#[allow(dead_code)]
fn set_last_error(msg: &'static str) {
    LAST_ERROR.with(|cell| cell.set(Some(msg)));
}

/// Get the last error message as a C string pointer.
#[no_mangle]
pub extern "C" fn spoolq_last_error() -> *const c_char {
    LAST_ERROR.with(|cell| {
        match cell.get() {
            Some(msg) => {
                // Leak a CString so it persists
                let cs = CString::new(msg).unwrap_or_else(|_| CString::new("error").unwrap());
                cs.into_raw()
            }
            None => std::ptr::null(),
        }
    })
}

/// Free a string returned by spoolq_last_error.
#[no_mangle]
pub extern "C" fn spoolq_free_string(s: *mut c_char) {
    if !s.is_null() {
        unsafe { drop(CString::from_raw(s)) };
    }
}

/// Query the ABI version.
#[no_mangle]
pub extern "C" fn spoolq_abi_version() -> c_uint {
    1
}

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_uint};
use std::ptr;

use spoolq_core::{
    CreateOptions, EnqueueInput, EnqueueOutcome, LeaseInfo, LeaseOutcome, OpenOptions, Queue,
    TransitionOutcome, WorkBudget,
};

/// Opaque queue handle.
#[repr(C)]
pub struct SpoolqQueue {
    inner: Queue,
}

/// Opaque lease handle.
#[repr(C)]
pub struct SpoolqLease {
    inner: LeaseInfo,
}

/// Result codes matching the spec exit codes.
#[allow(dead_code)]
pub const SPOOLQ_OK: c_int = 0;
#[allow(dead_code)]
pub const SPOOLQ_NOT_COMMITTED: c_int = 1;
#[allow(dead_code)]
pub const SPOOLQ_INDETERMINATE: c_int = 2;
#[allow(dead_code)]
pub const SPOOLQ_CORRUPTION: c_int = 3;
#[allow(dead_code)]
pub const SPOOLQ_RESOURCE_EXHAUSTED: c_int = 4;
#[allow(dead_code)]
pub const SPOOLQ_PERMISSION_DENIED: c_int = 5;
#[allow(dead_code)]
pub const SPOOLQ_IO_FAILURE: c_int = 6;
#[allow(dead_code)]
pub const SPOOLQ_UNSUPPORTED: c_int = 64;

/// Job ID (128 bits = 16 bytes).
#[repr(C)]
pub struct SpoolqJobId {
    pub bytes: [u8; 16],
}

/// Initialize a new queue.
/// Returns null on failure, or a queue handle on success.
#[no_mangle]
pub extern "C" fn spoolq_init(path: *const c_char, shard_count: c_uint) -> *mut SpoolqQueue {
    // B-08: Null check
    if path.is_null() {
        return ptr::null_mut();
    }
    let path = match unsafe { CStr::from_ptr(path) }.to_str() {
        Ok(s) => std::path::PathBuf::from(s),
        Err(_) => return ptr::null_mut(),
    };
    let opts = CreateOptions {
        shard_count,
        ..Default::default()
    };
    match Queue::init(&path, &opts) {
        Ok(_) => {
            match Queue::open(
                &path,
                &OpenOptions {
                    allow_unsupported_fs: true,
                    ..Default::default()
                },
            ) {
                Ok(q) => Box::into_raw(Box::new(SpoolqQueue { inner: q })),
                Err(_) => ptr::null_mut(),
            }
        }
        Err(_) => ptr::null_mut(),
    }
}

/// Open an existing queue.
#[no_mangle]
pub extern "C" fn spoolq_open(path: *const c_char) -> *mut SpoolqQueue {
    // B-08: Null check
    if path.is_null() {
        return ptr::null_mut();
    }
    let path = match unsafe { CStr::from_ptr(path) }.to_str() {
        Ok(s) => std::path::PathBuf::from(s),
        Err(_) => return ptr::null_mut(),
    };
    match Queue::open(
        &path,
        &OpenOptions {
            allow_unsupported_fs: true,
            ..Default::default()
        },
    ) {
        Ok(q) => Box::into_raw(Box::new(SpoolqQueue { inner: q })),
        Err(_) => ptr::null_mut(),
    }
}

/// Close a queue handle.
#[no_mangle]
pub extern "C" fn spoolq_close(queue: *mut SpoolqQueue) {
    // B-08: Null check
    if !queue.is_null() {
        unsafe { drop(Box::from_raw(queue)) };
    }
}

/// Enqueue a job.
/// Returns SPOOLQ_OK on success, error code on failure.
/// Fills job_id_out with the generated job ID.
#[no_mangle]
pub extern "C" fn spoolq_enqueue(
    queue: *mut SpoolqQueue,
    payload: *const u8,
    payload_len: usize,
    content_type: *const c_char,
    max_attempts: c_uint,
    job_id_out: *mut SpoolqJobId,
) -> c_int {
    // B-08: Null check queue pointer
    if queue.is_null() {
        return SPOOLQ_NOT_COMMITTED;
    }
    let queue = unsafe { &mut *queue };

    // B-08: Null payload with nonzero length is an error
    let payload = if payload.is_null() {
        if payload_len != 0 {
            return SPOOLQ_NOT_COMMITTED;
        }
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(payload, payload_len) }.to_vec()
    };

    // B-08: Handle null content_type
    let content_type = if content_type.is_null() {
        "application/octet-stream".to_string()
    } else {
        unsafe { CStr::from_ptr(content_type) }
            .to_str()
            .unwrap_or("application/octet-stream")
            .to_string()
    };

    let outcome = queue.inner.enqueue(EnqueueInput {
        maximum_attempts: max_attempts,
        content_type,
        payload,
        ..Default::default()
    });

    match outcome {
        EnqueueOutcome::Committed(ticket) => {
            if !job_id_out.is_null() {
                unsafe { (*job_id_out).bytes = ticket.job_id };
            }
            SPOOLQ_OK
        }
        EnqueueOutcome::NotCommitted(_, e) => match e {
            spoolq_core::Error::QueueCorrupt(_) => SPOOLQ_CORRUPTION,
            spoolq_core::Error::UnsupportedFilesystem | spoolq_core::Error::UnsupportedFormat => {
                SPOOLQ_UNSUPPORTED
            }
            spoolq_core::Error::PermissionDenied => SPOOLQ_PERMISSION_DENIED,
            spoolq_core::Error::ResourceExhausted | spoolq_core::Error::StateExhausted => {
                SPOOLQ_RESOURCE_EXHAUSTED
            }
            _ => SPOOLQ_NOT_COMMITTED,
        },
        EnqueueOutcome::OutcomeUnknown(ticket, _) => {
            if !job_id_out.is_null() {
                unsafe { (*job_id_out).bytes = ticket.job_id };
            }
            SPOOLQ_INDETERMINATE
        }
    }
}

/// Lease a job.
/// Returns SPOOLQ_OK on success with lease_out filled.
/// Returns SPOOLQ_NOT_COMMITTED if no jobs available (lease_out is null).
#[no_mangle]
pub extern "C" fn spoolq_lease(
    queue: *mut SpoolqQueue,
    lease_duration_ns: u64,
    lease_out: *mut *mut SpoolqLease,
) -> c_int {
    // B-08: Null check queue and lease_out
    if queue.is_null() || lease_out.is_null() {
        return SPOOLQ_NOT_COMMITTED;
    }
    let queue = unsafe { &mut *queue };
    match queue.inner.lease(0, lease_duration_ns) {
        LeaseOutcome::Leased(lease) => {
            unsafe { *lease_out = Box::into_raw(Box::new(SpoolqLease { inner: lease })) };
            SPOOLQ_OK
        }
        LeaseOutcome::Empty => {
            unsafe { *lease_out = ptr::null_mut() };
            SPOOLQ_NOT_COMMITTED
        }
        LeaseOutcome::NotCommitted(e) => match e {
            spoolq_core::Error::QueueCorrupt(_) => SPOOLQ_CORRUPTION,
            spoolq_core::Error::UnsupportedFilesystem | spoolq_core::Error::UnsupportedFormat => {
                SPOOLQ_UNSUPPORTED
            }
            spoolq_core::Error::PermissionDenied => SPOOLQ_PERMISSION_DENIED,
            spoolq_core::Error::ResourceExhausted | spoolq_core::Error::StateExhausted => {
                SPOOLQ_RESOURCE_EXHAUSTED
            }
            _ => SPOOLQ_NOT_COMMITTED,
        },
        LeaseOutcome::OutcomeUnknown(_) => SPOOLQ_INDETERMINATE,
    }
}

/// Acknowledge a lease.
#[no_mangle]
pub extern "C" fn spoolq_ack(queue: *mut SpoolqQueue, lease: *mut SpoolqLease) -> c_int {
    // B-08: Null checks
    if queue.is_null() || lease.is_null() {
        return SPOOLQ_NOT_COMMITTED;
    }
    let queue = unsafe { &mut *queue };
    let lease = unsafe { &*lease };
    match queue.inner.ack(&lease.inner) {
        spoolq_core::AckOutcome::Acked => SPOOLQ_OK,
        spoolq_core::AckOutcome::AlreadyAcked => SPOOLQ_OK,
        spoolq_core::AckOutcome::LeaseLost => SPOOLQ_NOT_COMMITTED,
        spoolq_core::AckOutcome::NotCommitted(e) => match e {
            spoolq_core::Error::QueueCorrupt(_) => SPOOLQ_CORRUPTION,
            spoolq_core::Error::PermissionDenied => SPOOLQ_PERMISSION_DENIED,
            spoolq_core::Error::ResourceExhausted => SPOOLQ_RESOURCE_EXHAUSTED,
            _ => SPOOLQ_NOT_COMMITTED,
        },
        spoolq_core::AckOutcome::OutcomeUnknown(_) => SPOOLQ_INDETERMINATE,
    }
}

/// Retry a lease immediately.
#[no_mangle]
pub extern "C" fn spoolq_retry(queue: *mut SpoolqQueue, lease: *mut SpoolqLease) -> c_int {
    // B-08: Null checks
    if queue.is_null() || lease.is_null() {
        return SPOOLQ_NOT_COMMITTED;
    }
    let queue = unsafe { &mut *queue };
    let lease = unsafe { &*lease };
    match queue.inner.retry_now(&lease.inner) {
        TransitionOutcome::Committed => SPOOLQ_OK,
        TransitionOutcome::LeaseLost => SPOOLQ_NOT_COMMITTED,
        TransitionOutcome::NotCommitted(e) => match e {
            spoolq_core::Error::QueueCorrupt(_) => SPOOLQ_CORRUPTION,
            spoolq_core::Error::PermissionDenied => SPOOLQ_PERMISSION_DENIED,
            spoolq_core::Error::ResourceExhausted => SPOOLQ_RESOURCE_EXHAUSTED,
            _ => SPOOLQ_NOT_COMMITTED,
        },
        TransitionOutcome::OutcomeUnknown(_) => SPOOLQ_INDETERMINATE,
    }
}

/// Bury a lease.
#[no_mangle]
pub extern "C" fn spoolq_bury(
    queue: *mut SpoolqQueue,
    lease: *mut SpoolqLease,
    reason: c_uint,
) -> c_int {
    // B-08: Null checks
    if queue.is_null() || lease.is_null() {
        return SPOOLQ_NOT_COMMITTED;
    }
    let queue = unsafe { &mut *queue };
    let lease = unsafe { &*lease };
    let reason = spoolq_core::DeadReason::from_u16(reason as u16)
        .unwrap_or(spoolq_core::DeadReason::Unspecified);
    match queue.inner.bury(&lease.inner, reason) {
        TransitionOutcome::Committed => SPOOLQ_OK,
        TransitionOutcome::LeaseLost => SPOOLQ_NOT_COMMITTED,
        TransitionOutcome::NotCommitted(e) => match e {
            spoolq_core::Error::QueueCorrupt(_) => SPOOLQ_CORRUPTION,
            spoolq_core::Error::PermissionDenied => SPOOLQ_PERMISSION_DENIED,
            spoolq_core::Error::ResourceExhausted => SPOOLQ_RESOURCE_EXHAUSTED,
            _ => SPOOLQ_NOT_COMMITTED,
        },
        TransitionOutcome::OutcomeUnknown(_) => SPOOLQ_INDETERMINATE,
    }
}

/// Run a recovery pass.
#[no_mangle]
pub extern "C" fn spoolq_recover(queue: *mut SpoolqQueue) -> c_int {
    // B-08: Null check
    if queue.is_null() {
        return SPOOLQ_NOT_COMMITTED;
    }
    let queue = unsafe { &mut *queue };
    queue.inner.recover(&WorkBudget::default());
    SPOOLQ_OK
}

/// Free a lease handle.
#[no_mangle]
pub extern "C" fn spoolq_lease_free(lease: *mut SpoolqLease) {
    if !lease.is_null() {
        unsafe { drop(Box::from_raw(lease)) };
    }
}

/// Get the job ID from a lease handle.
#[no_mangle]
pub extern "C" fn spoolq_lease_job_id(lease: *const SpoolqLease, out: *mut SpoolqJobId) {
    if lease.is_null() || out.is_null() {
        return;
    }
    let lease = unsafe { &*lease };
    unsafe { (*out).bytes = lease.inner.job_id };
}

/// Get the generation from a lease handle.
#[no_mangle]
pub extern "C" fn spoolq_lease_generation(lease: *const SpoolqLease) -> u64 {
    if lease.is_null() {
        return 0;
    }
    unsafe { (*lease).inner.generation }
}

/// Get the attempt from a lease handle.
#[no_mangle]
pub extern "C" fn spoolq_lease_attempt(lease: *const SpoolqLease) -> c_uint {
    if lease.is_null() {
        return 0;
    }
    unsafe { (*lease).inner.attempt as c_uint }
}
