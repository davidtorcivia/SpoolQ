// SpoolQ/1 C ABI.
// R2-B07: Queue handle is wrapped in a Mutex for thread safety.
// R2-B07: All FFI functions catch panics to prevent process termination.
#![allow(clippy::not_unsafe_ptr_arg_deref)]
#![allow(clippy::unnecessary_cast)]

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_uint};
use std::ptr;
use std::sync::Mutex;

use spoolq_core::{
    CreateOptions, EnqueueInput, EnqueueOutcome, Error, LeaseInfo, LeaseOutcome, OpenOptions,
    Queue, TransitionOutcome, WorkBudget,
};

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
    static LAST_ERROR_RET: RefCell<Option<CString>> = const { RefCell::new(None) };
}

#[allow(dead_code)]
fn clear_last_error() {
    LAST_ERROR.with(|cell| {
        *cell.borrow_mut() = None;
    });
}

fn set_last_error(msg: &str) {
    LAST_ERROR.with(|cell| {
        *cell.borrow_mut() = CString::new(msg).ok();
    });
}

/// R2-H17: Centralized error-to-code mapping.
fn error_to_code(e: &Error) -> c_int {
    match e {
        Error::QueueCorrupt(_) => SPOOLQ_CORRUPTION,
        Error::PayloadCorrupt => SPOOLQ_CORRUPTION,
        Error::UnsupportedFilesystem | Error::UnsupportedFormat => SPOOLQ_UNSUPPORTED,
        Error::PermissionDenied => SPOOLQ_PERMISSION_DENIED,
        Error::ResourceExhausted | Error::StateExhausted => SPOOLQ_RESOURCE_EXHAUSTED,
        Error::IoFailure(_) => SPOOLQ_IO_FAILURE,
        Error::InvalidClock => SPOOLQ_IO_FAILURE,
        Error::MaintenanceBusy => SPOOLQ_NOT_COMMITTED,
        Error::QueuePoisoned(_) => SPOOLQ_CORRUPTION,
        Error::NotCommitted(_) => SPOOLQ_NOT_COMMITTED,
        Error::IdentityCollision => SPOOLQ_NOT_COMMITTED,
        Error::InvalidInput(_) => SPOOLQ_NOT_COMMITTED,
    }
}

/// Result codes matching the spec exit codes.
pub const SPOOLQ_OK: c_int = 0;
pub const SPOOLQ_NOT_COMMITTED: c_int = 1;
pub const SPOOLQ_INDETERMINATE: c_int = 2;
pub const SPOOLQ_CORRUPTION: c_int = 3;
pub const SPOOLQ_RESOURCE_EXHAUSTED: c_int = 4;
pub const SPOOLQ_PERMISSION_DENIED: c_int = 5;
pub const SPOOLQ_IO_FAILURE: c_int = 6;
pub const SPOOLQ_UNSUPPORTED: c_int = 64;

/// Opaque queue handle. R2-B07: Wrapped in Mutex for thread safety.
#[repr(C)]
pub struct SpoolqQueue {
    inner: Mutex<Queue>,
}

/// Opaque lease handle.
#[repr(C)]
pub struct SpoolqLease {
    inner: LeaseInfo,
}

/// Job ID (128 bits = 16 bytes).
#[repr(C)]
pub struct SpoolqJobId {
    pub bytes: [u8; 16],
}

/// C1: Get last error as a C string. Returns pointer to thread-local storage.
/// The pointer is valid until the next SpoolQ call on the same thread.
/// Do not free.
#[no_mangle]
pub extern "C" fn spoolq_last_error() -> *const c_char {
    LAST_ERROR.with(|cell| {
        let b = cell.borrow();
        b.as_ref().map_or(ptr::null(), |cs| cs.as_ptr())
    })
}

/// C1: No-op. spoolq_last_error() returns thread-local storage that does not
/// need to be freed. Provided for ABI compatibility.
#[no_mangle]
pub extern "C" fn spoolq_free_string(_s: *const c_char) {
    // No-op: last error uses thread-local storage, not heap allocation.
}

/// Query the ABI version.
#[no_mangle]
pub extern "C" fn spoolq_abi_version() -> c_uint {
    1
}

/// Initialize a new queue. Returns null on failure.
#[no_mangle]
pub extern "C" fn spoolq_init(path: *const c_char, shard_count: c_uint) -> *mut SpoolqQueue {
    if path.is_null() {
        set_last_error("null path");
        return ptr::null_mut();
    }
    let result = std::panic::catch_unwind(|| {
        let path_str = match unsafe { CStr::from_ptr(path) }.to_str() {
            Ok(s) => s,
            Err(_) => return Err(Error::InvalidInput("invalid UTF-8 path".into())),
        };
        let opts = CreateOptions {
            shard_count,
            ..Default::default()
        };
        match Queue::init(std::path::Path::new(path_str), &opts) {
            Ok(_) => {}
            Err(e) => return Err(Error::IoFailure(e.to_string())),
        }
        Queue::open(std::path::Path::new(path_str), &OpenOptions::default())
    });
    match result {
        Ok(Ok(q)) => Box::into_raw(Box::new(SpoolqQueue {
            inner: Mutex::new(q),
        })),
        Ok(Err(e)) => {
            set_last_error(&leak_error(e));
            ptr::null_mut()
        }
        Err(_) => {
            set_last_error("panic in spoolq_init");
            ptr::null_mut()
        }
    }
}

/// Open an existing queue. Returns null on failure.
#[no_mangle]
pub extern "C" fn spoolq_open(path: *const c_char) -> *mut SpoolqQueue {
    if path.is_null() {
        set_last_error("null path");
        return ptr::null_mut();
    }
    let result = std::panic::catch_unwind(|| {
        let path_str = match unsafe { CStr::from_ptr(path) }.to_str() {
            Ok(s) => s,
            Err(_) => return Err(Error::InvalidInput("invalid UTF-8 path".into())),
        };
        Queue::open(std::path::Path::new(path_str), &OpenOptions::default())
    });
    match result {
        Ok(Ok(q)) => Box::into_raw(Box::new(SpoolqQueue {
            inner: Mutex::new(q),
        })),
        Ok(Err(e)) => {
            set_last_error(&leak_error(e));
            ptr::null_mut()
        }
        Err(_) => {
            set_last_error("panic in spoolq_open");
            ptr::null_mut()
        }
    }
}

/// Close a queue handle.
#[no_mangle]
pub extern "C" fn spoolq_close(queue: *mut SpoolqQueue) {
    if !queue.is_null() {
        unsafe { drop(Box::from_raw(queue)) };
    }
}

/// Enqueue a job.
#[no_mangle]
pub extern "C" fn spoolq_enqueue(
    queue: *mut SpoolqQueue,
    payload: *const u8,
    payload_len: usize,
    content_type: *const c_char,
    max_attempts: c_uint,
    job_id_out: *mut SpoolqJobId,
) -> c_int {
    if queue.is_null() {
        set_last_error("null queue");
        return SPOOLQ_NOT_COMMITTED;
    }
    let result = std::panic::catch_unwind(|| {
        let queue = unsafe { &*queue };
        let mut guard = queue.inner.lock().unwrap_or_else(|e| e.into_inner());
        let payload = if payload.is_null() {
            if payload_len != 0 {
                return Err(("null payload with nonzero length", SPOOLQ_NOT_COMMITTED));
            }
            Vec::new()
        } else {
            unsafe { std::slice::from_raw_parts(payload, payload_len) }.to_vec()
        };
        let content_type = if content_type.is_null() {
            "application/octet-stream".to_string()
        } else {
            unsafe { CStr::from_ptr(content_type) }
                .to_str()
                .unwrap_or("application/octet-stream")
                .to_string()
        };
        let outcome = guard.enqueue(EnqueueInput {
            maximum_attempts: max_attempts,
            content_type,
            payload,
            ..Default::default()
        });
        Ok::<_, (&str, c_int)>(outcome)
    });
    match result {
        Ok(Ok(EnqueueOutcome::Committed(ticket))) => {
            if !job_id_out.is_null() {
                unsafe { (*job_id_out).bytes = ticket.job_id };
            }
            SPOOLQ_OK
        }
        Ok(Ok(EnqueueOutcome::NotCommitted(_, e))) => {
            let code = error_to_code(&e);
            set_last_error(&leak_error(e));
            code
        }
        Ok(Ok(EnqueueOutcome::OutcomeUnknown(ticket, e))) => {
            if !job_id_out.is_null() {
                unsafe { (*job_id_out).bytes = ticket.job_id };
            }
            set_last_error(&leak_error(e));
            SPOOLQ_INDETERMINATE
        }
        Ok(Err((msg, code))) => {
            set_last_error(msg);
            code
        }
        Err(_) => {
            set_last_error("panic in spoolq_enqueue");
            SPOOLQ_IO_FAILURE
        }
    }
}

/// Lease a job.
#[no_mangle]
pub extern "C" fn spoolq_lease(
    queue: *mut SpoolqQueue,
    lease_duration_ns: u64,
    lease_out: *mut *mut SpoolqLease,
) -> c_int {
    if queue.is_null() || lease_out.is_null() {
        set_last_error("null queue or lease_out");
        return SPOOLQ_NOT_COMMITTED;
    }
    let result = std::panic::catch_unwind(|| {
        let queue = unsafe { &*queue };
        let mut guard = queue.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.lease(0, lease_duration_ns)
    });
    match result {
        Ok(LeaseOutcome::Leased(lease)) => {
            unsafe { *lease_out = Box::into_raw(Box::new(SpoolqLease { inner: lease })) };
            SPOOLQ_OK
        }
        Ok(LeaseOutcome::Empty) => {
            unsafe { *lease_out = ptr::null_mut() };
            SPOOLQ_NOT_COMMITTED
        }
        Ok(LeaseOutcome::NotCommitted(e)) => {
            let code = error_to_code(&e);
            set_last_error(&leak_error(e));
            code
        }
        Ok(LeaseOutcome::OutcomeUnknown(_)) => SPOOLQ_INDETERMINATE,
        Err(_) => {
            set_last_error("panic in spoolq_lease");
            SPOOLQ_IO_FAILURE
        }
    }
}

/// R2-B04: Verify a leased job's payload. Returns SPOOLQ_OK if verified.
/// After this call, spoolq_ack() will accept the lease.
#[no_mangle]
pub extern "C" fn spoolq_lease_verify(queue: *mut SpoolqQueue, lease: *mut SpoolqLease) -> c_int {
    if queue.is_null() || lease.is_null() {
        set_last_error("null queue or lease");
        return SPOOLQ_NOT_COMMITTED;
    }
    let result = std::panic::catch_unwind(|| {
        let queue = unsafe { &*queue };
        let guard = queue.inner.lock().unwrap_or_else(|e| e.into_inner());
        let lease_ref = unsafe { &mut *lease };
        guard.verify_lease_payload(&lease_ref.inner)
    });
    match result {
        Ok(Ok(verified)) => {
            let lease_ref = unsafe { &mut *lease };
            lease_ref.inner = verified;
            SPOOLQ_OK
        }
        Ok(Err(e)) => {
            let code = error_to_code(&e);
            set_last_error(&leak_error(e));
            code
        }
        Err(_) => {
            set_last_error("panic in spoolq_lease_verify");
            SPOOLQ_IO_FAILURE
        }
    }
}

/// Acknowledge a verified lease. R2-B04: Use strict ack().
#[no_mangle]
pub extern "C" fn spoolq_ack(queue: *mut SpoolqQueue, lease: *mut SpoolqLease) -> c_int {
    if queue.is_null() || lease.is_null() {
        set_last_error("null queue or lease");
        return SPOOLQ_NOT_COMMITTED;
    }
    let result = std::panic::catch_unwind(|| {
        let queue = unsafe { &*queue };
        let mut guard = queue.inner.lock().unwrap_or_else(|e| e.into_inner());
        let lease_ref = unsafe { &*lease };
        guard.ack(&lease_ref.inner)
    });
    match result {
        Ok(spoolq_core::AckOutcome::Acked) => SPOOLQ_OK,
        Ok(spoolq_core::AckOutcome::AlreadyAcked) => SPOOLQ_OK,
        Ok(spoolq_core::AckOutcome::LeaseLost) => {
            set_last_error("lease lost");
            SPOOLQ_NOT_COMMITTED
        }
        Ok(spoolq_core::AckOutcome::NotCommitted(e)) => {
            let code = error_to_code(&e);
            set_last_error(&leak_error(e));
            code
        }
        Ok(spoolq_core::AckOutcome::OutcomeUnknown(_)) => SPOOLQ_INDETERMINATE,
        Err(_) => {
            set_last_error("panic in spoolq_ack");
            SPOOLQ_IO_FAILURE
        }
    }
}

/// Retry a lease immediately.
#[no_mangle]
pub extern "C" fn spoolq_retry(queue: *mut SpoolqQueue, lease: *mut SpoolqLease) -> c_int {
    if queue.is_null() || lease.is_null() {
        set_last_error("null queue or lease");
        return SPOOLQ_NOT_COMMITTED;
    }
    let result = std::panic::catch_unwind(|| {
        let queue = unsafe { &*queue };
        let mut guard = queue.inner.lock().unwrap_or_else(|e| e.into_inner());
        let lease_ref = unsafe { &*lease };
        guard.retry_now(&lease_ref.inner)
    });
    match result {
        Ok(TransitionOutcome::Committed) => SPOOLQ_OK,
        Ok(TransitionOutcome::LeaseLost) => {
            set_last_error("lease lost");
            SPOOLQ_NOT_COMMITTED
        }
        Ok(TransitionOutcome::NotCommitted(e)) => {
            let code = error_to_code(&e);
            set_last_error(&leak_error(e));
            code
        }
        Ok(TransitionOutcome::OutcomeUnknown(_)) => SPOOLQ_INDETERMINATE,
        Err(_) => {
            set_last_error("panic in spoolq_retry");
            SPOOLQ_IO_FAILURE
        }
    }
}

/// Bury a lease.
#[no_mangle]
pub extern "C" fn spoolq_bury(
    queue: *mut SpoolqQueue,
    lease: *mut SpoolqLease,
    reason: c_uint,
) -> c_int {
    if queue.is_null() || lease.is_null() {
        set_last_error("null queue or lease");
        return SPOOLQ_NOT_COMMITTED;
    }
    let result = std::panic::catch_unwind(|| {
        let queue = unsafe { &*queue };
        let mut guard = queue.inner.lock().unwrap_or_else(|e| e.into_inner());
        let lease_ref = unsafe { &*lease };
        let reason = spoolq_core::DeadReason::from_u16(reason as u16)
            .unwrap_or(spoolq_core::DeadReason::Unspecified);
        guard.bury(&lease_ref.inner, reason)
    });
    match result {
        Ok(TransitionOutcome::Committed) => SPOOLQ_OK,
        Ok(TransitionOutcome::LeaseLost) => {
            set_last_error("lease lost");
            SPOOLQ_NOT_COMMITTED
        }
        Ok(TransitionOutcome::NotCommitted(e)) => {
            let code = error_to_code(&e);
            set_last_error(&leak_error(e));
            code
        }
        Ok(TransitionOutcome::OutcomeUnknown(_)) => SPOOLQ_INDETERMINATE,
        Err(_) => {
            set_last_error("panic in spoolq_bury");
            SPOOLQ_IO_FAILURE
        }
    }
}

/// Run a recovery pass.
#[no_mangle]
pub extern "C" fn spoolq_recover(queue: *mut SpoolqQueue) -> c_int {
    if queue.is_null() {
        set_last_error("null queue");
        return SPOOLQ_NOT_COMMITTED;
    }
    let result = std::panic::catch_unwind(|| {
        let queue = unsafe { &*queue };
        let mut guard = queue.inner.lock().unwrap_or_else(|e| e.into_inner());
        let stats = guard.recover(&WorkBudget::default());
        stats.errors.len()
    });
    match result {
        Ok(0) => SPOOLQ_OK,
        Ok(_) => {
            set_last_error("recovery completed with errors");
            SPOOLQ_IO_FAILURE
        }
        Err(_) => {
            set_last_error("panic in spoolq_recover");
            SPOOLQ_IO_FAILURE
        }
    }
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

/// Format an error for the thread-local last-error store.
fn leak_error(e: Error) -> String {
    format!("{e}")
}
