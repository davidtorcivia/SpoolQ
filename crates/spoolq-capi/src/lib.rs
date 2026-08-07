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
    clear_last_error();
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
    clear_last_error();
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
    clear_last_error();
    if queue.is_null() {
        set_last_error("null queue");
        return SPOOLQ_NOT_COMMITTED;
    }
    let result = std::panic::catch_unwind(|| {
        let queue = unsafe { &*queue };
        let mut guard = match queue.inner.lock() {
            Ok(guard) => guard,
            Err(_) => {
                set_last_error("queue mutex poisoned (previous panic during operation)");
                return SPOOLQ_CORRUPTION;
            }
        };
        let payload = if payload.is_null() {
            if payload_len != 0 {
                set_last_error("null payload with nonzero length");
                return SPOOLQ_NOT_COMMITTED;
            }
            Vec::new()
        } else {
            unsafe { std::slice::from_raw_parts(payload, payload_len) }.to_vec()
        };
        let content_type = if content_type.is_null() {
            "application/octet-stream".to_string()
        } else {
            match unsafe { CStr::from_ptr(content_type) }.to_str() {
                Ok(s) => s.to_string(),
                Err(_) => {
                    set_last_error("invalid UTF-8 in content_type");
                    return SPOOLQ_NOT_COMMITTED;
                }
            }
        };
        match guard.enqueue(EnqueueInput {
            maximum_attempts: max_attempts,
            content_type,
            payload,
            ..Default::default()
        }) {
            EnqueueOutcome::Committed(ticket) => {
                if !job_id_out.is_null() {
                    unsafe { (*job_id_out).bytes = ticket.job_id };
                }
                SPOOLQ_OK
            }
            EnqueueOutcome::NotCommitted(_, e) => {
                let code = error_to_code(&e);
                set_last_error(&leak_error(e));
                code
            }
            EnqueueOutcome::OutcomeUnknown(ticket, e) => {
                if !job_id_out.is_null() {
                    unsafe { (*job_id_out).bytes = ticket.job_id };
                }
                set_last_error(&leak_error(e));
                SPOOLQ_INDETERMINATE
            }
        }
    });
    match result {
        Ok(code) => code,
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
    clear_last_error();
    if queue.is_null() || lease_out.is_null() {
        set_last_error("null queue or lease_out");
        return SPOOLQ_NOT_COMMITTED;
    }
    let result = std::panic::catch_unwind(|| {
        let queue = unsafe { &*queue };
        let mut guard = match queue.inner.lock() {
            Ok(guard) => guard,
            Err(_) => {
                set_last_error("queue mutex poisoned");
                return SPOOLQ_CORRUPTION;
            }
        };
        match guard.lease(0, lease_duration_ns) {
            LeaseOutcome::Leased(lease) => {
                unsafe { *lease_out = Box::into_raw(Box::new(SpoolqLease { inner: lease })) };
                SPOOLQ_OK
            }
            LeaseOutcome::Empty => {
                unsafe { *lease_out = ptr::null_mut() };
                SPOOLQ_NOT_COMMITTED
            }
            LeaseOutcome::NotCommitted(e) => {
                let code = error_to_code(&e);
                set_last_error(&leak_error(e));
                code
            }
            LeaseOutcome::OutcomeUnknown(_) => SPOOLQ_INDETERMINATE,
        }
    });
    match result {
        Ok(code) => code,
        Err(_) => {
            set_last_error("panic in spoolq_lease");
            SPOOLQ_IO_FAILURE
        }
    }
}

/// R4-FFI: See spoolq.h for documentation.
#[no_mangle]
pub extern "C" fn spoolq_lease_verify(queue: *mut SpoolqQueue, lease: *mut SpoolqLease) -> c_int {
    clear_last_error();
    if queue.is_null() || lease.is_null() {
        set_last_error("null queue or lease");
        return SPOOLQ_NOT_COMMITTED;
    }
    let result = std::panic::catch_unwind(|| {
        let queue = unsafe { &*queue };
        let guard = match queue.inner.lock() {
            Ok(guard) => guard,
            Err(_) => {
                set_last_error("queue mutex poisoned");
                return SPOOLQ_CORRUPTION;
            }
        };
        let lease_ref = unsafe { &mut *lease };
        match guard.verify_lease_payload(&lease_ref.inner) {
            Ok(()) => SPOOLQ_OK,
            Err(e) => {
                let code = error_to_code(&e);
                set_last_error(&leak_error(e));
                code
            }
        }
    });
    match result {
        Ok(code) => code,
        Err(_) => {
            set_last_error("panic in spoolq_lease_verify");
            SPOOLQ_IO_FAILURE
        }
    }
}

/// R4-FFI: See spoolq.h for documentation.
#[no_mangle]
pub extern "C" fn spoolq_ack(queue: *mut SpoolqQueue, lease: *mut SpoolqLease) -> c_int {
    clear_last_error();
    if queue.is_null() || lease.is_null() {
        set_last_error("null queue or lease");
        return SPOOLQ_NOT_COMMITTED;
    }
    let result = std::panic::catch_unwind(|| {
        let queue = unsafe { &*queue };
        let mut guard = match queue.inner.lock() {
            Ok(guard) => guard,
            Err(_) => {
                set_last_error("queue mutex poisoned");
                return SPOOLQ_CORRUPTION;
            }
        };
        let lease_ref = unsafe { &*lease };
        match guard.ack(&lease_ref.inner) {
            spoolq_core::AckOutcome::Acked => SPOOLQ_OK,
            spoolq_core::AckOutcome::AlreadyAcked => SPOOLQ_OK,
            spoolq_core::AckOutcome::LeaseLost => {
                set_last_error("lease lost");
                SPOOLQ_NOT_COMMITTED
            }
            spoolq_core::AckOutcome::NotCommitted(e) => {
                let code = error_to_code(&e);
                set_last_error(&leak_error(e));
                code
            }
            spoolq_core::AckOutcome::OutcomeUnknown(_) => SPOOLQ_INDETERMINATE,
        }
    });
    match result {
        Ok(code) => code,
        Err(_) => {
            set_last_error("panic in spoolq_ack");
            SPOOLQ_IO_FAILURE
        }
    }
}

/// R4-FFI: See spoolq.h for documentation.
#[no_mangle]
pub extern "C" fn spoolq_retry(queue: *mut SpoolqQueue, lease: *mut SpoolqLease) -> c_int {
    clear_last_error();
    if queue.is_null() || lease.is_null() {
        set_last_error("null queue or lease");
        return SPOOLQ_NOT_COMMITTED;
    }
    let result = std::panic::catch_unwind(|| {
        let queue = unsafe { &*queue };
        let mut guard = match queue.inner.lock() {
            Ok(guard) => guard,
            Err(_) => {
                set_last_error("queue mutex poisoned");
                return SPOOLQ_CORRUPTION;
            }
        };
        let lease_ref = unsafe { &*lease };
        match guard.retry_now(&lease_ref.inner) {
            TransitionOutcome::Committed => SPOOLQ_OK,
            TransitionOutcome::LeaseLost => {
                set_last_error("lease lost");
                SPOOLQ_NOT_COMMITTED
            }
            TransitionOutcome::NotCommitted(e) => {
                let code = error_to_code(&e);
                set_last_error(&leak_error(e));
                code
            }
            TransitionOutcome::OutcomeUnknown(_) => SPOOLQ_INDETERMINATE,
        }
    });
    match result {
        Ok(code) => code,
        Err(_) => {
            set_last_error("panic in spoolq_retry");
            SPOOLQ_IO_FAILURE
        }
    }
}

/// R4-FFI: See spoolq.h for documentation.
#[no_mangle]
pub extern "C" fn spoolq_bury(
    queue: *mut SpoolqQueue,
    lease: *mut SpoolqLease,
    reason: c_uint,
) -> c_int {
    clear_last_error();
    if queue.is_null() || lease.is_null() {
        set_last_error("null queue or lease");
        return SPOOLQ_NOT_COMMITTED;
    }
    let result = std::panic::catch_unwind(|| {
        let queue = unsafe { &*queue };
        let mut guard = match queue.inner.lock() {
            Ok(guard) => guard,
            Err(_) => {
                set_last_error("queue mutex poisoned");
                return SPOOLQ_CORRUPTION;
            }
        };
        let lease_ref = unsafe { &*lease };
        let reason = spoolq_core::DeadReason::from_u16(reason as u16)
            .unwrap_or(spoolq_core::DeadReason::Unspecified);
        match guard.bury(&lease_ref.inner, reason) {
            TransitionOutcome::Committed => SPOOLQ_OK,
            TransitionOutcome::LeaseLost => {
                set_last_error("lease lost");
                SPOOLQ_NOT_COMMITTED
            }
            TransitionOutcome::NotCommitted(e) => {
                let code = error_to_code(&e);
                set_last_error(&leak_error(e));
                code
            }
            TransitionOutcome::OutcomeUnknown(_) => SPOOLQ_INDETERMINATE,
        }
    });
    match result {
        Ok(code) => code,
        Err(_) => {
            set_last_error("panic in spoolq_bury");
            SPOOLQ_IO_FAILURE
        }
    }
}

/// Run a recovery pass.
#[no_mangle]
pub extern "C" fn spoolq_recover(queue: *mut SpoolqQueue) -> c_int {
    clear_last_error();
    if queue.is_null() {
        set_last_error("null queue");
        return SPOOLQ_NOT_COMMITTED;
    }
    let result = std::panic::catch_unwind(|| {
        let queue = unsafe { &*queue };
        let mut guard = match queue.inner.lock() {
            Ok(guard) => guard,
            Err(_) => {
                set_last_error("queue mutex poisoned (previous panic during operation)");
                return SPOOLQ_CORRUPTION;
            }
        };
        let stats = guard.recover(&WorkBudget::default());
        if stats.errors.is_empty() {
            0
        } else {
            1
        }
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

/// Get the payload length from a lease handle.
#[no_mangle]
pub extern "C" fn spoolq_lease_payload_length(lease: *const SpoolqLease) -> u64 {
    if lease.is_null() {
        return 0;
    }
    unsafe { (*lease).inner.payload_length }
}

/// Get the boot_id from a lease handle as a C string.
/// Returns pointer to thread-local storage. Do not free.
#[no_mangle]
pub extern "C" fn spoolq_lease_boot_id(
    lease: *const SpoolqLease,
    out: *mut c_char,
    out_len: usize,
) -> c_int {
    if lease.is_null() || out.is_null() || out_len == 0 {
        return SPOOLQ_NOT_COMMITTED;
    }
    let lease = unsafe { &*lease };
    let boot_id = &lease.inner.boot_id;
    let bytes = boot_id.as_bytes();
    if bytes.len() + 1 > out_len {
        return SPOOLQ_NOT_COMMITTED;
    }
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), out as *mut u8, bytes.len());
        *out.add(bytes.len()) = 0; // null terminator
    }
    SPOOLQ_OK
}

/// Get the content type from a lease handle as a C string.
/// Returns pointer to thread-local storage. Do not free.
#[no_mangle]
pub extern "C" fn spoolq_lease_content_type(
    lease: *const SpoolqLease,
    out: *mut c_char,
    out_len: usize,
) -> c_int {
    if lease.is_null() || out.is_null() || out_len == 0 {
        return SPOOLQ_NOT_COMMITTED;
    }
    let lease = unsafe { &*lease };
    let ct = &lease.inner.content_type;
    if ct.len() + 1 > out_len {
        return SPOOLQ_NOT_COMMITTED;
    }
    unsafe {
        std::ptr::copy_nonoverlapping(ct.as_ptr(), out as *mut u8, ct.len());
        *out.add(ct.len()) = 0;
    }
    SPOOLQ_OK
}

/// Get the source path from a lease handle as a C string.
/// Returns pointer to thread-local storage. Do not free.
#[no_mangle]
pub extern "C" fn spoolq_lease_source_path(
    lease: *const SpoolqLease,
    out: *mut c_char,
    out_len: usize,
) -> c_int {
    if lease.is_null() || out.is_null() || out_len == 0 {
        return SPOOLQ_NOT_COMMITTED;
    }
    let lease = unsafe { &*lease };
    let path = &lease.inner.exact_source_path;
    if path.len() + 1 > out_len {
        return SPOOLQ_NOT_COMMITTED;
    }
    unsafe {
        std::ptr::copy_nonoverlapping(path.as_ptr(), out as *mut u8, path.len());
        *out.add(path.len()) = 0;
    }
    SPOOLQ_OK
}

/// Format an error for the thread-local last-error store.
fn leak_error(e: Error) -> String {
    format!("{e}")
}
