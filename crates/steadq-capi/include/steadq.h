/* SteadQ/1 C ABI header */
#ifndef STEADQ_H
#define STEADQ_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>

#define STEADQ_OK              0
#define STEADQ_NOT_COMMITTED   1
#define STEADQ_INDETERMINATE   2
#define STEADQ_CORRUPTION      3
#define STEADQ_RESOURCE        4
#define STEADQ_PERMISSION      5
#define STEADQ_IO_FAILURE      6
#define STEADQ_UNSUPPORTED     64

typedef struct SteadqQueue SteadqQueue;
typedef struct SteadqLease SteadqLease;

typedef struct {
    uint8_t bytes[16];
} SteadqJobId;

/* Thread safety: SteadqQueue is safe to share across C threads.
 * SteadqLease handles are NOT thread-safe; use from one thread at a time.
 * R4-FFI02: If a panic occurs during an operation, the mutex is poisoned
 * and subsequent calls return STEADQ_CORRUPTION. Reopen the queue to recover. */

SteadqQueue *steadq_init(const char *path, unsigned int shard_count);
SteadqQueue *steadq_open(const char *path);
void steadq_close(SteadqQueue *queue);

int steadq_enqueue(SteadqQueue *queue,
                   const uint8_t *payload, size_t payload_len,
                   const char *content_type,
                   unsigned int max_attempts,
                   SteadqJobId *job_id_out);

int steadq_lease(SteadqQueue *queue,
                 uint64_t lease_duration_ns,
                 SteadqLease **lease_out);

/* Verify a lease payload before acknowledgment.
 * Must be called before steadq_ack() for the safe acknowledgment path. */
int steadq_lease_verify(SteadqQueue *queue, SteadqLease *lease);

int steadq_ack(SteadqQueue *queue, SteadqLease *lease);
int steadq_retry(SteadqQueue *queue, SteadqLease *lease);
int steadq_bury(SteadqQueue *queue, SteadqLease *lease, unsigned int reason);
int steadq_recover(SteadqQueue *queue);

void steadq_lease_job_id(const SteadqLease *lease, SteadqJobId *out);
uint64_t steadq_lease_generation(const SteadqLease *lease);
unsigned int steadq_lease_attempt(const SteadqLease *lease);
uint64_t steadq_lease_payload_length(const SteadqLease *lease);

/* R4-FFI05: Lease metadata accessors. Copy string fields into caller buffer.
 * Returns STEADQ_OK on success, STEADQ_NOT_COMMITTED if buffer too small. */
int steadq_lease_boot_id(const SteadqLease *lease, char *out, size_t out_len);
int steadq_lease_content_type(const SteadqLease *lease, char *out, size_t out_len);
int steadq_lease_source_path(const SteadqLease *lease, char *out, size_t out_len);

void steadq_lease_free(SteadqLease *lease);

/* Last-error mechanism. Returns pointer to thread-local storage.
 * Valid until the next SteadQ call on the same thread. Do not free.
 * R4-FFI01: Error is cleared at the start of each operation. */
const char *steadq_last_error(void);
/* No-op kept for ABI compatibility. */
void steadq_free_string(const char *s);

/* ABI version query. */
unsigned int steadq_abi_version(void);

#ifdef __cplusplus
}
#endif

#endif /* STEADQ_H */
