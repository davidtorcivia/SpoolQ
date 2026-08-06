/* SpoolQ/1 C ABI header */
#ifndef SPOOLQ_H
#define SPOOLQ_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>

#define SPOOLQ_OK              0
#define SPOOLQ_NOT_COMMITTED   1
#define SPOOLQ_INDETERMINATE   2
#define SPOOLQ_CORRUPTION      3
#define SPOOLQ_RESOURCE        4
#define SPOOLQ_PERMISSION      5
#define SPOOLQ_IO_FAILURE      6
#define SPOOLQ_UNSUPPORTED     64

typedef struct SpoolqQueue SpoolqQueue;
typedef struct SpoolqLease SpoolqLease;

typedef struct {
    uint8_t bytes[16];
} SpoolqJobId;

SpoolqQueue *spoolq_init(const char *path, unsigned int shard_count);
SpoolqQueue *spoolq_open(const char *path);
void spoolq_close(SpoolqQueue *queue);

int spoolq_enqueue(SpoolqQueue *queue,
                   const uint8_t *payload, size_t payload_len,
                   const char *content_type,
                   unsigned int max_attempts,
                   SpoolqJobId *job_id_out);

int spoolq_lease(SpoolqQueue *queue,
                 uint64_t lease_duration_ns,
                 SpoolqLease **lease_out);

int spoolq_ack(SpoolqQueue *queue, SpoolqLease *lease);
int spoolq_retry(SpoolqQueue *queue, SpoolqLease *lease);
int spoolq_bury(SpoolqQueue *queue, SpoolqLease *lease, unsigned int reason);
int spoolq_recover(SpoolqQueue *queue);

void spoolq_lease_job_id(const SpoolqLease *lease, SpoolqJobId *out);
uint64_t spoolq_lease_generation(const SpoolqLease *lease);
unsigned int spoolq_lease_attempt(const SpoolqLease *lease);
void spoolq_lease_free(SpoolqLease *lease);

#ifdef __cplusplus
}
#endif

#endif /* SPOOLQ_H */
