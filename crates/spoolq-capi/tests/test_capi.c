/* SpoolQ/1 C ABI test program */
#include "spoolq.h"
#include <stdio.h>
#include <string.h>

int main(void) {
    const char *path = "/tmp/spoolq_capi_test";
    SpoolqQueue *q = spoolq_init(path, 64);
    if (!q) { fprintf(stderr, "init failed\n"); return 1; }

    SpoolqJobId job_id;
    const char *payload = "hello from C";
    int rc = spoolq_enqueue(q, (const uint8_t *)payload, strlen(payload),
                            "text/plain", 3, &job_id);
    if (rc != SPOOLQ_OK) { fprintf(stderr, "enqueue failed: %d\n", rc); return 1; }
    printf("enqueued job\n");

    SpoolqLease *lease = NULL;
    rc = spoolq_lease(q, 30000000000ULL, &lease);
    if (rc != SPOOLQ_OK || !lease) { fprintf(stderr, "lease failed: %d\n", rc); return 1; }

    SpoolqJobId leased_id;
    spoolq_lease_job_id(lease, &leased_id);
    uint64_t gen = spoolq_lease_generation(lease);
    unsigned int attempt = spoolq_lease_attempt(lease);
    printf("leased: gen=%llu attempt=%u\n", (unsigned long long)gen, attempt);

    rc = spoolq_lease_verify(q, lease);
    if (rc != SPOOLQ_OK) { fprintf(stderr, "verify failed: %d\n", rc); return 1; }
    printf("payload verified\n");

    rc = spoolq_ack(q, lease);
    if (rc != SPOOLQ_OK) { fprintf(stderr, "ack failed: %d\n", rc); return 1; }
    printf("acked\n");

    spoolq_lease_free(lease);
    spoolq_close(q);
    printf("C ABI test passed\n");
    return 0;
}
