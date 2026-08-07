/* SpoolQ/1 C ABI test program - hermetic with unique temp dir */
#include "spoolq.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

/* Remove a directory tree recursively. */
static void rmrf(const char *path) {
    /* Best-effort cleanup for hermetic repeatability. */
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", path);
    /* This is a test program, so system() is acceptable here. */
    int rc = system(cmd);
    (void)rc;
}

int main(void) {
    /* P1-24: Use a unique temp directory and clean it up. */
    char tmpl[] = "/tmp/spoolq_capi_XXXXXX";
    char *d = mkdtemp(tmpl);
    if (!d) { fprintf(stderr, "mkdtemp failed\n"); return 1; }
    char path[512];
    snprintf(path, sizeof(path), "%s/queue", d);
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
    /* P1-24: Cleanup temp directory for repeatability. */
    rmrf(d);
    return 0;
}
