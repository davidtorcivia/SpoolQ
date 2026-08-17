use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::time::Instant;
use steadq_core::{
    BatchAckOutcome, BatchEnqueueOutcome, BatchLeaseOutcome, CreateOptions, EnqueueInput,
    OpenOptions, Queue,
};
use tempfile::TempDir;

const MAX_CONTENTION_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

fn benchmark_tempdir() -> TempDir {
    match std::env::var_os("STEADQ_BENCH_ROOT") {
        Some(root) => TempDir::new_in(root).unwrap(),
        None => TempDir::new().unwrap(),
    }
}

fn pause_for_contention(started: std::time::Instant, operation: &str) {
    assert!(
        started.elapsed() < MAX_CONTENTION_WAIT,
        "{operation} remained contended for {MAX_CONTENTION_WAIT:?}"
    );
    std::thread::yield_now();
}

fn enqueue_pending(batch: &mut steadq_core::Batch<'_>, input: EnqueueInput) {
    let started = std::time::Instant::now();
    loop {
        match batch.enqueue(input.clone()) {
            BatchEnqueueOutcome::Pending(_) => return,
            BatchEnqueueOutcome::NotCommitted(_, steadq_core::Error::MaintenanceBusy) => {
                pause_for_contention(started, "batch enqueue");
            }
            outcome => panic!("batch enqueue did not become pending: {outcome:?}"),
        }
    }
}

fn ack_pending(batch: &mut steadq_core::Batch<'_>, lease: &steadq_core::LeaseInfo) {
    let started = std::time::Instant::now();
    loop {
        match batch.ack(lease) {
            BatchAckOutcome::Pending => return,
            BatchAckOutcome::NotCommitted(steadq_core::Error::MaintenanceBusy) => {
                pause_for_contention(started, "batch ack");
            }
            outcome => panic!("batch ack did not become pending: {outcome:?}"),
        }
    }
}

fn complete_lifecycles(queue: &mut Queue, payload: &[u8], jobs: usize) -> u64 {
    if jobs == 0 {
        return 0;
    }
    let mut enqueue_batch = queue.batch();
    for _ in 0..jobs {
        enqueue_pending(
            &mut enqueue_batch,
            EnqueueInput {
                maximum_attempts: 3,
                content_type: "x".to_string(),
                payload: payload.to_vec(),
                ..Default::default()
            },
        );
    }
    let enqueues = enqueue_batch.commit().expect("enqueue batch commit failed");
    assert_eq!(enqueues.committed_enqueues.len(), jobs);
    assert!(enqueues.outcome_unknown_enqueues.is_empty());

    let mut lifecycle_batch = queue.batch();
    for _ in 0..jobs {
        let lease = match lifecycle_batch.lease(30_000_000_000, 30_000_000_000) {
            BatchLeaseOutcome::Pending(lease) => lease,
            outcome => panic!("batch lease did not become pending: {outcome:?}"),
        };
        lifecycle_batch
            .verify_lease_payload(&lease)
            .expect("leased payload verification failed");
        ack_pending(&mut lifecycle_batch, &lease);
    }
    let completed = lifecycle_batch
        .commit()
        .expect("lease/ack batch commit failed");
    assert_eq!(completed.committed_leases, jobs);
    assert_eq!(completed.committed_acks, jobs);
    assert!(completed.outcome_unknown_leases.is_empty());
    assert!(completed.outcome_unknown_acks.is_empty());
    jobs as u64
}

fn bench_strict_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("strict_batch_completed");
    group.warm_up_time(std::time::Duration::from_secs(2));
    group.measurement_time(std::time::Duration::from_secs(10));
    group.sample_size(30);

    for payload_size in [64usize, 16384] {
        for batch_size in [1usize, 8, 32, 64, 128, 256] {
            for n_workers in [1usize, 4, 8] {
                let label = format!("{payload_size}B_batch{batch_size}_{n_workers}w");
                group.throughput(Throughput::Elements(batch_size as u64));
                group.bench_with_input(
                    BenchmarkId::from_parameter(&label),
                    &(payload_size, batch_size, n_workers),
                    |b, &(payload_size, batch_size, n_workers)| {
                        b.iter_custom(|iters| {
                            let tmp = benchmark_tempdir();
                            Queue::init(tmp.path(), &CreateOptions::default()).unwrap();
                            let payload = vec![0xABu8; payload_size];
                            let mut total_jobs = 0u64;
                            let start = Instant::now();
                            let handles: Vec<_> = (0..n_workers)
                                .filter_map(|worker| {
                                    let jobs = batch_size / n_workers
                                        + usize::from(worker < batch_size % n_workers);
                                    if jobs == 0 {
                                        return None;
                                    }
                                    let p = tmp.path().to_path_buf();
                                    let payload = payload.clone();
                                    Some(std::thread::spawn(move || {
                                        let mut queue = Queue::open(
                                            &p,
                                            &OpenOptions {
                                                allow_unsupported_fs: true,
                                                ..Default::default()
                                            },
                                        )
                                        .unwrap();
                                        let mut latencies = Vec::with_capacity(iters as usize);
                                        let mut completed = 0;
                                        for _ in 0..iters {
                                            let batch_start = Instant::now();
                                            completed +=
                                                complete_lifecycles(&mut queue, &payload, jobs);
                                            latencies.push(batch_start.elapsed().as_nanos() as u64);
                                        }
                                        (completed, latencies)
                                    }))
                                })
                                .collect();
                            let mut latencies = Vec::with_capacity(iters as usize * handles.len());
                            for handle in handles {
                                let (completed, mut worker_latencies) = handle.join().unwrap();
                                total_jobs += completed;
                                latencies.append(&mut worker_latencies);
                            }
                            let total_elapsed = start.elapsed();
                            assert_eq!(total_jobs, iters * batch_size as u64);
                            latencies.sort_unstable();
                            let p99_idx = (latencies.len() as f64 * 0.99) as usize;
                            let p99 = latencies.get(p99_idx).copied().unwrap_or(0);
                            eprintln!(
                                "strict_batch {}: jobs={} batch_size={} workers={} payload={}B total={:?} p99_worker_batch={}ns jobs/sec={:.0}",
                                label,
                                total_jobs,
                                batch_size,
                                n_workers,
                                payload_size,
                                total_elapsed,
                                p99,
                                total_jobs as f64 / total_elapsed.as_secs_f64()
                            );
                            total_elapsed
                        });
                    },
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench_strict_batch);
criterion_main!(benches);
