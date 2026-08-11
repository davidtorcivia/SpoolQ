use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::time::Instant;
use steadq_core::{CreateOptions, EnqueueInput, OpenOptions, Queue};
use tempfile::TempDir;

fn benchmark_tempdir() -> TempDir {
    match std::env::var_os("STEADQ_BENCH_ROOT") {
        Some(root) => TempDir::new_in(root).unwrap(),
        None => TempDir::new().unwrap(),
    }
}

fn bench_strict_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("strict_batch_completed");

    for payload_size in [64usize, 16384] {
        for batch_size in [1usize, 8, 32, 64] {
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
                            let mut latencies = Vec::with_capacity(iters as usize);
                            let start = Instant::now();
                            for _ in 0..iters {
                                let batch_start = Instant::now();
                                // Single queue for simplicity; workers would contend via separate Queue handles
                                // For multi-worker, spawn threads each with their own Queue handle
                                if n_workers == 1 {
                                    let mut q = Queue::open(
                                        tmp.path(),
                                        &OpenOptions {
                                            allow_unsupported_fs: true,
                                            ..Default::default()
                                        },
                                    )
                                    .unwrap();
                                    let mut batch = q.batch();
                                    for _ in 0..batch_size {
                                        let outcome = batch.enqueue(EnqueueInput {
                                            maximum_attempts: 3,
                                            content_type: "x".to_string(),
                                            payload: payload.clone(),
                                            ..Default::default()
                                        });
                                        match outcome {
                                            steadq_core::BatchEnqueueOutcome::Pending(_) => {}
                                            steadq_core::BatchEnqueueOutcome::NotCommitted(_, e) => {
                                                panic!("enqueue not committed: {e:?}")
                                            }
                                        }
                                    }
                                    let commit = batch.commit().expect("batch commit failed");
                                    assert_eq!(commit.committed_enqueues.len(), batch_size);
                                    // Now lease and ack the same jobs
                                    for _ in 0..batch_size {
                                        let lease = match q.lease(0, 30_000_000_000) {
                                            steadq_core::LeaseOutcome::Leased(l) => l,
                                            o => panic!("lease failed: {o:?}"),
                                        };
                                        q.verify_lease_payload(&lease).unwrap();
                                        match q.ack(&lease) {
                                            steadq_core::AckOutcome::Acked => {}
                                            o => panic!("ack failed: {o:?}"),
                                        }
                                    }
                                    total_jobs += batch_size as u64;
                                } else {
                                    // Multi-worker: each worker does batch_size / n_workers jobs per batch
                                    let per_worker = batch_size.div_ceil(n_workers);
                                    let handles: Vec<_> = (0..n_workers)
                                        .map(|_| {
                                            let p = tmp.path().to_path_buf();
                                            let payload = payload.clone();
                                            std::thread::spawn(move || {
                                                let mut q = Queue::open(
                                                    &p,
                                                    &OpenOptions {
                                                        allow_unsupported_fs: true,
                                                        ..Default::default()
                                                    },
                                                )
                                                .unwrap();
                                                let mut batch = q.batch();
                                                for _ in 0..per_worker {
                                                    let _ = batch.enqueue(EnqueueInput {
                                                        maximum_attempts: 3,
                                                        content_type: "x".to_string(),
                                                        payload: payload.clone(),
                                                        ..Default::default()
                                                    });
                                                }
                                                let _ = batch.commit().unwrap();
                                                // Lease and ack one by one (not batched for now)
                                                for _ in 0..per_worker {
                                                    if let steadq_core::LeaseOutcome::Leased(lease) =
                                                        q.lease(0, 30_000_000_000)
                                                    {
                                                        let _ = q.verify_lease_payload(&lease);
                                                        let _ = q.ack(&lease);
                                                    }
                                                }
                                            })
                                        })
                                        .collect();
                                    for h in handles {
                                        h.join().unwrap();
                                    }
                                    total_jobs += batch_size as u64;
                                }
                                let elapsed = batch_start.elapsed();
                                latencies.push(elapsed.as_nanos() as u64);
                            }
                            let total_elapsed = start.elapsed();
                            // Compute p99
                            latencies.sort_unstable();
                            let p99_idx = (latencies.len() as f64 * 0.99) as usize;
                            let p99 = latencies.get(p99_idx).copied().unwrap_or(0);
                            eprintln!(
                                "strict_batch {}: jobs={} batch_size={} workers={} payload={}B total={:?} p99_batch={}ns jobs/sec={:.0}",
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
