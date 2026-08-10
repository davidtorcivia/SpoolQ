use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use steadq_core::{CreateOptions, EnqueueInput, EnqueueOutcome, LeaseOutcome, OpenOptions, Queue};
use tempfile::TempDir;

fn bench_enqueue(c: &mut Criterion) {
    let mut group = c.benchmark_group("enqueue");
    for payload_size in [64usize, 1024, 16384, 65536].iter() {
        group.throughput(criterion::Throughput::Bytes(*payload_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(payload_size),
            payload_size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let tmp = TempDir::new().unwrap();
                        Queue::init(tmp.path(), &CreateOptions::default()).unwrap();
                        let q = Queue::open(
                            tmp.path(),
                            &OpenOptions {
                                allow_unsupported_fs: true,
                                ..Default::default()
                            },
                        )
                        .unwrap();
                        (tmp, q)
                    },
                    |(_tmp, mut q)| {
                        let payload = vec![0xABu8; size];
                        let outcome = q.enqueue(EnqueueInput {
                            maximum_attempts: 3,
                            content_type: "x".to_string(),
                            payload,
                            ..Default::default()
                        });
                        assert!(matches!(outcome, EnqueueOutcome::Committed(_)));
                        black_box(outcome);
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_lease_empty(c: &mut Criterion) {
    let mut group = c.benchmark_group("lease_empty");
    for shard_count in [16u32, 64, 256].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(shard_count),
            shard_count,
            |b, &sc| {
                b.iter_batched(
                    || {
                        let tmp = TempDir::new().unwrap();
                        Queue::init(
                            tmp.path(),
                            &CreateOptions {
                                shard_count: sc,
                                ..Default::default()
                            },
                        )
                        .unwrap();
                        let q = Queue::open(
                            tmp.path(),
                            &OpenOptions {
                                allow_unsupported_fs: true,
                                ..Default::default()
                            },
                        )
                        .unwrap();
                        (tmp, q)
                    },
                    |(_tmp, mut q)| {
                        let outcome = q.lease(0, 30_000_000_000);
                        assert!(matches!(outcome, LeaseOutcome::Empty));
                        black_box(outcome);
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_lease_hit(c: &mut Criterion) {
    let mut group = c.benchmark_group("lease_hit");
    for n_jobs in [1u32, 10, 100].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(n_jobs), n_jobs, |b, &n| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    Queue::init(tmp.path(), &CreateOptions::default()).unwrap();
                    let mut q = Queue::open(
                        tmp.path(),
                        &OpenOptions {
                            allow_unsupported_fs: true,
                            ..Default::default()
                        },
                    )
                    .unwrap();
                    for _ in 0..n {
                        q.enqueue(EnqueueInput {
                            maximum_attempts: 3,
                            content_type: "x".to_string(),
                            payload: b"data".to_vec(),
                            ..Default::default()
                        });
                    }
                    (tmp, q)
                },
                |(_tmp, mut q)| {
                    let outcome = q.lease(0, 30_000_000_000);
                    assert!(matches!(outcome, LeaseOutcome::Leased(_)));
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_ack(c: &mut Criterion) {
    c.bench_function("ack", |b| {
        b.iter_batched(
            || {
                let tmp = TempDir::new().unwrap();
                Queue::init(tmp.path(), &CreateOptions::default()).unwrap();
                let mut q = Queue::open(
                    tmp.path(),
                    &OpenOptions {
                        allow_unsupported_fs: true,
                        ..Default::default()
                    },
                )
                .unwrap();
                q.enqueue(EnqueueInput {
                    maximum_attempts: 3,
                    content_type: "x".to_string(),
                    payload: b"data".to_vec(),
                    ..Default::default()
                });
                let lease = match q.lease(0, 30_000_000_000) {
                    LeaseOutcome::Leased(l) => l,
                    _ => panic!("lease failed"),
                };
                (tmp, q, lease)
            },
            |(_tmp, mut q, lease)| {
                q.verify_lease_payload(&lease).unwrap();
                let outcome = q.ack(&lease);
                assert!(matches!(outcome, steadq_core::AckOutcome::Acked));
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_sustained_enqueue(c: &mut Criterion) {
    let mut group = c.benchmark_group("sustained_enqueue");
    for payload_size in [64usize, 1024, 16384].iter() {
        group.throughput(criterion::Throughput::Bytes(*payload_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(payload_size),
            payload_size,
            |b, &size| {
                let tmp = TempDir::new().unwrap();
                Queue::init(tmp.path(), &CreateOptions::default()).unwrap();
                let mut q = Queue::open(
                    tmp.path(),
                    &OpenOptions {
                        allow_unsupported_fs: true,
                        ..Default::default()
                    },
                )
                .unwrap();
                let payload = vec![0xABu8; size];
                b.iter(|| {
                    let outcome = q.enqueue(EnqueueInput {
                        maximum_attempts: 3,
                        content_type: "x".to_string(),
                        payload: payload.clone(),
                        ..Default::default()
                    });
                    assert!(matches!(outcome, EnqueueOutcome::Committed(_)));
                    black_box(outcome);
                });
            },
        );
    }
    group.finish();
}

fn bench_sustained_ack(c: &mut Criterion) {
    c.bench_function("sustained_ack", |b| {
        let tmp = TempDir::new().unwrap();
        Queue::init(tmp.path(), &CreateOptions::default()).unwrap();
        let mut q = Queue::open(
            tmp.path(),
            &OpenOptions {
                allow_unsupported_fs: true,
                ..Default::default()
            },
        )
        .unwrap();
        b.iter(|| {
            q.enqueue(EnqueueInput {
                maximum_attempts: 3,
                content_type: "x".to_string(),
                payload: b"data".to_vec(),
                ..Default::default()
            });
            let lease = match q.lease(0, 30_000_000_000) {
                LeaseOutcome::Leased(l) => l,
                _ => panic!("lease failed"),
            };
            q.verify_lease_payload(&lease).unwrap();
            q.ack(&lease);
        });
    });
}

fn bench_sustained_completed(c: &mut Criterion) {
    let mut group = c.benchmark_group("sustained_completed");
    for payload_size in [64usize, 1024, 16384].iter() {
        group.throughput(criterion::Throughput::Bytes(*payload_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(payload_size),
            payload_size,
            |b, &size| {
                let tmp = TempDir::new().unwrap();
                Queue::init(tmp.path(), &CreateOptions::default()).unwrap();
                let mut q = Queue::open(
                    tmp.path(),
                    &OpenOptions {
                        allow_unsupported_fs: true,
                        ..Default::default()
                    },
                )
                .unwrap();
                let payload = vec![0xABu8; size];
                b.iter(|| {
                    // Enqueue
                    q.enqueue(EnqueueInput {
                        maximum_attempts: 3,
                        content_type: "x".to_string(),
                        payload: payload.clone(),
                        ..Default::default()
                    });
                    // Lease
                    let lease = match q.lease(0, 30_000_000_000) {
                        LeaseOutcome::Leased(l) => l,
                        _ => panic!("lease failed in completed benchmark"),
                    };
                    // Verify + Ack (full payload verification)
                    q.verify_lease_payload(&lease).unwrap();
                    q.ack(&lease);
                });
            },
        );
    }
    group.finish();
}

fn bench_concurrent_completed(c: &mut Criterion) {
    use std::thread;

    let mut group = c.benchmark_group("concurrent_completed");
    for &payload_size in &[64usize, 16384] {
        for &n_threads in &[1u32, 4, 8] {
            group.throughput(criterion::Throughput::Elements(1));
            let label = format!("{payload_size}B_{n_threads}t");
            group.bench_with_input(
                BenchmarkId::from_parameter(label),
                &(payload_size, n_threads),
                |b, &(size, n)| {
                    b.iter_custom(|iters| {
                        let tmp = TempDir::new().unwrap();
                        Queue::init(tmp.path(), &CreateOptions::default()).unwrap();
                        let path = tmp.path().to_path_buf();
                        let start = std::time::Instant::now();

                        let handles: Vec<_> = (0..n)
                            .map(|_| {
                                let p = path.clone();
                                thread::spawn(move || {
                                    let queue = Queue::open(
                                        &p,
                                        &OpenOptions {
                                            allow_unsupported_fs: true,
                                            ..Default::default()
                                        },
                                    )
                                    .unwrap();
                                    let mut queue = queue;
                                    let payload = vec![0xABu8; size];
                                    for _ in 0..iters {
                                        queue.enqueue(EnqueueInput {
                                            maximum_attempts: 3,
                                            content_type: "x".to_string(),
                                            payload: payload.clone(),
                                            ..Default::default()
                                        });
                                        if let LeaseOutcome::Leased(lease) =
                                            queue.lease(0, 30_000_000_000)
                                        {
                                            let _ = queue.verify_lease_payload(&lease);
                                            let _ = queue.ack(&lease);
                                        }
                                    }
                                })
                            })
                            .collect();
                        for h in handles {
                            h.join().unwrap();
                        }
                        let elapsed = start.elapsed();
                        drop(tmp);
                        elapsed
                    });
                },
            );
        }
    }
    group.finish();
}

fn bench_deferred_completed(c: &mut Criterion) {
    let mut group = c.benchmark_group("deferred_completed");
    for &payload_size in &[64usize, 16384] {
        group.throughput(criterion::Throughput::Bytes(payload_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(payload_size),
            &payload_size,
            |b, &size| {
                let tmp = TempDir::new().unwrap();
                Queue::init(tmp.path(), &CreateOptions::default()).unwrap();
                let mut q = Queue::open(
                    tmp.path(),
                    &OpenOptions {
                        allow_unsupported_fs: true,
                        deferred_dir_sync: true,
                        ..Default::default()
                    },
                )
                .unwrap();
                let payload = vec![0xABu8; size];
                b.iter(|| {
                    q.enqueue(EnqueueInput {
                        maximum_attempts: 3,
                        content_type: "x".to_string(),
                        payload: payload.clone(),
                        ..Default::default()
                    });
                    let lease = match q.lease(0, 30_000_000_000) {
                        LeaseOutcome::Leased(l) => l,
                        _ => panic!("lease failed"),
                    };
                    q.verify_lease_payload(&lease).unwrap();
                    q.ack(&lease);
                    q.sync().unwrap();
                });
            },
        );
    }
    group.finish();
}

fn bench_batch_deferred(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_deferred");
    for &batch_size in &[1u32, 10, 50] {
        group.throughput(criterion::Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &n| {
                let tmp = TempDir::new().unwrap();
                Queue::init(tmp.path(), &CreateOptions::default()).unwrap();
                let mut q = Queue::open(
                    tmp.path(),
                    &OpenOptions {
                        allow_unsupported_fs: true,
                        deferred_dir_sync: true,
                        ..Default::default()
                    },
                )
                .unwrap();
                b.iter(|| {
                    for _ in 0..n {
                        q.enqueue(EnqueueInput {
                            maximum_attempts: 3,
                            content_type: "x".to_string(),
                            payload: b"data".to_vec(),
                            ..Default::default()
                        });
                    }
                    q.sync().unwrap();
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_enqueue,
    bench_lease_empty,
    bench_lease_hit,
    bench_ack,
    bench_sustained_enqueue,
    bench_sustained_ack,
    bench_concurrent_throughput,
    bench_sustained_completed,
    bench_concurrent_completed,
    bench_deferred_completed,
    bench_batch_deferred,
);
criterion_main!(benches);

fn bench_concurrent_throughput(c: &mut Criterion) {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;

    let mut group = c.benchmark_group("concurrent");
    for n_threads in [1u32, 2, 4, 8].iter() {
        group.throughput(criterion::Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::from_parameter(n_threads),
            n_threads,
            |b, &n| {
                b.iter_custom(|iters| {
                    let tmp = TempDir::new().unwrap();
                    Queue::init(tmp.path(), &CreateOptions::default()).unwrap();
                    let path = tmp.path().to_path_buf();
                    let total = Arc::new(AtomicUsize::new(0));
                    let start = std::time::Instant::now();

                    let handles: Vec<_> = (0..n)
                        .map(|_| {
                            let p = path.clone();
                            let t = total.clone();
                            thread::spawn(move || {
                                let queue = Queue::open(
                                    &p,
                                    &OpenOptions {
                                        allow_unsupported_fs: true,
                                        ..Default::default()
                                    },
                                )
                                .unwrap();
                                let mut queue = queue;
                                let mut local = 0usize;
                                for _ in 0..iters {
                                    if matches!(
                                        queue.enqueue(EnqueueInput {
                                            maximum_attempts: 3,
                                            content_type: "x".to_string(),
                                            payload: b"data".to_vec(),
                                            ..Default::default()
                                        }),
                                        EnqueueOutcome::Committed(_)
                                    ) {
                                        local += 1;
                                    }
                                }
                                t.fetch_add(local, Ordering::Relaxed);
                            })
                        })
                        .collect();
                    for h in handles {
                        h.join().unwrap();
                    }
                    let elapsed = start.elapsed();
                    // Keep temp dir alive for the measurement
                    drop(tmp);
                    elapsed
                });
            },
        );
    }
    group.finish();
}
