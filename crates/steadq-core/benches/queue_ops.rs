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

criterion_group!(
    benches,
    bench_enqueue,
    bench_lease_empty,
    bench_lease_hit,
    bench_ack
);
criterion_main!(benches);
