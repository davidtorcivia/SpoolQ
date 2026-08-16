// Init a queue or run one enqueue + lease + verify + ack.
// `init <dir>` then `run <dir>` so strace can exclude setup.
use std::path::PathBuf;

use steadq_core::{
    AckOutcome, CreateOptions, EnqueueInput, EnqueueOutcome, LeaseOutcome, OpenOptions, Queue,
};

fn main() {
    let mut args = std::env::args().skip(1);
    let cmd = args
        .next()
        .expect("usage: one_completed_job init|run|run-deferred <dir> [count]");
    let dir = PathBuf::from(args.next().expect("missing dir"));
    match cmd.as_str() {
        "init" => {
            std::fs::create_dir_all(&dir).expect("mkdir");
            Queue::init(&dir, &CreateOptions::default()).expect("init");
        }
        "run" => {
            let mut queue = Queue::open(&dir, &Default::default()).expect("open");
            complete(&mut queue);
        }
        "run-deferred" => {
            let count: u32 = args
                .next()
                .unwrap_or_else(|| "1".into())
                .parse()
                .expect("count");
            let mut queue = Queue::open(
                &dir,
                &OpenOptions {
                    deferred_dir_sync: true,
                    ..Default::default()
                },
            )
            .expect("open");
            for _ in 0..count {
                complete(&mut queue);
            }
            queue.sync().expect("sync");
        }
        other => panic!("unknown command {other}"),
    }
}

fn complete(queue: &mut Queue) {
    match queue.enqueue(EnqueueInput {
        maximum_attempts: 3,
        content_type: "text/plain".into(),
        payload: vec![0xAB; 64],
        ..Default::default()
    }) {
        EnqueueOutcome::Committed(_) | EnqueueOutcome::Deferred(_) => {}
        other => panic!("enqueue: {other:?}"),
    }
    let lease = match queue.lease(0, 30_000_000_000) {
        LeaseOutcome::Leased(lease) => lease,
        other => panic!("lease: {other:?}"),
    };
    queue.verify_lease_payload(&lease).expect("verify");
    match queue.ack(&lease) {
        AckOutcome::Acked => {}
        other => panic!("ack: {other:?}"),
    }
}
