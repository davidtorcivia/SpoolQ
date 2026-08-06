# SpoolQ/1 Formal Model

TLA+ specification of the SpoolQ/1 queue protocol.

## Running

Requires the TLA+ toolkit (TLA+ Toolbox or command-line TLC).

```
java -jar tla2tools.jar model/SpoolQ.tla
```

## Model scope

Bounded configuration: 2 jobs, 2 workers, 2 crash events, MaxAttempts=2.

## Invariants checked

I1: No visible active object has an incomplete envelope (fileSynced must be true).

I9: Committed lease returns never exceed MaxAttempts.

I11: A delivered (leased) job has attempt >= 1.

## Crash model

The Crash action resets sync flags. In the strong profile, a leased job
whose file was synced stays leased (atomic rename persistence). A leased
job whose file was not synced rolls back to ready (claim never completed).
A ready job that was never synced or dir-synced rolls back to hidden.

## Not modeled

Full delayed scheduling, wall watermark, bucket creation, receipt compaction,
and quarantine are represented at the state level but not exhaustively
crash-tested in this bounded model. These will be added as the model is
refined.
