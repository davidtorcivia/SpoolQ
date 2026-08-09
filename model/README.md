# SteadQ/1 Formal Model

TLA+ specification of the SteadQ/1 queue protocol.

## Running

Requires Java 17 and the TLA+ toolkit.

```
java -cp /tmp/tla2tools.jar tlc2.TLC -config model/SteadQ.cfg model/SteadQ.tla -workers auto
```

Download `tla2tools.jar` from https://github.com/tlaplus/tlaplus/releases.

Local check (after `curl -fsSL .../tla2tools.jar -o /tmp/tla2tools.jar`):

```
java -cp /tmp/tla2tools.jar tlc2.TLC -config model/SteadQ.cfg model/SteadQ.tla
```

CI runs the same command in the `tla` job.

## Model scope

Bounded configuration: 2 jobs, 2 workers, and MaxAttempts=2. The `Crash` action is not count-bounded.

`model/SteadQProtocol.tla` is generated from the versioned protocol IR. It supplies the model's state values and complete transition, exceptional mutation, and re-entry metadata. The current actions still model only the bounded abstract behavior described below; generating the metadata does not make those actions implementation-complete.

## Invariants checked

I1: No visible active object has an incomplete envelope (fileSynced must be true).

I9: Committed lease returns never exceed MaxAttempts.

I11: A delivered (leased) job has attempt >= 1.

All three are conjoined as `Inv` in `model/SteadQ.cfg`.

## Fixes applied

Enqueue now sets `fileSynced` to `TRUE` before publish, so `I1` holds for `Ready`.
`Claim`, `Ack`, `RetryNow`, `Bury` carry a per-worker token `w \in Workers` and
require `token[j] = w` with `w \notin poisoned`. `Crash` preserves file
durability (`fileSynced[j]` stays `TRUE` if it was) and clears a stale lease
token when `Leased /\ ~fileSynced` rolls back to `Ready`. `ELSEIF` syntax
fixed to `ELSE IF`.

## Crash model

The Crash action preserves file content durability and resets volatile
directory sync flags. In the strong profile, a leased job whose file was
synced stays leased (atomic rename persistence). A leased job whose file
was not synced rolls back to ready (claim never completed) and its token
is cleared. A ready job that was never synced or dir-synced rolls back to
hidden.

## Not modeled

Full delayed scheduling, wall watermark, bucket creation, receipt compaction,
and quarantine are represented at the state level but not exhaustively
crash-tested in this bounded model. These will be added as the model is
refined.
