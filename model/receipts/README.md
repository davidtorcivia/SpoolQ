# Receipt evidence model

`SteadQReceipts.tla` is a bounded model of strict acknowledgment, legacy or unverified receipt evidence, duplicate acknowledgment, verified compaction, authenticated retention deletion, and phase-aware operation outcomes. It imports the generated protocol IR and validates the exact acknowledge transition, receipt-compaction exception, and full and compact retention-unlink rows.

The model uses one receipt and six explicit classes: absent, unverified full, verified full, unverified compact, verified compact, and deleted. Initial states include absent and legacy full or compact evidence. Current protocol acknowledgment creates verified full evidence. Only verified full evidence can compact into verified compact evidence. Successful duplicate acknowledgment requires authenticated wall authority and existing verified evidence, then preserves that evidence class. Retention deletion requires an authenticated wall floor at or after the configured deadline.

Acknowledgment, compaction, and deletion each have distinct before-linearization `not_committed`, committed, and post-linearization `outcome_unknown` actions. A before-linearization failure preserves evidence. A committed or outcome-unknown action applies the same namespace evidence change because both occur after the operation's linearization point; the difference is whether every required barrier completed before return.

## Invariants checked

- `TypeInvariant`: every variable and generated receipt-operation definition remains in its bounded domain and exact protocol form.

- `VerifiedCompactRequiresVerifiedFull`: verified compact evidence can exist only after payload-verified full evidence.

- `ReceiptStateRemainsTerminal`: once receipt evidence exists, normal modeled actions cannot reactivate the job or return it to the absent state.

- `DuplicateAckPreservesEvidence`: successful duplicate acknowledgment requires authenticated wall authority and strict verified evidence, and cannot weaken, strengthen, or replace that evidence class.

- `RetentionDeletionUsesAuthenticatedEligibility`: deletion records authenticated wall evidence at or after the retention deadline.

- `LinearizationOutcomeMatchesEvidence`: committed and post-linearization outcome-unknown actions apply the operation's exact receipt-evidence transition.

- `NotCommittedPreservesReceiptEvidence`: a failure before linearization cannot manufacture, upgrade, downgrade, or delete receipt evidence.

- `UnverifiedEvidenceCannotSatisfyDuplicateAck`: legacy or unverified evidence cannot enter the successful duplicate-acknowledgment action.

- `UnverifiedEvidenceCannotBeCompacted`: unverified full evidence cannot take the verified compaction transition.

The configuration checks every predicate separately. The checksum-pinned TLA+ tools 1.7.4 artifact used by CI completes with 1,298 generated states and 350 distinct states, leaves no queued states, and reports no errors.

## Not modeled

The model does not establish payload hashing, record decoding, filename authentication, file locking, inode revalidation, syscall behavior, namespace durability, retention bucket arithmetic implementation, resolver implementation, or production equivalence. It models one receipt, wall floors from `0..3`, a retention deadline of `2`, and stuttering. Legacy or unverified classes are compatibility inputs; no current modeled operation creates or upgrades them.
