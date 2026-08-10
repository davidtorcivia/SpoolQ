# Formal and empirical evidence scope

SteadQ is not formally verified, crash-certified, or production-certified.

## Current evidence

- The TLA+ model uses a bounded configuration of two jobs, one worker, two lease tokens, `MaxAttempts=2`, and `MaxGeneration=4`. The generation bound makes repeated renewal finite while permitting a retired capability to be presented against a later lease on the same job. The `Crash` action is not count-bounded.
- TLC checks `TypeInvariant`, `CompleteVisibleEnvelope`, `LeaseHasToken`, `TokenAuthorityRequiresLease`, `ActiveLeaseTokensAreUnique`, `RetiredTokenCannotMutate`, `OtherJobTokenCannotMutate`, `AttemptWithinLimit`, `ReceiptRemainsTerminal`, and `DeliveredAttemptIsPositive` separately. Their exact bounded meanings and omissions are documented in `model/README.md`; they are abstract predicates, not production-code equivalence evidence.
- The namespace durability model checks one abstract cross-directory no-overwrite move under ordered and weak crash profiles. It represents volatile and durable source and destination entries, pre-linearization and post-linearization failures, all five resolver observations, exact-identity both-same stabilization, and both-different conflict refusal. Its exact invariants, state counts, and omissions are documented in `model/namespace/README.md`.
- The scheduling model checks one delayed object, one receipt, and one current-boot lease over a bounded scalar time domain. It models authenticated wall-floor acquisition, realtime rollback, fail-closed wall-sensitive work, monotonic boottime expiration, and durable watermark history. Its exact invariants, state counts, and omissions are documented in `model/scheduling/README.md`.
- The in-process resolver observation harness injects failures and manufactures namespace observations in a temporary directory. It is not a storage power-loss test.
- The simulator models selected directory-entry durability behavior independently of the Linux executor.
- Fuzz CI uses bounded parser smoke runs.
- Pull-request mutation testing is diff-scoped; broad nightly mutation is advisory and excludes critical paths listed in `.cargo/mutants.toml`.
- No filesystem power-cut matrix has been independently run for this repository snapshot.

## Claim rule

Every release claim must name the exact model configuration, invariants checked, production coupling, filesystem/kernel/mount profile, injected phase set, and known omissions. Generic “formally verified,” “power-loss proven,” or “crash-certified” wording is prohibited until the corresponding release gates exist and pass.
