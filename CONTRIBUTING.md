# Contributing

SteadQ changes are reviewed as protocol changes, even when the patch looks local.

Before modifying a transition, read `spec/contract.md`, `spec/state-machine.json`, `docs/traceability.md`, and the relevant entry in `docs/complexity-ledger.md`.

Every change must state:

1. Which protocol fact changes and where it is authoritative.
2. Which invalid states become impossible.
3. What happens at every failure point before and after linearization.
4. Which tests, model checks, or crash evidence cover it.
5. Which independent reviewer is qualified to approve the residual complexity.

Run:

```text
cargo xtask check
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

Do not add direct authoritative filesystem mutation outside the transition executor, caller-controlled protocol paths, lossy namespace handling, ignored mutation results, invalid public defaults, or new claims broader than the evidence.

ADR-0033 is not present in the audited snapshot. Changes that would freeze the protocol IR or a public authority-bearing API remain blocked until the accepted ADR is added and reconciled.
