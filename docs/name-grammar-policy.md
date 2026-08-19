# Filename Grammar Evolution Policy

Status: prototype policy, not a stable compatibility promise.

The canonical filename grammar lives in [`spec/filenames.abnf`](../spec/filenames.abnf). The longest canonical name (leased) is 196 bytes of the 255-byte `NAME_MAX` bound, leaving 59 bytes of headroom; `steadq-names` pins both numbers in a test. This document states how that headroom may be spent and what old readers do when they see a name they do not recognize. It exists because the grammar already has one layout migration behind it (the `leased/` hierarchy was replaced by colocated names in `ready/<shard>/`, with recovery still walking the old layout), and retrofitting policy onto live queues is the expensive way to make this decision.

## Principles

1. The wire format is the filename. Any producer may write a name onto a queue an older reader scans, so a grammar change is a protocol change, not a refactor.
2. New fields append, never insert or reorder. Every existing field keeps its prefix letter and position in the dot-separated sequence. A reader that parses left to right can stop at the first component it does not recognize.
3. The 59-byte budget is shared, not per-field. A grammar revision that spends bytes must keep the longest name at or under 255 bytes including the `.k` name tag and extension, and the pinning test's numbers move with the revision in the same commit.
4. `NAME_MAX` is fixed by the filesystem profile; the policy never assumes it can grow.

## Adding a field

A future field enters a name as one new dot-separated component with a single unused prefix letter, placed after the last existing per-state component and before `.k`:

```abnf
; hypothetical future field, e.g. priority
ready-name = job-id ".g" generation ".a" attempt ".m" maximum-attempts
             [".p" priority]           ; appended, optional-position
             ".k" name-tag ".sqj"
```

Cost accounting: one prefix letter + one dot + fixed-width lowercase hex, chosen at declaration. Widths are fixed per field, never variable, so the headroom math stays a constant.

### Name tag interaction

The name tag authenticates the exact canonical name: `tag = SHA256(domain || queue_id || canonical_context)` where the context includes the full filename without tag or extension. A new field changes the hashed bytes, so a grammar revision MUST bump the tag context version together with the field: the domain string (`"SteadQ-1-name\0"`) gains a version suffix (for example `"SteadQ-1-name.v2\0"`), and the FORMAT record's minor version increments. Old tags never validate under the new context and vice versa, which is the desired fail-closed property: an old reader rejects v2 names as unauthenticated rather than misreading them.

### Old-reader behavior

Readers are versioned by FORMAT minor version and are required to check it on open. A reader that does not know a field treats the object as foreign, not corrupt: it leaves the file in place and reports it through fsck as a warning-class finding (`unrecognized_name_version`), never quarantining it. Producers must not write a newer grammar onto a queue whose FORMAT minor version predates it; `init`/`open` enforce this by refusing the write class outright. Mixed-version operation therefore has a defined, testable shape: old readers see inert warnings, new readers see old names as canonical v1.

## Directory layout changes

A layout change (as happened with `leased/`) is a separate axis from a filename change and follows recovery-driven migration: the new layout is written by all new transitions, the old layout is read by recovery until a retention horizon passes, and no in-place rename of existing objects is performed. Layout changes add nothing to the name grammar and spend none of the 59-byte budget.

## What this policy forbids

- Inserting, reordering, or repurposing existing components or prefix letters.
- Variable-width fields.
- Spending the budget without moving the pinned test in the same commit.
- A grammar revision without the corresponding tag-context version bump and FORMAT minor bump.
