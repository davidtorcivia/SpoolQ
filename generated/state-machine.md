<!-- Source: spec/state-machine.json; SHA-256: 454b8dbb183e68d24608501f888bdecc6e9910ae43b675ee1d38af9fb859d3eb -->

# SteadQ/1 State Machine (Generated)

## Transitions

| Operation | Source | Source kind | Destination | Destination kind | Gen | Attempt | Token | Reason | Clock requirement | Required syncs | Linearization | Before failure | After failure | Resolver probes | Qualification |
|-----------|--------|-------------|-------------|------------------|-----|---------|-------|--------|-------------------|----------------|---------------|----------------|---------------|-----------------|---------------|
| enqueue_immediate | hidden | full_job | ready | full_job | zero | zero | none | none | authenticated_wall_floor | file_fsync, destination_dir_fsync | publish_noreplace | not_committed | outcome_unknown | destination_only | none |
| enqueue_delayed | hidden | full_job | delayed | full_job | zero | zero | none | none | authenticated_wall_floor | file_fsync, destination_dir_fsync | publish_noreplace | not_committed | outcome_unknown | destination_only | none |
| promote | delayed | full_job | ready | full_job | increment | unchanged | none | none | authenticated_wall_floor | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | source_and_destination | none |
| claim | ready | full_job | leased | full_job | increment | increment | new | none | boottime_and_authenticated_wall_floor | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | source_and_destination | none |
| exhausted_ready_cleanup | ready | full_job | dead | full_job | increment | unchanged | none | attempts_exhausted | authenticated_wall_floor | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | source_and_destination | none |
| renew | leased | full_job | leased | full_job | increment | unchanged | same | none | boottime_and_authenticated_wall_floor | same_or_destination_dir_fsync | rename_noreplace | not_committed | outcome_unknown | source_and_destination | none |
| acknowledge | leased | full_job | receipt | full_receipt | increment | unchanged | same | none | authenticated_wall_floor | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | receipt_candidates_and_source | none |
| retry_now | leased | full_job | ready | full_job | increment | unchanged | none | none | none | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | source_and_destination | none |
| retry_later | leased | full_job | delayed | full_job | increment | unchanged | none | none | authenticated_wall_floor | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | source_and_destination | none |
| bury | leased | full_job | dead | full_job | increment | unchanged | none | application_defined | authenticated_wall_floor | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | source_and_destination | none |
| reap_expired_to_ready | leased | full_job | ready | full_job | increment | unchanged | none | none | lease_expiration_evidence | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | source_and_destination | attempts_remaining |
| reap_expired_to_dead | leased | full_job | dead | full_job | increment | unchanged | none | attempts_exhausted | lease_expiration_evidence_and_authenticated_wall_floor | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | source_and_destination | attempts_exhausted |
| quarantine | active | raw_object | quarantine | raw_object | increment | unchanged | none | corruption | none | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | source_and_destination | raw_bytes_preserved |

## Exceptional mutations

| Operation | Source kind | Destination kind | Clock requirement | Class | Linearization | Required syncs | Before failure | After failure | Description |
|-----------|-------------|------------------|-------------------|-------|---------------|----------------|----------------|---------------|-------------|
| receipt_compaction | full_receipt | compact_receipt | none | replacing_move | rename_replace | file_fsync, same_or_destination_dir_fsync | not_committed | outcome_unknown | Terminal full-job receipt replaced by byte-deterministic compact receipt at same pathname |
| wall_watermark_advancement | watermark_record | watermark_record | authenticated_wall_floor | replacing_move | rename_replace | file_fsync, same_or_destination_dir_fsync | not_committed | outcome_unknown | Monotone wall-watermark record replaced under exclusive OFD lock |

## Administrative re-entry (creates new identity)

- **requeue_dead** (from dead): Verified resubmission: creates new job identity, copies payload and safe metadata, adds old job_id as provenance (creates new identity: true)
- **requeue_quarantine** (from quarantine): Verified resubmission after full structural and payload verification: creates new job identity (creates new identity: true)
