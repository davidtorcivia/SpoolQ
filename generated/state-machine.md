<!-- Source: spec/state-machine.json; SHA-256: 51784a8a723ff47b4cdc878dd1eecb55dd23ed71b2111ebfa661768a8e96b478 -->

# SteadQ/1 State Machine (Generated)

## Transitions

| Operation | Source | Destination | Gen | Attempt | Token | Reason | Clock requirement | Required syncs | Linearization | Before failure | After failure | Resolution | Notes |
|-----------|--------|-------------|-----|---------|-------|--------|-------------------|----------------|---------------|----------------|---------------|------------|-------|
| enqueue_immediate | hidden | ready | zero | zero | none | none | authenticated_wall_floor | file_fsync, destination_dir_fsync | publish_noreplace | not_committed | outcome_unknown | probe destination: observed = committed, absent = not committed | none |
| enqueue_delayed | hidden | delayed | zero | zero | none | none | authenticated_wall_floor | file_fsync, destination_dir_fsync | publish_noreplace | not_committed | outcome_unknown | probe destination: observed = committed, absent = not committed | none |
| promote | delayed | ready | increment | unchanged | none | none | authenticated_wall_floor | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | probe both: destination observed = committed, source only = not committed | none |
| claim | ready | leased | increment | increment | new | none | boottime_and_authenticated_wall_floor | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | probe both directories | none |
| exhausted_ready_cleanup | ready | dead | increment | unchanged | none | attempts_exhausted | authenticated_wall_floor | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | probe both | none |
| renew | leased | leased | increment | unchanged | same | none | boottime_and_authenticated_wall_floor | same_or_destination_dir_fsync | rename_noreplace | not_committed | outcome_unknown | probe destination: new generation observed = renewed, old gen observed = lease lost | none |
| acknowledge | leased | receipt | increment | unchanged | same | none | authenticated_wall_floor | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | probe receipt buckets by exact name | none |
| retry_now | leased | ready | increment | unchanged | none | none | none | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | probe both | none |
| retry_later | leased | delayed | increment | unchanged | none | none | authenticated_wall_floor | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | probe both | none |
| bury | leased | dead | increment | unchanged | none | application_defined | authenticated_wall_floor | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | probe both | none |
| reap_expired_to_ready | leased | ready | increment | unchanged | none | none | lease_expiration_evidence | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | probe both | attempt < maximum_attempts |
| reap_expired_to_dead | leased | dead | increment | unchanged | none | attempts_exhausted | lease_expiration_evidence_and_authenticated_wall_floor | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | probe both | attempt >= maximum_attempts |
| quarantine | active | quarantine | increment | unchanged | none | corruption | none | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | probe both | raw bytes preserved |

## Exceptional mutations

| Operation | Clock requirement | Class | Linearization | Required syncs | Before failure | After failure | Description |
|-----------|-------------------|-------|---------------|----------------|----------------|---------------|-------------|
| receipt_compaction | none | replacing_move | rename_replace | file_fsync, same_or_destination_dir_fsync | not_committed | outcome_unknown | Terminal full-job receipt replaced by byte-deterministic compact receipt at same pathname |
| wall_watermark_advancement | authenticated_wall_floor | replacing_move | rename_replace | file_fsync, same_or_destination_dir_fsync | not_committed | outcome_unknown | Monotone wall-watermark record replaced under exclusive OFD lock |

## Administrative re-entry (creates new identity)

- **requeue_dead** (from dead): Verified resubmission: creates new job identity, copies payload and safe metadata, adds old job_id as provenance (creates new identity: true)
- **requeue_quarantine** (from quarantine): Verified resubmission after full structural and payload verification: creates new job identity (creates new identity: true)
